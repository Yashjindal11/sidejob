"""Cooperative cancellation and per-attempt context for code running inside a job.

Python threads cannot be killed safely, so stopping running work requires the
job's cooperation. These helpers make that easy and are **no-ops outside a
job**, so the same function also works when called directly::

    import sidejob

    def export_partitions(df, partitions):
        for p in partitions:
            sidejob.check_cancelled()      # raises JobCancelledError if cancel() was requested
            write_partition(df, p)

    def poll_until_ready(client, query_id):
        while not client.is_done(query_id):
            sidejob.sleep(5)               # wakes up immediately on cancellation

    def upload(df, table):
        conn = connect()
        sidejob.on_cancel(conn.cancel)     # abort the in-flight query on cancel()
        ...
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any, Callable, Optional

from .errors import JobCancelledError

if TYPE_CHECKING:
    from .manager import JobManager, _JobRecord

__all__ = [
    "JobContext",
    "cancel_requested",
    "check_cancelled",
    "current_job",
    "on_cancel",
    "sleep",
]

_state = threading.local()


class JobContext:
    """Information about the job attempt running on the current thread.

    Obtain it with :func:`sidejob.current_job` inside a job function.

    Attributes:
        job_id: The job's ID.
        name: The job's display name.
        attempt: The current attempt number (1 = first attempt).
        max_attempts: The configured attempt budget.
        workflow_id: The workflow the job belongs to, if any.
    """

    __slots__ = ("_manager", "_rec", "attempt", "job_id", "max_attempts", "name", "workflow_id")

    def __init__(self, manager: JobManager, rec: _JobRecord, attempt: int) -> None:
        self._manager = manager
        self._rec = rec
        self.job_id: str = rec.job_id
        self.name: str = rec.name
        self.attempt = attempt
        self.max_attempts: int = rec.policy.max_attempts
        self.workflow_id: Optional[str] = rec.workflow_id

    @property
    def is_retry(self) -> bool:
        """True from the second attempt on.

        Check it before repeating a side effect: an earlier attempt may have
        committed its write before the error reached your code.
        """
        return self.attempt > 1

    @property
    def idempotency_key(self) -> str:
        """A key that stays the same across all attempts of this job.

        Store it with the data you write (or use it as a deduplication key) so a
        retry can detect that an earlier attempt already succeeded. It is the
        job ID, which is unique per submission; for a key that survives process
        restarts, pass your own (for example ``f"{table}:{run_date}"``).
        """
        return self.job_id

    @property
    def cancel_requested(self) -> bool:
        """True once cancellation of this job has been requested."""
        return self._rec.cancel_event.is_set()

    def check_cancelled(self) -> None:
        """Raise :class:`JobCancelledError` if cancellation was requested."""
        if self._rec.cancel_event.is_set():
            raise JobCancelledError(self.job_id)

    def sleep(self, seconds: float) -> None:
        """Sleep, but wake up and raise :class:`JobCancelledError` on cancellation."""
        if seconds < 0:
            raise ValueError("sleep length must be non-negative")
        if self._rec.cancel_event.wait(seconds):
            raise JobCancelledError(self.job_id)

    def on_cancel(self, callback: Callable[[], Any]) -> None:
        """Register ``callback()`` to run when cancellation is requested.

        Use it to abort blocking work from the outside, for example
        ``conn.cancel`` (psycopg2/psycopg, many DB-API drivers) or
        ``lambda: spark.sparkContext.cancelJobGroup(group)``.

        * Callbacks run on the thread that requested cancellation (or on the
          scheduler thread for ``cancel_on_deadline``), not on the job's thread.
        * They apply to the current attempt only and are discarded when it ends.
        * If cancellation was already requested, ``callback`` runs immediately.
        * Exceptions raised by a callback are logged, never propagated.
        * A callback may race with the attempt finishing, so it must be safe to
          call on an already-closed resource.
        """
        self._manager._register_cancel_callback(self._rec, self.attempt, callback)

    def __repr__(self) -> str:
        return (
            f"JobContext(job_id={self.job_id!r}, name={self.name!r}, "
            f"attempt={self.attempt}/{self.max_attempts})"
        )


def current_job() -> Optional[JobContext]:
    """The :class:`JobContext` of the job running on this thread, or ``None``.

    The context is thread-local: threads that a job starts itself do not see
    it. Pass the context object to them explicitly if needed.
    """
    ctx: Optional[JobContext] = getattr(_state, "context", None)
    return ctx


def cancel_requested() -> bool:
    """True if the current job has a pending cancellation request (False outside jobs)."""
    ctx = current_job()
    return ctx is not None and ctx.cancel_requested


def check_cancelled() -> None:
    """Raise :class:`JobCancelledError` if the current job should stop (no-op outside jobs)."""
    ctx = current_job()
    if ctx is not None:
        ctx.check_cancelled()


def sleep(seconds: float) -> None:
    """Cancellation-aware ``time.sleep`` (plain ``time.sleep`` outside jobs)."""
    ctx = current_job()
    if ctx is None:
        time.sleep(seconds)
    else:
        ctx.sleep(seconds)


def on_cancel(callback: Callable[[], Any]) -> None:
    """Register a cancellation hook for the current attempt (no-op outside jobs).

    See :meth:`JobContext.on_cancel`.
    """
    ctx = current_job()
    if ctx is not None:
        ctx.on_cancel(callback)


def _set_context(ctx: Optional[JobContext]) -> Optional[JobContext]:
    prev: Optional[JobContext] = getattr(_state, "context", None)
    _state.context = ctx
    return prev
