"""Exception types raised by sidejob."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence, Tuple

if TYPE_CHECKING:
    from .records import JobRun, WaitReport


class SidejobError(Exception):
    """Base class for all sidejob errors."""


class TransientError(SidejobError):
    """Raise (or subclass) inside a job to mark a failure as retryable.

    It is part of the default retryable set, so wrapping a driver error is an
    easy way to opt into retries::

        try:
            cursor.execute(sql)
        except redshift_connector.InterfaceError as exc:
            raise sidejob.TransientError("connection dropped") from exc
    """


class UnknownJobError(SidejobError, LookupError):
    """The job ID (or handle) does not belong to this manager."""

    def __init__(self, job_id: str, hint: str = "") -> None:
        super().__init__(f"Unknown job id: {job_id!r}{hint}")
        self.job_id = job_id


class ManagerShutdownError(SidejobError, RuntimeError):
    """A job was submitted to a manager that has been shut down."""


class WorkflowClosedError(SidejobError, RuntimeError):
    """A job was submitted to a workflow that is not open (not entered or already exited)."""


class JobCancelledError(SidejobError):
    """The job was cancelled.

    Raised by ``get_result()`` for a CANCELLED job, and inside a job by
    :func:`sidejob.check_cancelled` / :func:`sidejob.sleep` once cancellation
    has been requested.
    """

    def __init__(self, job_id: str) -> None:
        super().__init__(f"Job {job_id} was cancelled before it ran to completion")
        self.job_id = job_id


class JobInterrupted(BaseException):
    """Raised *inside* a running job's thread by :meth:`JobManager.interrupt`.

    It derives from ``BaseException`` (like ``KeyboardInterrupt``) so that
    ``except Exception:`` blocks in job code do not swallow it. ``finally``
    blocks and ``with`` statements still run while it propagates.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*(args or ("job interrupted by interrupt()",)))


class WaitTimeoutError(SidejobError, TimeoutError):
    """Waiting for job(s) exceeded the allowed time.

    A wait timeout is *not* a job failure: the jobs listed in ``pending`` are
    still queued, running or retrying, and their status is left untouched.
    A timeout never means success.
    """

    def __init__(
        self,
        message: str,
        pending: Sequence[JobRun] = (),
        failures: Sequence[JobRun] = (),
    ) -> None:
        super().__init__(message)
        self.pending: Tuple[JobRun, ...] = tuple(pending)
        self.failures: Tuple[JobRun, ...] = tuple(failures)

    @property
    def pending_job_ids(self) -> Tuple[str, ...]:
        return tuple(r.job_id for r in self.pending)


class JobsFailedError(SidejobError):
    """One or more *required* jobs failed or were cancelled.

    Attributes:
        failures: Snapshots of the required jobs that did not complete.
        job_ids: Their IDs.
        exceptions: The original final exception of each FAILED job.
        report: The full :class:`~sidejob.WaitReport` (all selected jobs).
    """

    def __init__(self, failures: Sequence[JobRun], report: Optional[WaitReport] = None) -> None:
        self.failures: Tuple[JobRun, ...] = tuple(failures)
        self.report = report
        super().__init__(self._build_message())

    @property
    def job_ids(self) -> Tuple[str, ...]:
        return tuple(r.job_id for r in self.failures)

    @property
    def exceptions(self) -> Tuple[BaseException, ...]:
        return tuple(r.exception for r in self.failures if r.exception is not None)

    def _build_message(self) -> str:
        lines = [f"{len(self.failures)} required job(s) did not complete successfully:"]
        lines.extend(f"  - {r.describe()}" for r in self.failures)
        return "\n".join(lines)


class WaitAllNotCalledError(SidejobError):
    """A strict workflow exited without a successful explicit ``wait_all()``.

    The jobs were still waited for and checked; this error exists so that a
    forgotten safety barrier is caught instead of silently tolerated.
    """
