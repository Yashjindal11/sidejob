"""Managed workflows: a scoped group of jobs with a mandatory safety barrier."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, List, Mapping, Optional, Set, Tuple, Type

from ._status import JobStatus
from .errors import (
    JobsFailedError,
    SidejobError,
    WaitAllNotCalledError,
    WaitTimeoutError,
    WorkflowClosedError,
)
from .records import JobRun, WaitReport
from .retry import RetryPolicy

if TYPE_CHECKING:
    from .manager import Job, JobManager, _JobRecord

_CREATED, _OPEN, _CLOSED = "created", "open", "closed"


class Workflow:
    """A group of jobs whose success gates the surrounding code.

    Use it only as a context manager::

        with manager.workflow("nightly") as run:
            run.parallel_job(upload, df)
            ...main-thread work...
            run.wait_all()            # MANDATORY barrier
        write_success_marker()        # only reached if every required job COMPLETED

    ``run.wait_all()`` checks only jobs submitted through ``run`` -- never
    unrelated jobs from the manager or other workflows.

    Exit policy:

    * **Body succeeded:** wait for all workflow jobs (up to ``exit_timeout``),
      raise :class:`WaitTimeoutError` / :class:`JobsFailedError` on unfinished /
      failed required jobs. If some jobs were never covered by a successful
      explicit ``run.wait_all()`` (with ``raise_on_failure=True``), raise
      :class:`WaitAllNotCalledError` when ``strict=True`` (default) or log a
      warning when ``strict=False``.
    * **Body raised:** the original exception always propagates unchanged.
      If ``cancel_on_error`` (default), jobs that have not started (QUEUED or
      waiting for a retry) are cancelled, and cooperative cancellation is
      requested for running jobs (see :meth:`JobManager.cancel`). Running jobs are waited for up to
      ``exit_timeout``, except when the exception is a ``WaitTimeoutError``
      from this workflow's own barrier or a non-``Exception`` such as
      ``KeyboardInterrupt``, in which case it does not wait again. Background
      failures are logged and, on Python 3.11+, added as a note to the
      exception.
    """

    def __init__(
        self,
        manager: JobManager,
        workflow_id: str,
        name: str,
        *,
        strict: bool,
        cancel_on_error: bool,
        exit_timeout: Optional[float],
    ) -> None:
        self._manager = manager
        self.id = workflow_id
        self.name = name
        self.strict = strict
        self.cancel_on_error = cancel_on_error
        self.exit_timeout = exit_timeout
        self._records: List[_JobRecord] = []  # guarded by manager._lock
        self._job_ids: List[str] = []  # kept after exit, when _records is released
        self._verified: Set[str] = set()
        self._state = _CREATED

    @property
    def _log(self) -> logging.Logger:
        return self._manager.logger

    @property
    def _label(self) -> str:
        return f"workflow {self.id}" + (f" ({self.name})" if self.name != self.id else "")

    def _check_open(self) -> None:
        """Called by the manager with its lock held."""
        if self._state == _CREATED:
            raise WorkflowClosedError(
                f"{self._label} has not been entered; use it as `with manager.workflow() as run:`"
            )
        if self._state == _CLOSED:
            raise WorkflowClosedError(f"{self._label} has already exited; cannot submit new jobs")

    def _current_records(self) -> List[_JobRecord]:
        with self._manager._lock:
            if self._records or self._state != _CLOSED:
                return list(self._records)
            # After exit only jobs still in the manager's history are reachable.
            jobs = self._manager._jobs
            return [jobs[i] for i in self._job_ids if i in jobs]

    # ------------------------------------------------------------------ public API

    def parallel_job(
        self,
        func: Callable[..., Any],
        *args: Any,
        required: bool = True,
        retries: Optional[int] = None,
        max_attempts: Optional[int] = None,
        retry_policy: Optional[RetryPolicy] = None,
        job_name: Optional[str] = None,
        job_deadline: Optional[float] = None,
        cancel_on_deadline: bool = False,
        func_kwargs: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> Job:
        """Submit a job belonging to this workflow.

        Same options as :meth:`JobManager.parallel_job`.
        """
        return self._manager._submit(
            func,
            args,
            kwargs,
            required=required,
            retries=retries,
            max_attempts=max_attempts,
            retry_policy=retry_policy,
            job_name=job_name,
            job_deadline=job_deadline,
            cancel_on_deadline=cancel_on_deadline,
            func_kwargs=func_kwargs,
            workflow=self,
        )

    def wait_all(
        self, timeout: Optional[float] = None, raise_on_failure: bool = True
    ) -> WaitReport:
        """MANDATORY safety barrier for this workflow's jobs.

        Waits for every job submitted through this workflow (including jobs
        submitted while waiting) to reach a terminal state, then verifies them.

        Raises:
            WaitTimeoutError: Jobs still unfinished after ``timeout``. Never success.
            JobsFailedError: A required job FAILED or was CANCELLED
                (only if ``raise_on_failure=True``).

        ``raise_on_failure=False`` returns the report without raising and does
        **not** count as passing the mandatory barrier: the workflow exit will
        still enforce it.
        """
        report = self._manager._wait_all_records(
            self._current_records,
            timeout,
            raise_on_failure,
            label=self._label,
            scope=self.id,
        )
        if raise_on_failure and report.ok:
            with self._manager._lock:
                self._verified.update(r.job_id for r in report.runs)
        return report

    @property
    def jobs(self) -> Tuple[Job, ...]:
        from .manager import Job

        return tuple(Job(self._manager, r) for r in self._current_records())

    def get_runs(self) -> List[JobRun]:
        """Snapshots of this workflow's jobs, in submission order."""
        return [self._manager._snapshot(r) for r in self._current_records()]

    # ------------------------------------------------------------------ context manager

    def __enter__(self) -> Workflow:
        with self._manager._lock:
            if self._state != _CREATED:
                raise SidejobError(f"{self._label} can only be entered once")
            self._state = _OPEN
            self._manager._open_workflows.add(self.id)
        self._log.info("%s: started", self._label)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Any,
    ) -> None:
        with self._manager._lock:
            self._state = _CLOSED
        try:
            if exc is None:
                self._exit_success()
            else:
                self._exit_error(exc)  # never suppresses: the original exception propagates
        finally:
            with self._manager._lock:
                self._manager._open_workflows.discard(self.id)
                # Release strong references so history_limit/clear_history can free memory.
                self._job_ids = [r.job_id for r in self._records]
                self._records = []
                self._manager._evict_locked()

    def _exit_success(self) -> None:
        with self._manager._lock:
            all_verified = all(r.job_id in self._verified for r in self._records)
        if not all_verified:
            report = self._manager._wait_all_records(
                self._current_records,
                self.exit_timeout,
                True,
                label=f"{self._label} exit barrier",
                scope=self.id,
            )
            with self._manager._lock:
                unverified = [r.job_id for r in report.runs if r.job_id not in self._verified]
            if unverified:
                msg = (
                    f"{self._label} exited without a successful explicit wait_all() covering "
                    f"job(s) {unverified}. They have now finished successfully, but the "
                    "mandatory barrier `run.wait_all(raise_on_failure=True)` must be called "
                    "before the end of the `with` block."
                )
                if self.strict:
                    self._log.error(msg)
                    raise WaitAllNotCalledError(msg)
                self._log.warning(msg)
        self._log.info("%s: finished successfully", self._label)

    def _exit_error(self, exc: BaseException) -> None:
        try:
            # The workflow's own barrier already logged full details; don't repeat them.
            own_barrier = isinstance(exc, (JobsFailedError, WaitTimeoutError)) and (
                getattr(exc, "_sidejob_scope", None) == self.id
            )
            if own_barrier:
                self._log.info(
                    "%s: exiting with %s from wait_all() (cancel_on_error=%s)",
                    self._label,
                    type(exc).__name__,
                    self.cancel_on_error,
                )
            else:
                self._log.error(
                    "%s: body raised %s: %s; settling background jobs (cancel_on_error=%s)",
                    self._label,
                    type(exc).__name__,
                    exc,
                    self.cancel_on_error,
                )
            if self.cancel_on_error:
                self._manager._cancel_records(
                    lambda: [r for r in self._records if not r.status.is_terminal],
                    reason=f"{self._label} body raised",
                )
            skip_wait = not isinstance(exc, Exception) or own_barrier
            if skip_wait:
                pending = [r for r in self._current_records() if not r.done.is_set()]
            else:
                _, pending = self._manager._wait_until_terminal(
                    self._current_records, self.exit_timeout
                )
            runs = self.get_runs()
            failed = [r for r in runs if r.status is JobStatus.FAILED]
            cancelled = [r for r in runs if r.status is JobStatus.CANCELLED]
            if not own_barrier:
                for r in failed:
                    self._log.error("%s: background %s", self._label, r.describe())
            if pending:
                self._log.warning(
                    "%s: %d job(s) still running after the body failed (they keep running "
                    "and remain tracked): %s",
                    self._label,
                    len(pending),
                    [r.job_id for r in pending],
                )
            if not own_barrier and (failed or cancelled or pending) and hasattr(exc, "add_note"):
                exc.add_note(
                    f"sidejob {self._label}: {len(failed)} background job(s) failed "
                    f"{[r.job_id for r in failed]}, {len(cancelled)} cancelled, "
                    f"{len(pending)} unfinished. See manager.get_run(job_id) for details."
                )
        except Exception:  # never mask the original exception
            self._log.exception("%s: error while settling jobs after body failure", self._label)

    def __repr__(self) -> str:
        return f"Workflow(id={self.id!r}, name={self.name!r}, state={self._state!r})"
