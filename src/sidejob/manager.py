"""The job manager: one bounded thread pool, a thread-safe registry, and safety barriers."""

from __future__ import annotations

import ctypes
import functools
import heapq
import itertools
import logging
import platform
import sys
import threading
import time
import traceback as _traceback
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from ._status import JobStatus
from .context import JobContext, _set_context, current_job
from .errors import (
    JobCancelledError,
    JobInterrupted,
    JobsFailedError,
    ManagerShutdownError,
    SidejobError,
    UnknownJobError,
    WaitTimeoutError,
)
from .records import AttemptRecord, JobEvent, JobRun, WaitReport
from .retry import RetryPolicy, resolve_max_attempts

if TYPE_CHECKING:
    from .workflow import Workflow

__all__ = ["Job", "JobManager", "initialize"]

DEFAULT_MAX_WORKERS = 4
LOGGER_NAME = "sidejob"
_HANDLER_MARK = "_sidejob_console_handler"
_manager_counter = itertools.count(1)

JobRef = Union["Job", str]
StatusFilter = Union[JobStatus, str, Iterable[Union[JobStatus, str]], None]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _callable_name(func: Callable[..., Any]) -> str:
    while isinstance(func, functools.partial):
        func = func.func
    name = getattr(func, "__qualname__", None) or getattr(func, "__name__", None)
    return str(name) if name else type(func).__name__


#: Whether interrupt() can raise inside a running thread (CPython only).
INTERRUPT_SUPPORTED = platform.python_implementation() == "CPython" and hasattr(ctypes, "pythonapi")


def _async_raise(ident: int, exc_type: Optional[Type[BaseException]]) -> int:
    """Schedule ``exc_type`` in thread ``ident`` (``None`` clears a pending one).

    Uses CPython's ``PyThreadState_SetAsyncExc``. The exception is raised the
    next time that thread executes Python bytecode. Returns the number of
    threads affected.
    """
    if not INTERRUPT_SUPPORTED:
        return 0
    arg = ctypes.py_object(exc_type) if exc_type is not None else None
    n = int(ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(ident), arg))
    if n > 1:  # pragma: no cover - would mean an invalid thread id; undo
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(ident), None)
        return 0
    return n


class _DrainInterrupt(BaseException):
    """Private placeholder used to discard a pending, undelivered interrupt."""


def _drain_point() -> None:
    """A Python call: a pending async exception is delivered at its entry."""


def _drain_pending_interrupt(ident: int) -> None:
    """Discard any undelivered interrupt for the *current* thread.

    Must run on the job's own thread. Instead of clearing with NULL, which on
    CPython 3.12 leaves the interpreter's eval-breaker flag set and slows every
    thread, the pending exception is replaced by a private one and consumed here.
    """
    try:
        if _async_raise(ident, _DrainInterrupt) == 1:
            for _ in range(8):
                _drain_point()
    except _DrainInterrupt:
        pass


def _release_leaked_logging_locks() -> None:
    """Backstop, run on the job's thread after an interrupted attempt.

    Once the job function has exited, this thread cannot legitimately hold a
    logging lock. If the interrupt landed between a handler's acquire and its
    ``try`` (a tiny race the safe-point check can't fully close in pure Python),
    release it so other threads don't deadlock.
    """
    locks = [getattr(logging, "_lock", None)]
    for ref in list(getattr(logging, "_handlerList", [])):
        handler = ref()
        if handler is not None:
            locks.append(getattr(handler, "lock", None))
    for lock in locks:
        is_owned = getattr(lock, "_is_owned", None)
        if is_owned is None:
            continue
        try:
            while is_owned():
                lock.release()  # type: ignore[union-attr]
        except RuntimeError:  # pragma: no cover - defensive
            pass


#: Modules whose code must never be interrupted: an exception landing between a
#: lock acquire and its ``try`` there would leak the lock (e.g. a logging handler).
_UNSAFE_INTERRUPT_MODULES = (
    "logging",
    "threading",
    "queue",
    "concurrent.futures",
    "importlib",
    "_weakrefset",
    "weakref",
    "sidejob",
)


def _at_safe_point(ident: int) -> bool:
    """True if thread ``ident`` is running the job's own code, not lock-sensitive code.

    Walks the thread's stack from the innermost frame out to sidejob's
    ``_invoke`` frame (the call into the job function). Called with the GIL
    held, so the snapshot is current at the moment it is taken.
    """
    frame = sys._current_frames().get(ident)
    while frame is not None:
        if frame.f_code is _INVOKE_CODE:
            return True
        module = frame.f_globals.get("__name__", "") or ""
        for unsafe in _UNSAFE_INTERRUPT_MODULES:
            if module == unsafe or module.startswith(unsafe + "."):
                return False
        frame = frame.f_back
    return False  # not inside the job's function at all


def _release_frames(exc: BaseException) -> None:
    """Drop local variables held by an exception's traceback (and its chain).

    The traceback text is kept separately; this frees large objects such as
    DataFrames that were local variables in the failing code.
    """
    seen = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if cur.__traceback__ is not None:
            _traceback.clear_frames(cur.__traceback__)
        cur = cur.__cause__ or cur.__context__


def _validate_timeout(timeout: Optional[float]) -> None:
    if timeout is not None and timeout < 0:
        raise ValueError("timeout must be >= 0 or None")


class _AttemptState:
    """Mutable attempt record; only touched while holding the manager lock."""

    __slots__ = (
        "duration",
        "exception",
        "finished_at",
        "number",
        "retry_decision",
        "retry_delay",
        "started_at",
        "started_mono",
        "succeeded",
        "traceback",
    )

    def __init__(self, number: int) -> None:
        self.number = number
        self.started_at = _now()
        self.started_mono = time.monotonic()
        self.finished_at: Optional[datetime] = None
        self.duration: Optional[float] = None
        self.succeeded: Optional[bool] = None
        self.exception: Optional[BaseException] = None
        self.traceback: Optional[str] = None
        self.retry_decision: Optional[str] = None
        self.retry_delay: Optional[float] = None

    def close(self, succeeded: bool) -> None:
        self.finished_at = _now()
        self.duration = time.monotonic() - self.started_mono
        self.succeeded = succeeded

    def freeze(self) -> AttemptRecord:
        exc = self.exception
        return AttemptRecord(
            number=self.number,
            started_at=self.started_at,
            finished_at=self.finished_at,
            duration=self.duration,
            succeeded=self.succeeded,
            error_type=type(exc).__name__ if exc is not None else None,
            error_message=str(exc) if exc is not None else None,
            exception=exc,
            traceback=self.traceback,
            retry_decision=self.retry_decision,
            retry_delay=self.retry_delay,
        )


class _JobRecord:
    """Internal mutable job state. All mutation happens under the manager lock."""

    def __init__(
        self,
        job_id: str,
        func: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        name: str,
        required: bool,
        policy: RetryPolicy,
        deadline: Optional[float],
        cancel_on_deadline: bool,
        workflow_id: Optional[str],
    ) -> None:
        self.job_id = job_id
        # Inputs are released when the job reaches a terminal state.
        self.func: Optional[Callable[..., Any]] = func
        self.args = args
        self.kwargs = kwargs
        self.name = name
        self.required = required
        self.policy = policy
        self.deadline = deadline
        self.cancel_on_deadline = cancel_on_deadline
        self.workflow_id = workflow_id
        self.status = JobStatus.QUEUED
        self.submitted_at = _now()
        self.started_at: Optional[datetime] = None
        self.started_mono: Optional[float] = None
        self.finished_at: Optional[datetime] = None
        self.finished_mono: Optional[float] = None
        self.attempt = 0
        self.result: Any = None
        self.exception: Optional[BaseException] = None
        self.attempts: List[_AttemptState] = []
        self.events: List[JobEvent] = []
        self.overdue_flagged = False
        self.done = threading.Event()
        # Set by _finalize; wakes the in-thread backoff fallback.
        self.wake = threading.Event()
        self.cancel_event = threading.Event()
        self.cancel_reason: Optional[str] = None
        self.cancel_callbacks: List[Callable[[], Any]] = []
        # Thread ident while the job's own function runs; interrupt() only targets
        # a thread while this is set, so the exception can't hit sidejob or another job.
        self.worker_ident: Optional[int] = None
        # Set by interrupt(); checked when arming, so an interrupt that arrives
        # between RUNNING and the function call is not lost.
        self.interrupt_requested = False
        self.interrupt_injected = False
        self.interrupt_polls = 0
        self.future: Optional[Future[None]] = None

    def release_inputs(self) -> None:
        self.func = None
        self.args = ()
        self.kwargs = {}

    def is_overdue(self, now_mono: float) -> bool:
        if self.overdue_flagged:
            return True
        if self.deadline is None or self.started_mono is None:
            return False
        end = self.finished_mono if self.finished_mono is not None else now_mono
        return end - self.started_mono >= self.deadline


class Job:
    """Lightweight handle returned by ``parallel_job()``.

    Holds the job ID and convenience methods that delegate to the manager.
    Comparing/hashing uses the job ID. A handle keeps its job's history alive,
    so it still works after the job was evicted by ``history_limit`` or
    ``clear_history()``; look-ups by ID string do not.
    """

    __slots__ = ("_manager", "_rec", "id", "name", "required", "workflow_id")

    def __init__(self, manager: JobManager, rec: _JobRecord) -> None:
        self._manager = manager
        self._rec = rec
        self.id = rec.job_id
        self.name = rec.name
        self.required = rec.required
        self.workflow_id = rec.workflow_id

    @property
    def status(self) -> JobStatus:
        return self._manager.get_status(self)

    def snapshot(self) -> JobRun:
        return self._manager.get_run(self)

    def wait(self, timeout: Optional[float] = None) -> JobRun:
        return self._manager.wait(self, timeout=timeout)

    def result(self, timeout: Optional[float] = None) -> Any:
        return self._manager.get_result(self, timeout=timeout)

    def cancel(self) -> bool:
        return self._manager.cancel(self)

    def interrupt(self, timeout: Optional[float] = 5.0) -> JobStatus:
        return self._manager.interrupt(self, timeout=timeout)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Job) and other._manager is self._manager and other.id == self.id

    def __hash__(self) -> int:
        return hash(self.id)

    def __repr__(self) -> str:
        return f"Job(id={self.id!r}, name={self.name!r}, required={self.required})"


class JobManager:
    """Owns one bounded thread pool and the complete history of jobs submitted to it.

    Create one with :func:`sidejob.initialize`. All public methods are thread-safe.
    History is retained for the lifetime of the manager object (including
    after shutdown) unless ``history_limit`` is set or :meth:`clear_history`
    is called. A job's function and arguments are released as soon as it
    finishes.
    """

    def __init__(
        self,
        max_workers: int = DEFAULT_MAX_WORKERS,
        *,
        retry_policy: Optional[RetryPolicy] = None,
        log_level: Optional[int] = logging.INFO,
        name: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        log_to_console: Optional[bool] = None,
        history_limit: Optional[int] = None,
    ) -> None:
        if not isinstance(max_workers, int) or isinstance(max_workers, bool) or max_workers < 1:
            raise ValueError("max_workers must be an int >= 1")
        if history_limit is not None and (
            not isinstance(history_limit, int)
            or isinstance(history_limit, bool)
            or history_limit < 0
        ):
            raise ValueError("history_limit must be None or an int >= 0")
        self.name = name or f"manager-{next(_manager_counter)}"
        self._max_workers = max_workers
        self._policy = retry_policy if retry_policy is not None else RetryPolicy()
        self._lock = threading.RLock()
        self._jobs: Dict[str, _JobRecord] = {}
        self._seq = itertools.count(1)
        self._wf_seq = itertools.count(1)
        self._closed = False
        self._cancel_retries = False
        self._history_limit = history_limit
        self._evicted = 0
        self._open_workflows: Set[str] = set()
        # Retry/deadline timers: heap of (due_monotonic, seq, kind, record), served by one
        # scheduler thread that runs only while timers are pending.
        self._cond = threading.Condition(self._lock)
        self._timers: List[Tuple[float, int, str, _JobRecord]] = []
        self._timer_seq = itertools.count()
        self._scheduler: Optional[threading.Thread] = None
        self._executor_released = False
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix=f"sidejob-{self.name}"
        )
        self.logger = (
            logger if logger is not None else logging.getLogger(f"{LOGGER_NAME}.{self.name}")
        )
        if log_level is not None:
            self.logger.setLevel(log_level)
        if logger is None:
            _configure_console_handler(log_to_console)
        self.logger.debug("manager %s initialized (max_workers=%d)", self.name, max_workers)

    # ------------------------------------------------------------------ properties

    @property
    def max_workers(self) -> int:
        return self._max_workers

    @property
    def retry_policy(self) -> RetryPolicy:
        """The default retry policy applied to jobs submitted to this manager."""
        return self._policy

    @property
    def is_shutdown(self) -> bool:
        return self._closed

    # ------------------------------------------------------------------ submission

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
        """Submit ``func(*args, **kwargs)`` for background execution.

        Sidejob options (keyword-only, never passed to ``func``):
            required: Whether this job's success is needed for the workflow to
                succeed. Defaults to ``True``.
            retries / max_attempts: Per-job attempt budget (mutually exclusive):
                ``retries`` counts retries after the first attempt,
                ``max_attempts`` counts total attempts.
            retry_policy: A full :class:`RetryPolicy` for this job.
            job_name: Display name (defaults to the function's name).
            job_deadline: Seconds after the first attempt starts after which
                the job is reported as overdue (logged as soon as it happens).
            cancel_on_deadline: Also request cooperative cancellation when the
                deadline passes. A thread is never killed: the job stops only if
                it checks :func:`sidejob.check_cancelled`, uses
                :func:`sidejob.sleep`, or registers :func:`sidejob.on_cancel`.
            func_kwargs: Keyword arguments passed verbatim to ``func``; use this
                if ``func`` itself takes a parameter named like a sidejob option.

        Returns:
            A :class:`Job` handle.

        Raises:
            ManagerShutdownError: The manager has been shut down.
        """
        return self._submit(
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
            workflow=None,
        )

    def workflow(
        self,
        name: Optional[str] = None,
        *,
        strict: bool = True,
        cancel_on_error: bool = True,
        exit_timeout: Optional[float] = None,
    ) -> Workflow:
        """Create a managed workflow; use it as a context manager.

        See :class:`sidejob.Workflow` for the exit policy.
        """
        from .workflow import Workflow

        _validate_timeout(exit_timeout)
        with self._lock:
            if self._closed:
                raise ManagerShutdownError(f"manager {self.name} is shut down")
            wf_id = f"wf-{next(self._wf_seq):04d}"
        return Workflow(
            self,
            wf_id,
            name or wf_id,
            strict=strict,
            cancel_on_error=cancel_on_error,
            exit_timeout=exit_timeout,
        )

    def _submit(
        self,
        func: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        *,
        required: bool,
        retries: Optional[int],
        max_attempts: Optional[int],
        retry_policy: Optional[RetryPolicy],
        job_name: Optional[str],
        job_deadline: Optional[float],
        cancel_on_deadline: bool,
        func_kwargs: Optional[Mapping[str, Any]],
        workflow: Optional[Workflow],
    ) -> Job:
        if not callable(func):
            raise TypeError(f"func must be callable, got {type(func).__name__}")
        if func_kwargs:
            overlap = set(func_kwargs) & set(kwargs)
            if overlap:
                raise TypeError(f"duplicate keyword argument(s) for func: {sorted(overlap)}")
            kwargs = {**kwargs, **func_kwargs}
        if job_deadline is not None and job_deadline <= 0:
            raise ValueError("job_deadline must be > 0 seconds")
        if cancel_on_deadline and job_deadline is None:
            raise ValueError("cancel_on_deadline=True requires job_deadline")
        if retry_policy is not None and not isinstance(retry_policy, RetryPolicy):
            raise TypeError("retry_policy must be a sidejob.RetryPolicy")
        policy = retry_policy if retry_policy is not None else self._policy
        attempts = resolve_max_attempts(retries, max_attempts)
        if attempts is not None:
            policy = policy.with_attempts(max_attempts=attempts)
        name = job_name or _callable_name(func)

        with self._lock:
            if self._closed:
                raise ManagerShutdownError(
                    f"manager {self.name} is shut down; cannot submit {name!r}"
                )
            if workflow is not None:
                workflow._check_open()
            job_id = f"{next(self._seq):04d}-{uuid.uuid4().hex[:8]}"
            rec = _JobRecord(
                job_id,
                func,
                tuple(args),
                dict(kwargs),
                name,
                bool(required),
                policy,
                job_deadline,
                bool(cancel_on_deadline),
                workflow.id if workflow is not None else None,
            )
            self._jobs[job_id] = rec
            try:
                rec.future = self._executor.submit(self._execute, rec)
            except RuntimeError as exc:  # interpreter shutting down
                del self._jobs[job_id]
                self._closed = True
                raise ManagerShutdownError(f"cannot submit {name!r}: {exc}") from exc
            if workflow is not None:
                workflow._records.append(rec)
            msg = (
                f"submitted ({'required' if rec.required else 'optional'}, "
                f"max_attempts={policy.max_attempts}"
                + (f", workflow={rec.workflow_id}" if rec.workflow_id else "")
                + ")"
            )
            self._add_event(rec, "submitted", msg)
        self._log(logging.INFO, rec, msg)
        return Job(self, rec)

    # ------------------------------------------------------------------ execution

    def _execute(self, rec: _JobRecord) -> None:
        """Pool entry point: run one attempt (more only in the no-scheduler fallback).

        ``done`` is set here only if *this* call finalized the job; a job finalized
        by a concurrent cancel is signalled by the cancelling thread after it logs.
        """
        finalized = False
        try:
            while True:
                inline_delay, finalized = self._run_attempt(rec)
                if inline_delay is None:
                    break
                # Fallback when no scheduler thread can be started (interpreter exit):
                # wait out the backoff here. ``wake`` is set if the job is cancelled.
                rec.wake.wait(inline_delay)
        except BaseException as exc:  # pragma: no cover - defensive safety net
            with self._lock:
                if not rec.status.is_terminal:
                    self._finalize(rec, JobStatus.FAILED, exception=exc)
                    finalized = True
            self._log(
                logging.CRITICAL, rec, "internal sidejob error; job marked FAILED", exc_info=exc
            )
        finally:
            if finalized:
                rec.done.set()
                self._after_terminal()

    def _run_attempt(self, rec: _JobRecord) -> Tuple[Optional[float], bool]:
        """Run one attempt.

        Returns ``(inline_delay, finalized)``: a delay only if the retry must be
        waited out on this thread, and whether this call moved the job to a
        terminal state.
        """
        max_attempts = rec.policy.max_attempts
        with self._lock:
            if rec.status not in (JobStatus.QUEUED, JobStatus.RETRYING):
                return None, False  # cancelled while queued
            rec.attempt += 1
            n = rec.attempt
            att = _AttemptState(n)
            if rec.started_mono is None:
                rec.started_at = att.started_at
                rec.started_mono = att.started_mono
                if rec.deadline is not None:
                    self._schedule_locked(rec.started_mono + rec.deadline, "deadline", rec)
            rec.status = JobStatus.RUNNING
            rec.attempts.append(att)
            self._add_event(rec, "attempt_started", f"attempt {n}/{max_attempts} started", n)
            func, args, kwargs = rec.func, rec.args, rec.kwargs
        self._log(logging.INFO, rec, "started", attempt=n)
        assert func is not None

        prev_ctx = _set_context(JobContext(self, rec, n))
        ok, value = self._invoke(rec, func, args, kwargs)
        _set_context(prev_ctx)
        del func, args, kwargs
        if not ok:
            return self._attempt_failed(rec, att, value)
        result = value
        with self._lock:
            att.close(succeeded=True)
            rec.cancel_callbacks.clear()
            was_cancel_requested = rec.cancel_event.is_set()
            self._finalize(rec, JobStatus.COMPLETED, result=result)
            overdue_msg = self._overdue_check_locked(rec)
            duration = rec.finished_mono - rec.started_mono  # type: ignore[operator]
        if overdue_msg:
            self._log(logging.WARNING, rec, overdue_msg, attempt=n)
        note = " (cancellation had been requested, but the job finished normally)"
        self._log(
            logging.INFO,
            rec,
            f"COMPLETED in {duration:.3f}s" + (note if was_cancel_requested else ""),
            attempt=n,
        )
        return None, True

    def _invoke(
        self,
        rec: _JobRecord,
        func: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
    ) -> Tuple[bool, Any]:
        """Call the job function, keeping interrupt() confined to it.

        Returns ``(True, result)`` or ``(False, exception)``. interrupt() injects
        at most one ``JobInterrupted`` per job, only while ``worker_ident`` is set
        (under the lock) and when the thread is at a safe point. If it surfaces just
        outside ``func`` it is caught by the loop below, and an injection not yet
        delivered is drained while disarming, so it can never escape into
        sidejob's code or the next job on this worker.
        """
        ident = threading.get_ident()
        outcome: Optional[Tuple[bool, Any]] = None
        armed = False
        while True:
            try:
                if not armed:
                    with self._lock:
                        rec.worker_ident = ident
                        preempted = rec.interrupt_requested
                    armed = True
                    if preempted and outcome is None:
                        outcome = (False, JobInterrupted())
                if outcome is None:
                    try:
                        outcome = (True, func(*args, **kwargs))
                    except BaseException as exc:
                        outcome = (False, exc)
                with self._lock:
                    rec.worker_ident = None
                    if rec.interrupt_injected:
                        _drain_pending_interrupt(ident)
                if rec.interrupt_injected:
                    _release_leaked_logging_locks()
                return outcome
            except JobInterrupted as exc:
                armed = True
                if outcome is None:
                    outcome = (False, exc)

    def _attempt_failed(
        self, rec: _JobRecord, att: _AttemptState, exc: BaseException
    ) -> Tuple[Optional[float], bool]:
        n = att.number
        tb_text = "".join(_traceback.format_exception(type(exc), exc, exc.__traceback__))
        decision, delay = self._decide(rec, exc, n)
        err = f"{type(exc).__name__}: {exc}"
        with self._lock:
            # Re-checked under the lock so a concurrent cancel()/shutdown(cancel_pending=True)
            # can never be followed by another attempt.
            cancel_reason: Optional[str] = None
            if rec.cancel_event.is_set():
                cancel_reason = rec.cancel_reason or "cancellation requested"
            elif decision == "retry" and self._cancel_retries:
                cancel_reason = "manager is shutting down with cancel_pending=True"
            if cancel_reason is not None:
                decision, delay = "cancelled", None
            att.exception = exc
            att.traceback = tb_text
            att.retry_decision = decision
            att.retry_delay = delay
            att.close(succeeded=False)
            rec.cancel_callbacks.clear()
            self._add_event(rec, "attempt_failed", f"attempt {n} failed: {err} -> {decision}", n)
            if decision == "retry":
                rec.status = JobStatus.RETRYING
                self._add_event(rec, "retry_scheduled", f"retry in {delay:.2f}s", n)
            elif decision == "cancelled":
                self._finalize(rec, JobStatus.CANCELLED)
            else:
                self._finalize(rec, JobStatus.FAILED, exception=exc)
            overdue_msg = self._overdue_check_locked(rec)
        if decision in ("retry", "cancelled"):
            # Only the final FAILED exception keeps its frames (for debugging);
            # earlier attempts keep their traceback text but release locals.
            _release_frames(exc)
        if overdue_msg:
            self._log(logging.WARNING, rec, overdue_msg, attempt=n)

        if decision == "retry":
            assert delay is not None
            self._log(
                logging.WARNING,
                rec,
                f"failed with retryable error {err}; RETRYING in {delay:.2f}s",
                attempt=n,
            )
            # Scheduled after logging so the retry's own log lines always come later.
            # The worker thread is released during the backoff.
            with self._lock:
                if rec.status is not JobStatus.RETRYING:
                    return None, False  # cancelled meanwhile; the canceller signals done
                if self._schedule_locked(time.monotonic() + delay, "retry", rec):
                    return None, False
            return delay, False
        if decision == "cancelled":
            self._log(
                logging.WARNING,
                rec,
                f"failed ({err}) after {cancel_reason} -> CANCELLED (not retried)",
                attempt=n,
            )
            return None, True
        reason = (
            "non-retryable error"
            if decision == "not_retryable"
            else f"retries exhausted after {n} attempt(s)"
        )
        self._log(
            logging.ERROR,
            rec,
            f"FAILED ({reason}): {err}",
            attempt=n,
            exc_info=exc,
        )
        return None, True

    def _decide(self, rec: _JobRecord, exc: BaseException, n: int) -> Tuple[str, Optional[float]]:
        try:
            retryable = rec.policy.is_retryable(exc)
        except Exception as pred_exc:  # a broken user predicate must not hang the job
            self._log(
                logging.ERROR,
                rec,
                f"retry predicate raised {type(pred_exc).__name__}: {pred_exc}; "
                "treating as not retryable",
                attempt=n,
            )
            retryable = False
        if not retryable:
            return "not_retryable", None
        if n >= rec.policy.max_attempts:
            return "exhausted", None
        return "retry", rec.policy.compute_delay(n)

    def _finalize(
        self,
        rec: _JobRecord,
        status: JobStatus,
        *,
        result: Any = None,
        exception: Optional[BaseException] = None,
    ) -> None:
        """Move ``rec`` to a terminal state. Caller must hold the lock and set ``rec.done``."""
        rec.status = status
        rec.finished_at = _now()
        rec.finished_mono = time.monotonic()
        rec.result = result
        rec.exception = exception
        rec.cancel_callbacks.clear()
        rec.release_inputs()
        if status is JobStatus.COMPLETED:
            self._add_event(rec, "completed", "completed successfully", rec.attempt)
        elif status is JobStatus.FAILED:
            self._add_event(
                rec, "failed", f"failed: {type(exception).__name__}: {exception}", rec.attempt
            )
        else:
            self._add_event(rec, "cancelled", "cancelled", rec.attempt)
        # ``done`` is set by the caller only after the terminal log line is emitted,
        # so waiters never observe a final state before it has been logged.
        rec.wake.set()
        self._cond.notify_all()  # lets the scheduler drop this job's timers

    # ------------------------------------------------------------------ timers

    def _schedule_locked(self, due: float, kind: str, rec: _JobRecord) -> bool:
        """Add a timer (caller holds the lock). Returns False if no scheduler can run."""
        entry = (due, next(self._timer_seq), kind, rec)
        heapq.heappush(self._timers, entry)
        if self._scheduler is not None:
            self._cond.notify_all()
            return True
        thread = threading.Thread(
            target=self._scheduler_loop, name=f"sidejob-{self.name}-scheduler", daemon=False
        )
        try:
            thread.start()
        except RuntimeError:  # e.g. interpreter shutting down
            self._timers.remove(entry)
            heapq.heapify(self._timers)
            return False
        self._scheduler = thread
        return True

    def _purge_timers_locked(self) -> None:
        def stale(entry: Tuple[float, int, str, _JobRecord]) -> bool:
            rec = entry[3]
            if rec.status.is_terminal:
                return True
            if entry[2] == "interrupt":
                return rec.status is not JobStatus.RUNNING or rec.interrupt_injected
            return entry[2] == "retry" and rec.status is not JobStatus.RETRYING

        if any(stale(e) for e in self._timers):
            self._timers = [e for e in self._timers if not stale(e)]
            heapq.heapify(self._timers)

    def _scheduler_loop(self) -> None:
        """Fires retry, deadline and interrupt timers; exits as soon as none are pending."""
        while True:
            fired: List[Tuple[float, int, str, _JobRecord]] = []
            with self._cond:
                while True:
                    self._purge_timers_locked()
                    if not self._timers:
                        self._scheduler = None
                        return
                    wait_for = self._timers[0][0] - time.monotonic()
                    if wait_for <= 0:
                        break
                    self._cond.wait(wait_for)
                now = time.monotonic()
                while self._timers and self._timers[0][0] <= now:
                    fired.append(heapq.heappop(self._timers))
            for _, _, kind, rec in fired:
                try:
                    if kind == "retry":
                        self._dispatch_retry(rec)
                    elif kind == "interrupt":
                        self._retry_interrupt(rec)
                    else:
                        self._deadline_reached(rec)
                except Exception:  # pragma: no cover - defensive
                    self.logger.exception("sidejob scheduler error for job %s", rec.job_id)

    def _dispatch_retry(self, rec: _JobRecord) -> None:
        with self._lock:
            if rec.status is not JobStatus.RETRYING:
                return
            try:
                rec.future = self._executor.submit(self._execute, rec)
                return
            except RuntimeError:
                pass
        self._log(
            logging.WARNING,
            rec,
            "thread pool unavailable (interpreter shutting down); running the retry on the "
            "scheduler thread",
        )
        self._execute(rec)

    def _deadline_reached(self, rec: _JobRecord) -> None:
        with self._lock:
            msg = self._overdue_check_locked(rec)
            # Independent of who logged the overdue warning first (a status query may have).
            cancel = (
                rec.cancel_on_deadline
                and not rec.status.is_terminal
                and not rec.cancel_event.is_set()
                and rec.is_overdue(time.monotonic())
            )
        if msg:
            self._log(logging.WARNING, rec, msg)
        if cancel:
            self._cancel_records(
                lambda: [] if rec.status.is_terminal else [rec],
                reason=f"exceeded its {rec.deadline:g}s deadline (cancel_on_deadline=True)",
            )

    # ------------------------------------------------------------------ cancel hooks

    def _register_cancel_callback(
        self, rec: _JobRecord, attempt: int, callback: Callable[[], Any]
    ) -> None:
        if not callable(callback):
            raise TypeError("on_cancel() expects a callable")
        with self._lock:
            if rec.attempt != attempt or rec.status is not JobStatus.RUNNING:
                return  # that attempt is over
            if not rec.cancel_event.is_set():
                rec.cancel_callbacks.append(callback)
                return
        self._run_cancel_callbacks(rec, [callback])

    def _run_cancel_callbacks(self, rec: _JobRecord, callbacks: List[Callable[[], Any]]) -> None:
        for cb in callbacks:
            try:
                cb()
            except Exception as exc:
                self._log(
                    logging.ERROR,
                    rec,
                    f"on_cancel hook {_callable_name(cb)} raised {type(exc).__name__}: {exc}",
                    exc_info=exc,
                )

    # ------------------------------------------------------------------ retention

    def _after_terminal(self) -> None:
        """Housekeeping after a job settles: retention policy and executor release."""
        with self._lock:
            self._evict_locked()
        self._maybe_release_executor()

    def _evict_locked(self) -> int:
        """Apply ``history_limit``: drop the oldest COMPLETED jobs outside open workflows.

        FAILED/CANCELLED and unfinished jobs are never evicted automatically, so
        an unhandled failure can never disappear before ``wait_all()`` sees it.
        """
        limit = self._history_limit
        if limit is None:
            return 0
        excess = len(self._jobs) - limit
        if excess <= 0:
            return 0
        victims: List[str] = []
        for job_id, rec in self._jobs.items():
            if len(victims) >= excess:
                break
            if rec.status is JobStatus.COMPLETED and rec.workflow_id not in self._open_workflows:
                victims.append(job_id)
        for job_id in victims:
            del self._jobs[job_id]
        self._evicted += len(victims)
        return len(victims)

    def clear_history(self, *, include_failed: bool = False) -> int:
        """Forget finished jobs to free memory. Returns the number removed.

        Removes COMPLETED jobs (and FAILED/CANCELLED ones too if
        ``include_failed=True``). Unfinished jobs and jobs of open workflows are
        always kept. Removed jobs are no longer returned by queries or checked
        by ``manager.wait_all()``; ``Job`` handles you still hold keep working.
        """
        removable = (
            {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}
            if include_failed
            else {JobStatus.COMPLETED}
        )
        with self._lock:
            victims = [
                job_id
                for job_id, rec in self._jobs.items()
                if rec.status in removable and rec.workflow_id not in self._open_workflows
            ]
            for job_id in victims:
                del self._jobs[job_id]
            self._evicted += len(victims)
        self.logger.info(
            "manager %s: cleared %d finished job(s) from history (include_failed=%s)",
            self.name,
            len(victims),
            include_failed,
        )
        return len(victims)

    def _maybe_release_executor(self, join: bool = False) -> None:
        """Shut the pool down once the manager is closed and no job is unfinished.

        Kept open until then so that scheduled retries can still be dispatched.
        """
        with self._lock:
            if not self._closed:
                return
            if not self._executor_released:
                if any(not r.status.is_terminal for r in self._jobs.values()):
                    return
                self._executor_released = True
        self._executor.shutdown(wait=join)

    # ------------------------------------------------------------------ logging/events

    def _add_event(
        self, rec: _JobRecord, kind: str, message: str, attempt: Optional[int] = None
    ) -> None:
        rec.events.append(
            JobEvent(_now(), kind, message, rec.attempt if attempt is None else attempt)
        )

    def _log(
        self,
        level: int,
        rec: _JobRecord,
        message: str,
        *,
        attempt: Optional[int] = None,
        exc_info: Optional[BaseException] = None,
    ) -> None:
        if not self.logger.isEnabledFor(level):
            return
        n = rec.attempt if attempt is None else attempt
        where = f" attempt {n}/{rec.policy.max_attempts}" if n else ""
        self.logger.log(
            level,
            "job %s [%s]%s: %s",
            rec.job_id,
            rec.name,
            where,
            message,
            exc_info=exc_info,
            extra={
                "sidejob_job_id": rec.job_id,
                "sidejob_job_name": rec.name,
                "sidejob_attempt": n,
                "sidejob_max_attempts": rec.policy.max_attempts,
                "sidejob_workflow_id": rec.workflow_id,
            },
        )

    def _overdue_check_locked(self, rec: _JobRecord) -> Optional[str]:
        if rec.overdue_flagged or rec.deadline is None or rec.started_mono is None:
            return None
        if not rec.is_overdue(time.monotonic()):
            return None
        rec.overdue_flagged = True
        if rec.status.is_terminal:
            msg = f"OVERDUE: finished after its {rec.deadline:g}s deadline"
        elif rec.cancel_on_deadline:
            msg = (
                f"OVERDUE: exceeded its {rec.deadline:g}s deadline (status {rec.status.value}); "
                "requesting cooperative cancellation"
            )
        else:
            msg = (
                f"OVERDUE: exceeded its {rec.deadline:g}s deadline (status {rec.status.value}). "
                "It keeps running: threads cannot be forcibly stopped (see cancel_on_deadline)."
            )
        self._add_event(rec, "overdue", msg)
        return msg

    def _check_overdue(self, rec: _JobRecord) -> None:
        with self._lock:
            msg = self._overdue_check_locked(rec)
        if msg:
            self._log(logging.WARNING, rec, msg)

    # ------------------------------------------------------------------ lookup/snapshots

    def _resolve(self, job: JobRef) -> _JobRecord:
        if isinstance(job, Job):
            if job._manager is not self:
                raise UnknownJobError(job.id)
            return job._rec
        if not isinstance(job, str):
            raise TypeError(f"expected a Job handle or job id string, got {type(job).__name__}")
        with self._lock:
            rec = self._jobs.get(job)
            evicted = self._evicted
        if rec is None:
            hint = (
                f" ({evicted} finished job(s) were removed from history by history_limit or "
                "clear_history(); keep the Job handle to access them)"
                if evicted
                else ""
            )
            raise UnknownJobError(job, hint)
        return rec

    def _snapshot(self, rec: _JobRecord) -> JobRun:
        with self._lock:
            now_mono = time.monotonic()
            exc = rec.exception
            if exc is None and rec.status is JobStatus.CANCELLED and rec.attempts:
                last = rec.attempts[-1].exception
            else:
                last = exc
            duration = (
                rec.finished_mono - rec.started_mono
                if rec.finished_mono is not None and rec.started_mono is not None
                else None
            )
            return JobRun(
                job_id=rec.job_id,
                name=rec.name,
                status=rec.status,
                required=rec.required,
                submitted_at=rec.submitted_at,
                started_at=rec.started_at,
                finished_at=rec.finished_at,
                duration=duration,
                attempt=rec.attempt,
                max_attempts=rec.policy.max_attempts,
                result=rec.result,
                exception=exc,
                error_type=type(last).__name__ if last is not None else None,
                error_message=str(last) if last is not None else None,
                attempts=tuple(a.freeze() for a in rec.attempts),
                events=tuple(rec.events),
                workflow_id=rec.workflow_id,
                deadline=rec.deadline,
                overdue=rec.is_overdue(now_mono),
                cancel_requested=rec.cancel_event.is_set(),
            )

    def _all_records(self) -> List[_JobRecord]:
        with self._lock:
            return list(self._jobs.values())

    # ------------------------------------------------------------------ queries

    def get_run(self, job: JobRef) -> JobRun:
        """Snapshot of one job. Raises :class:`UnknownJobError` for unknown IDs."""
        rec = self._resolve(job)
        self._check_overdue(rec)
        return self._snapshot(rec)

    def get_status(self, job: JobRef) -> JobStatus:
        """Current :class:`JobStatus` of one job (non-blocking)."""
        rec = self._resolve(job)
        self._check_overdue(rec)
        with self._lock:
            return rec.status

    def get_all_runs(
        self, status: StatusFilter = None, *, required: Optional[bool] = None
    ) -> List[JobRun]:
        """Snapshots of all jobs in submission order.

        Args:
            status: Optional status (or iterable of statuses) to filter by.
                Strings such as ``"FAILED"`` are accepted.
            required: If given, keep only required (``True``) or optional
                (``False``) jobs.
        """
        wanted = _normalize_statuses(status)
        out = []
        for rec in self._all_records():
            self._check_overdue(rec)
            snap = self._snapshot(rec)
            if wanted is not None and snap.status not in wanted:
                continue
            if required is not None and snap.required != required:
                continue
            out.append(snap)
        return out

    def get_failed_runs(self, *, include_cancelled: bool = False) -> List[JobRun]:
        """Snapshots of FAILED jobs (optionally also CANCELLED ones)."""
        statuses = [JobStatus.FAILED] + ([JobStatus.CANCELLED] if include_cancelled else [])
        return self.get_all_runs(statuses)

    def get_summary(self) -> Dict[str, Any]:
        """Aggregate counts and IDs suitable for logging or a notebook cell.

        ``ok`` is True only when every job is terminal and no required job
        failed or was cancelled.
        """
        runs = self.get_all_runs()
        by_status = {s.value: 0 for s in JobStatus}
        for r in runs:
            by_status[r.status.value] += 1
        unfinished = [r.job_id for r in runs if not r.is_terminal]
        req_bad = [
            r.job_id
            for r in runs
            if r.required and r.status in (JobStatus.FAILED, JobStatus.CANCELLED)
        ]
        return {
            "manager": self.name,
            "total": len(runs),
            "by_status": by_status,
            "unfinished_job_ids": unfinished,
            "failed_job_ids": [r.job_id for r in runs if r.status is JobStatus.FAILED],
            "cancelled_job_ids": [r.job_id for r in runs if r.status is JobStatus.CANCELLED],
            "required_failed_job_ids": req_bad,
            "optional_failed_job_ids": [
                r.job_id
                for r in runs
                if not r.required and r.status in (JobStatus.FAILED, JobStatus.CANCELLED)
            ],
            "overdue_job_ids": [r.job_id for r in runs if r.overdue],
            "cancel_requested_job_ids": [
                r.job_id for r in runs if r.cancel_requested and not r.is_terminal
            ],
            "evicted": self._evicted,
            "ok": not unfinished and not req_bad,
            "is_shutdown": self._closed,
        }

    # ------------------------------------------------------------------ waiting

    def wait(self, job: JobRef, timeout: Optional[float] = None) -> JobRun:
        """Block until the job is terminal and return its snapshot.

        Does not raise on job failure (inspect ``status``); use
        :meth:`get_result` for that.

        Raises:
            WaitTimeoutError: The job did not finish within ``timeout`` seconds.
                The job's status is not changed.
        """
        _validate_timeout(timeout)
        rec = self._resolve(job)
        _, pending = self._wait_until_terminal(lambda: [rec], timeout)
        if pending:
            snap = self._snapshot(rec)
            with self._lock:
                self._add_event(rec, "wait_timeout", f"wait() timed out after {timeout}s")
            msg = f"timed out after {timeout}s waiting for {snap.describe()}"
            self._log(
                logging.WARNING,
                rec,
                f"wait() timed out after {timeout}s; job still {snap.status.value}",
            )
            raise WaitTimeoutError(msg, pending=[snap])
        return self._snapshot(rec)

    def get_result(self, job: JobRef, timeout: Optional[float] = None) -> Any:
        """Wait for the job and return its result.

        Raises:
            UnknownJobError: Unknown job ID.
            WaitTimeoutError: Not finished within ``timeout`` (job keeps running).
            JobCancelledError: The job was cancelled.
            Exception: The job's original final exception if it FAILED.
        """
        snap = self.wait(job, timeout=timeout)
        if snap.status is JobStatus.COMPLETED:
            return snap.result
        if snap.status is JobStatus.CANCELLED:
            raise JobCancelledError(snap.job_id)
        if snap.exception is None:  # pragma: no cover - defensive
            raise SidejobError(f"job {snap.job_id} failed without a recorded exception")
        raise snap.exception

    def wait_all(
        self,
        jobs: Optional[Iterable[JobRef]] = None,
        timeout: Optional[float] = None,
        raise_on_failure: bool = True,
    ) -> WaitReport:
        """Safety barrier: wait for jobs to finish and verify their outcomes.

        Args:
            jobs: Jobs to check. ``None`` means every job in this manager,
                including jobs submitted while waiting.
            timeout: Overall time limit in seconds (``None`` = no limit).
            raise_on_failure: If ``True`` (default), raise
                :class:`JobsFailedError` when a required job FAILED or was
                CANCELLED. ``False`` disables exception-based enforcement:
                failures are logged and returned in the report only, and you
                must check ``report.ok`` yourself.

        Returns:
            A :class:`WaitReport`.

        Raises:
            WaitTimeoutError: Some jobs are still unfinished after ``timeout``.
                Raised regardless of ``raise_on_failure``; a timeout is never
                success.
            JobsFailedError: See ``raise_on_failure``.
        """
        if jobs is None:
            supplier: Callable[[], List[_JobRecord]] = self._all_records
        else:
            fixed = [self._resolve(j) for j in jobs]
            supplier = lambda: fixed  # noqa: E731
        return self._wait_all_records(
            supplier, timeout, raise_on_failure, label=f"manager {self.name}", scope=None
        )

    def _wait_until_terminal(
        self, supplier: Callable[[], List[_JobRecord]], timeout: Optional[float]
    ) -> Tuple[List[_JobRecord], List[_JobRecord]]:
        """Wait until every supplied record is terminal. Returns (records, still_pending)."""
        ctx = current_job()
        own = ctx._rec if ctx is not None and ctx._manager is self else None
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            recs = supplier()
            if own is not None and any(r is own for r in recs):
                raise SidejobError(f"job {own.job_id} cannot wait for itself; this would deadlock")
            pending = [r for r in recs if not r.done.is_set()]
            if not pending:
                return recs, []
            for rec in pending:
                while not rec.done.is_set():
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        return recs, [r for r in recs if not r.done.is_set()]
                    rec.done.wait(remaining)

    def _wait_all_records(
        self,
        supplier: Callable[[], List[_JobRecord]],
        timeout: Optional[float],
        raise_on_failure: bool,
        *,
        label: str,
        scope: Optional[str],
    ) -> WaitReport:
        _validate_timeout(timeout)
        self.logger.debug("%s: wait_all started (timeout=%s)", label, timeout)
        recs, pending = self._wait_until_terminal(supplier, timeout)
        report = WaitReport(tuple(self._snapshot(r) for r in recs))

        for r in report.optional_failures:
            self.logger.warning("%s: optional %s", label, r.describe())

        if pending:
            pending_snaps = [self._snapshot(r) for r in pending]
            with self._lock:
                for rec in pending:
                    self._add_event(
                        rec, "wait_timeout", f"{label}: wait_all timed out after {timeout}s"
                    )
            lines = [
                f"{label}: wait_all timed out after {timeout}s; {len(pending_snaps)} job(s) "
                "unfinished. Their outcome is UNKNOWN and the workflow is NOT successful:"
            ]
            lines += [f"  - {s.describe()}" for s in pending_snaps]
            if report.required_failures:
                lines.append("Required jobs that already failed:")
                lines += [f"  - {s.describe()}" for s in report.required_failures]
            msg = "\n".join(lines)
            self.logger.error(msg)
            err = WaitTimeoutError(msg, pending=pending_snaps, failures=report.required_failures)
            err._sidejob_scope = scope  # type: ignore[attr-defined]
            raise err

        if report.required_failures:
            failure = JobsFailedError(report.required_failures, report)
            failure._sidejob_scope = scope  # type: ignore[attr-defined]
            self.logger.error("%s: %s", label, failure)
            if raise_on_failure:
                cause = next((e for e in failure.exceptions), None)
                raise failure from cause
            self.logger.warning(
                "%s: raise_on_failure=False -- required failures were NOT raised; "
                "the caller must check report.ok",
                label,
            )
        else:
            self.logger.info(
                "%s: wait_all verified %d job(s): %d completed, %d optional failure(s)",
                label,
                len(report.runs),
                len(report.completed),
                len(report.optional_failures),
            )
        return report

    # ------------------------------------------------------------------ cancellation

    def cancel(self, job: JobRef) -> bool:
        """Cancel a job.

        * QUEUED, or RETRYING (waiting for its next attempt): the job becomes
          CANCELLED immediately and no further attempt runs. Returns ``True``.
        * RUNNING: Python threads cannot be forcibly stopped, so this *requests*
          cooperative cancellation and returns ``False``. The request runs the
          job's :func:`sidejob.on_cancel` hooks and makes
          :func:`sidejob.check_cancelled` / :func:`sidejob.sleep` raise inside
          the job. If the attempt then raises (any exception), the job ends
          CANCELLED and is not retried; if it returns normally it is COMPLETED.
          ``JobRun.cancel_requested`` shows that a request is pending.
        * Already terminal: no-op, returns ``False``.
        """
        rec = self._resolve(job)
        return self._cancel_record(rec, reason="cancel() requested")

    def interrupt(self, job: JobRef, timeout: Optional[float] = 5.0) -> JobStatus:
        """Stop a job now, including a RUNNING one, and return its resulting status.

        1. Same as :meth:`cancel`: a QUEUED/RETRYING job becomes CANCELLED at once;
           for a RUNNING job a cancellation request is made and its
           :func:`sidejob.on_cancel` hooks run (use them to abort blocking
           database/Spark/network calls).
        2. For a RUNNING job, :class:`sidejob.JobInterrupted` is raised *inside*
           the job's thread (CPython). Python code stops at its next bytecode,
           even inside ``except Exception`` blocks. A thread blocked in a single
           C call (socket read, ``time.sleep``, a driver call) is interrupted as
           soon as that call returns, which an ``on_cancel`` hook can force.
        3. Waits up to ``timeout`` seconds (``0`` = don't wait) for the job to
           finish, then returns its status: ``CANCELLED`` once stopped,
           ``RUNNING`` if still inside a blocking call (it stops when the call
           returns), or ``COMPLETED``/``FAILED`` if it ended first.

        The exception is only raised while the job's thread is in its own code
        or ordinary libraries, never while it is inside ``logging``,
        ``threading``, ``queue``, ``concurrent.futures``, ``importlib`` or
        sidejob, where it could leak a lock. If the job stays inside such code,
        the interrupt stays pending and is raised as soon as the job returns to
        its own code (retried in the background, also after interrupt() returns).

        An interrupted job is never retried. Its ``finally`` blocks and ``with``
        exits run normally, but if the exception lands *inside* cleanup code that
        cleanup is cut short, so prefer transactional writes (see the README).
        A job cannot interrupt itself.
        """
        _validate_timeout(timeout)
        rec = self._resolve(job)
        ctx = current_job()
        if ctx is not None and ctx._rec is rec:
            raise SidejobError(
                f"job {rec.job_id} cannot interrupt itself; raise an exception or return instead"
            )
        self._cancel_records(lambda: [rec], reason="interrupt() requested")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            outcome = self._try_interrupt_locked(rec)
        # While waiting anyway, poll for a safe point faster than the scheduler does.
        while outcome == "pending" and (deadline is None or time.monotonic() < deadline):
            if rec.done.wait(0.001):
                break
            with self._lock:
                if rec.interrupt_injected:
                    outcome = "injected_later"
                elif self._inject_if_safe_locked(rec):
                    outcome = "injected"
                elif rec.status is not JobStatus.RUNNING or rec.worker_ident is None:
                    break
        if outcome == "injected":
            self._log(logging.WARNING, rec, "INTERRUPT: raised JobInterrupted in the job's thread")
        elif outcome == "preempted":
            self._log(
                logging.WARNING,
                rec,
                "INTERRUPT: requested while the job function was not executing; "
                "it will not be (re)started",
            )
        elif outcome == "pending":
            self._log(
                logging.WARNING,
                rec,
                "INTERRUPT: pending; the job is inside logging/threading code (e.g. waiting "
                "on an Event, queue or lock) and is interrupted as soon as it returns to its "
                "own code",
            )
        elif outcome == "unsupported":
            self._log(
                logging.WARNING,
                rec,
                f"interrupt() is not supported on {platform.python_implementation()}; "
                "only cooperative cancellation was requested",
            )
        if deadline is None:
            rec.done.wait()
        else:
            rec.done.wait(max(0.0, deadline - time.monotonic()))
        with self._lock:
            status = rec.status
        if not status.is_terminal:
            self._log(
                logging.WARNING,
                rec,
                f"still {status.value} after interrupt(timeout={timeout}): it is probably "
                "blocked in a call that Python cannot interrupt (network/database/C code). "
                "It stops when that call returns; register sidejob.on_cancel(...) to abort it.",
            )
        return status

    def _inject_if_safe_locked(self, rec: _JobRecord) -> bool:
        """Raise JobInterrupted in the job's thread if it is at a safe point (lock held).

        Nothing here may block or log, so the target thread cannot move into
        lock-sensitive code between the safe-point check and the injection.
        """
        ident = rec.worker_ident
        if (
            rec.status is not JobStatus.RUNNING
            or rec.interrupt_injected
            or ident is None
            or not _at_safe_point(ident)
            or _async_raise(ident, JobInterrupted) != 1
        ):
            return False
        rec.interrupt_injected = True
        self._add_event(rec, "interrupted", "JobInterrupted raised in the job's thread")
        return True

    def _try_interrupt_locked(self, rec: _JobRecord) -> str:
        """First interrupt attempt (lock held).

        Returns "injected", "preempted", "pending", "unsupported" or "not_running".
        """
        if rec.status is not JobStatus.RUNNING:
            return "not_running"
        if not INTERRUPT_SUPPORTED:
            return "unsupported"
        rec.interrupt_requested = True
        if rec.interrupt_injected or self._inject_if_safe_locked(rec):
            return "injected"
        if rec.worker_ident is None:
            return "preempted"  # the function isn't executing; it won't be started
        # Keep trying in the background until the job is back in its own code.
        rec.interrupt_polls = 0
        self._schedule_locked(time.monotonic() + 0.002, "interrupt", rec)
        return "pending"

    def _retry_interrupt(self, rec: _JobRecord) -> None:
        """Scheduler: retry a pending interrupt (2ms polls for ~2s, then up to 50ms)."""
        with self._lock:
            if rec.status is not JobStatus.RUNNING or rec.interrupt_injected:
                return
            injected = self._inject_if_safe_locked(rec)
            if not injected and rec.worker_ident is not None:
                rec.interrupt_polls += 1
                delay = 0.002 if rec.interrupt_polls < 1000 else 0.05
                self._schedule_locked(time.monotonic() + delay, "interrupt", rec)
        if injected:
            self._log(logging.WARNING, rec, "INTERRUPT: raised JobInterrupted in the job's thread")

    def job(self, job_id: str) -> Job:
        """Get a :class:`Job` handle from a job ID, e.g. ``manager.job(id).interrupt()``."""
        return Job(self, self._resolve(job_id))

    def _cancel_record(self, rec: _JobRecord, reason: str) -> bool:
        return self._cancel_records(lambda: [rec], reason)[0]

    def _cancel_records(self, select: Callable[[], List[_JobRecord]], reason: str) -> List[bool]:
        """Cancel (or request cancellation of) several jobs atomically.

        ``select`` runs under the lock, and all state transitions happen in the
        same lock hold, so no selected job can start in between. Returns, per
        job, whether it is now CANCELLED.
        """
        outcomes: List[
            Tuple[_JobRecord, JobStatus, str, Optional[Future[None]], List[Callable[[], Any]]]
        ] = []
        with self._lock:
            for rec in select():
                status = rec.status
                first_request = not rec.cancel_event.is_set()
                if status in (JobStatus.QUEUED, JobStatus.RETRYING):
                    if first_request:
                        rec.cancel_reason = reason
                        rec.cancel_event.set()
                    self._finalize(rec, JobStatus.CANCELLED)
                    outcomes.append((rec, status, "cancelled", rec.future, []))
                elif status is JobStatus.RUNNING and first_request:
                    rec.cancel_reason = reason
                    rec.cancel_event.set()
                    self._add_event(rec, "cancel_requested", f"cancellation requested: {reason}")
                    callbacks = list(rec.cancel_callbacks)
                    rec.cancel_callbacks.clear()
                    outcomes.append((rec, status, "requested", None, callbacks))
                else:
                    outcomes.append((rec, status, "ignored", None, []))
        any_cancelled = False
        for rec, status, action, fut, callbacks in outcomes:
            if action == "cancelled":
                any_cancelled = True
                if fut is not None:
                    fut.cancel()  # frees the pool slot if the attempt never started
                self._log(logging.WARNING, rec, f"CANCELLED while {status.value} ({reason})")
                rec.done.set()
            elif action == "requested":
                hooks = f"; running {len(callbacks)} on_cancel hook(s)" if callbacks else ""
                self._log(
                    logging.WARNING,
                    rec,
                    f"cancellation requested while RUNNING ({reason}){hooks}. The attempt stops "
                    "only if the job cooperates (sidejob.check_cancelled/sleep/on_cancel); "
                    "Python threads cannot be forcibly stopped.",
                )
                self._run_cancel_callbacks(rec, callbacks)
            else:
                state = (
                    "cancellation already requested"
                    if status is JobStatus.RUNNING
                    else (f"already {status.value}")
                )
                self._log(logging.DEBUG, rec, f"cancel ignored: {state}")
        if any_cancelled:
            self._after_terminal()
        return [o[2] == "cancelled" for o in outcomes]

    # ------------------------------------------------------------------ lifecycle

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_pending: bool = False,
        raise_on_failure: bool = True,
        timeout: Optional[float] = None,
    ) -> Optional[WaitReport]:
        """Stop accepting jobs and (optionally) wait for and verify existing ones.

        * New submissions raise :class:`ManagerShutdownError` afterwards.
        * ``cancel_pending=True`` cancels QUEUED jobs, jobs waiting for a retry,
          and any retry that would be scheduled later, and requests cooperative
          cancellation of RUNNING jobs (see :meth:`cancel`). A running attempt
          that ignores the request runs to completion.
        * ``wait=True`` behaves like :meth:`wait_all` over all jobs and returns
          its report (raising on timeout or required failures). ``wait=False``
          returns ``None`` immediately; queued jobs and scheduled retries still
          run unless cancelled. The thread pool is released once every job has
          finished.
        * Repeated calls are safe; each ``wait=True`` call re-verifies outcomes.
        * History remains queryable after shutdown.
        """
        _validate_timeout(timeout)
        with self._lock:
            repeated = self._closed
            self._closed = True
            if cancel_pending:
                self._cancel_retries = True
        self.logger.log(
            logging.DEBUG if repeated else logging.INFO,
            "manager %s: shutdown%s (wait=%s, cancel_pending=%s)",
            self.name,
            " (repeated call)" if repeated else "",
            wait,
            cancel_pending,
        )
        if cancel_pending:
            self._cancel_records(
                lambda: [r for r in self._jobs.values() if not r.status.is_terminal],
                reason="manager shutdown with cancel_pending=True",
            )

        if not wait:
            self._maybe_release_executor()
            return None
        try:
            report = self._wait_all_records(
                self._all_records,
                timeout,
                raise_on_failure,
                label=f"manager {self.name} shutdown",
                scope=None,
            )
        finally:
            # If jobs are still unfinished (timeout), the pool stays open for them and
            # is released automatically when the last one finishes.
            self._maybe_release_executor(join=True)
        return report

    def __enter__(self) -> JobManager:
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb: Any
    ) -> None:
        if exc is None:
            self.shutdown(wait=True, raise_on_failure=True)
            return
        try:
            if isinstance(exc, Exception):
                self.shutdown(wait=True, cancel_pending=True, raise_on_failure=False)
            else:
                self.shutdown(wait=False, cancel_pending=True)
        except Exception:  # never mask the original exception
            self.logger.exception("manager %s: error during shutdown after an exception", self.name)

    def __repr__(self) -> str:
        with self._lock:
            n = len(self._jobs)
        return (
            f"JobManager(name={self.name!r}, max_workers={self._max_workers}, jobs={n}, "
            f"shutdown={self._closed})"
        )


_INVOKE_CODE = JobManager._invoke.__code__


def _normalize_statuses(status: StatusFilter) -> Optional[FrozenSet[JobStatus]]:
    if status is None:
        return None
    if isinstance(status, (JobStatus, str)):
        status = [status]
    out = set()
    for s in status:
        out.add(s if isinstance(s, JobStatus) else JobStatus(str(s).upper()))
    return frozenset(out)


def _configure_console_handler(enabled: Optional[bool]) -> None:
    """Attach at most one stderr handler to the ``sidejob`` logger.

    ``None`` (auto): only if neither the root logger nor the ``sidejob`` logger
    has handlers, i.e. the application has not configured logging.
    The root logger is never modified.
    """
    base = logging.getLogger(LOGGER_NAME)
    ours = [h for h in base.handlers if getattr(h, _HANDLER_MARK, False)]
    if enabled is None:
        enabled = not logging.getLogger().handlers and not base.handlers
    if enabled and not ours:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-8s %(name)s | %(message)s")
        )
        setattr(handler, _HANDLER_MARK, True)
        base.addHandler(handler)
    elif enabled is False:
        for h in ours:
            base.removeHandler(h)


def initialize(
    max_workers: int = DEFAULT_MAX_WORKERS,
    log_level: Optional[int] = logging.INFO,
    retries: Optional[int] = None,
    retry_backoff: Optional[float] = None,
    *,
    max_attempts: Optional[int] = None,
    backoff_multiplier: Optional[float] = None,
    max_backoff: Optional[float] = None,
    jitter: Optional[float] = None,
    retry_on: Optional[Tuple[Type[BaseException], ...]] = None,
    retry_if: Optional[Callable[[BaseException], bool]] = None,
    never_retry: Optional[Tuple[Type[BaseException], ...]] = None,
    retry_policy: Optional[RetryPolicy] = None,
    name: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    log_to_console: Optional[bool] = None,
    history_limit: Optional[int] = None,
) -> JobManager:
    """Create a :class:`JobManager` with its own bounded thread pool.

    Args:
        max_workers: Maximum concurrently running jobs (default 4).
        log_level: Level for this manager's logger (``None`` leaves it unchanged).
        retries: Retries *after* the first attempt (``retries=2`` -> 3 attempts).
        retry_backoff: Seconds before the first retry (default 2.0).
        max_attempts: Total attempts including the first. Mutually exclusive
            with ``retries``. Default: 1, i.e. **no retries** unless you enable
            them with ``retries=`` or ``max_attempts=``.
        backoff_multiplier, max_backoff, jitter, retry_on, retry_if, never_retry:
            See :class:`RetryPolicy`.
        retry_policy: A complete :class:`RetryPolicy`; cannot be combined with
            the individual retry options above.
        name: Manager name, used in the logger name ``sidejob.<name>``.
        logger: Use this logger instead of ``sidejob.<name>``.
        log_to_console: ``True`` attaches one stderr handler to the ``sidejob``
            logger, ``False`` never does (and removes ours), ``None`` (default)
            attaches one only if the application has not configured logging.
        history_limit: Keep at most this many jobs in history by evicting the
            oldest COMPLETED ones (never FAILED, CANCELLED, unfinished, or
            jobs of an open workflow). ``None`` (default) keeps everything.
    """
    individual: Dict[str, Any] = {
        "backoff": retry_backoff,
        "multiplier": backoff_multiplier,
        "max_backoff": max_backoff,
        "jitter": jitter,
        "retry_on": retry_on,
        "retry_if": retry_if,
        "never_retry": never_retry,
    }
    provided = {k: v for k, v in individual.items() if v is not None}
    attempts = resolve_max_attempts(retries, max_attempts)
    if retry_policy is not None:
        if provided or attempts is not None:
            raise ValueError(
                "retry_policy cannot be combined with retries/max_attempts/retry_backoff/"
                "other individual retry options"
            )
        policy = retry_policy
    else:
        if attempts is not None:
            provided["max_attempts"] = attempts
        policy = RetryPolicy(**provided)
    return JobManager(
        max_workers,
        retry_policy=policy,
        log_level=log_level,
        name=name,
        logger=logger,
        log_to_console=log_to_console,
        history_limit=history_limit,
    )
