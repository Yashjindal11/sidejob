"""Immutable, thread-safe snapshots of job state returned to callers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from ._status import JobStatus


@dataclass(frozen=True)
class JobEvent:
    """One entry in a job's chronological event history.

    ``kind`` is one of: ``submitted``, ``attempt_started``, ``attempt_failed``,
    ``retry_scheduled``, ``completed``, ``failed``, ``cancelled``,
    ``cancel_requested``, ``wait_timeout``, ``overdue``.
    """

    timestamp: datetime
    kind: str
    message: str
    attempt: int = 0


@dataclass(frozen=True)
class AttemptRecord:
    """The outcome of a single execution attempt.

    ``retry_decision`` is ``None`` for a successful attempt, otherwise one of:
    ``"retry"``, ``"not_retryable"``, ``"exhausted"``, ``"cancelled"``.
    """

    number: int
    started_at: datetime
    finished_at: Optional[datetime] = None
    duration: Optional[float] = None
    succeeded: Optional[bool] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    exception: Optional[BaseException] = field(default=None, repr=False, compare=False)
    traceback: Optional[str] = field(default=None, repr=False, compare=False)
    retry_decision: Optional[str] = None
    retry_delay: Optional[float] = None


@dataclass(frozen=True)
class JobRun:
    """Point-in-time snapshot of a job. Safe to keep and pass around.

    ``duration`` is the wall time from the start of the first attempt until the
    job reached a terminal state (including backoff waits), or ``None`` if the
    job has not both started and finished.
    """

    job_id: str
    name: str
    status: JobStatus
    required: bool
    submitted_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    duration: Optional[float]
    attempt: int
    max_attempts: int
    result: Any = field(default=None, repr=False, compare=False)
    exception: Optional[BaseException] = field(default=None, repr=False, compare=False)
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    attempts: Tuple[AttemptRecord, ...] = ()
    events: Tuple[JobEvent, ...] = field(default=(), repr=False)
    workflow_id: Optional[str] = None
    deadline: Optional[float] = None
    overdue: bool = False
    cancel_requested: bool = False

    @property
    def is_terminal(self) -> bool:
        return self.status.is_terminal

    @property
    def succeeded(self) -> bool:
        return self.status is JobStatus.COMPLETED

    def describe(self) -> str:
        """One-line human readable description, used in error messages."""
        head = f"job {self.job_id} ({self.name}, {'required' if self.required else 'optional'})"
        if self.status is JobStatus.FAILED:
            return (
                f"{head}: FAILED after {self.attempt}/{self.max_attempts} attempt(s): "
                f"{self.error_type}: {self.error_message}"
            )
        if self.status is JobStatus.CANCELLED:
            if self.attempt:
                return (
                    f"{head}: CANCELLED after {self.attempt} failed attempt(s) "
                    f"(last error {self.error_type}: {self.error_message})"
                )
            return f"{head}: CANCELLED before starting"
        if self.status.is_terminal:
            return f"{head}: {self.status.value}"
        extra = " [OVERDUE]" if self.overdue else ""
        return (
            f"{head}: still {self.status.value} (attempt {self.attempt}/{self.max_attempts}){extra}"
        )

    def to_dict(self, include_result: bool = False) -> Dict[str, Any]:
        """Flat dict, convenient for ``pandas.DataFrame([...])``."""
        data: Dict[str, Any] = {
            "job_id": self.job_id,
            "name": self.name,
            "status": self.status.value,
            "required": self.required,
            "workflow_id": self.workflow_id,
            "submitted_at": self.submitted_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration": self.duration,
            "attempt": self.attempt,
            "max_attempts": self.max_attempts,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "overdue": self.overdue,
            "cancel_requested": self.cancel_requested,
        }
        if include_result:
            data["result"] = self.result
        return data


@dataclass(frozen=True)
class WaitReport:
    """Outcome of a ``wait_all()`` call: snapshots of every job it checked."""

    runs: Tuple[JobRun, ...]

    @property
    def completed(self) -> Tuple[JobRun, ...]:
        return tuple(r for r in self.runs if r.status is JobStatus.COMPLETED)

    @property
    def failed(self) -> Tuple[JobRun, ...]:
        return tuple(r for r in self.runs if r.status is JobStatus.FAILED)

    @property
    def cancelled(self) -> Tuple[JobRun, ...]:
        return tuple(r for r in self.runs if r.status is JobStatus.CANCELLED)

    @property
    def required_failures(self) -> Tuple[JobRun, ...]:
        """Required jobs that ended FAILED or CANCELLED."""
        return tuple(
            r
            for r in self.runs
            if r.required and r.status in (JobStatus.FAILED, JobStatus.CANCELLED)
        )

    @property
    def optional_failures(self) -> Tuple[JobRun, ...]:
        return tuple(
            r
            for r in self.runs
            if not r.required and r.status in (JobStatus.FAILED, JobStatus.CANCELLED)
        )

    @property
    def ok(self) -> bool:
        """True only if every job is terminal and every required job COMPLETED."""
        return all(r.is_terminal for r in self.runs) and not self.required_failures
