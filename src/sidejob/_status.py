"""Job status enumeration."""

from __future__ import annotations

from enum import Enum


class JobStatus(str, Enum):
    """Lifecycle state of a background job.

    ``QUEUED``     Submitted, waiting for a free worker.
    ``RUNNING``    An attempt is currently executing.
    ``RETRYING``   An attempt failed with a retryable error; waiting out the backoff
                   delay before the next attempt.
    ``COMPLETED``  The submitted function returned normally (terminal).
    ``FAILED``     The final attempt raised, or the error was not retryable (terminal).
    ``CANCELLED``  Cancelled before an attempt started (terminal).
    """

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        """True for ``COMPLETED``, ``FAILED`` and ``CANCELLED``."""
        return self in _TERMINAL

    def __str__(self) -> str:
        return self.value


_TERMINAL = frozenset({JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED})
TERMINAL_STATUSES = _TERMINAL
