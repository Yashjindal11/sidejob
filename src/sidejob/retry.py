"""Retry policy: which errors are retried, how many times, and with what delay."""

from __future__ import annotations

import random
import socket
from dataclasses import dataclass, field, replace
from typing import Callable, Optional, Tuple, Type

from .errors import TransientError

ExcTypes = Tuple[Type[BaseException], ...]

#: Retried by default: network/connection problems and explicit TransientError.
DEFAULT_RETRYABLE: ExcTypes = (ConnectionError, TimeoutError, socket.timeout, TransientError)

#: Never retried by default, even if matched by ``retry_on``/``retry_if``:
#: these almost always indicate a bug or bad data, and repeating will not help.
DEFAULT_NEVER_RETRY: ExcTypes = (
    TypeError,
    ValueError,
    AttributeError,
    NameError,
    LookupError,
    ArithmeticError,
    AssertionError,
    ImportError,
    NotImplementedError,
    MemoryError,
)


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded retry with exponential backoff and optional jitter.

    Args:
        max_attempts: Maximum *total* attempts, including the first one (>= 1).
            The default, ``1``, means no retries. ``max_attempts=3`` means
            "1 attempt + up to 2 retries".
        backoff: Delay in seconds before the first retry.
        multiplier: Each further retry waits ``multiplier`` times longer.
        max_backoff: Upper bound for a single delay, in seconds.
        jitter: Random spread as a fraction of the delay (0 to 1). ``0.1``
            means each delay is randomly scaled within +/-10%.
        retry_on: Exception types considered retryable.
        retry_if: Optional predicate ``(exc) -> bool``. When given it replaces
            the ``retry_on`` check.
        never_retry: Exception types that are never retried. Checked first.

    Only subclasses of ``Exception`` are ever retried.
    """

    max_attempts: int = 1
    backoff: float = 2.0
    multiplier: float = 2.0
    max_backoff: float = 60.0
    jitter: float = 0.0
    retry_on: ExcTypes = DEFAULT_RETRYABLE
    retry_if: Optional[Callable[[BaseException], bool]] = field(default=None, compare=False)
    never_retry: ExcTypes = DEFAULT_NEVER_RETRY

    def __post_init__(self) -> None:
        if not isinstance(self.max_attempts, int) or isinstance(self.max_attempts, bool):
            raise TypeError("max_attempts must be an int")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1 (it counts the first attempt)")
        if self.backoff < 0 or self.max_backoff < 0:
            raise ValueError("backoff and max_backoff must be >= 0")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")
        object.__setattr__(self, "retry_on", tuple(self.retry_on))
        object.__setattr__(self, "never_retry", tuple(self.never_retry))

    @classmethod
    def from_retries(cls, retries: int, **kwargs: object) -> RetryPolicy:
        """Build a policy from the number of retries *after* the first attempt."""
        if not isinstance(retries, int) or isinstance(retries, bool) or retries < 0:
            raise ValueError("retries must be an int >= 0")
        return cls(max_attempts=retries + 1, **kwargs)  # type: ignore[arg-type]

    @property
    def retries(self) -> int:
        """Number of retries allowed after the first attempt."""
        return self.max_attempts - 1

    def with_attempts(
        self, *, retries: Optional[int] = None, max_attempts: Optional[int] = None
    ) -> RetryPolicy:
        """Return a copy with a different attempt budget (at most one argument)."""
        resolved = resolve_max_attempts(retries, max_attempts)
        return self if resolved is None else replace(self, max_attempts=resolved)

    def is_retryable(self, exc: BaseException) -> bool:
        """Whether ``exc`` is a transient error per this policy (ignores the attempt budget)."""
        if not isinstance(exc, Exception):
            return False
        if self.never_retry and isinstance(exc, self.never_retry):
            return False
        if self.retry_if is not None:
            return bool(self.retry_if(exc))
        return isinstance(exc, self.retry_on)

    def compute_delay(self, retry_number: int, rng: Optional[random.Random] = None) -> float:
        """Delay before retry number ``retry_number`` (1 = first retry)."""
        if retry_number < 1:
            raise ValueError("retry_number must be >= 1")
        try:
            delay = self.backoff * (self.multiplier ** (retry_number - 1))
        except OverflowError:
            delay = self.max_backoff
        delay = min(delay, self.max_backoff)
        if self.jitter and delay > 0:
            r = rng if rng is not None else random
            delay *= 1 + r.uniform(-self.jitter, self.jitter)
        return max(0.0, delay)


def resolve_max_attempts(retries: Optional[int], max_attempts: Optional[int]) -> Optional[int]:
    """Translate the mutually exclusive ``retries`` / ``max_attempts`` options."""
    if retries is not None and max_attempts is not None:
        raise ValueError(
            "Pass either retries (retries after the first attempt) or max_attempts "
            "(total attempts), not both"
        )
    if retries is not None:
        if not isinstance(retries, int) or isinstance(retries, bool) or retries < 0:
            raise ValueError("retries must be an int >= 0")
        return retries + 1
    if max_attempts is not None:
        if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or max_attempts < 1:
            raise ValueError("max_attempts must be an int >= 1")
        return max_attempts
    return None
