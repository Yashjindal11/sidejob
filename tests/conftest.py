from __future__ import annotations

import threading
import time
from typing import Any, Callable, Iterator, List

import pytest

import sidejob


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    """Condition wait (not a fixed sleep): poll until ``predicate`` is true."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met within timeout")
        time.sleep(0.002)


class Gate:
    """A job body that signals when it starts and blocks until released."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    def __call__(self, value: Any = None) -> Any:
        self.started.set()
        if not self.release.wait(10):
            raise RuntimeError("gate never released")
        return value


@pytest.fixture
def make_manager() -> Iterator[Callable[..., sidejob.JobManager]]:
    created: List[sidejob.JobManager] = []
    gates: List[Gate] = []

    def factory(**kwargs: Any) -> sidejob.JobManager:
        kwargs.setdefault("max_workers", 4)
        kwargs.setdefault("retry_backoff", 0)
        kwargs.setdefault("log_to_console", False)
        m = sidejob.initialize(**kwargs)
        created.append(m)
        return m

    factory.gates = gates  # type: ignore[attr-defined]
    yield factory
    for g in gates:
        g.release.set()
    for m in created:
        m.shutdown(wait=True, cancel_pending=True, raise_on_failure=False, timeout=10)


@pytest.fixture
def gate(make_manager: Any) -> Callable[[], Gate]:
    def new() -> Gate:
        g = Gate()
        make_manager.gates.append(g)
        return g

    return new


class Flaky:
    """Fails with ``exc`` for the first ``failures`` calls, then returns ``result``."""

    def __init__(self, failures: int, exc: BaseException, result: Any = "ok") -> None:
        self.failures = failures
        self.exc = exc
        self.result = result
        self.calls = 0
        self._lock = threading.Lock()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        with self._lock:
            self.calls += 1
            n = self.calls
        if n <= self.failures:
            raise type(self.exc)(f"{self.exc} (call {n})")
        return self.result
