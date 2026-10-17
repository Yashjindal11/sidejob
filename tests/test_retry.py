from __future__ import annotations

import logging
import random

import pytest

import sidejob
from conftest import Flaky
from sidejob import JobStatus, RetryPolicy

# ----------------------------------------------------------------- policy unit tests


def test_policy_defaults_no_retries():
    p = RetryPolicy()
    assert p.max_attempts == 1 and p.retries == 0  # retries are opt-in
    assert p.backoff == 2.0
    m = sidejob.initialize(log_to_console=False)
    try:
        assert m.retry_policy.max_attempts == 1
        assert m.retry_policy.backoff == 2.0
    finally:
        m.shutdown()


def test_retries_vs_max_attempts():
    assert RetryPolicy.from_retries(0).max_attempts == 1
    assert RetryPolicy.from_retries(3).max_attempts == 4
    m = sidejob.initialize(retries=3, log_to_console=False)
    assert m.retry_policy.max_attempts == 4 and m.retry_policy.retries == 3
    m.shutdown()
    m = sidejob.initialize(max_attempts=3, log_to_console=False)
    assert m.retry_policy.max_attempts == 3
    m.shutdown()
    with pytest.raises(ValueError):
        sidejob.initialize(retries=1, max_attempts=2)
    with pytest.raises(ValueError):
        sidejob.initialize(retry_policy=RetryPolicy(), retries=1)
    with pytest.raises(ValueError):
        sidejob.initialize(retries=-1)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"backoff": -1},
        {"multiplier": 0.5},
        {"jitter": 1.5},
        {"max_backoff": -1},
    ],
)
def test_policy_validation(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


def test_exponential_backoff_with_cap():
    p = RetryPolicy(backoff=2, multiplier=2, max_backoff=10)
    assert [p.compute_delay(n) for n in (1, 2, 3, 4, 5)] == [2, 4, 8, 10, 10]
    assert RetryPolicy(backoff=1, max_backoff=5).compute_delay(10_000) == 5
    with pytest.raises(ValueError):
        p.compute_delay(0)


def test_jitter_bounds():
    p = RetryPolicy(backoff=10, jitter=0.2)
    rng = random.Random(42)
    delays = [p.compute_delay(1, rng) for _ in range(500)]
    assert all(8 <= d <= 12 for d in delays)
    assert len(set(delays)) > 1


def test_retryable_classification():
    p = RetryPolicy()
    assert p.is_retryable(ConnectionError())
    assert p.is_retryable(TimeoutError())
    assert p.is_retryable(sidejob.TransientError())
    assert not p.is_retryable(ValueError())
    assert not p.is_retryable(KeyError())
    assert not p.is_retryable(RuntimeError())
    assert not p.is_retryable(KeyboardInterrupt())
    broad = RetryPolicy(retry_on=(Exception,))
    assert broad.is_retryable(RuntimeError())
    assert not broad.is_retryable(TypeError())  # never_retry wins
    assert RetryPolicy(retry_on=(Exception,), never_retry=()).is_retryable(TypeError())
    pred = RetryPolicy(retry_if=lambda e: "deadlock" in str(e))
    assert pred.is_retryable(RuntimeError("deadlock detected"))
    assert not pred.is_retryable(ConnectionError("refused"))


# ----------------------------------------------------------------- execution behavior


def test_regression_b_retry_then_success(make_manager, caplog):
    """Mandatory regression test B."""
    m = make_manager(max_attempts=3)
    flaky = Flaky(failures=1, exc=ConnectionError("connection reset"), result="uploaded")
    with m.workflow() as run:
        job = run.parallel_job(flaky, job_name="upload")
        report = run.wait_all(raise_on_failure=True)  # returns normally

    assert report.ok
    run_ = m.get_run(job)
    assert run_.status is JobStatus.COMPLETED
    assert run_.result == "uploaded"
    assert run_.attempt == 2 and flaky.calls == 2
    a1, a2 = run_.attempts
    assert (a1.number, a1.succeeded, a1.error_type, a1.retry_decision) == (
        1,
        False,
        "ConnectionError",
        "retry",
    )
    assert (a2.number, a2.succeeded, a2.error_type) == (2, True, None)
    assert run_.error_type is None and run_.exception is None  # earlier failure not final
    kinds = [e.kind for e in run_.events]
    assert kinds == [
        "submitted",
        "attempt_started",
        "attempt_failed",
        "retry_scheduled",
        "attempt_started",
        "completed",
    ]
    retry_logs = [
        r
        for r in caplog.records
        if getattr(r, "sidejob_job_id", None) == job.id and "RETRYING" in r.getMessage()
    ]
    assert len(retry_logs) == 1
    assert retry_logs[0].levelno == logging.WARNING
    assert "attempt 1/3" in retry_logs[0].getMessage()
    assert "ConnectionError" in retry_logs[0].getMessage()


def test_exhausted_retries_fail(make_manager, caplog):
    m = make_manager(retries=2)
    flaky = Flaky(failures=10, exc=TimeoutError("query timed out"))
    job = m.parallel_job(flaky)
    run = m.wait(job, timeout=5)
    assert run.status is JobStatus.FAILED
    assert flaky.calls == 3
    assert [a.retry_decision for a in run.attempts] == ["retry", "retry", "exhausted"]
    assert run.error_type == "TimeoutError" and "call 3" in run.error_message
    with pytest.raises(TimeoutError, match="call 3"):
        m.get_result(job)
    final = [r for r in caplog.records if "FAILED" in r.getMessage() and r.levelno == logging.ERROR]
    assert final and final[0].exc_info is not None
    assert "retries exhausted" in final[0].getMessage()


def test_permanent_error_not_retried(make_manager):
    m = make_manager(retries=5)
    flaky = Flaky(failures=10, exc=KeyError("missing column"))
    job = m.parallel_job(flaky)
    run = m.wait(job, timeout=5)
    assert run.status is JobStatus.FAILED
    assert flaky.calls == 1
    assert run.attempts[0].retry_decision == "not_retryable"
    assert "KeyError" in run.attempts[0].traceback


def test_retryable_error_not_retried_unless_enabled(make_manager):
    m = make_manager()
    flaky = Flaky(failures=1, exc=ConnectionError("blip"))
    run = m.wait(m.parallel_job(flaky), timeout=5)
    assert run.status is JobStatus.FAILED and flaky.calls == 1
    assert run.attempts[0].retry_decision == "exhausted"
    enabled = make_manager(retries=1)
    assert (
        enabled.get_result(enabled.parallel_job(Flaky(1, ConnectionError("x"))), timeout=5) == "ok"
    )


def test_unlisted_error_not_retried_by_default(make_manager):
    m = make_manager()
    flaky = Flaky(failures=10, exc=RuntimeError("driver error"))
    assert m.wait(m.parallel_job(flaky), timeout=5).attempt == 1


def test_custom_retry_on_globally_and_per_job(make_manager):
    class DriverError(Exception):
        pass

    m = make_manager(retry_on=(DriverError,), max_attempts=3)
    flaky = Flaky(failures=2, exc=DriverError("operational error"))
    assert m.get_result(m.parallel_job(flaky), timeout=5) == "ok"
    assert flaky.calls == 3

    per_job = Flaky(failures=1, exc=RuntimeError("deadlock"))
    policy = RetryPolicy(max_attempts=2, backoff=0, retry_if=lambda e: "deadlock" in str(e))
    assert m.get_result(m.parallel_job(per_job, retry_policy=policy), timeout=5) == "ok"


def test_per_job_attempt_overrides(make_manager):
    m = make_manager(max_attempts=5)
    f1 = Flaky(failures=10, exc=ConnectionError("x"))
    f2 = Flaky(failures=10, exc=ConnectionError("x"))
    j1 = m.parallel_job(f1, retries=0, required=False)
    j2 = m.parallel_job(f2, max_attempts=2, required=False)
    m.wait_all(timeout=5)
    assert (f1.calls, m.get_run(j1).max_attempts) == (1, 1)
    assert (f2.calls, m.get_run(j2).max_attempts) == (2, 2)


def test_broken_retry_predicate_fails_job_not_hangs(make_manager):
    def bad_predicate(exc):
        raise RuntimeError("predicate bug")

    m = make_manager()
    job = m.parallel_job(
        Flaky(5, ConnectionError("x")), retry_policy=RetryPolicy(retry_if=bad_predicate, backoff=0)
    )
    run = m.wait(job, timeout=5)
    assert run.status is JobStatus.FAILED and run.attempt == 1


def test_base_exception_in_job_is_recorded_not_retried(make_manager):
    m = make_manager(retry_on=(BaseException,), never_retry=())

    def exits():
        raise SystemExit(3)

    run = m.wait(m.parallel_job(exits), timeout=5)
    assert run.status is JobStatus.FAILED and run.attempt == 1
    assert run.error_type == "SystemExit"
