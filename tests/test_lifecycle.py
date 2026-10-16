from __future__ import annotations

import logging

import pytest

import sidejob
from conftest import Flaky, wait_until
from sidejob import JobStatus


def test_cancel_queued_job(make_manager, gate, caplog):
    m = make_manager(max_workers=1)
    g = gate()
    calls = []
    m.parallel_job(g)
    queued = m.parallel_job(lambda: calls.append(1))
    assert g.started.wait(5)
    assert m.cancel(queued) is True
    assert m.get_status(queued) is JobStatus.CANCELLED
    assert m.cancel(queued) is False  # already terminal
    g.release.set()
    m.wait_all(raise_on_failure=False, timeout=5)
    assert calls == []
    run = m.get_run(queued)
    assert run.started_at is None and run.attempt == 0
    assert run.events[-1].kind == "cancelled"
    assert any("CANCELLED while QUEUED" in r.getMessage() for r in caplog.records)


def test_cancel_running_job_that_ignores_request_completes(make_manager, gate, caplog):
    m = make_manager()
    g = gate()
    job = m.parallel_job(g, "result")
    assert g.started.wait(5)
    assert m.cancel(job) is False  # not cancelled yet: only requested
    run = m.get_run(job)
    assert run.status is JobStatus.RUNNING and run.cancel_requested is True
    assert any(e.kind == "cancel_requested" for e in run.events)
    assert m.get_summary()["cancel_requested_job_ids"] == [job.id]
    assert any(
        r.levelno == logging.WARNING and "cannot be forcibly stopped" in r.getMessage()
        for r in caplog.records
    )
    assert m.cancel(job) is False  # repeated request is a no-op
    g.release.set()
    assert job.result(timeout=5) == "result"  # it did finish: truthfully COMPLETED
    assert m.get_status(job) is JobStatus.COMPLETED


def test_submit_after_shutdown(make_manager):
    m = make_manager()
    m.shutdown()
    assert m.is_shutdown
    with pytest.raises(sidejob.ManagerShutdownError):
        m.parallel_job(lambda: 1)
    with pytest.raises(RuntimeError):  # also a RuntimeError
        m.parallel_job(lambda: 1)


def test_repeated_shutdown_is_safe(make_manager):
    m = make_manager()
    m.parallel_job(lambda: 1)
    r1 = m.shutdown()
    r2 = m.shutdown()
    assert r1 is not None and r2 is not None and r1.ok and r2.ok
    assert m.shutdown(wait=False) is None


def test_shutdown_waits_for_running_and_queued(make_manager, gate):
    m = make_manager(max_workers=1)
    g = gate()
    running = m.parallel_job(g)
    queued = m.parallel_job(lambda: "q")
    assert g.started.wait(5)
    import threading

    threading.Timer(0.05, g.release.set).start()
    report = m.shutdown(wait=True)
    assert report.ok
    assert m.get_status(running) is JobStatus.COMPLETED
    assert m.get_result(queued) == "q"
    assert len(m.get_all_runs()) == 2  # history retained after shutdown


def test_shutdown_cancel_pending(make_manager, gate):
    import threading

    m = make_manager(max_workers=1, retry_backoff=30)
    flaky_fn = Flaky(5, ConnectionError("x"))
    flaky = m.parallel_job(flaky_fn, required=False)
    wait_until(lambda: m.get_status(flaky) is JobStatus.RETRYING)  # waiting 30s for a retry
    g = gate()
    running = m.parallel_job(g)  # the backoff does not hold the only worker
    assert g.started.wait(5)
    queued = m.parallel_job(lambda: 1, required=False)
    assert m.get_status(queued) is JobStatus.QUEUED
    threading.Timer(0.05, g.release.set).start()
    report = m.shutdown(wait=True, cancel_pending=True, timeout=5)
    assert report.ok  # only optional jobs were cancelled
    assert m.get_status(running) is JobStatus.COMPLETED  # ignored the cancel request
    assert m.get_run(running).cancel_requested is True
    assert m.get_status(flaky) is JobStatus.CANCELLED
    assert m.get_status(queued) is JobStatus.CANCELLED
    assert flaky_fn.calls == 1


def test_shutdown_cancel_pending_queued_required_raises(make_manager, gate):
    m = make_manager(max_workers=1)
    g = gate()
    m.parallel_job(g)
    queued = m.parallel_job(lambda: 1)
    assert g.started.wait(5)
    import threading

    threading.Timer(0.05, g.release.set).start()
    with pytest.raises(sidejob.JobsFailedError) as excinfo:
        m.shutdown(cancel_pending=True)
    assert excinfo.value.job_ids == (queued.id,)
    assert m.get_status(queued) is JobStatus.CANCELLED


def test_shutdown_cancel_pending_stops_future_retries(make_manager, gate):
    m = make_manager()
    g = gate()

    def fails_after_gate():
        g()
        raise ConnectionError("lost connection")

    job = m.parallel_job(fails_after_gate)
    assert g.started.wait(5)
    import threading

    threading.Timer(0.05, g.release.set).start()
    report = m.shutdown(cancel_pending=True, raise_on_failure=False, timeout=5)
    run = m.get_run(job)
    assert run.status is JobStatus.CANCELLED
    assert run.attempt == 1 and run.attempts[0].retry_decision == "cancelled"
    assert not report.ok


def test_shutdown_raises_on_required_failure(make_manager):
    m = make_manager()
    bad = m.parallel_job(int, "x")
    with pytest.raises(sidejob.JobsFailedError):
        m.shutdown()
    report = m.shutdown(raise_on_failure=False)
    assert [r.job_id for r in report.required_failures] == [bad.id]


def test_shutdown_timeout_and_no_wait(make_manager, gate):
    m = make_manager()
    g = gate()
    job = m.parallel_job(g)
    assert g.started.wait(5)
    with pytest.raises(sidejob.WaitTimeoutError):
        m.shutdown(timeout=0.05)
    assert m.shutdown(wait=False) is None
    assert m.get_status(job) is JobStatus.RUNNING
    g.release.set()
    assert m.wait(job, timeout=5).status is JobStatus.COMPLETED


def test_manager_context_manager(make_manager):
    with sidejob.initialize(log_to_console=False, retry_backoff=0) as m:
        job = m.parallel_job(lambda: 5)
    assert m.is_shutdown and m.get_result(job) == 5
    with pytest.raises(sidejob.JobsFailedError):
        with sidejob.initialize(log_to_console=False) as m2:
            m2.parallel_job(int, "x")
    with pytest.raises(ZeroDivisionError):
        with sidejob.initialize(log_to_console=False) as m3:
            m3.parallel_job(int, "x")
            1 / 0  # noqa: B018
    assert m3.is_shutdown


def test_invalid_max_workers():
    for bad in (0, -1, 1.5, True):
        with pytest.raises(ValueError):
            sidejob.initialize(max_workers=bad)  # type: ignore[arg-type]


def test_shutdown_cancel_pending_race_with_retry_decision(make_manager):
    """A retry decided just before shutdown(cancel_pending=True) must not run."""
    import threading

    deciding, shutdown_done = threading.Event(), threading.Event()

    def slow_predicate(exc):
        deciding.set()
        assert shutdown_done.wait(5)
        return True

    calls = []

    def fails():
        calls.append(1)
        raise ConnectionError("x")

    m = make_manager()
    policy = sidejob.RetryPolicy(max_attempts=3, backoff=0, retry_if=slow_predicate)
    job = m.parallel_job(fails, retry_policy=policy)
    assert deciding.wait(5)  # worker is between the failed attempt and committing its decision
    assert m.shutdown(wait=False, cancel_pending=True) is None
    shutdown_done.set()
    run = m.wait(job, timeout=5)
    assert run.status is JobStatus.CANCELLED
    assert calls == [1]
    assert run.attempts[0].retry_decision == "cancelled"
