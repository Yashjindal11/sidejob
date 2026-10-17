from __future__ import annotations

import logging
import threading

import pytest

import sidejob
from conftest import Flaky
from sidejob import JobStatus


def test_regression_a_failed_upload_blocks_success_marker(make_manager, caplog):
    """Mandatory regression test A."""
    m = make_manager(max_workers=2, max_attempts=3)
    upload_calls = []
    marker_calls = []

    def upload_to_redshift(df, table):
        upload_calls.append(table)
        raise ConnectionError(f"could not connect to cluster (call {len(upload_calls)})")

    def analyze(df):
        return sum(df) / len(df)

    def write_success_marker():
        marker_calls.append(True)

    df = [1, 2, 3, 4]
    with pytest.raises(sidejob.JobsFailedError) as excinfo:
        with m.workflow("nightly") as run:
            job = run.parallel_job(upload_to_redshift, df, "analytics_table", required=True)
            summary = analyze(df)  # main-thread analysis succeeds
            assert summary == 2.5
            run.wait_all(raise_on_failure=True)
        write_success_marker()

    assert marker_calls == []
    err = excinfo.value
    assert err.job_ids == (job.id,)
    assert job.id in str(err) and "upload_to_redshift" in str(err)
    assert isinstance(err.__cause__, ConnectionError)
    assert isinstance(err.exceptions[0], ConnectionError)

    run_ = m.get_run(job)
    assert run_.status is JobStatus.FAILED
    assert len(upload_calls) == 3
    assert [a.number for a in run_.attempts] == [1, 2, 3]
    assert [a.error_type for a in run_.attempts] == ["ConnectionError"] * 3
    assert [f"call {i}" in a.error_message for i, a in enumerate(run_.attempts, 1)] == [True] * 3
    assert all(isinstance(a.exception, ConnectionError) for a in run_.attempts)
    assert [a.retry_decision for a in run_.attempts] == ["retry", "retry", "exhausted"]

    job_logs = [
        r.getMessage() for r in caplog.records if getattr(r, "sidejob_job_id", None) == job.id
    ]
    for n in (1, 2, 3):
        assert any(f"attempt {n}/3: started" in t for t in job_logs)
    assert sum("RETRYING" in t for t in job_logs) == 2
    assert any("FAILED" in t and "retries exhausted" in t for t in job_logs)
    assert any(r.levelno == logging.ERROR and job.id in r.getMessage() for r in caplog.records)


def test_regression_c_job_outlives_main_analysis(make_manager, gate):
    """Mandatory regression test C."""
    m = make_manager(max_workers=2)
    g = gate()
    marker_calls = []
    order = []

    def long_upload():
        g()
        order.append("job finished")
        return "uploaded"

    # Part 1: the barrier blocks until the job reaches a terminal state.
    with m.workflow() as run:
        job = run.parallel_job(long_upload)
        assert g.started.wait(5)
        order.append("analysis finished")
        assert m.get_status(job) is JobStatus.RUNNING
        threading.Timer(0.2, g.release.set).start()
        run.wait_all(raise_on_failure=True)
        order.append("barrier passed")
    marker_calls.append(True)
    assert order == ["analysis finished", "job finished", "barrier passed"]
    assert m.get_status(job) is JobStatus.COMPLETED

    # Part 2: a barrier timeout never yields success and never mislabels the job.
    g2 = gate()
    marker_calls.clear()
    with pytest.raises(sidejob.WaitTimeoutError) as excinfo:
        with m.workflow() as run:
            job2 = run.parallel_job(g2, "late")
            assert g2.started.wait(5)
            run.wait_all(timeout=0.1, raise_on_failure=True)
        marker_calls.append(True)
    assert marker_calls == []
    assert excinfo.value.pending_job_ids == (job2.id,)
    assert "NOT successful" in str(excinfo.value)
    assert m.get_status(job2) is JobStatus.RUNNING  # not COMPLETED, FAILED or CANCELLED
    assert any(e.kind == "wait_timeout" for e in m.get_run(job2).events)
    assert m.get_summary()["ok"] is False
    g2.release.set()
    assert m.wait(job2, timeout=5).status is JobStatus.COMPLETED


def test_wait_all_timeout_raises_even_without_raise_on_failure(make_manager, gate):
    m = make_manager()
    g = gate()
    job = m.parallel_job(g)
    assert g.started.wait(5)
    with pytest.raises(sidejob.WaitTimeoutError):
        m.wait_all(timeout=0.05, raise_on_failure=False)
    with pytest.raises(TimeoutError):  # also a builtin TimeoutError
        m.wait_all(timeout=0)
    assert m.get_status(job) is JobStatus.RUNNING
    g.release.set()
    assert m.wait_all(timeout=5).ok


def test_wait_all_includes_queued_and_retrying(make_manager, gate):
    m = make_manager(max_attempts=3, max_workers=1)
    g = gate()
    blocker = m.parallel_job(g)
    flaky = Flaky(1, ConnectionError("x"))
    queued = m.parallel_job(flaky)
    assert g.started.wait(5)
    assert m.get_status(queued) is JobStatus.QUEUED
    threading.Timer(0.05, g.release.set).start()
    report = m.wait_all(timeout=10)
    assert {r.job_id for r in report.completed} == {blocker.id, queued.id}
    assert m.get_run(queued).attempt == 2


def test_wait_all_optional_failure_does_not_raise(make_manager, caplog):
    m = make_manager()
    ok = m.parallel_job(lambda: 1)
    opt = m.parallel_job(int, "x", required=False)
    report = m.wait_all(timeout=5)
    assert report.ok
    assert [r.job_id for r in report.optional_failures] == [opt.id]
    assert [r.job_id for r in report.completed] == [ok.id]
    assert any(
        r.levelno == logging.WARNING and opt.id in r.getMessage() and "optional" in r.getMessage()
        for r in caplog.records
    )


def test_wait_all_required_failure_aggregates_all(make_manager):
    m = make_manager()
    a = m.parallel_job(int, "a")
    b = m.parallel_job(Flaky(5, KeyError("k")))
    c = m.parallel_job(lambda: "fine")
    with pytest.raises(sidejob.JobsFailedError) as excinfo:
        m.wait_all(timeout=5)
    err = excinfo.value
    assert set(err.job_ids) == {a.id, b.id}
    assert "2 required job(s)" in str(err)
    assert err.report is not None and len(err.report.runs) == 3
    assert m.get_status(c) is JobStatus.COMPLETED


def test_wait_all_raise_on_failure_false_returns_report(make_manager, caplog):
    m = make_manager()
    bad = m.parallel_job(int, "x")
    report = m.wait_all(timeout=5, raise_on_failure=False)
    assert not report.ok
    assert [r.job_id for r in report.required_failures] == [bad.id]
    assert any("raise_on_failure=False" in r.getMessage() for r in caplog.records)


def test_wait_all_specific_jobs_only(make_manager, gate):
    m = make_manager()
    g = gate()
    m.parallel_job(g)  # unrelated, still running
    bad = m.parallel_job(int, "x", required=False)
    good = m.parallel_job(lambda: 1)
    assert m.wait_all(jobs=[good, bad.id], timeout=5).ok


def test_wait_all_catches_jobs_submitted_while_waiting(make_manager):
    m = make_manager()
    started = threading.Event()
    children = []

    def parent():
        started.set()
        children.append(m.parallel_job(int, "late failure"))

    m.parallel_job(parent)
    with pytest.raises(sidejob.JobsFailedError) as excinfo:
        m.wait_all(timeout=5)
    assert excinfo.value.job_ids == (children[0].id,)


def test_cancelled_required_job_fails_barrier(make_manager, gate):
    m = make_manager(max_workers=1)
    g = gate()
    m.parallel_job(g)
    queued = m.parallel_job(lambda: 1)
    assert g.started.wait(5)
    assert m.cancel(queued) is True
    g.release.set()
    with pytest.raises(sidejob.JobsFailedError) as excinfo:
        m.wait_all(timeout=5)
    assert excinfo.value.job_ids == (queued.id,)
    assert "CANCELLED" in str(excinfo.value)


def test_job_cannot_wait_for_itself(make_manager):
    m = make_manager()

    def selfish():
        m.wait_all()

    job = m.parallel_job(selfish, required=False)
    run = m.wait(job, timeout=5)
    assert run.status is JobStatus.FAILED
    assert "wait for itself" in run.error_message


def test_get_result_timeout_and_cancel_semantics(make_manager, gate):
    m = make_manager(max_workers=1)
    g = gate()
    running = m.parallel_job(g, "v")
    queued = m.parallel_job(lambda: 1)
    assert g.started.wait(5)
    with pytest.raises(sidejob.WaitTimeoutError):
        m.get_result(running, timeout=0.05)
    assert m.get_status(running) is JobStatus.RUNNING
    assert m.cancel(queued)
    with pytest.raises(sidejob.JobCancelledError):
        m.get_result(queued)
    g.release.set()
    assert m.get_result(running, timeout=5) == "v"
    with pytest.raises(ValueError):
        m.wait(running, timeout=-1)
