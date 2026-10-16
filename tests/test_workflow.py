from __future__ import annotations

import logging
import sys

import pytest

import sidejob
from conftest import Flaky
from sidejob import JobStatus


def test_intended_api_happy_path(make_manager):
    m = make_manager(max_workers=4, log_level=logging.INFO, retries=3, retry_backoff=0)
    marker = []
    with m.workflow() as run:
        j1 = run.parallel_job(
            lambda df, t: f"{t}:{len(df)}", [1, 2], "analytics_table", required=True
        )
        j2 = run.parallel_job(lambda df, p: p, [1, 2], "output.parquet", required=True)
        j3 = run.parallel_job(lambda msg: msg, "Analysis started", required=False)
        summary = sum([1, 2])
        report = run.wait_all(raise_on_failure=True)
    marker.append(summary)
    assert marker == [3]
    assert report.ok and len(report.runs) == 3
    assert m.get_run(j1).max_attempts == 4  # retries=3 -> 4 total attempts
    assert {j.workflow_id for j in (j1, j2, j3)} == {run.id}
    assert [r.job_id for r in run.get_runs()] == [j1.id, j2.id, j3.id]
    assert [j.id for j in run.jobs] == [j1.id, j2.id, j3.id]


def test_workflow_wait_all_ignores_unrelated_jobs(make_manager, gate):
    m = make_manager()
    g = gate()
    m.parallel_job(int, "unrelated failure")  # manager-level, required, fails
    m.parallel_job(g)  # manager-level, still running
    with m.workflow() as other:
        other.parallel_job(int, "other workflow failure", required=False)
        other.wait_all()
    with m.workflow() as run:
        run.parallel_job(lambda: "mine")
        report = run.wait_all(timeout=5)
    assert len(report.runs) == 1 and report.ok


def test_optional_failure_does_not_fail_workflow(make_manager):
    m = make_manager()
    marker = []
    with m.workflow() as run:
        opt = run.parallel_job(Flaky(9, ConnectionError("smtp down")), required=False, retries=1)
        run.parallel_job(lambda: 1)
        report = run.wait_all()
    marker.append(1)
    assert marker == [1]
    assert [r.job_id for r in report.optional_failures] == [opt.id]
    assert m.get_status(opt) is JobStatus.FAILED
    assert [r.job_id for r in m.get_failed_runs()] == [opt.id]


def test_exit_enforces_barrier_when_wait_all_forgotten_and_job_failed(make_manager):
    m = make_manager()
    marker = []
    with pytest.raises(sidejob.JobsFailedError):
        with m.workflow() as run:
            run.parallel_job(int, "x")
        marker.append(1)
    assert marker == []


def test_strict_exit_raises_if_wait_all_never_called(make_manager):
    m = make_manager()
    with pytest.raises(sidejob.WaitAllNotCalledError) as excinfo:
        with m.workflow() as run:
            job = run.parallel_job(lambda: 1)
    assert job.id in str(excinfo.value)
    assert m.get_status(job) is JobStatus.COMPLETED  # it was still waited for


def test_non_strict_exit_warns(make_manager, caplog):
    m = make_manager()
    with m.workflow(strict=False) as run:
        job = run.parallel_job(lambda: 1)
    assert m.get_status(job) is JobStatus.COMPLETED
    assert any(
        r.levelno == logging.WARNING and "without a successful explicit wait_all" in r.getMessage()
        for r in caplog.records
    )


def test_jobs_submitted_after_wait_all_are_not_verified(make_manager):
    m = make_manager()
    with pytest.raises(sidejob.WaitAllNotCalledError) as excinfo:
        with m.workflow() as run:
            first = run.parallel_job(lambda: 1)
            run.wait_all()
            late = run.parallel_job(lambda: 2)
    assert late.id in str(excinfo.value) and first.id not in str(excinfo.value)


def test_raise_on_failure_false_does_not_satisfy_barrier(make_manager):
    m = make_manager()
    with pytest.raises(sidejob.JobsFailedError):
        with m.workflow() as run:
            run.parallel_job(int, "x")
            report = run.wait_all(raise_on_failure=False)
            assert not report.ok
    with pytest.raises(sidejob.WaitAllNotCalledError):
        with m.workflow() as run:
            run.parallel_job(lambda: 1)
            run.wait_all(raise_on_failure=False)


def test_empty_workflow_is_fine(make_manager):
    m = make_manager()
    with m.workflow() as run:
        pass
    with m.workflow() as run:
        assert run.wait_all().ok


def test_main_thread_exception_is_preserved(make_manager, gate, caplog):
    m = make_manager(max_workers=1)
    g = gate()

    class AnalysisError(Exception):
        pass

    with pytest.raises(AnalysisError) as excinfo:
        with m.workflow() as run:
            failing = run.parallel_job(int, "bad upload")
            m.wait(failing, timeout=5)
            blocker = run.parallel_job(g)
            queued = run.parallel_job(lambda: "never runs")
            assert g.started.wait(5)
            g.release.set()
            raise AnalysisError("analysis blew up")
    # original exception, not JobsFailedError / WaitAllNotCalledError
    assert str(excinfo.value) == "analysis blew up"
    assert m.get_status(failing) is JobStatus.FAILED
    assert m.get_status(blocker) is JobStatus.COMPLETED  # running job was waited for
    assert m.get_status(queued) in (JobStatus.CANCELLED, JobStatus.COMPLETED)
    assert any(
        r.levelno == logging.ERROR
        and "background" in r.getMessage()
        and failing.id in r.getMessage()
        for r in caplog.records
    )
    if sys.version_info >= (3, 11):
        assert any(failing.id in n for n in excinfo.value.__notes__)


def test_cancel_on_error_cancels_queued_jobs(make_manager, gate):
    m = make_manager(max_workers=1)
    g = gate()
    with pytest.raises(RuntimeError, match="stop"):
        with m.workflow() as run:
            running = run.parallel_job(g)
            queued = run.parallel_job(lambda: 1)
            assert g.started.wait(5)
            g.release.set()
            raise RuntimeError("stop")
    assert m.get_status(queued) is JobStatus.CANCELLED or m.get_run(queued).attempt == 1
    assert m.get_status(running) is JobStatus.COMPLETED


def test_cancel_on_error_deterministic(make_manager, gate):
    m = make_manager(max_workers=1)
    g = gate()
    blocker = m.parallel_job(g)  # occupies the only worker, outside the workflow
    with pytest.raises(RuntimeError):
        with m.workflow(exit_timeout=5) as run:
            queued = run.parallel_job(lambda: 1)
            assert g.started.wait(5)
            raise RuntimeError("analysis failed")
    assert m.get_status(queued) is JobStatus.CANCELLED
    g.release.set()
    assert m.wait(blocker, timeout=5).status is JobStatus.COMPLETED


def test_error_exit_with_keep_queued_jobs(make_manager, gate):
    m = make_manager(max_workers=1)
    g = gate()
    m.parallel_job(g)
    with pytest.raises(RuntimeError):
        with m.workflow(cancel_on_error=False, exit_timeout=0.05) as run:
            queued = run.parallel_job(lambda: 1)
            raise RuntimeError("x")
    assert m.get_status(queued) is JobStatus.QUEUED  # not cancelled, still tracked
    g.release.set()
    assert m.wait(queued, timeout=5).status is JobStatus.COMPLETED


def test_keyboard_interrupt_does_not_wait(make_manager, gate):
    m = make_manager()
    g = gate()
    with pytest.raises(KeyboardInterrupt):
        with m.workflow() as run:
            job = run.parallel_job(g)
            assert g.started.wait(5)
            raise KeyboardInterrupt
    assert m.get_status(job) is JobStatus.RUNNING
    g.release.set()


def test_exit_timeout_on_success_path(make_manager, gate):
    m = make_manager()
    g = gate()
    marker = []
    with pytest.raises(sidejob.WaitTimeoutError):
        with m.workflow(exit_timeout=0.05) as run:
            job = run.parallel_job(g)
        marker.append(1)
    assert marker == []
    assert m.get_status(job) is JobStatus.RUNNING
    g.release.set()


def test_workflow_closed_and_reentry(make_manager):
    m = make_manager()
    wf = m.workflow()
    with pytest.raises(sidejob.WorkflowClosedError):
        wf.parallel_job(lambda: 1)
    with wf as run:
        run.wait_all()
    with pytest.raises(sidejob.WorkflowClosedError):
        run.parallel_job(lambda: 1)
    with pytest.raises(sidejob.SidejobError):
        with wf:
            pass


def test_workflow_after_shutdown(make_manager):
    m = make_manager()
    m.shutdown()
    with pytest.raises(sidejob.ManagerShutdownError):
        m.workflow()
