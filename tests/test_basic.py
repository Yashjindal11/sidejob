from __future__ import annotations

import logging
import threading
from datetime import datetime

import pytest

import sidejob
from conftest import Gate, wait_until
from sidejob import JobStatus


def add(a: int, b: int = 0) -> int:
    return a + b


def test_success_and_return_value(make_manager):
    m = make_manager()
    job = m.parallel_job(add, 2, b=3)
    assert isinstance(job, sidejob.Job)
    assert m.get_result(job, timeout=5) == 5
    assert job.result() == 5
    run = m.get_run(job.id)
    assert run.status is JobStatus.COMPLETED
    assert run.name == "add"
    assert run.required is True
    assert run.result == 5
    assert run.attempt == 1 and run.max_attempts == 3
    assert isinstance(run.submitted_at, datetime) and run.submitted_at.tzinfo is not None
    assert run.submitted_at <= run.started_at <= run.finished_at
    assert run.duration is not None and run.duration >= 0
    assert [a.succeeded for a in run.attempts] == [True]
    assert run.error_type is None and run.exception is None


def test_unique_ids_across_jobs_and_managers(make_manager):
    m1, m2 = make_manager(), make_manager()
    ids = [m1.parallel_job(add, i).id for i in range(50)]
    ids += [m2.parallel_job(add, i).id for i in range(50)]
    assert len(set(ids)) == 100
    m1.wait_all(timeout=5)
    m2.wait_all(timeout=5)


def test_multiple_concurrent_submissions(make_manager):
    m = make_manager(max_workers=4)
    barrier = threading.Barrier(4, timeout=5)

    def rendezvous(i):
        barrier.wait()  # only passes if 4 jobs truly run at the same time
        return i

    jobs = [m.parallel_job(rendezvous, i) for i in range(4)]
    report = m.wait_all(timeout=10)
    assert report.ok
    assert [j.result() for j in jobs] == [0, 1, 2, 3]


def test_worker_pool_limit_is_respected(make_manager, gate):
    m = make_manager(max_workers=2)
    lock = threading.Lock()
    running = {"now": 0, "max": 0}
    g = gate()

    def tracked():
        with lock:
            running["now"] += 1
            running["max"] = max(running["max"], running["now"])
        try:
            g()
        finally:
            with lock:
                running["now"] -= 1

    jobs = [m.parallel_job(tracked) for _ in range(6)]
    wait_until(lambda: sum(m.get_status(j) is JobStatus.RUNNING for j in jobs) == 2)
    statuses = [m.get_status(j) for j in jobs]
    assert statuses.count(JobStatus.RUNNING) == 2
    assert statuses.count(JobStatus.QUEUED) == 4
    g.release.set()
    m.wait_all(timeout=10)
    assert running["max"] == 2
    assert m.max_workers == 2


def test_status_transitions_queued_running_completed(make_manager, gate):
    m = make_manager(max_workers=1)
    blocker, target = gate(), gate()
    j1 = m.parallel_job(blocker)
    j2 = m.parallel_job(target, "value")
    assert blocker.started.wait(5)
    assert m.get_status(j2) is JobStatus.QUEUED
    blocker.release.set()
    assert target.started.wait(5)
    assert m.get_status(j2) is JobStatus.RUNNING
    assert m.get_status(j1) is JobStatus.COMPLETED
    target.release.set()
    assert m.wait(j2, timeout=5).status is JobStatus.COMPLETED
    kinds = [e.kind for e in m.get_run(j2).events]
    assert kinds == ["submitted", "attempt_started", "completed"]


def test_status_retrying_during_backoff(make_manager):
    m = make_manager(retry_backoff=30)
    calls = []

    def fails():
        calls.append(1)
        raise ConnectionError("db unavailable")

    job = m.parallel_job(fails)
    wait_until(lambda: m.get_status(job) is JobStatus.RETRYING)
    run = m.get_run(job)
    assert run.attempts[0].retry_decision == "retry"
    assert run.attempts[0].retry_delay == 30
    assert m.cancel(job) is True  # cancel during backoff: no further attempts
    assert m.wait(job, timeout=5).status is JobStatus.CANCELLED
    assert len(calls) == 1


def test_unknown_job_ids(make_manager):
    m, other = make_manager(), make_manager()
    foreign = other.parallel_job(add, 1)
    for call in (m.get_run, m.get_status, m.get_result, m.wait, m.cancel):
        with pytest.raises(sidejob.UnknownJobError):
            call("nope")
        with pytest.raises(sidejob.UnknownJobError):
            call(foreign)
    with pytest.raises(sidejob.UnknownJobError):
        m.wait_all(jobs=["nope"])
    with pytest.raises(LookupError):  # UnknownJobError is a LookupError
        m.get_status("nope")
    with pytest.raises(TypeError):
        m.get_status(123)  # type: ignore[arg-type]


def test_get_all_runs_filters_and_snapshots(make_manager):
    m = make_manager()
    ok = m.parallel_job(add, 1)
    bad = m.parallel_job(int, "x", required=False)  # ValueError, not retried
    m.wait_all(timeout=5)
    runs = m.get_all_runs()
    assert [r.job_id for r in runs] == [ok.id, bad.id]
    assert all(isinstance(r, sidejob.JobRun) for r in runs)
    assert [r.job_id for r in m.get_all_runs("FAILED")] == [bad.id]
    assert [r.job_id for r in m.get_all_runs(JobStatus.COMPLETED)] == [ok.id]
    assert [r.job_id for r in m.get_all_runs(["completed", "failed"])] == [ok.id, bad.id]
    assert [r.job_id for r in m.get_all_runs(required=False)] == [bad.id]
    assert [r.job_id for r in m.get_failed_runs()] == [bad.id]
    d = runs[1].to_dict()
    assert d["status"] == "FAILED" and d["error_type"] == "ValueError" and "result" not in d
    with pytest.raises(dataclass_frozen_error()):
        runs[0].status = JobStatus.FAILED  # type: ignore[misc]


def dataclass_frozen_error():
    import dataclasses

    return dataclasses.FrozenInstanceError


def test_get_summary(make_manager, gate):
    m = make_manager()
    g = gate()
    m.parallel_job(add, 1)
    failed = m.parallel_job(int, "x")
    opt = m.parallel_job(int, "y", required=False)
    m.wait_all(jobs=[failed, opt], raise_on_failure=False, timeout=5)
    running = m.parallel_job(g)
    assert g.started.wait(5)
    s = m.get_summary()
    assert s["total"] == 4
    assert s["by_status"]["RUNNING"] == 1
    assert s["unfinished_job_ids"] == [running.id]
    assert s["failed_job_ids"] == [failed.id, opt.id]
    assert s["required_failed_job_ids"] == [failed.id]
    assert s["optional_failed_job_ids"] == [opt.id]
    assert s["ok"] is False
    g.release.set()


def test_thread_safe_submissions_and_reads(make_manager):
    m = make_manager(max_workers=8)
    ids, errors = [], []
    lock = threading.Lock()
    start = threading.Barrier(8)

    def submitter(t):
        try:
            start.wait()
            for i in range(50):
                job = m.parallel_job(add, t, i)
                m.get_status(job)
                m.get_all_runs()
                m.get_summary()
                with lock:
                    ids.append(job.id)
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=submitter, args=(t,)) for t in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert not errors
    assert len(ids) == len(set(ids)) == 400
    report = m.wait_all(timeout=30)
    assert len(report.completed) == 400
    assert m.get_summary()["by_status"]["COMPLETED"] == 400


def test_independent_managers(make_manager, gate):
    m1, m2 = make_manager(max_workers=1, name="a"), make_manager(max_workers=1, name="b")
    g = gate()
    j1 = m1.parallel_job(g)
    assert g.started.wait(5)
    # m1's only worker is busy; m2 has its own pool and runs immediately.
    j2 = m2.parallel_job(add, 1, 1)
    assert m2.get_result(j2, timeout=5) == 2
    assert [r.job_id for r in m2.get_all_runs()] == [j2.id]
    m2.shutdown()
    with pytest.raises(sidejob.ManagerShutdownError):
        m2.parallel_job(add, 1)
    assert not m1.is_shutdown
    g.release.set()
    assert m1.get_result(j1, timeout=5) is None


def test_logging_includes_job_id_and_attempt(make_manager, caplog):
    m = make_manager(name="logtest")
    caplog.set_level(logging.DEBUG, logger="sidejob")
    job = m.parallel_job(add, 1, job_name="adder")
    m.wait_all(timeout=5)
    msgs = [r for r in caplog.records if getattr(r, "sidejob_job_id", None) == job.id]
    text = [r.getMessage() for r in msgs]
    assert any("submitted" in t for t in text)
    assert any("attempt 1/3: started" in t for t in text)
    assert any("COMPLETED" in t for t in text)
    assert all(job.id in t and "[adder]" in t for t in text)
    assert all(r.name == "sidejob.logtest" for r in msgs)
    assert {r.sidejob_attempt for r in msgs if "started" in r.getMessage()} == {1}


def test_log_level_and_no_root_configuration(make_manager):
    root_handlers = list(logging.getLogger().handlers)
    m = make_manager(log_level=logging.WARNING, name="quiet")
    assert m.logger.level == logging.WARNING
    assert logging.getLogger().handlers == root_handlers


def test_console_handler_added_at_most_once():
    base = logging.getLogger("sidejob")
    before = list(base.handlers)
    try:
        m1 = sidejob.initialize(log_to_console=True)
        m2 = sidejob.initialize(log_to_console=True)
        ours = [h for h in base.handlers if h not in before]
        assert len(ours) == 1
        m1.shutdown()
        m2.shutdown()
        sidejob.initialize(log_to_console=False).shutdown()
        assert [h for h in base.handlers if h not in before] == []
    finally:
        base.handlers = before


def test_custom_logger(make_manager, caplog):
    lg = logging.getLogger("myapp.jobs")
    m = make_manager(logger=lg, log_level=logging.INFO)
    m.parallel_job(add, 1)
    m.wait_all(timeout=5)
    assert any(r.name == "myapp.jobs" for r in caplog.records)


def test_failures_visible_with_logging_disabled(make_manager):
    m = make_manager(log_level=logging.CRITICAL + 10)
    job = m.parallel_job(int, "boom")
    with pytest.raises(sidejob.JobsFailedError):
        m.wait_all(timeout=5)
    assert m.get_status(job) is JobStatus.FAILED
    assert m.get_run(job).error_type == "ValueError"
    assert [r.job_id for r in m.get_failed_runs()] == [job.id]
    assert m.get_summary()["required_failed_job_ids"] == [job.id]


def test_func_kwargs_escape_hatch_and_option_validation(make_manager):
    m = make_manager()

    def takes_required(required):
        return required

    job = m.parallel_job(takes_required, func_kwargs={"required": "passed-through"})
    assert job.result(timeout=5) == "passed-through"
    assert job.required is True
    with pytest.raises(TypeError):
        m.parallel_job(add, 1, b=2, func_kwargs={"b": 3})
    with pytest.raises(TypeError):
        m.parallel_job("not callable")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        m.parallel_job(add, 1, retries=1, max_attempts=2)
    with pytest.raises(ValueError):
        m.parallel_job(add, 1, job_deadline=0)


def test_job_deadline_reports_overdue_without_killing(make_manager, gate, caplog):
    m = make_manager()
    g = gate()
    job = m.parallel_job(g, "done", job_deadline=0.05)
    assert g.started.wait(5)
    with pytest.raises(sidejob.WaitTimeoutError):
        m.wait(job, timeout=0.2)
    run = m.get_run(job)
    assert run.overdue is True and run.status is JobStatus.RUNNING
    assert any(e.kind == "overdue" for e in run.events)
    assert any("OVERDUE" in r.getMessage() for r in caplog.records)
    g.release.set()
    assert job.result(timeout=5) == "done"
    assert m.get_summary()["overdue_job_ids"] == [job.id]


def test_job_handle_helpers(make_manager):
    m = make_manager()
    job = m.parallel_job(add, 40, 2)
    assert job.wait(timeout=5).status is JobStatus.COMPLETED
    assert job.status is JobStatus.COMPLETED
    assert job.snapshot().result == 42
    assert job.cancel() is False
    assert job.id in repr(job)
    assert job in {job}
    assert m.get_status(job.id) is JobStatus.COMPLETED


def test_partial_name(make_manager):
    import functools

    m = make_manager()
    job = m.parallel_job(functools.partial(add, 1), 2)
    assert job.name == "add"
    assert job.result(timeout=5) == 3


def test_gate_helper_sanity():
    g = Gate()
    g.release.set()
    assert g("x") == "x"
