"""Cooperative cancellation, non-blocking retries, retention, and retry context."""

from __future__ import annotations

import gc
import logging
import threading
import time
import weakref

import pytest

import sidejob
from conftest import Flaky, wait_until
from sidejob import JobStatus

# ----------------------------------------------------------------- cooperative cancellation


def test_helpers_are_noops_outside_jobs():
    assert sidejob.current_job() is None
    assert sidejob.cancel_requested() is False
    sidejob.check_cancelled()
    sidejob.on_cancel(lambda: None)
    sidejob.sleep(0)


def test_check_cancelled_stops_running_job(make_manager):
    m = make_manager()
    started, progress = threading.Event(), []

    def export(chunks):
        started.set()
        for i in range(chunks):
            sidejob.check_cancelled()
            progress.append(i)
            time.sleep(0.005)
        return "done"

    job = m.parallel_job(export, 10_000, required=False)
    assert started.wait(5)
    assert m.cancel(job) is False  # requested, not yet stopped
    run = m.wait(job, timeout=5)
    assert run.status is JobStatus.CANCELLED
    assert run.attempt == 1  # a cancelled job is never retried
    assert run.attempts[0].error_type == "JobCancelledError"
    assert run.attempts[0].retry_decision == "cancelled"
    assert len(progress) < 10_000
    with pytest.raises(sidejob.JobCancelledError):
        m.get_result(job)


def test_cancellation_aware_sleep_wakes_immediately(make_manager):
    m = make_manager()
    sleeping = threading.Event()

    def poll():
        sleeping.set()
        sidejob.sleep(60)

    job = m.parallel_job(poll, required=False)
    assert sleeping.wait(5)
    t0 = time.monotonic()
    m.cancel(job)
    assert m.wait(job, timeout=5).status is JobStatus.CANCELLED
    assert time.monotonic() - t0 < 2


def test_on_cancel_hook_aborts_blocking_call(make_manager, caplog):
    """Simulates a DB driver: conn.cancel() from another thread aborts the query."""
    m = make_manager()

    class FakeConnection:
        def __init__(self):
            self.aborted = threading.Event()
            self.cancel_thread = None

        def cancel(self):
            self.cancel_thread = threading.current_thread().name
            self.aborted.set()

        def execute(self):
            if self.aborted.wait(10):
                raise RuntimeError("canceling statement due to user request")
            return "rows"

    conn = FakeConnection()
    in_query = threading.Event()

    def run_query():
        sidejob.on_cancel(conn.cancel)
        in_query.set()
        return conn.execute()

    job = m.parallel_job(run_query, required=False)
    assert in_query.wait(5)
    m.cancel(job)
    run = m.wait(job, timeout=5)
    assert run.status is JobStatus.CANCELLED
    assert run.attempts[0].error_type == "RuntimeError"  # driver's error is recorded
    assert conn.cancel_thread == threading.current_thread().name
    assert any("on_cancel hook" in r.getMessage() for r in caplog.records)


def test_on_cancel_runs_immediately_if_already_requested_and_errors_are_logged(
    make_manager, caplog
):
    m = make_manager()
    proceed, calls = threading.Event(), []

    def job_body():
        assert proceed.wait(5)
        sidejob.on_cancel(lambda: calls.append("late"))

        def broken():
            raise OSError("hook failed")

        sidejob.on_cancel(broken)
        return "ignored the request"

    job = m.parallel_job(job_body, required=False)
    wait_until(lambda: m.get_status(job) is JobStatus.RUNNING)
    m.cancel(job)
    proceed.set()
    assert job.result(timeout=5) == "ignored the request"
    assert calls == ["late"]
    assert any(
        r.levelno == logging.ERROR and "hook failed" in r.getMessage() for r in caplog.records
    )


def test_cancel_on_deadline(make_manager, caplog):
    m = make_manager()

    def slow():
        sidejob.sleep(60)

    job = m.parallel_job(slow, job_deadline=0.1, cancel_on_deadline=True, required=False)
    run = m.wait(job, timeout=5)
    assert run.status is JobStatus.CANCELLED
    assert run.overdue is True
    assert any("requesting cooperative cancellation" in r.getMessage() for r in caplog.records)
    with pytest.raises(ValueError):
        m.parallel_job(slow, cancel_on_deadline=True)


def test_overdue_is_reported_without_anyone_waiting(make_manager, gate, caplog):
    m = make_manager()
    g = gate()
    job = m.parallel_job(g, job_deadline=0.05)
    assert g.started.wait(5)
    wait_until(
        lambda: any("OVERDUE" in r.getMessage() for r in caplog.records)
    )  # scheduler-driven, no query/wait involved
    assert m.get_status(job) is JobStatus.RUNNING
    g.release.set()


def test_workflow_error_requests_cancellation_of_running_jobs(make_manager):
    m = make_manager()
    started = threading.Event()

    def long_upload():
        started.set()
        sidejob.sleep(60)

    with pytest.raises(KeyError):
        with m.workflow(exit_timeout=5) as run:
            job = run.parallel_job(long_upload)
            assert started.wait(5)
            raise KeyError("analysis failed")
    assert m.get_status(job) is JobStatus.CANCELLED


# ----------------------------------------------------------------- non-blocking backoff


def test_retry_backoff_does_not_hold_a_worker(make_manager):
    m = make_manager(max_attempts=3, max_workers=1, retry_backoff=30)
    flaky = m.parallel_job(Flaky(5, ConnectionError("down")), required=False)
    wait_until(lambda: m.get_status(flaky) is JobStatus.RETRYING)
    other = m.parallel_job(lambda: "ran during the backoff")
    assert other.result(timeout=5) == "ran during the backoff"
    assert m.get_status(flaky) is JobStatus.RETRYING
    assert m.cancel(flaky) is True


def test_retry_is_redispatched_to_the_pool(make_manager):
    m = make_manager(max_attempts=3, max_workers=1, retry_backoff=0.05)
    threads = []

    def record_thread():
        threads.append(threading.current_thread().name)
        if len(threads) < 3:
            raise ConnectionError("blip")
        return "ok"

    assert m.get_result(m.parallel_job(record_thread), timeout=5) == "ok"
    assert len(threads) == 3
    assert all(t.startswith(f"sidejob-{m.name}") and "scheduler" not in t for t in threads)


def test_scheduler_thread_exits_when_idle(make_manager):
    m = make_manager(max_attempts=3, retry_backoff=0.01)
    m.get_result(m.parallel_job(Flaky(1, ConnectionError("x"))), timeout=5)
    wait_until(lambda: m._scheduler is None)
    assert not any(
        t.name.endswith("-scheduler") and m.name in t.name for t in threading.enumerate()
    )


def test_shutdown_without_wait_still_runs_scheduled_retry(make_manager):
    m = make_manager(max_attempts=3, retry_backoff=0.1)
    job = m.parallel_job(Flaky(1, ConnectionError("x"), result="late success"))
    wait_until(lambda: m.get_status(job) is JobStatus.RETRYING)
    assert m.shutdown(wait=False) is None
    assert job.result(timeout=5) == "late success"
    wait_until(lambda: m._executor_released)


# ----------------------------------------------------------------- memory retention


def test_inputs_are_released_after_completion(make_manager):
    m = make_manager()

    class Big:
        pass

    big = Big()
    ref = weakref.ref(big)
    job = m.parallel_job(lambda obj: "done", big)
    assert job.result(timeout=5) == "done"
    del big
    gc.collect()
    assert ref() is None


def test_retried_attempt_tracebacks_release_locals(make_manager):
    m = make_manager(max_attempts=3, retry_backoff=0)
    refs = []

    class Big:
        pass

    def flaky():
        local_df = Big()
        refs.append(weakref.ref(local_df))
        if len(refs) == 1:
            raise ConnectionError("blip")
        return "ok"

    job = m.parallel_job(flaky)
    assert job.result(timeout=5) == "ok"
    gc.collect()
    assert refs[0]() is None
    assert "ConnectionError" in m.get_run(job).attempts[0].traceback


def test_history_limit_evicts_only_completed(make_manager):
    m = make_manager(history_limit=3)
    bad = m.parallel_job(int, "x", required=False)
    m.wait(bad, timeout=5)
    handles = [m.parallel_job(lambda i=i: i) for i in range(10)]
    m.wait_all(jobs=handles, timeout=5)
    ids = [r.job_id for r in m.get_all_runs()]
    assert len(ids) == 3
    assert bad.id in ids  # failures are never evicted automatically
    assert handles[0].result() == 0  # a held handle still works
    with pytest.raises(sidejob.UnknownJobError, match="history_limit"):
        m.get_run(handles[0].id)
    assert m.get_summary()["evicted"] == 8


def test_history_limit_keeps_open_workflow_jobs(make_manager):
    m = make_manager(history_limit=1)
    with m.workflow() as run:
        jobs = [run.parallel_job(lambda i=i: i) for i in range(5)]
        report = run.wait_all(timeout=5)
        assert len(report.runs) == 5
        assert len(m.get_all_runs()) == 5
    assert len(m.get_all_runs()) == 1
    assert [j.result() for j in jobs] == [0, 1, 2, 3, 4]


def test_clear_history(make_manager):
    m = make_manager()
    ok = m.parallel_job(lambda: 1)
    bad = m.parallel_job(int, "x", required=False)
    m.wait_all(timeout=5)
    assert m.clear_history() == 1
    assert [r.job_id for r in m.get_all_runs()] == [bad.id]
    assert m.clear_history(include_failed=True) == 1
    assert m.get_all_runs() == []
    assert ok.result() == 1
    with pytest.raises(ValueError):
        sidejob.initialize(history_limit=-1)


# ----------------------------------------------------------------- idempotent retries


def test_job_context_for_idempotent_retries(make_manager):
    """A retry can detect that an earlier attempt's write already committed."""
    m = make_manager(max_attempts=3, retry_backoff=0)
    table = {}
    seen = []

    def upload(rows):
        ctx = sidejob.current_job()
        seen.append((ctx.attempt, ctx.is_retry, ctx.idempotency_key))
        if ctx.is_retry and ctx.idempotency_key in table:
            return "already loaded"
        table[ctx.idempotency_key] = rows  # the write commits ...
        if ctx.attempt == 1:
            raise ConnectionError("... but the connection drops before the ack")
        return "loaded"

    job = m.parallel_job(upload, [1, 2, 3])
    assert job.result(timeout=5) == "already loaded"
    assert len(table) == 1  # no duplicate write
    assert seen == [(1, False, job.id), (2, True, job.id)]
    ctx_repr = []
    m.get_result(m.parallel_job(lambda: ctx_repr.append(repr(sidejob.current_job()))), timeout=5)
    assert "attempt=1/3" in ctx_repr[0]


# ----------------------------------------------------------------- review regressions


def test_done_is_signalled_only_after_cancel_is_logged(make_manager):
    """A worker racing with cancel() must not set ``done`` before CANCELLED is logged."""
    m = make_manager(max_attempts=3, retry_backoff=30)

    class Pause(logging.Handler):
        def __init__(self):
            super().__init__()
            self.retry_in, self.retry_go = threading.Event(), threading.Event()
            self.cancel_in, self.cancel_go = threading.Event(), threading.Event()

        def handle(self, record):  # bypass the handler lock so both threads can block
            msg = record.getMessage()
            if "RETRYING in" in msg:
                self.retry_in.set()
                self.retry_go.wait(5)
            elif "CANCELLED while" in msg:
                self.cancel_in.set()
                self.cancel_go.wait(5)
            return True

    pause = Pause()
    m.logger.addHandler(pause)
    try:
        job = m.parallel_job(Flaky(5, ConnectionError("x")), required=False)
        assert pause.retry_in.wait(5)  # worker is between its two lock holds
        canceller = threading.Thread(target=m.cancel, args=(job,))
        canceller.start()
        assert pause.cancel_in.wait(5)  # status is CANCELLED, log line not yet written
        pause.retry_go.set()
        wait_until(lambda: job._rec.future.done())  # worker returned
        assert not job._rec.done.is_set()
        with pytest.raises(sidejob.WaitTimeoutError):
            m.wait(job, timeout=0.05)
        pause.cancel_go.set()
        canceller.join(5)
        assert m.wait(job, timeout=5).status is JobStatus.CANCELLED
    finally:
        pause.retry_go.set()
        pause.cancel_go.set()
        m.logger.removeHandler(pause)


def test_cancel_on_deadline_not_skipped_when_a_query_flags_overdue_first(make_manager):
    m = make_manager()
    hook_entered, hook_go = threading.Event(), threading.Event()

    def blocking_hook():
        hook_entered.set()
        hook_go.wait(5)  # keeps the scheduler thread busy

    def job_a():
        sidejob.on_cancel(blocking_hook)
        sidejob.sleep(60)

    def job_b():
        sidejob.sleep(60)

    a = m.parallel_job(job_a, job_deadline=0.05, cancel_on_deadline=True, required=False)
    assert hook_entered.wait(5)
    b = m.parallel_job(job_b, job_deadline=0.05, cancel_on_deadline=True, required=False)
    wait_until(lambda: m.get_run(b).overdue)  # a query sets the overdue flag first
    assert m.get_run(b).cancel_requested is False
    hook_go.set()
    assert m.wait(b, timeout=5).status is JobStatus.CANCELLED
    assert m.wait(a, timeout=5).status is JobStatus.CANCELLED
