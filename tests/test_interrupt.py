"""manager.interrupt(): stopping a RUNNING job from outside."""

from __future__ import annotations

import random
import threading
import time

import pytest

import sidejob
from conftest import wait_until
from sidejob import JobStatus

pytestmark = pytest.mark.skipif(
    not sidejob.manager.INTERRUPT_SUPPORTED, reason="interrupt() requires CPython"
)


def busy_loop(stop_after: float = 30.0, entered: threading.Event | None = None) -> str:
    """Pure-Python work that never checks for cancellation."""
    if entered is not None:
        entered.set()
    end = time.monotonic() + stop_after
    n = 0
    while time.monotonic() < end:
        n += 1
    return "finished"


def test_interrupt_stops_non_cooperative_running_job(make_manager, caplog):
    m = make_manager(max_attempts=3)  # retries enabled, but an interrupt is never retried
    entered = threading.Event()
    job = m.parallel_job(busy_loop, entered=entered, required=False)
    assert entered.wait(5)  # the function itself is running
    t0 = time.monotonic()
    status = m.interrupt(job)
    assert status is JobStatus.CANCELLED
    assert time.monotonic() - t0 < 2
    run = m.get_run(job)
    assert run.attempt == 1
    assert run.attempts[0].error_type == "JobInterrupted"
    assert run.attempts[0].retry_decision == "cancelled"
    assert run.cancel_requested is True
    assert [e.kind for e in run.events].count("interrupted") == 1
    assert any("INTERRUPT" in r.getMessage() for r in caplog.records)
    with pytest.raises(sidejob.JobCancelledError):
        job.result()


def test_interrupt_is_not_swallowed_by_except_exception(make_manager):
    m = make_manager()
    swallowed = []
    entered = threading.Event()

    def stubborn():
        entered.set()
        while True:
            try:
                busy_loop(0.01)
            except Exception as exc:  # typical "log and keep going" loop
                swallowed.append(exc)

    job = m.parallel_job(stubborn, required=False)
    assert entered.wait(5)
    assert m.interrupt(job) is JobStatus.CANCELLED
    assert swallowed == []


def test_interrupt_runs_finally_and_with_cleanup(make_manager):
    m = make_manager()
    cleaned, entered = threading.Event(), threading.Event()

    class Resource:
        def __enter__(self):
            entered.set()
            return self

        def __exit__(self, *exc):
            cleaned.set()

    def uses_resource():
        with Resource():
            busy_loop()

    job = m.parallel_job(uses_resource, required=False)
    assert entered.wait(5)
    assert m.interrupt(job) is JobStatus.CANCELLED
    assert cleaned.is_set()


def test_interrupt_via_job_id_lookup(make_manager):
    m = make_manager()
    job = m.parallel_job(busy_loop, required=False)
    wait_until(lambda: m.get_status(job) is JobStatus.RUNNING)  # either window must work
    handle = m.job(job.id)
    assert handle == job
    assert handle.interrupt() is JobStatus.CANCELLED
    with pytest.raises(sidejob.UnknownJobError):
        m.job("no-such-job")


def test_interrupt_queued_job_cancels_it(make_manager, gate):
    m = make_manager(max_workers=1)
    g = gate()
    m.parallel_job(g, required=False)
    assert g.started.wait(5)
    calls = []
    queued = m.parallel_job(lambda: calls.append(1), required=False)
    assert m.interrupt(queued, timeout=0) is JobStatus.CANCELLED
    g.release.set()
    m.wait_all(raise_on_failure=False, timeout=5)
    assert calls == []


def test_interrupt_finished_job_is_noop(make_manager):
    m = make_manager()
    job = m.parallel_job(lambda: 7)
    assert job.result(timeout=5) == 7
    assert m.interrupt(job) is JobStatus.COMPLETED
    assert m.get_run(job).cancel_requested is False


def test_interrupt_blocked_call_then_on_cancel_hook(make_manager, caplog):
    """A blocking C-level wait can't be interrupted directly; an on_cancel hook unblocks it."""
    m = make_manager()
    started = threading.Event()

    def blocked_without_hook():
        started.set()
        threading.Event().wait(0.5)  # blocks in C; interrupt lands when it returns
        busy_loop()

    job = m.parallel_job(blocked_without_hook, required=False)
    assert started.wait(5)
    assert m.interrupt(job, timeout=0.05) is JobStatus.RUNNING
    assert any("blocked in a call" in r.getMessage() for r in caplog.records)
    assert m.wait(job, timeout=5).status is JobStatus.CANCELLED  # stops once the call returns

    released = threading.Event()
    started2 = threading.Event()

    def blocked_with_hook():
        sidejob.on_cancel(released.set)  # e.g. conn.cancel
        started2.set()
        released.wait(30)
        busy_loop()

    job2 = m.parallel_job(blocked_with_hook, required=False)
    assert started2.wait(5)
    t0 = time.monotonic()
    assert m.interrupt(job2) is JobStatus.CANCELLED
    assert time.monotonic() - t0 < 2


def test_required_interrupted_job_fails_the_barrier(make_manager):
    m = make_manager()
    marker = []
    with pytest.raises(sidejob.JobsFailedError):
        with m.workflow() as run:
            job = run.parallel_job(busy_loop)
            wait_until(lambda: m.get_status(job) is JobStatus.RUNNING)  # either window
            m.interrupt(job)
            run.wait_all()
        marker.append(1)
    assert marker == []


def test_job_cannot_interrupt_itself(make_manager):
    m = make_manager()

    def selfish():
        m.interrupt(sidejob.current_job().job_id)

    run = m.wait(m.parallel_job(selfish, required=False), timeout=5)
    assert run.status is JobStatus.FAILED
    assert "cannot interrupt itself" in run.error_message


def test_interrupt_never_leaks_into_sidejob_or_the_next_job(make_manager):
    """Fire interrupts at random moments, including as jobs finish.

    Every interrupted job must end CANCELLED or COMPLETED (if it finished
    first), and the job that runs next on the same worker must never see a
    stray JobInterrupted.
    """
    m = make_manager(max_workers=1)
    rng = random.Random(1234)
    outcomes = {JobStatus.CANCELLED: 0, JobStatus.COMPLETED: 0}
    for _ in range(200):
        target = m.parallel_job(busy_loop, rng.uniform(0, 0.03), required=False)
        follow_up = m.parallel_job(busy_loop, 0.001)
        time.sleep(rng.uniform(0, 0.03))
        status = m.interrupt(target, timeout=5)
        assert status in outcomes, status
        outcomes[status] += 1
        assert follow_up.result(timeout=5) == "finished"
    # Both races were exercised: interrupted mid-run, and finished just before.
    assert outcomes[JobStatus.CANCELLED] > 20
    assert outcomes[JobStatus.COMPLETED] > 20
    assert m.get_summary()["required_failed_job_ids"] == []
    # The pool's worker is still healthy.
    assert m.get_result(m.parallel_job(lambda: "alive"), timeout=5) == "alive"


def test_interrupt_between_running_and_function_call_is_not_lost(make_manager):
    """Regression: an interrupt landing after RUNNING but before the call must still stop it."""
    import logging

    m = make_manager()
    at_start, go = threading.Event(), threading.Event()
    calls = []

    class PauseAtStart(logging.Handler):
        def handle(self, record):
            if record.getMessage().endswith(": started"):
                at_start.set()
                go.wait(5)
            return True

    hook = PauseAtStart()
    m.logger.addHandler(hook)
    try:
        job = m.parallel_job(lambda: calls.append(1) or busy_loop(), required=False)
        assert at_start.wait(5)  # RUNNING, function not called yet
        assert m.get_status(job) is JobStatus.RUNNING
        assert m.interrupt(job, timeout=0) is JobStatus.RUNNING
        go.set()
        run = m.wait(job, timeout=5)
    finally:
        go.set()
        m.logger.removeHandler(hook)
    assert run.status is JobStatus.CANCELLED
    assert run.attempts[0].error_type == "JobInterrupted"
    assert calls == []  # the function never ran
