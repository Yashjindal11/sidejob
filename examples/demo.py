"""Runnable sidejob demo (standard library only).

    python examples/demo.py

Shows: parallel jobs, a retry that succeeds, an optional job failing without
breaking the workflow, a required job failing and *blocking* the success
marker, cooperative cancellation and an immediate interrupt of running jobs,
and the structured history you can inspect afterwards.
"""

from __future__ import annotations

import logging
import random
import tempfile
import time
from pathlib import Path

import sidejob

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")

MARKER = Path(tempfile.gettempdir()) / "sidejob_demo_SUCCESS"


# ---------------------------------------------------------------- fake workload
class FlakyWarehouse:
    """Simulates a database that drops the first connection attempt."""

    def __init__(self, failures_before_success: int) -> None:
        self.remaining_failures = failures_before_success

    def upload(self, rows: list, table: str) -> int:
        time.sleep(0.3)  # pretend network I/O
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise ConnectionError(f"connection to warehouse reset while loading {table}")
        return len(rows)


def export_to_file(rows: list, path: str) -> str:
    time.sleep(0.2)
    Path(path).write_text("\n".join(map(str, rows)))
    return path


def send_notification(message: str) -> None:
    raise ConnectionError("SMTP server unavailable")  # optional: must not fail the workflow


def long_export(n_chunks: int) -> int:
    """Checks for cancellation between chunks, so it can be stopped while running."""
    for _ in range(n_chunks):
        sidejob.check_cancelled()
        sidejob.sleep(0.1)  # stands in for writing one chunk; wakes up on cancel
    return n_chunks


def stubborn_report(rows: list) -> float:
    """Never checks for cancellation and swallows ordinary errors."""
    total = 0.0
    while True:
        try:
            total += sum(rows) / len(rows)
        except Exception:
            pass


def analyze(rows: list) -> dict:
    return {"n": len(rows), "mean": sum(rows) / len(rows)}


def write_success_marker() -> None:
    MARKER.write_text("ok")
    print(f"\n>>> success marker written: {MARKER}\n")


# ---------------------------------------------------------------- demo
def main() -> None:
    MARKER.unlink(missing_ok=True)
    rows = [random.randint(0, 100) for _ in range(1_000)]
    out_file = str(Path(tempfile.gettempdir()) / "sidejob_demo_export.txt")

    manager = sidejob.initialize(
        max_workers=4,
        log_level=logging.INFO,
        max_attempts=3,  # 1 attempt + up to 2 retries
        retry_backoff=0.5,  # 0.5s, then 1.0s
        name="demo",
    )

    print("\n=== Run 1: transient failure is retried, optional failure is tolerated ===")
    warehouse = FlakyWarehouse(failures_before_success=1)
    with manager.workflow("run-1") as run:
        run.parallel_job(warehouse.upload, rows, "analytics_table", required=True)
        run.parallel_job(export_to_file, rows, out_file, required=True)
        run.parallel_job(send_notification, "Analysis started", required=False, retries=0)

        summary = analyze(rows)  # main-thread work continues meanwhile
        print(f"main-thread analysis: {summary}")

        run.wait_all(raise_on_failure=True)  # MANDATORY barrier
    write_success_marker()

    print("=== Run 2: a required upload fails permanently -> no success marker ===")
    MARKER.unlink(missing_ok=True)
    broken = FlakyWarehouse(failures_before_success=99)
    try:
        with manager.workflow("run-2") as run:
            upload = run.parallel_job(broken.upload, rows, "analytics_table", required=True)
            analyze(rows)
            run.wait_all(raise_on_failure=True)
        write_success_marker()  # never reached
    except sidejob.JobsFailedError as exc:
        print(f"\n>>> workflow failed as expected:\n{exc}\n")
    assert not MARKER.exists(), "success marker must not exist after a failed run"

    print("=== Run 3: a long export is cancelled cooperatively while RUNNING ===")
    export = manager.parallel_job(
        long_export, 1_000, required=False, job_deadline=0.5, cancel_on_deadline=True
    )
    final = manager.wait(export, timeout=10)
    print(
        f"\n>>> long export ended as {final.status.value} after {final.duration:.2f}s "
        f"(overdue={final.overdue})\n"
    )

    print("=== Run 4: interrupt a job that never checks for cancellation ===")
    report = manager.parallel_job(stubborn_report, rows, required=False)
    sidejob.sleep(0.2)  # it is busy now
    status = manager.job(report.id).interrupt()
    print(f"\n>>> manager.job({report.id!r}).interrupt() -> {status.value}\n")

    print("=== Inspecting history ===")
    failed = manager.get_run(upload)
    for attempt in failed.attempts:
        print(
            f"  attempt {attempt.number}: {attempt.error_type}: {attempt.error_message} "
            f"-> decision={attempt.retry_decision} delay={attempt.retry_delay}"
        )
    print("\nAll runs:")
    for r in manager.get_all_runs():
        print(
            f"  {r.job_id}  {r.workflow_id}  {r.name:<22} {r.status.value:<9} attempts={r.attempt}"
        )
    print("\nSummary:", manager.get_summary())

    manager.shutdown(raise_on_failure=False)  # failures already handled above


if __name__ == "__main__":
    main()
