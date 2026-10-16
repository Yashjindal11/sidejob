"""Analytics-flavoured sidejob examples (pandas, Parquet, a database, Spark).

    pip install pandas pyarrow          # for the pandas examples
    python examples/analytics_examples.py

To stay runnable anywhere, the "Redshift" upload below writes to a local SQLite
file. The real Redshift version is shown in ``upload_to_redshift_real`` and in
the README; swap it in unchanged.
"""

from __future__ import annotations

import logging
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

import sidejob

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")
WORKDIR = Path(tempfile.mkdtemp(prefix="sidejob_analytics_"))
DB_PATH = WORKDIR / "warehouse.db"


# ------------------------------------------------------------------ background tasks
def upload_to_warehouse(df: pd.DataFrame, table: str, run_date: str) -> int:
    """Idempotent load: replace this run's partition inside one transaction.

    The connection is opened *inside* the job: DB connections are generally not
    safe to share between threads.
    """
    with sqlite3.connect(DB_PATH) as conn:  # commits on success, rolls back on error
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} (run_date TEXT, region TEXT, revenue REAL)"
        )
        conn.execute(f"DELETE FROM {table} WHERE run_date = ?", (run_date,))
        rows = df.assign(run_date=run_date)[["run_date", "region", "revenue"]]
        conn.executemany(
            f"INSERT INTO {table} VALUES (?, ?, ?)", rows.itertuples(index=False, name=None)
        )
    return len(df)


def upload_to_redshift_real(df: pd.DataFrame, table: str, run_date: str) -> int:  # pragma: no cover
    """The same idempotent pattern against Redshift with ``redshift_connector``.

    Note: a retry can repeat a write that actually committed before the
    connection dropped. Delete+insert of the run's partition in one transaction
    makes a repeat harmless.
    """
    import redshift_connector  # type: ignore[import-not-found]

    conn = redshift_connector.connect(host="...", database="analytics", user="...", password="...")
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table} WHERE run_date = %s", (run_date,))
        cur.write_dataframe(df.assign(run_date=run_date), table)
        conn.commit()
        return len(df)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def export_to_parquet(df: pd.DataFrame, path: Path) -> str:
    tmp = path.with_suffix(".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(path)  # atomic rename: readers never see a half-written file
    return str(path)


def export_csv(df: pd.DataFrame, path: Path) -> str:
    df.to_csv(path, index=False)
    return str(path)


def spark_write(spark_df: Any, path: str) -> int:  # pragma: no cover - needs Spark
    """Spark DataFrames are lazy: the action inside the job does the real work."""
    spark_df.write.mode("overwrite").parquet(path)
    return spark_df.count()


# ------------------------------------------------------------------ main workflow
def main() -> None:
    sales = pd.DataFrame(
        {
            "region": ["north", "south", "east", "west"] * 25_000,
            "revenue": range(100_000),
        }
    )

    manager = sidejob.initialize(
        max_workers=4,
        log_level=logging.INFO,
        max_attempts=3,
        retry_backoff=2,
        # Database drivers raise their own exception types; list the transient ones.
        retry_on=(ConnectionError, TimeoutError, sqlite3.OperationalError),
        name="analytics",
    )

    with manager.workflow("daily-sales") as run:
        # Pass a copy if the main thread will keep mutating the DataFrame.
        snapshot = sales.copy()
        run.parallel_job(upload_to_warehouse, snapshot, "sales", "2026-10-16")
        run.parallel_job(export_to_parquet, snapshot, WORKDIR / "sales.parquet")
        run.parallel_job(export_csv, snapshot, WORKDIR / "sales.csv", required=False)

        # Independent pandas analysis continues on the main thread.
        by_region = sales.groupby("region")["revenue"].agg(["sum", "mean"])
        print(by_region)

        run.wait_all(raise_on_failure=True)  # MANDATORY before declaring success

    (WORKDIR / "_SUCCESS").write_text("ok")
    print(f"\nAll required outputs verified; success marker in {WORKDIR}")
    print(pd.DataFrame([r.to_dict() for r in manager.get_all_runs()]))
    manager.shutdown()


if __name__ == "__main__":
    main()
