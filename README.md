# sidejob

[![PyPI](https://img.shields.io/pypi/v/sidejob.svg)](https://pypi.org/project/sidejob/)
[![Python versions](https://img.shields.io/pypi/pyversions/sidejob.svg)](https://pypi.org/project/sidejob/)
[![CI](https://github.com/Yashjindal11/sidejob/actions/workflows/ci.yml/badge.svg)](https://github.com/Yashjindal11/sidejob/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/Yashjindal11/sidejob/blob/main/LICENSE)

**Simple background execution, automatic observability, reliable retries, and explicit failure propagation for Python data workflows.**

`sidejob` lets analysts run functions in the background (uploads, exports,
notifications, Spark writes, …) while the notebook or script keeps working,
and then **refuses to let the workflow look successful unless every required
job really succeeded**.

```python
import logging
import sidejob

manager = sidejob.initialize(max_workers=4, log_level=logging.INFO, max_attempts=3, retry_backoff=2)

with manager.workflow("daily-sales") as run:
    run.parallel_job(upload_to_redshift, df, "analytics_table", required=True)
    run.parallel_job(export_to_parquet, df, "output.parquet", required=True)
    run.parallel_job(send_notification, "Analysis started", required=False)

    summary = analyze_data(df)            # main thread keeps working

    run.wait_all(raise_on_failure=True)   # MANDATORY safety barrier

write_success_marker()                     # reached only if every required job COMPLETED
```

* Single process, standard library only (`concurrent.futures`, `threading`, `logging`). No services, no database, no dashboard.
* Python 3.9+. Fully typed (`py.typed`).
* License: **MIT** (see [LICENSE](https://github.com/Yashjindal11/sidejob/blob/main/LICENSE)).

---

## Contents

- [Installation](#installation)
- [The safety contract](#the-safety-contract)
- [Core concepts](#core-concepts)
- [API reference](#api-reference)
- [Retries](#retries)
- [Logging and observability](#logging-and-observability)
- [Timeouts, deadlines and cancellation](#timeouts-deadlines-and-cancellation)
- [Stopping running jobs (cooperative cancellation)](#stopping-running-jobs-cooperative-cancellation)
- [Memory and history retention](#memory-and-history-retention)
- [Shutdown](#shutdown)
- [Data analytics examples](#data-analytics-examples)
- [Limitations and caveats](#limitations-and-caveats)
- [Development and testing](#development-and-testing)

---

## Installation

```bash
pip install sidejob
```

From a checkout, with the test and lint tools:

```bash
git clone https://github.com/Yashjindal11/sidejob.git && cd sidejob
pip install -e ".[dev]"
```

There are no runtime dependencies.

---

## The safety contract

1. **A job is `COMPLETED` only when its function returns normally.** A worker
   thread finishing is not success.
2. **`wait_all()` is mandatory before a workflow counts as successful.** It
   waits for every selected job (queued, running, or retrying) and then:
   * raises `JobsFailedError` if any **required** job is `FAILED` or `CANCELLED`,
     listing the job IDs, function names, attempts, and the original errors;
   * raises `WaitTimeoutError` if any job is still unfinished when the timeout
     expires. **A timeout is never success**, and it never changes a job's
     status: a still-running job stays `RUNNING`.
3. **Failures stay observable.** Every failure is logged (final failures at
   `ERROR` with a traceback) and kept in the in-memory history. You can always
   find it with `get_status()`, `get_run()`, `get_failed_runs()` and
   `get_summary()`, even when logging is turned off.
4. **The workflow context is a second safety net, not a replacement.** When the
   `with` block exits normally, it waits for and checks its jobs again. If
   `run.wait_all()` was never called (successfully, with
   `raise_on_failure=True`), a strict workflow (the default) raises
   `WaitAllNotCalledError`.
5. **sidejob never writes your success marker.** Put it *after* the `with`
   block (or after a successful `wait_all()`).

> **You can still bypass the safeguards on purpose.** For example: catching and
> ignoring `JobsFailedError`, passing `raise_on_failure=False` and not checking
> `report.ok`, writing the success marker inside the `with` block before
> `wait_all()`, or writing it from another thread or code path. sidejob makes
> failures hard to overlook. It cannot stop code that deliberately ignores them.

---

## Core concepts

| Concept | What it is |
|---|---|
| `JobManager` | Returned by `sidejob.initialize()`. Owns **one bounded `ThreadPoolExecutor`**, a thread-safe job registry, retry defaults and a logger. Managers are independent of each other. |
| `Job` | Lightweight handle returned by `parallel_job()`: `job.id`, `job.status`, `job.result()`, `job.wait()`, `job.cancel()`, `job.snapshot()`. |
| `JobRun` | Immutable snapshot of a job's metadata, attempts and events. |
| `Workflow` | `manager.workflow()` context. `run.parallel_job()` submits jobs that belong to the workflow. `run.wait_all()` checks **only those jobs**. |
| `required` | Defaults to **`True`**. Required jobs decide whether the workflow succeeds. Optional failures are logged and recorded but don't fail it. |

### Statuses

```
QUEUED ──► RUNNING ──► COMPLETED                 (the function returned)
  │           │
  │           ├──► FAILED                        (non-retryable error, or attempts exhausted)
  │           │
  │           ├──► RETRYING ──► RUNNING ...      (retryable error; waiting for the next attempt)
  │           │        │
  │           │        ▼
  └───────────┴──► CANCELLED                     (cancelled before starting, while waiting to retry,
                                                  or a running attempt stopped after a cancel request)
```

`COMPLETED`, `FAILED` and `CANCELLED` are terminal. A job that fails and then
succeeds on a retry ends as `COMPLETED`. The earlier failed attempt stays in
`attempts` but is never reported as the final outcome.

---

## API reference

### `sidejob.initialize(...) -> JobManager`

```python
sidejob.initialize(
    max_workers=4,              # bounded pool size (default 4)
    log_level=logging.INFO,     # level of this manager's logger; None = leave unchanged
    retries=None,               # retries AFTER the first attempt  (retries=2 -> 3 attempts)
    retry_backoff=None,         # seconds before the first retry (default 2.0)
    *,
    max_attempts=None,          # TOTAL attempts incl. the first (default 3); exclusive with retries
    backoff_multiplier=None,    # default 2.0
    max_backoff=None,           # default 60.0 seconds
    jitter=None,                # 0..1 fraction, default 0
    retry_on=None,              # retryable exception types
    retry_if=None,              # predicate(exc) -> bool, replaces retry_on
    never_retry=None,           # never retried even if matched
    retry_policy=None,          # full RetryPolicy (not combinable with the options above)
    name=None,                  # logger becomes "sidejob.<name>"
    logger=None,                # use your own logger instead
    log_to_console=None,        # see Logging
    history_limit=None,         # see Memory and history retention
)
```

> **`retries` vs `max_attempts`.** `retries=3` means *3 retries after the first
> attempt*, so **4 attempts in total**. `max_attempts=3` means *3 attempts in
> total*. Passing both raises `ValueError`. The default is `max_attempts=3`
> with a 2-second initial backoff.

### Submitting jobs

```python
job = manager.parallel_job(func, *args, **kwargs)       # manager-level job
job = run.parallel_job(func, *args, **kwargs)           # job belonging to a workflow
```

These keyword-only options are consumed by sidejob and **never passed to `func`**:

| Option | Meaning |
|---|---|
| `required=True` | Whether the job's success is needed for the workflow to succeed. |
| `retries=` / `max_attempts=` | Per-job attempt budget. |
| `retry_policy=` | A full `RetryPolicy` for this job. |
| `job_name=` | Display name (default: the function's `__qualname__`). |
| `job_deadline=` | Seconds after the first attempt starts after which the job is reported **overdue**. The warning is logged as soon as the deadline passes. |
| `cancel_on_deadline=False` | Also request [cooperative cancellation](#stopping-running-jobs-cooperative-cancellation) when `job_deadline` passes. |
| `func_kwargs={...}` | Keyword arguments passed verbatim to `func`. Use this if `func` has a parameter with one of the names above, e.g. `func_kwargs={"retries": 5}`. |

Submitting to a shut-down manager raises `ManagerShutdownError`. Submitting to
a workflow that hasn't been entered, or has already exited, raises
`WorkflowClosedError`.

### Querying

| Method | Returns / behavior |
|---|---|
| `get_run(job)` | `JobRun` snapshot. |
| `get_status(job)` | `JobStatus` (non-blocking). |
| `get_result(job, timeout=None)` | Waits, then returns the result. Raises the job's **original exception** if it `FAILED`, `JobCancelledError` if it was cancelled, and `WaitTimeoutError` on timeout (the job keeps running). |
| `wait(job, timeout=None)` | Waits until terminal and returns the `JobRun`. Does not raise on job failure. Raises `WaitTimeoutError` on timeout. |
| `get_all_runs(status=None, *, required=None)` | All snapshots in submission order. `status` can be a `JobStatus`, a string (`"FAILED"`), or an iterable of them. |
| `get_failed_runs(*, include_cancelled=False)` | Snapshots of `FAILED` (and optionally `CANCELLED`) jobs. |
| `get_summary()` | Dict: `total`, `by_status`, `unfinished_job_ids`, `failed_job_ids`, `cancelled_job_ids`, `required_failed_job_ids`, `optional_failed_job_ids`, `overdue_job_ids`, `cancel_requested_job_ids`, `evicted`, `ok`, `is_shutdown`. |
| `clear_history(*, include_failed=False)` | Forget finished jobs to free memory. See [retention](#memory-and-history-retention). |

`job` may be a `Job` handle or a job ID string. Unknown IDs (or handles from
another manager) raise `UnknownJobError`, which is also a `LookupError`.

`JobRun` fields: `job_id`, `name`, `status`, `required`, `workflow_id`,
`submitted_at`, `started_at`, `finished_at` (timezone-aware UTC), `duration`
(seconds from first start to terminal state, including backoff waits),
`attempt`, `max_attempts`, `result`, `exception`, `error_type`,
`error_message`, `attempts` (tuple of `AttemptRecord`), `events` (tuple of
`JobEvent`), `deadline`, `overdue`, `cancel_requested`. Use `run.describe()` for a one-line summary
and `run.to_dict()` to build a table:

```python
import pandas as pd
pd.DataFrame([r.to_dict() for r in manager.get_all_runs()])
```

`AttemptRecord` fields: `number`, `started_at`, `finished_at`, `duration`,
`succeeded`, `error_type`, `error_message`, `exception`, `traceback`,
`retry_decision` (`"retry"`, `"not_retryable"`, `"exhausted"`, `"cancelled"`,
or `None` on success), `retry_delay`.

### Waiting: `wait_all`

```python
report = manager.wait_all(jobs=None, timeout=None, raise_on_failure=True)
report = run.wait_all(timeout=None, raise_on_failure=True)
```

* `manager.wait_all(jobs=None)` checks **every job of the manager**, including
  jobs submitted while it is waiting. Pass `jobs=[...]` (handles or IDs) to
  restrict it.
* `run.wait_all()` checks only the workflow's jobs.
* It returns a `WaitReport` with `runs`, `completed`, `failed`, `cancelled`,
  `required_failures`, `optional_failures` and `ok`.
* On timeout it **always** raises `WaitTimeoutError` (also a builtin
  `TimeoutError`), with `.pending` and `.failures`, regardless of
  `raise_on_failure`.
* It raises `JobsFailedError` (`.failures`, `.job_ids`, `.exceptions`,
  `.report`) when a required job failed or was cancelled. The first original
  exception is chained as `__cause__`.
* **`raise_on_failure=False` explicitly disables exception-based enforcement.**
  Failures are logged and returned in the report, and *you* must check
  `report.ok`. In a workflow, such a call **does not satisfy** the mandatory
  barrier.
* A job calling `wait_all()` / `wait()` on itself raises `SidejobError`
  instead of deadlocking.

### Workflows

```python
with manager.workflow(name=None, *, strict=True, cancel_on_error=True, exit_timeout=None) as run:
    ...
```

**Exit policy when the body succeeds:**

1. Wait for all workflow jobs (up to `exit_timeout`; `None` means no limit).
   Raise `WaitTimeoutError` if they don't finish.
2. Raise `JobsFailedError` if a required job failed or was cancelled.
3. If some jobs were not covered by a successful explicit
   `run.wait_all(raise_on_failure=True)`, raise `WaitAllNotCalledError` when
   `strict=True`, or log a warning when `strict=False`. This also catches jobs
   submitted *after* the last `wait_all()`.

**Exit policy when the body raises:**

* The **original exception always propagates unchanged**. Cleanup problems are
  logged, never raised in its place.
* With `cancel_on_error=True` (default), jobs that have not started (`QUEUED`,
  or `RETRYING` during a backoff) are cancelled, and cooperative cancellation is
  requested for running jobs.
* Running jobs are waited for (up to `exit_timeout`). Exceptions: if the error
  is a `WaitTimeoutError` from this workflow's own barrier, or a
  `KeyboardInterrupt`/`SystemExit`, it does not wait again. Those jobs keep
  running and stay tracked.
* Background failures are logged at `ERROR`. On Python 3.11+ a note listing
  them is added to the exception (`exc.__notes__`).

A workflow can be entered only once. Its jobs are also part of the manager's
history.

### Exceptions

```
SidejobError
├── UnknownJobError (LookupError)
├── ManagerShutdownError (RuntimeError)
├── WorkflowClosedError (RuntimeError)
├── JobCancelledError
├── WaitTimeoutError (TimeoutError)
├── JobsFailedError
├── WaitAllNotCalledError
└── TransientError        # raise inside a job to request a retry
```

---

## Retries

```python
from sidejob import RetryPolicy

RetryPolicy(
    max_attempts=3,       # total attempts including the first
    backoff=2.0,          # delay before the 1st retry
    multiplier=2.0,       # 2s, 4s, 8s, ...
    max_backoff=60.0,     # cap per delay
    jitter=0.0,           # 0.1 => each delay randomly scaled by ±10%
    retry_on=DEFAULT_RETRYABLE,      # (ConnectionError, TimeoutError, socket.timeout, TransientError)
    retry_if=None,                   # predicate; replaces retry_on when set
    never_retry=DEFAULT_NEVER_RETRY, # TypeError, ValueError, AttributeError, NameError, LookupError,
)                                    # ArithmeticError, AssertionError, ImportError, NotImplementedError, MemoryError
```

A failed attempt is retried only if **all** of these hold:

1. The error is an `Exception`, not `KeyboardInterrupt` or `SystemExit`.
2. It is not an instance of `never_retry`.
3. It matches `retry_if(exc)` if that is set, otherwise `retry_on`.
4. The attempt budget isn't used up.
5. The manager is not shutting down with `cancel_pending=True`.

Every attempt records its exception, traceback, timestamps, the retry
decision and the delay. Programming errors (`TypeError`, `KeyError`, …) fail
immediately. Errors that are neither retryable nor listed (for example
`RuntimeError`) are **not** retried by default.

**Database drivers raise their own exception types.** Tell sidejob which ones
are transient:

```python
import psycopg2, redshift_connector

manager = sidejob.initialize(
    retry_on=(ConnectionError, TimeoutError,
              psycopg2.OperationalError, redshift_connector.InterfaceError),
)
# or per job:
run.parallel_job(load, df, retry_policy=RetryPolicy(max_attempts=5, backoff=5, jitter=0.2,
                                                    retry_if=lambda e: "deadlock" in str(e)))
# or raise sidejob.TransientError("...") from inside the job
```

**Waiting for a retry does not use a worker.** The job's worker is released
when an attempt fails. A small per-manager scheduler thread (started on
demand, exiting when idle) puts the job back on the pool's queue when the
delay is up. Other jobs use the freed worker in the meantime. A `RETRYING`
job can be cancelled at any time before its next attempt starts.

### ⚠️ Retries can repeat side effects

A retry runs your function **again**. A database write may have **committed
before** the connection error or timeout reached your code. A retry then
writes the data a second time. **sidejob does not and cannot guarantee
exactly-once execution.** Make retried operations idempotent:

* Load into a staging table, then `DELETE` the target partition and `INSERT`
  it, or `MERGE`, **in one transaction**.
* Key writes by a run ID or date partition so repeating is harmless.
* Write files to a temporary path and atomically rename them.
* Use `retries=0` for operations that must not repeat, such as emails or
  payments.

**Let a retry check what the previous attempt did.** Inside a job,
`sidejob.current_job()` returns a `JobContext` with `attempt`, `is_retry`
and `idempotency_key` (the same for every attempt of the job):

```python
def upload_to_redshift(df, table, run_date):
    ctx = sidejob.current_job()          # None when called outside sidejob
    conn = connect()
    try:
        cur = conn.cursor()
        if ctx is not None and ctx.is_retry:
            # The previous attempt may have committed before the error reached us.
            cur.execute("SELECT 1 FROM etl_loads WHERE load_key = %s", (ctx.idempotency_key,))
            if cur.fetchone():
                return "already loaded"
        cur.execute(f"DELETE FROM {table} WHERE run_date = %s", (run_date,))
        cur.write_dataframe(df.assign(run_date=run_date), table)
        cur.execute("INSERT INTO etl_loads (load_key) VALUES (%s)", (ctx.idempotency_key,))
        conn.commit()                    # data and load marker commit together
        return "loaded"
    finally:
        conn.close()
```

`idempotency_key` is the job ID, which is unique per submission. To skip
work across *process restarts* as well, use a key you control, for example
`f"{table}:{run_date}"`.

---

## Logging and observability

* sidejob uses the standard `logging` module. Each manager logs to
  `sidejob.<name>` (or your own `logger=`). `log_level` sets **that logger's**
  level only. The root logger is never configured.
* `log_to_console=None` (default) attaches **one** stderr handler to the
  `sidejob` logger only if your application has not configured logging (no
  handlers on the root or `sidejob` logger). `True` forces it, and `False`
  never attaches it and removes ours. It is never added twice. If you call
  `logging.basicConfig()` *after* `initialize()`, pass `log_to_console=False`
  to avoid duplicate lines.
* Every job log line includes the job ID, name and attempt, for example:

  ```
  job 0001-3f9c2a1b [upload_to_redshift] attempt 1/3: failed with retryable error ConnectionError: reset; RETRYING in 2.00s
  ```

  Records also carry `extra` fields for structured handlers:
  `sidejob_job_id`, `sidejob_job_name`, `sidejob_attempt`,
  `sidejob_max_attempts`, `sidejob_workflow_id`.

| Event | Level |
|---|---|
| submitted, attempt started, completed, wait_all verified | INFO |
| retryable failure + retry delay, optional job failed, cancellation, overdue, wait timeout of a single job | WARNING |
| final failure (with traceback), required failures, wait_all timeout | ERROR |

The in-memory history (`events`, `attempts`) is recorded **independently of
log levels**.

History retention is configurable; see
[Memory and history retention](#memory-and-history-retention).

---

## Timeouts, deadlines and cancellation

| Situation | Behavior |
|---|---|
| `wait`/`get_result`/`wait_all` timeout | Raises `WaitTimeoutError`. The job's status is **unchanged**, the thread keeps running, and a `wait_timeout` event is recorded. Never success. |
| `cancel()` on a `QUEUED` job | Returns `True`. The job becomes `CANCELLED` and never runs. |
| `cancel()` during a retry backoff (`RETRYING`) | Returns `True`. The job becomes `CANCELLED` and no further attempts run. |
| `cancel()` on a `RUNNING` job | Returns `False` and **requests cooperative cancellation** (see below). It logs a warning, records a `cancel_requested` event, sets `JobRun.cancel_requested`, and runs the job's `on_cancel` hooks. |
| `cancel()` on a terminal job | Returns `False` (no-op). |
| `job_deadline=` exceeded | Detected by the scheduler as soon as it happens: `overdue=True`, one WARNING, an `overdue` event. The job is not stopped unless `cancel_on_deadline=True`. |

Cancellation and starting a job are serialized by one lock, so a job is
either cancelled or started, never both.

---

## Stopping running jobs (cooperative cancellation)

Python has no safe way to kill a thread: killing one could leave locks,
connections or files in a broken state. So sidejob stops running work
**cooperatively**. `cancel()`, `cancel_on_deadline`, a failing workflow body,
and `shutdown(cancel_pending=True)` all send a cancellation request. Your job
reacts to it in one of three ways:

```python
import sidejob

# 1. Check between units of work
def export_partitions(df, partitions):
    for p in partitions:
        sidejob.check_cancelled()          # raises JobCancelledError once cancel is requested
        write_partition(df, p)

# 2. Wait without blocking cancellation
def wait_for_cluster(client, run_id):
    while not client.is_done(run_id):
        sidejob.sleep(10)                  # returns early and raises on cancel

# 3. Abort a blocking call from outside
def upload(df, table):
    conn = psycopg2.connect(...)
    sidejob.on_cancel(conn.cancel)         # cancel() aborts the running SQL statement
    ...

def spark_export(sdf, path):
    sc = spark.sparkContext
    group = sidejob.current_job().job_id
    sc.setJobGroup(group, "sidejob export", interruptOnCancel=True)
    sidejob.on_cancel(lambda: sc.cancelJobGroup(group))   # cancels the Spark stages
    sdf.write.mode("overwrite").parquet(path)
```

Rules:

* If the attempt **raises anything** after cancellation was requested
  (including the driver's own "query cancelled" error), the job becomes
  `CANCELLED` and is **not retried**. The error is kept in `attempts`.
* If the job ignores the request and returns normally, it is `COMPLETED`,
  because it really did finish. The log notes that cancellation had been
  requested.
* `on_cancel` hooks run on the thread that requested cancellation. Each hook
  applies to the current attempt only. A hook raising is logged, never
  propagated.
* All helpers are **no-ops outside a job** (`sidejob.sleep` becomes
  `time.sleep`), so the same function also works when called directly.
* Code that never checks still can't be stopped. It runs to completion and
  keeps its worker until then.

---

## Memory and history retention

By default, history (status, attempts, events, results) is kept for the
lifetime of the manager so failures can't vanish. To keep memory low:

* **Inputs are released automatically.** When a job finishes, sidejob drops
  its references to the function and its arguments, such as the DataFrame you
  passed.
* **Tracebacks of retried attempts don't keep locals alive.** Their text is
  kept, but the stack frames, and with them large local variables, are
  released. Only the final exception of a `FAILED` job keeps its frames, for
  debugging.
* **`history_limit=N`** (in `initialize`) keeps at most `N` jobs by evicting
  the oldest `COMPLETED` ones. `FAILED` jobs, `CANCELLED` jobs, unfinished
  jobs, and jobs of an open workflow are **never evicted automatically**, so
  an unhandled failure can't disappear before `wait_all()` sees it.
* **`manager.clear_history(include_failed=False)`** forgets finished jobs on
  demand, for example after each scheduled run in a long-lived process.
* **`Job` handles keep working.** A handle you hold keeps its job's result
  and history reachable even after eviction; dropping the handle frees it.
  Looking up an evicted job by ID string raises `UnknownJobError`.

Results stay in memory until the job is evicted or cleared. Prefer returning
small values (paths, row counts) over large DataFrames.

---

## Shutdown

```python
manager.shutdown(wait=True, *, cancel_pending=False, raise_on_failure=True, timeout=None)
```

| Situation | Behavior |
|---|---|
| Submissions after shutdown | Raise `ManagerShutdownError`. |
| Queued jobs | Run normally, unless `cancel_pending=True` cancels them. |
| Running jobs | Run to completion. `cancel_pending=True` sends them a [cooperative cancellation](#stopping-running-jobs-cooperative-cancellation) request. `wait=True` waits for them. |
| Jobs waiting for a retry | Continue retrying, unless `cancel_pending=True` cancels them (including retries that would be scheduled later). |
| `wait=True` | Same as `wait_all()` over all jobs. Returns a `WaitReport` and raises on timeout or required failures. |
| `wait=False` | Returns `None` immediately. Queued jobs and scheduled retries still run. The thread pool is released once the last job finishes. |
| Repeated calls | Safe. Each `wait=True` call re-verifies outcomes. |
| History | Remains queryable. |

`JobManager` is also a context manager. A normal exit calls
`shutdown(wait=True)`. After an exception it cancels pending jobs, waits
without raising, and re-raises your exception.

> Do not call `shutdown()` from inside a job. At interpreter exit, Python
> waits for running pool threads, as with any `ThreadPoolExecutor`.

---

## Data analytics examples

The runnable files are [`examples/demo.py`](https://github.com/Yashjindal11/sidejob/blob/main/examples/demo.py) (standard
library only) and
[`examples/analytics_examples.py`](https://github.com/Yashjindal11/sidejob/blob/main/examples/analytics_examples.py) (pandas,
Parquet, a SQLite stand-in for Redshift, and a Spark helper).

### Upload a DataFrame to Redshift + export Parquet + keep analysing

```python
import logging, os, pandas as pd, redshift_connector, sidejob

def upload_to_redshift(df: pd.DataFrame, table: str, run_date: str) -> int:
    conn = redshift_connector.connect(...)          # open the connection INSIDE the job
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table} WHERE run_date = %s", (run_date,))   # idempotent
        cur.write_dataframe(df.assign(run_date=run_date), table)
        conn.commit()
        return len(df)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def export_to_parquet(df: pd.DataFrame, path: str) -> str:
    tmp = path + ".tmp"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)                            # atomic
    return path

manager = sidejob.initialize(
    max_workers=4, log_level=logging.INFO, max_attempts=3, retry_backoff=5, jitter=0.2,
    retry_on=(ConnectionError, TimeoutError, redshift_connector.InterfaceError),
)

with manager.workflow("daily-sales") as run:
    frozen = df.copy()                               # don't mutate what background jobs read
    run.parallel_job(upload_to_redshift, frozen, "analytics.sales", "2026-10-16")
    run.parallel_job(export_to_parquet, frozen, "/dbfs/exports/sales.parquet")
    run.parallel_job(send_slack_message, "sales export started", required=False, retries=0)

    by_region = df.groupby("region")["revenue"].sum()   # independent pandas work

    run.wait_all(raise_on_failure=True)
```

### Several database and file operations

```python
with manager.workflow("refresh") as run:
    for table, frame in {"orders": orders, "customers": customers, "items": items}.items():
        run.parallel_job(upload_to_redshift, frame, table, run_date, job_name=f"load:{table}")
        run.parallel_job(export_to_parquet, frame, f"/exports/{table}.parquet",
                         job_name=f"export:{table}")
    run.wait_all(timeout=45 * 60)        # raises if anything is unfinished after 45 minutes
```

### Spark DataFrames

```python
def write_spark(sdf, path: str) -> int:
    sdf.write.mode("overwrite").parquet(path)   # the ACTION runs here, in the background thread
    return spark.read.parquet(path).count()

with manager.workflow("spark-export") as run:
    run.parallel_job(write_spark, sales_sdf.filter("region = 'EU'"), "s3://bucket/eu/")
    run.parallel_job(write_spark, sales_sdf.filter("region = 'US'"), "s3://bucket/us/")
    local_summary = pandas_df.describe()
    run.wait_all()
```

A Spark DataFrame is a **lazy plan**. Passing it to a job costs nothing, and
the work happens when the job triggers an action. Concurrent actions **compete
for the same cluster** (executors, memory, shuffle). Keep `max_workers` small,
and consider Spark fair scheduler pools
(`spark.sparkContext.setLocalProperty("spark.scheduler.pool", ...)` inside the
job, since local properties are per thread). Cache shared upstream
DataFrames deliberately.

### Scheduled job: success marker only after all critical work

```python
def main() -> None:
    manager = sidejob.initialize(max_workers=4, name="nightly")
    try:
        with manager.workflow("nightly") as run:
            run.parallel_job(upload_to_redshift, df, "analytics.daily", today)
            run.parallel_job(export_to_parquet, df, f"/exports/{today}.parquet")
            build_report(df)
            run.wait_all(timeout=3600)          # MANDATORY
        write_success_marker(today)             # only here
    finally:
        manager.shutdown(wait=True, raise_on_failure=False)   # failures already raised above
        print(manager.get_summary())

if __name__ == "__main__":
    main()          # an unhandled JobsFailedError -> non-zero exit code for the scheduler
```

---

## Limitations and caveats

* **Threads suit I/O-bound work** (database calls, network, file I/O, Spark
  driver calls that wait on the cluster). CPU-heavy pure-Python code won't run
  faster because of the GIL. Pandas/NumPy release the GIL only in some
  operations. sidejob v1 deliberately has no multiprocessing or distributed
  execution.
* **Shared mutable objects need care.** Don't mutate a DataFrame in the main
  thread while a job reads it. Pass a `.copy()` or treat it as read-only.
* **Database connections and cursors are generally not thread-safe.** Open a
  connection inside each job, or use a thread-safe pool. Many connections at
  once can hit warehouse concurrency limits, so size `max_workers`
  accordingly.
* **Running threads cannot be killed.** Stopping running work needs the
  job's cooperation (`check_cancelled`, `sleep`, or `on_cancel` hooks; see
  [above](#stopping-running-jobs-cooperative-cancellation)). Code that never
  checks, such as a single long C call without a cancel hook, runs to
  completion. Timeouts bound how long you *wait*, and Python waits for running
  jobs at interpreter exit.
* **No exactly-once guarantee.** Retries can repeat side effects. Use
  idempotent writes and `current_job().is_retry` / `idempotency_key` (see
  [Retries](#retries)).
* **In-memory, single-process only.** History disappears when the process
  exits, and jobs are not persisted or resumed. Use an orchestrator (Airflow,
  Databricks Jobs, …) for durable scheduling.
* Notebook note: a job started in one cell keeps running while you run other
  cells. Call `run.wait_all()` (or `manager.wait_all()`) before treating
  results as final.

---

## Development and testing

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

pytest                  # run the test suite
ruff check src tests    # lint
ruff format src tests   # format
mypy                    # strict type check of src/sidejob
python examples/demo.py # end-to-end demo
```

The tests use events, barriers and condition waits instead of fixed sleeps
wherever possible. They cover status transitions, pool limits, retries,
logging, cancellation, shutdown, thread-safe concurrent submission,
independent managers, workflow exit behavior, and three mandatory regression
scenarios:

* **A:** a failed upload blocks the success marker.
* **B:** a retry succeeds.
* **C:** a job outlives the main analysis.

The suite has been run on CPython 3.9 and 3.12 (macOS).

## License

MIT. See [LICENSE](https://github.com/Yashjindal11/sidejob/blob/main/LICENSE). MIT was chosen as a permissive license that is
common for small Python libraries and lets the library be used in commercial
analytics code without restrictions.
