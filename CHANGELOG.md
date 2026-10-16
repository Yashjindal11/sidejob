# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
uses [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-10-16

### Added

- `sidejob.initialize()` returning a `JobManager` with one bounded thread pool,
  a thread-safe job registry, retry defaults and a logger.
- `parallel_job()`, `get_run()`, `get_status()`, `get_result()`,
  `get_all_runs()`, `get_failed_runs()`, `get_summary()`, `wait()`,
  `wait_all()`, `cancel()` and `shutdown()`.
- Managed workflows (`manager.workflow()`) with a mandatory `wait_all()`
  barrier that checks only the workflow's own jobs.
- `RetryPolicy`: bounded retries with exponential backoff, jitter and
  configurable retryable / never-retry exception types.
- Structured per-job attempt and event history, independent of log levels.
- Job deadlines, detected by a scheduler as soon as they pass, with optional
  `cancel_on_deadline`.
- Cooperative cancellation for running jobs: `sidejob.check_cancelled()`,
  `sidejob.sleep()`, `sidejob.cancel_requested()` and `sidejob.on_cancel()`
  hooks (for example `conn.cancel` or Spark `cancelJobGroup`).
- `sidejob.current_job()` returning a `JobContext` with `attempt`, `is_retry`
  and a stable `idempotency_key`, so retries can avoid repeating side effects.
- Retries wait on a scheduler thread instead of holding a worker during the
  backoff.
- Memory retention controls: job inputs are released on completion, retried
  attempt tracebacks release their frames, `history_limit=` evicts old
  COMPLETED jobs, and `clear_history()` frees memory on demand.

[0.1.0]: https://github.com/Yashjindal11/sidejob/releases/tag/v0.1.0
