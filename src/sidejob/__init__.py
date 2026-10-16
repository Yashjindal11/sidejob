"""sidejob: simple, observable, fail-loud background jobs for data workflows.

Quick start::

    import sidejob

    manager = sidejob.initialize(max_workers=4)
    with manager.workflow() as run:
        run.parallel_job(upload_to_redshift, df, "analytics_table")
        summary = analyze(df)
        run.wait_all()          # MANDATORY: raises if a required job failed
    write_success_marker()      # reached only on full success
"""

from ._status import JobStatus
from .context import (
    JobContext,
    cancel_requested,
    check_cancelled,
    current_job,
    on_cancel,
    sleep,
)
from .errors import (
    JobCancelledError,
    JobsFailedError,
    ManagerShutdownError,
    SidejobError,
    TransientError,
    UnknownJobError,
    WaitAllNotCalledError,
    WaitTimeoutError,
    WorkflowClosedError,
)
from .manager import Job, JobManager, initialize
from .records import AttemptRecord, JobEvent, JobRun, WaitReport
from .retry import DEFAULT_NEVER_RETRY, DEFAULT_RETRYABLE, RetryPolicy
from .workflow import Workflow

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_NEVER_RETRY",
    "DEFAULT_RETRYABLE",
    "AttemptRecord",
    "Job",
    "JobCancelledError",
    "JobContext",
    "JobEvent",
    "JobManager",
    "JobRun",
    "JobStatus",
    "JobsFailedError",
    "ManagerShutdownError",
    "RetryPolicy",
    "SidejobError",
    "TransientError",
    "UnknownJobError",
    "WaitAllNotCalledError",
    "WaitReport",
    "WaitTimeoutError",
    "Workflow",
    "WorkflowClosedError",
    "__version__",
    "cancel_requested",
    "check_cancelled",
    "current_job",
    "initialize",
    "on_cancel",
    "sleep",
]
