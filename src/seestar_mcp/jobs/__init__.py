"""Celery Jobs layer for seestar-mcp (broker + result backend: Redis).

Exposes ``app`` so the Celery CLI works as ``celery -A seestar_mcp.jobs:app``.
Importing this package also imports :mod:`seestar_mcp.jobs.tasks`,
:mod:`seestar_mcp.jobs.scope` and :mod:`seestar_mcp.jobs.compute` so every
Job is registered with the app.
"""

from seestar_mcp.jobs import compute as compute
from seestar_mcp.jobs import scope as scope
from seestar_mcp.jobs import tasks as tasks
from seestar_mcp.jobs.celery_app import (
    COMPUTE_QUEUE,
    REDIS_URL,
    RESULT_TTL_SECONDS,
    SCOPE_QUEUE,
    app,
)
from seestar_mcp.jobs.submit import submit_job
from seestar_mcp.jobs.tasks import JobTask, TransientJobError

__all__ = [
    "COMPUTE_QUEUE",
    "REDIS_URL",
    "RESULT_TTL_SECONDS",
    "SCOPE_QUEUE",
    "JobTask",
    "TransientJobError",
    "app",
    "compute",
    "scope",
    "submit_job",
    "tasks",
]
