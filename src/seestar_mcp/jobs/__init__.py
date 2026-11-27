"""Celery Jobs layer for seestar-mcp (broker + result backend: Redis).

Exposes ``app`` so the Celery CLI works as ``celery -A seestar_mcp.jobs:app``.
Importing this package also imports :mod:`seestar_mcp.jobs.tasks` so every
Job is registered with the app.
"""

from seestar_mcp.jobs import tasks as tasks
from seestar_mcp.jobs.celery_app import COMPUTE_QUEUE, REDIS_URL, SCOPE_QUEUE, app

__all__ = ["COMPUTE_QUEUE", "REDIS_URL", "SCOPE_QUEUE", "app", "tasks"]
