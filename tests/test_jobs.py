"""Tests for the Celery Jobs layer (issue #4).

Two kinds of tests:

- App/queue configuration: asserted unconditionally, no Redis needed.
- Round-trip: a REAL Celery worker is started as a subprocess (solo pool,
  consuming the Compute queue) and the ``echo`` Job is sent through real
  Redis via ``REDIS_URL``; the AsyncResult is polled to SUCCESS. These tests
  are SKIPPED when Redis is unreachable (e.g. a local run without redis), so
  local runs stay green; in CI the redis service container sets REDIS_URL
  and the round-trip runs for real.

The skip pattern (``requires_redis``) and the ``celery_worker`` fixture live
in ``tests/conftest.py`` and are shared with the Job plumbing Tool tests.
"""

import os
import subprocess
import uuid

from celery.result import AsyncResult
from conftest import requires_redis

from seestar_mcp.jobs import app
from seestar_mcp.jobs.celery_app import COMPUTE_QUEUE, REDIS_URL, SCOPE_QUEUE
from seestar_mcp.jobs.tasks import echo

# --- App / queue configuration (no Redis needed) ---------------------------


def test_task_queues_declare_scope_and_compute() -> None:
    """app.conf.task_queues contains exactly the Scope queue and Compute queue."""
    queues = app.conf.task_queues
    assert queues is not None
    assert isinstance(queues, (list, tuple))
    names = {q.name for q in queues}
    assert names == {SCOPE_QUEUE, COMPUTE_QUEUE}


def test_broker_and_result_backend_from_redis_url() -> None:
    """Broker and result backend both come from REDIS_URL (default localhost)."""
    assert app.conf.broker_url == REDIS_URL
    assert app.conf.result_backend == REDIS_URL
    assert REDIS_URL == os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def test_job_results_expire_after_24h() -> None:
    assert app.conf.result_expires == 24 * 60 * 60


def test_compute_queue_is_default_scope_queue_routed() -> None:
    """Unrouted Jobs default to the Compute queue; scope.* Jobs route to the Scope queue."""
    assert app.conf.task_default_queue == COMPUTE_QUEUE
    routes = app.conf.task_routes
    assert isinstance(routes, dict)
    assert routes["seestar_mcp.jobs.scope.*"]["queue"] == SCOPE_QUEUE


# --- Round-trip through real Redis (skipped when Redis is unreachable) ------


@requires_redis
def test_echo_job_round_trips_through_redis(
    celery_worker: subprocess.Popen[str],
) -> None:
    """Submit the echo Job, poll the AsyncResult to SUCCESS, re-poll by id.

    The client-chosen idempotency key becomes the Celery task id, so
    re-polling the same key reads the stored Job result (spec #9/#10).
    The worker only consumes the Compute queue, so a SUCCESS here also
    proves the demo Job is routed to the Compute queue.
    """
    key = f"test-echo-{uuid.uuid4().hex}"
    result = echo.apply_async(args=["hello seestar"], task_id=key)
    assert result.id == key

    assert result.get(timeout=30) == "hello seestar"
    assert result.state == "SUCCESS"

    # Re-poll the same idempotency key: reads the stored result, no new Job.
    again = AsyncResult(key, app=app)
    assert again.state == "SUCCESS"
    assert again.get(timeout=30) == "hello seestar"
