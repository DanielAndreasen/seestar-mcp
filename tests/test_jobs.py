"""Tests for the Celery Jobs layer (issue #4).

Two kinds of tests:

- App/queue configuration: asserted unconditionally, no Redis needed.
- Round-trip: a REAL Celery worker is started as a subprocess (solo pool,
  consuming the Compute queue) and the ``echo`` Job is sent through real
  Redis via ``REDIS_URL``; the AsyncResult is polled to SUCCESS. These tests
  are SKIPPED when Redis is unreachable (e.g. a local run without redis), so
  local runs stay green; in CI the redis service container sets REDIS_URL
  and the round-trip runs for real.
"""

import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from urllib.parse import urlparse

import pytest
from celery.result import AsyncResult

from seestar_mcp.jobs import app
from seestar_mcp.jobs.celery_app import COMPUTE_QUEUE, REDIS_URL, SCOPE_QUEUE
from seestar_mcp.jobs.tasks import echo


def _redis_reachable() -> bool:
    """Socket-connect check against the REDIS_URL host:port (no redis client)."""
    parsed = urlparse(REDIS_URL)
    host = parsed.hostname or "localhost"
    port = parsed.port or 6379
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


requires_redis = pytest.mark.skipif(
    not _redis_reachable(),
    reason=f"Redis at {REDIS_URL} unreachable — local run without redis; "
    "CI provides a redis service container via REDIS_URL",
)


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


@pytest.fixture(scope="module")
def celery_worker() -> Iterator[subprocess.Popen[str]]:
    """Start a real Celery worker subprocess consuming the Compute queue.

    ``--pool=solo`` keeps it to one process; ``-Q compute`` mirrors how the
    demo Job is routed (the Compute queue is the default queue). Only used
    by tests already skipped when Redis is unreachable; the guard below is
    a second line of defense.
    """
    if not _redis_reachable():
        pytest.skip(f"Redis at {REDIS_URL} unreachable")
    env = os.environ.copy()
    env["REDIS_URL"] = REDIS_URL
    env.setdefault("C_FORCE_ROOT", "1")  # harmless when not root
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "celery",
            "-A",
            "seestar_mcp.jobs:app",
            "worker",
            "--pool=solo",
            "-Q",
            COMPUTE_QUEUE,
            "-n",
            f"test-{COMPUTE_QUEUE}@%h",
            "--loglevel=INFO",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    lines: list[str] = []

    def _pump() -> None:
        assert proc.stdout is not None
        lines.extend(proc.stdout)

    threading.Thread(target=_pump, daemon=True).start()

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if any("ready." in line for line in lines):
            break
        if proc.poll() is not None:
            pytest.fail("celery worker exited early:\n" + "".join(lines))
        time.sleep(0.2)
    else:
        proc.kill()
        pytest.fail("celery worker did not become ready:\n" + "".join(lines))

    yield proc

    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


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
