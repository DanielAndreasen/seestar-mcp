"""Shared test fixtures: real-Redis gating and a real Celery worker subprocess.

Round-trip tests use a REAL Celery worker started as a subprocess (solo pool,
consuming the Compute queue) against the real Redis at ``REDIS_URL``. They
are SKIPPED when Redis is unreachable (e.g. a local run without redis), so
local runs stay green; in CI the redis service container sets REDIS_URL and
the round-trip runs for real (spec testing decisions).
"""

import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from urllib.parse import urlparse

import pytest

from seestar_mcp.jobs.celery_app import COMPUTE_QUEUE, REDIS_URL


def redis_reachable() -> bool:
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
    not redis_reachable(),
    reason=f"Redis at {REDIS_URL} unreachable — local run without redis; "
    "CI provides a redis service container via REDIS_URL",
)


@pytest.fixture(scope="module")
def celery_worker() -> Iterator[subprocess.Popen[str]]:
    """Start a real Celery worker subprocess consuming the Compute queue.

    ``--pool=solo`` keeps it to one process; ``-Q compute`` mirrors how the
    demo Jobs are routed (the Compute queue is the default queue). Only used
    by tests already skipped when Redis is unreachable; the guard below is
    a second line of defense.
    """
    if not redis_reachable():
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
