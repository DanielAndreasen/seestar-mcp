"""Worker entrypoint: ``python -m seestar_mcp.worker``.

ADR-0004 requires the Scope queue to have exactly one consumer while the
Compute queue fans out. A single Celery prefork worker cannot give one queue
concurrency 1 and another more (all ``-Q`` queues share one pool), so this
entrypoint runs TWO Celery workers in one container — the compose file has a
single ``worker`` service (ADR-0002), and this module is its command:

- ``scope@...``   worker: ``-Q scope --concurrency=1`` — exactly one consumer
  for Link-touching Jobs (the Scope queue);
- ``compute@...`` worker: ``-Q compute`` — the Compute queue, with the
  default pool concurrency (one process per CPU; override with the
  ``SEESTAR_MCP_WORKER_CONCURRENCY`` env var).

Both workers connect to ``REDIS_URL`` (wired to the compose ``redis``
service). Both queues are therefore consumed by this one service, and the
worker logs show the scope queue at concurrency 1.
"""

import multiprocessing as mp
import os
import signal
import sys
from types import FrameType

from seestar_mcp.jobs import app
from seestar_mcp.jobs.celery_app import COMPUTE_QUEUE, SCOPE_QUEUE


def _worker_main(argv: list[str]) -> None:
    """Run one Celery worker in this (child) process."""
    app.worker_main(argv)


def _worker_argv(queue: str, hostname: str, concurrency: str | None) -> list[str]:
    argv = [
        "worker",
        "--loglevel=INFO",
        "-n",
        hostname,
        "-Q",
        queue,
    ]
    if concurrency is not None:
        argv += ["--concurrency", concurrency]
    return argv


def main() -> None:
    """Start the scope worker (concurrency 1) and the compute worker."""
    ctx = mp.get_context("spawn")
    workers = [
        ctx.Process(
            target=_worker_main,
            name="scope-worker",
            args=(
                _worker_argv(
                    SCOPE_QUEUE,
                    f"scope-{SCOPE_QUEUE}@%h",
                    concurrency="1",  # ADR-0004: exactly one consumer
                ),
            ),
        ),
        ctx.Process(
            target=_worker_main,
            name="compute-worker",
            args=(
                _worker_argv(
                    COMPUTE_QUEUE,
                    f"compute-{COMPUTE_QUEUE}@%h",
                    concurrency=os.environ.get("SEESTAR_MCP_WORKER_CONCURRENCY"),
                ),
            ),
        ),
    ]
    for w in workers:
        w.start()

    def _forward_signal(signum: int, frame: FrameType | None) -> None:
        # PID 1 in the container only gets the signal itself; forward it to
        # the child workers so they shut down warmly.
        for w in workers:
            if w.is_alive():
                w.terminate()

    signal.signal(signal.SIGTERM, _forward_signal)
    signal.signal(signal.SIGINT, _forward_signal)

    exit_code = 0
    for w in workers:
        w.join()
        if w.exitcode:
            exit_code = exit_code or w.exitcode
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
