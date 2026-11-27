"""Celery application for seestar-mcp Jobs.

Broker and result backend are both Redis, configured via the ``REDIS_URL``
environment variable (default ``redis://localhost:6379/0``). Job results
expire after 24h (``result_expires``) so the broker does not grow
unboundedly.

Two queues (ADR-0004, GLOSSARY.md):

- the **Scope queue** (``scope``): Jobs that touch the Link. Exactly one
  consumer, so only one Job talks to the Scope at a time. Tasks whose name
  starts with ``seestar_mcp.jobs.scope.`` are routed here.
- the **Compute queue** (``compute``): Jobs that only process
  already-downloaded frames (solve, photometry, stacking) and never touch
  the Link. Parallel consumers; also the default queue for new tasks.

Use the app as ``celery -A seestar_mcp.jobs:app ...`` or via
``python -m seestar_mcp.worker`` (see ``seestar_mcp/worker.py``).
"""

import os

from celery import Celery
from kombu import Exchange, Queue

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

#: Name of the Scope queue (GLOSSARY.md): serialized Link access, one consumer.
SCOPE_QUEUE = "scope"
#: Name of the Compute queue (GLOSSARY.md): parallel processing of downloaded data.
COMPUTE_QUEUE = "compute"

#: Job results expire from Redis after 24h (spec user story 13).
RESULT_TTL_SECONDS = 24 * 60 * 60

_exchange = Exchange("seestar_mcp", type="direct")

app = Celery("seestar_mcp")
app.conf.update(
    broker_url=REDIS_URL,
    result_backend=REDIS_URL,
    # Retry the initial broker connection instead of crashing (worker
    # containers may start before redis is ready).
    broker_connection_retry_on_startup=True,
    # Job results expire after 24h so the broker does not grow unboundedly.
    result_expires=RESULT_TTL_SECONDS,
    task_queues=(
        Queue(SCOPE_QUEUE, _exchange, routing_key=SCOPE_QUEUE),
        Queue(COMPUTE_QUEUE, _exchange, routing_key=COMPUTE_QUEUE),
    ),
    # Unrouted Jobs (the Compute-queue kind) default to the Compute queue.
    task_default_queue=COMPUTE_QUEUE,
    task_default_exchange="seestar_mcp",
    task_default_exchange_type="direct",
    task_default_routing_key=COMPUTE_QUEUE,
    # Link-touching Jobs go to the Scope queue by task-name prefix; the
    # scope worker consumes that queue with concurrency 1 (ADR-0004).
    task_routes={
        "seestar_mcp.jobs.scope.*": {"queue": SCOPE_QUEUE},
    },
)
