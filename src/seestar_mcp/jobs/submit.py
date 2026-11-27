"""Job submission helper: the Idempotency key IS the Celery task id.

Spec user story 9 / GLOSSARY.md "Idempotency key": every Job submission
accepts a client-chosen key and returns it; resubmitting the same key refers
to the existing Job instead of starting a duplicate, so transient failures
are safe to retry.

Celery does not deduplicate messages by task id at the broker — a second
message with the same task id would be executed again and overwrite the
stored result. This module therefore claims the key atomically in Redis
(``SET NX EX``) before sending: only the FIRST submission with a given key
reaches the broker, so same key = same Job, no duplicate execution. The
claim key expires together with the Job result (24h, ``result_expires``).
"""

from collections.abc import Mapping, Sequence
from typing import Any

import redis

from seestar_mcp.jobs.celery_app import REDIS_URL, RESULT_TTL_SECONDS, app

#: Prefix of the Redis key marking an Idempotency key as claimed.
CLAIM_PREFIX = "seestar-mcp:job-claim:"

_redis_client: redis.Redis | None = None


def _client() -> redis.Redis:
    """Lazily built Redis client for the claim keys (same broker/backend URL)."""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis.from_url(REDIS_URL)
    return _redis_client


def submit_job(
    idempotency_key: str,
    task_name: str,
    args: Sequence[Any] | None = None,
    kwargs: Mapping[str, Any] | None = None,
    queue: str | None = None,
) -> str:
    """Submit the Job ``task_name`` under the Idempotency key; return the key.

    The idempotency key is used AS the Celery task id, so ``AsyncResult(key)``
    (and the ``job_status`` / ``job_result`` Tools) address this exact Job.

    Resubmitting the same key returns the key without sending a duplicate
    message — the Job is never executed twice under one key. If the broker
    send fails after the claim, the claim is released so a resubmit retries.

    ``queue`` optionally overrides routing (Scope queue / Compute queue,
    ADR-0004); by default the app's routes decide.
    """
    client = _client()
    claim = CLAIM_PREFIX + idempotency_key
    # SET NX EX: atomic "first submitter wins"; TTL matches the 24h result TTL.
    claimed = client.set(claim, task_name, nx=True, ex=RESULT_TTL_SECONDS)
    if not claimed:
        # Same key = same Job: do NOT send a duplicate message.
        return idempotency_key
    options: dict[str, Any] = {"task_id": idempotency_key}
    if queue is not None:
        options["queue"] = queue
    try:
        app.send_task(
            task_name,
            args=list(args or ()),
            kwargs=dict(kwargs or {}),
            **options,
        )
    except Exception:
        # Broker unreachable: release the claim so a resubmit can retry.
        client.delete(claim)
        raise
    return idempotency_key
