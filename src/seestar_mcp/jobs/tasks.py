"""Job task wrappers around seestar-controller's pure layers.

Every seestar-mcp Job runs on the :class:`JobTask` base, which carries the
retry policy (spec user story 12): a Job raising :class:`TransientJobError`
is autoretried with exponential backoff (``retry_backoff`` -> 2**n seconds,
capped at ``retry_backoff_max``, with full jitter), at most ``max_retries``
times. Job results expire from Redis after 24h (``result_expires`` in
:mod:`seestar_mcp.jobs.celery_app`).

The Jobs here are demos proving the plumbing without hardware:

- ``echo``: trivial round-trip through broker -> worker -> result backend.
- ``flaky_echo``: fails transiently N times, then succeeds — proves autoretry.
- ``count``: returns how many times THIS Job (task id) has executed — proves
  that resubmitting the same Idempotency key does not re-execute the Job.

They touch no Link, so they route to the Compute queue (the default queue).
"""

import redis

from seestar_mcp.jobs.celery_app import REDIS_URL, RESULT_TTL_SECONDS, app


class TransientJobError(Exception):
    """A transient Job failure: the Job is autoretried with backoff (spec #12).

    Raise this for failures that may heal on retry (flaky solve, photometry
    hiccup); anything else fails the Job immediately without retrying.
    """


class JobTask(app.Task):
    """Base class for every seestar-mcp Job: autoretry + exponential backoff.

    Retry policy (spec user story 12, documented here per issue #5):
    ``autoretry_for=(TransientJobError,)`` makes the worker catch
    :class:`TransientJobError` and reschedule the Job with
    ``countdown = min(retry_backoff_max, 2**retries)`` seconds of full-jitter
    exponential backoff, at most ``max_retries`` times; after that the Job
    ends in FAILURE with the original exception as error info.
    """

    abstract = True
    autoretry_for = (TransientJobError,)
    retry_backoff = True
    retry_backoff_max = 600  # cap the exponential backoff at 10 minutes
    retry_jitter = True
    max_retries = 5


def _redis() -> redis.Redis:
    """Redis client for the demo Jobs' attempt/execution counters."""
    return redis.Redis.from_url(REDIS_URL)


def _counted(key: str) -> int:
    """INCR ``key`` in Redis and (re)arm its 24h TTL; return the new value."""
    client = _redis()
    value = client.incr(key)
    client.expire(key, RESULT_TTL_SECONDS)
    return int(value)


@app.task(name="seestar_mcp.jobs.tasks.echo", base=JobTask)
def echo(message: str) -> str:
    """Echo ``message`` back, proving a Job round-trips through Redis."""
    return message


@app.task(
    name="seestar_mcp.jobs.tasks.flaky_echo",
    base=JobTask,
    # Demo override: keep the backoff short (<= 2s) so tests stay fast.
    retry_backoff_max=2,
)
def flaky_echo(message: str, fail_times: int = 2) -> str:
    """Demo Job that fails transiently ``fail_times`` times, then succeeds.

    Attempts are counted in Redis keyed by the task id (the Idempotency key),
    so retries count correctly even across prefork worker processes.
    """
    task_id = flaky_echo.request.id or "no-task-id"
    attempts = _counted(f"seestar-mcp:flaky:{task_id}")
    if attempts <= fail_times:
        raise TransientJobError(
            f"transient failure {attempts}/{fail_times} for Job {task_id}"
        )
    return message


@app.task(name="seestar_mcp.jobs.tasks.count", base=JobTask)
def count() -> int:
    """Demo Job returning how many times THIS Job (task id) has executed.

    Used to prove Idempotency: resubmitting the same key must not re-execute
    the Job, so for a fresh key the result is 1 — and stays 1 on resubmit.
    """
    task_id = count.request.id or "no-task-id"
    return _counted(f"seestar-mcp:executions:{task_id}")
