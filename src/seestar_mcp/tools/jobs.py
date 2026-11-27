"""Job plumbing Tools: ``job_status`` and ``job_result`` (issue #5).

Spec user stories 10/11: poll a Job by its Idempotency key and fetch its
payload. Both Tools read the Celery result backend through ``AsyncResult``
keyed by the Idempotency key — which :func:`seestar_mcp.jobs.submit_job`
maps onto the Celery task id — so polling is free and never re-executes the
Job. Payloads are small and JSON-serializable by contract (spec Further
Notes: FITS stay on disk in the Project).
"""

from typing import Any

from celery.result import AsyncResult
from mcp.server.fastmcp import FastMCP

from seestar_mcp.jobs import app

#: Celery states that mean "a worker has the Job" (not done yet).
_RUNNING_STATES = frozenset({"RECEIVED", "STARTED", "RETRY"})


def _job(idempotency_key: str) -> AsyncResult:
    """The Job addressed by the Idempotency key (key = Celery task id)."""
    return AsyncResult(idempotency_key, app=app)


def _public_state(result: AsyncResult) -> str:
    """Map the raw Celery state onto PENDING / RUNNING / SUCCESS / FAILURE."""
    if result.state == "SUCCESS":
        return "SUCCESS"
    if result.state in ("FAILURE", "REVOKED"):
        return "FAILURE"
    if result.state in _RUNNING_STATES:
        return "RUNNING"
    return "PENDING"


def _error_info(result: AsyncResult) -> str | None:
    """Short error info for a failed Job (exception type + message)."""
    if result.state not in ("FAILURE", "REVOKED"):
        return None
    exc = result.result
    if exc is None:
        return result.state
    return f"{type(exc).__name__}: {exc}"


def register(mcp: FastMCP) -> None:
    """Register the Job Tools on the FastMCP app."""

    @mcp.tool()
    def job_status(idempotency_key: str) -> dict[str, str | None]:
        """Poll a Job by its Idempotency key.

        Returns the Job's ``state`` — PENDING, RUNNING, SUCCESS or FAILURE —
        plus ``error`` info (exception type and message) when the Job failed.
        Polling never re-executes the Job: the key is the Celery task id.
        """
        result = _job(idempotency_key)
        return {
            "idempotency_key": idempotency_key,
            "state": _public_state(result),
            "error": _error_info(result),
        }

    @mcp.tool()
    def job_result(idempotency_key: str) -> Any:
        """Return a Job's result payload by its Idempotency key.

        On SUCCESS this is the Job's payload (small, JSON-serializable by
        contract — FITS stay on disk in the Project). Before SUCCESS, or on
        FAILURE, a small status dict is returned instead, with ``state`` and
        ``error``; poll with ``job_status`` to tell them apart.
        """
        result = _job(idempotency_key)
        if result.state == "SUCCESS":
            return result.result
        return {
            "idempotency_key": idempotency_key,
            "state": _public_state(result),
            "error": _error_info(result),
        }


__all__ = ["register"]
