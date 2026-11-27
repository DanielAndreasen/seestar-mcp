"""Tests for the Job plumbing Tools (issue #5).

Two kinds of tests:

- Config-only (run everywhere, no Redis): the ``job_status`` / ``job_result``
  Tools are registered on the FastMCP app, the Jobs' retry policy is
  configured on the ``JobTask`` base, and ``submit_job`` maps the Idempotency
  key onto the Celery task id (same key = not re-sent).
- Real Redis (same skip pattern as ``test_jobs.py``, via ``tests/conftest.py``):
  the full MCP-over-stdio round-trip — submit the echo Job with key K, poll
  ``job_status`` to SUCCESS, ``job_result`` returns the payload, resubmitting
  K returns the same Job without re-executing — plus autoretry of a
  transiently failing Job until SUCCESS and FAILURE error info.
"""

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from typing import Any

import pytest
import redis
from celery.result import AsyncResult
from conftest import requires_redis
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from seestar_mcp.jobs import app, submit_job
from seestar_mcp.jobs import submit as submit_module
from seestar_mcp.jobs.celery_app import REDIS_URL
from seestar_mcp.jobs.tasks import JobTask, TransientJobError, count, echo, flaky_echo
from seestar_mcp.server import mcp

# --- helpers ----------------------------------------------------------------


def _payload(res: Any) -> Any:
    """Normalize a tool result to its payload.

    Handles ``mcp.types.CallToolResult`` (real MCP session), the
    ``(content, structured)`` tuple / content list returned by
    ``FastMCP.call_tool``, and raw JSON text vs plain string payloads.
    """
    if isinstance(res, tuple):  # FastMCP.call_tool with structured output
        content, structured = res
        return structured if structured is not None else _payload(content)
    if isinstance(res, list):  # FastMCP.call_tool content blocks
        text = res[0].text if res else ""
        try:
            return json.loads(text)
        except ValueError:
            return text
    # mcp.types.CallToolResult over a real session
    if res.structuredContent is not None:
        structured = res.structuredContent
        if isinstance(structured, dict) and list(structured.keys()) == ["result"]:
            return structured["result"]
        return structured
    text = res.content[0].text if res.content else ""
    try:
        return json.loads(text)
    except ValueError:
        return text


class _FakeRedis:
    """In-memory stand-in for the claim ``SET NX EX`` / ``DELETE`` calls."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def set(
        self, key: str, value: str, nx: bool = False, ex: int | None = None
    ) -> bool | None:
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    def delete(self, *keys: str) -> None:
        for key in keys:
            self.store.pop(key, None)


def _patch_submit(
    monkeypatch: pytest.MonkeyPatch, fake_redis: _FakeRedis, sent: list[dict[str, Any]]
) -> None:
    """Patch the claim client and ``app.send_task``; record every send."""
    monkeypatch.setattr(submit_module, "_client", lambda: fake_redis)

    def _spy(
        name: str,
        args: Any = None,
        kwargs: Any = None,
        task_id: str | None = None,
        **options: Any,
    ) -> None:
        sent.append(
            {
                "name": name,
                "args": args,
                "kwargs": kwargs,
                "task_id": task_id,
                **options,
            }
        )

    monkeypatch.setattr(app, "send_task", _spy)


# --- Config-only tests (no Redis needed) ------------------------------------


def test_job_tools_registered_on_app() -> None:
    """job_status and job_result are Tools on the FastMCP app, keyed by Idempotency key."""
    tools = {t.name: t for t in asyncio.run(mcp.list_tools())}
    assert {"job_status", "job_result"} <= tools.keys()
    for name in ("job_status", "job_result"):
        schema = tools[name].inputSchema
        assert schema["properties"]["idempotency_key"]["type"] == "string"
        assert "idempotency_key" in schema["required"]


def test_job_task_base_defines_autoretry_with_exponential_backoff() -> None:
    """Retry policy (spec #12) lives on the JobTask base; demo Jobs use it."""
    assert issubclass(JobTask, app.Task)
    assert JobTask.autoretry_for == (TransientJobError,)
    assert JobTask.retry_backoff is True  # exponential backoff: 2**retries
    assert JobTask.retry_backoff_max == 600
    assert JobTask.retry_jitter is True
    assert JobTask.max_retries == 5
    for task in (echo, flaky_echo, count):
        assert isinstance(task, JobTask)
        assert task.autoretry_for == (TransientJobError,)
    # Result TTL 24h (spec #13) — already set by issue #4, asserted here too.
    assert app.conf.result_expires == 24 * 60 * 60


def test_submit_job_maps_idempotency_key_to_task_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """submit_job sends the Job with task_id == idempotency key and returns the key."""
    fake = _FakeRedis()
    sent: list[dict[str, Any]] = []
    _patch_submit(monkeypatch, fake, sent)
    key = f"cfg-{uuid.uuid4().hex}"
    returned = submit_job(
        key,
        "seestar_mcp.jobs.tasks.echo",
        args=["hi"],
        kwargs={"a": 1},
        queue="compute",
    )
    assert returned == key
    assert sent == [
        {
            "name": "seestar_mcp.jobs.tasks.echo",
            "args": ["hi"],
            "kwargs": {"a": 1},
            "task_id": key,  # the Idempotency key IS the Celery task id
            "queue": "compute",
        }
    ]
    assert fake.store == {
        submit_module.CLAIM_PREFIX + key: "seestar_mcp.jobs.tasks.echo"
    }


def test_submit_job_same_key_is_not_resent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resubmitting the same key returns the key without a duplicate send."""
    fake = _FakeRedis()
    sent: list[dict[str, Any]] = []
    _patch_submit(monkeypatch, fake, sent)
    key = f"cfg-{uuid.uuid4().hex}"
    assert submit_job(key, "seestar_mcp.jobs.tasks.echo", args=["first"]) == key
    # Same key = same Job: no duplicate execution on resubmit.
    assert submit_job(key, "seestar_mcp.jobs.tasks.echo", args=["second"]) == key
    assert len(sent) == 1
    assert sent[0]["args"] == ["first"]


def test_submit_job_releases_claim_when_broker_send_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the broker send fails after the claim, the claim is released for a retry."""
    fake = _FakeRedis()
    monkeypatch.setattr(submit_module, "_client", lambda: fake)

    def _boom(name: str, **kwargs: Any) -> None:
        raise ConnectionError("broker down")

    monkeypatch.setattr(app, "send_task", _boom)
    key = f"cfg-{uuid.uuid4().hex}"
    with pytest.raises(ConnectionError):
        submit_job(key, "seestar_mcp.jobs.tasks.echo")
    assert fake.store == {}  # claim released: a resubmit can retry


# --- Real Redis + real worker (skipped when Redis is unreachable) -----------


@requires_redis
def test_job_tools_round_trip_over_mcp_stdio(
    celery_worker: subprocess.Popen[str],
) -> None:
    """Full MCP-layer round-trip: submit with key K, poll to SUCCESS, get payload.

    Spawns the real server over stdio (official SDK ClientSession), submits
    the echo Job with key K via ``submit_job``, polls ``job_status`` until
    SUCCESS, then ``job_result`` returns the payload. Resubmitting K returns
    K and the Job is not re-executed (payload unchanged).
    """
    key = f"test-mcp-{uuid.uuid4().hex}"
    assert submit_job(key, "seestar_mcp.jobs.tasks.echo", args=["hello seestar"]) == key

    async def run() -> None:
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "seestar_mcp"],
            env={**os.environ, "REDIS_URL": REDIS_URL},
        )
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            names = {t.name for t in (await session.list_tools()).tools}
            assert {"job_status", "job_result"} <= names

            async def call(tool: str) -> Any:
                res = await session.call_tool(tool, {"idempotency_key": key})
                assert not res.isError
                return _payload(res)

            # Poll job_status: pending/running -> SUCCESS.
            states: list[str] = []
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                status = await call("job_status")
                assert status["idempotency_key"] == key
                states.append(status["state"])
                if status["state"] == "SUCCESS":
                    break
                assert status["state"] in ("PENDING", "RUNNING"), status
                assert status["error"] is None
                await asyncio.sleep(0.2)
            assert states[-1] == "SUCCESS"
            assert all(s in ("PENDING", "RUNNING") for s in states[:-1])

            # job_result returns the payload.
            assert await call("job_result") == "hello seestar"

            # Resubmit with the same key K: same Job, no re-execution.
            assert (
                submit_job(key, "seestar_mcp.jobs.tasks.echo", args=["DIFFERENT"])
                == key
            )
            status = await call("job_status")
            assert status["state"] == "SUCCESS"
            assert await call("job_result") == "hello seestar"

    asyncio.run(run())


@requires_redis
def test_resubmit_same_key_returns_same_job_without_reexecution(
    celery_worker: subprocess.Popen[str],
) -> None:
    """The ``count`` Job proves it: resubmitting key K does not execute again."""
    key = f"test-resubmit-{uuid.uuid4().hex}"
    assert submit_job(key, "seestar_mcp.jobs.tasks.count") == key
    result = AsyncResult(key, app=app)
    assert result.get(timeout=30) == 1  # first (and only) execution

    # Resubmitting the same key returns the key; the Job must NOT re-run.
    assert submit_job(key, "seestar_mcp.jobs.tasks.count") == key
    time.sleep(3)  # give a (buggy) duplicate message time to execute
    again = AsyncResult(key, app=app)
    assert again.state == "SUCCESS"
    assert again.get(timeout=30) == 1  # still 1: same Job, no re-execution
    client = redis.Redis.from_url(REDIS_URL)
    assert client.get(f"seestar-mcp:executions:{key}") == b"1"


@requires_redis
def test_transient_failure_is_autoretried_until_success(
    celery_worker: subprocess.Popen[str],
) -> None:
    """A Job raising TransientJobError twice is autoretried and ends SUCCESS."""
    key = f"test-flaky-{uuid.uuid4().hex}"
    assert (
        submit_job(
            key,
            "seestar_mcp.jobs.tasks.flaky_echo",
            args=["recovered"],
            kwargs={"fail_times": 2},
        )
        == key
    )
    result = AsyncResult(key, app=app)
    assert result.get(timeout=60) == "recovered"
    assert result.state == "SUCCESS"
    # 3 attempts = 2 transient failures + 1 success: the retries really happened.
    client = redis.Redis.from_url(REDIS_URL)
    assert client.get(f"seestar-mcp:flaky:{key}") == b"3"

    # The Tool layer reports the same SUCCESS.
    status = _payload(
        asyncio.run(mcp.call_tool("job_status", {"idempotency_key": key}))
    )
    assert status == {"idempotency_key": key, "state": "SUCCESS", "error": None}


@requires_redis
def test_permanent_failure_ends_in_failure_with_error_info(
    celery_worker: subprocess.Popen[str],
) -> None:
    """A Job that keeps failing ends FAILURE; job_status/job_result carry error info."""
    key = f"test-fail-{uuid.uuid4().hex}"
    submit_job(
        key,
        "seestar_mcp.jobs.tasks.flaky_echo",
        args=["never"],
        kwargs={"fail_times": 99},
    )
    result = AsyncResult(key, app=app)
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline and result.state not in ("SUCCESS", "FAILURE"):
        time.sleep(0.5)
    assert result.state == "FAILURE"

    status = _payload(
        asyncio.run(mcp.call_tool("job_status", {"idempotency_key": key}))
    )
    assert status["state"] == "FAILURE"
    assert "TransientJobError" in status["error"]

    payload = _payload(
        asyncio.run(mcp.call_tool("job_result", {"idempotency_key": key}))
    )
    assert payload["state"] == "FAILURE"
    assert "TransientJobError" in payload["error"]
