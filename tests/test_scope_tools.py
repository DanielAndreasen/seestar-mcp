"""Tests for the scope-mutating Tools ``goto`` / ``park`` / ``capture`` (issue #7).

Three kinds of tests:

- Config-only (run everywhere, no redis): the Tools are registered with the
  right schemas, the scope tasks live under ``seestar_mcp.jobs.scope.*``
  (routed to the Scope queue by the existing prefix route).
- Always-on, no redis: the **Slew gate** (ADR-0003) — ``goto`` / ``park``
  are refused when the server subprocess is started without
  ``SEESTAR_MCP_ALLOW_SLEW=1``, and pass the gate with it set.
- Real redis + ``mock_scope`` + a scope-queue Celery worker (skipped
  locally without redis, run in CI): ``goto`` slews the mock (explicit
  RA/Dec and a catalog name resolved from a seeded offline cache) and
  ``job_status`` walks to SUCCESS; ``park`` parks the mock; a ``capture``
  Job stores sub-frame FITS under the tmp ``SEESTAR_PROJECT_ROOT`` and
  ``job_result`` lists them; two concurrent capture Jobs never overlap on
  the Link (``mock_scope.client_count <= 1`` while both run).

All Tool calls go through the real MCP stdio layer (``ClientSession`` on a
spawned server subprocess), like the other Tool tests.
"""

import asyncio
import dataclasses
import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from conftest import requires_redis
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from seestar_controller.catalog import cache as catalog_cache
from seestar_controller.catalog import simbad
from seestar_controller.catalog.simbad import CatalogCandidate
from seestar_controller.link.mock_scope import MockScope
from seestar_controller.store import frames as store_frames
from seestar_controller.store import project as store_project
from test_link_tools import (
    _assert_no_held_connections,
    _error_text,
    _payload,
    _server_params,
)

from seestar_mcp.jobs import app
from seestar_mcp.jobs.celery_app import SCOPE_QUEUE
from seestar_mcp.jobs.scope import CAPTURE_TASK, GOTO_TASK, PARK_TASK
from seestar_mcp.server import mcp

#: M 13 (Hercules Cluster) J2000, as SIMBAD would answer for "M13".
M13 = CatalogCandidate(
    name="M 13",
    ra_deg=250.4229,  # 16h 41m 41s
    dec_deg=36.4614,  # +36d 27' 41"
    magnitude=5.8,
    otype="Glob. Cluster",
    matched_id="M 13",
)


# --- helpers ----------------------------------------------------------------


def _key(prefix: str) -> str:
    """A fresh Idempotency key."""
    return f"{prefix}-{uuid.uuid4().hex}"


def _server_params_with_redis(
    scope_key_path: Path, extra_env: dict[str, str] | None = None
) -> StdioServerParameters:
    """Stdio params spawning the server with ``REDIS_URL`` + ``extra_env`` set.

    The server process submits Jobs, so it needs the broker URL (in CI it is
    already in the environment; default to localhost otherwise).
    """
    env = dict(extra_env or {})
    env.setdefault(
        "REDIS_URL", os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    )
    return _server_params(scope_key_path, env)


def _seed_catalog_cache(cache_dir: Path, name: str, candidate: CatalogCandidate) -> None:
    """Pre-populate the offline catalog cache so ``lookup_name(name)`` needs no network.

    The cache key is exactly the one :func:`seestar_controller.catalog.facade.lookup_name`
    computes (ADQL + maxrec); a fresh entry (``ttl=None``) is served without
    any fetch. The server subprocess gets ``SEESTAR_CACHE_DIR=<cache_dir>``.
    """
    adql = simbad.build_lookup_query(name.strip())
    query = catalog_cache._name_query(adql, maxrec=simbad.DEFAULT_MAX_CANDIDATES)
    catalog_cache.QueryCache(cache_dir).set(
        query, [dataclasses.asdict(candidate)], ttl=None
    )


async def _poll_until_done(
    session: ClientSession, idempotency_key: str, timeout_s: float = 90.0
) -> dict[str, Any]:
    """Poll ``job_status`` until the Job is SUCCESS or FAILURE; return the last status."""
    deadline = time.monotonic() + timeout_s
    status: dict[str, Any] = {}
    while time.monotonic() < deadline:
        res = await session.call_tool(
            "job_status", {"idempotency_key": idempotency_key}
        )
        assert not res.isError
        status = _payload(res)
        if status["state"] in ("SUCCESS", "FAILURE"):
            return status
        await asyncio.sleep(0.2)
    raise AssertionError(f"Job {idempotency_key} did not finish in time: {status}")


async def _job_result(session: ClientSession, idempotency_key: str) -> Any:
    """``job_result`` for a finished Job."""
    res = await session.call_tool("job_result", {"idempotency_key": idempotency_key})
    assert not res.isError
    return _payload(res)


def _optional_typed(schema: dict[str, Any], prop: str, type_name: str) -> None:
    """An optional param is an anyOf with a null variant plus a typed one."""
    variants = schema["properties"][prop]["anyOf"]
    assert any(v.get("type") == type_name for v in variants)
    assert any(v.get("type") == "null" for v in variants)


# --- Config-only tests (no redis needed) ------------------------------------


def test_scope_tools_registered_on_app() -> None:
    """goto / park / capture are Tools, all keyed by the Idempotency key."""
    tools = {t.name: t for t in asyncio.run(mcp.list_tools())}
    assert {"goto", "park", "capture"} <= tools.keys()
    for name in ("goto", "park", "capture"):
        schema = tools[name].inputSchema
        assert schema["properties"]["idempotency_key"]["type"] == "string"
        assert "idempotency_key" in schema["required"]

    goto_schema = tools["goto"].inputSchema
    _optional_typed(goto_schema, "target", "string")
    _optional_typed(goto_schema, "ra", "number")
    _optional_typed(goto_schema, "dec", "number")
    for optional in ("target", "ra", "dec", "ip", "timeout"):
        assert optional not in goto_schema.get("required", [])
    capture_schema = tools["capture"].inputSchema
    assert capture_schema["properties"]["frames"]["type"] == "integer"
    assert capture_schema["properties"]["exposure"]["type"] == "number"
    assert capture_schema["properties"]["gain"]["type"] == "integer"


def test_scope_tasks_registered_and_routed_to_scope_queue() -> None:
    """The scope tasks live under seestar_mcp.jobs.scope.* -> the Scope queue."""
    for name in (GOTO_TASK, PARK_TASK, CAPTURE_TASK):
        assert name.startswith("seestar_mcp.jobs.scope.")
        assert name in app.tasks
    assert app.conf.task_routes == {
        "seestar_mcp.jobs.scope.*": {"queue": SCOPE_QUEUE}
    }


# --- Slew gate (ADR-0003): always-on, no redis -------------------------------


def test_goto_and_park_refused_without_slew_flag(scope_key_path: Path) -> None:
    """Without SEESTAR_MCP_ALLOW_SLEW=1, goto/park raise a clear error (no redis needed).

    The gate is evaluated before anything is submitted, so this runs with no
    redis and no mock: the server subprocess is spawned WITHOUT the flag.
    ``capture`` is NOT gated (ADR-0003): it gets past the gate and fails on
    argument validation instead — the error must not mention the Slew gate.
    Nothing is submitted here, so the shared Scope queue stays clean for the
    redis-gated tests.
    """

    async def run() -> None:
        params = _server_params(scope_key_path)  # no SEESTAR_MCP_ALLOW_SLEW
        assert params.env is not None
        params.env.pop("SEESTAR_MCP_ALLOW_SLEW", None)
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            names = {t.name for t in (await session.list_tools()).tools}
            assert {"goto", "park", "capture"} <= names

            res = await session.call_tool(
                "goto", {"idempotency_key": _key("gate"), "ra": 10.5, "dec": 41.3}
            )
            assert res.isError
            assert "SEESTAR_MCP_ALLOW_SLEW" in _error_text(res)

            res = await session.call_tool(
                "park", {"idempotency_key": _key("gate")}
            )
            assert res.isError
            assert "SEESTAR_MCP_ALLOW_SLEW" in _error_text(res)

            # capture is not a Slew: no gate error. frames=0 fails argument
            # validation before any submission, so this needs no redis.
            res = await session.call_tool(
                "capture", {"idempotency_key": _key("gate"), "frames": 0}
            )
            assert res.isError
            text = _error_text(res)
            assert "SEESTAR_MCP_ALLOW_SLEW" not in text
            assert "frames" in text

    asyncio.run(run())


def test_goto_passes_the_gate_when_slew_flag_set(scope_key_path: Path) -> None:
    """With SEESTAR_MCP_ALLOW_SLEW=1 the gate no longer refuses goto (no redis needed).

    An out-of-range ``ra`` fails target validation — which runs AFTER the
    gate — so the error proves the call got past the Slew gate without
    submitting anything (no redis needed, nothing left in the Scope queue).
    """

    async def run() -> None:
        params = _server_params(scope_key_path, {"SEESTAR_MCP_ALLOW_SLEW": "1"})
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            res = await session.call_tool(
                "goto", {"idempotency_key": _key("open"), "ra": 99.0, "dec": 41.3}
            )
            assert res.isError
            text = _error_text(res)
            assert "SEESTAR_MCP_ALLOW_SLEW" not in text
            assert "ra must be in [0, 24)" in text

    asyncio.run(run())


# --- Real redis + mock Scope + scope-queue worker (skipped without redis) ----


@requires_redis
def test_goto_job_slews_mock_scope_and_reaches_success(
    mock_scope: MockScope, scope_key_path: Path, scope_worker: subprocess.Popen[str]
) -> None:
    """goto with the Slew flag submits a scope Job; the worker slews the mock.

    Explicit RA/Dec (no catalog needed): the Job goes to the Scope queue,
    the worker opens a per-job Link, ``Device.goto`` waits out the mock's
    ``slew_duration`` and the arrival event, ``job_status`` walks to
    SUCCESS and ``job_result`` reports the arrived state. Afterwards the
    mock sees no held connection (ADR-0004).
    """
    mock_scope.slew_duration = 0.3
    address = f"127.0.0.1:{mock_scope.command_port}"
    key = _key("goto")

    async def run() -> Any:
        params = _server_params_with_redis(
            scope_key_path, {"SEESTAR_MCP_ALLOW_SLEW": "1"}
        )
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            res = await session.call_tool(
                "goto",
                {
                    "idempotency_key": key,
                    "ra": 10.5,
                    "dec": 41.3,
                    "ip": address,
                },
            )
            assert not res.isError
            submitted = _payload(res)
            assert submitted["idempotency_key"] == key
            assert submitted["task"] == GOTO_TASK
            assert submitted["queue"] == SCOPE_QUEUE
            assert submitted["address"] == address
            assert submitted["goto"] == {"ra": 10.5, "dec": 41.3, "name": None}

            status = await _poll_until_done(session, key)
            assert status["state"] == "SUCCESS", status
            assert status["error"] is None

            result = await _job_result(session, key)
            assert result["requested"] == {"ra": 10.5, "dec": 41.3, "name": None}
            assert result["mode"] == "idle"  # arrived: no longer slewing
            assert result["slewing"] is False
            assert result["ra"] == pytest.approx(10.5)
            assert result["dec"] == pytest.approx(41.3)
            return result

    result = asyncio.run(run())
    assert result["address"] == address
    # the mock Scope really slewed to the requested coordinates
    assert mock_scope.last_goto == {"ra": 10.5, "dec": 41.3}
    state = mock_scope.scope_state()
    assert state["mode"] == "idle"
    assert state["target"]["ra"] == 10.5
    assert state["target"]["dec"] == 41.3
    _assert_no_held_connections(mock_scope)  # per-job Link, closed


@requires_redis
def test_goto_resolves_target_name_via_catalog(
    mock_scope: MockScope,
    scope_key_path: Path,
    scope_worker: subprocess.Popen[str],
    tmp_path: Path,
) -> None:
    """goto(target="M13") resolves the name via the catalog (SIMBAD cache) and slews there.

    The server subprocess gets a seeded offline catalog cache
    (``SEESTAR_CACHE_DIR``), so the SIMBAD lookup is served from disk — no
    network in CI. The brightest match's J2000 coordinates are what the
    scope Job slews to.
    """
    mock_scope.slew_duration = 0.2
    address = f"127.0.0.1:{mock_scope.command_port}"
    cache_dir = tmp_path / "catalog-cache"
    _seed_catalog_cache(cache_dir, "M13", M13)
    key = _key("goto-name")
    expected_ra = M13.ra_deg / 15.0  # hours

    async def run() -> Any:
        params = _server_params_with_redis(
            scope_key_path,
            {
                "SEESTAR_MCP_ALLOW_SLEW": "1",
                "SEESTAR_CACHE_DIR": str(cache_dir),
            },
        )
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            res = await session.call_tool(
                "goto", {"idempotency_key": key, "target": "M13", "ip": address}
            )
            assert not res.isError
            submitted = _payload(res)
            assert submitted["goto"]["name"] == "M 13"
            assert submitted["goto"]["ra"] == pytest.approx(expected_ra)
            assert submitted["goto"]["dec"] == pytest.approx(M13.dec_deg)

            status = await _poll_until_done(session, key)
            assert status["state"] == "SUCCESS", status
            return await _job_result(session, key)

    result = asyncio.run(run())
    assert result["requested"]["name"] == "M 13"
    assert result["requested"]["ra"] == pytest.approx(expected_ra)
    assert result["requested"]["dec"] == pytest.approx(M13.dec_deg)
    assert result["ra"] == pytest.approx(expected_ra)
    assert result["dec"] == pytest.approx(M13.dec_deg)
    assert mock_scope.last_goto is not None
    assert mock_scope.last_goto["ra"] == pytest.approx(expected_ra)
    assert mock_scope.last_goto["dec"] == pytest.approx(M13.dec_deg)
    _assert_no_held_connections(mock_scope)


@requires_redis
def test_park_job_parks_mock_scope(
    mock_scope: MockScope, scope_key_path: Path, scope_worker: subprocess.Popen[str]
) -> None:
    """park with the Slew flag submits a scope Job; the worker parks the mock."""
    address = f"127.0.0.1:{mock_scope.command_port}"
    key = _key("park")

    async def run() -> Any:
        params = _server_params_with_redis(
            scope_key_path, {"SEESTAR_MCP_ALLOW_SLEW": "1"}
        )
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            res = await session.call_tool(
                "park", {"idempotency_key": key, "ip": address}
            )
            assert not res.isError
            submitted = _payload(res)
            assert submitted["task"] == PARK_TASK
            assert submitted["queue"] == SCOPE_QUEUE
            status = await _poll_until_done(session, key)
            assert status["state"] == "SUCCESS", status
            return await _job_result(session, key)

    result = asyncio.run(run())
    assert result["parked"] is True
    assert result["mode"] == "parked"
    assert mock_scope.scope_state()["mode"] == "parked"
    _assert_no_held_connections(mock_scope)


@requires_redis
def test_capture_job_stores_subframe_fits_in_project(
    mock_scope: MockScope,
    scope_key_path: Path,
    scope_worker: subprocess.Popen[str],
    tmp_path: Path,
) -> None:
    """A capture Job stores sub-frame FITS in the tmp Project; job_result lists them.

    The worker (``SEESTAR_PROJECT_ROOT`` = tmp) stacks 3 sub-frames on the
    mock, downloads them via ``link.frames`` and stores them with
    ``store.frames.store_frame`` under the project's open session. The
    ``job_result`` payload lists frame ids + project-relative paths, and the
    FITS files really exist under the Project root.
    """
    mock_scope.stack_event_interval = 0.05
    address = f"127.0.0.1:{mock_scope.command_port}"
    key = _key("capture")
    project_root = tmp_path / "projects"

    async def run() -> Any:
        params = _server_params_with_redis(scope_key_path)  # capture: no Slew gate
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            res = await session.call_tool(
                "capture",
                {
                    "idempotency_key": key,
                    "frames": 3,
                    "exposure": 1.0,
                    "gain": 80,
                    "project": "capture-proj",
                    "ip": address,
                },
            )
            assert not res.isError
            submitted = _payload(res)
            assert submitted["task"] == CAPTURE_TASK
            assert submitted["queue"] == SCOPE_QUEUE
            assert submitted["capture"] == {
                "project": "capture-proj",
                "frames": 3,
                "exposure": 1.0,
                "gain": 80,
            }
            status = await _poll_until_done(session, key)
            assert status["state"] == "SUCCESS", status
            return await _job_result(session, key)

    result = asyncio.run(run())
    assert result["project"] == "capture-proj"
    assert result["session_id"]
    assert result["requested"] == {"frames": 3, "exposure": 1.0, "gain": 80}
    # at least the requested 3 sub-frames (the Stack may record one more
    # before stop_stack lands — every captured frame is stored, none lost)
    assert result["count"] >= 3
    assert len(result["frames"]) == result["count"]
    project_path = Path(result["project_path"])
    assert project_path == project_root / "capture-proj"
    for frame in result["frames"]:
        assert frame["kind"] == "sub"
        assert frame["path"].startswith(f"subs/{result['session_id']}/")
        assert frame["path"].endswith(".fits")
        assert frame["exposure"] == 1.0
        assert frame["gain"] == 80
        fits_file = project_path / frame["path"]
        assert fits_file.is_file()
        assert fits_file.read_bytes()[:6] == b"SIMPLE"  # a real FITS file

    # the Project store agrees: the session's db rows are exactly the payload
    with store_project.open_project(project_path) as project:
        stored = store_frames.list_frames(project, result["session_id"], kind="sub")
        assert len(stored) == result["count"]
        assert {f.id for f in stored} == {f["id"] for f in result["frames"]}
        assert all((project_path / f.path).is_file() for f in stored)
    # the mock produced exactly the sub-frames this Job downloaded
    assert len(mock_scope.list_frames("sub")) == result["count"]
    _assert_no_held_connections(mock_scope)


@requires_redis
def test_concurrent_capture_jobs_never_overlap_on_the_link(
    mock_scope: MockScope,
    scope_key_path: Path,
    scope_worker: subprocess.Popen[str],
    tmp_path: Path,
) -> None:
    """Two concurrent capture Jobs never touch the Link at the same time.

    Both Jobs go to the Scope queue, whose single consumer (ADR-0004: the
    ``scope_worker`` fixture runs one solo consumer, like production's
    ``--concurrency=1``) executes them one at a time, each on its own
    short-lived per-job Link. While both run, the mock Scope never sees
    more than one client connection (``client_count <= 1``).
    """
    mock_scope.stack_event_interval = 0.1  # slow the stack down so both Jobs span time
    address = f"127.0.0.1:{mock_scope.command_port}"
    keys = [_key("ser-a"), _key("ser-b")]

    async def run() -> dict[str, Any]:
        params = _server_params_with_redis(scope_key_path)
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            for key in keys:
                res = await session.call_tool(
                    "capture",
                    {
                        "idempotency_key": key,
                        "frames": 2,
                        "exposure": 1.0,
                        "gain": 80,
                        "project": "ser-proj",
                        "ip": address,
                    },
                )
                assert not res.isError
                assert _payload(res)["queue"] == SCOPE_QUEUE

            max_clients = 0
            saw_link = False
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                max_clients = max(max_clients, mock_scope.client_count)
                saw_link = saw_link or mock_scope.client_count > 0
                states = []
                for key in keys:
                    res = await session.call_tool(
                        "job_status", {"idempotency_key": key}
                    )
                    assert not res.isError
                    states.append(_payload(res)["state"])
                if all(s in ("SUCCESS", "FAILURE") for s in states):
                    break
                await asyncio.sleep(0.02)
            return {
                "states": states,
                "max_clients": max_clients,
                "saw_link": saw_link,
                "results": {
                    key: await _job_result(session, key) for key in keys
                },
            }

    outcome = asyncio.run(run())
    assert outcome["states"] == ["SUCCESS", "SUCCESS"], outcome["states"]
    # ADR-0004: while both Jobs ran, the Link never saw two clients at once
    assert outcome["saw_link"] is True  # the sampling window really saw the Link in use
    assert outcome["max_clients"] <= 1
    # both Jobs stored their frames in the Project
    total = 0
    for key in keys:
        result = outcome["results"][key]
        assert result["count"] >= 2
        total += result["count"]
        project_path = Path(result["project_path"])
        assert project_path == tmp_path / "projects" / "ser-proj"
        for frame in result["frames"]:
            assert (project_path / frame["path"]).is_file()
    # every sub-frame the mock stacked across both Jobs was downloaded+stored
    assert len(mock_scope.list_frames("sub")) == total
    _assert_no_held_connections(mock_scope)
