"""Tests for the read-only Link Tools ``connect`` / ``status`` (issue #6).

Every test drives the Tools through the real MCP layer (stdio
``ClientSession``, like ``test_job_tools.py``) against
``seestar_controller.link.mock_scope`` running IN THIS process on ephemeral
ports — no redis, no hardware, so these tests always run (no skip). The MCP
server subprocess gets a generated test key via ``SEESTAR_KEY_PATH`` (the
mock verifies the PEM-signed handshake) and is pointed at the mock either via
``SEESTAR_MCP_SCOPE_IP`` or the per-call ``ip`` override (``host:port``).

Acceptance criteria of issue #6: ``connect`` succeeds against the mock and
fails against a dead port; ``status`` returns the mock Scope's state without
side effects.
"""

import asyncio
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from seestar_controller.link.mock_scope import MockScope

from seestar_mcp.server import mcp

# --- helpers ----------------------------------------------------------------


def _payload(res: Any) -> Any:
    """Normalize a tool result to its payload (see test_job_tools.py)."""
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


def _error_text(res: Any) -> str:
    """The text of a CallToolResult's first content block (error messages)."""
    return res.content[0].text if res.content else ""


def _server_params(
    scope_key_path: Path, extra_env: dict[str, str] | None = None
) -> StdioServerParameters:
    """Stdio params spawning the real MCP server with a test key configured.

    ``SEESTAR_MCP_SCOPE_IP`` is dropped from the environment unless given in
    ``extra_env``, so the per-call ``ip`` override is exercised by default.
    """
    env = {
        **os.environ,
        "SEESTAR_PROTOCOL_LOG": "0",
        "SEESTAR_KEY_PATH": str(scope_key_path),
    }
    env.pop("SEESTAR_MCP_SCOPE_IP", None)
    env.update(extra_env or {})
    return StdioServerParameters(
        command=sys.executable, args=["-m", "seestar_mcp"], env=env
    )


def _free_port() -> int:
    """A port that is free right now (nothing listening -> 'dead' address)."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _assert_no_held_connections(mock_scope: MockScope) -> None:
    """ADR-0004: after the Tools returned, the mock sees no client sockets."""
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and mock_scope.client_count != 0:
        time.sleep(0.05)
    assert mock_scope.client_count == 0


# --- Tool registration (no mock needed) --------------------------------------


def test_link_tools_registered_on_app() -> None:
    """connect and status are Tools on the FastMCP app; ``ip`` is optional."""
    tools = {t.name: t for t in asyncio.run(mcp.list_tools())}
    assert {"connect", "status"} <= tools.keys()
    for name in ("connect", "status"):
        schema = tools[name].inputSchema
        ip_schema = schema["properties"]["ip"]
        # optional per-call override: nullable string, not required
        assert any(
            variant.get("type") == "string" for variant in ip_schema["anyOf"]
        )
        assert "ip" not in schema.get("required", [])


# --- MCP-over-stdio against the in-process mock Scope ------------------------


def test_connect_succeeds_against_mock_scope_and_pins_its_address(
    mock_scope: MockScope, scope_key_path: Path
) -> None:
    """connect against the mock: reachable + identity, and the address is pinned.

    After ``connect(ip=...)``, ``status`` with no arguments resolves to the
    pinned address (not the default), proving the server-side pin.
    """
    address = f"127.0.0.1:{mock_scope.command_port}"

    async def run() -> None:
        async with (
            stdio_client(_server_params(scope_key_path)) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            names = {t.name for t in (await session.list_tools()).tools}
            assert {"connect", "status"} <= names

            res = await session.call_tool("connect", {"ip": address})
            assert not res.isError
            payload = _payload(res)
            assert payload["reachable"] is True
            assert payload["address"] == address
            assert payload["connection_state"] == "connected"
            assert payload["verified"] is True  # PEM-signed handshake ok
            assert payload["mode"] == "idle"
            assert payload["target"] is None

            # connect pinned the address: status with no args uses it.
            res = await session.call_tool("status", {})
            assert not res.isError
            status = _payload(res)
            assert status["address"] == address
            assert status["connection_state"] == "connected"
            assert status["verified"] is True
            assert status["mode"] == "idle"
            assert status["target"] is None
            assert status["stacking"] == {
                "state": "idle",
                "lapse_ms": 0,
                "frames": 0,
            }
            assert status["slewing"] is False
            assert status["parked"] is False
            assert status["stacking_active"] is False

    asyncio.run(run())
    _assert_no_held_connections(mock_scope)  # short-lived Links, closed


def test_connect_fails_against_dead_port(scope_key_path: Path) -> None:
    """connect against a dead port: reachable=false with error, not a crash.

    The failed connect still pins the address it was called with (ADR-0004:
    connect pins the scope address), so a following ``status`` without
    arguments targets the dead address and raises a tool error.
    """
    dead = f"127.0.0.1:{_free_port()}"

    async def run() -> None:
        async with (
            stdio_client(_server_params(scope_key_path)) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            res = await session.call_tool("connect", {"ip": dead})
            assert not res.isError  # reachability is an answer, not a tool error
            payload = _payload(res)
            assert payload["reachable"] is False
            assert payload["address"] == dead
            assert payload["error"]

            # pinned to the dead address by the connect call above
            res = await session.call_tool("status", {})
            assert res.isError
            assert "unreachable" in _error_text(res)

    asyncio.run(run())


def test_status_returns_mock_state_without_side_effects(
    mock_scope: MockScope, scope_key_path: Path
) -> None:
    """status returns the mock Scope's state (pointing, stacking, connection).

    Uses the per-call ``ip`` override (which does NOT re-pin). The mock's
    device state is identical before and after a ``status`` call: read-only.
    """
    address = f"127.0.0.1:{mock_scope.command_port}"
    mock_scope.set_stacking_progress("working", lapse_ms=42000, frames=4)

    async def run() -> tuple[Any, Any]:
        async with (
            stdio_client(_server_params(scope_key_path)) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            # start a simulated slew from the test process, then read state
            # through the MCP layer while it runs
            mock_scope.slew_duration = 0.8
            mock_scope.begin_goto(3.25, -15.5, name="M 31")
            res = await session.call_tool("status", {"ip": address})
            assert not res.isError
            slewing = _payload(res)
            # wait out the simulated slew (mode -> idle, target stays)
            deadline = time.monotonic() + 5.0
            while (
                time.monotonic() < deadline
                and mock_scope.scope_state()["mode"] != "idle"
            ):
                await asyncio.sleep(0.05)
            before = mock_scope.scope_state()
            res = await session.call_tool("status", {"ip": address})
            assert not res.isError
            status = _payload(res)
            # no side effects: the mock's device state is untouched
            assert mock_scope.scope_state() == before
            return slewing, status

    slewing, status = asyncio.run(run())
    expected_stacking = {"state": "working", "lapse_ms": 42000, "frames": 4}
    expected_target = {"ra": 3.25, "dec": -15.5, "name": "M 31"}
    # while the slew runs: mode "slewing", target already set
    assert slewing["address"] == address
    assert slewing["connection_state"] == "connected"
    assert slewing["mode"] == "slewing"
    assert slewing["slewing"] is True
    assert slewing["target"] == expected_target
    assert slewing["stacking"] == expected_stacking
    # after the slew: mode "idle", pointing (RA/Dec + name) still reported
    assert status["mode"] == "idle"
    assert status["slewing"] is False
    assert status["parked"] is False
    assert status["target"] == expected_target
    assert status["stacking"] == expected_stacking
    _assert_no_held_connections(mock_scope)  # short-lived Link, closed


def test_scope_ip_from_env_configures_the_default_address(
    mock_scope: MockScope, scope_key_path: Path
) -> None:
    """SEESTAR_MCP_SCOPE_IP (host:port) is the default address without a pin."""
    address = f"127.0.0.1:{mock_scope.command_port}"

    async def run() -> None:
        async with (
            stdio_client(
                _server_params(scope_key_path, {"SEESTAR_MCP_SCOPE_IP": address})
            ) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            # no per-call override: the env-configured address is used
            res = await session.call_tool("connect", {})
            assert not res.isError
            payload = _payload(res)
            assert payload["reachable"] is True
            assert payload["address"] == address

            res = await session.call_tool("status", {})
            assert not res.isError
            assert _payload(res)["address"] == address

    asyncio.run(run())
    _assert_no_held_connections(mock_scope)


def test_status_fails_against_dead_port(scope_key_path: Path) -> None:
    """status against a dead port raises a tool error (nothing to report)."""
    dead = f"127.0.0.1:{_free_port()}"

    async def run() -> None:
        async with (
            stdio_client(_server_params(scope_key_path)) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            res = await session.call_tool("status", {"ip": dead})
            assert res.isError
            assert "unreachable" in _error_text(res)

    asyncio.run(run())
