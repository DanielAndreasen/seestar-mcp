"""Read-only Link Tools: ``connect`` and ``status`` (issue #6).

Spec user stories 1/2/21: ``connect`` verifies the Scope is reachable and
pins its address for later Tools; ``status`` reads the Scope's current state
(connection state, pointing / RA-DEC, stacking) without side effects.

ADR-0004: both Tools open a **short-lived** Link connection (connect, read,
disconnect) — the MCP server process never holds a connection to the Scope.
The pinned scope address lives in server-side state only.

Configuration: the scope address comes from ``SEESTAR_MCP_SCOPE_IP`` (default
``192.168.87.185`` — the Seestar S50 in station mode, live-probed in
seestar-controller's link docs) and may carry an explicit command-port as
``host:port`` (default port 4700). ``connect`` takes a per-call ``ip``
override and pins the address it was called with, so later Tools (``status``
and, in later tickets, the scope Jobs) use it.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Any

from mcp.server.fastmcp import FastMCP
from seestar_controller.link.device import Device, DeviceState
from seestar_controller.link.transport import COMMAND_PORT

#: Environment variable holding the scope address (``host`` or ``host:port``).
SCOPE_IP_ENV_VAR = "SEESTAR_MCP_SCOPE_IP"
#: Default scope address: the Seestar S50 in station mode on the home LAN.
DEFAULT_SCOPE_IP = "192.168.87.185"

#: How long a short-lived Link may take to open / answer (seconds).
CONNECT_TIMEOUT = 5.0
REQUEST_TIMEOUT = 10.0


@dataclass(frozen=True)
class ScopeAddress:
    """A scope address: host plus command-channel port."""

    host: str
    port: int = COMMAND_PORT

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


def _parse_address(value: str) -> ScopeAddress:
    """Parse ``host`` or ``host:port`` into a :class:`ScopeAddress`."""
    text = value.strip()
    if not text:
        raise ValueError("scope address must not be empty")
    if ":" in text:
        host, _, port_text = text.rpartition(":")
        if not host:
            raise ValueError(f"invalid scope address {value!r}: empty host")
        try:
            port = int(port_text)
        except ValueError:
            raise ValueError(
                f"invalid scope address {value!r}: port {port_text!r} is not a number"
            ) from None
        if not 0 < port < 65536:
            raise ValueError(
                f"invalid scope address {value!r}: port {port} out of range"
            )
        return ScopeAddress(host, port)
    return ScopeAddress(text)


def _configured_address() -> ScopeAddress:
    """The address from ``SEESTAR_MCP_SCOPE_IP`` (or the default)."""
    return _parse_address(os.environ.get(SCOPE_IP_ENV_VAR) or DEFAULT_SCOPE_IP)


_pinned_lock = threading.Lock()
_pinned: ScopeAddress | None = None


def _resolve_address(ip: str | None) -> ScopeAddress:
    """Per-call ``ip`` > address pinned by ``connect`` > ``SEESTAR_MCP_SCOPE_IP``."""
    if ip:
        return _parse_address(ip)
    with _pinned_lock:
        if _pinned is not None:
            return _pinned
    return _configured_address()


def _pin(address: ScopeAddress) -> None:
    """Pin the scope address in server-side state for later Tools."""
    global _pinned
    with _pinned_lock:
        _pinned = address


def _open_short_lived(address: ScopeAddress) -> Device:
    """Open a short-lived Link to the Scope (ADR-0004: never held open).

    ``reconnect=None`` disables auto-reconnect: these Tools are one-shot
    reads, so a dead Link must fail fast instead of retrying in the
    background. The signed-handshake key resolves exactly as in
    seestar-controller (``SEESTAR_KEY_PATH`` env var > the default config
    path).
    """
    return Device.connect(
        address.host,
        address.port,
        reconnect=None,
        connect_timeout=CONNECT_TIMEOUT,
        request_timeout=REQUEST_TIMEOUT,
    )


def _target_payload(state: DeviceState) -> dict[str, Any] | None:
    """The current target as a JSON payload (RA/Dec in hours/degrees + name)."""
    if state.target is None:
        return None
    return {"ra": state.ra, "dec": state.dec, "name": state.target_name}


def register(mcp: FastMCP) -> None:
    """Register the read-only Link Tools on the FastMCP app."""

    @mcp.tool()
    def connect(ip: str | None = None) -> dict[str, Any]:
        """Verify the Scope is reachable and pin its address for later Tools.

        Opens a short-lived Link connection to the scope address — the ``ip``
        argument (``host`` or ``host:port``), else the address pinned by a
        previous ``connect``, else ``SEESTAR_MCP_SCOPE_IP`` (default
        192.168.87.185) — reads basic identity, and closes the connection
        again (ADR-0004: the server never holds a connection to the Scope).

        The address this call used is pinned as the server-side default for
        later Tools (``status``, and the scope Jobs of later tickets), even
        when the Scope turns out to be unreachable. Returns ``reachable``
        plus identity on success (address, connection state, signed-
        handshake ``verified``, current mode and target), or
        ``reachable: false`` with an ``error`` when the Scope does not
        answer.
        """
        address = _resolve_address(ip)
        _pin(address)
        try:
            device = _open_short_lived(address)
        except Exception as exc:  # noqa: BLE001 - any Link failure means "not reachable"
            return {
                "reachable": False,
                "address": str(address),
                "error": f"{type(exc).__name__}: {exc}",
            }
        try:
            state = device.state()
            return {
                "reachable": True,
                "address": str(address),
                "connection_state": str(device.connection_state),
                "verified": device.transport.verified,
                "mode": state.mode,
                "target": _target_payload(state),
            }
        except Exception as exc:  # noqa: BLE001 - report, don't crash the Tool
            return {
                "reachable": True,
                "address": str(address),
                "connection_state": str(device.connection_state),
                "error": f"{type(exc).__name__}: {exc}",
            }
        finally:
            device.disconnect()

    @mcp.tool()
    def status(ip: str | None = None) -> dict[str, Any]:
        """Read the Scope's current state — read-only, no side effects.

        Opens a short-lived Link connection (ADR-0004), reads the device
        state, and closes the connection again. Address resolution is the
        same as for ``connect`` (per-call ``ip`` > address pinned by
        ``connect`` > ``SEESTAR_MCP_SCOPE_IP``); unlike ``connect``, a
        per-call ``ip`` here does NOT re-pin the address.

        Returns the connection state, mode, current target (RA/Dec + name)
        and stacking progress. Raises a tool error when the Scope is
        unreachable.
        """
        address = _resolve_address(ip)
        try:
            device = _open_short_lived(address)
        except Exception as exc:  # any Link failure is "unreachable" (re-raised below)
            raise RuntimeError(
                f"scope at {address} unreachable: {type(exc).__name__}: {exc}"
            ) from exc
        try:
            state = device.state()
            return {
                "address": str(address),
                "connection_state": str(device.connection_state),
                "verified": device.transport.verified,
                "mode": state.mode,
                "target": _target_payload(state),
                "stacking": {
                    "state": state.stacking.state,
                    "lapse_ms": state.stacking.lapse_ms,
                    "frames": state.stacking.frames,
                },
                "slewing": state.slewing,
                "parked": state.parked,
                "stacking_active": state.stacking_active,
            }
        finally:
            device.disconnect()


__all__ = ["register"]
