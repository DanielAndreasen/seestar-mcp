"""Scope-queue Jobs: the worker-side tasks that touch the Link (issue #7).

ADR-0004: Jobs that touch the Link (``goto``, ``park``, ``capture``) live
under ``seestar_mcp.jobs.scope.*`` so the existing route
(``task_routes = {"seestar_mcp.jobs.scope.*": {"queue": "scope"}}``) sends
them to the Scope queue with exactly one consumer — only one Job talks to
the Scope at a time. Each task opens a **short-lived, per-job Link**
(:meth:`seestar_controller.link.device.Device.connect` ... ``disconnect``):
the MCP server process never holds a connection, and neither does an idle
worker.

- ``goto``   — slew to RA/Dec (name resolved server-side by the Tool) and
  wait for arrival (``Device.goto`` waits for the pushed ``ScopeGoto``
  "complete" event).
- ``park``   — park the Scope.
- ``capture`` — stack ``frames`` sub-frames with the given exposure/gain,
  download them via :mod:`seestar_controller.link.frames` and store them in
  the seestar-controller Project (``SEESTAR_PROJECT_ROOT``) under the open
  session, returning the stored frame ids/paths.

The scope address (``host``/``port``) is resolved by the submitting Tool
(per-call ``ip`` > address pinned by ``connect`` > ``SEESTAR_MCP_SCOPE_IP``)
and passed in the Job kwargs — the worker is a separate process and cannot
see the server's pinned state. The signed-handshake key resolves in the
worker as usual (``SEESTAR_KEY_PATH`` env var).
"""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from typing import Any

from seestar_controller.link import frames as link_frames
from seestar_controller.link.device import Device
from seestar_controller.store import frames as store_frames
from seestar_controller.store import project as store_project
from seestar_controller.store import sessions as store_sessions

from seestar_mcp.jobs.celery_app import app
from seestar_mcp.jobs.tasks import JobTask

#: Task names — the ``seestar_mcp.jobs.scope.`` prefix routes to the Scope
#: queue (ADR-0004), where exactly one consumer serializes Link access.
GOTO_TASK = "seestar_mcp.jobs.scope.goto"
PARK_TASK = "seestar_mcp.jobs.scope.park"
CAPTURE_TASK = "seestar_mcp.jobs.scope.capture"

#: Default Project display name for capture Jobs (slugged under
#: ``SEESTAR_PROJECT_ROOT``).
DEFAULT_PROJECT_NAME = "seestar-mcp"

#: Link timeouts for the per-job connection: a generous request timeout so
#: large frame downloads cannot be cut short, no auto-reconnect (a dead
#: Link must fail the Job fast instead of retrying in the background).
LINK_CONNECT_TIMEOUT = 5.0
LINK_REQUEST_TIMEOUT = 60.0
#: How long ``goto`` waits for the arrival event (Device default is 30 s).
GOTO_TIMEOUT = 60.0
#: Poll interval while waiting for stacked sub-frames to arrive.
CAPTURE_POLL_INTERVAL_S = 0.1
#: Extra seconds per requested sub-frame on top of its exposure when no
#: explicit capture timeout is given.
CAPTURE_SLACK_PER_FRAME_S = 5.0
CAPTURE_SLACK_FIXED_S = 30.0
#: Per-request timeout for the frame downloads of one capture Job.
DOWNLOAD_TIMEOUT_S = 60.0


def _open_link(host: str, port: int) -> Device:
    """Open the short-lived per-job Link to the Scope (ADR-0004).

    ``reconnect=None`` disables auto-reconnect: a scope Job is a one-shot
    operation, so a dropped Link must fail the Job, not retry forever.
    """
    return Device.connect(
        host,
        port,
        reconnect=None,
        connect_timeout=LINK_CONNECT_TIMEOUT,
        request_timeout=LINK_REQUEST_TIMEOUT,
        goto_timeout=GOTO_TIMEOUT,
    )


def _open_or_create_project(name: str) -> store_project.Project:
    """Open the Project ``name`` under ``SEESTAR_PROJECT_ROOT``, creating it.

    The project root resolves via seestar-controller's
    :func:`~seestar_controller.store.project.default_project_root`
    (``SEESTAR_PROJECT_ROOT`` env var > ``~/SeestarProjects/``).
    """
    root = store_project.default_project_root()
    path = root / store_project.slugify(name)
    if (path / store_project.PROJECT_FILE).is_file():
        return store_project.open_project(path)
    return store_project.create_project(name, root)


def _session_id(project: store_project.Project) -> str:
    """The project's open session, or a newly started one."""
    open_session = store_sessions.get_open_session(project)
    if open_session is not None:
        return open_session.id
    return store_sessions.start_session(project)


def _state_payload(state: Any) -> dict[str, Any]:
    """A JSON-serializable snapshot of a Device state."""
    return {
        "mode": state.mode,
        "ra": state.ra,
        "dec": state.dec,
        "target_name": state.target_name,
        "slewing": state.slewing,
        "parked": state.parked,
        "stacking_active": state.stacking_active,
        "stacking": {
            "state": state.stacking.state,
            "lapse_ms": state.stacking.lapse_ms,
            "frames": state.stacking.frames,
        },
    }


@app.task(name=GOTO_TASK, base=JobTask)
def goto(
    host: str,
    port: int,
    *,
    ra: float,
    dec: float,
    name: str | None = None,
    timeout: float | None = None,
) -> dict[str, Any]:
    """Scope Job: slew to ``ra`` (hours) / ``dec`` (degrees) and wait for arrival.

    Opens a per-job Link (ADR-0004), issues ``Device.goto`` — which waits
    for the pushed ``ScopeGoto`` "complete" event — and returns the state
    after arrival. ``name`` is the target's display name (resolved from the
    catalog by the submitting Tool when the caller passed a target name).
    """
    with _open_link(host, port) as device:
        state = device.goto(ra, dec, name=name, timeout=timeout)
        return {
            "address": f"{host}:{port}",
            "requested": {"ra": float(ra), "dec": float(dec), "name": name},
            **_state_payload(state),
        }


@app.task(name=PARK_TASK, base=JobTask)
def park(host: str, port: int) -> dict[str, Any]:
    """Scope Job: park the Scope. Opens a per-job Link (ADR-0004)."""
    with _open_link(host, port) as device:
        state = device.park()
        return {
            "address": f"{host}:{port}",
            **_state_payload(state),
        }


@app.task(name=CAPTURE_TASK, base=JobTask)
def capture(
    host: str,
    port: int,
    *,
    project: str = DEFAULT_PROJECT_NAME,
    frames: int = 1,
    exposure: float = 10.0,
    gain: int = 80,
    timeout: float | None = None,
) -> dict[str, Any]:
    """Scope Job: capture ``frames`` sub-frames and store them in the Project.

    Opens a per-job Link (ADR-0004), starts a Stack with ``exposure`` /
    ``gain``, waits until ``frames`` sub-frames have been stacked (or the
    Stack ends / the timeout hits), stops the Stack, downloads the new
    sub-frames via :func:`seestar_controller.link.frames.download_frames`
    and stores each FITS into the Project (``SEESTAR_PROJECT_ROOT``) under
    the open session. Returns the stored frame ids/paths — small and JSON-
    serializable; the FITS stay on disk in the Project.
    """
    frames = int(frames)
    if frames < 1:
        raise ValueError(f"frames must be >= 1, got {frames}")
    exposure = float(exposure)
    gain = int(gain)
    project_handle = _open_or_create_project(project)
    try:
        session_id = _session_id(project_handle)
        with _open_link(host, port) as device:
            started_epoch = time.time()
            device.start_stack(exposure, gain)
            deadline = time.monotonic() + (
                timeout
                if timeout is not None
                else frames * (exposure + CAPTURE_SLACK_PER_FRAME_S)
                + CAPTURE_SLACK_FIXED_S
            )
            while True:
                state = device.state()
                if state.stacking.frames >= frames or not state.stacking_active:
                    break
                if time.monotonic() >= deadline:
                    break
                time.sleep(CAPTURE_POLL_INTERVAL_S)
            final = (
                device.stop_stack() if device.state().stacking_active else device.state()
            )
            with tempfile.TemporaryDirectory(prefix="seestar-mcp-capture-") as staging:
                downloaded = device.download_frames(
                    staging,
                    link_frames.KIND_SUB,
                    since=started_epoch,
                    timeout=DOWNLOAD_TIMEOUT_S,
                )
                stored = _store_downloaded(project_handle, session_id, downloaded)
        if not stored:
            raise RuntimeError(
                f"capture stored no sub-frames (scope at {host}:{port} reported "
                f"{final.stacking.frames} stacked frames)"
            )
        return {
            "address": f"{host}:{port}",
            "project": project_handle.slug,
            "project_path": str(project_handle.path),
            "session_id": session_id,
            "requested": {"frames": frames, "exposure": exposure, "gain": gain},
            "stacking": {
                "state": final.stacking.state,
                "lapse_ms": final.stacking.lapse_ms,
                "frames": final.stacking.frames,
            },
            "count": len(stored),
            "frames": stored,
        }
    finally:
        project_handle.close()


def _store_downloaded(
    project: store_project.Project,
    session_id: str,
    downloaded: list[link_frames.DownloadedFrame],
) -> list[dict[str, Any]]:
    """Store downloaded sub-frames in the Project; return their payloads."""
    stored: list[dict[str, Any]] = []
    for frame in downloaded:
        fits_bytes = Path(frame.path).read_bytes()
        meta: dict[str, Any] | None = None
        sidecar = Path(frame.sidecar) if frame.sidecar is not None else None
        if sidecar is not None and sidecar.is_file():
            meta = json.loads(sidecar.read_text(encoding="utf-8"))
        record = store_frames.store_frame(
            project,
            session_id,
            store_frames.normalize_kind(link_frames.KIND_SUB),
            fits_bytes,
            meta=meta,
        )
        stored.append(
            {
                "id": record.id,
                "path": record.path,
                "kind": record.kind,
                "utc": record.utc,
                "exposure": record.exposure,
                "gain": record.gain,
                "target": record.target,
            }
        )
    return stored


__all__ = [
    "CAPTURE_TASK",
    "DEFAULT_PROJECT_NAME",
    "GOTO_TASK",
    "PARK_TASK",
    "capture",
    "goto",
    "park",
]
