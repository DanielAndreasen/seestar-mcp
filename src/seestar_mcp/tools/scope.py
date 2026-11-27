"""Scope-mutating Tools: ``goto``, ``park`` (Slew-gated, ADR-0003) and
``capture`` (issue #7).

Spec user stories 3/4/5/6/19/21:

- ``goto`` resolves a target name (e.g. ``"M13"``) to J2000 RA/Dec via
  seestar-controller's catalog (SIMBAD, cached + offline-graceful) or
  accepts explicit ``ra``/``dec``, then submits a **scope-queue Job** whose
  worker task slews the Scope on a short-lived per-job Link and waits for
  arrival (ADR-0004).
- ``park`` submits the same kind of scope-queue Job that parks the Scope.
- ``capture`` submits a scope-queue Job that stacks N sub-frames at the
  current pointing and stores the downloaded FITS in the Project.

**Slew gate (ADR-0003)**: ``goto`` and ``park`` raise a clear error unless
the server was started with ``SEESTAR_MCP_ALLOW_SLEW=1`` — the flag is read
once at server start (module import), because MCP tool calls are
agent-initiated and there is no human in the loop to confirm each one.
``capture`` is NOT gated: capturing at the current pointing is harmless.

Every submission takes a client-chosen ``idempotency_key`` (GLOSSARY.md),
which becomes the Celery task id — poll the Job with ``job_status`` /
``job_result``. The scope address resolves per call as for the Link Tools
(``ip`` > address pinned by ``connect`` > ``SEESTAR_MCP_SCOPE_IP``) and is
passed to the worker in the Job kwargs, since worker processes cannot see
the server's pinned state.
"""

from __future__ import annotations

import os
from typing import Any

from mcp.server.fastmcp import FastMCP
from seestar_controller.catalog import facade as catalog_facade
from seestar_controller.catalog.simbad import CatalogError
from seestar_controller.link import stacking

from seestar_mcp.jobs.celery_app import SCOPE_QUEUE
from seestar_mcp.jobs.scope import (
    CAPTURE_TASK,
    DEFAULT_PROJECT_NAME,
    GOTO_TASK,
    PARK_TASK,
)
from seestar_mcp.jobs.submit import submit_job
from seestar_mcp.tools.link import resolve_address

#: Environment variable that arms the Slew gate (ADR-0003).
ALLOW_SLEW_ENV_VAR = "SEESTAR_MCP_ALLOW_SLEW"

#: Whether Slew operations are allowed — read ONCE at server start, so the
#: gate cannot be toggled at runtime by anything but a server restart.
ALLOW_SLEW = os.environ.get(ALLOW_SLEW_ENV_VAR) == "1"


def _require_slew_allowed(tool_name: str) -> None:
    """ADR-0003: refuse a Slew Tool unless the operator opted in at startup."""
    if not ALLOW_SLEW:
        raise RuntimeError(
            f"{tool_name} refused: it moves the Scope (Slew), and this server was "
            f"started without {ALLOW_SLEW_ENV_VAR}=1 (ADR-0003). Restart the server "
            f"with {ALLOW_SLEW_ENV_VAR}=1 to allow goto/park."
        )


def _resolve_target(
    target: str | None, ra: float | None, dec: float | None
) -> tuple[float, float, str | None]:
    """Resolve the goto target to ``(ra_hours, dec_deg, name)``.

    Either a catalog ``target`` name (SIMBAD via
    :func:`seestar_controller.catalog.facade.lookup_name`, brightest match
    first) or explicit ``ra``/``dec`` — never both, never neither.
    """
    if target is not None and (ra is not None or dec is not None):
        raise ValueError("pass either `target` or `ra` + `dec`, not both")
    if target is not None:
        try:
            candidates = catalog_facade.lookup_name(target)
        except (CatalogError, ValueError) as exc:
            raise RuntimeError(
                f"cannot resolve target {target!r} via the catalog (SIMBAD): {exc}"
            ) from exc
        best = candidates[0]  # brightest-first; the Tool reports what it picked
        return best.ra_hours, best.dec_deg, best.name
    if ra is None or dec is None:
        raise ValueError("goto needs a `target` name or both `ra` and `dec`")
    ra_value = float(ra)
    dec_value = float(dec)
    if not 0.0 <= ra_value < 24.0:
        raise ValueError(f"ra must be in [0, 24) hours, got {ra_value}")
    if not -90.0 <= dec_value <= 90.0:
        raise ValueError(f"dec must be in [-90, 90] degrees, got {dec_value}")
    return ra_value, dec_value, None


def _submission_payload(
    idempotency_key: str, task_name: str, address: Any, **extra: Any
) -> dict[str, Any]:
    """The Tool's answer right after submitting: the key + where the Job went."""
    return {
        "idempotency_key": idempotency_key,
        "task": task_name,
        "queue": SCOPE_QUEUE,  # routed by the seestar_mcp.jobs.scope.* prefix
        "address": str(address),
        **extra,
    }


def register(mcp: FastMCP) -> None:
    """Register the scope-mutating Tools on the FastMCP app."""

    @mcp.tool()
    def goto(
        idempotency_key: str,
        target: str | None = None,
        ra: float | None = None,
        dec: float | None = None,
        ip: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Slew the Scope to a target — a scope-queue Job (Slew-gated, ADR-0003).

        Refused unless the server was started with ``SEESTAR_MCP_ALLOW_SLEW=1``.
        The target is either a catalog ``target`` name (e.g. ``"M13"``,
        resolved via SIMBAD to J2000 RA/Dec) or explicit ``ra`` (hours) /
        ``dec`` (degrees). Submits a Job on the Scope queue under the given
        ``idempotency_key`` (resubmitting the same key refers to the same
        Job); the worker task opens a short-lived per-job Link (ADR-0004),
        issues the goto and waits for arrival. Poll with ``job_status`` /
        ``job_result``. ``ip`` overrides the scope address for this call;
        ``timeout`` caps the wait for the slew to finish (seconds).
        """
        _require_slew_allowed("goto")
        ra_hours, dec_deg, name = _resolve_target(target, ra, dec)
        address = resolve_address(ip)
        key = submit_job(
            idempotency_key,
            GOTO_TASK,
            kwargs={
                "host": address.host,
                "port": address.port,
                "ra": ra_hours,
                "dec": dec_deg,
                "name": name,
                "timeout": timeout,
            },
        )
        return _submission_payload(
            key,
            GOTO_TASK,
            address,
            goto={"ra": ra_hours, "dec": dec_deg, "name": name},
        )

    @mcp.tool()
    def park(idempotency_key: str, ip: str | None = None) -> dict[str, Any]:
        """Park the Scope — a scope-queue Job (Slew-gated, ADR-0003).

        Refused unless the server was started with ``SEESTAR_MCP_ALLOW_SLEW=1``.
        Submits a Job on the Scope queue under the given ``idempotency_key``;
        the worker task opens a short-lived per-job Link (ADR-0004) and
        parks the Scope. Poll with ``job_status`` / ``job_result``. ``ip``
        overrides the scope address for this call.
        """
        _require_slew_allowed("park")
        address = resolve_address(ip)
        key = submit_job(
            idempotency_key,
            PARK_TASK,
            kwargs={"host": address.host, "port": address.port},
        )
        return _submission_payload(key, PARK_TASK, address)

    @mcp.tool()
    def capture(
        idempotency_key: str,
        frames: int = 1,
        exposure: float = 10.0,
        gain: int = 80,
        project: str | None = None,
        ip: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Capture sub-frames at the current pointing — a scope-queue Job.

        NOT Slew-gated (ADR-0003: capturing at the current pointing is
        harmless). Submits a Job on the Scope queue under the given
        ``idempotency_key``; the worker task opens a short-lived per-job
        Link (ADR-0004), stacks ``frames`` sub-frames with ``exposure``
        (seconds) / ``gain``, downloads them and stores the FITS in the
        Project ``project`` (default ``seestar-mcp``) under
        ``SEESTAR_PROJECT_ROOT``. ``job_result`` returns the stored frame
        ids/paths. ``ip`` overrides the scope address; ``timeout`` caps the
        whole capture (seconds).
        """
        frames = int(frames)
        if frames < 1:
            raise ValueError(f"frames must be >= 1, got {frames}")
        # fail fast on out-of-range exposure/gain (same validation the
        # Scope applies) instead of failing inside the Job
        stacking.stack_request_params(exposure, gain)
        address = resolve_address(ip)
        project_name = project if project else DEFAULT_PROJECT_NAME
        key = submit_job(
            idempotency_key,
            CAPTURE_TASK,
            kwargs={
                "host": address.host,
                "port": address.port,
                "project": project_name,
                "frames": frames,
                "exposure": float(exposure),
                "gain": int(gain),
                "timeout": timeout,
            },
        )
        return _submission_payload(
            key,
            CAPTURE_TASK,
            address,
            capture={
                "project": project_name,
                "frames": frames,
                "exposure": float(exposure),
                "gain": int(gain),
            },
        )


__all__ = ["ALLOW_SLEW", "ALLOW_SLEW_ENV_VAR", "register"]
