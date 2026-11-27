"""Compute Tools: ``solve_field`` and ``photometry_run`` (issue #8).

Spec user stories 7/8/11/20: plate-solving a downloaded frame and running
photometry over a session's sub-frames are long-running, so both Tools submit
a **compute-queue Job** (ADR-0004: parallel workers, never the Link) and
return the Idempotency key right away; the agent polls ``job_status`` and
fetches the payload with ``job_result``.

- ``solve_field`` — plate-solve one frame: either a **frame of the Project**
  (``project`` + ``frame_id``, optionally scoped by ``session``) — the worker
  persists the solved WCS in the project db (``frame_wcs``) and the FITS
  header — or a plain **FITS path** (``fits_path``). Solver tuning
  (``solver``, hints, tolerances, offline ``gaia_catalog``) is forwarded to
  seestar-controller's pure :mod:`seestar_controller.astrometry.solve`.
- ``photometry_run`` — run aperture photometry over all sub-frames of
  ``session`` of ``project`` via the pure
  :class:`~seestar_controller.photometry.run.RunController`, storing one
  Measurement per sub-frame in the Project. Picks come from the project's
  stored star picks, or from the optional ``picks`` argument (saved into the
  project by the worker first).

Every submission takes a client-chosen ``idempotency_key`` (GLOSSARY.md),
which becomes the Celery task id — resubmitting the same key refers to the
same Job. Both Jobs are read-only towards the Scope (no Link), so they route
to the Compute queue (the default queue); the Tool passes ``queue=compute``
explicitly.

The Project root is resolved server-side (``SEESTAR_PROJECT_ROOT`` >
``~/SeestarProjects/``, via
:func:`seestar_controller.store.project.default_project_root`) and passed to
the worker in the Job kwargs (``project_root``), so a worker executes the Job
against the same Project the server validated — worker processes cannot see
the server's environment. ``fits_path`` / ``gaia_catalog`` must be readable
by the worker (a shared filesystem path).
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from seestar_controller.astrometry.solve import SOLVER_AUTO, SOLVERS
from seestar_controller.photometry.picking import ApertureParams
from seestar_controller.store import project as store_project
from seestar_controller.store.stars import ROLE_CHECK, ROLE_COMP, ROLE_TARGET, ROLES

from seestar_mcp.jobs.celery_app import COMPUTE_QUEUE
from seestar_mcp.jobs.compute import PHOTOMETRY_RUN_TASK, SOLVE_FIELD_TASK
from seestar_mcp.jobs.submit import submit_job

#: Aperture keys accepted in the ``aperture`` argument of ``photometry_run``.
_APERTURE_FIELDS = ("radius_px", "sky_in_px", "sky_out_px")


def _resolve_project(name: str) -> tuple[Path, str]:
    """Resolve a project ``name`` to ``(project_root, slug)``, failing fast.

    The project must already exist under the root (compute Jobs process
    existing data; ``capture`` creates projects) — the Tool raises before
    submitting anything when it does not.
    """
    root = store_project.default_project_root()
    slug = store_project.slugify(name)
    path = root / slug
    if not (path / store_project.PROJECT_FILE).is_file():
        raise ValueError(
            f"no project {name!r} under {root} — looked for "
            f"{path / store_project.PROJECT_FILE}"
        )
    return root, slug


def _existing_file(value: str, *, what: str) -> str:
    """Validate that ``value`` is an existing file; return its absolute path."""
    path = Path(value)
    if not path.is_file():
        raise ValueError(f"{what} {value!r} is not an existing file")
    return str(path.resolve())


def _check_center_hint(center_hint: list[float] | None) -> list[float] | None:
    """Validate ``center_hint`` = ``[ra_deg, dec_deg]`` (fail fast in the Tool)."""
    if center_hint is None:
        return None
    if (
        not isinstance(center_hint, (list, tuple))
        or len(center_hint) != 2
        or any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in center_hint)
    ):
        raise ValueError(
            f"center_hint must be a [ra_deg, dec_deg] pair of numbers, got {center_hint!r}"
        )
    ra, dec = float(center_hint[0]), float(center_hint[1])
    if not math.isfinite(ra) or not math.isfinite(dec) or not -90.0 <= dec <= 90.0:
        raise ValueError(f"center_hint must be finite with dec in [-90, 90], got {center_hint!r}")
    return [ra % 360.0, dec]


def _check_picks(picks: list[dict[str, Any]]) -> None:
    """Structural validation of the ``picks`` argument (fail fast in the Tool).

    Mirrors the ensemble rules of the measure layer (exactly one ``target``,
    at least one ``comp``, at most one ``check``) and the per-pick shape
    (role + numeric x/y); the worker's :class:`StarPick` re-validates values.
    """
    if not isinstance(picks, list) or not picks:
        raise ValueError("picks must be a non-empty list of pick objects")
    roles: list[str] = []
    for pick in picks:
        if not isinstance(pick, dict):
            raise TypeError(f"each pick must be an object, got {pick!r}")
        role = pick.get("role")
        if role not in ROLES:
            raise ValueError(f"pick role must be one of {list(ROLES)}, got {role!r}")
        for coord in ("x", "y"):
            value = pick.get(coord)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TypeError(
                    f"pick {pick!r} needs a numeric {coord!r} (pixel position)"
                )
            if not math.isfinite(float(value)):
                raise ValueError(f"pick {pick!r} has a non-finite {coord!r}")
        roles.append(str(role))
    if roles.count(ROLE_TARGET) != 1:
        raise ValueError(
            f"picks must contain exactly one {ROLE_TARGET!r} pick, got "
            f"{roles.count(ROLE_TARGET)}"
        )
    if roles.count(ROLE_COMP) < 1:
        raise ValueError("picks need at least one 'comp' (comparison star) pick")
    if roles.count(ROLE_CHECK) > 1:
        raise ValueError("picks may contain at most one 'check' pick")


def _check_aperture(aperture: dict[str, Any] | None) -> None:
    """Validate the ``aperture`` argument (fail fast via ApertureParams)."""
    if aperture is None:
        return
    if not isinstance(aperture, dict):
        raise TypeError(f"aperture must be an object, got {aperture!r}")
    unknown = set(aperture) - set(_APERTURE_FIELDS)
    if unknown:
        raise ValueError(
            f"unknown aperture keys {sorted(unknown)} — allowed: {list(_APERTURE_FIELDS)}"
        )
    ApertureParams(**{k: aperture[k] for k in _APERTURE_FIELDS if k in aperture})


def register(mcp: FastMCP) -> None:
    """Register the compute Tools on the FastMCP app."""

    @mcp.tool()
    def solve_field(
        idempotency_key: str,
        project: str | None = None,
        session: str | None = None,
        frame_id: int | None = None,
        fits_path: str | None = None,
        solver: str = SOLVER_AUTO,
        center_hint: list[float] | None = None,
        pixel_scale_hint: float | None = None,
        gaia_catalog: str | None = None,
        match_radius_arcsec: float | None = None,
        min_matches: int | None = None,
        rms_tolerance_arcsec: float | None = None,
        solve_field_timeout_s: float | None = None,
    ) -> dict[str, Any]:
        """Plate-solve a frame — a compute-queue Job (never touches the Link).

        Submits a Job on the Compute queue under the given ``idempotency_key``
        (resubmitting the same key refers to the same Job); poll with
        ``job_status`` / ``job_result``, which returns the WCS solution (solver,
        field center, matched-star count, RMS, and the WCS as FITS cards) plus
        the frame's paths in the Project.

        The frame is either a **frame of the Project** — ``project`` +
        ``frame_id`` (with optional ``session`` to scope the lookup; the worker
        persists the solved WCS in the project db and the FITS header) — or a
        plain **FITS file** at ``fits_path`` (nothing written back); never both.
        ``solver`` is ``"auto"`` (local ``solve-field`` binary when on PATH,
        else the Gaia pattern match), ``"solve-field"`` or ``"gaia"``;
        ``center_hint`` is ``[ra_deg, dec_deg]``, ``pixel_scale_hint`` arcsec/px,
        ``gaia_catalog`` a JSON file of Gaia stars replacing the network query,
        and the remaining options tune the solve. The Project root resolves
        via ``SEESTAR_PROJECT_ROOT``; ``fits_path`` / ``gaia_catalog`` must be
        readable by the worker (shared filesystem path).
        """
        if solver not in SOLVERS:
            raise ValueError(f"solver must be one of {list(SOLVERS)}, got {solver!r}")
        if fits_path is not None and (project is not None or frame_id is not None):
            raise ValueError(
                "pass either fits_path or project + frame_id, not both "
                f"(got fits_path={fits_path!r}, project={project!r}, frame_id={frame_id!r})"
            )
        if project is not None and frame_id is None:
            raise ValueError(
                f"solve_field with project={project!r} needs frame_id (the frame to "
                "solve in the Project) — or pass fits_path for a plain FITS file"
            )
        if frame_id is not None and project is None:
            raise ValueError("frame_id needs project — a frame id only exists inside a Project")
        if fits_path is None and project is None:
            raise ValueError("give either fits_path or project + frame_id")
        if frame_id is not None and (isinstance(frame_id, bool) or not isinstance(frame_id, int)):
            raise ValueError(f"frame_id must be an int, got {frame_id!r}")
        checked_hint = _check_center_hint(center_hint)
        if pixel_scale_hint is not None:
            pixel_scale_hint = float(pixel_scale_hint)
            if not math.isfinite(pixel_scale_hint) or pixel_scale_hint <= 0:
                raise ValueError(f"pixel_scale_hint must be a finite number > 0, got {pixel_scale_hint!r}")

        kwargs: dict[str, Any] = {"solver": solver}
        if fits_path is not None:
            kwargs["fits_path"] = _existing_file(fits_path, what="fits_path")
        else:
            root, _slug = _resolve_project(str(project))
            kwargs["project"] = project
            kwargs["project_root"] = str(root)
            kwargs["frame_id"] = frame_id
            if session is not None:
                if not isinstance(session, str) or not session:
                    raise ValueError(f"session must be a non-empty string, got {session!r}")
                kwargs["session"] = session
        if checked_hint is not None:
            kwargs["center_hint"] = checked_hint
        if pixel_scale_hint is not None:
            kwargs["pixel_scale_hint"] = pixel_scale_hint
        if gaia_catalog is not None:
            kwargs["gaia_catalog"] = _existing_file(gaia_catalog, what="gaia_catalog")
        for name, value in (
            ("match_radius_arcsec", match_radius_arcsec),
            ("min_matches", min_matches),
            ("rms_tolerance_arcsec", rms_tolerance_arcsec),
            ("solve_field_timeout_s", solve_field_timeout_s),
        ):
            if value is not None:
                kwargs[name] = value

        key = submit_job(idempotency_key, SOLVE_FIELD_TASK, kwargs=kwargs, queue=COMPUTE_QUEUE)
        return {
            "idempotency_key": key,
            "task": SOLVE_FIELD_TASK,
            "queue": COMPUTE_QUEUE,
            "solve_field": {
                "project": str(project) if project is not None else None,
                "session": session,
                "frame_id": frame_id,
                "fits_path": fits_path,
                "solver": solver,
            },
        }

    @mcp.tool()
    def photometry_run(
        idempotency_key: str,
        project: str,
        session: str,
        picks: list[dict[str, Any]] | None = None,
        aperture: dict[str, Any] | None = None,
        excluded_frames: list[int] | None = None,
    ) -> dict[str, Any]:
        """Run aperture photometry over a session's sub-frames — a compute-queue Job.

        Submits a Job on the Compute queue under the given ``idempotency_key``
        (resubmitting the same key refers to the same Job). The worker runs
        seestar-controller's photometry RunController over ALL sub-frames of
        ``session`` of ``project`` and stores one Measurement per measured
        sub-frame in the Project db (the GUI lightcurve sees them too);
        ``job_result`` returns the summary (measured/skipped/excluded counts
        plus the measurement rows: jd, mag, mag_err) — never raw FITS.

        The star picks come from the project's stored picks, or from the
        optional ``picks`` argument — a list of pick objects with ``role``
        (``"target"`` / ``"comp"`` / ``"check"``), ``x`` / ``y`` (pixels) and
        optional ``label``, ``ra_deg`` / ``dec_deg`` and per-pick aperture
        fields — which the worker saves as the project's picks first (exactly
        one target and at least one comp are required). ``aperture`` sets the
        default aperture ``{"radius_px", "sky_in_px", "sky_out_px"}`` for
        picks without their own. ``excluded_frames`` optionally skips the
        given sub-frame ids. The Project root resolves via
        ``SEESTAR_PROJECT_ROOT``.
        """
        if not isinstance(session, str) or not session:
            raise ValueError(f"session must be a non-empty string, got {session!r}")
        _check_aperture(aperture)
        if picks is not None:
            _check_picks(picks)
        if excluded_frames is not None and (
            not isinstance(excluded_frames, list)
            or any(isinstance(f, bool) or not isinstance(f, int) for f in excluded_frames)
        ):
            raise TypeError(
                f"excluded_frames must be a list of frame ids (ints), got {excluded_frames!r}"
            )
        root, slug = _resolve_project(project)

        kwargs: dict[str, Any] = {
            "project": project,
            "project_root": str(root),
            "session": session,
        }
        if picks is not None:
            kwargs["picks"] = picks
        if aperture is not None:
            kwargs["aperture"] = aperture
        if excluded_frames is not None:
            kwargs["excluded_frames"] = excluded_frames

        key = submit_job(
            idempotency_key, PHOTOMETRY_RUN_TASK, kwargs=kwargs, queue=COMPUTE_QUEUE
        )
        return {
            "idempotency_key": key,
            "task": PHOTOMETRY_RUN_TASK,
            "queue": COMPUTE_QUEUE,
            "photometry_run": {
                "project": slug,
                "session": session,
                "picks": len(picks) if picks is not None else None,
                "excluded_frames": excluded_frames,
            },
        }


__all__ = ["register"]
