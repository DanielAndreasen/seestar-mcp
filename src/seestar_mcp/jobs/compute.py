"""Compute-queue Jobs: ``solve_field`` and ``photometry_run`` (issue #8).

ADR-0004: Jobs that only process already-downloaded data — plate-solving and
photometry on frames in the Project — never touch the Link, so they live
under ``seestar_mcp.jobs.compute.*``. That prefix does NOT match the
``seestar_mcp.jobs.scope.*`` route, so these tasks go to the **Compute queue**
(the app's default queue, ``compute``), where a worker pool runs them in
parallel while scope access stays serialized on the Scope queue.

- ``solve_field`` — plate-solve one frame and return the WCS solution. The
  frame is either a **frame of the Project** (``project`` + ``frame_id``,
  optionally scoped by ``session``) — solved via
  :func:`seestar_controller.astrometry.session.solve_project_frame`, which
  writes the solved WCS cards back into the stored FITS header AND persists
  the WCS record in the project db (``frame_wcs``, via
  :func:`seestar_controller.store.wcs.save_frame_wcs`) — or a plain **FITS
  path** (``fits_path``), solved via
  :func:`seestar_controller.astrometry.solve.solve_fits_file` with nothing
  written back. The solver is ``auto`` (the local ``solve-field`` binary when
  on PATH, else the pure-Python Gaia pattern match), ``solve-field`` or
  ``gaia``; an offline ``gaia_catalog`` JSON file can replace the network
  Gaia query (see :func:`_gaia_query_from_catalog`).
- ``photometry_run`` — run the photometry
  :class:`~seestar_controller.photometry.run.RunController` over ALL
  sub-frames of one ``session`` of the Project, storing one
  :class:`~seestar_controller.store.measurements.Measurement` per measured
  sub-frame in the project db. The picks come from the project's stored star
  picks, or from the Job's optional ``picks`` (saved into the project first).

Both Jobs take the Project root explicitly (``project_root``): the submitting
Tool resolves it server-side (``SEESTAR_PROJECT_ROOT`` > ``~/SeestarProjects/``)
and passes it in the kwargs, so any compute worker can execute the Job
correctly regardless of its own environment. ``fits_path`` / ``gaia_catalog``
must likewise be readable by the worker (a shared filesystem path).

Both return small JSON-serializable summaries (paths + key numbers + the WCS
cards / measurement rows) — never raw FITS, which stay on disk in the Project.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from seestar_controller.astrometry.session import (
    solve_project_frame,
    solve_session_frame,
)
from seestar_controller.astrometry.solve import (
    DEFAULT_MATCH_RADIUS_ARCSEC,
    DEFAULT_MIN_MATCHES,
    DEFAULT_RMS_TOLERANCE_ARCSEC,
    DEFAULT_SOLVE_FIELD_TIMEOUT_S,
    SOLVER_AUTO,
    solve_fits_file,
)
from seestar_controller.gaia.query import GaiaStar
from seestar_controller.photometry.picking import ApertureParams
from seestar_controller.photometry.run import RunController
from seestar_controller.store import frames as store_frames
from seestar_controller.store import project as store_project
from seestar_controller.store import stars as store_stars
from seestar_controller.store.stars import StarPick

from seestar_mcp.jobs.celery_app import app
from seestar_mcp.jobs.tasks import JobTask

#: Task names — the ``seestar_mcp.jobs.compute.`` prefix does NOT match the
#: scope route, so these go to the Compute queue (the default queue, ADR-0004).
SOLVE_FIELD_TASK = "seestar_mcp.jobs.compute.solve_field"
PHOTOMETRY_RUN_TASK = "seestar_mcp.jobs.compute.photometry_run"

#: Keys of one ``picks`` entry accepted by the ``photometry_run`` Job (a
#: JSON-serializable StarPick: aperture fields fall back to ``aperture``).
_PICK_FIELDS = (
    "role",
    "label",
    "x",
    "y",
    "ra_deg",
    "dec_deg",
    "aperture_radius",
    "sky_annulus_inner",
    "sky_annulus_outer",
)


def _open_project(name: str, project_root: str | None) -> store_project.Project:
    """Open the EXISTING Project ``name`` under ``project_root`` (or the
    default project root when ``project_root`` is None).

    Compute Jobs never create projects — they process frames that are already
    in one — so a missing project raises a clear :class:`ValueError` before
    anything is submitted to the heavy work.
    """
    root = Path(project_root) if project_root else store_project.default_project_root()
    path = root / store_project.slugify(name)
    if not (path / store_project.PROJECT_FILE).is_file():
        raise ValueError(
            f"no project {name!r} under {root} — looked for "
            f"{path / store_project.PROJECT_FILE}"
        )
    return store_project.open_project(path)


def _gaia_query_from_catalog(catalog_path: str | Path) -> Any:
    """Build an offline ``gaia_query`` from a JSON catalog file.

    The file holds a JSON list of Gaia reference stars, each a mapping with
    at least ``source_id``, ``ra_deg``, ``dec_deg`` (and optionally ``g``,
    ``bp``, ``rp``, ... — the :class:`~seestar_controller.gaia.query.GaiaStar`
    fields). The returned query answers any cone with the catalog's stars (the
    cross-match filters by radius), so a solve can run with NO network access
    — e.g. in CI or with a catalog exported from a previous online query.
    """
    path = Path(catalog_path)
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"cannot read the Gaia catalog {path}: {exc}") from exc
    if not isinstance(rows, list):
        raise TypeError(
            f"the Gaia catalog {path} must be a JSON list of star objects, "
            f"got {type(rows).__name__}"
        )
    stars: list[GaiaStar] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or "source_id" not in row:
            raise ValueError(
                f"Gaia catalog entry {index} of {path} must be an object with "
                f"at least source_id/ra_deg/dec_deg, got {row!r}"
            )
        stars.append(
            GaiaStar(
                source_id=int(row["source_id"]),
                ra_deg=float(row["ra_deg"]),
                dec_deg=float(row["dec_deg"]),
                g=None if row.get("g") is None else float(row["g"]),
            )
        )

    def query(center_ra_deg: float, center_dec_deg: float, radius_deg: float) -> list[GaiaStar]:
        return stars

    return query


def _solve_payload(
    result: Any,
    *,
    project: store_project.Project | None,
    frame: store_frames.Frame | None,
    fits_path: str | None,
) -> dict[str, Any]:
    """The JSON-serializable ``solve_field`` Job result: the WCS solution
    (key numbers + cards), the frame identity, and where it lives on disk."""
    payload: dict[str, Any] = {
        "solver": result.solver,
        "solved_at": result.solved_at,
        "center_ra_deg": result.center_ra_deg,
        "center_dec_deg": result.center_dec_deg,
        "n_stars": result.n_stars,
        "rms_arcsec": result.rms_arcsec,
        "n_matches": len(result.matches),
        "cards": dict(result.cards),
    }
    if project is not None and frame is not None:
        payload.update(
            project=project.slug,
            project_path=str(project.path),
            frame_id=frame.id,
            session_id=frame.session_id,
            frame_path=str(store_frames.get_frame_path(project, frame)),
            wcs_stored=True,  # solve_project_frame persisted frame_wcs + FITS header
        )
    else:
        payload.update(fits_path=fits_path, wcs_stored=False)
    return payload


@app.task(name=SOLVE_FIELD_TASK, base=JobTask)
def solve_field(
    *,
    project: str | None = None,
    project_root: str | None = None,
    session: str | None = None,
    frame_id: int | None = None,
    fits_path: str | None = None,
    solver: str = SOLVER_AUTO,
    center_hint: list[float] | None = None,
    pixel_scale_hint: float | None = None,
    gaia_catalog: str | None = None,
    match_radius_arcsec: float = DEFAULT_MATCH_RADIUS_ARCSEC,
    min_matches: int = DEFAULT_MIN_MATCHES,
    rms_tolerance_arcsec: float = DEFAULT_RMS_TOLERANCE_ARCSEC,
    solve_field_timeout_s: float = DEFAULT_SOLVE_FIELD_TIMEOUT_S,
) -> dict[str, Any]:
    """Compute Job: plate-solve one frame and return the WCS solution.

    Either ``project`` + ``frame_id`` (a frame of the Project — the solved WCS
    is written into its FITS header and persisted in the project db via
    :func:`seestar_controller.store.wcs.save_frame_wcs`; ``session`` optionally
    scopes the frame lookup) or ``fits_path`` (a plain FITS file, nothing
    written back) — never both. ``solver`` / hints / tolerances are forwarded
    to :func:`seestar_controller.astrometry.solve.solve_frame`;
    ``gaia_catalog`` replaces the network Gaia query with an offline catalog
    (see :func:`_gaia_query_from_catalog`). Raises
    :class:`~seestar_controller.astrometry.errors.SolveError` when the frame
    cannot be solved — the Job then ends in FAILURE with that message.
    """
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
    project_name = project  # narrowed: not None when fits_path is None

    solve_kwargs: dict[str, Any] = {
        "solver": solver,
        "match_radius_arcsec": match_radius_arcsec,
        "min_matches": min_matches,
        "rms_tolerance_arcsec": rms_tolerance_arcsec,
        "solve_field_timeout_s": solve_field_timeout_s,
    }
    if center_hint is not None:
        solve_kwargs["center_hint"] = tuple(center_hint)
    if pixel_scale_hint is not None:
        solve_kwargs["pixel_scale_hint"] = pixel_scale_hint
    if gaia_catalog is not None:
        solve_kwargs["gaia_query"] = _gaia_query_from_catalog(gaia_catalog)

    if fits_path is not None:
        result = solve_fits_file(fits_path, **solve_kwargs)
        return _solve_payload(result, project=None, frame=None, fits_path=fits_path)

    assert project_name is not None and frame_id is not None  # validated above
    project_handle = _open_project(project_name, project_root)
    try:
        if session is not None:
            result = solve_session_frame(project_handle, session, frame_id, **solve_kwargs)
        else:
            result = solve_project_frame(project_handle, frame_id, **solve_kwargs)
        frame = next(
            (f for f in store_frames.list_frames(project_handle) if f.id == frame_id), None
        )
        return _solve_payload(
            result, project=project_handle, frame=frame, fits_path=None
        )
    finally:
        project_handle.close()


def _star_pick_from_dict(spec: dict[str, Any], aperture: ApertureParams) -> StarPick:
    """One ``picks`` entry of the ``photometry_run`` Job -> a store
    :class:`~seestar_controller.store.stars.StarPick`.

    Missing aperture fields fall back to the Job's ``aperture`` (an
    :class:`~seestar_controller.photometry.picking.ApertureParams`, default
    aperture when the Job got none). Unknown keys are rejected; validation of
    the values happens in :class:`StarPick` itself.
    """
    unknown = set(spec) - set(_PICK_FIELDS)
    if unknown:
        raise ValueError(f"unknown pick keys {sorted(unknown)} — allowed: {list(_PICK_FIELDS)}")
    for required in ("role", "x", "y"):
        if required not in spec:
            raise ValueError(f"pick {spec!r} is missing the required key {required!r}")
    return StarPick(
        role=spec["role"],
        label=spec.get("label"),
        x=spec["x"],
        y=spec["y"],
        ra_deg=spec.get("ra_deg"),
        dec_deg=spec.get("dec_deg"),
        aperture_radius=spec.get("aperture_radius", aperture.radius_px),
        sky_annulus_inner=spec.get("sky_annulus_inner", aperture.sky_in_px),
        sky_annulus_outer=spec.get("sky_annulus_outer", aperture.sky_out_px),
    )


def _measurement_payload(measurement: Any) -> dict[str, Any]:
    """One stored :class:`~seestar_controller.store.measurements.Measurement`
    as a JSON-serializable dict."""
    return {
        "id": measurement.id,
        "frame_id": measurement.frame_id,
        "session_id": measurement.session_id,
        "jd": measurement.jd,
        "utc": measurement.utc,
        "mag": measurement.mag,
        "mag_err": measurement.mag_err,
        "excluded": measurement.excluded,
    }


@app.task(name=PHOTOMETRY_RUN_TASK, base=JobTask)
def photometry_run(
    *,
    project: str,
    project_root: str | None = None,
    session: str,
    picks: list[dict[str, Any]] | None = None,
    aperture: dict[str, float] | None = None,
    excluded_frames: list[int] | None = None,
) -> dict[str, Any]:
    """Compute Job: run aperture photometry over all sub-frames of ``session``.

    Opens the Project ``project`` under ``project_root``, optionally saves the
    Job's ``picks`` (a list of JSON pick dicts — role/label/x/y/ra_deg/dec_deg
    plus optional per-pick aperture fields, defaulting to ``aperture`` =
    ``{"radius_px", "sky_in_px", "sky_out_px"}`` or the default aperture) as the
    project's star picks, then runs
    :meth:`seestar_controller.photometry.run.RunController.run_session` over
    the session's sub-frames. Every measured sub-frame is stored as a
    :class:`~seestar_controller.store.measurements.Measurement` in the
    project db (the GUI's lightcurve sees it too); ``excluded_frames`` skips
    the given sub-frames. Returns a JSON-serializable summary: counts + the
    stored measurement rows — never raw FITS.
    """
    if not isinstance(session, str) or not session:
        raise ValueError(f"session must be a non-empty string, got {session!r}")
    aperture_fields = ("radius_px", "sky_in_px", "sky_out_px")
    if aperture is not None:
        unknown = set(aperture) - set(aperture_fields)
        if unknown:
            raise ValueError(
                f"unknown aperture keys {sorted(unknown)} — allowed: {list(aperture_fields)}"
            )
    aperture_params = ApertureParams(**(aperture or {}))
    project_handle = _open_project(project, project_root)
    slug = project_handle.slug
    project_path = str(project_handle.path)
    try:
        if picks is not None:
            stored = [_star_pick_from_dict(spec, aperture_params) for spec in picks]
            store_stars.save_star_picks(project_handle, stored)
        controller = RunController(project=project_handle)
        report = controller.run_session(session, excluded_frames=excluded_frames)
    finally:
        project_handle.close()
    return {
        "project": slug,
        "project_path": project_path,
        "session_id": report.session_id,
        "measured": len(report.measurements),
        "skipped": [
            {"frame_id": s.frame_id, "reason": s.reason, "kind": s.kind}
            for s in report.skipped
        ],
        "excluded": [
            {"frame_id": e.frame_id, "reasons": list(e.reasons)} for e in report.excluded
        ],
        "cleared_measurements": report.cleared_measurements,
        "skip_reasons": report.skip_reasons,
        "wcs_frames": report.wcs_frames,
        "pixel_frames": report.pixel_frames,
        "refined_frames": report.refined_frames,
        "measurements": [_measurement_payload(m) for m in report.measurements],
    }


__all__ = [
    "PHOTOMETRY_RUN_TASK",
    "SOLVE_FIELD_TASK",
    "photometry_run",
    "solve_field",
]
