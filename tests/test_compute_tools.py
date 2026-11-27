"""Tests for the compute Tools ``solve_field`` and ``photometry_run`` (issue #8).

Three kinds of tests:

- Config-only (run everywhere, no redis): the Tools are registered with the
  right schemas, the compute tasks live under ``seestar_mcp.jobs.compute.*``
  and route to the Compute queue (the default queue, ADR-0004 — NOT the
  ``seestar_mcp.jobs.scope.*`` route), and ``submit_job`` sends them with
  ``task_id == idempotency_key`` and ``queue="compute"`` (spied ``send_task``
  + fake claim redis, like ``test_job_tools.py``).
- Always-on argument-validation tests over real MCP stdio (no redis): bad
  combinations (fits_path + project, unknown project, bad picks, bad solver)
  raise a tool error BEFORE anything is submitted.
- Real redis + a compute-queue Celery worker (skipped locally without redis,
  run in CI): the full MCP-over-stdio round-trip — build a tmp Project with
  synthetic FITS frames (a plate-solvable frame with an injected offline Gaia
  catalog, and a session of photometry sub-frames with known injected fluxes),
  submit ``solve_field`` / ``photometry_run`` with an idempotency key, poll
  ``job_status`` to SUCCESS, and check ``job_result`` (WCS solution /
  measurement summary) plus what the Project store persisted (``frame_wcs``
  row, Measurements). Resubmitting the same key refers to the same Job.

The synthetic frames mirror seestar-controller's own pure-layer tests
(``tests/test_astrometry_solve.py`` / ``tests/test_photometry_run.py``): the
Gaia fallback's reference query is replaced by an offline JSON catalog at the
true sky positions of the injected stars, so no test touches the network and
no ``solve-field`` binary is needed (it is not installed in CI).
"""

import asyncio
import io
import json
import math
import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from astropy.io import fits as pyfits
from astropy.wcs import WCS
from conftest import requires_redis
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from seestar_controller.store import frames as store_frames
from seestar_controller.store import measurements as store_measurements
from seestar_controller.store import project as store_project
from seestar_controller.store import sessions as store_sessions
from seestar_controller.store import stars as store_stars
from seestar_controller.store import wcs as store_wcs
from test_link_tools import _error_text, _payload
from test_scope_tools import _job_result, _key, _poll_until_done

from seestar_mcp.jobs import app
from seestar_mcp.jobs import submit as submit_module
from seestar_mcp.jobs.celery_app import COMPUTE_QUEUE, REDIS_URL
from seestar_mcp.jobs.compute import PHOTOMETRY_RUN_TASK, SOLVE_FIELD_TASK
from seestar_mcp.server import mcp

# --- synthetic plate-solvable frame (mirrors test_astrometry_solve.py) --------

#: The true pixel scale (deg/px) of the synthetic solve frame.
SOLVE_SCALE = 0.001
#: The synthetic solve frame size (px).
SOLVE_SIZE = 256
#: The true field center (CRVAL) of the synthetic solve frame.
SOLVE_CRVAL = (83.0, 22.0)
#: Gaussian star sigma (px) / background / noise of the synthetic solve frame.
SOLVE_STAR_SIGMA = 1.5
SOLVE_BACKGROUND = 100.0
SOLVE_NOISE = 2.0
#: Star positions on the solve frame as (cy, cx, peak).
SOLVE_STARS = (
    (30, 30, 20000.0),
    (30, 120, 18000.0),
    (90, 60, 15000.0),
    (90, 170, 12000.0),
    (60, 100, 9000.0),
    (150, 40, 8000.0),
    (150, 200, 7000.0),
    (200, 90, 6000.0),
    (200, 180, 5500.0),
    (120, 220, 5000.0),
)


def _solve_true_wcs() -> WCS:
    """The TRUE WCS of the synthetic solve frame (TAN, 0.001 deg/px)."""
    return WCS(
        {
            "CTYPE1": "RA---TAN",
            "CTYPE2": "DEC--TAN",
            "CRVAL1": SOLVE_CRVAL[0],
            "CRVAL2": SOLVE_CRVAL[1],
            "CRPIX1": (SOLVE_SIZE + 1) / 2.0,
            "CRPIX2": (SOLVE_SIZE + 1) / 2.0,
            "CDELT1": -SOLVE_SCALE,
            "CDELT2": SOLVE_SCALE,
        }
    )


def _solve_image(seed: int = 42) -> np.ndarray:
    """The synthetic 256x256 frame: background + Gaussian stars + noise."""
    rng = np.random.default_rng(seed)
    yy, xx = np.mgrid[0:SOLVE_SIZE, 0:SOLVE_SIZE]
    data = np.full((SOLVE_SIZE, SOLVE_SIZE), SOLVE_BACKGROUND, dtype=np.float64)
    for cy, cx, peak in SOLVE_STARS:
        data = data + peak * np.exp(
            -(((yy - cy) ** 2 + (xx - cx) ** 2) / (2.0 * SOLVE_STAR_SIGMA**2))
        )
    data = data + rng.normal(0.0, SOLVE_NOISE, size=(SOLVE_SIZE, SOLVE_SIZE))
    return np.ascontiguousarray(data, dtype=np.float32)


def _blank_image(seed: int = 7) -> np.ndarray:
    """A starless frame (background + noise only) — unsolvable."""
    rng = np.random.default_rng(seed)
    data = np.full((SOLVE_SIZE, SOLVE_SIZE), SOLVE_BACKGROUND, dtype=np.float64)
    data = data + rng.normal(0.0, SOLVE_NOISE, size=(SOLVE_SIZE, SOLVE_SIZE))
    return np.ascontiguousarray(data, dtype=np.float32)


def _fits_bytes(data: np.ndarray, header: dict[str, Any] | None = None) -> bytes:
    """A small valid FITS file with ``data`` and ``header`` cards."""
    hdu = pyfits.PrimaryHDU(data=np.asarray(data, dtype=np.float32))
    for key, value in (header or {}).items():
        hdu.header[key] = value
    buf = io.BytesIO()
    pyfits.HDUList([hdu]).writeto(buf)
    return buf.getvalue()


def _solve_blind_header() -> dict[str, Any]:
    """A header WITHOUT a WCS but with RA/DEC hint cards (like a raw sub)."""
    return {
        "RA": SOLVE_CRVAL[0],
        "DEC": SOLVE_CRVAL[1],
        "DATE-OBS": "2026-11-20T22:00:00.000",
    }


def _gaia_catalog(path: Path) -> Path:
    """Write the offline Gaia catalog: synthetic Gaia stars at the TRUE sky
    positions of the injected image stars. Returns the catalog path."""
    wcs = _solve_true_wcs()
    rows = []
    for index, (cy, cx, _peak) in enumerate(SOLVE_STARS):
        coord = wcs.pixel_to_world(cx, cy)
        rows.append(
            {
                "source_id": 1000 + index,
                "ra_deg": coord.ra.deg,
                "dec_deg": coord.dec.deg,
                "g": 12.0,
            }
        )
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


def _center_sep_arcsec(ra_deg: float, dec_deg: float) -> float:
    """The angular separation of ``(ra_deg, dec_deg)`` from SOLVE_CRVAL."""
    return math.hypot(
        (ra_deg - SOLVE_CRVAL[0]) * 3600.0 * math.cos(math.radians(SOLVE_CRVAL[1])),
        (dec_deg - SOLVE_CRVAL[1]) * 3600.0,
    )


# --- synthetic photometry session (mirrors test_photometry_run.py) -----------

PHOTO_SIZE = 64
PHOTO_BACKGROUND = 100.0
PHOTO_NOISE = 5.0
PHOTO_F0 = 20000.0  # constant comp peak flux
PHOTO_TARGET_POS = (30, 40)  # (row, col)
PHOTO_COMP_POSITIONS = ((16, 16), (12, 50), (50, 12))
PHOTO_T0 = datetime(2026, 11, 20, 22, 0, 0, tzinfo=UTC)

#: The picks passed to the photometry_run Tool (x = column, y = row).
PHOTO_PICKS = [
    {"role": "target", "label": "Target", "x": 40.0, "y": 30.0},
    {"role": "comp", "label": "Comp A", "x": 16.0, "y": 16.0},
    {"role": "comp", "label": "Comp B", "x": 50.0, "y": 12.0},
    {"role": "comp", "label": "Comp C", "x": 12.0, "y": 50.0},
]


def _photo_date_obs(i: int, seconds: int = 10) -> str:
    """The DATE-OBS of photometry frame ``i``: T0 + i*seconds (naive UTC)."""
    return (
        (PHOTO_T0 + timedelta(seconds=seconds * i))
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "")
    )


def _photo_frame_data(target_peak: float, seed: int) -> np.ndarray:
    """A synthetic 64x64 frame: background + Gaussian stars + noise; the
    target at PHOTO_TARGET_POS with ``target_peak``, 3 comps at constant F0."""
    rng = np.random.default_rng(seed)
    yy, xx = np.mgrid[0:PHOTO_SIZE, 0:PHOTO_SIZE]
    data = np.full((PHOTO_SIZE, PHOTO_SIZE), PHOTO_BACKGROUND, dtype=np.float64)
    stars = [(PHOTO_TARGET_POS[0], PHOTO_TARGET_POS[1], target_peak)] + [
        (cy, cx, PHOTO_F0) for cy, cx in PHOTO_COMP_POSITIONS
    ]
    for cy, cx, peak in stars:
        data = data + peak * np.exp(-(((yy - cy) ** 2 + (xx - cx) ** 2) / 8.0))
    data = data + rng.normal(0.0, PHOTO_NOISE, size=(PHOTO_SIZE, PHOTO_SIZE))
    return np.ascontiguousarray(data, dtype=np.float32)


def _photo_fits_bytes(data: np.ndarray, date_obs: str) -> bytes:
    hdu = pyfits.PrimaryHDU(data=np.asarray(data, dtype=np.float32))
    hdu.header["DATE-OBS"] = date_obs
    hdu.header["EXPTIME"] = 10.0
    hdu.header["GAIN"] = 50
    hdu.header["OBJECT"] = "M 42"
    buf = io.BytesIO()
    pyfits.HDUList([hdu]).writeto(buf)
    return buf.getvalue()


def _make_photo_session(project: Any, n: int = 3) -> tuple[str, list[Any], list[float]]:
    """One session with ``n`` synthetic sub-frames (sinusoidal target flux).

    Returns ``(session_id, frames, injected_mags)`` — the injected
    differential magnitude of frame ``i`` is ``m_i = -2.5*log10(F_i/F0)``.
    """
    session_id = store_sessions.start_session(project, "2026-11-20T22:00:00")
    frames = []
    injected: list[float] = []
    for i in range(n):
        m_inj = 0.3 * math.sin(2.0 * math.pi * i / n)
        injected.append(m_inj)
        target_peak = PHOTO_F0 * 10 ** (-0.4 * m_inj)
        frames.append(
            store_frames.store_frame(
                project,
                session_id,
                "sub",
                _photo_fits_bytes(_photo_frame_data(target_peak, 100 + i), _photo_date_obs(i)),
            )
        )
    return session_id, frames, injected


# --- helpers ------------------------------------------------------------------


def _server_params(extra_env: dict[str, str] | None = None) -> StdioServerParameters:
    """Stdio params spawning the real MCP server (compute Tools need no key).

    ``REDIS_URL`` is included so the server can submit Jobs when redis runs;
    ``SEESTAR_MCP_SCOPE_IP`` is dropped (irrelevant here).
    """
    env = {
        **os.environ,
        "SEESTAR_PROTOCOL_LOG": "0",
        "REDIS_URL": REDIS_URL,
    }
    env.pop("SEESTAR_MCP_SCOPE_IP", None)
    env.update(extra_env or {})
    return StdioServerParameters(
        command=sys.executable, args=["-m", "seestar_mcp"], env=env
    )


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


# --- Config-only tests (no redis needed) --------------------------------------


def test_compute_tools_registered_on_app() -> None:
    """solve_field / photometry_run are Tools, keyed by the Idempotency key."""
    tools = {t.name: t for t in asyncio.run(mcp.list_tools())}
    assert {"solve_field", "photometry_run"} <= tools.keys()
    for name in ("solve_field", "photometry_run"):
        schema = tools[name].inputSchema
        assert schema["properties"]["idempotency_key"]["type"] == "string"
        assert "idempotency_key" in schema["required"]

    solve_schema = tools["solve_field"].inputSchema
    for prop in (
        "project",
        "session",
        "frame_id",
        "fits_path",
        "solver",
        "center_hint",
        "pixel_scale_hint",
        "gaia_catalog",
    ):
        assert prop in solve_schema["properties"]
        assert prop not in solve_schema.get("required", [])
    assert solve_schema["properties"]["solver"]["default"] == "auto"

    photo_schema = tools["photometry_run"].inputSchema
    assert photo_schema["required"] == ["idempotency_key", "project", "session"]
    for prop in ("picks", "aperture", "excluded_frames"):
        assert prop in photo_schema["properties"]
        assert prop not in photo_schema["required"]


def test_compute_tasks_registered_and_routed_to_compute_queue() -> None:
    """The compute tasks are JobTasks NOT matching the scope route -> Compute queue.

    ADR-0004: only ``seestar_mcp.jobs.scope.*`` routes to the Scope queue;
    everything else (the compute tasks) defaults to the Compute queue.
    """
    from seestar_mcp.jobs.tasks import JobTask, TransientJobError

    for name in (SOLVE_FIELD_TASK, PHOTOMETRY_RUN_TASK):
        assert name.startswith("seestar_mcp.jobs.compute.")
        assert not name.startswith("seestar_mcp.jobs.scope.")
        task = app.tasks[name]
        assert isinstance(task, JobTask)
        assert task.autoretry_for == (TransientJobError,)
    assert app.conf.task_routes == {"seestar_mcp.jobs.scope.*": {"queue": "scope"}}
    assert app.conf.task_default_queue == COMPUTE_QUEUE


def test_compute_tools_submit_to_compute_queue_with_idempotency_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Both Tools submit with task_id == idempotency key and queue == compute.

    The claim redis and ``app.send_task`` are spied, so no broker is needed:
    the submission is recorded, and resubmitting the same key sends nothing.
    """
    monkeypatch.setenv("SEESTAR_PROJECT_ROOT", str(tmp_path))
    project = store_project.create_project("Compute Proj", root=tmp_path)
    try:
        fake = _FakeRedis()
        sent: list[dict[str, Any]] = []
        _patch_submit(monkeypatch, fake, sent)

        # --- solve_field: project + frame_id ---
        key = _key("cfg-solve")
        result = _payload(
            asyncio.run(
                mcp.call_tool(
                    "solve_field",
                    {
                        "idempotency_key": key,
                        "project": "Compute Proj",
                        "frame_id": 7,
                        "solver": "gaia",
                    },
                )
            )
        )
        assert result["idempotency_key"] == key
        assert result["task"] == SOLVE_FIELD_TASK
        assert result["queue"] == COMPUTE_QUEUE
        assert result["solve_field"]["frame_id"] == 7
        assert sent == [
            {
                "name": SOLVE_FIELD_TASK,
                "args": [],
                "kwargs": {
                    "solver": "gaia",
                    "project": "Compute Proj",
                    "project_root": str(tmp_path),
                    "frame_id": 7,
                },
                "task_id": key,
                "queue": COMPUTE_QUEUE,
            }
        ]

        # resubmitting the same key: same Job, no duplicate send
        again = _payload(
            asyncio.run(
                mcp.call_tool(
                    "solve_field",
                    {
                        "idempotency_key": key,
                        "project": "Compute Proj",
                        "frame_id": 8,
                    },
                )
            )
        )
        assert again["idempotency_key"] == key
        assert len(sent) == 1  # not re-sent: the key is claimed

        # --- photometry_run: project + session + picks ---
        photo_key = _key("cfg-photo")
        result = _payload(
            asyncio.run(
                mcp.call_tool(
                    "photometry_run",
                    {
                        "idempotency_key": photo_key,
                        "project": "Compute Proj",
                        "session": "20261120T220000000000Z",
                        "picks": PHOTO_PICKS,
                        "aperture": {"radius_px": 5.0},
                    },
                )
            )
        )
        assert result["task"] == PHOTOMETRY_RUN_TASK
        assert result["queue"] == COMPUTE_QUEUE
        assert result["photometry_run"] == {
            "project": "compute-proj",
            "session": "20261120T220000000000Z",
            "picks": 4,
            "excluded_frames": None,
        }
        assert len(sent) == 2
        photo_send = sent[1]
        assert photo_send["name"] == PHOTOMETRY_RUN_TASK
        assert photo_send["task_id"] == photo_key
        assert photo_send["queue"] == COMPUTE_QUEUE
        assert photo_send["kwargs"]["project_root"] == str(tmp_path)
        assert photo_send["kwargs"]["session"] == "20261120T220000000000Z"
        assert photo_send["kwargs"]["picks"] == PHOTO_PICKS
        assert photo_send["kwargs"]["aperture"] == {"radius_px": 5.0}
    finally:
        project.close()


# --- Argument validation over real MCP stdio (no redis, nothing submitted) ---


def test_compute_tools_reject_bad_arguments_before_submitting(
    tmp_path: Path,
) -> None:
    """Bad solve_field / photometry_run arguments raise a tool error, no redis.

    Validation runs before ``submit_job``, so the shared compute queue stays
    clean and no broker is needed.
    """

    async def run() -> None:
        params = _server_params({"SEESTAR_PROJECT_ROOT": str(tmp_path)})
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            names = {t.name for t in (await session.list_tools()).tools}
            assert {"solve_field", "photometry_run"} <= names

            async def call(tool: str, args: dict[str, Any]) -> str:
                res = await session.call_tool(tool, args)
                assert res.isError
                return _error_text(res)

            # fits_path AND project: never both
            text = await call(
                "solve_field",
                {
                    "idempotency_key": _key("bad"),
                    "project": "whatever",
                    "frame_id": 1,
                    "fits_path": str(tmp_path / "x.fits"),
                },
            )
            assert "not both" in text

            # neither fits_path nor project + frame_id
            text = await call("solve_field", {"idempotency_key": _key("bad")})
            assert "give either fits_path or project + frame_id" in text

            # frame_id without project
            text = await call(
                "solve_field", {"idempotency_key": _key("bad"), "frame_id": 3}
            )
            assert "frame_id needs project" in text

            # project without frame_id
            text = await call(
                "solve_field",
                {"idempotency_key": _key("bad"), "project": "whatever"},
            )
            assert "needs frame_id" in text

            # unknown project (fail fast before submitting)
            text = await call(
                "solve_field",
                {
                    "idempotency_key": _key("bad"),
                    "project": f"missing-{uuid.uuid4().hex}",
                    "frame_id": 1,
                },
            )
            assert "no project" in text

            # bad solver name
            text = await call(
                "solve_field",
                {
                    "idempotency_key": _key("bad"),
                    "fits_path": __file__,
                    "solver": "bogus",
                },
            )
            assert "solver must be one of" in text

            # fits_path that does not exist
            text = await call(
                "solve_field",
                {
                    "idempotency_key": _key("bad"),
                    "fits_path": str(tmp_path / "nope.fits"),
                },
            )
            assert "not an existing file" in text

            # photometry_run: unknown project
            text = await call(
                "photometry_run",
                {
                    "idempotency_key": _key("bad"),
                    "project": f"missing-{uuid.uuid4().hex}",
                    "session": "s1",
                },
            )
            assert "no project" in text

            # photometry_run: picks without exactly one target
            text = await call(
                "photometry_run",
                {
                    "idempotency_key": _key("bad"),
                    "project": "whatever",
                    "session": "s1",
                    "picks": [
                        {"role": "target", "x": 1.0, "y": 2.0},
                        {"role": "target", "x": 3.0, "y": 4.0},
                        {"role": "comp", "x": 5.0, "y": 6.0},
                    ],
                },
            )
            assert "exactly one 'target'" in text

            # photometry_run: no comp pick
            text = await call(
                "photometry_run",
                {
                    "idempotency_key": _key("bad"),
                    "project": "whatever",
                    "session": "s1",
                    "picks": [{"role": "target", "x": 1.0, "y": 2.0}],
                },
            )
            assert "at least one 'comp'" in text

            # photometry_run: bad aperture key
            text = await call(
                "photometry_run",
                {
                    "idempotency_key": _key("bad"),
                    "project": "whatever",
                    "session": "s1",
                    "aperture": {"bogus": 1.0},
                },
            )
            assert "unknown aperture keys" in text

    asyncio.run(run())


# --- Real redis + compute-queue worker (skipped without redis) ---------------


@requires_redis
def test_solve_field_job_solves_project_frame_and_persists_wcs(
    celery_worker: subprocess.Popen[str], tmp_path: Path
) -> None:
    """A solve_field Job on a Project frame returns the WCS and stores it.

    The tmp Project holds one synthetic sub-frame (10 stars, known center,
    blind RA/DEC header). The Job solves it with the Gaia fallback against an
    OFFLINE JSON catalog at the stars' true sky positions (no network, no
    solve-field binary — CI-proof). ``job_result`` carries the WCS solution;
    the project db got the ``frame_wcs`` row and the FITS header the cards.
    Resubmitting the same idempotency key refers to the same Job.
    """
    project_root = tmp_path / "projects"
    project = store_project.create_project("Solve Proj", root=project_root)
    try:
        session_id = store_sessions.start_session(project, "2026-11-20T22:00:00")
        frame = store_frames.store_frame(
            project,
            session_id,
            "sub",
            _fits_bytes(_solve_image(), _solve_blind_header()),
        )
        catalog = _gaia_catalog(tmp_path / "gaia-catalog.json")
        key = _key("solve")

        async def run() -> Any:
            params = _server_params({"SEESTAR_PROJECT_ROOT": str(project_root)})
            async with (
                stdio_client(params) as (read, write),
                ClientSession(read, write) as session,
            ):
                await session.initialize()
                res = await session.call_tool(
                    "solve_field",
                    {
                        "idempotency_key": key,
                        "project": "Solve Proj",
                        "session": session_id,
                        "frame_id": frame.id,
                        "solver": "gaia",
                        "gaia_catalog": str(catalog),
                        "pixel_scale_hint": SOLVE_SCALE * 3600.0,
                    },
                )
                assert not res.isError
                submitted = _payload(res)
                assert submitted["idempotency_key"] == key
                assert submitted["task"] == SOLVE_FIELD_TASK
                assert submitted["queue"] == COMPUTE_QUEUE
                assert submitted["solve_field"]["frame_id"] == frame.id

                status = await _poll_until_done(session, key, timeout_s=180.0)
                assert status["state"] == "SUCCESS", status
                assert status["error"] is None
                return await _job_result(session, key)

        result = asyncio.run(run())

        # the WCS solution in the payload (small, JSON-serializable — no FITS)
        assert result["solver"] == "gaia"
        assert result["n_stars"] == len(SOLVE_STARS)
        assert result["rms_arcsec"] is not None and result["rms_arcsec"] < 1.0
        assert _center_sep_arcsec(result["center_ra_deg"], result["center_dec_deg"]) < 5.0
        assert result["wcs_stored"] is True
        assert result["project"] == "solve-proj"
        assert result["frame_id"] == frame.id
        assert result["session_id"] == session_id
        assert result["frame_path"].endswith(".fits")
        assert Path(result["frame_path"]).is_file()
        cards = result["cards"]
        assert cards["CTYPE1"] == "RA---TAN"
        assert cards["CTYPE2"] == "DEC--TAN"
        assert "CRPIX1" in cards and "CDELT1" in cards

        # resubmitting the same key: same Job, unchanged result
        async def resubmit() -> Any:
            params = _server_params({"SEESTAR_PROJECT_ROOT": str(project_root)})
            async with (
                stdio_client(params) as (read, write),
                ClientSession(read, write) as session,
            ):
                await session.initialize()
                res = await session.call_tool(
                    "solve_field",
                    {
                        "idempotency_key": key,
                        "project": "Solve Proj",
                        "frame_id": frame.id,
                        "solver": "gaia",
                        "gaia_catalog": str(catalog),
                        "pixel_scale_hint": SOLVE_SCALE * 3600.0,
                    },
                )
                assert not res.isError
                assert _payload(res)["idempotency_key"] == key
                status = await _poll_until_done(session, key, timeout_s=30.0)
                assert status["state"] == "SUCCESS"
                return await _job_result(session, key)

        assert asyncio.run(resubmit()) == result

        # the Project store persisted the solve: the frame_wcs row ...
        stored = store_wcs.load_frame_wcs(project, frame.id)
        assert stored is not None
        assert stored.solver == "gaia"
        assert stored.n_stars == len(SOLVE_STARS)
        assert stored.session_id == session_id
        assert _center_sep_arcsec(stored.center_ra_deg, stored.center_dec_deg) < 5.0
        assert stored.cards["CTYPE1"] == "RA---TAN"
        # ... and the solved cards were written into the stored FITS header
        header = pyfits.getheader(str(store_frames.get_frame_path(project, frame)))
        assert header["CTYPE1"] == "RA---TAN"
        assert header["CRVAL1"] == pytest.approx(stored.cards["CRVAL1"])
    finally:
        project.close()


@requires_redis
def test_solve_field_job_on_fits_path_returns_wcs_without_persisting(
    celery_worker: subprocess.Popen[str], tmp_path: Path
) -> None:
    """solve_field on a plain FITS path (no Project) returns the WCS, stores nothing."""
    fits_path = tmp_path / "standalone.fits"
    fits_path.write_bytes(_fits_bytes(_solve_image(), _solve_blind_header()))
    catalog = _gaia_catalog(tmp_path / "gaia-catalog.json")
    key = _key("solve-path")

    async def run() -> Any:
        params = _server_params({"SEESTAR_PROJECT_ROOT": str(tmp_path / "projects")})
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            res = await session.call_tool(
                "solve_field",
                {
                    "idempotency_key": key,
                    "fits_path": str(fits_path),
                    "solver": "gaia",
                    "gaia_catalog": str(catalog),
                    "pixel_scale_hint": SOLVE_SCALE * 3600.0,
                },
            )
            assert not res.isError
            submitted = _payload(res)
            assert submitted["task"] == SOLVE_FIELD_TASK
            assert submitted["queue"] == COMPUTE_QUEUE
            assert submitted["solve_field"]["fits_path"] == str(fits_path)
            status = await _poll_until_done(session, key, timeout_s=180.0)
            assert status["state"] == "SUCCESS", status
            return await _job_result(session, key)

    result = asyncio.run(run())
    assert result["solver"] == "gaia"
    assert result["n_stars"] == len(SOLVE_STARS)
    assert result["wcs_stored"] is False  # nothing persisted: no Project involved
    assert result["fits_path"] == str(fits_path)
    assert "frame_id" not in result
    assert _center_sep_arcsec(result["center_ra_deg"], result["center_dec_deg"]) < 5.0
    # the input file itself was NOT modified (no WCS written into the header)
    header = pyfits.getheader(str(fits_path))
    assert "CRVAL1" not in header


@requires_redis
def test_solve_field_job_unsolvable_frame_fails_with_solve_error(
    celery_worker: subprocess.Popen[str], tmp_path: Path
) -> None:
    """An unsolvable frame (starless) ends in FAILURE; job_status carries the error."""
    project_root = tmp_path / "projects"
    project = store_project.create_project("Blank Proj", root=project_root)
    try:
        session_id = store_sessions.start_session(project, "2026-11-20T22:00:00")
        frame = store_frames.store_frame(
            project,
            session_id,
            "sub",
            _fits_bytes(_blank_image(), _solve_blind_header()),
        )
        catalog = _gaia_catalog(tmp_path / "gaia-catalog.json")
        key = _key("solve-blank")

        async def run() -> dict[str, Any]:
            params = _server_params({"SEESTAR_PROJECT_ROOT": str(project_root)})
            async with (
                stdio_client(params) as (read, write),
                ClientSession(read, write) as session,
            ):
                await session.initialize()
                res = await session.call_tool(
                    "solve_field",
                    {
                        "idempotency_key": key,
                        "project": "Blank Proj",
                        "frame_id": frame.id,
                        "solver": "gaia",
                        "gaia_catalog": str(catalog),
                        "pixel_scale_hint": SOLVE_SCALE * 3600.0,
                    },
                )
                assert not res.isError
                status = await _poll_until_done(session, key, timeout_s=180.0)
                result = await _job_result(session, key)
                return {"status": status, "result": result}

        outcome = asyncio.run(run())
        assert outcome["status"]["state"] == "FAILURE"
        assert "SolveError" in outcome["status"]["error"]
        # before SUCCESS, job_result returns the status dict, not a payload
        assert outcome["result"]["state"] == "FAILURE"
        assert "SolveError" in outcome["result"]["error"]
        # nothing was persisted for the failed solve
        assert store_wcs.load_frame_wcs(project, frame.id) is None
    finally:
        project.close()


@requires_redis
def test_photometry_run_job_stores_measurements_in_project(
    celery_worker: subprocess.Popen[str], tmp_path: Path
) -> None:
    """A photometry_run Job over a session's sub-frames stores Measurements.

    The tmp Project holds one session with 3 synthetic sub-frames (target flux
    varying sinusoidally, 3 constant comps — injected differential mags known).
    The Job gets the picks via the Tool's ``picks`` argument (saved into the
    Project by the worker), runs the RunController over the session's subs and
    stores one Measurement per frame. ``job_result`` carries the summary;
    the project db lightcurve really has the rows. Resubmitting the same
    idempotency key refers to the same Job (no re-run, nothing cleared).
    """
    project_root = tmp_path / "projects"
    project = store_project.create_project("Photo Proj", root=project_root)
    try:
        session_id, frames, injected = _make_photo_session(project, n=3)
        key = _key("photo")

        async def submit_and_poll() -> Any:
            params = _server_params({"SEESTAR_PROJECT_ROOT": str(project_root)})
            async with (
                stdio_client(params) as (read, write),
                ClientSession(read, write) as session,
            ):
                await session.initialize()
                res = await session.call_tool(
                    "photometry_run",
                    {
                        "idempotency_key": key,
                        "project": "Photo Proj",
                        "session": session_id,
                        "picks": PHOTO_PICKS,
                        "aperture": {"radius_px": 4.0, "sky_in_px": 8.0, "sky_out_px": 12.0},
                    },
                )
                assert not res.isError
                submitted = _payload(res)
                assert submitted["idempotency_key"] == key
                assert submitted["task"] == PHOTOMETRY_RUN_TASK
                assert submitted["queue"] == COMPUTE_QUEUE
                assert submitted["photometry_run"]["session"] == session_id
                status = await _poll_until_done(session, key, timeout_s=180.0)
                assert status["state"] == "SUCCESS", status
                return await _job_result(session, key)

        result = asyncio.run(submit_and_poll())

        # the summary in job_result (small, JSON-serializable — no FITS)
        assert result["project"] == "photo-proj"
        assert result["session_id"] == session_id
        assert result["measured"] == 3
        assert result["skipped"] == []
        assert result["excluded"] == []
        assert result["cleared_measurements"] == 0
        assert len(result["measurements"]) == 3
        assert [m["frame_id"] for m in result["measurements"]] == [f.id for f in frames]
        for row, m_inj in zip(result["measurements"], injected):
            assert row["session_id"] == session_id
            assert row["mag"] == pytest.approx(m_inj, abs=0.02)  # injected diff mag recovered
            assert row["mag_err"] is not None and row["mag_err"] > 0.0
            assert row["excluded"] is False

        # resubmitting the same key: same Job — no re-execution (a re-run would
        # have cleared the 3 stored rows first: cleared_measurements would be 3)
        async def resubmit() -> Any:
            params = _server_params({"SEESTAR_PROJECT_ROOT": str(project_root)})
            async with (
                stdio_client(params) as (read, write),
                ClientSession(read, write) as session,
            ):
                await session.initialize()
                res = await session.call_tool(
                    "photometry_run",
                    {
                        "idempotency_key": key,
                        "project": "Photo Proj",
                        "session": session_id,
                    },
                )
                assert not res.isError
                assert _payload(res)["idempotency_key"] == key
                status = await _poll_until_done(session, key, timeout_s=30.0)
                assert status["state"] == "SUCCESS"
                return await _job_result(session, key)

        assert asyncio.run(resubmit()) == result

        # the Project db really has the measurements (the GUI lightcurve too)
        lc = store_measurements.lightcurve(project, include_excluded=True)
        assert len(lc) == 3
        assert [m.frame_id for m in lc] == [f.id for f in frames]
        assert all(m.session_id == session_id for m in lc)
        for meas, m_inj in zip(lc, injected):
            assert meas.mag == pytest.approx(m_inj, abs=0.02)
            assert meas.mag_err is not None and meas.mag_err > 0.0
        for frame in frames:
            assert len(store_measurements.measurements_for_frame(project, frame.id)) == 1
        # the worker saved the Tool's picks as the project's star picks
        picks = store_stars.load_star_picks(project)
        assert [p.role for p in picks] == ["target", "comp", "comp", "comp"]
        assert picks[0].aperture_radius == 4.0
    finally:
        project.close()
