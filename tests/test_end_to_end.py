"""End-to-end test (issue #9): "point at M13 and measure it" (README goal).

One full-stack test drives the REAL MCP stdio server subprocess against the
in-process ``mock_scope`` (real wire protocol, ephemeral ports) with a real
Redis broker and REAL Celery workers for both queues (reusing the conftest
fixtures: ``scope_worker`` on the Scope queue, ``celery_worker`` on the
Compute queue). The server subprocess is configured exactly like an
operator's agent-spawned server:

- ``SEESTAR_MCP_SCOPE_IP=<mock host:port>`` — the scope address,
- ``SEESTAR_MCP_ALLOW_SLEW=1`` — the operator opted into Slews (ADR-0003),
- ``SEESTAR_PROJECT_ROOT=<tmp>`` — frames/measurements land in a tmp Project,
- ``SEESTAR_KEY_PATH=<test key>`` — signs the per-job Link handshakes,
- ``SEESTAR_CACHE_DIR`` — seeded so ``"M13"`` resolves offline (no network),
- ``REDIS_URL`` — the broker for the Jobs.

The chain through the Tools, each long-running one under its own Idempotency
key and polled with ``job_status`` / ``job_result``:

1. ``connect`` (address from ``SEESTAR_MCP_SCOPE_IP``) — reachable + verified;
2. ``goto(target="M13")`` (k1) — catalog-resolved slew on the Scope queue
   -> SUCCESS, the mock really slewed to M 13;
3. ``capture(frames=3)`` (k2) — Scope queue -> SUCCESS, sub-frame FITS stored
   in the Project under ``SEESTAR_PROJECT_ROOT``;
4. ``solve_field`` on a captured frame (k3) — Compute queue -> SUCCESS, WCS
   persisted in the project db (``frame_wcs``) and the FITS header;
5. ``photometry_run`` on the capture session (k4) — Compute queue ->
   SUCCESS, ``job_result`` shows the measurements;
6. finally the Project db itself is opened and asserted to hold the
   Measurements (``store.measurements`` — the GUI lightcurve sees them too).

The mock's stacked sub-frames are pure noise, so the test patches the mock's
:func:`~seestar_controller.link.mock_scope.make_fits_bytes` (the mock runs in
this process; the worker only downloads the bytes over the wire) to emit a
synthetic star field at the true sky position of M 13 — the same trick as
``test_compute_tools.py``'s offline-Gaia solve — so the captured frames are
plate-solvable and measurable without network or a ``solve-field`` binary.

The second test is always-on (no redis): the SAME chain's ``goto`` is
refused when the server subprocess is spawned WITHOUT
``SEESTAR_MCP_ALLOW_SLEW=1`` — the Slew gate exercised the other way (ADR-0003).

The redis-gated test is skipped when Redis is unreachable (local run without
redis); CI provides a redis service container via ``REDIS_URL`` and runs it.
"""

import asyncio
import io
import json
import math
import subprocess
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from astropy.io import fits as pyfits
from astropy.wcs import WCS
from conftest import requires_redis
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from seestar_controller.link import frames as link_frames
from seestar_controller.link import mock_scope as mock_scope_module
from seestar_controller.link.mock_scope import MockScope
from seestar_controller.store import frames as store_frames
from seestar_controller.store import measurements as store_measurements
from seestar_controller.store import project as store_project
from seestar_controller.store import wcs as store_wcs
from test_compute_tools import (
    SOLVE_BACKGROUND,
    SOLVE_NOISE,
    SOLVE_SCALE,
    SOLVE_SIZE,
    SOLVE_STAR_SIGMA,
    SOLVE_STARS,
)
from test_link_tools import _assert_no_held_connections, _error_text, _payload
from test_scope_tools import (
    M13,
    _job_result,
    _key,
    _poll_until_done,
    _seed_catalog_cache,
    _server_params_with_redis,
)

from seestar_mcp.jobs.celery_app import COMPUTE_QUEUE, SCOPE_QUEUE

#: The Project the e2e capture stores into (display name; slug "e2e-m13").
E2E_PROJECT = "E2E M13"

#: The true field center of the synthetic star field: M 13 itself (deg), so
#: the captured frames' RA/DEC header cards and the solve's center hint agree.
E2E_CRVAL = (M13.ra_deg, M13.dec_deg)

#: The picks passed to photometry_run (x = column, y = row) — one target star
#: plus three constant comparison stars of the synthetic field.
E2E_PICKS = [
    {"role": "target", "label": "M 13", "x": 100.0, "y": 60.0},
    {"role": "comp", "label": "Comp A", "x": 30.0, "y": 30.0},
    {"role": "comp", "label": "Comp B", "x": 120.0, "y": 30.0},
    {"role": "comp", "label": "Comp C", "x": 60.0, "y": 90.0},
]

#: The default aperture for the e2e photometry run.
E2E_APERTURE = {"radius_px": 4.0, "sky_in_px": 8.0, "sky_out_px": 12.0}


# --- synthetic star-field frames served by the mock Scope -------------------


def _e2e_true_wcs() -> WCS:
    """The TRUE WCS of the synthetic frames: TAN, 0.001 deg/px, centered on M 13.

    Same construction as ``test_compute_tools._solve_true_wcs`` but with the
    field center at M 13 — so the provisional WCS the Gaia solver builds from
    the center hint + pixel scale hint is exactly the true one, and the
    detected stars match the offline catalog perfectly.
    """
    return WCS(
        {
            "CTYPE1": "RA---TAN",
            "CTYPE2": "DEC--TAN",
            "CRVAL1": E2E_CRVAL[0],
            "CRVAL2": E2E_CRVAL[1],
            "CRPIX1": (SOLVE_SIZE + 1) / 2.0,
            "CRPIX2": (SOLVE_SIZE + 1) / 2.0,
            "CDELT1": -SOLVE_SCALE,
            "CDELT2": SOLVE_SCALE,
        }
    )


def _e2e_image(seed: int) -> np.ndarray:
    """One synthetic frame: background + the fixed Gaussian stars + noise."""
    rng = np.random.default_rng(seed)
    yy, xx = np.mgrid[0:SOLVE_SIZE, 0:SOLVE_SIZE]
    data = np.full((SOLVE_SIZE, SOLVE_SIZE), SOLVE_BACKGROUND, dtype=np.float64)
    for cy, cx, peak in SOLVE_STARS:
        data = data + peak * np.exp(
            -(((yy - cy) ** 2 + (xx - cx) ** 2) / (2.0 * SOLVE_STAR_SIGMA**2))
        )
    data = data + rng.normal(0.0, SOLVE_NOISE, size=(SOLVE_SIZE, SOLVE_SIZE))
    return np.ascontiguousarray(data, dtype=np.float32)


def _starfield_fits_bytes(
    *,
    when: datetime,
    exposure: float,
    gain: int,
    target: Mapping[str, Any] | None = None,
    seed: int = 0,
    size: tuple[int, int] = (16, 16),
) -> bytes:
    """Replacement for the mock's ``make_fits_bytes``: a solvable star field.

    Same header contract as the original (``DATE-OBS`` / ``EXPTIME`` /
    ``GAIN``, plus ``OBJECT`` / ``RA`` / ``DEC`` when a target is set), but
    the image is the synthetic star field and ``RA`` is written in DEGREES
    (the wire carries RA in hours) so the solver's header center hint is the
    true field center. Patched onto the mock module for the duration of the
    e2e test — the mock runs in this process, so every frame the capture Job
    downloads carries this image.
    """
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    hdu = pyfits.PrimaryHDU(data=_e2e_image(seed))
    hdu.header["DATE-OBS"] = link_frames.iso_utc(when)
    hdu.header["EXPTIME"] = float(exposure)
    hdu.header["GAIN"] = int(gain)
    if target:
        if target.get("name") is not None:
            hdu.header["OBJECT"] = str(target["name"])
        if "ra" in target:
            hdu.header["RA"] = (float(target["ra"]) * 15.0) % 360.0  # hours -> deg
        if "dec" in target:
            hdu.header["DEC"] = float(target["dec"])
    buf = io.BytesIO()
    pyfits.HDUList([hdu]).writeto(buf)
    return buf.getvalue()


def _e2e_gaia_catalog(path: Path) -> Path:
    """The offline Gaia catalog: synthetic stars at the TRUE sky positions of
    the injected image stars (so the solve needs no network)."""
    wcs = _e2e_true_wcs()
    rows = []
    for index, (cy, cx, _peak) in enumerate(SOLVE_STARS):
        coord = wcs.pixel_to_world(cx, cy)
        rows.append(
            {
                "source_id": 2000 + index,
                "ra_deg": coord.ra.deg,
                "dec_deg": coord.dec.deg,
                "g": 12.0,
            }
        )
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


# --- server subprocess configured like the operator's agent-spawned server --


def _e2e_server_params(
    scope_key_path: Path,
    address: str,
    project_root: Path,
    cache_dir: Path,
    *,
    allow_slew: bool = True,
) -> StdioServerParameters:
    """Stdio params for the real MCP server with the e2e environment."""
    env: dict[str, str] = {
        "SEESTAR_MCP_SCOPE_IP": address,
        "SEESTAR_PROJECT_ROOT": str(project_root),
        "SEESTAR_CACHE_DIR": str(cache_dir),
    }
    if allow_slew:
        env["SEESTAR_MCP_ALLOW_SLEW"] = "1"
    params = _server_params_with_redis(scope_key_path, env)
    assert params.env is not None
    if not allow_slew:
        params.env.pop("SEESTAR_MCP_ALLOW_SLEW", None)
    return params


# --- the Slew gate, the other way: always-on, no redis ------------------------


def test_e2e_goto_refused_without_slew_flag(
    mock_scope: MockScope, scope_key_path: Path, tmp_path: Path
) -> None:
    """The same chain's ``goto`` is refused without ``SEESTAR_MCP_ALLOW_SLEW=1``.

    The server subprocess is spawned exactly like in the e2e test (mock
    address in ``SEESTAR_MCP_SCOPE_IP``, tmp Project root, seeded catalog
    cache) but WITHOUT the Slew flag: ``connect`` still works (not gated),
    while ``goto("M13")`` raises a tool error naming the flag — before any
    catalog lookup or Job submission, so this needs no redis and leaves the
    shared Scope queue clean.
    """
    address = f"127.0.0.1:{mock_scope.command_port}"
    cache_dir = tmp_path / "catalog-cache"
    _seed_catalog_cache(cache_dir, "M13", M13)

    async def run() -> None:
        params = _e2e_server_params(
            scope_key_path,
            address,
            tmp_path / "projects",
            cache_dir,
            allow_slew=False,
        )
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            # connect is not a Slew: it succeeds against the mock
            res = await session.call_tool("connect", {})
            assert not res.isError
            assert _payload(res)["reachable"] is True

            # goto IS a Slew: refused by the gate (ADR-0003), no Job submitted
            res = await session.call_tool(
                "goto", {"idempotency_key": _key("e2e-gate"), "target": "M13"}
            )
            assert res.isError
            text = _error_text(res)
            assert "SEESTAR_MCP_ALLOW_SLEW" in text
            assert "goto refused" in text

    asyncio.run(run())
    # the gate fired before anything moved: the mock never slewed
    assert mock_scope.last_goto is None


# --- the full chain: redis + mock_scope + real scope/compute workers ----------


@requires_redis
def test_point_at_m13_and_measure_it_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    mock_scope: MockScope,
    scope_key_path: Path,
    scope_worker: subprocess.Popen[str],
    celery_worker: subprocess.Popen[str],
    tmp_path: Path,
) -> None:
    """One test drives the whole chain through the Tools and asserts the
    Project db holds the Measurements — the README goal end to end.

    connect -> goto "M13" (k1) -> capture (k2) -> solve_field on a captured
    frame (k3) -> photometry_run on the capture session (k4), each Job polled
    to SUCCESS via ``job_status`` and read with ``job_result``; finally the
    tmp Project is opened directly and ``store.measurements`` must list one
    Measurement per captured sub-frame.
    """
    # the mock's stacked sub-frames become solvable/measurable star fields
    monkeypatch.setattr(mock_scope_module, "make_fits_bytes", _starfield_fits_bytes)
    mock_scope.slew_duration = 0.3
    address = f"127.0.0.1:{mock_scope.command_port}"
    project_root = tmp_path / "projects"
    cache_dir = tmp_path / "catalog-cache"
    _seed_catalog_cache(cache_dir, "M13", M13)  # "M13" resolves offline
    catalog = _e2e_gaia_catalog(tmp_path / "gaia-catalog.json")
    expected_ra_h = M13.ra_deg / 15.0

    async def run() -> dict[str, Any]:
        params = _e2e_server_params(
            scope_key_path, address, project_root, cache_dir, allow_slew=True
        )
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()

            # 1. connect — the address comes from SEESTAR_MCP_SCOPE_IP
            res = await session.call_tool("connect", {})
            assert not res.isError
            conn = _payload(res)
            assert conn["reachable"] is True
            assert conn["address"] == address
            assert conn["verified"] is True  # PEM-signed handshake ok

            # 2. goto "M13" (k1): catalog-resolved slew on the Scope queue
            k1 = _key("e2e-goto")
            res = await session.call_tool(
                "goto", {"idempotency_key": k1, "target": "M13"}
            )
            assert not res.isError
            submitted = _payload(res)
            assert submitted["idempotency_key"] == k1
            assert submitted["queue"] == SCOPE_QUEUE
            assert submitted["address"] == address
            assert submitted["goto"]["name"] == "M 13"
            assert submitted["goto"]["ra"] == pytest.approx(expected_ra_h)
            assert submitted["goto"]["dec"] == pytest.approx(M13.dec_deg)
            status = await _poll_until_done(session, k1)
            assert status["state"] == "SUCCESS", status
            assert status["error"] is None
            goto_result = await _job_result(session, k1)
            assert goto_result["requested"]["name"] == "M 13"
            assert goto_result["ra"] == pytest.approx(expected_ra_h)
            assert goto_result["dec"] == pytest.approx(M13.dec_deg)
            assert goto_result["slewing"] is False

            # 3. capture (k2): sub-frames at the M 13 pointing, stored in the Project
            k2 = _key("e2e-capture")
            res = await session.call_tool(
                "capture",
                {
                    "idempotency_key": k2,
                    "frames": 3,
                    "exposure": 1.0,
                    "gain": 80,
                    "project": E2E_PROJECT,
                },
            )
            assert not res.isError
            assert _payload(res)["queue"] == SCOPE_QUEUE
            status = await _poll_until_done(session, k2, timeout_s=120.0)
            assert status["state"] == "SUCCESS", status
            capture_result = await _job_result(session, k2)
            assert capture_result["count"] >= 2  # frames>=2
            assert capture_result["project"] == "e2e-m13"
            project_path = Path(capture_result["project_path"])
            assert project_path == project_root / "e2e-m13"
            for frame in capture_result["frames"]:
                assert (project_path / frame["path"]).is_file()  # FITS in the Project
            session_id = capture_result["session_id"]
            frame_id = capture_result["frames"][0]["id"]

            # 4. solve_field on a captured frame (k3): Compute queue, WCS persisted
            k3 = _key("e2e-solve")
            res = await session.call_tool(
                "solve_field",
                {
                    "idempotency_key": k3,
                    "project": E2E_PROJECT,
                    "session": session_id,
                    "frame_id": frame_id,
                    "solver": "gaia",
                    "gaia_catalog": str(catalog),
                    "center_hint": [M13.ra_deg, M13.dec_deg],
                    "pixel_scale_hint": SOLVE_SCALE * 3600.0,
                },
            )
            assert not res.isError
            assert _payload(res)["queue"] == COMPUTE_QUEUE
            status = await _poll_until_done(session, k3, timeout_s=180.0)
            assert status["state"] == "SUCCESS", status
            solve_result = await _job_result(session, k3)
            assert solve_result["solver"] == "gaia"
            assert solve_result["n_stars"] == len(SOLVE_STARS)
            assert solve_result["wcs_stored"] is True
            assert solve_result["project"] == "e2e-m13"
            assert solve_result["frame_id"] == frame_id
            assert solve_result["session_id"] == session_id
            assert solve_result["rms_arcsec"] is not None
            assert solve_result["rms_arcsec"] < 1.0
            sep_arcsec = math.hypot(
                (solve_result["center_ra_deg"] - E2E_CRVAL[0])
                * 3600.0
                * math.cos(math.radians(E2E_CRVAL[1])),
                (solve_result["center_dec_deg"] - E2E_CRVAL[1]) * 3600.0,
            )
            assert sep_arcsec < 5.0  # solved at M 13's position

            # 5. photometry_run on the capture session (k4): measurements stored
            k4 = _key("e2e-photo")
            res = await session.call_tool(
                "photometry_run",
                {
                    "idempotency_key": k4,
                    "project": E2E_PROJECT,
                    "session": session_id,
                    "picks": E2E_PICKS,
                    "aperture": E2E_APERTURE,
                },
            )
            assert not res.isError
            assert _payload(res)["queue"] == COMPUTE_QUEUE
            status = await _poll_until_done(session, k4, timeout_s=180.0)
            assert status["state"] == "SUCCESS", status
            photo_result = await _job_result(session, k4)
            assert photo_result["project"] == "e2e-m13"
            assert photo_result["session_id"] == session_id
            assert photo_result["measured"] == capture_result["count"]
            assert photo_result["skipped"] == []
            assert len(photo_result["measurements"]) == capture_result["count"]
            for row in photo_result["measurements"]:
                assert row["session_id"] == session_id
                assert math.isfinite(row["mag"])
                assert row["mag_err"] is not None and row["mag_err"] > 0.0
                assert row["excluded"] is False

            return {
                "capture": capture_result,
                "solve": solve_result,
                "photo": photo_result,
                "session_id": session_id,
                "frame_id": frame_id,
            }

    outcome = asyncio.run(run())

    # 6. the mock Scope really slewed to M 13, and no Link is held afterwards
    assert mock_scope.last_goto is not None
    assert mock_scope.last_goto["ra"] == pytest.approx(expected_ra_h)
    assert mock_scope.last_goto["dec"] == pytest.approx(M13.dec_deg)
    assert mock_scope.scope_state()["target"]["name"] == "M 13"
    _assert_no_held_connections(mock_scope)

    # 7. the Project db itself: frames, the persisted WCS, and the Measurements
    project = store_project.open_project(project_root / "e2e-m13")
    try:
        subs = store_frames.list_frames(project, session_id=outcome["session_id"], kind="sub")
        assert len(subs) == outcome["capture"]["count"]
        assert [f.id for f in subs] == [f["id"] for f in outcome["capture"]["frames"]]

        wcs = store_wcs.load_frame_wcs(project, outcome["frame_id"])
        assert wcs is not None  # WCS persisted by the solve Job
        assert wcs.solver == "gaia"
        assert wcs.n_stars == len(SOLVE_STARS)
        assert wcs.session_id == outcome["session_id"]

        lc = store_measurements.lightcurve(project, include_excluded=True)
        assert len(lc) == outcome["capture"]["count"]  # Measurements exist in the Project
        assert [m.frame_id for m in lc] == [f["id"] for f in outcome["capture"]["frames"]]
        assert all(m.session_id == outcome["session_id"] for m in lc)
        for meas in lc:
            assert math.isfinite(meas.mag)
            assert meas.mag_err is not None and meas.mag_err > 0.0
        for frame in outcome["capture"]["frames"]:
            assert len(store_measurements.measurements_for_frame(project, frame["id"])) == 1
    finally:
        project.close()
