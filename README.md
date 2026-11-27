# seestar-mcp

MCP (Model Context Protocol) server that exposes the Seestar S50 smart
telescope as tools for LLM agents.

Status: **v1 Tools in progress** — uv-managed Python 3.14 project (src layout,
path dependency on the sibling `seestar-controller` repo, FastMCP from the
MCP Python SDK, celery + redis, pytest-xdist dev group, ty/ruff configured).
Implemented so far: the read-only Link Tools `connect` / `status`, the Job
plumbing Tools `job_status` / `job_result`, the scope-mutating Tools
`goto` / `park` / `capture`, and the compute Tools `solve_field` /
`photometry_run` / `hr_diagram`. Example agent prompts live in
[`examples/prompts.md`](examples/prompts.md).

Related project: [`seestar-controller`](https://github.com/DanielAndreasen/seestar-controller) —
the desktop controller whose `link` / `astrometry` / `photometry` / `analysis`
modules this server wraps as MCP tools.

## Link Tools (`connect`, `status`)

Read-only Tools over seestar-controller's `link` layer (ADR-0004: each Tool
opens a short-lived Link connection and closes it again — the server never
holds a connection to the Scope):

- `connect(ip?)` — verifies the Scope is reachable (signed handshake) and
  **pins the scope address** in server-side state for later Tools; returns
  reachability plus basic identity (address, connection state, current mode
  and target), or `reachable: false` with an error. `ip` is an optional
  per-call override (`host` or `host:port`, default port 4700).
- `status(ip?)` — reads the Scope's current state (connection state, mode,
  current target RA/Dec + name, stacking progress) with no side effects;
  raises a tool error when the Scope is unreachable.

The scope address defaults to `SEESTAR_MCP_SCOPE_IP` (default
`192.168.87.185`, the S50 in station mode) and is overridden per call on
`connect`. The Link handshake key resolves as in seestar-controller
(`SEESTAR_KEY_PATH` or `~/.config/seestar-controller/key.pem`). Tests drive
both Tools over real MCP stdio against `link.mock_scope` on ephemeral ports
(`tests/test_link_tools.py`) — no redis, no hardware.

## Scope Tools (`goto`, `park`, `capture`)

The scope-mutating Tools submit **Jobs on the Scope queue** (`scope`, exactly
one consumer — ADR-0004) instead of blocking the Tool call; each Job opens a
**short-lived per-job Link** inside the worker (connect, operate, disconnect).
Every Tool takes a client-chosen `idempotency_key` and returns it; poll with
`job_status` / `job_result`:

- `goto(idempotency_key, target?, ra?, dec?, ip?, timeout?)` — slews the Scope
  to a target. `target` is a catalog name (e.g. `"M13"`, resolved to J2000
  RA/Dec via seestar-controller's SIMBAD catalog, cached + offline-graceful),
  or pass explicit `ra` (hours) / `dec` (degrees) — never both. The worker
  task issues `Device.goto` and waits for arrival.
- `park(idempotency_key, ip?)` — parks the Scope (same kind of Job).
- `capture(idempotency_key, frames?, exposure?, gain?, project?, ip?, timeout?)`
  — stacks `frames` sub-frames at the current pointing, downloads them via
  `link.frames` and stores the FITS in the Project (`project`, default
  `seestar-mcp`, under `SEESTAR_PROJECT_ROOT`) under the open session;
  `job_result` lists the stored frame ids/paths.

**Slew gate (ADR-0003)**: `goto` and `park` raise a clear error unless the
server was started with `SEESTAR_MCP_ALLOW_SLEW=1` (read once at server
start). `capture` is not gated — capturing at the current pointing is
harmless. The scope address resolves like the Link Tools (`ip` > pinned >
`SEESTAR_MCP_SCOPE_IP`) and is passed to the worker in the Job kwargs.

The worker-side tasks live under `seestar_mcp.jobs.scope.*`
(`src/seestar_mcp/jobs/scope.py`), so the existing task route sends them to
the Scope queue; `src/seestar_mcp/tools/scope.py` holds the Tools.
`tests/test_scope_tools.py` drives them over real MCP stdio: the Slew-gate
refusal always runs (no redis); goto/park/capture against `link.mock_scope`
with a real scope-queue worker and a tmp `SEESTAR_PROJECT_ROOT` run when
Redis is reachable (CI), including a test that two concurrent scope Jobs
never overlap on the Link.

## Compute Tools (`solve_field`, `photometry_run`, `hr_diagram`)

The compute Tools submit **Jobs on the Compute queue** (`compute`, the default
queue — ADR-0004; parallel consumers) instead of blocking the Tool call. The
worker tasks live under `seestar_mcp.jobs.compute.*`
(`src/seestar_mcp/jobs/compute.py`) and wrap seestar-controller's pure layers
(plate solving, photometry) over frames already stored in a Project; the
Tools live in `src/seestar_mcp/tools/compute.py`. Like every Job Tool they
take a client-chosen `idempotency_key` (== Celery task id, claimed in Redis
before sending) and return it; poll with `job_status` / `job_result`. Jobs
carry `project_root` in their kwargs, so any compute worker can run them.
`job_result` returns a small JSON-serializable summary (paths + key numbers) —
raw FITS stay on disk in the Project.

- `solve_field(idempotency_key, project?, session?, frame_id?, fits_path?,
  solver?, center_hint?, pixel_scale_hint?, gaia_catalog?, ...)` — solves one
  frame. Pass either a frame in a Project (`project` + `frame_id`, optionally
  scoped by `session` — the WCS is persisted via the project's `frame_wcs`
  store and written into the stored FITS header) or a plain `fits_path`
  (solved in place, nothing persisted). `solver` is one of
  seestar-controller's solvers (`auto`, `solve-field`, `gaia`; default
  `auto`); `center_hint` / `pixel_scale_hint` steer the solve, and
  `gaia_catalog` points at an offline JSON catalog file the worker can read
  (used by tests / air-gapped setups instead of the network Gaia query).
  `job_result` carries the WCS solution: solver, center RA/Dec, RMS, star
  count, WCS cards, and the frame path.
- `photometry_run(idempotency_key, project, session, picks?, aperture?,
  excluded_frames?)` — runs the photometry `RunController` over the
  session's sub-frames and stores one Measurement per frame in the Project
  (`aperture` = `{radius_px, sky_in_px, sky_out_px}`; `picks` = target +
  comp(/check) star picks `{role, label, x, y}` — saved as the project's
  star picks when given, otherwise the previously stored picks are used).
  `job_result` summarizes the run (measured / skipped / excluded frame ids,
  measurements with mag + mag_err) and `job_result` picks them up from the
  Project's lightcurve.
- `hr_diagram(idempotency_key, project, frame_id, session?, recompute?,
  gaia_catalog?, age?, feh?, dist_mod?)` — the H-R / CMD diagram data of one
  project stack: uses its persisted Gaia cross-match table (or recomputes it
  with `recompute=true` / an offline `gaia_catalog`; the frame must be
  plate-solved then). `job_result` carries the cross-match counts, cluster
  membership, the CMD points (G vs BP-RP), the H-R points (abs G vs BP-RP,
  parallax-SNR cut) and, when `age`/`feh`/`dist_mod` are all given, the
  PARSEC isochrone overlay.

`tests/test_compute_tools.py`: registration + queue-routing + submission
(idempotency key as task id, `queue="compute"`) and argument validation run
everywhere; the end-to-end Jobs (synthetic FITS frame solved against an
offline Gaia catalog -> `frame_wcs` row + FITS header; a 3-frame session ->
Measurements in the project db) run over real MCP stdio with a compute-queue
worker when Redis is reachable (CI), and never touch the network or the
`solve-field` binary.

## General idea

`seestar-controller` already speaks the telescope's JSON-RPC wire protocol and
has photometry/astrometry/analysis pipelines. Wrap that functionality as MCP
tools so an LLM agent (Claude Code, etc.) can drive an observing session:
connect to the scope, capture a frame, solve the field, run photometry —
through natural-language tool calls instead of a GUI.

## Goal

An agent can say "point at M13 and measure it" and the MCP server:

1. connects to the telescope over the home LAN,
2. submits long-running work (WCS solve, photometry, lightcurve, stacking) as
   **async jobs** instead of blocking the tool call,
3. runs those jobs on **Celery workers** with **Redis** as broker + result
   backend, with **retries and idempotency keys** (solving/photometry can fail
   transiently and must be safe to re-run),
4. lets the client poll job status and fetch results.

## Technologies

- Python 3.14, managed with `uv`
- MCP Python SDK (or FastMCP) for the server
- Reuse `seestar-controller` modules (`link`, `astrometry`, `photometry`,
  `analysis`) as a library, or shell out to its headless CLI
- Celery + Redis (async task queue, retries, idempotency, result backend)
- JSON-RPC (telescope wire protocol, via seestar-controller)
- Docker / Docker Compose for redis + worker + MCP server
- pytest, with seestar-controller's `mock_scope` for tests without hardware

## Jobs and deployment (Celery + Redis)

Long-running work is submitted as a **Job** to Celery with Redis as broker
and result backend (`REDIS_URL`, default `redis://localhost:6379/0`; results
expire after 24h). Two queues (ADR-0004):

- **Scope queue** (`scope`) — Jobs that touch the Link; exactly one consumer.
- **Compute queue** (`compute`) — Jobs on already-downloaded frames; parallel
  consumers, and the default queue.

`docker compose up` starts only `redis` + `worker` (ADR-0002); the worker
image is built from the repo's parent directory so it can copy both
`seestar-controller` and `seestar-mcp`. The worker entrypoint
(`python -m seestar_mcp.worker`) runs two Celery workers in one container:
`-Q scope --concurrency=1` and `-Q compute`. The MCP server itself runs on
the host via `uv run` over stdio. `tests/test_jobs.py` round-trips a demo
`echo` Job through real Redis (skipped locally when Redis is unreachable).

### Idempotency keys, retries, and the Job Tools

- **Idempotency key = Celery task id.** `seestar_mcp.jobs.submit_job(key,
  task_name, args, kwargs, queue)` submits a Job with `task_id=key` and
  returns `key`. The key is claimed atomically in Redis (`SET NX EX`, TTL 24h)
  before sending, so **resubmitting the same key returns the same Job without
  re-executing it** (Celery itself does not deduplicate by task id).
- **Retries.** Every Job runs on the `JobTask` base: raising
  `TransientJobError` triggers autoretry with exponential backoff
  (`2**retries` seconds, full jitter, capped at 10 min, max 5 retries), after
  which the Job ends in FAILURE with the exception as error info. Job results
  expire from Redis after 24h.
- **Tools.** `job_status(idempotency_key)` returns the Job's state
  (PENDING / RUNNING / SUCCESS / FAILURE, plus `error` on failure) via
  `AsyncResult`; `job_result(idempotency_key)` returns the Job's payload on
  SUCCESS (small, JSON-serializable by contract — FITS stay on disk in the
  Project) and a small status dict otherwise. Both live in
  `src/seestar_mcp/tools/jobs.py`; `tests/test_job_tools.py` drives them over
  real MCP stdio against real Redis.

## Open questions

- Auth/safety beyond stdio-only + the slew gate (ADR-0002/0003).
