# seestar-mcp

MCP (Model Context Protocol) server that exposes the Seestar S50 smart
telescope as tools for LLM agents.

Status: **scaffolded** — uv-managed Python 3.14 project (src layout, path
dependency on the sibling `seestar-controller` repo, FastMCP from the MCP
Python SDK, celery + redis, pytest-xdist dev group, ty/ruff configured).
The MCP Tools themselves are not implemented yet.

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

## Open questions

- Import seestar-controller as a library vs. subprocess its CLI — decide when
  looking at its packaging.
- Which tools to expose first: `connect`, `capture`, `solve_field`,
  `photometry_run`, `job_status`.
- Auth/safety: MCP server on LAN only; agents must not slew the scope
  unsupervised.
