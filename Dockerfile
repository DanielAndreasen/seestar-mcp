# syntax=docker/dockerfile:1
#
# Worker image for seestar-mcp. The build context is the PARENT directory of
# this repo (docker-compose.yml sets `context: ..`), so both sibling
# checkouts are visible to COPY:
#
#   <context>/                 <- parent dir of the seestar-mcp clone
#     seestar-controller/      <- ADR-0001 path dependency (uv resolves ../seestar-controller)
#     seestar-mcp/             <- this repo (this Dockerfile lives here)
#
# Build from the seestar-mcp repo root with:  docker compose build worker

FROM python:3.14-slim

# uv installs the locked dependency set (uv.lock) into the project venv.
RUN pip install --no-cache-dir uv
# Use the image's own Python 3.14 instead of downloading a managed one.
ENV UV_PYTHON=/usr/local/bin/python

WORKDIR /app
COPY seestar-controller/ /app/seestar-controller/
COPY seestar-mcp/ /app/seestar-mcp/

WORKDIR /app/seestar-mcp
# Runtime deps only (no dev group: the image runs the worker, not pytest).
RUN uv sync --locked --no-dev

# Wired to the compose `redis` service by docker-compose.yml.
ENV REDIS_URL=redis://redis:6379/0

# Worker entrypoint: starts the scope-queue worker (concurrency 1) and the
# compute-queue worker in one container (ADR-0002 + ADR-0004).
CMD [".venv/bin/python", "-m", "seestar_mcp.worker"]
