# stdio transport only; no network listener

Status: accepted

The MCP server speaks stdio and is spawned by the agent client (e.g. Claude
Code's MCP config). A streamable-HTTP listener on the LAN was considered and
rejected for v1: it would expose scope control to anything on the network, and
the use case is a local agent, not a remote service. Only redis and the Celery
workers run as networked services (in Docker Compose); the MCP server itself
runs on the host via `uv run`.

## Consequences

- No bind-address/port configuration exists to get wrong; adding HTTP later is
  additive and does not change the Tool surface.
- Docker Compose contains `redis` + `worker` only, never the MCP server.
