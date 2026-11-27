# Serialize Scope access: one consumer, per-job connections

Status: accepted

The Scope is one physical device, so Jobs that touch the Link (`goto`,
`capture`, frame download) go to a dedicated scope queue with exactly one
consumer. Each such Job opens a short-lived connection inside the worker
(connect, operate, disconnect) — the MCP server (host process) and workers
(containers) cannot share a socket, and a persistent shared connection would
couple the server's lifetime to the Scope's. Jobs that only process
already-downloaded frames (solve, photometry, stacking) go to a parallel
compute queue and never touch the Link.

## Considered options

- Parallel connections from multiple workers: rejected, concurrent command
  channels risk conflicting state on the device.
- One long-lived connection held by the MCP server: rejected, not shareable
  across processes and a single point of failure for all Jobs.

## Consequences

- The scope queue is the throughput bottleneck for observing by design;
  scaling means faster hardware, not more workers, for Link-touching Jobs.
- The MCP `connect` Tool only verifies reachability and pins the scope
  address (`SEESTAR_MCP_SCOPE_IP`, overridable per call); it does not hold a
  connection open.
