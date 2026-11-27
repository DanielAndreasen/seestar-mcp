# seestar-mcp

MCP server that exposes the Seestar S50 as tools for LLM agents. Vocabulary
shared with the controller (Scope, Link, Sub-frame, Project, Measurement,
Lightcurve, ...) is inherited from seestar-controller's GLOSSARY.md and not
redefined here.

## Language

**Tool**:
One operation the MCP server exposes to the agent (`connect`, `status`, `goto`,
`capture`, `solve_field`, `photometry_run`, `job_status`, `job_result`).
_Avoid_: command, endpoint, RPC

**Job**:
One unit of long-running work (WCS solve, photometry, stacking) submitted
asynchronously instead of blocking a Tool call.
_Avoid_: task, request, execution

**Idempotency key**:
A client-chosen string identifying a Job. Resubmitting the same key refers to
the existing Job instead of starting a new one, so transient failures are safe
to retry.
_Avoid_: job id, request id, correlation id

**Slew**:
Any operation that changes the Scope's pointing (`goto`, `park`). Refused
unless explicitly enabled by the operator.
_Avoid_: move, repoint

**Scope queue**:
The serialized queue carrying Jobs that touch the Link. Exactly one consumer,
so only one Job talks to the Scope at a time.
_Avoid_: device queue, hardware queue

**Compute queue**:
The parallel queue for Jobs that only process already-downloaded data (solve,
photometry, stacking) and never touch the Link.
_Avoid_: default queue, worker pool
