# Slew operations gated behind an explicit operator opt-in

Status: accepted

`goto` and `park` — the Tools that move the Scope — are refused unless the
environment variable `SEESTAR_MCP_ALLOW_SLEW=1` is set. All other Tools
(`connect`, `status`, `capture`, `solve_field`, `photometry_run`, `job_status`,
`job_result`) are always available: capturing at the current pointing is
harmless, pointing the hardware elsewhere unsupervised is not. The gate is an
environment flag rather than a per-call confirmation because MCP tool calls
are agent-initiated and there is no human in the loop to confirm each one.

## Consequences

- "Point at M13 and measure it" requires the operator to start the server with
  the flag set; without it the agent can only observe the current pointing.
