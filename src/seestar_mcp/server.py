"""FastMCP server app for seestar-mcp.

stdio transport only (ADR-0002): the server is spawned by the agent client
(e.g. Claude Code's MCP config), never listens on the network. Tool groups
live in :mod:`seestar_mcp.tools` (one module per group) and are registered
on the app below.
"""

from mcp.server.fastmcp import FastMCP

from seestar_mcp.tools import register_all

mcp = FastMCP("seestar-mcp")

# Tool groups (spec "Modules": one module per Tool group): the read-only Link
# Tools connect / status (issue #6), the Job plumbing job_status /
# job_result (issue #5), the scope-mutating Tools goto / park / capture
# (issue #7, Slew-gated per ADR-0003) and the compute Tools solve_field /
# photometry_run (issue #8, on the Compute queue per ADR-0004).
register_all(mcp)


def main() -> None:
    """Run the MCP server over stdio."""
    mcp.run()


if __name__ == "__main__":
    main()
