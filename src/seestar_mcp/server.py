"""FastMCP server app for seestar-mcp.

stdio transport only (ADR-0002): the server is spawned by the agent client
(e.g. Claude Code's MCP config), never listens on the network. Tools are
added in later tickets; this module is the runnable app skeleton.
"""

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("seestar-mcp")


def main() -> None:
    """Run the MCP server over stdio."""
    mcp.run()


if __name__ == "__main__":
    main()
