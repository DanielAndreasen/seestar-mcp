"""MCP Tool groups for seestar-mcp (one module per Tool group, per the spec).

Each group module exposes ``register(mcp)`` adding its Tools to the FastMCP
app; :func:`register_all` wires every group into ``seestar_mcp.server``.
"""

from mcp.server.fastmcp import FastMCP

from seestar_mcp.tools import compute as compute
from seestar_mcp.tools import jobs as jobs
from seestar_mcp.tools import link as link
from seestar_mcp.tools import scope as scope

__all__ = ["compute", "jobs", "link", "register_all", "scope"]


def register_all(mcp: FastMCP) -> None:
    """Register every Tool group on the FastMCP app."""
    link.register(mcp)
    jobs.register(mcp)
    scope.register(mcp)
    compute.register(mcp)
