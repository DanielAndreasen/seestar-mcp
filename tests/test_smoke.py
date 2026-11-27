"""Smoke tests for the scaffold: the venv resolves the declared dependencies."""

from importlib.metadata import version

import seestar_controller

import seestar_mcp
from seestar_mcp.server import mcp


def test_seestar_controller_importable() -> None:
    """Acceptance criterion: `import seestar_controller` works in the venv."""
    assert version("seestar-controller")
    assert seestar_controller.__name__ == "seestar_controller"


def test_package_importable() -> None:
    assert version("seestar-mcp")
    assert seestar_mcp.__name__ == "seestar_mcp"


def test_server_app_constructed() -> None:
    """The FastMCP app from the official MCP SDK exists and is named."""
    assert mcp.name == "seestar-mcp"
