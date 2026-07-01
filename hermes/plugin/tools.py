"""Hermes plugin placeholder.

Use Hermes MCP configuration to launch `caduceus-mcp`. This module documents
the intended wrapper boundary without introducing a hard dependency on a
specific Hermes plugin API in Phase I.
"""


def describe():
    return {
        "name": "caduceus",
        "mode": "mcp",
        "command": "caduceus-mcp",
    }
