"""Hermes Agent plugin for Caduceus."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


SKILL_NAME = "remote-prompt-delegation"
SKILL_PATH = (
    Path(__file__).parent / "skills" / "remote-prompt-delegation" / "SKILL.md"
)
STATUS_TOOL = "mcp__caduceus__caduceus_get_local_node_status"


def _status(ctx: Any, raw_args: str = "") -> str:
    """Return Caduceus daemon status through Hermes's MCP registry."""
    if raw_args.strip():
        return "Usage: /caduceus-status"

    result = ctx.dispatch_tool(STATUS_TOOL, {})
    if not isinstance(result, str):
        return str(result)

    try:
        payload = json.loads(result)
    except (TypeError, json.JSONDecodeError):
        return result

    error = payload.get("error")
    if error:
        if str(error).startswith("Unknown tool:"):
            return (
                "Caduceus MCP is not registered. Configure "
                "mcp_servers.caduceus.command, run /reload-mcp, and retry."
            )
        return f"Caduceus status failed: {error}"

    structured = payload.get("structuredContent")
    if structured is not None:
        return json.dumps(structured, indent=2, ensure_ascii=False)
    return str(payload.get("result", result))


def register(ctx: Any) -> None:
    """Register the packaged skill and connectivity command with Hermes."""
    ctx.register_skill(
        name=SKILL_NAME,
        path=SKILL_PATH,
        description="Delegate prompts to trusted Caduceus LAN workers.",
    )
    ctx.register_command(
        name="caduceus-status",
        handler=lambda raw_args: _status(ctx, raw_args),
        description="Check Caduceus daemon connectivity through MCP.",
    )
