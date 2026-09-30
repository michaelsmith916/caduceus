"""Hermes Agent plugin for Caduceus."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any


SKILL_NAME = "remote-prompt-delegation"
SKILL_PATH = (
    Path(__file__).parent / "skills" / "remote-prompt-delegation" / "SKILL.md"
)
STATUS_TOOL = "mcp__caduceus__caduceus_get_local_node_status"


def _load_enrollment_v1() -> Any:
    """Load the packaged adapter by exact path under every Hermes loader."""
    path = Path(__file__).parent / "enrollment_v1.py"
    spec = importlib.util.spec_from_file_location(
        "caduceus_hermes_enrollment_v1", path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the Caduceus enrollment adapter")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_ENROLLMENT_V1 = _load_enrollment_v1()
EnrollmentV1Adapter = _ENROLLMENT_V1.EnrollmentV1Adapter
ENROLLMENT_LIST_TOOL = _ENROLLMENT_V1.LIST_TOOL
ENROLLMENT_APPROVE_TOOL = _ENROLLMENT_V1.APPROVE_TOOL
ENROLLMENT_DENY_TOOL = _ENROLLMENT_V1.DENY_TOOL


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
    """Register packaged guidance and explicitly invoked MCP commands."""
    enrollment = EnrollmentV1Adapter(ctx)
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
    ctx.register_command(
        name="caduceus-enrollments",
        handler=enrollment.handle,
        description="Review and decide pending Caduceus enrollment requests.",
    )
