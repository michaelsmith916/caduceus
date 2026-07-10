# Hermes plugin specification

Status: implemented  
Plugin version: 0.2.0  
Verified host: Hermes Agent v0.18.2 (`v2026.7.7.2`)

## Purpose

The plugin connects Hermes-native guidance and diagnostics to the existing
Caduceus MCP integration without reimplementing Caduceus tools in Python.

## Package contract

The installable directory contains:

- `plugin.yaml`: Hermes manifest version 1 for the `caduceus` standalone plugin.
- `__init__.py`: dependency-free `register(ctx)` entry point.
- `skills/remote-prompt-delegation/SKILL.md`: packaged, instruction-only delegation
  workflow.
- `after-install.md`: setup guidance displayed by the Hermes installer.
- `LICENSE`: MIT terms retained in the installed subdirectory package.

Hermes installs the directory with:

```bash
hermes plugins install michaelsmith916/caduceus/hermes/plugin --enable
```

## Runtime contract

On registration the plugin must:

1. Register `caduceus:remote-prompt-delegation` with `ctx.register_skill`.
2. Register `/caduceus-status` with `ctx.register_command`.
3. Dispatch status only through
   `mcp__caduceus__caduceus_get_local_node_status` using `ctx.dispatch_tool`.
4. Return an actionable configuration message when that MCP tool is absent.

The required Hermes MCP server key is `caduceus`. Caduceus's advertised tool
names contain dots; Hermes v0.18.2 exposes them as
`mcp__caduceus__caduceus_<tool>`.

`caduceus-mcp` uses newline-delimited JSON-RPC on stdio, matching the MCP
transport consumed by Hermes's official Python MCP SDK. Legacy
`Content-Length` requests are accepted only for backward compatibility.

## Security invariants

- Plugin import and registration perform no network, process, or filesystem
  mutations.
- The plugin never receives or stores Caduceus credentials.
- The skill requires task validation and an explicitly trusted worker before
  execution.
- The plugin never replaces or overrides built-in Hermes tools.

## Non-goals

- Reimplementing MCP or Caduceus control APIs in Python.
- Starting, stopping, installing, or updating Caduceus processes.
- Silently editing an operator's Hermes configuration.
- Sending tasks automatically from a lifecycle hook.

## Acceptance checks

- The manifest parses as version 1 and names the plugin `caduceus`.
- Hermes loads the plugin without errors and records one skill and one command.
- `/caduceus-status` dispatches the expected MCP tool with empty arguments.
- The packaged and standalone skill copies are byte-identical.
- Repository tests and `git diff --check` pass.

## Normative references

- [Hermes Agent v0.18.2 release](https://github.com/NousResearch/hermes-agent/releases/tag/v2026.7.7.2)
- [Hermes plugin loader contract](https://github.com/NousResearch/hermes-agent/blob/v2026.7.7.2/hermes_cli/plugins.py)
- [Hermes subdirectory installer](https://github.com/NousResearch/hermes-agent/blob/v2026.7.7.2/hermes_cli/plugins_cmd.py)
- [Hermes MCP tool naming](https://github.com/NousResearch/hermes-agent/blob/v2026.7.7.2/tools/mcp_tool.py)
- [MCP stdio transport](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
