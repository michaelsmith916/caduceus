# MCP

`caduceus-mcp` is a native stdio MCP server. It speaks newline-delimited
JSON-RPC as required by the
[MCP stdio transport](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
and proxies calls to the local daemon. Legacy `Content-Length` framed clients
remain supported.

Hermes Agent v0.18.2 prefixes MCP tools as
`mcp__<server>__<sanitized_tool>`. With the required `caduceus` server key,
`caduceus.get_local_node_status` is exposed as
`mcp__caduceus__caduceus_get_local_node_status`. See the
[Hermes integration guide](../hermes/README.md).

On Windows, run the MCP smoke test while `caduceusd.exe` is running:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File C:\path\to\test-mcp.ps1 `
  -McpExe "C:\path\to\caduceus-mcp.exe" `
  -Config "C:\path\to\config.yaml"
```

The script launches `caduceus-mcp.exe` as an MCP client would, completes the
handshake, lists tools, and calls `caduceus.get_local_node_status` to verify the
MCP server can reach the daemon. Use `-McpExe` or `-Config` when using
non-default paths.

Tools:

- `caduceus.list_workers`
- `caduceus.get_worker`
- `caduceus.run_remote_task`
- `caduceus.run_remote_prompt`
- `caduceus.list_tasks`
- `caduceus.get_task_status`
- `caduceus.get_task_result`
- `caduceus.get_task_events`
- `caduceus.get_task_artifacts`
- `caduceus.cancel_task`
- `caduceus.get_local_node_status`
- `caduceus.get_local_config`
- `caduceus.validate_task`

Resources:

- `caduceus://local/status`
- `caduceus://local/config`
- `caduceus://workers`
- `caduceus://workers/{worker_id}`
- `caduceus://tasks/{task_id}`
- `caduceus://tasks/{task_id}/events`
- `caduceus://tasks/{task_id}/artifacts/{artifact_id}`
