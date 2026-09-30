# MCP

`caduceus-mcp` is a native stdio MCP server. It uses newline-delimited JSON-RPC for the [MCP stdio transport](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) and proxies calls to the local daemon. Legacy `Content-Length` framed clients remain supported.

Hermes Agent v0.18.2 prefixes MCP tools as `mcp__<server>__<sanitized_tool>`. With the required `caduceus` server key, `caduceus.get_local_node_status` appears as `mcp__caduceus__caduceus_get_local_node_status`. See the [Hermes integration guide](../hermes/README.md).

## Tools

Worker and routing:

- `caduceus.list_workers`
- `caduceus.get_worker`
- `caduceus.explain_route` — evaluates and explains a task without dispatching it
- `caduceus.validate_task`

Task operations:

- `caduceus.run_remote_task`
- `caduceus.run_remote_prompt`
- `caduceus.list_tasks`
- `caduceus.get_task_status`
- `caduceus.get_task_result`
- `caduceus.get_task_events`
- `caduceus.get_task_artifacts`
- `caduceus.cancel_task`

Local node:

- `caduceus.get_local_node_status`
- `caduceus.get_local_config`

Enrollment administration:

- `caduceus.list_enrollment_requests` with `{}`
- `caduceus.approve_enrollment` with `{"request_id":"<id>"}`
- `caduceus.deny_enrollment` with `{"request_id":"<id>"}`

Enrollment tools return sanitized metadata only. Invitation tokens, routing/shared secrets, prompts, results, and local activity details are not exposed in enrollment responses. Peer-supplied display names are untrusted data. Approval should be an explicit user decision after verifying the peer identity/fingerprint.

Hermes exposes enrollment as the invoked command `/caduceus-enrollments [list|approve <id>|deny <id>]`. It polls only when invoked; there is no supported unsolicited enrollment notification hook.

## Resources

- `caduceus://local/status`
- `caduceus://local/config`
- `caduceus://workers`
- `caduceus://workers/{worker_id}`
- `caduceus://tasks/{task_id}`
- `caduceus://tasks/{task_id}/events`
- `caduceus://tasks/{task_id}/artifacts/{artifact_id}`

## Windows smoke test

While `caduceusd.exe` is running:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File C:\path\to\test-mcp.ps1 `
  -McpExe "C:\path\to\caduceus-mcp.exe" `
  -Config "C:\path\to\config.yaml"
```

The script completes the MCP handshake, lists tools, and calls `caduceus.get_local_node_status` to verify that the MCP process can reach the daemon.
