# MCP

`caduceus-mcp` is a native stdio MCP server. It speaks JSON-RPC over `Content-Length` framed stdio and proxies calls to the local daemon.

On Windows, run the MCP smoke test while `caduceusd.exe` is running:

ie
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
