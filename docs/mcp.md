# MCP

`caduceus-mcp` is a native stdio MCP server. It speaks JSON-RPC over `Content-Length` framed stdio and proxies calls to the local daemon.

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
