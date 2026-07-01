# Caduceus and Hermes

Caduceus does not require Hermes. The normal integration path is MCP: configure Hermes to launch `caduceus-mcp`, then call the standard Caduceus MCP tools.

Example MCP server config:

```json
{
  "mcpServers": {
    "caduceus": {
      "command": "caduceus-mcp",
      "args": []
    }
  }
}
```

The plugin folder is a lightweight placeholder for Hermes installations that prefer plugin-managed tool wrappers. The Phase I recommendation is still to use MCP directly.
