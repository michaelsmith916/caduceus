RUN_REMOTE_PROMPT = {
    "type": "object",
    "properties": {
        "worker_id": {"type": "string", "default": "auto"},
        "prompt": {"type": "string"},
        "model": {"type": "string"},
        "timeout_seconds": {"type": "integer", "default": 300},
    },
    "required": ["prompt"],
}
