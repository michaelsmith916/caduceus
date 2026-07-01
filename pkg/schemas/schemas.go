package schemas

type JSONSchema map[string]any

type Tool struct {
	Name         string         `json:"name"`
	Description  string         `json:"description"`
	InputSchema  JSONSchema     `json:"inputSchema"`
	OutputSchema JSONSchema     `json:"outputSchema,omitempty"`
	Annotations  map[string]any `json:"annotations,omitempty"`
}

func SuccessEnvelope(data JSONSchema) JSONSchema {
	return JSONSchema{
		"type": "object",
		"properties": map[string]any{
			"ok":   map[string]any{"type": "boolean", "const": true},
			"data": data,
		},
		"required": []string{"ok", "data"},
	}
}

func ErrorEnvelope() JSONSchema {
	return JSONSchema{
		"type": "object",
		"properties": map[string]any{
			"ok": map[string]any{"type": "boolean", "const": false},
			"error": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"code":    map[string]any{"type": "string"},
					"message": map[string]any{"type": "string"},
					"details": map[string]any{"type": "object"},
				},
				"required": []string{"code", "message"},
			},
		},
		"required": []string{"ok", "error"},
	}
}

func Tools() []Tool {
	readOnly := map[string]any{"readOnlyHint": true}
	stateChanging := map[string]any{"destructiveHint": false}
	return []Tool{
		{Name: "caduceus.list_workers", Description: "List trusted LAN workers known to the local daemon.", InputSchema: object(nil), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.get_worker", Description: "Get details for one worker.", InputSchema: object(map[string]any{"worker_id": stringProp("Worker peer ID.")}, "worker_id"), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.run_remote_task", Description: "Run a validated Phase I prompt task on a selected or automatic worker.", InputSchema: runTaskInput(), OutputSchema: genericOutput(), Annotations: stateChanging},
		{Name: "caduceus.run_remote_prompt", Description: "Convenience wrapper for running a prompt task.", InputSchema: runPromptInput(), OutputSchema: genericOutput(), Annotations: stateChanging},
		{Name: "caduceus.list_tasks", Description: "List locally known task metadata.", InputSchema: object(map[string]any{"status": stringProp("Optional task status filter.")}), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.get_task_status", Description: "Get local status metadata for a task.", InputSchema: taskIDInput(), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.get_task_result", Description: "Get final task result.", InputSchema: taskIDInput(), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.get_task_events", Description: "Get task events after a cursor.", InputSchema: object(map[string]any{"task_id": stringProp("Task ID."), "cursor": map[string]any{"type": "integer", "minimum": 0}}, "task_id"), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.get_task_artifacts", Description: "List task artifacts.", InputSchema: taskIDInput(), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.cancel_task", Description: "Cancel a running local or remote task. This is state-changing but not destructive.", InputSchema: taskIDInput(), OutputSchema: genericOutput(), Annotations: stateChanging},
		{Name: "caduceus.get_local_node_status", Description: "Inspect local daemon status.", InputSchema: object(nil), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.get_local_config", Description: "Inspect sanitized local configuration.", InputSchema: object(nil), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.validate_task", Description: "Validate a Phase I task request without executing it.", InputSchema: runTaskInput(), OutputSchema: genericOutput(), Annotations: readOnly},
	}
}

func ToolByName(name string) (Tool, bool) {
	for _, tool := range Tools() {
		if tool.Name == name {
			return tool, true
		}
	}
	return Tool{}, false
}

func Resources() []string {
	return []string{
		"caduceus://local/status",
		"caduceus://local/config",
		"caduceus://workers",
		"caduceus://workers/{worker_id}",
		"caduceus://tasks/{task_id}",
		"caduceus://tasks/{task_id}/events",
		"caduceus://tasks/{task_id}/artifacts/{artifact_id}",
	}
}

func object(props map[string]any, required ...string) JSONSchema {
	if props == nil {
		props = map[string]any{}
	}
	s := JSONSchema{
		"$schema":              "https://json-schema.org/draft/2020-12/schema",
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func stringProp(description string) map[string]any {
	return map[string]any{"type": "string", "minLength": 1, "description": description}
}

func taskIDInput() JSONSchema {
	return object(map[string]any{"task_id": stringProp("Task ID.")}, "task_id")
}

func runPromptInput() JSONSchema {
	return object(map[string]any{
		"worker_id":       map[string]any{"type": "string", "description": "Worker peer ID or auto.", "default": "auto"},
		"prompt":          stringProp("User prompt to run on the remote worker."),
		"system":          map[string]any{"type": "string"},
		"model":           map[string]any{"type": "string"},
		"temperature":     map[string]any{"type": "number", "minimum": 0, "maximum": 2},
		"max_tokens":      map[string]any{"type": "integer", "minimum": 0, "maximum": 200000},
		"stream":          map[string]any{"type": "boolean", "default": true},
		"timeout_seconds": map[string]any{"type": "integer", "minimum": 0, "maximum": 3600},
		"trust_level":     map[string]any{"type": "string", "default": "trusted-lan"},
	}, "prompt")
}

func runTaskInput() JSONSchema {
	return object(map[string]any{
		"worker_id": map[string]any{"type": "string", "description": "Worker peer ID or auto.", "default": "auto"},
		"task": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"prompt":      stringProp("Prompt text."),
				"system":      map[string]any{"type": "string"},
				"model":       map[string]any{"type": "string"},
				"temperature": map[string]any{"type": "number", "minimum": 0, "maximum": 2},
				"max_tokens":  map[string]any{"type": "integer", "minimum": 0},
				"stream":      map[string]any{"type": "boolean"},
			},
			"required":             []string{"prompt"},
			"additionalProperties": false,
		},
		"constraints": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"required_capabilities": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"preferred_models":      map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"max_runtime_seconds":   map[string]any{"type": "integer", "minimum": 0, "maximum": 3600},
			},
			"additionalProperties": false,
		},
		"timeout_seconds": map[string]any{"type": "integer", "minimum": 0, "maximum": 3600},
		"trust_level":     map[string]any{"type": "string", "default": "trusted-lan"},
	}, "task")
}

func genericOutput() JSONSchema {
	return JSONSchema{
		"oneOf": []any{SuccessEnvelope(map[string]any{"type": "object"}), ErrorEnvelope()},
	}
}
