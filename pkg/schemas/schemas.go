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
		{Name: "caduceus.explain_route", Description: "Explain the deterministic Phase 2 routing decision without enqueueing or executing the task.", InputSchema: runTaskInput(), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.run_remote_task", Description: "Run a validated Phase 2 prompt task on a selected or automatically scheduled worker.", InputSchema: runTaskInput(), OutputSchema: genericOutput(), Annotations: stateChanging},
		{Name: "caduceus.run_remote_prompt", Description: "Convenience wrapper for running a Phase 2 prompt task with resource constraints and an explicit retry policy.", InputSchema: runPromptInput(), OutputSchema: genericOutput(), Annotations: stateChanging},
		{Name: "caduceus.list_tasks", Description: "List locally known task metadata.", InputSchema: object(map[string]any{"status": stringProp("Optional task status filter.")}), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.get_task_status", Description: "Get local status metadata for a task.", InputSchema: taskIDInput(), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.get_task_result", Description: "Get final task result.", InputSchema: taskIDInput(), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.get_task_events", Description: "Get task events after a cursor.", InputSchema: object(map[string]any{"task_id": stringProp("Task ID."), "cursor": map[string]any{"type": "integer", "minimum": 0}}, "task_id"), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.get_task_artifacts", Description: "List task artifacts.", InputSchema: taskIDInput(), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.cancel_task", Description: "Cancel a running local or remote task. This is state-changing but not destructive.", InputSchema: taskIDInput(), OutputSchema: genericOutput(), Annotations: stateChanging},
		{Name: "caduceus.get_local_node_status", Description: "Inspect local daemon status.", InputSchema: object(nil), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.get_local_config", Description: "Inspect sanitized local configuration.", InputSchema: object(nil), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.validate_task", Description: "Validate a Phase 2 task request without enqueueing or executing it.", InputSchema: runTaskInput(), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.list_enrollment_requests", Description: "List pending and decided trusted-LAN enrollment requests without exposing invitation tokens.", InputSchema: object(nil), OutputSchema: genericOutput(), Annotations: readOnly},
		{Name: "caduceus.approve_enrollment", Description: "Approve one pending trusted-LAN enrollment request.", InputSchema: enrollmentDecisionInput("approved_by", "Optional approving actor recorded in the audit log."), OutputSchema: genericOutput(), Annotations: stateChanging},
		{Name: "caduceus.deny_enrollment", Description: "Deny one pending trusted-LAN enrollment request.", InputSchema: enrollmentDecisionInput("denied_by", "Optional denying actor recorded in the audit log."), OutputSchema: genericOutput(), Annotations: stateChanging},
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
	schema := object(map[string]any{
		"task_id":         identifierProp("Optional caller-supplied task ID used for correlation and deduplication."),
		"kind":            map[string]any{"type": "string", "const": "prompt", "default": "prompt"},
		"worker_id":       map[string]any{"type": "string", "maxLength": 512, "description": "Worker peer ID, auto, or omitted for automatic scheduling.", "default": "auto"},
		"prompt":          map[string]any{"type": "string", "minLength": 1, "maxLength": 131072, "description": "User prompt to run on the worker (server limit: 128 KiB)."},
		"system":          map[string]any{"type": "string", "maxLength": 65536},
		"model":           boundedStringProp("Requested model name.", 512),
		"temperature":     map[string]any{"type": "number", "minimum": 0, "maximum": 2},
		"max_tokens":      map[string]any{"type": "integer", "minimum": 0, "maximum": 200000},
		"stream":          map[string]any{"type": "boolean", "default": true},
		"timeout_seconds": map[string]any{"type": "integer", "minimum": 0, "maximum": 3600},
		"trust_level":     map[string]any{"type": "string", "maxLength": 512, "default": "trusted-lan"},
		"constraints":     constraintsInput(),
		"idempotent":      idempotentInput(),
		"max_attempts":    maxAttemptsInput(),
	}, "prompt")
	addAttemptPolicy(schema)
	return schema
}

func runTaskInput() JSONSchema {
	schema := object(map[string]any{
		"task_id":   identifierProp("Optional caller-supplied task ID used for correlation and deduplication."),
		"kind":      map[string]any{"type": "string", "const": "prompt", "default": "prompt"},
		"worker_id": map[string]any{"type": "string", "maxLength": 512, "description": "Worker peer ID, auto, or omitted for automatic scheduling.", "default": "auto"},
		"task": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"prompt":      map[string]any{"type": "string", "minLength": 1, "maxLength": 131072, "description": "Prompt text (server limit: 128 KiB)."},
				"system":      map[string]any{"type": "string", "maxLength": 65536},
				"model":       boundedStringProp("Requested model name.", 512),
				"temperature": map[string]any{"type": "number", "minimum": 0, "maximum": 2},
				"max_tokens":  map[string]any{"type": "integer", "minimum": 0, "maximum": 200000},
				"stream":      map[string]any{"type": "boolean"},
			},
			"required":             []string{"prompt"},
			"additionalProperties": false,
		},
		"constraints":     constraintsInput(),
		"timeout_seconds": map[string]any{"type": "integer", "minimum": 0, "maximum": 3600},
		"trust_level":     map[string]any{"type": "string", "maxLength": 512, "default": "trusted-lan"},
		"idempotent":      idempotentInput(),
		"max_attempts":    maxAttemptsInput(),
	}, "task")
	addAttemptPolicy(schema)
	return schema
}

func constraintsInput() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"required_capabilities":     constraintList("Capabilities every eligible worker must advertise."),
			"preferred_models":          constraintList("Models used as a scheduling preference, not a hard filter."),
			"max_runtime_seconds":       map[string]any{"type": "integer", "minimum": 0, "maximum": 3600},
			"min_cpu":                   map[string]any{"type": "number", "minimum": 0, "maximum": 1024, "description": "Minimum available logical CPU capacity."},
			"min_ram_bytes":             resourceBytes("Minimum available RAM in bytes."),
			"gpu_required":              map[string]any{"type": "boolean", "default": false},
			"gpu_vendor":                boundedStringProp("Required GPU vendor.", 512),
			"min_gpu_memory_bytes":      resourceBytes("Minimum available GPU memory in bytes."),
			"required_gpu_capabilities": constraintList("GPU capabilities every eligible worker must advertise."),
			"required_model":            boundedStringProp("Model every eligible worker must make available.", 512),
			"required_runtime":          boundedStringProp("Runtime every eligible worker must advertise.", 512),
			"required_trust_level":      boundedStringProp("Minimum required worker trust label.", 512),
			"allowed_worker_ids":        constraintList("Hard allowlist of eligible worker peer IDs."),
			"allowed_groups":            constraintList("Hard allowlist of eligible worker group identifiers or labels."),
		},
		"additionalProperties": false,
	}
}

func constraintList(description string) map[string]any {
	return map[string]any{
		"type":        "array",
		"description": description,
		"maxItems":    256,
		"uniqueItems": true,
		"items":       map[string]any{"type": "string", "minLength": 1, "maxLength": 512},
	}
}

func resourceBytes(description string) map[string]any {
	return map[string]any{"type": "integer", "minimum": 0, "maximum": uint64(1 << 60), "description": description}
}

func identifierProp(description string) map[string]any {
	return boundedStringProp(description, 512)
}

func boundedStringProp(description string, maximum int) map[string]any {
	return map[string]any{"type": "string", "minLength": 1, "maxLength": maximum, "description": description}
}

func idempotentInput() map[string]any {
	return map[string]any{
		"type":        "boolean",
		"default":     false,
		"description": "Declare that retrying the task cannot duplicate unsafe side effects. False preserves single-attempt behavior.",
	}
}

func maxAttemptsInput() map[string]any {
	return map[string]any{
		"type":        "integer",
		"minimum":     0,
		"maximum":     10,
		"default":     1,
		"description": "Maximum attempts. Values above one require idempotent=true; omitted or zero is effectively one.",
	}
}

func addAttemptPolicy(schema JSONSchema) {
	schema["allOf"] = []any{
		map[string]any{
			"if": map[string]any{
				"properties": map[string]any{"max_attempts": map[string]any{"minimum": 2}},
				"required":   []string{"max_attempts"},
			},
			"then": map[string]any{
				"properties": map[string]any{"idempotent": map[string]any{"const": true}},
				"required":   []string{"idempotent"},
			},
		},
	}
}

func enrollmentDecisionInput(actorField, actorDescription string) JSONSchema {
	return object(map[string]any{
		"request_id": identifierProp("Enrollment request ID."),
		actorField:   boundedStringProp(actorDescription, 512),
	}, "request_id")
}

func genericOutput() JSONSchema {
	return JSONSchema{
		"oneOf": []any{SuccessEnvelope(JSONSchema{}), ErrorEnvelope()},
	}
}
