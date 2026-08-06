package schemas

import (
	"strings"
	"testing"
)

func TestRequiredToolsExist(t *testing.T) {
	required := []string{
		"caduceus.list_workers",
		"caduceus.get_worker",
		"caduceus.run_remote_task",
		"caduceus.run_remote_prompt",
		"caduceus.explain_route",
		"caduceus.cancel_task",
		"caduceus.get_local_config",
		"caduceus.list_enrollment_requests",
		"caduceus.approve_enrollment",
		"caduceus.deny_enrollment",
	}
	for _, name := range required {
		tool, ok := ToolByName(name)
		if !ok {
			t.Fatalf("missing tool %s", name)
		}
		if tool.InputSchema == nil {
			t.Fatalf("missing input schema for %s", name)
		}
	}
}

func TestPhase2TaskSchemasExposeConstraintsAndAttemptPolicy(t *testing.T) {
	constraintNames := []string{
		"required_capabilities",
		"preferred_models",
		"max_runtime_seconds",
		"min_cpu",
		"min_ram_bytes",
		"gpu_required",
		"gpu_vendor",
		"min_gpu_memory_bytes",
		"required_gpu_capabilities",
		"required_model",
		"required_runtime",
		"required_trust_level",
		"allowed_worker_ids",
		"allowed_groups",
	}
	for _, name := range []string{"caduceus.run_remote_task", "caduceus.run_remote_prompt", "caduceus.explain_route", "caduceus.validate_task"} {
		t.Run(name, func(t *testing.T) {
			tool, ok := ToolByName(name)
			if !ok {
				t.Fatalf("missing tool %s", name)
			}
			properties := schemaProperties(t, tool.InputSchema)
			for _, property := range []string{"task_id", "kind", "worker_id", "constraints", "idempotent", "max_attempts"} {
				if _, ok := properties[property]; !ok {
					t.Errorf("%s schema is missing %s", name, property)
				}
			}
			constraints, ok := properties["constraints"].(map[string]any)
			if !ok {
				t.Fatalf("constraints type = %T", properties["constraints"])
			}
			constraintProperties := schemaProperties(t, constraints)
			for _, constraint := range constraintNames {
				if _, ok := constraintProperties[constraint]; !ok {
					t.Errorf("%s constraints schema is missing %s", name, constraint)
				}
			}
			if _, ok := tool.InputSchema["allOf"]; !ok {
				t.Errorf("%s schema does not express the idempotent retry condition", name)
			}
		})
	}
}

func TestEnrollmentToolSchemasUseStableHermesArguments(t *testing.T) {
	tests := []struct {
		name       string
		actorField string
	}{
		{name: "caduceus.approve_enrollment", actorField: "approved_by"},
		{name: "caduceus.deny_enrollment", actorField: "denied_by"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tool, ok := ToolByName(test.name)
			if !ok {
				t.Fatalf("missing tool %s", test.name)
			}
			properties := schemaProperties(t, tool.InputSchema)
			if _, ok := properties["request_id"]; !ok {
				t.Fatal("missing request_id")
			}
			if _, ok := properties[test.actorField]; !ok {
				t.Fatalf("missing %s", test.actorField)
			}
			if _, ok := properties["actor"]; ok {
				t.Fatal("generic actor field would break the Hermes mapping")
			}
		})
	}
}

func TestToolDescriptionsAndAnnotationsReflectPhase2(t *testing.T) {
	for _, tool := range Tools() {
		if strings.Contains(tool.Description, "Phase I") {
			t.Errorf("tool %s still advertises Phase I", tool.Name)
		}
	}
	for _, name := range []string{"caduceus.explain_route", "caduceus.list_enrollment_requests"} {
		tool, _ := ToolByName(name)
		if readOnly, _ := tool.Annotations["readOnlyHint"].(bool); !readOnly {
			t.Errorf("tool %s is not annotated read-only", name)
		}
	}
}

func schemaProperties(t *testing.T, schema map[string]any) map[string]any {
	t.Helper()
	properties, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties type = %T", schema["properties"])
	}
	return properties
}
