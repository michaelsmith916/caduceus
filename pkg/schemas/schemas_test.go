package schemas

import "testing"

func TestRequiredToolsExist(t *testing.T) {
	required := []string{
		"caduceus.list_workers",
		"caduceus.get_worker",
		"caduceus.run_remote_task",
		"caduceus.run_remote_prompt",
		"caduceus.cancel_task",
		"caduceus.get_local_config",
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
