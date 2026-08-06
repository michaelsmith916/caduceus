package tasks

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestValidatePromptTask(t *testing.T) {
	req := Request{Kind: KindPrompt, Task: PromptTask{Prompt: "hello"}}
	if err := ValidateRequest(req); err != nil {
		t.Fatal(err)
	}
}

func TestValidateTemperature(t *testing.T) {
	temp := 3.0
	req := Request{Kind: KindPrompt, Task: PromptTask{Prompt: "hello", Temperature: &temp}}
	if err := ValidateRequest(req); err == nil {
		t.Fatal("expected temperature validation error")
	}
}

func TestPhaseIRequestCompatibilityDefaultsToOneNonIdempotentAttempt(t *testing.T) {
	legacy := []byte(`{"task_id":"task-1","kind":"prompt","task":{"prompt":"hello","stream":true},"constraints":{},"timeout_seconds":30,"trust_level":"trusted-lan"}`)
	var req Request
	if err := json.Unmarshal(legacy, &req); err != nil {
		t.Fatal(err)
	}
	if req.Idempotent {
		t.Fatal("legacy request unexpectedly became idempotent")
	}
	if got := req.EffectiveMaxAttempts(); got != 1 {
		t.Fatalf("effective max attempts = %d, want 1", got)
	}
	if err := ValidateRequest(req); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "idempotent") || strings.Contains(string(encoded), "max_attempts") {
		t.Fatalf("zero-value Phase 2 fields should remain omitted: %s", encoded)
	}
}

func TestValidateResourceConstraintsAndRetryPolicy(t *testing.T) {
	base := Request{
		TaskID:      "task-1",
		Kind:        KindPrompt,
		Task:        PromptTask{Prompt: "hello", Model: "model-a"},
		Idempotent:  true,
		MaxAttempts: 3,
		Constraints: Constraints{
			RequiredCapabilities:    []string{"llm"},
			PreferredModels:         []string{"model-a"},
			MinCPU:                  2.5,
			MinRAMBytes:             8 << 30,
			GPURequired:             true,
			GPUVendor:               "nvidia",
			MinGPUMemoryBytes:       16 << 30,
			RequiredGPUCapabilities: []string{"cuda"},
			RequiredModel:           "model-a",
			RequiredRuntime:         "cuda-12",
			RequiredTrustLevel:      "trusted-lan",
			AllowedWorkerIDs:        []string{"worker-a"},
			AllowedGroups:           []string{"group-a"},
		},
	}
	if err := ValidateRequest(base); err != nil {
		t.Fatal(err)
	}
	if got := base.EffectiveMaxAttempts(); got != 3 {
		t.Fatalf("effective max attempts = %d, want 3", got)
	}

	tests := []struct {
		name   string
		mutate func(*Request)
	}{
		{name: "nan cpu", mutate: func(req *Request) { req.Constraints.MinCPU = math.NaN() }},
		{name: "oversized memory", mutate: func(req *Request) { req.Constraints.MinRAMBytes = 1<<60 + 1 }},
		{name: "duplicate capability", mutate: func(req *Request) { req.Constraints.RequiredCapabilities = []string{"llm", "llm"} }},
		{name: "conflicting model", mutate: func(req *Request) { req.Constraints.RequiredModel = "model-b" }},
		{name: "too many attempts", mutate: func(req *Request) { req.MaxAttempts = MaxAttemptsLimit + 1 }},
		{name: "non-idempotent replay", mutate: func(req *Request) { req.Idempotent = false; req.MaxAttempts = 2 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := base
			tt.mutate(&req)
			if err := ValidateRequest(req); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestAttemptIdentityAndValidation(t *testing.T) {
	first, err := NewAttempt("task-1", "worker-1", "session-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewAttempt("task-1", "worker-1", "session-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.AttemptID == second.AttemptID || first.AttemptToken == second.AttemptToken {
		t.Fatal("attempt IDs and tokens must be unique")
	}
	if len(first.AttemptToken) != 64 {
		t.Fatalf("attempt token length = %d, want 64", len(first.AttemptToken))
	}
	if err := first.Validate(); err != nil {
		t.Fatal(err)
	}
	first.State = "invented"
	if err := first.Validate(); err == nil {
		t.Fatal("expected invalid attempt state to be rejected")
	}
	if !ValidStatus(StatusInterrupted) || !ValidStatus(StatusLost) {
		t.Fatal("Phase 2 terminal statuses should be valid")
	}
}

func TestTaskRejectionValidationAndSerialization(t *testing.T) {
	rejection := NewRejection("task-1", "attempt-1", "worker-1", RejectQueueFull, "queue is full")
	rejection.Retryable = true
	rejection.RetryAfterSeconds = 5
	rejection.CurrentQueueDepth = 2
	rejection.CurrentRunningTasks = 1
	rejection.MaxConcurrentTasks = 1
	available := float64(4 << 30)
	rejection.ResourceDeficiencies = []ResourceDeficiency{{Resource: "ram", Required: float64(8 << 30), Available: &available, Unit: "bytes"}}
	if err := rejection.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(rejection)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"reason":"queue_full"`) || !strings.Contains(string(encoded), `"retry_after":5`) {
		t.Fatalf("rejection lacks stable machine fields: %s", encoded)
	}
	var decoded TaskRejection
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.RetryAfterDuration().Seconds() != 5 {
		t.Fatalf("retry duration = %s", decoded.RetryAfterDuration())
	}
	decoded.Reason = RejectionReason("free_form_only")
	if err := decoded.Validate(); err == nil {
		t.Fatal("expected unknown rejection reason to fail")
	}
}

func TestResultAttemptFencingRoundTrip(t *testing.T) {
	want := Result{TaskID: "task-1", AttemptID: "attempt-2", AttemptToken: "opaque-token", Status: StatusCompleted, OutputText: "done"}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got Result
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.AttemptID != want.AttemptID || got.AttemptToken != want.AttemptToken {
		t.Fatalf("attempt fencing fields lost: %#v", got)
	}
}
