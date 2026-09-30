package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/caduceus/caduceus/internal/control"
	"github.com/caduceus/caduceus/internal/tasks"
)

func TestReadMessageJSONLines(t *testing.T) {
	body, framing, err := readMessage(bufio.NewReader(strings.NewReader("  {\"jsonrpc\":\"2.0\"}\r\n")))
	if err != nil {
		t.Fatal(err)
	}
	if framing != framingJSONLines {
		t.Fatalf("framing = %v, want JSON lines", framing)
	}
	if got := string(body); got != `{"jsonrpc":"2.0"}` {
		t.Fatalf("body = %q", got)
	}
}

func TestReadMessageContentLengthCompatibility(t *testing.T) {
	input := "Content-Length: 17\r\nContent-Type: application/json\r\n\r\n{\"jsonrpc\":\"2.0\"}"
	body, framing, err := readMessage(bufio.NewReader(strings.NewReader(input)))
	if err != nil {
		t.Fatal(err)
	}
	if framing != framingContentLength {
		t.Fatalf("framing = %v, want Content-Length", framing)
	}
	if got := string(body); got != `{"jsonrpc":"2.0"}` {
		t.Fatalf("body = %q", got)
	}
}

func TestServerUsesJSONLinesForStandardClient(t *testing.T) {
	request := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}` + "\n"
	var output bytes.Buffer
	server := NewServer(nil, strings.NewReader(request), &output)

	if err := server.Serve(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "Content-Length:") {
		t.Fatalf("standard response used legacy framing: %q", output.String())
	}
	if strings.Count(output.String(), "\n") != 1 {
		t.Fatalf("response is not one JSON line: %q", output.String())
	}

	var response rpcResponse
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil {
		t.Fatalf("initialize error: %+v", response.Error)
	}
}

func TestServerHandlesJSONLineNotificationBeforeRequest(t *testing.T) {
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`,
		`{"jsonrpc":"2.0","id":2,"method":"ping","params":{}}`,
		"",
	}, "\n")
	var output bytes.Buffer
	server := NewServer(nil, strings.NewReader(input), &output)

	if err := server.Serve(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), "\n") != 1 {
		t.Fatalf("notification produced an unexpected response: %q", output.String())
	}

	var response rpcResponse
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &response); err != nil {
		t.Fatal(err)
	}
	if string(response.ID) != "2" {
		t.Fatalf("response id = %s, want 2", response.ID)
	}
}

func TestServerPreservesLegacyResponseFraming(t *testing.T) {
	request, err := MarshalFrame(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "ping",
	})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	server := NewServer(nil, bytes.NewReader(request), &output)

	if err := server.Serve(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(output.String(), "Content-Length:") {
		t.Fatalf("legacy response framing was not preserved: %q", output.String())
	}
}

func TestDecodeRunTaskPhase2Fields(t *testing.T) {
	args := json.RawMessage(`{
		"task_id":"task-client-1",
		"kind":"prompt",
		"worker_id":"worker-a",
		"task":{"prompt":"hello","model":"model-a","stream":true},
		"constraints":{
			"required_capabilities":["llm","tools"],
			"preferred_models":["model-a","model-b"],
			"max_runtime_seconds":120,
			"min_cpu":4.5,
			"min_ram_bytes":8589934592,
			"gpu_required":true,
			"gpu_vendor":"nvidia",
			"min_gpu_memory_bytes":17179869184,
			"required_gpu_capabilities":["cuda","tensor-cores"],
			"required_model":"model-a",
			"required_runtime":"ollama",
			"required_trust_level":"trusted-lan",
			"allowed_worker_ids":["worker-a"],
			"allowed_groups":["group-a"]
		},
		"timeout_seconds":180,
		"trust_level":"trusted-lan",
		"idempotent":true,
		"max_attempts":3
	}`)

	req, err := decodeRunTask(args)
	if err != nil {
		t.Fatal(err)
	}
	wantConstraints := tasks.Constraints{
		RequiredCapabilities:    []string{"llm", "tools"},
		PreferredModels:         []string{"model-a", "model-b"},
		MaxRuntimeSeconds:       120,
		MinCPU:                  4.5,
		MinRAMBytes:             8589934592,
		GPURequired:             true,
		GPUVendor:               "nvidia",
		MinGPUMemoryBytes:       17179869184,
		RequiredGPUCapabilities: []string{"cuda", "tensor-cores"},
		RequiredModel:           "model-a",
		RequiredRuntime:         "ollama",
		RequiredTrustLevel:      "trusted-lan",
		AllowedWorkerIDs:        []string{"worker-a"},
		AllowedGroups:           []string{"group-a"},
	}
	if !reflect.DeepEqual(req.Constraints, wantConstraints) {
		t.Fatalf("constraints = %#v, want %#v", req.Constraints, wantConstraints)
	}
	if req.TaskID != "task-client-1" || req.Kind != tasks.KindPrompt || req.WorkerID != "worker-a" {
		t.Fatalf("unexpected request identity: %#v", req)
	}
	if !req.Idempotent || req.MaxAttempts != 3 || req.TimeoutSeconds != 180 {
		t.Fatalf("unexpected attempt policy: %#v", req)
	}
}

func TestDecodeRunTaskRejectsInvalidPhase2Arguments(t *testing.T) {
	tests := []struct {
		name string
		args string
	}{
		{name: "unknown field", args: `{"task":{"prompt":"hello"},"surprise":true}`},
		{name: "unknown constraint", args: `{"task":{"prompt":"hello"},"constraints":{"disk_bytes":1}}`},
		{name: "unsafe retries", args: `{"task":{"prompt":"hello"},"max_attempts":2}`},
		{name: "too many attempts", args: `{"task":{"prompt":"hello"},"idempotent":true,"max_attempts":11}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeRunTask(json.RawMessage(test.args)); err == nil {
				t.Fatal("expected invalid arguments to be rejected")
			}
		})
	}
}

func TestDecodeRunPromptPhase2Fields(t *testing.T) {
	req, err := decodeRunPrompt(json.RawMessage(`{
		"task_id":"task-prompt-1",
		"worker_id":"auto",
		"prompt":"hello",
		"constraints":{
			"required_capabilities":["tools"],
			"min_cpu":2,
			"min_ram_bytes":4096,
			"gpu_required":true,
			"gpu_vendor":"amd",
			"min_gpu_memory_bytes":8192,
			"required_gpu_capabilities":["rocm"],
			"required_model":"model-z",
			"required_runtime":"llama.cpp",
			"required_trust_level":"trusted-lan",
			"allowed_worker_ids":["worker-z"],
			"allowed_groups":["group-z"],
			"max_runtime_seconds":90,
			"preferred_models":["model-z"]
		},
		"idempotent":true,
		"max_attempts":2
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.TaskID != "task-prompt-1" || req.Kind != tasks.KindPrompt || !req.Task.Stream {
		t.Fatalf("unexpected prompt request defaults: %#v", req)
	}
	if !reflect.DeepEqual(req.Constraints.RequiredCapabilities, []string{"tools", "llm"}) {
		t.Fatalf("required capabilities = %#v", req.Constraints.RequiredCapabilities)
	}
	if req.Constraints.GPUVendor != "amd" || req.Constraints.RequiredRuntime != "llama.cpp" || req.MaxAttempts != 2 || !req.Idempotent {
		t.Fatalf("Phase 2 fields were not preserved: %#v", req)
	}
}

func TestCallToolDispatchesPhase2Operations(t *testing.T) {
	tests := []struct {
		name          string
		tool          string
		args          string
		method        string
		path          string
		wantActor     string
		response      control.Response
		wantErrorCode string
	}{
		{
			name: "explain route", tool: "caduceus.explain_route",
			args:   `{"task_id":"route-1","task":{"prompt":"hello"},"idempotent":true,"max_attempts":2}`,
			method: http.MethodPost, path: "/v1/route/explain", response: control.Success(map[string]any{"selected_worker_id": "worker-a"}),
		},
		{
			name: "list preserves safe error", tool: "caduceus.list_enrollment_requests", args: `{}`,
			method: http.MethodGet, path: "/v1/enrollment/requests",
			response: control.Failure("enrollment_disabled", "trusted-LAN enrollment is disabled", nil), wantErrorCode: "enrollment_disabled",
		},
		{
			name: "approve", tool: "caduceus.approve_enrollment", args: `{"request_id":"request-1","approved_by":"operator-a"}`,
			method: http.MethodPost, path: "/v1/enrollment/requests/request-1/approve", wantActor: "operator-a", response: control.Success(map[string]any{"status": "approved"}),
		},
		{
			name: "deny", tool: "caduceus.deny_enrollment", args: `{"request_id":"request-2","denied_by":"operator-b"}`,
			method: http.MethodPost, path: "/v1/enrollment/requests/request-2/deny", wantActor: "operator-b", response: control.Success(map[string]any{"status": "denied"}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/config" {
					_ = json.NewEncoder(w).Encode(control.Success(map[string]any{"config": map[string]any{"enrollment": map[string]any{"trusted_lan": map[string]any{"hermes_mode": "invoked"}}}}))
					return
				}
				if r.Method != test.method || r.URL.Path != test.path {
					t.Errorf("request = %s %s, want %s %s", r.Method, r.URL.Path, test.method, test.path)
				}
				if r.Header.Get("Authorization") != "Bearer token" {
					t.Errorf("authorization header was not forwarded")
				}
				if test.tool == "caduceus.explain_route" {
					var request tasks.Request
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Errorf("decode route request: %v", err)
					} else if request.TaskID != "route-1" || !request.Idempotent || request.MaxAttempts != 2 {
						t.Errorf("unexpected route request: %#v", request)
					}
				}
				if test.wantActor != "" {
					var decision control.EnrollmentDecisionRequest
					if err := json.NewDecoder(r.Body).Decode(&decision); err != nil {
						t.Errorf("decode enrollment decision: %v", err)
					} else if decision.Actor != test.wantActor {
						t.Errorf("actor = %q, want %q", decision.Actor, test.wantActor)
					}
				}
				_ = json.NewEncoder(w).Encode(test.response)
			}))
			defer httpServer.Close()

			server := NewServer(control.NewClient(httpServer.URL, "token"), nil, nil)
			result, err := server.callTool(context.Background(), test.tool, json.RawMessage(test.args))
			if err != nil {
				t.Fatal(err)
			}
			response := toolStructuredResponse(t, result)
			if test.wantErrorCode == "" {
				if !response.OK {
					t.Fatalf("unexpected tool failure: %#v", response)
				}
			} else if response.OK || response.Error == nil || response.Error.Code != test.wantErrorCode {
				t.Fatalf("response = %#v, want error code %q", response, test.wantErrorCode)
			}
		})
	}
}

func TestEnrollmentToolArgumentValidationDoesNotCallControl(t *testing.T) {
	server := NewServer(control.NewClient("http://127.0.0.1:1", ""), nil, nil)
	result, err := server.callTool(context.Background(), "caduceus.approve_enrollment", json.RawMessage(`{"request_id":"","actor":"unexpected"}`))
	if err != nil {
		t.Fatal(err)
	}
	response := toolStructuredResponse(t, result)
	if response.OK || response.Error == nil || response.Error.Code != "bad_request" {
		t.Fatalf("response = %#v, want local bad_request", response)
	}
}

func toolStructuredResponse(t *testing.T, result any) control.Response {
	t.Helper()
	values, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T", result)
	}
	response, ok := values["structuredContent"].(control.Response)
	if !ok {
		t.Fatalf("structuredContent type = %T", values["structuredContent"])
	}
	return response
}
