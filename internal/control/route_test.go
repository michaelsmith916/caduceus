package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/scheduler"
	"github.com/caduceus/caduceus/internal/tasks"
)

type routeBackendStub struct {
	explainCalls int
	runCalls     int
	request      tasks.Request
	result       any
	err          error
}

func (b *routeBackendStub) Status(context.Context) (any, error)                     { return nil, nil }
func (b *routeBackendStub) Config(context.Context) (any, error)                     { return nil, nil }
func (b *routeBackendStub) ListWorkers(context.Context) (any, error)                { return nil, nil }
func (b *routeBackendStub) GetWorker(context.Context, string) (any, error)          { return nil, nil }
func (b *routeBackendStub) ListTasks(context.Context, ListTasksFilter) (any, error) { return nil, nil }
func (b *routeBackendStub) GetTaskStatus(context.Context, string) (any, error)      { return nil, nil }
func (b *routeBackendStub) GetTaskResult(context.Context, string) (any, error)      { return nil, nil }
func (b *routeBackendStub) GetTaskEvents(context.Context, string, int64) (any, error) {
	return nil, nil
}
func (b *routeBackendStub) GetTaskArtifacts(context.Context, string) (any, error) {
	return nil, nil
}
func (b *routeBackendStub) CancelTask(context.Context, string) (any, error) { return nil, nil }
func (b *routeBackendStub) ValidateTask(context.Context, RunTaskRequest) (any, error) {
	return nil, nil
}
func (b *routeBackendStub) RunTask(context.Context, RunTaskRequest) (any, error) {
	b.runCalls++
	return nil, errors.New("RunTask must not be called by route explanation")
}
func (b *routeBackendStub) ExplainRoute(_ context.Context, req tasks.Request) (any, error) {
	b.explainCalls++
	b.request = req
	return b.result, b.err
}

type legacyBackend struct{ Backend }

func TestExplainRouteHandlerIsReadOnlyAndForwardsTaskRequest(t *testing.T) {
	decision := scheduler.RoutingDecision{
		TaskID:            "route-task",
		SelectedWorkerID:  "worker-a",
		RegistryTimestamp: time.Date(2026, 8, 5, 20, 0, 0, 0, time.UTC),
		RegistryVersion:   4,
		Explanation:       "selected worker-a",
	}
	backend := &routeBackendStub{result: decision}
	server := NewServer("unused", "secret", backend)
	body := `{"task_id":"route-task","kind":"prompt","task":{"prompt":"hello","model":"model-a"},"constraints":{"required_capabilities":["llm"]}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/route/explain", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	recorder := httptest.NewRecorder()

	server.wrap(server.handleExplainRoute).ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if backend.explainCalls != 1 || backend.runCalls != 0 {
		t.Fatalf("explain calls=%d run calls=%d", backend.explainCalls, backend.runCalls)
	}
	if backend.request.TaskID != "route-task" || backend.request.Task.Prompt != "hello" {
		t.Fatalf("unexpected request: %#v", backend.request)
	}
	var response Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.OK {
		t.Fatalf("unexpected response: %#v", response)
	}
}

func TestExplainRouteHandlerRejectsInvalidInputWithoutCallingBackend(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed", body: `{"task":`},
		{name: "unknown field", body: `{"task":{"prompt":"hello"},"surprise":true}`},
		{name: "multiple values", body: `{"task":{"prompt":"hello"}} {"task":{"prompt":"again"}}`},
		{name: "invalid task", body: `{"task":{"prompt":""}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &routeBackendStub{}
			server := NewServer("unused", "", backend)
			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/route/explain", strings.NewReader(test.body))
			server.wrap(server.handleExplainRoute).ServeHTTP(recorder, req)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if backend.explainCalls != 0 || backend.runCalls != 0 {
				t.Fatalf("unexpected calls: explain=%d run=%d", backend.explainCalls, backend.runCalls)
			}
		})
	}
}

func TestExplainRouteHandlerOptionalBackendAndMethod(t *testing.T) {
	backend := &routeBackendStub{}
	legacy := legacyBackend{Backend: backend}
	server := NewServer("unused", "", legacy)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/route/explain", strings.NewReader(`{"task":{"prompt":"hello"}}`))
	server.wrap(server.handleExplainRoute).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotImplemented {
		t.Fatalf("unsupported status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	server = NewServer("unused", "", backend)
	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/v1/route/explain", nil)
	server.wrap(server.handleExplainRoute).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestClientExplainRoutePostsExpectedPathAndShape(t *testing.T) {
	var got tasks.Request
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/route/explain" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("unexpected authorization header")
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(Success(scheduler.RoutingDecision{SelectedWorkerID: "worker-a"}))
	}))
	defer httpServer.Close()

	client := NewClient(httpServer.URL, "token")
	response, err := client.ExplainRoute(context.Background(), tasks.Request{Task: tasks.PromptTask{Prompt: "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if !response.OK || got.Task.Prompt != "hello" {
		t.Fatalf("response=%#v request=%#v", response, got)
	}
}
