package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/caduceus/caduceus/internal/control"
	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/caduceus/caduceus/pkg/caduceus"
	"github.com/caduceus/caduceus/pkg/schemas"
)

type Server struct {
	client *control.Client
	in     io.Reader
	out    io.Writer
	mu     sync.Mutex
}

func NewServer(client *control.Client, in io.Reader, out io.Writer) *Server {
	return &Server{client: client, in: in, out: out}
}

func (s *Server) Serve(ctx context.Context) error {
	reader := bufio.NewReader(s.in)
	for {
		body, framing, err := readMessage(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		var req rpcRequest
		if err := json.Unmarshal(body, &req); err != nil {
			continue
		}
		if len(req.ID) == 0 {
			_ = s.handleNotification(ctx, req)
			continue
		}
		resp := s.handle(ctx, req)
		if err := s.write(resp, framing); err != nil {
			return err
		}
	}
}

func (s *Server) handle(ctx context.Context, req rpcRequest) rpcResponse {
	result, err := s.dispatch(ctx, req.Method, req.Params)
	if err != nil {
		return rpcResponse{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{Code: -32000, Message: err.Error()}}
	}
	return rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: result}
}

func (s *Server) handleNotification(ctx context.Context, req rpcRequest) error {
	_, err := s.dispatch(ctx, req.Method, req.Params)
	return err
}

func (s *Server) dispatch(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case "initialize":
		return map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities": map[string]any{
				"tools":     map[string]any{},
				"resources": map[string]any{},
			},
			"serverInfo": map[string]any{"name": "caduceus-mcp", "version": caduceus.Version},
		}, nil
	case "notifications/initialized":
		return map[string]any{}, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		return map[string]any{"tools": schemas.Tools()}, nil
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, err
		}
		return s.callTool(ctx, p.Name, p.Arguments)
	case "resources/list":
		resources := make([]map[string]any, 0, len(schemas.Resources()))
		for _, uri := range schemas.Resources() {
			resources = append(resources, map[string]any{"uri": uri, "name": uri, "mimeType": "application/json"})
		}
		return map[string]any{"resources": resources}, nil
	case "resources/read":
		var p struct {
			URI string `json:"uri"`
		}
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, err
		}
		return s.readResource(ctx, p.URI)
	case "resources/templates/list":
		return map[string]any{"resourceTemplates": []map[string]any{
			{"uriTemplate": "caduceus://workers/{worker_id}", "name": "Worker", "mimeType": "application/json"},
			{"uriTemplate": "caduceus://tasks/{task_id}", "name": "Task", "mimeType": "application/json"},
			{"uriTemplate": "caduceus://tasks/{task_id}/events", "name": "Task events", "mimeType": "application/json"},
			{"uriTemplate": "caduceus://tasks/{task_id}/artifacts/{artifact_id}", "name": "Task artifact", "mimeType": "application/json"},
		}}, nil
	case "prompts/list":
		return map[string]any{"prompts": []any{}}, nil
	default:
		return nil, fmt.Errorf("unsupported MCP method %q", method)
	}
}

func (s *Server) callTool(ctx context.Context, name string, args json.RawMessage) (any, error) {
	var resp control.Response
	var err error
	switch name {
	case "caduceus.list_workers":
		resp, err = s.client.ListWorkers(ctx)
	case "caduceus.get_worker":
		var p struct {
			WorkerID string `json:"worker_id"`
		}
		_ = json.Unmarshal(args, &p)
		resp, err = s.client.GetWorker(ctx, p.WorkerID)
	case "caduceus.explain_route":
		req, e := decodeTaskRequest(args)
		if e != nil {
			resp = control.Failure("bad_request", e.Error(), nil)
		} else {
			resp, err = s.client.ExplainRoute(ctx, req)
		}
	case "caduceus.run_remote_task":
		req, e := decodeRunTask(args)
		if e != nil {
			resp = control.Failure("bad_request", e.Error(), nil)
		} else {
			resp, err = s.client.RunTask(ctx, req)
		}
	case "caduceus.run_remote_prompt":
		req, e := decodeRunPrompt(args)
		if e != nil {
			resp = control.Failure("bad_request", e.Error(), nil)
		} else {
			resp, err = s.client.RunTask(ctx, req)
		}
	case "caduceus.list_tasks":
		var p struct {
			Status string `json:"status"`
		}
		_ = json.Unmarshal(args, &p)
		resp, err = s.client.ListTasks(ctx, p.Status)
	case "caduceus.get_task_status":
		resp, err = s.client.GetTaskStatus(ctx, getStringArg(args, "task_id"))
	case "caduceus.get_task_result":
		resp, err = s.client.GetTaskResult(ctx, getStringArg(args, "task_id"))
	case "caduceus.get_task_events":
		var p struct {
			TaskID string `json:"task_id"`
			Cursor int64  `json:"cursor"`
		}
		_ = json.Unmarshal(args, &p)
		resp, err = s.client.GetTaskEvents(ctx, p.TaskID, p.Cursor)
	case "caduceus.get_task_artifacts":
		resp, err = s.client.GetTaskArtifacts(ctx, getStringArg(args, "task_id"))
	case "caduceus.cancel_task":
		resp, err = s.client.CancelTask(ctx, getStringArg(args, "task_id"))
	case "caduceus.get_local_node_status":
		resp, err = s.client.Status(ctx)
	case "caduceus.get_local_config":
		resp, err = s.client.Config(ctx)
	case "caduceus.validate_task":
		req, e := decodeRunTask(args)
		if e != nil {
			resp = control.Failure("bad_request", e.Error(), nil)
		} else {
			resp, err = s.client.ValidateTask(ctx, req)
		}
	case "caduceus.list_enrollment_requests":
		if e := decodeEmptyArguments(args); e != nil {
			resp = control.Failure("bad_request", e.Error(), nil)
		} else {
			resp, err = s.enrollmentTool(ctx, func() (control.Response, error) { return s.client.ListEnrollmentRequests(ctx) })
		}
	case "caduceus.approve_enrollment":
		var p struct {
			RequestID  string `json:"request_id"`
			ApprovedBy string `json:"approved_by"`
		}
		if e := decodeArguments(args, &p); e != nil {
			resp = control.Failure("bad_request", e.Error(), nil)
		} else if strings.TrimSpace(p.RequestID) == "" {
			resp = control.Failure("bad_request", "request_id is required", nil)
		} else {
			resp, err = s.enrollmentTool(ctx, func() (control.Response, error) {
				return s.client.ApproveEnrollment(ctx, strings.TrimSpace(p.RequestID), strings.TrimSpace(p.ApprovedBy))
			})
		}
	case "caduceus.deny_enrollment":
		var p struct {
			RequestID string `json:"request_id"`
			DeniedBy  string `json:"denied_by"`
		}
		if e := decodeArguments(args, &p); e != nil {
			resp = control.Failure("bad_request", e.Error(), nil)
		} else if strings.TrimSpace(p.RequestID) == "" {
			resp = control.Failure("bad_request", "request_id is required", nil)
		} else {
			resp, err = s.enrollmentTool(ctx, func() (control.Response, error) {
				return s.client.DenyEnrollment(ctx, strings.TrimSpace(p.RequestID), strings.TrimSpace(p.DeniedBy))
			})
		}
	default:
		return nil, fmt.Errorf("unknown tool %q", name)
	}
	if err != nil {
		resp = control.Failure("control_error", err.Error(), nil)
	}
	text, _ := json.MarshalIndent(resp, "", "  ")
	return map[string]any{
		"content":           []map[string]any{{"type": "text", "text": string(text)}},
		"structuredContent": resp,
		"isError":           !resp.OK,
	}, nil
}

// Check the daemon configuration on every invocation, including calls made
// directly through MCP rather than through the Hermes command adapter.
func (s *Server) enrollmentTool(ctx context.Context, action func() (control.Response, error)) (control.Response, error) {
	response, err := s.client.Config(ctx)
	if err != nil || !response.OK {
		return response, err
	}
	data, err := json.Marshal(response.Data)
	if err != nil {
		return control.Failure("invalid_config", "could not read integration mode", nil), nil
	}
	var settings struct {
		Config struct {
			Enrollment struct {
				TrustedLAN struct {
					HermesMode string `json:"hermes_mode"`
				} `json:"trusted_lan"`
			} `json:"enrollment"`
		} `json:"config"`
	}
	if err := json.Unmarshal(data, &settings); err != nil || settings.Config.Enrollment.TrustedLAN.HermesMode == "" {
		return control.Failure("invalid_config", "could not read integration mode", nil), nil
	}
	if settings.Config.Enrollment.TrustedLAN.HermesMode != "invoked" {
		return control.Failure("hermes_mode_disabled", "Hermes enrollment integration is disabled", nil), nil
	}
	return action()
}

func (s *Server) readResource(ctx context.Context, uri string) (any, error) {
	var resp control.Response
	var err error
	switch {
	case uri == "caduceus://local/status":
		resp, err = s.client.Status(ctx)
	case uri == "caduceus://local/config":
		resp, err = s.client.Config(ctx)
	case uri == "caduceus://workers":
		resp, err = s.client.ListWorkers(ctx)
	case strings.HasPrefix(uri, "caduceus://workers/"):
		resp, err = s.client.GetWorker(ctx, strings.TrimPrefix(uri, "caduceus://workers/"))
	case strings.HasPrefix(uri, "caduceus://tasks/") && strings.HasSuffix(uri, "/events"):
		taskID := strings.TrimSuffix(strings.TrimPrefix(uri, "caduceus://tasks/"), "/events")
		resp, err = s.client.GetTaskEvents(ctx, taskID, 0)
	case strings.HasPrefix(uri, "caduceus://tasks/") && strings.Contains(uri, "/artifacts/"):
		taskID := strings.Split(strings.TrimPrefix(uri, "caduceus://tasks/"), "/artifacts/")[0]
		resp, err = s.client.GetTaskArtifacts(ctx, taskID)
	case strings.HasPrefix(uri, "caduceus://tasks/"):
		resp, err = s.client.GetTaskStatus(ctx, strings.TrimPrefix(uri, "caduceus://tasks/"))
	default:
		return nil, fmt.Errorf("unknown resource %q", uri)
	}
	if err != nil {
		resp = control.Failure("control_error", err.Error(), nil)
	}
	text, _ := json.MarshalIndent(resp, "", "  ")
	return map[string]any{
		"contents": []map[string]any{{"uri": uri, "mimeType": "application/json", "text": string(text)}},
	}, nil
}

func decodeRunTask(args json.RawMessage) (control.RunTaskRequest, error) {
	var req control.RunTaskRequest
	if err := decodeArguments(args, &req); err != nil {
		return req, err
	}
	if strings.TrimSpace(req.Kind) == "" {
		req.Kind = tasks.KindPrompt
	}
	if err := tasks.ValidateRequest(taskRequest(req)); err != nil {
		return req, err
	}
	return req, nil
}

func decodeTaskRequest(args json.RawMessage) (tasks.Request, error) {
	req, err := decodeRunTask(args)
	if err != nil {
		return tasks.Request{}, err
	}
	return taskRequest(req), nil
}

func taskRequest(req control.RunTaskRequest) tasks.Request {
	return tasks.Request{
		TaskID:         req.TaskID,
		Kind:           req.Kind,
		Task:           req.Task,
		Constraints:    req.Constraints,
		TimeoutSeconds: req.TimeoutSeconds,
		TrustLevel:     req.TrustLevel,
		WorkerID:       req.WorkerID,
		Idempotent:     req.Idempotent,
		MaxAttempts:    req.MaxAttempts,
	}
}

func decodeRunPrompt(args json.RawMessage) (control.RunTaskRequest, error) {
	var p struct {
		TaskID         string            `json:"task_id"`
		Kind           string            `json:"kind"`
		WorkerID       string            `json:"worker_id"`
		Prompt         string            `json:"prompt"`
		System         string            `json:"system"`
		Model          string            `json:"model"`
		Temperature    *float64          `json:"temperature"`
		MaxTokens      int               `json:"max_tokens"`
		Stream         *bool             `json:"stream"`
		TimeoutSeconds int               `json:"timeout_seconds"`
		TrustLevel     string            `json:"trust_level"`
		Constraints    tasks.Constraints `json:"constraints"`
		Idempotent     bool              `json:"idempotent"`
		MaxAttempts    int               `json:"max_attempts"`
	}
	if err := decodeArguments(args, &p); err != nil {
		return control.RunTaskRequest{}, err
	}
	if strings.TrimSpace(p.Prompt) == "" {
		return control.RunTaskRequest{}, errors.New("prompt is required")
	}
	if strings.TrimSpace(p.Kind) == "" {
		p.Kind = tasks.KindPrompt
	}
	if !containsString(p.Constraints.RequiredCapabilities, "llm") {
		p.Constraints.RequiredCapabilities = append(p.Constraints.RequiredCapabilities, "llm")
	}
	stream := true
	if p.Stream != nil {
		stream = *p.Stream
	}
	req := control.RunTaskRequest{
		TaskID:   p.TaskID,
		Kind:     p.Kind,
		WorkerID: p.WorkerID,
		Task: tasks.PromptTask{
			Prompt:      p.Prompt,
			System:      p.System,
			Model:       p.Model,
			Temperature: p.Temperature,
			MaxTokens:   p.MaxTokens,
			Stream:      stream,
		},
		Constraints:    p.Constraints,
		TimeoutSeconds: p.TimeoutSeconds,
		TrustLevel:     p.TrustLevel,
		Idempotent:     p.Idempotent,
		MaxAttempts:    p.MaxAttempts,
	}
	if err := tasks.ValidateRequest(taskRequest(req)); err != nil {
		return control.RunTaskRequest{}, err
	}
	return req, nil
}

func decodeArguments(args json.RawMessage, target any) error {
	if len(bytes.TrimSpace(args)) == 0 {
		return errors.New("arguments are required")
	}
	decoder := json.NewDecoder(bytes.NewReader(args))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func decodeEmptyArguments(args json.RawMessage) error {
	if len(bytes.TrimSpace(args)) == 0 {
		return nil
	}
	var values map[string]json.RawMessage
	if err := decodeArguments(args, &values); err != nil {
		return err
	}
	if len(values) != 0 {
		return errors.New("this tool does not accept arguments")
	}
	return nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func getStringArg(args json.RawMessage, key string) string {
	var m map[string]any
	_ = json.Unmarshal(args, &m)
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

type stdioFraming int

const (
	framingJSONLines stdioFraming = iota
	framingContentLength
)

// readMessage reads the MCP newline-delimited stdio format. It also accepts
// the Content-Length framing used by earlier Caduceus clients and records the
// input format so the response can preserve compatibility for that request.
func readMessage(r *bufio.Reader) ([]byte, stdioFraming, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, framingJSONLines, err
	}
	trimmed := strings.TrimSpace(line)
	for trimmed == "" {
		line, err = r.ReadString('\n')
		if err != nil {
			return nil, framingJSONLines, err
		}
		trimmed = strings.TrimSpace(line)
	}

	if !strings.HasPrefix(strings.ToLower(trimmed), "content-length:") {
		return []byte(trimmed), framingJSONLines, nil
	}

	length := -1
	for {
		header := strings.TrimRight(line, "\r\n")
		if header == "" {
			break
		}
		parts := strings.SplitN(header, ":", 2)
		if len(parts) != 2 {
			return nil, framingContentLength, fmt.Errorf("invalid MCP header %q", header)
		} else if strings.EqualFold(strings.TrimSpace(parts[0]), "Content-Length") {
			n, err := strconv.Atoi(strings.TrimSpace(parts[1]))
			if err != nil {
				return nil, framingContentLength, err
			}
			length = n
		}
		line, err = r.ReadString('\n')
		if err != nil {
			return nil, framingContentLength, err
		}
	}
	if length < 0 {
		return nil, framingContentLength, errors.New("missing Content-Length")
	}
	body := make([]byte, length)
	_, err = io.ReadFull(r, body)
	return body, framingContentLength, err
}

func (s *Server) write(resp rpcResponse, framing stdioFraming) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if framing == framingContentLength {
		_, err = fmt.Fprintf(s.out, "Content-Length: %d\r\n\r\n%s", len(data), data)
		return err
	}
	_, err = fmt.Fprintf(s.out, "%s\n", data)
	return err
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// MarshalFrame encodes the legacy Content-Length format for compatibility
// tests and clients. New MCP stdio clients should use newline-delimited JSON.
func MarshalFrame(v any) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Content-Length: %d\r\n\r\n", len(body))
	buf.Write(body)
	return buf.Bytes(), nil
}
