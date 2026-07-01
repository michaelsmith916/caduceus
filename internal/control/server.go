package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	endpoint string
	token    string
	backend  Backend
	server   *http.Server
	listener net.Listener
}

func NewServer(endpoint, token string, backend Backend) *Server {
	return &Server{endpoint: endpoint, token: token, backend: backend}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", s.wrap(s.handleStatus))
	mux.HandleFunc("/v1/config", s.wrap(s.handleConfig))
	mux.HandleFunc("/v1/workers", s.wrap(s.handleWorkers))
	mux.HandleFunc("/v1/workers/", s.wrap(s.handleWorker))
	mux.HandleFunc("/v1/tasks", s.wrap(s.handleTasks))
	mux.HandleFunc("/v1/tasks/run", s.wrap(s.handleRunTask))
	mux.HandleFunc("/v1/tasks/validate", s.wrap(s.handleValidateTask))
	mux.HandleFunc("/v1/tasks/", s.wrap(s.handleTaskSubresource))

	network, address, err := listenTarget(s.endpoint)
	if err != nil {
		return err
	}
	if network == "unix" {
		_ = os.Remove(address)
	}
	ln, err := net.Listen(network, address)
	if err != nil {
		return err
	}
	s.listener = ln
	s.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = s.server.Serve(ln)
	}()
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	err := s.server.Shutdown(ctx)
	if s.listener != nil && strings.HasPrefix(s.endpoint, "unix://") {
		_ = os.Remove(strings.TrimPrefix(s.endpoint, "unix://"))
	}
	return err
}

func (s *Server) wrap(fn func(http.ResponseWriter, *http.Request) Response) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if s.token != "" && r.Header.Get("Authorization") != "Bearer "+s.token {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(Failure("unauthorized", "invalid local control token", nil))
			return
		}
		resp := fn(w, r)
		if !resp.OK {
			w.WriteHeader(statusFor(resp.Error))
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func (s *Server) handleStatus(_ http.ResponseWriter, r *http.Request) Response {
	if r.Method != http.MethodGet {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	data, err := s.backend.Status(r.Context())
	return from(data, err)
}

func (s *Server) handleConfig(_ http.ResponseWriter, r *http.Request) Response {
	if r.Method != http.MethodGet {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	data, err := s.backend.Config(r.Context())
	return from(data, err)
}

func (s *Server) handleWorkers(_ http.ResponseWriter, r *http.Request) Response {
	if r.Method != http.MethodGet {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	data, err := s.backend.ListWorkers(r.Context())
	return from(data, err)
}

func (s *Server) handleWorker(_ http.ResponseWriter, r *http.Request) Response {
	if r.Method != http.MethodGet {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	workerID := strings.TrimPrefix(r.URL.Path, "/v1/workers/")
	data, err := s.backend.GetWorker(r.Context(), workerID)
	return from(data, err)
}

func (s *Server) handleTasks(_ http.ResponseWriter, r *http.Request) Response {
	if r.Method != http.MethodGet {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	data, err := s.backend.ListTasks(r.Context(), ListTasksFilter{Status: r.URL.Query().Get("status")})
	return from(data, err)
}

func (s *Server) handleRunTask(_ http.ResponseWriter, r *http.Request) Response {
	if r.Method != http.MethodPost {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	var req RunTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return Failure("bad_request", "invalid JSON body", map[string]any{"error": err.Error()})
	}
	data, err := s.backend.RunTask(r.Context(), req)
	return from(data, err)
}

func (s *Server) handleValidateTask(_ http.ResponseWriter, r *http.Request) Response {
	if r.Method != http.MethodPost {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	var req RunTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return Failure("bad_request", "invalid JSON body", map[string]any{"error": err.Error()})
	}
	data, err := s.backend.ValidateTask(r.Context(), req)
	return from(data, err)
}

func (s *Server) handleTaskSubresource(_ http.ResponseWriter, r *http.Request) Response {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return Failure("bad_request", "task id is required", nil)
	}
	taskID := parts[0]
	sub := "status"
	if len(parts) > 1 {
		sub = parts[1]
	}
	switch sub {
	case "status":
		if r.Method != http.MethodGet {
			return Failure("method_not_allowed", "method not allowed", nil)
		}
		data, err := s.backend.GetTaskStatus(r.Context(), taskID)
		return from(data, err)
	case "result":
		if r.Method != http.MethodGet {
			return Failure("method_not_allowed", "method not allowed", nil)
		}
		data, err := s.backend.GetTaskResult(r.Context(), taskID)
		return from(data, err)
	case "events":
		if r.Method != http.MethodGet {
			return Failure("method_not_allowed", "method not allowed", nil)
		}
		cursor, _ := strconv.ParseInt(r.URL.Query().Get("cursor"), 10, 64)
		data, err := s.backend.GetTaskEvents(r.Context(), taskID, cursor)
		return from(data, err)
	case "artifacts":
		if r.Method != http.MethodGet {
			return Failure("method_not_allowed", "method not allowed", nil)
		}
		data, err := s.backend.GetTaskArtifacts(r.Context(), taskID)
		return from(data, err)
	case "cancel":
		if r.Method != http.MethodPost {
			return Failure("method_not_allowed", "method not allowed", nil)
		}
		data, err := s.backend.CancelTask(r.Context(), taskID)
		return from(data, err)
	default:
		return Failure("not_found", "unknown task subresource", map[string]any{"subresource": sub})
	}
}

func from(data any, err error) Response {
	if err != nil {
		return Failure("request_failed", err.Error(), nil)
	}
	return Success(data)
}

func statusFor(err *APIError) int {
	if err == nil {
		return http.StatusInternalServerError
	}
	switch err.Code {
	case "bad_request":
		return http.StatusBadRequest
	case "unauthorized":
		return http.StatusUnauthorized
	case "not_found":
		return http.StatusNotFound
	case "method_not_allowed":
		return http.StatusMethodNotAllowed
	default:
		return http.StatusInternalServerError
	}
}

func listenTarget(endpoint string) (network, address string, err error) {
	if strings.HasPrefix(endpoint, "unix://") {
		address = strings.TrimPrefix(endpoint, "unix://")
		if address == "" {
			return "", "", errors.New("empty unix socket path")
		}
		return "unix", address, nil
	}
	if strings.HasPrefix(endpoint, "http://") {
		address = strings.TrimPrefix(endpoint, "http://")
		return "tcp", address, nil
	}
	if strings.HasPrefix(endpoint, "tcp://") {
		address = strings.TrimPrefix(endpoint, "tcp://")
		return "tcp", address, nil
	}
	if endpoint == "" {
		return "", "", errors.New("empty endpoint")
	}
	if strings.Contains(endpoint, ":") {
		return "tcp", endpoint, nil
	}
	return "", "", fmt.Errorf("unsupported control endpoint %q", endpoint)
}
