package control

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"

	"github.com/caduceus/caduceus/internal/tasks"
)

type Response struct {
	OK    bool      `json:"ok"`
	Data  any       `json:"data,omitempty"`
	Error *APIError `json:"error,omitempty"`
}

type APIError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

type RunTaskRequest struct {
	WorkerID       string            `json:"worker_id,omitempty"`
	Task           tasks.PromptTask  `json:"task"`
	Constraints    tasks.Constraints `json:"constraints,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	TrustLevel     string            `json:"trust_level,omitempty"`
}

type ListTasksFilter struct {
	Status string `json:"status,omitempty"`
}

type Backend interface {
	Status(ctx context.Context) (any, error)
	Config(ctx context.Context) (any, error)
	ListWorkers(ctx context.Context) (any, error)
	GetWorker(ctx context.Context, workerID string) (any, error)
	RunTask(ctx context.Context, req RunTaskRequest) (any, error)
	ListTasks(ctx context.Context, filter ListTasksFilter) (any, error)
	GetTaskStatus(ctx context.Context, taskID string) (any, error)
	GetTaskResult(ctx context.Context, taskID string) (any, error)
	GetTaskEvents(ctx context.Context, taskID string, cursor int64) (any, error)
	GetTaskArtifacts(ctx context.Context, taskID string) (any, error)
	CancelTask(ctx context.Context, taskID string) (any, error)
	ValidateTask(ctx context.Context, req RunTaskRequest) (any, error)
}

func Success(data any) Response {
	return Response{OK: true, Data: data}
}

func Failure(code, message string, details map[string]any) Response {
	return Response{OK: false, Error: &APIError{Code: code, Message: message, Details: details}}
}

func EnsureAuthToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		token := string(bytesTrimSpace(data))
		if token != "" {
			return token, nil
		}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := hex.EncodeToString(raw)
	return token, os.WriteFile(path, []byte(token+"\n"), 0o600)
}

func bytesTrimSpace(in []byte) []byte {
	start := 0
	end := len(in)
	for start < end && (in[start] == ' ' || in[start] == '\n' || in[start] == '\r' || in[start] == '\t') {
		start++
	}
	for end > start && (in[end-1] == ' ' || in[end-1] == '\n' || in[end-1] == '\r' || in[end-1] == '\t') {
		end--
	}
	return in[start:end]
}
