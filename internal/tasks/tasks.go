package tasks

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	KindPrompt = "prompt"

	StatusQueued    = "queued"
	StatusAccepted  = "accepted"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCanceled  = "canceled"

	EventAccepted  = "accepted"
	EventStarted   = "started"
	EventToken     = "token"
	EventProgress  = "progress"
	EventArtifact  = "artifact"
	EventCompleted = "completed"
	EventFailed    = "failed"
	EventCanceled  = "canceled"
)

type PromptTask struct {
	Prompt      string   `json:"prompt" yaml:"prompt"`
	System      string   `json:"system,omitempty" yaml:"system,omitempty"`
	Model       string   `json:"model,omitempty" yaml:"model,omitempty"`
	Temperature *float64 `json:"temperature,omitempty" yaml:"temperature,omitempty"`
	MaxTokens   int      `json:"max_tokens,omitempty" yaml:"max_tokens,omitempty"`
	Stream      bool     `json:"stream" yaml:"stream"`
}

type Constraints struct {
	RequiredCapabilities []string `json:"required_capabilities,omitempty" yaml:"required_capabilities,omitempty"`
	PreferredModels      []string `json:"preferred_models,omitempty" yaml:"preferred_models,omitempty"`
	MaxRuntimeSeconds    int      `json:"max_runtime_seconds,omitempty" yaml:"max_runtime_seconds,omitempty"`
}

type Request struct {
	TaskID         string      `json:"task_id" yaml:"task_id"`
	Kind           string      `json:"kind" yaml:"kind"`
	Task           PromptTask  `json:"task" yaml:"task"`
	Constraints    Constraints `json:"constraints" yaml:"constraints"`
	TimeoutSeconds int         `json:"timeout_seconds" yaml:"timeout_seconds"`
	TrustLevel     string      `json:"trust_level" yaml:"trust_level"`
	WorkerID       string      `json:"worker_id,omitempty" yaml:"worker_id,omitempty"`
}

type Metadata struct {
	TaskID        string     `json:"task_id" yaml:"task_id"`
	Kind          string     `json:"kind" yaml:"kind"`
	Status        string     `json:"status" yaml:"status"`
	RequesterPeer string     `json:"requester_peer" yaml:"requester_peer"`
	WorkerPeer    string     `json:"worker_peer" yaml:"worker_peer"`
	CreatedAt     time.Time  `json:"created_at" yaml:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at" yaml:"updated_at"`
	StartedAt     *time.Time `json:"started_at,omitempty" yaml:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty" yaml:"finished_at,omitempty"`
	Request       Request    `json:"request" yaml:"request"`
	Error         string     `json:"error,omitempty" yaml:"error,omitempty"`
}

type Event struct {
	TaskID     string    `json:"task_id" yaml:"task_id"`
	Cursor     int64     `json:"cursor" yaml:"cursor"`
	EventType  string    `json:"event_type" yaml:"event_type"`
	Message    string    `json:"message,omitempty" yaml:"message,omitempty"`
	Delta      string    `json:"delta,omitempty" yaml:"delta,omitempty"`
	ArtifactID string    `json:"artifact_id,omitempty" yaml:"artifact_id,omitempty"`
	Timestamp  time.Time `json:"timestamp" yaml:"timestamp"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens" yaml:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens" yaml:"completion_tokens"`
	TotalTokens      int `json:"total_tokens" yaml:"total_tokens"`
}

type Artifact struct {
	ArtifactID  string    `json:"artifact_id" yaml:"artifact_id"`
	Name        string    `json:"name" yaml:"name"`
	ContentType string    `json:"content_type" yaml:"content_type"`
	SizeBytes   int64     `json:"size_bytes" yaml:"size_bytes"`
	Path        string    `json:"path,omitempty" yaml:"path,omitempty"`
	CreatedAt   time.Time `json:"created_at" yaml:"created_at"`
}

type Result struct {
	TaskID     string     `json:"task_id" yaml:"task_id"`
	Status     string     `json:"status" yaml:"status"`
	OutputText string     `json:"output_text" yaml:"output_text"`
	Error      any        `json:"error" yaml:"error"`
	Usage      Usage      `json:"usage" yaml:"usage"`
	Artifacts  []Artifact `json:"artifacts" yaml:"artifacts"`
}

func ValidateRequest(req Request) error {
	if strings.TrimSpace(req.Kind) == "" {
		req.Kind = KindPrompt
	}
	if req.Kind != KindPrompt {
		return fmt.Errorf("unsupported task kind %q", req.Kind)
	}
	if strings.TrimSpace(req.Task.Prompt) == "" {
		return errors.New("prompt is required")
	}
	if len(req.Task.Prompt) > 128*1024 {
		return errors.New("prompt exceeds 128 KiB phase I limit")
	}
	if len(req.Task.System) > 64*1024 {
		return errors.New("system prompt exceeds 64 KiB phase I limit")
	}
	if req.Task.MaxTokens < 0 {
		return errors.New("max_tokens must be non-negative")
	}
	if req.TimeoutSeconds < 0 {
		return errors.New("timeout_seconds must be non-negative")
	}
	if req.TimeoutSeconds > 3600 {
		return errors.New("timeout_seconds exceeds one hour phase I limit")
	}
	if req.Constraints.MaxRuntimeSeconds < 0 {
		return errors.New("max_runtime_seconds must be non-negative")
	}
	if req.Constraints.MaxRuntimeSeconds > 3600 {
		return errors.New("max_runtime_seconds exceeds one hour phase I limit")
	}
	if req.Task.Temperature != nil && (*req.Task.Temperature < 0 || *req.Task.Temperature > 2) {
		return errors.New("temperature must be between 0 and 2")
	}
	for _, cap := range req.Constraints.RequiredCapabilities {
		if strings.TrimSpace(cap) == "" {
			return errors.New("required capabilities cannot contain empty values")
		}
	}
	return nil
}

func NewEvent(taskID string, cursor int64, eventType, message, delta string) Event {
	return Event{
		TaskID:    taskID,
		Cursor:    cursor,
		EventType: eventType,
		Message:   message,
		Delta:     delta,
		Timestamp: time.Now().UTC(),
	}
}

func NewID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	hexed := hex.EncodeToString(b[:])
	id := hexed[0:8] + "-" + hexed[8:12] + "-" + hexed[12:16] + "-" + hexed[16:20] + "-" + hexed[20:32]
	if strings.TrimSpace(prefix) == "" {
		return id
	}
	return prefix + "-" + id
}
