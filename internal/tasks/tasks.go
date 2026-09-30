package tasks

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	KindPrompt = "prompt"

	StatusQueued      = "queued"
	StatusAccepted    = "accepted"
	StatusRunning     = "running"
	StatusCompleted   = "completed"
	StatusFailed      = "failed"
	StatusCanceled    = "canceled"
	StatusInterrupted = "interrupted"
	StatusLost        = "lost"

	EventQueued      = "queued"
	EventAccepted    = "accepted"
	EventStarted     = "started"
	EventToken       = "token"
	EventProgress    = "progress"
	EventArtifact    = "artifact"
	EventCompleted   = "completed"
	EventFailed      = "failed"
	EventCanceled    = "canceled"
	EventInterrupted = "interrupted"
	EventLost        = "lost"
	EventRejected    = "rejected"

	AttemptStateQueued      = "queued"
	AttemptStateAccepted    = "accepted"
	AttemptStateRunning     = "running"
	AttemptStateCompleted   = "completed"
	AttemptStateFailed      = "failed"
	AttemptStateCanceled    = "canceled"
	AttemptStateInterrupted = "interrupted"
	AttemptStateLost        = "lost"
	AttemptStateRejected    = "rejected"

	DefaultMaxAttempts           = 1
	MaxAttemptsLimit             = 10
	TaskRejectionProtocolVersion = "2.0"

	maxIdentifierBytes = 512
	maxConstraintItems = 256
	maxConstraintBytes = 512
	maxDetailBytes     = 4 * 1024
	maxRetryAfter      = 24 * time.Hour
	maxResourceBytes   = uint64(1 << 60)
)

type RejectionReason string

const (
	RejectAtCapacity            RejectionReason = "at_capacity"
	RejectQueueFull             RejectionReason = "queue_full"
	RejectWorkerDraining        RejectionReason = "worker_draining"
	RejectWorkerUnavailable     RejectionReason = "worker_unavailable"
	RejectCapabilityMissing     RejectionReason = "capability_missing"
	RejectModelUnavailable      RejectionReason = "model_unavailable"
	RejectInsufficientCPU       RejectionReason = "insufficient_cpu"
	RejectInsufficientRAM       RejectionReason = "insufficient_ram"
	RejectInsufficientGPUMemory RejectionReason = "insufficient_gpu_memory"
	RejectNotAllowed            RejectionReason = "not_allowed"
	RejectInsufficientTrust     RejectionReason = "insufficient_trust"
	RejectStaleAssignment       RejectionReason = "stale_assignment"
	RejectUnsupportedTask       RejectionReason = "unsupported_task"
	RejectInternalError         RejectionReason = "internal_error"
)

type PromptTask struct {
	Prompt      string   `json:"prompt" yaml:"prompt"`
	System      string   `json:"system,omitempty" yaml:"system,omitempty"`
	Model       string   `json:"model,omitempty" yaml:"model,omitempty"`
	Temperature *float64 `json:"temperature,omitempty" yaml:"temperature,omitempty"`
	MaxTokens   int      `json:"max_tokens,omitempty" yaml:"max_tokens,omitempty"`
	Stream      bool     `json:"stream" yaml:"stream"`
}

// Constraints contains intentionally small, explicit hard requirements. The
// original Phase I fields retain their names and wire representation.
type Constraints struct {
	RequiredCapabilities    []string `json:"required_capabilities,omitempty" yaml:"required_capabilities,omitempty"`
	PreferredModels         []string `json:"preferred_models,omitempty" yaml:"preferred_models,omitempty"`
	MaxRuntimeSeconds       int      `json:"max_runtime_seconds,omitempty" yaml:"max_runtime_seconds,omitempty"`
	MinCPU                  float64  `json:"min_cpu,omitempty" yaml:"min_cpu,omitempty"`
	MinRAMBytes             uint64   `json:"min_ram_bytes,omitempty" yaml:"min_ram_bytes,omitempty"`
	GPURequired             bool     `json:"gpu_required,omitempty" yaml:"gpu_required,omitempty"`
	GPUVendor               string   `json:"gpu_vendor,omitempty" yaml:"gpu_vendor,omitempty"`
	MinGPUMemoryBytes       uint64   `json:"min_gpu_memory_bytes,omitempty" yaml:"min_gpu_memory_bytes,omitempty"`
	RequiredGPUCapabilities []string `json:"required_gpu_capabilities,omitempty" yaml:"required_gpu_capabilities,omitempty"`
	RequiredModel           string   `json:"required_model,omitempty" yaml:"required_model,omitempty"`
	RequiredRuntime         string   `json:"required_runtime,omitempty" yaml:"required_runtime,omitempty"`
	RequiredTrustLevel      string   `json:"required_trust_level,omitempty" yaml:"required_trust_level,omitempty"`
	AllowedWorkerIDs        []string `json:"allowed_worker_ids,omitempty" yaml:"allowed_worker_ids,omitempty"`
	AllowedGroups           []string `json:"allowed_groups,omitempty" yaml:"allowed_groups,omitempty"`
}

type Request struct {
	TaskID         string      `json:"task_id" yaml:"task_id"`
	Kind           string      `json:"kind" yaml:"kind"`
	Task           PromptTask  `json:"task" yaml:"task"`
	Constraints    Constraints `json:"constraints" yaml:"constraints"`
	TimeoutSeconds int         `json:"timeout_seconds" yaml:"timeout_seconds"`
	TrustLevel     string      `json:"trust_level" yaml:"trust_level"`
	WorkerID       string      `json:"worker_id,omitempty" yaml:"worker_id,omitempty"`
	Idempotent     bool        `json:"idempotent,omitempty" yaml:"idempotent,omitempty"`
	MaxAttempts    int         `json:"max_attempts,omitempty" yaml:"max_attempts,omitempty"`
}

// EffectiveMaxAttempts preserves the conservative Phase I behavior: an
// omitted value allows one attempt, and non-idempotent work is never replayed.
func (r Request) EffectiveMaxAttempts() int {
	if !r.Idempotent || r.MaxAttempts <= 0 {
		return DefaultMaxAttempts
	}
	return r.MaxAttempts
}

type TaskAttempt struct {
	AttemptID     string     `json:"attempt_id" yaml:"attempt_id"`
	AttemptToken  string     `json:"attempt_token" yaml:"attempt_token"`
	AttemptNumber int        `json:"attempt_number" yaml:"attempt_number"`
	TaskID        string     `json:"task_id" yaml:"task_id"`
	WorkerID      string     `json:"worker_id" yaml:"worker_id"`
	WorkerSession string     `json:"worker_session_id,omitempty" yaml:"worker_session_id,omitempty"`
	State         string     `json:"state" yaml:"state"`
	CreatedAt     time.Time  `json:"created_at" yaml:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at" yaml:"updated_at"`
	StartedAt     *time.Time `json:"started_at,omitempty" yaml:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty" yaml:"finished_at,omitempty"`
	Error         string     `json:"error,omitempty" yaml:"error,omitempty"`
	Interruption  string     `json:"interruption_reason,omitempty" yaml:"interruption_reason,omitempty"`
}

func NewAttempt(taskID, workerID, sessionID string, number int) (TaskAttempt, error) {
	token, err := newOpaqueToken()
	if err != nil {
		return TaskAttempt{}, err
	}
	now := time.Now().UTC()
	attempt := TaskAttempt{
		AttemptID:     NewID("attempt"),
		AttemptToken:  token,
		AttemptNumber: number,
		TaskID:        taskID,
		WorkerID:      workerID,
		WorkerSession: sessionID,
		State:         AttemptStateQueued,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := attempt.Validate(); err != nil {
		return TaskAttempt{}, err
	}
	return attempt, nil
}

func (a TaskAttempt) Validate() error {
	if err := validateRequiredIdentifier("attempt_id", a.AttemptID); err != nil {
		return err
	}
	if err := validateRequiredIdentifier("attempt_token", a.AttemptToken); err != nil {
		return err
	}
	if err := validateRequiredIdentifier("task_id", a.TaskID); err != nil {
		return err
	}
	if err := validateRequiredIdentifier("worker_id", a.WorkerID); err != nil {
		return err
	}
	if a.AttemptNumber < 1 || a.AttemptNumber > MaxAttemptsLimit {
		return fmt.Errorf("attempt_number must be between 1 and %d", MaxAttemptsLimit)
	}
	if !ValidAttemptState(a.State) {
		return fmt.Errorf("invalid attempt state %q", a.State)
	}
	if a.CreatedAt.IsZero() {
		return errors.New("attempt created_at is required")
	}
	if len(a.Error) > maxDetailBytes || len(a.Interruption) > maxDetailBytes {
		return errors.New("attempt error or interruption reason is too large")
	}
	if a.StartedAt != nil && a.StartedAt.Before(a.CreatedAt) {
		return errors.New("attempt started_at precedes created_at")
	}
	if a.FinishedAt != nil && a.StartedAt != nil && a.FinishedAt.Before(*a.StartedAt) {
		return errors.New("attempt finished_at precedes started_at")
	}
	return nil
}

type ResourceDeficiency struct {
	Resource  string   `json:"resource" yaml:"resource"`
	Required  float64  `json:"required" yaml:"required"`
	Available *float64 `json:"available,omitempty" yaml:"available,omitempty"`
	Unit      string   `json:"unit" yaml:"unit"`
	Detail    string   `json:"detail,omitempty" yaml:"detail,omitempty"`
}

type TaskRejection struct {
	ProtocolVersion      string               `json:"protocol_version" yaml:"protocol_version"`
	TaskID               string               `json:"task_id" yaml:"task_id"`
	AttemptID            string               `json:"attempt_id,omitempty" yaml:"attempt_id,omitempty"`
	WorkerID             string               `json:"worker_id" yaml:"worker_id"`
	Reason               RejectionReason      `json:"reason" yaml:"reason"`
	Detail               string               `json:"detail,omitempty" yaml:"detail,omitempty"`
	Retryable            bool                 `json:"retryable" yaml:"retryable"`
	RetryAfterSeconds    int                  `json:"retry_after,omitempty" yaml:"retry_after,omitempty"`
	CurrentQueueDepth    int                  `json:"current_queue_depth" yaml:"current_queue_depth"`
	CurrentRunningTasks  int                  `json:"current_running_tasks" yaml:"current_running_tasks"`
	MaxConcurrentTasks   int                  `json:"max_concurrent_tasks" yaml:"max_concurrent_tasks"`
	ResourceDeficiencies []ResourceDeficiency `json:"resource_deficiencies,omitempty" yaml:"resource_deficiencies,omitempty"`
	Timestamp            time.Time            `json:"timestamp" yaml:"timestamp"`
}

func NewRejection(taskID, attemptID, workerID string, reason RejectionReason, detail string) TaskRejection {
	return TaskRejection{
		ProtocolVersion: TaskRejectionProtocolVersion,
		TaskID:          taskID,
		AttemptID:       attemptID,
		WorkerID:        workerID,
		Reason:          reason,
		Detail:          detail,
		Timestamp:       time.Now().UTC(),
	}
}

func (r TaskRejection) RetryAfterDuration() time.Duration {
	return time.Duration(r.RetryAfterSeconds) * time.Second
}

func (r TaskRejection) Validate() error {
	if r.ProtocolVersion != TaskRejectionProtocolVersion {
		return fmt.Errorf("unsupported task rejection protocol version %q", r.ProtocolVersion)
	}
	if err := validateRequiredIdentifier("task_id", r.TaskID); err != nil {
		return err
	}
	if err := validateRequiredIdentifier("worker_id", r.WorkerID); err != nil {
		return err
	}
	if r.AttemptID != "" && len(r.AttemptID) > maxIdentifierBytes {
		return errors.New("attempt_id is too large")
	}
	if !ValidRejectionReason(r.Reason) {
		return fmt.Errorf("invalid task rejection reason %q", r.Reason)
	}
	if len(r.Detail) > maxDetailBytes {
		return errors.New("task rejection detail is too large")
	}
	if r.RetryAfterSeconds < 0 || r.RetryAfterSeconds > int(maxRetryAfter/time.Second) {
		return errors.New("retry_after must be between zero and 24 hours")
	}
	if r.CurrentQueueDepth < 0 || r.CurrentRunningTasks < 0 || r.MaxConcurrentTasks < 0 {
		return errors.New("task rejection capacity values must be non-negative")
	}
	if r.Timestamp.IsZero() {
		return errors.New("task rejection timestamp is required")
	}
	if len(r.ResourceDeficiencies) > 32 {
		return errors.New("too many resource deficiencies")
	}
	for i, deficiency := range r.ResourceDeficiencies {
		if strings.TrimSpace(deficiency.Resource) == "" || len(deficiency.Resource) > 128 {
			return fmt.Errorf("resource deficiency %d has invalid resource", i)
		}
		if !finiteNonNegative(deficiency.Required) {
			return fmt.Errorf("resource deficiency %d has invalid required value", i)
		}
		if deficiency.Available != nil && !finiteNonNegative(*deficiency.Available) {
			return fmt.Errorf("resource deficiency %d has invalid available value", i)
		}
		if len(deficiency.Unit) > 32 || len(deficiency.Detail) > 1024 {
			return fmt.Errorf("resource deficiency %d is too large", i)
		}
	}
	return nil
}

type RoutingCandidateSummary struct {
	WorkerID        string             `json:"worker_id" yaml:"worker_id"`
	Eligible        bool               `json:"eligible" yaml:"eligible"`
	FilterReasons   []string           `json:"filter_reasons,omitempty" yaml:"filter_reasons,omitempty"`
	MissingMetrics  []string           `json:"missing_metrics,omitempty" yaml:"missing_metrics,omitempty"`
	ComponentScores map[string]float64 `json:"component_scores,omitempty" yaml:"component_scores,omitempty"`
	TotalScore      float64            `json:"total_score,omitempty" yaml:"total_score,omitempty"`
	Rank            int                `json:"rank,omitempty" yaml:"rank,omitempty"`
	Selected        bool               `json:"selected,omitempty" yaml:"selected,omitempty"`
	TieBreak        string             `json:"tie_break,omitempty" yaml:"tie_break,omitempty"`
}

type RoutingDecision struct {
	TaskID                  string                    `json:"task_id" yaml:"task_id"`
	SelectedWorkerID        string                    `json:"selected_worker_id,omitempty" yaml:"selected_worker_id,omitempty"`
	RegistrySnapshotTime    time.Time                 `json:"registry_snapshot_time" yaml:"registry_snapshot_time"`
	RegistrySnapshotVersion uint64                    `json:"registry_snapshot_version" yaml:"registry_snapshot_version"`
	Candidates              []RoutingCandidateSummary `json:"candidates" yaml:"candidates"`
	AppliedWeights          map[string]float64        `json:"applied_weights,omitempty" yaml:"applied_weights,omitempty"`
	MissingMetrics          []string                  `json:"missing_metrics,omitempty" yaml:"missing_metrics,omitempty"`
	TieBreakDetails         string                    `json:"tie_break_details,omitempty" yaml:"tie_break_details,omitempty"`
	Explanation             string                    `json:"explanation,omitempty" yaml:"explanation,omitempty"`
	Timestamp               time.Time                 `json:"timestamp" yaml:"timestamp"`
}

type QueueEntry struct {
	TaskID     string    `json:"task_id" yaml:"task_id"`
	Owner      string    `json:"owner" yaml:"owner"`
	Sequence   uint64    `json:"sequence" yaml:"sequence"`
	EnqueuedAt time.Time `json:"enqueued_at" yaml:"enqueued_at"`
	Position   int       `json:"position,omitempty" yaml:"position,omitempty"`
}

type Metadata struct {
	TaskID              string           `json:"task_id" yaml:"task_id"`
	Kind                string           `json:"kind" yaml:"kind"`
	Status              string           `json:"status" yaml:"status"`
	RequesterPeer       string           `json:"requester_peer" yaml:"requester_peer"`
	WorkerPeer          string           `json:"worker_peer" yaml:"worker_peer"`
	CreatedAt           time.Time        `json:"created_at" yaml:"created_at"`
	UpdatedAt           time.Time        `json:"updated_at" yaml:"updated_at"`
	StartedAt           *time.Time       `json:"started_at,omitempty" yaml:"started_at,omitempty"`
	FinishedAt          *time.Time       `json:"finished_at,omitempty" yaml:"finished_at,omitempty"`
	Request             Request          `json:"request" yaml:"request"`
	Error               string           `json:"error,omitempty" yaml:"error,omitempty"`
	Idempotent          bool             `json:"idempotent,omitempty" yaml:"idempotent,omitempty"`
	MaxAttempts         int              `json:"max_attempts,omitempty" yaml:"max_attempts,omitempty"`
	CurrentAttempt      int              `json:"current_attempt,omitempty" yaml:"current_attempt,omitempty"`
	TotalAttempts       int              `json:"total_attempts,omitempty" yaml:"total_attempts,omitempty"`
	Attempts            []TaskAttempt    `json:"attempts,omitempty" yaml:"attempts,omitempty"`
	Rejections          []TaskRejection  `json:"rejections,omitempty" yaml:"rejections,omitempty"`
	QueueOwner          string           `json:"queue_owner,omitempty" yaml:"queue_owner,omitempty"`
	QueueSequence       uint64           `json:"queue_sequence,omitempty" yaml:"queue_sequence,omitempty"`
	QueuePosition       int              `json:"queue_position,omitempty" yaml:"queue_position,omitempty"`
	QueuePositionExact  bool             `json:"queue_position_exact,omitempty" yaml:"queue_position_exact,omitempty"`
	QueuedAt            *time.Time       `json:"queued_at,omitempty" yaml:"queued_at,omitempty"`
	InterruptionReason  string           `json:"interruption_reason,omitempty" yaml:"interruption_reason,omitempty"`
	LastRoutingDecision *RoutingDecision `json:"last_routing_decision,omitempty" yaml:"last_routing_decision,omitempty"`
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
	TaskID       string     `json:"task_id" yaml:"task_id"`
	AttemptID    string     `json:"attempt_id,omitempty" yaml:"attempt_id,omitempty"`
	AttemptToken string     `json:"attempt_token,omitempty" yaml:"attempt_token,omitempty"`
	Status       string     `json:"status" yaml:"status"`
	OutputText   string     `json:"output_text" yaml:"output_text"`
	Error        any        `json:"error" yaml:"error"`
	Usage        Usage      `json:"usage" yaml:"usage"`
	Artifacts    []Artifact `json:"artifacts" yaml:"artifacts"`
}

func ValidateRequest(req Request) error {
	kind := strings.TrimSpace(req.Kind)
	if kind == "" {
		kind = KindPrompt
	}
	if kind != KindPrompt {
		return fmt.Errorf("unsupported task kind %q", req.Kind)
	}
	if err := validateOptionalIdentifier("task_id", req.TaskID); err != nil {
		return err
	}
	if err := validateOptionalIdentifier("worker_id", req.WorkerID); err != nil {
		return err
	}
	if strings.TrimSpace(req.Task.Prompt) == "" {
		return errors.New("prompt is required")
	}
	if len(req.Task.Prompt) > 128*1024 {
		return errors.New("prompt exceeds 128 KiB limit")
	}
	if len(req.Task.System) > 64*1024 {
		return errors.New("system prompt exceeds 64 KiB limit")
	}
	if len(req.Task.Model) > maxConstraintBytes {
		return errors.New("model is too large")
	}
	if req.Task.MaxTokens < 0 || req.Task.MaxTokens > 200_000 {
		return errors.New("max_tokens must be between zero and 200000")
	}
	if req.TimeoutSeconds < 0 || req.TimeoutSeconds > 3600 {
		return errors.New("timeout_seconds must be between zero and one hour")
	}
	if req.Constraints.MaxRuntimeSeconds < 0 || req.Constraints.MaxRuntimeSeconds > 3600 {
		return errors.New("max_runtime_seconds must be between zero and one hour")
	}
	if req.Task.Temperature != nil && (!finiteNonNegative(*req.Task.Temperature) || *req.Task.Temperature > 2) {
		return errors.New("temperature must be between 0 and 2")
	}
	if req.MaxAttempts < 0 || req.MaxAttempts > MaxAttemptsLimit {
		return fmt.Errorf("max_attempts must be between zero and %d", MaxAttemptsLimit)
	}
	if !req.Idempotent && req.MaxAttempts > DefaultMaxAttempts {
		return errors.New("non-idempotent tasks cannot request more than one attempt")
	}
	if !finiteNonNegative(req.Constraints.MinCPU) || req.Constraints.MinCPU > 1024 {
		return errors.New("min_cpu must be finite and between zero and 1024")
	}
	if req.Constraints.MinRAMBytes > maxResourceBytes || req.Constraints.MinGPUMemoryBytes > maxResourceBytes {
		return errors.New("memory requirement exceeds the supported limit")
	}
	if err := validateStringList("required_capabilities", req.Constraints.RequiredCapabilities); err != nil {
		return err
	}
	if err := validateStringList("preferred_models", req.Constraints.PreferredModels); err != nil {
		return err
	}
	if err := validateStringList("required_gpu_capabilities", req.Constraints.RequiredGPUCapabilities); err != nil {
		return err
	}
	if err := validateStringList("allowed_worker_ids", req.Constraints.AllowedWorkerIDs); err != nil {
		return err
	}
	if err := validateStringList("allowed_groups", req.Constraints.AllowedGroups); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"gpu_vendor":           req.Constraints.GPUVendor,
		"required_model":       req.Constraints.RequiredModel,
		"required_runtime":     req.Constraints.RequiredRuntime,
		"required_trust_level": req.Constraints.RequiredTrustLevel,
		"trust_level":          req.TrustLevel,
	} {
		if len(value) > maxConstraintBytes {
			return fmt.Errorf("%s is too large", name)
		}
	}
	if req.Constraints.RequiredModel != "" && req.Task.Model != "" && req.Constraints.RequiredModel != req.Task.Model {
		return errors.New("required_model conflicts with task.model")
	}
	return nil
}

func ValidStatus(status string) bool {
	switch status {
	case StatusQueued, StatusAccepted, StatusRunning, StatusCompleted, StatusFailed, StatusCanceled, StatusInterrupted, StatusLost:
		return true
	default:
		return false
	}
}

func ValidAttemptState(state string) bool {
	switch state {
	case AttemptStateQueued, AttemptStateAccepted, AttemptStateRunning, AttemptStateCompleted, AttemptStateFailed, AttemptStateCanceled, AttemptStateInterrupted, AttemptStateLost, AttemptStateRejected:
		return true
	default:
		return false
	}
}

func ValidRejectionReason(reason RejectionReason) bool {
	switch reason {
	case RejectAtCapacity, RejectQueueFull, RejectWorkerDraining, RejectWorkerUnavailable,
		RejectCapabilityMissing, RejectModelUnavailable, RejectInsufficientCPU,
		RejectInsufficientRAM, RejectInsufficientGPUMemory, RejectNotAllowed,
		RejectInsufficientTrust, RejectStaleAssignment, RejectUnsupportedTask,
		RejectInternalError:
		return true
	default:
		return false
	}
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

func newOpaqueToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate attempt token: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func validateRequiredIdentifier(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	return validateOptionalIdentifier(name, value)
}

func validateOptionalIdentifier(name, value string) error {
	if len(value) > maxIdentifierBytes {
		return fmt.Errorf("%s is too large", name)
	}
	if strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("%s contains invalid control characters", name)
	}
	return nil
}

func validateStringList(name string, values []string) error {
	if len(values) > maxConstraintItems {
		return fmt.Errorf("%s contains too many values", name)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("%s cannot contain empty values", name)
		}
		if len(value) > maxConstraintBytes {
			return fmt.Errorf("%s contains an oversized value", name)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%s contains duplicate value %q", name, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func finiteNonNegative(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
