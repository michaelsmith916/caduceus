package workers

import (
	"math"
	"strings"
	"time"
)

// State is the worker's advertised lifecycle and schedulability state. Liveness
// is derived by the registry from heartbeat receipt times rather than trusted
// from this worker-provided value.
type State string

const (
	StateStarting    State = "starting"
	StateAvailable   State = "available"
	StateBusy        State = "busy"
	StateUnavailable State = "unavailable"
	StateDraining    State = "draining"
	StateOffline     State = "offline"
)

func (s State) Valid() bool {
	switch s {
	case StateStarting, StateAvailable, StateBusy, StateUnavailable, StateDraining, StateOffline:
		return true
	default:
		return false
	}
}

// MetricStatus keeps an absent or unsupported measurement distinct from a
// measured zero. Current measurements can later be marked stale without
// discarding their diagnostic value.
type MetricStatus string

const (
	MetricUnknown     MetricStatus = "unknown"
	MetricUnsupported MetricStatus = "unsupported"
	MetricCurrent     MetricStatus = "current"
	MetricStale       MetricStatus = "stale"
)

func (s MetricStatus) Valid() bool {
	switch s {
	case MetricUnknown, MetricUnsupported, MetricCurrent, MetricStale:
		return true
	default:
		return false
	}
}

// Int64Metric represents exact integral quantities such as bytes and logical
// processor counts. Value is a pointer so a measured zero is not confused with
// an unknown value.
type Int64Metric struct {
	Value       *int64       `json:"value,omitempty" yaml:"value,omitempty"`
	Unit        string       `json:"unit,omitempty" yaml:"unit,omitempty"`
	Status      MetricStatus `json:"status" yaml:"status"`
	CollectedAt time.Time    `json:"collected_at,omitempty" yaml:"collected_at,omitempty"`
}

// FloatMetric represents utilization, capacity, and other fractional values.
type FloatMetric struct {
	Value       *float64     `json:"value,omitempty" yaml:"value,omitempty"`
	Unit        string       `json:"unit,omitempty" yaml:"unit,omitempty"`
	Status      MetricStatus `json:"status" yaml:"status"`
	CollectedAt time.Time    `json:"collected_at,omitempty" yaml:"collected_at,omitempty"`
}

type CPUResources struct {
	LogicalProcessors Int64Metric `json:"logical_processors" yaml:"logical_processors"`
	AvailableCapacity FloatMetric `json:"available_capacity" yaml:"available_capacity"`
	Utilization       FloatMetric `json:"utilization" yaml:"utilization"`
}

type MemoryResources struct {
	TotalBytes     Int64Metric `json:"total_bytes" yaml:"total_bytes"`
	AvailableBytes Int64Metric `json:"available_bytes" yaml:"available_bytes"`
}

type GPUResource struct {
	Vendor               string      `json:"vendor,omitempty" yaml:"vendor,omitempty"`
	Model                string      `json:"model,omitempty" yaml:"model,omitempty"`
	DeviceID             string      `json:"device_id,omitempty" yaml:"device_id,omitempty"`
	TotalMemoryBytes     Int64Metric `json:"total_memory_bytes" yaml:"total_memory_bytes"`
	AvailableMemoryBytes Int64Metric `json:"available_memory_bytes" yaml:"available_memory_bytes"`
	Utilization          FloatMetric `json:"utilization" yaml:"utilization"`
	DriverVersion        string      `json:"driver_version,omitempty" yaml:"driver_version,omitempty"`
	Runtime              string      `json:"runtime,omitempty" yaml:"runtime,omitempty"`
	Capabilities         []string    `json:"capabilities,omitempty" yaml:"capabilities,omitempty"`
}

type ResourceSnapshot struct {
	Status      MetricStatus    `json:"status" yaml:"status"`
	CollectedAt time.Time       `json:"collected_at,omitempty" yaml:"collected_at,omitempty"`
	CPU         CPUResources    `json:"cpu" yaml:"cpu"`
	RAM         MemoryResources `json:"ram" yaml:"ram"`
	GPUStatus   MetricStatus    `json:"gpu_status" yaml:"gpu_status"`
	GPUs        []GPUResource   `json:"gpus,omitempty" yaml:"gpus,omitempty"`
}

type ModelPerformance struct {
	Model           string    `json:"model" yaml:"model"`
	TokensPerSecond *float64  `json:"tokens_per_second,omitempty" yaml:"tokens_per_second,omitempty"`
	SampleCount     int       `json:"sample_count" yaml:"sample_count"`
	UpdatedAt       time.Time `json:"updated_at,omitempty" yaml:"updated_at,omitempty"`
	Measured        bool      `json:"measured" yaml:"measured"`
}

type Capabilities struct {
	LLM                bool     `json:"llm" yaml:"llm"`
	Streaming          bool     `json:"streaming" yaml:"streaming"`
	Artifacts          bool     `json:"artifacts" yaml:"artifacts"`
	Models             []string `json:"models" yaml:"models"`
	MaxConcurrentTasks int      `json:"max_concurrent_tasks" yaml:"max_concurrent_tasks"`
	Tools              []string `json:"tools,omitempty" yaml:"tools,omitempty"`
}

// Worker retains all Phase I fields and adds optional Phase 2 status fields.
// Pointers on load and metric values preserve the difference between unknown
// and a reported zero when decoding older worker advertisements.
type Worker struct {
	WorkerID     string       `json:"worker_id" yaml:"worker_id"`
	PeerID       string       `json:"peer_id" yaml:"peer_id"`
	Name         string       `json:"name" yaml:"name"`
	Capabilities Capabilities `json:"capabilities" yaml:"capabilities"`
	ListenAddrs  []string     `json:"listen_addrs" yaml:"listen_addrs"`
	Labels       []string     `json:"labels" yaml:"labels"`
	Version      string       `json:"version" yaml:"version"`
	GroupHash    string       `json:"group_hash" yaml:"group_hash"`
	LastSeen     time.Time    `json:"last_seen" yaml:"last_seen"`
	Local        bool         `json:"local" yaml:"local"`
	Allowed      bool         `json:"allowed" yaml:"allowed"`
	TrustLevel   string       `json:"trust_level" yaml:"trust_level"`

	ProtocolVersion    string             `json:"protocol_version,omitempty" yaml:"protocol_version,omitempty"`
	SessionID          string             `json:"session_id,omitempty" yaml:"session_id,omitempty"`
	Sequence           uint64             `json:"sequence,omitempty" yaml:"sequence,omitempty"`
	StatusTimestamp    time.Time          `json:"status_timestamp,omitempty" yaml:"status_timestamp,omitempty"`
	State              State              `json:"state,omitempty" yaml:"state,omitempty"`
	AcceptingWork      *bool              `json:"accepting_work,omitempty" yaml:"accepting_work,omitempty"`
	RunningTasks       *int               `json:"running_tasks,omitempty" yaml:"running_tasks,omitempty"`
	MaxConcurrentTasks *int               `json:"max_concurrent_tasks,omitempty" yaml:"max_concurrent_tasks,omitempty"`
	QueueDepth         *int               `json:"queue_depth,omitempty" yaml:"queue_depth,omitempty"`
	MaxQueueDepth      *int               `json:"max_queue_depth,omitempty" yaml:"max_queue_depth,omitempty"`
	Resources          *ResourceSnapshot  `json:"resources,omitempty" yaml:"resources,omitempty"`
	LoadedModels       []string           `json:"loaded_models,omitempty" yaml:"loaded_models,omitempty"`
	CostWeight         *float64           `json:"cost_weight,omitempty" yaml:"cost_weight,omitempty"`
	RTTMillis          *float64           `json:"rtt_ms,omitempty" yaml:"rtt_ms,omitempty"`
	RTTUpdatedAt       time.Time          `json:"rtt_updated_at,omitempty" yaml:"rtt_updated_at,omitempty"`
	Performance        []ModelPerformance `json:"performance,omitempty" yaml:"performance,omitempty"`
}

// StatusAdvertisement is a versionable heartbeat payload. Administrative
// fields such as Allowed and TrustLevel are intentionally absent: a worker may
// report capacity, but it cannot grant itself authorization or trust.
type StatusAdvertisement struct {
	WorkerID           string             `json:"worker_id" yaml:"worker_id"`
	PeerID             string             `json:"peer_id" yaml:"peer_id"`
	SessionID          string             `json:"session_id" yaml:"session_id"`
	ProtocolVersion    string             `json:"protocol_version" yaml:"protocol_version"`
	Sequence           uint64             `json:"sequence" yaml:"sequence"`
	Timestamp          time.Time          `json:"timestamp" yaml:"timestamp"`
	State              State              `json:"state" yaml:"state"`
	AcceptingWork      *bool              `json:"accepting_work,omitempty" yaml:"accepting_work,omitempty"`
	RunningTasks       *int               `json:"running_tasks,omitempty" yaml:"running_tasks,omitempty"`
	MaxConcurrentTasks *int               `json:"max_concurrent_tasks,omitempty" yaml:"max_concurrent_tasks,omitempty"`
	QueueDepth         *int               `json:"queue_depth,omitempty" yaml:"queue_depth,omitempty"`
	MaxQueueDepth      *int               `json:"max_queue_depth,omitempty" yaml:"max_queue_depth,omitempty"`
	Capabilities       *Capabilities      `json:"capabilities,omitempty" yaml:"capabilities,omitempty"`
	Resources          *ResourceSnapshot  `json:"resources,omitempty" yaml:"resources,omitempty"`
	LoadedModels       []string           `json:"loaded_models,omitempty" yaml:"loaded_models,omitempty"`
	CostWeight         *float64           `json:"cost_weight,omitempty" yaml:"cost_weight,omitempty"`
	Performance        []ModelPerformance `json:"performance,omitempty" yaml:"performance,omitempty"`
}

func (w Worker) EffectiveMaxConcurrentTasks() (int, bool) {
	if w.MaxConcurrentTasks != nil {
		return *w.MaxConcurrentTasks, true
	}
	if w.Capabilities.MaxConcurrentTasks > 0 {
		return w.Capabilities.MaxConcurrentTasks, true
	}
	return 0, false
}

func (w Worker) AcceptsNewWork() bool {
	if w.AcceptingWork != nil && !*w.AcceptingWork {
		return false
	}
	switch w.State {
	case "", StateAvailable, StateBusy:
		return true
	default:
		return false
	}
}

func (w Worker) HasCapability(required string) bool {
	required = strings.ToLower(strings.TrimSpace(required))
	switch required {
	case "llm":
		return w.Capabilities.LLM
	case "streaming":
		return w.Capabilities.Streaming
	case "artifacts":
		return w.Capabilities.Artifacts
	}
	for _, tool := range w.Capabilities.Tools {
		if strings.EqualFold(strings.TrimSpace(tool), required) {
			return true
		}
	}
	return false
}

func (w Worker) HasModel(model string) bool {
	model = strings.TrimSpace(model)
	if model == "" {
		return true
	}
	for _, candidate := range w.Capabilities.Models {
		if strings.TrimSpace(candidate) == model {
			return true
		}
	}
	return false
}

func (w Worker) HasLoadedModel(model string) bool {
	model = strings.TrimSpace(model)
	if model == "" {
		return false
	}
	for _, candidate := range w.LoadedModels {
		if strings.TrimSpace(candidate) == model {
			return true
		}
	}
	return false
}

func (w Worker) ModelPerformance(model string) (ModelPerformance, bool) {
	for _, measurement := range w.Performance {
		if strings.TrimSpace(measurement.Model) == strings.TrimSpace(model) {
			return measurement, true
		}
	}
	return ModelPerformance{}, false
}

func ValidPositiveFloat(value *float64) bool {
	return value != nil && *value > 0 && !math.IsNaN(*value) && !math.IsInf(*value, 0)
}

type Registry struct {
	Workers []Worker `json:"workers" yaml:"workers"`
}
