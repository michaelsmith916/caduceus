package admission

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/caduceus/caduceus/internal/tasks"
)

const (
	StateStarting    = "starting"
	StateAvailable   = "available"
	StateBusy        = "busy"
	StateUnavailable = "unavailable"
	StateDraining    = "draining"
	StateOffline     = "offline"

	MaxConcurrentTasksLimit = 1024
	MaxQueueDepthLimit      = 1_000_000
)

type GPUSnapshot struct {
	Vendor               string
	AvailableMemoryBytes *uint64
	Capabilities         []string
}

// AdmissionSnapshot is deliberately independent of the worker registry. A
// caller builds it from its latest authoritative local state immediately
// before admission. Nil pointer metrics mean unknown, not zero.
type AdmissionSnapshot struct {
	WorkerID          string
	State             string
	Accepting         *bool
	Allowed           *bool
	Stale             bool
	Capabilities      []string
	AvailableModels   []string
	ModelsKnown       bool
	CanLoadModels     bool
	TrustLevel        string
	GroupIDs          []string
	AvailableCPU      *float64
	AvailableRAMBytes *uint64
	GPUs              []GPUSnapshot
	Runtimes          []string
}

type Stats struct {
	RunningTasks       int  `json:"running_tasks"`
	QueuedTasks        int  `json:"queued_tasks"`
	MaxConcurrentTasks int  `json:"max_concurrent_tasks"`
	MaxQueueDepth      int  `json:"max_queue_depth"`
	Accepting          bool `json:"accepting"`
	Draining           bool `json:"draining"`
	Closed             bool `json:"closed"`
}

type Controller struct {
	mu            sync.Mutex
	permits       chan struct{}
	maxConcurrent int
	maxQueue      int
	running       int
	waiting       int
	accepting     bool
	draining      bool
	closed        bool
	changed       chan struct{}
}

func New(maxConcurrentTasks, maxQueueDepth int) (*Controller, error) {
	if maxConcurrentTasks <= 0 || maxConcurrentTasks > MaxConcurrentTasksLimit {
		return nil, fmt.Errorf("max concurrent tasks must be between 1 and %d", MaxConcurrentTasksLimit)
	}
	if maxQueueDepth < 0 || maxQueueDepth > MaxQueueDepthLimit {
		return nil, fmt.Errorf("max queue depth must be between 0 and %d", MaxQueueDepthLimit)
	}
	return &Controller{
		permits:       make(chan struct{}, maxConcurrentTasks),
		maxConcurrent: maxConcurrentTasks,
		maxQueue:      maxQueueDepth,
		accepting:     true,
		changed:       make(chan struct{}),
	}, nil
}

func (c *Controller) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.statsLocked()
}

func (c *Controller) SetAccepting(accepting bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.accepting == accepting {
		return
	}
	c.accepting = accepting
	c.signalLocked()
}

func (c *Controller) SetDraining(draining bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.draining == draining {
		return
	}
	c.draining = draining
	c.accepting = !draining
	c.signalLocked()
}

// Close stops new admission and wakes queued callers. Existing leases remain
// valid and release normally; Close never steals capacity from running work.
func (c *Controller) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	c.accepting = false
	c.signalLocked()
}

// Validate performs the authoritative non-capacity admission checks against a
// freshly collected local snapshot. Callers that waited for a permit use this
// immediately before execution so telemetry changes during the wait cannot
// bypass resource, availability, trust, or placement constraints.
func (c *Controller) Validate(req tasks.Request, snapshot AdmissionSnapshot) *tasks.TaskRejection {
	return c.precheck(req, snapshot)
}

// TryAcquire performs immediate, authoritative revalidation and never queues.
func (c *Controller) TryAcquire(req tasks.Request, snapshot AdmissionSnapshot) (*Lease, *tasks.TaskRejection) {
	if rejection := c.precheck(req, snapshot); rejection != nil {
		return nil, rejection
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if reason, detail := c.stateRejectionLocked(); reason != "" {
		return nil, c.rejectionLocked(req, snapshot, reason, detail)
	}
	select {
	case c.permits <- struct{}{}:
		c.running++
		c.signalLocked()
		return &Lease{controller: c}, nil
	default:
		return nil, c.rejectionLocked(req, snapshot, tasks.RejectAtCapacity, "worker concurrency limit reached")
	}
}

// Acquire queues only when MaxQueueDepth is positive. Waiting is bounded,
// context-aware, and interrupted immediately by drain, unavailable, or close.
func (c *Controller) Acquire(ctx context.Context, req tasks.Request, snapshot AdmissionSnapshot) (*Lease, *tasks.TaskRejection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if rejection := c.precheck(req, snapshot); rejection != nil {
		return nil, rejection, nil
	}
	c.mu.Lock()
	if reason, detail := c.stateRejectionLocked(); reason != "" {
		rejection := c.rejectionLocked(req, snapshot, reason, detail)
		c.mu.Unlock()
		return nil, rejection, nil
	}
	select {
	case c.permits <- struct{}{}:
		c.running++
		c.signalLocked()
		c.mu.Unlock()
		return &Lease{controller: c}, nil, nil
	default:
	}
	if c.maxQueue == 0 {
		rejection := c.rejectionLocked(req, snapshot, tasks.RejectAtCapacity, "worker concurrency limit reached and queuing is disabled")
		c.mu.Unlock()
		return nil, rejection, nil
	}
	if c.waiting >= c.maxQueue {
		rejection := c.rejectionLocked(req, snapshot, tasks.RejectQueueFull, "worker admission queue is full")
		c.mu.Unlock()
		return nil, rejection, nil
	}
	c.waiting++
	c.signalLocked()
	changed := c.changed
	c.mu.Unlock()

	queued := true
	removeWaiter := func() {
		if !queued {
			return
		}
		c.mu.Lock()
		if queued {
			queued = false
			c.waiting--
			c.signalLocked()
		}
		c.mu.Unlock()
	}

	for {
		select {
		case c.permits <- struct{}{}:
			c.mu.Lock()
			if queued {
				queued = false
				c.waiting--
			}
			if reason, detail := c.stateRejectionLocked(); reason != "" {
				<-c.permits
				c.signalLocked()
				rejection := c.rejectionLocked(req, snapshot, reason, detail)
				c.mu.Unlock()
				return nil, rejection, nil
			}
			c.running++
			c.signalLocked()
			c.mu.Unlock()
			lease := &Lease{controller: c}
			if rejection := c.precheck(req, snapshot); rejection != nil {
				lease.Release()
				return nil, rejection, nil
			}
			return lease, nil, nil
		case <-ctx.Done():
			removeWaiter()
			return nil, nil, ctx.Err()
		case <-changed:
			c.mu.Lock()
			if reason, detail := c.stateRejectionLocked(); reason != "" {
				if queued {
					queued = false
					c.waiting--
				}
				c.signalLocked()
				rejection := c.rejectionLocked(req, snapshot, reason, detail)
				c.mu.Unlock()
				return nil, rejection, nil
			}
			changed = c.changed
			c.mu.Unlock()
		}
	}
}

// Run guarantees permit release for success, errors, cancellation, and panic.
// A panic is deliberately not swallowed; the deferred lease still releases.
func (c *Controller) Run(ctx context.Context, req tasks.Request, snapshot AdmissionSnapshot, fn func(context.Context) error) (*tasks.TaskRejection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	lease, rejection, err := c.Acquire(ctx, req, snapshot)
	if err != nil || rejection != nil {
		return rejection, err
	}
	defer lease.Release()
	if fn == nil {
		return nil, nil
	}
	return nil, fn(ctx)
}

type Lease struct {
	controller *Controller
	once       sync.Once
}

// Release is idempotent and may safely be deferred on every exit path.
func (l *Lease) Release() {
	if l == nil || l.controller == nil {
		return
	}
	l.once.Do(func() {
		c := l.controller
		c.mu.Lock()
		defer c.mu.Unlock()
		<-c.permits
		if c.running > 0 {
			c.running--
		}
		c.signalLocked()
	})
}

func (c *Controller) precheck(req tasks.Request, snapshot AdmissionSnapshot) *tasks.TaskRejection {
	if err := tasks.ValidateRequest(req); err != nil {
		return c.rejection(req, snapshot, tasks.RejectUnsupportedTask, err.Error())
	}
	if snapshot.Stale {
		return c.rejection(req, snapshot, tasks.RejectStaleAssignment, "worker status is stale")
	}
	if snapshot.Accepting != nil && !*snapshot.Accepting {
		return c.rejection(req, snapshot, tasks.RejectWorkerUnavailable, "worker is not accepting new work")
	}
	if snapshot.Allowed != nil && !*snapshot.Allowed {
		return c.rejection(req, snapshot, tasks.RejectNotAllowed, "worker is not authorized for this task")
	}
	switch snapshot.State {
	case "", StateAvailable, StateBusy:
	case StateDraining:
		return c.rejection(req, snapshot, tasks.RejectWorkerDraining, "worker is draining")
	case StateStarting, StateUnavailable, StateOffline:
		return c.rejection(req, snapshot, tasks.RejectWorkerUnavailable, "worker is not available")
	default:
		return c.rejection(req, snapshot, tasks.RejectWorkerUnavailable, "worker has an invalid state")
	}
	constraints := req.Constraints
	if len(constraints.AllowedWorkerIDs) > 0 && !contains(constraints.AllowedWorkerIDs, snapshot.WorkerID) {
		return c.rejection(req, snapshot, tasks.RejectNotAllowed, "worker is not in the task allowlist")
	}
	if len(constraints.AllowedGroups) > 0 && !intersects(constraints.AllowedGroups, snapshot.GroupIDs) {
		return c.rejection(req, snapshot, tasks.RejectNotAllowed, "worker is not in an allowed group")
	}
	requiredTrust := constraints.RequiredTrustLevel
	if requiredTrust == "" {
		requiredTrust = req.TrustLevel
	}
	if requiredTrust != "" && snapshot.TrustLevel != requiredTrust {
		return c.rejection(req, snapshot, tasks.RejectInsufficientTrust, "worker trust level does not satisfy the task")
	}
	for _, capability := range constraints.RequiredCapabilities {
		if !contains(snapshot.Capabilities, capability) {
			return c.rejection(req, snapshot, tasks.RejectCapabilityMissing, "required capability is unavailable: "+capability)
		}
	}
	requiredModel := constraints.RequiredModel
	if requiredModel == "" {
		requiredModel = req.Task.Model
	}
	if requiredModel != "" && !contains(snapshot.AvailableModels, requiredModel) && !snapshot.CanLoadModels {
		detail := "required model is unavailable: " + requiredModel
		if !snapshot.ModelsKnown {
			detail = "required model availability is unknown: " + requiredModel
		}
		return c.rejection(req, snapshot, tasks.RejectModelUnavailable, detail)
	}
	if constraints.RequiredRuntime != "" && !contains(snapshot.Runtimes, constraints.RequiredRuntime) {
		return c.rejection(req, snapshot, tasks.RejectCapabilityMissing, "required runtime is unavailable: "+constraints.RequiredRuntime)
	}
	if constraints.MinCPU > 0 {
		if snapshot.AvailableCPU == nil || !finite(*snapshot.AvailableCPU) || *snapshot.AvailableCPU < constraints.MinCPU {
			rejection := c.rejection(req, snapshot, tasks.RejectInsufficientCPU, "available CPU capacity does not satisfy the task")
			rejection.ResourceDeficiencies = []tasks.ResourceDeficiency{{Resource: "cpu", Required: constraints.MinCPU, Available: snapshot.AvailableCPU, Unit: "logical_cpus"}}
			return rejection
		}
	}
	if constraints.MinRAMBytes > 0 {
		if snapshot.AvailableRAMBytes == nil || *snapshot.AvailableRAMBytes < constraints.MinRAMBytes {
			rejection := c.rejection(req, snapshot, tasks.RejectInsufficientRAM, "available RAM does not satisfy the task")
			rejection.ResourceDeficiencies = []tasks.ResourceDeficiency{{Resource: "ram", Required: float64(constraints.MinRAMBytes), Available: uint64Float(snapshot.AvailableRAMBytes), Unit: "bytes"}}
			return rejection
		}
	}
	if gpuRequired(constraints) {
		matched, memoryAvailable := matchGPU(constraints, snapshot.GPUs)
		if !matched {
			if constraints.MinGPUMemoryBytes > 0 {
				rejection := c.rejection(req, snapshot, tasks.RejectInsufficientGPUMemory, "no GPU satisfies the required memory and capabilities")
				rejection.ResourceDeficiencies = []tasks.ResourceDeficiency{{Resource: "gpu_memory", Required: float64(constraints.MinGPUMemoryBytes), Available: memoryAvailable, Unit: "bytes"}}
				return rejection
			}
			return c.rejection(req, snapshot, tasks.RejectCapabilityMissing, "no GPU satisfies the task requirements")
		}
	}
	return nil
}

func (c *Controller) rejection(req tasks.Request, snapshot AdmissionSnapshot, reason tasks.RejectionReason, detail string) *tasks.TaskRejection {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rejectionLocked(req, snapshot, reason, detail)
}

func (c *Controller) rejectionLocked(req tasks.Request, snapshot AdmissionSnapshot, reason tasks.RejectionReason, detail string) *tasks.TaskRejection {
	workerID := snapshot.WorkerID
	if strings.TrimSpace(workerID) == "" {
		workerID = "unknown"
	}
	taskID := req.TaskID
	if strings.TrimSpace(taskID) == "" {
		taskID = "unassigned"
	}
	rejection := tasks.NewRejection(taskID, "", workerID, reason, detail)
	stats := c.statsLocked()
	rejection.CurrentQueueDepth = stats.QueuedTasks
	rejection.CurrentRunningTasks = stats.RunningTasks
	rejection.MaxConcurrentTasks = stats.MaxConcurrentTasks
	switch reason {
	case tasks.RejectAtCapacity, tasks.RejectStaleAssignment:
		rejection.Retryable = true
		rejection.RetryAfterSeconds = 1
	case tasks.RejectQueueFull, tasks.RejectWorkerUnavailable:
		rejection.Retryable = true
		rejection.RetryAfterSeconds = 2
	}
	return &rejection
}

func (c *Controller) stateRejectionLocked() (tasks.RejectionReason, string) {
	if c.closed {
		return tasks.RejectWorkerUnavailable, "worker admission is shut down"
	}
	if c.draining {
		return tasks.RejectWorkerDraining, "worker is draining"
	}
	if !c.accepting {
		return tasks.RejectWorkerUnavailable, "worker is not accepting new work"
	}
	return "", ""
}

func (c *Controller) statsLocked() Stats {
	return Stats{
		RunningTasks:       c.running,
		QueuedTasks:        c.waiting,
		MaxConcurrentTasks: c.maxConcurrent,
		MaxQueueDepth:      c.maxQueue,
		Accepting:          c.accepting,
		Draining:           c.draining,
		Closed:             c.closed,
	}
}

func (c *Controller) signalLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func gpuRequired(constraints tasks.Constraints) bool {
	return constraints.GPURequired || constraints.GPUVendor != "" || constraints.MinGPUMemoryBytes > 0 || len(constraints.RequiredGPUCapabilities) > 0
}

func matchGPU(constraints tasks.Constraints, gpus []GPUSnapshot) (bool, *float64) {
	var maxAvailable *float64
	for _, gpu := range gpus {
		if gpu.AvailableMemoryBytes != nil {
			value := float64(*gpu.AvailableMemoryBytes)
			if maxAvailable == nil || value > *maxAvailable {
				copyValue := value
				maxAvailable = &copyValue
			}
		}
		if constraints.GPUVendor != "" && !strings.EqualFold(constraints.GPUVendor, gpu.Vendor) {
			continue
		}
		if !allContained(gpu.Capabilities, constraints.RequiredGPUCapabilities) {
			continue
		}
		if constraints.MinGPUMemoryBytes > 0 && (gpu.AvailableMemoryBytes == nil || *gpu.AvailableMemoryBytes < constraints.MinGPUMemoryBytes) {
			continue
		}
		return true, maxAvailable
	}
	return false, maxAvailable
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func intersects(left, right []string) bool {
	for _, value := range left {
		if contains(right, value) {
			return true
		}
	}
	return false
}

func allContained(have, required []string) bool {
	for _, value := range required {
		if !contains(have, value) {
			return false
		}
	}
	return true
}

func finite(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func uint64Float(value *uint64) *float64 {
	if value == nil {
		return nil
	}
	converted := float64(*value)
	return &converted
}
