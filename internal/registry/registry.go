package registry

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/caduceus/caduceus/internal/workers"
)

const (
	DefaultSuspectAfter = 30 * time.Second
	DefaultEvictAfter   = 2 * time.Minute
	maxRetiredSessions  = 4096
	maxFutureClockSkew  = 5 * time.Minute
)

var (
	ErrNotRegistered   = errors.New("worker is not registered")
	ErrPeerMismatch    = errors.New("worker peer does not match registration")
	ErrSessionMismatch = errors.New("worker session does not match registration")
	ErrOldSession      = errors.New("worker session is retired")
	ErrSessionHistory  = errors.New("worker session history is full")
	ErrOutOfOrder      = errors.New("worker status sequence is not newer")
)

type Options struct {
	SuspectAfter time.Duration
	EvictAfter   time.Duration
	Now          func() time.Time
}

type Registry struct {
	mu           sync.RWMutex
	entries      map[string]*entry
	tombstones   map[string]*sessionTombstone
	version      uint64
	suspectAfter time.Duration
	evictAfter   time.Duration
	now          func() time.Time
}

type entry struct {
	worker          workers.Worker
	retiredSessions map[string]struct{}
}

// sessionTombstone retains the authenticated peer binding and every retired
// process incarnation after an active registry entry is evicted. Session IDs
// are never discarded while the registry process is alive, so a delayed hello
// cannot become current merely by waiting out the liveness lease.
type sessionTombstone struct {
	peerID   string
	sessions map[string]struct{}
}

type RegistrationResult struct {
	WorkerID          string `json:"worker_id"`
	SessionID         string `json:"session_id,omitempty"`
	PreviousSessionID string `json:"previous_session_id,omitempty"`
	SessionReplaced   bool   `json:"session_replaced"`
}

type WorkerSnapshot struct {
	Worker      workers.Worker `json:"worker"`
	Alive       bool           `json:"alive"`
	Healthy     bool           `json:"healthy"`
	Suspect     bool           `json:"suspect"`
	Stale       bool           `json:"stale"`
	Schedulable bool           `json:"schedulable"`
	LastSeenAge time.Duration  `json:"last_seen_age"`
}

type Snapshot struct {
	Timestamp time.Time        `json:"timestamp"`
	Version   uint64           `json:"version"`
	Workers   []WorkerSnapshot `json:"workers"`
}

func New(opts Options) (*Registry, error) {
	if opts.SuspectAfter == 0 {
		opts.SuspectAfter = DefaultSuspectAfter
	}
	if opts.EvictAfter == 0 {
		opts.EvictAfter = DefaultEvictAfter
	}
	if opts.SuspectAfter <= 0 {
		return nil, errors.New("suspect threshold must be positive")
	}
	if opts.EvictAfter <= opts.SuspectAfter {
		return nil, errors.New("eviction threshold must be greater than suspect threshold")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Registry{
		entries:      make(map[string]*entry),
		tombstones:   make(map[string]*sessionTombstone),
		suspectAfter: opts.SuspectAfter,
		evictAfter:   opts.EvictAfter,
		now:          opts.Now,
	}, nil
}

// Register records a local worker or an authenticated worker hello.
func (r *Registry) Register(worker workers.Worker) error {
	_, err := r.RegisterWithResult(worker)
	return err
}

// RegisterWithResult is Register with an explicit signal when a live session
// is superseded. Callers use that signal to fence in-flight work immediately.
// Retired sessions survive eviction for the lifetime of the registry.
func (r *Registry) RegisterWithResult(worker workers.Worker) (RegistrationResult, error) {
	worker.WorkerID = strings.TrimSpace(worker.WorkerID)
	worker.PeerID = strings.TrimSpace(worker.PeerID)
	worker.SessionID = strings.TrimSpace(worker.SessionID)
	result := RegistrationResult{WorkerID: worker.WorkerID, SessionID: worker.SessionID}
	if err := validateWorker(worker); err != nil {
		return result, err
	}
	if worker.State == "" {
		worker.State = workers.StateAvailable
	}

	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()

	current, ok := r.entries[worker.WorkerID]
	if !ok {
		retired := make(map[string]struct{})
		if tombstone := r.tombstones[worker.WorkerID]; tombstone != nil {
			if tombstone.peerID != worker.PeerID {
				return result, fmt.Errorf("%w: worker %q is bound to peer %q", ErrPeerMismatch, worker.WorkerID, tombstone.peerID)
			}
			if worker.SessionID == "" && len(tombstone.sessions) > 0 {
				return result, fmt.Errorf("%w: worker %q requires a new session", ErrSessionMismatch, worker.WorkerID)
			}
			if sessionRetired(tombstone.sessions, worker.SessionID) {
				return result, fmt.Errorf("%w: worker %q session %q", ErrOldSession, worker.WorkerID, worker.SessionID)
			}
			if worker.SessionID != "" && len(tombstone.sessions) >= maxRetiredSessions {
				return result, fmt.Errorf("%w: worker %q", ErrSessionHistory, worker.WorkerID)
			}
			retired = cloneSessions(tombstone.sessions)
			delete(r.tombstones, worker.WorkerID)
		}
		worker.LastSeen = now
		r.entries[worker.WorkerID] = &entry{worker: cloneWorker(worker), retiredSessions: retired}
		r.version++
		return result, nil
	}
	if current.worker.PeerID != worker.PeerID {
		return result, fmt.Errorf("%w: worker %q is bound to peer %q", ErrPeerMismatch, worker.WorkerID, current.worker.PeerID)
	}
	if current.worker.SessionID != worker.SessionID {
		if worker.SessionID == "" {
			return result, fmt.Errorf("%w: worker %q requires session %q", ErrSessionMismatch, worker.WorkerID, current.worker.SessionID)
		}
		if sessionRetired(current.retiredSessions, worker.SessionID) {
			return result, fmt.Errorf("%w: worker %q session %q", ErrOldSession, worker.WorkerID, worker.SessionID)
		}
		knownSessions := len(current.retiredSessions) + 1 // incoming active session
		if current.worker.SessionID != "" {
			knownSessions++
		}
		if knownSessions > maxRetiredSessions {
			return result, fmt.Errorf("%w: worker %q", ErrSessionHistory, worker.WorkerID)
		}
		result.PreviousSessionID = current.worker.SessionID
		result.SessionReplaced = true
		current.retire(current.worker.SessionID)
		worker.LastSeen = now
		current.worker = cloneWorker(worker)
		r.version++
		return result, nil
	}

	worker = mergeRegistration(current.worker, worker)
	worker.LastSeen = now
	current.worker = cloneWorker(worker)
	r.version++
	return result, nil
}

// Heartbeat applies a status advertisement only to the active registered
// session. Receipt time, not the worker-provided timestamp, controls liveness.
func (r *Registry) Heartbeat(status workers.StatusAdvertisement) error {
	status.WorkerID = strings.TrimSpace(status.WorkerID)
	status.PeerID = strings.TrimSpace(status.PeerID)
	status.SessionID = strings.TrimSpace(status.SessionID)
	now := r.now()
	if err := validateStatus(status, now); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	current, ok := r.entries[status.WorkerID]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotRegistered, status.WorkerID)
	}
	if current.worker.PeerID != status.PeerID {
		return fmt.Errorf("%w: worker %q is bound to peer %q", ErrPeerMismatch, status.WorkerID, current.worker.PeerID)
	}
	if current.worker.SessionID != status.SessionID {
		if sessionRetired(current.retiredSessions, status.SessionID) {
			return fmt.Errorf("%w: worker %q session %q", ErrOldSession, status.WorkerID, status.SessionID)
		}
		return fmt.Errorf("%w: worker %q", ErrSessionMismatch, status.WorkerID)
	}
	if status.Sequence <= current.worker.Sequence {
		return fmt.Errorf("%w: got %d after %d", ErrOutOfOrder, status.Sequence, current.worker.Sequence)
	}

	applyStatus(&current.worker, status)
	current.worker.LastSeen = now
	r.version++
	return nil
}

// SetRTT records a coordinator-measured round-trip time. It is deliberately not
// part of StatusAdvertisement because workers must not score their own network
// proximity.
func (r *Registry) SetRTT(workerID string, milliseconds float64, measuredAt time.Time) error {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return errors.New("worker id is required")
	}
	if milliseconds < 0 || math.IsNaN(milliseconds) || math.IsInf(milliseconds, 0) {
		return errors.New("RTT must be finite and non-negative")
	}
	if measuredAt.IsZero() {
		measuredAt = r.now()
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.entries[workerID]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotRegistered, workerID)
	}
	current.worker.RTTMillis = float64Ptr(milliseconds)
	current.worker.RTTUpdatedAt = measuredAt
	r.version++
	return nil
}

func (r *Registry) Get(workerID string) (workers.Worker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	current, ok := r.entries[strings.TrimSpace(workerID)]
	if !ok {
		return workers.Worker{}, false
	}
	return cloneWorker(current.worker), true
}

func (r *Registry) Remove(workerID string) (workers.Worker, bool) {
	workerID = strings.TrimSpace(workerID)
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.entries[workerID]
	if !ok {
		return workers.Worker{}, false
	}
	r.rememberEntry(workerID, current)
	delete(r.entries, workerID)
	r.version++
	removed := cloneWorker(current.worker)
	removed.State = workers.StateOffline
	removed.AcceptingWork = boolPtr(false)
	return removed, true
}

func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// Sweep evicts workers whose receipt-time lease has expired. Returned records
// are detached copies marked offline for logging or diagnostic history.
func (r *Registry) Sweep() []workers.Worker {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sweepLocked(now)
}

// Snapshot returns a sorted deep copy. It also performs expiry so a scheduler
// cannot receive a worker that is already beyond the eviction threshold.
func (r *Registry) Snapshot() Snapshot {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweepLocked(now)

	out := Snapshot{
		Timestamp: now,
		Version:   r.version,
		Workers:   make([]WorkerSnapshot, 0, len(r.entries)),
	}
	for _, current := range r.entries {
		age := now.Sub(current.worker.LastSeen)
		if age < 0 {
			age = 0
		}
		healthy := age < r.suspectAfter
		worker := cloneWorker(current.worker)
		out.Workers = append(out.Workers, WorkerSnapshot{
			Worker:      worker,
			Alive:       true,
			Healthy:     healthy,
			Suspect:     !healthy,
			Stale:       !healthy,
			Schedulable: healthy && worker.Allowed && worker.AcceptsNewWork(),
			LastSeenAge: age,
		})
	}
	sort.Slice(out.Workers, func(i, j int) bool {
		return out.Workers[i].Worker.WorkerID < out.Workers[j].Worker.WorkerID
	})
	return out
}

func (r *Registry) sweepLocked(now time.Time) []workers.Worker {
	var evicted []workers.Worker
	for workerID, current := range r.entries {
		if now.Sub(current.worker.LastSeen) < r.evictAfter {
			continue
		}
		worker := cloneWorker(current.worker)
		worker.State = workers.StateOffline
		worker.AcceptingWork = boolPtr(false)
		evicted = append(evicted, worker)
		r.rememberEntry(workerID, current)
		delete(r.entries, workerID)
	}
	if len(evicted) > 0 {
		r.version++
		sort.Slice(evicted, func(i, j int) bool {
			return evicted[i].WorkerID < evicted[j].WorkerID
		})
	}
	return evicted
}

func (e *entry) retire(sessionID string) {
	if sessionID == "" {
		return
	}
	if e.retiredSessions == nil {
		e.retiredSessions = make(map[string]struct{})
	}
	e.retiredSessions[sessionID] = struct{}{}
}

func (r *Registry) rememberEntry(workerID string, current *entry) {
	if current == nil {
		return
	}
	tombstone := r.tombstones[workerID]
	if tombstone == nil {
		tombstone = &sessionTombstone{peerID: current.worker.PeerID, sessions: make(map[string]struct{})}
		r.tombstones[workerID] = tombstone
	}
	for sessionID := range current.retiredSessions {
		tombstone.sessions[sessionID] = struct{}{}
	}
	if current.worker.SessionID != "" {
		tombstone.sessions[current.worker.SessionID] = struct{}{}
	}
}

func cloneSessions(sessions map[string]struct{}) map[string]struct{} {
	cloned := make(map[string]struct{}, len(sessions))
	for sessionID := range sessions {
		cloned[sessionID] = struct{}{}
	}
	return cloned
}

func sessionRetired(sessions map[string]struct{}, sessionID string) bool {
	if sessionID == "" {
		return false
	}
	_, retired := sessions[sessionID]
	return retired
}

func validateWorker(worker workers.Worker) error {
	if worker.WorkerID == "" {
		return errors.New("worker id is required")
	}
	if worker.PeerID == "" {
		return errors.New("peer id is required")
	}
	if worker.State != "" && !worker.State.Valid() {
		return fmt.Errorf("invalid worker state %q", worker.State)
	}
	if err := validateLoad(worker.RunningTasks, worker.MaxConcurrentTasks, worker.QueueDepth, worker.MaxQueueDepth); err != nil {
		return err
	}
	if worker.CostWeight != nil && !workers.ValidPositiveFloat(worker.CostWeight) {
		return errors.New("cost weight must be finite and positive")
	}
	return validateResources(worker.Resources)
}

func validateStatus(status workers.StatusAdvertisement, now time.Time) error {
	if status.WorkerID == "" {
		return errors.New("worker id is required")
	}
	if status.PeerID == "" {
		return errors.New("peer id is required")
	}
	if status.SessionID == "" {
		return errors.New("session id is required")
	}
	if strings.TrimSpace(status.ProtocolVersion) == "" {
		return errors.New("protocol version is required")
	}
	if status.Timestamp.IsZero() {
		return errors.New("status timestamp is required")
	}
	if status.Timestamp.After(now.Add(maxFutureClockSkew)) {
		return errors.New("status timestamp is too far in the future")
	}
	if !status.State.Valid() {
		return fmt.Errorf("invalid worker state %q", status.State)
	}
	if err := validateLoad(status.RunningTasks, status.MaxConcurrentTasks, status.QueueDepth, status.MaxQueueDepth); err != nil {
		return err
	}
	if status.CostWeight != nil && !workers.ValidPositiveFloat(status.CostWeight) {
		return errors.New("cost weight must be finite and positive")
	}
	for _, measurement := range status.Performance {
		if strings.TrimSpace(measurement.Model) == "" {
			return errors.New("performance model is required")
		}
		if measurement.SampleCount < 0 {
			return errors.New("performance sample count must be non-negative")
		}
		if measurement.TokensPerSecond != nil && !workers.ValidPositiveFloat(measurement.TokensPerSecond) {
			return errors.New("tokens per second must be finite and positive")
		}
	}
	return validateResources(status.Resources)
}

func validateLoad(running, maximum, queued, maxQueued *int) error {
	for name, value := range map[string]*int{
		"running tasks":       running,
		"maximum concurrency": maximum,
		"queue depth":         queued,
		"maximum queue depth": maxQueued,
	} {
		if value != nil && *value < 0 {
			return fmt.Errorf("%s must be non-negative", name)
		}
	}
	return nil
}

func validateResources(resources *workers.ResourceSnapshot) error {
	if resources == nil {
		return nil
	}
	if resources.Status != "" && !resources.Status.Valid() {
		return fmt.Errorf("invalid resource status %q", resources.Status)
	}
	if resources.GPUStatus != "" && !resources.GPUStatus.Valid() {
		return fmt.Errorf("invalid GPU status %q", resources.GPUStatus)
	}
	if resources.Status == workers.MetricCurrent && resources.CollectedAt.IsZero() {
		return errors.New("current resource snapshot requires collection time")
	}
	intMetrics := []workers.Int64Metric{
		resources.CPU.LogicalProcessors,
		resources.RAM.TotalBytes,
		resources.RAM.AvailableBytes,
	}
	floatMetrics := []workers.FloatMetric{
		resources.CPU.AvailableCapacity,
		resources.CPU.Utilization,
	}
	for _, gpu := range resources.GPUs {
		intMetrics = append(intMetrics, gpu.TotalMemoryBytes, gpu.AvailableMemoryBytes)
		floatMetrics = append(floatMetrics, gpu.Utilization)
	}
	for _, metric := range intMetrics {
		if metric.Status != "" && !metric.Status.Valid() {
			return fmt.Errorf("invalid integral metric status %q", metric.Status)
		}
		if metric.Value != nil && *metric.Value < 0 {
			return errors.New("integral resource metrics must be non-negative")
		}
		if metric.Status == workers.MetricCurrent && metric.Value == nil {
			return errors.New("current integral metric requires a value")
		}
	}
	for _, metric := range floatMetrics {
		if metric.Status != "" && !metric.Status.Valid() {
			return fmt.Errorf("invalid floating metric status %q", metric.Status)
		}
		if metric.Value != nil && (*metric.Value < 0 || math.IsNaN(*metric.Value) || math.IsInf(*metric.Value, 0)) {
			return errors.New("floating resource metrics must be finite and non-negative")
		}
		if metric.Status == workers.MetricCurrent && metric.Value == nil {
			return errors.New("current floating metric requires a value")
		}
	}
	return nil
}

func applyStatus(worker *workers.Worker, status workers.StatusAdvertisement) {
	worker.ProtocolVersion = status.ProtocolVersion
	worker.Sequence = status.Sequence
	worker.StatusTimestamp = status.Timestamp
	worker.State = status.State
	if status.AcceptingWork != nil {
		worker.AcceptingWork = boolPtr(*status.AcceptingWork)
	}
	if status.RunningTasks != nil {
		worker.RunningTasks = intPtr(*status.RunningTasks)
	}
	if status.MaxConcurrentTasks != nil {
		worker.MaxConcurrentTasks = intPtr(*status.MaxConcurrentTasks)
	}
	if status.QueueDepth != nil {
		worker.QueueDepth = intPtr(*status.QueueDepth)
	}
	if status.MaxQueueDepth != nil {
		worker.MaxQueueDepth = intPtr(*status.MaxQueueDepth)
	}
	if status.Capabilities != nil {
		worker.Capabilities = cloneCapabilities(*status.Capabilities)
	}
	if status.Resources != nil {
		worker.Resources = cloneResources(status.Resources)
	}
	if status.LoadedModels != nil {
		worker.LoadedModels = append([]string(nil), status.LoadedModels...)
	}
	if status.CostWeight != nil {
		worker.CostWeight = float64Ptr(*status.CostWeight)
	}
	if status.Performance != nil {
		worker.Performance = clonePerformance(status.Performance)
	}
}

func mergeRegistration(current, incoming workers.Worker) workers.Worker {
	if incoming.ProtocolVersion == "" {
		incoming.ProtocolVersion = current.ProtocolVersion
	}
	if incoming.Sequence < current.Sequence {
		incoming.Sequence = current.Sequence
	}
	if incoming.StatusTimestamp.IsZero() {
		incoming.StatusTimestamp = current.StatusTimestamp
	}
	if incoming.State == "" {
		incoming.State = current.State
	}
	if incoming.AcceptingWork == nil {
		incoming.AcceptingWork = cloneBool(current.AcceptingWork)
	}
	if incoming.RunningTasks == nil {
		incoming.RunningTasks = cloneInt(current.RunningTasks)
	}
	if incoming.MaxConcurrentTasks == nil {
		incoming.MaxConcurrentTasks = cloneInt(current.MaxConcurrentTasks)
	}
	if incoming.QueueDepth == nil {
		incoming.QueueDepth = cloneInt(current.QueueDepth)
	}
	if incoming.MaxQueueDepth == nil {
		incoming.MaxQueueDepth = cloneInt(current.MaxQueueDepth)
	}
	if incoming.Resources == nil {
		incoming.Resources = cloneResources(current.Resources)
	}
	if incoming.LoadedModels == nil {
		incoming.LoadedModels = append([]string(nil), current.LoadedModels...)
	}
	if incoming.CostWeight == nil {
		incoming.CostWeight = cloneFloat(current.CostWeight)
	}
	if incoming.RTTMillis == nil {
		incoming.RTTMillis = cloneFloat(current.RTTMillis)
		incoming.RTTUpdatedAt = current.RTTUpdatedAt
	}
	if incoming.Performance == nil {
		incoming.Performance = clonePerformance(current.Performance)
	}
	return incoming
}

func cloneWorker(worker workers.Worker) workers.Worker {
	worker.ListenAddrs = append([]string(nil), worker.ListenAddrs...)
	worker.Labels = append([]string(nil), worker.Labels...)
	worker.Capabilities = cloneCapabilities(worker.Capabilities)
	worker.AcceptingWork = cloneBool(worker.AcceptingWork)
	worker.RunningTasks = cloneInt(worker.RunningTasks)
	worker.MaxConcurrentTasks = cloneInt(worker.MaxConcurrentTasks)
	worker.QueueDepth = cloneInt(worker.QueueDepth)
	worker.MaxQueueDepth = cloneInt(worker.MaxQueueDepth)
	worker.Resources = cloneResources(worker.Resources)
	worker.LoadedModels = append([]string(nil), worker.LoadedModels...)
	worker.CostWeight = cloneFloat(worker.CostWeight)
	worker.RTTMillis = cloneFloat(worker.RTTMillis)
	worker.Performance = clonePerformance(worker.Performance)
	return worker
}

func cloneCapabilities(capabilities workers.Capabilities) workers.Capabilities {
	capabilities.Models = append([]string(nil), capabilities.Models...)
	capabilities.Tools = append([]string(nil), capabilities.Tools...)
	return capabilities
}

func cloneResources(resources *workers.ResourceSnapshot) *workers.ResourceSnapshot {
	if resources == nil {
		return nil
	}
	copy := *resources
	copy.CPU.LogicalProcessors.Value = cloneInt64(resources.CPU.LogicalProcessors.Value)
	copy.CPU.AvailableCapacity.Value = cloneFloat(resources.CPU.AvailableCapacity.Value)
	copy.CPU.Utilization.Value = cloneFloat(resources.CPU.Utilization.Value)
	copy.RAM.TotalBytes.Value = cloneInt64(resources.RAM.TotalBytes.Value)
	copy.RAM.AvailableBytes.Value = cloneInt64(resources.RAM.AvailableBytes.Value)
	copy.GPUs = append([]workers.GPUResource(nil), resources.GPUs...)
	for i := range copy.GPUs {
		copy.GPUs[i].TotalMemoryBytes.Value = cloneInt64(resources.GPUs[i].TotalMemoryBytes.Value)
		copy.GPUs[i].AvailableMemoryBytes.Value = cloneInt64(resources.GPUs[i].AvailableMemoryBytes.Value)
		copy.GPUs[i].Utilization.Value = cloneFloat(resources.GPUs[i].Utilization.Value)
		copy.GPUs[i].Capabilities = append([]string(nil), resources.GPUs[i].Capabilities...)
	}
	return &copy
}

func clonePerformance(performance []workers.ModelPerformance) []workers.ModelPerformance {
	out := append([]workers.ModelPerformance(nil), performance...)
	for i := range out {
		out[i].TokensPerSecond = cloneFloat(performance[i].TokensPerSecond)
	}
	return out
}

func boolPtr(value bool) *bool          { return &value }
func intPtr(value int) *int             { return &value }
func int64Ptr(value int64) *int64       { return &value }
func float64Ptr(value float64) *float64 { return &value }

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	return boolPtr(*value)
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	return intPtr(*value)
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	return int64Ptr(*value)
}

func cloneFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	return float64Ptr(*value)
}
