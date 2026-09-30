package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caduceus/caduceus/internal/admission"
	"github.com/caduceus/caduceus/internal/availability"
	"github.com/caduceus/caduceus/internal/registry"
	"github.com/caduceus/caduceus/internal/scheduler"
	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/caduceus/caduceus/internal/telemetry"
	"github.com/caduceus/caduceus/internal/workers"
	"github.com/caduceus/caduceus/pkg/caduceus"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type phase2Runtime struct {
	ctx          context.Context
	cancel       context.CancelFunc
	registry     *registry.Registry
	scheduler    *scheduler.Scheduler
	admission    *admission.Controller
	availability *availability.Evaluator
	telemetry    *telemetry.Collector
	performance  *telemetry.PerformanceTracker
	sessionID    string
	sequence     atomic.Uint64

	mu              sync.RWMutex
	queueMu         sync.Mutex
	queueChanged    chan struct{}
	dispatchPermits chan struct{}
	assignments     scheduler.AssignmentHistory
	cooldowns       map[string]time.Time
	now             func() time.Time
	waitCooldown    func(context.Context, time.Duration) error
	dispatchCancels map[string]context.CancelFunc
	active          map[string]activeAttempt
	inbound         map[string]inboundAttemptClaim
	wg              sync.WaitGroup
}

type activeAttempt struct {
	AttemptID string
	WorkerID  string
	Cancel    context.CancelFunc
}

type inboundAttemptClaim struct {
	Requester string
	Attempt   tasks.TaskAttempt
	Cancel    context.CancelFunc
}

type TaskRejectedError struct {
	Rejection tasks.TaskRejection
}

func (e *TaskRejectedError) Error() string {
	if e == nil {
		return "task rejected"
	}
	return fmt.Sprintf("task rejected by %s: %s (%s)", e.Rejection.WorkerID, e.Rejection.Detail, e.Rejection.Reason)
}

func newPhase2Runtime(parent context.Context, node *Node) (*phase2Runtime, error) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	workerRegistry, err := registry.New(registry.Options{
		SuspectAfter: node.cfg.Heartbeat.SuspectAfter(),
		EvictAfter:   node.cfg.Heartbeat.EvictAfter(),
	})
	if err != nil {
		cancel()
		return nil, err
	}
	router, err := scheduler.New(scheduler.Config{
		Weights: scheduler.Weights{
			Throughput:       node.cfg.Scheduler.Weights.Throughput,
			Load:             node.cfg.Scheduler.Weights.RunningTasks,
			QueueDepth:       node.cfg.Scheduler.Weights.QueueDepth,
			ModelResidency:   node.cfg.Scheduler.Weights.ModelResidency,
			RTT:              node.cfg.Scheduler.Weights.RTT,
			Cost:             node.cfg.Scheduler.Weights.Cost,
			ResourceHeadroom: node.cfg.Scheduler.Weights.ResourceHeadroom,
		},
		MinimumMetricSamples: node.cfg.Scheduler.MinimumMetricSamples,
		MetricMaxAge:         node.cfg.Scheduler.MetricMaxAge(),
		ScoreEpsilon:         node.cfg.Scheduler.ScoreEpsilon,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	controller, err := admission.New(node.cfg.Worker.MaxConcurrentTasks, node.cfg.Worker.MaxQueueDepth)
	if err != nil {
		cancel()
		return nil, err
	}
	performance, err := telemetry.NewPerformanceTracker(telemetry.PerformanceOptions{})
	if err != nil {
		cancel()
		return nil, err
	}
	runtime := &phase2Runtime{
		ctx:             ctx,
		cancel:          cancel,
		registry:        workerRegistry,
		scheduler:       router,
		admission:       controller,
		availability:    availability.New(node.cfg.Worker.Availability, nil, nil),
		telemetry:       telemetry.New(telemetry.Options{}),
		performance:     performance,
		sessionID:       tasks.NewID("session"),
		queueChanged:    make(chan struct{}),
		dispatchPermits: make(chan struct{}, node.cfg.Scheduler.MaxInFlightTasks),
		assignments:     scheduler.AssignmentHistory{},
		cooldowns:       make(map[string]time.Time),
		now:             time.Now,
		waitCooldown:    waitForWorkerCooldown,
		dispatchCancels: make(map[string]context.CancelFunc),
		active:          map[string]activeAttempt{},
		inbound:         map[string]inboundAttemptClaim{},
	}
	return runtime, nil
}

func (r *phase2Runtime) start(node *Node) {
	if r == nil {
		return
	}
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		r.heartbeatLoop(node)
	}()
	go func() {
		defer r.wg.Done()
		r.sweepLoop(node)
	}()
	node.resumePersistedQueue()
}

func (r *phase2Runtime) close() {
	if r == nil {
		return
	}
	r.cancel()
	r.admission.Close()
	r.mu.Lock()
	for taskID, attempt := range r.active {
		attempt.Cancel()
		delete(r.active, taskID)
	}
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *phase2Runtime) heartbeatLoop(node *Node) {
	ticker := time.NewTicker(node.cfg.Heartbeat.Interval())
	defer ticker.Stop()
	r.advertise(node)
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.advertise(node)
		}
	}
}

func (r *phase2Runtime) sweepLoop(node *Node) {
	interval := node.cfg.Heartbeat.Interval()
	if half := node.cfg.Heartbeat.SuspectAfter() / 2; half > 0 && half < interval {
		interval = half
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			evicted := r.registry.Sweep()
			for _, worker := range evicted {
				node.removeLegacyWorker(worker.WorkerID)
				r.cancelWorkerAttempts(worker.WorkerID)
				if node.log != nil {
					node.log.Info("worker evicted", "worker_id", worker.WorkerID, "session_id", worker.SessionID)
				}
			}
			r.cancelStaleAttempts()
		}
	}
}

func (r *phase2Runtime) advertise(node *Node) {
	status := r.localStatus(node)
	err := r.registry.Heartbeat(status)
	if errors.Is(err, registry.ErrNotRegistered) {
		// A suspended process can also lose its own receipt-time lease.
		r.renewSession(node, status.SessionID)
		status = r.localStatus(node)
		err = r.registry.Heartbeat(status)
	}
	if err != nil && !errors.Is(err, registry.ErrOutOfOrder) {
		if node.log != nil {
			node.log.Debug("local heartbeat update failed", "error", err.Error())
		}
	}
	worker, ok := r.registry.Get(node.HostID())
	if ok {
		node.setLegacyWorker(worker)
	}
	snapshot := r.registry.Snapshot()
	// A partition may evict both sides. Authenticated connected peers remain
	// rendezvous targets even when their registry entries have been removed.
	known := make(map[string]bool, len(snapshot.Workers))
	for _, candidate := range snapshot.Workers {
		known[candidate.Worker.PeerID] = true
	}
	for _, id := range node.host.Network().Peers() {
		_, _, allowed := node.allowedPeer(id.String())
		if allowed && !known[id.String()] {
			snapshot.Workers = append(snapshot.Workers, registry.WorkerSnapshot{Worker: workers.Worker{WorkerID: id.String(), PeerID: id.String()}})
		}
	}
	jobs := make([]heartbeatFanoutJob, 0, len(snapshot.Workers))
	for _, candidate := range snapshot.Workers {
		worker := candidate.Worker
		if worker.Local || worker.PeerID == "" || worker.PeerID == node.HostID() {
			continue
		}
		target := worker
		jobs = append(jobs, func(ctx context.Context) {
			if err := r.sendStatus(ctx, node, target, status); err != nil && node.log != nil {
				node.log.Debug("worker heartbeat failed", "worker_id", target.WorkerID, "peer_id", target.PeerID, "error", err.Error())
			}
		})
	}
	fanoutCtx, cancel := context.WithTimeout(r.ctx, node.cfg.Heartbeat.Interval())
	defer cancel()
	runHeartbeatFanout(fanoutCtx, heartbeatFanoutConcurrency, jobs)
}

func (r *phase2Runtime) sendStatus(ctx context.Context, node *Node, worker workers.Worker, status workers.StatusAdvertisement) error {
	return r.sendStatusOnce(ctx, node, worker, status, true)
}

func (r *phase2Runtime) sendStatusOnce(ctx context.Context, node *Node, worker workers.Worker, status workers.StatusAdvertisement, allowRejoin bool) error {
	peerID, err := peer.Decode(worker.PeerID)
	if err != nil {
		return err
	}
	started := time.Now()
	stream, err := node.host.NewStream(ctx, peerID, protocol.ID(WorkerProtocolV2), protocol.ID(WorkerProtocol))
	if err != nil {
		return err
	}
	defer stream.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}
	stopReset := context.AfterFunc(ctx, func() { _ = stream.Reset() })
	defer stopReset()
	encoder := json.NewEncoder(stream)
	decoder := newEnvelopeDecoder(stream)
	messageType := TypeWorkerStatus
	payload := any(status)
	if stream.Protocol() == protocol.ID(WorkerProtocol) {
		messageType = TypeWorkerHello
		payload = node.selfWorker(ctx)
	}
	envelope, err := node.envelopeForProtocol(stream.Protocol(), messageType, payload)
	if err != nil {
		return err
	}
	if err := encoder.Encode(envelope); err != nil {
		return err
	}
	var reply Envelope
	if err := decoder.Decode(&reply); err != nil {
		return err
	}
	if err := node.verifyEnvelope(reply, peerID); err != nil {
		return err
	}
	switch reply.Type {
	case TypeWorkerStatusAck:
		var acknowledgement StatusAck
		if err := json.Unmarshal(reply.Payload, &acknowledgement); err != nil {
			return err
		}
		if acknowledgement.Sequence != status.Sequence || acknowledgement.SessionID != status.SessionID {
			return errors.New("heartbeat acknowledgement mismatch")
		}
	case TypeError:
		var problem ErrorPayload
		if err := json.Unmarshal(reply.Payload, &problem); err != nil {
			return err
		}
		if allowRejoin && (problem.Code == "worker_not_registered" || problem.Code == "stale_session") {
			return r.rejoinWorker(ctx, node, peerID, status.SessionID)
		}
		return &workerProtocolError{Code: problem.Code}
	case TypeWorkerHello:
		var remote workers.Worker
		if err := json.Unmarshal(reply.Payload, &remote); err != nil {
			return err
		}
		node.registerWorker(remote, peerID.String())
	default:
		return fmt.Errorf("unexpected heartbeat response %q", reply.Type)
	}
	if err := r.registry.SetRTT(worker.WorkerID, float64(time.Since(started))/float64(time.Millisecond), time.Now().UTC()); err != nil {
		// Enrollment deliberately sends and acknowledges the first status before
		// registering the coordinator locally, preventing sequence overtaking by
		// the periodic fanout. RTT is recorded after later heartbeats.
		if !errors.Is(err, registry.ErrNotRegistered) {
			return err
		}
	}
	return nil
}

type workerProtocolError struct{ Code string }

func (e *workerProtocolError) Error() string {
	return "worker protocol rejected message (" + e.Code + ")"
}

func (r *phase2Runtime) currentSession() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sessionID
}

// Only a verified response from an authorized Noise peer can request renewal.
// Compare the challenged session so delayed responses cannot rotate a newer one.
func (r *phase2Runtime) renewSession(node *Node, retired string) {
	r.mu.Lock()
	if r.sessionID != retired {
		r.mu.Unlock()
		return
	}
	r.sessionID = tasks.NewID("session")
	cancels := make([]context.CancelFunc, 0, len(r.inbound))
	for _, claim := range r.inbound {
		cancels = append(cancels, claim.Cancel)
	}
	r.mu.Unlock()
	for _, cancel := range cancels {
		if cancel != nil {
			cancel()
		}
	}
	r.cancelWorkerAttempts(node.HostID())
	node.registerSelf(r.ctx)
}

func (r *phase2Runtime) rejoinWorker(ctx context.Context, node *Node, id peer.ID, challenged string) error {
	remote, err := node.exchangeWorkerHelloV2(ctx, id)
	var problem *workerProtocolError
	if errors.As(err, &problem) && problem.Code == "session_retired" {
		r.renewSession(node, challenged)
		remote, err = node.exchangeWorkerHelloV2(ctx, id)
	}
	if err != nil {
		return err
	}
	if err := node.registerWorker(remote, id.String()); err != nil {
		return err
	}
	return r.sendStatusOnce(ctx, node, remote, r.localStatus(node), false)
}

func (r *phase2Runtime) localStatus(node *Node) workers.StatusAdvertisement {
	result := r.applyAvailability(node)
	stats := r.admission.Stats()
	accepting := result.AcceptingWork && stats.Accepting && !stats.Draining && !stats.Closed
	state := workers.State(result.State)
	if state == workers.StateAvailable && stats.RunningTasks > 0 {
		state = workers.StateBusy
	}
	maximum, queueDepth, running, maxQueue := stats.MaxConcurrentTasks, stats.QueuedTasks, stats.RunningTasks, stats.MaxQueueDepth
	worker, _ := r.registry.Get(node.HostID())
	resources, resourceErr := r.telemetry.Collect(r.ctx)
	if resourceErr != nil && node.log != nil {
		node.log.Debug("worker resource collection failed", "error", resourceErr.Error())
	}
	return workers.StatusAdvertisement{
		WorkerID:           node.HostID(),
		PeerID:             node.HostID(),
		SessionID:          r.currentSession(),
		ProtocolVersion:    caduceus.Protocol,
		Sequence:           r.sequence.Add(1),
		Timestamp:          time.Now().UTC(),
		State:              state,
		AcceptingWork:      boolPointer(accepting),
		RunningTasks:       &running,
		MaxConcurrentTasks: &maximum,
		QueueDepth:         &queueDepth,
		MaxQueueDepth:      &maxQueue,
		Capabilities:       &worker.Capabilities,
		Resources:          &resources,
		LoadedModels:       append([]string(nil), worker.Capabilities.Models...),
		CostWeight:         floatPointer(node.cfg.Worker.CostWeight),
		Performance:        r.performance.Snapshot(),
	}
}

func (r *phase2Runtime) evaluate(request tasks.Request) (scheduler.RoutingDecision, error) {
	now := r.cooldownNow()
	r.mu.Lock()
	r.pruneCooldownsLocked(now)
	history := make(scheduler.AssignmentHistory, len(r.assignments))
	for workerID, assigned := range r.assignments {
		history[workerID] = assigned
	}
	cooldowns := make(map[string]time.Time, len(r.cooldowns))
	for workerID, until := range r.cooldowns {
		cooldowns[workerID] = until
	}
	r.mu.Unlock()
	decision, err := r.scheduler.Evaluate(request, r.registry.Snapshot(), history)
	if err != nil {
		return scheduler.RoutingDecision{}, err
	}
	applyWorkerCooldowns(&decision, cooldowns, now)
	return decision, nil
}

func (r *phase2Runtime) markAssigned(workerID string) {
	r.mu.Lock()
	r.assignments[workerID] = time.Now().UTC()
	r.mu.Unlock()
}

func (r *phase2Runtime) trackDispatch(taskID string, cancel context.CancelFunc) {
	r.mu.Lock()
	r.dispatchCancels[taskID] = cancel
	r.mu.Unlock()
}

func (r *phase2Runtime) untrackDispatch(taskID string) {
	r.mu.Lock()
	delete(r.dispatchCancels, taskID)
	r.mu.Unlock()
}

func (r *phase2Runtime) track(taskID string, attempt tasks.TaskAttempt, cancel context.CancelFunc) {
	r.mu.Lock()
	r.active[taskID] = activeAttempt{AttemptID: attempt.AttemptID, WorkerID: attempt.WorkerID, Cancel: cancel}
	r.mu.Unlock()
}

func (r *phase2Runtime) untrack(taskID, attemptID string) {
	r.mu.Lock()
	if current, ok := r.active[taskID]; ok && current.AttemptID == attemptID {
		delete(r.active, taskID)
	}
	r.mu.Unlock()
}

func (r *phase2Runtime) cancelTask(taskID string) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	attempt, attemptOK := r.active[taskID]
	dispatchCancel, dispatchOK := r.dispatchCancels[taskID]
	r.mu.RUnlock()
	if attemptOK && attempt.Cancel != nil {
		attempt.Cancel()
	}
	if dispatchOK && dispatchCancel != nil {
		dispatchCancel()
	}
	return (attemptOK && attempt.Cancel != nil) || (dispatchOK && dispatchCancel != nil)
}

func (r *phase2Runtime) cancelWorkerAttempts(workerID string) {
	r.mu.RLock()
	var cancels []context.CancelFunc
	for _, attempt := range r.active {
		if attempt.WorkerID == workerID {
			cancels = append(cancels, attempt.Cancel)
		}
	}
	r.mu.RUnlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (r *phase2Runtime) cancelStaleAttempts() {
	snapshot := r.registry.Snapshot()
	healthy := make(map[string]bool, len(snapshot.Workers))
	for _, worker := range snapshot.Workers {
		healthy[worker.Worker.WorkerID] = worker.Healthy && !worker.Stale
	}
	r.mu.RLock()
	var cancels []context.CancelFunc
	for _, attempt := range r.active {
		if !healthy[attempt.WorkerID] {
			cancels = append(cancels, attempt.Cancel)
		}
	}
	r.mu.RUnlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (r *phase2Runtime) admissionSnapshot(node *Node, request tasks.Request) admission.AdmissionSnapshot {
	worker, _ := r.registry.Get(node.HostID())
	result := r.effectiveAvailability(node)
	accepting, allowed := result.AcceptingWork, true
	snapshot := admission.AdmissionSnapshot{
		WorkerID:        node.HostID(),
		State:           result.State,
		Accepting:       &accepting,
		Allowed:         &allowed,
		Capabilities:    capabilityNames(worker.Capabilities),
		AvailableModels: append([]string(nil), worker.Capabilities.Models...),
		ModelsKnown:     len(worker.Capabilities.Models) > 0,
		TrustLevel:      worker.TrustLevel,
	}
	if strings.TrimSpace(worker.GroupHash) != "" {
		snapshot.GroupIDs = []string{worker.GroupHash}
	}
	populateAdmissionResources(&snapshot, worker.Resources)
	sort.Strings(snapshot.Capabilities)
	sort.Strings(snapshot.Runtimes)
	return snapshot
}

func populateAdmissionResources(snapshot *admission.AdmissionSnapshot, resources *workers.ResourceSnapshot) {
	if snapshot == nil || resources == nil || resources.Status != workers.MetricCurrent {
		return
	}
	if metric := resources.CPU.AvailableCapacity; metric.Status == workers.MetricCurrent && metric.Value != nil {
		snapshot.AvailableCPU = floatPointer(*metric.Value)
	}
	if metric := resources.RAM.AvailableBytes; metric.Status == workers.MetricCurrent && metric.Value != nil && *metric.Value >= 0 {
		value := uint64(*metric.Value)
		snapshot.AvailableRAMBytes = &value
	}
	for _, gpu := range resources.GPUs {
		item := admission.GPUSnapshot{Vendor: gpu.Vendor, Capabilities: append([]string(nil), gpu.Capabilities...)}
		if metric := gpu.AvailableMemoryBytes; metric.Status == workers.MetricCurrent && metric.Value != nil && *metric.Value >= 0 {
			value := uint64(*metric.Value)
			item.AvailableMemoryBytes = &value
		}
		snapshot.GPUs = append(snapshot.GPUs, item)
		if gpu.Runtime != "" {
			snapshot.Runtimes = append(snapshot.Runtimes, gpu.Runtime)
		}
	}
}

func (r *phase2Runtime) freshAdmissionSnapshot(ctx context.Context, node *Node, request tasks.Request) admission.AdmissionSnapshot {
	snapshot := r.admissionSnapshot(node, request)
	snapshot.AvailableCPU = nil
	snapshot.AvailableRAMBytes = nil
	snapshot.GPUs = nil
	snapshot.Runtimes = nil
	if ctx == nil {
		ctx = r.ctx
	}
	resources, err := r.telemetry.Collect(ctx)
	if err != nil {
		if node.log != nil {
			node.log.Debug("fresh admission telemetry failed", "error", err.Error())
		}
		return snapshot
	}
	populateAdmissionResources(&snapshot, &resources)
	sort.Strings(snapshot.Runtimes)
	return snapshot
}

func capabilityNames(capabilities workers.Capabilities) []string {
	var out []string
	if capabilities.LLM {
		out = append(out, "llm")
	}
	if capabilities.Streaming {
		out = append(out, "streaming")
	}
	if capabilities.Artifacts {
		out = append(out, "artifacts")
	}
	out = append(out, capabilities.Tools...)
	return out
}

func (n *Node) setLegacyWorker(worker workers.Worker) {
	n.mu.Lock()
	n.workers[worker.WorkerID] = worker
	n.mu.Unlock()
}

func (n *Node) removeLegacyWorker(workerID string) {
	n.mu.Lock()
	delete(n.workers, workerID)
	n.mu.Unlock()
}

func boolPointer(value bool) *bool        { return &value }
func floatPointer(value float64) *float64 { return &value }

func normalizeFilterReason(reason string) string {
	if index := strings.IndexByte(reason, ':'); index >= 0 {
		return reason[:index]
	}
	return reason
}
