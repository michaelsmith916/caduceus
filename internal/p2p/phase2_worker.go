package p2p

import (
	"context"
	"time"

	"github.com/caduceus/caduceus/internal/availability"
	"github.com/caduceus/caduceus/internal/security"
	"github.com/caduceus/caduceus/internal/workers"
	"github.com/caduceus/caduceus/pkg/caduceus"
)

func (r *phase2Runtime) effectiveAvailability(node *Node) availability.Result {
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	result := r.availability.Evaluate(ctx)
	if node != nil && !node.cfg.Worker.Enabled {
		return availability.Result{
			State:         availability.StateUnavailable,
			AcceptingWork: false,
			Reason:        "worker_disabled",
		}
	}
	return result
}

func (r *phase2Runtime) applyAvailability(node *Node) availability.Result {
	result := r.effectiveAvailability(node)
	if r.admission != nil {
		draining := result.State == availability.StateDraining
		r.admission.SetDraining(draining)
		r.admission.SetAccepting(result.AcceptingWork && !draining)
	}
	return result
}

func (n *Node) decorateSelfWorker(worker workers.Worker) workers.Worker {
	if n.phase2 == nil {
		return worker
	}
	result := n.phase2.applyAvailability(n)
	stats := n.phase2.admission.Stats()
	accepting := result.AcceptingWork && stats.Accepting && !stats.Draining && !stats.Closed
	state := workers.State(result.State)
	if state == workers.StateAvailable && stats.RunningTasks > 0 {
		state = workers.StateBusy
	}
	worker.ProtocolVersion = caduceus.Protocol
	worker.SessionID = n.phase2.sessionID
	worker.StatusTimestamp = time.Now().UTC()
	worker.State = state
	worker.AcceptingWork = boolPointer(accepting)
	worker.RunningTasks = intPointer(stats.RunningTasks)
	worker.MaxConcurrentTasks = intPointer(stats.MaxConcurrentTasks)
	worker.QueueDepth = intPointer(stats.QueuedTasks)
	worker.MaxQueueDepth = intPointer(stats.MaxQueueDepth)
	worker.CostWeight = floatPointer(n.cfg.Worker.CostWeight)
	return worker
}

func (n *Node) registerPhase2Worker(worker workers.Worker) {
	if n.phase2 == nil {
		return
	}
	result, err := n.phase2.registry.RegisterWithResult(worker)
	if err != nil {
		if n.log != nil {
			n.log.Debug("worker registry update rejected", "worker_id", worker.WorkerID, "peer_id", worker.PeerID, "session_id", worker.SessionID, "error", err.Error())
		}
		return
	}
	if result.SessionReplaced {
		n.phase2.cancelWorkerAttempts(worker.WorkerID)
		if n.log != nil {
			n.log.Info("worker session replaced", "worker_id", worker.WorkerID, "previous_session_id", result.PreviousSessionID, "session_id", result.SessionID)
		}
	}
}

func (n *Node) applyWorkerStatus(status workers.StatusAdvertisement) error {
	if n.phase2 == nil {
		return nil
	}
	if err := n.phase2.registry.Heartbeat(status); err != nil {
		return err
	}
	if worker, ok := n.phase2.registry.Get(status.WorkerID); ok {
		n.setLegacyWorker(worker)
	}
	return nil
}

func (n *Node) UpdateAllowedPeers(allowed security.AllowedPeers) {
	n.mu.Lock()
	n.allowed = allowed
	for workerID, worker := range n.workers {
		if worker.Local {
			continue
		}
		entry, ok := allowed.Get(worker.PeerID)
		worker.Allowed = allowed.IsAllowed(worker.PeerID, n.cfg.Security.RequireAllowlist)
		if ok && entry.TrustLevel != "" {
			worker.TrustLevel = entry.TrustLevel
		}
		n.workers[workerID] = worker
		if n.phase2 != nil {
			_ = n.phase2.registry.Register(worker)
		}
	}
	n.mu.Unlock()
}

func (n *Node) allowedPeer(peerID string) (security.AllowedPeer, bool, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	entry, exists := n.allowed.Get(peerID)
	return entry, exists, n.allowed.IsAllowed(peerID, n.cfg.Security.RequireAllowlist)
}

func intPointer(value int) *int { return &value }
