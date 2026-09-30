package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/admission"
	"github.com/caduceus/caduceus/internal/availability"
	"github.com/caduceus/caduceus/internal/config"
	"github.com/caduceus/caduceus/internal/registry"
	"github.com/caduceus/caduceus/internal/workers"
)

func TestDisabledWorkerIsUnavailableAtEveryLocalBoundary(t *testing.T) {
	cfg, err := config.Default()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Worker.Enabled = false
	controller, err := admission.New(cfg.Worker.MaxConcurrentTasks, cfg.Worker.MaxQueueDepth)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &phase2Runtime{
		ctx:          context.Background(),
		admission:    controller,
		availability: availability.New(cfg.Worker.Availability, nil, nil),
		sessionID:    "session-local",
	}
	node := &Node{cfg: cfg, phase2: runtime}
	worker := node.decorateSelfWorker(workers.Worker{
		WorkerID: "worker-local",
		PeerID:   "peer-local",
		Allowed:  true,
		State:    workers.StateAvailable,
	})

	if worker.State != workers.StateUnavailable {
		t.Fatalf("worker state = %q, want unavailable", worker.State)
	}
	if worker.AcceptingWork == nil || *worker.AcceptingWork {
		t.Fatalf("accepting work = %v, want false", worker.AcceptingWork)
	}
	if stats := controller.Stats(); stats.Accepting {
		t.Fatalf("disabled worker admission remained accepting: %#v", stats)
	}

	workerRegistry, err := registry.New(registry.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := workerRegistry.Register(worker); err != nil {
		t.Fatal(err)
	}
	snapshot := workerRegistry.Snapshot()
	if len(snapshot.Workers) != 1 || snapshot.Workers[0].Schedulable {
		t.Fatalf("disabled worker became schedulable: %#v", snapshot.Workers)
	}
}

func TestSessionReplacementCancelsInflightWorkerAttempts(t *testing.T) {
	workerRegistry, err := registry.New(registry.Options{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &phase2Runtime{
		registry: workerRegistry,
		active:   make(map[string]activeAttempt),
	}
	node := &Node{phase2: runtime}
	first := workers.Worker{
		WorkerID:  "worker-a",
		PeerID:    "peer-a",
		SessionID: "session-a",
		Allowed:   true,
		State:     workers.StateAvailable,
	}
	node.registerPhase2Worker(first)

	canceled := make(chan struct{}, 1)
	runtime.active["task-a"] = activeAttempt{
		AttemptID: "attempt-a",
		WorkerID:  first.WorkerID,
		Cancel: func() {
			select {
			case canceled <- struct{}{}:
			default:
			}
		},
	}
	second := first
	second.SessionID = "session-b"
	node.registerPhase2Worker(second)

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("session replacement did not cancel in-flight work")
	}
	got, ok := workerRegistry.Get(first.WorkerID)
	if !ok || got.SessionID != second.SessionID {
		t.Fatalf("active worker = %#v, want replacement session", got)
	}
}
