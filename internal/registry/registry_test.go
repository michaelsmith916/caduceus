package registry

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/workers"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func newTestRegistry(t *testing.T) (*Registry, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)}
	registry, err := New(Options{
		SuspectAfter: 10 * time.Second,
		EvictAfter:   30 * time.Second,
		Now:          clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry, clock
}

func testWorker(id, session string) workers.Worker {
	accepting := true
	return workers.Worker{
		WorkerID:      id,
		PeerID:        "peer-" + id,
		SessionID:     session,
		Allowed:       true,
		State:         workers.StateAvailable,
		AcceptingWork: &accepting,
		TrustLevel:    "trusted-lan",
		Capabilities: workers.Capabilities{
			LLM:                true,
			Streaming:          true,
			Models:             []string{"model-a"},
			MaxConcurrentTasks: 2,
		},
	}
}

func testStatus(clock *fakeClock, worker workers.Worker, sequence uint64) workers.StatusAdvertisement {
	return workers.StatusAdvertisement{
		WorkerID:        worker.WorkerID,
		PeerID:          worker.PeerID,
		SessionID:       worker.SessionID,
		ProtocolVersion: "2.0",
		Sequence:        sequence,
		Timestamp:       clock.Now(),
		State:           workers.StateAvailable,
		AcceptingWork:   boolPtr(true),
	}
}

func TestRegisterSupportsLegacyHelloAndDefaultsState(t *testing.T) {
	registry, clock := newTestRegistry(t)
	worker := testWorker("worker-a", "")
	worker.State = ""
	worker.AcceptingWork = nil

	if err := registry.Register(worker); err != nil {
		t.Fatal(err)
	}
	got, ok := registry.Get(worker.WorkerID)
	if !ok {
		t.Fatal("worker was not registered")
	}
	if got.State != workers.StateAvailable {
		t.Fatalf("state = %q, want available", got.State)
	}
	if !got.LastSeen.Equal(clock.Now()) {
		t.Fatalf("last seen = %v, want %v", got.LastSeen, clock.Now())
	}
	snapshot := registry.Snapshot()
	if snapshot.Version != 1 || len(snapshot.Workers) != 1 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	if !snapshot.Workers[0].Healthy || !snapshot.Workers[0].Schedulable {
		t.Fatalf("new worker should be healthy and schedulable: %#v", snapshot.Workers[0])
	}
}

func TestLegacyToVersionedSessionSignalsReplacement(t *testing.T) {
	registry, _ := newTestRegistry(t)
	legacy := testWorker("worker-a", "")
	if err := registry.Register(legacy); err != nil {
		t.Fatal(err)
	}
	versioned := legacy
	versioned.SessionID = "session-a"
	result, err := registry.RegisterWithResult(versioned)
	if err != nil {
		t.Fatal(err)
	}
	if !result.SessionReplaced || result.PreviousSessionID != "" || result.SessionID != versioned.SessionID {
		t.Fatalf("unexpected registration result: %#v", result)
	}
	got, ok := registry.Get(versioned.WorkerID)
	if !ok || got.SessionID != versioned.SessionID {
		t.Fatalf("active worker = %#v, want versioned session", got)
	}
}

func TestHeartbeatUpdatesHealthyWorker(t *testing.T) {
	registry, clock := newTestRegistry(t)
	worker := testWorker("worker-a", "session-a")
	if err := registry.Register(worker); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	status := testStatus(clock, worker, 1)
	status.State = workers.StateBusy
	status.RunningTasks = intPtr(1)
	status.MaxConcurrentTasks = intPtr(2)
	status.QueueDepth = intPtr(0)
	status.LoadedModels = []string{"model-a"}

	if err := registry.Heartbeat(status); err != nil {
		t.Fatal(err)
	}
	got, _ := registry.Get(worker.WorkerID)
	if got.Sequence != 1 || got.State != workers.StateBusy {
		t.Fatalf("heartbeat was not applied: %#v", got)
	}
	if got.RunningTasks == nil || *got.RunningTasks != 1 {
		t.Fatalf("running tasks = %v, want 1", got.RunningTasks)
	}
	if !got.LastSeen.Equal(clock.Now()) {
		t.Fatalf("liveness used worker timestamp instead of receipt time")
	}
	snapshot := registry.Snapshot()
	if snapshot.Version != 2 || !snapshot.Workers[0].Healthy {
		t.Fatalf("unexpected snapshot after heartbeat: %#v", snapshot)
	}
}

func TestHeartbeatRejectsOutOfOrderSequence(t *testing.T) {
	registry, clock := newTestRegistry(t)
	worker := testWorker("worker-a", "session-a")
	if err := registry.Register(worker); err != nil {
		t.Fatal(err)
	}
	if err := registry.Heartbeat(testStatus(clock, worker, 2)); err != nil {
		t.Fatal(err)
	}
	for _, sequence := range []uint64{2, 1} {
		err := registry.Heartbeat(testStatus(clock, worker, sequence))
		if !errors.Is(err, ErrOutOfOrder) {
			t.Fatalf("sequence %d error = %v, want ErrOutOfOrder", sequence, err)
		}
	}
	if got := registry.Snapshot().Version; got != 2 {
		t.Fatalf("rejected heartbeats changed version to %d", got)
	}
}

func TestReconnectionRetiresOldSession(t *testing.T) {
	registry, clock := newTestRegistry(t)
	first := testWorker("worker-a", "session-a")
	if err := registry.Register(first); err != nil {
		t.Fatal(err)
	}
	if err := registry.Heartbeat(testStatus(clock, first, 1)); err != nil {
		t.Fatal(err)
	}

	second := testWorker("worker-a", "session-b")
	result, err := registry.RegisterWithResult(second)
	if err != nil {
		t.Fatal(err)
	}
	if !result.SessionReplaced || result.PreviousSessionID != first.SessionID || result.SessionID != second.SessionID {
		t.Fatalf("unexpected registration result: %#v", result)
	}
	if err := registry.Heartbeat(testStatus(clock, second, 1)); err != nil {
		t.Fatalf("new session heartbeat failed: %v", err)
	}
	if err := registry.Heartbeat(testStatus(clock, first, 2)); !errors.Is(err, ErrOldSession) {
		t.Fatalf("old heartbeat error = %v, want ErrOldSession", err)
	}
	if err := registry.Register(first); !errors.Is(err, ErrOldSession) {
		t.Fatalf("old hello error = %v, want ErrOldSession", err)
	}
	got, _ := registry.Get(first.WorkerID)
	if got.SessionID != second.SessionID || got.Sequence != 1 {
		t.Fatalf("old session altered active registration: %#v", got)
	}
}

func TestEvictionRetainsPeerAndSessionTombstones(t *testing.T) {
	registry, clock := newTestRegistry(t)
	first := testWorker("worker-a", "session-a")
	if err := registry.Register(first); err != nil {
		t.Fatal(err)
	}
	second := testWorker("worker-a", "session-b")
	if err := registry.Register(second); err != nil {
		t.Fatal(err)
	}

	clock.Advance(30 * time.Second)
	if got := registry.Sweep(); len(got) != 1 {
		t.Fatalf("evicted workers = %#v, want one", got)
	}
	for _, stale := range []workers.Worker{first, second} {
		if err := registry.Register(stale); !errors.Is(err, ErrOldSession) {
			t.Fatalf("session %q registration error = %v, want ErrOldSession", stale.SessionID, err)
		}
	}
	legacyRollback := first
	legacyRollback.SessionID = ""
	if err := registry.Register(legacyRollback); !errors.Is(err, ErrSessionMismatch) {
		t.Fatalf("legacy rollback error = %v, want ErrSessionMismatch", err)
	}
	impostor := testWorker("worker-a", "session-c")
	impostor.PeerID = "different-peer"
	if err := registry.Register(impostor); !errors.Is(err, ErrPeerMismatch) {
		t.Fatalf("impostor registration error = %v, want ErrPeerMismatch", err)
	}

	third := testWorker("worker-a", "session-c")
	if err := registry.Register(third); err != nil {
		t.Fatalf("new authenticated session was not allowed after eviction: %v", err)
	}
	if err := registry.Register(first); !errors.Is(err, ErrOldSession) {
		t.Fatalf("oldest session registration error = %v, want ErrOldSession", err)
	}
	got, ok := registry.Get(third.WorkerID)
	if !ok || got.SessionID != third.SessionID {
		t.Fatalf("active worker = %#v, want session-c", got)
	}
}

func TestSessionHistoryFailsClosedInsteadOfForgettingOldSessions(t *testing.T) {
	registry, _ := newTestRegistry(t)
	active := testWorker("worker-a", "session-active")
	if err := registry.Register(active); err != nil {
		t.Fatal(err)
	}
	current := registry.entries[active.WorkerID]
	for index := 0; index < maxRetiredSessions-1; index++ {
		current.retiredSessions[fmt.Sprintf("retired-%d", index)] = struct{}{}
	}

	replacement := active
	replacement.SessionID = "session-new"
	if err := registry.Register(replacement); !errors.Is(err, ErrSessionHistory) {
		t.Fatalf("registration error = %v, want ErrSessionHistory", err)
	}
	got, ok := registry.Get(active.WorkerID)
	if !ok || got.SessionID != active.SessionID {
		t.Fatalf("history exhaustion altered active worker: %#v", got)
	}
	old := active
	old.SessionID = "retired-0"
	if err := registry.Register(old); !errors.Is(err, ErrOldSession) {
		t.Fatalf("retired session error = %v, want ErrOldSession", err)
	}
}

func TestPeerIdentityCannotReplaceWorker(t *testing.T) {
	registry, _ := newTestRegistry(t)
	worker := testWorker("worker-a", "session-a")
	if err := registry.Register(worker); err != nil {
		t.Fatal(err)
	}
	impostor := testWorker("worker-a", "session-b")
	impostor.PeerID = "different-peer"
	if err := registry.Register(impostor); !errors.Is(err, ErrPeerMismatch) {
		t.Fatalf("registration error = %v, want ErrPeerMismatch", err)
	}
}

func TestStaleExclusionRecoveryAndEviction(t *testing.T) {
	registry, clock := newTestRegistry(t)
	worker := testWorker("worker-a", "session-a")
	if err := registry.Register(worker); err != nil {
		t.Fatal(err)
	}

	clock.Advance(10 * time.Second)
	stale := registry.Snapshot()
	if len(stale.Workers) != 1 || !stale.Workers[0].Suspect || !stale.Workers[0].Stale {
		t.Fatalf("worker did not become stale: %#v", stale)
	}
	if stale.Workers[0].Schedulable {
		t.Fatal("stale worker remained schedulable")
	}

	if err := registry.Heartbeat(testStatus(clock, worker, 1)); err != nil {
		t.Fatal(err)
	}
	if recovered := registry.Snapshot().Workers[0]; !recovered.Healthy || !recovered.Schedulable {
		t.Fatalf("heartbeat did not restore health: %#v", recovered)
	}

	clock.Advance(30 * time.Second)
	evicted := registry.Sweep()
	if len(evicted) != 1 || evicted[0].State != workers.StateOffline {
		t.Fatalf("unexpected eviction records: %#v", evicted)
	}
	if registry.Len() != 0 || len(registry.Snapshot().Workers) != 0 {
		t.Fatal("evicted worker remains active")
	}
}

func TestDrainingWorkerIsAliveButNotSchedulable(t *testing.T) {
	registry, clock := newTestRegistry(t)
	worker := testWorker("worker-a", "session-a")
	if err := registry.Register(worker); err != nil {
		t.Fatal(err)
	}
	status := testStatus(clock, worker, 1)
	status.State = workers.StateDraining
	status.AcceptingWork = boolPtr(false)
	if err := registry.Heartbeat(status); err != nil {
		t.Fatal(err)
	}
	got := registry.Snapshot().Workers[0]
	if !got.Healthy || !got.Alive || got.Schedulable {
		t.Fatalf("unexpected draining lifecycle: %#v", got)
	}
}

func TestMissingTelemetryIsAccepted(t *testing.T) {
	registry, clock := newTestRegistry(t)
	worker := testWorker("worker-a", "session-a")
	worker.Resources = nil
	if err := registry.Register(worker); err != nil {
		t.Fatal(err)
	}
	status := testStatus(clock, worker, 1)
	if err := registry.Heartbeat(status); err != nil {
		t.Fatalf("missing telemetry rejected: %v", err)
	}
}

func TestSnapshotIsSortedAndImmutable(t *testing.T) {
	registry, _ := newTestRegistry(t)
	total := int64(100)
	available := int64(50)
	running := 1
	workerB := testWorker("worker-b", "session-b")
	workerB.RunningTasks = &running
	workerB.LoadedModels = []string{"model-a"}
	workerB.Resources = &workers.ResourceSnapshot{
		Status:      workers.MetricCurrent,
		CollectedAt: time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC),
		RAM: workers.MemoryResources{
			TotalBytes:     workers.Int64Metric{Value: &total, Status: workers.MetricCurrent},
			AvailableBytes: workers.Int64Metric{Value: &available, Status: workers.MetricCurrent},
		},
	}
	if err := registry.Register(workerB); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(testWorker("worker-a", "session-a")); err != nil {
		t.Fatal(err)
	}

	snapshot := registry.Snapshot()
	if snapshot.Workers[0].Worker.WorkerID != "worker-a" || snapshot.Workers[1].Worker.WorkerID != "worker-b" {
		t.Fatalf("snapshot not sorted: %#v", snapshot.Workers)
	}
	mutable := &snapshot.Workers[1].Worker
	mutable.Capabilities.Models[0] = "changed"
	mutable.LoadedModels[0] = "changed"
	*mutable.RunningTasks = 99
	*mutable.Resources.RAM.AvailableBytes.Value = 0

	got, _ := registry.Get("worker-b")
	if got.Capabilities.Models[0] != "model-a" || got.LoadedModels[0] != "model-a" {
		t.Fatal("snapshot slice mutation leaked into registry")
	}
	if *got.RunningTasks != 1 || *got.Resources.RAM.AvailableBytes.Value != 50 {
		t.Fatal("snapshot pointer mutation leaked into registry")
	}
}

func TestSetRTTAndRemove(t *testing.T) {
	registry, clock := newTestRegistry(t)
	worker := testWorker("worker-a", "session-a")
	if err := registry.Register(worker); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetRTT(worker.WorkerID, 4.5, time.Time{}); err != nil {
		t.Fatal(err)
	}
	got, _ := registry.Get(worker.WorkerID)
	if got.RTTMillis == nil || *got.RTTMillis != 4.5 || !got.RTTUpdatedAt.Equal(clock.Now()) {
		t.Fatalf("RTT was not recorded: %#v", got)
	}
	removed, ok := registry.Remove(worker.WorkerID)
	if !ok || removed.State != workers.StateOffline || removed.AcceptingWork == nil || *removed.AcceptingWork {
		t.Fatalf("unexpected removed worker: %#v", removed)
	}
}

func TestValidation(t *testing.T) {
	if _, err := New(Options{SuspectAfter: -time.Second, EvictAfter: time.Minute}); err == nil {
		t.Fatal("negative suspect threshold accepted")
	}
	if _, err := New(Options{SuspectAfter: time.Minute, EvictAfter: time.Minute}); err == nil {
		t.Fatal("eviction threshold equal to suspect threshold accepted")
	}

	registry, clock := newTestRegistry(t)
	worker := testWorker("worker-a", "session-a")
	if err := registry.Register(worker); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*workers.StatusAdvertisement)
	}{
		{name: "missing session", mutate: func(status *workers.StatusAdvertisement) { status.SessionID = "" }},
		{name: "missing protocol", mutate: func(status *workers.StatusAdvertisement) { status.ProtocolVersion = "" }},
		{name: "invalid state", mutate: func(status *workers.StatusAdvertisement) { status.State = "bogus" }},
		{name: "negative running", mutate: func(status *workers.StatusAdvertisement) { status.RunningTasks = intPtr(-1) }},
		{name: "invalid cost", mutate: func(status *workers.StatusAdvertisement) { status.CostWeight = float64Ptr(math.NaN()) }},
		{name: "future timestamp", mutate: func(status *workers.StatusAdvertisement) { status.Timestamp = clock.Now().Add(10 * time.Minute) }},
		{name: "invalid GPU status", mutate: func(status *workers.StatusAdvertisement) {
			status.Resources = &workers.ResourceSnapshot{Status: workers.MetricCurrent, GPUStatus: "bogus", CollectedAt: clock.Now()}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			status := testStatus(clock, worker, 1)
			test.mutate(&status)
			if err := registry.Heartbeat(status); err == nil {
				t.Fatal("invalid status was accepted")
			}
		})
	}
	if err := registry.Heartbeat(testStatus(clock, testWorker("missing", "session"), 1)); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("unregistered heartbeat error = %v, want ErrNotRegistered", err)
	}
}

func TestConcurrentRegistrationHeartbeatAndSnapshots(t *testing.T) {
	registry, clock := newTestRegistry(t)
	const count = 32
	var wait sync.WaitGroup
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			id := time.Unix(int64(index), 0).UTC().Format("150405")
			worker := testWorker(id, "session-"+id)
			if err := registry.Register(worker); err != nil {
				errs <- err
				return
			}
			if err := registry.Heartbeat(testStatus(clock, worker, 1)); err != nil {
				errs <- err
			}
			_ = registry.Snapshot()
		}(i)
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if registry.Len() != count {
		t.Fatalf("registry length = %d, want %d", registry.Len(), count)
	}
}
