package admission

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/tasks"
)

func TestNewValidatesBounds(t *testing.T) {
	if _, err := New(0, 0); err == nil {
		t.Fatal("expected zero concurrency to be rejected")
	}
	if _, err := New(1, -1); err == nil {
		t.Fatal("expected negative queue depth to be rejected")
	}
	if _, err := New(1, 0); err != nil {
		t.Fatal(err)
	}
}

func TestTryAcquireEnforcesLimitAndReleaseIsIdempotent(t *testing.T) {
	controller, err := New(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	lease, rejection := controller.TryAcquire(testRequest(), testSnapshot())
	if rejection != nil || lease == nil {
		t.Fatalf("first admission failed: %#v", rejection)
	}
	if got, rejection := controller.TryAcquire(testRequest(), testSnapshot()); got != nil || rejection == nil || rejection.Reason != tasks.RejectAtCapacity {
		t.Fatalf("second admission = lease %#v rejection %#v", got, rejection)
	}
	lease.Release()
	lease.Release()
	if stats := controller.Stats(); stats.RunningTasks != 0 {
		t.Fatalf("running tasks = %d, want 0", stats.RunningTasks)
	}
	lease, rejection = controller.TryAcquire(testRequest(), testSnapshot())
	if rejection != nil || lease == nil {
		t.Fatalf("capacity was not restored: %#v", rejection)
	}
	lease.Release()
}

func TestAcquireUsesBoundedQueueAndPreservesCounters(t *testing.T) {
	controller, err := New(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	holder, rejection := controller.TryAcquire(testRequest(), testSnapshot())
	if rejection != nil {
		t.Fatal(rejection.Detail)
	}
	type outcome struct {
		lease     *Lease
		rejection *tasks.TaskRejection
		err       error
	}
	result := make(chan outcome, 1)
	go func() {
		lease, rejection, err := controller.Acquire(context.Background(), testRequest(), testSnapshot())
		result <- outcome{lease: lease, rejection: rejection, err: err}
	}()
	waitFor(t, func() bool { return controller.Stats().QueuedTasks == 1 })
	if lease, rejection, err := controller.Acquire(context.Background(), testRequest(), testSnapshot()); err != nil || lease != nil || rejection == nil || rejection.Reason != tasks.RejectQueueFull {
		t.Fatalf("queue-full admission = lease %#v rejection %#v err %v", lease, rejection, err)
	}
	holder.Release()
	got := <-result
	if got.err != nil || got.rejection != nil || got.lease == nil {
		t.Fatalf("queued admission failed: %#v", got)
	}
	got.lease.Release()
	stats := controller.Stats()
	if stats.RunningTasks != 0 || stats.QueuedTasks != 0 {
		t.Fatalf("leaked counters: %#v", stats)
	}
}

func TestAcquireCancellationAndDrainWakeQueuedCallers(t *testing.T) {
	controller, err := New(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	holder, rejection := controller.TryAcquire(testRequest(), testSnapshot())
	if rejection != nil {
		t.Fatal(rejection.Detail)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, err := controller.Acquire(ctx, testRequest(), testSnapshot())
		result <- err
	}()
	waitFor(t, func() bool { return controller.Stats().QueuedTasks == 1 })
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire error = %v, want context cancellation", err)
	}
	if stats := controller.Stats(); stats.QueuedTasks != 0 {
		t.Fatalf("queued tasks after cancel = %d", stats.QueuedTasks)
	}

	drainResult := make(chan *tasks.TaskRejection, 1)
	go func() {
		_, rejection, _ := controller.Acquire(context.Background(), testRequest(), testSnapshot())
		drainResult <- rejection
	}()
	waitFor(t, func() bool { return controller.Stats().QueuedTasks == 1 })
	controller.SetDraining(true)
	if rejection := <-drainResult; rejection == nil || rejection.Reason != tasks.RejectWorkerDraining {
		t.Fatalf("drain rejection = %#v", rejection)
	}
	holder.Release()
	if stats := controller.Stats(); stats.RunningTasks != 0 || stats.QueuedTasks != 0 || !stats.Draining {
		t.Fatalf("unexpected drain stats: %#v", stats)
	}
}

func TestAdmissionRevalidatesCapabilitiesTrustAndResources(t *testing.T) {
	controller, err := New(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	baseRequest := testRequest()
	baseRequest.Constraints.MinCPU = 2
	baseRequest.Constraints.MinRAMBytes = 8 << 30
	baseRequest.Constraints.GPURequired = true
	baseRequest.Constraints.GPUVendor = "nvidia"
	baseRequest.Constraints.MinGPUMemoryBytes = 12 << 30
	baseRequest.Constraints.RequiredGPUCapabilities = []string{"cuda"}
	baseRequest.Constraints.RequiredRuntime = "cuda-12"
	baseRequest.Constraints.AllowedWorkerIDs = []string{"worker-1"}
	baseRequest.Constraints.AllowedGroups = []string{"group-1"}
	baseRequest.Constraints.RequiredTrustLevel = "trusted-lan"
	baseSnapshot := testSnapshot()
	cpu := 8.0
	ram := uint64(64 << 30)
	gpuMemory := uint64(24 << 30)
	baseSnapshot.AvailableCPU = &cpu
	baseSnapshot.AvailableRAMBytes = &ram
	baseSnapshot.GPUs = []GPUSnapshot{{Vendor: "NVIDIA", AvailableMemoryBytes: &gpuMemory, Capabilities: []string{"cuda"}}}
	baseSnapshot.Runtimes = []string{"cuda-12"}
	baseSnapshot.GroupIDs = []string{"group-1"}
	lease, rejection := controller.TryAcquire(baseRequest, baseSnapshot)
	if rejection != nil || lease == nil {
		t.Fatalf("valid resource request rejected: %#v", rejection)
	}
	lease.Release()

	tests := []struct {
		name   string
		reason tasks.RejectionReason
		mutate func(*AdmissionSnapshot)
	}{
		{name: "capability", reason: tasks.RejectCapabilityMissing, mutate: func(snapshot *AdmissionSnapshot) { snapshot.Capabilities = nil }},
		{name: "model", reason: tasks.RejectModelUnavailable, mutate: func(snapshot *AdmissionSnapshot) { snapshot.AvailableModels = nil; snapshot.CanLoadModels = false }},
		{name: "trust", reason: tasks.RejectInsufficientTrust, mutate: func(snapshot *AdmissionSnapshot) { snapshot.TrustLevel = "untrusted" }},
		{name: "worker allowlist", reason: tasks.RejectNotAllowed, mutate: func(snapshot *AdmissionSnapshot) { snapshot.WorkerID = "other" }},
		{name: "cpu unknown", reason: tasks.RejectInsufficientCPU, mutate: func(snapshot *AdmissionSnapshot) { snapshot.AvailableCPU = nil }},
		{name: "ram low", reason: tasks.RejectInsufficientRAM, mutate: func(snapshot *AdmissionSnapshot) { low := uint64(1); snapshot.AvailableRAMBytes = &low }},
		{name: "gpu memory", reason: tasks.RejectInsufficientGPUMemory, mutate: func(snapshot *AdmissionSnapshot) { low := uint64(1); snapshot.GPUs[0].AvailableMemoryBytes = &low }},
		{name: "runtime", reason: tasks.RejectCapabilityMissing, mutate: func(snapshot *AdmissionSnapshot) { snapshot.Runtimes = nil }},
		{name: "stale", reason: tasks.RejectStaleAssignment, mutate: func(snapshot *AdmissionSnapshot) { snapshot.Stale = true }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := baseSnapshot
			snapshot.GPUs = append([]GPUSnapshot(nil), baseSnapshot.GPUs...)
			tt.mutate(&snapshot)
			lease, rejection := controller.TryAcquire(baseRequest, snapshot)
			if lease != nil || rejection == nil || rejection.Reason != tt.reason {
				t.Fatalf("admission = lease %#v rejection %#v, want %s", lease, rejection, tt.reason)
			}
		})
	}
}

func TestValidateDetectsResourceChangeAfterPermitAcquisition(t *testing.T) {
	controller, err := New(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest()
	request.Constraints.MinRAMBytes = 1024
	available := uint64(2048)
	snapshot := testSnapshot()
	snapshot.AvailableRAMBytes = &available
	lease, rejection, err := controller.Acquire(context.Background(), request, snapshot)
	if err != nil || rejection != nil || lease == nil {
		t.Fatalf("initial admission = lease %#v rejection %#v err %v", lease, rejection, err)
	}
	defer lease.Release()

	available = 512
	snapshot.AvailableRAMBytes = &available
	if rejection := controller.Validate(request, snapshot); rejection == nil || rejection.Reason != tasks.RejectInsufficientRAM {
		t.Fatalf("fresh validation = %#v, want %s", rejection, tasks.RejectInsufficientRAM)
	}
}

func TestRunReleasesOnErrorAndPanic(t *testing.T) {
	controller, err := New(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("task failed")
	if rejection, err := controller.Run(context.Background(), testRequest(), testSnapshot(), func(context.Context) error { return wantErr }); rejection != nil || !errors.Is(err, wantErr) {
		t.Fatalf("Run = rejection %#v error %v", rejection, err)
	}
	if stats := controller.Stats(); stats.RunningTasks != 0 {
		t.Fatalf("permit leaked after error: %#v", stats)
	}
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		_, _ = controller.Run(context.Background(), testRequest(), testSnapshot(), func(context.Context) error { panic("boom") })
	}()
	if !panicked {
		t.Fatal("Run swallowed panic")
	}
	if stats := controller.Stats(); stats.RunningTasks != 0 {
		t.Fatalf("permit leaked after panic: %#v", stats)
	}
}

func TestConcurrentRunNeverExceedsLimit(t *testing.T) {
	controller, err := New(3, 64)
	if err != nil {
		t.Fatal(err)
	}
	var active atomic.Int64
	var maximum atomic.Int64
	var wg sync.WaitGroup
	errorsSeen := make(chan error, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rejection, err := controller.Run(context.Background(), testRequest(), testSnapshot(), func(context.Context) error {
				current := active.Add(1)
				for {
					previous := maximum.Load()
					if current <= previous || maximum.CompareAndSwap(previous, current) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				active.Add(-1)
				return nil
			})
			if err != nil {
				errorsSeen <- err
			} else if rejection != nil {
				errorsSeen <- errors.New(rejection.Detail)
			}
		}()
	}
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatal(err)
	}
	if got := maximum.Load(); got > 3 {
		t.Fatalf("maximum concurrent runs = %d, want <= 3", got)
	}
	if stats := controller.Stats(); stats.RunningTasks != 0 || stats.QueuedTasks != 0 {
		t.Fatalf("leaked counters after concurrent run: %#v", stats)
	}
}

func testRequest() tasks.Request {
	return tasks.Request{
		TaskID:     "task-1",
		Kind:       tasks.KindPrompt,
		Task:       tasks.PromptTask{Prompt: "hello", Model: "model-a"},
		TrustLevel: "trusted-lan",
		Constraints: tasks.Constraints{
			RequiredCapabilities: []string{"llm"},
		},
	}
}

func testSnapshot() AdmissionSnapshot {
	accepting, allowed := true, true
	return AdmissionSnapshot{
		WorkerID:        "worker-1",
		State:           StateAvailable,
		Accepting:       &accepting,
		Allowed:         &allowed,
		Capabilities:    []string{"llm"},
		AvailableModels: []string{"model-a"},
		ModelsKnown:     true,
		TrustLevel:      "trusted-lan",
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for admission state")
		}
		time.Sleep(time.Millisecond)
	}
}
