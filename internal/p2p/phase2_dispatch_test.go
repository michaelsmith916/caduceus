package p2p

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/registry"
	"github.com/caduceus/caduceus/internal/scheduler"
	"github.com/caduceus/caduceus/internal/store"
	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/caduceus/caduceus/internal/workers"
)

func TestFenceResultRequiresActiveTokenAndTerminalStatus(t *testing.T) {
	attempt, err := tasks.NewAttempt("task-1", "worker-1", "session-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	result := tasks.Result{TaskID: attempt.TaskID, Status: tasks.StatusCompleted}
	if err := fenceResult(attempt, &result); err != nil {
		t.Fatal(err)
	}
	if result.AttemptID != attempt.AttemptID || result.AttemptToken != attempt.AttemptToken {
		t.Fatal("fenceResult did not stamp the active attempt identity")
	}

	for name, mutate := range map[string]func(*tasks.Result){
		"task":    func(result *tasks.Result) { result.TaskID = "task-2" },
		"attempt": func(result *tasks.Result) { result.AttemptID = "attempt-other" },
		"token":   func(result *tasks.Result) { result.AttemptToken = "token-other" },
		"status":  func(result *tasks.Result) { result.Status = tasks.StatusRunning },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := tasks.Result{TaskID: attempt.TaskID, Status: tasks.StatusCompleted}
			mutate(&candidate)
			if err := fenceResult(attempt, &candidate); err == nil {
				t.Fatal("expected result fencing failure")
			}
		})
	}
}

func TestAttemptHistoryCountersSupportRestartRecovery(t *testing.T) {
	attempts := []tasks.TaskAttempt{
		{AttemptNumber: 2, State: tasks.AttemptStateInterrupted},
		{AttemptNumber: 4, State: tasks.AttemptStateRejected},
		{AttemptNumber: 3, State: tasks.AttemptStateFailed},
	}
	if got := maxAttemptNumber(attempts); got != 4 {
		t.Fatalf("maxAttemptNumber() = %d, want 4", got)
	}
	if got := countExecutionAttempts(attempts); got != 2 {
		t.Fatalf("countExecutionAttempts() = %d, want 2", got)
	}
}

func TestInboundAttemptConflictPolicy(t *testing.T) {
	request := tasks.Request{
		TaskID:      "task-1",
		Kind:        tasks.KindPrompt,
		Task:        tasks.PromptTask{Prompt: "hello"},
		Idempotent:  true,
		MaxAttempts: 2,
	}
	first, err := tasks.NewAttempt(request.TaskID, "worker-1", "session-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	first.State = tasks.AttemptStateFailed
	second, err := tasks.NewAttempt(request.TaskID, "worker-1", "session-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	meta := tasks.Metadata{
		TaskID:        request.TaskID,
		Status:        tasks.StatusFailed,
		RequesterPeer: "peer-a",
		Request:       request,
		Attempts:      []tasks.TaskAttempt{first},
	}
	assignment := TaskAssignment{Request: request, Attempt: second}
	if inboundAttemptConflicts(meta, "peer-a", assignment) {
		t.Fatal("idempotent retry within its execution budget should be accepted")
	}

	changed := meta
	changed.RequesterPeer = "peer-b"
	if !inboundAttemptConflicts(changed, "peer-a", assignment) {
		t.Fatal("a different requester must not reuse a task id")
	}
	changed = meta
	changed.Status = tasks.StatusCompleted
	if !inboundAttemptConflicts(changed, "peer-a", assignment) {
		t.Fatal("a completed task must not be replayed")
	}
	changed = meta
	changed.Request.Idempotent = false
	if !inboundAttemptConflicts(changed, "peer-a", TaskAssignment{Request: changed.Request, Attempt: second}) {
		t.Fatal("non-idempotent work must not be replayed after an execution")
	}
	duplicate := assignment
	duplicate.Attempt = first
	if !inboundAttemptConflicts(meta, "peer-a", duplicate) {
		t.Fatal("a duplicate attempt id/token must be fenced")
	}
	changedRequest := request
	changedRequest.Task.Prompt = "different payload"
	if !inboundAttemptConflicts(meta, "peer-a", TaskAssignment{Request: changedRequest, Attempt: second}) {
		t.Fatal("a retry must not change the task payload")
	}
}

func TestEventImpliesAcceptance(t *testing.T) {
	for _, eventType := range []string{tasks.EventAccepted, tasks.EventStarted, tasks.EventToken, tasks.EventCompleted, tasks.EventFailed} {
		if !eventImpliesAcceptance(eventType) {
			t.Fatalf("eventImpliesAcceptance(%q) = false", eventType)
		}
	}
	if eventImpliesAcceptance(tasks.EventQueued) {
		t.Fatal("queued event must not imply worker acceptance")
	}
}

func TestPhase2RuntimeCancelTaskCancelsTrackedAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &phase2Runtime{active: map[string]activeAttempt{
		"task-1": {AttemptID: "attempt-1", WorkerID: "worker-1", Cancel: cancel},
	}}
	if !runtime.cancelTask("task-1") {
		t.Fatal("tracked attempt was not canceled")
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("tracked attempt context was not canceled")
	}
	if runtime.cancelTask("missing") {
		t.Fatal("missing attempt reported canceled")
	}
}

func TestPhase2RuntimeCancelTaskCancelsDispatchWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &phase2Runtime{
		dispatchCancels: map[string]context.CancelFunc{"task-1": cancel},
		active:          map[string]activeAttempt{},
	}
	if !runtime.cancelTask("task-1") {
		t.Fatal("tracked dispatch was not canceled")
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("tracked dispatch context was not canceled")
	}
	if runtime.cancelTask("missing") {
		t.Fatal("missing dispatch reported canceled")
	}
}

func TestCancelTaskInterruptsCoolingDispatchBeforeAttempt(t *testing.T) {
	taskStore := store.New(t.TempDir())
	if err := taskStore.Init(); err != nil {
		t.Fatal(err)
	}
	req := tasks.Request{
		TaskID:     "cooldown-cancel",
		Kind:       tasks.KindPrompt,
		TrustLevel: "trusted-lan",
		Task:       tasks.PromptTask{Prompt: "test", Model: "model-a"},
		Constraints: tasks.Constraints{
			RequiredCapabilities: []string{"llm"},
		},
	}
	if err := taskStore.SaveTask(tasks.Metadata{
		TaskID: req.TaskID, Kind: req.Kind, Status: tasks.StatusQueued, Request: req,
	}); err != nil {
		t.Fatal(err)
	}

	workerRegistry, err := registry.New(registry.Options{})
	if err != nil {
		t.Fatal(err)
	}
	accepting := true
	running := 0
	maximum := 1
	queueDepth := 0
	maxQueue := 1
	cost := 1.0
	worker := workers.Worker{
		WorkerID:           "worker-a",
		PeerID:             "peer-worker-a",
		SessionID:          "session-worker-a",
		Allowed:            true,
		TrustLevel:         "trusted-lan",
		State:              workers.StateAvailable,
		AcceptingWork:      &accepting,
		RunningTasks:       &running,
		MaxConcurrentTasks: &maximum,
		QueueDepth:         &queueDepth,
		MaxQueueDepth:      &maxQueue,
		LoadedModels:       []string{"model-a"},
		CostWeight:         &cost,
		Capabilities: workers.Capabilities{
			LLM: true, Models: []string{"model-a"}, MaxConcurrentTasks: 1,
		},
	}
	if err := workerRegistry.Register(worker); err != nil {
		t.Fatal(err)
	}
	router, err := scheduler.New(scheduler.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	waiting := make(chan struct{})
	runtime := &phase2Runtime{
		registry:        workerRegistry,
		scheduler:       router,
		assignments:     scheduler.AssignmentHistory{},
		cooldowns:       map[string]time.Time{worker.WorkerID: time.Now().Add(time.Hour)},
		now:             time.Now,
		dispatchCancels: make(map[string]context.CancelFunc),
		active:          make(map[string]activeAttempt),
		waitCooldown: func(ctx context.Context, _ time.Duration) error {
			close(waiting)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	node := &Node{store: taskStore, phase2: runtime, cancels: make(map[string]context.CancelFunc)}
	done := make(chan error, 1)
	go func() {
		_, dispatchErr := node.dispatchPrepared(context.Background(), req)
		done <- dispatchErr
	}()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not enter cooldown wait")
	}
	if err := node.CancelTask(context.Background(), req.TaskID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("dispatch error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("task cancellation did not interrupt cooldown wait")
	}
	meta, err := taskStore.GetTask(req.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != tasks.StatusCanceled || len(meta.Attempts) != 0 {
		t.Fatalf("canceled cooling task created an attempt: %#v", meta)
	}
}

func TestInboundAttemptCancellationIsRequesterAndTokenFenced(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempt := tasks.TaskAttempt{
		TaskID:       "task-1",
		AttemptID:    "attempt-1",
		AttemptToken: "0123456789abcdef0123456789abcdef",
	}
	runtime := &phase2Runtime{inbound: map[string]inboundAttemptClaim{
		inboundAttemptKey(attempt): {Requester: "peer-a", Attempt: attempt, Cancel: cancel},
	}}
	node := &Node{phase2: runtime}
	payload := CancelPayload{TaskID: attempt.TaskID, AttemptID: attempt.AttemptID, AttemptToken: attempt.AttemptToken}
	if node.cancelInboundAttempt("peer-b", payload) {
		t.Fatal("different requester canceled an inbound attempt")
	}
	select {
	case <-ctx.Done():
		t.Fatal("different requester canceled the context")
	default:
	}
	payload.AttemptToken = "fedcba9876543210fedcba9876543210"
	if node.cancelInboundAttempt("peer-a", payload) {
		t.Fatal("wrong token canceled an inbound attempt")
	}
	payload.AttemptToken = attempt.AttemptToken
	if !node.cancelInboundAttempt("peer-a", payload) {
		t.Fatal("valid requester and token did not cancel inbound attempt")
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("inbound attempt context was not canceled")
	}
}

func TestDurableDispatchTurnKeepsFIFOQueuedUntilPermit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	taskStore := store.New(t.TempDir())
	if err := taskStore.Init(); err != nil {
		t.Fatal(err)
	}
	for _, taskID := range []string{"fifo-1", "fifo-2"} {
		request := tasks.Request{TaskID: taskID, Kind: tasks.KindPrompt, Task: tasks.PromptTask{Prompt: "test"}}
		if err := taskStore.SaveTask(tasks.Metadata{TaskID: taskID, Kind: request.Kind, Status: tasks.StatusQueued, Request: request}); err != nil {
			t.Fatal(err)
		}
		if _, err := taskStore.EnqueueBounded(taskID, 2, "requester"); err != nil {
			t.Fatal(err)
		}
	}
	runtime := &phase2Runtime{
		ctx:             ctx,
		queueChanged:    make(chan struct{}),
		dispatchPermits: make(chan struct{}, 1),
	}
	node := &Node{store: taskStore, phase2: runtime}
	if err := node.awaitDispatchTurn(ctx, "fifo-1"); err != nil {
		t.Fatal(err)
	}

	second := make(chan error, 1)
	go func() { second <- node.awaitDispatchTurn(ctx, "fifo-2") }()
	select {
	case err := <-second:
		t.Fatalf("second task bypassed the in-flight bound: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if position, queued, err := taskStore.QueuePosition("fifo-2"); err != nil || !queued || position != 1 {
		t.Fatalf("second task position = %d queued=%t err=%v", position, queued, err)
	}

	runtime.releaseDispatchPermit()
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second task did not claim the released dispatch permit")
	}
	runtime.releaseDispatchPermit()
	queue, err := taskStore.ListQueue()
	if err != nil {
		t.Fatal(err)
	}
	if len(queue) != 0 {
		t.Fatalf("queue after ordered claims = %#v", queue)
	}
}

func TestDurableDispatchWaitCancellationRemovesQueuedTask(t *testing.T) {
	runtimeCtx, stopRuntime := context.WithCancel(context.Background())
	defer stopRuntime()
	taskStore := store.New(t.TempDir())
	if err := taskStore.Init(); err != nil {
		t.Fatal(err)
	}
	request := tasks.Request{TaskID: "cancel-wait", Kind: tasks.KindPrompt, Task: tasks.PromptTask{Prompt: "test"}}
	if err := taskStore.SaveTask(tasks.Metadata{TaskID: request.TaskID, Kind: request.Kind, Status: tasks.StatusQueued, Request: request}); err != nil {
		t.Fatal(err)
	}
	if _, err := taskStore.EnqueueBounded(request.TaskID, 1, "requester"); err != nil {
		t.Fatal(err)
	}
	runtime := &phase2Runtime{
		ctx:             runtimeCtx,
		queueChanged:    make(chan struct{}),
		dispatchPermits: make(chan struct{}, 1),
	}
	runtime.dispatchPermits <- struct{}{}
	node := &Node{store: taskStore, phase2: runtime}
	waitCtx, cancelWait := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- node.awaitDispatchTurn(waitCtx, request.TaskID) }()
	cancelWait()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled dispatch wait did not return")
	}
	meta, err := taskStore.GetTask(request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != tasks.StatusCanceled || meta.QueuePosition != 0 {
		t.Fatalf("canceled queued metadata = %#v", meta)
	}
	queue, err := taskStore.ListQueue()
	if err != nil {
		t.Fatal(err)
	}
	if len(queue) != 0 {
		t.Fatalf("canceled task remained queued: %#v", queue)
	}
	runtime.releaseDispatchPermit()
}

func TestCancelTaskPersistsIntentBeforeSignalingExecution(t *testing.T) {
	taskStore := store.New(t.TempDir())
	if err := taskStore.Init(); err != nil {
		t.Fatal(err)
	}
	attempt, err := tasks.NewAttempt("cancel-running", "local-worker", "session-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	attempt.State = tasks.AttemptStateRunning
	request := tasks.Request{TaskID: attempt.TaskID, Kind: tasks.KindPrompt, Task: tasks.PromptTask{Prompt: "test"}}
	if err := taskStore.SaveTask(tasks.Metadata{
		TaskID: attempt.TaskID, Kind: request.Kind, Status: tasks.StatusRunning,
		Request: request, CurrentAttempt: 1, TotalAttempts: 1, Attempts: []tasks.TaskAttempt{attempt},
	}); err != nil {
		t.Fatal(err)
	}
	attemptCtx, cancelAttempt := context.WithCancel(context.Background())
	runtime := &phase2Runtime{
		ctx:          context.Background(),
		queueChanged: make(chan struct{}),
		active: map[string]activeAttempt{
			attempt.TaskID: {AttemptID: attempt.AttemptID, WorkerID: attempt.WorkerID, Cancel: cancelAttempt},
		},
	}
	node := &Node{store: taskStore, phase2: runtime, cancels: map[string]context.CancelFunc{}}
	if err := node.CancelTask(context.Background(), attempt.TaskID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-attemptCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("active attempt was not canceled")
	}
	meta, err := taskStore.GetTask(attempt.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != tasks.StatusCanceled || len(meta.Attempts) != 1 || meta.Attempts[0].State != tasks.AttemptStateCanceled {
		t.Fatalf("cancellation intent was not durably fenced: %#v", meta)
	}
}

func TestFinishAttemptCannotOverwriteCancellation(t *testing.T) {
	taskStore := store.New(t.TempDir())
	if err := taskStore.Init(); err != nil {
		t.Fatal(err)
	}
	attempt, err := tasks.NewAttempt("fenced-cancel", "worker-1", "session-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	attempt.State = tasks.AttemptStateCanceled
	request := tasks.Request{TaskID: attempt.TaskID, Kind: tasks.KindPrompt, Task: tasks.PromptTask{Prompt: "test"}}
	if err := taskStore.SaveTask(tasks.Metadata{
		TaskID: attempt.TaskID, Kind: request.Kind, Status: tasks.StatusCanceled,
		Request: request, CurrentAttempt: 1, TotalAttempts: 1, Attempts: []tasks.TaskAttempt{attempt},
	}); err != nil {
		t.Fatal(err)
	}
	node := &Node{store: taskStore}
	if err := node.finishAttempt(attempt, tasks.AttemptStateCompleted, "", ""); !errors.Is(err, errAttemptFenced) {
		t.Fatalf("finish error = %v, want attempt fenced", err)
	}
	meta, err := taskStore.GetTask(attempt.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != tasks.StatusCanceled || meta.Attempts[0].State != tasks.AttemptStateCanceled {
		t.Fatalf("late completion overwrote cancellation: %#v", meta)
	}
}

func TestValidateAttemptCommitRejectsReplacedWorkerSession(t *testing.T) {
	workerRegistry, err := registry.New(registry.Options{SuspectAfter: time.Minute, EvictAfter: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	old := workers.Worker{WorkerID: "worker-1", PeerID: "peer-1", SessionID: "session-old", State: workers.StateAvailable, Allowed: true}
	if err := workerRegistry.Register(old); err != nil {
		t.Fatal(err)
	}
	attempt, err := tasks.NewAttempt("session-fence", old.WorkerID, old.SessionID, 1)
	if err != nil {
		t.Fatal(err)
	}
	attempt.State = tasks.AttemptStateRunning
	taskStore := store.New(t.TempDir())
	if err := taskStore.Init(); err != nil {
		t.Fatal(err)
	}
	request := tasks.Request{TaskID: attempt.TaskID, Kind: tasks.KindPrompt, Task: tasks.PromptTask{Prompt: "test"}}
	if err := taskStore.SaveTask(tasks.Metadata{
		TaskID: attempt.TaskID, Kind: request.Kind, Status: tasks.StatusRunning,
		Request: request, CurrentAttempt: 1, TotalAttempts: 1, Attempts: []tasks.TaskAttempt{attempt},
	}); err != nil {
		t.Fatal(err)
	}
	replacement := old
	replacement.SessionID = "session-new"
	if err := workerRegistry.Register(replacement); err != nil {
		t.Fatal(err)
	}
	node := &Node{store: taskStore, phase2: &phase2Runtime{registry: workerRegistry}}
	if err := node.validateAttemptCommit(attempt); !errors.Is(err, errAttemptFenced) {
		t.Fatalf("commit validation error = %v, want attempt fenced", err)
	}
}

func TestApplyWorkerCooldownsVisibleAndExpires(t *testing.T) {
	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	newDecision := func() scheduler.RoutingDecision {
		return scheduler.RoutingDecision{
			SelectedWorkerID: "worker-a",
			Candidates: []scheduler.CandidateScore{
				{WorkerID: "worker-a", Eligible: true, Rank: 1, Selected: true, TotalScore: 10},
				{WorkerID: "worker-b", Eligible: true, Rank: 2, TotalScore: 5},
			},
		}
	}

	decision := newDecision()
	applyWorkerCooldowns(&decision, map[string]time.Time{"worker-a": now.Add(10 * time.Second)}, now)
	if decision.SelectedWorkerID != "worker-b" || !decision.Candidates[1].Selected {
		t.Fatalf("cooldown did not rerank selection: %#v", decision)
	}
	if decision.Candidates[0].Eligible || !candidateHasFilter(decision.Candidates[0], retryCooldownFilterReason) {
		t.Fatalf("cooling worker was not visibly filtered: %#v", decision.Candidates[0])
	}

	expired := newDecision()
	applyWorkerCooldowns(&expired, map[string]time.Time{"worker-a": now}, now)
	if expired.SelectedWorkerID != "worker-a" || !expired.Candidates[0].Eligible {
		t.Fatalf("expired cooldown still affected routing: %#v", expired)
	}
}

func TestWorkerCooldownIsBoundedAndKeepsLongerDeadline(t *testing.T) {
	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	runtime := &phase2Runtime{now: func() time.Time { return now }, cooldowns: make(map[string]time.Time)}
	for index := 0; index < maxWorkerCooldownEntries; index++ {
		workerID := fmt.Sprintf("worker-%04d", index)
		runtime.cooldowns[workerID] = now.Add(time.Hour + time.Duration(index)*time.Nanosecond)
	}
	runtime.setWorkerCooldown("worker-new", maxWorkerRetryCooldown+time.Hour)
	if len(runtime.cooldowns) != maxWorkerCooldownEntries {
		t.Fatalf("cooldown map size = %d, want %d", len(runtime.cooldowns), maxWorkerCooldownEntries)
	}
	if _, exists := runtime.cooldowns["worker-0000"]; exists {
		t.Fatal("earliest cooldown was not evicted")
	}
	if got := runtime.cooldowns["worker-new"]; !got.Equal(now.Add(maxWorkerRetryCooldown)) {
		t.Fatalf("capped deadline = %v, want %v", got, now.Add(maxWorkerRetryCooldown))
	}

	keep := &phase2Runtime{now: func() time.Time { return now }, cooldowns: make(map[string]time.Time)}
	keep.setWorkerCooldown("worker-a", time.Hour)
	keep.setWorkerCooldown("worker-a", time.Second)
	if got := keep.cooldowns["worker-a"]; !got.Equal(now.Add(time.Hour)) {
		t.Fatalf("shorter retry_after replaced longer deadline: %v", got)
	}
}

func TestNextCooldownDelayOnlyWaitsForUntriedCoolingWorkers(t *testing.T) {
	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	runtime := &phase2Runtime{
		now: func() time.Time { return now },
		cooldowns: map[string]time.Time{
			"worker-a": now.Add(10 * time.Second),
			"worker-b": now.Add(5 * time.Second),
		},
	}
	decision := scheduler.RoutingDecision{Candidates: []scheduler.CandidateScore{
		{WorkerID: "worker-a", FilterReasons: []string{retryCooldownFilterReason}},
		{WorkerID: "worker-b", FilterReasons: []string{retryCooldownFilterReason}},
	}}
	delay, found := runtime.nextCooldownDelay(decision, map[string]struct{}{"worker-b": {}})
	if !found || delay != 10*time.Second {
		t.Fatalf("delay = %v, found = %v; want 10s, true", delay, found)
	}
	if _, found := runtime.nextCooldownDelay(decision, map[string]struct{}{"worker-a": {}, "worker-b": {}}); found {
		t.Fatal("tried cooling workers should not cause another wait")
	}
}

func TestWaitForWorkerCooldownHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if err := waitForWorkerCooldown(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want context canceled", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("canceled cooldown wait took %v", elapsed)
	}
}

func TestApplyTriedWorkersRecomputesSelection(t *testing.T) {
	decision := scheduler.RoutingDecision{
		SelectedWorkerID: "worker-a",
		Candidates: []scheduler.CandidateScore{
			{WorkerID: "worker-a", Eligible: true, Rank: 1, Selected: true, TotalScore: 10},
			{WorkerID: "worker-b", Eligible: true, Rank: 2, TotalScore: 5},
		},
	}
	applyTriedWorkers(&decision, map[string]struct{}{"worker-a": {}})
	if decision.SelectedWorkerID != "worker-b" || !decision.Candidates[1].Selected || decision.Candidates[1].Rank != 1 {
		t.Fatalf("tried-worker rerank failed: %#v", decision)
	}
	if decision.Candidates[0].Eligible || !candidateHasFilter(decision.Candidates[0], triedWorkerFilterReason) {
		t.Fatalf("tried worker was not visibly filtered: %#v", decision.Candidates[0])
	}
	if !strings.Contains(decision.Explanation, "Selected worker-b") || strings.Contains(decision.Explanation, "Selected worker-a") {
		t.Fatalf("reranked explanation is inconsistent: %q", decision.Explanation)
	}
}
