package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/tasks"
)

func TestTaskStoreRoundTrip(t *testing.T) {
	s := New(t.TempDir())
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	meta := taskMetadata("task-1", tasks.StatusAccepted, false, 0)
	if err := s.SaveTask(meta); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvent(tasks.NewEvent("task-1", 0, tasks.EventStarted, "started", "")); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveResult(tasks.Result{TaskID: "task-1", AttemptID: "attempt-1", AttemptToken: "token-1", Status: tasks.StatusCompleted, OutputText: "hi"}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetTask("task-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.TaskID != "task-1" || got.MaxAttempts != 1 || got.Idempotent {
		t.Fatalf("unexpected task metadata %#v", got)
	}
	events, err := s.EventsSince("task-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Cursor != 1 {
		t.Fatalf("unexpected events %#v", events)
	}
	result, err := s.GetResult("task-1")
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputText != "hi" || result.AttemptID != "attempt-1" || result.AttemptToken != "token-1" {
		t.Fatalf("unexpected result %#v", result)
	}
	version, err := s.StateVersion()
	if err != nil {
		t.Fatal(err)
	}
	if version != CurrentStateVersion {
		t.Fatalf("state version = %d, want %d", version, CurrentStateVersion)
	}
}

func TestQueueFIFOPositionCancellationAndRestore(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	for _, taskID := range []string{"task-1", "task-2", "task-3"} {
		if err := s.SaveTask(taskMetadata(taskID, tasks.StatusAccepted, false, 0)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Enqueue(taskID, "requester"); err != nil {
			t.Fatal(err)
		}
	}
	queue, err := s.ListQueue()
	if err != nil {
		t.Fatal(err)
	}
	assertQueue(t, queue, "task-1", "task-2", "task-3")
	for index, entry := range queue {
		if entry.Position != index+1 || entry.Owner != "requester" {
			t.Fatalf("queue entry %d = %#v", index, entry)
		}
		if index > 0 && entry.Sequence <= queue[index-1].Sequence {
			t.Fatal("queue sequences are not strictly increasing")
		}
	}
	meta, err := s.GetTask("task-2")
	if err != nil {
		t.Fatal(err)
	}
	if meta.QueuePosition != 2 || !meta.QueuePositionExact {
		t.Fatalf("task queue status = %#v", meta)
	}
	entry, ok, err := s.Dequeue()
	if err != nil || !ok || entry.TaskID != "task-1" {
		t.Fatalf("dequeue = %#v %v %v", entry, ok, err)
	}
	if canceled, err := s.CancelQueued("task-2"); err != nil || !canceled {
		t.Fatalf("cancel queued = %v %v", canceled, err)
	}
	queue, err = s.ListQueue()
	if err != nil {
		t.Fatal(err)
	}
	assertQueue(t, queue, "task-3")

	restarted := New(root)
	if err := restarted.Init(); err != nil {
		t.Fatal(err)
	}
	restored, err := restarted.RestoreQueue()
	if err != nil {
		t.Fatal(err)
	}
	assertQueue(t, restored, "task-3")
	canceled, err := restarted.GetTask("task-2")
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != tasks.StatusCanceled || canceled.QueuePosition != 0 {
		t.Fatalf("canceled metadata = %#v", canceled)
	}
	dequeued, err := restarted.GetTask("task-1")
	if err != nil {
		t.Fatal(err)
	}
	if dequeued.Status != tasks.StatusInterrupted {
		t.Fatalf("accepted task after coordinator restart = %q, want interrupted", dequeued.Status)
	}
}

func TestCancelQueuedCrashAfterMetadataCommitDoesNotResurrectTask(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveTask(taskMetadata("cancel-crash", tasks.StatusAccepted, false, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue("cancel-crash"); err != nil {
		t.Fatal(err)
	}

	injected := errors.New("injected state write failure")
	var writes []string
	s.beforeWrite = func(path string) error {
		writes = append(writes, filepath.Base(path))
		if filepath.Base(path) == rootStateFile {
			return injected
		}
		return nil
	}
	canceled, err := s.CancelQueued("cancel-crash")
	if !canceled || !errors.Is(err, injected) {
		t.Fatalf("cancel with injected crash = %t, %v", canceled, err)
	}
	if len(writes) != 2 || writes[0] != "task.json" || writes[1] != rootStateFile {
		t.Fatalf("cancellation persistence order = %v", writes)
	}

	// The crafted on-disk crash state retains the old queue entry while the
	// canceled metadata is already durable.
	var crashedState rootState
	if err := readJSON(filepath.Join(root, rootStateFile), &crashedState); err != nil {
		t.Fatal(err)
	}
	if len(crashedState.Queue) != 1 || crashedState.Queue[0].TaskID != "cancel-crash" {
		t.Fatalf("injected crash state queue = %#v", crashedState.Queue)
	}
	var crashedMeta tasks.Metadata
	if err := readJSON(filepath.Join(root, "tasks", "cancel-crash", "task.json"), &crashedMeta); err != nil {
		t.Fatal(err)
	}
	if crashedMeta.Status != tasks.StatusCanceled {
		t.Fatalf("injected crash metadata status = %q", crashedMeta.Status)
	}

	restarted := New(root)
	if err := restarted.Init(); err != nil {
		t.Fatal(err)
	}
	queue, err := restarted.RestoreQueue()
	if err != nil {
		t.Fatal(err)
	}
	if len(queue) != 0 {
		t.Fatalf("canceled task resurrected in queue: %#v", queue)
	}
	got, err := restarted.GetTask("cancel-crash")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != tasks.StatusCanceled || got.QueuePosition != 0 {
		t.Fatalf("recovered canceled task = %#v", got)
	}
}

func TestRecoveryReconcilesPersistedTerminalResultBeforeInterrupting(t *testing.T) {
	tests := []struct {
		name         string
		idempotent   bool
		maxAttempts  int
		resultStatus string
		attemptState string
		resultError  any
	}{
		{name: "non-idempotent completion", resultStatus: tasks.StatusCompleted, attemptState: tasks.AttemptStateCompleted},
		{name: "idempotent failure", idempotent: true, maxAttempts: 2, resultStatus: tasks.StatusFailed, attemptState: tasks.AttemptStateFailed, resultError: "model failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			s := New(root)
			if err := s.Init(); err != nil {
				t.Fatal(err)
			}
			meta := runningMetadata(t, "result-crash", tt.idempotent, tt.maxAttempts)
			if err := s.SaveTask(meta); err != nil {
				t.Fatal(err)
			}
			attempt := meta.Attempts[0]
			result := tasks.Result{
				TaskID:       meta.TaskID,
				AttemptID:    attempt.AttemptID,
				AttemptToken: attempt.AttemptToken,
				Status:       tt.resultStatus,
				Error:        tt.resultError,
				Artifacts:    []tasks.Artifact{},
			}
			if err := s.SaveResult(result); err != nil {
				t.Fatal(err)
			}

			restarted := New(root)
			if err := restarted.Init(); err != nil {
				t.Fatal(err)
			}
			got, err := restarted.GetTask(meta.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != tt.resultStatus || got.Attempts[0].State != tt.attemptState {
				t.Fatalf("terminal result recovery = %#v", got)
			}
			if got.Attempts[0].Interruption != "" || got.InterruptionReason != "" || got.FinishedAt == nil || got.Attempts[0].FinishedAt == nil {
				t.Fatalf("terminal result was marked interrupted or unfinished: %#v", got)
			}
			queue, err := restarted.RestoreQueue()
			if err != nil {
				t.Fatal(err)
			}
			if len(queue) != 0 {
				t.Fatalf("terminal task was requeued: %#v", queue)
			}
		})
	}
}

func TestRecoveryRejectsUnfencedStaleOrNonterminalResult(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*tasks.Metadata, *tasks.Result)
	}{
		{name: "task id", mutate: func(_ *tasks.Metadata, result *tasks.Result) { result.TaskID = "other-task" }},
		{name: "attempt id", mutate: func(_ *tasks.Metadata, result *tasks.Result) { result.AttemptID = "stale-attempt" }},
		{name: "attempt token", mutate: func(_ *tasks.Metadata, result *tasks.Result) { result.AttemptToken = "stale-token" }},
		{name: "current attempt", mutate: func(meta *tasks.Metadata, _ *tasks.Result) { meta.CurrentAttempt = 2; meta.TotalAttempts = 2 }},
		{name: "nonterminal status", mutate: func(_ *tasks.Metadata, result *tasks.Result) { result.Status = tasks.StatusRunning }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			taskDir := filepath.Join(root, "tasks", "untrusted-result")
			if err := os.MkdirAll(taskDir, 0o700); err != nil {
				t.Fatal(err)
			}
			meta := runningMetadata(t, "untrusted-result", true, 3)
			attempt := meta.Attempts[0]
			result := tasks.Result{TaskID: meta.TaskID, AttemptID: attempt.AttemptID, AttemptToken: attempt.AttemptToken, Status: tasks.StatusCompleted}
			tt.mutate(&meta, &result)
			if err := writeJSON(filepath.Join(taskDir, "task.json"), meta); err != nil {
				t.Fatal(err)
			}
			// Write directly into this task directory so even a mismatched
			// result.TaskID exercises recovery's fence validation.
			if err := writeJSON(filepath.Join(taskDir, "result.json"), result); err != nil {
				t.Fatal(err)
			}

			restarted := New(root)
			if err := restarted.Init(); err != nil {
				t.Fatal(err)
			}
			got, err := restarted.GetTask(meta.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != tasks.StatusQueued || got.Attempts[0].State != tasks.AttemptStateInterrupted || got.Attempts[0].Interruption != RestartInterruptionReason {
				t.Fatalf("untrusted result became authoritative: %#v", got)
			}
			queue, err := restarted.RestoreQueue()
			if err != nil {
				t.Fatal(err)
			}
			assertQueue(t, queue, meta.TaskID)
		})
	}
}

func TestRestartInterruptsAttemptsAndOnlyRequeuesRetryableIdempotentTask(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	queued := taskMetadata("queued-first", tasks.StatusAccepted, false, 0)
	if err := s.SaveTask(queued); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(queued.TaskID); err != nil {
		t.Fatal(err)
	}

	nonID := runningMetadata(t, "non-idempotent", false, 1)
	if err := s.SaveTask(nonID); err != nil {
		t.Fatal(err)
	}
	idempotent := runningMetadata(t, "idempotent", true, 2)
	if err := s.SaveTask(idempotent); err != nil {
		t.Fatal(err)
	}
	defaultOne := runningMetadata(t, "idempotent-default-one", true, 0)
	if err := s.SaveTask(defaultOne); err != nil {
		t.Fatal(err)
	}

	restarted := New(root)
	if err := restarted.Init(); err != nil {
		t.Fatal(err)
	}
	nonIDGot, err := restarted.GetTask(nonID.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if nonIDGot.Status != tasks.StatusInterrupted || nonIDGot.Attempts[0].State != tasks.AttemptStateInterrupted || nonIDGot.Attempts[0].Interruption != RestartInterruptionReason {
		t.Fatalf("non-idempotent recovery = %#v", nonIDGot)
	}
	idempotentGot, err := restarted.GetTask(idempotent.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if idempotentGot.Status != tasks.StatusQueued || idempotentGot.Attempts[0].State != tasks.AttemptStateInterrupted || idempotentGot.QueuePosition != 2 {
		t.Fatalf("idempotent recovery = %#v", idempotentGot)
	}
	defaultGot, err := restarted.GetTask(defaultOne.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if defaultGot.Status != tasks.StatusInterrupted {
		t.Fatalf("default one-attempt task was replayed: %#v", defaultGot)
	}
	queue, err := restarted.ListQueue()
	if err != nil {
		t.Fatal(err)
	}
	assertQueue(t, queue, "queued-first", "idempotent")
}

func TestLegacyPhaseIRecordMigratesWithoutLosingFields(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "tasks", "legacy-task")
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := `{
  "task_id": "legacy-task",
  "kind": "prompt",
  "status": "completed",
  "requester_peer": "requester",
  "worker_peer": "worker",
  "created_at": "2026-08-05T00:00:00Z",
  "updated_at": "2026-08-05T00:00:01Z",
  "request": {"task_id":"legacy-task","kind":"prompt","task":{"prompt":"hello","stream":false},"constraints":{},"timeout_seconds":30,"trust_level":"trusted-lan"}
}`
	if err := os.WriteFile(filepath.Join(taskDir, "task.json"), []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	s := New(root)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	meta, err := s.GetTask("legacy-task")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != tasks.StatusCompleted || meta.Request.Task.Prompt != "hello" || meta.Idempotent || meta.MaxAttempts != 1 {
		t.Fatalf("legacy task migration = %#v", meta)
	}
	if _, err := os.Stat(filepath.Join(root, rootStateFile)); err != nil {
		t.Fatalf("versioned root state was not created: %v", err)
	}
}

func TestCorruptTaskRecordsAreReturnedAsActionableErrors(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveTask(taskMetadata("valid-task", tasks.StatusCompleted, false, 0)); err != nil {
		t.Fatal(err)
	}
	corruptDir := filepath.Join(root, "tasks", "corrupt-task")
	if err := os.MkdirAll(corruptDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(corruptDir, "task.json"), []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListTasks()
	if err == nil || !strings.Contains(err.Error(), "corrupt-task") || !strings.Contains(err.Error(), "parse") {
		t.Fatalf("corrupt record error = %v", err)
	}
	if len(got) != 1 || got[0].TaskID != "valid-task" {
		t.Fatalf("valid records should be returned alongside joined error: %#v", got)
	}

	root2 := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root2, "tasks"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root2, rootStateFile), []byte("{bad-state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := New(root2).Init(); err == nil || !strings.Contains(err.Error(), "root state") {
		t.Fatalf("corrupt root state error = %v", err)
	}
}

func TestConcurrentEnqueueIsDurableAndSequenceUnique(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	const count = 40
	for i := 0; i < count; i++ {
		taskID := tasks.NewID("queue-test")
		if err := s.SaveTask(taskMetadata(taskID, tasks.StatusAccepted, false, 0)); err != nil {
			t.Fatal(err)
		}
	}
	all, err := s.ListTasks()
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errorsSeen := make(chan error, count)
	for _, meta := range all {
		wg.Add(1)
		go func(taskID string) {
			defer wg.Done()
			_, err := s.Enqueue(taskID)
			errorsSeen <- err
		}(meta.TaskID)
	}
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	queue, err := s.ListQueue()
	if err != nil {
		t.Fatal(err)
	}
	if len(queue) != count {
		t.Fatalf("queue length = %d, want %d", len(queue), count)
	}
	seen := make(map[string]struct{}, count)
	var previous uint64
	for index, entry := range queue {
		if entry.Position != index+1 || entry.Sequence <= previous {
			t.Fatalf("invalid FIFO entry %d: %#v", index, entry)
		}
		if _, duplicate := seen[entry.TaskID]; duplicate {
			t.Fatalf("duplicate task %q", entry.TaskID)
		}
		seen[entry.TaskID] = struct{}{}
		previous = entry.Sequence
	}
	restarted := New(root)
	if err := restarted.Init(); err != nil {
		t.Fatal(err)
	}
	restored, err := restarted.RestoreQueue()
	if err != nil {
		t.Fatal(err)
	}
	if len(restored) != count {
		t.Fatalf("restored queue length = %d, want %d", len(restored), count)
	}
}

func TestEnqueueBoundedRejectsAtCapacityAndRecoversAfterCancellation(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	for _, taskID := range []string{"bounded-1", "bounded-2", "bounded-3"} {
		if err := s.SaveTask(taskMetadata(taskID, tasks.StatusAccepted, false, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.EnqueueBounded("bounded-1", 2, "requester"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnqueueBounded("bounded-2", 2, "requester"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnqueueBounded("bounded-1", 2, "requester"); err != nil {
		t.Fatalf("idempotent enqueue at capacity: %v", err)
	}
	if _, err := s.EnqueueBounded("bounded-3", 2, "requester"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("third enqueue error = %v, want ErrQueueFull", err)
	}
	if canceled, err := s.CancelQueued("bounded-1"); err != nil || !canceled {
		t.Fatalf("cancel queued entry = %t, %v", canceled, err)
	}
	if _, err := s.EnqueueBounded("bounded-3", 2, "requester"); err != nil {
		t.Fatalf("enqueue after cancellation: %v", err)
	}

	restarted := New(root)
	if err := restarted.Init(); err != nil {
		t.Fatal(err)
	}
	queue, err := restarted.RestoreQueue()
	if err != nil {
		t.Fatal(err)
	}
	if len(queue) != 2 || queue[0].TaskID != "bounded-2" || queue[1].TaskID != "bounded-3" {
		t.Fatalf("restored bounded queue = %#v", queue)
	}
}

func TestStoreRejectsTaskPathTraversalAndCleansAtomicTemps(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveTask(taskMetadata("../escape", tasks.StatusQueued, false, 0)); err == nil {
		t.Fatal("expected path-like task ID to be rejected")
	}
	if err := s.SaveTask(taskMetadata("safe-task", tasks.StatusAccepted, false, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue("safe-task"); err != nil {
		t.Fatal(err)
	}
	matches, err := filepath.Glob(filepath.Join(root, ".*.tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	taskMatches, err := filepath.Glob(filepath.Join(root, "tasks", "safe-task", ".*.tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches)+len(taskMatches) != 0 {
		t.Fatalf("atomic temp files leaked: %v %v", matches, taskMatches)
	}
}

func taskMetadata(taskID, status string, idempotent bool, maxAttempts int) tasks.Metadata {
	now := time.Now().UTC()
	return tasks.Metadata{
		TaskID:    taskID,
		Kind:      tasks.KindPrompt,
		Status:    status,
		CreatedAt: now,
		Request: tasks.Request{
			TaskID:      taskID,
			Kind:        tasks.KindPrompt,
			Task:        tasks.PromptTask{Prompt: "hello"},
			Idempotent:  idempotent,
			MaxAttempts: maxAttempts,
		},
	}
}

func runningMetadata(t *testing.T, taskID string, idempotent bool, maxAttempts int) tasks.Metadata {
	t.Helper()
	meta := taskMetadata(taskID, tasks.StatusRunning, idempotent, maxAttempts)
	attempt, err := tasks.NewAttempt(taskID, "worker-1", "session-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	attempt.State = tasks.AttemptStateRunning
	started := time.Now().UTC()
	attempt.StartedAt = &started
	attempt.UpdatedAt = started
	meta.Attempts = []tasks.TaskAttempt{attempt}
	meta.CurrentAttempt = 1
	meta.TotalAttempts = 1
	meta.StartedAt = &started
	return meta
}

func assertQueue(t *testing.T, queue []tasks.QueueEntry, taskIDs ...string) {
	t.Helper()
	if len(queue) != len(taskIDs) {
		t.Fatalf("queue length = %d, want %d: %#v", len(queue), len(taskIDs), queue)
	}
	for index, taskID := range taskIDs {
		if queue[index].TaskID != taskID || queue[index].Position != index+1 {
			t.Fatalf("queue[%d] = %#v, want task %q at exact position %d", index, queue[index], taskID, index+1)
		}
	}
}
