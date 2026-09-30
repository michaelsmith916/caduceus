package store

import (
	"github.com/caduceus/caduceus/internal/tasks"
	"testing"
)

func TestReviewRecoveryExcludesRejectedExecutionBudget(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	meta := runningMetadata(t, "review-budget", true, 2)
	rejected, _ := tasks.NewAttempt(meta.TaskID, "worker-a", "session-a", 1)
	rejected.State = tasks.AttemptStateRejected
	meta.Attempts[0].AttemptNumber = 2
	meta.Attempts = append([]tasks.TaskAttempt{rejected}, meta.Attempts...)
	meta.TotalAttempts = 2
	meta.CurrentAttempt = 2
	if err := s.SaveTask(meta); err != nil {
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
	if got.Status != tasks.StatusQueued || got.CurrentAttempt != 2 || got.TotalAttempts != 2 || len(got.Attempts) != 2 {
		t.Fatalf("rejected assignment consumed execution budget: %+v", got)
	}
}

func TestReviewRecoveryNeverDispatchesForeignRequesterWork(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	meta := runningMetadata(t, "review-worker", true, 2)
	meta.RequesterPeer = "remote-requester"
	meta.WorkerPeer = "local-worker"
	if err := s.SaveTask(meta); err != nil {
		t.Fatal(err)
	}
	restarted := NewForRequester(root, "local-worker")
	if err := restarted.Init(); err != nil {
		t.Fatal(err)
	}
	got, err := restarted.GetTask(meta.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := restarted.ListQueue()
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != tasks.StatusInterrupted || len(queue) != 0 {
		t.Fatalf("worker assignment resurrected in requester FIFO: status=%s queue=%+v", got.Status, queue)
	}
}
