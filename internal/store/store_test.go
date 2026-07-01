package store

import (
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/tasks"
)

func TestTaskStoreRoundTrip(t *testing.T) {
	s := New(t.TempDir())
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	meta := tasks.Metadata{
		TaskID:    "task-1",
		Kind:      tasks.KindPrompt,
		Status:    tasks.StatusAccepted,
		CreatedAt: time.Now().UTC(),
		Request: tasks.Request{
			TaskID: "task-1",
			Kind:   tasks.KindPrompt,
			Task:   tasks.PromptTask{Prompt: "hello"},
		},
	}
	if err := s.SaveTask(meta); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvent(tasks.NewEvent("task-1", 0, tasks.EventStarted, "started", "")); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveResult(tasks.Result{TaskID: "task-1", Status: tasks.StatusCompleted, OutputText: "hi"}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetTask("task-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.TaskID != "task-1" {
		t.Fatalf("unexpected task id %q", got.TaskID)
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
	if result.OutputText != "hi" {
		t.Fatalf("unexpected output %q", result.OutputText)
	}
}
