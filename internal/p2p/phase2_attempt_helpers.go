package p2p

import (
	"context"
	"time"

	"github.com/caduceus/caduceus/internal/tasks"
)

type taskAttemptContextKey struct{}

func withTaskAttempt(ctx context.Context, attempt tasks.TaskAttempt) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, taskAttemptContextKey{}, attempt)
}

func taskAttemptFromContext(ctx context.Context) (tasks.TaskAttempt, bool) {
	if ctx == nil {
		return tasks.TaskAttempt{}, false
	}
	attempt, ok := ctx.Value(taskAttemptContextKey{}).(tasks.TaskAttempt)
	return attempt, ok
}

func decorateResultWithAttempt(ctx context.Context, result *tasks.Result) {
	if result == nil {
		return
	}
	attempt, ok := taskAttemptFromContext(ctx)
	if !ok {
		return
	}
	result.AttemptID = attempt.AttemptID
	result.AttemptToken = attempt.AttemptToken
}

func updateContextAttempt(ctx context.Context, meta *tasks.Metadata, state, detail string, now time.Time) {
	if meta == nil {
		return
	}
	attempt, ok := taskAttemptFromContext(ctx)
	if !ok {
		return
	}
	for index := range meta.Attempts {
		item := &meta.Attempts[index]
		if item.AttemptID != attempt.AttemptID {
			continue
		}
		item.State = state
		item.UpdatedAt = now
		item.Error = detail
		if state == tasks.AttemptStateRunning && item.StartedAt == nil {
			item.StartedAt = &now
		}
		if terminalAttemptState(state) {
			item.FinishedAt = &now
		}
		return
	}
}

func countExecutionAttempts(attempts []tasks.TaskAttempt) int {
	count := 0
	for _, attempt := range attempts {
		if attempt.State != tasks.AttemptStateRejected {
			count++
		}
	}
	return count
}

func terminalAttemptState(state string) bool {
	switch state {
	case tasks.AttemptStateCompleted, tasks.AttemptStateFailed, tasks.AttemptStateCanceled,
		tasks.AttemptStateInterrupted, tasks.AttemptStateLost, tasks.AttemptStateRejected:
		return true
	default:
		return false
	}
}

func eventImpliesAcceptance(eventType string) bool {
	switch eventType {
	case tasks.EventAccepted, tasks.EventStarted, tasks.EventToken, tasks.EventProgress,
		tasks.EventArtifact, tasks.EventCompleted, tasks.EventFailed, tasks.EventCanceled,
		tasks.EventInterrupted, tasks.EventLost:
		return true
	default:
		return false
	}
}
