package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/caduceus/caduceus/internal/scheduler"
	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/caduceus/caduceus/internal/workers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var (
	errQueuedTaskCanceled = errors.New("queued task was canceled")
	errAttemptFenced      = errors.New("task attempt is no longer current")
)

// ExplainRoute evaluates the same immutable registry snapshot and policy used
// by RunTask, but it does not enqueue, assign, or execute the task.
func (n *Node) ExplainRoute(req tasks.Request) (scheduler.RoutingDecision, error) {
	req = n.prepareTaskRequest(req)
	if err := n.ValidateTask(req); err != nil {
		return scheduler.RoutingDecision{}, err
	}
	if n.phase2 == nil {
		return scheduler.RoutingDecision{}, errors.New("phase 2 scheduler is unavailable")
	}
	return n.phase2.evaluate(req)
}

func (n *Node) prepareTaskRequest(req tasks.Request) tasks.Request {
	if req.TaskID == "" {
		req.TaskID = tasks.NewID("task")
	}
	if req.Kind == "" {
		req.Kind = tasks.KindPrompt
	}
	if req.TrustLevel == "" {
		req.TrustLevel = n.cfg.Security.TrustLevelDefault
	}
	if req.TimeoutSeconds == 0 {
		req.TimeoutSeconds = n.cfg.OpenAI.TimeoutSeconds
	}
	if req.Task.Model == "" {
		req.Task.Model = n.cfg.OpenAI.DefaultModel
	}
	return req
}

func (n *Node) dispatchTask(ctx context.Context, req tasks.Request) (tasks.Result, error) {
	if n.phase2 == nil {
		return tasks.Result{}, errors.New("phase 2 dispatcher is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	now := time.Now().UTC()
	meta := tasks.Metadata{
		TaskID:        req.TaskID,
		Kind:          req.Kind,
		Status:        tasks.StatusQueued,
		RequesterPeer: n.HostID(),
		CreatedAt:     now,
		Request:       req,
		Idempotent:    req.Idempotent,
		MaxAttempts:   req.EffectiveMaxAttempts(),
	}

	// Enqueue is durable before this call waits for a bounded dispatch slot.
	// The queue remains exact FIFO because only its current head may be claimed.
	n.phase2.queueMu.Lock()
	if _, err := n.store.GetTask(req.TaskID); err == nil {
		n.phase2.queueMu.Unlock()
		return tasks.Result{}, fmt.Errorf("task %q already exists", req.TaskID)
	} else if !errors.Is(err, os.ErrNotExist) {
		n.phase2.queueMu.Unlock()
		return tasks.Result{}, fmt.Errorf("check existing task %q: %w", req.TaskID, err)
	}
	if err := n.store.SaveTask(meta); err != nil {
		n.phase2.queueMu.Unlock()
		return tasks.Result{}, err
	}
	if _, err := n.store.EnqueueBounded(req.TaskID, n.cfg.Scheduler.MaxQueueDepth, n.HostID()); err != nil {
		n.phase2.queueMu.Unlock()
		return n.finishDispatchError(req.TaskID, err)
	}
	n.phase2.signalQueueLocked()
	n.phase2.queueMu.Unlock()

	if err := n.awaitDispatchTurn(ctx, req.TaskID); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errQueuedTaskCanceled) {
			return tasks.Result{TaskID: req.TaskID, Status: tasks.StatusCanceled, Error: err.Error(), Artifacts: []tasks.Artifact{}}, err
		}
		return n.finishDispatchError(req.TaskID, err)
	}
	defer n.phase2.releaseDispatchPermit()
	return n.dispatchPrepared(ctx, req)
}

// resumePersistedQueue drains recovered FIFO entries only as bounded dispatcher
// permits become available. It never creates a goroutine per queued task.
func (n *Node) resumePersistedQueue() {
	n.phase2.wg.Add(1)
	go func() {
		defer n.phase2.wg.Done()
		for {
			entries, err := n.store.RestoreQueue()
			if err != nil || len(entries) == 0 {
				if err != nil && n.log != nil {
					n.log.Error("restore durable task queue", "error", err.Error())
				}
				return
			}
			taskID := entries[0].TaskID
			meta, err := n.store.GetTask(taskID)
			if err != nil {
				return
			}
			taskCtx := n.phase2.ctx
			cancelTask := func() {}
			if meta.Request.TimeoutSeconds > 0 {
				taskCtx, cancelTask = context.WithTimeout(taskCtx, time.Duration(meta.Request.TimeoutSeconds)*time.Second)
			}
			if err := n.awaitRecoveredWorker(taskCtx, meta.Request); err != nil {
				cancelTask()
				if n.phase2.ctx.Err() != nil {
					return
				}
				if !errors.Is(err, errQueuedTaskCanceled) {
					n.phase2.queueMu.Lock()
					canceled, cancelErr := n.store.CancelQueued(taskID)
					if canceled {
						n.phase2.signalQueueLocked()
					}
					n.phase2.queueMu.Unlock()
					if cancelErr != nil {
						if n.log != nil {
							n.log.Error("cancel restored task", "task_id", taskID, "error", cancelErr.Error())
						}
						return
					}
				}
				continue
			}
			if err := n.awaitDispatchTurn(taskCtx, taskID); err != nil {
				cancelTask()
				if n.phase2.ctx.Err() == nil && (errors.Is(err, errQueuedTaskCanceled) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					continue
				}
				if n.phase2.ctx.Err() == nil && n.log != nil {
					n.log.Error("claim restored task", "task_id", taskID, "error", err.Error())
				}
				return
			}
			meta, err = n.store.GetTask(taskID)
			if err != nil {
				cancelTask()
				n.phase2.releaseDispatchPermit()
				if n.log != nil {
					n.log.Error("load restored task", "task_id", taskID, "error", err.Error())
				}
				continue
			}
			n.phase2.wg.Add(1)
			go func(ctx context.Context, cancel context.CancelFunc, req tasks.Request) {
				defer cancel()
				defer n.phase2.wg.Done()
				defer n.phase2.releaseDispatchPermit()
				if _, err := n.dispatchPrepared(ctx, req); err != nil && n.log != nil {
					n.log.Error("restored task failed", "task_id", req.TaskID, "error", err.Error())
				}
			}(taskCtx, cancelTask, meta.Request)
		}
	}()
}

// Keep restored work durable while discovery rebuilds the registry. Only the
// FIFO head waits here, with no per-task goroutine or consumed dispatch permit.
func (n *Node) awaitRecoveredWorker(ctx context.Context, req tasks.Request) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		n.phase2.queueMu.Lock()
		changed := n.phase2.queueChanged
		n.phase2.queueMu.Unlock()
		if n.taskCanceled(req.TaskID) {
			return errQueuedTaskCanceled
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		decision, err := n.phase2.evaluate(req)
		if err != nil {
			return err
		}
		meta, err := n.store.GetTask(req.TaskID)
		if err != nil {
			return err
		}
		tried := make(map[string]struct{}, len(meta.Attempts))
		for _, attempt := range meta.Attempts {
			tried[attempt.WorkerID] = struct{}{}
		}
		applyTriedWorkers(&decision, tried)
		if len(eligibleCandidates(decision)) > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.phase2.ctx.Done():
			return n.phase2.ctx.Err()
		case <-changed:
		case <-ticker.C:
		}
	}
}

func (r *phase2Runtime) signalQueueLocked() {
	close(r.queueChanged)
	r.queueChanged = make(chan struct{})
}

func (r *phase2Runtime) releaseDispatchPermit() {
	<-r.dispatchPermits
}

func (n *Node) awaitDispatchTurn(ctx context.Context, taskID string) error {
	for {
		n.phase2.queueMu.Lock()
		position, queued, err := n.store.QueuePosition(taskID)
		changed := n.phase2.queueChanged
		n.phase2.queueMu.Unlock()
		if err != nil {
			return err
		}
		if !queued {
			meta, getErr := n.store.GetTask(taskID)
			if getErr == nil && meta.Status == tasks.StatusCanceled {
				return errQueuedTaskCanceled
			}
			if getErr != nil {
				return getErr
			}
			return fmt.Errorf("task %q is no longer in the durable dispatch queue", taskID)
		}
		if position != 1 {
			select {
			case <-ctx.Done():
				if n.phase2.ctx.Err() != nil {
					return n.phase2.ctx.Err()
				}
				return n.cancelQueuedForContext(taskID, ctx.Err())
			case <-n.phase2.ctx.Done():
				return n.phase2.ctx.Err()
			case <-changed:
				continue
			}
		}

		select {
		case n.phase2.dispatchPermits <- struct{}{}:
		case <-ctx.Done():
			if n.phase2.ctx.Err() != nil {
				return n.phase2.ctx.Err()
			}
			return n.cancelQueuedForContext(taskID, ctx.Err())
		case <-n.phase2.ctx.Done():
			return n.phase2.ctx.Err()
		case <-changed:
			continue
		}

		n.phase2.queueMu.Lock()
		if n.phase2.ctx.Err() != nil || ctx.Err() != nil {
			n.phase2.queueMu.Unlock()
			n.phase2.releaseDispatchPermit()
			if n.phase2.ctx.Err() != nil {
				return n.phase2.ctx.Err()
			}
			return n.cancelQueuedForContext(taskID, ctx.Err())
		}
		position, queued, err = n.store.QueuePosition(taskID)
		if err == nil && queued && position == 1 {
			entry, ok, dequeueErr := n.store.Dequeue()
			if dequeueErr == nil && (!ok || entry.TaskID != taskID) {
				dequeueErr = fmt.Errorf("durable dispatch queue returned %q while claiming %q", entry.TaskID, taskID)
			}
			n.phase2.signalQueueLocked()
			n.phase2.queueMu.Unlock()
			if dequeueErr != nil {
				n.phase2.releaseDispatchPermit()
			}
			return dequeueErr
		}
		n.phase2.queueMu.Unlock()
		n.phase2.releaseDispatchPermit()
		if err != nil {
			return err
		}
	}
}

func (n *Node) cancelQueuedForContext(taskID string, cause error) error {
	n.phase2.queueMu.Lock()
	canceled, err := n.store.CancelQueued(taskID)
	if canceled {
		n.phase2.signalQueueLocked()
	}
	n.phase2.queueMu.Unlock()
	if err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (n *Node) dispatchPrepared(ctx context.Context, req tasks.Request) (tasks.Result, error) {
	dispatchCtx, cancelDispatch := context.WithCancel(ctx)
	n.phase2.trackDispatch(req.TaskID, cancelDispatch)
	defer n.phase2.untrackDispatch(req.TaskID)
	defer cancelDispatch()
	ctx = dispatchCtx

	if n.taskCanceled(req.TaskID) {
		return tasks.Result{TaskID: req.TaskID, Status: tasks.StatusCanceled, Error: "task canceled", Artifacts: []tasks.Artifact{}}, context.Canceled
	}

	meta, err := n.store.GetTask(req.TaskID)
	if err != nil {
		return tasks.Result{}, err
	}
	baseAttemptNumber := maxAttemptNumber(meta.Attempts)
	executionBudget := req.EffectiveMaxAttempts()
	executions := countExecutionAttempts(meta.Attempts)
	if executions >= executionBudget {
		return n.finishDispatchError(req.TaskID, fmt.Errorf("task exhausted its execution budget (%d/%d)", executions, executionBudget))
	}

	assignmentLimit := 1 + n.cfg.Scheduler.MaxRerouteAttempts
	remainingAttemptNumbers := tasks.MaxAttemptsLimit - baseAttemptNumber
	if assignmentLimit > remainingAttemptNumbers {
		assignmentLimit = remainingAttemptNumbers
	}
	if assignmentLimit <= 0 {
		return n.finishDispatchError(req.TaskID, errors.New("task exhausted its assignment attempt numbers"))
	}
	tried := make(map[string]struct{}, len(meta.Attempts))
	for _, previous := range meta.Attempts {
		if previous.WorkerID != "" {
			tried[previous.WorkerID] = struct{}{}
		}
	}
	var lastErr error
	var lastRejection *tasks.TaskRejection
	assignmentsMade := 0

	for assignmentsMade < assignmentLimit {
		if ctx.Err() != nil || n.taskCanceled(req.TaskID) {
			return tasks.Result{TaskID: req.TaskID, Status: tasks.StatusCanceled, Error: "task canceled", Artifacts: []tasks.Artifact{}}, context.Canceled
		}
		decision, evaluateErr := n.phase2.evaluate(req)
		if evaluateErr != nil {
			return n.finishDispatchError(req.TaskID, evaluateErr)
		}
		applyTriedWorkers(&decision, tried)
		persistedDecision := routingDecisionForStore(decision)
		if _, updateErr := n.store.UpdateTask(req.TaskID, func(meta *tasks.Metadata) error {
			meta.LastRoutingDecision = &persistedDecision
			return nil
		}); updateErr != nil {
			return tasks.Result{}, updateErr
		}
		candidates := eligibleCandidates(decision)
		if len(candidates) == 0 {
			if delay, cooling := n.phase2.nextCooldownDelay(decision, tried); cooling {
				if waitErr := n.phase2.waitForCooldown(ctx, delay); waitErr != nil {
					return n.finishDispatchError(req.TaskID, waitErr)
				}
				continue
			}
			if lastErr == nil {
				lastErr = errors.New("no untried worker satisfied the task constraints")
			}
			break
		}
		candidate := candidates[0]
		worker := candidate.Worker.Worker
		tried[worker.WorkerID] = struct{}{}
		attempt, err := tasks.NewAttempt(req.TaskID, worker.WorkerID, worker.SessionID, baseAttemptNumber+assignmentsMade+1)
		assignmentsMade++
		if err != nil {
			return n.finishDispatchError(req.TaskID, err)
		}
		if err := n.beginAttempt(req, worker, attempt); err != nil {
			if errors.Is(err, errAttemptFenced) && n.taskCanceled(req.TaskID) {
				return tasks.Result{TaskID: req.TaskID, Status: tasks.StatusCanceled, Error: "task canceled", Artifacts: []tasks.Artifact{}}, context.Canceled
			}
			return tasks.Result{}, err
		}
		n.phase2.markAssigned(worker.WorkerID)

		attemptCtx, cancel := context.WithCancel(ctx)
		n.phase2.track(req.TaskID, attempt, cancel)
		result, rejection, accepted, runErr := n.runAttempt(attemptCtx, worker, req, attempt)
		n.phase2.untrack(req.TaskID, attempt.AttemptID)
		cancel()

		if ctx.Err() != nil || n.taskCanceled(req.TaskID) {
			message := "task canceled"
			if runErr != nil {
				message = runErr.Error()
			}
			result = tasks.Result{TaskID: req.TaskID, AttemptID: attempt.AttemptID, AttemptToken: attempt.AttemptToken, Status: tasks.StatusCanceled, Error: message, Artifacts: []tasks.Artifact{}}
			_ = n.store.SaveResult(result)
			_ = n.finishAttempt(attempt, tasks.AttemptStateCanceled, message, "canceled")
			return result, context.Canceled
		}

		if rejection != nil {
			lastRejection = rejection
			lastErr = &TaskRejectedError{Rejection: *rejection}
			n.phase2.setWorkerCooldown(worker.WorkerID, rejection.RetryAfterDuration())
			if err := n.recordRejection(attempt, *rejection); err != nil {
				return tasks.Result{}, err
			}
			// A structured pre-execution rejection is safe to reroute even for
			// non-idempotent work because the worker did not accept it.
			continue
		}

		if result.TaskID != "" {
			if err := n.validateAttemptCommit(attempt); err != nil {
				result = tasks.Result{}
				runErr = err
				accepted = true
			}
		}

		// A terminal result is authoritative even when local execution also
		// returns its application error. Only transport/liveness ambiguity is
		// eligible for failover.
		if result.TaskID != "" || runErr == nil {
			if err := fenceResult(attempt, &result); err != nil {
				_ = n.finishAttempt(attempt, tasks.AttemptStateLost, err.Error(), "result_fence_mismatch")
				return n.finishDispatchError(req.TaskID, err)
			}
			if err := n.store.SaveResult(result); err != nil {
				return tasks.Result{}, err
			}
			if err := n.finishAttempt(attempt, attemptStateForResult(result), resultError(result), ""); err != nil {
				if errors.Is(err, errAttemptFenced) {
					fenced := tasks.Result{TaskID: req.TaskID, AttemptID: attempt.AttemptID, AttemptToken: attempt.AttemptToken, Status: tasks.StatusLost, Error: err.Error(), Artifacts: []tasks.Artifact{}}
					_ = n.store.SaveResult(fenced)
					return fenced, err
				}
				return tasks.Result{}, err
			}
			return result, runErr
		}

		if accepted {
			executions++
		}
		lastErr = runErr
		state := tasks.AttemptStateInterrupted
		if !accepted {
			// Once an assignment is written, loss of the stream is ambiguous:
			// the remote side might have accepted it before the reply was lost.
			state = tasks.AttemptStateLost
			executions++
		}
		retry := req.Idempotent && executions < executionBudget && assignmentsMade < assignmentLimit
		if err := n.finishAttemptState(attempt, state, runErr.Error(), "worker_or_transport_lost", retry); err != nil {
			return tasks.Result{}, err
		}
		if !req.Idempotent || executions >= executionBudget {
			break
		}
	}

	if lastErr == nil && lastRejection != nil {
		lastErr = &TaskRejectedError{Rejection: *lastRejection}
	}
	if lastErr == nil {
		lastErr = errors.New("no remaining worker candidate")
	}
	return n.finishDispatchError(req.TaskID, lastErr)
}

func eligibleCandidates(decision scheduler.RoutingDecision) []scheduler.CandidateScore {
	out := make([]scheduler.CandidateScore, 0, len(decision.Candidates))
	for _, candidate := range decision.Candidates {
		if candidate.Eligible {
			out = append(out, candidate)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Rank < out[j].Rank })
	return out
}

func (n *Node) runAttempt(ctx context.Context, worker workers.Worker, req tasks.Request, attempt tasks.TaskAttempt) (tasks.Result, *tasks.TaskRejection, bool, error) {
	if worker.Local || worker.PeerID == n.HostID() {
		lease, rejection, err := n.phase2.admission.Acquire(ctx, req, n.phase2.admissionSnapshot(n, req))
		if err != nil || rejection != nil {
			return tasks.Result{}, rejection, false, err
		}
		if rejection := n.phase2.admission.Validate(req, n.phase2.freshAdmissionSnapshot(ctx, n, req)); rejection != nil {
			lease.Release()
			return tasks.Result{}, rejection, false, nil
		}
		defer lease.Release()
		if err := n.markAttemptRunning(attempt); err != nil {
			return tasks.Result{}, nil, true, err
		}
		result, err := n.executeLocal(withTaskAttempt(ctx, attempt), req, n.HostID(), n.HostID(), nil)
		result.AttemptID = attempt.AttemptID
		result.AttemptToken = attempt.AttemptToken
		return result, nil, true, err
	}
	return n.runRemoteAttempt(ctx, worker, req, attempt)
}

func (n *Node) runRemoteAttempt(ctx context.Context, worker workers.Worker, req tasks.Request, attempt tasks.TaskAttempt) (tasks.Result, *tasks.TaskRejection, bool, error) {
	id, err := peer.Decode(worker.PeerID)
	if err != nil {
		return tasks.Result{}, nil, false, err
	}
	stream, err := n.host.NewStream(ctx, id, protocol.ID(TaskProtocolV2), protocol.ID(TaskProtocol))
	if err != nil {
		return tasks.Result{}, nil, false, err
	}
	defer stream.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}
	stopReset := context.AfterFunc(ctx, func() { _ = stream.Reset() })
	defer stopReset()

	encoder := json.NewEncoder(stream)
	decoder := newEnvelopeDecoder(stream)
	payload := any(TaskAssignment{Request: req, Attempt: attempt})
	if stream.Protocol() == protocol.ID(TaskProtocol) {
		payload = req
	}
	envelope, err := n.envelopeForProtocol(stream.Protocol(), TypeTaskRequest, payload)
	if err != nil {
		return tasks.Result{}, nil, false, err
	}
	if err := encoder.Encode(envelope); err != nil {
		return tasks.Result{}, nil, false, err
	}

	accepted := false
	for {
		var incoming Envelope
		if err := decoder.Decode(&incoming); err != nil {
			if errors.Is(err, io.EOF) && ctx.Err() != nil {
				err = ctx.Err()
			}
			return tasks.Result{}, nil, accepted, err
		}
		if err := n.verifyEnvelope(incoming, id); err != nil {
			return tasks.Result{}, nil, accepted, err
		}
		switch incoming.Type {
		case TypeTaskReject:
			if accepted {
				return tasks.Result{}, nil, true, errors.New("worker rejected an already accepted task attempt")
			}
			var rejection tasks.TaskRejection
			if err := json.Unmarshal(incoming.Payload, &rejection); err != nil {
				return tasks.Result{}, nil, accepted, err
			}
			if err := validateRejectionForAttempt(rejection, attempt, worker.WorkerID); err != nil {
				return tasks.Result{}, nil, accepted, err
			}
			return tasks.Result{}, &rejection, false, nil
		case TypeTaskAccept:
			if stream.Protocol() == protocol.ID(TaskProtocolV2) {
				var acceptance TaskAcceptance
				if err := json.Unmarshal(incoming.Payload, &acceptance); err != nil {
					return tasks.Result{}, nil, accepted, err
				}
				if acceptance.TaskID != req.TaskID || acceptance.AttemptID != attempt.AttemptID || acceptance.AttemptToken != attempt.AttemptToken ||
					acceptance.WorkerID != worker.WorkerID || acceptance.SessionID != attempt.WorkerSession {
					return tasks.Result{}, nil, accepted, errors.New("task acceptance did not match the active attempt")
				}
			} else {
				var event tasks.Event
				if err := json.Unmarshal(incoming.Payload, &event); err != nil {
					return tasks.Result{}, nil, accepted, err
				}
				if err := n.recordRemoteEvent(attempt, event); err != nil {
					return tasks.Result{}, nil, accepted, err
				}
			}
			accepted = true
			if err := n.markAttemptRunning(attempt); err != nil {
				return tasks.Result{}, nil, accepted, err
			}
		case TypeTaskEvent:
			var event tasks.Event
			if err := json.Unmarshal(incoming.Payload, &event); err != nil {
				return tasks.Result{}, nil, accepted, err
			}
			if event.TaskID != req.TaskID {
				return tasks.Result{}, nil, accepted, errors.New("task event did not match the active task")
			}
			if err := n.recordRemoteEvent(attempt, event); err != nil {
				return tasks.Result{}, nil, accepted, err
			}
			if !accepted && eventImpliesAcceptance(event.EventType) {
				accepted = true
				if err := n.markAttemptRunning(attempt); err != nil {
					return tasks.Result{}, nil, accepted, err
				}
			}
		case TypeTaskResult:
			var result tasks.Result
			if stream.Protocol() == protocol.ID(TaskProtocolV2) {
				var payload TaskResultPayload
				if err := json.Unmarshal(incoming.Payload, &payload); err != nil {
					return tasks.Result{}, nil, accepted, err
				}
				if payload.AttemptID != attempt.AttemptID || payload.AttemptToken != attempt.AttemptToken {
					return tasks.Result{}, nil, accepted, errors.New("late or mismatched task result was fenced")
				}
				result = payload.Result
				if result.AttemptID == "" {
					result.AttemptID = payload.AttemptID
				}
				if result.AttemptToken == "" {
					result.AttemptToken = payload.AttemptToken
				}
			} else {
				if err := json.Unmarshal(incoming.Payload, &result); err != nil {
					return tasks.Result{}, nil, accepted, err
				}
				result.AttemptID = attempt.AttemptID
				result.AttemptToken = attempt.AttemptToken
			}
			if result.TaskID != req.TaskID {
				return tasks.Result{}, nil, accepted, errors.New("task result did not match the active task")
			}
			return result, nil, accepted, nil
		case TypeTaskInterrupt:
			var interruption TaskInterruption
			if err := json.Unmarshal(incoming.Payload, &interruption); err != nil {
				return tasks.Result{}, nil, accepted, err
			}
			if interruption.AttemptID != attempt.AttemptID || interruption.AttemptToken != attempt.AttemptToken {
				return tasks.Result{}, nil, accepted, errors.New("task interruption did not match the active attempt")
			}
			return tasks.Result{}, nil, accepted, errors.New(interruption.Reason)
		case TypeError:
			var payload ErrorPayload
			_ = json.Unmarshal(incoming.Payload, &payload)
			if payload.Message == "" {
				payload.Message = "remote task error"
			}
			return tasks.Result{}, nil, accepted, errors.New(payload.Message)
		default:
			return tasks.Result{}, nil, accepted, fmt.Errorf("unexpected task response %q", incoming.Type)
		}
	}
}

func (n *Node) beginAttempt(req tasks.Request, worker workers.Worker, attempt tasks.TaskAttempt) error {
	now := time.Now().UTC()
	_, err := n.store.UpdateTask(req.TaskID, func(meta *tasks.Metadata) error {
		if meta.Status == tasks.StatusCanceled || terminalTaskStatus(meta.Status) {
			return errAttemptFenced
		}
		meta.Status = tasks.StatusAccepted
		meta.WorkerPeer = worker.PeerID
		meta.CurrentAttempt = attempt.AttemptNumber
		meta.TotalAttempts = len(meta.Attempts) + 1
		attempt.State = tasks.AttemptStateAccepted
		attempt.UpdatedAt = now
		meta.Attempts = append(meta.Attempts, attempt)
		meta.Error = ""
		meta.FinishedAt = nil
		return nil
	})
	return err
}

func (n *Node) markAttemptRunning(attempt tasks.TaskAttempt) error {
	now := time.Now().UTC()
	_, err := n.store.UpdateTask(attempt.TaskID, func(meta *tasks.Metadata) error {
		if meta.Status == tasks.StatusCanceled {
			return errAttemptFenced
		}
		if _, err := currentAttempt(meta, attempt); err != nil {
			return err
		}
		meta.Status = tasks.StatusRunning
		if meta.StartedAt == nil {
			meta.StartedAt = &now
		}
		return updateAttempt(meta, attempt.AttemptID, func(item *tasks.TaskAttempt) {
			item.State = tasks.AttemptStateRunning
			item.UpdatedAt = now
			if item.StartedAt == nil {
				item.StartedAt = &now
			}
		})
	})
	return err
}

func (n *Node) finishAttempt(attempt tasks.TaskAttempt, state, detail, interruption string) error {
	return n.finishAttemptState(attempt, state, detail, interruption, false)
}

func (n *Node) finishAttemptState(attempt tasks.TaskAttempt, state, detail, interruption string, retry bool) error {
	now := time.Now().UTC()
	_, err := n.store.UpdateTask(attempt.TaskID, func(meta *tasks.Metadata) error {
		current, err := currentAttempt(meta, attempt)
		if err != nil {
			return err
		}
		if meta.Status == tasks.StatusCanceled && state != tasks.AttemptStateCanceled {
			return errAttemptFenced
		}
		meta.Status = taskStatusForAttempt(state)
		meta.Error = detail
		meta.InterruptionReason = interruption
		meta.FinishedAt = &now
		if retry {
			meta.Status = tasks.StatusQueued
			meta.FinishedAt = nil
		}
		current.State = state
		current.UpdatedAt = now
		current.FinishedAt = &now
		current.Error = detail
		current.Interruption = interruption
		return nil
	})
	return err
}

func (n *Node) recordRejection(attempt tasks.TaskAttempt, rejection tasks.TaskRejection) error {
	now := time.Now().UTC()
	_, err := n.store.UpdateTask(attempt.TaskID, func(meta *tasks.Metadata) error {
		if meta.Status == tasks.StatusCanceled {
			return errAttemptFenced
		}
		if _, err := currentAttempt(meta, attempt); err != nil {
			return err
		}
		meta.Status = tasks.StatusQueued
		meta.Rejections = append(meta.Rejections, rejection)
		return updateAttempt(meta, attempt.AttemptID, func(item *tasks.TaskAttempt) {
			item.State = tasks.AttemptStateRejected
			item.UpdatedAt = now
			item.FinishedAt = &now
			item.Error = rejection.Detail
		})
	})
	return err
}

func (n *Node) recordRemoteEvent(attempt tasks.TaskAttempt, event tasks.Event) error {
	if event.TaskID != attempt.TaskID {
		return errors.New("remote event task id mismatch")
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if err := n.store.AppendEvent(event); err != nil {
		return err
	}
	_, err := n.store.UpdateTask(attempt.TaskID, func(meta *tasks.Metadata) error {
		if meta.Status == tasks.StatusCanceled {
			return errAttemptFenced
		}
		if _, err := currentAttempt(meta, attempt); err != nil {
			return err
		}
		switch event.EventType {
		case tasks.EventAccepted:
			meta.Status = tasks.StatusAccepted
		case tasks.EventStarted, tasks.EventToken, tasks.EventProgress, tasks.EventArtifact:
			meta.Status = tasks.StatusRunning
			if meta.StartedAt == nil {
				when := event.Timestamp
				meta.StartedAt = &when
			}
		}
		return nil
	})
	return err
}

func updateAttempt(meta *tasks.Metadata, attemptID string, fn func(*tasks.TaskAttempt)) error {
	for index := range meta.Attempts {
		if meta.Attempts[index].AttemptID == attemptID {
			fn(&meta.Attempts[index])
			return nil
		}
	}
	return fmt.Errorf("attempt %q is not recorded for task %q", attemptID, meta.TaskID)
}

func currentAttempt(meta *tasks.Metadata, attempt tasks.TaskAttempt) (*tasks.TaskAttempt, error) {
	if meta == nil || len(meta.Attempts) == 0 {
		return nil, errAttemptFenced
	}
	current := &meta.Attempts[len(meta.Attempts)-1]
	if current.AttemptID != attempt.AttemptID ||
		current.AttemptToken != attempt.AttemptToken ||
		current.AttemptNumber != attempt.AttemptNumber ||
		current.WorkerID != attempt.WorkerID ||
		current.WorkerSession != attempt.WorkerSession {
		return nil, errAttemptFenced
	}
	return current, nil
}

func (n *Node) finishDispatchError(taskID string, cause error) (tasks.Result, error) {
	message := cause.Error()
	now := time.Now().UTC()
	status := tasks.StatusFailed
	updated, err := n.store.UpdateTask(taskID, func(meta *tasks.Metadata) error {
		if terminalTaskStatus(meta.Status) {
			status = meta.Status
		} else if meta.Status != tasks.StatusInterrupted && meta.Status != tasks.StatusLost {
			meta.Status = tasks.StatusFailed
		}
		status = meta.Status
		meta.Error = message
		meta.FinishedAt = &now
		return nil
	})
	if err == nil {
		status = updated.Status
	}
	result := tasks.Result{TaskID: taskID, Status: status, Error: message, Artifacts: []tasks.Artifact{}}
	if status == tasks.StatusCanceled {
		result.Error = "task canceled"
	}
	_ = n.store.SaveResult(result)
	return result, cause
}

func (n *Node) validateAttemptCommit(attempt tasks.TaskAttempt) error {
	if n.phase2 == nil {
		return nil
	}
	if attempt.WorkerSession != "" {
		current := false
		for _, snapshot := range n.phase2.registry.Snapshot().Workers {
			if snapshot.Worker.WorkerID == attempt.WorkerID {
				current = snapshot.Healthy && !snapshot.Stale && snapshot.Worker.SessionID == attempt.WorkerSession
				break
			}
		}
		if !current {
			return fmt.Errorf("%w: worker session is no longer healthy and current", errAttemptFenced)
		}
	}
	meta, err := n.store.GetTask(attempt.TaskID)
	if err != nil {
		return err
	}
	if meta.Status == tasks.StatusCanceled {
		return fmt.Errorf("%w: task was canceled", errAttemptFenced)
	}
	_, err = currentAttempt(&meta, attempt)
	return err
}

func fenceResult(attempt tasks.TaskAttempt, result *tasks.Result) error {
	if result == nil {
		return errors.New("nil task result")
	}
	if result.TaskID != attempt.TaskID {
		return errors.New("result task id does not match active attempt")
	}
	if result.AttemptID != "" && result.AttemptID != attempt.AttemptID {
		return errors.New("result attempt id does not match active attempt")
	}
	if result.AttemptToken != "" && result.AttemptToken != attempt.AttemptToken {
		return errors.New("result attempt token does not match active attempt")
	}
	if !terminalTaskStatus(result.Status) {
		return fmt.Errorf("result has non-terminal status %q", result.Status)
	}
	result.AttemptID = attempt.AttemptID
	result.AttemptToken = attempt.AttemptToken
	return nil
}

func terminalTaskStatus(status string) bool {
	switch status {
	case tasks.StatusCompleted, tasks.StatusFailed, tasks.StatusCanceled, tasks.StatusInterrupted, tasks.StatusLost:
		return true
	default:
		return false
	}
}

func maxAttemptNumber(attempts []tasks.TaskAttempt) int {
	maximum := 0
	for _, attempt := range attempts {
		if attempt.AttemptNumber > maximum {
			maximum = attempt.AttemptNumber
		}
	}
	return maximum
}

func (n *Node) taskCanceled(taskID string) bool {
	meta, err := n.store.GetTask(taskID)
	return err == nil && meta.Status == tasks.StatusCanceled
}

func validateRejectionForAttempt(rejection tasks.TaskRejection, attempt tasks.TaskAttempt, workerID string) error {
	if err := rejection.Validate(); err != nil {
		return err
	}
	if rejection.TaskID != attempt.TaskID || rejection.AttemptID != attempt.AttemptID || rejection.WorkerID != workerID {
		return errors.New("task rejection did not match the active attempt")
	}
	return nil
}

func routingDecisionForStore(decision scheduler.RoutingDecision) tasks.RoutingDecision {
	out := tasks.RoutingDecision{
		TaskID:                  decision.TaskID,
		SelectedWorkerID:        decision.SelectedWorkerID,
		RegistrySnapshotTime:    decision.RegistryTimestamp,
		RegistrySnapshotVersion: decision.RegistryVersion,
		AppliedWeights:          decision.AppliedWeights.Map(),
		TieBreakDetails:         decision.TieBreak,
		Explanation:             decision.Explanation,
		Timestamp:               time.Now().UTC(),
	}
	missing := map[string]struct{}{}
	for _, candidate := range decision.Candidates {
		components := make(map[string]float64, len(candidate.Components))
		for name, component := range candidate.Components {
			components[name] = component.Score
		}
		out.Candidates = append(out.Candidates, tasks.RoutingCandidateSummary{
			WorkerID:        candidate.WorkerID,
			Eligible:        candidate.Eligible,
			FilterReasons:   append([]string(nil), candidate.FilterReasons...),
			MissingMetrics:  append([]string(nil), candidate.MissingMetrics...),
			ComponentScores: components,
			TotalScore:      candidate.TotalScore,
			Rank:            candidate.Rank,
			Selected:        candidate.Selected,
			TieBreak:        candidate.TieBreak,
		})
		for _, metric := range candidate.MissingMetrics {
			missing[candidate.WorkerID+":"+metric] = struct{}{}
		}
	}
	for metric := range missing {
		out.MissingMetrics = append(out.MissingMetrics, metric)
	}
	sort.Strings(out.MissingMetrics)
	return out
}

func attemptStateForResult(result tasks.Result) string {
	switch result.Status {
	case tasks.StatusCompleted:
		return tasks.AttemptStateCompleted
	case tasks.StatusCanceled:
		return tasks.AttemptStateCanceled
	case tasks.StatusInterrupted:
		return tasks.AttemptStateInterrupted
	case tasks.StatusLost:
		return tasks.AttemptStateLost
	default:
		return tasks.AttemptStateFailed
	}
}

func taskStatusForAttempt(state string) string {
	switch state {
	case tasks.AttemptStateCompleted:
		return tasks.StatusCompleted
	case tasks.AttemptStateCanceled:
		return tasks.StatusCanceled
	case tasks.AttemptStateInterrupted:
		return tasks.StatusInterrupted
	case tasks.AttemptStateLost:
		return tasks.StatusLost
	case tasks.AttemptStateRejected:
		return tasks.StatusQueued
	default:
		return tasks.StatusFailed
	}
}

func resultError(result tasks.Result) string {
	if result.Error == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(result.Error))
}
