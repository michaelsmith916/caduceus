package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func (n *Node) handleTaskStreamPhase2(stream network.Stream) {
	defer stream.Close()
	remote := stream.Conn().RemotePeer()
	decoder := newEnvelopeDecoder(stream)
	encoder := json.NewEncoder(stream)
	var envelope Envelope
	if err := decoder.Decode(&envelope); err != nil {
		_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "bad_message", err.Error())
		return
	}
	if err := n.verifyEnvelope(envelope, remote); err != nil {
		_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "peer_rejected", err.Error())
		return
	}

	switch envelope.Type {
	case TypeTaskRequest:
		n.handleTaskAssignment(stream, remote.String(), envelope, encoder)
	case TypeTaskCancel:
		n.handleTaskCancellation(stream, remote.String(), envelope, encoder)
	default:
		_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "bad_message", "unsupported task message type")
	}
}

func (n *Node) handleTaskAssignment(stream network.Stream, requester string, envelope Envelope, encoder *json.Encoder) {
	protocolID := stream.Protocol()
	var assignment TaskAssignment
	if protocolID == protocol.ID(TaskProtocolV2) {
		if err := json.Unmarshal(envelope.Payload, &assignment); err != nil {
			_ = n.sendErrorForProtocol(protocolID, encoder, "bad_payload", err.Error())
			return
		}
	} else {
		if err := json.Unmarshal(envelope.Payload, &assignment.Request); err != nil {
			_ = n.sendErrorForProtocol(protocolID, encoder, "bad_payload", err.Error())
			return
		}
		assignment.Request = n.prepareTaskRequest(assignment.Request)
		attempt, err := tasks.NewAttempt(assignment.Request.TaskID, n.HostID(), n.phase2.sessionID, 1)
		if err != nil {
			_ = n.sendErrorForProtocol(protocolID, encoder, "internal_error", err.Error())
			return
		}
		assignment.Attempt = attempt
	}
	assignment.Request = n.prepareTaskRequest(assignment.Request)
	if err := validateTaskAssignment(assignment); err != nil {
		_ = n.sendErrorForProtocol(protocolID, encoder, "invalid_task", err.Error())
		return
	}
	if assignment.Attempt.WorkerID != n.HostID() {
		n.sendAssignmentRejection(protocolID, encoder, assignment, tasks.RejectStaleAssignment, "assignment targets a different worker")
		return
	}
	if protocolID == protocol.ID(TaskProtocolV2) && assignment.Attempt.WorkerSession != n.phase2.sessionID {
		n.sendAssignmentRejection(protocolID, encoder, assignment, tasks.RejectStaleAssignment, "assignment targets an expired worker session")
		return
	}
	if requestedWorker := assignment.Request.WorkerID; requestedWorker != "" && requestedWorker != "auto" && requestedWorker != n.HostID() {
		n.sendAssignmentRejection(protocolID, encoder, assignment, tasks.RejectNotAllowed, "task request targets a different worker")
		return
	}
	assignmentCtx := n.phase2.ctx
	assignmentCancel := func() {}
	if assignment.Request.TimeoutSeconds > 0 {
		assignmentCtx, assignmentCancel = context.WithTimeout(n.phase2.ctx, time.Duration(assignment.Request.TimeoutSeconds)*time.Second)
	} else {
		assignmentCtx, assignmentCancel = context.WithCancel(n.phase2.ctx)
	}
	defer assignmentCancel()
	claimed, err := n.claimInboundAttempt(requester, assignment, assignmentCancel)
	if err != nil {
		_ = n.sendErrorForProtocol(protocolID, encoder, "persistence_error", err.Error())
		return
	}
	if !claimed {
		n.sendAssignmentRejection(protocolID, encoder, assignment, tasks.RejectStaleAssignment, "assignment attempt is duplicate, conflicting, or older than worker state")
		return
	}
	defer n.releaseInboundAttempt(assignment.Attempt)

	lease, rejection, err := n.phase2.admission.Acquire(assignmentCtx, assignment.Request, n.phase2.admissionSnapshot(n, assignment.Request))
	if err != nil {
		_ = n.sendErrorForProtocol(protocolID, encoder, "admission_error", err.Error())
		return
	}
	if rejection != nil {
		rejection.AttemptID = assignment.Attempt.AttemptID
		if protocolID == protocol.ID(TaskProtocolV2) {
			reply, envelopeErr := n.envelopeForProtocol(protocolID, TypeTaskReject, rejection)
			if envelopeErr == nil {
				_ = encoder.Encode(reply)
			}
		} else {
			_ = n.sendErrorForProtocol(protocolID, encoder, string(rejection.Reason), rejection.Detail)
		}
		return
	}
	if rejection := n.phase2.admission.Validate(assignment.Request, n.phase2.freshAdmissionSnapshot(assignmentCtx, n, assignment.Request)); rejection != nil {
		lease.Release()
		rejection.AttemptID = assignment.Attempt.AttemptID
		if protocolID == protocol.ID(TaskProtocolV2) {
			reply, envelopeErr := n.envelopeForProtocol(protocolID, TypeTaskReject, rejection)
			if envelopeErr == nil {
				_ = encoder.Encode(reply)
			}
		}
		return
	}
	defer lease.Release()

	now := time.Now().UTC()
	acceptedAttempt := assignment.Attempt
	acceptedAttempt.State = tasks.AttemptStateAccepted
	acceptedAttempt.UpdatedAt = now
	meta := tasks.Metadata{
		TaskID:         assignment.Request.TaskID,
		Kind:           assignment.Request.Kind,
		Status:         tasks.StatusAccepted,
		RequesterPeer:  requester,
		WorkerPeer:     n.HostID(),
		CreatedAt:      now,
		Request:        assignment.Request,
		Idempotent:     assignment.Request.Idempotent,
		MaxAttempts:    assignment.Request.EffectiveMaxAttempts(),
		CurrentAttempt: assignment.Attempt.AttemptNumber,
		TotalAttempts:  1,
		Attempts:       []tasks.TaskAttempt{acceptedAttempt},
	}
	if existing, getErr := n.store.GetTask(assignment.Request.TaskID); getErr == nil {
		meta = existing
		meta.Status = tasks.StatusAccepted
		meta.RequesterPeer = requester
		meta.WorkerPeer = n.HostID()
		meta.Request = assignment.Request
		meta.CurrentAttempt = assignment.Attempt.AttemptNumber
		meta.TotalAttempts = len(meta.Attempts) + 1
		meta.Attempts = append(meta.Attempts, acceptedAttempt)
		meta.FinishedAt = nil
		meta.Error = ""
	}
	if err := n.store.SaveTask(meta); err != nil {
		_ = n.sendErrorForProtocol(protocolID, encoder, "persistence_error", err.Error())
		return
	}

	send := func(kind string, payload any) error {
		if protocolID == protocol.ID(TaskProtocolV2) {
			switch kind {
			case TypeTaskAccept:
				payload = TaskAcceptance{
					TaskID:       assignment.Request.TaskID,
					AttemptID:    assignment.Attempt.AttemptID,
					AttemptToken: assignment.Attempt.AttemptToken,
					WorkerID:     n.HostID(),
					SessionID:    n.phase2.sessionID,
					Timestamp:    time.Now().UTC(),
				}
			case TypeTaskResult:
				result, ok := payload.(tasks.Result)
				if !ok {
					return errors.New("invalid task result payload")
				}
				result.AttemptID = assignment.Attempt.AttemptID
				result.AttemptToken = assignment.Attempt.AttemptToken
				payload = TaskResultPayload{AttemptID: assignment.Attempt.AttemptID, AttemptToken: assignment.Attempt.AttemptToken, Result: result}
			}
		}
		reply, err := n.envelopeForProtocol(protocolID, kind, payload)
		if err != nil {
			return err
		}
		return encoder.Encode(reply)
	}

	result, runErr := n.executeLocal(withTaskAttempt(assignmentCtx, acceptedAttempt), assignment.Request, requester, n.HostID(), send)
	if result.TaskID == "" {
		result = tasks.Result{TaskID: assignment.Request.TaskID, Status: tasks.StatusFailed, Artifacts: []tasks.Artifact{}}
		if runErr != nil {
			result.Error = runErr.Error()
		}
	}
	result.AttemptID = assignment.Attempt.AttemptID
	result.AttemptToken = assignment.Attempt.AttemptToken
	_ = n.store.SaveResult(result)
	state := attemptStateForResult(result)
	_ = n.finishAttempt(assignment.Attempt, state, resultError(result), "")
	if runErr != nil && n.log != nil {
		n.log.Debug("remote task execution failed", "task_id", assignment.Request.TaskID, "attempt_id", assignment.Attempt.AttemptID, "error", runErr.Error())
	}
}

func (n *Node) handleTaskCancellation(stream network.Stream, requester string, envelope Envelope, encoder *json.Encoder) {
	var payload CancelPayload
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "bad_payload", err.Error())
		return
	}
	if payload.TaskID == "" {
		_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "bad_payload", "task id is required")
		return
	}
	pendingCanceled := false
	if stream.Protocol() == protocol.ID(TaskProtocolV2) {
		pendingCanceled = n.cancelInboundAttempt(requester, payload)
	}
	if !pendingCanceled {
		meta, err := n.store.GetTask(payload.TaskID)
		if err != nil {
			_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "task_not_found", err.Error())
			return
		}
		if meta.RequesterPeer != requester {
			_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "not_allowed", "only the task requester may cancel this task")
			return
		}
		if stream.Protocol() == protocol.ID(TaskProtocolV2) {
			if len(meta.Attempts) == 0 {
				_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "stale_cancel", "task has no active attempt")
				return
			}
			current := meta.Attempts[len(meta.Attempts)-1]
			if payload.AttemptID != current.AttemptID || payload.AttemptToken != current.AttemptToken || !activeAttemptState(current.State) {
				_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "stale_cancel", "cancel token does not match the active attempt")
				return
			}
		}
	}
	n.mu.Lock()
	cancel := n.cancels[payload.TaskID]
	n.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	event := tasks.NewEvent(payload.TaskID, 0, tasks.EventCanceled, "cancel requested by remote peer", "")
	_ = n.store.AppendEvent(event)
	reply, _ := n.envelopeForProtocol(stream.Protocol(), TypeTaskEvent, event)
	_ = encoder.Encode(reply)
}

func (n *Node) claimInboundAttempt(requester string, assignment TaskAssignment, cancel context.CancelFunc) (bool, error) {
	key := inboundAttemptKey(assignment.Attempt)
	n.phase2.mu.Lock()
	defer n.phase2.mu.Unlock()
	if _, exists := n.phase2.inbound[key]; exists {
		return false, nil
	}
	seen, err := n.assignmentAlreadySeen(requester, assignment)
	if err != nil || seen {
		return false, err
	}
	n.phase2.inbound[key] = inboundAttemptClaim{Requester: requester, Attempt: assignment.Attempt, Cancel: cancel}
	return true, nil
}

func (n *Node) cancelInboundAttempt(requester string, payload CancelPayload) bool {
	key := payload.TaskID + "\x00" + payload.AttemptID + "\x00" + payload.AttemptToken
	n.phase2.mu.RLock()
	claim, ok := n.phase2.inbound[key]
	n.phase2.mu.RUnlock()
	if !ok || claim.Requester != requester || claim.Cancel == nil {
		return false
	}
	claim.Cancel()
	return true
}

func (n *Node) releaseInboundAttempt(attempt tasks.TaskAttempt) {
	n.phase2.mu.Lock()
	delete(n.phase2.inbound, inboundAttemptKey(attempt))
	n.phase2.mu.Unlock()
}

func inboundAttemptKey(attempt tasks.TaskAttempt) string {
	return attempt.TaskID + "\x00" + attempt.AttemptID + "\x00" + attempt.AttemptToken
}

func (n *Node) assignmentAlreadySeen(requester string, assignment TaskAssignment) (bool, error) {
	attempt := assignment.Attempt
	meta, err := n.store.GetTask(attempt.TaskID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return inboundAttemptConflicts(meta, requester, assignment), nil
}

func inboundAttemptConflicts(meta tasks.Metadata, requester string, assignment TaskAssignment) bool {
	attempt := assignment.Attempt
	if meta.RequesterPeer != "" && meta.RequesterPeer != requester {
		return true
	}
	if !reflect.DeepEqual(meta.Request, assignment.Request) {
		return true
	}
	if meta.Status == tasks.StatusCompleted || meta.Status == tasks.StatusCanceled {
		return true
	}
	maxAttemptNumber := 0
	for _, existing := range meta.Attempts {
		if existing.AttemptID == attempt.AttemptID || existing.AttemptToken == attempt.AttemptToken {
			return true
		}
		if existing.AttemptNumber > maxAttemptNumber {
			maxAttemptNumber = existing.AttemptNumber
		}
	}
	if attempt.AttemptNumber <= maxAttemptNumber {
		return true
	}
	executions := countExecutionAttempts(meta.Attempts)
	if executions == 0 {
		return false
	}
	if !meta.Request.Idempotent || !assignment.Request.Idempotent {
		return true
	}
	return executions >= meta.Request.EffectiveMaxAttempts()
}

func (n *Node) sendAssignmentRejection(protocolID protocol.ID, encoder *json.Encoder, assignment TaskAssignment, reason tasks.RejectionReason, detail string) {
	rejection := tasks.TaskRejection{
		ProtocolVersion: tasks.TaskRejectionProtocolVersion,
		TaskID:          assignment.Request.TaskID,
		AttemptID:       assignment.Attempt.AttemptID,
		WorkerID:        n.HostID(),
		Reason:          reason,
		Detail:          detail,
		Retryable:       true,
		Timestamp:       time.Now().UTC(),
	}
	if err := rejection.Validate(); err != nil {
		_ = n.sendErrorForProtocol(protocolID, encoder, "internal_error", fmt.Sprintf("construct task rejection: %v", err))
		return
	}
	if protocolID == protocol.ID(TaskProtocol) {
		_ = n.sendErrorForProtocol(protocolID, encoder, string(reason), detail)
		return
	}
	reply, err := n.envelopeForProtocol(protocolID, TypeTaskReject, rejection)
	if err == nil {
		_ = encoder.Encode(reply)
	}
}

func activeAttemptState(state string) bool {
	switch state {
	case tasks.AttemptStateAccepted, tasks.AttemptStateRunning:
		return true
	default:
		return false
	}
}
