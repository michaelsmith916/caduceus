package p2p

import (
	"encoding/json"
	"time"

	"github.com/caduceus/caduceus/pkg/caduceus"
)

const (
	WorkerProtocol = "/caduceus/worker/1.0.0"
	TaskProtocol   = "/caduceus/task/1.0.0"
	EventsProtocol = "/caduceus/events/1.0.0"

	TypeWorkerHello  = "worker_hello"
	TypeWorkerStatus = "worker_status"
	TypeTaskRequest  = "task_request"
	TypeTaskAccept   = "task_accept"
	TypeTaskEvent    = "task_event"
	TypeTaskResult   = "task_result"
	TypeTaskCancel   = "task_cancel"
	TypeError        = "error"
)

type Envelope struct {
	Version      string          `json:"version"`
	Type         string          `json:"type"`
	ID           string          `json:"id"`
	Timestamp    string          `json:"timestamp"`
	SenderPeerID string          `json:"sender_peer_id"`
	GroupHash    string          `json:"group_hash"`
	Payload      json.RawMessage `json:"payload"`
}

type ErrorPayload struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

type CancelPayload struct {
	TaskID string `json:"task_id"`
}

func newEnvelope(kind, sender, group string, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		Version:      caduceus.Protocol,
		Type:         kind,
		ID:           "msg-" + time.Now().UTC().Format("20060102T150405.000000000"),
		Timestamp:    time.Now().UTC().Format(time.RFC3339Nano),
		SenderPeerID: sender,
		GroupHash:    group,
		Payload:      raw,
	}, nil
}
