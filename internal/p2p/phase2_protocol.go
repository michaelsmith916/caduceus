package p2p

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/caduceus/caduceus/internal/workers"
	"github.com/caduceus/caduceus/pkg/caduceus"
	libp2pprotocol "github.com/libp2p/go-libp2p/core/protocol"
)

const (
	WorkerProtocolV2     = "/caduceus/worker/2.0.0"
	TaskProtocolV2       = "/caduceus/task/2.0.0"
	EnrollmentProtocolV1 = "/caduceus/enrollment/1.0.0"

	TypeWorkerStatusAck    = "worker_status_ack"
	TypeTaskReject         = "task_reject"
	TypeTaskInterrupt      = "task_interrupt"
	TypeEnrollmentRequest  = "enrollment_request"
	TypeEnrollmentDecision = "enrollment_decision"
	TypeEnrollmentStatus   = "enrollment_status"
	TypeRoutingExplanation = "routing_explanation"

	MaxEnvelopeBytes = 1 << 20
	maxClockSkew     = 5 * time.Minute
	maxMessageAge    = 24 * time.Hour
)

type StatusAck struct {
	WorkerID   string    `json:"worker_id"`
	SessionID  string    `json:"session_id"`
	Sequence   uint64    `json:"sequence"`
	ReceivedAt time.Time `json:"received_at"`
}

type TaskAssignment struct {
	Request tasks.Request     `json:"request"`
	Attempt tasks.TaskAttempt `json:"attempt"`
}

type TaskAcceptance struct {
	TaskID       string    `json:"task_id"`
	AttemptID    string    `json:"attempt_id"`
	AttemptToken string    `json:"attempt_token"`
	WorkerID     string    `json:"worker_id"`
	SessionID    string    `json:"session_id"`
	Timestamp    time.Time `json:"timestamp"`
}

type TaskResultPayload struct {
	AttemptID    string       `json:"attempt_id"`
	AttemptToken string       `json:"attempt_token"`
	Result       tasks.Result `json:"result"`
}

type TaskInterruption struct {
	TaskID       string    `json:"task_id"`
	AttemptID    string    `json:"attempt_id"`
	AttemptToken string    `json:"attempt_token,omitempty"`
	WorkerID     string    `json:"worker_id"`
	Reason       string    `json:"reason"`
	Retryable    bool      `json:"retryable"`
	Timestamp    time.Time `json:"timestamp"`
}

// EnrollmentWireRequest deliberately excludes private keys and group secrets.
// Token is accepted only on the enrollment stream and must never be logged.
type EnrollmentWireRequest struct {
	Token                string    `json:"token"`
	Nonce                string    `json:"nonce"`
	PeerID               string    `json:"peer_id"`
	DisplayName          string    `json:"display_name"`
	PublicKeyFingerprint string    `json:"public_key_fingerprint"`
	Timestamp            time.Time `json:"timestamp"`
}

type EnrollmentStatusRequest struct {
	RequestID string `json:"request_id"`
}

type EnrollmentWireDecision struct {
	RequestID              string    `json:"request_id"`
	Status                 string    `json:"status"`
	Detail                 string    `json:"detail,omitempty"`
	ExpiresAt              time.Time `json:"expires_at,omitempty"`
	CoordinatorPeerID      string    `json:"coordinator_peer_id,omitempty"`
	CoordinatorFingerprint string    `json:"coordinator_fingerprint,omitempty"`
	CoordinatorAddrs       []string  `json:"coordinator_addrs,omitempty"`
	GroupHash              string    `json:"group_hash,omitempty"`
}

type envelopeDecoder struct {
	reader *bufio.Reader
}

func newEnvelopeDecoder(reader io.Reader) *envelopeDecoder {
	return &envelopeDecoder{reader: bufio.NewReaderSize(reader, MaxEnvelopeBytes+1)}
}

func (d *envelopeDecoder) Decode(envelope *Envelope) error {
	if d == nil || d.reader == nil {
		return errors.New("nil envelope decoder")
	}
	line, err := d.reader.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) || len(line) > MaxEnvelopeBytes {
		return fmt.Errorf("envelope exceeds %d bytes", MaxEnvelopeBytes)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		return errors.New("empty envelope")
	}
	if !json.Valid(line) {
		return errors.New("invalid envelope JSON")
	}
	if unmarshalErr := json.Unmarshal(line, envelope); unmarshalErr != nil {
		return unmarshalErr
	}
	return nil
}

func envelopeVersionForProtocol(protocolID libp2pprotocol.ID) string {
	switch protocolID {
	case libp2pprotocol.ID(WorkerProtocol), libp2pprotocol.ID(TaskProtocol), libp2pprotocol.ID(EventsProtocol):
		return caduceus.ProtocolV1
	default:
		return caduceus.Protocol
	}
}

func (n *Node) envelopeForProtocol(protocolID libp2pprotocol.ID, kind string, payload any) (Envelope, error) {
	return newEnvelopeVersion(envelopeVersionForProtocol(protocolID), kind, n.HostID(), n.groupID(), payload)
}

func (n *Node) sendErrorForProtocol(protocolID libp2pprotocol.ID, encoder *json.Encoder, code, message string) error {
	envelope, err := n.envelopeForProtocol(protocolID, TypeError, ErrorPayload{Code: code, Message: message})
	if err != nil {
		return err
	}
	return encoder.Encode(envelope)
}

func validateEnvelopeFields(envelope Envelope, remotePeerID string, now time.Time) error {
	if envelope.Version != caduceus.Protocol && envelope.Version != caduceus.ProtocolV1 {
		return fmt.Errorf("unsupported protocol version %q", envelope.Version)
	}
	if !validEnvelopeType(envelope.Type) {
		return fmt.Errorf("invalid envelope type %q", envelope.Type)
	}
	if len(envelope.ID) == 0 || len(envelope.ID) > 256 || strings.ContainsAny(envelope.ID, "\x00\r\n") {
		return errors.New("invalid envelope id")
	}
	timestamp, err := time.Parse(time.RFC3339Nano, envelope.Timestamp)
	if err != nil {
		return errors.New("invalid envelope timestamp")
	}
	if now.IsZero() {
		now = time.Now()
	}
	if timestamp.After(now.Add(maxClockSkew)) || timestamp.Before(now.Add(-maxMessageAge)) {
		return errors.New("envelope timestamp outside allowed window")
	}
	if strings.TrimSpace(envelope.SenderPeerID) == "" || len(envelope.SenderPeerID) > 512 {
		return errors.New("invalid sender peer id")
	}
	if remotePeerID != "" && envelope.SenderPeerID != remotePeerID {
		return errors.New("sender peer id mismatch")
	}
	if len(envelope.GroupHash) != 64 {
		return errors.New("invalid group hash")
	}
	decoded, err := hex.DecodeString(envelope.GroupHash)
	if err != nil || len(decoded) != 32 || strings.ToLower(envelope.GroupHash) != envelope.GroupHash {
		return errors.New("invalid group hash")
	}
	if len(envelope.Payload) == 0 || len(envelope.Payload) > MaxEnvelopeBytes || !json.Valid(envelope.Payload) {
		return errors.New("invalid envelope payload")
	}
	return nil
}

func validEnvelopeType(messageType string) bool {
	switch messageType {
	case TypeWorkerHello, TypeWorkerStatus, TypeWorkerStatusAck,
		TypeTaskRequest, TypeTaskAccept, TypeTaskReject, TypeTaskEvent, TypeTaskResult,
		TypeTaskCancel, TypeTaskInterrupt, TypeEnrollmentRequest, TypeEnrollmentDecision,
		TypeEnrollmentStatus, TypeRoutingExplanation, TypeError:
		return true
	default:
		return false
	}
}

func validateStatusAdvertisement(status workers.StatusAdvertisement) error {
	if strings.TrimSpace(status.WorkerID) == "" || strings.TrimSpace(status.PeerID) == "" {
		return errors.New("worker and peer ids are required")
	}
	if strings.TrimSpace(status.SessionID) == "" || strings.TrimSpace(status.ProtocolVersion) == "" {
		return errors.New("session and protocol version are required")
	}
	if status.Sequence == 0 || status.Timestamp.IsZero() || !status.State.Valid() {
		return errors.New("invalid worker status sequence, timestamp, or state")
	}
	return nil
}

func validateTaskAssignment(assignment TaskAssignment) error {
	if err := tasks.ValidateRequest(assignment.Request); err != nil {
		return err
	}
	if err := assignment.Attempt.Validate(); err != nil {
		return err
	}
	if assignment.Attempt.TaskID != assignment.Request.TaskID {
		return errors.New("attempt task id does not match request")
	}
	return nil
}
