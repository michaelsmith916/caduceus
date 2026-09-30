package p2p

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/caduceus/caduceus/internal/workers"
	"github.com/caduceus/caduceus/pkg/caduceus"
	libp2pprotocol "github.com/libp2p/go-libp2p/core/protocol"
)

const testGroupHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestPhase2EnvelopeValidation(t *testing.T) {
	now := time.Now().UTC()
	envelope, err := newEnvelope(TypeWorkerStatus, "peer-a", testGroupHash, map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Version != caduceus.Protocol {
		t.Fatalf("version = %q", envelope.Version)
	}
	if err := validateEnvelopeFields(envelope, "peer-a", now); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Envelope){
		"version":   func(e *Envelope) { e.Version = "99" },
		"type":      func(e *Envelope) { e.Type = "made_up" },
		"sender":    func(e *Envelope) { e.SenderPeerID = "peer-b" },
		"group":     func(e *Envelope) { e.GroupHash = "private-address-is-not-trust" },
		"timestamp": func(e *Envelope) { e.Timestamp = now.Add(10 * time.Minute).Format(time.RFC3339Nano) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := envelope
			mutate(&changed)
			if err := validateEnvelopeFields(changed, "peer-a", now); err == nil {
				t.Fatal("expected validation failure")
			}
		})
	}
}

func TestEnvelopeDecoderBoundsMessages(t *testing.T) {
	decoder := newEnvelopeDecoder(strings.NewReader(strings.Repeat("x", MaxEnvelopeBytes+2) + "\n"))
	if err := decoder.Decode(&Envelope{}); err == nil {
		t.Fatal("expected oversized envelope rejection")
	}
	envelope, err := newEnvelope(TypeTaskCancel, "peer-a", testGroupHash, CancelPayload{TaskID: "task-1"})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(envelope)
	decoder = newEnvelopeDecoder(bytes.NewReader(append(data, '\n')))
	var decoded Envelope
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ID != envelope.ID {
		t.Fatalf("decoded id = %q", decoded.ID)
	}
}

func TestEnvelopeVersionFollowsNegotiatedProtocol(t *testing.T) {
	tests := []struct {
		protocol libp2pprotocol.ID
		want     string
	}{
		{protocol: libp2pprotocol.ID(WorkerProtocol), want: caduceus.ProtocolV1},
		{protocol: libp2pprotocol.ID(TaskProtocol), want: caduceus.ProtocolV1},
		{protocol: libp2pprotocol.ID(EventsProtocol), want: caduceus.ProtocolV1},
		{protocol: libp2pprotocol.ID(WorkerProtocolV2), want: caduceus.Protocol},
		{protocol: libp2pprotocol.ID(TaskProtocolV2), want: caduceus.Protocol},
		{protocol: libp2pprotocol.ID(EnrollmentProtocolV1), want: caduceus.Protocol},
	}
	for _, test := range tests {
		if got := envelopeVersionForProtocol(test.protocol); got != test.want {
			t.Fatalf("envelopeVersionForProtocol(%q) = %q, want %q", test.protocol, got, test.want)
		}
	}
}

func TestStatusAndAssignmentValidation(t *testing.T) {
	now := time.Now().UTC()
	accepting := true
	status := workers.StatusAdvertisement{
		WorkerID: "worker-a", PeerID: "peer-a", SessionID: "session-a",
		ProtocolVersion: caduceus.Protocol, Sequence: 1, Timestamp: now,
		State: workers.StateAvailable, AcceptingWork: &accepting,
	}
	if err := validateStatusAdvertisement(status); err != nil {
		t.Fatal(err)
	}
	status.Sequence = 0
	if err := validateStatusAdvertisement(status); err == nil {
		t.Fatal("expected zero sequence rejection")
	}

	request := tasks.Request{TaskID: "task-1", Kind: tasks.KindPrompt, Task: tasks.PromptTask{Prompt: "hello"}}
	attempt, err := tasks.NewAttempt(request.TaskID, "worker-a", "session-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateTaskAssignment(TaskAssignment{Request: request, Attempt: attempt}); err != nil {
		t.Fatal(err)
	}
	attempt.TaskID = "task-2"
	if err := validateTaskAssignment(TaskAssignment{Request: request, Attempt: attempt}); err == nil {
		t.Fatal("expected mismatched task id rejection")
	}
}
