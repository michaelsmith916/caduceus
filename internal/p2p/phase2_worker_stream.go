package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/caduceus/caduceus/internal/registry"
	"github.com/caduceus/caduceus/internal/workers"
	"github.com/libp2p/go-libp2p/core/network"
)

func (n *Node) handleWorkerStreamPhase2(stream network.Stream) {
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
	case TypeWorkerHello:
		var worker workers.Worker
		if err := json.Unmarshal(envelope.Payload, &worker); err != nil {
			_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "bad_payload", err.Error())
			return
		}
		n.registerWorker(worker, remote.String())
		reply, err := n.envelopeForProtocol(stream.Protocol(), TypeWorkerHello, n.selfWorker(context.Background()))
		if err == nil {
			_ = encoder.Encode(reply)
		}
	case TypeWorkerStatus:
		var status workers.StatusAdvertisement
		if err := json.Unmarshal(envelope.Payload, &status); err != nil {
			_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "bad_payload", err.Error())
			return
		}
		if status.WorkerID != remote.String() || status.PeerID != remote.String() {
			_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "identity_mismatch", "worker status identity does not match authenticated peer")
			return
		}
		if err := validateStatusAdvertisement(status); err != nil {
			_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "invalid_status", err.Error())
			return
		}
		if err := n.applyWorkerStatus(status); err != nil {
			code := "invalid_status"
			if errors.Is(err, registry.ErrOutOfOrder) {
				code = "out_of_order_status"
			} else if errors.Is(err, registry.ErrOldSession) || errors.Is(err, registry.ErrSessionMismatch) {
				code = "stale_session"
			}
			_ = n.sendErrorForProtocol(stream.Protocol(), encoder, code, err.Error())
			return
		}
		ack := StatusAck{WorkerID: status.WorkerID, SessionID: status.SessionID, Sequence: status.Sequence, ReceivedAt: time.Now().UTC()}
		reply, err := n.envelopeForProtocol(stream.Protocol(), TypeWorkerStatusAck, ack)
		if err == nil {
			_ = encoder.Encode(reply)
		}
	default:
		_ = n.sendErrorForProtocol(stream.Protocol(), encoder, "bad_message", "expected worker_hello or worker_status")
	}
}
