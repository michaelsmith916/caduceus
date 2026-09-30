package p2p

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/caduceus/caduceus/internal/enrollment"
	"github.com/caduceus/caduceus/internal/security"
	"github.com/caduceus/caduceus/internal/tasks"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// RequestEnrollment submits a one-time invitation over an authenticated Noise
// connection. The invitation token is confined to this encrypted stream and
// is never included in logs or the persisted request/audit records.
func (n *Node) RequestEnrollment(ctx context.Context, coordinatorAddress, token, displayName string) (EnrollmentWireDecision, error) {
	address, err := ma.NewMultiaddr(strings.TrimSpace(coordinatorAddress))
	if err != nil {
		return EnrollmentWireDecision{}, fmt.Errorf("parse coordinator address: %w", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(address)
	if err != nil {
		return EnrollmentWireDecision{}, fmt.Errorf("coordinator address must include /p2p/<peer-id>: %w", err)
	}
	if err := n.host.Connect(ctx, *info); err != nil {
		return EnrollmentWireDecision{}, err
	}
	publicKey := n.host.Peerstore().PubKey(n.host.ID())
	fingerprint, err := fingerprintPublicKey(publicKey)
	if err != nil {
		return EnrollmentWireDecision{}, err
	}
	request := EnrollmentWireRequest{
		Token:                token,
		Nonce:                tasks.NewID("nonce"),
		PeerID:               n.HostID(),
		DisplayName:          displayName,
		PublicKeyFingerprint: fingerprint,
		Timestamp:            time.Now().UTC(),
	}
	return n.exchangeEnrollment(ctx, *info, TypeEnrollmentRequest, request)
}

// EnrollmentStatus polls a previously submitted request. Knowledge of the
// unguessable request ID is necessary but not sufficient: the server also
// requires the same authenticated libp2p peer identity that submitted it.
func (n *Node) EnrollmentStatus(ctx context.Context, coordinatorAddress, requestID string) (EnrollmentWireDecision, error) {
	address, err := ma.NewMultiaddr(strings.TrimSpace(coordinatorAddress))
	if err != nil {
		return EnrollmentWireDecision{}, fmt.Errorf("parse coordinator address: %w", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(address)
	if err != nil {
		return EnrollmentWireDecision{}, err
	}
	if err := n.host.Connect(ctx, *info); err != nil {
		return EnrollmentWireDecision{}, err
	}
	return n.exchangeEnrollment(ctx, *info, TypeEnrollmentStatus, EnrollmentStatusRequest{RequestID: requestID})
}

func (n *Node) exchangeEnrollment(ctx context.Context, coordinator peer.AddrInfo, messageType string, payload any) (EnrollmentWireDecision, error) {
	stream, err := n.host.NewStream(ctx, coordinator.ID, protocol.ID(EnrollmentProtocolV1))
	if err != nil {
		return EnrollmentWireDecision{}, err
	}
	defer stream.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}
	envelope, err := n.envelope(messageType, payload)
	if err != nil {
		return EnrollmentWireDecision{}, err
	}
	if err := json.NewEncoder(stream).Encode(envelope); err != nil {
		return EnrollmentWireDecision{}, err
	}
	var reply Envelope
	if err := newEnvelopeDecoder(stream).Decode(&reply); err != nil {
		return EnrollmentWireDecision{}, err
	}
	// Enrollment intentionally precedes group membership, so normal group and
	// allowlist verification is not applicable. Noise authenticates the peer;
	// the one-time token and fingerprint bind the enrollment request.
	if err := validateEnvelopeFields(reply, coordinator.ID.String(), time.Now().UTC()); err != nil {
		return EnrollmentWireDecision{}, err
	}
	if reply.Type == TypeError {
		var problem ErrorPayload
		_ = json.Unmarshal(reply.Payload, &problem)
		return EnrollmentWireDecision{}, errors.New(problem.Message)
	}
	if reply.Type != TypeEnrollmentDecision {
		return EnrollmentWireDecision{}, fmt.Errorf("unexpected enrollment response %q", reply.Type)
	}
	var decision EnrollmentWireDecision
	if err := json.Unmarshal(reply.Payload, &decision); err != nil {
		return EnrollmentWireDecision{}, err
	}
	if decision.Status == string(enrollment.StatusApproved) {
		if strings.TrimSpace(decision.CoordinatorPeerID) != coordinator.ID.String() {
			return EnrollmentWireDecision{}, errors.New("approved enrollment coordinator does not match the authenticated peer")
		}
		coordinatorKey := n.host.Peerstore().PubKey(coordinator.ID)
		expectedFingerprint, fingerprintErr := fingerprintPublicKey(coordinatorKey)
		if fingerprintErr != nil {
			return EnrollmentWireDecision{}, fingerprintErr
		}
		if decision.CoordinatorFingerprint != expectedFingerprint {
			return EnrollmentWireDecision{}, errors.New("approved enrollment coordinator fingerprint does not match the authenticated peer")
		}
	}
	return decision, nil
}

func (n *Node) handleEnrollmentStream(stream network.Stream) {
	defer stream.Close()
	encoder := json.NewEncoder(stream)
	remote := stream.Conn().RemotePeer()
	if n.enrollment == nil || !n.enrollment.Enabled() {
		_ = n.sendError(encoder, "enrollment_disabled", enrollment.ErrDisabled.Error())
		return
	}
	var envelope Envelope
	if err := newEnvelopeDecoder(stream).Decode(&envelope); err != nil {
		_ = n.sendError(encoder, "bad_message", err.Error())
		return
	}
	if err := validateEnvelopeFields(envelope, remote.String(), time.Now().UTC()); err != nil {
		_ = n.sendError(encoder, "bad_message", err.Error())
		return
	}

	var request enrollment.Request
	var err error
	switch envelope.Type {
	case TypeEnrollmentRequest:
		request, err = n.submitEnrollment(stream, remote, envelope)
	case TypeEnrollmentStatus:
		var status EnrollmentStatusRequest
		if decodeErr := json.Unmarshal(envelope.Payload, &status); decodeErr != nil {
			err = decodeErr
			break
		}
		request, err = n.enrollment.Get(status.RequestID)
		if err == nil && request.PeerID != remote.String() {
			err = errors.New("enrollment request belongs to a different peer")
		}
	default:
		err = fmt.Errorf("unsupported enrollment message %q", envelope.Type)
	}
	if err != nil {
		_ = n.sendError(encoder, enrollmentErrorCode(err), err.Error())
		return
	}
	if envelope.Type == TypeEnrollmentRequest && request.Status == enrollment.StatusApproved {
		n.ActivateEnrollment(request)
	}
	decision := n.enrollmentDecision(request)
	reply, err := n.envelope(TypeEnrollmentDecision, decision)
	if err == nil {
		_ = encoder.Encode(reply)
	}
}

func (n *Node) submitEnrollment(stream network.Stream, remote peer.ID, envelope Envelope) (enrollment.Request, error) {
	var wire EnrollmentWireRequest
	if err := json.Unmarshal(envelope.Payload, &wire); err != nil {
		return enrollment.Request{}, err
	}
	if wire.PeerID != remote.String() {
		return enrollment.Request{}, errors.New("claimed enrollment peer does not match the authenticated connection")
	}
	if wire.Timestamp.IsZero() || wire.Timestamp.Before(time.Now().Add(-maxMessageAge)) || wire.Timestamp.After(time.Now().Add(maxClockSkew)) {
		return enrollment.Request{}, errors.New("enrollment request timestamp is outside the allowed window")
	}
	expected, err := fingerprintPublicKey(stream.Conn().RemotePublicKey())
	if err != nil {
		return enrollment.Request{}, err
	}
	if !strings.EqualFold(expected, wire.PublicKeyFingerprint) {
		return enrollment.Request{}, errors.New("enrollment public-key fingerprint does not match the authenticated peer")
	}
	ip, err := manet.ToIP(stream.Conn().RemoteMultiaddr())
	if err != nil || ip == nil {
		return enrollment.Request{}, enrollment.ErrInvalidSource
	}
	return n.enrollment.Submit(ip.String(), enrollment.Submission{
		Token:                wire.Token,
		PeerID:               wire.PeerID,
		DisplayName:          wire.DisplayName,
		PublicKeyFingerprint: wire.PublicKeyFingerprint,
		Nonce:                wire.Nonce,
	})
}

func (n *Node) enrollmentDecision(request enrollment.Request) EnrollmentWireDecision {
	decision := EnrollmentWireDecision{
		RequestID: request.ID,
		Status:    string(request.Status),
		ExpiresAt: request.ExpiresAt,
	}
	if request.Status == enrollment.StatusApproved {
		decision.CoordinatorPeerID = n.HostID()
		decision.GroupHash = n.groupID()
		decision.CoordinatorAddrs = make([]string, 0, len(n.ListenAddrs()))
		for _, address := range n.ListenAddrs() {
			decision.CoordinatorAddrs = append(decision.CoordinatorAddrs, strings.TrimRight(address, "/")+"/p2p/"+n.HostID())
		}
		decision.CoordinatorFingerprint, _ = fingerprintPublicKey(n.host.Peerstore().PubKey(n.host.ID()))
	}
	return decision
}

// ActivateEnrollment updates the in-memory authorization view after the
// manager has atomically persisted approval. It is idempotent.
func (n *Node) ActivateEnrollment(request enrollment.Request) {
	if request.Status != enrollment.StatusApproved {
		return
	}
	n.mu.Lock()
	allowed := n.allowed
	peerEntry := security.AllowedPeer{
		PeerID:               request.PeerID,
		Name:                 request.DisplayName,
		PublicKeyFingerprint: request.PublicKeyFingerprint,
		TrustLevel:           "trusted-lan",
		Allowed:              true,
		Notes:                "approved trusted-LAN enrollment " + request.ID,
	}
	found := false
	for index := range allowed.Peers {
		if allowed.Peers[index].PeerID == request.PeerID {
			allowed.Peers[index] = peerEntry
			found = true
			break
		}
	}
	if !found {
		allowed.Peers = append(allowed.Peers, peerEntry)
	}
	n.allowed = allowed
	n.mu.Unlock()
}

func fingerprintPublicKey(publicKey p2pcrypto.PubKey) (string, error) {
	if publicKey == nil {
		return "", errors.New("authenticated peer public key is unavailable")
	}
	encoded, err := p2pcrypto.MarshalPublicKey(publicKey)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func enrollmentErrorCode(err error) string {
	switch {
	case errors.Is(err, enrollment.ErrDisabled):
		return "enrollment_disabled"
	case errors.Is(err, enrollment.ErrInvalidToken), errors.Is(err, enrollment.ErrReplay), errors.Is(err, enrollment.ErrNonceReplay):
		return "invalid_or_replayed_invitation"
	case errors.Is(err, enrollment.ErrRateLimited):
		return "rate_limited"
	case errors.Is(err, enrollment.ErrSourceNotAllowed), errors.Is(err, enrollment.ErrInvalidSource):
		return "source_not_allowed"
	case errors.Is(err, enrollment.ErrExpired):
		return "expired"
	case errors.Is(err, enrollment.ErrNotFound):
		return "not_found"
	default:
		return "enrollment_rejected"
	}
}
