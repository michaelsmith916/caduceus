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

	"github.com/caduceus/caduceus/internal/security"
	"github.com/caduceus/caduceus/internal/workers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
)

const enrollmentJoinTimeout = 10 * time.Second

func (n *Node) groupID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.groupHash
}

// ApplyEnrollmentMembership activates approved membership without restarting
// the daemon. The group hash is a routing namespace, not an authentication
// secret; authorization remains bound to Noise peer identity and allowlists.
func (n *Node) ApplyEnrollmentMembership(groupHash string, coordinator security.AllowedPeer) error {
	normalizedGroup, _, err := validateEnrollmentMembership(groupHash, coordinator)
	if err != nil {
		return err
	}
	groupHash = normalizedGroup
	coordinator.Allowed = true
	if coordinator.TrustLevel == "" {
		coordinator.TrustLevel = "trusted-lan"
	}
	n.mu.Lock()
	n.groupHash = groupHash
	found := false
	for index := range n.allowed.Peers {
		if n.allowed.Peers[index].PeerID == coordinator.PeerID {
			n.allowed.Peers[index] = coordinator
			found = true
			break
		}
	}
	if !found {
		n.allowed.Peers = append(n.allowed.Peers, coordinator)
	}
	n.mu.Unlock()
	n.refreshLocalEnrollmentMembership(groupHash)
	return nil
}

// ValidateEnrollmentCoordinator validates all durable trust material before
// the application changes its config or allowlist.
func (n *Node) ValidateEnrollmentCoordinator(groupHash string, coordinator security.AllowedPeer, primaryAddress string, advertisedAddresses []string) error {
	_, coordinatorID, err := validateEnrollmentMembership(groupHash, coordinator)
	if err != nil {
		return err
	}
	_, err = enrollmentCoordinatorInfo(coordinatorID, primaryAddress, advertisedAddresses)
	return err
}

// AnnounceEnrollmentMembership connects to the approved coordinator and
// synchronously performs a v2 hello followed by a status heartbeat. Durable
// membership is intentionally left intact if this transient network step
// fails, so the operation can be retried safely.
func (n *Node) AnnounceEnrollmentMembership(ctx context.Context, coordinator security.AllowedPeer, primaryAddress string, advertisedAddresses []string) error {
	if n == nil || n.host == nil || n.phase2 == nil {
		return errors.New("p2p node is not ready for enrollment activation")
	}
	_, coordinatorID, err := validateEnrollmentMembership(n.groupID(), coordinator)
	if err != nil {
		return err
	}
	info, err := enrollmentCoordinatorInfo(coordinatorID, primaryAddress, advertisedAddresses)
	if err != nil {
		return err
	}
	entry, exists, allowed := n.allowedPeer(coordinatorID.String())
	if !exists || !allowed || entry.PublicKeyFingerprint != coordinator.PublicKeyFingerprint {
		return errors.New("approved coordinator is not active in the local allowlist")
	}
	ctx, cancel := enrollmentAnnouncementContext(ctx)
	defer cancel()
	n.host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
	if err := n.host.Connect(ctx, info); err != nil {
		return fmt.Errorf("connect to approved coordinator: %w", err)
	}
	remote, err := n.exchangeWorkerHelloV2(ctx, info.ID)
	if err != nil {
		return err
	}
	status := n.phase2.localStatus(n)
	if err := n.phase2.sendStatus(ctx, n, remote, status); err != nil {
		return fmt.Errorf("send initial worker status: %w", err)
	}
	// Do not expose the coordinator to periodic heartbeat fanout until the
	// synchronous initial status has been acknowledged. Otherwise a later
	// sequence can overtake this one and make an otherwise successful join
	// appear to fail with an out-of-order acknowledgement.
	n.registerWorker(remote, coordinatorID.String())
	if _, ok := n.phase2.registry.Get(coordinatorID.String()); !ok {
		n.removeLegacyWorker(coordinatorID.String())
		return errors.New("approved coordinator was not registered after initial worker status")
	}
	return nil
}

func (n *Node) refreshLocalEnrollmentMembership(groupHash string) {
	if n == nil || n.phase2 == nil {
		return
	}
	worker, ok := n.phase2.registry.Get(n.HostID())
	if !ok {
		return
	}
	worker.GroupHash = groupHash
	worker.LastSeen = time.Now().UTC()
	n.setLegacyWorker(worker)
	n.registerPhase2Worker(worker)
}

func (n *Node) exchangeWorkerHelloV2(ctx context.Context, coordinatorID peer.ID) (workers.Worker, error) {
	stream, err := n.host.NewStream(ctx, coordinatorID, protocol.ID(WorkerProtocolV2))
	if err != nil {
		return workers.Worker{}, fmt.Errorf("open worker hello stream: %w", err)
	}
	defer stream.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}
	stopReset := context.AfterFunc(ctx, func() { _ = stream.Reset() })
	defer stopReset()
	envelope, err := n.envelopeForProtocol(stream.Protocol(), TypeWorkerHello, n.selfWorker(ctx))
	if err != nil {
		return workers.Worker{}, err
	}
	if err := json.NewEncoder(stream).Encode(envelope); err != nil {
		return workers.Worker{}, err
	}
	var reply Envelope
	if err := newEnvelopeDecoder(stream).Decode(&reply); err != nil {
		return workers.Worker{}, err
	}
	if err := n.verifyEnvelope(reply, coordinatorID); err != nil {
		return workers.Worker{}, err
	}
	if reply.Type == TypeError {
		var problem ErrorPayload
		_ = json.Unmarshal(reply.Payload, &problem)
		if strings.TrimSpace(problem.Code) == "" {
			problem.Code = "worker_hello_rejected"
		}
		return workers.Worker{}, &workerProtocolError{Code: problem.Code}
	}
	if reply.Type != TypeWorkerHello {
		return workers.Worker{}, fmt.Errorf("unexpected worker hello response %q", reply.Type)
	}
	var remote workers.Worker
	if err := json.Unmarshal(reply.Payload, &remote); err != nil {
		return workers.Worker{}, err
	}
	// The authenticated stream identity is authoritative over payload fields.
	// Registration is deliberately deferred until the initial status ack.
	remote.WorkerID = coordinatorID.String()
	remote.PeerID = coordinatorID.String()
	return remote, nil
}

func validateEnrollmentMembership(groupHash string, coordinator security.AllowedPeer) (string, peer.ID, error) {
	groupHash = strings.TrimSpace(groupHash)
	decoded, err := hex.DecodeString(groupHash)
	if err != nil || len(decoded) != sha256.Size || strings.ToLower(groupHash) != groupHash {
		return "", "", errors.New("approved enrollment returned an invalid group hash")
	}
	coordinatorID, err := peer.Decode(strings.TrimSpace(coordinator.PeerID))
	if err != nil {
		return "", "", errors.New("approved enrollment returned an invalid coordinator peer ID")
	}
	fingerprint := strings.TrimSpace(coordinator.PublicKeyFingerprint)
	decodedFingerprint, err := hex.DecodeString(fingerprint)
	if err != nil || len(decodedFingerprint) != sha256.Size || strings.ToLower(fingerprint) != fingerprint {
		return "", "", errors.New("approved enrollment returned an invalid coordinator fingerprint")
	}
	return groupHash, coordinatorID, nil
}

func enrollmentCoordinatorInfo(coordinatorID peer.ID, primaryAddress string, advertisedAddresses []string) (peer.AddrInfo, error) {
	if len(advertisedAddresses) > 64 {
		return peer.AddrInfo{}, errors.New("approved enrollment returned too many coordinator addresses")
	}
	candidates := make([]string, 0, len(advertisedAddresses)+1)
	candidates = append(candidates, primaryAddress)
	candidates = append(candidates, advertisedAddresses...)
	seen := make(map[string]struct{}, len(candidates))
	addresses := make([]ma.Multiaddr, 0, len(candidates))
	for _, raw := range candidates {
		raw = strings.TrimSpace(raw)
		if raw == "" || len(raw) > 2048 || strings.ContainsAny(raw, "\x00\r\n") {
			return peer.AddrInfo{}, errors.New("approved enrollment returned an invalid coordinator address")
		}
		address, err := ma.NewMultiaddr(raw)
		if err != nil {
			return peer.AddrInfo{}, fmt.Errorf("parse approved coordinator address: %w", err)
		}
		if withPeer, parseErr := peer.AddrInfoFromP2pAddr(address); parseErr == nil {
			if withPeer.ID != coordinatorID {
				return peer.AddrInfo{}, errors.New("approved coordinator address peer ID does not match the authenticated coordinator")
			}
			for _, base := range withPeer.Addrs {
				if _, duplicate := seen[base.String()]; !duplicate {
					seen[base.String()] = struct{}{}
					addresses = append(addresses, base)
				}
			}
			continue
		}
		if _, valueErr := address.ValueForProtocol(ma.P_P2P); valueErr == nil {
			return peer.AddrInfo{}, errors.New("approved enrollment returned a malformed coordinator peer address")
		}
		if _, duplicate := seen[address.String()]; !duplicate {
			seen[address.String()] = struct{}{}
			addresses = append(addresses, address)
		}
	}
	if len(addresses) == 0 {
		return peer.AddrInfo{}, errors.New("approved enrollment returned no coordinator addresses")
	}
	return peer.AddrInfo{ID: coordinatorID, Addrs: addresses}, nil
}

func enrollmentAnnouncementContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, enrollmentJoinTimeout)
}
