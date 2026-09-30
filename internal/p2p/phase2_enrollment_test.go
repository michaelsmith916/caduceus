package p2p

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/caduceus/caduceus/internal/config"
	"github.com/caduceus/caduceus/internal/enrollment"
	"github.com/caduceus/caduceus/internal/openai"
	"github.com/caduceus/caduceus/internal/security"
	"github.com/caduceus/caduceus/internal/store"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const alternateTestGroupHash = "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"

func TestRequestEnrollmentDoesNotRequireLocalEnrollmentManager(t *testing.T) {
	node := &Node{}
	_, err := node.RequestEnrollment(context.Background(), "not-a-multiaddr", "secret-token", "applicant")
	if err == nil {
		t.Fatal("RequestEnrollment unexpectedly succeeded")
	}
	if errors.Is(err, enrollment.ErrDisabled) {
		t.Fatalf("RequestEnrollment incorrectly required a local enrollment manager: %v", err)
	}
	if !strings.Contains(err.Error(), "parse coordinator address") {
		t.Fatalf("RequestEnrollment error = %v, want coordinator address validation", err)
	}
}

func TestEnrollmentHelloDefersCoordinatorRegistrationUntilInitialStatus(t *testing.T) {
	coordinator := newEnrollmentTestNode(t, testGroupHash)
	applicant := newEnrollmentTestNode(t, alternateTestGroupHash)
	applicantFingerprint, err := fingerprintPublicKey(applicant.host.Peerstore().PubKey(applicant.host.ID()))
	if err != nil {
		t.Fatal(err)
	}
	coordinator.ActivateEnrollment(enrollment.Request{
		ID:                   "enroll-ordering",
		Status:               enrollment.StatusApproved,
		PeerID:               applicant.HostID(),
		PublicKeyFingerprint: applicantFingerprint,
	})
	coordinatorFingerprint, err := fingerprintPublicKey(coordinator.host.Peerstore().PubKey(coordinator.host.ID()))
	if err != nil {
		t.Fatal(err)
	}
	coordinatorPeer := security.AllowedPeer{
		PeerID:               coordinator.HostID(),
		PublicKeyFingerprint: coordinatorFingerprint,
		TrustLevel:           "trusted-lan",
		Allowed:              true,
	}
	if err := applicant.ApplyEnrollmentMembership(testGroupHash, coordinatorPeer); err != nil {
		t.Fatal(err)
	}
	address, err := ma.NewMultiaddr(enrollmentTestAddress(t, coordinator))
	if err != nil {
		t.Fatal(err)
	}
	info, err := peer.AddrInfoFromP2pAddr(address)
	if err != nil {
		t.Fatal(err)
	}
	if err := applicant.host.Connect(context.Background(), *info); err != nil {
		t.Fatal(err)
	}
	remote, err := applicant.exchangeWorkerHelloV2(context.Background(), info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := applicant.GetWorker(coordinator.HostID()); ok {
		t.Fatal("worker hello exposed coordinator before initial status acknowledgement")
	}
	status := applicant.phase2.localStatus(applicant)
	if err := applicant.phase2.sendStatus(context.Background(), applicant, remote, status); err != nil {
		t.Fatal(err)
	}
	if _, ok := applicant.GetWorker(coordinator.HostID()); ok {
		t.Fatal("initial status unexpectedly registered coordinator before explicit commit")
	}
	applicant.registerWorker(remote, coordinator.HostID())
	if _, ok := applicant.phase2.registry.Get(coordinator.HostID()); !ok {
		t.Fatal("coordinator was not registrable after initial status acknowledgement")
	}
	seenApplicant, ok := coordinator.phase2.registry.Get(applicant.HostID())
	if !ok || seenApplicant.Sequence != status.Sequence {
		t.Fatalf("coordinator status = %#v, want acknowledged initial sequence %d", seenApplicant, status.Sequence)
	}
}

func TestEnrollmentAnnouncementImmediatelyRegistersAndHeartbeats(t *testing.T) {
	coordinator := newEnrollmentTestNode(t, testGroupHash)
	applicant := newEnrollmentTestNode(t, alternateTestGroupHash)
	applicantFingerprint, err := fingerprintPublicKey(applicant.host.Peerstore().PubKey(applicant.host.ID()))
	if err != nil {
		t.Fatal(err)
	}
	coordinator.ActivateEnrollment(enrollment.Request{
		ID:                   "enroll-integration",
		Status:               enrollment.StatusApproved,
		PeerID:               applicant.HostID(),
		DisplayName:          "applicant",
		PublicKeyFingerprint: applicantFingerprint,
	})
	coordinatorFingerprint, err := fingerprintPublicKey(coordinator.host.Peerstore().PubKey(coordinator.host.ID()))
	if err != nil {
		t.Fatal(err)
	}
	coordinatorPeer := security.AllowedPeer{
		PeerID:               coordinator.HostID(),
		PublicKeyFingerprint: coordinatorFingerprint,
		TrustLevel:           "trusted-lan",
		Allowed:              true,
	}
	primary := enrollmentTestAddress(t, coordinator)
	if err := applicant.ValidateEnrollmentCoordinator(testGroupHash, coordinatorPeer, primary, []string{strings.TrimSuffix(primary, "/p2p/"+coordinator.HostID())}); err != nil {
		t.Fatal(err)
	}
	if err := applicant.ApplyEnrollmentMembership(testGroupHash, coordinatorPeer); err != nil {
		t.Fatal(err)
	}
	if err := applicant.AnnounceEnrollmentMembership(context.Background(), coordinatorPeer, primary, nil); err != nil {
		t.Fatal(err)
	}

	local, ok := applicant.GetWorker(applicant.HostID())
	if !ok || local.GroupHash != testGroupHash {
		t.Fatalf("applicant local registry membership = %#v", local)
	}
	seenApplicant, ok := coordinator.GetWorker(applicant.HostID())
	if !ok {
		t.Fatal("coordinator did not register the approved applicant synchronously")
	}
	if seenApplicant.GroupHash != testGroupHash || seenApplicant.Sequence == 0 || seenApplicant.StatusTimestamp.IsZero() {
		t.Fatalf("coordinator applicant status = %#v, want immediate v2 heartbeat", seenApplicant)
	}
	if _, ok := applicant.GetWorker(coordinator.HostID()); !ok {
		t.Fatal("applicant did not register the coordinator hello response")
	}
	if addresses := applicant.host.Peerstore().Addrs(coordinator.host.ID()); len(addresses) == 0 {
		t.Fatal("coordinator addresses were not retained in the applicant peerstore")
	}
}

func TestEnrollmentCoordinatorValidationRejectsIdentityMismatch(t *testing.T) {
	coordinatorKey, _, err := p2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	coordinatorID, err := peer.IDFromPrivateKey(coordinatorKey)
	if err != nil {
		t.Fatal(err)
	}
	otherKey, _, err := p2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherID, err := peer.IDFromPrivateKey(otherKey)
	if err != nil {
		t.Fatal(err)
	}
	coordinator := security.AllowedPeer{PeerID: coordinatorID.String(), PublicKeyFingerprint: strings.Repeat("a", 64)}
	address := fmt.Sprintf("/ip4/127.0.0.1/tcp/1234/p2p/%s", otherID)
	if _, _, err := validateEnrollmentMembership(testGroupHash, coordinator); err != nil {
		t.Fatal(err)
	}
	if _, err := enrollmentCoordinatorInfo(coordinatorID, address, nil); err == nil {
		t.Fatal("mismatched address peer ID was accepted")
	}
	coordinator.PublicKeyFingerprint = strings.Repeat("A", 64)
	if _, _, err := validateEnrollmentMembership(testGroupHash, coordinator); err == nil {
		t.Fatal("non-canonical coordinator fingerprint was accepted")
	}
}

func newEnrollmentTestNode(t *testing.T, groupHash string) *Node {
	t.Helper()
	cfg, err := config.Default()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Node.EnableMDNS = false
	cfg.Node.ListenAddrs = []string{"/ip4/127.0.0.1/tcp/0"}
	cfg.Security.P2PKeyHash = groupHash
	cfg.Security.RequireAllowlist = true
	cfg.OpenAI.BaseURL = "http://127.0.0.1:1/v1"
	cfg.Storage.DataDir = t.TempDir()
	identity, _, err := p2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	taskStore := store.New(cfg.Storage.DataDir)
	if err := taskStore.Init(); err != nil {
		t.Fatal(err)
	}
	node, err := New(context.Background(), Options{
		Config:    cfg,
		Identity:  identity,
		GroupHash: groupHash,
		Allowed:   security.AllowedPeers{Peers: []security.AllowedPeer{}},
		Store:     taskStore,
		OpenAI:    openai.New(cfg.OpenAI.BaseURL, "", cfg.OpenAI.DefaultModel, cfg.OpenAITimeout()),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	return node
}

func enrollmentTestAddress(t *testing.T, node *Node) string {
	t.Helper()
	for _, address := range node.host.Network().ListenAddresses() {
		if strings.HasPrefix(address.String(), "/ip4/127.0.0.1/") {
			return address.Encapsulate(ma.StringCast("/p2p/" + node.HostID())).String()
		}
	}
	t.Fatal("test node has no loopback listener")
	return ""
}
