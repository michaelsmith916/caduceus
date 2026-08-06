package app

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/caduceus/caduceus/internal/config"
	"github.com/caduceus/caduceus/internal/security"
)

const (
	testOldGroupHash = "1111111111111111111111111111111111111111111111111111111111111111"
	testNewGroupHash = "2222222222222222222222222222222222222222222222222222222222222222"
	testFingerprint  = "3333333333333333333333333333333333333333333333333333333333333333"
)

func TestCommitEnrollmentMembershipRollsBackBothFiles(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "config.yaml")
	allowedPath := filepath.Join(directory, "allowed-peers.yaml")
	oldConfig, err := config.Default()
	if err != nil {
		t.Fatal(err)
	}
	oldConfig.Security.P2PKeyHash = testOldGroupHash
	oldConfig.Security.AllowedPeersPath = allowedPath
	oldConfig.Storage.DataDir = filepath.Join(directory, "data")
	oldPeers := security.AllowedPeers{Peers: []security.AllowedPeer{{PeerID: "existing-peer", TrustLevel: "trusted-lan", Allowed: true}}}
	if err := config.Save(configPath, oldConfig); err != nil {
		t.Fatal(err)
	}
	if err := security.SaveAllowedPeers(allowedPath, oldPeers); err != nil {
		t.Fatal(err)
	}
	newConfig := oldConfig
	newConfig.Security.P2PKeyHash = testNewGroupHash
	coordinator := security.AllowedPeer{PeerID: "coordinator-peer", PublicKeyFingerprint: testFingerprint, TrustLevel: "trusted-lan", Allowed: true}
	wantErr := errors.New("activation failed")
	err = commitEnrollmentMembership(configPath, oldConfig, newConfig, oldPeers, coordinator, func() error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("commit error = %v, want activation failure", err)
	}
	assertPersistedMembership(t, configPath, allowedPath, testOldGroupHash, false)
}

func TestCommitEnrollmentMembershipPersistsBothFiles(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "config.yaml")
	allowedPath := filepath.Join(directory, "allowed-peers.yaml")
	oldConfig, err := config.Default()
	if err != nil {
		t.Fatal(err)
	}
	oldConfig.Security.P2PKeyHash = testOldGroupHash
	oldConfig.Security.AllowedPeersPath = allowedPath
	oldConfig.Storage.DataDir = filepath.Join(directory, "data")
	oldPeers := security.AllowedPeers{Peers: []security.AllowedPeer{}}
	newConfig := oldConfig
	newConfig.Security.P2PKeyHash = testNewGroupHash
	coordinator := security.AllowedPeer{PeerID: "coordinator-peer", PublicKeyFingerprint: testFingerprint, Allowed: true}
	if err := commitEnrollmentMembership(configPath, oldConfig, newConfig, oldPeers, coordinator, nil); err != nil {
		t.Fatal(err)
	}
	assertPersistedMembership(t, configPath, allowedPath, testNewGroupHash, true)
}

func assertPersistedMembership(t *testing.T, configPath, allowedPath, wantGroup string, wantCoordinator bool) {
	t.Helper()
	gotConfig, _, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if gotConfig.Security.P2PKeyHash != wantGroup {
		t.Fatalf("persisted group = %q, want %q", gotConfig.Security.P2PKeyHash, wantGroup)
	}
	gotPeers, err := security.LoadAllowedPeers(allowedPath)
	if err != nil {
		t.Fatal(err)
	}
	_, exists := gotPeers.Get("coordinator-peer")
	if exists != wantCoordinator {
		t.Fatalf("coordinator persisted = %t, want %t", exists, wantCoordinator)
	}
}
