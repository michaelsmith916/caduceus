package security

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func TestAllowlistRequiresExplicitPeer(t *testing.T) {
	list := AllowedPeers{Peers: []AllowedPeer{{PeerID: "peer-a", Allowed: true}}}
	if !list.IsAllowed("peer-a", true) {
		t.Fatal("peer-a should be allowed")
	}
	if list.IsAllowed("peer-b", true) {
		t.Fatal("peer-b should be rejected when allowlist required")
	}
	if !list.IsAllowed("peer-b", false) {
		t.Fatal("peer-b should be allowed when allowlist disabled")
	}
}

func TestSaveAllowedPeersIsAtomicNormalizedAndDoesNotMutateCaller(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "allowed-peers.yaml")
	peers := AllowedPeers{Peers: []AllowedPeer{
		{PeerID: "peer-b", PublicKeyFingerprint: "SHA256:BB", Allowed: true},
		{PeerID: "peer-a", PublicKeyFingerprint: "SHA256:AA", Allowed: true},
	}}
	if err := SaveAllowedPeers(path, peers); err != nil {
		t.Fatal(err)
	}
	if peers.Peers[0].PeerID != "peer-b" {
		t.Fatal("SaveAllowedPeers reordered the caller's slice")
	}
	loaded, err := LoadAllowedPeers(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Peers) != 2 || loaded.Peers[0].PeerID != "peer-a" {
		t.Fatalf("loaded peers = %#v", loaded.Peers)
	}
	if loaded.Peers[0].PublicKeyFingerprint != "sha256:aa" {
		t.Fatalf("fingerprint = %q, want normalized lowercase", loaded.Peers[0].PublicKeyFingerprint)
	}
	temporary, err := filepath.Glob(filepath.Join(directory, ".allowed-peers-*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("temporary files remain after atomic write: %v", temporary)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("file mode = %o, want 600", got)
		}
	}
}

func TestSaveAllowedPeersRejectsDuplicatePeerWithoutReplacingValidFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "allowed-peers.yaml")
	original := AllowedPeers{Peers: []AllowedPeer{{PeerID: "peer-original", Allowed: true}}}
	if err := SaveAllowedPeers(path, original); err != nil {
		t.Fatal(err)
	}
	duplicates := AllowedPeers{Peers: []AllowedPeer{
		{PeerID: "peer-a", Allowed: true},
		{PeerID: " peer-a ", Allowed: true},
	}}
	if err := SaveAllowedPeers(path, duplicates); err == nil || !strings.Contains(err.Error(), "duplicate allowed peer ID") {
		t.Fatalf("duplicate save error = %v", err)
	}
	loaded, err := LoadAllowedPeers(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Peers) != 1 || loaded.Peers[0].PeerID != "peer-original" {
		t.Fatalf("valid file was replaced after rejected save: %#v", loaded.Peers)
	}
}

func TestLoadAllowedPeersRejectsDuplicatePeerIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "allowed-peers.yaml")
	data := []byte("peers:\n  - peer_id: peer-a\n    allowed: true\n  - peer_id: peer-a\n    allowed: true\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAllowedPeers(path); err == nil || !strings.Contains(err.Error(), "duplicate allowed peer ID") {
		t.Fatalf("load error = %v", err)
	}
}

func TestAddPeerRejectsDuplicatePublicKeyFingerprint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "allowed-peers.yaml")
	if err := AddPeer(path, AllowedPeer{
		PeerID:               "peer-a",
		PublicKeyFingerprint: "SHA256:AA",
	}); err != nil {
		t.Fatal(err)
	}
	err := AddPeer(path, AllowedPeer{
		PeerID:               "peer-b",
		PublicKeyFingerprint: "sha256:aa",
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate public-key fingerprint") {
		t.Fatalf("duplicate fingerprint error = %v", err)
	}
	loaded, loadErr := LoadAllowedPeers(path)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if len(loaded.Peers) != 1 || loaded.Peers[0].PeerID != "peer-a" {
		t.Fatalf("duplicate fingerprint partially updated allowlist: %#v", loaded.Peers)
	}
	if peer, ok := loaded.GetByFingerprint("SHA256:AA"); !ok || peer.PeerID != "peer-a" {
		t.Fatalf("GetByFingerprint = %#v, %v", peer, ok)
	}
}

func TestAddPeerPreservesExistingFingerprintWhenOmitted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "allowed-peers.yaml")
	if err := AddPeer(path, AllowedPeer{
		PeerID:               "peer-a",
		Name:                 "old",
		PublicKeyFingerprint: "sha256:aa",
	}); err != nil {
		t.Fatal(err)
	}
	if err := AddPeer(path, AllowedPeer{PeerID: "peer-a", Name: "new"}); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadAllowedPeers(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Peers) != 1 {
		t.Fatalf("peer count = %d, want 1", len(loaded.Peers))
	}
	if loaded.Peers[0].Name != "new" || loaded.Peers[0].PublicKeyFingerprint != "sha256:aa" {
		t.Fatalf("updated peer = %#v", loaded.Peers[0])
	}
}

func TestConcurrentAddPeerDoesNotLoseUpdates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "allowed-peers.yaml")
	const count = 12
	var wait sync.WaitGroup
	errorsByPeer := make(chan error, count)
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			peerID := "peer-" + string(rune('a'+index))
			errorsByPeer <- AddPeer(path, AllowedPeer{
				PeerID:               peerID,
				PublicKeyFingerprint: "fingerprint-" + peerID,
			})
		}(i)
	}
	wait.Wait()
	close(errorsByPeer)
	for err := range errorsByPeer {
		if err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := LoadAllowedPeers(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Peers) != count {
		t.Fatalf("peer count = %d, want %d", len(loaded.Peers), count)
	}
}
