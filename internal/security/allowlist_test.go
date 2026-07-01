package security

import "testing"

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
