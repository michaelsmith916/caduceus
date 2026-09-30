package security

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

type AllowedPeers struct {
	Peers []AllowedPeer `json:"peers" yaml:"peers"`
}

type AllowedPeer struct {
	PeerID               string `json:"peer_id" yaml:"peer_id"`
	Name                 string `json:"name" yaml:"name"`
	PublicKeyFingerprint string `json:"public_key_fingerprint,omitempty" yaml:"public_key_fingerprint,omitempty"`
	TrustLevel           string `json:"trust_level" yaml:"trust_level"`
	Allowed              bool   `json:"allowed" yaml:"allowed"`
	Notes                string `json:"notes" yaml:"notes"`
}

var allowedPeersMu sync.Mutex

func LoadAllowedPeers(path string) (AllowedPeers, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return AllowedPeers{Peers: []AllowedPeer{}}, nil
		}
		return AllowedPeers{}, err
	}
	var peers AllowedPeers
	if err := yaml.Unmarshal(data, &peers); err != nil {
		return AllowedPeers{}, err
	}
	if peers.Peers == nil {
		peers.Peers = []AllowedPeer{}
	}
	if err := ValidateAllowedPeers(peers); err != nil {
		return AllowedPeers{}, fmt.Errorf("validate allowed peers: %w", err)
	}
	return peers, nil
}

func SaveAllowedPeers(path string, peers AllowedPeers) error {
	allowedPeersMu.Lock()
	defer allowedPeersMu.Unlock()
	return saveAllowedPeers(path, peers)
}

func saveAllowedPeers(path string, peers AllowedPeers) error {
	normalized, err := normalizeAllowedPeers(peers)
	if err != nil {
		return err
	}
	sort.Slice(normalized.Peers, func(i, j int) bool {
		return normalized.Peers[i].PeerID < normalized.Peers[j].PeerID
	})
	data, err := yaml.Marshal(&normalized)
	if err != nil {
		return err
	}
	return atomicWriteAllowedPeers(path, data)
}

func ValidateAllowedPeers(peers AllowedPeers) error {
	_, err := normalizeAllowedPeers(peers)
	return err
}

func normalizeAllowedPeers(peers AllowedPeers) (AllowedPeers, error) {
	out := AllowedPeers{Peers: make([]AllowedPeer, len(peers.Peers))}
	peerIDs := make(map[string]struct{}, len(peers.Peers))
	fingerprints := make(map[string]string, len(peers.Peers))
	for i, peer := range peers.Peers {
		peer.PeerID = strings.TrimSpace(peer.PeerID)
		if peer.PeerID == "" {
			return AllowedPeers{}, errors.New("allowed peer ID is required")
		}
		if len(peer.PeerID) > 256 || containsControl(peer.PeerID) {
			return AllowedPeers{}, fmt.Errorf("allowed peer ID %q is invalid", peer.PeerID)
		}
		if _, exists := peerIDs[peer.PeerID]; exists {
			return AllowedPeers{}, fmt.Errorf("duplicate allowed peer ID %q", peer.PeerID)
		}
		peerIDs[peer.PeerID] = struct{}{}

		peer.PublicKeyFingerprint = strings.ToLower(strings.TrimSpace(peer.PublicKeyFingerprint))
		if len(peer.PublicKeyFingerprint) > 256 || containsControl(peer.PublicKeyFingerprint) {
			return AllowedPeers{}, fmt.Errorf("public-key fingerprint for peer %q is invalid", peer.PeerID)
		}
		if peer.PublicKeyFingerprint != "" {
			if existingPeer, exists := fingerprints[peer.PublicKeyFingerprint]; exists {
				return AllowedPeers{}, fmt.Errorf(
					"duplicate public-key fingerprint for peers %q and %q",
					existingPeer,
					peer.PeerID,
				)
			}
			fingerprints[peer.PublicKeyFingerprint] = peer.PeerID
		}
		if strings.TrimSpace(peer.TrustLevel) == "" {
			peer.TrustLevel = "trusted-lan"
		}
		out.Peers[i] = peer
	}
	return out, nil
}

func (a AllowedPeers) IsAllowed(peerID string, require bool) bool {
	peerID = strings.TrimSpace(peerID)
	if peerID == "" {
		return false
	}
	for _, p := range a.Peers {
		if strings.TrimSpace(p.PeerID) == peerID {
			return p.Allowed
		}
	}
	return !require
}

func (a AllowedPeers) Get(peerID string) (AllowedPeer, bool) {
	for _, p := range a.Peers {
		if strings.TrimSpace(p.PeerID) == strings.TrimSpace(peerID) {
			return p, true
		}
	}
	return AllowedPeer{}, false
}

func (a AllowedPeers) GetByFingerprint(fingerprint string) (AllowedPeer, bool) {
	fingerprint = strings.ToLower(strings.TrimSpace(fingerprint))
	if fingerprint == "" {
		return AllowedPeer{}, false
	}
	for _, peer := range a.Peers {
		if strings.EqualFold(strings.TrimSpace(peer.PublicKeyFingerprint), fingerprint) {
			return peer, true
		}
	}
	return AllowedPeer{}, false
}

func AddPeer(path string, peer AllowedPeer) error {
	allowedPeersMu.Lock()
	defer allowedPeersMu.Unlock()

	peers, err := LoadAllowedPeers(path)
	if err != nil {
		return err
	}
	peer.PeerID = strings.TrimSpace(peer.PeerID)
	peer.PublicKeyFingerprint = strings.ToLower(strings.TrimSpace(peer.PublicKeyFingerprint))
	if strings.TrimSpace(peer.TrustLevel) == "" {
		peer.TrustLevel = "trusted-lan"
	}
	peer.Allowed = true
	for i := range peers.Peers {
		if strings.TrimSpace(peers.Peers[i].PeerID) == peer.PeerID {
			if peer.PublicKeyFingerprint == "" {
				peer.PublicKeyFingerprint = peers.Peers[i].PublicKeyFingerprint
			}
			peers.Peers[i] = peer
			return saveAllowedPeers(path, peers)
		}
	}
	peers.Peers = append(peers.Peers, peer)
	return saveAllowedPeers(path, peers)
}

func RemovePeer(path, peerID string) error {
	allowedPeersMu.Lock()
	defer allowedPeersMu.Unlock()

	peers, err := LoadAllowedPeers(path)
	if err != nil {
		return err
	}
	peerID = strings.TrimSpace(peerID)
	out := peers.Peers[:0]
	for _, p := range peers.Peers {
		if strings.TrimSpace(p.PeerID) != peerID {
			out = append(out, p)
		}
	}
	peers.Peers = out
	return saveAllowedPeers(path, peers)
}

func atomicWriteAllowedPeers(path string, data []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".allowed-peers-*.tmp")
	if err != nil {
		return err
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := replaceFileAtomic(tempPath, path); err != nil {
		return err
	}
	return syncParentDirectory(directory)
}

func containsControl(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}
