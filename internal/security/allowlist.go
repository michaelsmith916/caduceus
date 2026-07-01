package security

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type AllowedPeers struct {
	Peers []AllowedPeer `json:"peers" yaml:"peers"`
}

type AllowedPeer struct {
	PeerID     string `json:"peer_id" yaml:"peer_id"`
	Name       string `json:"name" yaml:"name"`
	TrustLevel string `json:"trust_level" yaml:"trust_level"`
	Allowed    bool   `json:"allowed" yaml:"allowed"`
	Notes      string `json:"notes" yaml:"notes"`
}

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
	return peers, nil
}

func SaveAllowedPeers(path string, peers AllowedPeers) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	sort.Slice(peers.Peers, func(i, j int) bool {
		return peers.Peers[i].PeerID < peers.Peers[j].PeerID
	})
	data, err := yaml.Marshal(&peers)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
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

func AddPeer(path string, peer AllowedPeer) error {
	peers, err := LoadAllowedPeers(path)
	if err != nil {
		return err
	}
	if strings.TrimSpace(peer.TrustLevel) == "" {
		peer.TrustLevel = "trusted-lan"
	}
	peer.Allowed = true
	for i := range peers.Peers {
		if peers.Peers[i].PeerID == peer.PeerID {
			peers.Peers[i] = peer
			return SaveAllowedPeers(path, peers)
		}
	}
	peers.Peers = append(peers.Peers, peer)
	return SaveAllowedPeers(path, peers)
}

func RemovePeer(path, peerID string) error {
	peers, err := LoadAllowedPeers(path)
	if err != nil {
		return err
	}
	out := peers.Peers[:0]
	for _, p := range peers.Peers {
		if p.PeerID != peerID {
			out = append(out, p)
		}
	}
	peers.Peers = out
	return SaveAllowedPeers(path, peers)
}
