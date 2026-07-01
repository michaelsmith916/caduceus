package localnode

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func LoadOrCreateIdentity(path string) (crypto.PrivKey, peer.ID, bool, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		raw, err := base64.StdEncoding.DecodeString(string(trimSpace(data)))
		if err != nil {
			return nil, "", false, err
		}
		priv, err := crypto.UnmarshalPrivateKey(raw)
		if err != nil {
			return nil, "", false, err
		}
		id, err := peer.IDFromPrivateKey(priv)
		return priv, id, false, err
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, "", false, err
	}
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, "", false, err
	}
	raw, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, "", false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, "", false, err
	}
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(raw)+"\n"), 0o600); err != nil {
		return nil, "", false, err
	}
	id, err := peer.IDFromPrivateKey(priv)
	return priv, id, true, err
}

func trimSpace(in []byte) []byte {
	start := 0
	end := len(in)
	for start < end && (in[start] == ' ' || in[start] == '\n' || in[start] == '\r' || in[start] == '\t') {
		start++
	}
	for end > start && (in[end-1] == ' ' || in[end-1] == '\n' || in[end-1] == '\r' || in[end-1] == '\t') {
		end--
	}
	return in[start:end]
}
