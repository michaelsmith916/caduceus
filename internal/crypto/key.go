package cryptoutil

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const SharedKeyBytes = 32

func GenerateSharedKey() (string, error) {
	b := make([]byte, SharedKeyBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func NormalizeKey(raw string) (string, error) {
	key := strings.TrimSpace(raw)
	key = strings.TrimPrefix(key, "sha256:")
	key = strings.TrimPrefix(key, "key:")
	if key == "" {
		return "", errors.New("empty shared key")
	}
	decoded, err := hex.DecodeString(key)
	if err != nil {
		return "", fmt.Errorf("shared key must be hex: %w", err)
	}
	if len(decoded) < 16 {
		return "", errors.New("shared key must be at least 16 bytes")
	}
	return strings.ToLower(key), nil
}

func HashSharedKey(raw string) (string, error) {
	key, err := NormalizeKey(raw)
	if err != nil {
		return "", err
	}
	decoded, err := hex.DecodeString(key)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(decoded)
	return hex.EncodeToString(sum[:]), nil
}

func LoadSharedKey(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return NormalizeKey(string(data))
}

func SaveSharedKey(path, key string) error {
	normalized, err := NormalizeKey(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(normalized+"\n"), 0o600)
}

func LoadOrCreateSharedKey(path string) (string, bool, error) {
	key, err := LoadSharedKey(path)
	if err == nil {
		return key, false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	key, err = GenerateSharedKey()
	if err != nil {
		return "", false, err
	}
	if err := SaveSharedKey(path, key); err != nil {
		return "", false, err
	}
	return key, true, nil
}
