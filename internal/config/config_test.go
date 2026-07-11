package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "data"))

	cfg, path, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if path == "" {
		t.Fatal("expected default config path")
	}
	if cfg.OpenAI.BaseURL != DefaultOpenAIBaseURL {
		t.Fatalf("unexpected base url %q", cfg.OpenAI.BaseURL)
	}
	if !cfg.Security.RequireAllowlist {
		t.Fatal("allowlist should default to required")
	}
	if cfg.Node.MDNSServiceName != DefaultMDNSServiceName {
		t.Fatalf("unexpected mdns name %q", cfg.Node.MDNSServiceName)
	}
}

func TestEnsureConfigWritesFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "config-home"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data-home"))
	path := filepath.Join(dir, "config.yaml")
	cfg, resolved, err := EnsureConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != path {
		t.Fatalf("resolved path mismatch: %s", resolved)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cfg.Storage.DataDir); err != nil {
		t.Fatal(err)
	}
}

func TestLoadUsesEnvironmentRootsForBlankDerivedPaths(t *testing.T) {
	dir := t.TempDir()
	configDir := filepath.Join(dir, "config-root")
	dataDir := filepath.Join(dir, "data-root")
	t.Setenv("CADUCEUS_CONFIG_DIR", configDir)
	t.Setenv("CADUCEUS_DATA_DIR", dataDir)

	path := filepath.Join(dir, "existing.yaml")
	contents := []byte("control:\n  auth_token_path: \"\"\nstorage:\n  data_dir: \"\"\nsecurity:\n  p2p_key_path: \"\"\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, _, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.DataDir != dataDir {
		t.Fatalf("data dir = %q, want %q", cfg.Storage.DataDir, dataDir)
	}
	if cfg.Control.AuthTokenPath != filepath.Join(dataDir, "control.token") {
		t.Fatalf("control token path = %q", cfg.Control.AuthTokenPath)
	}
	if cfg.Security.P2PKeyPath != filepath.Join(configDir, "p2p.key") {
		t.Fatalf("p2p key path = %q", cfg.Security.P2PKeyPath)
	}
}
