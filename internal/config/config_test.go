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
