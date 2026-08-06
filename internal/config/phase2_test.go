package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPhase2DefaultsValidate(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "data"))
	cfg, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Heartbeat.Interval() != 10*time.Second || cfg.Heartbeat.SuspectAfter() != 30*time.Second {
		t.Fatalf("unexpected heartbeat defaults: %#v", cfg.Heartbeat)
	}
	if cfg.Enrollment.TrustedLAN.Enabled {
		t.Fatal("trusted-LAN enrollment must be disabled by default")
	}
	if cfg.Worker.MaxQueueDepth != 0 {
		t.Fatalf("worker queue default = %d, want immediate rejection", cfg.Worker.MaxQueueDepth)
	}
	if cfg.Scheduler.MaxInFlightTasks != 64 || cfg.Scheduler.MaxQueueDepth != 1024 {
		t.Fatalf("unexpected durable dispatcher defaults: %#v", cfg.Scheduler)
	}
}

func TestExplicitZeroConcurrencyRejected(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "config-home"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data-home"))
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("worker:\n  max_concurrent_tasks: 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(path); err == nil {
		t.Fatal("expected explicit zero concurrency to be rejected")
	}
}

func TestHeartbeatThresholdOrderingValidated(t *testing.T) {
	cfg, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Heartbeat.SuspectAfterSeconds = cfg.Heartbeat.IntervalSeconds
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid suspect threshold")
	}
}

func TestEnrollmentNetworkValidated(t *testing.T) {
	cfg, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Enrollment.TrustedLAN.AllowedNetworks = []string{"not-a-cidr"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid CIDR rejection")
	}
}

func TestDurableDispatcherBoundsValidated(t *testing.T) {
	cfg, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Scheduler.MaxInFlightTasks = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected zero max in-flight tasks to be rejected")
	}
	cfg, err = Default()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Scheduler.MaxQueueDepth = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected zero durable queue depth to be rejected")
	}
}
