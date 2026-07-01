package main

import "testing"

func TestParseGlobalFlags(t *testing.T) {
	opts, args, err := parseGlobal([]string{"--json", "--config", "/tmp/cfg.yaml", "status"})
	if err != nil {
		t.Fatal(err)
	}
	if !opts.json {
		t.Fatal("expected json option")
	}
	if opts.configPath != "/tmp/cfg.yaml" {
		t.Fatalf("unexpected config path %q", opts.configPath)
	}
	if len(args) != 1 || args[0] != "status" {
		t.Fatalf("unexpected args %#v", args)
	}
}
