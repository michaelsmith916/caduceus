package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAtomicReplaceExistingStateFile(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "state.json")
	source := filepath.Join(directory, "replacement.json")
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := replaceFileAtomic(source, target); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "new" {
		t.Fatalf("replacement content = %q, want new", data)
	}
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement source still exists or stat failed unexpectedly: %v", err)
	}
}
