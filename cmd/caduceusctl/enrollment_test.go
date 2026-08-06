package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadEnrollmentTokenFromReaderAndFile(t *testing.T) {
	token, err := readEnrollmentToken("-", strings.NewReader("  abc123\n"))
	if err != nil || token != "abc123" {
		t.Fatalf("stdin token=%q err=%v", token, err)
	}
	path := filepath.Join(t.TempDir(), "invitation.token")
	if err := os.WriteFile(path, []byte("def456\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	token, err = readEnrollmentToken(path, nil)
	if err != nil || token != "def456" {
		t.Fatalf("file token=%q err=%v", token, err)
	}
}

func TestReadEnrollmentTokenRejectsUnsafeShapes(t *testing.T) {
	for _, value := range []string{"", "two tokens", strings.Repeat("x", maxEnrollmentTokenBytes+1)} {
		if token, err := readEnrollmentToken("-", strings.NewReader(value)); err == nil {
			t.Fatalf("value of length %d accepted as %q", len(value), token)
		}
	}
}
