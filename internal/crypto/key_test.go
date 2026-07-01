package cryptoutil

import "testing"

func TestHashSharedKeyStable(t *testing.T) {
	key := "00112233445566778899aabbccddeeff"
	got, err := HashSharedKey(key)
	if err != nil {
		t.Fatal(err)
	}
	again, err := HashSharedKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if got != again {
		t.Fatal("hash should be stable")
	}
	if got == key {
		t.Fatal("hash should not expose raw key")
	}
}

func TestRejectShortKey(t *testing.T) {
	if _, err := HashSharedKey("0011"); err == nil {
		t.Fatal("expected short key rejection")
	}
}
