//go:build linux || darwin

package ownerlocalcredential

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestOwnerLocalMaximumAuthAndProspectiveDiskLimit(t *testing.T) {
	s, _, _, b := initializedStore(t)
	plain := bytes.Repeat([]byte("s"), maxAuthBytes)
	t.Cleanup(func() { clear(plain) })
	c, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(1), plain)
	if err != nil {
		t.Fatal("maximum admitted auth bytes rejected", err)
	}
	next := b
	next.Generation = 2
	next.SecretRef = c.Reference
	next.SecretFingerprint = c.Fingerprint
	if ok, err := s.CompareAndSwapCredentialBinding(t.Context(), 1, next); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := s.CommitCredentialCandidate(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadCredentialSecret(t.Context(), next, maxAuthBytes)
	if err != nil || len(got) != maxAuthBytes || !bytes.Equal(got, plain) {
		t.Fatal("maximum auth did not survive exact envelope", len(got), err)
	}
	clear(got)
	if err := s.fs.space(maxDiskBytes + 1); err != ErrDenied {
		t.Fatal("prospective disk overflow not rejected", err)
	}
}

func TestOwnerLocalCopiedRootCannotResetKeyAccounting(t *testing.T) {
	s, config, key, _ := initializedStore(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	destination, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(config.Root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == lockName {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(config.Root, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(destination, entry.Name()), raw, 0o600); err != nil {
			t.Fatal(err)
		}
		clear(raw)
	}
	copyConfig := config
	copyConfig.Root = destination
	if _, err := New(t.Context(), copyConfig, key); err != ErrDenied {
		t.Fatal("copied root automatically adopted original key/counter", err)
	}
}
