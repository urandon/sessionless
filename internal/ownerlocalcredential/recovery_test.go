//go:build linux || darwin

package ownerlocalcredential

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestOwnerLocalCASCommitRevokeUnknownCuts(t *testing.T) {
	for _, phase := range []string{"cas", "commit", "revoke"} {
		t.Run(phase, func(t *testing.T) {
			s, _, _, b := initializedStore(t)
			c, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(1), []byte("synthetic-cut-next"))
			if err != nil {
				t.Fatal(err)
			}
			next := b
			next.Generation = 2
			next.SecretRef = c.Reference
			next.SecretFingerprint = c.Fingerprint
			next.UpdatedAt = b.UpdatedAt.Add(time.Second)
			if phase != "cas" {
				if ok, err := s.CompareAndSwapCredentialBinding(t.Context(), 1, next); err != nil || !ok {
					t.Fatal(ok, err)
				}
			}
			if phase == "revoke" {
				if err := s.CommitCredentialCandidate(t.Context(), c); err != nil {
					t.Fatal(err)
				}
			}
			fired := false
			s.fs.fault = func(point string) error {
				if point == "after-rename" && !fired {
					fired = true
					return ErrUnavailable
				}
				return nil
			}
			switch phase {
			case "cas":
				_, err = s.CompareAndSwapCredentialBinding(t.Context(), 1, next)
			case "commit":
				err = s.CommitCredentialCandidate(t.Context(), c)
			case "revoke":
				_, err = s.RevokeCredentialBinding(t.Context(), ports.CredentialRevokeRequest{TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, SubscriptionConnectionID: b.SubscriptionConnectionID}, next.UpdatedAt)
			}
			if err != ErrUnknown || !fired {
				t.Fatal("original publication was not unknown", err, fired)
			}
			s.fs.fault = nil
			if err := s.Reconcile(t.Context()); err != nil {
				t.Fatal("exact publication reconcile", err)
			}
			current, found, err := s.LoadCredentialBinding(t.Context(), b.TenantID, b.SubscriptionConnectionID)
			if err != nil || !found {
				t.Fatal(found, err)
			}
			if phase == "revoke" {
				if !current.Revoked || current.Generation != 3 {
					t.Fatal("revocation lost after unknown")
				}
				return
			}
			if current != next {
				t.Fatal("unknown CAS changed exact next identity")
			}
			recovered, err := s.RecoverCredentialCandidate(t.Context(), current)
			if err != nil || recovered != (phase == "cas") {
				t.Fatal("exact post-cut candidate relation", recovered, err)
			}
			plain, err := s.ReadCredentialSecret(t.Context(), current, 128)
			if err != nil || string(plain) != "synthetic-cut-next" {
				t.Fatal("post-cut bytes", err)
			}
			clear(plain)
		})
	}
}

func TestOwnerLocalPersistentPreRenameAndBodyCuts(t *testing.T) {
	t.Run("old-image-after-persistent-sync-failure", func(t *testing.T) {
		s, _, _, b := initializedStore(t)
		uses := s.state.KeyUses
		s.fs.fault = func(point string) error {
			if point == "directory-sync" {
				return ErrUnavailable
			}
			return nil
		}
		if _, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(1), []byte("never-encrypted")); err != ErrUnknown {
			t.Fatal("persistent pre-rename cut", err)
		}
		s.fs.fault = nil
		if err := s.Reconcile(t.Context()); err != nil {
			t.Fatal("exact old image irrecoverably fenced", err)
		}
		if s.state.KeyUses != uses || len(s.state.Candidates) != 0 || s.state.Binding.Value != b {
			t.Fatal("unpublished reservation invented new authority")
		}
	})
	t.Run("ciphertext-cut-retains-reservation", func(t *testing.T) {
		s, _, _, _ := initializedStore(t)
		uses := s.state.KeyUses
		calls := 0
		s.fs.fault = func(point string) error {
			if point == "file-sync" {
				calls++
				if calls == 2 {
					return ErrUnavailable
				}
			}
			return nil
		}
		if _, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(1), []byte("synthetic-ciphertext-cut")); err != ErrUnknown {
			t.Fatal("body cut", err)
		}
		s.fs.fault = nil
		if err := s.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		if s.state.KeyUses != uses+1 || len(s.state.Candidates) != 1 || s.state.Candidates[0].Ready {
			t.Fatal("body cut reset nonce reservation or minted ready")
		}
		if err := s.DeleteCredentialCandidate(t.Context(), candidatePort(s, s.state.Candidates[0])); err != nil {
			t.Fatal("exact orphan cleanup", err)
		}
		if s.state.KeyUses != uses+1 {
			t.Fatal("orphan cleanup refunded key use")
		}
	})
	t.Run("unlink-cut-retains-cleanup-owner", func(t *testing.T) {
		s, _, _, _ := initializedStore(t)
		c, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(1), []byte("synthetic-orphan"))
		if err != nil {
			t.Fatal(err)
		}
		fired := false
		s.fs.fault = func(point string) error {
			if point == "directory-sync" && !fired {
				fired = true
				return ErrUnavailable
			}
			return nil
		}
		if err := s.DeleteCredentialCandidate(t.Context(), c); err != ErrUnknown {
			t.Fatal("ambiguous unlink misreported", err)
		}
		s.fs.fault = nil
		if err := s.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		if len(s.state.Candidates) != 1 {
			t.Fatal("ambiguous unlink discarded original cleanup relation")
		}
		if err := s.DeleteCredentialCandidate(t.Context(), c); err != nil {
			t.Fatal("missing exact body cleanup replay", err)
		}
	})
}

func TestOwnerLocalFilesystemReplacementAndHardlinkDenials(t *testing.T) {
	for _, mode := range []string{"hardlink", "mode", "snapshot-inode", "root-inode"} {
		t.Run(mode, func(t *testing.T) {
			s, config, _, b := initializedStore(t)
			body := filepath.Join(config.Root, bodyName(b.SecretRef.StorageValue()))
			switch mode {
			case "hardlink":
				alias := filepath.Join(config.Root, "foreign-link")
				if err := os.Link(body, alias); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Remove(alias) })
			case "mode":
				if err := os.Chmod(body, 0o644); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(body, 0o600) })
			case "snapshot-inode":
				replacement := filepath.Join(config.Root, "replacement")
				if err := os.WriteFile(replacement, s.image, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(replacement, filepath.Join(config.Root, metadataName)); err != nil {
					t.Fatal(err)
				}
			case "root-inode":
				moved := config.Root + "-original"
				if err := os.Rename(config.Root, moved); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(config.Root, 0o700); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Remove(config.Root); _ = os.Rename(moved, config.Root) })
			}
			if _, _, err := s.LoadCredentialBinding(t.Context(), b.TenantID, b.SubscriptionConnectionID); err != ErrDenied {
				t.Fatal("replaced or nonprivate filesystem remained authoritative", err)
			}
		})
	}
}

func TestOwnerLocalPrivateCodecAndKeyUseBounds(t *testing.T) {
	s, _, _, b := initializedStore(t)
	for _, raw := range [][]byte{
		bytes.Replace(s.image, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		bytes.Replace(s.image, []byte(`"key_uses":1`), []byte(`"KEY_USES":1`), 1),
		append(bytes.Clone(s.image), 0),
	} {
		if _, err := s.decodeSnapshot(raw); err != ErrDenied {
			t.Fatal("malformed or unauthenticated private shape admitted", err)
		}
	}
	state := cloneSnapshot(s.state)
	state.KeyUses = maxKeyUses - 1
	if err := s.saveLocked(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(b.Generation), []byte("exhausted")); err != ErrDenied {
		t.Fatal("key use counter wrapped", err)
	}
	if s.state.KeyUses != maxKeyUses-1 {
		t.Fatal("key exhaustion reset accounting")
	}
	zero := new(Store)
	if _, _, err := zero.LoadCredentialBinding(t.Context(), domain.TenantID("tenant-a"), domain.SubscriptionConnectionID("connection-a")); err != ErrDenied {
		t.Fatal("zero store accepted", err)
	}
}
