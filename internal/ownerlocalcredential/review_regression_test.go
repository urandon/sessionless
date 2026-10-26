//go:build linux || darwin

package ownerlocalcredential

import (
	"os"
	"path/filepath"
	"testing"

	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

func TestOwnerLocalReopenUnknownRequiresNamespaceDurability(t *testing.T) {
	s, config, key, b := initializedStore(t)
	fired := false
	s.fs.fault = func(point string) error {
		if point == "after-rename" && !fired {
			fired = true
			return ErrUnavailable
		}
		return nil
	}
	if _, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(b.Generation), []byte("synthetic-unknown-reopen")); err != ErrUnknown || !fired {
		t.Fatal("post-rename cut did not leave unknown visible image", err)
	}
	// Controlled process-loss model: close only actual test-owned metadata
	// FDs. No native/lifecycle handle exists and Close is NOT claimed to have
	// resolved the unknown image. Reopen must establish durability afresh.
	if err := s.fs.close(); err != nil {
		t.Fatal(err)
	}
	s.closed = true
	attempt, err := newStore(t.Context(), config, key, func(root string) (*filesystem, error) {
		fs, err := openFilesystem(root)
		if err == nil {
			fs.fault = func(point string) error {
				if point == "directory-sync" {
					return ErrUnavailable
				}
				return nil
			}
		}
		return fs, err
	})
	if attempt != nil {
		_ = attempt.Close()
	}
	if err != ErrUnknown {
		t.Fatalf("reopen accepted visible image without namespace durability: got=%v want unknown", err)
	}
	reopened, err := New(t.Context(), config, key)
	if err != nil {
		t.Fatal("reopen after successful directory durability", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	current, found, err := reopened.LoadCredentialBinding(t.Context(), b.TenantID, b.SubscriptionConnectionID)
	if err != nil || !found || current != b || reopened.state.KeyUses != 2 || len(reopened.state.Candidates) != 1 {
		t.Fatal("durable reopen lost exact original binding/reservation", found, err)
	}
}

func TestOwnerLocalNoHandleRevokeRetriesExactPendingCleanup(t *testing.T) {
	s, config, _, b := initializedStore(t)
	scratch, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	life, err := NewLifecycle(s, credentiallifecycle.Config{ScratchRoot: scratch, MaxAuthBytes: 128, Clock: testkit.NewFakeClock(fixtureTime), IDs: testkit.NewSequenceIDGenerator("synthetic-no-handle-")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := life.Close(); err != nil {
			t.Error(err)
		}
	})
	request := ports.CredentialRevokeRequest{TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, SubscriptionConnectionID: b.SubscriptionConnectionID}
	syncs := 0
	s.fs.fault = func(point string) error {
		if point == "directory-sync" {
			syncs++
			if syncs == 3 {
				return ErrUnavailable
			}
		}
		return nil
	}
	if err := life.RevokeConnection(t.Context(), request); err == nil || !s.unknown {
		t.Fatal("delete UNKNOWN was reported as completed revoke", err)
	}
	s.fs.fault = nil
	if err := s.Reconcile(t.Context()); err != nil {
		t.Fatal("reconcile exact unlink uncertainty", err)
	}
	current, found, err := s.LoadCredentialBinding(t.Context(), b.TenantID, b.SubscriptionConnectionID)
	if err != nil || !found || !current.Revoked || current.Generation != b.Generation+1 || len(s.state.Cleanups) != 1 {
		t.Fatal("deny-first shape/pending cleanup lost", found, err)
	}
	foreign := request
	foreign.OwnerUserID = "other-owner"
	if err := life.RevokeConnection(t.Context(), foreign); err == nil || len(s.state.Cleanups) != 1 {
		t.Fatal("foreign scope altered original cleanup ownership", err)
	}
	if err := life.RevokeConnection(t.Context(), request); err != nil {
		t.Fatal("retry original no-handle revoke", err)
	}
	if len(s.state.Cleanups) != 0 {
		t.Fatal("retry reported success while exact pending revocation cleanup remains")
	}
	if _, err := os.Stat(filepath.Join(config.Root, bodyName(b.SecretRef.StorageValue()))); !os.IsNotExist(err) {
		t.Fatal("original revoked body remains", err)
	}
	if _, err := life.Issue(t.Context(), lifecycleRequest()); err == nil {
		t.Fatal("cleanup retry restored active eligibility")
	}
}
