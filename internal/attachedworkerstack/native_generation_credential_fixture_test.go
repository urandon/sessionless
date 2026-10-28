//go:build darwin || linux

package attachedworkerstack_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ownerlocalcredential"
)

// Only an owned filesystem DATA path; this creates no key, lifecycle, producer,
// Session, admission, permission or credential binding.
func joinedGenerationCredentialRoot(t *testing.T) string {
	t.Helper()
	// Store.New requires an existing 0700 directory and refuses symlinks in
	// every ancestor. Canonicalize only this test-owned temporary parent.
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal("canonical joined credential parent", err)
	}
	root := filepath.Join(parent, "owner")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal("create joined private credential root", err)
	}
	return root
}

// Exercise the exact joined fixture's root setup using the real encrypted
// Store, but no DB, process, endpoint, native runner or real credential. The
// store intentionally refuses to create or follow a missing/symlink root.
func TestNativeGenerationJoinedCredentialRootNoDB(t *testing.T) {
	scope := ownerlocalcredential.Scope{TenantID: "tenant-joined-root", OwnerUserID: "owner-joined-root", SubscriptionConnectionID: "resource-joined-root"}
	key := bytes.Repeat([]byte{0x63}, 32)
	t.Cleanup(func() { clear(key) })
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal("canonical owned negative parent", err)
	}
	missing := filepath.Join(parent, "owner")
	denied, err := ownerlocalcredential.New(t.Context(), ownerlocalcredential.Config{Root: missing, Scope: scope, KeyEpoch: "synthetic-joined-only"}, key)
	if denied != nil {
		t.Cleanup(func() { _ = denied.Close() })
	}
	if !errors.Is(err, ownerlocalcredential.ErrDenied) || denied != nil {
		t.Fatal("missing root must remain denied by real credential validator", err)
	}
	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("denied constructor created missing root", err)
	}
	root := joinedGenerationCredentialRoot(t)
	store, err := ownerlocalcredential.New(t.Context(), ownerlocalcredential.Config{Root: root, Scope: scope, KeyEpoch: "synthetic-joined-only"}, key)
	if err != nil {
		t.Fatal("joined fixture must create canonical private owner root before credential New", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error("joined synthetic credential store close", err)
		}
	})
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root {
		t.Fatal("joined owner root is not canonical", err)
	}
	stat, err := os.Lstat(root)
	if err != nil || !stat.IsDir() || stat.Mode().Perm() != 0o700 || stat.Mode()&os.ModeSymlink != 0 {
		t.Fatal("joined owner root is not an actual private directory", err)
	}
	if err := store.Initialize(t.Context(), ownerlocalcredential.Registration{Provider: "openai", AuthMode: "chatgpt", Entitlement: domain.EntitlementActive, At: time.Unix(1_700_000_000, 0).UTC()}, []byte(`{"synthetic_generation_joined":"original"}`)); err != nil {
		t.Fatal("joined fixed synthetic auth initialization", err)
	}
}
