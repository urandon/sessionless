//go:build linux || darwin

package ownerlocalcredential

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

func metadataLifecycleFixture(t *testing.T) (*Lifecycle, *Store, domain.CredentialBinding) {
	t.Helper()
	s, _, _, binding := initializedStore(t)
	return newMetadataLifecycle(t, s), s, binding
}

func newMetadataLifecycle(t *testing.T, s *Store) *Lifecycle {
	t.Helper()
	scratch, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	life, err := NewLifecycle(s, credentiallifecycle.Config{ScratchRoot: scratch, MaxAuthBytes: 128, Clock: testkit.NewFakeClock(fixtureTime), IDs: testkit.NewSequenceIDGenerator("synthetic-metadata-")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := life.Close(); err != nil {
			t.Errorf("close metadata lifecycle: %v", err)
		}
	})
	return life
}

func TestOwnerLocalLifecycleMetadataUsesOriginalStoreWithoutMutation(t *testing.T) {
	life, store, binding := metadataLifecycleFixture(t)
	image, state := bytes.Clone(store.image), cloneSnapshot(store.state)
	got, found, err := life.LoadCredentialBinding(t.Context(), binding.TenantID, binding.SubscriptionConnectionID)
	if err != nil || !found || got != binding {
		t.Fatalf("original binding not returned: found=%v err=%v", found, err)
	}
	got.Generation++
	again, found, err := life.LoadCredentialBinding(t.Context(), binding.TenantID, binding.SubscriptionConnectionID)
	if err != nil || !found || again != binding || !bytes.Equal(image, store.image) || !reflect.DeepEqual(state, store.state) || store.guard.Load() || life.handle != nil {
		t.Fatal("metadata read mutated binding/credential lifecycle or acquired invocation authority", err)
	}
	// The same outer lifecycle owns a genuine synthetic generation advance.
	handle, err := life.Issue(t.Context(), lifecycleRequest())
	if err != nil {
		t.Fatal("issue synthetic original invocation", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
		defer cancel()
		if err := life.Release(cleanup, handle); err != nil {
			t.Errorf("release synthetic invocation: %v", err)
		}
	})
	material, err := life.Materialize(t.Context(), handle)
	if err != nil {
		t.Fatal("materialize synthetic original invocation", err)
	}
	if err := os.WriteFile(material.AuthFile, []byte("synthetic-metadata-refreshed-marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	changed, err := life.WriteBack(t.Context(), handle, material)
	if err != nil || !changed.Changed || changed.Generation != binding.Generation+1 {
		t.Fatal("synthetic writeback failed", err)
	}
	if err := life.Release(t.Context(), handle); err != nil {
		t.Fatal("release exact synthetic invocation", err)
	}
	advanced, found, err := life.LoadCredentialBinding(t.Context(), binding.TenantID, binding.SubscriptionConnectionID)
	if err != nil || !found || advanced.Generation != changed.Generation || advanced.Revoked {
		t.Fatalf("reader did not observe same lifecycle's writeback: found=%v err=%v", found, err)
	}
	request := ports.CredentialRevokeRequest{TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, SubscriptionConnectionID: binding.SubscriptionConnectionID}
	if err := life.RevokeConnection(t.Context(), request); err != nil {
		t.Fatal("revoke synthetic original binding", err)
	}
	current, found, err := life.LoadCredentialBinding(t.Context(), binding.TenantID, binding.SubscriptionConnectionID)
	if err != nil || !found || !current.Revoked || current.Generation != advanced.Generation+1 {
		t.Fatalf("metadata reader hid original store revocation: found=%v err=%v", found, err)
	}
}

func TestOwnerLocalLifecycleMetadataPreservesAbsentBinding(t *testing.T) {
	store, config, _ := storeFixture(t)
	life := newMetadataLifecycle(t, store)
	got, found, err := life.LoadCredentialBinding(t.Context(), config.Scope.TenantID, config.Scope.SubscriptionConnectionID)
	if err != nil || found || got != (domain.CredentialBinding{}) || store.guard.Load() || life.handle != nil {
		t.Fatalf("absent binding gained metadata/authority: found=%v err=%v", found, err)
	}
}

func TestOwnerLocalLifecycleMetadataDeniesForeignClosedAndCancelled(t *testing.T) {
	life, store, binding := metadataLifecycleFixture(t)
	assertDenied := func(t *testing.T, v *Lifecycle, ctx context.Context, tenant domain.TenantID, connection domain.SubscriptionConnectionID) {
		t.Helper()
		got, found, err := v.LoadCredentialBinding(ctx, tenant, connection)
		if err != ErrDenied || found || got != (domain.CredentialBinding{}) {
			t.Fatalf("invalid metadata read returned authority: found=%v err=%v", found, err)
		}
	}
	assertDenied(t, nil, t.Context(), binding.TenantID, binding.SubscriptionConnectionID)
	assertDenied(t, &Lifecycle{}, t.Context(), binding.TenantID, binding.SubscriptionConnectionID)
	assertDenied(t, &Lifecycle{lifecycle: &lifecycle{}}, t.Context(), binding.TenantID, binding.SubscriptionConnectionID)
	assertDenied(t, life, nil, binding.TenantID, binding.SubscriptionConnectionID)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	assertDenied(t, life, ctx, binding.TenantID, binding.SubscriptionConnectionID)
	assertDenied(t, life, t.Context(), "foreign-tenant", binding.SubscriptionConnectionID)
	assertDenied(t, life, t.Context(), binding.TenantID, "foreign-connection")
	store.unknown = true
	got, found, err := life.LoadCredentialBinding(t.Context(), binding.TenantID, binding.SubscriptionConnectionID)
	store.unknown = false
	if err != ErrUnknown || found || got != (domain.CredentialBinding{}) {
		t.Fatalf("uncertain original store returned metadata: found=%v err=%v", found, err)
	}
	if err := life.Close(); err != nil {
		t.Fatal(err)
	}
	assertDenied(t, life, t.Context(), binding.TenantID, binding.SubscriptionConnectionID)
}
