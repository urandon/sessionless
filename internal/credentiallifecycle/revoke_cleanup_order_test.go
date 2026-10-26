package credentiallifecycle

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestRevokeRetainsSecretUntilOriginalMaterializationCleanup(t *testing.T) {
	fixture := newCredentialFixture(t, &failureOnce{point: FailureBeforeCleanup})
	ctx := context.Background()
	handle, err := fixture.service.Issue(ctx, fixture.request)
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	t.Cleanup(func() {
		if err := fixture.service.Release(ctx, handle); err != nil {
			t.Errorf("original Release() cleanup error = %v", err)
		}
	})
	materialization, err := fixture.service.Materialize(ctx, handle)
	if err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}

	if err := fixture.service.RevokeConnection(ctx, revokeRequest()); !errors.Is(err, ErrCredentialMaterialization) {
		t.Fatalf("RevokeConnection() error = %v, want failed original cleanup", err)
	}
	current := fixture.state.current(fixture.binding)
	if !current.Revoked || current.Generation != fixture.binding.Generation+1 || !current.SecretRef.IsZero() {
		t.Fatalf("failed cleanup lost deny-first revocation: revoked=%t generation=%d referenceEmpty=%t", current.Revoked, current.Generation, current.SecretRef.IsZero())
	}
	if _, err := os.Lstat(materialization.AuthFile); err != nil {
		t.Fatalf("injected pre-cleanup failure did not retain original materialization: %v", err)
	}
	fixture.secrets.mu.Lock()
	_, retained := fixture.secrets.committed[fixture.binding.SecretRef]
	fixture.secrets.mu.Unlock()
	if !retained {
		t.Error("superseded secret deleted before original materialization cleanup was definitive")
	}
	if _, err := fixture.service.Issue(ctx, fixture.request); err == nil {
		t.Error("failed cleanup allowed Issue after revocation")
	}
	if err := fixture.service.Release(ctx, handle); err != nil {
		t.Fatalf("original Release() could not finish cleanup: %v", err)
	}
	if _, err := os.Lstat(materialization.RootDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("original materialization remains after successful Release: %v", err)
	}
	if current := fixture.state.current(fixture.binding); !current.Revoked || current.Generation != fixture.binding.Generation+1 {
		t.Errorf("successful cleanup changed durable revoke: revoked=%t generation=%d", current.Revoked, current.Generation)
	}
}
