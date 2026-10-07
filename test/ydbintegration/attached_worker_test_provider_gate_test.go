//go:build ydbintegration && (darwin || linux)

package ydbintegration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This lifecycle can exist only inside the YDB gate's test binary. It issues
// no real provider secret and records only owner-scoped lifecycle milestones.
type aw07TestCredentialLifecycle struct {
	root       string
	owner      domain.UserID
	resource   string
	generation uint64
	now        func() time.Time
}

func (lifecycle *aw07TestCredentialLifecycle) Issue(_ context.Context, request ports.CredentialIssueRequest) (ports.CredentialHandle, error) {
	if lifecycle == nil || lifecycle.now == nil || request.ValidateAt(lifecycle.now()) != nil ||
		request.OwnerUserID != lifecycle.owner || request.ProviderResource.OwnerUserID != lifecycle.owner ||
		request.ProviderResource.ResourceID != lifecycle.resource ||
		request.ProviderResource.CredentialGeneration != lifecycle.generation {
		return ports.CredentialHandle{}, errors.New("test provider issue authority mismatch")
	}
	handle := ports.CredentialHandle{
		HandleID: "handle-" + string(lifecycle.owner), TenantID: request.Run.TenantID,
		SubscriptionConnectionID: request.Run.SubscriptionConnectionID,
		OwnerUserID:              request.OwnerUserID, RunID: request.Run.ID, AttemptID: request.Attempt.ID,
		WorkerID: request.Lease.WorkerID, LeaseID: request.Lease.ID,
		LeaseFence: request.Lease.FenceToken, BindingGeneration: lifecycle.generation,
		ProviderResource: request.ProviderResource, ExpiresAt: request.ExpiresAt,
	}
	if handle.Validate() != nil {
		return ports.CredentialHandle{}, errors.New("test provider handle invalid")
	}
	return handle, lifecycle.record("issue")
}

func (lifecycle *aw07TestCredentialLifecycle) Materialize(_ context.Context, handle ports.CredentialHandle) (ports.CredentialMaterialization, error) {
	if err := lifecycle.check(handle); err != nil {
		return ports.CredentialMaterialization{}, err
	}
	if err := os.MkdirAll(lifecycle.root, 0o700); err != nil {
		return ports.CredentialMaterialization{}, err
	}
	dir := filepath.Join(lifecycle.root, handle.HandleID)
	if err := os.Mkdir(dir, 0o700); err != nil {
		return ports.CredentialMaterialization{}, err
	}
	authFile := filepath.Join(dir, "auth.json")
	if err := os.WriteFile(authFile, []byte(`{"fixture":true}`), 0o600); err != nil {
		return ports.CredentialMaterialization{}, err
	}
	return ports.CredentialMaterialization{RootDir: dir, AuthFile: authFile}, lifecycle.record("materialize")
}

func (lifecycle *aw07TestCredentialLifecycle) WriteBack(_ context.Context, handle ports.CredentialHandle, materialization ports.CredentialMaterialization) (ports.CredentialWriteBackResult, error) {
	if err := lifecycle.check(handle); err != nil || materialization.Validate() != nil ||
		materialization.RootDir != filepath.Join(lifecycle.root, handle.HandleID) {
		return ports.CredentialWriteBackResult{}, errors.New("test provider writeback authority mismatch")
	}
	return ports.CredentialWriteBackResult{Generation: lifecycle.generation}, lifecycle.record("writeback")
}

func (lifecycle *aw07TestCredentialLifecycle) Release(_ context.Context, handle ports.CredentialHandle) error {
	if err := lifecycle.check(handle); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(lifecycle.root, handle.HandleID)); err != nil {
		return err
	}
	return lifecycle.record("release")
}

func (lifecycle *aw07TestCredentialLifecycle) RevokeConnection(context.Context, ports.CredentialRevokeRequest) error {
	return errors.New("test provider connection revoke is not supported")
}

func (lifecycle *aw07TestCredentialLifecycle) check(handle ports.CredentialHandle) error {
	if lifecycle == nil || handle.Validate() != nil || handle.OwnerUserID != lifecycle.owner ||
		handle.ProviderResource.ResourceID != lifecycle.resource || handle.BindingGeneration != lifecycle.generation ||
		handle.HandleID != "handle-"+string(lifecycle.owner) {
		return errors.New("test provider handle authority mismatch")
	}
	return nil
}

func (lifecycle *aw07TestCredentialLifecycle) record(stage string) error {
	if err := os.MkdirAll(lifecycle.root, 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(lifecycle.root, "lifecycle.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString(stage + "\n")
	return errors.Join(writeErr, file.Close())
}

var _ ports.CredentialLifecycle = (*aw07TestCredentialLifecycle)(nil)
