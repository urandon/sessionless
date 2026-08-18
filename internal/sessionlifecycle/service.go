// Package sessionlifecycle coordinates bounded, resumable deletion of one
// canonical session without granting prefix-delete authority.
package sessionlifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	DefaultMaxRows    = uint64(10_000)
	DefaultMaxObjects = uint64(10_000)
)

type Service struct {
	store      ports.SessionLifecycleStore
	blobs      ports.BlobStore
	maxRows    uint64
	maxObjects uint64
}

type Plan struct {
	Deletion     domain.SessionDeletion          `json:"deletion"`
	Inventory    domain.SessionDeletionInventory `json:"inventory"`
	Confirmation string                          `json:"confirmation"`
}

func New(store ports.SessionLifecycleStore, blobs ports.BlobStore, maxRows, maxObjects uint64) (*Service, error) {
	if store == nil || blobs == nil {
		return nil, fmt.Errorf("session lifecycle store and blob store are required")
	}
	if maxRows == 0 {
		maxRows = DefaultMaxRows
	}
	if maxObjects == 0 {
		maxObjects = DefaultMaxObjects
	}
	if maxRows > DefaultMaxRows || maxObjects > DefaultMaxObjects {
		return nil, fmt.Errorf("session deletion bounds cannot exceed %d", DefaultMaxRows)
	}
	return &Service{store: store, blobs: blobs, maxRows: maxRows, maxObjects: maxObjects}, nil
}

func (service *Service) Plan(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
) (Plan, error) {
	deletion, found, err := service.store.GetSessionDeletion(ctx, tenantID, sessionID)
	if err != nil {
		return Plan{}, err
	}
	if !found {
		return Plan{}, fmt.Errorf("deletion for session %q has not been requested", sessionID)
	}
	if deletion.State == domain.SessionDeletionCompleted {
		return Plan{Deletion: deletion}, nil
	}
	inventory, err := service.store.BuildSessionDeletionInventory(
		ctx, tenantID, sessionID, service.maxRows, service.maxObjects,
	)
	if err != nil {
		return Plan{}, err
	}
	confirmation, err := expectedConfirmation(inventory)
	if err != nil {
		return Plan{}, err
	}
	return Plan{Deletion: deletion, Inventory: inventory, Confirmation: confirmation}, nil
}

func (service *Service) Execute(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	confirmation string,
	at time.Time,
) (domain.SessionDeletion, error) {
	plan, err := service.Plan(ctx, tenantID, sessionID)
	if err != nil {
		return domain.SessionDeletion{}, err
	}
	if plan.Deletion.State == domain.SessionDeletionCompleted {
		return plan.Deletion, nil
	}
	if confirmation == "" || confirmation != plan.Confirmation {
		return domain.SessionDeletion{}, fmt.Errorf("typed session deletion confirmation does not match the exact inventory")
	}
	deletion, err := service.store.StartSessionDeletion(ctx, tenantID, sessionID, at)
	if err != nil {
		return domain.SessionDeletion{}, err
	}
	if deletion.State == domain.SessionDeletionCompleted {
		return deletion, nil
	}
	for _, ref := range plan.Inventory.Objects {
		if err := service.blobs.Delete(ctx, tenantID, ref); err != nil {
			return domain.SessionDeletion{}, fmt.Errorf("delete exact session object %q: %w", ref.Key, err)
		}
	}
	return service.store.CompleteSessionDeletion(
		ctx, tenantID, sessionID, at,
		uint64(len(plan.Inventory.Objects)), plan.Inventory.TotalBytes,
	)
}

func expectedConfirmation(inventory domain.SessionDeletionInventory) (string, error) {
	if err := inventory.Validate(DefaultMaxObjects); err != nil {
		return "", err
	}
	payload, err := json.Marshal(inventory)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return fmt.Sprintf(
		"delete-session:%s:%s:%s",
		inventory.TenantID, inventory.SessionID, hex.EncodeToString(digest[:12]),
	), nil
}
