package ports

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// AttachedWorkerUXReadStore exposes only the owner-scoped authoritative reads
// needed by the public AW-06 read model. Implementations must not checkpoint
// presence, refresh credentials, advance protocol state, or perform any other
// mutation while serving these methods.
type AttachedWorkerUXReadStore interface {
	LoadAttachedWorker(
		context.Context,
		domain.TenantID,
		domain.UserID,
		domain.AttachedWorkerID,
	) (domain.AttachedWorker, bool, error)
	ListAttachedWorkers(
		context.Context,
		domain.TenantID,
		domain.UserID,
		domain.AttachedWorkerID,
		uint64,
	) ([]domain.AttachedWorker, error)
	LoadAttachedWorkerConnection(
		context.Context,
		domain.TenantID,
		domain.UserID,
		domain.AttachedWorkerID,
	) (domain.AttachedWorkerConnection, bool, error)
	LoadAttachedWorkerCapabilityManifest(
		context.Context,
		domain.TenantID,
		domain.UserID,
		domain.AttachedWorkerID,
		domain.AttachedWorkerCapabilityDigest,
	) (domain.AttachedWorkerCapabilityManifest, bool, error)
	LoadAttachedWorkerAttempt(
		context.Context,
		domain.TenantID,
		domain.UserID,
		domain.AttachedWorkerID,
	) (domain.AttachedWorkerAttemptV1, bool, error)
	ListAttachedWorkerAttemptMessages(
		context.Context,
		domain.TenantID,
		domain.UserID,
		domain.AttachedWorkerID,
		domain.AttemptID,
	) ([]domain.AttachedWorkerAttemptMessageV1, error)
}
