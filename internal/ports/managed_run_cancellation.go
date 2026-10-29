package ports

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// These are resource selectors, never current authority. The adapter must
// authenticate the principal and original canonical scope in the SAME write
// transaction. No caller time, hold, Source, entitlement or funding is accepted.
type ManagedRunCancellationRequestV1 struct {
	TenantID   domain.TenantID
	UserID     domain.UserID
	SessionID  domain.SessionID
	RunID      domain.RunID
	DispatchID domain.DispatchOutboxID
}

type ManagedRunCancellationResultV1 struct {
	RunID    domain.RunID
	Status   domain.RunStatus
	Replayed bool
	NoHold   bool
}

type ManagedRunCancellationStoreV1 interface {
	RequestManagedRunCancellationV1(context.Context, ManagedRunCancellationRequestV1) (ManagedRunCancellationResultV1, error)
}
