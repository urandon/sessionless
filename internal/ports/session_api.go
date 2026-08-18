package ports

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// SessionListPosition is an internal keyset cursor. Transport adapters must
// wrap it in an authenticated opaque token scoped to tenant, user and status.
type SessionListPosition struct {
	UpdatedAt time.Time
	SessionID domain.SessionID
}

type SessionListRequest struct {
	TenantID domain.TenantID
	UserID   domain.UserID
	Status   domain.SessionStatus
	Before   *SessionListPosition
	Limit    uint64
}

type RunListPosition struct {
	CreatedAt time.Time
	RunID     domain.RunID
}

type RunListRequest struct {
	TenantID  domain.TenantID
	UserID    domain.UserID
	SessionID domain.SessionID
	Before    *RunListPosition
	Limit     uint64
}

type SessionCreateRequest struct {
	Session        domain.Session
	Owner          domain.SessionParticipant
	IdempotencyKey domain.IdempotencyKey
}

type SessionRecord struct {
	Session  domain.Session
	Display  domain.SessionDisplay
	Run      *domain.Run
	Provider string
}

type RunRecord struct {
	Run      domain.Run
	Provider string
}

// FrontendBindingRequest binds a new frontend conversation when
// ExpectedRevision is zero, or switches its existing binding with optimistic
// revision fencing. The target session and an existing binding are both
// authorized for the requesting user inside the transaction.
type FrontendBindingRequest struct {
	TenantID               domain.TenantID
	UserID                 domain.UserID
	Frontend               domain.Frontend
	ExternalConversationID string
	BindingID              domain.FrontendBindingID
	SessionID              domain.SessionID
	ExpectedRevision       uint64
	At                     time.Time
}

// SessionAdminMetadata is deliberately payload-free. It is exposed through a
// separate administrative port so ordinary frontend and worker dependencies
// cannot acquire tenant-wide discovery by interface widening.
type SessionAdminMetadata struct {
	Session  domain.Session
	Display  domain.SessionDisplay
	Run      *domain.Run
	Provider string
}

// SessionAPIStore is the resource-authorized persistence boundary used by
// frontend-neutral session APIs. Selector reads deliberately combine the
// participant check with the bounded resource operation.
type SessionAPIStore interface {
	CreateSessionForUser(ctx context.Context, request SessionCreateRequest) (domain.Session, bool, error)
	GetSessionForUser(ctx context.Context, tenantID domain.TenantID, userID domain.UserID, sessionID domain.SessionID, write bool) (SessionRecord, bool, error)
	ListSessionsForUser(ctx context.Context, request SessionListRequest) ([]SessionRecord, error)
	ListSessionHistoryForUser(ctx context.Context, tenantID domain.TenantID, userID domain.UserID, sessionID domain.SessionID, afterSequence uint64, limit uint64) ([]domain.SessionEvent, error)
	ListRunsForUser(ctx context.Context, request RunListRequest) ([]RunRecord, error)
	BindOrSwitchFrontendForUser(ctx context.Context, request FrontendBindingRequest) (domain.FrontendBinding, error)
	SetSessionArchivedForUser(ctx context.Context, tenantID domain.TenantID, userID domain.UserID, sessionID domain.SessionID, archived bool, idempotencyKey domain.IdempotencyKey, at time.Time) (domain.Session, error)
}

type SessionAdminMetadataStore interface {
	GetSessionAdminMetadata(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID) (SessionAdminMetadata, bool, error)
}
