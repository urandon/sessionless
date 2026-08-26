package ports

import (
	"context"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// FrontendSessionRequest is the membership-gated request for resolving or
// creating the first canonical session behind a frontend conversation.
type FrontendSessionRequest struct {
	TenantID               domain.TenantID
	UserID                 domain.UserID
	Frontend               domain.Frontend
	ExternalConversationID string
	BindingID              domain.FrontendBindingID
	SessionID              domain.SessionID
	At                     time.Time
}

func (request FrontendSessionRequest) Validate() error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := request.UserID.Validate(); err != nil {
		return err
	}
	if err := request.Frontend.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(request.ExternalConversationID) == "" {
		return domain.ValidationError{Field: "external_conversation_id", Reason: "must not be empty"}
	}
	if err := request.BindingID.Validate(); err != nil {
		return err
	}
	if err := request.SessionID.Validate(); err != nil {
		return err
	}
	if request.At.IsZero() {
		return domain.ValidationError{Field: "frontend_session.at", Reason: "must not be zero"}
	}
	return nil
}

type FrontendSessionState struct {
	Session domain.Session
	Binding domain.FrontendBinding
}

// CanonicalSessionSwitchRequest creates a new active session and switches one
// existing binding with optimistic revision fencing. The previous session is
// retained unchanged.
type CanonicalSessionSwitchRequest struct {
	TenantID         domain.TenantID
	UserID           domain.UserID
	BindingID        domain.FrontendBindingID
	ExpectedRevision uint64
	SessionID        domain.SessionID
	At               time.Time
}

func (request CanonicalSessionSwitchRequest) Validate() error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := request.UserID.Validate(); err != nil {
		return err
	}
	if err := request.BindingID.Validate(); err != nil {
		return err
	}
	if request.ExpectedRevision == 0 {
		return domain.ValidationError{Field: "frontend_binding.expected_revision", Reason: "must be positive"}
	}
	if err := request.SessionID.Validate(); err != nil {
		return err
	}
	if request.At.IsZero() {
		return domain.ValidationError{Field: "canonical_session_switch.at", Reason: "must not be zero"}
	}
	return nil
}

// CanonicalUserEventCommit contains only frontend-neutral identities and
// already-persisted object references. Implementations atomically allocate the
// event sequence and write the event, run, attempt, manifest, dispatch outbox,
// and frontend deduplication fact.
type CanonicalUserEventCommit struct {
	TenantID                 domain.TenantID
	UserID                   domain.UserID
	BindingID                domain.FrontendBindingID
	ExpectedBindingRevision  uint64
	Origin                   domain.FrontendEventOrigin
	IdempotencyKey           domain.IdempotencyKey
	MutationDigest           string
	ExpireAt                 time.Time
	EventID                  domain.SessionEventID
	Payload                  domain.BlobRef
	DisplayText              string
	RunID                    domain.RunID
	AttemptID                domain.AttemptID
	SubscriptionConnectionID domain.SubscriptionConnectionID
	ManifestID               domain.ArtifactManifestID
	Artifacts                []domain.Artifact
	DispatchID               domain.DispatchOutboxID
	AllowedMCPServers        []string
	ExecutionPlacementV2     domain.ExecutionPlacementV2
	HarnessBinding           domain.HarnessBindingV1
	SubstrateBinding         domain.SubstrateBindingV1
	AdmissionCostCeiling     domain.AdmissionCostCeilingV1
	CommittedAt              time.Time
}

// HarnessBindingRequest contains only server-derived authority. Frontend
// adapters never construct or pass provider/harness selection fields.
type HarnessBindingRequest struct {
	TenantID                 domain.TenantID
	OwnerUserID              domain.UserID
	RunID                    domain.RunID
	AttemptID                domain.AttemptID
	SubscriptionConnectionID domain.SubscriptionConnectionID
	At                       time.Time
}

type ManagedExecutionAuthorityV2 struct {
	ExecutionPlacementV2 domain.ExecutionPlacementV2
	HarnessBinding       domain.HarnessBindingV1
	SubstrateBinding     domain.SubstrateBindingV1
	AdmissionCostCeiling domain.AdmissionCostCeilingV1
}

func (authority ManagedExecutionAuthorityV2) ValidateForScope(request HarnessBindingRequest) error {
	if err := authority.ExecutionPlacementV2.Validate(); err != nil {
		return err
	}
	if authority.ExecutionPlacementV2.Kind != domain.ExecutionPlacementManaged {
		return domain.ValidationError{Field: "managed_execution_authority.execution_placement", Reason: "must be managed"}
	}
	if err := authority.SubstrateBinding.Validate(); err != nil {
		return err
	}
	substrateDigest, err := authority.SubstrateBinding.Digest()
	if err != nil {
		return err
	}
	if authority.ExecutionPlacementV2.SubstrateBindingDigest != string(substrateDigest) {
		return domain.ValidationError{Field: "managed_execution_authority.execution_placement", Reason: "must seal the exact substrate binding"}
	}
	if err := authority.AdmissionCostCeiling.Validate(); err != nil {
		return err
	}
	costDigest, err := authority.AdmissionCostCeiling.Digest()
	if err != nil {
		return err
	}
	if authority.SubstrateBinding.AdmissionCostCeilingDigest != costDigest {
		return domain.ValidationError{Field: "managed_execution_authority.admission_cost_ceiling", Reason: "must match the substrate binding"}
	}
	return authority.HarnessBinding.ValidateForScope(request.TenantID, request.OwnerUserID, request.RunID, request.AttemptID, authority.ExecutionPlacementV2)
}

type HarnessBinder interface {
	BindHarness(context.Context, HarnessBindingRequest) (ManagedExecutionAuthorityV2, error)
}

type CanonicalUserEventResult struct {
	SessionID domain.SessionID
	EventID   domain.SessionEventID
	Sequence  uint64
	RunID     domain.RunID
	Created   bool
}

// CanonicalUserEventLookup resolves an already committed frontend delivery
// before the application writes immutable objects. A hit is returned only
// after current tenant and original-session write authorization succeeds.
// Binding revision is deliberately absent: delayed duplicates remain bound to
// the session in which the original event committed.
type CanonicalUserEventLookup struct {
	TenantID               domain.TenantID
	UserID                 domain.UserID
	BindingID              domain.FrontendBindingID
	Frontend               domain.Frontend
	ExternalConversationID string
	IdempotencyKey         domain.IdempotencyKey
	MutationDigest         string
	EventID                domain.SessionEventID
	RunID                  domain.RunID
}

type CanonicalUserEventLookupResult struct {
	Result CanonicalUserEventResult
	Found  bool
}

// CanonicalIngressStore is the persistence boundary used by every frontend.
// Transport update/message types are intentionally excluded.
type CanonicalIngressStore interface {
	EnsureFrontendSession(context.Context, FrontendSessionRequest) (FrontendSessionState, error)
	CreateAndSwitchFrontendSession(context.Context, CanonicalSessionSwitchRequest) (FrontendSessionState, error)
	LookupCanonicalUserEvent(context.Context, CanonicalUserEventLookup) (CanonicalUserEventLookupResult, error)
	CommitCanonicalUserEvent(context.Context, CanonicalUserEventCommit) (CanonicalUserEventResult, error)
}
