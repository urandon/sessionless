package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// These are prerequisite reads, not a new permission registry or an enabled
// owned profile. The caller keeps them in the SAME transaction as the current
// Job/Attempt/lease and effect reservation. No cookie authority is invented for
// a worker: its principal comes only from the validated admitted job.
func currentServerlessSessionPrincipalTx(ctx context.Context, query rowQuery, loaded ports.WorkerJobState, at time.Time) error {
	if err := validateLoadedWorkerJob(loaded); err != nil || at.IsZero() {
		return domain.ErrMembershipDenied
	}
	tenant, sessionID, userID := loaded.Run.TenantID, loaded.Run.SessionID, loaded.Job.CredentialOwnerUserID
	return currentExecutionSessionPrincipalTx(ctx, query, tenant, sessionID, userID, at)
}

// Admission has an authoritative Run/outbox but no WorkerJob/reservation yet.
// Reuse only these canonical principal reads, never synthesize an admitted job
// or borrow managed admission/effect authority to validate its owner.
func currentExecutionSessionPrincipalTx(ctx context.Context, query rowQuery, tenant domain.TenantID, sessionID domain.SessionID, userID domain.UserID, at time.Time) error {
	if ctx == nil || ctx.Err() != nil || tenant.Validate() != nil || sessionID.Validate() != nil || userID.Validate() != nil || at.IsZero() {
		return domain.ErrMembershipDenied
	}
	var encoded, creator, status string
	var sequence uint64
	var archived sql.NullTime
	if err := query.QueryRowContext(ctx, computeChoiceSessionSQL, tenant, sessionID).Scan(&encoded, &creator, &status, &sequence, &archived); err != nil {
		return currentExecutionReadError(err)
	}
	var session domain.Session
	if decodeCurrentExecutionRecord(encoded, &session, computeChoiceSessionFields) != nil || session.Validate() != nil ||
		session.TenantID != tenant || session.ID != sessionID || string(session.CreatedBy) != creator || string(session.Status) != status ||
		session.LastEventSequence != sequence || !computeChoiceOptionalTimeEqual(session.ArchivedAt, archived, false) ||
		session.Status != domain.SessionActive || session.UpdatedAt.After(at) {
		return domain.ErrMembershipDenied
	}
	// Any canonical deletion row denies new work, including malformed records.
	// No body is needed; legal hold is not an execution prohibition.
	var deletionID string
	err := query.QueryRowContext(ctx, `SELECT SUBSTRING(CAST(session_id AS String),0,161) FROM session_deletions WHERE tenant_id=$1 AND session_id=$2`, tenant, sessionID).Scan(&deletionID)
	if err == nil {
		return domain.ErrMembershipDenied
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	bucket, err := webBucket(string(userID))
	if err != nil {
		return domain.ErrMembershipDenied
	}
	var role string
	var securityVersion uint64
	if err := query.QueryRowContext(ctx, computeChoiceMembershipSQL, bucket, userID, tenant).Scan(&encoded, &role, &status, &securityVersion); err != nil {
		return currentExecutionReadError(err)
	}
	var membership domain.TenantMembership
	if decodeCurrentExecutionRecord(encoded, &membership, computeChoiceMembershipFields) != nil ||
		string(membership.Role) != role || string(membership.Status) != status || membership.SecurityVersion != securityVersion ||
		membership.UpdatedAt.After(at) || membership.Authorize(userID, tenant, domain.TenantPermissionWrite) != nil {
		return domain.ErrMembershipDenied
	}
	if err := query.QueryRowContext(ctx, computeChoiceParticipantSQL, tenant, sessionID, userID).Scan(&encoded, &role, &status); err != nil {
		return currentExecutionReadError(err)
	}
	var participant domain.SessionParticipant
	if decodeCurrentExecutionRecord(encoded, &participant, computeChoiceParticipantFields) != nil ||
		string(participant.Role) != role || string(participant.Status) != status || participant.UpdatedAt.After(at) ||
		participant.Authorize(tenant, sessionID, userID, true) != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}

func currentServerlessProviderResourceTx(ctx context.Context, query rowQuery, binding domain.HarnessBindingV2, at time.Time) error {
	if binding.ValidateAt(at) != nil {
		return domain.ErrMembershipDenied
	}
	resource := binding.Resource
	if binding.Backend.BackendKind == domain.HarnessBackendDeterministicFixtureV1 &&
		binding.Backend.ProviderContractKind == domain.ProviderContractCredentiallessFixtureV1 &&
		resource.Kind == domain.ProviderResourceCredentiallessFixtureV1 && resource.CredentialMode == domain.ProviderCredentialNoneV1 && resource.CredentialGeneration == 0 {
		return nil
	}
	// Subscription resource authority belongs to its existing connection
	// lifecycle. It must not be substituted with an API credential row.
	if resource.Kind != domain.ProviderResourceAPIAccountV1 && resource.Kind != domain.ProviderResourceRouterAccountV1 {
		return domain.ErrMembershipDenied
	}
	current, found, err := readProviderCredential(ctx, query, ports.ProviderCredentialLocatorV1{
		TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, ResourceKind: resource.Kind, ResourceID: resource.ResourceID,
	})
	if err != nil {
		return err
	}
	if !found || current.UpdatedAt.After(at) {
		return domain.ErrMembershipDenied
	}
	actual, err := current.ResourceBinding()
	if err != nil || actual != resource {
		return domain.ErrMembershipDenied
	}
	return nil
}

func currentExecutionReadError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ErrMembershipDenied
	}
	return err
}

// Reuse the established exact-field/duplicate parser and bounded canonical
// metadata projection. No unbounded persisted JSON is parsed for permission.
// Eight KiB comfortably covers the fixed ID/time/scalar-only canonical rows.
func decodeCurrentExecutionRecord(encoded string, target any, fields map[string]bool) error {
	if len(encoded) == 0 || len(encoded) > 8192 {
		return domain.ErrMembershipDenied
	}
	decoder := json.NewDecoder(strings.NewReader(encoded))
	if validateComputeChoiceObject(decoder, fields) != nil {
		return domain.ErrMembershipDenied
	}
	if _, err := decoder.Token(); err != io.EOF {
		return domain.ErrMembershipDenied
	}
	if json.Unmarshal([]byte(encoded), target) != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}
