package ydbstore

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Private read-only DATA. No browser, subject, cookie, proof, secret or successor
// session is returned. Phase is the ACTUAL stored phase; an expired pending
// view has Outcome=expired but no CompletedAt/receipt or invented terminal phase.
// An absent/retention-expired row returns only outcome_unavailable after auth.
type identityOperationOutcomeViewV2 struct {
	Kind           domain.IdentityOperationKindV2
	Phase          domain.IdentityOperationPhaseV2
	ExpiresAt      time.Time
	Outcome        domain.IdentityOperationOutcomeV2
	BeforeRevision domain.AuthRevisionV2
	AfterRevision  *domain.AuthRevisionV2
	CompletedAt    *time.Time
	TargetProvider domain.IdentityProvider
	AllowedActions []ports.IdentityAccountActionV2
}

type identityOutcomeReadV2 struct {
	OperationDigest domain.SecretDigest
	SessionDigest   domain.SecretDigest
	OperationCookie string
	Service         *identityAccountServiceV2 // Optional private projection owner; never GET authority.
}

func identityOperationOutcomeView(operation domain.IdentityOperationV2, at time.Time) identityOperationOutcomeViewV2 {
	view := identityOperationOutcomeViewV2{Kind: operation.Kind, Phase: operation.Phase, ExpiresAt: operation.ExpiresAt}
	if operation.Pending != nil {
		view.TargetProvider = operation.Pending.TargetProvider
		view.BeforeRevision = operation.Pending.Scope.ExpectedRevision
		if !at.Before(operation.ExpiresAt) {
			view.Outcome = domain.IdentityOutcomeExpiredV2
		}
		return view
	}
	receipt := operation.Receipt
	view.Outcome, view.BeforeRevision = receipt.Outcome, receipt.BeforeRevision
	completed := receipt.CompletedAt
	view.CompletedAt = &completed
	if receipt.AfterRevision != nil {
		revision := *receipt.AfterRevision
		view.AfterRevision = &revision
	}
	return view
}

// Fresh same-user READ authorization only. SensitiveOperationsEnabled, original
// browser/CSRF/session and original expected revision are deliberately NOT GET
// requirements: a legitimate relogin can read a lost-commit outcome. This never
// refreshes activity, prunes expiry, replays a mutation or releases a cookie.
func (store *Store) readIdentityOperationOutcomeV2(ctx context.Context, request identityOutcomeReadV2, config identityConfigurationV2) (identityOperationOutcomeViewV2, error) {
	var empty, result identityOperationOutcomeViewV2
	bindings, err := config.bindings()
	if err != nil || request.OperationDigest.Validate("identity_outcome_v2.operation") != nil || request.SessionDigest.Validate("identity_outcome_v2.session") != nil {
		return empty, domain.ErrMembershipDenied
	}
	err = store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = empty
		session, member, err := authorizeWebSessionV2(ctx, q, request.SessionDigest, domain.TenantPermissionRead, q.at)
		if err != nil {
			return err
		}
		if _, enabled := bindings[session.Session.AuthenticatedSubject.Provider]; !enabled {
			return domain.ErrMembershipDenied
		}
		operation, found, err := readIdentityOperationV2(ctx, q, request.OperationDigest)
		if err != nil {
			return err
		}
		if !found {
			result.Outcome = domain.IdentityOutcomeUnavailableV2
			return nil
		}
		owner := domain.UserID("")
		if operation.Pending != nil {
			owner = operation.Pending.Scope.UserID
		} else {
			owner = operation.Receipt.UserID
		}
		if owner != session.Session.UserID || q.at.Before(operation.CreatedAt) || operation.Receipt != nil && q.at.Before(operation.Receipt.CompletedAt) {
			return domain.ErrMembershipDenied
		}
		if !q.at.Before(operation.CreatedAt.Add(domain.IdentityReceiptRetentionV2)) {
			result.Outcome = domain.IdentityOutcomeUnavailableV2
			return nil
		}
		result = identityOperationOutcomeView(operation, q.at)
		if request.Service != nil {
			if err := identityAccountCopiesV2(q); err != nil {
				return err
			}
			result.AllowedActions, err = request.Service.outcomeActions(ctx, q, request.OperationCookie, operation, session, member)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return empty, err
	}
	return result, nil
}

type identityOperationStopIntentV2 uint8

// Prospective allowance for the terminal/audit encode and immutable SQL-copy
// cohorts. Both closed records fit the EXISTING 8-KiB bound; this consumes the
// same callback/call pool, not a new or reset allowance.
const identityStopMaterialBytesV2 = 4 * domain.IdentityAuthMaxRecordBytesV2
const (
	identityOperationCancelV2 identityOperationStopIntentV2 = iota + 1
	identityOperationExpireV2
)

func (store *Store) cancelIdentityOperationV2(ctx context.Context, request identityPhaseRequestV2, config identityConfigurationV2) (identityOperationOutcomeViewV2, error) {
	return store.stopIdentityOperationV2(ctx, request, config, identityOperationCancelV2)
}

// Called only after the owning negative transaction authenticates its exact
// original scope (and, for proof failure, its consumed/scrubbed child).
func (store *Store) writeIdentityStopInTxV2(ctx context.Context, q *ownedAuthorityQuery, operation domain.IdentityOperationV2, outcome domain.IdentityOperationOutcomeV2, action domain.WebSecurityAuditAction, provider domain.IdentityProvider) (identityOperationOutcomeViewV2, error) {
	var empty identityOperationOutcomeViewV2
	if operation.Validate() != nil || operation.Pending == nil || provider.Validate() != nil {
		return empty, domain.ErrMembershipDenied
	}
	if action == domain.WebSecurityIdentityOperationStopped {
		if outcome != domain.IdentityOutcomeCancelledV2 && outcome != domain.IdentityOutcomeExpiredV2 {
			return empty, domain.ErrMembershipDenied
		}
	} else if action == domain.WebSecurityIdentityProofFailed {
		if outcome != domain.IdentityOutcomeProofUnavailableV2 && outcome != domain.IdentityOutcomeProviderUnavailableV2 {
			return empty, domain.ErrMembershipDenied
		}
	} else {
		return empty, domain.ErrMembershipDenied
	}
	if q.budget.materialized(q.callback, identityStopMaterialBytesV2) != nil {
		return empty, domain.ErrMembershipDenied
	}
	terminal, err := operation.Terminalize(outcome, nil, q.at)
	if err != nil {
		return empty, err
	}
	scope := operation.Pending.Scope
	event := domain.WebSecurityAuditEvent{RequestID: "identity-stop-" + string(operation.OperationDigest), Action: action, Provider: provider, TenantID: scope.TenantID, UserID: scope.UserID, MembershipSecurityVersion: scope.MembershipSecurityVersion, ReasonCode: string(outcome), OccurredAt: q.at}
	prepared, err := prepareWebSecurityAuditEvent(event)
	if err != nil {
		return empty, err
	}
	if err := writeIdentityOperationV2(ctx, q, terminal); err != nil {
		return empty, err
	}
	if err := writePreparedWebSecurityAuditEvent(ctx, q, prepared, store.operationalRetention); err != nil {
		return empty, err
	}
	return identityOperationOutcomeView(terminal, q.at), nil
}

func (store *Store) expireIdentityOperationV2(ctx context.Context, request identityPhaseRequestV2, config identityConfigurationV2) (identityOperationOutcomeViewV2, error) {
	return store.stopIdentityOperationV2(ctx, request, config, identityOperationExpireV2)
}

// Negative-only owner. The caller selects an explicit action, not a failure
// reason. Current DB time derives expiry; unknown DB/auth/config failures never
// terminalize a pending operation. No identity/member/cookie/child is written.
// Cleanup remains usable with sensitive operations off, but requires the SAME
// original configuration/principals/browser/CSRF/revision for a new stop.
func (store *Store) stopIdentityOperationV2(ctx context.Context, request identityPhaseRequestV2, config identityConfigurationV2, intent identityOperationStopIntentV2) (identityOperationOutcomeViewV2, error) {
	var empty, result identityOperationOutcomeViewV2
	bindings, err := config.bindings()
	if err != nil || store == nil || store.operationalRetention <= 0 || request.ExpectedRevision.Validate() != nil || intent != identityOperationCancelV2 && intent != identityOperationExpireV2 {
		return empty, domain.ErrMembershipDenied
	}
	for _, digest := range []domain.SecretDigest{request.OperationDigest, request.SessionDigest, request.CSRFDigest, request.BrowserBindingDigest} {
		if digest.Validate("identity_stop_v2.digest") != nil {
			return empty, domain.ErrMembershipDenied
		}
	}
	err = store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = empty
		current, _, err := currentIdentityPhaseV2(ctx, q, request, config)
		if err != nil {
			return err
		}
		if _, enabled := bindings[current.AuthenticatedSubject.Provider]; !enabled {
			return domain.ErrMembershipDenied
		}
		operation, found, err := readIdentityOperationV2(ctx, q, request.OperationDigest)
		if err != nil || !found {
			return domain.ErrMembershipDenied
		}
		if q.at.Before(operation.CreatedAt) || !q.at.Before(operation.CreatedAt.Add(domain.IdentityReceiptRetentionV2)) {
			return domain.ErrMembershipDenied
		}
		if operation.Receipt != nil {
			// Current same-user retry can observe the immutable existing result;
			// no original browser fact is reconstructed after privacy pruning.
			// A legitimate relogin can have advanced beyond BeforeRevision. The
			// current authenticated revision must cover, never predate, the
			// original result; there is no pending cancellation effect to replay.
			if operation.Receipt.UserID != current.UserID || operation.Receipt.BeforeRevision > current.ExpectedRevision || operation.Receipt.AfterRevision != nil && *operation.Receipt.AfterRevision > current.ExpectedRevision || q.at.Before(operation.Receipt.CompletedAt) {
				return domain.ErrMembershipDenied
			}
			result = identityOperationOutcomeView(operation, q.at)
			return nil
		}
		if operation.Pending.Scope != current {
			return domain.ErrMembershipDenied
		}
		if _, enabled := bindings[operation.Pending.SourceSubject.Provider]; !enabled {
			return domain.ErrMembershipDenied
		}
		outcome := domain.IdentityOutcomeCancelledV2
		if !q.at.Before(operation.ExpiresAt) {
			outcome = domain.IdentityOutcomeExpiredV2
		} else if intent == identityOperationExpireV2 {
			return domain.ErrMembershipDenied // before the sealed deadline, nothing expired
		}
		result, err = store.writeIdentityStopInTxV2(ctx, q, operation, outcome, domain.WebSecurityIdentityOperationStopped, operation.Pending.TargetProvider)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return empty, err
	}
	return result, nil
}
