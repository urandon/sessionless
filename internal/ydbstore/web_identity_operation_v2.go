package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const identityOperationRowSQLV2 = `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM oidc_login_challenges WHERE shard_bucket=$1 AND state_digest=$2`

// This private configuration is supplied by the trusted installation, never
// reconstructed from an operation or browser request. It does not fence other
// deployment revisions: the design's freeze/drain/exclusive-writer cutover is
// still mandatory. No serving caller or V2 mode is installed by this file.
type identityConfigurationV2 struct {
	ManifestDigest             domain.SecretDigest
	Providers                  []domain.IdentityProviderBindingV2
	SensitiveOperationsEnabled bool
}

func (config identityConfigurationV2) bindings() (map[domain.IdentityProvider]domain.IdentityProviderBindingV2, error) {
	if config.ManifestDigest.Validate("identity_configuration_v2.manifest") != nil || len(config.Providers) < 1 || len(config.Providers) > 2 {
		return nil, domain.ErrMembershipDenied
	}
	bindings := make(map[domain.IdentityProvider]domain.IdentityProviderBindingV2, 2)
	for _, binding := range config.Providers {
		if binding.Validate() != nil {
			return nil, domain.ErrMembershipDenied
		}
		if _, duplicate := bindings[binding.Provider]; duplicate {
			return nil, domain.ErrMembershipDenied
		}
		bindings[binding.Provider] = binding
	}
	return bindings, nil
}

type identityConfirmationV2 struct {
	OperationDigest      domain.SecretDigest
	SessionDigest        domain.SecretDigest
	CSRFDigest           domain.SecretDigest
	BrowserBindingDigest domain.SecretDigest
	ExpectedRevision     domain.AuthRevisionV2
	NextSessionDigest    domain.SecretDigest
	NextCSRFDigest       domain.SecretDigest
}

type identityConfirmationResultV2 struct {
	Receipt domain.IdentityOperationReceiptV2
	Session domain.WebSessionV2
}

// Reuse the existing bounded statement/materialization accounting, not a new
// authorization engine. This entrypoint establishes its own original five-
// second identity transaction; it is NOT an owned-worker admission grant and
// does not relax the separate ownedAuthorityTx installation/authority gates.
func (store *Store) identityAuthorityTxV2(ctx context.Context, fn func(context.Context, *ownedAuthorityQuery) error) error {
	if ctx == nil || ctx.Err() != nil || store == nil || store.db == nil || fn == nil {
		return domain.ErrMembershipDenied
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ctx, release := ownedAuthorityCallContext(ctx)
	defer release()
	budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
	if budget.check() != nil {
		return domain.ErrMembershipDenied
	}
	err := retry.DoTx(ctx, store.db, func(ctx context.Context, tx *sql.Tx) error {
		callback, err := budget.begin()
		if err != nil {
			return callbackError{err: err}
		}
		defer budget.finish(callback)
		query := &ownedAuthorityQuery{tx: tx, budget: budget, callback: callback}
		returned, scanErr := scanOwnedCurrentRow(query.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`), &query.at)
		if budget.materialized(callback, returned) != nil || scanErr != nil {
			return callbackError{err: domain.ErrMembershipDenied}
		}
		query.at = query.at.UTC()
		if err := fn(ctx, query); err != nil {
			return callbackError{err: err}
		}
		if ctx.Err() != nil {
			return callbackError{err: domain.ErrMembershipDenied}
		}
		return budget.check()
	}, retry.WithIdempotent(true), retry.WithBudget(budget), retry.WithTxOptions(&sql.TxOptions{Isolation: sql.LevelSerializable}))
	if ctx.Err() != nil || budget.check() != nil {
		return domain.ErrMembershipDenied
	}
	var callback callbackError
	if errors.As(err, &callback) {
		return callback.err
	}
	if err != nil {
		// An ambiguous backend result is never a fresh permission or proof that
		// nothing committed. Recovery reads the exact retained receipt instead.
		return domain.ErrMembershipDenied
	}
	return nil
}

func readIdentityOperationV2(ctx context.Context, query *ownedAuthorityQuery, digest domain.SecretDigest) (domain.IdentityOperationV2, bool, error) {
	if digest.Validate("identity_operation_v2.operation_digest") != nil {
		return domain.IdentityOperationV2{}, false, domain.ErrMembershipDenied
	}
	bucket, err := webBucket(string(digest))
	if err != nil {
		return domain.IdentityOperationV2{}, false, err
	}
	raw, err := query.readMetadata(ctx, identityOperationRowSQLV2, domain.IdentityAuthMaxRecordBytesV2, bucket, digest)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.IdentityOperationV2{}, false, nil
	}
	if err != nil {
		return domain.IdentityOperationV2{}, false, err
	}
	operation, err := domain.DecodeIdentityOperationV2([]byte(raw))
	if err != nil || operation.OperationDigest != digest {
		return domain.IdentityOperationV2{}, false, domain.ErrMembershipDenied
	}
	return operation, true, nil
}

// Caller owns the existing exact operation read and validated transition in
// this SAME transaction. Terminal privacy pruning covers physical scalar
// columns too, not only the nested JSON receipt. Requires the forward
// delete_after migration; absence is an error, never a TTL-format fallback.
func writeIdentityOperationV2(ctx context.Context, query *ownedAuthorityQuery, operation domain.IdentityOperationV2) error {
	raw, err := domain.EncodeIdentityOperationV2(operation)
	if err != nil {
		return err
	}
	bucket, err := webBucket(string(operation.OperationDigest))
	if err != nil {
		return err
	}
	var browser domain.SecretDigest
	consumed := time.Unix(0, 0).UTC()
	if operation.Pending != nil {
		browser = operation.Pending.Scope.BrowserBindingDigest
	} else {
		consumed = operation.Receipt.CompletedAt
	}
	_, err = query.ExecContext(ctx, `UPSERT INTO oidc_login_challenges
 (shard_bucket,state_digest,browser_binding_digest,pkce_verifier,nonce,redirect_path,
 created_at,expires_at,consumed_at,record,delete_after)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,CAST($10 AS JsonDocument),$11)`,
		bucket, operation.OperationDigest, browser, "", "", "", operation.CreatedAt,
		operation.ExpiresAt, consumed, string(raw), operation.DeleteAfter)
	return err
}

// confirmIdentityOperationV2 is the private atomic storage protocol. Verified
// provider proof records must already have been written through the one-use
// consumption/verified-adapter path; this function never exchanges a code,
// calls a provider or accepts request-supplied proof metadata. No route calls it
// until full V2 writer/consumer cutover and #239 orchestration are installed.
func (store *Store) confirmIdentityOperationV2(ctx context.Context, request identityConfirmationV2, config identityConfigurationV2) (identityConfirmationResultV2, error) {
	var empty identityConfirmationResultV2
	bindings, err := config.bindings()
	if err != nil || request.ExpectedRevision.Validate() != nil {
		return empty, domain.ErrMembershipDenied
	}
	for _, digest := range []domain.SecretDigest{request.OperationDigest, request.SessionDigest, request.CSRFDigest, request.BrowserBindingDigest, request.NextSessionDigest, request.NextCSRFDigest} {
		if digest.Validate("identity_confirmation_v2.digest") != nil {
			return empty, domain.ErrMembershipDenied
		}
	}
	if store == nil || store.webSessionIdleTTL <= 0 || store.operationalRetention <= 0 {
		return empty, domain.ErrMembershipDenied
	}
	var result identityConfirmationResultV2
	err = store.identityAuthorityTxV2(ctx, func(ctx context.Context, query *ownedAuthorityQuery) error {
		result = empty
		operation, found, err := readIdentityOperationV2(ctx, query, request.OperationDigest)
		if err != nil || !found {
			return domain.ErrMembershipDenied
		}
		session, membership, err := authorizeWebSessionV2(ctx, query, request.SessionDigest, domain.TenantPermissionRead, query.at)
		if err != nil || session.Session.CSRFTokenDigest != request.CSRFDigest {
			return domain.ErrMembershipDenied
		}
		if operation.Receipt != nil {
			if operation.Receipt.UserID != session.Session.UserID || operation.Receipt.BeforeRevision != request.ExpectedRevision || !query.at.Before(operation.DeleteAfter) {
				return domain.ErrMembershipDenied
			}
			// A current same-user repeated confirm may read the exact result. It
			// cannot restore the old cookie or synthesize its lost replacement.
			result.Receipt = *operation.Receipt
			return nil
		}
		if !config.SensitiveOperationsEnabled {
			return domain.ErrMembershipDenied
		}
		if request.SessionDigest == request.NextSessionDigest || request.CSRFDigest == request.NextCSRFDigest {
			return domain.ErrWebSessionRotation
		}
		current := domain.IdentityOperationScopeV2{UserID: session.Session.UserID, SessionDigest: request.SessionDigest,
			AuthenticatedSubject: session.Session.AuthenticatedSubject, TenantID: session.Session.ActiveTenantID,
			MembershipSecurityVersion: membership.SecurityVersion, BrowserBindingDigest: request.BrowserBindingDigest,
			ConfigurationDigest: config.ManifestDigest, ExpectedRevision: session.IdentityRevision}
		if request.ExpectedRevision != session.IdentityRevision || operation.ValidateCurrentScope(current, query.at) != nil || operation.ValidateConfirm(query.at) != nil {
			return domain.ErrMembershipDenied
		}
		pending := operation.Pending
		for _, proof := range []*domain.IdentityProviderProofV2{pending.SourceProof, pending.TargetProof} {
			if proof == nil {
				continue // Unlink has no target-provider proof arm.
			}
			if binding, ok := bindings[proof.Subject.Provider]; !ok || binding != proof.Binding {
				return domain.ErrIdentityProviderUnavailableV2
			}
		}
		active, err := readActiveExternalIdentitiesV2(ctx, query, session.Session.UserID)
		if err != nil {
			return err
		}
		authFound, sourceFound := false, false
		for _, identity := range active {
			authFound = authFound || identity.Subject == session.Session.AuthenticatedSubject && identity.Revision == session.IdentityRevision
			sourceFound = sourceFound || identity.Subject == pending.SourceSubject && identity.Revision == session.IdentityRevision
		}
		if !authFound || !sourceFound || pending.TargetSubject == nil {
			return domain.ErrMembershipDenied
		}
		target, exists, err := readExternalIdentityV2(ctx, query, *pending.TargetSubject)
		if err != nil {
			return err
		}
		var previous *domain.ExternalIdentityV2
		if exists {
			previous = &target
		}
		enabled := make([]domain.IdentityProvider, 0, len(config.Providers))
		for _, binding := range config.Providers {
			enabled = append(enabled, binding.Provider)
		}
		plan, err := domain.PlanExternalIdentityChangeV2(session.Session.UserID, active, *pending.TargetSubject, previous, operation.Kind, enabled, query.at)
		if err != nil {
			return err
		}
		if _, exists, err := readWebSessionV2(ctx, query, request.NextSessionDigest); err != nil || exists {
			return domain.ErrWebSessionRotation
		}
		next := domain.WebSessionV2{Version: 2, IdentityRevision: plan.AfterRevision, Session: session.Session}
		next.Session.SessionDigest, next.Session.CSRFTokenDigest = request.NextSessionDigest, request.NextCSRFDigest
		next.Session.AuthenticatedSubject = pending.SourceSubject
		next.Session.IssuedAt, next.Session.LastSeenAt = query.at, query.at
		next.Session.IdleExpiresAt = query.at.Add(store.webSessionIdleTTL)
		if next.Session.IdleExpiresAt.After(next.Session.AbsoluteExpiresAt) {
			next.Session.IdleExpiresAt = next.Session.AbsoluteExpiresAt
		}
		next.Session.RevokedAt = nil
		if next.Validate() != nil {
			return domain.ErrWebSessionRotation
		}
		outcome := domain.IdentityOutcomeLinkedV2
		if operation.Kind == domain.IdentityOperationUnlinkV2 {
			outcome = domain.IdentityOutcomeUnlinkedV2
		}
		terminal, err := operation.Terminalize(outcome, &plan.AfterRevision, query.at)
		if err != nil {
			return err
		}
		for _, identity := range plan.Active {
			if err := writeExternalIdentityV2(ctx, query, identity); err != nil {
				return err
			}
		}
		if plan.Unlinked != nil {
			if err := writeExternalIdentityV2(ctx, query, *plan.Unlinked); err != nil {
				return err
			}
		}
		if err := session.Session.Revoke(query.at); err != nil {
			return err
		}
		if err := writeWebSessionV2(ctx, query, session); err != nil {
			return err
		}
		if err := writeWebSessionV2(ctx, query, next); err != nil {
			return err
		}
		if err := store.appendIdentityChangeAuditV2(ctx, query, *terminal.Receipt, operation.OperationDigest); err != nil {
			return err
		}
		if err := writeIdentityOperationV2(ctx, query, terminal); err != nil {
			return err
		}
		result = identityConfirmationResultV2{Receipt: *terminal.Receipt, Session: next}
		return nil
	})
	if err != nil {
		return empty, err
	}
	return result, nil
}

func (store *Store) appendIdentityChangeAuditV2(ctx context.Context, query *ownedAuthorityQuery, receipt domain.IdentityOperationReceiptV2, operation domain.SecretDigest) error {
	if receipt.AfterRevision == nil || operation.Validate("identity_audit_v2.operation_digest") != nil {
		return domain.ErrMembershipDenied
	}
	metadata := struct {
		Provider        domain.IdentityProvider `json:"provider"`
		BeforeRevision  domain.AuthRevisionV2   `json:"before_revision"`
		AfterRevision   domain.AuthRevisionV2   `json:"after_revision"`
		OperationDigest domain.SecretDigest     `json:"operation_digest"`
	}{receipt.Provider, receipt.BeforeRevision, *receipt.AfterRevision, operation}
	raw, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	action := "web.identity.linked"
	if receipt.Outcome == domain.IdentityOutcomeUnlinkedV2 {
		action = "web.identity.unlinked"
	}
	// The exact operation digest owns audit identity; two operations in the
	// same database microsecond must not collapse into a timestamp UPSERT.
	_, err = query.ExecContext(ctx, `INSERT INTO audit_events
 (tenant_id,occurred_at,audit_event_id,actor_id,action,subject_kind,subject_id,outcome,metadata,expire_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS JsonDocument),$10)`,
		receipt.TenantID, receipt.CompletedAt, "audit-identity-"+string(operation), receipt.UserID,
		action, "identity_operation", operation, "succeeded", string(raw), receipt.CompletedAt.Add(store.operationalRetention))
	return err
}
