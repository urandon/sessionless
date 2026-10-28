package ydbstore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// All types and entrypoints here are private installation components, not
// browser DTOs, serving activation, verified-adapter issuers or reusable grants.
// The future BFF owns exact Origin/CSRF protocol handling, including callback
// exceptions; these transactions still require the original session CSRF digest.
type identityPhaseRequestV2 struct {
	OperationDigest      domain.SecretDigest
	SessionDigest        domain.SecretDigest
	CSRFDigest           domain.SecretDigest
	BrowserBindingDigest domain.SecretDigest
	ExpectedRevision     domain.AuthRevisionV2
}
type identityOperationStartV2 struct {
	Phase                 identityPhaseRequestV2
	Kind                  domain.IdentityOperationKindV2
	TargetProvider        domain.IdentityProvider
	SourceChallengeDigest domain.SecretDigest
	TargetChallengeDigest domain.SecretDigest
}
type identityProofChildV2 struct {
	Phase           identityPhaseRequestV2
	ChallengeDigest domain.SecretDigest
	PKCEVerifier    string
	Nonce           string
}
type identityProofConsumeV2 struct {
	Phase           identityPhaseRequestV2
	ChallengeDigest domain.SecretDigest
	BrowserSecret   string
}

// Only the accepted private provider adapter may supply this observation AFTER
// cryptographic/protocol verification. It has no JSON contract or verified=true
// switch. The store derives every parent/child/phase/deadline/completion field
// from canonical consumed records and the original database clock instead.
type acceptedIdentityProofV2 struct {
	Subject        domain.ExternalSubject
	Kind           domain.IdentityProviderProofKindV2
	SignedAuthTime *time.Time
	Binding        domain.IdentityProviderBindingV2
}
type identityProofCompletionV2 struct {
	Phase           identityPhaseRequestV2
	ChallengeDigest domain.SecretDigest
	Verified        acceptedIdentityProofV2
}

// This bounds stored completion from the immutable canonical consume time; it
// does not establish provider duration. The future handler must separately keep
// its original 15s deadline from BEFORE consume and its 10s exchange timeout.
const identityProofCompletionWindowV2 = 15 * time.Second

const identityProofChildSQLV2 = `SELECT SUBSTRING(CAST(record AS String),0,8193),
 SUBSTRING(CAST(browser_binding_digest AS String),0,65),
 SUBSTRING(CAST(pkce_verifier AS String),0,129),
 SUBSTRING(CAST(nonce AS String),0,161),
 SUBSTRING(CAST(redirect_path AS String),0,161),
 created_at,expires_at,consumed_at,delete_after
 FROM oidc_login_challenges WHERE shard_bucket=$1 AND state_digest=$2`

func readIdentityProofChildV2(ctx context.Context, q *ownedAuthorityQuery, digest domain.SecretDigest) (domain.IdentityChallengeV2, bool, error) {
	var empty domain.IdentityChallengeV2
	if ctx == nil || ctx.Err() != nil || q == nil || digest.Validate("identity_proof_child_v2.digest") != nil {
		return empty, false, domain.ErrMembershipDenied
	}
	bucket, err := webBucket(string(digest))
	if err != nil {
		return empty, false, err
	}
	var raw, browser, pkce, nonce, redirect string
	// V2 writes every scalar explicitly, including the pending epoch marker.
	// The original strict time collector rejects NULL/foreign types while still
	// charging every returned scalar; do not add nullable/coercing fallbacks.
	var created, expires, consumed, deleteAfter time.Time
	err = q.scanMetadata(ctx, identityProofChildSQLV2, []any{bucket, digest}, &raw, &browser, &pkce, &nonce, &redirect, &created, &expires, &consumed, &deleteAfter)
	if errors.Is(err, sql.ErrNoRows) {
		return empty, false, nil
	}
	if err != nil || len(raw) > domain.IdentityAuthMaxRecordBytesV2 {
		return empty, false, domain.ErrMembershipDenied
	}
	child, err := domain.DecodeIdentityChallengeV2([]byte(raw))
	if err != nil || child.Purpose != domain.IdentityPurposeProviderProofV2 || child.StateDigest != digest || child.Proof == nil || redirect != "" || browser != string(child.BrowserBindingDigest) || !created.Equal(child.CreatedAt) || !expires.Equal(child.ExpiresAt) || !deleteAfter.Equal(child.DeleteAfter) {
		return empty, false, domain.ErrMembershipDenied
	}
	expectedConsumed := time.Unix(0, 0).UTC()
	expectedPKCE, expectedNonce := "", ""
	if child.ConsumedAt != nil {
		expectedConsumed = *child.ConsumedAt
	}
	if child.Secrets != nil {
		expectedPKCE, expectedNonce = child.Secrets.PKCEVerifier, child.Secrets.Nonce
	}
	if pkce != expectedPKCE || nonce != expectedNonce || !consumed.Equal(expectedConsumed) {
		return empty, false, domain.ErrMembershipDenied
	}
	return child, true, nil
}

func writeIdentityProofChildV2(ctx context.Context, q *ownedAuthorityQuery, child domain.IdentityChallengeV2) error {
	if child.Purpose != domain.IdentityPurposeProviderProofV2 {
		return domain.ErrMembershipDenied
	}
	raw, err := domain.EncodeIdentityChallengeV2(child)
	if err != nil {
		return err
	}
	bucket, err := webBucket(string(child.StateDigest))
	if err != nil {
		return err
	}
	pkce, nonce := "", ""
	consumed := time.Unix(0, 0).UTC()
	if child.Secrets != nil {
		pkce, nonce = child.Secrets.PKCEVerifier, child.Secrets.Nonce
	}
	if child.ConsumedAt != nil {
		consumed = *child.ConsumedAt
	}
	_, err = q.ExecContext(ctx, `UPSERT INTO oidc_login_challenges
 (shard_bucket,state_digest,browser_binding_digest,pkce_verifier,nonce,redirect_path,
 created_at,expires_at,consumed_at,record,delete_after)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,CAST($10 AS JsonDocument),$11)`, bucket, child.StateDigest, child.BrowserBindingDigest, pkce, nonce, "", child.CreatedAt, child.ExpiresAt, consumed, string(raw), child.DeleteAfter)
	return err
}

func identityProofBindingsV2(request identityPhaseRequestV2, config identityConfigurationV2) (map[domain.IdentityProvider]domain.IdentityProviderBindingV2, error) {
	if !config.SensitiveOperationsEnabled || request.ExpectedRevision.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	for _, digest := range []domain.SecretDigest{request.OperationDigest, request.SessionDigest, request.CSRFDigest, request.BrowserBindingDigest} {
		if digest.Validate("identity_phase_v2.digest") != nil {
			return nil, domain.ErrMembershipDenied
		}
	}
	return config.bindings()
}

func currentIdentityPhaseV2(ctx context.Context, q *ownedAuthorityQuery, request identityPhaseRequestV2, config identityConfigurationV2) (domain.IdentityOperationScopeV2, []domain.ExternalIdentityV2, error) {
	var empty domain.IdentityOperationScopeV2
	session, member, err := authorizeWebSessionV2(ctx, q, request.SessionDigest, domain.TenantPermissionRead, q.at)
	if err != nil || session.Session.CSRFTokenDigest != request.CSRFDigest || session.IdentityRevision != request.ExpectedRevision {
		return empty, nil, domain.ErrMembershipDenied
	}
	active, err := readActiveExternalIdentitiesV2(ctx, q, session.Session.UserID)
	if err != nil {
		return empty, nil, err
	}
	authFound := false
	for _, identity := range active {
		if identity.Revision != request.ExpectedRevision || identity.UpdatedAt.After(q.at) {
			return empty, nil, domain.ErrMembershipDenied
		}
		authFound = authFound || identity.Subject == session.Session.AuthenticatedSubject
	}
	if !authFound {
		return empty, nil, domain.ErrMembershipDenied
	}
	return domain.IdentityOperationScopeV2{UserID: session.Session.UserID, SessionDigest: request.SessionDigest, AuthenticatedSubject: session.Session.AuthenticatedSubject, TenantID: member.TenantID, MembershipSecurityVersion: member.SecurityVersion, BrowserBindingDigest: request.BrowserBindingDigest, ConfigurationDigest: config.ManifestDigest, ExpectedRevision: session.IdentityRevision}, active, nil
}

func currentIdentityProofOperationV2(ctx context.Context, q *ownedAuthorityQuery, request identityPhaseRequestV2, config identityConfigurationV2, bindings map[domain.IdentityProvider]domain.IdentityProviderBindingV2) (domain.IdentityOperationV2, error) {
	var empty domain.IdentityOperationV2
	operation, found, err := readIdentityOperationV2(ctx, q, request.OperationDigest)
	if err != nil || !found {
		return empty, domain.ErrMembershipDenied
	}
	scope, active, err := currentIdentityPhaseV2(ctx, q, request, config)
	if err != nil || operation.ValidateCurrentScope(scope, q.at) != nil {
		return empty, domain.ErrMembershipDenied
	}
	pending := operation.Pending
	if _, enabled := bindings[pending.SourceSubject.Provider]; !enabled {
		return empty, domain.ErrIdentityProviderUnavailableV2
	}
	if operation.Kind == domain.IdentityOperationLinkV2 {
		if _, enabled := bindings[pending.TargetProvider]; !enabled {
			return empty, domain.ErrIdentityProviderUnavailableV2
		}
	}
	sourceFound := false
	targetFound := operation.Kind == domain.IdentityOperationLinkV2
	for _, identity := range active {
		sourceFound = sourceFound || identity.Subject == pending.SourceSubject
		if pending.TargetSubject != nil {
			targetFound = targetFound || identity.Subject == *pending.TargetSubject
		}
	}
	if !sourceFound || !targetFound {
		return empty, domain.ErrMembershipDenied
	}
	for _, proof := range []*domain.IdentityProviderProofV2{pending.SourceProof, pending.TargetProof} {
		if proof != nil {
			if binding, ok := bindings[proof.Subject.Provider]; !ok || binding != proof.Binding || proof.ValidateFresh(q.at) != nil {
				return empty, domain.ErrMembershipDenied
			}
		}
	}
	return operation, nil
}

// Every first observation is frozen across callbacks of this one logical call.
// A later retry must still satisfy CURRENT authority and original expiry.
func identityProofFirstTimeV2(first *time.Time, at time.Time) error {
	if at.IsZero() {
		return domain.ErrMembershipDenied
	}
	if first.IsZero() {
		*first = at
	}
	if at.Before(*first) {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (store *Store) startIdentityOperationV2(ctx context.Context, request identityOperationStartV2, config identityConfigurationV2) (domain.IdentityOperationV2, error) {
	var empty, result domain.IdentityOperationV2
	bindings, err := identityProofBindingsV2(request.Phase, config)
	if err != nil {
		return empty, err
	}
	var first time.Time
	err = store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = empty
		if identityProofFirstTimeV2(&first, q.at) != nil {
			return domain.ErrMembershipDenied
		}
		if _, found, err := readIdentityOperationV2(ctx, q, request.Phase.OperationDigest); err != nil || found {
			return domain.ErrMembershipDenied
		}
		scope, active, err := currentIdentityPhaseV2(ctx, q, request.Phase, config)
		if err != nil {
			return err
		}
		pending := &domain.IdentityOperationPendingV2{Scope: scope, SourceSubject: scope.AuthenticatedSubject, TargetProvider: request.TargetProvider, SourceChallengeDigest: request.SourceChallengeDigest, TargetChallengeDigest: request.TargetChallengeDigest}
		if request.Kind == domain.IdentityOperationLinkV2 {
			if _, ok := bindings[scope.AuthenticatedSubject.Provider]; !ok {
				return domain.ErrIdentityProviderUnavailableV2
			}
			if _, ok := bindings[request.TargetProvider]; !ok {
				return domain.ErrIdentityProviderUnavailableV2
			}
			for _, identity := range active {
				if identity.Subject.Provider == request.TargetProvider {
					return domain.ErrExternalIdentityConflict
				}
			}
		} else if request.Kind == domain.IdentityOperationUnlinkV2 {
			pending.SourceSubject = domain.ExternalSubject{}
			for _, identity := range active {
				if identity.Subject.Provider == request.TargetProvider {
					subject := identity.Subject
					pending.TargetSubject = &subject
				} else if _, ok := bindings[identity.Subject.Provider]; ok {
					pending.SourceSubject = identity.Subject
				}
			}
			if pending.TargetSubject == nil {
				return domain.ErrExternalIdentityConflict
			}
			if pending.SourceSubject.Subject == "" {
				return domain.ErrIdentityLastMethodV2
			}
		}
		operation := domain.IdentityOperationV2{Version: 2, Purpose: domain.IdentityPurposeOperationV2, OperationDigest: request.Phase.OperationDigest, Kind: request.Kind, Phase: domain.IdentityPhaseSourceProofV2, CreatedAt: first, ExpiresAt: first.Add(domain.IdentityOperationMaxLifetimeV2), DeleteAfter: first.Add(domain.IdentityOperationMaxLifetimeV2), Pending: pending}
		if operation.Validate() != nil || !q.at.Before(operation.ExpiresAt) {
			return domain.ErrMembershipDenied
		}
		for _, digest := range []domain.SecretDigest{pending.SourceChallengeDigest, pending.TargetChallengeDigest} {
			if digest != "" {
				if _, found, err := readIdentityProofChildV2(ctx, q, digest); err != nil || found {
					return domain.ErrMembershipDenied
				}
			}
		}
		if err := writeIdentityOperationV2(ctx, q, operation); err != nil {
			return err
		}
		result = operation
		return nil
	})
	if err != nil {
		return empty, err
	}
	return result, nil
}

func (store *Store) createIdentityProofV2(ctx context.Context, request identityProofChildV2, config identityConfigurationV2) (domain.IdentityChallengeV2, error) {
	var empty, result domain.IdentityChallengeV2
	bindings, err := identityProofBindingsV2(request.Phase, config)
	if err != nil {
		return empty, err
	}
	var first time.Time
	err = store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = empty
		if identityProofFirstTimeV2(&first, q.at) != nil {
			return domain.ErrMembershipDenied
		}
		operation, err := currentIdentityProofOperationV2(ctx, q, request.Phase, config, bindings)
		if err != nil {
			return err
		}
		if _, found, err := readIdentityProofChildV2(ctx, q, request.ChallengeDigest); err != nil || found {
			return domain.ErrMembershipDenied
		}
		pending := operation.Pending
		provider := pending.SourceSubject.Provider
		proof := &domain.IdentityProofChallengeV2{ParentOperationDigest: operation.OperationDigest, ParentPhase: operation.Phase, Scope: pending.Scope}
		if operation.Phase == domain.IdentityPhaseSourceProofV2 {
			subject := pending.SourceSubject
			proof.ExpectedSubject = &subject
		} else {
			provider = pending.TargetProvider
		}
		child := domain.IdentityChallengeV2{Version: 2, Purpose: domain.IdentityPurposeProviderProofV2, StateDigest: request.ChallengeDigest, BrowserBindingDigest: pending.Scope.BrowserBindingDigest, Binding: bindings[provider], CreatedAt: first, ExpiresAt: operation.ExpiresAt, DeleteAfter: operation.ExpiresAt, Proof: proof, Secrets: &domain.IdentityChallengeSecretsV2{PKCEVerifier: request.PKCEVerifier, Nonce: request.Nonce}}
		if child.ValidateForOperation(operation) != nil {
			return domain.ErrMembershipDenied
		}
		if err := writeIdentityProofChildV2(ctx, q, child); err != nil {
			return err
		}
		result = child
		return nil
	})
	if err != nil {
		return empty, err
	}
	return result, nil
}

func (store *Store) consumeIdentityProofV2(ctx context.Context, request identityProofConsumeV2, config identityConfigurationV2) (domain.IdentityChallengeExchangeV2, error) {
	var empty, result domain.IdentityChallengeExchangeV2
	bindings, err := identityProofBindingsV2(request.Phase, config)
	if err != nil {
		return empty, err
	}
	if request.BrowserSecret == "" || domain.DigestSecret(request.BrowserSecret) != request.Phase.BrowserBindingDigest {
		return empty, domain.ErrMembershipDenied
	}
	var first time.Time
	err = store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = empty
		if identityProofFirstTimeV2(&first, q.at) != nil {
			return domain.ErrMembershipDenied
		}
		operation, err := currentIdentityProofOperationV2(ctx, q, request.Phase, config, bindings)
		if err != nil {
			return err
		}
		child, found, err := readIdentityProofChildV2(ctx, q, request.ChallengeDigest)
		if err != nil || !found || !q.at.Before(child.ExpiresAt) {
			return domain.ErrMembershipDenied
		}
		cleared, exchange, err := child.Consume(bindings[child.Binding.Provider], request.BrowserSecret, &operation, first)
		if err != nil {
			return err
		}
		if err := writeIdentityProofChildV2(ctx, q, cleared); err != nil {
			return err
		}
		result = exchange
		return nil
	})
	if err != nil {
		return empty, err
	}
	return result, nil
}

func (store *Store) recordIdentityProofV2(ctx context.Context, request identityProofCompletionV2, config identityConfigurationV2) (domain.IdentityOperationV2, error) {
	var empty, result domain.IdentityOperationV2
	bindings, err := identityProofBindingsV2(request.Phase, config)
	if err != nil {
		return empty, err
	}
	verified := request.Verified
	if verified.SignedAuthTime != nil {
		at := *verified.SignedAuthTime
		verified.SignedAuthTime = &at
	}
	var first time.Time
	err = store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = empty
		if identityProofFirstTimeV2(&first, q.at) != nil {
			return domain.ErrMembershipDenied
		}
		operation, err := currentIdentityProofOperationV2(ctx, q, request.Phase, config, bindings)
		if err != nil {
			return err
		}
		child, found, err := readIdentityProofChildV2(ctx, q, request.ChallengeDigest)
		if err != nil || !found || child.ConsumedAt == nil || child.Secrets != nil || child.ValidateForOperation(operation) != nil || !q.at.Before(child.ExpiresAt) || verified.Binding != child.Binding || bindings[verified.Subject.Provider] != verified.Binding {
			return domain.ErrMembershipDenied
		}
		// Recheck the CURRENT database observation on every transaction retry. A
		// retained first record timestamp cannot extend the consumed child's bound.
		if !q.at.Before(child.ConsumedAt.Add(identityProofCompletionWindowV2)) {
			return domain.ErrMembershipDenied
		}
		if operation.Phase == domain.IdentityPhaseTargetProofV2 {
			target, found, err := readExternalIdentityV2(ctx, q, verified.Subject)
			if err != nil {
				return err
			}
			if found && (target.UserID != operation.Pending.Scope.UserID || target.State != domain.ExternalIdentityUnlinkedV2) {
				return domain.ErrExternalIdentityConflict
			}
		}
		proof := domain.IdentityProviderProofV2{ChallengeDigest: child.StateDigest, ParentOperationDigest: operation.OperationDigest, ParentPhase: child.Proof.ParentPhase, Binding: child.Binding, Subject: verified.Subject, Kind: verified.Kind, ChallengeCreatedAt: child.CreatedAt, ChallengeExpiresAt: child.ExpiresAt, CompletedAt: first, SignedAuthTime: verified.SignedAuthTime}
		next, err := operation.RecordProof(child, proof, q.at)
		if err != nil {
			return err
		}
		if err := writeIdentityOperationV2(ctx, q, next); err != nil {
			return err
		}
		result = next
		return nil
	})
	if err != nil {
		return empty, err
	}
	return result, nil
}
