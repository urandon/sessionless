package ydbstore

import (
	"context"
	"database/sql"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const spendingAuditSQL = `SELECT SUBSTRING(CAST(intent_digest AS String),0,65), SUBSTRING(record,0,32769),
 SUBSTRING(CAST(record_digest AS String),0,65), revocation_generation, published_at,
 SUBSTRING(CAST(run_id AS String),0,161), SUBSTRING(CAST(session_id AS String),0,161)
 FROM attempt_spending_publication_audit_v1
 WHERE tenant_id=$1 AND attempt_id=$2 AND component_kind=$3 AND billing_owner_user_id=$4 AND mutation_id=$5`

const spendingWriteSQL = `UPSERT INTO attempt_spending_authorization_current_v1
 (tenant_id,attempt_id,component_kind,billing_owner_user_id,record,version,revocation_generation,state,digest,
 run_id,session_id,owner_user_id,published_at,expires_at,updated_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`

const spendingAuditWriteSQL = `INSERT INTO attempt_spending_publication_audit_v1
 (tenant_id,attempt_id,component_kind,billing_owner_user_id,mutation_id,intent_digest,record,record_digest,revocation_generation,published_at,run_id,session_id)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`

// Storage prerequisite only: authentic installed admission/source composition
// and lifecycle integration remain required before any serving caller exists.
// Creation point-joins the original profile in this same serializable snapshot.
// Revocation and exact receipt recovery are cleanup/history, not new effects:
// neither needs to resurrect an expired/revoked source. Verifier authentication
// remains required for both. No public grant decoder, balance or debit is added.
func (store *Store) PublishAttemptSpending(ctx context.Context, grant ports.AttemptSpendingPublicationGrantV1) (ports.AttemptSpendingPublicationReceiptV1, error) {
	denied := func() (ports.AttemptSpendingPublicationReceiptV1, error) {
		return ports.AttemptSpendingPublicationReceiptV1{}, domain.ErrMembershipDenied
	}
	if store == nil || store.db == nil || store.attemptSpendingPublicationVerifier == nil || grant.Validate() != nil {
		return denied()
	}
	grant = grant.Clone()
	intentDigest, err := grant.IntentDigest()
	if err != nil {
		return denied()
	}
	var pending ports.AttemptSpendingPublicationReceiptV1
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		pending = ports.AttemptSpendingPublicationReceiptV1{}
		if q.at.Before(grant.IssuedAt) || !q.at.Before(grant.ExpiresAt) || store.attemptSpendingPublicationVerifier.VerifyAttemptSpendingPublication(grant.Clone(), q.at) != nil {
			return domain.ErrMembershipDenied
		}
		prior, found, err := readSpendingPublicationAudit(ctx, q, grant, intentDigest)
		if err != nil {
			return err
		}
		if found {
			pending = prior
			return nil
		}
		old, exists, err := loadOwnedSpendingProjection(ctx, q, grant.Authorization.Locator)
		if err != nil || (grant.ExpectedGeneration == 0 && exists) || (grant.ExpectedGeneration != 0 && !exists) {
			return domain.ErrMembershipDenied
		}
		next := grant.Authorization.Clone()
		next.UpdatedAt = q.at
		if next.State == domain.OwnedAgentCurrentActiveV1 {
			next.Window.PublishedAt = q.at
		}
		if !exists {
			p, err := readOwnedProfileCurrent(ctx, q, next.SourceProfile)
			if err != nil || next.ValidateInitial() != nil || next.ValidateAgainstProfile(p, q.at) != nil {
				return domain.ErrMembershipDenied
			}
		} else {
			digest, digestErr := old.Digest()
			if digestErr != nil || old.RevocationGeneration != grant.ExpectedGeneration || digest != grant.ExpectedDigest || next.ValidateTransitionFrom(old) != nil {
				return domain.ErrMembershipDenied
			}
		}
		if next.Validate() != nil {
			return domain.ErrMembershipDenied
		}
		pending, err = writeAttemptSpendingPostimageTx(ctx, q, next, intentDigest)
		return err
	})
	if err != nil {
		return denied()
	}
	pending.Authorization = pending.Authorization.Clone()
	return pending, nil
}

// Exact existing head/audit writer, not an issuer or permission check. Callers
// authenticate the SAME original grant, current profile and initial absence or
// transition in THIS callback before composing this body. Recovery can thus
// materialize several missing components without nested public transactions.
func writeAttemptSpendingPostimageTx(ctx context.Context, q *ownedAuthorityQuery, next domain.AttemptSpendingAuthorizationCurrentV1, intentDigest string) (ports.AttemptSpendingPublicationReceiptV1, error) {
	deny := func() (ports.AttemptSpendingPublicationReceiptV1, error) {
		return ports.AttemptSpendingPublicationReceiptV1{}, q.budget.deny()
	}
	if next.Validate() != nil || !next.UpdatedAt.Equal(q.at) || domain.ValidateSHA256Digest("spending.intent", intentDigest) != nil {
		return deny()
	}
	raw, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(next)
	digest, digestErr := next.Digest()
	if err != nil || digestErr != nil {
		return deny()
	}
	l := next.Locator
	if _, err := q.ExecContext(ctx, spendingWriteSQL, l.TenantID, l.AttemptID, l.ComponentKind, l.BillingOwnerUserID, string(raw), uint64(next.Version), next.RevocationGeneration, next.State, digest, next.RunID, next.SessionID, next.OwnerUserID, next.Window.PublishedAt, next.Window.ExpiresAt, next.UpdatedAt); err != nil {
		return ports.AttemptSpendingPublicationReceiptV1{}, err
	}
	if _, err := q.ExecContext(ctx, spendingAuditWriteSQL, l.TenantID, l.AttemptID, l.ComponentKind, l.BillingOwnerUserID, next.MutationID, intentDigest, string(raw), digest, next.RevocationGeneration, q.at, next.RunID, next.SessionID); err != nil {
		return ports.AttemptSpendingPublicationReceiptV1{}, err
	}
	return ports.AttemptSpendingPublicationReceiptV1{Authorization: next.Clone(), Digest: digest}, nil
}

func readSpendingPublicationAudit(ctx context.Context, q *ownedAuthorityQuery, grant ports.AttemptSpendingPublicationGrantV1, intentDigest string) (ports.AttemptSpendingPublicationReceiptV1, bool, error) {
	denied := func() (ports.AttemptSpendingPublicationReceiptV1, bool, error) {
		return ports.AttemptSpendingPublicationReceiptV1{}, false, domain.ErrMembershipDenied
	}
	var intent, raw, digest, run, session string
	var generation uint64
	var published time.Time
	l := grant.Authorization.Locator
	row := q.QueryRowContext(ctx, spendingAuditSQL, l.TenantID, l.AttemptID, l.ComponentKind, l.BillingOwnerUserID, grant.Authorization.MutationID)
	returned, err := scanOwnedCurrentRow(row, &intent, &raw, &digest, &generation, &published, &run, &session)
	charged := q.budget.materialized(q.callback, returned)
	if charged == nil && err == sql.ErrNoRows {
		return ports.AttemptSpendingPublicationReceiptV1{}, false, nil
	}
	if charged != nil || err != nil || len(raw) > ownedAuthorityHeadBytes || len(digest) != 64 || len(run) > 160 || len(session) > 160 || intent != intentDigest || published.Before(grant.IssuedAt) || published.After(q.at) {
		return denied()
	}
	v, err := domain.DecodeAttemptSpendingAuthorizationCurrentV1([]byte(raw))
	actual, digestErr := v.Digest()
	if err != nil || digestErr != nil || actual != digest || v.Locator != l || v.MutationID != grant.Authorization.MutationID || v.RevocationGeneration != grant.Authorization.RevocationGeneration || generation != v.RevocationGeneration || !published.Equal(v.UpdatedAt) || string(v.RunID) != run || string(v.SessionID) != session {
		return denied()
	}
	expected := grant.Authorization.Clone()
	expected.UpdatedAt = published
	if expected.State == domain.OwnedAgentCurrentActiveV1 {
		expected.Window.PublishedAt = published
	}
	expectedDigest, err := expected.Digest()
	if err != nil || expectedDigest != digest {
		return denied()
	}
	return ports.AttemptSpendingPublicationReceiptV1{Authorization: v.Clone(), Digest: digest, Replayed: true}, true, nil
}
