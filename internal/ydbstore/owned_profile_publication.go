package ydbstore

import (
	"context"
	"database/sql"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const ownedProfileAuditSQL = `SELECT SUBSTRING(CAST(intent_digest AS String),0,65), SUBSTRING(record,0,32769),
 SUBSTRING(CAST(record_digest AS String),0,65), revocation_generation, published_at
 FROM owned_profile_publication_audit_v1 WHERE tenant_id=$1 AND locator_digest=$2 AND mutation_id=$3`

const ownedProfileWriteSQL = `UPSERT INTO owned_agent_profile_current_v1
 (tenant_id,owner_user_id,profile,backend_profile_digest,resource_kind,resource_id,model_vendor_id,model_id,input_data_class,
 record,version,profile_revision,revocation_generation,state,digest,installation_id,installation_revision,published_at,expires_at,updated_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`

const ownedProfileAuditWriteSQL = `INSERT INTO owned_profile_publication_audit_v1
 (tenant_id,locator_digest,mutation_id,intent_digest,record,record_digest,revocation_generation,published_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`

// PublishOwnedProfile is a storage prerequisite, not a public installation API.
// No production verifier/issuer is installed in this slice: the default Store
// denies before SQL. Only the trusted composition may supply an authenticated
// source-verified grant; its verification cannot perform I/O in this callback.
// Serializable exact-key reads plus immutable audit INSERT make head/audit one
// atomic CAS. No capability is minted, and every speculative result is reset on
// retry and discarded on denied/ambiguous final commit.
func (store *Store) PublishOwnedProfile(ctx context.Context, grant ports.OwnedProfilePublicationGrantV1) (ports.OwnedProfilePublicationReceiptV1, error) {
	denied := func() (ports.OwnedProfilePublicationReceiptV1, error) {
		return ports.OwnedProfilePublicationReceiptV1{}, domain.ErrMembershipDenied
	}
	if store == nil || store.db == nil || store.ownedProfilePublicationVerifier == nil || grant.Validate() != nil {
		return denied()
	}
	grant = grant.Clone()
	intentDigest, err := grant.IntentDigest()
	if err != nil {
		return denied()
	}
	locatorDigest, err := grant.Profile.Locator.Digest()
	if err != nil {
		return denied()
	}
	var pending ports.OwnedProfilePublicationReceiptV1
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		pending = ports.OwnedProfilePublicationReceiptV1{}
		if q.at.Before(grant.IssuedAt) || !q.at.Before(grant.ExpiresAt) || store.ownedProfilePublicationVerifier.VerifyOwnedProfilePublication(grant.Clone(), q.at) != nil {
			return domain.ErrMembershipDenied
		}
		prior, found, err := readOwnedProfilePublicationAudit(ctx, q, grant, locatorDigest, intentDigest)
		if err != nil {
			return err
		}
		if found {
			pending = prior
			return nil // historical readback; never write/revive a head on replay
		}
		old, exists, err := loadOwnedProfileProjection(ctx, q, grant.Profile.Locator)
		if err != nil || (grant.ExpectedGeneration == 0 && exists) || (grant.ExpectedGeneration != 0 && !exists) {
			return domain.ErrMembershipDenied
		}
		next := grant.Profile.Clone()
		next.UpdatedAt = q.at
		if next.State == domain.OwnedAgentCurrentActiveV1 {
			next.Window.PublishedAt = q.at
		}
		if !exists {
			if next.ValidateInitial() != nil {
				return domain.ErrMembershipDenied
			}
		} else {
			digest, digestErr := old.Digest()
			if digestErr != nil || old.RevocationGeneration != grant.ExpectedGeneration || digest != grant.ExpectedDigest || next.ValidateTransitionFrom(old) != nil {
				return domain.ErrMembershipDenied
			}
		}
		if next.Validate() != nil || (next.State == domain.OwnedAgentCurrentActiveV1 && next.ValidateAt(q.at) != nil) {
			return domain.ErrMembershipDenied
		}
		raw, err := domain.EncodeOwnedAgentProfileCurrentV1(next)
		digest, digestErr := next.Digest()
		if err != nil || digestErr != nil {
			return domain.ErrMembershipDenied
		}
		l := next.Locator
		if _, err := q.ExecContext(ctx, ownedProfileWriteSQL, l.TenantID, l.OwnerUserID, l.Profile, l.BackendProfileDigest, l.ResourceKind, l.ResourceID, l.ModelVendorID, l.ModelID, l.InputDataClass,
			string(raw), uint64(next.Version), next.ProfileRevision, next.RevocationGeneration, next.State, digest, next.InstallationID, next.InstallationRevision, next.Window.PublishedAt, next.Window.ExpiresAt, next.UpdatedAt); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, ownedProfileAuditWriteSQL, l.TenantID, locatorDigest, next.MutationID, intentDigest, string(raw), digest, next.RevocationGeneration, q.at); err != nil {
			return err
		}
		pending = ports.OwnedProfilePublicationReceiptV1{Profile: next.Clone(), Digest: digest}
		return nil
	})
	if err != nil {
		return denied()
	}
	pending.Profile = pending.Profile.Clone()
	return pending, nil
}

func readOwnedProfilePublicationAudit(ctx context.Context, q *ownedAuthorityQuery, grant ports.OwnedProfilePublicationGrantV1, locatorDigest, intentDigest string) (ports.OwnedProfilePublicationReceiptV1, bool, error) {
	denied := func() (ports.OwnedProfilePublicationReceiptV1, bool, error) {
		return ports.OwnedProfilePublicationReceiptV1{}, false, domain.ErrMembershipDenied
	}
	var intent, raw, digest string
	var generation uint64
	var published time.Time
	row := q.QueryRowContext(ctx, ownedProfileAuditSQL, grant.Profile.Locator.TenantID, locatorDigest, grant.Profile.MutationID)
	returned, err := scanOwnedCurrentRow(row, &intent, &raw, &digest, &generation, &published)
	charged := q.budget.materialized(q.callback, returned)
	if charged == nil && err == sql.ErrNoRows {
		return ports.OwnedProfilePublicationReceiptV1{}, false, nil
	}
	if charged != nil || err != nil || len(raw) > ownedAuthorityHeadBytes || len(digest) != 64 || intent != intentDigest || published.Before(grant.IssuedAt) || published.After(q.at) {
		return denied()
	}
	p, err := domain.DecodeOwnedAgentProfileCurrentV1([]byte(raw))
	actual, digestErr := p.Digest()
	if err != nil || digestErr != nil || actual != digest || p.Locator != grant.Profile.Locator || p.MutationID != grant.Profile.MutationID || p.RevocationGeneration != grant.Profile.RevocationGeneration || generation != p.RevocationGeneration || !published.Equal(p.UpdatedAt) {
		return denied()
	}
	// Reconstruct the exact original template with its DB-owned timestamps.
	expected := grant.Profile.Clone()
	expected.UpdatedAt = published
	if expected.State == domain.OwnedAgentCurrentActiveV1 {
		expected.Window.PublishedAt = published
	}
	expectedDigest, err := expected.Digest()
	if err != nil || expectedDigest != digest {
		return denied()
	}
	return ports.OwnedProfilePublicationReceiptV1{Profile: p.Clone(), Digest: digest, Replayed: true}, true, nil
}
