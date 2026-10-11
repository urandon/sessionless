package ydbstore

import (
	"context"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// One bounded exact-key read for the original profile and all four original
// spending heads. INNER JOIN makes any missing head a miss; it never discovers
// a replacement locator or scans history. This is correlation only, not source
// authentication or a spending/debit effect.
const ownedAdmissionSourcesJoinSQL = `SELECT
 SUBSTRING(CAST(p.record AS String),0,32769), p.version, p.profile_revision, p.revocation_generation,
 SUBSTRING(CAST(p.state AS String),0,17), SUBSTRING(CAST(p.digest AS String),0,65),
 SUBSTRING(CAST(p.installation_id AS String),0,161), p.installation_revision,
 p.published_at, p.expires_at, p.updated_at,
 SUBSTRING(CAST(s0.record AS String),0,32769), s0.version, s0.revocation_generation,
 SUBSTRING(CAST(s0.state AS String),0,17), SUBSTRING(CAST(s0.digest AS String),0,65),
 SUBSTRING(CAST(s0.run_id AS String),0,161), SUBSTRING(CAST(s0.session_id AS String),0,161),
 SUBSTRING(CAST(s0.owner_user_id AS String),0,161), s0.published_at, s0.expires_at, s0.updated_at,
 SUBSTRING(CAST(s1.record AS String),0,32769), s1.version, s1.revocation_generation,
 SUBSTRING(CAST(s1.state AS String),0,17), SUBSTRING(CAST(s1.digest AS String),0,65),
 SUBSTRING(CAST(s1.run_id AS String),0,161), SUBSTRING(CAST(s1.session_id AS String),0,161),
 SUBSTRING(CAST(s1.owner_user_id AS String),0,161), s1.published_at, s1.expires_at, s1.updated_at,
 SUBSTRING(CAST(s2.record AS String),0,32769), s2.version, s2.revocation_generation,
 SUBSTRING(CAST(s2.state AS String),0,17), SUBSTRING(CAST(s2.digest AS String),0,65),
 SUBSTRING(CAST(s2.run_id AS String),0,161), SUBSTRING(CAST(s2.session_id AS String),0,161),
 SUBSTRING(CAST(s2.owner_user_id AS String),0,161), s2.published_at, s2.expires_at, s2.updated_at,
 SUBSTRING(CAST(s3.record AS String),0,32769), s3.version, s3.revocation_generation,
 SUBSTRING(CAST(s3.state AS String),0,17), SUBSTRING(CAST(s3.digest AS String),0,65),
 SUBSTRING(CAST(s3.run_id AS String),0,161), SUBSTRING(CAST(s3.session_id AS String),0,161),
 SUBSTRING(CAST(s3.owner_user_id AS String),0,161), s3.published_at, s3.expires_at, s3.updated_at
 FROM owned_agent_profile_current_v1 AS p
 INNER JOIN attempt_spending_authorization_current_v1 AS s0
  ON s0.tenant_id=p.tenant_id
 INNER JOIN attempt_spending_authorization_current_v1 AS s1
  ON s1.tenant_id=p.tenant_id
 INNER JOIN attempt_spending_authorization_current_v1 AS s2
  ON s2.tenant_id=p.tenant_id
 INNER JOIN attempt_spending_authorization_current_v1 AS s3
  ON s3.tenant_id=p.tenant_id
 WHERE p.tenant_id=$1 AND p.owner_user_id=$2 AND p.profile=$3 AND p.backend_profile_digest=$4
 AND p.resource_kind=$5 AND p.resource_id=$6 AND p.model_vendor_id=$7 AND p.model_id=$8 AND p.input_data_class=$9
 AND s0.tenant_id=$10 AND s0.attempt_id=$11 AND s0.component_kind=$12 AND s0.billing_owner_user_id=$13
 AND s1.tenant_id=$14 AND s1.attempt_id=$15 AND s1.component_kind=$16 AND s1.billing_owner_user_id=$17
 AND s2.tenant_id=$18 AND s2.attempt_id=$19 AND s2.component_kind=$20 AND s2.billing_owner_user_id=$21
 AND s3.tenant_id=$22 AND s3.attempt_id=$23 AND s3.component_kind=$24 AND s3.billing_owner_user_id=$25`

// readOwnedAdmissionSources consumes only the original source attachment already
// loaded by its caller. It establishes exact current-row correlation in that
// caller's serializable transaction; it does not authenticate the attachment.
func readOwnedAdmissionSources(ctx context.Context, q *ownedAuthorityQuery, source domain.AttemptExecutionSourceV1) (ownedCurrentSnapshot, error) {
	denied := func() (ownedCurrentSnapshot, error) { return ownedCurrentSnapshot{}, domain.ErrMembershipDenied }
	if q == nil || q.budget == nil || source.Validate() != nil || q.at.IsZero() || q.at.Nanosecond()%1000 != 0 {
		return denied()
	}
	l := source.Profile.Locator
	args := []any{l.TenantID, l.OwnerUserID, l.Profile, l.BackendProfileDigest, l.ResourceKind, l.ResourceID, l.ModelVendorID, l.ModelID, l.InputDataClass}
	for _, spending := range source.Spending {
		locator := spending.Locator
		args = append(args, locator.TenantID, locator.AttemptID, locator.ComponentKind, locator.BillingOwnerUserID)
	}

	var pRaw, pState, pDigest, pInstallation string
	var pVersion, pRevision, pGeneration, pInstallationRevision uint64
	var pPublished, pExpires, pUpdated time.Time
	destinations := []any{&pRaw, &pVersion, &pRevision, &pGeneration, &pState, &pDigest, &pInstallation, &pInstallationRevision, &pPublished, &pExpires, &pUpdated}
	type spendingColumns struct {
		raw, state, digest, run, session, owner string
		version, generation                     uint64
		published, expires, updated             time.Time
	}
	var columns [4]spendingColumns
	for i := range columns {
		c := &columns[i]
		destinations = append(destinations, &c.raw, &c.version, &c.generation, &c.state, &c.digest, &c.run, &c.session, &c.owner, &c.published, &c.expires, &c.updated)
	}
	if len(destinations) != 55 {
		return denied()
	}
	// scanMetadata scans and charges every scalar before interpreting any of
	// them, including columns following malformed/nil values.
	if err := q.scanMetadata(ctx, ownedAdmissionSourcesJoinSQL, args, destinations...); err != nil {
		return denied()
	}
	if len(pRaw) > ownedAuthorityHeadBytes || len(pState) > 16 || len(pDigest) != 64 || len(pInstallation) > 160 ||
		!utf8.ValidString(pRaw) || !utf8.ValidString(pState) || !utf8.ValidString(pDigest) || !utf8.ValidString(pInstallation) {
		return denied()
	}
	profile, err := domain.DecodeOwnedAgentProfileCurrentV1([]byte(pRaw))
	if err != nil {
		return denied()
	}
	profileDigest, err := profile.Digest()
	if err != nil || profile.Locator != l || uint64(profile.Version) != pVersion || profile.ProfileRevision != pRevision ||
		profile.RevocationGeneration != pGeneration || string(profile.State) != pState || profileDigest != pDigest ||
		profile.InstallationID != pInstallation || profile.InstallationRevision != pInstallationRevision ||
		!profile.Window.PublishedAt.Equal(pPublished) || !profile.Window.ExpiresAt.Equal(pExpires) || !profile.UpdatedAt.Equal(pUpdated) {
		return denied()
	}

	var spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	for i, c := range columns {
		if len(c.raw) > ownedAuthorityHeadBytes || len(c.state) > 16 || len(c.digest) != 64 || len(c.run) > 160 || len(c.session) > 160 || len(c.owner) > 160 ||
			!utf8.ValidString(c.raw) || !utf8.ValidString(c.state) || !utf8.ValidString(c.digest) || !utf8.ValidString(c.run) || !utf8.ValidString(c.session) || !utf8.ValidString(c.owner) {
			return denied()
		}
		value, decodeErr := domain.DecodeAttemptSpendingAuthorizationCurrentV1([]byte(c.raw))
		if decodeErr != nil {
			return denied()
		}
		digest, digestErr := value.Digest()
		if digestErr != nil || value.Locator != source.Spending[i].Locator || uint64(value.Version) != c.version ||
			value.RevocationGeneration != c.generation || string(value.State) != c.state || digest != c.digest ||
			string(value.RunID) != c.run || string(value.SessionID) != c.session || string(value.OwnerUserID) != c.owner ||
			!value.Window.PublishedAt.Equal(c.published) || !value.Window.ExpiresAt.Equal(c.expires) || !value.UpdatedAt.Equal(c.updated) {
			return denied()
		}
		spending[i] = value
	}
	if source.ValidateAgainstCurrentSources(profile, spending, q.at) != nil {
		return denied()
	}
	return ownedCurrentSnapshot{profile: profile, spending: spending}, nil
}
