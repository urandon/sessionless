package ydbstore

import (
	"context"
	"database/sql"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const ownedProfileCurrentSQL = `SELECT SUBSTRING(record,0,32769), version, profile_revision, revocation_generation,
 SUBSTRING(CAST(state AS String),0,17), SUBSTRING(CAST(digest AS String),0,65),
 SUBSTRING(CAST(installation_id AS String),0,161), installation_revision,
 published_at, expires_at, updated_at FROM owned_agent_profile_current_v1
 WHERE tenant_id=$1 AND owner_user_id=$2 AND profile=$3 AND backend_profile_digest=$4
 AND resource_kind=$5 AND resource_id=$6 AND model_vendor_id=$7 AND model_id=$8 AND input_data_class=$9`

const ownedSpendingCurrentSQL = `SELECT SUBSTRING(record,0,32769), version, revocation_generation,
 SUBSTRING(CAST(state AS String),0,17), SUBSTRING(CAST(digest AS String),0,65),
 SUBSTRING(CAST(run_id AS String),0,161), SUBSTRING(CAST(session_id AS String),0,161),
 SUBSTRING(CAST(owner_user_id AS String),0,161), published_at, expires_at, updated_at
 FROM attempt_spending_authorization_current_v1
 WHERE tenant_id=$1 AND attempt_id=$2 AND component_kind=$3 AND billing_owner_user_id=$4`

type ownedSpendingReference struct {
	locator    domain.AttemptSpendingAuthorizationLocatorV1
	generation uint64
	digest     string
}

// These private arguments must come from the original canonical admission
// attachment loaded in this transaction, NEVER a public request. That attachment
// and its writer are a remaining #189 prerequisite. A successful reader result
// only establishes row correlation; it cannot authenticate the original source,
// establish Session/lease permission, debit, prepare, mint or send anything.
type ownedCurrentScope struct {
	tenant  domain.TenantID
	run     domain.RunID
	session domain.SessionID
	owner   domain.UserID
	attempt domain.AttemptID
}

type ownedCurrentSnapshot struct {
	profile  domain.OwnedAgentProfileCurrentV1
	spending [4]domain.AttemptSpendingAuthorizationCurrentV1
}

var ownedSpendingKinds = [4]domain.AttemptSpendingComponentKindV1{
	domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1,
	domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1,
}

// Exact point joins of the original profile and all four original grants. No
// historical-body fallback, model/registration discovery, scan or source RPC.
func readOwnedCurrentSnapshot(ctx context.Context, q *ownedAuthorityQuery, scope ownedCurrentScope, profileRef domain.OwnedAgentProfileReferenceV1, refs [4]ownedSpendingReference) (ownedCurrentSnapshot, error) {
	denied := func() (ownedCurrentSnapshot, error) { return ownedCurrentSnapshot{}, domain.ErrMembershipDenied }
	for _, validate := range []func() error{scope.tenant.Validate, scope.run.Validate, scope.session.Validate, scope.owner.Validate, scope.attempt.Validate, profileRef.Validate} {
		if validate() != nil {
			return denied()
		}
	}
	if scope.tenant != profileRef.Locator.TenantID || scope.owner != profileRef.Locator.OwnerUserID {
		return denied()
	}
	for i, ref := range refs {
		if ref.locator.Validate() != nil || ref.generation == 0 || domain.ValidateSHA256Digest("spending.digest", ref.digest) != nil || ref.locator.TenantID != scope.tenant || ref.locator.AttemptID != scope.attempt || ref.locator.ComponentKind != ownedSpendingKinds[i] {
			return denied()
		}
	}
	p, err := readOwnedProfileCurrent(ctx, q, profileRef)
	if err != nil {
		return denied()
	}
	result := ownedCurrentSnapshot{profile: p}
	for i, ref := range refs {
		if ref.locator.BillingOwnerUserID != p.BillingOwnerUserID {
			return denied()
		}
		v, err := readOwnedSpendingCurrent(ctx, q, ref)
		if err != nil || v.RunID != scope.run || v.SessionID != scope.session || v.OwnerUserID != scope.owner || v.ValidateAgainstProfile(p, q.at) != nil {
			return denied()
		}
		result.spending[i] = v
	}
	return result, nil
}

func readOwnedProfileCurrent(ctx context.Context, q *ownedAuthorityQuery, ref domain.OwnedAgentProfileReferenceV1) (domain.OwnedAgentProfileCurrentV1, error) {
	p, found, err := loadOwnedProfileProjection(ctx, q, ref.Locator)
	actualDigest, digestErr := p.Digest()
	if err != nil || !found || digestErr != nil || p.RevocationGeneration != ref.RevocationGeneration || actualDigest != ref.Digest || p.ValidateAt(q.at) != nil {
		return domain.OwnedAgentProfileCurrentV1{}, domain.ErrMembershipDenied
	}
	return p, nil
}

// Publication may inspect an absent head or a disabled/revoked tombstone.
// This structural load is NEVER current permission; effect readers additionally
// require the original reference and active DB-time freshness above.
func loadOwnedProfileProjection(ctx context.Context, q *ownedAuthorityQuery, l domain.OwnedAgentProfileLocatorV1) (domain.OwnedAgentProfileCurrentV1, bool, error) {
	denied := func() (domain.OwnedAgentProfileCurrentV1, bool, error) {
		return domain.OwnedAgentProfileCurrentV1{}, false, domain.ErrMembershipDenied
	}
	if l.Validate() != nil {
		return denied()
	}
	var raw, state, digest, installation string
	var version, revision, generation, installationRevision uint64
	var published, expires, updated time.Time
	row := q.QueryRowContext(ctx, ownedProfileCurrentSQL, l.TenantID, l.OwnerUserID, l.Profile, l.BackendProfileDigest, l.ResourceKind, l.ResourceID, l.ModelVendorID, l.ModelID, l.InputDataClass)
	returned, err := scanOwnedCurrentRow(row, &raw, &version, &revision, &generation, &state, &digest, &installation, &installationRevision, &published, &expires, &updated)
	// Scan every returned scalar even when an earlier column is NULL/malformed.
	// Charge all returned bytes before validation, never just the assigned fields.
	charged := q.budget.materialized(q.callback, returned)
	if charged == nil && err == sql.ErrNoRows {
		return domain.OwnedAgentProfileCurrentV1{}, false, nil
	}
	if charged != nil || err != nil || len(raw) > ownedAuthorityHeadBytes || len(state) > 16 || len(digest) != 64 || len(installation) > 160 {
		return denied()
	}
	p, err := domain.DecodeOwnedAgentProfileCurrentV1([]byte(raw))
	actualDigest, digestErr := p.Digest()
	if err != nil || digestErr != nil || p.Locator != l ||
		uint64(p.Version) != version || p.ProfileRevision != revision || p.RevocationGeneration != generation || string(p.State) != state || actualDigest != digest ||
		p.InstallationID != installation || p.InstallationRevision != installationRevision || !p.Window.PublishedAt.Equal(published) || !p.Window.ExpiresAt.Equal(expires) || !p.UpdatedAt.Equal(updated) {
		return denied()
	}
	return p, true, nil
}

func readOwnedSpendingCurrent(ctx context.Context, q *ownedAuthorityQuery, ref ownedSpendingReference) (domain.AttemptSpendingAuthorizationCurrentV1, error) {
	v, found, err := loadOwnedSpendingProjection(ctx, q, ref.locator)
	digest, digestErr := v.Digest()
	if err != nil || !found || digestErr != nil || v.RevocationGeneration != ref.generation || digest != ref.digest || v.ValidateAt(q.at) != nil {
		return domain.AttemptSpendingAuthorizationCurrentV1{}, domain.ErrMembershipDenied
	}
	return v, nil
}

// Structural tombstone/absence lookup for publication only, not permission.
func loadOwnedSpendingProjection(ctx context.Context, q *ownedAuthorityQuery, l domain.AttemptSpendingAuthorizationLocatorV1) (domain.AttemptSpendingAuthorizationCurrentV1, bool, error) {
	denied := func() (domain.AttemptSpendingAuthorizationCurrentV1, bool, error) {
		return domain.AttemptSpendingAuthorizationCurrentV1{}, false, domain.ErrMembershipDenied
	}
	if l.Validate() != nil {
		return denied()
	}
	var raw, state, digest, run, session, owner string
	var version, generation uint64
	var published, expires, updated time.Time
	row := q.QueryRowContext(ctx, ownedSpendingCurrentSQL, l.TenantID, l.AttemptID, l.ComponentKind, l.BillingOwnerUserID)
	returned, err := scanOwnedCurrentRow(row, &raw, &version, &generation, &state, &digest, &run, &session, &owner, &published, &expires, &updated)
	charged := q.budget.materialized(q.callback, returned)
	if charged == nil && err == sql.ErrNoRows {
		return domain.AttemptSpendingAuthorizationCurrentV1{}, false, nil
	}
	if charged != nil || err != nil || len(raw) > ownedAuthorityHeadBytes || len(state) > 16 || len(digest) != 64 || len(run) > 160 || len(session) > 160 || len(owner) > 160 {
		return denied()
	}
	v, err := domain.DecodeAttemptSpendingAuthorizationCurrentV1([]byte(raw))
	actualDigest, digestErr := v.Digest()
	if err != nil || digestErr != nil || v.Locator != l ||
		uint64(v.Version) != version || v.RevocationGeneration != generation || string(v.State) != state || actualDigest != digest || string(v.RunID) != run || string(v.SessionID) != session || string(v.OwnerUserID) != owner ||
		!v.Window.PublishedAt.Equal(published) || !v.Window.ExpiresAt.Equal(expires) || !v.UpdatedAt.Equal(updated) {
		return denied()
	}
	return v, true, nil
}

// Never let database/sql stop assigning subsequent columns at the first invalid
// scalar, or implicitly coerce a float/string into an authority generation.
// The collector itself returns nil so all columns are accounted; the enclosing
// scan then denies invalid shapes without exposing a partially usable record.
type ownedCurrentScalar struct {
	destination any
	bytes       uint64
	valid       bool
}

func (s *ownedCurrentScalar) Scan(value any) error {
	switch v := value.(type) {
	case string:
		s.bytes = uint64(len(v))
	case []byte:
		s.bytes = uint64(len(v))
	case nil:
		s.bytes = 0
	default:
		s.bytes = 8
	}
	switch destination := s.destination.(type) {
	case *string:
		switch v := value.(type) {
		case string:
			*destination = v
			s.valid = utf8.ValidString(v)
		case []byte:
			*destination = string(v)
			s.valid = utf8.Valid(v)
		}
	case *uint64:
		switch v := value.(type) {
		case uint64:
			*destination = v
			s.valid = true
		case int64:
			if v >= 0 {
				*destination = uint64(v)
				s.valid = true
			}
		}
	case *uint32:
		switch v := value.(type) {
		case uint64:
			if v <= uint64(^uint32(0)) {
				*destination, s.valid = uint32(v), true
			}
		case int64:
			if v >= 0 && uint64(v) <= uint64(^uint32(0)) {
				*destination, s.valid = uint32(v), true
			}
		}
	case *time.Time:
		if v, ok := value.(time.Time); ok {
			*destination = v
			s.valid = !v.IsZero() && v.Year() >= 1 && v.Year() <= 9999 && v.Nanosecond()%1000 == 0
		}
	}
	return nil
}

func scanOwnedCurrentRow(row interface{ Scan(...any) error }, destinations ...any) (uint64, error) {
	scalars := make([]ownedCurrentScalar, len(destinations))
	arguments := make([]any, len(destinations))
	for i, destination := range destinations {
		scalars[i].destination = destination
		arguments[i] = &scalars[i]
	}
	scanErr := row.Scan(arguments...)
	if scanErr == sql.ErrNoRows {
		return 0, sql.ErrNoRows
	}
	var returned uint64
	for _, scalar := range scalars {
		if scalar.bytes > ownedAuthorityMaxBytes-returned {
			return ownedAuthorityMaxBytes + 1, domain.ErrMembershipDenied
		}
		returned += scalar.bytes
		if !scalar.valid {
			scanErr = domain.ErrMembershipDenied
		}
	}
	return returned, scanErr
}
