package ydbstore

import (
	"context"
	"database/sql"
	"errors"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const identityCandidateReverseSQLV2 = `SELECT SUBSTRING(CAST(provider AS String),0,161) FROM external_identities_by_user WHERE user_bucket=$1 AND user_id=$2 ORDER BY provider, subject LIMIT 1`
const identityCandidateMembershipSQLV2 = `SELECT SUBSTRING(CAST(tenant_id AS String),0,161) FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 ORDER BY tenant_id LIMIT 1`

func (store *Store) resolveExternalIdentityServingV2(ctx context.Context, subject domain.ExternalSubject, candidate domain.UserID) (result domain.ExternalIdentity, created bool, err error) {
	if store == nil || store.webIdentityV2 == nil {
		return result, false, domain.ErrMembershipDenied
	}
	err = store.identityAuthorityTxV2(ctx, func(ctx context.Context, query *ownedAuthorityQuery) error {
		result, created = domain.ExternalIdentity{}, false
		identity, fresh, err := resolveOrCreateExternalIdentityV2(ctx, query, *store.webIdentityV2, subject, candidate)
		if err != nil {
			return err
		}
		// The old port carries identity display/selection data, not a durable V1
		// record or authorization receipt. Persist only the closed V2 form.
		result = domain.ExternalIdentity{Subject: identity.Subject, UserID: identity.UserID, CreatedAt: identity.CreatedAt, UpdatedAt: identity.UpdatedAt}
		created = fresh
		return nil
	})
	if err != nil {
		return domain.ExternalIdentity{}, false, err
	}
	return result, created, nil
}

// Compare instants, not time.Location pointers or caller monotonic clock data.
func sameIdentityMappingV2(left, right domain.ExternalIdentityV2) bool {
	return left.Version == right.Version && left.Subject == right.Subject && left.UserID == right.UserID &&
		left.State == right.State && left.Revision == right.Revision &&
		left.CreatedAt.Equal(right.CreatedAt) && left.UpdatedAt.Equal(right.UpdatedAt)
}

// The ordinary resolver is deliberately different from explicit linking. An
// existing active subject keeps its canonical owner; a tombstone never creates
// a new account. A new subject may initialize only a fresh server-selected user,
// not occupy a vacant provider slot on an existing account. The caller owns the
// original serializable transaction and its database clock/budget. This helper
// installs neither a serving format nor an operator/bootstrap membership grant.
// candidate must come from the trusted fresh-ID generator, never the browser.
// A completed cutover inventory must establish canonical/reverse correspondence;
// prefix absence does not prove nonexistence under arbitrary orphan corruption.
func resolveOrCreateExternalIdentityV2(ctx context.Context, query *ownedAuthorityQuery,
	config identityConfigurationV2, subject domain.ExternalSubject, candidate domain.UserID,
) (domain.ExternalIdentityV2, bool, error) {
	deny := func() (domain.ExternalIdentityV2, bool, error) {
		return domain.ExternalIdentityV2{}, false, domain.ErrMembershipDenied
	}
	bindings, err := config.bindings()
	if err != nil || query == nil || query.at.IsZero() || subject.Validate() != nil || candidate.Validate() != nil {
		return deny()
	}
	if _, enabled := bindings[subject.Provider]; !enabled {
		return domain.ExternalIdentityV2{}, false, domain.ErrIdentityProviderUnavailableV2
	}
	identity, found, err := readExternalIdentityV2(ctx, query, subject)
	if err != nil {
		return domain.ExternalIdentityV2{}, false, err
	}
	if found {
		if identity.State != domain.ExternalIdentityActiveV2 || identity.UpdatedAt.After(query.at) {
			return deny()
		}
		active, err := readActiveExternalIdentitiesV2(ctx, query, identity.UserID)
		if err != nil {
			return domain.ExternalIdentityV2{}, false, err
		}
		included := false
		for _, entry := range active {
			if entry.UpdatedAt.After(query.at) {
				return deny()
			}
			if entry.Subject == subject {
				included = sameIdentityMappingV2(entry, identity)
			}
		}
		if !included {
			return deny()
		}
		return identity, false, nil
	}

	bucket, err := webBucket(string(candidate))
	if err != nil {
		return deny()
	}
	// Exact bounded prefix reads establish absence, not a truncated active set.
	// Even a suspended membership or a malformed reverse entry reserves the
	// candidate. Inconsistency is maintenance work, never a login repair grant.
	for _, statement := range []string{identityCandidateReverseSQLV2, identityCandidateMembershipSQLV2} {
		_, err := query.readMetadata(ctx, statement, 161, bucket, candidate)
		if !errors.Is(err, sql.ErrNoRows) {
			if err != nil {
				return domain.ExternalIdentityV2{}, false, err
			}
			return domain.ExternalIdentityV2{}, false, domain.ErrExternalIdentityConflict
		}
	}
	identity = domain.ExternalIdentityV2{Version: 2, Subject: subject, UserID: candidate,
		CreatedAt: query.at, UpdatedAt: query.at, State: domain.ExternalIdentityActiveV2, Revision: 1}
	if err := writeExternalIdentityV2(ctx, query, identity); err != nil {
		return domain.ExternalIdentityV2{}, false, err
	}
	return identity, true, nil
}
