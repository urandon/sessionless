package ydbstore

import (
	"context"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Resolve only inside the caller's original explanation transaction/budget.
// No BFF preflight, nested auth transaction, activity refresh or public V2
// installer. The optional existing pure-port expectation never grants access.
func (store *Store) readExplanationAuthorization(ctx context.Context, query rowQuery, digest domain.SecretDigest, expected *ports.RunExplanationAuthorizationV1) (domain.WebSession, domain.TenantMembership, time.Time, error) {
	var emptySession domain.WebSession
	var emptyMember domain.TenantMembership
	var at time.Time
	deny := func(err error) (domain.WebSession, domain.TenantMembership, time.Time, error) {
		return emptySession, emptyMember, time.Time{}, err
	}
	var candidate domain.WebSession
	var snapshot *domain.WebSessionV2
	var err error
	if store.webIdentityV2 == nil {
		candidate, at, err = readExplanationSession(ctx, query, digest)
	} else {
		bindings, bindingErr := store.webIdentityV2.bindings()
		if bindingErr != nil {
			return deny(domain.ErrWebSessionRevoked)
		}
		if err := query.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
			return deny(err)
		}
		at = at.UTC()
		value, found, readErr := readWebSessionV2(ctx, query, digest)
		if readErr != nil {
			return deny(readErr)
		}
		if !found || value.Session.RevokedAt != nil {
			return deny(domain.ErrWebSessionRevoked)
		}
		if _, enabled := bindings[value.Session.AuthenticatedSubject.Provider]; !enabled {
			return deny(domain.ErrWebSessionRevoked)
		}
		snapshot = &value
		candidate = value.Session
		if !at.Before(candidate.IdleExpiresAt) || !at.Before(candidate.AbsoluteExpiresAt) {
			err = domain.ErrWebSessionExpired
		}
	}
	// Preserve the pure port's original expected-identity check before expiry
	// and membership. No speculative candidate is returned on any error.
	if expected != nil && (err == nil || errors.Is(err, domain.ErrWebSessionExpired)) && (candidate.UserID != expected.UserID || candidate.ActiveTenantID != expected.TenantID) {
		return deny(domain.ErrWebSessionRevoked)
	}
	if err != nil {
		return deny(err)
	}
	member, err := readExplanationMembership(ctx, query, candidate, at)
	if err != nil {
		return deny(err)
	}
	if snapshot != nil {
		// Reuse strict V2 codecs and this adapter's original bounded membership
		// read, not the generic membership reader's uncapped SQL. One additional
		// identity point read is charged to the same aggregate 20 statements.
		identity, found, err := readExternalIdentityV2(ctx, query, candidate.AuthenticatedSubject)
		if err != nil {
			return deny(err)
		}
		if !found {
			return deny(domain.ErrWebSessionRevoked)
		}
		if err := snapshot.AuthorizeCurrent(identity, member, domain.TenantPermissionRead, at); err != nil {
			return deny(err)
		}
	}
	if expected != nil {
		if err := authorizeExplanation(candidate, member, *expected, at); err != nil {
			return deny(err)
		}
	}
	return candidate, member, at, nil
}
