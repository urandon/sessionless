package ydbstore

import (
	"context"
	"database/sql"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Trusted mode is captured by the owning Store entry, never selected by cookie
// JSON, request parameters or a failed V2 parse. This private snapshot neither
// installs a mode nor authorizes a later transaction.
type ownedConsentIdentityMode struct{ config *identityConfigurationV2 }

type ownedConsentBrowser struct {
	session       domain.WebSession
	v2            *domain.WebSessionV2
	identity      domain.ExternalIdentityV2
	identityFound bool
	enabled       bool
}

// Structural bounded reads only; callers must authorize against membership in
// this SAME callback before disclosure/absence/decision or receipt consumption.
// Negative reconciliation separately interprets actual canonical stop facts.
func (mode ownedConsentIdentityMode) readSession(ctx context.Context, q *ownedAuthorityQuery, digest domain.SecretDigest) (ownedConsentBrowser, bool, error) {
	if digest.Validate("web_search_consent.web_session_digest") != nil {
		return ownedConsentBrowser{}, false, q.budget.deny()
	}
	if mode.config != nil {
		bindings, err := mode.config.bindings()
		if err != nil {
			return ownedConsentBrowser{}, false, q.budget.deny()
		}
		snapshot, found, err := readWebSessionV2(ctx, q, digest)
		if err != nil {
			return ownedConsentBrowser{}, false, q.budget.deny()
		}
		if !found {
			return ownedConsentBrowser{}, false, nil
		}
		identity, present, err := readExternalIdentityV2(ctx, q, snapshot.Session.AuthenticatedSubject)
		if err != nil {
			return ownedConsentBrowser{}, false, q.budget.deny()
		}
		_, enabled := bindings[snapshot.Session.AuthenticatedSubject.Provider]
		return ownedConsentBrowser{session: snapshot.Session, v2: &snapshot, identity: identity, identityFound: present, enabled: enabled}, true, nil
	}
	bucket, err := webBucket(string(digest))
	if err != nil {
		return ownedConsentBrowser{}, false, q.budget.deny()
	}
	raw, err := q.readMetadata(ctx, `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, ownedAuthorityHeadBytes, bucket, digest)
	if err == sql.ErrNoRows {
		return ownedConsentBrowser{}, false, nil
	}
	var session domain.WebSession
	if err != nil || decodeOwnedEffectCanonicalJSON(raw, &session) != nil || session.Validate() != nil || session.SessionDigest != digest {
		return ownedConsentBrowser{}, false, q.budget.deny()
	}
	return ownedConsentBrowser{session: session}, true, nil
}

func (browser ownedConsentBrowser) authorizeWrite(membership domain.TenantMembership, at time.Time) error {
	if browser.v2 != nil {
		if !browser.identityFound || !browser.enabled {
			return domain.ErrWebSessionRevoked
		}
		return browser.v2.AuthorizeCurrent(browser.identity, membership, domain.TenantPermissionWrite, at)
	}
	return browser.session.Authorize(membership, domain.TenantPermissionWrite, at)
}

// ONLY the negative owner consumes this observation. Do not catch a failed
// positive loader or fabricate a revoked cookie: the actual stored session is
// retained unchanged. Missing/malformed/future are deliberately distinguished.
func (browser ownedConsentBrowser) observedIdentityRevoked(at time.Time) (bool, error) {
	if browser.v2 == nil {
		return false, nil
	}
	if !browser.identityFound {
		return true, nil
	}
	identity := browser.identity
	if identity.Validate() != nil || identity.Subject != browser.session.AuthenticatedSubject || identity.UserID != browser.session.UserID || identity.CreatedAt.After(at) || identity.UpdatedAt.After(at) {
		return false, domain.ErrMembershipDenied
	}
	return !browser.enabled || identity.State != domain.ExternalIdentityActiveV2 || identity.Revision != browser.v2.IdentityRevision, nil
}
