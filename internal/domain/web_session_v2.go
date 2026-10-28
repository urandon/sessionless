package domain

import "time"

// WebSessionV2 is an explicit V2 record, not an alternate decoder for WebSession.
// Session is the unchanged membership/session carrier. Adapters may derive a
// transient V1-shaped view ONLY after decoding V2 and checking current identity.
type WebSessionV2 struct {
	Version          uint32         `json:"version"`
	Session          WebSession     `json:"session"`
	IdentityRevision AuthRevisionV2 `json:"identity_revision"`
}

func (session WebSessionV2) Validate() error {
	if session.Version != 2 {
		return ValidationError{Field: "web_session_v2.version", Reason: "must be 2"}
	}
	if err := session.Session.Validate(); err != nil {
		return err
	}
	if err := validateIdentitySubjectV2(session.Session.AuthenticatedSubject); err != nil {
		return err
	}
	return session.IdentityRevision.Validate()
}

// AuthorizeCurrent must run with a canonical identity point-read and membership
// from the owning transaction. A cached identity or BFF preflight is no grant
// for a later resource transaction. No identity failure yields a usable carrier.
func (session WebSessionV2) AuthorizeCurrent(
	identity ExternalIdentityV2,
	membership TenantMembership,
	permission TenantPermission,
	now time.Time,
) error {
	if err := session.Validate(); err != nil {
		return err
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	if identity.State != ExternalIdentityActiveV2 || identity.UserID != session.Session.UserID ||
		identity.Subject != session.Session.AuthenticatedSubject || identity.Revision != session.IdentityRevision {
		return ErrWebSessionRevoked
	}
	if now.IsZero() || session.Session.IssuedAt.After(now) || session.Session.LastSeenAt.After(now) ||
		identity.CreatedAt.After(now) || identity.UpdatedAt.After(now) || membership.CreatedAt.After(now) || membership.UpdatedAt.After(now) {
		return ErrWebSessionRevoked
	}
	return session.Session.Authorize(membership, permission, now)
}
