package ydbstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Private lifecycle components only: public installation waits for the complete
// V2 writer/consumer cutover. A V1-shaped carrier is returned only after the
// strict V2 decode and transaction-local canonical identity check.
func (store *Store) sessionServingBindingsV2() (map[domain.IdentityProvider]domain.IdentityProviderBindingV2, error) {
	if store == nil || store.webIdentityV2 == nil || store.webSessionIdleTTL <= 0 || store.operationalRetention <= 0 {
		return nil, domain.ErrWebSessionRevoked
	}
	return store.webIdentityV2.bindings()
}

func (store *Store) createWebSessionV2(ctx context.Context, session domain.WebSession) error {
	bindings, err := store.sessionServingBindingsV2()
	if err != nil || session.Validate() != nil || session.RevokedAt != nil {
		return domain.ErrWebSessionRevoked
	}
	if _, enabled := bindings[session.AuthenticatedSubject.Provider]; !enabled {
		return domain.ErrWebSessionRevoked
	}
	return store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		active, err := readActiveExternalIdentitiesV2(ctx, q, session.UserID)
		if err != nil {
			return err
		}
		var identity domain.ExternalIdentityV2
		for _, candidate := range active {
			if candidate.CreatedAt.After(q.at) || candidate.UpdatedAt.After(q.at) {
				return domain.ErrWebSessionRevoked
			}
			if candidate.Subject == session.AuthenticatedSubject {
				identity = candidate
			}
		}
		membership, found, err := readMembershipTx(ctx, q, session.UserID, session.ActiveTenantID)
		if err != nil || !found {
			return domain.ErrMembershipDenied
		}
		next := domain.WebSessionV2{Version: 2, Session: session, IdentityRevision: identity.Revision}
		// Service-prepared timestamps may precede the database observation. They
		// remain immutable; neither creation nor retries refresh their lifetimes.
		if next.AuthorizeCurrent(identity, membership, domain.TenantPermissionRead, q.at) != nil {
			return domain.ErrWebSessionRevoked
		}
		existing, found, err := readWebSessionV2(ctx, q, session.SessionDigest)
		if err != nil {
			return err
		}
		if found {
			if reflect.DeepEqual(existing, next) {
				return nil
			}
			return domain.ErrWebSessionRotation
		}
		if err := writeWebSessionV2(ctx, q, next); err != nil {
			return err
		}
		return store.appendSessionServingAuditV2(ctx, q, next, "web.login.succeeded", "user", string(session.UserID))
	})
}

func (store *Store) authorizeWebSessionServingV2(ctx context.Context, digest domain.SecretDigest, permission domain.TenantPermission, _ time.Time) (ports.WebAuthorization, error) {
	var empty, result ports.WebAuthorization
	bindings, err := store.sessionServingBindingsV2()
	if err != nil || digest.Validate("web_session_v2.session_digest") != nil {
		return empty, domain.ErrWebSessionRevoked
	}
	err = store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = empty
		session, membership, err := authorizeWebSessionV2(ctx, q, digest, permission, q.at)
		if err != nil {
			return err
		}
		if _, enabled := bindings[session.Session.AuthenticatedSubject.Provider]; !enabled {
			return domain.ErrWebSessionRevoked
		}
		if q.at.After(session.Session.LastSeenAt) {
			session.Session.LastSeenAt = q.at
			session.Session.IdleExpiresAt = q.at.Add(store.webSessionIdleTTL)
			if session.Session.IdleExpiresAt.After(session.Session.AbsoluteExpiresAt) {
				session.Session.IdleExpiresAt = session.Session.AbsoluteExpiresAt
			}
			if err := writeWebSessionV2(ctx, q, session); err != nil {
				return err
			}
		}
		result = ports.WebAuthorization{Session: session.Session, Membership: membership}
		return nil
	})
	if err != nil {
		return empty, err
	}
	return result, nil
}

func (store *Store) switchTenantV2(ctx context.Context, current domain.SecretDigest, next domain.WebSession, tenant domain.TenantID, preparedAt time.Time) (ports.WebAuthorization, error) {
	var empty, result ports.WebAuthorization
	bindings, err := store.sessionServingBindingsV2()
	if err != nil || current.Validate("web_session_v2.current_digest") != nil || next.Validate() != nil || next.ActiveTenantID != tenant || !next.IssuedAt.Equal(preparedAt) {
		return empty, domain.ErrWebSessionRotation
	}
	err = store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = empty
		previous, _, err := authorizeWebSessionV2(ctx, q, current, domain.TenantPermissionRead, q.at)
		if err != nil {
			return err
		}
		if _, enabled := bindings[previous.Session.AuthenticatedSubject.Provider]; !enabled {
			return domain.ErrWebSessionRevoked
		}
		selected, found, err := readMembershipTx(ctx, q, previous.Session.UserID, tenant)
		if err != nil || !found {
			return domain.ErrMembershipDenied
		}
		// preparedAt is only the immutable rotation timestamp, never the
		// authorization clock. Both old and next validity use this query's time.
		if next.IssuedAt.Before(previous.Session.IssuedAt) || next.IssuedAt.After(q.at) || next.LastSeenAt.After(q.at) || selected.CreatedAt.After(q.at) || selected.UpdatedAt.After(q.at) || domain.ValidateWebSessionRotation(previous.Session, next, selected, preparedAt) != nil || next.Authorize(selected, domain.TenantPermissionRead, q.at) != nil {
			return domain.ErrWebSessionRotation
		}
		if _, found, err := readWebSessionV2(ctx, q, next.SessionDigest); err != nil || found {
			return domain.ErrWebSessionRotation
		}
		if err := previous.Session.Revoke(q.at); err != nil {
			return err
		}
		rotated := domain.WebSessionV2{Version: 2, Session: next, IdentityRevision: previous.IdentityRevision}
		if err := writeWebSessionV2(ctx, q, previous); err != nil {
			return err
		}
		if err := writeWebSessionV2(ctx, q, rotated); err != nil {
			return err
		}
		if err := store.appendSessionServingAuditV2(ctx, q, rotated, "web.tenant.switched", "tenant", string(tenant)); err != nil {
			return err
		}
		result = ports.WebAuthorization{Session: next, Membership: selected}
		return nil
	})
	if err != nil {
		return empty, err
	}
	return result, nil
}

func (store *Store) revokeWebSessionV2(ctx context.Context, digest domain.SecretDigest, _ time.Time) error {
	bindings, err := store.sessionServingBindingsV2()
	if err != nil || digest.Validate("web_session_v2.session_digest") != nil {
		return domain.ErrWebSessionRevoked
	}
	return store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		session, found, err := readWebSessionV2(ctx, q, digest)
		if err != nil || !found {
			return err
		}
		identity, found, err := readExternalIdentityV2(ctx, q, session.Session.AuthenticatedSubject)
		if err != nil || !found || identity.State != domain.ExternalIdentityActiveV2 || identity.UserID != session.Session.UserID || identity.Revision != session.IdentityRevision || identity.CreatedAt.After(q.at) || identity.UpdatedAt.After(q.at) || session.Session.IssuedAt.After(q.at) || session.Session.LastSeenAt.After(q.at) || (session.Session.RevokedAt != nil && session.Session.RevokedAt.After(q.at)) {
			return domain.ErrWebSessionRevoked
		}
		if _, enabled := bindings[identity.Subject.Provider]; !enabled {
			return domain.ErrWebSessionRevoked
		}
		// Revocation grants nothing: expired sessions or memberships need not
		// prevent logout, but a stale/unlinked canonical identity cannot write.
		if session.Session.RevokedAt != nil {
			return nil
		}
		if err := session.Session.Revoke(q.at); err != nil {
			return err
		}
		if err := writeWebSessionV2(ctx, q, session); err != nil {
			return err
		}
		return store.appendSessionServingAuditV2(ctx, q, session, "web.logout", "user", string(session.Session.UserID))
	})
}

func (store *Store) appendSessionServingAuditV2(ctx context.Context, q *ownedAuthorityQuery, session domain.WebSessionV2, action, subjectKind, subjectID string) error {
	metadata, err := json.Marshal(map[string]any{"provider": session.Session.AuthenticatedSubject.Provider, "membership_security_version": session.Session.MembershipSecurityVersion, "identity_revision": session.IdentityRevision})
	if err != nil {
		return err
	}
	// Correlate by a one-way action+session key rather than a collision-prone
	// timestamp. Neither cookie digest nor raw external subject enters the audit.
	sum := sha256.Sum256([]byte(action + ":" + string(session.Session.SessionDigest)))
	auditID := "audit-web-v2-" + hex.EncodeToString(sum[:])
	_, err = q.ExecContext(ctx, `UPSERT INTO audit_events
 (tenant_id,occurred_at,audit_event_id,actor_id,action,subject_kind,subject_id,outcome,metadata,expire_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS JsonDocument),$10)`,
		session.Session.ActiveTenantID, q.at, auditID, session.Session.UserID, action, subjectKind, subjectID, "succeeded", string(metadata), q.at.Add(store.operationalRetention))
	return err
}
