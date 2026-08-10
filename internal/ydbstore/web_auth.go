package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

const maxWebMemberships = uint64(200)

func (store *Store) RecordWebSecurityEvent(ctx context.Context, event domain.WebSecurityAuditEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	bucket, err := webBucket(event.RequestID)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return store.authTx(ctx, "web_auth.record_security_event", func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`UPSERT INTO web_security_audit_events
			 (shard_bucket, occurred_at, request_id, action, provider,
			  subject_fingerprint, tenant_id, user_id,
			  membership_security_version, reason_code, record, expire_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			         CAST($11 AS JsonDocument), $12)`,
			bucket, event.OccurredAt, event.RequestID, event.Action, event.Provider,
			event.SubjectFingerprint, event.TenantID, event.UserID,
			event.MembershipSecurityVersion, event.ReasonCode, string(payload),
			event.OccurredAt.Add(store.operationalRetention),
		)
		return err
	})
}

func (store *Store) CreateLoginChallenge(ctx context.Context, challenge domain.OIDCLoginChallenge) error {
	if err := challenge.Validate(); err != nil {
		return err
	}
	bucket, err := webBucket(string(challenge.StateDigest))
	if err != nil {
		return err
	}
	return store.authTx(ctx, "web_auth.create_login_challenge", func(tx *sql.Tx) error {
		existing, found, err := readJSON[domain.OIDCLoginChallenge](ctx, tx,
			`SELECT record FROM oidc_login_challenges
			 WHERE shard_bucket = $1 AND state_digest = $2`,
			bucket, challenge.StateDigest,
		)
		if err != nil {
			return err
		}
		if found {
			if reflect.DeepEqual(existing, challenge) {
				return nil
			}
			return domain.ValidationError{Field: "oidc_challenge.state_digest", Reason: "already exists"}
		}
		return writeChallenge(ctx, tx, bucket, challenge)
	})
}

func (store *Store) ConsumeLoginChallenge(
	ctx context.Context,
	stateDigest domain.SecretDigest,
	browserBindingSecret string,
	at time.Time,
) (result domain.OIDCLoginChallenge, err error) {
	if err := stateDigest.Validate("oidc_challenge.state_digest"); err != nil {
		return result, err
	}
	bucket, err := webBucket(string(stateDigest))
	if err != nil {
		return result, err
	}
	err = store.authTx(ctx, "web_auth.consume_login_challenge", func(tx *sql.Tx) error {
		challenge, found, err := readJSON[domain.OIDCLoginChallenge](ctx, tx,
			`SELECT record FROM oidc_login_challenges
			 WHERE shard_bucket = $1 AND state_digest = $2`,
			bucket, stateDigest,
		)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrLoginChallengeExpired
		}
		if err := challenge.Consume(browserBindingSecret, at); err != nil {
			return err
		}
		if err := writeChallenge(ctx, tx, bucket, challenge); err != nil {
			return err
		}
		result = challenge
		return nil
	})
	return result, err
}

func (store *Store) ResolveOrCreateExternalIdentity(
	ctx context.Context,
	subject domain.ExternalSubject,
	candidateUserID domain.UserID,
	at time.Time,
) (identity domain.ExternalIdentity, created bool, err error) {
	if err := subject.Validate(); err != nil {
		return identity, false, err
	}
	if err := candidateUserID.Validate(); err != nil {
		return identity, false, err
	}
	if at.IsZero() {
		return identity, false, domain.ValidationError{Field: "external_identity.at", Reason: "must not be zero"}
	}
	err = store.authTx(ctx, "web_auth.resolve_external_identity", func(tx *sql.Tx) error {
		var txErr error
		identity, created, txErr = resolveOrCreateExternalIdentityTx(ctx, tx, subject, candidateUserID, at)
		return txErr
	})
	return identity, created, err
}

func (store *Store) ListTenantMemberships(
	ctx context.Context,
	userID domain.UserID,
	limit uint64,
) ([]domain.TenantMembership, error) {
	if err := userID.Validate(); err != nil {
		return nil, err
	}
	if limit == 0 || limit > maxWebMemberships {
		return nil, domain.ValidationError{Field: "tenant_memberships.limit", Reason: "must be between 1 and 200"}
	}
	bucket, err := webBucket(string(userID))
	if err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT record FROM tenant_memberships
		 WHERE user_bucket = $1 AND user_id = $2
		 ORDER BY tenant_id LIMIT $3`,
		bucket, userID, limit,
	)
	if err != nil {
		return nil, classifyYDB("web_auth.list_memberships", err)
	}
	defer rows.Close()
	result := make([]domain.TenantMembership, 0)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, classifyYDB("web_auth.list_memberships", err)
		}
		membership, err := decodeMembership(payload)
		if err != nil {
			return nil, err
		}
		result = append(result, membership)
	}
	if err := rows.Err(); err != nil {
		return nil, classifyYDB("web_auth.list_memberships", err)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].TenantID < result[right].TenantID })
	return result, nil
}

func (store *Store) Enroll(ctx context.Context, request ports.EnrollmentRequest) (result domain.TenantMembership, err error) {
	if err := validateEnrollmentRequest(request); err != nil {
		return result, err
	}
	err = store.authTx(ctx, "web_auth.enroll", func(tx *sql.Tx) error {
		mappedIdentity, found, err := readExternalIdentityTx(ctx, tx, request.Identity.Subject)
		if err != nil {
			return err
		}
		if !found || mappedIdentity.UserID != request.Identity.UserID {
			return domain.ErrExternalIdentityConflict
		}
		existing, found, err := readMembershipTx(ctx, tx, request.Identity.UserID, request.TenantID)
		if err != nil {
			return err
		}
		if found {
			if existing.Status != domain.TenantMembershipActive {
				return domain.ErrMembershipDenied
			}
			result = existing
			return nil
		}

		role := domain.TenantMembershipMember
		switch request.Source {
		case domain.EnrollmentExistingFrontend:
			if request.FrontendBindingID == nil {
				return domain.ErrEnrollmentGrantRequired
			}
			var sessionID string
			if err := tx.QueryRowContext(ctx,
				`SELECT session_id FROM frontend_bindings
				 WHERE tenant_id = $1 AND binding_id = $2`,
				request.TenantID, *request.FrontendBindingID,
			).Scan(&sessionID); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return domain.ErrEnrollmentGrantRequired
				}
				return err
			}
			var participantStatus string
			if err := tx.QueryRowContext(ctx,
				`SELECT status FROM session_participants
				 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3`,
				request.TenantID, sessionID, request.Identity.UserID,
			).Scan(&participantStatus); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return domain.ErrEnrollmentGrantRequired
				}
				return err
			}
			if participantStatus != string(domain.SessionParticipantActive) {
				return domain.ErrEnrollmentGrantRequired
			}
		case domain.EnrollmentTenantInvitation:
			if request.InvitationID == nil || request.InvitationDigest == nil {
				return domain.ErrEnrollmentGrantRequired
			}
			invitation, found, err := readJSON[domain.TenantInvitation](ctx, tx,
				`SELECT record FROM tenant_invitations
				 WHERE tenant_id = $1 AND invitation_id = $2`,
				request.TenantID, *request.InvitationID,
			)
			if err != nil {
				return err
			}
			if !found || invitation.SecretDigest != *request.InvitationDigest {
				return domain.ErrEnrollmentGrantRequired
			}
			if err := invitation.Consume(request.Identity.Subject, request.Identity.UserID, request.At); err != nil {
				return err
			}
			payload, err := marshal(invitation)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx,
				`UPDATE tenant_invitations
				 SET consumed_at = $1, record = CAST($2 AS JsonDocument)
				 WHERE tenant_id = $3 AND invitation_id = $4`,
				*invitation.ConsumedAt, payload, invitation.TenantID, invitation.ID,
			); err != nil {
				return err
			}
			role = invitation.Role
		case domain.EnrollmentDevelopmentBootstrap:
			if request.Bootstrap == nil || request.Bootstrap.UserID != request.Identity.UserID || request.Bootstrap.TenantID != request.TenantID {
				return domain.ErrEnrollmentGrantRequired
			}
			if err := request.Bootstrap.Validate(); err != nil {
				return err
			}
			role = request.Bootstrap.Role
		default:
			return domain.ErrEnrollmentGrantRequired
		}

		result = domain.TenantMembership{
			TenantID: request.TenantID, UserID: request.Identity.UserID,
			Role: role, Status: domain.TenantMembershipActive, SecurityVersion: 1,
			CreatedAt: request.At, UpdatedAt: request.At,
		}
		if err := putMembershipTx(ctx, tx, result); err != nil {
			return err
		}
		return store.appendWebAudit(ctx, tx, result.TenantID, result.UserID, request.At,
			"web.membership.enrolled", "tenant_membership", string(result.TenantID),
			map[string]any{"source": request.Source.String(), "role": result.Role, "security_version": result.SecurityVersion})
	})
	return result, err
}

func (store *Store) BootstrapDevelopmentMembership(
	ctx context.Context,
	grant domain.DevelopmentBootstrapGrant,
) (result domain.TenantMembership, err error) {
	if err := grant.Validate(); err != nil {
		return result, err
	}
	err = store.authTx(ctx, "web_auth.bootstrap_development_membership", func(tx *sql.Tx) error {
		previousGrant, grantFound, err := readJSON[domain.DevelopmentBootstrapGrant](ctx, tx,
			`SELECT record FROM development_bootstrap_grants
			 WHERE tenant_id = $1 AND user_id = $2`,
			grant.TenantID, grant.UserID,
		)
		if err != nil {
			return err
		}
		if grantFound && (previousGrant.TenantID != grant.TenantID || previousGrant.UserID != grant.UserID ||
			previousGrant.Role != grant.Role || previousGrant.Environment != grant.Environment ||
			previousGrant.Operator != grant.Operator || previousGrant.Reason != grant.Reason) {
			return domain.ErrMembershipDenied
		}
		userBucket, err := webBucket(string(grant.UserID))
		if err != nil {
			return err
		}
		var provider string
		if err := tx.QueryRowContext(ctx,
			`SELECT provider FROM external_identities_by_user
			 WHERE user_bucket = $1 AND user_id = $2
			 ORDER BY provider, subject LIMIT 1`,
			userBucket, grant.UserID,
		).Scan(&provider); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return domain.ErrEnrollmentGrantRequired
			}
			return err
		}
		existing, found, err := readMembershipTx(ctx, tx, grant.UserID, grant.TenantID)
		if err != nil {
			return err
		}
		if found {
			if existing.Role != grant.Role || existing.Status != domain.TenantMembershipActive {
				return domain.ErrMembershipDenied
			}
			if grantFound {
				result = existing
				return nil
			}
			result = existing
		} else {
			result = domain.TenantMembership{
				TenantID: grant.TenantID, UserID: grant.UserID, Role: grant.Role,
				Status: domain.TenantMembershipActive, SecurityVersion: 1,
				CreatedAt: grant.GrantedAt, UpdatedAt: grant.GrantedAt,
			}
			if err := putMembershipTx(ctx, tx, result); err != nil {
				return err
			}
		}
		grantPayload, err := marshal(grant)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO development_bootstrap_grants
			 (tenant_id, user_id, role, operator, reason, granted_at, record)
			 VALUES ($1, $2, $3, $4, $5, $6, CAST($7 AS JsonDocument))`,
			grant.TenantID, grant.UserID, grant.Role, grant.Operator, grant.Reason,
			grant.GrantedAt, grantPayload,
		); err != nil {
			return err
		}
		return store.appendWebAudit(ctx, tx, result.TenantID, result.UserID, grant.GrantedAt,
			"web.membership.cloud_dev_bootstrap", "tenant_membership", string(result.TenantID),
			map[string]any{"environment": grant.Environment, "operator": grant.Operator,
				"reason": grant.Reason, "role": grant.Role, "security_version": result.SecurityVersion})
	})
	return result, err
}

func (store *Store) CreateWebSession(ctx context.Context, session domain.WebSession) error {
	if err := session.Validate(); err != nil {
		return err
	}
	bucket, err := webBucket(string(session.SessionDigest))
	if err != nil {
		return err
	}
	return store.authTx(ctx, "web_auth.create_session", func(tx *sql.Tx) error {
		membership, found, err := readMembershipTx(ctx, tx, session.UserID, session.ActiveTenantID)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrMembershipDenied
		}
		if err := session.Authorize(membership, domain.TenantPermissionRead, session.IssuedAt); err != nil {
			return err
		}
		existing, found, err := readWebSessionTx(ctx, tx, bucket, session.SessionDigest)
		if err != nil {
			return err
		}
		if found {
			if reflect.DeepEqual(existing, session) {
				return nil
			}
			return domain.ValidationError{Field: "web_session.session_digest", Reason: "already exists"}
		}
		if err := writeWebSession(ctx, tx, bucket, session); err != nil {
			return err
		}
		return store.appendWebAudit(ctx, tx, session.ActiveTenantID, session.UserID, session.IssuedAt,
			"web.login.succeeded", "user", string(session.UserID),
			map[string]any{"provider": session.AuthenticatedSubject.Provider,
				"membership_security_version": session.MembershipSecurityVersion})
	})
}

func (store *Store) AuthorizeWebSession(
	ctx context.Context,
	sessionDigest domain.SecretDigest,
	permission domain.TenantPermission,
	at time.Time,
) (result ports.WebAuthorization, err error) {
	if err := sessionDigest.Validate("web_session.session_digest"); err != nil {
		return result, err
	}
	bucket, err := webBucket(string(sessionDigest))
	if err != nil {
		return result, err
	}
	err = store.authTx(ctx, "web_auth.authorize_session", func(tx *sql.Tx) error {
		session, found, err := readWebSessionTx(ctx, tx, bucket, sessionDigest)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrWebSessionRevoked
		}
		membership, found, err := readMembershipTx(ctx, tx, session.UserID, session.ActiveTenantID)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrMembershipDenied
		}
		if err := session.Authorize(membership, permission, at); err != nil {
			return err
		}
		if at.After(session.LastSeenAt) {
			session.LastSeenAt = at
			session.IdleExpiresAt = at.Add(store.webSessionIdleTTL)
			if session.IdleExpiresAt.After(session.AbsoluteExpiresAt) {
				session.IdleExpiresAt = session.AbsoluteExpiresAt
			}
			if err := writeWebSession(ctx, tx, bucket, session); err != nil {
				return err
			}
		}
		result = ports.WebAuthorization{Session: session, Membership: membership}
		return nil
	})
	return result, err
}

func (store *Store) SwitchTenant(
	ctx context.Context,
	currentDigest domain.SecretDigest,
	next domain.WebSession,
	selectedTenantID domain.TenantID,
	at time.Time,
) (result ports.WebAuthorization, err error) {
	if err := currentDigest.Validate("web_session.current_digest"); err != nil {
		return result, err
	}
	if err := next.Validate(); err != nil {
		return result, err
	}
	if next.ActiveTenantID != selectedTenantID {
		return result, domain.ErrWebSessionRotation
	}
	currentBucket, err := webBucket(string(currentDigest))
	if err != nil {
		return result, err
	}
	nextBucket, err := webBucket(string(next.SessionDigest))
	if err != nil {
		return result, err
	}
	err = store.authTx(ctx, "web_auth.switch_tenant", func(tx *sql.Tx) error {
		previous, found, err := readWebSessionTx(ctx, tx, currentBucket, currentDigest)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrWebSessionRevoked
		}
		currentMembership, found, err := readMembershipTx(ctx, tx, previous.UserID, previous.ActiveTenantID)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrMembershipDenied
		}
		if err := previous.Authorize(currentMembership, domain.TenantPermissionRead, at); err != nil {
			return err
		}
		selected, found, err := readMembershipTx(ctx, tx, previous.UserID, selectedTenantID)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrMembershipDenied
		}
		if err := domain.ValidateWebSessionRotation(previous, next, selected, at); err != nil {
			return err
		}
		if _, found, err := readWebSessionTx(ctx, tx, nextBucket, next.SessionDigest); err != nil {
			return err
		} else if found {
			return domain.ErrWebSessionRotation
		}
		if err := previous.Revoke(at); err != nil {
			return err
		}
		if err := writeWebSession(ctx, tx, currentBucket, previous); err != nil {
			return err
		}
		if err := writeWebSession(ctx, tx, nextBucket, next); err != nil {
			return err
		}
		if err := store.appendWebAudit(ctx, tx, selectedTenantID, next.UserID, at,
			"web.tenant.switched", "tenant", string(selectedTenantID),
			map[string]any{"membership_security_version": next.MembershipSecurityVersion}); err != nil {
			return err
		}
		result = ports.WebAuthorization{Session: next, Membership: selected}
		return nil
	})
	return result, err
}

func (store *Store) RevokeWebSession(ctx context.Context, sessionDigest domain.SecretDigest, at time.Time) error {
	if err := sessionDigest.Validate("web_session.session_digest"); err != nil {
		return err
	}
	bucket, err := webBucket(string(sessionDigest))
	if err != nil {
		return err
	}
	return store.authTx(ctx, "web_auth.revoke_session", func(tx *sql.Tx) error {
		session, found, err := readWebSessionTx(ctx, tx, bucket, sessionDigest)
		if err != nil || !found {
			return err
		}
		if err := session.Revoke(at); err != nil {
			return err
		}
		if err := writeWebSession(ctx, tx, bucket, session); err != nil {
			return err
		}
		return store.appendWebAudit(ctx, tx, session.ActiveTenantID, session.UserID, at,
			"web.logout", "user", string(session.UserID),
			map[string]any{"membership_security_version": session.MembershipSecurityVersion})
	})
}

func resolveOrCreateExternalIdentityTx(
	ctx context.Context,
	tx *sql.Tx,
	subject domain.ExternalSubject,
	candidateUserID domain.UserID,
	at time.Time,
) (domain.ExternalIdentity, bool, error) {
	bucket, err := webBucket(subject.String())
	if err != nil {
		return domain.ExternalIdentity{}, false, err
	}
	existing, found, err := readExternalIdentityTx(ctx, tx, subject)
	if err != nil {
		return domain.ExternalIdentity{}, false, err
	}
	if found {
		// candidateUserID is used only when the subject has no mapping. Returning
		// the immutable existing row prevents callers from learning or guessing
		// the internal ID before this point without ever permitting a remap.
		return existing, false, nil
	}
	identity := domain.ExternalIdentity{Subject: subject, UserID: candidateUserID, CreatedAt: at, UpdatedAt: at}
	if err := identity.Validate(); err != nil {
		return domain.ExternalIdentity{}, false, err
	}
	payload, err := marshal(identity)
	if err != nil {
		return domain.ExternalIdentity{}, false, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO external_identities
		 (shard_bucket, provider, subject, user_id, created_at, updated_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, CAST($7 AS JsonDocument))`,
		bucket, subject.Provider, subject.Subject, identity.UserID,
		identity.CreatedAt, identity.UpdatedAt, payload,
	); err != nil {
		return domain.ExternalIdentity{}, false, err
	}
	userBucket, err := webBucket(string(identity.UserID))
	if err != nil {
		return domain.ExternalIdentity{}, false, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO external_identities_by_user
		 (user_bucket, user_id, provider, subject, created_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		userBucket, identity.UserID, subject.Provider, subject.Subject, identity.CreatedAt,
	); err != nil {
		return domain.ExternalIdentity{}, false, err
	}
	return identity, true, nil
}

func readExternalIdentityTx(
	ctx context.Context,
	tx *sql.Tx,
	subject domain.ExternalSubject,
) (domain.ExternalIdentity, bool, error) {
	bucket, err := webBucket(subject.String())
	if err != nil {
		return domain.ExternalIdentity{}, false, err
	}
	return readJSON[domain.ExternalIdentity](ctx, tx,
		`SELECT record FROM external_identities
		 WHERE shard_bucket = $1 AND provider = $2 AND subject = $3`,
		bucket, subject.Provider, subject.Subject,
	)
}

func putMembershipTx(ctx context.Context, tx *sql.Tx, membership domain.TenantMembership) error {
	if err := membership.Validate(); err != nil {
		return err
	}
	bucket, err := webBucket(string(membership.UserID))
	if err != nil {
		return err
	}
	payload, err := marshal(membership)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`UPSERT INTO tenant_memberships
		 (user_bucket, user_id, tenant_id, role, status, security_version,
		  created_at, updated_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))`,
		bucket, membership.UserID, membership.TenantID, membership.Role,
		membership.Status, membership.SecurityVersion, membership.CreatedAt,
		membership.UpdatedAt, payload,
	)
	return err
}

func readMembershipTx(
	ctx context.Context,
	tx *sql.Tx,
	userID domain.UserID,
	tenantID domain.TenantID,
) (domain.TenantMembership, bool, error) {
	bucket, err := webBucket(string(userID))
	if err != nil {
		return domain.TenantMembership{}, false, err
	}
	return readJSON[domain.TenantMembership](ctx, tx,
		`SELECT record FROM tenant_memberships
		 WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3`,
		bucket, userID, tenantID,
	)
}

func writeChallenge(ctx context.Context, tx *sql.Tx, bucket uint32, challenge domain.OIDCLoginChallenge) error {
	payload, err := marshal(challenge)
	if err != nil {
		return err
	}
	consumedAt := time.Unix(0, 0).UTC()
	if challenge.ConsumedAt != nil {
		consumedAt = *challenge.ConsumedAt
	}
	_, err = tx.ExecContext(ctx,
		`UPSERT INTO oidc_login_challenges
		 (shard_bucket, state_digest, browser_binding_digest, pkce_verifier,
		  nonce, redirect_path, created_at, expires_at, consumed_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CAST($10 AS JsonDocument))`,
		bucket, challenge.StateDigest, challenge.BrowserBindingDigest,
		challenge.PKCEVerifier, challenge.Nonce, challenge.RedirectPath,
		challenge.CreatedAt, challenge.ExpiresAt, consumedAt, payload,
	)
	return err
}

func readWebSessionTx(
	ctx context.Context,
	tx *sql.Tx,
	bucket uint32,
	digest domain.SecretDigest,
) (domain.WebSession, bool, error) {
	return readJSON[domain.WebSession](ctx, tx,
		`SELECT record FROM web_sessions
		 WHERE shard_bucket = $1 AND session_digest = $2`,
		bucket, digest,
	)
}

func writeWebSession(ctx context.Context, tx *sql.Tx, bucket uint32, session domain.WebSession) error {
	if err := session.Validate(); err != nil {
		return err
	}
	payload, err := marshal(session)
	if err != nil {
		return err
	}
	revokedAt := time.Unix(0, 0).UTC()
	if session.RevokedAt != nil {
		revokedAt = *session.RevokedAt
	}
	_, err = tx.ExecContext(ctx,
		`UPSERT INTO web_sessions
		 (shard_bucket, session_digest, csrf_token_digest, user_id,
		  active_tenant_id, identity_provider, external_subject,
		  membership_security_version, issued_at, last_seen_at,
		  idle_expires_at, absolute_expires_at, revoked_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
		         CAST($14 AS JsonDocument))`,
		bucket, session.SessionDigest, session.CSRFTokenDigest, session.UserID,
		session.ActiveTenantID, session.AuthenticatedSubject.Provider,
		session.AuthenticatedSubject.Subject, session.MembershipSecurityVersion,
		session.IssuedAt, session.LastSeenAt, session.IdleExpiresAt,
		session.AbsoluteExpiresAt, revokedAt, payload,
	)
	return err
}

func (store *Store) authTx(ctx context.Context, operation string, fn func(*sql.Tx) error) error {
	err := retry.DoTx(ctx, store.db, func(ctx context.Context, tx *sql.Tx) error {
		if err := fn(tx); err != nil {
			return callbackError{err: err}
		}
		return nil
	}, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{Isolation: sql.LevelSerializable}))
	var callback callbackError
	if errors.As(err, &callback) {
		return callback.err
	}
	return classifyYDB(operation, err)
}

func classifyYDB(operation string, err error) error {
	if err == nil {
		return nil
	}
	kind := domain.ErrorTerminal
	if retry.Check(err).MustRetry(true) {
		kind = domain.ErrorRetryable
	}
	return &domain.ClassifiedError{Kind: kind, Code: "ydb_web_auth_failed", Operation: operation, Cause: err}
}

func webBucket(value string) (uint32, error) { return ydbpartition.BucketV1(value) }

func decodeMembership(payload string) (domain.TenantMembership, error) {
	var membership domain.TenantMembership
	if err := json.Unmarshal([]byte(payload), &membership); err != nil {
		return membership, fmt.Errorf("decode stored JSON: %w", err)
	}
	return membership, membership.Validate()
}

func validateEnrollmentRequest(request ports.EnrollmentRequest) error {
	if err := request.Identity.Validate(); err != nil {
		return err
	}
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if request.At.IsZero() {
		return domain.ValidationError{Field: "enrollment.at", Reason: "must not be zero"}
	}
	return nil
}

func (store *Store) appendWebAudit(
	ctx context.Context,
	tx *sql.Tx,
	tenantID domain.TenantID,
	userID domain.UserID,
	at time.Time,
	action, subjectKind, subjectID string,
	metadata map[string]any,
) error {
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(action + ":" + string(tenantID) + ":" + string(userID) + ":" + at.UTC().Format(time.RFC3339Nano)))
	auditID := "audit-web-" + hex.EncodeToString(sum[:12])
	_, err = tx.ExecContext(ctx,
		`UPSERT INTO audit_events
		 (tenant_id, occurred_at, audit_event_id, actor_id, action,
		  subject_kind, subject_id, outcome, metadata, expire_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
		         CAST($9 AS JsonDocument), $10)`,
		tenantID, at, auditID, userID, action, subjectKind, subjectID, "succeeded",
		string(encoded), at.Add(store.operationalRetention),
	)
	return err
}

var _ ports.WebAuthStore = (*Store)(nil)
