package ydbstore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// These helpers operate only inside the caller's original transaction. They
// neither install V2, prove a provider account nor authorize a link. In
// particular, no permissive V1 decode or an absent reverse row supplies V2
// ownership. The public operation must validate its original proofs first.
const webIdentityRowSQLV2 = `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM external_identities WHERE shard_bucket=$1 AND provider=$2 AND subject=$3`
const webIdentitySessionSQLV2 = `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`
const webIdentityActiveSetSQLV2 = `SELECT SUBSTRING(CAST(provider AS String),0,161), SUBSTRING(CAST(subject AS String),0,161), created_at FROM external_identities_by_user WHERE user_bucket=$1 AND user_id=$2 ORDER BY provider, subject LIMIT 3`

func readWebIdentityJSONV2(ctx context.Context, query rowQuery, statement string, args ...any) ([]byte, bool, error) {
	if ctx == nil || ctx.Err() != nil || query == nil ||
		(statement != webIdentityRowSQLV2 && statement != webIdentitySessionSQLV2) {
		return nil, false, domain.ErrMembershipDenied
	}
	var raw string
	var err error
	if owned, ok := query.(*ownedAuthorityQuery); ok {
		// Preserve the original statement/materialization budget. A V2 identity
		// lookup is one bounded point read, not an arbitrary canonical fallback.
		raw, err = owned.readMetadata(ctx, statement, domain.IdentityAuthMaxRecordBytesV2, args...)
	} else {
		err = query.QueryRowContext(ctx, statement, args...).Scan(&raw)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if len(raw) == 0 || len(raw) > domain.IdentityAuthMaxRecordBytesV2 {
		return nil, false, domain.ErrMembershipDenied
	}
	return []byte(raw), true, nil
}

func readExternalIdentityV2(ctx context.Context, query rowQuery, subject domain.ExternalSubject) (domain.ExternalIdentityV2, bool, error) {
	if subject.Validate() != nil {
		return domain.ExternalIdentityV2{}, false, domain.ErrMembershipDenied
	}
	bucket, err := webBucket(subject.String())
	if err != nil {
		return domain.ExternalIdentityV2{}, false, err
	}
	raw, found, err := readWebIdentityJSONV2(ctx, query, webIdentityRowSQLV2, bucket, subject.Provider, subject.Subject)
	if err != nil || !found {
		return domain.ExternalIdentityV2{}, false, err
	}
	identity, err := domain.DecodeExternalIdentityV2(raw)
	if err != nil || identity.Subject != subject {
		return domain.ExternalIdentityV2{}, false, domain.ErrMembershipDenied
	}
	return identity, true, nil
}

func readActiveExternalIdentitiesV2(ctx context.Context, query stateSQLExecutor, user domain.UserID) ([]domain.ExternalIdentityV2, error) {
	if ctx == nil || ctx.Err() != nil || query == nil || user.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	bucket, err := webBucket(string(user))
	if err != nil {
		return nil, err
	}
	rows, err := query.QueryContext(ctx, webIdentityActiveSetSQLV2, bucket, user)
	if err != nil {
		return nil, err
	}
	// Finish and close the range before issuing any canonical point read on
	// the same connection. LIMIT 3 is an excess sentinel, never truncation to 2.
	type projection struct {
		subject domain.ExternalSubject
		created time.Time
	}
	var projected []projection
	for rows.Next() {
		var value projection
		var provider, subject string
		var scanErr error
		if owned, ok := query.(*ownedAuthorityQuery); ok {
			// Charge every returned scalar, including malformed values and the
			// excess sentinel, to the ORIGINAL callback before interpreting it.
			returned, err := scanOwnedCurrentRow(rows, &provider, &subject, &value.created)
			charged := owned.budget.materialized(owned.callback, returned)
			if charged != nil {
				scanErr = charged
			} else {
				scanErr = err
			}
		} else {
			scanErr = rows.Scan(&provider, &subject, &value.created)
		}
		if scanErr != nil {
			_ = rows.Close()
			return nil, scanErr
		}
		value.subject = domain.ExternalSubject{Provider: domain.IdentityProvider(provider), Subject: subject}
		projected = append(projected, value)
		if len(projected) > 2 {
			_ = rows.Close()
			return nil, domain.ErrMembershipDenied
		}
	}
	rowErr := rows.Err()
	closeErr := rows.Close()
	if rowErr != nil {
		return nil, rowErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	active := make([]domain.ExternalIdentityV2, 0, len(projected))
	for _, value := range projected {
		identity, found, err := readExternalIdentityV2(ctx, query, value.subject)
		if err != nil {
			return nil, err
		}
		if !found || identity.UserID != user || !identity.CreatedAt.Equal(value.created) {
			return nil, domain.ErrMembershipDenied
		}
		active = append(active, identity)
	}
	if _, err := domain.ValidateExternalIdentitySetV2(user, active); err != nil {
		return nil, domain.ErrMembershipDenied
	}
	return active, nil
}

// writeExternalIdentityV2 never overwrites another owner's canonical mapping.
// The same transaction owns its forward and reverse writes. It is a private
// persistence primitive, not the public link or initialization operation.
func writeExternalIdentityV2(ctx context.Context, query stateSQLExecutor, next domain.ExternalIdentityV2) error {
	if next.Validate() != nil {
		return domain.ErrMembershipDenied
	}
	previous, found, err := readExternalIdentityV2(ctx, query, next.Subject)
	if err != nil {
		return err
	}
	if found {
		if err := domain.ValidateExternalIdentityOwnershipV2(previous, next); err != nil {
			return err
		}
	}
	raw, err := domain.EncodeExternalIdentityV2(next)
	if err != nil {
		return err
	}
	bucket, err := webBucket(next.Subject.String())
	if err != nil {
		return err
	}
	if _, err := query.ExecContext(ctx, `UPSERT INTO external_identities
 (shard_bucket, provider, subject, user_id, created_at, updated_at, record)
 VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS JsonDocument))`,
		bucket, next.Subject.Provider, next.Subject.Subject, next.UserID, next.CreatedAt, next.UpdatedAt, string(raw)); err != nil {
		return err
	}
	userBucket, err := webBucket(string(next.UserID))
	if err != nil {
		return err
	}
	if next.State == domain.ExternalIdentityUnlinkedV2 {
		_, err = query.ExecContext(ctx, `DELETE FROM external_identities_by_user
 WHERE user_bucket=$1 AND user_id=$2 AND provider=$3 AND subject=$4`,
			userBucket, next.UserID, next.Subject.Provider, next.Subject.Subject)
		return err
	}
	_, err = query.ExecContext(ctx, `UPSERT INTO external_identities_by_user
 (user_bucket,user_id,provider,subject,created_at) VALUES ($1,$2,$3,$4,$5)`,
		userBucket, next.UserID, next.Subject.Provider, next.Subject.Subject, next.CreatedAt)
	return err
}

func readWebSessionV2(ctx context.Context, query rowQuery, digest domain.SecretDigest) (domain.WebSessionV2, bool, error) {
	if digest.Validate("web_session_v2.session_digest") != nil {
		return domain.WebSessionV2{}, false, domain.ErrWebSessionRevoked
	}
	bucket, err := webBucket(string(digest))
	if err != nil {
		return domain.WebSessionV2{}, false, err
	}
	raw, found, err := readWebIdentityJSONV2(ctx, query, webIdentitySessionSQLV2, bucket, digest)
	if err != nil || !found {
		return domain.WebSessionV2{}, false, err
	}
	session, err := domain.DecodeWebSessionV2(raw)
	if err != nil || session.Session.SessionDigest != digest {
		return domain.WebSessionV2{}, false, domain.ErrWebSessionRevoked
	}
	return session, true, nil
}

func writeWebSessionV2(ctx context.Context, query stateSQLExecutor, value domain.WebSessionV2) error {
	raw, err := domain.EncodeWebSessionV2(value)
	if err != nil {
		return err
	}
	session := value.Session
	bucket, err := webBucket(string(session.SessionDigest))
	if err != nil {
		return err
	}
	revokedAt := time.Unix(0, 0).UTC()
	if session.RevokedAt != nil {
		revokedAt = *session.RevokedAt
	}
	_, err = query.ExecContext(ctx, `UPSERT INTO web_sessions
 (shard_bucket,session_digest,csrf_token_digest,user_id,active_tenant_id,
 identity_provider,external_subject,membership_security_version,issued_at,
 last_seen_at,idle_expires_at,absolute_expires_at,revoked_at,record)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,CAST($14 AS JsonDocument))`,
		bucket, session.SessionDigest, session.CSRFTokenDigest, session.UserID, session.ActiveTenantID,
		session.AuthenticatedSubject.Provider, session.AuthenticatedSubject.Subject,
		session.MembershipSecurityVersion, session.IssuedAt, session.LastSeenAt,
		session.IdleExpiresAt, session.AbsoluteExpiresAt, revokedAt, string(raw))
	return err
}

// authorizeWebSessionV2 is a current transaction-local join, never a reusable
// authorization receipt. Its caller must preserve this transaction through the
// protected effect and must not substitute a cached BFF identity observation.
func authorizeWebSessionV2(ctx context.Context, query rowQuery, digest domain.SecretDigest,
	permission domain.TenantPermission, at time.Time) (domain.WebSessionV2, domain.TenantMembership, error) {
	deny := func() (domain.WebSessionV2, domain.TenantMembership, error) {
		return domain.WebSessionV2{}, domain.TenantMembership{}, domain.ErrWebSessionRevoked
	}
	session, found, err := readWebSessionV2(ctx, query, digest)
	if err != nil || !found {
		return deny()
	}
	identity, found, err := readExternalIdentityV2(ctx, query, session.Session.AuthenticatedSubject)
	if err != nil || !found {
		return deny()
	}
	membership, found, err := readMembershipTx(ctx, query, session.Session.UserID, session.Session.ActiveTenantID)
	if err != nil || !found || session.AuthorizeCurrent(identity, membership, permission, at) != nil {
		return deny()
	}
	return session, membership, nil
}
