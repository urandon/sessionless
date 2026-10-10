package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"time"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
)

var (
	errComputeChoiceSource  = errors.New("compute choice source unavailable")
	errComputeChoiceSession = errors.New("compute choice session inaccessible")
)

// This private adapter is groundwork for the list/send transaction, not a
// mounted endpoint or an eligibility grant. A caller must keep all subsequent
// cutover, candidate and limiter reads in this SAME Serializable transaction.
type computeChoiceQuery struct {
	tx             *sql.Tx
	budget         *computechoice.AttemptBudget
	statementError error
}

func (query *computeChoiceQuery) QueryRowContext(ctx context.Context, statement string, args ...any) *sql.Row {
	if err := query.budget.BeforeStatement(); err != nil {
		query.statementError = err
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		return query.tx.QueryRowContext(cancelled, statement, args...)
	}
	return query.tx.QueryRowContext(ctx, statement, args...)
}

func (query *computeChoiceQuery) ExecContext(ctx context.Context, statement string, args ...any) (sql.Result, error) {
	if err := query.budget.BeforeStatement(); err != nil {
		return nil, err
	}
	return query.tx.ExecContext(ctx, statement, args...)
}

// QueryRow cannot return a direct error; the cancelled row prevents an extra
// driver call. Preserve the actual budget cause rather than misclassifying that
// synthetic cancellation as a deadline/network failure.
func (query *computeChoiceQuery) readError(err error) error {
	if query.statementError != nil {
		return query.statementError
	}
	return err
}

type computeChoiceAuth struct {
	scope              computechoice.Scope
	at                 time.Time
	authorityExpiresAt time.Time
	lastEventSequence  uint64
}

const computeChoiceCookieSQL = `SELECT SUBSTRING(CAST(record AS String),0,8193),
 SUBSTRING(CAST(user_id AS String),0,161), SUBSTRING(CAST(active_tenant_id AS String),0,161),
 SUBSTRING(CAST(csrf_token_digest AS String),0,65), membership_security_version,
 idle_expires_at, absolute_expires_at, revoked_at
 FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`
const computeChoiceMembershipSQL = `SELECT SUBSTRING(CAST(record AS String),0,8193),
 SUBSTRING(CAST(role AS String),0,161), SUBSTRING(CAST(status AS String),0,161), security_version
 FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`
const computeChoiceSessionSQL = `SELECT SUBSTRING(CAST(record AS String),0,8193),
 SUBSTRING(CAST(created_by AS String),0,161), SUBSTRING(CAST(status AS String),0,161), last_event_sequence, archived_at
 FROM sessions WHERE tenant_id=$1 AND session_id=$2`
const computeChoiceParticipantSQL = `SELECT SUBSTRING(CAST(record AS String),0,8193),
 SUBSTRING(CAST(role AS String),0,161), SUBSTRING(CAST(status AS String),0,161)
 FROM session_participants WHERE tenant_id=$1 AND session_id=$2 AND user_id=$3`

func readComputeChoiceAuth(ctx context.Context, query *computeChoiceQuery, digest domain.SecretDigest, sessionID domain.SessionID) (computeChoiceAuth, error) {
	var result computeChoiceAuth
	if digest.Validate("web_session.session_digest") != nil || sessionID.Validate() != nil {
		return result, errComputeChoiceSource
	}
	bucket, err := webBucket(string(digest))
	if err != nil {
		return result, errComputeChoiceSource
	}
	var at time.Time
	if err := query.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		return result, query.readError(err)
	}
	if at.IsZero() {
		return result, errComputeChoiceSource
	}
	at = at.UTC()
	var encoded, userID, tenantID, csrf string
	var version uint64
	var idle, absolute time.Time
	var revoked sql.NullTime
	err = query.QueryRowContext(ctx, computeChoiceCookieSQL, bucket, digest).Scan(&encoded, &userID, &tenantID, &csrf, &version, &idle, &absolute, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return result, domain.ErrWebSessionRevoked
	}
	if err != nil {
		return result, query.readError(err)
	}
	var cookie domain.WebSession
	if err := decodeComputeChoiceRecord(query.budget, encoded, &cookie, computeChoiceCookieFields, userID, tenantID, csrf); err != nil {
		return result, err
	}
	if cookie.Validate() != nil || cookie.SessionDigest != digest || string(cookie.UserID) != userID || string(cookie.ActiveTenantID) != tenantID || string(cookie.CSRFTokenDigest) != csrf || cookie.MembershipSecurityVersion != version ||
		!computeChoiceTimestampEqual(cookie.IdleExpiresAt, idle) || !computeChoiceTimestampEqual(cookie.AbsoluteExpiresAt, absolute) || !computeChoiceOptionalTimeEqual(cookie.RevokedAt, revoked, true) || cookie.IssuedAt.After(at) || cookie.LastSeenAt.After(at) {
		return result, errComputeChoiceSource
	}
	if cookie.RevokedAt != nil {
		return result, domain.ErrWebSessionRevoked
	}
	if !at.Before(cookie.IdleExpiresAt) || !at.Before(cookie.AbsoluteExpiresAt) {
		return result, domain.ErrWebSessionExpired
	}
	userBucket, err := webBucket(userID)
	if err != nil {
		return result, errComputeChoiceSource
	}
	var role, status string
	err = query.QueryRowContext(ctx, computeChoiceMembershipSQL, userBucket, cookie.UserID, cookie.ActiveTenantID).Scan(&encoded, &role, &status, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return result, domain.ErrMembershipDenied
	}
	if err != nil {
		return result, query.readError(err)
	}
	var membership domain.TenantMembership
	if err := decodeComputeChoiceRecord(query.budget, encoded, &membership, computeChoiceMembershipFields, role, status); err != nil {
		return result, err
	}
	if membership.Validate() != nil || membership.UserID != cookie.UserID || membership.TenantID != cookie.ActiveTenantID || string(membership.Role) != role || string(membership.Status) != status || membership.SecurityVersion != version || membership.UpdatedAt.After(at) {
		return result, errComputeChoiceSource
	}
	if err := cookie.Authorize(membership, domain.TenantPermissionWrite, at); err != nil {
		return result, err
	}
	var sequence uint64
	var archived sql.NullTime
	err = query.QueryRowContext(ctx, computeChoiceSessionSQL, cookie.ActiveTenantID, sessionID).Scan(&encoded, &userID, &status, &sequence, &archived)
	if errors.Is(err, sql.ErrNoRows) {
		return result, errComputeChoiceSession
	}
	if err != nil {
		return result, query.readError(err)
	}
	var session domain.Session
	if err := decodeComputeChoiceRecord(query.budget, encoded, &session, computeChoiceSessionFields, userID, status); err != nil {
		return result, err
	}
	if session.Validate() != nil || session.ID != sessionID || session.TenantID != cookie.ActiveTenantID || string(session.CreatedBy) != userID || string(session.Status) != status || session.LastEventSequence != sequence || !computeChoiceOptionalTimeEqual(session.ArchivedAt, archived, false) || session.UpdatedAt.After(at) {
		return result, errComputeChoiceSource
	}
	if session.Status != domain.SessionActive {
		return result, errComputeChoiceSession
	}
	err = query.QueryRowContext(ctx, computeChoiceParticipantSQL, cookie.ActiveTenantID, sessionID, cookie.UserID).Scan(&encoded, &role, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return result, errComputeChoiceSession
	}
	if err != nil {
		return result, query.readError(err)
	}
	var participant domain.SessionParticipant
	if err := decodeComputeChoiceRecord(query.budget, encoded, &participant, computeChoiceParticipantFields, role, status); err != nil {
		return result, err
	}
	if participant.Validate() != nil || participant.TenantID != cookie.ActiveTenantID || participant.SessionID != sessionID || participant.UserID != cookie.UserID || string(participant.Role) != role || string(participant.Status) != status || participant.UpdatedAt.After(at) {
		return result, errComputeChoiceSource
	}
	if participant.Authorize(cookie.ActiveTenantID, sessionID, cookie.UserID, true) != nil {
		return result, errComputeChoiceSession
	}
	expires := cookie.IdleExpiresAt
	if cookie.AbsoluteExpiresAt.Before(expires) {
		expires = cookie.AbsoluteExpiresAt
	}
	return computeChoiceAuth{scope: computechoice.Scope{TenantID: cookie.ActiveTenantID, UserID: cookie.UserID, SessionID: sessionID, MembershipSecurityVersion: membership.SecurityVersion}, at: at, authorityExpiresAt: expires, lastEventSequence: session.LastEventSequence}, nil
}

// YDB Timestamp stores microseconds; canonical JSON may retain nanoseconds.
// Compare the persisted scalar at its actual precision, never a millisecond
// tolerance or process clock. Existing Session readers use the same rule.
func computeChoiceTimestampEqual(record, scalar time.Time) bool {
	return record.UTC().Truncate(time.Microsecond).Equal(scalar)
}

func computeChoiceOptionalTimeEqual(record *time.Time, scalar sql.NullTime, zeroSentinel bool) bool {
	if record != nil {
		return scalar.Valid && computeChoiceTimestampEqual(*record, scalar.Time)
	}
	return !scalar.Valid || (zeroSentinel && scalar.Time.Equal(time.Unix(0, 0)))
}

// true means required; optional timestamps alone may be omitted/null. Exact
// JSON field spelling and duplicate rejection avoid permissive struct decoding.
var computeChoiceCookieFields = map[string]bool{"session_digest": true, "csrf_token_digest": true, "user_id": true, "active_tenant_id": true, "authenticated_subject": true, "membership_security_version": true, "issued_at": true, "last_seen_at": true, "idle_expires_at": true, "absolute_expires_at": true, "revoked_at": false}
var computeChoiceMembershipFields = map[string]bool{"tenant_id": true, "user_id": true, "role": true, "status": true, "security_version": true, "created_at": true, "updated_at": true}
var computeChoiceSessionFields = map[string]bool{"id": true, "tenant_id": true, "created_by": true, "status": true, "last_event_sequence": true, "created_at": true, "updated_at": true, "archived_at": false}
var computeChoiceParticipantFields = map[string]bool{"tenant_id": true, "session_id": true, "user_id": true, "role": true, "status": true, "created_at": true, "updated_at": true}

func decodeComputeChoiceRecord(budget *computechoice.AttemptBudget, encoded string, target any, fields map[string]bool, scalars ...string) error {
	if err := budget.RecordMaterialized(computechoice.SourceOrdinary, int64(len(encoded))); err != nil {
		return err
	}
	var scalarBytes int64
	for _, scalar := range scalars {
		scalarBytes += int64(len(scalar))
	}
	if err := budget.RecordMaterialized(computechoice.SourceOrdinary, scalarBytes); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewBufferString(encoded))
	if validateComputeChoiceObject(decoder, fields) != nil {
		return errComputeChoiceSource
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errComputeChoiceSource
	}
	if json.Unmarshal([]byte(encoded), target) != nil {
		return errComputeChoiceSource
	}
	return nil
}

func validateComputeChoiceObject(decoder *json.Decoder, fields map[string]bool) error {
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return errComputeChoiceSource
	}
	seen := make(map[string]bool, len(fields))
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		required, allowed := fields[key]
		if err != nil || !ok || !allowed || seen[key] {
			return errComputeChoiceSource
		}
		seen[key] = true
		if key == "authenticated_subject" {
			if validateComputeChoiceObject(decoder, map[string]bool{"provider": true, "subject": true}) != nil {
				return errComputeChoiceSource
			}
			continue
		}
		var raw json.RawMessage
		if decoder.Decode(&raw) != nil || len(raw) == 0 || raw[0] == '{' || raw[0] == '[' || (required && bytes.Equal(raw, []byte("null"))) {
			return errComputeChoiceSource
		}
	}
	if ending, err := decoder.Token(); err != nil || ending != json.Delim('}') {
		return errComputeChoiceSource
	}
	for key, required := range fields {
		if required && !seen[key] {
			return errComputeChoiceSource
		}
	}
	return nil
}
