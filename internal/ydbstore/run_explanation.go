package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

var _ ports.RunExplanationReadStoreV1 = (*Store)(nil)

// The budget belongs to the entire resource operation, not a retry closure.
// A failed commit may retry the full snapshot, but cannot spend >20 statements.
type explanationStatementBudget struct{ used int }
type explanationBudgetQuery struct {
	tx     *sql.Tx
	budget *explanationStatementBudget
}

func (query explanationBudgetQuery) QueryRowContext(ctx context.Context, statement string, args ...any) *sql.Row {
	if query.budget.used >= runexplanation.MaxPointStatements {
		// database/sql checks cancellation before invoking the driver, so this
		// produces a Scan error without sending an additional database statement.
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		return query.tx.QueryRowContext(cancelled, statement, args...)
	}
	query.budget.used++
	return query.tx.QueryRowContext(ctx, statement, args...)
}
func (query explanationBudgetQuery) ExecContext(ctx context.Context, statement string, args ...any) (sql.Result, error) {
	if query.budget.used >= runexplanation.MaxPointStatements {
		return nil, context.Canceled
	}
	query.budget.used++
	return query.tx.ExecContext(ctx, statement, args...)
}
func explanationQueryTx(tx *stateTx) rowQuery {
	if tx.explanationQuery != nil {
		return tx.explanationQuery
	}
	return tx.sqlTx
}

// All JSON sources here have a fixed typed field set. Bound the transferred
// value in SQL, before decoding; corrupt oversize rows fail content-free.
func readExplanationJSON[T any](ctx context.Context, query rowQuery, statement string, args ...any) (value T, found bool, err error) {
	var encoded string
	err = query.QueryRowContext(ctx, statement, args...).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return value, false, nil
	}
	if err != nil {
		return value, false, err
	}
	if len(encoded) > runexplanation.MaxProjectionBytes || json.Unmarshal([]byte(encoded), &value) != nil {
		return value, false, runexplanation.ErrCorruptProjection
	}
	return value, true, nil
}

func authorizeExplanation(session domain.WebSession, membership domain.TenantMembership, auth ports.RunExplanationAuthorizationV1, at time.Time) error {
	if session.SessionDigest != auth.WebSessionDigest || session.UserID != auth.UserID || session.ActiveTenantID != auth.TenantID {
		return domain.ErrWebSessionRevoked
	}
	if err := session.Authorize(membership, domain.TenantPermissionRead, at); err != nil {
		return err
	}
	if membership.SecurityVersion != auth.MembershipSecurityVersion || session.MembershipSecurityVersion != auth.MembershipSecurityVersion {
		return domain.ErrMembershipVersionChanged
	}
	return nil
}

// These shared exact readers never refresh activity or construct resource
// authority from caller-provided identity fields.
func readExplanationSession(ctx context.Context, query rowQuery, digest domain.SecretDigest) (session domain.WebSession, at time.Time, err error) {
	bucket, err := webBucket(string(digest))
	if err != nil {
		return session, at, err
	}
	if err = query.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		return session, at, err
	}
	session, found, err := readExplanationJSON[domain.WebSession](ctx, query, `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, bucket, digest)
	if err != nil {
		return session, at, err
	}
	if !found || session.SessionDigest != digest || session.RevokedAt != nil {
		return session, at, domain.ErrWebSessionRevoked
	}
	if !at.Before(session.IdleExpiresAt) || !at.Before(session.AbsoluteExpiresAt) {
		return session, at, domain.ErrWebSessionExpired
	}
	return session, at, nil
}

func readExplanationMembership(ctx context.Context, query rowQuery, session domain.WebSession, at time.Time) (membership domain.TenantMembership, err error) {
	if session.UserID.Validate() != nil || session.ActiveTenantID.Validate() != nil {
		return membership, runexplanation.ErrInvalidSource
	}
	userBucket, err := webBucket(string(session.UserID))
	if err != nil {
		return membership, err
	}
	membership, found, err := readExplanationJSON[domain.TenantMembership](ctx, query, `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`, userBucket, session.UserID, session.ActiveTenantID)
	if err != nil {
		return membership, err
	}
	if !found {
		return membership, domain.ErrMembershipDenied
	}
	return membership, session.Authorize(membership, domain.TenantPermissionRead, at)
}

// ReadRunExplanationV1 has no mounted HTTP route. Each retry is one complete
// serializable snapshot within the same three-second deadline. No side effects,
// nested auth transaction, historical discovery, worker API or blob call.
func (store *Store) ReadRunExplanationV1(ctx context.Context, auth ports.RunExplanationAuthorizationV1, runID domain.RunID) (result webcontract.RunExplanationV1, err error) {
	if auth.TenantID.Validate() != nil || auth.UserID.Validate() != nil || auth.WebSessionDigest.Validate("web_session.session_digest") != nil || auth.MembershipSecurityVersion == 0 || runID.Validate() != nil {
		return result, runexplanation.ErrInvalidSource
	}
	ctx, cancel := context.WithTimeout(ctx, runexplanation.ResourceDeadline)
	defer cancel()
	budget := &explanationStatementBudget{}
	err = store.Transact(ctx, auth.TenantID, func(state ports.StateTx) error {
		result = webcontract.RunExplanationV1{}
		tx := state.(*stateTx)
		query := explanationBudgetQuery{tx: tx.sqlTx, budget: budget}
		tx.explanationQuery = query
		// #1 transaction clock, #2 exact Web session, #3 exact membership.
		session, at, err := readExplanationSession(ctx, query, auth.WebSessionDigest)
		// Preserve the pure port's expected-identity check before expiry and
		// membership; the rated port has no caller-selected identity context.
		if (err == nil || errors.Is(err, domain.ErrWebSessionExpired)) && (session.UserID != auth.UserID || session.ActiveTenantID != auth.TenantID) {
			return domain.ErrWebSessionRevoked
		}
		if err != nil {
			return err
		}
		membership, err := readExplanationMembership(ctx, query, session, at)
		if err != nil {
			return err
		}
		if err := authorizeExplanation(session, membership, auth, at); err != nil {
			return err
		}
		result, err = readExplanationResource(ctx, tx, session.UserID, runID, at)
		return err
	})
	if err != nil {
		return webcontract.RunExplanationV1{}, explanationReadError(err)
	}
	return result, nil
}

func explanationReadError(err error) error {
	switch {
	case errors.Is(err, domain.ErrWebSessionRevoked), errors.Is(err, domain.ErrWebSessionExpired), errors.Is(err, domain.ErrMembershipDenied), errors.Is(err, domain.ErrMembershipVersionChanged), errors.Is(err, ports.ErrRunExplanationNotFound):
		return err
	default:
		return ports.ErrRunExplanationUnavailable
	}
}

func readExplanationResource(ctx context.Context, tx *stateTx, userID domain.UserID, runID domain.RunID, at time.Time) (webcontract.RunExplanationV1, error) {
	var result webcontract.RunExplanationV1
	query := explanationQueryTx(tx)
	tenantID := tx.tenantID
	// #4 canonical Run, #5 exact Session, #6 current participation. Target
	// inaccessible and absent cases deliberately use one opaque error.
	run, found, err := readExplanationJSON[domain.Run](ctx, query, `SELECT SUBSTRING(CAST(payload AS String),0,8193) FROM runs WHERE tenant_id=$1 AND run_id=$2`, tenantID, runID)
	if err != nil {
		return result, err
	}
	if !found || run.ID != runID || run.TenantID != tenantID {
		return result, ports.ErrRunExplanationNotFound
	}
	canonicalSession, found, err := readExplanationJSON[domain.Session](ctx, query, `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM sessions WHERE tenant_id=$1 AND session_id=$2`, tenantID, run.SessionID)
	if err != nil {
		return result, err
	}
	if !found || canonicalSession.ID != run.SessionID || canonicalSession.TenantID != tenantID {
		return result, ports.ErrRunExplanationNotFound
	}
	participant, found, err := readExplanationJSON[domain.SessionParticipant](ctx, query, `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM session_participants WHERE tenant_id=$1 AND session_id=$2 AND user_id=$3`, tenantID, run.SessionID, userID)
	if err != nil {
		return result, err
	}
	if !found || participant.Authorize(tenantID, run.SessionID, userID, false) != nil {
		return result, ports.ErrRunExplanationNotFound
	}
	if canonicalSession.Validate() != nil {
		return result, runexplanation.ErrInvalidSource
	}
	// #7 exact locator, #8 selected Attempt. No latest-attempt scan.
	head, err := readExplanationHeadTx(ctx, tx, runID)
	if err != nil {
		return result, err
	}
	snapshot := runexplanation.Snapshot{Run: run, ReadAt: at, Head: head}
	if head != nil && head.TenantID == run.TenantID && head.RunID == run.ID && head.SessionID == run.SessionID {
		attempt, found, err := readExplanationJSON[domain.Attempt](ctx, query, `SELECT SUBSTRING(CAST(payload AS String),0,8193) FROM attempts WHERE tenant_id=$1 AND attempt_id=$2`, tenantID, head.SelectedAttemptID)
		if err != nil {
			return result, err
		}
		if found {
			snapshot.Attempt = &attempt
		}
		if found && attempt.ID == head.SelectedAttemptID && attempt.RunID == run.ID && attempt.TenantID == run.TenantID && head.Placement.Kind == domain.ExecutionPlacementAttachedWorker {
			if err := readExplanationAttachedTx(ctx, tx, &snapshot); err != nil {
				return result, err
			}
		}
	}
	value, err := webcontract.NewRunExplanationV1(snapshot)
	if err != nil {
		return result, err
	}
	if _, err := json.Marshal(value); err != nil {
		return result, runexplanation.ErrInvalidResponse
	}
	return value, nil
}
