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

// ReadRunExplanationV1 has no mounted HTTP route. Each retry is one complete
// serializable snapshot within the same three-second deadline. No side effects,
// nested auth transaction, historical discovery, worker API or blob call.
func (store *Store) ReadRunExplanationV1(ctx context.Context, auth ports.RunExplanationAuthorizationV1, runID domain.RunID) (result webcontract.RunExplanationV1, err error) {
	if auth.TenantID.Validate() != nil || auth.UserID.Validate() != nil || auth.WebSessionDigest.Validate("web_session.session_digest") != nil || auth.MembershipSecurityVersion == 0 || runID.Validate() != nil {
		return result, runexplanation.ErrInvalidSource
	}
	ctx, cancel := context.WithTimeout(ctx, runexplanation.ResourceDeadline)
	defer cancel()
	bucket, err := webBucket(string(auth.WebSessionDigest))
	if err != nil {
		return result, err
	}
	userBucket, err := webBucket(string(auth.UserID))
	if err != nil {
		return result, err
	}
	budget := &explanationStatementBudget{}
	err = store.Transact(ctx, auth.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		query := explanationBudgetQuery{tx: tx.sqlTx, budget: budget}
		tx.explanationQuery = query
		// #1 transaction clock, #2 exact Web session, #3 exact membership.
		var at time.Time
		if err := query.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
			return err
		}
		session, found, err := readExplanationJSON[domain.WebSession](ctx, query, `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, bucket, auth.WebSessionDigest)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrWebSessionRevoked
		}
		// Session validity must dominate missing membership, preserving revoked/
		// expired mapping even when a membership is simultaneously removed.
		if session.UserID != auth.UserID || session.ActiveTenantID != auth.TenantID || session.SessionDigest != auth.WebSessionDigest {
			return domain.ErrWebSessionRevoked
		}
		if session.RevokedAt != nil {
			return domain.ErrWebSessionRevoked
		}
		if !at.Before(session.IdleExpiresAt) || !at.Before(session.AbsoluteExpiresAt) {
			return domain.ErrWebSessionExpired
		}
		membership, found, err := readExplanationJSON[domain.TenantMembership](ctx, query, `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`, userBucket, auth.UserID, auth.TenantID)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrMembershipDenied
		}
		if err := authorizeExplanation(session, membership, auth, at); err != nil {
			return err
		}
		// #4 canonical Run, #5 exact Session, #6 current participation. Target
		// inaccessible and absent cases deliberately use one opaque error.
		run, found, err := readExplanationJSON[domain.Run](ctx, query, `SELECT SUBSTRING(CAST(payload AS String),0,8193) FROM runs WHERE tenant_id=$1 AND run_id=$2`, auth.TenantID, runID)
		if err != nil {
			return err
		}
		if !found || run.ID != runID || run.TenantID != auth.TenantID {
			return ports.ErrRunExplanationNotFound
		}
		canonicalSession, found, err := readExplanationJSON[domain.Session](ctx, query, `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM sessions WHERE tenant_id=$1 AND session_id=$2`, auth.TenantID, run.SessionID)
		if err != nil {
			return err
		}
		if !found || canonicalSession.ID != run.SessionID || canonicalSession.TenantID != auth.TenantID {
			return ports.ErrRunExplanationNotFound
		}
		participant, found, err := readExplanationJSON[domain.SessionParticipant](ctx, query, `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM session_participants WHERE tenant_id=$1 AND session_id=$2 AND user_id=$3`, auth.TenantID, run.SessionID, auth.UserID)
		if err != nil {
			return err
		}
		if !found || participant.Authorize(auth.TenantID, run.SessionID, auth.UserID, false) != nil {
			return ports.ErrRunExplanationNotFound
		}
		if canonicalSession.Validate() != nil {
			return runexplanation.ErrInvalidSource
		}
		// #7 exact locator, #8 selected Attempt. No latest-attempt scan.
		head, err := readExplanationHeadTx(ctx, tx, runID)
		if err != nil {
			return err
		}
		snapshot := runexplanation.Snapshot{Run: run, ReadAt: at, Head: head}
		if head != nil && head.TenantID == run.TenantID && head.RunID == run.ID && head.SessionID == run.SessionID {
			attempt, found, err := readExplanationJSON[domain.Attempt](ctx, query, `SELECT SUBSTRING(CAST(payload AS String),0,8193) FROM attempts WHERE tenant_id=$1 AND attempt_id=$2`, auth.TenantID, head.SelectedAttemptID)
			if err != nil {
				return err
			}
			if found {
				snapshot.Attempt = &attempt
			}
			if found && attempt.ID == head.SelectedAttemptID && attempt.RunID == run.ID && attempt.TenantID == run.TenantID && head.Placement.Kind == domain.ExecutionPlacementAttachedWorker {
				if err := readExplanationAttachedTx(ctx, tx, &snapshot); err != nil {
					return err
				}
			}
		}
		value, err := webcontract.NewRunExplanationV1(snapshot)
		if err != nil {
			return err
		}
		if _, err := json.Marshal(value); err != nil {
			return runexplanation.ErrInvalidResponse
		}
		result = value
		return nil
	})
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrWebSessionRevoked), errors.Is(err, domain.ErrWebSessionExpired), errors.Is(err, domain.ErrMembershipDenied), errors.Is(err, domain.ErrMembershipVersionChanged), errors.Is(err, ports.ErrRunExplanationNotFound):
			return webcontract.RunExplanationV1{}, err
		default:
			// Do not leak database statements, private records or decode details.
			return webcontract.RunExplanationV1{}, ports.ErrRunExplanationUnavailable
		}
	}
	return result, nil
}
