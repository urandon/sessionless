package ydbstore

import (
	"context"
	"database/sql"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type ownedSessionAdmissionKey struct {
	attempt domain.AttemptID
	run     domain.RunID
}

// Reverse-key inventory, never a global Attempt scan. The shared Session
// inventory charges these rows against its original transaction-wide limit.
func listOwnedSessionAdmissionKeys(ctx context.Context, query stateSQLExecutor, tenant domain.TenantID, session domain.SessionID, limit uint64) ([]ownedSessionAdmissionKey, error) {
	if tenant.Validate() != nil || session.Validate() != nil || limit > maxSessionDeletionRows {
		return nil, domain.ErrMembershipDenied
	}
	rows, err := query.QueryContext(ctx, `SELECT SUBSTRING(CAST(attempt_id AS String),0,161), SUBSTRING(CAST(run_id AS String),0,161)
 FROM attempt_execution_admissions_by_session_v1 WHERE tenant_id=$1 AND session_id=$2 ORDER BY attempt_id LIMIT $3`, tenant, session, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []ownedSessionAdmissionKey
	for rows.Next() {
		var attempt, run string
		if _, err := scanOwnedCurrentRow(rows, &attempt, &run); err != nil || len(attempt) > 160 || len(run) > 160 || domain.AttemptID(attempt).Validate() != nil || domain.RunID(run).Validate() != nil || uint64(len(keys)) >= limit {
			return nil, domain.ErrMembershipDenied
		}
		keys = append(keys, ownedSessionAdmissionKey{attempt: domain.AttemptID(attempt), run: domain.RunID(run)})
	}
	return keys, rows.Err()
}

// Called ONLY by canonical CompleteSessionDeletion, after same-Tx legal-hold,
// terminal Run and pending-receipt gates. Global profile revocation heads/audits
// are intentionally not touched. Further operation/unknown-effect children
// must join those shared gates before #189 cutover; this is not their cleanup.
func deleteOwnedSessionAdmissionsTx(ctx context.Context, tx *stateTx, session domain.SessionID) error {
	keys, err := listOwnedSessionAdmissionKeys(ctx, tx.sqlTx, tx.tenantID, session, maxSessionDeletionRows)
	if err != nil {
		return err
	}
	for _, key := range keys {
		var raw, digest, run, storedSession, owner, reservation string
		var admitted time.Time
		_, err := scanOwnedCurrentRow(tx.sqlTx.QueryRowContext(ctx, ownedAdmissionSQL, tx.tenantID, key.attempt), &raw, &digest, &run, &storedSession, &owner, &reservation, &admitted)
		if err == sql.ErrNoRows {
			return domain.ErrMembershipDenied
		} // orphan inventory is not successful erasure
		a, decodeErr := domain.DecodeAttemptExecutionAdmissionV1([]byte(raw))
		actual, digestErr := a.Digest()
		if err != nil || decodeErr != nil || digestErr != nil || len(raw) > ownedAuthorityHeadBytes || actual != digest ||
			a.Source.Scope.TenantID != tx.tenantID || a.Source.Scope.SessionID != session || a.Source.Scope.AttemptID != key.attempt || a.Source.Scope.RunID != key.run ||
			string(a.Source.Scope.RunID) != run || string(a.Source.Scope.SessionID) != storedSession || string(a.Source.Scope.OwnerUserID) != owner || string(a.ReservationID) != reservation || !a.AdmittedAt.Equal(admitted) {
			return domain.ErrMembershipDenied
		}
		for _, statement := range []struct {
			query string
			args  []any
		}{
			{`DELETE FROM attempt_execution_admissions_v1 WHERE tenant_id=$1 AND attempt_id=$2`, []any{tx.tenantID, key.attempt}},
			{`DELETE FROM attempt_execution_admissions_by_session_v1 WHERE tenant_id=$1 AND session_id=$2 AND attempt_id=$3`, []any{tx.tenantID, session, key.attempt}},
		} {
			if _, err := tx.sqlTx.ExecContext(ctx, statement.query, statement.args...); err != nil {
				return err
			}
		}
	}
	return nil
}
