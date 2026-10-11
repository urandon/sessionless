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
		if err := deleteUnpreparedOwnedParentTx(ctx, tx, a); err != nil {
			return err
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

// Uses the SAME indexed Final key and canonical Session deletion transaction.
// No independent TTL, global scan or lifecycle is introduced. A durable burn,
// even with a retired registration, is NOT proof of cleanup or of no unknown
// effects. Until the real authenticated residual-disposition gate is implemented,
// keep its parent/Final/observations and deny completion, rather than orphaning
// them or reporting successful erasure. A never-prepared parent has no physical
// effects through this bridge and can be erased under the existing shared gates.
func deleteUnpreparedOwnedParentTx(ctx context.Context, tx *stateTx, final domain.AttemptExecutionAdmissionV1) error {
	var raw, digest string
	var legacy, recordPresent, digestPresent, children uint64
	err := tx.sqlTx.QueryRowContext(ctx, `SELECT
 COALESCE(SUBSTRING(owned_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(owned_digest AS String),0,65),CAST('' AS String)),
 CASE WHEN record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN owned_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN owned_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN preparation_started_at IS NOT NULL OR preparation_parent_digest IS NOT NULL
 OR allocation_original_record IS NOT NULL OR allocation_original_digest IS NOT NULL
 OR allocation_current_record IS NOT NULL OR allocation_current_digest IS NOT NULL
 OR allocation_publication_digest IS NOT NULL OR allocation_transition_parent_digest IS NOT NULL
 THEN CAST(1 AS Uint64) ELSE CAST(0 AS Uint64) END
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`,
		tx.tenantID, final.Source.Scope.AttemptID, domain.ProviderEffectTurnV1).Scan(&raw, &digest, &legacy, &recordPresent, &digestPresent, &children)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil || legacy != 0 || recordPresent != 1 || digestPresent != 1 || children != 0 || len(raw) > ownedAuthorityHeadBytes || len(digest) != 64 {
		return domain.ErrMembershipDenied
	}
	parent, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(raw))
	actual, digestErr := parent.Digest()
	if err != nil || digestErr != nil || actual != digest || parent.ValidateForAdmission(final) != nil {
		return domain.ErrMembershipDenied
	}
	_, err = tx.sqlTx.ExecContext(ctx, `DELETE FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`,
		tx.tenantID, final.Source.Scope.AttemptID, domain.ProviderEffectTurnV1)
	return err
}
