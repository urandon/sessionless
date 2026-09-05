package ydbstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

var ErrSubstrateExecutionEvidenceConflict = errors.New("substrate execution evidence conflict")

func putSubstrateExecutionEvidenceTx(
	ctx context.Context,
	tx *stateTx,
	run domain.Run,
	attempt domain.Attempt,
	evidence domain.SubstrateExecutionEvidenceV1,
	recordedAt time.Time,
) error {
	if tx == nil || run.TenantID != tx.tenantID || attempt.TenantID != tx.tenantID || attempt.RunID != run.ID || recordedAt.IsZero() {
		return domain.ValidationError{Field: "substrate_execution_evidence.scope", Reason: "must match the terminal transaction"}
	}
	record, found, err := readAttemptEffectRecordTx(ctx, tx, attempt.ID)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("attempt effect reservation not found for substrate evidence")
	}
	if record.Authority.HarnessBinding.TenantID != run.TenantID || record.Authority.HarnessBinding.RunID != run.ID || record.Authority.HarnessBinding.AttemptID != attempt.ID {
		return domain.ValidationError{Field: "substrate_execution_evidence.scope", Reason: "does not match persisted effect authority"}
	}
	if err := evidence.ValidateForPersistedAuthority(record.Authority, record.Reservation); err != nil {
		return fmt.Errorf("validate substrate execution evidence: %w", err)
	}
	var existing domain.SubstrateExecutionEvidenceDigestV1
	err = tx.sqlTx.QueryRowContext(ctx,
		`SELECT evidence_digest FROM substrate_execution_evidence
		 WHERE tenant_id=$1 AND attempt_id=$2`,
		tx.tenantID, attempt.ID,
	).Scan(&existing)
	if err == nil {
		if existing == evidence.EvidenceDigest {
			return nil
		}
		return ErrSubstrateExecutionEvidenceConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	payload, err := marshal(evidence.Clone())
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO substrate_execution_evidence
		 (tenant_id,attempt_id,run_id,physical_invocation_claim_id,evidence_digest,recorded_at,record)
		 VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS JsonDocument))`,
		tx.tenantID, attempt.ID, run.ID, evidence.PhysicalInvocationClaimID,
		evidence.EvidenceDigest, recordedAt.UTC(), payload,
	)
	return err
}
