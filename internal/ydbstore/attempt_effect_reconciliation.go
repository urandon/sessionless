package ydbstore

import (
	"context"
	"database/sql"
	"errors"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func (store *Store) RecordAttemptEffectReconciliation(
	ctx context.Context,
	record ports.AttemptEffectReconciliationRecordV1,
) error {
	if err := record.Validate(); err != nil {
		return err
	}
	return store.Transact(ctx, record.TenantID, func(state ports.StateTx) error {
		return putAttemptEffectReconciliationTx(ctx, state.(*stateTx), record)
	})
}

func putAttemptEffectReconciliationTx(
	ctx context.Context,
	tx *stateTx,
	record ports.AttemptEffectReconciliationRecordV1,
) error {
	if tx == nil || record.TenantID != tx.tenantID {
		return domain.ValidationError{Field: "attempt_effect_reconciliation.scope", Reason: "must match the transaction tenant"}
	}
	effect, found, err := readAttemptEffectRecordTx(ctx, tx, record.AttemptID)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("attempt effect reservation not found for reconciliation evidence")
	}
	if effect.Authority.HarnessBinding.TenantID != record.TenantID || effect.Authority.HarnessBinding.RunID != record.RunID ||
		effect.Authority.HarnessBinding.AttemptID != record.AttemptID {
		return domain.ValidationError{Field: "attempt_effect_reconciliation.scope", Reason: "does not match persisted effect authority"}
	}
	if err := record.Evidence.ValidateForPersistedAuthority(effect.Authority, effect.Reservation); err != nil {
		return err
	}
	var existing string
	err = tx.sqlTx.QueryRowContext(ctx,
		`SELECT evidence_digest FROM attempt_effect_reconciliation_evidence
		 WHERE tenant_id=$1 AND attempt_id=$2 AND evidence_digest=$3`,
		tx.tenantID, record.AttemptID, record.Evidence.EvidenceDigest,
	).Scan(&existing)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	payload, err := marshal(record.Evidence.Clone())
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO attempt_effect_reconciliation_evidence
		 (tenant_id,attempt_id,evidence_digest,run_id,physical_invocation_claim_id,operation_state,observed_at,record)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,CAST($8 AS JsonDocument))`,
		tx.tenantID, record.AttemptID, record.Evidence.EvidenceDigest, record.RunID,
		record.Evidence.PhysicalInvocationClaimID, record.Evidence.Observation.State,
		record.Evidence.Observation.ObservedAt.UTC(), payload,
	)
	return err
}

var _ ports.AttemptEffectStoreV1 = (*Store)(nil)
