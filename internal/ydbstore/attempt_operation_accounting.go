package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const ownedOperationAccountingSQL = `SELECT
 CASE WHEN operation_accounting_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN operation_accounting_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 COALESCE(SUBSTRING(operation_accounting_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(operation_accounting_digest AS String),0,65),CAST('' AS String))
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`

const ownedOperationAccountingWriteSQL = `UPDATE attempt_effect_reservations
 SET operation_accounting_record=$1,operation_accounting_digest=$2
 WHERE tenant_id=$3 AND attempt_id=$4 AND kind=$5 AND owned_digest=$6 AND record IS NULL`

// SAME original parent's accounting. Component order is provider, substrate,
// MCP, search, matching the admitted Source. Setup reserves the full original
// substrate envelope before any physical preparation; no usage refunds it.
type ownedAttemptAccounting struct {
	Version         uint32                         `json:"version"`
	ParentDigest    string                         `json:"parent_digest"`
	SetupReservedAt time.Time                      `json:"setup_reserved_at"`
	Aggregate       domain.AttemptOperationDebitV1 `json:"aggregate"`
	Amounts         [4]uint64                      `json:"amounts"`
	UpdatedAt       time.Time                      `json:"updated_at"`
}

func (v ownedAttemptAccounting) validate(parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, at time.Time) error {
	pd, err := parent.Digest()
	if err != nil || v.Version != 1 || v.ParentDigest != pd || !ownedEffectTime(v.SetupReservedAt) || v.SetupReservedAt.Before(parent.Reservation.ReservedAt) || !v.SetupReservedAt.Before(parent.PreparationExpiresAt) || !ownedEffectTime(v.UpdatedAt) || v.UpdatedAt.Before(v.SetupReservedAt) || v.UpdatedAt.After(at) || source.Validate() != nil || parent.Authority.HarnessBinding.AgentLoopPolicy == nil || v.Aggregate.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil {
		return domain.ErrMembershipDenied
	}
	var total, operations uint64
	for i, amount := range v.Amounts {
		ceiling := source.Spending[i].MaxAmountMicrounits
		if ceiling == nil || amount > *ceiling || amount > math.MaxUint64-total {
			return domain.ErrMembershipDenied
		}
		total += amount
		if i != 1 {
			if amount > math.MaxUint64-operations {
				return domain.ErrMembershipDenied
			}
			operations += amount
		}
	}
	if v.Amounts[1] != *source.Spending[1].MaxAmountMicrounits || total > source.MaxTotalAmountMicrounits || operations != v.Aggregate.AmountMicrounits {
		return domain.ErrMembershipDenied
	}
	return nil
}

func accountingBytes(v ownedAttemptAccounting) ([]byte, string, error) {
	raw, err := json.Marshal(v)
	if err != nil || len(raw) > ownedAuthorityHeadBytes {
		return nil, "", domain.ErrMembershipDenied
	}
	sum := sha256.Sum256(raw)
	return raw, hex.EncodeToString(sum[:]), nil
}

func readOwnedAttemptAccounting(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) (*ownedAttemptAccounting, error) {
	var recordPresent, digestPresent uint64
	var raw, digest string
	r := parent.Reservation
	if q.scanMetadata(ctx, ownedOperationAccountingSQL, []any{r.TenantID, r.AttemptID, r.Kind}, &recordPresent, &digestPresent, &raw, &digest) != nil {
		return nil, q.budget.deny()
	}
	if recordPresent == 0 && digestPresent == 0 && raw == "" && digest == "" {
		return nil, nil
	}
	var value ownedAttemptAccounting
	if recordPresent != 1 || digestPresent != 1 || len(raw) > ownedAuthorityHeadBytes || json.Unmarshal([]byte(raw), &value) != nil || value.validate(parent, source, q.at) != nil {
		return nil, q.budget.deny()
	}
	canonical, actual, err := accountingBytes(value)
	if err != nil || actual != digest || !bytes.Equal(canonical, []byte(raw)) {
		return nil, q.budget.deny()
	}
	return &value, nil
}

func writeOwnedAttemptAccounting(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, value ownedAttemptAccounting) error {
	if value.validate(parent, source, q.at) != nil {
		return q.budget.deny()
	}
	raw, digest, err := accountingBytes(value)
	if err != nil {
		return q.budget.deny()
	}
	r := parent.Reservation
	_, err = q.ExecContext(ctx, ownedOperationAccountingWriteSQL, string(raw), digest, r.TenantID, r.AttemptID, r.Kind, value.ParentDigest)
	return err
}

func reserveOwnedSetupAccounting(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) error {
	old, err := readOwnedAttemptAccounting(ctx, q, parent, source)
	if err != nil || old != nil {
		return q.budget.deny()
	}
	pd, err := parent.Digest()
	if err != nil || source.Spending[1].MaxAmountMicrounits == nil {
		return q.budget.deny()
	}
	value := ownedAttemptAccounting{Version: 1, ParentDigest: pd, SetupReservedAt: q.at, UpdatedAt: q.at}
	// Reserve immutable original grant custody before physical preparation, in
	// the SAME aggregate body budget. Actual canonical grant bytes are <=4 KiB
	// each; this conservative reservation introduces no parallel metadata ledger.
	value.Aggregate.BodyBytes = ownedSetupBodyBytes(parent)
	value.Amounts[1] = *source.Spending[1].MaxAmountMicrounits
	return writeOwnedAttemptAccounting(ctx, q, parent, source, value)
}

func ownedSetupBodyBytes(parent domain.OwnedAttemptEffectParentV1) uint64 {
	var bytes uint64
	for _, cost := range []*domain.ExternalServiceCostCeilingV1{parent.Authority.AdmissionCostCeiling.MCPToolCost, parent.Authority.AdmissionCostCeiling.WebSearchCost} {
		if cost != nil {
			bytes += 4 << 10
		}
	}
	return bytes
}

func debitOwnedOperation(value ownedAttemptAccounting, operation domain.AttemptOperationV1, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, at time.Time) (ownedAttemptAccounting, error) {
	index := 0
	switch operation.Kind {
	case domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1:
	case domain.AttemptOperationMCPToolV1:
		index = 2
	case domain.AttemptOperationWebSearchV1:
		index = 3
	default:
		return ownedAttemptAccounting{}, domain.ErrMembershipDenied
	}
	// MODEL/COMPACTION's exact upward quote has already been authenticated from
	// original rate custody and current profile/head BEFORE this debit. External
	// tools still reserve their complete admitted component ceiling: no authentic
	// per-call tool rate producer is installed. SAME component/aggregate ledger.
	ceiling := source.Spending[index].MaxAmountMicrounits
	if ceiling == nil || operation.Debit.AmountMicrounits > *ceiling || (index != 0 && operation.Debit.AmountMicrounits != *ceiling) || operation.Sequence != value.Aggregate.Operations+1 || value.Amounts[index] > math.MaxUint64-operation.Debit.AmountMicrounits {
		return ownedAttemptAccounting{}, domain.ErrMembershipDenied
	}
	aggregate, err := value.Aggregate.Add(operation.Debit)
	if err != nil {
		return ownedAttemptAccounting{}, domain.ErrMembershipDenied
	}
	value.Aggregate = aggregate
	value.Amounts[index] += operation.Debit.AmountMicrounits
	value.UpdatedAt = at
	if value.validate(parent, source, at) != nil {
		return ownedAttemptAccounting{}, domain.ErrMembershipDenied
	}
	return value, nil
}
