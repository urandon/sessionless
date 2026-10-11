package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const ownedMCPSemanticInventorySQL = `SELECT sequence,SUBSTRING(record,0,32769),
 SUBSTRING(CAST(digest AS String),0,65),SUBSTRING(CAST(operation_digest AS String),0,65),
 SUBSTRING(CAST(call_id AS String),0,161) FROM attempt_operations_v1
 WHERE tenant_id=$1 AND attempt_id=$2 ORDER BY sequence LIMIT $3`

// Only the single fixed credential-free fixture MCP method is installed. Its
// original grant identifies that resource/endpoint; a live multi-name catalog
// has no authentic descriptor producer here and remains unsupported. Compare
// the canonical argument digest, NOT a regenerated model call ID or response.
// This is a bounded read of the SAME original operation rows, not another key,
// reservation, ledger or retry permission. Any prior reservation is one-way,
// whether its eventual remote result was successful, failed or unknown.
func denyOwnedMCPSemanticRepeat(ctx context.Context, q *ownedAuthorityQuery, op domain.AttemptOperationV1, parent domain.OwnedAttemptEffectParentV1) error {
	if op.Kind != domain.AttemptOperationMCPToolV1 || parent.Authority.HarnessBinding.Backend.BackendKind != domain.HarnessBackendDeterministicFixtureV1 || op.ValidateForParent(parent.Authority, parent.Reservation) != nil {
		return q.budget.deny()
	}
	rows, err := q.QueryContext(ctx, ownedMCPSemanticInventorySQL, op.Scope.TenantID, op.Scope.AttemptID, uint64(13))
	if err != nil {
		return q.budget.deny()
	}
	defer rows.Close()
	var count uint64
	for rows.Next() {
		var sequence uint64
		var raw, digest, operationDigest, call string
		n, err := scanOwnedCurrentRow(rows, &sequence, &raw, &digest, &operationDigest, &call)
		if err != nil || q.budget.materialized(q.callback, n) != nil || count >= 12 || len(raw) > ownedAuthorityHeadBytes || len(digest) != 64 || len(operationDigest) != 64 || len(call) > 160 || sequence != count+1 || sequence > op.Sequence {
			return q.budget.deny()
		}
		count++
		var reservation domain.AttemptOperationReservationV1
		if json.Unmarshal([]byte(raw), &reservation) != nil || reservation.ValidateForParent(parent.Authority, parent.Reservation) != nil {
			return q.budget.deny()
		}
		canonical, _ := json.Marshal(reservation)
		actual, _ := reservation.Digest()
		previous := reservation.Operation
		if !bytes.Equal(canonical, []byte(raw)) || string(actual) != digest || string(reservation.OperationDigest) != operationDigest || previous.Sequence != sequence || previous.Scope != op.Scope || previous.CallID != call {
			return q.budget.deny()
		}
		if sequence == op.Sequence {
			expected, _ := op.Digest()
			if expected != reservation.OperationDigest {
				return q.budget.deny()
			}
			continue // exact current reserved row is not another physical call
		}
		if previous.Kind == domain.AttemptOperationMCPToolV1 && previous.ToolGrantDigest == op.ToolGrantDigest && previous.ArgumentsDigest == op.ArgumentsDigest {
			return q.budget.deny()
		}
	}
	if rows.Err() != nil || count < op.Sequence-1 {
		return q.budget.deny()
	}
	return nil
}
