package ydbstore

import (
	"context"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Historical SQL DATA only. No live invocation entry, residual receipt or
// cleanup capability is reconstructed from the finalizer's sealed records.
type retainedDeletionWriterData struct {
	producer       retainedDeletionProducerData
	operations     []ownedOperationRow
	reconciliation uint64
}

const retainedDeletionCallsSQL = `SELECT SUBSTRING(CAST(call_id AS String),0,161),sequence,
 SUBSTRING(CAST(operation_digest AS String),0,65)
 FROM attempt_operation_calls_v1 WHERE tenant_id=$1 AND attempt_id=$2 ORDER BY call_id LIMIT 13`

const retainedDeletionReconciliationSQL = `SELECT SUBSTRING(CAST(evidence_digest AS String),0,65),
 SUBSTRING(CAST(run_id AS String),0,161),SUBSTRING(CAST(physical_invocation_claim_id AS String),0,161),
 SUBSTRING(CAST(operation_state AS String),0,33),observed_at,SUBSTRING(CAST(record AS String),0,32769)
 FROM attempt_effect_reconciliation_evidence WHERE tenant_id=$1 AND attempt_id=$2
 ORDER BY evidence_digest LIMIT 10001`

// Join ALL original operation/call/proxy/reconciliation rows in a bounded
// batch, including empty-range facts captured by the Session metadata stream.
// The producer join remains responsible for original parent/admission/burn,
// retired allocation and the genuine successful V2 finalization correspondence.
func readRetainedDeletionWriter(ctx context.Context, q *ownedAuthorityQuery, run domain.Run, anchor domain.SessionEvent, counts retainedDeletionAttemptRows) (retainedDeletionWriterData, error) {
	deny := func() (retainedDeletionWriterData, error) { return retainedDeletionWriterData{}, q.budget.deny() }
	p, err := readRetainedDeletionProducer(ctx, q, run, anchor)
	if err != nil {
		return deny()
	}
	ops, err := readRetainedDeletionOperations(ctx, q, p)
	if err != nil || counts.parents != 1 || counts.evidence != 1 || counts.operations != uint64(len(ops)) || counts.calls != uint64(len(ops)) || len(ops) == 0 || counts.proxies != uint64(len(ops)-1) {
		return deny()
	}
	if readRetainedDeletionCalls(ctx, q, p, ops) != nil || readRetainedDeletionProxies(ctx, q, p, ops) != nil {
		return deny()
	}
	n, err := readRetainedDeletionReconciliation(ctx, q, p)
	if err != nil || n != counts.reconciliation {
		return deny()
	}
	return retainedDeletionWriterData{producer: p, operations: ops, reconciliation: n}, nil
}

func readRetainedDeletionOperations(ctx context.Context, q *ownedAuthorityQuery, p retainedDeletionProducerData) ([]ownedOperationRow, error) {
	v := p.terminal
	parent, source, accounting := v.Parent, v.Admission.Source, v.Accounting
	scope := ownedOperationScope(parent)
	rows, err := q.QueryContext(ctx, localCompactionOperationsSQL, scope.TenantID, scope.AttemptID)
	if err != nil {
		return nil, q.budget.deny()
	}
	defer rows.Close()
	baseline := ownedAttemptAccounting{Version: 1, ParentDigest: accounting.ParentDigest, SetupReservedAt: accounting.SetupReservedAt, UpdatedAt: accounting.SetupReservedAt}
	baseline.Aggregate.BodyBytes = ownedSetupBodyBytes(parent)
	baseline.Amounts[1] = *source.Spending[1].MaxAmountMicrounits
	var result []ownedOperationRow
	var predecessor domain.AttemptOperationObservationDigestV1
	for rows.Next() {
		var c ownedOperationColumns
		var sequence, indexed uint64
		var indexedDigest string
		dest := append([]any{&sequence}, c.destinations()...)
		dest = append(dest, &indexed, &indexedDigest)
		n, e := scanOwnedCurrentRow(rows, dest...)
		if q.budget.materialized(q.callback, n) != nil || e != nil || len(result) >= 12 || sequence != uint64(len(result))+1 || sequence > accounting.Aggregate.Operations {
			return nil, q.budget.deny()
		}
		row, e := c.decode(q, scope, sequence)
		if e != nil || row == nil || row.reservation.Operation.Scope != scope || row.reservation.ValidateForParent(parent.Authority, parent.Reservation) != nil || indexed != sequence || indexedDigest != c.operationDigest || row.reservation.Operation.PredecessorObservationDigest != predecessor || row.reservation.ReservedAt.Before(baseline.UpdatedAt) || row.observation == nil || row.observation.ObservedAt.After(v.Run.TerminalCustody.LocalEvidence.StoppedAt) {
			return nil, q.budget.deny()
		}
		if !ownedObservationAllowsSuccessor(row.reservation, *row.observation) {
			// SQL DATA collection only, not deletion eligibility or fresh effect
			// authority. This arm MUST subsequently match an actual V2 capsule,
			// complete V3 accounting and canonical seal outside this callback.
			candidate, err := ports.BuildOwnedOperationAccountingRowV3(row.reservation, *row.observation, parent, source)
			if err != nil || candidate.Kind != domain.AttemptOperationMCPToolV1 || candidate.Disposition != ports.OwnedOperationUnknownFullReservedV3 {
				return nil, q.budget.deny()
			}
		}
		if len(result) > 0 && row.reservation.ReservedAt.Before(result[len(result)-1].observation.ObservedAt) {
			return nil, q.budget.deny()
		}
		baseline, e = debitOwnedOperation(baseline, row.reservation.Operation, parent, source, row.reservation.ReservedAt)
		if e != nil || baseline.Aggregate != row.reservation.AggregateDebit || baseline.Amounts != row.reservation.ComponentAmounts {
			return nil, q.budget.deny()
		}
		predecessor, _ = row.observation.Digest()
		result = append(result, *row)
	}
	if rows.Err() != nil || rows.Close() != nil || uint64(len(result)) != accounting.Aggregate.Operations || !reflect.DeepEqual(baseline, accounting) {
		return nil, q.budget.deny()
	}
	return result, nil
}

func readRetainedDeletionCalls(ctx context.Context, q *ownedAuthorityQuery, p retainedDeletionProducerData, ops []ownedOperationRow) error {
	scope := p.terminal.Parent.Scope
	rows, err := q.QueryContext(ctx, retainedDeletionCallsSQL, scope.TenantID, scope.AttemptID)
	if err != nil {
		return q.budget.deny()
	}
	defer rows.Close()
	var previous string
	count := 0
	for rows.Next() {
		var call, digest string
		var sequence uint64
		n, e := scanOwnedCurrentRow(rows, &call, &sequence, &digest)
		if q.budget.materialized(q.callback, n) != nil || e != nil || count >= len(ops) || sequence == 0 || sequence > uint64(len(ops)) || len(call) > 160 || len(digest) != 64 || (count > 0 && call <= previous) {
			return q.budget.deny()
		}
		op := ops[sequence-1].reservation
		if call != op.Operation.CallID || digest != string(op.OperationDigest) {
			return q.budget.deny()
		}
		previous = call
		count++
	}
	if rows.Err() != nil || rows.Close() != nil || count != len(ops) {
		return q.budget.deny()
	}
	return nil
}

func readRetainedDeletionProxies(ctx context.Context, q *ownedAuthorityQuery, p retainedDeletionProducerData, ops []ownedOperationRow) error {
	parent, source, allocation := p.terminal.Parent, p.terminal.Admission.Source, p.allocation.original
	first, err := readOwnedProxySnapshotAt(ctx, q, parent, source, allocation, 1)
	if err != nil || first == nil || len(ops) == 0 {
		return q.budget.deny()
	}
	validate := func(proxy *ownedProxySnapshot, sequence uint64) error {
		if sequence == 0 || sequence > uint64(len(ops)) {
			return q.budget.deny()
		}
		r := ops[sequence-1].reservation
		d, e := proxy.original.Digest()
		a, ae := allocation.Digest()
		if e != nil || ae != nil || r.RegisteredAllocationDigest != a || r.RegisteredProxyDigest != d || proxy.original.OperationDigest != r.OperationDigest || proxy.original.OperationSequence != sequence || proxy.original.ServiceKind != ownedOperationProxyService(r.Operation.Kind) {
			return q.budget.deny()
		}
		return nil
	}
	if validate(first, 1) != nil {
		return q.budget.deny()
	}
	rows, err := q.QueryContext(ctx, localCompactionProxiesSQL, parent.Scope.TenantID, parent.Scope.AttemptID)
	if err != nil {
		return q.budget.deny()
	}
	defer rows.Close()
	count := 1
	for rows.Next() {
		var sequence uint64
		var c ownedProxyColumns
		dest := append([]any{&sequence}, c.destinations(2)...)
		n, e := scanOwnedCurrentRow(rows, dest...)
		if q.budget.materialized(q.callback, n) != nil || e != nil || sequence != uint64(count)+1 || sequence > uint64(len(ops)) {
			return q.budget.deny()
		}
		proxy, e := c.decode(q, parent, source, allocation, sequence)
		if e != nil || proxy == nil || validate(proxy, sequence) != nil {
			return q.budget.deny()
		}
		count++
	}
	if rows.Err() != nil || rows.Close() != nil || count != len(ops) {
		return q.budget.deny()
	}
	return nil
}

func readRetainedDeletionReconciliation(ctx context.Context, q *ownedAuthorityQuery, p retainedDeletionProducerData) (uint64, error) {
	parent := p.terminal.Parent
	rows, err := q.QueryContext(ctx, retainedDeletionReconciliationSQL, parent.Scope.TenantID, parent.Scope.AttemptID)
	if err != nil {
		return 0, q.budget.deny()
	}
	defer rows.Close()
	var count uint64
	var previous string
	for rows.Next() {
		var digest, run, claim, state, raw string
		var at time.Time
		n, e := scanOwnedCurrentRow(rows, &digest, &run, &claim, &state, &at, &raw)
		if q.budget.materialized(q.callback, n) != nil || e != nil || count >= maxSessionDeletionRows || len(raw) > ownedAuthorityHeadBytes || len(digest) != 64 || len(run) > 160 || len(claim) > 160 || (count > 0 && digest <= previous) {
			return 0, q.budget.deny()
		}
		var v domain.AttemptEffectReconciliationEvidenceV1
		if decodeOwnedEffectCanonicalJSON(raw, &v) != nil || v.ValidateForPersistedAuthority(parent.Authority, parent.Reservation) != nil || digest != string(v.EvidenceDigest) || run != string(parent.Scope.RunID) || claim != v.PhysicalInvocationClaimID || state != string(v.Observation.State) || !at.Equal(v.Observation.ObservedAt) || !ownedEffectTime(at) || at.Before(parent.Reservation.ReservedAt) || at.After(p.terminal.Run.TerminalCustody.LocalEvidence.StoppedAt) || v.Observation.State == domain.SubstrateOperationUnknownV1 {
			return 0, q.budget.deny()
		}
		previous = digest
		count++
	}
	if rows.Err() != nil || rows.Close() != nil {
		return 0, q.budget.deny()
	}
	return count, nil
}

func (v retainedDeletionWriterData) matchesCapsule(c localRetainedTerminalCapsuleV1) bool {
	if len(v.operations) == 0 || len(v.operations) != len(c.Operations) {
		return false
	}
	for i, row := range v.operations {
		if row.observation == nil || !ownedObservationAllowsSuccessor(row.reservation, *row.observation) || !reflect.DeepEqual(row.reservation, c.Operations[i].Reservation) || !reflect.DeepEqual(*row.observation, c.Operations[i].Observation) {
			return false
		}
	}
	return true
}
