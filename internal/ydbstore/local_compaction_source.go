package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// These reads retain the admission's EXACT pinned window. A bounded cell and
// a sentinel row detect overflow; neither fallback nor partial history is valid.
const localCompactionSnapshotSQL = `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM session_snapshots
 WHERE tenant_id=$1 AND session_id=$2 AND version=$3`

// YDB Substring's length is Uint32, while the shared SQL argument bridge binds
// Go uint64 bounds as Uint64. The producer caps this value at 8193 before SQL;
// keep the cast at the physical query rather than relying on implicit narrowing.
const localCompactionEventsSQL = `SELECT SUBSTRING(CAST(record AS String),0,CAST($6 AS Uint32)) FROM session_events
 WHERE tenant_id=$1 AND session_id=$2 AND sequence>$3 AND sequence<=$4
 ORDER BY sequence ASC LIMIT $5`

// The complete <=12-operation inventory and exact call-index correlation cost
// ONE statement, not N nested parent/history joins. Row 13 is always overflow.
const localCompactionOperationsSQL = `SELECT o.sequence,
 SUBSTRING(o.record,0,32769),SUBSTRING(CAST(o.digest AS String),0,65),
 SUBSTRING(CAST(o.operation_digest AS String),0,65),SUBSTRING(CAST(o.call_id AS String),0,161),
 CASE WHEN o.observation_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN o.observation_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 COALESCE(SUBSTRING(o.observation_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(o.observation_digest AS String),0,65),CAST('' AS String)),
 COALESCE(c.sequence,CAST(0 AS Uint64)),
 COALESCE(SUBSTRING(CAST(c.operation_digest AS String),0,65),CAST('' AS String))
 FROM attempt_operations_v1 AS o LEFT JOIN attempt_operation_calls_v1 AS c
 ON o.tenant_id=c.tenant_id AND o.attempt_id=c.attempt_id AND o.call_id=c.call_id
 WHERE o.tenant_id=$1 AND o.attempt_id=$2 ORDER BY o.sequence ASC LIMIT 13`

const localCompactionProxiesSQL = `SELECT sequence,
 CASE WHEN original_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN original_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN current_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN current_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN policy_body IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN attestation_body IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 COALESCE(SUBSTRING(original_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(original_digest AS String),0,65),CAST('' AS String)),
 COALESCE(SUBSTRING(current_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(current_digest AS String),0,65),CAST('' AS String)),
 COALESCE(SUBSTRING(policy_body,0,16385),CAST('' AS String)),
 COALESCE(SUBSTRING(attestation_body,0,16385),CAST('' AS String)),
 SUBSTRING(CAST(service_kind AS String),0,33)
 FROM attempt_operation_proxies_v1 WHERE tenant_id=$1 AND attempt_id=$2
 ORDER BY sequence ASC LIMIT 13`

// LocalCompactionOperationV1 exposes historical DATA, never a send grant or
// canonical event. Request/Response are independent caller-owned copies.
type LocalCompactionOperationV1 struct {
	Reservation       domain.AttemptOperationReservationV1
	Observation       domain.AttemptOperationObservationV1
	Request, Response []byte
}

type localCompactionSnapshot struct {
	Job          ports.WorkerJobState
	Input        domain.SessionContextInput
	Accounting   ownedAttemptAccounting
	Operations   []ownedOperationRow
	ProxyDigests []domain.RegisteredProxyAttestationDigestV2
}

type localCompactionSourceState struct {
	mu         sync.Mutex
	cleared    bool
	view       *compactioncontext.ViewV1
	operations []LocalCompactionOperationV1
	snapshot   localCompactionSnapshot
	digest     string
}

// LocalCompactionSourceV1 can only originate from the original invocation's
// Store and BlobStore. Its digest/accessors remain DATA, not head publication,
// semantic summary fidelity, token fit or permission to send another request.
type LocalCompactionSourceV1 struct {
	self  *LocalCompactionSourceV1
	entry *LocalInvocationEntryV1
	state *localCompactionSourceState
}

func (*LocalCompactionSourceV1) String() string               { return "LocalCompactionSourceV1{private:[redacted]}" }
func (s *LocalCompactionSourceV1) GoString() string           { return s.String() }
func (s *LocalCompactionSourceV1) Format(f fmt.State, _ rune) { io.WriteString(f, s.String()) }
func (*LocalCompactionSourceV1) MarshalJSON() ([]byte, error) {
	return []byte(`{"kind":"local_compaction_source","private":"[redacted]"}`), nil
}
func (LocalCompactionOperationV1) String() string {
	return "LocalCompactionOperationV1{private:[redacted]}"
}
func (v LocalCompactionOperationV1) GoString() string           { return v.String() }
func (v LocalCompactionOperationV1) Format(f fmt.State, _ rune) { io.WriteString(f, v.String()) }
func (LocalCompactionOperationV1) MarshalJSON() ([]byte, error) {
	return []byte(`{"kind":"local_compaction_operation","private":"[redacted]"}`), nil
}

func (s *LocalCompactionSourceV1) Digest() string {
	if s == nil || s.state == nil {
		return ""
	}
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.state.cleared {
		return ""
	}
	return s.state.digest
}
func (s *LocalCompactionSourceV1) Records() []sessioncontext.EventPayload {
	if s == nil || s.state == nil {
		return nil
	}
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.state.cleared {
		return nil
	}
	return s.state.view.Records()
}
func (s *LocalCompactionSourceV1) Units() []compactioncontext.UnitV1 {
	if s == nil || s.state == nil {
		return nil
	}
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.state.cleared {
		return nil
	}
	return s.state.view.Units()
}
func (s *LocalCompactionSourceV1) Operations() []LocalCompactionOperationV1 {
	if s == nil || s.state == nil {
		return nil
	}
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.state.cleared {
		return nil
	}
	result := make([]LocalCompactionOperationV1, len(s.state.operations))
	for i, op := range s.state.operations {
		result[i] = LocalCompactionOperationV1{Reservation: op.Reservation.Clone(), Observation: op.Observation.Clone(), Request: bytes.Clone(op.Request), Response: bytes.Clone(op.Response)}
	}
	return result
}
func (s *LocalCompactionSourceV1) Clear() {
	if s == nil || s.state == nil {
		return
	}
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.state.cleared {
		return
	}
	s.state.cleared = true
	if s.state.view != nil {
		s.state.view.Clear()
	}
	for i := range s.state.operations {
		clear(s.state.operations[i].Request)
		clear(s.state.operations[i].Response)
	}
	s.state.operations = nil
	s.state.snapshot = localCompactionSnapshot{}
	s.state.digest = ""
}

// CaptureCompactionSource takes no caller reader, history, scope or approval.
// SQL metadata is captured, actual original immutable bytes are verified outside
// SQL, then ALL captured metadata/frontier is rejoined before returning.
func (entry *LocalInvocationEntryV1) CaptureCompactionSource(ctx context.Context) (*LocalCompactionSourceV1, error) {
	return entry.captureCompactionSource(ctx, localSourceStandalone)
}

type localSourceCapturePurpose uint8

const (
	localSourceStandalone localSourceCapturePurpose = iota
	localSourcePreparation
)

// Preparation's original factory owns the two subsequent current joins. Its
// materialized source is private until that factory completes; sharing this
// first read avoids a fourth callback or a reset of the logical-call budget.
func (entry *LocalInvocationEntryV1) captureCompactionSource(ctx context.Context, purpose localSourceCapturePurpose) (*LocalCompactionSourceV1, error) {
	if purpose != localSourceStandalone && purpose != localSourcePreparation {
		return nil, domain.ErrMembershipDenied
	}
	if !entry.original() || entry.canonicalBlobs == nil {
		return nil, domain.ErrMembershipDenied
	}
	control := entry.compactionSourceControl()
	call, finish, err := entry.retainedContext(ctx, control)
	if err != nil {
		return nil, err
	}
	defer finish()
	call, cancel := ownedAuthorityCallContext(call)
	defer cancel()
	var snapshot localCompactionSnapshot
	err = entry.owner.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		var e error
		snapshot, e = entry.readCompactionSnapshot(ctx, q)
		return e
	})
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	state := &localCompactionSourceState{snapshot: snapshot}
	result := &LocalCompactionSourceV1{entry: entry, state: state}
	result.self = result
	valid := false
	defer func() {
		if !valid {
			result.Clear()
		}
	}()
	view, err := entry.materializeCompactionCanonical(call, snapshot)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	state.view = view
	for _, row := range snapshot.Operations {
		op, o := row.reservation.Operation, *row.observation
		// The current base has no real local MCP/search normalized producer.
		// Their semantic continuation is deliberately not guessed from fixtures.
		if op.Kind != domain.AttemptOperationModelV1 && op.Kind != domain.AttemptOperationCompactionV1 {
			return nil, domain.ErrMembershipDenied
		}
		if chargeOwnedAuthorityBody(call, op.RequestBytes+o.ResponseBytes) != nil {
			return nil, domain.ErrMembershipDenied
		}
		request, e := entry.owner.store.readOwnedOperationBody(call, op.Scope, op.RequestRef, op.RequestDigest, op.RequestBytes, 1<<20)
		if e != nil {
			return nil, domain.ErrMembershipDenied
		}
		response, e := entry.owner.store.readOwnedOperationBody(call, op.Scope, o.ResponseRef, o.ResponseDigest, o.ResponseBytes, min(op.ResultBytes, 64<<10))
		if e != nil {
			clear(request)
			return nil, domain.ErrMembershipDenied
		}
		state.operations = append(state.operations, LocalCompactionOperationV1{Reservation: row.reservation.Clone(), Observation: o.Clone(), Request: request, Response: response})
		decoded, e := configuredinference.DecodeNormalizedResponseBodyV1(entry.owner.configuration, request, response, op.Kind)
		pendingCall := decoded.Call != nil
		if decoded.Call != nil {
			clear(decoded.Call.Arguments)
		}
		usage, ue := decoded.Usage.OperationUsage()
		if e != nil || ue != nil || pendingCall || !reflect.DeepEqual(usage, o.Usage) {
			return nil, domain.ErrMembershipDenied
		}
	}
	state.digest, err = entry.compactionSourceDigest(snapshot, view.Digest())
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	if purpose == localSourceStandalone {
		err = entry.owner.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error { return result.rejoinLocked(ctx, q, nil, nil) })
	}
	if err != nil || call.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	valid = true
	return result, nil
}

func (entry *LocalInvocationEntryV1) compactionSourceControl() domain.AttemptOperationV1 {
	return domain.AttemptOperationV1{Scope: ownedOperationScope(entry.parent), Deadline: entry.allocation.Window.ExpiresAt}
}

func (entry *LocalInvocationEntryV1) readCompactionSnapshot(ctx context.Context, q *ownedAuthorityQuery) (localCompactionSnapshot, error) {
	return entry.readLocalPreparedSnapshot(ctx, q, localSnapshotOwner{}, localCompactionCapture)
}

func (entry *LocalInvocationEntryV1) readLocalPreparedSnapshot(ctx context.Context, q *ownedAuthorityQuery, owner localSnapshotOwner, phase localCompactionGenerationPhase) (localCompactionSnapshot, error) {
	if phase == localCompactionCapture {
		if owner.generation != nil || owner.model != nil {
			return localCompactionSnapshot{}, q.budget.deny()
		}
	} else if !owner.original(entry, phase) {
		return localCompactionSnapshot{}, q.budget.deny()
	}
	var v localCompactionSnapshot
	deny := func() (localCompactionSnapshot, error) { return localCompactionSnapshot{}, q.budget.deny() }
	if q.metadataCache == nil {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
	}
	if _, err := entry.currentRetainedRun(ctx, q, entry.compactionSourceControl()); err != nil {
		return deny()
	}
	job, err := loadOwnedEffectWorkerJob(ctx, q, operationParentRequest(ownedOperationScope(entry.parent)))
	if err != nil {
		return deny()
	}
	v.Job = job
	w := job.Job.ContextWindow
	policy := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if w == nil || w.Validate() != nil || w.ThroughSequence > min(job.Job.Limits.EffectiveMaxContextEvents(), 4096) || policy == nil {
		return deny()
	}
	v.Input = domain.SessionContextInput{TenantID: job.Job.TenantID, SessionID: job.Job.SessionID}
	if w.SnapshotVersion != nil {
		var raw string
		if q.scanMetadata(ctx, localCompactionSnapshotSQL, []any{job.Job.TenantID, job.Job.SessionID, *w.SnapshotVersion}, &raw) != nil || len(raw) > 8192 {
			return deny()
		}
		var snapshot domain.SessionSnapshot
		if decodeOwnedEffectCanonicalJSON(raw, &snapshot) != nil || snapshot.Validate() != nil || snapshot.TenantID != job.Job.TenantID || snapshot.SessionID != job.Job.SessionID || snapshot.Version != *w.SnapshotVersion || snapshot.ThroughSequence != w.AfterSequence || snapshot.CreatedAt.After(q.at) {
			return deny()
		}
		v.Input.Snapshot = &snapshot
	}
	// Bound the entire range's possible transferred metadata to <=1 MiB,
	// including its sentinel. No truncated cell is ever accepted. Large valid
	// envelopes may honestly fail the SAME logical 4-MiB source budget.
	rowLimit := w.ThroughSequence - w.AfterSequence + 1
	cellLimit := min(uint64(8192), (ownedAuthorityMaxBytes/4)/rowLimit-1)
	rows, err := q.QueryContext(ctx, localCompactionEventsSQL, job.Job.TenantID, job.Job.SessionID, w.AfterSequence, w.ThroughSequence, rowLimit, cellLimit+1)
	if err != nil {
		return deny()
	}
	for rows.Next() {
		var raw string
		n, e := scanOwnedCurrentRow(rows, &raw)
		if q.budget.materialized(q.callback, n) != nil || e != nil || uint64(len(raw)) > cellLimit || uint64(len(v.Input.Events)) >= w.ThroughSequence-w.AfterSequence {
			rows.Close()
			return deny()
		}
		var event domain.SessionEvent
		if decodeOwnedEffectCanonicalJSON(raw, &event) != nil || event.Validate() != nil || event.TenantID != job.Job.TenantID || event.SessionID != job.Job.SessionID || event.Sequence != w.AfterSequence+uint64(len(v.Input.Events))+1 || event.CreatedAt.After(q.at) {
			rows.Close()
			return deny()
		}
		v.Input.Events = append(v.Input.Events, event)
	}
	rowErr, closeErr := rows.Err(), rows.Close()
	if rowErr != nil || closeErr != nil || uint64(len(v.Input.Events)) != w.ThroughSequence-w.AfterSequence || v.Input.Validate() != nil {
		return deny()
	}
	accounting, err := readOwnedAttemptAccounting(ctx, q, entry.parent, entry.source)
	if err != nil || accounting == nil {
		return deny()
	}
	v.Accounting = *accounting
	v.Operations, err = entry.readLocalPreparedOperations(ctx, q, *accounting, owner, phase)
	if err != nil {
		return deny()
	}
	v.ProxyDigests, err = entry.readLocalPreparedProxies(ctx, q, v.Operations, owner, phase)
	if err != nil {
		return deny()
	}
	return v, nil
}

func (entry *LocalInvocationEntryV1) readLocalPreparedOperations(ctx context.Context, q *ownedAuthorityQuery, accounting ownedAttemptAccounting, owner localSnapshotOwner, phase localCompactionGenerationPhase) ([]ownedOperationRow, error) {
	scope := ownedOperationScope(entry.parent)
	rows, err := q.QueryContext(ctx, localCompactionOperationsSQL, scope.TenantID, scope.AttemptID)
	if err != nil {
		return nil, q.budget.deny()
	}
	defer rows.Close()
	var result []ownedOperationRow
	baseline := ownedAttemptAccounting{Version: 1, ParentDigest: accounting.ParentDigest, SetupReservedAt: accounting.SetupReservedAt, UpdatedAt: accounting.SetupReservedAt}
	baseline.Amounts[1] = *entry.source.Spending[1].MaxAmountMicrounits
	for _, cost := range []*domain.ExternalServiceCostCeilingV1{entry.parent.Authority.AdmissionCostCeiling.MCPToolCost, entry.parent.Authority.AdmissionCostCeiling.WebSearchCost} {
		if cost != nil {
			baseline.Aggregate.BodyBytes += 4 << 10
		}
	}
	var predecessor domain.AttemptOperationObservationDigestV1
	for rows.Next() {
		var columns ownedOperationColumns
		var sequence, indexedSequence uint64
		var indexedDigest string
		dest := append([]any{&sequence}, columns.destinations()...)
		dest = append(dest, &indexedSequence, &indexedDigest)
		n, e := scanOwnedCurrentRow(rows, dest...)
		if q.budget.materialized(q.callback, n) != nil || e != nil || len(result) >= 12 || sequence != uint64(len(result))+1 || sequence > accounting.Aggregate.Operations {
			return nil, q.budget.deny()
		}
		row, e := columns.decode(q, scope, sequence)
		if e != nil || row == nil || row.reservation.Operation.Scope != scope || row.reservation.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil || indexedSequence != sequence || indexedDigest != columns.operationDigest || row.reservation.Operation.PredecessorObservationDigest != predecessor || row.reservation.ReservedAt.Before(baseline.UpdatedAt) {
			return nil, q.budget.deny()
		}
		pending := owner.present() && phase == localCompactionBeforeSend && sequence == owner.operation().Sequence
		if pending {
			if row.observation != nil || !reflect.DeepEqual(row.reservation.Operation, owner.operation()) {
				return nil, q.budget.deny()
			}
		} else if row.observation == nil || !ownedObservationAllowsSuccessor(row.reservation, *row.observation) {
			return nil, q.budget.deny()
		}
		if len(result) > 0 {
			previous := result[len(result)-1].observation
			if previous == nil || row.reservation.ReservedAt.Before(previous.ObservedAt) {
				return nil, q.budget.deny()
			}
		}
		baseline, e = debitOwnedOperation(baseline, row.reservation.Operation, entry.parent, entry.source, row.reservation.ReservedAt)
		if e != nil || baseline.Aggregate != row.reservation.AggregateDebit || baseline.Amounts != row.reservation.ComponentAmounts {
			return nil, q.budget.deny()
		}
		if row.observation != nil {
			predecessor, _ = row.observation.Digest()
		}
		result = append(result, *row)
	}
	if rows.Err() != nil || rows.Close() != nil || uint64(len(result)) != accounting.Aggregate.Operations || !reflect.DeepEqual(baseline, accounting) {
		return nil, q.budget.deny()
	}
	return result, nil
}

func (entry *LocalInvocationEntryV1) readLocalPreparedProxies(ctx context.Context, q *ownedAuthorityQuery, operations []ownedOperationRow, owner localSnapshotOwner, phase localCompactionGenerationPhase) ([]domain.RegisteredProxyAttestationDigestV2, error) {
	var result []domain.RegisteredProxyAttestationDigestV2
	future := owner.present() && phase == localCompactionReserve
	count := len(operations)
	if future {
		count++
	}
	p, err := readOwnedProxySnapshotAt(ctx, q, entry.parent, entry.source, entry.allocation, 1)
	if err != nil || (count == 0 && p != nil) {
		return nil, q.budget.deny()
	}
	if count > 0 {
		if err != nil || p == nil {
			return nil, q.budget.deny()
		}
		var d domain.RegisteredProxyAttestationDigestV2
		if len(operations) == 0 {
			d, err = owner.futureProxyDigest(q, p)
		} else {
			if owner.present() && owner.operation().Sequence == 1 {
				if _, err := owner.futureProxyDigest(q, p); err != nil {
					return nil, err
				}
			}
			d, err = entry.compactionProxyDigest(p, operations[0])
		}
		if err != nil {
			return nil, q.budget.deny()
		}
		result = append(result, d)
	}
	rows, err := q.QueryContext(ctx, localCompactionProxiesSQL, entry.parent.Scope.TenantID, entry.parent.Reservation.AttemptID)
	if err != nil {
		return nil, q.budget.deny()
	}
	defer rows.Close()
	for rows.Next() {
		var sequence uint64
		var columns ownedProxyColumns
		dest := append([]any{&sequence}, columns.destinations(2)...)
		n, e := scanOwnedCurrentRow(rows, dest...)
		if q.budget.materialized(q.callback, n) != nil || e != nil || sequence != uint64(len(result))+1 || sequence <= 1 || sequence > uint64(count) {
			return nil, q.budget.deny()
		}
		p, e := columns.decode(q, entry.parent, entry.source, entry.allocation, sequence)
		if e != nil || p == nil {
			return nil, q.budget.deny()
		}
		var d domain.RegisteredProxyAttestationDigestV2
		if future && sequence == owner.operation().Sequence {
			d, e = owner.futureProxyDigest(q, p)
		} else {
			if owner.present() && sequence == owner.operation().Sequence {
				if _, err := owner.futureProxyDigest(q, p); err != nil {
					return nil, err
				}
			}
			d, e = entry.compactionProxyDigest(p, operations[sequence-1])
		}
		if e != nil {
			return nil, q.budget.deny()
		}
		result = append(result, d)
	}
	if rows.Err() != nil || rows.Close() != nil || len(result) != count {
		return nil, q.budget.deny()
	}
	return result, nil
}

func (entry *LocalInvocationEntryV1) compactionProxyDigest(p *ownedProxySnapshot, row ownedOperationRow) (domain.RegisteredProxyAttestationDigestV2, error) {
	d, e := p.original.Digest()
	ad, ae := entry.allocation.Digest()
	if e != nil || ae != nil || row.reservation.RegisteredAllocationDigest != ad || row.reservation.RegisteredProxyDigest != d || p.original.OperationDigest != row.reservation.OperationDigest || p.original.OperationSequence != row.reservation.Operation.Sequence || p.original.ServiceKind != ownedOperationProxyService(row.reservation.Operation.Kind) {
		return "", domain.ErrMembershipDenied
	}
	return d, nil
}

func (entry *LocalInvocationEntryV1) materializeCompactionCanonical(ctx context.Context, snapshot localCompactionSnapshot) (*compactioncontext.ViewV1, error) {
	job := snapshot.Job.Job
	limit := min(job.Limits.MaxContextBytes, entry.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxContextViewBytes, 1<<20)
	proof := &sessioncontext.CanonicalProof{Input: snapshot.Input}
	defer proof.Clear()
	var err error
	if proof.Input.Snapshot != nil {
		proof.SnapshotBytes, err = entry.readCompactionBlob(ctx, proof.Input.Snapshot.Payload, limit)
		if err != nil {
			return nil, err
		}
	}
	for _, event := range proof.Input.Events {
		body, e := entry.readCompactionBlob(ctx, event.Payload, limit)
		if e != nil {
			return nil, e
		}
		proof.EventBodies = append(proof.EventBodies, body)
	}
	history, refs, err := sessioncontext.ProjectCanonical(job, proof, limit)
	if err != nil {
		return nil, err
	}
	defer clear(history)
	for _, ref := range refs {
		body, e := entry.readCompactionBlob(ctx, ref.Blob, limit)
		if e != nil {
			return nil, e
		}
		proof.Attachments = append(proof.Attachments, sessioncontext.AttachmentPayload{AttachmentRef: ref, Body: body})
	}
	if sessioncontext.VerifyCanonicalAttachments(history, refs, proof, limit) != nil {
		return nil, domain.ErrMembershipDenied
	}
	records, err := sessioncontext.DecodeJSONL(history, job.TenantID, job.SessionID)
	if err != nil {
		return nil, err
	}
	defer func() {
		for i := range records {
			clear(records[i].Payload)
		}
	}()
	return compactioncontext.BuildCanonicalV1(records, limit, min(job.Limits.EffectiveMaxContextEvents(), 4096))
}

func (entry *LocalInvocationEntryV1) readCompactionBlob(ctx context.Context, ref domain.BlobRef, cap uint64) ([]byte, error) {
	if ctx.Err() != nil || ref.Validate() != nil || ref.TenantID != entry.parent.Scope.TenantID || ref.Size <= 0 || uint64(ref.Size) > cap || cap > 1<<20 || chargeOwnedAuthorityBody(ctx, uint64(ref.Size)+1) != nil {
		return nil, domain.ErrMembershipDenied
	}
	r, err := entry.canonicalBlobs.Open(ctx, ref.TenantID, ref)
	if err != nil || r == nil {
		if r != nil {
			_ = r.Close()
		}
		return nil, domain.ErrMembershipDenied
	}
	body, readErr := io.ReadAll(io.LimitReader(r, ref.Size+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || ctx.Err() != nil || !localRetainedBytes(ref, body) {
		clear(body)
		return nil, domain.ErrMembershipDenied
	}
	return body, nil
}

func (entry *LocalInvocationEntryV1) compactionSourceDigest(snapshot localCompactionSnapshot, canonical string) (string, error) {
	// Explicit typed preimage includes original scope/policy/instruction/model,
	// complete Run metadata, job/manifest, exact event references and full durable
	// reservation/observation/proxy frontier. No caller's digest is accepted.
	raw, err := json.Marshal(struct {
		Version      uint32
		Scope        domain.AttemptOperationScopeV1
		Policy       *domain.AgentLoopPolicyV1
		Profile      domain.ConfiguredInferenceProfileV1
		Job          ports.WorkerJobState
		Input        domain.SessionContextInput
		Accounting   ownedAttemptAccounting
		Reservations []domain.AttemptOperationReservationV1
		Observations []domain.AttemptOperationObservationV1
		Proxies      []domain.RegisteredProxyAttestationDigestV2
		Canonical    string
	}{Version: 1, Scope: ownedOperationScope(entry.parent), Policy: entry.parent.Authority.HarnessBinding.AgentLoopPolicy, Profile: entry.owner.configuration, Job: snapshot.Job, Input: snapshot.Input, Accounting: snapshot.Accounting, Reservations: compactionReservations(snapshot.Operations), Observations: compactionObservations(snapshot.Operations), Proxies: snapshot.ProxyDigests, Canonical: canonical})
	if err != nil {
		return "", domain.ErrMembershipDenied
	}
	defer clear(raw)
	h := sha256.New()
	h.Write([]byte("sessionless.local-compaction-source.v1\x00"))
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil)), nil
}
func compactionReservations(rows []ownedOperationRow) []domain.AttemptOperationReservationV1 {
	v := make([]domain.AttemptOperationReservationV1, len(rows))
	for i, row := range rows {
		v[i] = row.reservation
	}
	return v
}
func compactionObservations(rows []ownedOperationRow) []domain.AttemptOperationObservationV1 {
	v := make([]domain.AttemptOperationObservationV1, len(rows))
	for i, row := range rows {
		if row.observation != nil {
			v[i] = *row.observation
		}
	}
	return v
}

// Caller holds the shared source mutex once it has escaped Capture.
func (s *LocalCompactionSourceV1) rejoinLocked(ctx context.Context, q *ownedAuthorityQuery, appended *domain.AttemptOperationReservationV1, observed *domain.AttemptOperationObservationV1) error {
	_, err := s.rejoinRunLocked(ctx, q, appended, observed)
	return err
}

// The source-bound uploader consumes this SAME joined Run and full inventory,
// rather than performing another nested parent/operationHistory point path.
func (s *LocalCompactionSourceV1) rejoinRunLocked(ctx context.Context, q *ownedAuthorityQuery, appended *domain.AttemptOperationReservationV1, observed *domain.AttemptOperationObservationV1) (domain.Run, error) {
	if s == nil || s.self != s || s.entry == nil || !s.entry.original() || s.state == nil || s.state.cleared {
		return domain.Run{}, q.budget.deny()
	}
	current, err := s.entry.readCompactionSnapshot(ctx, q)
	if err != nil {
		return domain.Run{}, q.budget.deny()
	}
	expected := s.state.snapshot
	if appended != nil {
		if observed == nil || appended.Operation.Kind != domain.AttemptOperationCompactionV1 || appended.Operation.Sequence != expected.Accounting.Aggregate.Operations+1 || len(current.Operations) != len(expected.Operations)+1 || !reflect.DeepEqual(current.Operations[len(current.Operations)-1].reservation, *appended) || !reflect.DeepEqual(*current.Operations[len(current.Operations)-1].observation, *observed) {
			return domain.Run{}, q.budget.deny()
		}
		current.Operations = current.Operations[:len(current.Operations)-1]
		current.ProxyDigests = current.ProxyDigests[:len(current.ProxyDigests)-1]
		if len(expected.Operations) == 0 {
			current.Operations = nil
			current.ProxyDigests = nil
		}
		// Replay the original frozen debit; the complete current accounting was
		// independently checked against ALL durable operations above.
		next, e := debitOwnedOperation(expected.Accounting, appended.Operation, s.entry.parent, s.entry.source, appended.ReservedAt)
		if e != nil || !reflect.DeepEqual(next, current.Accounting) {
			return domain.Run{}, q.budget.deny()
		}
		current.Accounting = expected.Accounting
	}
	if !reflect.DeepEqual(current, expected) {
		return domain.Run{}, q.budget.deny()
	}
	return current.Job.Run.Clone(), nil
}

func (s *LocalCompactionSourceV1) PrepareRetainedCompaction(ctx context.Context, op domain.AttemptOperationV1, generation uint64) (*LocalRetainedCompactionV1, error) {
	if s == nil || s.self != s || s.state == nil || s.entry == nil {
		return nil, domain.ErrMembershipDenied
	}
	unlock, err := s.entry.beginLocalMutation(ctx)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	defer unlock()
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.state.cleared || op.Scope != ownedOperationScope(s.entry.parent) || op.Sequence != s.state.snapshot.Accounting.Aggregate.Operations+1 {
		return nil, domain.ErrMembershipDenied
	}
	call, finish, err := s.entry.retainedContext(ctx, op)
	if err != nil {
		return nil, err
	}
	defer finish()
	call, cancel := ownedAuthorityCallContext(call)
	defer cancel()
	if s.entry.owner.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error { return s.rejoinLocked(ctx, q, nil, nil) }) != nil {
		return nil, domain.ErrMembershipDenied
	}
	h, err := s.entry.prepareRetainedCompactionLocked(call, op, generation)
	if err != nil {
		return nil, err
	}
	h.source = s
	return h, nil
}
