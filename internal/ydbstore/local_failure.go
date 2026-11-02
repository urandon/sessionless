package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const localKnownFailureCapture localCompactionGenerationPhase = 255

// LocalFailureV1 is a distinct original private owner. Matching JSON/digests
// cannot recreate it, select success, dispatch, renew authority or claim erasure.
type LocalFailureV1 struct {
	self *LocalFailureV1
	*localFailureState
}
type localFailureState struct {
	entry                                                                                        *LocalInvocationEntryV1
	source                                                                                       *LocalCompactionSourceV1
	terminal                                                                                     ports.AuthenticatedAttemptOperationObservationV1
	version                                                                                      uint32
	deadline                                                                                     time.Time
	capturedAt                                                                                   time.Time
	gate                                                                                         chan struct{}
	mu                                                                                           sync.Mutex
	closed, prepareAttempted, prepared, publishAttempted, registered, published, commitAttempted bool
	failed                                                                                       atomic.Bool
	cancel                                                                                       context.CancelFunc
	snapshot                                                                                     localCompactionSnapshot
	ops                                                                                          []LocalCompactionOperationV1
	run                                                                                          domain.Run
	accounting                                                                                   ports.OwnedOperationFailureAccountingV1
	plan                                                                                         domain.TerminalCustodyPlanV1
	metadata                                                                                     domain.RunTerminalFailureV1
	canonical                                                                                    *canonicaloutput.FailurePlanV1
	drafts                                                                                       []domain.SessionEventDraft
	capsule                                                                                      []byte
	retained                                                                                     *operationbody.RetainedNamespaceV1
	retired                                                                                      ports.OwnedAllocationRegistrationReceiptV2
}

func (h *LocalFailureV1) original() bool {
	return h != nil && h.self == h && h.localFailureState != nil && h.entry.original() && h.entry.failure != nil && h.entry.failure.Load() == h && h.entry.terminalSelection != nil && h.entry.terminalSelection.Load() == 2 && h.entry.terminal.Load() == nil
}
func (LocalFailureV1) MarshalJSON() ([]byte, error) { return nil, domain.ErrMembershipDenied }
func (LocalFailureV1) String() string               { return "LocalFailureV1{private:[redacted]}" }
func (h LocalFailureV1) GoString() string           { return h.String() }

// ClaimFailure latches the original effect gate BEFORE session teardown.
// It accepts no error DTO/code/frontier. Actual durable rows are independently
// rejoined later; this private claim alone proves no terminal outcome.
func (e *LocalInvocationEntryV1) ClaimFailure() (*LocalFailureV1, error) {
	if !e.original() || e.failure == nil || e.terminalSelection == nil {
		return nil, domain.ErrMembershipDenied
	}
	// Prefer an actually reserved/authenticated UNKNOWN MODEL tail. It cannot
	// be replaced with the preceding successful MCP merely to finish a prefix.
	if m := e.preparedModel.Load(); m != nil && m.self == m && m.entry == e && m.source != nil && m.source.state != nil {
		m.source.state.mu.Lock()
		o := m.observation
		valid := !m.source.state.cleared && o.Validate() == nil && reflect.DeepEqual(o.Reservation, m.reservation) && o.Observation.Outcome == domain.AttemptOperationAcceptedOutcomeUnknownV1 && ports.FailureModelUnknownReasonV2(o.Observation.Reason) && o.Observation.ResponseRef == "" && o.Observation.ResponseDigest == "" && o.Observation.ResponseBytes == 0 && e.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(o.Clone()) == nil
		knownTool := false
		for _, op := range m.source.state.operations {
			knownTool = knownTool || op.Reservation.Operation.Kind == domain.AttemptOperationMCPToolV1 && op.Observation.Outcome == domain.AttemptOperationCompletedV1 && op.Observation.Reason == domain.AttemptOperationReasonNoneV1
		}
		if valid && knownTool {
			h, err := e.claimFailureHolder(m.source, o, 2)
			m.source.state.mu.Unlock()
			return h, err
		}
		m.source.state.mu.Unlock()
	}
	m := e.preparedMCP.Load()
	if m == nil || m.self != m || m.entry != e || m.source == nil || m.source.state == nil {
		return nil, domain.ErrMembershipDenied
	}
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	o := m.observation
	if o.Validate() != nil || !reflect.DeepEqual(o.Reservation, m.reservation) || o.Observation.Outcome != domain.AttemptOperationCompletedV1 || o.Observation.Reason != domain.AttemptOperationReasonServiceErrorV1 || e.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(o.Clone()) != nil {
		return nil, domain.ErrMembershipDenied
	}
	return e.claimFailureHolder(m.source, o, 1)
}

// Only ClaimFailure's two original private holders call this after their
// issuer-authenticated observation is retained under the original source gate.
func (e *LocalInvocationEntryV1) claimFailureHolder(source *LocalCompactionSourceV1, terminal ports.AuthenticatedAttemptOperationObservationV1, version uint32) (*LocalFailureV1, error) {
	p := e.parent.Authority.HarnessBinding.AgentLoopPolicy
	if p == nil {
		return nil, domain.ErrMembershipDenied
	}
	duration := min(p.Limits.MaxCleanupDuration, e.owner.configuration.Limits.CleanupTimeout, 30*time.Second)
	if duration <= 0 || !e.terminalSelection.CompareAndSwap(0, 2) {
		return nil, domain.ErrMembershipDenied
	}
	h := &LocalFailureV1{localFailureState: &localFailureState{entry: e, source: source, terminal: terminal.Clone(), version: version, deadline: time.Now().Add(duration), gate: make(chan struct{}, 1)}}
	h.self = h
	e.failure.Store(h)
	return h, nil
}
func (h *LocalFailureV1) Deadline() (time.Time, error) {
	if !h.original() {
		return time.Time{}, domain.ErrMembershipDenied
	}
	return h.deadline, nil
}

func (h *LocalFailureV1) begin(ctx context.Context) (context.Context, func(), error) {
	if !h.original() || ctx == nil || ctx.Err() != nil {
		return nil, nil, domain.ErrMembershipDenied
	}
	select {
	case h.gate <- struct{}{}:
	default:
		return nil, nil, domain.ErrMembershipDenied
	}
	h.mu.Lock()
	if h.closed || h.failed.Load() {
		h.mu.Unlock()
		<-h.gate
		return nil, nil, domain.ErrMembershipDenied
	}
	call, cancel := context.WithDeadline(ctx, h.deadline)
	h.cancel = cancel
	h.mu.Unlock()
	return call, func() { cancel(); h.mu.Lock(); h.cancel = nil; h.mu.Unlock(); <-h.gate }, nil
}

func (h *LocalFailureV1) authenticatedRow(row ownedOperationRow, total uint64) bool {
	if !h.original() || row.observation == nil {
		return false
	}
	r, o := row.reservation, *row.observation
	var original ports.AuthenticatedAttemptOperationObservationV1
	if r.Operation.Sequence == total {
		if !reflect.DeepEqual(r, h.terminal.Reservation) || !reflect.DeepEqual(o, h.terminal.Observation) || (h.version == 1 && (r.Operation.Kind != domain.AttemptOperationMCPToolV1 || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonServiceErrorV1)) || (h.version == 2 && (r.Operation.Kind != domain.AttemptOperationModelV1 || o.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || !ports.FailureModelUnknownReasonV2(o.Reason))) || (h.version != 1 && h.version != 2) {
			return false
		}
		original = h.terminal
	} else if m := h.entry.preparedMCP.Load(); m != nil && m.self == m && m.entry == h.entry && reflect.DeepEqual(m.reservation, r) {
		original = m.observation
	} else if m := h.entry.preparedModel.Load(); m != nil && m.self == m && m.entry == h.entry && reflect.DeepEqual(m.reservation, r) {
		original = m.observation
	} else if g := h.entry.compactionGeneration.Load(); g != nil && g.self == g && g.entry == h.entry && reflect.DeepEqual(g.reservation, r) {
		original = g.observation
	} else if retained, ok := h.entry.historicalOriginalObservation(r, o); ok {
		original = retained
	} else {
		return false
	}
	return original.Validate() == nil && reflect.DeepEqual(original.Reservation, r) && reflect.DeepEqual(original.Observation, o) && h.entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(original.Clone()) == nil
}

// Historical read authority belongs only to this original stop owner and its
// unchanged absolute cleanup deadline. No active grant is read at an old time.
func (h *LocalFailureV1) readSnapshot(ctx context.Context, q *ownedAuthorityQuery) (localCompactionSnapshot, error) {
	deny := func() (localCompactionSnapshot, error) { return localCompactionSnapshot{}, q.budget.deny() }
	d, bounded := ctx.Deadline()
	if !h.original() || !bounded || d.After(h.deadline) || !time.Now().Before(h.deadline) {
		return deny()
	}
	e := h.entry
	parent, source, err := readOwnedAllocationParentData(ctx, q, ownedAllocationParentRequest(e.parent.Reservation))
	if err != nil || !reflect.DeepEqual(parent, e.parent) || !reflect.DeepEqual(source, e.source) {
		_, err := deny()
		return localCompactionSnapshot{}, fmt.Errorf("known MCP failure retained parent: %w", err)
	}
	burned, burn, err := readOwnedPreparationBurnObservation(ctx, q, parent)
	if err != nil || !burned {
		_, err := deny()
		return localCompactionSnapshot{}, fmt.Errorf("known MCP failure original burn: %w", err)
	}
	a, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burn)
	if err != nil || a == nil || !reflect.DeepEqual(a.original, e.allocation) {
		_, err := deny()
		return localCompactionSnapshot{}, fmt.Errorf("known MCP failure allocation: %w", err)
	}
	if h.retired.Current.State != "" && !reflect.DeepEqual(a.current, h.retired.Current) {
		return deny()
	}
	v, err := e.readLocalPreparedSnapshotBody(ctx, q, localSnapshotOwner{}, localKnownFailureCapture)
	if err != nil || !v.Accounting.SetupReservedAt.Equal(burn) || v.Job.Run.Status != domain.RunRunning || v.Job.Run.CancellationRequestedAt != nil || v.Job.Run.TerminalFailure != nil {
		_, denied := deny()
		if err != nil {
			return localCompactionSnapshot{}, fmt.Errorf("known MCP failure current snapshot: %w", err)
		}
		return localCompactionSnapshot{}, fmt.Errorf("known MCP failure current snapshot state: %w", denied)
	}
	if h.registered {
		if v.Job.Run.TerminalCustody == nil || v.Job.Run.TerminalCustody.Plan != h.plan || v.Job.Run.TerminalCustody.Correspondence != nil || v.Job.Run.TerminalCustody.LocalEvidence != nil {
			return deny()
		}
		v.Job.Run.TerminalCustody = nil
	} else if v.Job.Run.TerminalCustody != nil {
		return deny()
	}
	var deletion string
	if err = q.QueryRowContext(ctx, `SELECT state FROM session_deletions WHERE tenant_id = $1 AND session_id = $2`, e.parent.Scope.TenantID, e.parent.Scope.SessionID).Scan(&deletion); err != sql.ErrNoRows {
		_, err := deny()
		return localCompactionSnapshot{}, fmt.Errorf("known MCP failure deletion fence: %w", err)
	}
	return v, nil
}

func (h *LocalFailureV1) capture(ctx context.Context) error {
	e := h.entry
	var snapshot localCompactionSnapshot
	var snapshotErr error // readSnapshot emits only closed content-free denials.
	if err := e.owner.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		snapshot, snapshotErr = h.readSnapshot(ctx, q)
		if snapshotErr == nil {
			h.capturedAt = q.at
		}
		return snapshotErr
	}); err != nil {
		if snapshotErr != nil {
			return snapshotErr
		}
		return fmt.Errorf("known MCP failure snapshot: %w", err)
	}
	if len(snapshot.Operations) < 2 || snapshot.Accounting.Aggregate.Operations != uint64(len(snapshot.Operations)) {
		return domain.ErrMembershipDenied
	}
	// The historical capture is not a refresh of stale DATA. Normalize ONLY
	// the exact authenticated appended MCP/MODEL observation against its actual
	// pre-send source, including original canonical metadata and full Job.
	state := h.source.state
	state.mu.Lock()
	if state.cleared || state.view == nil || len(snapshot.Operations) != len(state.snapshot.Operations)+1 || len(snapshot.ProxyDigests) < 1 {
		state.mu.Unlock()
		return domain.ErrMembershipDenied
	}
	expected := state.snapshot
	priorView := state.view.Digest()
	current := snapshot
	last := current.Operations[len(current.Operations)-1]
	if !reflect.DeepEqual(last.reservation, h.terminal.Reservation) || last.observation == nil || !reflect.DeepEqual(*last.observation, h.terminal.Observation) {
		state.mu.Unlock()
		return domain.ErrMembershipDenied
	}
	debit, de := debitOwnedOperation(expected.Accounting, last.reservation.Operation, e.parent, e.source, last.reservation.ReservedAt)
	current.Operations = current.Operations[:len(current.Operations)-1]
	current.ProxyDigests = current.ProxyDigests[:len(current.ProxyDigests)-1]
	current.Accounting = expected.Accounting
	matched := de == nil && reflect.DeepEqual(debit, snapshot.Accounting) && reflect.DeepEqual(current, expected)
	state.mu.Unlock()
	if !matched {
		return fmt.Errorf("known MCP failure source correspondence: %w", domain.ErrMembershipDenied)
	}
	view, ve := e.materializeCompactionCanonical(ctx, snapshot)
	if ve != nil {
		return ve
	}
	sameView := view.Digest() == priorView
	view.Clear()
	if !sameView {
		return fmt.Errorf("known MCP failure canonical correspondence: %w", domain.ErrMembershipDenied)
	}
	h.snapshot = snapshot
	_, ad, err := accountingBytes(snapshot.Accounting)
	if err != nil {
		return err
	}
	h.accounting = ports.OwnedOperationFailureAccountingV1{Version: h.version, Scope: ownedOperationScope(e.parent), AccountingDigest: ad, AggregateDebit: snapshot.Accounting.Aggregate, ComponentAmounts: snapshot.Accounting.Amounts}
	for _, row := range snapshot.Operations {
		if row.observation == nil {
			return domain.ErrMembershipDenied
		}
		r, o := row.reservation, *row.observation
		if chargeOwnedAuthorityBody(ctx, r.Operation.RequestBytes+o.ResponseBytes) != nil {
			return domain.ErrMembershipDenied
		}
		request, err := e.bodies.ReadAttemptOperationBody(ctx, r.Operation.Scope, r.Operation.RequestRef, min(r.Operation.RequestBytes, 1<<20))
		if err != nil {
			return err
		}
		unknown := h.version == 2 && r.Operation.Sequence == snapshot.Accounting.Aggregate.Operations
		var response []byte
		if !unknown {
			response, err = e.bodies.ReadAttemptOperationBody(ctx, r.Operation.Scope, o.ResponseRef, min(o.ResponseBytes, 64<<10))
			if err != nil {
				clear(request)
				return err
			}
		}
		valid := localFailureBytes(request, r.Operation.RequestBytes, r.Operation.RequestDigest)
		if unknown {
			valid = valid && r.Operation.Kind == domain.AttemptOperationModelV1 && o.Outcome == domain.AttemptOperationAcceptedOutcomeUnknownV1 && ports.FailureModelUnknownReasonV2(o.Reason) && o.ResponseRef == "" && o.ResponseDigest == "" && o.ResponseBytes == 0
		} else {
			valid = valid && localFailureBytes(response, o.ResponseBytes, o.ResponseDigest)
		}
		if r.Operation.Kind != domain.AttemptOperationMCPToolV1 && !unknown {
			decoded, de := configuredinference.DecodeNormalizedResponseBodyV1(e.owner.configuration, request, response, r.Operation.Kind)
			usage, ue := decoded.Usage.OperationUsage()
			if decoded.Call != nil {
				clear(decoded.Call.Arguments)
			}
			valid = valid && de == nil && ue == nil && reflect.DeepEqual(usage, o.Usage)
		} else if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			tool, de := mcpadmission.DecodeSealedToolResultDataV1(r, o, response)
			if len(h.ops) == 0 {
				clear(request)
				clear(response)
				return domain.ErrMembershipDenied
			}
			prev := h.ops[len(h.ops)-1]
			valid = valid && de == nil && tool.IsError() == (h.version == 1 && r.Operation.Sequence == snapshot.Accounting.Aggregate.Operations) && tool.ValidateConfiguredCallV1(e.owner.configuration, prev.Reservation, prev.Observation, prev.Request, prev.Response) == nil
		}
		if !valid {
			clear(request)
			clear(response)
			return fmt.Errorf("known MCP failure operation material: %w", domain.ErrMembershipDenied)
		}
		h.ops = append(h.ops, LocalCompactionOperationV1{Reservation: r.Clone(), Observation: o.Clone(), Request: request, Response: response})
		rowData, err := ports.BuildOwnedOperationFailureRowV1(r, o, e.parent, e.source)
		if h.version == 2 {
			rowData, err = ports.BuildOwnedOperationFailureRowV2(r, o, e.parent, e.source)
		}
		if err != nil {
			return err
		}
		h.accounting.Operations = append(h.accounting.Operations, rowData)
	}
	if err := validateLocalFailureAccounting(h.accounting, e.parent, e.source); err != nil {
		return fmt.Errorf("known MCP failure complete accounting: %w", err)
	}
	return nil
}

func localFailureBytes(body []byte, n uint64, d string) bool {
	s := sha256.Sum256(body)
	return uint64(len(body)) == n && hex.EncodeToString(s[:]) == d
}

// StopAndJoin joins only this owner's synchronous work; it neither claims
// erasure nor closes the borrowed Source, database or inference endpoint.
func (h *LocalFailureV1) StopAndJoin(ctx context.Context) error {
	if !h.original() || ctx == nil {
		return domain.ErrMembershipDenied
	}
	h.mu.Lock()
	h.closed = true
	if h.cancel != nil {
		h.cancel()
	}
	h.mu.Unlock()
	call, cancel := context.WithDeadline(ctx, h.deadline)
	defer cancel()
	select {
	case h.gate <- struct{}{}:
		defer func() { <-h.gate }()
	case <-call.Done():
		select {
		case h.gate <- struct{}{}:
			defer func() { <-h.gate }()
		default:
			return domain.ErrMembershipDenied
		}
	}
	for i := range h.ops {
		clear(h.ops[i].Request)
		clear(h.ops[i].Response)
	}
	h.ops = nil
	clear(h.capsule)
	h.capsule = nil
	if h.canonical != nil {
		h.canonical.Release()
	}
	return nil
}

func (h *LocalFailureV1) retire(ctx context.Context) error {
	e := h.entry
	expected, err := e.allocation.Digest()
	if err != nil {
		return err
	}
	return e.owner.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		parent, source, err := readOwnedAllocationParentData(ctx, q, ownedAllocationParentRequest(e.parent.Reservation))
		if err != nil || !reflect.DeepEqual(parent, e.parent) || !reflect.DeepEqual(source, e.source) {
			return q.budget.deny()
		}
		burned, burn, err := readOwnedPreparationBurnObservation(ctx, q, parent)
		if err != nil || !burned {
			return q.budget.deny()
		}
		a, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burn)
		if err != nil || a == nil || !reflect.DeepEqual(a.original, e.allocation) {
			return q.budget.deny()
		}
		h.retired, err = retireOwnedAllocationSnapshotsTx(ctx, q, parent, source, burn, a, string(expected), domain.RegisteredPreparationRetiredV1)
		return err
	})
}

// Used only by exact post-I/O rejoin; registration is normalized only after its
// complete postimage has been checked, never from a matching capsule digest.
func (h *LocalFailureV1) rejoin(ctx context.Context, q *ownedAuthorityQuery) error {
	v, err := h.readSnapshot(ctx, q)
	if err != nil || !reflect.DeepEqual(v, h.snapshot) {
		return q.budget.deny()
	}
	return nil
}
