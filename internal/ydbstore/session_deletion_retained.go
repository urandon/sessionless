package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// This file implements historical DATA/body checks, not Start permission.
// The lifecycle's custody and prepared-parent guards remain in place until the
// complete SQL postimage/range capture and transactional rejoin are integrated.

type retainedDeletionProducerData struct {
	terminal   localTerminalHistorySnapshot
	allocation ownedAllocationSnapshots
	burnedAt   time.Time
}

// Original historical producer join. No current Source verifier, active lease
// or publisher is called, and no erased opaque receipt is reconstructed. The
// genuine tagged finalizer seal attests its ORIGINAL physical cleanup checks.
// This join alone is not Session-wide writer coverage or deletion permission.
func readRetainedDeletionProducer(ctx context.Context, q *ownedAuthorityQuery, run domain.Run, anchor domain.SessionEvent) (retainedDeletionProducerData, error) {
	deny := func() (retainedDeletionProducerData, error) {
		return retainedDeletionProducerData{}, q.budget.deny()
	}
	v, err := readHistoricalTerminalSnapshot(ctx, q, run, anchor)
	if err != nil || v.Evidence.ValidateForPersistedAuthority(v.Parent.Authority, v.Parent.Reservation) != nil {
		return deny()
	}
	burned, at, err := readOwnedPreparationBurnObservation(ctx, q, v.Parent)
	if err != nil || !burned {
		return deny()
	}
	allocation, err := readOwnedAllocationSnapshots(ctx, q, v.Parent, v.Admission.Source, at)
	if err != nil || allocation == nil || allocation.original.LocalRuntime == nil || allocation.current.State != domain.RegisteredPreparationRetiredV1 || allocation.current.ValidateTransitionFrom(allocation.original) != nil {
		return deny()
	}
	original, oe := allocation.original.Digest()
	retired, re := allocation.current.Digest()
	e := v.Run.TerminalCustody.LocalEvidence
	if oe != nil || re != nil || string(original) != e.OriginalAllocationDigest || string(retired) != e.RetiredAllocationDigest || !allocation.current.UpdatedAt.Equal(e.RetiredAt) || e.StoppedAt.Before(at) || e.ErasedAt.After(v.Run.UpdatedAt) {
		return deny()
	}
	return retainedDeletionProducerData{terminal: v, allocation: *allocation, burnedAt: at}, nil
}

// retainedDeletionRunRefsData uses ONLY original SQL-owned Run metadata and
// the original deterministic cancellation codec. It does not discover objects
// from blob contents and grants no permission to delete unknown writers.
func retainedDeletionRunRefsData(run domain.Run) ([]domain.BlobRef, error) {
	if run.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	if run.TerminalCustody == nil {
		if run.DerivedContext != nil {
			return nil, domain.ErrMembershipDenied
		}
		return nil, nil
	}
	refs := []domain.BlobRef{run.TerminalCustody.Plan.Capsule}
	if run.DerivedContext != nil {
		for _, plan := range run.DerivedContext.Inventory {
			if plan.Scope != run.TerminalCustody.Plan.Scope {
				return nil, domain.ErrMembershipDenied
			}
			refs = append(refs, plan.Summary, plan.Manifest)
		}
	}
	notice, err := canonicaloutput.EncodeFailureV1(run.ID, true, "cancelled")
	if err != nil {
		return nil, err
	}
	defer clear(notice.Payload)
	draft, err := canonicaloutput.PlanEventDraftV1(run, notice.ID, notice.Kind, notice.Payload, run.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return append(refs, draft.Payload), nil
}

// Each dependency must match its COMPLETE original same-Session SQL owner.
// Maps contain DATA copied from the captured SQL inventory, not blob-discovered
// references or an authorization DTO. Their capture/rejoin is still required.
type retainedDeletionOwnershipData struct {
	tenant  domain.TenantID
	session domain.SessionID
	rows    uint64
	events  map[domain.SessionEventID]domain.SessionEvent
	plans   map[string]domain.DerivedContextPlanV1
	runs    map[domain.RunID]bool
}

func newRetainedDeletionOwnershipData(tenant domain.TenantID, session domain.SessionID) (retainedDeletionOwnershipData, error) {
	if tenant.Validate() != nil || session.Validate() != nil {
		return retainedDeletionOwnershipData{}, domain.ErrMembershipDenied
	}
	return retainedDeletionOwnershipData{tenant: tenant, session: session, events: make(map[domain.SessionEventID]domain.SessionEvent), plans: make(map[string]domain.DerivedContextPlanV1), runs: make(map[domain.RunID]bool)}, nil
}

// Capture can stream one Run at a time. This index retains only bounded exact
// reference/identity DATA, never a slice of every 32-KiB Run payload.
func (v *retainedDeletionOwnershipData) addEvent(event domain.SessionEvent) error {
	if v == nil || v.events == nil || v.rows >= maxSessionDeletionRows || event.Validate() != nil || event.TenantID != v.tenant || event.SessionID != v.session {
		return domain.ErrMembershipDenied
	}
	if _, duplicate := v.events[event.ID]; duplicate {
		return domain.ErrMembershipDenied
	}
	v.events[event.ID] = event
	v.rows++
	return nil
}

func (v *retainedDeletionOwnershipData) addRun(run domain.Run) error {
	if v == nil || v.runs == nil || v.plans == nil || run.Validate() != nil || run.TenantID != v.tenant || run.SessionID != v.session || v.runs[run.ID] || v.rows >= maxSessionDeletionRows {
		return domain.ErrMembershipDenied
	}
	// Charge/validate the entire row before mutating the reference index.
	var plans []domain.DerivedContextPlanV1
	if run.DerivedContext != nil {
		plans = run.DerivedContext.Inventory
	}
	if uint64(len(plans))+1 > maxSessionDeletionRows-v.rows {
		return domain.ErrMembershipDenied
	}
	for _, plan := range plans {
		digest, err := plan.Digest()
		if err != nil {
			return domain.ErrMembershipDenied
		}
		if _, duplicate := v.plans[digest]; duplicate {
			return domain.ErrMembershipDenied
		}
	}
	v.runs[run.ID] = true
	v.rows++
	for _, plan := range plans {
		digest, _ := plan.Digest()
		v.plans[digest] = plan
		v.rows++
	}
	return nil
}

func (v retainedDeletionOwnershipData) contains(dependency *compactioncontext.RetainedV1) bool {
	if dependency == nil || v.events == nil || v.plans == nil {
		return false
	}
	plan := dependency.Plan()
	digest, err := plan.Digest()
	if err != nil || v.plans[digest] != plan {
		return false
	}
	events, plans := dependency.DependenciesV1()
	for _, event := range events {
		if !reflect.DeepEqual(v.events[event.ID], event) {
			return false
		}
	}
	for _, parent := range plans {
		digest, err := parent.Digest()
		if err != nil || v.plans[digest] != parent {
			return false
		}
	}
	return true
}

// Bounded exact Open/EOF/Close and hash verification outside SQL. The shared
// logical authority budget is mandatory; a missing or active callback denies
// BEFORE Open. The reader exposes no Put/Delete or prefix-list operation.
func readRetainedDeletionBlob(ctx context.Context, reader SessionDeletionBlobReader, ref domain.BlobRef, limit uint64) ([]byte, error) {
	if ctx == nil || reader == nil || ref.Validate() != nil || ref.Size < 0 || uint64(ref.Size) > limit || limit == 0 || limit > domain.MaxTerminalCapsuleBytesV1 || chargeOwnedAuthorityBody(ctx, uint64(ref.Size)+1) != nil {
		return nil, domain.ErrMembershipDenied
	}
	r, err := reader.Open(ctx, ref.TenantID, ref)
	if err != nil {
		if r != nil {
			_ = r.Close()
		}
		return nil, domain.ErrMembershipDenied
	}
	if r == nil {
		return nil, domain.ErrMembershipDenied
	}
	body, readErr := io.ReadAll(io.LimitReader(r, ref.Size+1))
	closeErr := r.Close()
	sum := sha256.Sum256(body)
	if readErr != nil || closeErr != nil || ctx.Err() != nil || int64(len(body)) != ref.Size || hex.EncodeToString(sum[:]) != ref.SHA256 {
		clear(body)
		return nil, domain.ErrMembershipDenied
	}
	return body, nil
}

// Verify actual original capsule, canonical payload and ALL registered pairs.
// No current Source/profile/lease is reopened. This unexported result is DATA,
// not writer-quiescence proof, capture installation, erasure or a delete grant.
// The SQL verifier must first authenticate snapshot including the tagged seal,
// original allocation/burn and complete accounting, then rejoin after this I/O.
func verifyRetainedDeletionBodies(ctx context.Context, reader SessionDeletionBlobReader, snapshot localTerminalHistorySnapshot, ownership retainedDeletionOwnershipData, previous *compactioncontext.RetainedV1) (*compactioncontext.RetainedV1, error) {
	deny := func() (*compactioncontext.RetainedV1, error) { return nil, domain.ErrMembershipDenied }
	run := snapshot.Run
	if run.Validate() != nil || run.Status != domain.RunSucceeded || run.CancellationRequestedAt != nil || run.TerminalCustody == nil || run.TerminalCustody.LocalEvidence == nil || run.TerminalCustody.Correspondence == nil || !isLocalFinalizationMarkerV2(snapshot.Seal) || !reflect.DeepEqual(ownership.events[snapshot.Anchor.ID], snapshot.Anchor) || (previous != nil && !ownership.contains(previous)) {
		return deny()
	}
	body, err := readRetainedDeletionBlob(ctx, reader, run.TerminalCustody.Plan.Capsule, domain.MaxTerminalCapsuleBytesV1)
	if err != nil {
		return deny()
	}
	defer clear(body)
	if chargeOwnedAuthorityBody(ctx, uint64(len(body))) != nil {
		return deny()
	}
	var capsule localRetainedTerminalCapsuleV1
	if decodeOwnedEffectCanonicalJSON(string(body), &capsule) != nil {
		return deny()
	}
	defer func() {
		for i := range capsule.Operations {
			clear(capsule.Operations[i].NormalizedBody)
		}
	}()
	ops := make([]LocalCompactionOperationV1, len(capsule.Operations))
	for i, item := range capsule.Operations {
		ops[i] = LocalCompactionOperationV1{Reservation: item.Reservation, Observation: item.Observation, Response: item.NormalizedBody}
	}
	if chargeOwnedAuthorityBody(ctx, uint64(len(body))) != nil {
		return deny()
	}
	encoded, err := encodeLocalTerminalCapsule(capsule.Scope, capsule.ProfileDigest, capsule.FrontierDigest, ops, capsule.DerivedContext, capsule.Events, capsule.PublicManifest, domain.MaxTerminalCapsuleBytesV1)
	equal := bytes.Equal(encoded, body)
	clear(encoded)
	if err != nil || !equal {
		return deny()
	}
	payload, err := readRetainedDeletionBlob(ctx, reader, snapshot.Anchor.Payload, domain.MaxTerminalCapsuleBytesV1)
	if err != nil {
		return deny()
	}
	defer clear(payload)
	if validateHistoricalCapsuleData(snapshot, capsule, []sessioncontext.EventPayload{{Event: snapshot.Anchor, Payload: payload}}, nil) != nil {
		return deny()
	}
	if run.DerivedContext == nil {
		return previous, nil
	}
	policy := snapshot.Parent.Authority.HarnessBinding.AgentLoopPolicy
	if policy == nil || run.DerivedContext.Head == nil {
		return deny()
	}
	for _, plan := range run.DerivedContext.Inventory {
		if plan.Scope != run.TerminalCustody.Plan.Scope {
			return deny()
		}
		summary, err := readRetainedDeletionBlob(ctx, reader, plan.Summary, compactioncontext.MaxRetainedSummaryBytesV1)
		if err != nil {
			return deny()
		}
		manifest, err := readRetainedDeletionBlob(ctx, reader, plan.Manifest, compactioncontext.MaxRetainedManifestBytesV1)
		if err != nil {
			clear(summary)
			return deny()
		}
		// Decoder owns copies plus parsed bounded DATA; precharge before decoding.
		if chargeOwnedAuthorityBody(ctx, 3*uint64(len(summary)+len(manifest))) != nil {
			clear(summary)
			clear(manifest)
			return deny()
		}
		decoded, err := compactioncontext.DecodeSealedRetainedV1(capsule.ProfileDigest, *policy, plan, summary, manifest, previous)
		clear(summary)
		clear(manifest)
		if err != nil || !ownership.contains(decoded) {
			return deny()
		}
		previous = decoded
	}
	last, err := previous.Plan().Digest()
	if err != nil || last != run.DerivedContext.Head.PlanDigest {
		return deny()
	}
	return previous, nil
}
