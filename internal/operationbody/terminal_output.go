package operationbody

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// LocalTerminalCapsuleBytesV1 is the fixed local terminal profile envelope,
// prepaid at original intake. The domain's 512-KiB hard cap remains unchanged.
// This physical reservation does NOT establish prospective capsule-format fit.
const LocalTerminalCapsuleBytesV1 uint64 = 256 << 10

// LocalTerminalFailureNoticeBytesV1 precharges a separate canonical failure
// envelope, not another artifact/body debit or a second terminal capsule.
const LocalTerminalFailureNoticeBytesV1 uint64 = 1024

type terminalOutputState struct {
	maxArtifacts, maxBytes uint64
	artifacts, charged     uint64
	bodyLimit              uint64
	capsule                outputSlot
	failure                outputSlot
}

type outputSlot struct {
	owner   *terminalOutputState
	limit   uint64
	used    bool
	failure bool
}

// TerminalOutputV1 is original process-local physical capacity only. Copies
// share counters and one-use capsule ownership. Neither it nor a planned ref
// authorizes a PUT, finalization, deletion or receipt; the original publisher
// must separately establish those current joins before physical writes.
type TerminalOutputV1 struct {
	state *bodyState
	owner *terminalOutputState
}

// VisibleOutputV1 is one nonrefundable physical output reservation. Its actual
// publisher must register its exact planned reference before Write. It grants
// no ability to publish a private capsule as a visible artifact.
type VisibleOutputV1 struct {
	state *bodyState
	slot  *outputSlot
}

// AdmitTerminalOutput must run once before ANY body/pair write or reservation.
// Its limits come from the same original Store admission, never a model DTO.
// Capsule bytes debit the original body pool AND shared artifact allowance;
// ordinary request/result write count and retained-pair count are unchanged.
func (s *StoreV1) AdmitTerminalOutput(ctx context.Context, product domain.ProductLimits, substrate domain.SubstrateLimitsV1) (*TerminalOutputV1, error) {
	if product.Validate() != nil || substrate.Validate() != nil {
		return nil, ErrBody
	}
	call, finish, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	st := s.state
	if call.Err() != nil || st.writeFailed || st.terminal != nil || st.writes != 0 || st.chargedBytes != 0 || st.retainedCount != 0 || LocalTerminalCapsuleBytesV1 >= st.maxBytes || LocalTerminalCapsuleBytesV1 > substrate.ArtifactBytes || product.MaxArtifacts == 0 {
		return nil, ErrBody
	}
	owner := &terminalOutputState{maxArtifacts: uint64(product.MaxArtifacts), maxBytes: substrate.ArtifactBytes, artifacts: 1, charged: LocalTerminalCapsuleBytesV1, bodyLimit: st.maxBytes - LocalTerminalCapsuleBytesV1}
	owner.capsule = outputSlot{owner: owner, limit: LocalTerminalCapsuleBytesV1}
	owner.failure = outputSlot{owner: owner, limit: LocalTerminalFailureNoticeBytesV1, failure: true}
	st.terminal = owner
	st.chargedBytes += LocalTerminalCapsuleBytesV1
	return &TerminalOutputV1{state: st, owner: owner}, nil
}

// OperationBodyLimit is immutable after admission. Existing durable operation
// debit must fit this ceiling; the capsule is not a fabricated model operation.
func (t *TerminalOutputV1) OperationBodyLimit() uint64 {
	if t == nil || t.state == nil || t.owner == nil || t.state.terminal != t.owner {
		return 0
	}
	return t.owner.bodyLimit
}

func (t *TerminalOutputV1) FailureNoticeLimit() uint64 {
	if t.OperationBodyLimit() == 0 {
		return 0
	}
	return t.owner.failure.limit
}

func (t *TerminalOutputV1) CapsuleLimit() uint64 {
	if t.OperationBodyLimit() == 0 || t.owner.capsule.owner != t.owner {
		return 0
	}
	return t.owner.capsule.limit
}

func (t *terminalOutputState) reserveArtifacts(count, upperBytes uint64) bool {
	if count == 0 || upperBytes == 0 || count > t.maxArtifacts-t.artifacts || upperBytes > t.maxBytes-t.charged {
		return false
	}
	t.artifacts += count
	t.charged += upperBytes
	return true
}

func (t *TerminalOutputV1) ReserveVisibleOutput(ctx context.Context, encodedUpperBound uint64) (*VisibleOutputV1, error) {
	if t.OperationBodyLimit() == 0 || encodedUpperBound == 0 {
		return nil, ErrBody
	}
	s := StoreV1{state: t.state}
	call, finish, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	if call.Err() != nil || t.state.writeFailed || !t.owner.reserveArtifacts(1, encodedUpperBound) {
		return nil, ErrBody
	}
	return &VisibleOutputV1{state: t.state, slot: &outputSlot{owner: t.owner, limit: encodedUpperBound}}, nil
}

// WriteCapsule is for the trusted ORIGINAL publisher only, AFTER its actual
// Session-fenced registration. A matching DATA plan here never proves that
// transaction and does not create a standalone serving/upload interface.
func (t *TerminalOutputV1) WriteCapsule(ctx context.Context, plan domain.TerminalCustodyPlanV1, body []byte) error {
	if t.OperationBodyLimit() == 0 || plan.Validate() != nil || plan.Scope != t.state.scope {
		return ErrBody
	}
	return writeOutput(ctx, t.state, &t.owner.capsule, plan.Capsule, body)
}

// The authentic local composer separately selects a closed safe error code and
// canonical identity. This method supplies only the original single physical
// notice slot, including after an uncertain artifact write; it cannot clear
// that earlier uncertainty or turn a failure notice into success evidence.
func (t *TerminalOutputV1) WriteFailureNotice(ctx context.Context, draft domain.SessionEventDraft, body []byte) error {
	if t.OperationBodyLimit() == 0 || draft.ID.Validate() != nil || draft.Kind != domain.SessionEventSystemNotice || draft.Payload.Validate() != nil || draft.Payload.TenantID != t.state.scope.TenantID || draft.Payload.Key != domain.SessionEventObjectPrefix(t.state.scope.TenantID, t.state.scope.SessionID, draft.ID)+"payloads/sha256/"+draft.Payload.SHA256+".json" {
		return ErrBody
	}
	return writeOutput(ctx, t.state, &t.owner.failure, draft.Payload, body)
}

func (o *VisibleOutputV1) Write(ctx context.Context, ref domain.BlobRef, body []byte) error {
	if o == nil || o.state == nil || o.slot == nil || o.state.terminal == nil || o.slot.owner != o.state.terminal || ref.Validate() != nil || ref.TenantID != o.state.scope.TenantID || ref.Key != domain.SessionRunObjectPrefix(o.state.scope.TenantID, o.state.scope.SessionID, o.state.scope.RunID)+"artifacts/sha256/"+ref.SHA256 {
		return ErrBody
	}
	return writeOutput(ctx, o.state, o.slot, ref, body)
}

func writeOutput(ctx context.Context, st *bodyState, slot *outputSlot, ref domain.BlobRef, body []byte) error {
	if len(body) == 0 || uint64(len(body)) > slot.limit || int64(len(body)) != ref.Size {
		return ErrBody
	}
	s := StoreV1{state: st}
	call, finish, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if (st.writeFailed && !slot.failure) || slot.used || slot.owner != st.terminal {
		return ErrBody
	}
	slot.used = true
	priorFailure := st.writeFailed
	st.writeFailed = true
	sealed := bytes.Clone(body)
	defer clear(sealed)
	// Authenticate only the owned bytes after irrevocable admission. The
	// caller's context can run code while begin acquires the original gate.
	sum := sha256.Sum256(sealed)
	if hex.EncodeToString(sum[:]) != ref.SHA256 || call.Err() != nil {
		return ErrBody
	}
	actual, err := st.blobs.Put(call, st.scope.TenantID, ref.Key, bytes.NewReader(sealed))
	if err != nil || call.Err() != nil || actual != ref {
		return ErrBody
	}
	confirmed, err := st.readVerified(call, ref)
	clear(confirmed)
	if err != nil || call.Err() != nil {
		return ErrBody
	}
	st.writeFailed = priorFailure
	return nil
}
