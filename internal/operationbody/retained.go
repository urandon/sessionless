package operationbody

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// RetainedPairBytesV1 is the nonrefundable physical reservation for one bounded
// summary/manifest pair, shared with the original operation's aggregate budget.
const RetainedPairBytesV1 uint64 = (32 << 10) + (128 << 10)

type retainedSlot struct {
	sequence, generation uint64
	used                 bool
}

// RetainedPairV1 is original process-local physical capacity, not registration,
// upload permission or current authority. Copies share the same one-use slot.
// Its ownership cannot be reconstructed from a plan, reference or fresh Store.
type RetainedPairV1 struct {
	state *bodyState
	slot  *retainedSlot
}

// ReserveRetainedPair burns both encoded upper bounds before generation, even
// if generation, registration or upload later fails. Ordinary body writes and
// retained artifacts consume the same original aggregate allowance and gate.
func (s *StoreV1) ReserveRetainedPair(ctx context.Context, operationSequence, generation uint64) (*RetainedPairV1, error) {
	call, finish, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	st := s.state
	if st.writeFailed || call.Err() != nil || st.retainedCount >= st.maxRetainedPairs || st.retainedCount >= 2 || generation != st.retainedCount+1 || operationSequence == 0 || operationSequence > st.maxWrites/2 || RetainedPairBytesV1 > st.maxBytes-st.chargedBytes {
		return nil, ErrBody
	}
	if st.retainedCount > 0 && operationSequence <= st.retained[st.retainedCount-1].sequence {
		return nil, ErrBody
	}
	slot := &st.retained[st.retainedCount]
	*slot = retainedSlot{sequence: operationSequence, generation: generation}
	st.retainedCount++
	st.chargedBytes += RetainedPairBytesV1
	return &RetainedPairV1{state: st, slot: slot}, nil
}

// Write requires the trusted caller to have separately registered the exact
// pair under current Store authority before calling. This adapter verifies
// only original physical ownership and DATA integrity, never that transaction.
// It attempts summary then manifest once each, with actual bounded readback.
func (p *RetainedPairV1) Write(ctx context.Context, plan domain.DerivedContextPlanV1, summaryBytes, manifestBytes []byte) error {
	if p == nil || p.state == nil || p.slot == nil {
		return ErrBody
	}
	s := StoreV1{state: p.state}
	call, finish, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	st := p.state
	owned := false
	for i := uint64(0); i < st.retainedCount; i++ {
		if p.slot == &st.retained[i] {
			owned = true
		}
	}
	if !owned || st.writeFailed || p.slot.used || plan.Scope != st.scope || plan.OperationSequence != p.slot.sequence || plan.Generation != p.slot.generation || plan.Validate() != nil || !retainedBytesMatch(plan.Summary, summaryBytes) || !retainedBytesMatch(plan.Manifest, manifestBytes) {
		return ErrBody
	}
	// Burn the entire pair before copies/PUT. Any subsequent uncertainty stops
	// all original writes, including ordinary body writes and other pair slots.
	p.slot.used = true
	st.writeFailed = true
	summary, manifest := bytes.Clone(summaryBytes), bytes.Clone(manifestBytes)
	defer clear(summary)
	defer clear(manifest)
	for _, object := range []struct {
		ref  domain.BlobRef
		body []byte
	}{{plan.Summary, summary}, {plan.Manifest, manifest}} {
		if call.Err() != nil || !retainedBytesMatch(object.ref, object.body) {
			return ErrBody
		}
		actual, err := st.blobs.Put(call, st.scope.TenantID, object.ref.Key, bytes.NewReader(object.body))
		if err != nil || call.Err() != nil || actual != object.ref || actual.Validate() != nil {
			return ErrBody
		}
		confirmed, err := st.readVerified(call, object.ref)
		clear(confirmed)
		if err != nil || call.Err() != nil {
			return ErrBody
		}
	}
	st.writeFailed = false
	return nil
}

func retainedBytesMatch(ref domain.BlobRef, body []byte) bool {
	if ref.Size <= 0 || int64(len(body)) != ref.Size {
		return false
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]) == ref.SHA256
}
