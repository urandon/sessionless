package codexexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// ErrInlineReceiptPendingV2 reports retained original semantic custody, not
// successful retirement. Close fences execution/export; only the existing
// exact ACK/journal retirement path may relinquish these bytes.
var ErrInlineReceiptPendingV2 = errors.New("codex exec inline receipt remains pending reconciliation")

func (owner *PreparedAttachedWorkerRunnerV1) pendingInlineReceiptLockedV2() bool {
	slot := owner.slot
	return owner.nativeIdentityV2 == owner && owner.nativeConfigV2 != nil && owner.nativeConfigV2.InlineOutputV2 &&
		slot != nil && slot.finalized && slot.stageClean && slot.result.Succeeded(slot.runErr) &&
		slot.original != nil && slot.original.self == slot.original && slot.original.nativeFinalV2 != nil &&
		slot.original.nativeFinalV2.self == slot.original.nativeFinalV2 && slot.original.nativeFinalV2.original == slot.original
}

// NativeInlineOutputV2 retains the original private producer, not a generic
// output DTO. Metadata is copyable; the original artifact stream is one-shot.
// Construction itself neither seals a journal nor authorizes a network send.
type NativeInlineOutputV2 struct {
	self      *NativeInlineOutputV2
	owner     *PreparedAttachedWorkerRunnerV1
	slot      *preparedResultSlot
	candidate attachedworkeroutput.InlineCandidateV2
	producer  attachedworkeroutput.InlineProducerBindingV2
	expires   time.Time
	bodyUsed  bool
}

func (*NativeInlineOutputV2) String() string     { return "NativeInlineOutputV2{private:[redacted]}" }
func (v *NativeInlineOutputV2) GoString() string { return v.String() }

// InlineReceiptCandidate accepts only the exact successful outer result after
// its sole WriteBack/Release and staging cleanup. The same slot/window is kept
// across retries; no public status, declaration or copied owner can mint it.
func (owner *PreparedAttachedWorkerRunnerV1) InlineReceiptCandidate(ctx context.Context, identity attachedworkerdaemon.InvocationIdentity,
	result attachedworkerdaemon.InvocationResult, runErr error) (*NativeInlineOutputV2, error) {
	if owner == nil || owner.nativeIdentityV2 != owner || ctx == nil || ctx.Err() != nil {
		return nil, ErrContract
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	slot := owner.slot
	if owner.nativeConfigV2 == nil || !owner.nativeConfigV2.InlineOutputV2 || owner.closed || slot == nil ||
		!slot.finalized || !slot.stageClean || identity != slot.identity || !reflect.DeepEqual(result, slot.result) ||
		!reflect.DeepEqual(runErr, slot.runErr) || !result.Succeeded(runErr) || !result.CleanupComplete() ||
		slot.original == nil || slot.original.self != slot.original || slot.original.nativeFinalV2 == nil ||
		slot.driver == nil || !slot.driver.nativeInlineOutputV2 || slot.semantic.ProviderEvidence == nil ||
		slot.semantic.ProviderEvidence.FinishClass != domain.ProviderFinishCompletedV1 {
		return nil, ErrContract
	}
	if slot.inline != nil {
		if !slot.inline.validLocked() {
			return nil, ErrContract
		}
		return slot.inline, nil // immutable metadata recovery never resets expiry
	}
	f := slot.original.nativeFinalV2
	boundary, concrete := slot.driver.boundary.(*PreparedProcessBoundaryV1)
	if !concrete || boundary.nativeIdentity != boundary || slot.original.boundary != boundary ||
		boundary.native == nil || slot.original.native != boundary.native || f.self != f || f.original != slot.original ||
		f.profileDigest != owner.nativeProfileV2.descriptor.BackendProfileDigest || f.schema != owner.nativeProfileV2.launch.Schema ||
		f.rawDigest != slot.original.rawFinalDigestV2 || f.answer != slot.semantic.Summary ||
		boundary.native.stageV2 == nil || !boundary.native.stageV2.dispatched || !boundary.native.stageV2.quiesced ||
		slot.accepted.ReservationID.Validate() != nil || slot.accepted.CreatedAt.IsZero() {
		return nil, ErrContract
	}
	candidate := attachedworkeroutput.InlineCandidateV2{Summary: f.answer}
	for _, artifact := range f.artifacts {
		digest := sha256.Sum256(artifact.content)
		candidate.Artifacts = append(candidate.Artifacts, attachedworkeroutput.InlineArtifactV2{
			Name: artifact.name, MediaType: artifact.mediaType, Size: uint64(len(artifact.content)), SHA256: hex.EncodeToString(digest[:]),
		})
	}
	harness, err := slot.request.HarnessBinding.Digest()
	if err != nil || candidate.Validate() != nil {
		return nil, ErrContract
	}
	answer := sha256.Sum256([]byte(f.answer))
	producer := attachedworkeroutput.InlineProducerBindingV2{SessionID: slot.request.SessionID, HarnessBindingDigest: harness,
		ReservationID: slot.accepted.ReservationID, AcceptedAt: slot.accepted.CreatedAt, SchemaDigest: f.schema.SHA256,
		FinalDigest: hex.EncodeToString(f.rawDigest[:]), AnswerDigest: hex.EncodeToString(answer[:])}
	now := owner.nativeNowV2()
	expires := now.Add(15 * time.Second)
	for _, bound := range []time.Time{slot.accepted.LeaseExpiresAt, boundary.native.runtimeDeadlineV2, slot.credentialDeadline} {
		if bound.Before(expires) {
			expires = bound
		}
	}
	if !slot.callerDeadline.IsZero() && slot.callerDeadline.Before(expires) {
		expires = slot.callerDeadline
	}
	if bound := slot.request.HarnessBinding.EvidenceExpiresAt; bound != nil && bound.Before(expires) {
		expires = *bound
	}
	if bound, ok := ctx.Deadline(); ok && bound.Before(expires) {
		expires = bound
	}
	snapshot := owner.nativeSessionV2.Snapshot()
	if snapshot.AuthenticationExpires == nil {
		return nil, ErrAuthorityUnavailable
	}
	if snapshot.AuthenticationExpires.Before(expires) {
		expires = *snapshot.AuthenticationExpires
	}
	v := &NativeInlineOutputV2{owner: owner, slot: slot, candidate: candidate, producer: producer, expires: expires.UTC()}
	v.self, slot.inline = v, v
	if err := v.currentLocked(ctx); err != nil {
		slot.inline = nil
		return nil, err
	}
	slot.candidateBuilt = true
	return v, nil
}

func (v *NativeInlineOutputV2) validLocked() bool {
	return v != nil && v.self == v && v.owner != nil && v.owner.nativeIdentityV2 == v.owner &&
		v.slot != nil && v.owner.slot == v.slot && v.slot.inline == v && v.slot.finalized && v.slot.stageClean &&
		v.slot.original != nil && v.slot.original.self == v.slot.original && v.slot.original.nativeFinalV2 != nil &&
		v.slot.original.nativeFinalV2.self == v.slot.original.nativeFinalV2 && v.slot.original.nativeFinalV2.original == v.slot.original
}

func (v *NativeInlineOutputV2) currentLocked(ctx context.Context) error {
	if !v.validLocked() || v.owner.closed || ctx == nil || ctx.Err() != nil || !v.owner.nativeNowV2().Before(v.expires) {
		return ErrAuthorityUnavailable
	}
	native := v.slot.original.native
	now := v.owner.nativeNowV2()
	if native == nil || native.self != native || native.child == nil || native.child.CheckReceiptScopeV2(ctx) != nil ||
		validateNativeEnvelopeEvidenceV2(native.profileV2, native.evidenceV2, native.bindingV2, now) != nil ||
		nativeCredentialCurrentV2(ctx, native.credentialBindingsV2, native.authority, now) != nil || ctx.Err() != nil {
		return ErrAuthorityUnavailable
	}
	return nil
}

// Declaration returns detached untrusted wire metadata, never body/provenance
// authority. An already issued carrier remains readable for metadata-only
// recovery after expiry or cooperative owner Close, never for a fresh send.
func (v *NativeInlineOutputV2) Declaration() (attachedworkeroutput.InlineCandidateV2, attachedworkeroutput.InlineProducerBindingV2, time.Time, error) {
	if v == nil || v.owner == nil {
		return attachedworkeroutput.InlineCandidateV2{}, attachedworkeroutput.InlineProducerBindingV2{}, time.Time{}, ErrContract
	}
	v.owner.mu.Lock()
	defer v.owner.mu.Unlock()
	if !v.validLocked() {
		return attachedworkeroutput.InlineCandidateV2{}, attachedworkeroutput.InlineProducerBindingV2{}, time.Time{}, ErrContract
	}
	candidate := v.candidate
	candidate.Artifacts = append([]attachedworkeroutput.InlineArtifactV2(nil), candidate.Artifacts...)
	return candidate, v.producer, v.expires, nil
}

// CheckCurrent rechecks the original receipt-only scope immediately before a
// fresh export phase. It never reopens the already closed execution child.
func (v *NativeInlineOutputV2) CheckCurrent(ctx context.Context) error {
	if v == nil || v.owner == nil {
		return ErrContract
	}
	v.owner.mu.Lock()
	defer v.owner.mu.Unlock()
	return v.currentLocked(ctx)
}

// OpenBody irreversibly burns the single original body stream before returning
// it. The caller must FIRST seal the immutable compact journal/send commitment.
// Close/error/lost response never permits opening another stream. No allocation
// concatenates or transforms the original ordered semantic content.
func (v *NativeInlineOutputV2) OpenBody(ctx context.Context) (io.ReadCloser, int64, error) {
	if v == nil || v.owner == nil {
		return nil, 0, ErrContract
	}
	v.owner.mu.Lock()
	defer v.owner.mu.Unlock()
	if err := v.currentLocked(ctx); err != nil {
		return nil, 0, err
	}
	if v.bodyUsed || len(v.candidate.Artifacts) == 0 {
		return nil, 0, ErrProcessAlreadyConsumed
	}
	v.bodyUsed = true
	var size int64
	for _, artifact := range v.candidate.Artifacts {
		size += int64(artifact.Size)
	}
	return &nativeInlineReaderV2{source: v, ctx: ctx}, size, nil
}

type nativeInlineReaderV2 struct {
	source          *NativeInlineOutputV2
	ctx             context.Context
	ordinal, offset int
	closed          bool
}

func (r *nativeInlineReaderV2) Read(p []byte) (int, error) {
	v := r.source
	v.owner.mu.Lock()
	defer v.owner.mu.Unlock()
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if err := v.currentLocked(r.ctx); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	artifacts := v.slot.original.nativeFinalV2.artifacts
	for r.ordinal < len(artifacts) {
		body := artifacts[r.ordinal].content
		if r.offset == len(body) {
			r.ordinal++
			r.offset = 0
			continue
		}
		n := copy(p, body[r.offset:])
		r.offset += n
		return n, nil
	}
	return 0, io.EOF
}

func (r *nativeInlineReaderV2) Close() error {
	r.source.owner.mu.Lock()
	defer r.source.owner.mu.Unlock()
	r.closed = true
	return nil // original bytes belong to exact ACK/journal retirement, not HTTP
}
