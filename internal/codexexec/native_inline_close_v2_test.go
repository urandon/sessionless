//go:build darwin || linux

package codexexec

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Original prepared Run/Session/file-journal composition with the existing
// owned shell and synthetic isolation/credential metadata. Journal CAS below
// is metadata, not a forged TerminalAck or canonical server receipt. The actual
// adapter ACK->durable retirement->RetireReceipt join is tested in stack.
func TestNativeInlineCloseV2RetainsPendingJournalAndFencesExport(t *testing.T) {
	r, invocation, result, _ := nativeInlineFixtureV2(t, nativeInlineFinalFixtureV2)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	source, err := r.InlineReceiptCandidate(ctx, invocation.Identity, result, nil)
	if err != nil {
		t.Fatal(err)
	}
	candidate, producer, expiry, err := source.Declaration()
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := nativeInlineCloseCheckpointV2(t, r, invocation, result, candidate, producer, expiry)
	initial, err := attachedworkeroutput.EncodeInlineCheckpointV2(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(initial)
	if err := r.nativeSessionV2.SealInlineReceiptSubmissionV2(ctx, initial); err != nil {
		t.Fatalf("seal original declaration: %v", err)
	}
	checkpoint.Progress.Revision++
	checkpoint.Progress.BodyBurned = true
	bodyBurned, err := attachedworkeroutput.EncodeInlineCheckpointV2(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(bodyBurned)
	if applied, err := r.nativeSessionV2.AdvanceInlineReceiptSubmissionV2(ctx, initial, bodyBurned); err != nil || !applied {
		t.Fatalf("reserve original body before opening: applied=%t err=%v", applied, err)
	}
	reader, _, err := source.OpenBody(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if n, err := reader.Read(make([]byte, 1)); n != 1 || err != nil {
		t.Fatalf("first original byte n=%d err=%v", n, err)
	}
	slot, final := r.slot, r.slot.original.nativeFinalV2
	retained := final.artifacts[0].content
	before := bytes.Clone(retained)
	defer clear(before)
	for range 2 {
		if err := r.Close(ctx); !errors.Is(err, ErrInlineReceiptPendingV2) {
			t.Fatalf("cooperative Close=%v want explicit pending custody", err)
		}
	}
	if r.slot != slot || !r.closed || final.answer != candidate.Summary || !bytes.Equal(retained, before) || len(final.artifacts) != 2 {
		t.Fatal("cooperative Close discarded the original pending semantic owner")
	}
	after, afterProducer, afterExpiry, err := source.Declaration()
	if err != nil || !reflect.DeepEqual(after, candidate) || afterProducer != producer || !afterExpiry.Equal(expiry) {
		t.Fatalf("retained declaration changed after Close: %v", err)
	}
	if err := source.CheckCurrent(ctx); !errors.Is(err, ErrAuthorityUnavailable) {
		t.Fatalf("closed export authority=%v", err)
	}
	if _, _, err := source.OpenBody(ctx); err == nil {
		t.Error("closed owner opened another body")
	}
	if n, err := reader.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Errorf("existing reader survived Close: n=%d err=%v", n, err)
	}
	if _, err := r.InlineReceiptCandidate(ctx, invocation.Identity, result, nil); err == nil {
		t.Error("closed owner minted/reacquired export carrier")
	}
	if _, err := r.Run(ctx, invocation); !errors.Is(err, ErrProcessAlreadyConsumed) {
		t.Errorf("closed original Run=%v", err)
	}
	if err := r.nativeSessionV2.RetireInlineReceiptSubmissionV2(ctx, bodyBurned); err == nil {
		t.Fatal("Close substituted for journal ACK")
	}
	// A successful exact prior-record CAS after Close proves that the real
	// Session's pending file was not removed/reset by owner Close. This only
	// burns reconciliation metadata; it does not authorize another body.
	checkpoint.Progress.Revision++
	checkpoint.Progress.ReconcileBurned = true
	next, err := attachedworkeroutput.EncodeInlineCheckpointV2(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(next)
	if applied, err := r.nativeSessionV2.AdvanceInlineReceiptSubmissionV2(ctx, bodyBurned, next); err != nil || !applied {
		t.Fatalf("retained original journal CAS applied=%t err=%v", applied, err)
	}
	if applied, err := r.nativeSessionV2.AdvanceInlineReceiptSubmissionV2(ctx, bodyBurned, next); err != nil || applied {
		t.Fatalf("duplicate journal observation minted token: applied=%t err=%v", applied, err)
	}
	if !bytes.Equal(retained, before) || r.slot != slot {
		t.Fatal("metadata recovery retired pending original bytes without ACK")
	}
}

func TestNativeInlineCloseV2BeforeCarrierRetainsOriginalCompletion(t *testing.T) {
	r, invocation, result, _ := nativeInlineFixtureV2(t, nativeInlineFinalFixtureV2)
	slot := r.slot
	final := slot.original.nativeFinalV2
	before := bytes.Clone(final.artifacts[0].content)
	defer clear(before)
	if slot.inline != nil {
		t.Fatal("fixture unexpectedly exported a carrier")
	}
	if err := r.Close(t.Context()); !errors.Is(err, ErrInlineReceiptPendingV2) {
		t.Fatalf("Close before export=%v want pending", err)
	}
	if r.slot != slot || !bytes.Equal(final.artifacts[0].content, before) || final.answer == "" {
		t.Fatal("Close before carrier erased the original completed output")
	}
	if _, err := r.InlineReceiptCandidate(t.Context(), invocation.Identity, result, nil); err == nil || slot.inline != nil {
		t.Fatal("Close before carrier minted export authority")
	}
}

func nativeInlineCloseCheckpointV2(t *testing.T, r *PreparedAttachedWorkerRunnerV1, invocation attachedworkerdaemon.Invocation,
	result attachedworkerdaemon.InvocationResult, candidate attachedworkeroutput.InlineCandidateV2,
	producer attachedworkeroutput.InlineProducerBindingV2, expiry time.Time) attachedworkeroutput.InlineReceiptCheckpointV2 {
	t.Helper()
	a := invocation.VerifiedInput.AcceptedAttempt
	observation := attachedworkeroutput.ProcessObservationV1{Version: 1, ExitCode: result.Process.ExitCode,
		DescendantsReaped: result.Process.DescendantsReaped, BoundaryReleased: result.Process.BoundaryReleased,
		CleanupSucceeded: result.Process.CleanupSucceeded, CredentialReleaseRequired: result.CredentialReleaseRequired, CredentialReleased: result.CredentialReleased}
	observationDigest, err := observation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := r.nativeSessionV2.Snapshot()
	request := attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Nonce: "native-inline-close",
		Candidate: candidate, Producer: producer, Observation: observation, ExpiresAt: expiry,
		Authorization: ports.AttachedWorkerSealedInputAuthorization{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, WorkerID: a.WorkerID,
			ConnectionID: snapshot.ConnectionID, EnrollmentGeneration: snapshot.EnrollmentGeneration, ConnectionGeneration: snapshot.ConnectionGeneration,
			RunID: a.RunID, AttemptID: a.AttemptID, AttemptSequence: 1, LeaseID: a.LeaseID, LeaseGeneration: a.LeaseGeneration,
			FenceToken: a.FenceToken, LeaseExpiresAtUnixMicro: a.LeaseExpiresAt.UnixMicro(),
			ContextDigest: a.ContextDigest, CapabilityDigest: snapshot.CapabilityDigest, PolicyDigest: a.PolicyDigest}}
	fingerprint, err := attachedworkeroutput.InlineOutputRequestFingerprintV2(request)
	if err != nil {
		t.Fatal(err)
	}
	plan := attachedworkeroutput.InlineReceiptPlanV2{Authorization: request.Authorization, Nonce: request.Nonce, Fingerprint: fingerprint,
		Producer: producer, Artifacts: candidate.Artifacts, ObservationDigest: observationDigest, ExpiresAt: expiry,
		WorkerAttemptSequence: 1, PlatformAttemptSequence: 1, TerminalSequence: 1}
	digest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return attachedworkeroutput.InlineReceiptCheckpointV2{Version: 2, Plan: plan, PlanSHA256: digest,
		Progress: attachedworkeroutput.InlineReceiptProgressV2{Revision: 1, PrepareBurned: true}}
}
