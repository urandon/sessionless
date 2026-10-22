package attachedworkersession

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func inlineReceiptCheckpointForSessionV2(t *testing.T, attempt domain.AttachedWorkerAttemptV1, job domain.WorkerJob) attachedworkeroutput.InlineReceiptCheckpointV2 {
	t.Helper()
	// These SHA-256-shaped values are synthetic declaration metadata only; this
	// test exercises persistence and scope, not producer provenance or receipt IO.
	harnessDigest, err := job.HarnessBinding.Digest()
	if err != nil {
		t.Fatal(err)
	}
	plan := attachedworkeroutput.InlineReceiptPlanV2{
		Authorization: ports.AttachedWorkerSealedInputAuthorization{
			TenantID: attempt.TenantID, OwnerUserID: attempt.OwnerUserID, WorkerID: attempt.WorkerID,
			ConnectionID: attempt.ConnectionID, EnrollmentGeneration: attempt.EnrollmentGeneration,
			ConnectionGeneration: attempt.ConnectionGeneration, RunID: attempt.RunID, AttemptID: attempt.AttemptID,
			AttemptSequence: 1, LeaseID: attempt.LeaseID, LeaseGeneration: attempt.LeaseGeneration,
			FenceToken: attempt.FenceToken, LeaseExpiresAtUnixMicro: attempt.LeaseExpiresAt.UnixMicro(),
			ContextDigest: attempt.ContextDigest, CapabilityDigest: attempt.CapabilityDigest, PolicyDigest: attempt.PolicyDigest,
			ExpectedAttemptRevision: 0,
		},
		Nonce: "inline-checkpoint-test-nonce", Fingerprint: strings.Repeat("a", 64),
		Producer: attachedworkeroutput.InlineProducerBindingV2{
			SessionID: job.SessionID, HarnessBindingDigest: harnessDigest, ReservationID: job.ReservationID,
			AcceptedAt: attempt.CreatedAt, SchemaDigest: strings.Repeat("b", 64),
			FinalDigest: strings.Repeat("c", 64), AnswerDigest: strings.Repeat("d", 64),
		},
		Artifacts: []attachedworkeroutput.InlineArtifactV2{{
			Name: "answer.json", MediaType: "application/json", Size: 2, SHA256: strings.Repeat("e", 64),
		}},
		ObservationDigest: strings.Repeat("f", 64), ExpiresAt: attempt.CreatedAt.Add(30 * time.Second),
		WorkerAttemptSequence: attempt.WorkerAttemptSequence, PlatformAttemptSequence: attempt.PlatformAttemptSequence,
		TerminalSequence: 1,
	}
	digest, err := plan.Digest()
	if err != nil {
		t.Fatalf("digest synthetic declaration metadata: %v", err)
	}
	return attachedworkeroutput.InlineReceiptCheckpointV2{
		Version: 2, Plan: plan, PlanSHA256: digest,
		Progress: attachedworkeroutput.InlineReceiptProgressV2{Revision: 1, PrepareBurned: true},
	}
}

func encodeInlineReceiptCheckpointForSessionV2(t *testing.T, value attachedworkeroutput.InlineReceiptCheckpointV2) []byte {
	t.Helper()
	encoded, err := attachedworkeroutput.EncodeInlineCheckpointV2(value)
	if err != nil {
		t.Fatalf("encode inline checkpoint: %v", err)
	}
	return encoded
}

func loadInlineReceiptCheckpointForSessionV2(t *testing.T, session *Session) ([]byte, attachedworkeroutput.InlineReceiptCheckpointV2) {
	t.Helper()
	lease, ok := session.lease.(receiptCheckpointLease)
	if !ok {
		t.Fatal("claimed Session does not own the receipt checkpoint runtime lease")
	}
	stored, err := lease.LoadReceiptCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("load durable receipt checkpoint: %v", err)
	}
	defer clear(stored.Payload)
	decoded, err := attachedworkeroutput.DecodeInlineCheckpointV2(stored.Payload)
	if err != nil {
		t.Fatalf("decode durable receipt checkpoint: %v", err)
	}
	return bytes.Clone(stored.Payload), decoded
}

func TestInlineReceiptCheckpointV2SessionJournalLifecycle(t *testing.T) {
	session, _, attempt, job, _ := nativeClaimedSession(t)
	initial := inlineReceiptCheckpointForSessionV2(t, attempt, job)
	initialBytes := encodeInlineReceiptCheckpointForSessionV2(t, initial)
	ctx := context.Background()
	if err := session.SealInlineReceiptSubmissionV2(ctx, initialBytes); err != nil {
		t.Fatalf("seal initial declaration: %v", err)
	}

	body := initial
	body.Progress.Revision++
	body.Progress.BodyBurned = true
	bodyBytes := encodeInlineReceiptCheckpointForSessionV2(t, body)
	if applied, err := session.AdvanceInlineReceiptSubmissionV2(ctx, initialBytes, bodyBytes); err != nil || !applied {
		t.Fatalf("first body-burn CAS applied=%v err=%v", applied, err)
	}
	if applied, err := session.AdvanceInlineReceiptSubmissionV2(ctx, initialBytes, bodyBytes); err != nil || applied {
		t.Fatalf("replayed body-burn CAS reminted a token: applied=%v err=%v", applied, err)
	}
	_, persisted := loadInlineReceiptCheckpointForSessionV2(t, session)
	if !persisted.Progress.BodyBurned || persisted.Progress.Revision != 2 || persisted.PlanSHA256 != initial.PlanSHA256 {
		t.Fatalf("durable body-burn checkpoint has unexpected state: %+v", persisted.Progress)
	}

	reconciled := body
	reconciled.Progress.Revision++
	reconciled.Progress.ReconcileBurned = true
	reconciledBytes := encodeInlineReceiptCheckpointForSessionV2(t, reconciled)
	if applied, err := session.AdvanceInlineReceiptSubmissionV2(ctx, bodyBytes, reconciledBytes); err != nil || !applied {
		t.Fatalf("reconcile-burn CAS applied=%v err=%v", applied, err)
	}
	canonical := reconciled
	canonical.Progress.Revision++
	canonical.Progress.CanonicalDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("1", 64))
	canonicalBytes := encodeInlineReceiptCheckpointForSessionV2(t, canonical)
	if applied, err := session.AdvanceInlineReceiptSubmissionV2(ctx, reconciledBytes, canonicalBytes); err != nil || !applied {
		t.Fatalf("canonical-digest CAS applied=%v err=%v", applied, err)
	}
	acked := canonical
	// The ACK bit is metadata in this journal test, not evidence of a real ACK.
	acked.Progress.Revision++
	acked.Progress.TerminalAcked = true
	ackedBytes := encodeInlineReceiptCheckpointForSessionV2(t, acked)
	if applied, err := session.AdvanceInlineReceiptSubmissionV2(ctx, canonicalBytes, ackedBytes); err != nil || !applied {
		t.Fatalf("terminal-ACK metadata CAS applied=%v err=%v", applied, err)
	}
	if err := session.RetireInlineReceiptSubmissionV2(ctx, ackedBytes); err != nil {
		t.Fatalf("retire acknowledged metadata checkpoint: %v", err)
	}
	lease, ok := session.lease.(receiptCheckpointLease)
	if !ok {
		t.Fatal("claimed Session lost its receipt checkpoint runtime lease")
	}
	if _, err := lease.LoadReceiptCheckpoint(ctx); !errors.Is(err, attachedworkerlocal.ErrStateMissing) {
		t.Fatalf("retired checkpoint remains present: %v", err)
	}
}

func TestInlineReceiptCheckpointV2SessionJournalRejectsForeignAndNonInitialSeal(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*attachedworkeroutput.InlineReceiptCheckpointV2)
	}{
		{name: "foreign connection", mutate: func(value *attachedworkeroutput.InlineReceiptCheckpointV2) {
			value.Plan.Authorization.ConnectionID = "foreign-connection"
		}},
		{name: "foreign generation", mutate: func(value *attachedworkeroutput.InlineReceiptCheckpointV2) {
			value.Plan.Authorization.ConnectionGeneration++
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			session, _, attempt, job, _ := nativeClaimedSession(t)
			value := inlineReceiptCheckpointForSessionV2(t, attempt, job)
			testCase.mutate(&value)
			digest, err := value.Plan.Digest()
			if err != nil {
				t.Fatalf("digest foreign metadata: %v", err)
			}
			value.PlanSHA256 = digest
			payload := encodeInlineReceiptCheckpointForSessionV2(t, value)
			if err := session.SealInlineReceiptSubmissionV2(context.Background(), payload); !errors.Is(err, ErrSessionFenced) {
				t.Fatalf("foreign scope seal error=%v, want session fenced", err)
			}
		})
	}

	t.Run("non-initial progress", func(t *testing.T) {
		session, _, attempt, job, _ := nativeClaimedSession(t)
		value := inlineReceiptCheckpointForSessionV2(t, attempt, job)
		value.Progress.Revision = 2
		value.Progress.BodyBurned = true
		payload := encodeInlineReceiptCheckpointForSessionV2(t, value)
		if err := session.SealInlineReceiptSubmissionV2(context.Background(), payload); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("non-initial seal error=%v, want invalid configuration", err)
		}
	})
}

func TestInlineReceiptCheckpointV2SessionJournalRejectsRollbackAndDivergence(t *testing.T) {
	session, _, attempt, job, _ := nativeClaimedSession(t)
	initial := inlineReceiptCheckpointForSessionV2(t, attempt, job)
	initialBytes := encodeInlineReceiptCheckpointForSessionV2(t, initial)
	if err := session.SealInlineReceiptSubmissionV2(context.Background(), initialBytes); err != nil {
		t.Fatal(err)
	}
	body := initial
	body.Progress.Revision++
	body.Progress.BodyBurned = true
	bodyBytes := encodeInlineReceiptCheckpointForSessionV2(t, body)
	if applied, err := session.AdvanceInlineReceiptSubmissionV2(context.Background(), initialBytes, bodyBytes); err != nil || !applied {
		t.Fatalf("seed body-burn state applied=%v err=%v", applied, err)
	}
	if applied, err := session.AdvanceInlineReceiptSubmissionV2(context.Background(), bodyBytes, initialBytes); err == nil || applied {
		t.Fatalf("backward checkpoint accepted: applied=%v err=%v", applied, err)
	}
	divergent := body
	divergent.Progress.Revision++
	divergent.Progress.ReconcileBurned = true
	divergent.Plan.Nonce = "divergent-inline-checkpoint-nonce"
	divergent.PlanSHA256, _ = divergent.Plan.Digest()
	divergentBytes := encodeInlineReceiptCheckpointForSessionV2(t, divergent)
	if applied, err := session.AdvanceInlineReceiptSubmissionV2(context.Background(), bodyBytes, divergentBytes); err == nil || applied {
		t.Fatalf("divergent plan update accepted: applied=%v err=%v", applied, err)
	}
	persistedBytes, persisted := loadInlineReceiptCheckpointForSessionV2(t, session)
	if !bytes.Equal(persistedBytes, bodyBytes) || !persisted.Progress.BodyBurned || persisted.Progress.ReconcileBurned {
		t.Fatalf("denied updates changed durable checkpoint: %+v", persisted.Progress)
	}
}

func TestInlineReceiptCheckpointV2SessionJournalRequiresAckBeforeRetirement(t *testing.T) {
	session, _, attempt, job, _ := nativeClaimedSession(t)
	initial := inlineReceiptCheckpointForSessionV2(t, attempt, job)
	initialBytes := encodeInlineReceiptCheckpointForSessionV2(t, initial)
	if err := session.SealInlineReceiptSubmissionV2(context.Background(), initialBytes); err != nil {
		t.Fatal(err)
	}
	if err := session.RetireInlineReceiptSubmissionV2(context.Background(), initialBytes); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("retire before ACK error=%v, want invalid configuration", err)
	}
	persistedBytes, _ := loadInlineReceiptCheckpointForSessionV2(t, session)
	if !bytes.Equal(persistedBytes, initialBytes) {
		t.Fatal("denied early retirement changed the durable checkpoint")
	}
}

func TestInlineReceiptCheckpointV2SessionCloseFencesAdvanceAndPreservesFile(t *testing.T) {
	session, fixture, attempt, job, _ := nativeClaimedSession(t)
	initial := inlineReceiptCheckpointForSessionV2(t, attempt, job)
	initialBytes := encodeInlineReceiptCheckpointForSessionV2(t, initial)
	if err := session.SealInlineReceiptSubmissionV2(context.Background(), initialBytes); err != nil {
		t.Fatal(err)
	}
	next := initial
	next.Progress.Revision++
	next.Progress.BodyBurned = true
	nextBytes := encodeInlineReceiptCheckpointForSessionV2(t, next)
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("close claimed Session: %v", err)
	}
	if applied, err := session.AdvanceInlineReceiptSubmissionV2(context.Background(), initialBytes, nextBytes); err == nil || applied {
		t.Fatalf("closed Session advanced checkpoint: applied=%v err=%v", applied, err)
	}

	lease, err := fixture.store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatalf("reopen owned local runtime lease: %v", err)
	}
	t.Cleanup(func() {
		if err := lease.Close(); err != nil {
			t.Errorf("close reopened local runtime lease: %v", err)
		}
	})
	stored, err := lease.LoadReceiptCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("read preserved checkpoint after Session close: %v", err)
	}
	defer clear(stored.Payload)
	if !bytes.Equal(stored.Payload, initialBytes) {
		t.Fatal("Session close removed or advanced the durable checkpoint")
	}
	if _, err := attachedworkeroutput.DecodeInlineCheckpointV2(stored.Payload); err != nil {
		t.Fatalf("preserved checkpoint became unreadable: %v", err)
	}
}
