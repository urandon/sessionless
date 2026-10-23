package attachedworkerdaemontransport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// These are metadata/CAS orchestration fixtures, not authentic native producer
// evidence. The production entry must obtain that separately from pinned Stack.
type inlineJournalFixtureV2 struct {
	*fakeSession
	payload                   []byte
	advances, retires         int
	applied                   bool
	err                       error
	cancelCallerBeforeAdvance context.CancelFunc
}

func (j *inlineJournalFixtureV2) SealInlineReceiptSubmissionV2(_ context.Context, payload []byte) error {
	if len(j.payload) != 0 {
		return ErrReconciliationRequired
	}
	j.payload = bytes.Clone(payload)
	return nil
}

func (j *inlineJournalFixtureV2) AdvanceInlineReceiptSubmissionV2(ctx context.Context, previous, next []byte) (bool, error) {
	j.advances++
	if j.cancelCallerBeforeAdvance != nil {
		j.cancelCallerBeforeAdvance()
		j.cancelCallerBeforeAdvance = nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if !bytes.Equal(j.payload, previous) {
		return false, ErrReconciliationRequired
	}
	if !j.applied || j.err != nil {
		return j.applied, j.err
	}
	j.payload = bytes.Clone(next)
	return true, nil
}

func (j *inlineJournalFixtureV2) RetireInlineReceiptSubmissionV2(_ context.Context, payload []byte) error {
	j.retires++
	v, err := attachedworkeroutput.DecodeInlineCheckpointV2(payload)
	if err != nil || !v.Progress.TerminalAcked || !bytes.Equal(j.payload, payload) {
		return ErrReconciliationRequired
	}
	clear(j.payload)
	j.payload = nil
	return nil
}

type inlinePublisherFixtureV2 struct {
	prepares, bodies, reconciles int
	response                     attachedworkeroutput.AttachedWorkerInlineResultV2
	err                          error
	lastReconcile                attachedworkeroutput.AttachedWorkerInlineReconcileV2
	reconcileDeadline            time.Time
}

func (p *inlinePublisherFixtureV2) Prepare(context.Context, attachedworkeroutput.AttachedWorkerInlinePrepareV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	p.prepares++
	return p.response, p.err
}
func (p *inlinePublisherFixtureV2) Submit(context.Context, attachedworkeroutput.AttachedWorkerInlineBodyV2, int64, io.Reader) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	p.bodies++
	return p.response, p.err
}
func (p *inlinePublisherFixtureV2) Reconcile(ctx context.Context, input attachedworkeroutput.AttachedWorkerInlineReconcileV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	p.reconciles++
	p.lastReconcile = input
	p.reconcileDeadline, _ = ctx.Deadline()
	return p.response, p.err
}

func TestInlineReceiptV2ExpiredWindowRecoversOnlyReadyMetadata(t *testing.T) {
	for _, stage := range []attachedworkeroutput.AttachedWorkerInlineStageV2{attachedworkeroutput.InlineReadyV2, attachedworkeroutput.InlinePreparedV2} {
		t.Run(string(stage), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a, state, _, publisher := inlineMetadataFixtureV2(t)
				originalExpiry := state.checkpoint.Plan.ExpiresAt
				publisher.response.Stage = stage
				if stage == attachedworkeroutput.InlinePreparedV2 {
					publisher.response.CanonicalDigest = ""
					publisher.response.Capability = strings.Repeat("c", 64)
				}
				// Virtual time only: no wall-clock sleep or external operation.
				time.Sleep(16 * time.Second)
				evidence, err := a.reconcileInlineReceiptV2(context.Background(), state)
				if stage == attachedworkeroutput.InlineReadyV2 && (err != nil || len(evidence) != 32) {
					t.Fatalf("lost READY response cannot recover after original export expiry: %v", err)
				}
				if stage != attachedworkeroutput.InlineReadyV2 && err == nil {
					t.Fatal("expired metadata recovery restored body capability")
				}
				if publisher.reconciles != 1 || !publisher.lastReconcile.BodySendBurned || publisher.prepares != 0 || publisher.bodies != 0 ||
					!state.checkpoint.Plan.ExpiresAt.Equal(originalExpiry) || !publisher.reconcileDeadline.After(time.Now()) || publisher.reconcileDeadline.After(time.Now().Add(15*time.Second)) {
					t.Fatal("post-expiry metadata recovery renewed export authority, changed plan, or lacked finite bound")
				}
			})
		})
	}
}

// A child's deadline timer may publish cancellation before its parent's timer
// at the same absolute deadline. Model only that pending-notification instant:
// Deadline has elapsed, while the caller has not yet observed Done or Err.
// This is a context scheduling fixture, never execution/producer authority.
type inlineDeadlineNotificationPendingV2 struct {
	context.Context
	deadline time.Time
}

func (c inlineDeadlineNotificationPendingV2) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func TestInlineReceiptV2ElapsedCallerDeadlinePreservesRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, state, journal, publisher := inlineMetadataFixtureV2(t)
		originalPlan := state.checkpoint.PlanSHA256
		originalExpiry := state.checkpoint.Plan.ExpiresAt
		// Advance the single virtual authority past the original export window.
		time.Sleep(16 * time.Second)
		parent, publishCancellation := context.WithCancel(context.Background())
		defer publishCancellation()
		pending := inlineDeadlineNotificationPendingV2{Context: parent, deadline: originalExpiry}
		// Publish the pending notification at the next actual CAS entry. This
		// reproduces the child-first/parent-later cancellation order without
		// scheduler luck. No CAS should be attempted for this elapsed caller.
		journal.cancelCallerBeforeAdvance = publishCancellation
		if pending.Err() != nil || time.Now().Before(originalExpiry) {
			t.Fatal("fixture did not establish elapsed deadline before cancellation publication")
		}
		if _, err := a.reconcileInlineReceiptV2(pending, state); !errors.Is(err, ErrReconciliationRequired) {
			t.Fatalf("elapsed caller must return reconciliation: %v", err)
		}
		if journal.advances != 0 || state.blocked || state.checkpoint.Progress.ReconcileBurned || publisher.reconciles != 0 || publisher.prepares != 0 || publisher.bodies != 0 {
			t.Fatalf("elapsed caller spent recovery before CAS authority: advances=%d blocked=%v burned=%v calls=%d/%d/%d", journal.advances, state.blocked, state.checkpoint.Progress.ReconcileBurned, publisher.prepares, publisher.bodies, publisher.reconciles)
		}
		journal.cancelCallerBeforeAdvance = nil
		fresh, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		evidence, err := a.reconcileInlineReceiptV2(fresh, state)
		if err != nil || len(evidence) != 32 || publisher.reconciles != 1 || publisher.prepares != 0 || publisher.bodies != 0 || state.blocked || state.checkpoint.PlanSHA256 != originalPlan || !state.checkpoint.Plan.ExpiresAt.Equal(originalExpiry) || !publisher.lastReconcile.BodySendBurned {
			t.Fatalf("fresh caller failed original metadata-only recovery: evidence=%d calls=%d/%d/%d blocked=%v err=%v", len(evidence), publisher.prepares, publisher.bodies, publisher.reconciles, state.blocked, err)
		}
	})
}

func inlineMetadataFixtureV2(t *testing.T) (*Adapter, *inlineReceiptAttemptV2, *inlineJournalFixtureV2, *inlinePublisherFixtureV2) {
	t.Helper()
	now := time.Now().UTC()
	fence, err := domain.NewAttachedWorkerFenceTokenV1("tenant", "owner", "worker", "run", "attempt", "lease", 1)
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	plan := attachedworkeroutput.InlineReceiptPlanV2{
		Authorization: ports.AttachedWorkerSealedInputAuthorization{TenantID: "tenant", OwnerUserID: "owner", WorkerID: "worker", ConnectionID: "connection",
			EnrollmentGeneration: 1, ConnectionGeneration: 1, RunID: "run", AttemptID: "attempt", AttemptSequence: 1, LeaseID: "lease", LeaseGeneration: 1,
			FenceToken: fence, LeaseExpiresAtUnixMicro: now.Add(time.Minute).UnixMicro(), ContextDigest: domain.AttachedWorkerContextDigest(digest), CapabilityDigest: domain.AttachedWorkerCapabilityDigest(digest), PolicyDigest: domain.AttachedWorkerPolicyDigest(digest)},
		Nonce: "nonce", Fingerprint: digest, ObservationDigest: digest, ExpiresAt: now.Add(15 * time.Second),
		Producer:              attachedworkeroutput.InlineProducerBindingV2{SessionID: "session", ReservationID: "reservation", HarnessBindingDigest: domain.HarnessBindingDigestV2(digest), AcceptedAt: now, SchemaDigest: digest, FinalDigest: digest, AnswerDigest: digest},
		WorkerAttemptSequence: 2, PlatformAttemptSequence: 3, TerminalSequence: 1,
	}
	planDigest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	state := &inlineReceiptAttemptV2{checkpoint: attachedworkeroutput.InlineReceiptCheckpointV2{Version: 2, Plan: plan, PlanSHA256: planDigest, Progress: attachedworkeroutput.InlineReceiptProgressV2{Revision: 1, PrepareBurned: true}}}
	payload, err := attachedworkeroutput.EncodeInlineCheckpointV2(state.checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	authExpires := now.Add(time.Hour)
	j := &inlineJournalFixtureV2{fakeSession: &fakeSession{snapshot: attachedworkersession.SnapshotV1{Version: 1, State: attachedworkersession.StateReady,
		TenantID: "tenant", OwnerUserID: "owner", WorkerID: "worker", ConnectionID: "connection", EnrollmentGeneration: 1, ConnectionGeneration: 1,
		ProtocolVersion: 1, CapabilityDigest: plan.Authorization.CapabilityDigest, AuthenticationExpires: &authExpires}}, payload: payload, applied: true}
	p := &inlinePublisherFixtureV2{response: attachedworkeroutput.AttachedWorkerInlineResultV2{Version: 2, Stage: attachedworkeroutput.InlineReadyV2, Nonce: plan.Nonce, Fingerprint: plan.Fingerprint, ExpiresAt: plan.ExpiresAt, CanonicalDigest: domain.AttachedWorkerTerminalEvidenceDigest(digest)}}
	t.Cleanup(func() { clear(j.payload) })
	return &Adapter{session: j, config: Config{Now: time.Now, InlineReceiptPublisher: p, Profile: LocalProfileV1{CapabilityDigest: plan.Authorization.CapabilityDigest}}}, state, j, p
}

func TestInlineReceiptV2LostPrepareReconcilesOnceAndRetiresAfterAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, state, journal, publisher := inlineMetadataFixtureV2(t)
		evidence, err := a.reconcileInlineReceiptV2(context.Background(), state)
		if err != nil || len(evidence) != 32 || publisher.reconciles != 1 || publisher.bodies != 0 || publisher.prepares != 0 ||
			!state.checkpoint.Progress.ReconcileBurned || state.checkpoint.Progress.CanonicalDigest == "" || journal.retires != 0 {
			t.Fatalf("answer-only metadata recovery evidence=%d calls=%d/%d/%d retires=%d err=%v", len(evidence), publisher.prepares, publisher.bodies, publisher.reconciles, journal.retires, err)
		}
		if _, err := a.reconcileInlineReceiptV2(context.Background(), state); err == nil || publisher.reconciles != 1 {
			t.Fatal("metadata recovery reminted reconciliation")
		}
		if err := a.ackInlineReceiptV2(context.Background(), &activeAttempt{inline: state}); err != nil || journal.retires != 1 || !state.checkpoint.Progress.TerminalAcked {
			t.Fatalf("ACK retirement: %v retires=%d", err, journal.retires)
		}
		if err := a.ackInlineReceiptV2(context.Background(), &activeAttempt{inline: state}); err != nil || journal.retires != 1 {
			t.Fatal("repeated observed retirement performed another removal")
		}
	})
}

func TestInlineReceiptV2CASLoserAndAmbiguitySendNothing(t *testing.T) {
	for _, kind := range []string{"observed", "failed", "ambiguous", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a, state, journal, publisher := inlineMetadataFixtureV2(t)
				journal.applied = false
				ctx := context.Background()
				switch kind {
				case "failed":
					journal.err = errors.New("owned fixture write failed")
				case "ambiguous":
					journal.applied, journal.err = true, errors.New("owned fixture post-write ambiguity")
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				if _, err := a.reconcileInlineReceiptV2(ctx, state); err == nil || publisher.reconciles != 0 || publisher.bodies != 0 || publisher.prepares != 0 {
					t.Fatal("CAS loser/ambiguous/cancelled path performed network effect")
				}
			})
		})
	}
}

func TestInlineReceiptV2ForeignReadyAndPublicPreparedCannotPublish(t *testing.T) {
	for _, kind := range []string{"foreign-nonce", "foreign-fingerprint", "changed-expiry", "bad-digest", "capability-on-ready", "prepared-without-original"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a, state, journal, publisher := inlineMetadataFixtureV2(t)
				r := publisher.response
				switch kind {
				case "foreign-nonce":
					r.Nonce = "foreign"
				case "foreign-fingerprint":
					r.Fingerprint = strings.Repeat("b", 64)
				case "changed-expiry":
					r.ExpiresAt = r.ExpiresAt.Add(time.Nanosecond)
				case "bad-digest":
					r.CanonicalDigest = "not-a-digest"
				case "capability-on-ready":
					r.Capability = strings.Repeat("c", 64)
				case "prepared-without-original":
					r.Stage, r.CanonicalDigest, r.Capability = attachedworkeroutput.InlinePreparedV2, "", strings.Repeat("c", 64)
				}
				if _, err := a.resolveInlineReceiptV2(context.Background(), state, r); err == nil || journal.advances != 0 || journal.retires != 0 || publisher.bodies != 0 {
					t.Fatal("public/foreign metadata acquired receipt progress or original body")
				}
			})
		})
	}
}
