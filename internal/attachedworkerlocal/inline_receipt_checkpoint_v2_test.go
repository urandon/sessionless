package attachedworkerlocal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func inlineCheckpointFixtureV2(t *testing.T, maximum ...bool) (*Store, *RuntimeLease, ReceiptCheckpointV1, attachedworkeroutput.InlineReceiptCheckpointV2) {
	t.Helper()
	store, manifest, secret := newFixture(t)
	maximal := len(maximum) != 0 && maximum[0]
	if maximal {
		id := strings.Repeat("x", 160)
		manifest.TenantID, manifest.OwnerUserID, manifest.WorkerID = domain.TenantID(id), domain.UserID(id), domain.AttachedWorkerID(id)
		manifest.EnrollmentGeneration = math.MaxUint64 - 1
		secret.TenantID, secret.OwnerUserID, secret.WorkerID, secret.EnrollmentGeneration = manifest.TenantID, manifest.OwnerUserID, manifest.WorkerID, manifest.EnrollmentGeneration
	}
	if err := store.Initialize(context.Background(), manifest, secret); err != nil {
		t.Fatal(err)
	}
	next, nextSecret := manifest, secret
	next.Revision, next.ConnectionGeneration, next.UpdatedAt = 2, 1, testTime.Add(time.Second)
	nextSecret.ManifestRevision, nextSecret.ConnectionGeneration, nextSecret.ConnectionSecret = 2, 1, bytes.Repeat([]byte{0x41}, 32)
	if err := store.Update(context.Background(), manifest.Revision, next, nextSecret); err != nil {
		t.Fatal(err)
	}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(next.TenantID, next.OwnerUserID, next.WorkerID, "inline-run", "inline-attempt", "inline-lease", 1)
	if err != nil {
		t.Fatal(err)
	}
	plan := attachedworkeroutput.InlineReceiptPlanV2{
		Authorization: ports.AttachedWorkerSealedInputAuthorization{TenantID: next.TenantID, OwnerUserID: next.OwnerUserID, WorkerID: next.WorkerID,
			ConnectionID: "inline-connection", EnrollmentGeneration: next.EnrollmentGeneration, ConnectionGeneration: next.ConnectionGeneration,
			RunID: "inline-run", AttemptID: "inline-attempt", AttemptSequence: 1, LeaseID: "inline-lease", LeaseGeneration: 1, FenceToken: fence,
			LeaseExpiresAtUnixMicro: testTime.Add(time.Minute).UnixMicro(), ContextDigest: domain.AttachedWorkerContextDigest(strings.Repeat("a", 64)),
			CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("b", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("c", 64))},
		Nonce: "inline-nonce", Fingerprint: strings.Repeat("d", 64), ObservationDigest: strings.Repeat("e", 64), ExpiresAt: testTime.Add(15 * time.Second),
		Producer: attachedworkeroutput.InlineProducerBindingV2{SessionID: "inline-session", HarnessBindingDigest: domain.HarnessBindingDigestV2(strings.Repeat("f", 64)),
			ReservationID: "inline-reservation", AcceptedAt: testTime, SchemaDigest: strings.Repeat("1", 64), FinalDigest: strings.Repeat("2", 64), AnswerDigest: strings.Repeat("3", 64)},
		Artifacts:             []attachedworkeroutput.InlineArtifactV2{{Name: "data.json", MediaType: "application/json", Size: 2, SHA256: strings.Repeat("4", 64)}},
		WorkerAttemptSequence: 2, PlatformAttemptSequence: 3, TerminalSequence: 1,
	}
	if maximal {
		id := strings.Repeat("x", 160)
		a := &plan.Authorization
		a.RunID, a.AttemptID, a.LeaseID, a.ConnectionID = domain.RunID(id), domain.AttemptID(id), domain.LeaseID(id), domain.AttachedWorkerConnectionID(id)
		a.LeaseGeneration = math.MaxUint64 - 1
		a.FenceToken, err = domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
		if err != nil {
			t.Fatal(err)
		}
		plan.Nonce, plan.Producer.SessionID, plan.Producer.ReservationID = domain.IdempotencyKey(id), domain.SessionID(id), domain.QuotaReservationID(id)
		plan.WorkerAttemptSequence, plan.PlatformAttemptSequence = math.MaxUint64-1, math.MaxUint64-1
		plan.Artifacts = []attachedworkeroutput.InlineArtifactV2{
			{Name: "a" + strings.Repeat("<", 254), MediaType: "application/json", Size: attachedworkeroutput.MaxInlineArtifactBytesV2, SHA256: strings.Repeat("a", 64)},
			{Name: "b" + strings.Repeat("<", 254), MediaType: "text/plain", Size: attachedworkeroutput.MaxInlineArtifactBytesV2, SHA256: strings.Repeat("b", 64)},
		}
	}
	digest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	value := attachedworkeroutput.InlineReceiptCheckpointV2{Version: 2, Plan: plan, PlanSHA256: digest,
		Progress: attachedworkeroutput.InlineReceiptProgressV2{Revision: 1, PrepareBurned: true}}
	record := ReceiptCheckpointV1{Version: ReceiptCheckpointVersionV1, ManifestRevision: next.Revision, TenantID: next.TenantID, OwnerUserID: next.OwnerUserID,
		WorkerID: next.WorkerID, EnrollmentGeneration: next.EnrollmentGeneration, ConnectionGeneration: next.ConnectionGeneration, ConnectionID: plan.Authorization.ConnectionID}
	record = inlineCheckpointRecordV2(t, record, value)
	lease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Close(); err != nil {
			t.Errorf("close owned runtime: %v", err)
		}
	})
	return store, lease, record, value
}

func inlineCheckpointRecordV2(t *testing.T, scope ReceiptCheckpointV1, value attachedworkeroutput.InlineReceiptCheckpointV2) ReceiptCheckpointV1 {
	t.Helper()
	encoded, err := attachedworkeroutput.EncodeInlineCheckpointV2(value)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	scope.Payload, scope.PayloadSHA256 = encoded, hex.EncodeToString(digest[:])
	return scope
}

func TestInlineReceiptCheckpointV2CASPreservesOriginalPlanAndRestartFence(t *testing.T) {
	store, lease, initial, value := inlineCheckpointFixtureV2(t)
	if err := lease.PersistInlineReceiptCheckpointV2(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	if err := lease.RetireReceiptCheckpoint(context.Background(), initial); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("generic legacy route retired V2 before ACK: %v", err)
	}
	value.Progress.Revision++
	value.Progress.BodyBurned = true
	next := inlineCheckpointRecordV2(t, initial, value)
	if err := lease.PersistReceiptCheckpoint(context.Background(), next); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("V1 immutable seal allowed overwrite: %v", err)
	}
	if applied, err := lease.AdvanceInlineReceiptCheckpointV2(context.Background(), initial, next); err != nil || !applied {
		t.Fatalf("first CAS applied=%v err=%v", applied, err)
	}
	if applied, err := lease.AdvanceInlineReceiptCheckpointV2(context.Background(), initial, next); err != nil || applied {
		t.Fatalf("idempotent CAS reminted send token: applied=%v err=%v", applied, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	loaded, err := restarted.LoadReceiptCheckpoint(context.Background())
	if err != nil || !bytes.Equal(loaded.Payload, next.Payload) {
		t.Fatalf("restart lost burn: %v", err)
	}
	decoded, err := attachedworkeroutput.DecodeInlineCheckpointV2(loaded.Payload)
	if err != nil || !decoded.Progress.BodyBurned || decoded.PlanSHA256 != value.PlanSHA256 {
		t.Fatalf("restart changed immutable identity/progress: %v", err)
	}
	if applied, err := restarted.AdvanceInlineReceiptCheckpointV2(context.Background(), next, initial); err == nil || applied {
		t.Fatal("restart rolled back consumed body")
	}
	info, err := os.Stat(store.path(ReceiptCheckpointFileName))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("checkpoint mode err=%v", err)
	}
}

func TestInlineReceiptCheckpointV2ConcurrentCASHasOneWinner(t *testing.T) {
	_, lease, initial, value := inlineCheckpointFixtureV2(t)
	if err := lease.PersistInlineReceiptCheckpointV2(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	value.Progress.BodyBurned, value.Progress.Revision = true, 2
	next := inlineCheckpointRecordV2(t, initial, value)
	start := make(chan struct{})
	type outcome struct {
		applied bool
		err     error
	}
	done := make(chan outcome, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			<-start
			applied, err := lease.AdvanceInlineReceiptCheckpointV2(context.Background(), initial, next)
			done <- outcome{applied, err}
		})
	}
	close(start)
	workers.Wait()
	close(done)
	winners := 0
	for result := range done {
		// The existing state lock is deliberately nonblocking. A concurrent
		// loser may observe busy rather than the already-committed next value;
		// neither observation may grant a send token.
		if result.err != nil && !errors.Is(result.err, ErrStateBusy) {
			t.Errorf("CAS contender: %v", result.err)
		}
		if result.err != nil && result.applied {
			t.Error("failed contender acquired transition")
		}
		if result.applied {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("newly applied CAS count=%d want1", winners)
	}
	if applied, err := lease.AdvanceInlineReceiptCheckpointV2(context.Background(), initial, next); err != nil || applied {
		t.Fatalf("settled contender reminted token applied=%v err=%v", applied, err)
	}
}

func TestInlineReceiptCheckpointV2RejectsForeignLegacyAndDivergentUpdates(t *testing.T) {
	for _, kind := range []string{"foreign-scope", "changed-plan", "legacy", "initial-progress", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			_, lease, initial, value := inlineCheckpointFixtureV2(t)
			if kind == "foreign-scope" {
				foreign := initial
				foreign.ConnectionID = "foreign"
				if err := lease.PersistInlineReceiptCheckpointV2(context.Background(), foreign); err == nil {
					t.Fatal("foreign plan scope sealed")
				}
				return
			}
			if kind == "initial-progress" {
				value.Progress.Revision, value.Progress.BodyBurned = 2, true
				if err := lease.PersistInlineReceiptCheckpointV2(context.Background(), inlineCheckpointRecordV2(t, initial, value)); err == nil {
					t.Fatal("non-initial progress sealed as first result")
				}
				return
			}
			if kind == "legacy" {
				initial.Payload = []byte(`{"legacy":"bounded result"}`)
				digest := sha256.Sum256(initial.Payload)
				initial.PayloadSHA256 = hex.EncodeToString(digest[:])
			}
			if err := lease.PersistReceiptCheckpoint(context.Background(), initial); err != nil {
				t.Fatal(err)
			}
			value.Progress.Revision, value.Progress.BodyBurned = 2, true
			if kind == "changed-plan" {
				value.Plan.Nonce = "other-nonce"
				value.PlanSHA256, _ = value.Plan.Digest()
			}
			next := inlineCheckpointRecordV2(t, initial, value)
			ctx := context.Background()
			if kind == "cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if applied, err := lease.AdvanceInlineReceiptCheckpointV2(ctx, initial, next); err == nil || applied {
				t.Fatal("denied CAS applied")
			}
			loaded, err := lease.LoadReceiptCheckpoint(context.Background())
			if err != nil || !bytes.Equal(loaded.Payload, initial.Payload) {
				t.Fatalf("denied CAS changed durable checkpoint: %v", err)
			}
		})
	}
}

func TestInlineReceiptCheckpointV2LegacySealCannotSkipProgress(t *testing.T) {
	_, lease, initial, value := inlineCheckpointFixtureV2(t)
	value.Progress.Revision, value.Progress.BodyBurned = 4, true
	value.Progress.CanonicalDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("a", 64))
	value.Progress.TerminalAcked = true
	forged := inlineCheckpointRecordV2(t, initial, value)
	if err := lease.PersistReceiptCheckpoint(context.Background(), forged); err == nil {
		t.Fatal("generic legacy seal inserted a V2 ACK without initial seal/CAS history")
	}
	if _, err := lease.LoadReceiptCheckpoint(context.Background()); !errors.Is(err, ErrStateMissing) {
		t.Fatalf("rejected first insert left a forged journal: %v", err)
	}
}

func TestInlineReceiptCheckpointV2AmbiguousSyncDoesNotGrantTransition(t *testing.T) {
	store, lease, initial, value := inlineCheckpointFixtureV2(t)
	if err := lease.PersistInlineReceiptCheckpointV2(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	value.Progress.BodyBurned, value.Progress.Revision = true, 2
	next := inlineCheckpointRecordV2(t, initial, value)
	store.syncRootOverride = func() error { return ErrLocalIO }
	t.Cleanup(func() { store.syncRootOverride = nil })
	if applied, err := lease.AdvanceInlineReceiptCheckpointV2(context.Background(), initial, next); applied || !errors.Is(err, ErrStateAmbiguous) {
		t.Fatalf("ambiguous directory sync granted transition applied=%v err=%v", applied, err)
	}
	store.syncRootOverride = nil
	loaded, err := lease.LoadReceiptCheckpoint(context.Background())
	if err != nil || !bytes.Equal(loaded.Payload, next.Payload) {
		t.Fatalf("visible post-rename fence lost: %v", err)
	}
	if applied, err := lease.AdvanceInlineReceiptCheckpointV2(context.Background(), initial, next); err != nil || applied {
		t.Fatalf("ambiguous transition observation reminted token applied=%v err=%v", applied, err)
	}
	if err := lease.PersistReceiptCheckpoint(context.Background(), initial); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("ambiguity permitted rollback: %v", err)
	}
}

// This is a real RuntimeLease/file durability leaf, not evidence of a native
// producer or an authenticated Session ACK. Its ACK bit is metadata advanced
// through the real CAS history solely to reach the retirement boundary.
func TestInlineReceiptCheckpointV2AmbiguousRemovalIsNotRetirement(t *testing.T) {
	store, lease, initial, value := inlineCheckpointFixtureV2(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := lease.PersistInlineReceiptCheckpointV2(ctx, initial); err != nil {
		t.Fatalf("seal original checkpoint: %v", err)
	}
	previous := initial
	for _, progress := range []attachedworkeroutput.InlineReceiptProgressV2{
		{Revision: 2, PrepareBurned: true, BodyBurned: true},
		{Revision: 3, PrepareBurned: true, BodyBurned: true, CanonicalDigest: domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("a", 64))},
		{Revision: 4, PrepareBurned: true, BodyBurned: true, CanonicalDigest: domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("a", 64)), TerminalAcked: true},
	} {
		value.Progress = progress
		next := inlineCheckpointRecordV2(t, initial, value)
		if applied, err := lease.AdvanceInlineReceiptCheckpointV2(ctx, previous, next); err != nil || !applied {
			t.Fatalf("establish retirement progress revision=%d applied=%v err=%v", progress.Revision, applied, err)
		}
		previous = next
	}
	acked := previous
	loaded, err := lease.LoadReceiptCheckpoint(ctx)
	if err != nil || !bytes.Equal(loaded.Payload, acked.Payload) {
		t.Fatalf("fully ACKed metadata was not persisted before fault: %v", err)
	}
	// Install the existing private fault only after every seal/CAS has finished.
	// The one failed sync must observe removeDurable's physical unlink first;
	// no write/rename or unrelated file operation receives this injected error.
	syncs, unlinkedAtSync := 0, false
	store.syncRootOverride = func() error {
		syncs++
		_, statErr := os.Stat(store.path(ReceiptCheckpointFileName))
		unlinkedAtSync = errors.Is(statErr, os.ErrNotExist)
		return ErrLocalIO
	}
	t.Cleanup(func() { store.syncRootOverride = nil })
	if err := lease.RetireReceiptCheckpoint(ctx, acked); !errors.Is(err, ErrStateAmbiguous) {
		t.Fatalf("post-unlink directory sync failure reported retirement success: %v", err)
	}
	if syncs != 1 || !unlinkedAtSync {
		t.Fatalf("fault did not isolate post-unlink sync: syncs=%d unlinked=%v", syncs, unlinkedAtSync)
	}
	if _, err := lease.LoadReceiptCheckpoint(ctx); !errors.Is(err, ErrStateMissing) {
		t.Fatalf("expected physical absence after ambiguous unlink: %v", err)
	}
	if err := lease.RetireReceiptCheckpoint(ctx, acked); !errors.Is(err, ErrStateConflict) || syncs != 1 {
		t.Fatalf("missing-file retry manufactured durable retirement: err=%v syncs=%d", err, syncs)
	}
	store.syncRootOverride = nil
	if err := lease.RetireReceiptCheckpoint(ctx, acked); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("restored sync converted physical absence to retirement success: %v", err)
	}
	// Absence is not a newly won body-send CAS token or permission to recreate
	// the original checkpoint. Replays must still require an exact prior row.
	body := value
	body.Progress = attachedworkeroutput.InlineReceiptProgressV2{Revision: 2, PrepareBurned: true, BodyBurned: true}
	if applied, err := lease.AdvanceInlineReceiptCheckpointV2(ctx, initial, inlineCheckpointRecordV2(t, initial, body)); applied || !errors.Is(err, ErrStateConflict) {
		t.Fatalf("physical absence reminted original CAS transition: applied=%v err=%v", applied, err)
	}
	if _, err := lease.LoadReceiptCheckpoint(ctx); !errors.Is(err, ErrStateMissing) {
		t.Fatalf("denied replay recreated checkpoint: %v", err)
	}
}

func TestInlineReceiptCheckpointV2MaximumOuterEnvelope(t *testing.T) {
	store, lease, initial, _ := inlineCheckpointFixtureV2(t, true)
	if err := lease.PersistInlineReceiptCheckpointV2(context.Background(), initial); err != nil {
		t.Fatalf("maximum complete envelope: %v", err)
	}
	encoded, err := store.readFileSecure(ReceiptCheckpointFileName)
	if err != nil || len(encoded) > attachedworkeroutput.MaxInlineCheckpointBytesV2 {
		t.Fatalf("maximum outer bytes=%d err=%v", len(encoded), err)
	}
	t.Logf("complete maximum-declaration local journal including base64/scope: %d bytes", len(encoded))
	loaded, err := lease.LoadReceiptCheckpoint(context.Background())
	if err != nil || !bytes.Equal(loaded.Payload, initial.Payload) {
		t.Fatalf("maximum complete envelope changed payload: %v", err)
	}
}

func TestInlineReceiptCheckpointV2CorruptFileCannotUseLegacyDecoder(t *testing.T) {
	for _, kind := range []string{"unknown-outer", "alias-outer", "duplicate-outer", "noncanonical-outer", "unknown-inner-version", "malformed-inner-version", "oversized-outer"} {
		t.Run(kind, func(t *testing.T) {
			store, lease, initial, _ := inlineCheckpointFixtureV2(t)
			encoded, err := encodeInlineReceiptCheckpointFileV2(initial)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "unknown-outer":
				encoded = append([]byte(`{"unexpected":true,`), encoded[1:]...)
			case "alias-outer":
				encoded = bytes.Replace(encoded, []byte(`"version"`), []byte(`"Version"`), 1)
			case "duplicate-outer":
				encoded = append([]byte(`{"version":1,`), encoded[1:]...)
			case "noncanonical-outer":
				encoded = append([]byte(" "), encoded...)
			case "unknown-inner-version", "malformed-inner-version":
				replacement := []byte(`"version":3`)
				if kind == "malformed-inner-version" {
					replacement = []byte(`"version":"2"`)
				}
				initial.Payload = bytes.Replace(initial.Payload, []byte(`"version":2`), replacement, 1)
				digest := sha256.Sum256(initial.Payload)
				initial.PayloadSHA256 = hex.EncodeToString(digest[:])
				encoded, err = json.Marshal(initial)
				if err != nil {
					t.Fatal(err)
				}
				encoded = append(encoded, '\n')
			case "oversized-outer":
				encoded = append(bytes.Repeat([]byte(" "), attachedworkeroutput.MaxInlineCheckpointBytesV2+1-len(encoded)), encoded...)
			}
			// Simulate a corrupted persisted file, bypassing the validated writer.
			// Only this test's private runtime directory is affected.
			if err := os.WriteFile(store.path(ReceiptCheckpointFileName), encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := lease.LoadReceiptCheckpoint(context.Background()); !errors.Is(err, ErrInvalidState) {
				t.Fatalf("%s recovered corrupted V2 through legacy decoder: %v", kind, err)
			}
			retained, err := os.ReadFile(store.path(ReceiptCheckpointFileName))
			if err != nil || !bytes.Equal(retained, encoded) {
				t.Fatalf("%s loader changed ambiguous evidence: %v", kind, err)
			}
		})
	}
}
