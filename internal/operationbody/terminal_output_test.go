package operationbody_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
)

func terminalOutputLimits(count uint32, bytes uint64) (domain.ProductLimits, domain.SubstrateLimitsV1) {
	return domain.ProductLimits{MaxTenantQueueDepth: 1, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 1, MaxInputBytes: 1, MaxContextBytes: 1024, MaxArtifacts: count},
		domain.SubstrateLimitsV1{InvocationTimeout: time.Minute, ExecutionTimeout: 10 * time.Second, CleanupTimeout: time.Second, CPUMillis: 1, MemoryBytes: 1, ScratchBytes: 1, StdoutBytes: 1, StderrBytes: 1, NativeEventCount: 1, ArtifactBytes: bytes}
}

func terminalOutputStore(t *testing.T, b *bodyBlobs, count uint32, bytes uint64) (*operationbody.StoreV1, *operationbody.TerminalOutputV1) {
	t.Helper()
	s := retainedStore(t, b, retainedLimits())
	p, substrate := terminalOutputLimits(count, bytes)
	owner, err := s.AdmitTerminalOutput(t.Context(), p, substrate)
	if err != nil {
		t.Fatal("original physical admission", err)
	}
	return s, owner
}

// These cooperative storage fixtures prove physical capacity and fixed-object
// integrity only. No Store plan registration/current permission is fabricated;
// capsule-format prospective fit remains REQUIRED in the authentic planner.
func TestTerminalOutputAdmissionBeforeWritesAndFiniteLimits(t *testing.T) {
	for _, tc := range []struct {
		name            string
		body, artifacts uint64
		want            bool
	}{
		{name: "body_no_operation_room", body: operationbody.LocalTerminalCapsuleBytesV1, artifacts: 1 << 20},
		{name: "body_first_available", body: operationbody.LocalTerminalCapsuleBytesV1 + 1, artifacts: 1 << 20, want: true},
		{name: "artifact_one_short", body: 1 << 20, artifacts: operationbody.LocalTerminalCapsuleBytesV1 - 1},
		{name: "artifact_exact_capsule", body: 1 << 20, artifacts: operationbody.LocalTerminalCapsuleBytesV1, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBodyBlobs()
			limits := retainedLimits()
			limits.MaxAggregateBodyBytes = tc.body
			s := retainedStore(t, b, limits)
			p, substrate := terminalOutputLimits(1, tc.artifacts)
			owner, err := s.AdmitTerminalOutput(t.Context(), p, substrate)
			if (err == nil) != tc.want || (owner != nil) != tc.want {
				t.Fatalf("admission body=%d artifact=%d: owner=%v err=%v want=%v", tc.body, tc.artifacts, owner != nil, err, tc.want)
			}
			if tc.want {
				if owner.CapsuleLimit() != 256<<10 || owner.FailureNoticeLimit() != 1024 || owner.OperationBodyLimit() != tc.body-operationbody.LocalTerminalCapsuleBytesV1 {
					t.Fatal("immutable original limits differ from prepaid capacities")
				}
				copyStore := *s
				if again, err := copyStore.AdmitTerminalOutput(t.Context(), p, substrate); again != nil || err == nil {
					t.Fatal("adapter copy admitted a second terminal envelope")
				}
			}
			if puts, opens, _, _, _ := b.counts(); puts != 0 || opens != 0 {
				t.Fatal("admission/denial performed physical effects")
			}
		})
	}
	b := newBodyBlobs()
	s := retainedStore(t, b, retainedLimits())
	if _, err := s.Write(t.Context(), []byte("existing")); err != nil {
		t.Fatal(err)
	}
	p, substrate := terminalOutputLimits(5, 1<<20)
	if owner, err := s.AdmitTerminalOutput(t.Context(), p, substrate); owner != nil || err == nil {
		t.Fatal("late admission silently consumed/retrofitted existing body writes")
	}
}

func TestTerminalOutputSharedPairsVisibleAndOriginalBodyPool(t *testing.T) {
	b := newBodyBlobs()
	s, owner := terminalOutputStore(t, b, 6, (256<<10)+2*operationbody.RetainedPairBytesV1+3)
	copyOwner := *owner
	for generation := uint64(1); generation <= 2; generation++ {
		if _, err := s.ReserveRetainedPair(t.Context(), generation, generation); err != nil {
			t.Fatalf("pair %d charged against same admitted artifact pool: %v", generation, err)
		}
	}
	if slot, err := copyOwner.ReserveVisibleOutput(t.Context(), 4); slot != nil || err == nil {
		t.Fatal("first-over aggregate artifact bytes admitted")
	}
	slot, err := owner.ReserveVisibleOutput(t.Context(), 3)
	if err != nil || slot == nil {
		t.Fatal("exact shared artifact byte/count boundary denied", err)
	}
	if slot, err := copyOwner.ReserveVisibleOutput(t.Context(), 1); slot != nil || err == nil {
		t.Fatal("holder alias reset shared artifact bounds")
	}
	visible := []byte("out")
	sum := sha256.Sum256(visible)
	digest := hex.EncodeToString(sum[:])
	ref := domain.BlobRef{TenantID: bodyScope().TenantID, Key: domain.SessionRunObjectPrefix(bodyScope().TenantID, bodyScope().SessionID, bodyScope().RunID) + "artifacts/sha256/" + digest, Size: int64(len(visible)), SHA256: digest}
	copySlot := *slot
	if err := slot.Write(t.Context(), ref, visible); err != nil {
		t.Fatal("exact prepaid visible output did not verify", err)
	}
	if err := copySlot.Write(t.Context(), ref, visible); err == nil {
		t.Fatal("visible reservation copy restored physical write ownership")
	}
	// Both retained pairs and the capsule remain prepaid; ordinary bodies can
	// use exactly the remainder, independently of unused artifact upload slots.
	remainder := (1 << 20) - operationbody.LocalTerminalCapsuleBytesV1 - 2*operationbody.RetainedPairBytesV1
	if _, err := s.Write(t.Context(), []byte(strings.Repeat("x", int(remainder)))); err != nil {
		t.Fatal("exact original body remainder denied", err)
	}
	if _, err := s.Write(t.Context(), []byte("x")); err == nil {
		t.Fatal("capsule/pair reservations failed to debit original body pool")
	}
}

func terminalCapsulePlan(t *testing.T, body []byte) domain.TerminalCustodyPlanV1 {
	t.Helper()
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	key, err := domain.TerminalCustodyObjectKeyV1(bodyScope(), digest)
	if err != nil {
		t.Fatal(err)
	}
	return domain.TerminalCustodyPlanV1{Scope: bodyScope(), FrontierDigest: strings.Repeat("b", 64), Capsule: domain.BlobRef{TenantID: bodyScope().TenantID, Key: key, Size: int64(len(body)), SHA256: digest}}
}

func TestTerminalOutputOneUseReadbackAndProtectedFailureNotice(t *testing.T) {
	b := newBodyBlobs()
	s, owner := terminalOutputStore(t, b, 2, 1<<20)
	copyOwner := *owner
	body := []byte(`{"fixture":"capsule DATA, not semantic validation"}`)
	plan := terminalCapsulePlan(t, body)
	wrong := plan
	wrong.Scope.AttemptID = "foreign-attempt"
	wrong.Capsule.Key, _ = domain.TerminalCustodyObjectKeyV1(wrong.Scope, wrong.Capsule.SHA256)
	if err := wrong.Validate(); err != nil {
		t.Fatal("foreign control must be structurally valid DATA", err)
	}
	if err := owner.WriteCapsule(t.Context(), wrong, body); err == nil {
		t.Fatal("foreign planned scope accepted")
	}
	if puts, _, _, _, _ := b.counts(); puts != 0 {
		t.Fatal("invalid DATA caused a PUT")
	}
	b.putFault = "ambiguous"
	if err := owner.WriteCapsule(t.Context(), plan, body); err == nil {
		t.Fatal("ambiguous actual PUT reported success")
	}
	if err := copyOwner.WriteCapsule(t.Context(), plan, body); err == nil {
		t.Fatal("holder copy retried an uncertain capsule")
	}
	if _, err := s.Write(t.Context(), []byte("new operation")); err == nil {
		t.Fatal("uncertain capsule allowed another ordinary write")
	}
	// A protected separate notice is still physically available; its success
	// must never clear the prior artifact/operation uncertainty.
	b.putFault = ""
	notice := []byte(`{"schema":"sessionless.run-terminal-notice.v1","code":"cancelled","cancelled":true}`)
	sum := sha256.Sum256(notice)
	digest := hex.EncodeToString(sum[:])
	id := domain.SessionEventID("terminal-fixture-notice")
	draft := domain.SessionEventDraft{ID: id, Kind: domain.SessionEventSystemNotice, Payload: domain.BlobRef{TenantID: bodyScope().TenantID, Key: domain.SessionEventObjectPrefix(bodyScope().TenantID, bodyScope().SessionID, id) + "payloads/sha256/" + digest + ".json", Size: int64(len(notice)), SHA256: digest}}
	if err := copyOwner.WriteFailureNotice(t.Context(), draft, notice); err != nil {
		t.Fatal("prepaid separate failure notice unavailable", err)
	}
	if err := owner.WriteFailureNotice(t.Context(), draft, notice); err == nil {
		t.Fatal("second failure notice physical attempt admitted")
	}
	if _, err := s.Write(t.Context(), []byte("retry after notice")); err == nil {
		t.Fatal("failure notice success erased prior uncertainty")
	}
	if puts, opens, closes, _, _ := b.counts(); puts != 2 || opens != 1 || closes != 1 {
		t.Fatalf("physical one-use/readback count: put=%d open=%d close=%d", puts, opens, closes)
	}
}

func TestTerminalOutputCapsuleExactEnvelopeAndOrdinaryCount(t *testing.T) {
	b := newBodyBlobs()
	s, owner := terminalOutputStore(t, b, 1, 1<<20)
	tooBig := []byte(strings.Repeat("x", int(operationbody.LocalTerminalCapsuleBytesV1)+1))
	tooBigPlan := terminalCapsulePlan(t, tooBig)
	if err := tooBigPlan.Validate(); err != nil {
		t.Fatal("first-over control must fit unchanged512KiB domain hardcap", err)
	}
	if err := owner.WriteCapsule(t.Context(), tooBigPlan, tooBig); err == nil {
		t.Fatal("first-over admitted original capsule envelope")
	}
	if puts, opens, _, _, _ := b.counts(); puts != 0 || opens != 0 {
		t.Fatal("first-over capsule performed physical I/O")
	}
	body := tooBig[:operationbody.LocalTerminalCapsuleBytesV1]
	plan := terminalCapsulePlan(t, body)
	copyOwner := *owner
	if err := owner.WriteCapsule(t.Context(), plan, body); err != nil {
		t.Fatal("exact original capsule envelope denied", err)
	}
	if err := copyOwner.WriteCapsule(t.Context(), plan, body); err == nil {
		t.Fatal("capsule copy restored consumed physical ownership")
	}
	for range 2 * retainedLimits().MaxOperations {
		if _, err := s.Write(t.Context(), []byte("x")); err != nil {
			t.Fatal("capsule consumed ordinary request/result write count", err)
		}
	}
	if _, err := s.Write(t.Context(), []byte("x")); err == nil {
		t.Fatal("capsule expanded ordinary write count")
	}
	if puts, opens, closes, _, _ := b.counts(); puts != 9 || opens != 9 || closes != 9 {
		t.Fatalf("original physical counts: put=%d open=%d close=%d", puts, opens, closes)
	}
	if body[0] != 'x' || tooBig[len(tooBig)-1] != 'x' {
		t.Fatal("physical adapter cleared caller-owned bytes")
	}
}

func TestTerminalOutputSharesOriginalIOGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBodyBlobs()
		s, owner := terminalOutputStore(t, b, 8, 1<<20)
		release, done := make(chan struct{}), make(chan error, 1)
		joined := make(chan struct{})
		b.blockPut = release
		ctx, cancel := context.WithCancel(t.Context())
		var once sync.Once
		t.Cleanup(func() {
			cancel()
			once.Do(func() { close(release) })
			synctest.Wait()
			select {
			case <-joined:
			default:
				t.Error("owned synchronous capsule writer did not join")
			}
		})
		body := []byte("capsule fixture DATA")
		plan := terminalCapsulePlan(t, body)
		go func() { defer close(joined); done <- owner.WriteCapsule(ctx, plan, body) }()
		synctest.Wait()
		if _, err := s.Write(t.Context(), []byte("ordinary")); !errors.Is(err, operationbody.ErrBusy) {
			t.Fatalf("ordinary writer bypassed same gate: %v", err)
		}
		if _, err := s.ReserveRetainedPair(t.Context(), 1, 1); !errors.Is(err, operationbody.ErrBusy) {
			t.Fatalf("pair reservation bypassed same gate: %v", err)
		}
		if _, err := owner.ReserveVisibleOutput(t.Context(), 1); !errors.Is(err, operationbody.ErrBusy) {
			t.Fatalf("visible output bypassed same gate: %v", err)
		}
		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("cancelled capsule writer succeeded")
			}
		default:
			t.Fatal("cancelled original writer did not synchronously join")
		}
	})
}

type terminalMutatingContext struct {
	context.Context
	once   sync.Once
	mutate func()
}

func (c *terminalMutatingContext) Err() error {
	c.once.Do(c.mutate)
	return c.Context.Err()
}

func TestTerminalOutputAuthenticatesOwnedCopyBeforePUT(t *testing.T) {
	for _, mutate := range []bool{false, true} {
		name := "live_unchanged_positive"
		if mutate {
			name = "caller_changes_during_begin_zero_put"
		}
		t.Run(name, func(t *testing.T) {
			b := newBodyBlobs()
			s, owner := terminalOutputStore(t, b, 2, 1<<20)
			body := []byte("exact caller capsule DATA")
			plan := terminalCapsulePlan(t, body)
			changed := false
			ctx := &terminalMutatingContext{Context: t.Context(), mutate: func() {
				if mutate {
					body[0] ^= 1
					changed = true
				}
			}}
			err := owner.WriteCapsule(ctx, plan, body)
			puts, opens, _, _, _ := b.counts()
			if !mutate {
				if err != nil || puts != 1 || opens != 1 {
					t.Fatalf("live exact physical positive denied: puts=%d opens=%d err=%v", puts, opens, err)
				}
				return
			}
			if !changed || err == nil || puts != 0 || opens != 0 {
				t.Errorf("caller mutation before owned-copy authentication reached physical storage: changed=%v puts=%d opens=%d err=%v", changed, puts, opens, err)
			}
			// The unexpected change happens after irrevocable admission; fixing
			// caller bytes cannot restore this original capsule or body writer.
			body[0] ^= 1
			copyOwner := *owner
			if err := copyOwner.WriteCapsule(t.Context(), plan, body); err == nil {
				t.Error("changed caller buffer refunded original capsule ownership")
			}
			if _, err := s.Write(t.Context(), []byte("ordinary retry")); err == nil {
				t.Error("unexpected mutation failed to poison future original writes")
			}
		})
	}
}

func TestTerminalOutputArtifactCountIndependentOfBytes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		count      uint32
		pair, want bool
	}{
		{name: "capsule_visible_count1_denied", count: 1},
		{name: "capsule_visible_count2_exact", count: 2, want: true},
		{name: "capsule_pair_count2_denied", count: 2, pair: true},
		{name: "capsule_pair_count3_exact", count: 3, pair: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBodyBlobs()
			s, owner := terminalOutputStore(t, b, tc.count, 1<<20)
			var err error
			if tc.pair {
				_, err = s.ReserveRetainedPair(t.Context(), 1, 1)
			} else {
				_, err = owner.ReserveVisibleOutput(t.Context(), 1)
			}
			if (err == nil) != tc.want {
				t.Fatalf("independent count=%d pair=%v: err=%v want=%v", tc.count, tc.pair, err, tc.want)
			}
			if tc.pair && !tc.want {
				// A rejected two-object pair must not consume the one remaining
				// artifact slot; fill it with one visible object before first-over.
				if _, err := owner.ReserveVisibleOutput(t.Context(), 1); err != nil {
					t.Fatalf("rejected pair consumed remaining artifact slot: %v", err)
				}
			}
			// More than 600KiB remains in the artifact byte allowance in every
			// row; the count predicate alone must reject this next reservation.
			if _, err := owner.ReserveVisibleOutput(t.Context(), 1); err == nil {
				t.Fatal("count-only first-over admitted despite ample byte capacity")
			}
			if puts, opens, _, _, _ := b.counts(); puts != 0 || opens != 0 {
				t.Fatal("count capacity checks performed physical effects")
			}
		})
	}
}
