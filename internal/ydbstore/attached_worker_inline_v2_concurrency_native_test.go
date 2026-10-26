package ydbstore_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type inlineNativeRaceResultV2 struct {
	index  int
	result attachedworkeroutput.AttachedWorkerInlineResultV2
	err    error
}

// Both callers reach the start gate before either may enter the public API.
// Native transactions decide the winner. This does not force two SQL snapshots
// to overlap and does not infer their serialization order from handler order.
func startInlineNativePairV2(t *testing.T, f *inlineNativeFixtureV2, calls [2]func() (attachedworkeroutput.AttachedWorkerInlineResultV2, error)) <-chan inlineNativeRaceResultV2 {
	t.Helper()
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	done := make(chan inlineNativeRaceResultV2, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	for i, call := range calls {
		go func() {
			defer workers.Done()
			ready <- struct{}{}
			select {
			case <-start:
				result, err := call()
				done <- inlineNativeRaceResultV2{index: i, result: result, err: err}
			case <-f.ctx.Done():
				done <- inlineNativeRaceResultV2{index: i, err: f.ctx.Err()}
			}
		}()
	}
	joined := make(chan struct{})
	go func() { workers.Wait(); close(joined) }()
	// Registered after the fixture, before waiting: workers must stop before
	// its fake peer buffers or exact disposable SQL tenant can be cleaned up.
	t.Cleanup(func() {
		f.cancel()
		bound, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		select {
		case <-joined:
		case <-bound.Done():
			select {
			case <-joined:
			default:
				t.Error("inline native race workers did not join bounded cleanup")
			}
		}
	})
	defer close(start)
	for range 2 {
		select {
		case <-ready:
		case <-f.ctx.Done():
			t.Fatal("inline native pair start gate", f.ctx.Err())
		}
	}
	return done
}

func TestAttachedInlineOutputYDBConcurrentPrepare(t *testing.T) {
	f := newInlineNativeFixtureV2(t)
	fingerprint, err := attachedworkeroutput.InlineOutputRequestFingerprintV2(f.prepare)
	if err != nil {
		t.Fatal(err)
	}
	call := func() (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
		return f.store.PrepareAttachedWorkerInlineOutput(f.ctx, f.blobs, f.prepare)
	}
	done := startInlineNativePairV2(t, f, [2]func() (attachedworkeroutput.AttachedWorkerInlineResultV2, error){call, call})
	winners := 0
	for range 2 {
		select {
		case outcome := <-done:
			if outcome.err == nil {
				winners++
				if outcome.result.Version != 2 || outcome.result.Stage != attachedworkeroutput.InlinePreparedV2 || len(outcome.result.Capability) != 64 ||
					outcome.result.Fingerprint != fingerprint || outcome.result.Nonce != f.prepare.Nonce || !outcome.result.ExpiresAt.Equal(f.prepare.ExpiresAt) || outcome.result.CanonicalDigest != "" {
					t.Fatalf("Prepare racer=%d returned changed original capability identity", outcome.index)
				}
			} else if !errors.Is(outcome.err, ydbstore.ErrAttachedWorkerAttemptConflict) || outcome.result != (attachedworkeroutput.AttachedWorkerInlineResultV2{}) {
				t.Fatalf("Prepare racer=%d did not fail closed without a capability: %v", outcome.index, outcome.err)
			}
		case <-f.ctx.Done():
			t.Fatal("concurrent native Prepare completion", f.ctx.Err())
		}
	}
	requireInlineNativeAuthorityWindowV2(t, f)
	record, err := readInlineNativeReceiptV2(f.ctx, f.db, f.prepare.Authorization)
	if err != nil || winners != 1 || record.Stage != attachedworkeroutput.InlinePreparedV2 || record.BodyClaimID != "" || record.CopyOwnerID != "" ||
		record.CandidateFingerprint != fingerprint || !reflect.DeepEqual(record.Candidate, f.prepare.Candidate) || record.Producer != f.prepare.Producer ||
		!record.ExpiresAt.Equal(f.prepare.ExpiresAt) || record.Materialization.Completion != nil || f.blobs.writes() != [2]uint64{} {
		t.Fatalf("concurrent Prepare did not leave one original no-write plan: winners=%d stage=%s writes=%v err=%v", winners, record.Stage, f.blobs.writes(), err)
	}
}

type inlineNativeRaceReaderV2 struct {
	*inlineNativeClaimReaderV2
	reads atomic.Int32
}

func (r *inlineNativeRaceReaderV2) Read(p []byte) (int, error) {
	r.reads.Add(1)
	return r.inlineNativeClaimReaderV2.Read(p)
}

func TestAttachedInlineOutputYDBConcurrentBodyClaim(t *testing.T) {
	f := newInlineNativeFixtureV2(t)
	request := f.prepared(t)
	var readers [2]*inlineNativeRaceReaderV2
	var calls [2]func() (attachedworkeroutput.AttachedWorkerInlineResultV2, error)
	for i := range readers {
		readers[i] = &inlineNativeRaceReaderV2{inlineNativeClaimReaderV2: &inlineNativeClaimReaderV2{
			ctx: f.ctx, db: f.db, auth: request.Authorization, nonce: request.Nonce, fp: request.Fingerprint,
			body: bytes.NewReader(f.body), entered: make(chan struct{}), observed: make(chan error, 1), resume: make(chan struct{}),
		}}
		calls[i] = func() (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
			return f.store.SubmitAttachedWorkerInlineOutput(f.ctx, f.blobs, request, int64(len(f.body)), readers[i])
		}
	}
	done := startInlineNativePairV2(t, f, calls)
	winner, loser := -1, -1
	entered := [2]<-chan struct{}{readers[0].entered, readers[1].entered}
	for winner < 0 || loser < 0 {
		select {
		case <-entered[0]:
			if winner >= 0 {
				t.Fatal("both native body racers acquired Read authority")
			}
			winner, entered[0] = 0, nil
		case <-entered[1]:
			if winner >= 0 {
				t.Fatal("both native body racers acquired Read authority")
			}
			winner, entered[1] = 1, nil
		case outcome := <-done:
			if loser >= 0 || !errors.Is(outcome.err, ydbstore.ErrAttachedWorkerAttemptConflict) || outcome.result != (attachedworkeroutput.AttachedWorkerInlineResultV2{}) {
				t.Fatalf("body racer=%d returned before the sole Read was released: %v", outcome.index, outcome.err)
			}
			loser = outcome.index
		case <-f.ctx.Done():
			t.Fatal("concurrent native body claim/loser barrier", f.ctx.Err())
		}
	}
	if winner == loser || readers[loser].reads.Load() != 0 || readers[winner].reads.Load() != 1 {
		t.Fatalf("native body ownership: winner=%d loser=%d reads=%d/%d", winner, loser, readers[0].reads.Load(), readers[1].reads.Load())
	}
	if err := <-readers[winner].observed; err != nil {
		t.Fatal("concurrent winner Read lacked a committed original body claim", err)
	}
	requireInlineNativeAuthorityWindowV2(t, f)
	claimed, err := readInlineNativeReceiptV2(f.ctx, f.db, request.Authorization)
	if err != nil || claimed.Stage != attachedworkeroutput.InlineBodyClaimedV2 || len(claimed.BodyClaimID) != 64 || claimed.CopyOwnerID != "" ||
		claimed.Materialization.Completion != nil || f.blobs.writes() != [2]uint64{} {
		t.Fatalf("concurrent claim issued premature canonical writes: stage=%s writes=%v err=%v", claimed.Stage, f.blobs.writes(), err)
	}
	readers[winner].release.Do(func() { close(readers[winner].resume) })
	var completed inlineNativeRaceResultV2
	select {
	case completed = <-done:
	case <-f.ctx.Done():
		t.Fatal("sole concurrent native body promotion", f.ctx.Err())
	}
	requireInlineNativeAuthorityWindowV2(t, f)
	record, err := readInlineNativeReceiptV2(f.ctx, f.db, request.Authorization)
	if err != nil || completed.index != winner || completed.err != nil || completed.result.Stage != attachedworkeroutput.InlineReadyV2 || completed.result.Capability != "" ||
		completed.result.CanonicalDigest == "" || record.CanonicalDigest != completed.result.CanonicalDigest || record.Stage != attachedworkeroutput.InlineReadyV2 ||
		completed.result.Nonce != request.Nonce || completed.result.Fingerprint != request.Fingerprint || !completed.result.ExpiresAt.Equal(request.ExpiresAt) ||
		record.BodyClaimID != claimed.BodyClaimID || record.CopyOwnerID != f.blobs.copyOwner || !reflect.DeepEqual(record.Candidate, claimed.Candidate) || record.Producer != claimed.Producer ||
		readers[loser].reads.Load() != 0 || f.blobs.writes() != [2]uint64{2, 0} {
		t.Fatalf("concurrent native winner changed custody or repeated writes: index=%d stage=%s writes=%v result-err=%v record-err=%v", completed.index, record.Stage, f.blobs.writes(), completed.err, err)
	}
	reconciled, err := f.store.ReconcileAttachedWorkerInlineOutput(f.ctx, attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: request.Authorization,
		Nonce: request.Nonce, Fingerprint: request.Fingerprint, BodySendBurned: true})
	if err != nil || reconciled != completed.result || readers[loser].reads.Load() != 0 || f.blobs.writes() != [2]uint64{2, 0} {
		t.Fatalf("concurrent native reconciliation changed READY or repeated effects: writes=%v err=%v", f.blobs.writes(), err)
	}
	denied := new(inlineNativeDeniedReaderV2)
	if _, err := f.store.SubmitAttachedWorkerInlineOutput(f.ctx, f.blobs, request, int64(len(f.body)), denied); !errors.Is(err, ydbstore.ErrAttachedWorkerAttemptConflict) || denied.reads != 0 || f.blobs.writes() != [2]uint64{2, 0} {
		t.Fatalf("concurrent native READY reopened body/write authority: reads=%d writes=%v err=%v", denied.reads, f.blobs.writes(), err)
	}
}
