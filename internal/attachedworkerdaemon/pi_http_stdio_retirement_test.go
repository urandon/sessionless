//go:build darwin || linux

package attachedworkerdaemon

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
)

func TestPiHTTPStdioUnjoinedDriverRetainsOwnedBudget(t *testing.T) {
	s, b, spec, ledger, cancel := piHTTPProcessFixture(t, "ok")
	s.grace = 20 * time.Millisecond // cleanup failure bound, NOT readiness or model expiry
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	b.driver = func(ctx context.Context, v *piHTTPIO) error {
		if _, _, err := v.request(); err != nil {
			return err
		}
		close(entered)
		<-release // deliberately noncooperative test-owned borrower
		return ctx.Err()
	}
	var result AttemptResult
	var runErr error
	go func() { defer close(done); result, runErr = s.Run(b.ctx, spec) }()
	t.Cleanup(func() { cancel(); unblock(); apiWait(t, done); apiWait(t, b.done) })
	apiWait(t, entered)
	live := ledger.live
	request := bytes.Clone(b.core.requestFrame.Body)
	cancel()
	apiWait(t, done)
	if runErr == nil || result.CleanupSucceeded || result.piStdio != nil || b.complete {
		t.Fatalf("unjoined borrower declared clean: %+v err=%v", result, runErr)
	}
	plan := &PreparedPiHTTPStdioV1{preparedPiHTTPState: &preparedPiHTTPState{bridge: b, ledger: ledger}}
	plan.state.Store(2) // actual Run returned above; driver still owns its borrow
	copyPlan := *plan
	if err := copyPlan.Close(); !errors.Is(err, ErrPiHTTPRetirementPending) || ledger.live != live || !bytes.Equal(b.core.requestFrame.Body, request) {
		t.Fatal("cleanup timeout cleared/refunded an unjoined request borrower")
	}
	unblock()
	apiWait(t, b.done)
	if err := plan.Close(); err != nil || ledger.live != 0 {
		t.Fatalf("actual postjoin retirement failed: live=%d err=%v", ledger.live, err)
	}
}

func waitPiHTTPCondition(t *testing.T, name string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for !ready() {
		select {
		case <-poll.C:
		case <-deadline.C:
			if !ready() {
				t.Fatalf("condition not observed: %s", name)
			}
		}
	}
}

func TestPiHTTPStdioRetirementExcludesRunClaimBeforeAliasClear(t *testing.T) {
	_, b, _, _, _ := piHTTPProcessFixture(t, "ok")
	b.core.mu.Lock() // hold retirement at the outstanding Init borrow completion
	unlocked, consumed := false, false
	joined := make(chan error, 1)
	t.Cleanup(func() {
		if !unlocked {
			b.core.mu.Unlock()
		}
		if !consumed {
			<-joined
		}
	})
	go func() { joined <- b.clear() }()
	waitPiHTTPCondition(t, "exclusive retiring lifecycle", func() bool { return b.state.Load() == 4 })
	// Model Run already having passed its initial parent.Err check. Selection
	// itself must exclude retirement before validating send/ledger/buffer aliases.
	if err := b.selectForRun(b.ctx); err == nil {
		b.state.Store(4)
		t.Fatal("Run claimed a retiring plan")
	}
	b.core.mu.Unlock()
	unlocked = true
	err := <-joined
	consumed = true
	if err != nil || b.state.Load() != 3 {
		t.Fatalf("retirement did not finish: state=%d err=%v", b.state.Load(), err)
	}
}

func TestPiHTTPStdioRetainedLeasePointerRacesRetirementSafely(t *testing.T) {
	_, b, _, _, _ := piHTTPProcessFixture(t, "ok")
	lease := &b.lease
	close(b.requestReady) // no request is borrowed: prepared lifecycle denies it
	started, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		close(started)
		for range 100 {
			_, _, _ = lease.request()
			_ = lease.write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.ResponseStart})
		}
	}()
	t.Cleanup(func() { apiWait(t, done) })
	apiWait(t, started)
	if err := b.clear(); err != nil {
		t.Fatal(err)
	}
	apiWait(t, done)
	if lease.owner != b {
		t.Fatal("lease owner was mutated instead of lifecycle-gated")
	}
}

func TestPiHTTPStdioRejectedWriterStillOwnsFailurePath(t *testing.T) {
	s, b, spec, _, cancel := piHTTPProcessFixture(t, "ok")
	frames := piHTTPTestFrames(b.core.limits, []byte("0123456789"))
	written, allowSecond, returnDriver := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseWrite, failEntered, releaseFail := make(chan struct{}), make(chan struct{}), make(chan struct{})
	winnerDone, loserDone, driverReturned, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var writeOnce, secondOnce, returnOnce, failOnce sync.Once
	unblockWrite := func() { writeOnce.Do(func() { close(releaseWrite) }) }
	unblockSecond := func() { secondOnce.Do(func() { close(allowSecond) }) }
	unblockReturn := func() { returnOnce.Do(func() { close(returnDriver) }) }
	unblockFail := func() { failOnce.Do(func() { close(releaseFail) }) }
	var cancelOnce sync.Once
	underlyingCancel := b.send.cancel.Load().cancel
	b.send.cancel.Store(&preparedAPICancel{cancel: func() { cancelOnce.Do(func() { close(failEntered); <-releaseFail; underlyingCancel() }) }})
	b.afterWrite = func(k pisdkhttpwire.Kind) {
		if k == pisdkhttpwire.ResponseStart {
			close(written)
			<-releaseWrite
		}
	}
	var winnerStarted, loserStarted atomic.Bool
	b.driver = func(ctx context.Context, v *piHTTPIO) error {
		defer close(driverReturned)
		req, _, err := v.request()
		if err != nil {
			return err
		}
		if _, err = b.send.Authorize(req.Body, "fixture-profile"); err != nil {
			return err
		}
		winnerStarted.Store(true)
		go func() { defer close(winnerDone); _ = v.write(frames[2]) }()
		select {
		case <-allowSecond:
		case <-ctx.Done():
			return ctx.Err()
		}
		loserStarted.Store(true)
		go func() { defer close(loserDone); _ = v.write(frames[4]) }()
		<-returnDriver // intentional returning driver; arm must join admitted calls
		return nil
	}
	var runErr error
	go func() { defer close(done); _, runErr = s.Run(b.ctx, spec) }()
	t.Cleanup(func() {
		unblockFail()
		unblockWrite()
		unblockSecond()
		unblockReturn()
		cancel()
		apiWait(t, done)
		if winnerStarted.Load() {
			apiWait(t, winnerDone)
		}
		if loserStarted.Load() {
			apiWait(t, loserDone)
		}
	})
	apiWait(t, written)
	unblockSecond()
	apiWait(t, failEntered)
	unblockWrite()
	apiWait(t, winnerDone)
	if b.writing.Load() {
		t.Fatal("winning writer not returned")
	}
	unblockReturn()
	apiWait(t, driverReturned)
	if b.leaseMu.TryLock() {
		b.leaseMu.Unlock()
		t.Fatal("losing writer failure escaped admission ownership before return")
	}
	select {
	case <-done:
		t.Fatal("aggregate completed while rejected caller still owned fail path")
	default:
	}
	unblockFail()
	apiWait(t, loserDone)
	apiWait(t, done)
	if runErr == nil {
		t.Fatal("overlapping writer became successful process result")
	}
}

func TestPiHTTPStdioPreparationExactAndOneShortOriginalBudget(t *testing.T) {
	l := piHTTPTestLimits()
	outer, err := measurePiHTTPStdio(l)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := measurePiHTTPCoordinator(l)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"exact", "work_short", "live_short", "nodes_short"} {
		t.Run(name, func(t *testing.T) {
			ledger := &boundedAPIBorrowLedger{remainingCopy: outer.Work + inner.Work, remainingLive: outer.Live + inner.Live, remainingNodes: outer.Nodes + inner.Nodes}
			switch name {
			case "work_short":
				ledger.remainingCopy--
			case "live_short":
				ledger.remainingLive--
			case "nodes_short":
				ledger.remainingNodes--
			}
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			send := &preparedAPIPhysicalSend{ownerCtx: ctx, ctx: ctx, ledger: ledger}
			if err := send.prepareDeadline(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { send.retire() })
			if err := send.deadline.bindOwner(ctx); err != nil {
				t.Fatal(err)
			}
			b, err := preparePiHTTPStdio(&Supervisor{grace: time.Second}, l, ledger, send, pisdkhttpwire.Frame{Kind: pisdkhttpwire.Initialize, Body: []byte("{}")}, func(context.Context, *piHTTPIO) error { return nil })
			if name != "exact" {
				if err == nil || b != nil || ledger.live != 0 || ledger.copy > outer.Work || ledger.nodes > outer.Nodes {
					t.Fatalf("unfunded preparation b=%v err=%v debit=%d/%d/%d", b, err, ledger.copy, ledger.live, ledger.nodes)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			work, nodes := ledger.copy, ledger.nodes
			if err := b.clear(); err != nil || ledger.live != 0 || ledger.copy != work || ledger.nodes != nodes {
				t.Fatalf("prestart live-only retirement: %v", err)
			}
			if err := b.clear(); err != nil {
				t.Fatal("retirement replay", err)
			}
		})
	}
}
