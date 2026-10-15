package attachedworkerdaemon

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func deadlineTestSend(t *testing.T, owner context.Context) *preparedAPIPhysicalSend {
	t.Helper()
	ctx, cancel := context.WithCancel(owner)
	t.Cleanup(cancel)
	send := &preparedAPIPhysicalSend{ctx: ctx, ownerCtx: owner}
	send.cancel.Store(&preparedAPICancel{cancel: cancel})
	if err := send.prepareDeadline(); err != nil {
		t.Fatal("prepare original monitor:", err)
	}
	t.Cleanup(func() { send.retire() })
	if err := send.deadline.bindOwner(owner); err != nil {
		t.Fatal("bind original owner:", err)
	}
	return send
}

func TestPreparedAPIDeadlineOriginalFAndNormalExitDrain(t *testing.T) {
	mono := time.Now()
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		owner, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		send := deadlineTestSend(t, owner)
		send.deadline.clock = func() time.Time { return mono.Add(time.Since(start)) }
		ioCtx := send.deadline.ioContext()
		f := mono.Add(30 * time.Minute)
		if err := send.deadline.arm(send, f); err != nil || send.deadline.cutoff != f {
			t.Fatalf("arm exact F: cutoff=%v want=%v err=%v", send.deadline.cutoff, f, err)
		}
		if err := send.deadline.arm(send, mono.Add(time.Hour)); err == nil || send.deadline.cutoff != f {
			t.Fatal("second arm renewed original F")
		}
		time.Sleep(15 * time.Minute)
		send.revoke() // actual supervisor normal-exit semantics, NOT invalid use
		if send.ctx.Err() == nil || ioCtx.Err() != nil || owner.Err() != nil || send.invalid.Load() {
			t.Fatal("normal exit failed to withdraw Source or truncated bounded Final drain")
		}
		time.Sleep(15 * time.Minute)
		synctest.Wait()
		if !send.invalid.Load() || !send.deadline.expired.Load() || ioCtx.Err() == nil || owner.Err() != nil {
			t.Fatal("original F did not stop drain independently of Source revocation")
		}
		send.retire()
		if send.deadline.timer != nil || send.deadline.done != nil || send.deadline.ctx != nil || send.deadline.cancel.Load() != nil {
			t.Fatal("joined deadline retained timer/context/cancellation aliases")
		}
	})
}

func TestPreparedAPIDeadlineDisarmDoesNotExpireOrCancelOwner(t *testing.T) {
	mono := time.Now()
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		owner, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		send := deadlineTestSend(t, owner)
		send.deadline.clock = func() time.Time { return mono.Add(time.Since(start)) }
		ioCtx := send.deadline.ioContext()
		if err := send.deadline.arm(send, mono.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		send.retire()
		time.Sleep(2 * time.Hour)
		synctest.Wait()
		if send.invalid.Load() || send.deadline.expired.Load() || ioCtx.Err() == nil || owner.Err() != nil {
			t.Fatal("normal retirement invented expiry or cancelled immutable owner")
		}
		if send.deadline.arm(send, mono.Add(3*time.Hour)) == nil || send.prepareDeadline() == nil || send.deadline.bindOwner(owner) == nil {
			t.Fatal("retired monitor was reset/rebound")
		}
	})
}

func TestPreparedAPIDeadlineInvalidUseCancelsBothContexts(t *testing.T) {
	owner, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	send := deadlineTestSend(t, owner)
	ioCtx := send.deadline.ioContext()
	borrow := &PreparedAPISendBorrowV1{send: send}
	borrow.Revoke()
	if send.ctx.Err() == nil || ioCtx.Err() == nil || owner.Err() != nil {
		t.Fatal("invalid-use revocation did not cancel Source+IO independently of owner")
	}
	if send.deadline.arm(send, time.Now().Add(time.Hour)) == nil {
		t.Fatal("revoked-before-arm send accepted F")
	}
}

func TestPreparedAPIDeadlineDeniesInvalidOrUnpreparedArm(t *testing.T) {
	for _, name := range []string{"zero", "wall", "past", "owner cancelled", "unprepared", "no owner", "stopped", "duplicate bind", "duplicate prepare"} {
		t.Run(name, func(t *testing.T) {
			owner, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			send := deadlineTestSend(t, owner)
			f := time.Now().Add(time.Hour)
			switch name {
			case "zero":
				f = time.Time{}
			case "wall":
				f = f.Round(0)
			case "past":
				f = time.Now().Add(-time.Hour)
			case "owner cancelled":
				cancel()
			case "unprepared":
				send = &preparedAPIPhysicalSend{ctx: owner}
			case "no owner":
				send = &preparedAPIPhysicalSend{ctx: owner}
				if err := send.prepareDeadline(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { send.retire() })
			case "stopped":
				send.deadline.stopAndJoin()
			case "duplicate bind":
				if send.deadline.bindOwner(owner) == nil {
					t.Fatal("second owner bind accepted")
				}
				return
			case "duplicate prepare":
				if send.prepareDeadline() == nil {
					t.Fatal("second preparation accepted")
				}
				return
			}
			if err := send.deadline.arm(send, f); err == nil || send.deadline.armed {
				t.Fatalf("%s armed: err=%v armed=%v", name, err, send.deadline.armed)
			}
		})
	}
}

func TestPreparedAPIDeadlineJoinsAlreadyStartedCallback(t *testing.T) {
	mono := time.Now()
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		send := deadlineTestSend(t, context.Background())
		send.deadline.clock = func() time.Time { return mono.Add(time.Since(start)) }
		entered, resume, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var enteredOnce, resumeOnce sync.Once
		release := func() { resumeOnce.Do(func() { close(resume) }) }
		t.Cleanup(release)
		// Test-only held canceller proves Stop(false) alone is not retirement.
		// Production uses context.CancelFunc, never this blocking replacement.
		send.cancel.Store(&preparedAPICancel{cancel: func() { enteredOnce.Do(func() { close(entered) }); <-resume }})
		if err := send.deadline.arm(send, mono.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		<-entered
		go func() { send.deadline.stopAndJoin(); close(joined) }()
		synctest.Wait()
		select {
		case <-joined:
			t.Fatal("retired before actual timer callback returned")
		default:
		}
		release()
		<-joined
		send.cancel.Store(nil) // callback has joined; do not replay synthetic canceller
		if send.deadline.timer != nil || send.deadline.done != nil || send.deadline.ctx != nil {
			t.Fatal("callback joined but original aliases survived")
		}
	})
}

func TestAPIOwnerArmsOriginalDeadlineBeforeReturningF(t *testing.T) {
	inv, parent, life := longLivedAPIOwnerFixture(t)
	var original *preparedAPIPhysicalSend
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(ctx context.Context, in PreparedInvocationV1) (AttemptResult, error) {
		original = in.PhysicalSend.(*preparedAPIPhysicalSend)
		if original.deadline.ioContext() == nil || original.deadline.armed {
			t.Error("original pre-Issue monitor absent or already armed")
		}
		c, l, n := parent.ledger.copy, parent.ledger.live, parent.ledger.nodes
		f, err := in.PhysicalSend.Authorize(in.APIRequest.Body, original.profile)
		if err != nil {
			return AttemptResult{}, err
		}
		if !original.deadline.armed || original.deadline.cutoff != f || !original.authorized || original.failed || original.deadline.ioContext().Err() != nil {
			t.Error("original Source returned F before monitor was armed")
		}
		if parent.ledger.copy != c || parent.ledger.live != l || parent.ledger.nodes != n {
			t.Error("physical send allocated a fresh ledger partition")
		}
		return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Hour)
	t.Cleanup(cancel)
	result, err := runner.Run(ctx, inv)
	if !result.Succeeded(err) || life.finalizeCount != 1 || life.releaseCount != 1 || original == nil {
		t.Fatalf("original owner/finalize: success=%v finalize=%d release=%d err=%v", result.Succeeded(err), life.finalizeCount, life.releaseCount, err)
	}
	if original.invalid.Load() || original.deadline.ctx != nil || original.deadline.timer != nil || original.deadline.cancel.Load() != nil {
		t.Fatal("normal retirement invented failure or retained deadline resources")
	}
}

func TestPreparedAPIDeadlineRetirementWhileSourceHeld(t *testing.T) {
	send := deadlineTestSend(t, context.Background())
	ioCtx := send.deadline.ioContext()
	entered, resume, done, retired := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	send.body, send.profile = []byte("original"), "profile"
	send.authorize = func() (time.Time, error) { close(entered); <-resume; return time.Now().Add(time.Hour), nil }
	var f time.Time
	var callErr error
	go func() { defer close(done); f, callErr = send.Authorize(send.body, send.profile) }()
	t.Cleanup(func() { unblock(); apiWait(t, done) })
	apiWait(t, entered)
	go func() { defer close(retired); send.retire() }()
	t.Cleanup(func() { unblock(); apiWait(t, retired) })
	apiWait(t, send.ctx.Done()) // revoke BEFORE Source join
	apiWait(t, ioCtx.Done())    // stop IO BEFORE Source join
	select {
	case <-retired:
		t.Fatal("retired borrowed body/context before held Source returned")
	default:
	}
	unblock()
	apiWait(t, done)
	apiWait(t, retired)
	if callErr == nil || !f.IsZero() || send.deadline.timer != nil || send.deadline.ctx != nil {
		t.Fatalf("held Source escaped retirement: cutoff=%v err=%v", f, callErr)
	}
}
