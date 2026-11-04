package ydbstore

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// Pure private scheduling/accounting tests. No Source, serving approval, SQL,
// body I/O, external HTTP, process or socket runs inside these clock bubbles.
func TestLocalSearchConsentWaitSharedFinitePoolAndCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := &localSearchWaitBudget{}
		entry := &LocalInvocationEntryV1{searchWait: w}
		copyEntry := *entry
		start := time.Now()
		for i := 0; i < 24; i++ {
			owner := entry
			if i%2 != 0 {
				owner = &copyEntry
			} // shared across request/entry aliases
			ctx, finish, err := owner.searchWait.pollContext(t.Context(), start.Add(2*time.Minute))
			if err != nil {
				t.Fatal("within original finite scheduling bounds", i, err)
			}
			if !time.Now().Equal(start.Add(time.Duration(i) * 5 * time.Second)) {
				t.Fatal("poll cadence renewed or accelerated", i, time.Since(start))
			}
			budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
			if budget.bytes != uint64(i) {
				t.Fatal("second request reset actual byte charges")
			}
			if err := chargeOwnedAuthorityBody(ctx, 1); err != nil || finish(nil) != nil {
				t.Fatal("healthy poll", err)
			}
		}
		if _, _, err := w.pollContext(t.Context(), start.Add(2*time.Minute)); err == nil || w.polls != 24 || w.bytes != 24 {
			t.Fatal("twenty-fifth poll admitted or counters refunded")
		}
	})
}

func TestLocalSearchConsentWaitRetainsFailureChargesAndProspectiveCap(t *testing.T) {
	for _, mode := range []string{"read-error", "byte-cap", "callback-cap"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w := &localSearchWaitBudget{bytes: ownedAuthorityMaxBytes - 1}
				if mode != "byte-cap" {
					w.bytes = 37
				}
				ctx, finish, err := w.pollContext(t.Context(), time.Now().Add(time.Minute))
				if err != nil {
					t.Fatal(err)
				}
				budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
				readErr := error(nil)
				switch mode {
				case "read-error":
					if chargeOwnedAuthorityBody(ctx, 123) != nil {
						t.Fatal("actual failed-read bytes")
					}
					readErr = errors.New("controlled storage failure")
				case "byte-cap":
					if chargeOwnedAuthorityBody(ctx, 2) == nil || budget.bytes != ownedAuthorityMaxBytes-1 {
						t.Fatal("prospective over-cap copy was charged/materialized")
					}
				case "callback-cap":
					for range 3 {
						callback, err := budget.begin()
						if err != nil {
							t.Fatal(err)
						}
						for range 32 {
							if budget.statement(ctx, callback) != nil {
								t.Fatal("unchanged per-callback language")
							}
						}
						if budget.materialized(callback, 1) != nil {
							t.Fatal("actual callback metadata")
						}
						budget.finish(callback)
					}
					if _, err := budget.begin(); err == nil || budget.statements != 96 {
						t.Fatal("fourth callback or statement budget widened")
					}
				}
				if finish(readErr) == nil || !w.failed || w.polls != 1 || w.bytes != budget.bytes {
					t.Fatal("failed poll refunded charges or remained healthy")
				}
				if _, _, err := w.pollContext(t.Context(), time.Now().Add(time.Minute)); err == nil {
					t.Fatal("sticky failed read retried as pending")
				}
			})
		})
	}
}

func TestLocalSearchConsentWaitConcurrencySlowPollAndShorterDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := &localSearchWaitBudget{}
		start := time.Now()
		ctx, finish, err := w.pollContext(t.Context(), start.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		denied := make(chan error, 1)
		go func() { _, _, err := w.pollContext(t.Context(), start.Add(time.Minute)); denied <- err }()
		if <-denied == nil {
			t.Fatal("concurrent poll copied its own allowance")
		}
		// Simulate real elapsed read time on a synthetic clock, not a scheduling
		// sleep against external I/O. A slow poll does not demand another delay.
		<-time.After(7 * time.Second)
		if finish(nil) != nil || ctx.Err() != context.Canceled {
			t.Fatal("finished poll did not close its call")
		}
		_, finish, err = w.pollContext(t.Context(), start.Add(time.Minute))
		if err != nil || !time.Now().Equal(start.Add(7*time.Second)) {
			t.Fatal("slow poll created a renewed cadence window", err)
		}
		if finish(nil) != nil {
			t.Fatal("healthy second poll")
		}
		short, cancel := context.WithDeadline(t.Context(), start.Add(8*time.Second))
		defer cancel()
		if _, _, err := w.pollContext(short, start.Add(time.Minute)); err == nil || w.polls != 2 || !w.failed || !time.Now().Equal(start.Add(8*time.Second)) {
			t.Fatal("shorter caller deadline did not stop cadence before another poll")
		}
	})
	synctest.Test(t, func(t *testing.T) {
		w := &localSearchWaitBudget{}
		start := time.Now()
		_, finish, err := w.pollContext(t.Context(), start.Add(3*time.Second))
		if err != nil || finish(nil) != nil {
			t.Fatal("first immediate poll", err)
		}
		if _, _, err := w.pollContext(t.Context(), start.Add(3*time.Second)); err == nil || w.polls != 1 || !time.Now().Equal(start.Add(3*time.Second)) {
			t.Fatal("request deadline renewed to reach next cadence")
		}
	})
}

func TestLocalSearchConsentWaitRejectsNonOriginalHolder(t *testing.T) {
	for _, p := range []*localSearchConsentV1{nil, {}, {entry: &LocalInvocationEntryV1{}}} {
		if _, err := p.Wait(t.Context()); err == nil {
			t.Fatal("non-original pending DATA acquired wait authority")
		}
	}
}

func TestLocalSearchConsentExpiryStopUsesLastSharedPollAndBytes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		deadline := start.Add(3 * time.Second)
		active, cancel := context.WithDeadline(t.Context(), deadline)
		defer cancel()
		w := &localSearchWaitBudget{polls: 23, bytes: 37, lastStart: start}
		if _, _, err := w.pollContext(active, deadline); err != errLocalSearchWaitExpiry || !w.failed || !w.expiryPending || w.polls != 23 || !time.Now().Equal(deadline) {
			t.Fatal("healthy active deadline did not select negative-only boundary", err)
		}
		cleanup, stop := context.WithDeadline(t.Context(), start.Add(30*time.Second))
		defer stop()
		call, finish, err := w.stopContext(cleanup)
		if err != nil || !time.Now().Equal(deadline) || !w.lastStart.Equal(start) || w.polls != 24 || !w.failed || w.expiryPending {
			t.Fatal("terminal stop renewed cadence/counter or restored positive waiting", err)
		}
		budget := call.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		if budget.bytes != 37 || chargeOwnedAuthorityBody(call, 123) != nil || finish(nil) != nil || w.bytes != 160 || !w.failed || w.active {
			t.Fatal("terminal call reset/refunded shared actual bytes or closed latch")
		}
		if _, _, err := w.stopContext(cleanup); err == nil {
			t.Fatal("terminal stop acquired an uncounted twenty-fifth poll")
		}
		if _, _, err := w.pollContext(t.Context(), start.Add(time.Minute)); err == nil {
			t.Fatal("negative completion reopened positive polling")
		}
	})
}

func TestLocalSearchConsentExpiryNeverRecoversStickyFailureOrCaps(t *testing.T) {
	for _, mode := range []string{"read_error", "caller_cancelled", "earlier_deadline", "poll_cap", "byte_cap", "terminal_error", "terminal_overflow"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				deadline := start.Add(3 * time.Second)
				w := &localSearchWaitBudget{bytes: 37}
				ctx := t.Context()
				switch mode {
				case "read_error":
					call, finish, err := w.pollContext(ctx, deadline)
					if err != nil || chargeOwnedAuthorityBody(call, 23) != nil || finish(errors.New("actual failed read")) == nil || w.bytes != 60 {
						t.Fatal("failed read setup lost charges")
					}
				case "caller_cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "earlier_deadline":
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, start.Add(time.Second))
					defer cancel()
				case "poll_cap":
					w.polls = 24
				case "byte_cap":
					w.bytes = ownedAuthorityMaxBytes
				}
				<-time.After(3 * time.Second)
				_, _, err := w.pollContext(ctx, deadline)
				terminal := mode == "terminal_error" || mode == "terminal_overflow"
				if !terminal {
					if err == nil || err == errLocalSearchWaitExpiry || w.expiryPending {
						t.Fatal("unknown/cancel/cap failure was reclassified as expiry", err)
					}
					if _, _, err := w.stopContext(t.Context()); err == nil {
						t.Fatal("sticky failure acquired negative fallback")
					}
					return
				}
				if err != errLocalSearchWaitExpiry {
					t.Fatal("healthy terminal baseline", err)
				}
				call, finish, err := w.stopContext(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if mode == "terminal_overflow" {
					if chargeOwnedAuthorityBody(call, ownedAuthorityMaxBytes) == nil {
						t.Fatal("prospective terminal overflow accepted")
					}
					err = nil
				} else {
					if chargeOwnedAuthorityBody(call, 23) != nil {
						t.Fatal("actual terminal failed-read charge")
					}
					err = errors.New("terminal storage failure")
				}
				if finish(err) == nil || !w.failed || w.expiryPending || w.polls != 1 {
					t.Fatal("terminal failure reset or refunded shared state")
				}
				if _, _, err := w.stopContext(t.Context()); err == nil {
					t.Fatal("failed terminal call retried")
				}
			})
		})
	}
}

func TestLocalSearchConsentTerminalStopDoesNotSpendDOnPollCadence(t *testing.T) {
	limits := newOwnedReaderFixtureAt(t, time.Now().UTC().Truncate(time.Microsecond), "-expiry-budget").profile.Limits
	limits.MaxCleanupDuration = time.Second
	if limits.Validate() != nil {
		t.Fatal("one-second cleanup allowance must be a valid existing policy")
	}
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		expiry := start.Add(time.Nanosecond)
		active, cancel := context.WithDeadline(t.Context(), expiry)
		defer cancel()
		w := &localSearchWaitBudget{polls: 1, bytes: 37, lastStart: start}
		if _, _, err := w.pollContext(active, expiry); err != errLocalSearchWaitExpiry || !time.Now().Equal(expiry) || !w.failed {
			t.Fatal("healthy final interval did not reach its original cutoff", err)
		}
		deadline := expiry.Add(limits.MaxCleanupDuration)
		cleanup, stop := context.WithDeadline(t.Context(), deadline)
		defer stop()
		call, finish, err := w.stopContext(cleanup)
		if err != nil {
			t.Fatal("negative cleanup spent its original D waiting for positive poll cadence", err)
		}
		actual, bounded := call.Deadline()
		if !time.Now().Equal(expiry) || !bounded || !actual.Equal(deadline) || !w.lastStart.Equal(start) || w.polls != 2 || !w.failed || w.expiryPending {
			t.Fatal("negative cleanup renewed D, waited, updated positive cadence or reset its shared counter")
		}
		if chargeOwnedAuthorityBody(call, 23) != nil || finish(nil) != nil || w.bytes != 60 || !w.failed {
			t.Fatal("negative cleanup reset/refunded bytes or reopened polling")
		}
		if _, _, err := w.stopContext(cleanup); err == nil {
			t.Fatal("terminal cleanup context replayed")
		}
	})
}
