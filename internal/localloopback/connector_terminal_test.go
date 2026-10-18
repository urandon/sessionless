package localloopback

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// These synthetic owners exercise deadline/taint bookkeeping only. Genuine
// Source/Store/socket terminal controls live in ydbstore's connected tests.
func terminalBookkeepingKeeper() *KeeperV1 {
	k := &KeeperV1{source: new(localoperator.Source), bootstrap: new(ydbstore.LocalOwnedStoreBootstrapV1), sourceLifetime: context.Background(), config: domain.ConfiguredInferenceProfileV1{Limits: testLimits()}, policy: ports.LocalLoopbackEgressPolicyV1{Limits: testLimits()}, terminalSealed: make(chan struct{})}
	k.self = k
	return k
}

func TestLocalLoopbackTerminalDeadlineAndOriginalOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		k := terminalBookkeepingKeeper()
		d := time.Now().Add(time.Second)
		for _, bad := range []*KeeperV1{nil, {}, {self: k}} {
			if err := bad.SealTerminal(d); !errors.Is(err, domain.ErrMembershipDenied) {
				t.Fatalf("nonoriginal seal: %v", err)
			}
			if err := bad.StopAndJoinTerminal(t.Context()); !errors.Is(err, domain.ErrMembershipDenied) {
				t.Fatalf("nonoriginal join: %v", err)
			}
		}
		for _, bad := range []time.Time{{}, time.Now(), d.Add(time.Nanosecond)} {
			if err := k.SealTerminal(bad); !errors.Is(err, domain.ErrMembershipDenied) || k.closed {
				t.Fatalf("invalid deadline changed keeper: deadline=%v err=%v", bad, err)
			}
		}
		if err := k.SealTerminal(d); err != nil {
			t.Fatal(err)
		}
		if err := k.SealTerminal(d); err != nil || !k.closed || !k.terminalEligible {
			t.Fatalf("idle repeated seal: %v", err)
		}
		if err := k.SealTerminal(d.Add(time.Nanosecond)); !errors.Is(err, domain.ErrMembershipDenied) || !k.terminalDeadline.Equal(d) {
			t.Fatal("repeated seal renewed original deadline")
		}
		if err := k.StopAndJoinTerminal(t.Context()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second) // virtual expiry after actual idle join
		if err := k.Close(); err != nil || !k.terminalDeadline.Equal(d) {
			t.Fatalf("completed original join was renewed/lost: %v", err)
		}
	})
}

func TestLocalLoopbackTerminalActiveDrainNeverBecomesEligible(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		k := terminalBookkeepingKeeper()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		c := &ConnectionV1{keeper: k, ctx: ctx, cancel: cancel, cleanupTimeout: time.Second, cleanupDone: make(chan struct{})}
		c.self, k.active = c, c
		d := time.Now().Add(time.Second)
		if err := k.SealTerminal(d); !errors.Is(err, domain.ErrMembershipDenied) || !k.closed {
			t.Fatalf("active seal was eligible/not sticky: %v", err)
		}
		c.cleanupCommitted = true
		close(c.cleanupDone)
		if err := k.StopAndJoinTerminal(t.Context()); err != nil {
			t.Fatalf("actual completed failure-cleanup denied: %v", err)
		}
		if err := k.SealTerminal(d); !errors.Is(err, domain.ErrMembershipDenied) || k.terminalEligible {
			t.Fatal("drained active seal became successful frontier")
		}
	})
}

func TestLocalLoopbackTerminalClosedOrCancelledNeverEligible(t *testing.T) {
	for _, name := range []string{"ordinary_closed", "source_cancelled"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				k := terminalBookkeepingKeeper()
				life, cancel := context.WithCancel(t.Context())
				defer cancel()
				k.sourceLifetime = life
				if name == "ordinary_closed" {
					if err := k.Close(); err != nil {
						t.Fatal(err)
					}
				} else {
					cancel()
				}
				d := time.Now().Add(time.Second)
				if err := k.SealTerminal(d); !errors.Is(err, domain.ErrMembershipDenied) || !k.closed || k.terminalEligible {
					t.Fatalf("%s minted eligibility: %v", name, err)
				}
				if err := k.StopAndJoinTerminal(t.Context()); err != nil {
					t.Fatalf("idle failure cleanup denied: %v", err)
				}
				if err := k.SealTerminal(d); !errors.Is(err, domain.ErrMembershipDenied) {
					t.Fatalf("%s cleanup reset failed seal", name)
				}
			})
		})
	}
}

func TestLocalLoopbackTerminalSharedDeadlineStickyTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		k := terminalBookkeepingKeeper()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		c := &ConnectionV1{keeper: k, ctx: ctx, cancel: cancel, cleanupTimeout: time.Second, cleanupDone: make(chan struct{})}
		c.self, k.active = c, c
		d := time.Now().Add(500 * time.Millisecond)
		_ = k.SealTerminal(d) // active seal deliberately ineligible
		start := time.Now()
		done := make(chan error, 1)
		joined := make(chan struct{})
		var release sync.Once
		t.Cleanup(func() {
			cancel()
			release.Do(func() { close(c.cleanupDone) })
			synctest.Wait()
			select {
			case <-joined:
			default:
				t.Error("terminal bookkeeping waiter did not join during cleanup")
			}
		})
		go func() {
			defer close(joined)
			done <- k.StopAndJoinTerminal(t.Context())
		}()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("join returned before original deadline: %v", err)
		default:
		}
		if err := c.Close(); !errors.Is(err, domain.ErrMembershipDenied) || time.Since(start) != 500*time.Millisecond {
			t.Fatalf("ordinary Close ignored terminal cap: elapsed=%v err=%v", time.Since(start), err)
		}
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, domain.ErrMembershipDenied) {
				t.Fatalf("unjoined terminal succeeded: %v", err)
			}
		default:
			t.Fatal("concurrent terminal waiter did not finish")
		}
		release.Do(func() { close(c.cleanupDone) })
		if err := k.Close(); !errors.Is(err, domain.ErrMembershipDenied) || !k.terminalTainted || !c.cleanupTainted || !k.terminalDeadline.Equal(d) {
			t.Fatal("late completion cleared terminal taint or renewed deadline")
		}
	})
}

func TestLocalLoopbackTerminalReadyCompletionWinsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		k := terminalBookkeepingKeeper()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		c := &ConnectionV1{keeper: k, ctx: ctx, cancel: cancel, cleanupTimeout: time.Second, cleanupDone: make(chan struct{}), cleanupCommitted: true}
		c.self, k.active = c, c
		d := time.Now().Add(time.Second)
		_ = k.SealTerminal(d)
		close(c.cleanupDone)
		time.Sleep(time.Second) // both actual completion and deadline ready
		if err := k.StopAndJoinTerminal(t.Context()); err != nil || k.terminalTainted || c.cleanupTainted {
			t.Fatalf("ready real join lost to deadline: %v", err)
		}
	})
}

func TestLocalLoopbackTerminalWakesPreexistingOrdinaryClose(t *testing.T) {
	for _, tc := range []struct {
		name           string
		terminal, want time.Duration
	}{
		{name: "earlier_terminal_wakes", terminal: 500 * time.Millisecond, want: 500 * time.Millisecond},
		{name: "later_terminal_never_renews_operation", terminal: 1200 * time.Millisecond, want: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				k := terminalBookkeepingKeeper()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				c := &ConnectionV1{keeper: k, ctx: ctx, cancel: cancel, cleanupTimeout: time.Second, cleanupDone: make(chan struct{})}
				c.self, k.active = c, c
				start := time.Now()
				done, joined := make(chan error, 1), make(chan struct{})
				var release sync.Once
				t.Cleanup(func() {
					cancel()
					release.Do(func() { close(c.cleanupDone) })
					synctest.Wait()
					select {
					case <-joined:
					default:
						t.Error("preexisting Close waiter did not join during cleanup")
					}
				})
				go func() {
					defer close(joined)
					done <- c.Close()
				}()
				synctest.Wait() // Close is already blocked on its original operation bound
				select {
				case err := <-done:
					t.Fatalf("ordinary Close completed before cleanup: %v", err)
				default:
				}
				time.Sleep(200 * time.Millisecond) // virtual terminal boundary, not readiness
				deadline := start.Add(tc.terminal)
				if err := k.SealTerminal(deadline); !errors.Is(err, domain.ErrMembershipDenied) {
					t.Fatalf("active original became terminal eligible: %v", err)
				}
				// Waiting permits synctest to advance to the first actual pending bound.
				bound := time.NewTimer(2 * time.Second)
				defer bound.Stop()
				var err error
				select {
				case err = <-done:
				case <-bound.C:
					select {
					case err = <-done:
					default:
						t.Fatal("original Close waiter exceeded its explicit test bound")
					}
				}
				if !errors.Is(err, domain.ErrMembershipDenied) || time.Since(start) != tc.want {
					t.Fatalf("preexisting Close ignored original minimum bound: elapsed=%v want=%v err=%v", time.Since(start), tc.want, err)
				}
				if !c.cleanupTainted {
					t.Fatal("unjoined original Close lost timeout taint")
				}
			})
		})
	}
}
