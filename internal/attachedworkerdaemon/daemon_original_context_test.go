package attachedworkerdaemon

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

type originalContextValueKey struct{}

func TestDaemonOriginalContextSurvivesAcquisitionRunAndInputRetirement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		value := &struct{ label string }{"original caller"}
		parent, cancel := context.WithTimeout(context.WithValue(t.Context(), originalContextValueKey{}, value), time.Minute)
		defer cancel()
		deadline, _ := parent.Deadline()
		input := canonicalVerifiedInvocation(t)
		carrier := input.VerifiedInput
		bodies := [][]byte{carrier.Context, carrier.Artifacts[0].Body, carrier.CanonicalContext.EventBodies[0], carrier.CanonicalContext.Attachments[0].Body}
		var acquired context.Context
		var daemon *Daemon
		retired, reported := false, false
		source := ownedCarrierSourceFunc(func(ctx context.Context) (Invocation, bool, error) {
			acquired = ctx
			if got, ok := ctx.Deadline(); !ok || got != deadline || ctx.Value(originalContextValueKey{}) != value {
				t.Error("Next did not inherit original caller deadline/value")
			}
			return input, true, nil
		})
		runner := ownedCarrierRunnerFunc(func(ctx context.Context, actual Invocation) (InvocationResult, error) {
			if ctx != acquired || ctx.Err() != nil || actual.VerifiedInput != carrier {
				t.Error("Run did not borrow the SAME live acquisition context/input")
			}
			if got, ok := ctx.Deadline(); !ok || got != deadline || ctx.Value(originalContextValueKey{}) != value {
				t.Error("Run replaced the original caller deadline/value")
			}
			defer func() {
				if ctx.Err() != nil {
					t.Error("acquisition context cancelled before synchronous runner retirement returned")
				}
				retired = true
			}()
			if err := daemon.RequestDrain(parent); err != nil || ctx.Err() != nil {
				t.Errorf("Drain cancelled accepted original acquisition: %v", err)
			}
			return InvocationResult{Process: AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}}, nil
		})
		sink := ownedCarrierSinkFunc(func(ctx context.Context, _ InvocationIdentity, _ InvocationResult, _ error) error {
			reported = true
			if !retired || !errors.Is(acquired.Err(), context.Canceled) || parent.Err() != nil || ctx.Err() != nil {
				t.Error("terminal report did not follow original retirement/cancel with detached report lifetime")
			}
			for i, body := range bodies {
				if !bytes.Equal(body, make([]byte, len(body))) {
					t.Errorf("source-owned buffer %d remains before terminal report", i)
				}
			}
			if carrier.CanonicalContext != nil || carrier.Job.TenantID != "" {
				t.Error("input carrier metadata remains after original buffer retirement")
			}
			return nil
		})
		var err error
		daemon, err = NewDaemon(DaemonConfig{}, source, runner, sink)
		if err != nil {
			t.Fatal(err)
		}
		if err := daemon.Run(parent); err != nil || !retired || !reported {
			t.Fatalf("original lifecycle: retired=%t reported=%t err=%v", retired, reported, err)
		}
	})
}

func TestDaemonOriginalContextAcquisitionConsumesCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		entry := time.Now()
		deadline, _ := parent.Deadline()
		var acquired context.Context
		var daemon *Daemon
		source := ownedCarrierSourceFunc(func(ctx context.Context) (Invocation, bool, error) {
			acquired = ctx
			// A causal acquisition operation consumes virtual time. This is not
			// a real-time sleep or a fresh runtime timeout after acquisition.
			timer := time.NewTimer(8 * time.Second)
			defer timer.Stop()
			<-timer.C
			return validDaemonInvocation("original-deadline"), true, nil
		})
		runner := ownedCarrierRunnerFunc(func(ctx context.Context, _ Invocation) (InvocationResult, error) {
			got, ok := ctx.Deadline()
			if ctx != acquired || !ok || got != deadline || time.Until(got) != 2*time.Second {
				t.Error("acquisition elapsed time was discarded or original context replaced")
			}
			<-ctx.Done()
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) || time.Since(entry) != 10*time.Second {
				t.Error("runner did not expire at the original caller deadline")
			}
			return InvocationResult{}, ctx.Err()
		})
		sink := ownedCarrierSinkFunc(func(ctx context.Context, _ InvocationIdentity, _ InvocationResult, runErr error) error {
			if ctx.Err() != nil || !errors.Is(runErr, context.DeadlineExceeded) {
				t.Error("original expiry lost its causal error or cancelled detached terminal report")
			}
			return daemon.RequestDrain(t.Context())
		})
		var err error
		daemon, err = NewDaemon(DaemonConfig{}, source, runner, sink)
		if err != nil {
			t.Fatal(err)
		}
		if err := daemon.Run(parent); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("original acquisition deadline: %v", err)
		}
	})
}

func TestDaemonOriginalContextRejectedAcquisitionsRetire(t *testing.T) {
	for _, mode := range []string{"source_error", "invalid", "drain_before_handoff"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				input := canonicalVerifiedInvocation(t)
				carrier := input.VerifiedInput
				body := carrier.Context
				var acquired context.Context
				var daemon *Daemon
				cause := errors.New("source rejected original acquisition")
				calls := 0
				source := ownedCarrierSourceFunc(func(ctx context.Context) (Invocation, bool, error) {
					acquired = ctx
					switch mode {
					case "source_error":
						return input, false, cause
					case "invalid":
						input.Identity.OwnerUserID = ""
					case "drain_before_handoff":
						if err := daemon.RequestDrain(parent); err != nil {
							t.Error(err)
						}
					}
					return input, true, nil
				})
				runner := ownedCarrierRunnerFunc(func(context.Context, Invocation) (InvocationResult, error) {
					calls++
					return InvocationResult{}, nil
				})
				sink := ownedCarrierSinkFunc(func(context.Context, InvocationIdentity, InvocationResult, error) error {
					t.Error("rejected acquisition reached terminal sink")
					return nil
				})
				var err error
				daemon, err = NewDaemon(DaemonConfig{}, source, runner, sink)
				if err != nil {
					t.Fatal(err)
				}
				err = daemon.Run(parent)
				if acquired == nil || !errors.Is(acquired.Err(), context.Canceled) || parent.Err() != nil || calls != 0 || daemon.Status().Accepted != 0 {
					t.Fatalf("mode=%s acquisition was not retired without admission: calls=%d err=%v", mode, calls, err)
				}
				if !bytes.Equal(body, make([]byte, len(body))) || carrier.CanonicalContext != nil {
					t.Error("rejected acquisition retained source-owned canonical input")
				}
				if mode == "source_error" && !errors.Is(err, cause) || mode == "invalid" && !errors.Is(err, ErrInvocationInvalid) || mode == "drain_before_handoff" && err != nil {
					t.Errorf("mode=%s lost causal rejection: %v", mode, err)
				}
			})
		})
	}
}

func TestDaemonOriginalContextUnavailableRetiresBeforeIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		seen := make(chan context.Context, 1)
		done := make(chan error, 1)
		calls := 0
		source := ownedCarrierSourceFunc(func(ctx context.Context) (Invocation, bool, error) {
			calls++
			seen <- ctx
			return Invocation{}, false, nil
		})
		runner := ownedCarrierRunnerFunc(func(context.Context, Invocation) (InvocationResult, error) {
			t.Error("unavailable acquisition reached runner")
			return InvocationResult{}, nil
		})
		sink := ownedCarrierSinkFunc(func(context.Context, InvocationIdentity, InvocationResult, error) error { return nil })
		daemon, err := NewDaemon(DaemonConfig{IdleBackoff: time.Hour}, source, runner, sink)
		if err != nil {
			t.Fatal(err)
		}
		go func() { done <- daemon.Run(parent) }()
		acquired := <-seen
		synctest.Wait()
		if !errors.Is(acquired.Err(), context.Canceled) || parent.Err() != nil || calls != 1 {
			t.Error("unavailable original context remains live during idle")
		}
		if err := daemon.RequestDrain(parent); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
}

func TestDaemonOriginalContextAcceptedDrainAndExactCancellationHandoff(t *testing.T) {
	for _, control := range []string{"cancel_active", "shutdown"} {
		t.Run(control, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				seen, started := make(chan context.Context, 1), make(chan context.Context, 1)
				retire := make(chan struct{})
				done := make(chan error, 1)
				input := validDaemonInvocation("original-cancel-handoff")
				source := ownedCarrierSourceFunc(func(ctx context.Context) (Invocation, bool, error) {
					seen <- ctx
					return input, true, nil
				})
				runner := ownedCarrierRunnerFunc(func(ctx context.Context, _ Invocation) (InvocationResult, error) {
					started <- ctx
					<-ctx.Done()
					<-retire
					return InvocationResult{Process: AttemptResult{Cancelled: true, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}}, nil
				})
				sink := ownedCarrierSinkFunc(func(context.Context, InvocationIdentity, InvocationResult, error) error { return nil })
				daemon, err := NewDaemon(DaemonConfig{}, source, runner, sink)
				if err != nil {
					t.Fatal(err)
				}
				go func() { done <- daemon.Run(parent) }()
				acquired, running := <-seen, <-started
				if acquired != running || acquired.Err() != nil {
					t.Error("accepted invocation did not retain live original context")
				}
				if err := daemon.RequestDrain(parent); err != nil || acquired.Err() != nil {
					t.Errorf("Drain crossed active cancellation handoff: %v", err)
				}
				mismatch := input.Identity
				mismatch.AttemptID = "foreign-attempt"
				if err := daemon.CancelActive(parent, mismatch); !errors.Is(err, ErrActiveAttemptMismatch) || acquired.Err() != nil {
					t.Errorf("foreign cancellation affected original context: %v", err)
				}
				shutdown := make(chan error, 1)
				if control == "cancel_active" {
					if err := daemon.CancelActive(parent, input.Identity); err != nil {
						t.Error(err)
					}
					if err := daemon.CancelActive(parent, input.Identity); err != nil {
						t.Error("exact cancellation replay:", err)
					}
				} else {
					go func() { shutdown <- daemon.Shutdown(parent) }()
				}
				synctest.Wait()
				if !errors.Is(acquired.Err(), context.Canceled) || parent.Err() != nil {
					t.Error("active cancellation did not target SAME original acquisition")
				}
				select {
				case <-done:
					t.Error("daemon returned before synchronous runner retirement joined")
				default:
				}
				close(retire)
				if err := <-done; err != nil {
					t.Error(err)
				}
				if control == "shutdown" {
					if err := <-shutdown; err != nil {
						t.Error(err)
					}
				}
			})
		})
	}
}

func TestDaemonOriginalContextDrainVersusAcceptanceHasNoCancelInstallGap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for i := 0; i < 16; i++ {
			parent, cancel := context.WithCancel(t.Context())
			if i%2 == 0 {
				// An unbounded ordinary parent remains supported.
				if _, bounded := parent.Deadline(); bounded {
					t.Fatal("ordinary control unexpectedly bounded")
				}
			}
			entered, release := make(chan context.Context, 1), make(chan struct{})
			done, drained := make(chan error, 1), make(chan error, 1)
			input := validDaemonInvocation("original-drain-race")
			calls := 0
			source := ownedCarrierSourceFunc(func(ctx context.Context) (Invocation, bool, error) {
				entered <- ctx
				<-release
				return input, true, nil
			})
			var daemon *Daemon
			runner := ownedCarrierRunnerFunc(func(ctx context.Context, _ Invocation) (InvocationResult, error) {
				calls++
				// If admission won the mutex, Drain cannot cancel this context.
				if err := daemon.RequestDrain(parent); err != nil || ctx.Err() != nil {
					t.Errorf("accepted race winner was cancelled by Drain: %v", err)
				}
				return InvocationResult{}, nil
			})
			sink := ownedCarrierSinkFunc(func(context.Context, InvocationIdentity, InvocationResult, error) error { return nil })
			var err error
			daemon, err = NewDaemon(DaemonConfig{}, source, runner, sink)
			if err != nil {
				t.Fatal(err)
			}
			go func() { done <- daemon.Run(parent) }()
			acquired := <-entered
			go func() { drained <- daemon.RequestDrain(parent) }()
			close(release)
			if err := <-done; err != nil {
				t.Error(err)
			}
			if err := <-drained; err != nil && !errors.Is(err, ErrDaemonNotRunning) {
				t.Error(err)
			}
			if !errors.Is(acquired.Err(), context.Canceled) || calls > 1 || daemon.Status().Accepted != uint64(calls) {
				t.Errorf("race lost original lifetime/admission accounting: calls=%d accepted=%d", calls, daemon.Status().Accepted)
			}
			cancel()
		}
	})
}
