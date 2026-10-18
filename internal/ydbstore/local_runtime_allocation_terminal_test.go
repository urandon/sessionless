package ydbstore

import (
	"context"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// Genuine original file Source, allocation factory, issuer, Store preparation
// burn, registration and Enter/operation lease lifecycle. SQL isolation/funding
// remain scripted. These are bridge stop facts, NOT a Keeper/socket join,
// durable retirement, namespace erasure or terminal completion receipt.
func TestLocalRuntimeTerminalBridgeAfterActualOperationRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, allocation, op := localModelLeaseFixture(t)
		if err := f.bridge.VerifyStoppedLocalTerminal(allocation); err == nil {
			t.Error("live entered allocation claimed stopped")
		}
		lease, err := f.bridge.BeginLocalModelOperation(f.actual.source, allocation, op, f.parent.graph.at)
		if err != nil || lease == nil {
			t.Fatal("original entered operation", err)
		}
		t.Cleanup(func() {
			if err := lease.Close(); err != nil {
				t.Error(err)
			}
		})
		if lease.Context().Err() != nil {
			t.Fatal("positive original operation was already cancelled")
		}
		if err := lease.Close(); err != nil {
			t.Fatal("actual original lease release", err)
		}
		if lease.Context().Err() == nil {
			t.Error("released actual operation retained its lifetime")
		}
		if err := f.bridge.StopAndJoinLocalTerminal(t.Context(), allocation); err != nil {
			t.Fatal("original bridge after actual slot release", err)
		}
		if err := f.bridge.VerifyStoppedLocalTerminal(allocation); err != nil {
			t.Error("original historical stopped verification", err)
		}
		if err := f.bridge.VerifyEnteredLocalAllocation(allocation, f.parent.graph.at); err == nil {
			t.Error("stopped bridge retained live entry authority")
		}
		if _, err := f.actual.source.Configuration(); err != nil || f.actual.ctx.Err() != nil {
			t.Error("owned allocation stop closed borrowed Source/caller", err)
		}
		if err := f.bridge.VerifyOwnedAllocationPublication(f.publication, f.parent.graph.at); err != nil {
			t.Error("stop erased original historical preparation authenticity", err)
		}
	})
}

func TestLocalRuntimeTerminalBridgeOutstandingCancelledSlotDenies(t *testing.T) {
	for _, cause := range []string{"allocation_closed", "invocation_cancelled"} {
		t.Run(cause, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, allocation, op := localModelLeaseFixture(t)
				lease, err := f.bridge.BeginLocalModelOperation(f.actual.source, allocation, op, f.parent.graph.at)
				if err != nil || lease == nil {
					t.Fatal("original outstanding operation", err)
				}
				t.Cleanup(func() {
					if err := lease.Close(); err != nil {
						t.Error(err)
					}
				})
				if cause == "allocation_closed" {
					if err := f.bridge.Close(); err != nil {
						t.Fatal(err)
					}
				} else {
					f.actual.cancel()
				}
				synctest.Wait()
				if lease.Context().Err() == nil {
					t.Fatal("causal original operation cancellation not observed")
				}
				if err := f.bridge.StopAndJoinLocalTerminal(t.Context(), allocation); err == nil {
					t.Error("cancelled but unreleased actual slot fabricated a join")
				}
				if err := f.bridge.VerifyStoppedLocalTerminal(allocation); err == nil {
					t.Error("outstanding actual slot authenticated as stopped")
				}
				// Only the real lease lifecycle removes that slot. No test mutates
				// allocation closed/operation internals to manufacture success.
				if err := lease.Close(); err != nil {
					t.Fatal(err)
				}
				if err := f.bridge.VerifyStoppedLocalTerminal(allocation); err != nil {
					t.Error("actual release did not resolve original outstanding slot", err)
				}
				if _, err := f.actual.source.Configuration(); err != nil {
					t.Error("stop/caller cancellation closed borrowed Source", err)
				}
			})
		})
	}
}

func TestLocalRuntimeTerminalBridgeRejectsForeignCopiesAndInvalidContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, allocation, op := localModelLeaseFixture(t)
		copyBridge := *f.bridge // pointer fields only; no sync primitive is copied
		var absent *serverlessharness.BoundedOwnedAllocationPreparationV1
		if absent.StopAndJoinLocalTerminal(t.Context(), allocation) == nil || absent.VerifyStoppedLocalTerminal(allocation) == nil {
			t.Error("nil bridge supplied stop proof")
		}
		if copyBridge.StopAndJoinLocalTerminal(t.Context(), allocation) == nil || copyBridge.VerifyStoppedLocalTerminal(allocation) == nil {
			t.Error("bridge copy supplied original stop proof")
		}
		if f.bridge.StopAndJoinLocalTerminal(nil, allocation) == nil {
			t.Error("nil cleanup context accepted")
		}
		cancelled, cancel := context.WithCancel(t.Context())
		cancel()
		if f.bridge.StopAndJoinLocalTerminal(cancelled, allocation) == nil {
			t.Error("cancelled cleanup context accepted")
		}
		other, foreign, _ := localModelLeaseFixture(t)
		if err := foreign.ValidateInitial(); err != nil {
			t.Fatal("genuine foreign initial allocation required", err)
		}
		if err := f.bridge.StopAndJoinLocalTerminal(t.Context(), foreign); err == nil {
			t.Error("foreign genuine allocation authenticated at original bridge")
		}
		if err := f.bridge.VerifyStoppedLocalTerminal(foreign); err == nil {
			t.Error("foreign original identity passed historical stop verification")
		}
		if err := f.bridge.StopAndJoinLocalTerminal(t.Context(), domain.RegisteredAllocationV2{}); err == nil {
			t.Error("malformed zero allocation authenticated at original bridge")
		}
		// Invalid/copied callers must not close the original winner: prove the
		// original can still acquire and release its genuine operation slot.
		lease, err := f.bridge.BeginLocalModelOperation(f.actual.source, allocation, op, f.parent.graph.at)
		if err != nil || lease == nil {
			t.Fatal("original winner after invalid callers", err)
		}
		t.Cleanup(func() {
			if err := lease.Close(); err != nil {
				t.Error(err)
			}
		})
		if lease.Context().Err() != nil {
			t.Fatal("foreign/malformed denial cancelled original real operation")
		}
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
		if err := other.bridge.StopAndJoinLocalTerminal(t.Context(), foreign); err != nil {
			t.Error("genuine independent foreign-owner positive failed", err)
		}
		if err := f.bridge.StopAndJoinLocalTerminal(t.Context(), allocation); err != nil {
			t.Fatal("valid original stop after invalid callers and real lease release", err)
		}
		if err := f.bridge.VerifyStoppedLocalTerminal(allocation); err != nil {
			t.Error("original stopped allocation lost its actual identity", err)
		}
	})
}
