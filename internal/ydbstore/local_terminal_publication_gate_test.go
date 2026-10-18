package ydbstore

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/operationbody"
)

func TestLocalTerminalPublicationCanonicalUsesOriginalPhysicalGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _, p := terminalPublicationFixture(t)
		bodies, err := f.entry.Bodies()
		if err != nil {
			t.Fatal(err)
		}
		entered := false
		f.canonical.beforeOrdinary = func(context.Context) {
			f.canonical.beforeOrdinary = func(ctx context.Context) {
				// Synchronous re-entry while the actual event PUT is in flight;
				// no fabricated gate, permit or physical-owner receipt.
				f.canonical.beforeOrdinary = nil
				entered = true
				calls := f.canonical.calls
				if _, err := bodies.Write(ctx, []byte("overlapping original adapter write")); !errors.Is(err, operationbody.ErrBusy) || f.canonical.calls != calls {
					t.Errorf("event PUT did not own physical gate: err=%v extra effects=%d", err, f.canonical.calls-calls)
				}
				if _, err := bodies.ReserveRetainedPair(ctx, 2, 1); !errors.Is(err, operationbody.ErrBusy) {
					t.Errorf("event PUT permitted retained reservation: %v", err)
				}
			}
		}
		if err := p.Publish(t.Context()); err != nil || !entered {
			t.Fatal("original canonical write control", err, entered)
		}
	})
}
