//go:build darwin || linux

package codexexec

import (
	"context"
	"errors"
	"testing"
)

// Original Session/claim and native entry, but no process, socket or model call.
// Completion must sample the original child's authority, not public result flags.
func TestNativeCompletionArbitratesOriginalAuthority(t *testing.T) {
	for _, kind := range []string{"current", "child retired", "session closed", "explicit cancel"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeConstructorFixture(t)
			entry, err := NewNativePreparedProcessBoundaryV2(t.Context(), f.config, f.session, f.input, f.bundle)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			active := &activePreparedProcess{authority: f.config.Authority, context: ctx, cancel: cancel}
			entry.active, entry.consumed = active, true
			switch kind {
			case "child retired":
				if err := entry.native.child.Close(); err != nil {
					t.Fatal(err)
				}
			case "session closed":
				if err := f.session.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "explicit cancel":
				if err := entry.Cancel(t.Context(), f.config.Authority); err != nil {
					t.Fatal(err)
				}
			}
			wantCancelled := kind != "current"
			if got := entry.finish(active); got != wantCancelled {
				t.Fatalf("finish(%s) cancelled=%t want=%t", kind, got, wantCancelled)
			}
			if entry.active != nil || !active.finished || !entry.consumed {
				t.Fatal("completion did not retain consumed authority and retire the active slot")
			}
			if err := entry.Cancel(t.Context(), f.config.Authority); !errors.Is(err, ErrProcessNotActive) {
				t.Fatalf("late cancel=%v want not-active", err)
			}
			if kind == "current" && entry.native.child.Current() != nil {
				t.Fatal("completion retired its own child before observing current authority")
			}
			if err := entry.native.child.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
