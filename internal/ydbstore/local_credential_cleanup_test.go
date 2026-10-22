package ydbstore

import (
	"testing"
	"testing/synctest"
	"time"
)

// Pure original-window DATA tests; no Source, SQL or transport authority is
// inferred from these internal values. Joined tests cover the actual entry.
func TestLocalSessionCredentialCleanupDeadlineNeverRenews(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		w := &localCredentialCleanup{prepared: true, active: start.Add(2 * time.Minute), ceiling: start.Add(150 * time.Second), duration: 30 * time.Second}
		first := w.begin()
		if !first.Equal(start.Add(30 * time.Second)) {
			t.Fatal("early cleanup borrowed unused execution allowance")
		}
		time.Sleep(20 * time.Second) // deterministic virtual advancement, not readiness
		if second := w.begin(); !second.Equal(first) {
			t.Fatal("second cleanup branch renewed its absolute deadline")
		}
		time.Sleep(20 * time.Second)
		if late := w.begin(); !late.Equal(first) || !late.Before(time.Now()) {
			t.Fatal("expired cleanup deadline was renewed")
		}
	})
}

func TestLocalSessionCredentialCleanupClampsOriginalOuterCeiling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		w := &localCredentialCleanup{prepared: true, active: start.Add(time.Minute), ceiling: start.Add(90 * time.Second), duration: 30 * time.Second}
		time.Sleep(80 * time.Second)
		if d := w.begin(); !d.Equal(w.ceiling) {
			t.Fatal("late cleanup exceeded the original active-plus-cleanup ceiling")
		}
		time.Sleep(20 * time.Second)
		if d := w.begin(); !d.Equal(w.ceiling) {
			t.Fatal("late repeated branch minted a new cleanup window")
		}
	})
}
