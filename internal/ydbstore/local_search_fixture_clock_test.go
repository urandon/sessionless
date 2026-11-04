//go:build sessionless_search_offline

package ydbstore

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestLocalSessionWebSearchOfflineFixtureAuthorityClockPrecision(t *testing.T) {
	at := time.Date(2026, 11, 4, 1, 2, 3, 123456000, time.UTC)
	window := domain.OwnedAgentAuthorityWindowV1{ObservedAt: at.Add(-time.Second), PublishedAt: at, ExpiresAt: at.Add(time.Second)}
	if err := window.ValidateAt(at); err != nil {
		t.Fatal("canonical valid authority-window baseline", err)
	}
	raw := at.Add(789 * time.Nanosecond).In(time.FixedZone("fixture-offset", 3600))
	if err := window.ValidateAt(raw); err == nil {
		t.Fatal("nanosecond wall time unexpectedly satisfies canonical SQL authority time")
	}
	canonical := localSearchFixtureAuthorityTime(raw)
	if canonical.Location() != time.UTC || !canonical.Equal(at) || window.ValidateAt(canonical) != nil {
		t.Fatal("fixture clock did not preserve the original microsecond authority sample")
	}
	// Canonical precision is not a renewal, clamp into a valid window or retry.
	for _, sample := range []time.Time{at.Add(-time.Microsecond), window.ExpiresAt, window.ExpiresAt.Add(789 * time.Nanosecond)} {
		if err := window.ValidateAt(localSearchFixtureAuthorityTime(sample)); err == nil {
			t.Fatalf("canonicalized out-of-window sample became authority: %s", sample.Format(time.RFC3339Nano))
		}
	}
	if !window.PublishedAt.Equal(at) || !window.ExpiresAt.Equal(at.Add(time.Second)) {
		t.Fatal("clock conversion mutated the original authority window")
	}
}
