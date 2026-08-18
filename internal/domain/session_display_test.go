package domain_test

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestSessionDisplayIsBoundedAndConsistent(t *testing.T) {
	at := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	runID, status, origin := domain.RunID("run-1"), domain.RunRunning, domain.Frontend("web")
	display := domain.SessionDisplay{
		TenantID: "tenant-a", SessionID: "session-1", Title: "A title", Preview: "A preview",
		Origin: &origin, CurrentRunID: &runID, CurrentStatus: &status, UpdatedAt: at,
	}
	if err := display.Validate(); err != nil {
		t.Fatal(err)
	}
	display.CurrentRunID = nil
	if err := display.Validate(); err == nil {
		t.Fatal("run status without run ID accepted")
	}
	display.CurrentStatus = nil
	display.Title = strings.Repeat("x", domain.MaxSessionTitleRunes+1)
	if err := display.Validate(); err == nil {
		t.Fatal("oversized title accepted")
	}
}

func TestBoundedSessionTextNormalizesWhitespaceAndUnicode(t *testing.T) {
	if got := domain.BoundedSessionText("  hello\n\tworld  ", 20); got != "hello world" {
		t.Fatalf("normalized text = %q", got)
	}
	if got := domain.BoundedSessionText("абвгд", 3); got != "абв" {
		t.Fatalf("Unicode truncation = %q", got)
	}
}
