package webcontract_test

import (
	"encoding/json"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

// Correlation is an additive contract, not a claim that an explanation outside
// the loaded page was read. Existing BFF/client integration is owned by #188.
func TestSessionEventCanonicalRunCorrelation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		eventID domain.SessionEventID
		runID   *domain.RunID
	}{
		{name: "unlinked_notice", eventID: "evt-run-like-but-opaque"},
		{name: "managed_notice", eventID: "opaque-managed-event", runID: eventRunID("run-managed")},
		{name: "attached_notice", eventID: "unrelated-attached-event", runID: eventRunID("run-attached")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			event := webcontract.SessionEvent{
				EventID: tc.eventID, RunID: tc.runID, Sequence: 5,
				Kind:      domain.SessionEventSystemNotice,
				CreatedAt: time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC),
			}
			encoded, err := json.Marshal(event)
			if err != nil {
				t.Fatalf("marshal event %q: %v", tc.eventID, err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatalf("decode event %q: %v", tc.eventID, err)
			}
			raw, found := fields["run_id"]
			if tc.runID == nil {
				if found {
					t.Fatalf("unlinked event got run_id %s; want omitted", raw)
				}
				return
			}
			var got domain.RunID
			if !found {
				t.Fatal("explicit canonical RunID omitted")
			}
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("decode run_id: %v", err)
			}
			if got != *tc.runID {
				t.Errorf("run_id got %q; want exact %q", got, *tc.runID)
			}
			for _, private := range []string{"tenant_id", "owner_user_id", "worker_id", "lease_id", "provider"} {
				if value, found := fields[private]; found {
					t.Errorf("private field %s leaked as %s", private, value)
				}
			}
		})
	}
}

func eventRunID(value domain.RunID) *domain.RunID { return &value }
