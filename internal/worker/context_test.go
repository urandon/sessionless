package worker

import (
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func TestHistoricalToolContextEnforcesCountAndByteLimits(t *testing.T) {
	t.Parallel()
	records := []sessioncontext.EventPayload{
		{Event: domain.SessionEvent{Kind: domain.SessionEventToolCall}, Payload: []byte(`{"call":1}`)},
		{Event: domain.SessionEvent{Kind: domain.SessionEventToolResult}, Payload: []byte(`{"result":1}`)},
	}
	limits := domain.ProductLimits{MaxToolEvents: 1, MaxToolEventBytes: 1 << 20}
	if err := validateContextToolLimit(records, limits); err == nil {
		t.Fatal("historical tool event count above the admitted limit was accepted")
	}
	limits.MaxToolEvents = 2
	limits.MaxToolEventBytes = uint64(len(records[0].Payload) + len(records[1].Payload) - 1)
	if err := validateContextToolLimit(records, limits); err == nil {
		t.Fatal("historical tool payload bytes above the admitted limit were accepted")
	}
	limits.MaxToolEventBytes++
	if err := validateContextToolLimit(records, limits); err != nil {
		t.Fatalf("historical tool context at the admitted bounds was rejected: %v", err)
	}
}
