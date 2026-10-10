package ports_test

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

func TestRunExplanationCommittedOutcomeShape(t *testing.T) {
	now := time.Date(2026, 10, 10, 15, 0, 0, 0, time.UTC)
	v, err := webcontract.NewRunExplanationV1(runexplanation.Snapshot{ReadAt: now, Run: domain.Run{ID: "run", TenantID: "tenant", SessionID: "session", TriggerEventID: "trigger", SubscriptionConnectionID: "connection", Status: domain.RunCreated, IdempotencyKey: "key", CreatedAt: now, UpdatedAt: now}})
	if err != nil {
		t.Fatal(err)
	}
	for _, good := range []ports.RunExplanationReadOutcomeV1{{Kind: ports.RunExplanationReadSuccessV1, Explanation: &v}, {Kind: ports.RunExplanationReadNotFoundV1}, {Kind: ports.RunExplanationReadRateLimitedV1, RetryAfter: time.Second}, {Kind: ports.RunExplanationReadRateLimitedV1, RetryAfter: 5 * time.Second}} {
		if good.Validate() != nil {
			t.Fatal("valid outcome rejected")
		}
	}
	for _, bad := range []ports.RunExplanationReadOutcomeV1{{}, {Kind: "raw"}, {Kind: ports.RunExplanationReadSuccessV1}, {Kind: ports.RunExplanationReadSuccessV1, Explanation: &v, RetryAfter: time.Second}, {Kind: ports.RunExplanationReadNotFoundV1, Explanation: &v}, {Kind: ports.RunExplanationReadNotFoundV1, RetryAfter: time.Second}, {Kind: ports.RunExplanationReadRateLimitedV1}, {Kind: ports.RunExplanationReadRateLimitedV1, Explanation: &v, RetryAfter: time.Second}, {Kind: ports.RunExplanationReadRateLimitedV1, RetryAfter: -time.Second}, {Kind: ports.RunExplanationReadRateLimitedV1, RetryAfter: 6 * time.Second}, {Kind: ports.RunExplanationReadRateLimitedV1, RetryAfter: time.Millisecond}} {
		if bad.Validate() == nil {
			t.Fatal("invalid outcome accepted")
		}
	}
}
