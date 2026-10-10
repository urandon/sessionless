package webcontract_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

func explanationFixture(t *testing.T) (webcontract.RunExplanationV1, runexplanation.Snapshot) {
	t.Helper()
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	run := domain.Run{ID: "public-run", TenantID: "private-tenant", SessionID: "public-session", TriggerEventID: "private-trigger", SubscriptionConnectionID: "private-subscription", Status: domain.RunFailed, IdempotencyKey: "private-idempotency", CreatedAt: at, UpdatedAt: at.Add(time.Second)}
	finished := run.UpdatedAt
	run.FinishedAt = &finished
	attempt := domain.Attempt{ID: "public-attempt", TenantID: run.TenantID, RunID: run.ID, Number: 1, Status: domain.AttemptRunning, WorkerID: "private-worker", CreatedAt: at, UpdatedAt: at}
	placement, _ := domain.ManagedExecutionPlacementV2(strings.Repeat("1", 64))
	initialRun := run
	initialRun.Status = domain.RunCreated
	initialRun.UpdatedAt = at
	initialRun.FinishedAt = nil
	head, _, err := runexplanation.Select(nil, initialRun, attempt, placement, at)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	notice := runexplanation.NoticeInput{Schema: runexplanation.TerminalNoticeSchemaV1, Code: "harness_failed"}
	head, _, _, err = runexplanation.RecordTerminal(head, run, attempt, &notice, "public-event", 42, finished)
	if err != nil {
		t.Fatalf("terminal: %v", err)
	}
	snapshot := runexplanation.Snapshot{Run: run, ReadAt: finished.Add(time.Hour), Head: &head, Attempt: &attempt}
	value, err := webcontract.NewRunExplanationV1(snapshot)
	if err != nil {
		t.Fatalf("public response: %v", err)
	}
	return value, snapshot
}

func TestRunExplanationSafeManifestAndHistoricalOmission(t *testing.T) {
	t.Parallel()
	value, snapshot := explanationFixture(t)
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(encoded) > runexplanation.MaxResponseBytes {
		t.Errorf("body bytes=%d ceiling=%d", len(encoded), runexplanation.MaxResponseBytes)
	}
	for _, private := range []string{"private-tenant", "private-trigger", "private-subscription", "private-idempotency", "private-worker", "tenant_id", "owner_user_id", "worker_id", "connection_id", "generation", "fingerprint", "harness_failed", "execution_placement", "provider"} {
		if strings.Contains(string(encoded), private) {
			t.Errorf("public JSON contains private source %q: %s", private, encoded)
		}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	want := []string{"version", "run_id", "session_id", "read_at", "status", "created_at", "updated_at", "finished_at", "admission", "terminal", "attempt", "attached", "coverage"}
	if len(fields) != len(want) {
		t.Errorf("field count=%d want=%d JSON=%s", len(fields), len(want), encoded)
	}
	for _, name := range want {
		if _, ok := fields[name]; !ok {
			t.Errorf("missing manifest field=%s", name)
		}
	}
	if string(fields["admission"]) != `{"availability":"unknown"}` || string(fields["attached"]) != `{"availability":"unknown"}` {
		t.Errorf("unknown groups leaked unavailable fields: %s", encoded)
	}
	if value.Status != domain.RunFailed || value.Attempt.Status != domain.AttemptRunning || value.Terminal.ReasonCode != runexplanation.ExecutionFailed {
		t.Errorf("independent groups=%+v", value)
	}
	snapshot.Head = nil
	historical, err := webcontract.NewRunExplanationV1(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(historical)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "event_id") || strings.Contains(string(encoded), "attempt_id") || strings.Contains(string(encoded), "reason_code") || strings.Contains(string(encoded), "observed_at") {
		t.Errorf("historical response fabricated fields: %s", encoded)
	}
}

func TestRunExplanationRejectsCorruptTransport(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*webcontract.RunExplanationV1)
	}{
		{"version", func(v *webcontract.RunExplanationV1) { v.Version = 2 }},
		{"status_raw", func(v *webcontract.RunExplanationV1) { v.Status = "secret status" }},
		{"attempt_status_raw", func(v *webcontract.RunExplanationV1) { v.Attempt.Status = "secret attempt" }},
		{"unknown_with_reason", func(v *webcontract.RunExplanationV1) { v.Admission.ReasonCode = runexplanation.CapacityBusy }},
		{"raw_terminal_reason", func(v *webcontract.RunExplanationV1) { v.Terminal.ReasonCode = "raw provider failure" }},
		{"cancel_status_mismatch", func(v *webcontract.RunExplanationV1) { v.Terminal.ReasonCode = runexplanation.CanonicalCancelled }},
		{"terminal_missing_sequence", func(v *webcontract.RunExplanationV1) { v.Terminal.EventSequence = 0 }},
		{"coverage_mismatch", func(v *webcontract.RunExplanationV1) { v.Coverage.Operational = runexplanation.Recorded }},
		{"future_read_source", func(v *webcontract.RunExplanationV1) { v.ReadAt = v.CreatedAt.Add(-time.Second) }},
		{"nonutc", func(v *webcontract.RunExplanationV1) { v.ReadAt = v.ReadAt.In(time.FixedZone("offset", 3600)) }},
		{"raw_fact", func(v *webcontract.RunExplanationV1) { v.Attached.Fact = "raw host value" }},
		{"fabricated_attempt_zero", func(v *webcontract.RunExplanationV1) { v.Attempt.Number = 0 }},
		{"oversized_id", func(v *webcontract.RunExplanationV1) {
			v.RunID = domain.RunID(strings.Repeat("a", runexplanation.MaxResponseBytes))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := explanationFixture(t)
			tc.mutate(&v)
			if err := v.Validate(); err == nil {
				t.Errorf("invalid response accepted=%+v", v)
			}
			if encoded, err := json.Marshal(v); err == nil {
				t.Errorf("invalid enum/source emitted JSON=%s", encoded)
			}
		})
	}
}

func TestClosedEnumsCannotEmitRawStrings(t *testing.T) {
	t.Parallel()
	values := []any{runexplanation.Availability("private"), runexplanation.AdmissionOutcome("private"), runexplanation.AdmissionReason("private"), runexplanation.TerminalReason("private"), runexplanation.AttachedFact("private"), runexplanation.Freshness("private"), runexplanation.AdmissionCoverage("private"), webcontract.RunTerminalNoticeV1{Version: 1, ReasonCode: "private"}}
	for _, value := range values {
		t.Run(reflect.TypeOf(value).String(), func(t *testing.T) {
			if encoded, err := json.Marshal(value); err == nil {
				t.Errorf("type=%T leaked raw enum JSON=%s", value, encoded)
			}
		})
	}
}

func TestRunExplanationUnknownNonterminalAndDurableTime(t *testing.T) {
	t.Parallel()
	_, s := explanationFixture(t)
	s.Head = nil
	s.Run.Status = domain.RunRunning
	s.Run.FinishedAt = nil
	value, err := webcontract.NewRunExplanationV1(s)
	if err != nil {
		t.Fatal(err)
	}
	if value.Terminal.Availability != runexplanation.NotApplicable || value.Admission.Availability != runexplanation.Unknown || value.Attempt.Availability != runexplanation.Unknown {
		t.Errorf("missing nonterminal evidence=%+v", value)
	}
	s.Run.Status = domain.RunSucceeded
	finished := s.Run.UpdatedAt
	s.Run.FinishedAt = &finished
	value, err = webcontract.NewRunExplanationV1(s)
	if err != nil {
		t.Fatal(err)
	}
	if value.Terminal.Availability != runexplanation.Unknown {
		t.Errorf("terminal with no notice must remain unknown=%+v", value.Terminal)
	}
	if runexplanation.MaxProjectionBytes != 8192 || runexplanation.MaxResponseBytes != 16384 || runexplanation.MaxPointStatements != 20 || runexplanation.ResourceDeadline != 3*time.Second || runexplanation.MinimumRefreshInterval != 5*time.Second {
		t.Error("accepted numeric budgets changed")
	}
}
