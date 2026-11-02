package domain_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func failureRunData(t *testing.T) domain.Run {
	t.Helper()
	run := validRun()
	c := terminalCustodyFixture(t, run)
	run.TerminalCustody = &c
	at := run.CreatedAt.Add(time.Second)
	run.Status = domain.RunFailed
	run.UpdatedAt = at
	run.FinishedAt = &at
	d := strings.Repeat("a", 64)
	run.TerminalFailure = &domain.RunTerminalFailureV1{Version: 1, Scope: c.Plan.Scope, ParentDigest: d, Plan: c.Plan, CapsuleSchema: "sessionless.terminal-failure-custody.v1", AccountingDigest: d, EventsDigest: d, RetainedBodiesDigest: d, RetiredAllocationDigest: d, CapturedAt: at, Stop: domain.KnownMCPServiceErrorV1, Cleanup: "joined_retained"}
	if err := run.Validate(); err != nil {
		t.Fatal("structural F1 DATA", err)
	}
	return run
}
func TestRunTerminalFailureClosedRoundTripAndClone(t *testing.T) {
	run := failureRunData(t)
	body, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	var actual domain.Run
	if err := json.Unmarshal(body, &actual); err != nil || actual.Validate() != nil || !domain.EqualRunTerminalFailureV1(actual.TerminalFailure, run.TerminalFailure) {
		t.Fatalf("F1 roundtrip: %v", err)
	}
	clone := run.Clone()
	clone.TerminalFailure.Plan.Capsule.Size++
	clone.TerminalFailure.Scope.OwnerUserID = "other-owner"
	again, _ := json.Marshal(run)
	if !bytes.Equal(body, again) {
		t.Fatal("failure clone aliases original")
	}
	old, _ := json.Marshal(validRun())
	if bytes.Contains(old, []byte("terminal_failure")) {
		t.Fatal("nil F1 changed legacy bytes")
	}
	for _, change := range []func(*domain.Run){func(r *domain.Run) { r.Status = domain.RunSucceeded }, func(r *domain.Run) { r.TerminalFailure.Stop = "unknown" }, func(r *domain.Run) { r.TerminalFailure.Cleanup = "erased" }, func(r *domain.Run) { r.TerminalFailure.RetiredAllocationDigest = "" }, func(r *domain.Run) { r.TerminalFailure.Plan.Capsule.Size++ }, func(r *domain.Run) { r.TerminalFailure.CapturedAt = r.UpdatedAt.Add(time.Microsecond) }, func(r *domain.Run) { r.TerminalCustody.Correspondence = &domain.TerminalCorrespondenceV1{} }} {
		r := run.Clone()
		change(&r)
		if r.Validate() == nil {
			t.Error("invalid failure correspondence accepted")
		}
	}
}
func TestRunTerminalFailureStrictNestedJSON(t *testing.T) {
	run := failureRunData(t)
	body, _ := json.Marshal(run)
	for _, raw := range []string{
		strings.Replace(string(body), `"terminal_failure":{`, `"terminal_failure":{"unknown":0,`, 1),
		strings.Replace(string(body), `"terminal_failure":{`, `"terminal_failure":{"version":1,`, 1),
		strings.Replace(string(body), `"captured_at":`, `"Captured_at":`, 1),
		strings.Replace(string(body), `"stop":"mcp_service_error"`, `"stop":"\ud800"`, 1),
		strings.Replace(string(body), `"retained_bodies_digest":`, `"retained_bodies_digest":null,"unused":`, 1),
	} {
		var out domain.Run
		if json.Unmarshal([]byte(raw), &out) == nil {
			t.Error("noncanonical failure shape accepted")
		}
	}
}

func TestRunTerminalFailureModelUnknownRequiresExactVersionedPair(t *testing.T) {
	for _, schema := range []string{"sessionless.terminal-failure-custody.v1", "sessionless.terminal-failure-custody.v2", "sessionless.terminal-failure-custody.v3"} {
		for _, stop := range []string{domain.KnownMCPServiceErrorV1, domain.ModelOutcomeUnknownV2, "arbitrary_transport_error"} {
			run := failureRunData(t)
			run.TerminalFailure.CapsuleSchema, run.TerminalFailure.Stop = schema, stop
			want := schema == "sessionless.terminal-failure-custody.v1" && stop == domain.KnownMCPServiceErrorV1 || schema == "sessionless.terminal-failure-custody.v2" && stop == domain.ModelOutcomeUnknownV2
			if (run.Validate() == nil) != want {
				t.Fatal("crossed or unknown failure format/stop language", schema, stop)
			}
			if want {
				raw, err := json.Marshal(run)
				var restored domain.Run
				if err != nil || json.Unmarshal(raw, &restored) != nil || restored.Validate() != nil || restored.TerminalFailure.Version != 1 || !domain.EqualRunTerminalFailureV1(restored.TerminalFailure, run.TerminalFailure) {
					t.Fatal("same compact V1 commitment lost strict capsule variant")
				}
			}
		}
	}
}
