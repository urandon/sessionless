package domain

import (
	"bytes"
	"encoding/json"
	"time"
)

const KnownMCPServiceErrorV1 = "mcp_service_error"

// RunTerminalFailureV1 is retained correspondence DATA, never a producer,
// dispatch grant, measured bill or physical erasure receipt. The first writer
// admits only a known MCP service error before any success candidate exists.
type RunTerminalFailureV1 struct {
	Version                 uint32                  `json:"version"`
	Scope                   AttemptOperationScopeV1 `json:"scope"`
	ParentDigest            string                  `json:"parent_digest"`
	Plan                    TerminalCustodyPlanV1   `json:"plan"`
	CapsuleSchema           string                  `json:"capsule_schema"`
	AccountingDigest        string                  `json:"accounting_digest"`
	EventsDigest            string                  `json:"events_digest"`
	RetainedBodiesDigest    string                  `json:"retained_bodies_digest"`
	RetiredAllocationDigest string                  `json:"retired_allocation_digest"`
	CapturedAt              time.Time               `json:"captured_at"`
	Stop                    string                  `json:"stop"`
	Cleanup                 string                  `json:"cleanup"`
}

func (v RunTerminalFailureV1) ValidateForRun(run Run) error {
	deny := func() error {
		return ValidationError{Field: "run.terminal_failure", Reason: "requires exact known-error retained correspondence"}
	}
	if v.Version != 1 || v.Scope.Validate() != nil || v.Plan.Validate() != nil || v.Plan.Scope != v.Scope || v.Scope.TenantID != run.TenantID || v.Scope.SessionID != run.SessionID || v.Scope.RunID != run.ID || v.CapturedAt.IsZero() || v.CapturedAt.Nanosecond()%1000 != 0 || v.CapturedAt.Before(run.CreatedAt) || v.CapturedAt.After(run.UpdatedAt) || v.CapsuleSchema != "sessionless.terminal-failure-custody.v1" || v.Stop != KnownMCPServiceErrorV1 || v.Cleanup != "joined_retained" || run.Status != RunFailed || run.TerminalCustody == nil || run.TerminalCustody.Plan != v.Plan || run.TerminalCustody.Correspondence != nil || run.TerminalCustody.LocalEvidence != nil {
		return deny()
	}
	if v.CapturedAt != v.CapturedAt.UTC().Truncate(time.Microsecond) || run.StartedAt != nil && v.CapturedAt.Before(*run.StartedAt) || run.FinishedAt == nil || !run.FinishedAt.Equal(run.UpdatedAt) {
		return deny()
	}
	for _, d := range []string{v.ParentDigest, v.AccountingDigest, v.EventsDigest, v.RetainedBodiesDigest, v.RetiredAllocationDigest} {
		if !derivedDigestValid(d) {
			return deny()
		}
	}
	return nil
}

func EqualRunTerminalFailureV1(a, b *RunTerminalFailureV1) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func (v *RunTerminalFailureV1) UnmarshalJSON(body []byte) error {
	if v == nil || len(body) > MaxRunTerminalCustodyBytesV1 || terminalJSONPreflight(body, "terminal_failure") != nil {
		return terminalCustodyError()
	}
	type plain RunTerminalFailureV1
	var p plain
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil {
		return terminalCustodyError()
	}
	*v = RunTerminalFailureV1(p)
	return nil
}
