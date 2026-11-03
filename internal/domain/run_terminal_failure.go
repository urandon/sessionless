package domain

import (
	"bytes"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/strictjson"
)

const KnownMCPServiceErrorV1 = "mcp_service_error"
const ModelOutcomeUnknownV2 = "model_outcome_unknown"
const MCPOutcomeUnknownV3 = "mcp_outcome_unknown"

const (
	SearchConsentStoppedV4     = "search_consent_stopped"
	SearchRejectedBeforeSendV4 = "search_rejected_before_send"
	SearchServiceErrorV4       = "search_service_error"
	SearchOutcomeUnknownV4     = "search_outcome_unknown"
	ModelAfterSearchFailureV4  = "model_after_search_failure"
)

func SearchFailureStopV4(stop string) bool {
	switch stop {
	case SearchConsentStoppedV4, SearchRejectedBeforeSendV4, SearchServiceErrorV4, SearchOutcomeUnknownV4, ModelAfterSearchFailureV4:
		return true
	}
	return false
}

func SearchFailureRejectedReasonV4(reason AttemptOperationReasonV1) bool {
	switch reason {
	case AttemptOperationReasonAuthorityDeniedV1, AttemptOperationReasonCancelledV1, AttemptOperationReasonDeadlineV1, AttemptOperationReasonTransportUnknownV1, AttemptOperationReasonWorkerLostV1:
		return true
	}
	return false
}
func SearchFailureUnknownReasonV4(reason AttemptOperationReasonV1) bool {
	return SearchFailureRejectedReasonV4(reason) || reason == AttemptOperationReasonMalformedV1 || reason == AttemptOperationReasonOverflowV1
}

// SearchFailureNoticeV4 is one canonical strict DATA codec shared by output and
// material readers. Only original capsule/SQL/seal verification authenticates
// these actual tail commitments; the notice cannot confer serving authority.
type SearchFailureNoticeV4 struct {
	Schema      string                              `json:"schema"`
	Code        string                              `json:"code"`
	Execution   string                              `json:"execution"`
	Kind        AttemptOperationKindV1              `json:"tail_kind"`
	Outcome     AttemptOperationOutcomeV1           `json:"tail_outcome"`
	Reason      AttemptOperationReasonV1            `json:"tail_reason"`
	Operation   AttemptOperationDigestV1            `json:"operation_digest"`
	Reservation AttemptOperationReservationDigestV1 `json:"reservation_digest"`
	Observation AttemptOperationObservationDigestV1 `json:"observation_digest"`
}

func (v SearchFailureNoticeV4) Validate() error {
	if v.Schema != "sessionless.run-terminal-notice.v4" || !SearchFailureStopV4(v.Code) || v.Operation.Validate() != nil || v.Reservation.Validate() != nil || v.Observation.Validate() != nil {
		return ErrMembershipDenied
	}
	valid := false
	switch v.Code {
	case SearchConsentStoppedV4:
		valid = v.Execution == "not_executed" && v.Kind == AttemptOperationModelV1 && v.Outcome == AttemptOperationCompletedV1 && v.Reason == AttemptOperationReasonNoneV1
	case SearchRejectedBeforeSendV4:
		valid = v.Execution == "not_sent" && v.Kind == AttemptOperationWebSearchV1 && v.Outcome == AttemptOperationRejectedBeforeSendV1 && SearchFailureRejectedReasonV4(v.Reason)
	case SearchServiceErrorV4:
		valid = v.Execution == "known_service_error" && v.Kind == AttemptOperationWebSearchV1 && v.Outcome == AttemptOperationCompletedV1 && v.Reason == AttemptOperationReasonServiceErrorV1
	case SearchOutcomeUnknownV4:
		valid = v.Execution == "unknown" && v.Kind == AttemptOperationWebSearchV1 && v.Outcome == AttemptOperationAcceptedOutcomeUnknownV1 && SearchFailureUnknownReasonV4(v.Reason)
	case ModelAfterSearchFailureV4:
		valid = v.Kind == AttemptOperationModelV1 && (v.Execution == "not_sent" && v.Outcome == AttemptOperationRejectedBeforeSendV1 && v.Reason == AttemptOperationReasonTransportUnknownV1 || v.Execution == "unknown" && v.Outcome == AttemptOperationAcceptedOutcomeUnknownV1 && (v.Reason == AttemptOperationReasonTransportUnknownV1 || v.Reason == AttemptOperationReasonMalformedV1 || v.Reason == AttemptOperationReasonWorkerLostV1))
	}
	if !valid {
		return ErrMembershipDenied
	}
	return nil
}

func DecodeSearchFailureNoticeV4(body []byte) (SearchFailureNoticeV4, error) {
	var v SearchFailureNoticeV4
	if _, err := strictjson.Parse(body, 2048, 8, 16); err != nil {
		return v, ErrMembershipDenied
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&v) != nil || v.Validate() != nil {
		return SearchFailureNoticeV4{}, ErrMembershipDenied
	}
	raw, err := json.Marshal(v)
	if err != nil || !bytes.Equal(raw, body) {
		return SearchFailureNoticeV4{}, ErrMembershipDenied
	}
	return v, nil
}

// The compact commitment keeps its original shape. Each admitted capsule schema
// selects exactly one stop language; a crossed/unknown pair is never a fallback.
func TerminalFailureSchemaStopValid(schema, stop string) bool {
	return schema == "sessionless.terminal-failure-custody.v1" && stop == KnownMCPServiceErrorV1 || schema == "sessionless.terminal-failure-custody.v2" && stop == ModelOutcomeUnknownV2 || schema == "sessionless.terminal-failure-custody.v3" && stop == MCPOutcomeUnknownV3 || schema == "sessionless.terminal-failure-custody.v4" && SearchFailureStopV4(stop)
}

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
	if v.Version != 1 || v.Scope.Validate() != nil || v.Plan.Validate() != nil || v.Plan.Scope != v.Scope || v.Scope.TenantID != run.TenantID || v.Scope.SessionID != run.SessionID || v.Scope.RunID != run.ID || v.CapturedAt.IsZero() || v.CapturedAt.Nanosecond()%1000 != 0 || v.CapturedAt.Before(run.CreatedAt) || v.CapturedAt.After(run.UpdatedAt) || !TerminalFailureSchemaStopValid(v.CapsuleSchema, v.Stop) || v.Cleanup != "joined_retained" || run.Status != RunFailed || run.TerminalCustody == nil || run.TerminalCustody.Plan != v.Plan || run.TerminalCustody.Correspondence != nil || run.TerminalCustody.LocalEvidence != nil {
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
