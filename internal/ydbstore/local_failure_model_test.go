package ydbstore

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestLocalFailureCapsuleV2StrictAbsentDiscriminant(t *testing.T) {
	// Codec DATA only, deliberately not a valid parent/accounting producer.
	// Parsing this shape authenticates no operation and cannot admit a writer.
	c := localFailureCapsule{Schema: "sessionless.terminal-failure-custody.v2", Version: 2, Stop: domain.ModelOutcomeUnknownV2, Cleanup: "joined_retained", Accounting: ports.OwnedOperationFailureAccountingV1{Version: 2}, Operations: make([]localFailureOperationV1, 3)}
	for i := range c.Operations {
		c.Operations[i].Reservation.Operation.Kind = domain.AttemptOperationModelV1
		c.Operations[i].Observation.Outcome = domain.AttemptOperationCompletedV1
		c.Operations[i].Observation.Reason = domain.AttemptOperationReasonNoneV1
		c.Operations[i].Normalized = json.RawMessage(`{}`)
	}
	c.Operations[1].Reservation.Operation.Kind = domain.AttemptOperationMCPToolV1
	c.Operations[2].Observation.Outcome, c.Operations[2].Observation.Reason = domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonMalformedV1
	c.Operations[2].Normalized = json.RawMessage(`null`)
	body, err := encodeLocalFailureCapsule(c, domain.MaxTerminalCapsuleBytesV1)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(body)
	parsed, err := decodeLocalFailureCapsule(body)
	if err != nil || parsed.Version != 2 || string(parsed.Operations[2].Normalized) != "null" {
		t.Fatal("explicit UNKNOWN null lost", err)
	}
	clearLocalFailureNormalized(parsed)
	if _, err := localFailurePlan(domain.Run{}, domain.ProductLimits{}, c); err == nil {
		t.Fatal("codec DATA became authenticated original writer")
	}
	for _, raw := range []string{
		strings.Replace(string(body), `,"normalized":null`, ``, 1),
		strings.Replace(string(body), `"normalized":null`, `"normalized":{}`, 1),
		strings.Replace(string(body), `"normalized":null`, `"normalized":"null"`, 1),
		strings.Replace(string(body), `"normalized":{}`, `"normalized":null`, 1),
		strings.Replace(string(body), `"schema":"sessionless.terminal-failure-custody.v2"`, `"schema":"sessionless.terminal-failure-custody.v1"`, 1),
		strings.Replace(string(body), `"stop":"model_outcome_unknown"`, `"stop":"mcp_service_error"`, 1),
		strings.Replace(string(body), `"version":2`, `"version":1`, 1),
		strings.Replace(string(body), `"normalized":null`, `"normalized":null,"normalized":null`, 1),
	} {
		if bytes.Equal([]byte(raw), body) {
			t.Fatal("codec mutation did not change candidate")
		}
		out, err := decodeLocalFailureCapsule([]byte(raw))
		clearLocalFailureNormalized(out)
		if err == nil {
			t.Fatal("unknown response/member/version ambiguity accepted")
		}
	}
}
