package canonicaloutput_test

import (
	"bytes"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestMCPUnknownToolOutcomeStrictFiveFieldsAndPair(t *testing.T) {
	d := strings.Repeat("a", 64)
	body, err := canonicaloutput.EncodeMCPUnknownToolOutcomeV1(domain.AttemptOperationDigestV1(d), domain.AttemptOperationReservationDigestV1(d), domain.AttemptOperationObservationDigestV1(d))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(body)
	if _, err := canonicaloutput.DecodeMCPUnknownToolOutcomeV1(body); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		strings.Replace(string(body), `"accepted_outcome_unknown"`, `"service_error"`, 1),
		strings.Replace(string(body), `"operation_digest":`, `"Operation_digest":`, 1),
		strings.Replace(string(body), `"schema":`, `"extra":0,"schema":`, 1),
		strings.Replace(string(body), `"schema":`, `"schema":"sessionless.tool-outcome.v1","schema":`, 1),
		strings.Replace(string(body), `"schema":`, `"content":[],"schema":`, 1),
		strings.Replace(string(body), `"schema":`, `"domain_error":[],"schema":`, 1),
		strings.Replace(string(body), `"schema":`, `"response":null,"schema":`, 1),
		string(body) + " ",
	} {
		if _, err := canonicaloutput.DecodeMCPUnknownToolOutcomeV1([]byte(raw)); err == nil {
			t.Fatal("nonclosed UNKNOWN outcome accepted", raw)
		}
	}
	pair := []ports.ExecutionToolEvent{{Kind: domain.SessionEventToolCall, CallID: "c", ToolName: "lookup", Payload: []byte(`{"arguments":{"query":"<>&\\\n"}}`)}, {Kind: domain.SessionEventToolResult, CallID: "c", ToolName: "lookup", Payload: body}}
	p, err := canonicaloutput.EncodeMCPOutcomeUnknownFailureV3("run-output", domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 4096}, pair)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release()
	notice, _ := canonicaloutput.EncodeFailureV1("run-output", false, domain.MCPOutcomeUnknownV3)
	defer clear(notice.Payload)
	if len(p.Events()) != 3 || p.Events()[2].Kind != domain.SessionEventSystemNotice || !bytes.Equal(p.Events()[2].Payload, notice.Payload) {
		t.Fatal("UNKNOWN pair did not select exactly one fixed notice")
	}
	n := uint64(len(p.Events()[0].Payload) + len(p.Events()[1].Payload))
	for _, limit := range []uint64{n, n - 1} {
		got, err := canonicaloutput.EncodeMCPOutcomeUnknownFailureV3("run-output", domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: limit}, pair)
		if (err == nil) != (limit == n) {
			t.Fatal("exact UNKNOWN canonical byte bound", limit, n, err)
		}
		if got != nil {
			got.Release()
		}
	}
}
