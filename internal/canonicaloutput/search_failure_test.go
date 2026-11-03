package canonicaloutput_test

import (
	"encoding/json"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestSearchFailureV4StatusAndNoticeClosedData(t *testing.T) {
	d := strings.Repeat("a", 64)
	status := canonicaloutput.SearchOperationStatusV4{Schema: "sessionless.search-operation-status.v4", Outcome: domain.AttemptOperationAcceptedOutcomeUnknownV1, Reason: domain.AttemptOperationReasonTransportUnknownV1, Operation: domain.AttemptOperationDigestV1(d), Reservation: domain.AttemptOperationReservationDigestV1(d), Observation: domain.AttemptOperationObservationDigestV1(d)}
	body, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := canonicaloutput.DecodeSearchOperationStatusV4(body); err != nil {
		t.Fatal("closed unknown SEARCH status DATA", err)
	}
	notice := domain.SearchFailureNoticeV4{Schema: "sessionless.run-terminal-notice.v4", Code: domain.SearchOutcomeUnknownV4, Execution: "unknown", Kind: domain.AttemptOperationWebSearchV1, Outcome: status.Outcome, Reason: status.Reason, Operation: status.Operation, Reservation: status.Reservation, Observation: status.Observation}
	noticeBody, _ := json.Marshal(notice)
	if _, err := canonicaloutput.DecodeSearchFailureNoticeV4(noticeBody); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		strings.Replace(string(body), `"schema":`, `"schema":null,"schema":`, 1),
		strings.Replace(string(body), `"operation_digest":`, `"Operation_digest":`, 1),
		strings.Replace(string(body), `"outcome":"accepted_outcome_unknown"`, `"outcome":"completed"`, 1),
		strings.Replace(string(body), `"schema":`, `"response":null,"schema":`, 1),
		strings.Replace(string(body), `"reason":"transport_unknown"`, `"reason":"service_error"`, 1),
		string(body) + " ",
	} {
		if _, err := canonicaloutput.DecodeSearchOperationStatusV4([]byte(raw)); err == nil {
			t.Fatalf("nonclosed SEARCH status admitted: %s", raw)
		}
	}
	for _, raw := range []string{
		strings.Replace(string(noticeBody), `"unknown"`, `"known_empty"`, 1),
		strings.Replace(string(noticeBody), `"search_outcome_unknown"`, `"search_consent_stopped"`, 1),
		strings.Replace(string(noticeBody), `"tail_kind":"web_search"`, `"tail_kind":"model"`, 1),
		strings.Replace(string(noticeBody), `"code":`, `"code":null,"code":`, 1),
		string(noticeBody) + " ",
	} {
		if raw == string(noticeBody) {
			t.Fatal("mutation did not change notice")
		}
		if _, err := canonicaloutput.DecodeSearchFailureNoticeV4([]byte(raw)); err == nil {
			t.Fatalf("crossed SEARCH notice admitted: %s", raw)
		}
	}
	if _, err := canonicaloutput.DecodeMCPUnknownToolOutcomeV1(body); err == nil {
		t.Fatal("SEARCH status widened MCP language")
	}
}
