package sessionmaterial_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

func searchNoticeEvent(body []byte) domain.SessionEvent {
	d := sha256.Sum256(body)
	run := domain.RunID("notice-run")
	return domain.SessionEvent{ID: "notice-event", TenantID: "notice-tenant", SessionID: "notice-session", Kind: domain.SessionEventSystemNotice, RunID: &run, IdempotencyKey: "notice-key", Payload: domain.BlobRef{TenantID: "notice-tenant", Key: domain.SessionEventObjectPrefix("notice-tenant", "notice-session", "notice-event") + "payload.json", Size: int64(len(body)), SHA256: hex.EncodeToString(d[:])}, Sequence: 1, CreatedAt: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
}

func TestSearchFailureV4ActualPreparedFinalMeasureChain(t *testing.T) {
	d := strings.Repeat("a", 64)
	for _, mode := range []string{"consent", "rejected", "service_error", "unknown", "model_rejected", "model_unknown"} {
		t.Run(mode, func(t *testing.T) {
			v := domain.SearchFailureNoticeV4{Schema: "sessionless.run-terminal-notice.v4", Code: domain.SearchOutcomeUnknownV4, Execution: "unknown", Kind: domain.AttemptOperationWebSearchV1, Outcome: domain.AttemptOperationAcceptedOutcomeUnknownV1, Reason: domain.AttemptOperationReasonTransportUnknownV1, Operation: domain.AttemptOperationDigestV1(d), Reservation: domain.AttemptOperationReservationDigestV1(d), Observation: domain.AttemptOperationObservationDigestV1(d)}
			switch mode {
			case "consent":
				v.Code, v.Execution, v.Kind, v.Outcome, v.Reason = domain.SearchConsentStoppedV4, "not_executed", domain.AttemptOperationModelV1, domain.AttemptOperationCompletedV1, domain.AttemptOperationReasonNoneV1
			case "rejected":
				v.Code, v.Execution, v.Outcome = domain.SearchRejectedBeforeSendV4, "not_sent", domain.AttemptOperationRejectedBeforeSendV1
			case "service_error":
				v.Code, v.Execution, v.Outcome, v.Reason = domain.SearchServiceErrorV4, "known_service_error", domain.AttemptOperationCompletedV1, domain.AttemptOperationReasonServiceErrorV1
			case "model_rejected":
				v.Code, v.Execution, v.Kind, v.Outcome = domain.ModelAfterSearchFailureV4, "not_sent", domain.AttemptOperationModelV1, domain.AttemptOperationRejectedBeforeSendV1
			case "model_unknown":
				v.Code, v.Kind = domain.ModelAfterSearchFailureV4, domain.AttemptOperationModelV1
			}
			body, e := json.Marshal(v)
			if e != nil || v.Validate() != nil {
				t.Fatal("valid exact V4 tail DATA", e)
			}
			event := searchNoticeEvent(body)
			prepared, e := sessionmaterial.PrepareV1(event, body, nil)
			if e != nil {
				t.Fatal("mounted V4 preparation", e)
			}
			defer prepared.Release()
			measure, e := sessioncontext.MeasurePreparedRecordV1(event, prepared)
			if e != nil || !measure.Matches(event) {
				t.Fatal("actual final material measure/record codec", e)
			}
			m, ok := measure.SourceMeasures()
			if !ok || m.Kind != domain.SessionEventSystemNotice || m.PayloadBytes != uint64(len(body)) {
				t.Fatal("V4 exact source measures", m, ok)
			}
		})
	}
}

func TestSearchFailureV4MaterialDispatchRejectsAmbiguityWithoutOldFallback(t *testing.T) {
	d := strings.Repeat("a", 64)
	v := domain.SearchFailureNoticeV4{Schema: "sessionless.run-terminal-notice.v4", Code: domain.SearchOutcomeUnknownV4, Execution: "unknown", Kind: domain.AttemptOperationWebSearchV1, Outcome: domain.AttemptOperationAcceptedOutcomeUnknownV1, Reason: domain.AttemptOperationReasonTransportUnknownV1, Operation: domain.AttemptOperationDigestV1(d), Reservation: domain.AttemptOperationReservationDigestV1(d), Observation: domain.AttemptOperationObservationDigestV1(d)}
	body, _ := json.Marshal(v)
	for _, bad := range []string{strings.Replace(string(body), `"schema":`, `"Schema":`, 1), strings.Replace(string(body), `"schema":`, `"schema":null,"schema":`, 1), strings.Replace(string(body), `"code":`, `"code":null,"code":`, 1), strings.Replace(string(body), `"execution":"unknown"`, `"execution":"not_sent"`, 1), strings.Replace(string(body), `"operation_digest":"`+d+`"`, `"operation_digest":null`, 1), strings.Replace(string(body), `"tail_kind":"web_search"`, `"tail_kind":"model"`, 1), strings.Replace(string(body), `"schema":`, `"response":{},"schema":`, 1), strings.Replace(string(body), `sessionless.run-terminal-notice.v4`, `sessionless.run-terminal-notice.v1`, 1), string(body) + " ", string(body) + `{}`} {
		raw := []byte(bad)
		p, e := sessionmaterial.PrepareV1(searchNoticeEvent(raw), raw, nil)
		if p != nil {
			p.Release()
		}
		if e == nil {
			t.Fatal("crossed/ambiguous V4 material admitted", bad)
		}
	}
	// Existing v1's documented encoding/json behavior is intentionally unchanged;
	// strict V4 handling never becomes a silent global normalization rule.
	for _, old := range []string{`{"schema":"sessionless.run-terminal-notice.v1","Code":"old","cancelled":false}`, `{"schema":"sessionless.run-terminal-notice.v1","code":"first","code":"old","cancelled":false}`} {
		raw := []byte(old)
		p, e := sessionmaterial.PrepareV1(searchNoticeEvent(raw), raw, nil)
		if e != nil {
			t.Fatal("old v1 material semantics changed", e)
		}
		p.Release()
	}
}
