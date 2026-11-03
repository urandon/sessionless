package compactioncontext_test

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func unresolvedSearchRecordsV4(t *testing.T) []sessioncontext.EventPayload {
	t.Helper()
	records := unresolvedToolRecords(t)
	d := strings.Repeat("a", 64)
	status := canonicaloutput.SearchOperationStatusV4{Schema: "sessionless.search-operation-status.v4", Outcome: domain.AttemptOperationAcceptedOutcomeUnknownV1, Reason: domain.AttemptOperationReasonTransportUnknownV1, Operation: domain.AttemptOperationDigestV1(d), Reservation: domain.AttemptOperationReservationDigestV1(d), Observation: domain.AttemptOperationObservationDigestV1(d)}
	statusBody, _ := json.Marshal(status)
	for _, index := range []int{7, 8} {
		var envelope struct {
			Schema   string          `json:"schema"`
			CallID   string          `json:"call_id"`
			ToolName string          `json:"tool_name"`
			Content  json.RawMessage `json:"content"`
		}
		if err := json.Unmarshal(records[index].Payload, &envelope); err != nil {
			t.Fatal(err)
		}
		envelope.ToolName = "web_search"
		if index == 8 {
			envelope.Content = statusBody
		} else {
			envelope.Content = []byte(`{"call_id":"unknown-call","name":"web_search","arguments_digest":"` + d + `","query_digest":"` + d + `"}`)
		}
		body, _ := json.Marshal(envelope)
		replacePayload(&records[index], string(body))
	}
	notice := domain.SearchFailureNoticeV4{Schema: "sessionless.run-terminal-notice.v4", Code: domain.SearchOutcomeUnknownV4, Execution: "unknown", Kind: domain.AttemptOperationWebSearchV1, Outcome: status.Outcome, Reason: status.Reason, Operation: status.Operation, Reservation: status.Reservation, Observation: status.Observation}
	body, _ := json.Marshal(notice)
	replacePayload(&records[9], string(body))
	return records
}

func TestSearchRejectedV4PairAndRehashedCutStayProtected(t *testing.T) {
	records := unresolvedSearchRecordsV4(t)
	defer clearCopies(records)
	var envelope struct {
		Schema   string          `json:"schema"`
		CallID   string          `json:"call_id"`
		ToolName string          `json:"tool_name"`
		Content  json.RawMessage `json:"content"`
	}
	if json.Unmarshal(records[8].Payload, &envelope) != nil {
		t.Fatal("actual status envelope")
	}
	var status canonicaloutput.SearchOperationStatusV4
	if json.Unmarshal(envelope.Content, &status) != nil {
		t.Fatal("actual status DATA")
	}
	status.Outcome = domain.AttemptOperationRejectedBeforeSendV1
	envelope.Content, _ = json.Marshal(status)
	body, _ := json.Marshal(envelope)
	replacePayload(&records[8], string(body))
	var notice domain.SearchFailureNoticeV4
	if json.Unmarshal(records[9].Payload, &notice) != nil {
		t.Fatal("actual notice DATA")
	}
	notice.Code, notice.Execution, notice.Outcome = domain.SearchRejectedBeforeSendV4, "not_sent", domain.AttemptOperationRejectedBeforeSendV1
	body, _ = json.Marshal(notice)
	replacePayload(&records[9], string(body))
	view := build(t, records)
	protected := false
	for _, unit := range view.Units() {
		if unit.Start == 7 && unit.End == 10 && unit.Protected {
			protected = true
		}
	}
	if !protected {
		t.Error("actual rejected SEARCH pair split from its protected STOP notice")
	}
	c, p := generationFixture(t)
	g, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), records, nil, nil, nil)
	if err != nil {
		t.Fatal("unrelated completed prefix should remain compactable", err)
	}
	v, err := g.BuildRetainedV1(retainedEvidence(t, c, p, g, 1, nil, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"covered", "split"} {
		t.Run(mode, func(t *testing.T) {
			sb, mb := v.SummaryBody(), v.ManifestBody()
			defer clear(sb)
			defer clear(mb)
			var envelope struct {
				Cut json.RawMessage `json:"cut"`
			}
			if json.Unmarshal(mb, &envelope) != nil {
				t.Fatal("cut DATA")
			}
			var cut struct {
				Sources   []json.RawMessage `json:"sources"`
				Cut       uint32            `json:"cut"`
				Protected []uint32          `json:"protected"`
				Covered   []uint32          `json:"covered"`
				Suffix    []uint32          `json:"suffix"`
			}
			if json.Unmarshal(envelope.Cut, &cut) != nil || cut.Cut < 10 {
				t.Fatal("requires failed pair inside the retained prefix")
			}
			filter := func(in []uint32) []uint32 {
				out := []uint32{}
				for _, index := range in {
					if index != 7 && index != 8 {
						out = append(out, index)
					}
				}
				return out
			}
			cut.Protected, cut.Covered = filter(cut.Protected), filter(cut.Covered)
			cut.Covered = append(cut.Covered, 7)
			if mode == "split" {
				cut.Protected = append(cut.Protected, 8)
			} else {
				cut.Covered = append(cut.Covered, 8)
			}
			sort.Slice(cut.Covered, func(i, j int) bool { return cut.Covered[i] < cut.Covered[j] })
			sort.Slice(cut.Protected, func(i, j int) bool { return cut.Protected[i] < cut.Protected[j] })
			changed, _ := json.Marshal(cut)
			mb = bytes.Replace(mb, envelope.Cut, changed, 1)
			sb, mb = replaceRetainedCore(t, sb, mb, "cut_digest", retainedHash(changed))
			plan, sb, mb := retainedMutated(t, v, sb, mb)
			bad, err := compactioncontext.DecodeRetainedV1(c, p, plan, sb, mb, nil)
			if err != nil {
				t.Fatal("all public hashes and old retained structure must pass before payload guard", err)
			}
			if bad.ValidateUnresolvedToolProtection(records) == nil {
				t.Errorf("fully rehashed rejected SEARCH %s pair admitted apart from its STOP", mode)
			}
		})
	}
}

func TestSearchUnresolvedV4OriginalPairRemainsProtected(t *testing.T) {
	records := unresolvedSearchRecordsV4(t)
	defer clearCopies(records)
	view := build(t, records)
	protected := false
	for _, unit := range view.Units() {
		if unit.Start == 7 && unit.End == 10 && unit.Protected {
			protected = true
		}
	}
	if !protected {
		t.Fatal("actual unknown SEARCH pair/notice lost intrinsic STOP protection")
	}
	c, p := generationFixture(t)
	g, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), records, nil, nil, nil)
	if err != nil {
		t.Fatal("unrelated complete prefix remains compactable", err)
	}
	v, err := g.BuildRetainedV1(retainedEvidence(t, c, p, g, 1, nil, nil, nil))
	if err != nil || v.ValidateUnresolvedToolProtection(records) != nil {
		t.Fatal("original payload/protected retained cut", err)
	}
	for _, record := range records[7:10] {
		id, e := compactioncontext.CanonicalSourceKeyV1(record.Event)
		if e != nil {
			t.Fatal(e)
		}
		for _, covered := range g.CoverageIDs() {
			if covered == id {
				t.Fatal("unknown SEARCH became safe summary coverage")
			}
		}
	}
}

func TestSearchUnresolvedV4CrossedNoticeAndMissingPairDenied(t *testing.T) {
	for _, mutation := range []string{"notice_operation", "notice_stop", "missing_call", "foreign_run", "noncontiguous"} {
		t.Run(mutation, func(t *testing.T) {
			records := unresolvedSearchRecordsV4(t)
			defer clearCopies(records)
			switch mutation {
			case "notice_operation":
				replacePayload(&records[9], strings.Replace(string(records[9].Payload), `"operation_digest":"`+strings.Repeat("a", 64)+`"`, `"operation_digest":"`+strings.Repeat("b", 64)+`"`, 1))
			case "notice_stop":
				replacePayload(&records[9], strings.Replace(string(records[9].Payload), domain.SearchOutcomeUnknownV4, domain.SearchConsentStoppedV4, 1))
			case "missing_call":
				records[7].Event.Kind = domain.SessionEventSystemNotice
			case "foreign_run":
				foreign := domain.RunID("foreign-run")
				records[9].Event.RunID = &foreign
			case "noncontiguous":
				records[8].Event.Sequence++
			}
			if _, err := compactioncontext.BuildCanonicalV1(records, compactioncontext.MaxBytesV1, compactioncontext.MaxEventsV1); err == nil {
				t.Fatalf("crossed unknown SEARCH %s accepted", mutation)
			}
		})
	}
}
