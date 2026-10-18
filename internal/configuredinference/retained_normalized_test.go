package configuredinference

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Synthetic retained DATA from the real codec, not a signed observation or
// capsule. No request is passed to the retained decoder. Outer seal validation
// and actual erasure are deliberately not claimed by these tests.
func retainedNormalizedFixture(t testing.TB, kind domain.AttemptOperationKindV1, wire []byte) (domain.ConfiguredInferenceProfileV1, domain.AttemptOperationReservationV1, domain.AttemptOperationObservationV1, []byte) {
	t.Helper()
	p := testProfile(t)
	f := normalizedFreeze(t, p, p.Limits, testRequest())
	body := normalizedRecord(t, f, wire, kind)
	response, err := f.DecodeResponse(wire)
	if err != nil {
		t.Fatal("original decode", err)
	}
	usage, err := response.Usage.OperationUsage()
	if err != nil {
		t.Fatal("original usage", err)
	}
	h := strings.Repeat("1", 64)
	at := time.Unix(101, 0).UTC()
	op := domain.AttemptOperationV1{Version: 1, Scope: domain.AttemptOperationScopeV1{
		TenantID: p.TenantID, OwnerUserID: p.OwnerUserID, SessionID: "retained-session", RunID: "retained-run", AttemptID: "retained-attempt", LeaseID: "retained-lease", FenceToken: 1,
		PhysicalInvocationClaimID: "retained-claim", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(h), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(h),
		HarnessBindingDigest: domain.HarnessBindingDigestV2(h), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(h)},
		Sequence: 1, Kind: kind, CallID: "retained-operation", RequestRef: "original-erased-request", RequestDigest: normalizedSHA(f.Body()),
		Deadline: at.Add(2 * time.Second), CleanupDuration: time.Second, RequestBytes: uint64(len(f.Body())), ResultBytes: 64 << 10, ContextBytes: uint64(len(f.Body())),
		Currency: "RUB", PriceRevision: "retained-price", AdmissionCostCeilingDigest: domain.AdmissionCostCeilingDigestV2(h)}
	op.Debit = domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 128, OutputTokens: 32, BodyBytes: op.RequestBytes + op.ResultBytes, ResponseBytes: p.Limits.ResponseBytes, ActiveDuration: 2 * time.Second}
	if kind == domain.AttemptOperationCompactionV1 {
		op.SummaryBytes, op.Debit.CompactionCalls = 16<<10, 1
	}
	r := domain.AttemptOperationReservationV1{Version: 1, Operation: op, ReservedAt: at, AggregateDebit: op.Debit,
		RegisteredAllocationDigest: domain.RegisteredAllocationDigestV2(h), RegisteredProxyDigest: domain.RegisteredProxyAttestationDigestV2(h)}
	o := domain.AttemptOperationObservationV1{Version: 1, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1,
		ResponseRef: "original-erased-response", ObservedAt: at.Add(time.Second), Usage: usage}
	rebindRetainedData(t, &r, &o, body)
	if response.Call != nil {
		clear(response.Call.Arguments)
	}
	return p, r, o, body
}

func rebindRetainedData(t testing.TB, r *domain.AttemptOperationReservationV1, o *domain.AttemptOperationObservationV1, body []byte) {
	t.Helper()
	var err error
	r.OperationDigest, err = r.Operation.Digest()
	if err != nil {
		t.Fatal("operation DATA", err)
	}
	o.OperationDigest = r.OperationDigest
	o.ReservationDigest, err = r.Digest()
	if err != nil {
		t.Fatal("reservation DATA", err)
	}
	o.ResponseBytes, o.ResponseDigest = uint64(len(body)), normalizedSHA(body)
	if err := o.ValidateForReservation(*r); err != nil {
		t.Fatal("observation DATA", err)
	}
}

func retainedNormalizedDenied(t testing.TB, p domain.ConfiguredInferenceProfileV1, r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, body []byte) {
	t.Helper()
	out, err := DecodeRetainedNormalizedDataV1(p, r, o, body)
	if err == nil || !reflect.DeepEqual(out, ResponseV1{}) {
		t.Fatalf("invalid retained material returned usable DATA: err=%v stop=%s bytes=%d call=%t", err, out.Stop, len(out.Text), out.Call != nil)
	}
}

func TestRetainedNormalizedActualCodecWithoutRequest(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind domain.AttemptOperationKindV1
		wire []byte
		stop StopV1
	}{
		{name: "final", kind: domain.AttemptOperationModelV1, wire: responseWire("alias-local", "stop", "retained answer", nil), stop: StopFinalV1},
		{name: "tool", kind: domain.AttemptOperationModelV1, wire: toolWire("next-call", "lookup", ` { "query" : "docs" } `), stop: StopToolV1},
		{name: "compaction", kind: domain.AttemptOperationCompactionV1, wire: responseWire("model-local", "stop", "summary", nil), stop: StopFinalV1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, r, o, body := retainedNormalizedFixture(t, tc.kind, tc.wire)
			out, err := DecodeRetainedNormalizedDataV1(p, r, o, body)
			if err != nil || out.Stop != tc.stop || out.ConfiguredModel != p.ModelID || out.ModelProvenance != ModelReportedV1 || out.Usage.Provenance != UsageProviderReportedV1 || *out.Usage.InputTokens != 9 {
				t.Fatalf("retained genuine codec DATA: err=%v stop=%s provenance=%s", err, out.Stop, out.Usage.Provenance)
			}
			if out.Call != nil && (out.Call.ID != "next-call" || out.Call.Name != "lookup" || string(out.Call.Arguments) != `{"query":"docs"}`) {
				t.Fatal("retained call identity or arguments changed")
			}
			if strings.Contains(fmt.Sprintf("%+v %#v", out, out), "retained answer") {
				t.Fatal("retained response formatted content")
			}
			clear(body)
			if out.Stop == StopToolV1 && string(out.Call.Arguments) != `{"query":"docs"}` {
				t.Fatal("retained arguments alias caller bytes")
			}
		})
	}
}

func TestRetainedNormalizedExactIdentityJoins(t *testing.T) {
	p, r, o, body := retainedNormalizedFixture(t, domain.AttemptOperationModelV1, responseWire("model-local", "stop", "complete", nil))
	for _, tc := range []struct {
		name   string
		change func(*domain.ConfiguredInferenceProfileV1, *domain.AttemptOperationReservationV1, *domain.AttemptOperationObservationV1)
	}{
		{name: "profile", change: func(p *domain.ConfiguredInferenceProfileV1, _ *domain.AttemptOperationReservationV1, _ *domain.AttemptOperationObservationV1) {
			p.ProfileRevision++
		}},
		{name: "tenant", change: func(_ *domain.ConfiguredInferenceProfileV1, r *domain.AttemptOperationReservationV1, o *domain.AttemptOperationObservationV1) {
			r.Operation.Scope.TenantID = "other-tenant"
			rebindRetainedData(t, r, o, body)
		}},
		{name: "owner", change: func(_ *domain.ConfiguredInferenceProfileV1, r *domain.AttemptOperationReservationV1, o *domain.AttemptOperationObservationV1) {
			r.Operation.Scope.OwnerUserID = "other-owner"
			rebindRetainedData(t, r, o, body)
		}},
		{name: "raw-request-sha", change: func(_ *domain.ConfiguredInferenceProfileV1, r *domain.AttemptOperationReservationV1, o *domain.AttemptOperationObservationV1) {
			r.Operation.RequestDigest = strings.Repeat("7", 64)
			rebindRetainedData(t, r, o, body)
		}},
		{name: "observation-reservation", change: func(_ *domain.ConfiguredInferenceProfileV1, _ *domain.AttemptOperationReservationV1, o *domain.AttemptOperationObservationV1) {
			o.ReservationDigest = domain.AttemptOperationReservationDigestV1(strings.Repeat("7", 64))
		}},
		{name: "body-sha", change: func(_ *domain.ConfiguredInferenceProfileV1, _ *domain.AttemptOperationReservationV1, o *domain.AttemptOperationObservationV1) {
			o.ResponseDigest = strings.Repeat("7", 64)
		}},
		{name: "body-length", change: func(_ *domain.ConfiguredInferenceProfileV1, _ *domain.AttemptOperationReservationV1, o *domain.AttemptOperationObservationV1) {
			o.ResponseBytes--
		}},
		{name: "service-error", change: func(_ *domain.ConfiguredInferenceProfileV1, _ *domain.AttemptOperationReservationV1, o *domain.AttemptOperationObservationV1) {
			o.Reason = domain.AttemptOperationReasonServiceErrorV1
		}},
		{name: "usage", change: func(_ *domain.ConfiguredInferenceProfileV1, _ *domain.AttemptOperationReservationV1, o *domain.AttemptOperationObservationV1) {
			n := uint64(8)
			o.Usage.InputTokens = &n
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cp, cr, co := p.Clone(), r.Clone(), o.Clone()
			tc.change(&cp, &cr, &co)
			retainedNormalizedDenied(t, cp, cr, co, body)
		})
	}
	for _, state := range []domain.AttemptOperationOutcomeV1{domain.AttemptOperationRejectedBeforeSendV1, domain.AttemptOperationAcceptedOutcomeUnknownV1} {
		co := o.Clone()
		co.Outcome, co.Reason, co.ResponseRef, co.ResponseDigest, co.ResponseBytes = state, domain.AttemptOperationReasonTransportUnknownV1, "", "", 0
		if co.ValidateForReservation(r) != nil {
			t.Fatal("valid non-success fixture required")
		}
		retainedNormalizedDenied(t, p, r, co, body)
	}
}

func TestRetainedNormalizedClosedCanonicalAndSemanticDenials(t *testing.T) {
	p, r, o, body := retainedNormalizedFixture(t, domain.AttemptOperationModelV1, responseWire("model-local", "stop", "complete", nil))
	for _, tc := range []struct{ name, old, replacement string }{
		{"version", `"version":1`, `"version":2`}, {"kind", `"operation_kind":"model"`, `"operation_kind":"compaction"`},
		{"reported-model", `"reported_model":"model-local"`, `"reported_model":"foreign"`}, {"stop", `"stop":"final"`, `"stop":"length"`},
		{"empty-answer", `"text":"complete"`, `"text":""`}, {"zero-limit", `"tool_arguments_bytes":4096`, `"tool_arguments_bytes":0`},
		{"wide-limit", `"max_output_tokens":1024`, `"max_output_tokens":1025`}, {"case-alias", `"version":1`, `"Version":1`},
		{"duplicate", `"version":1`, `"version":1,"\u0076ersion":1`}, {"unknown", `"version":1`, `"version":1,"unknown":true`},
		{"surrogate", `"text":"complete"`, `"text":"\ud800"`}, {"invalid-UTF8", `complete`, string([]byte{0xff})},
		{"counter-overflow", `"input_tokens":9`, `"input_tokens":18446744073709551616`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := normalizedReplace(t, body, tc.old, tc.replacement)
			co := o.Clone()
			co.ResponseBytes, co.ResponseDigest = uint64(len(bad)), normalizedSHA(bad)
			retainedNormalizedDenied(t, p, r, co, bad)
		})
	}
	for _, bad := range [][]byte{nil, body[:len(body)-1], append(bytes.Clone(body), '\n'), append(bytes.Clone(body), []byte(`{}`)...), bytes.Repeat([]byte(" "), (64<<10)+1)} {
		co := o.Clone()
		co.ResponseBytes, co.ResponseDigest = uint64(len(bad)), normalizedSHA(bad)
		retainedNormalizedDenied(t, p, r, co, bad)
	}
}

func TestRetainedNormalizedUsagePreservesUnknownPartialAndOverrun(t *testing.T) {
	for _, usage := range []domain.AttemptOperationUsageV1{
		{State: domain.AttemptOperationUsageUnknownV1},
		{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: new(uint64(0))},
		{State: domain.AttemptOperationUsageProviderReportedV1, OutputTokens: new(uint64(10000))},
	} {
		p, r, o, body := retainedNormalizedFixture(t, domain.AttemptOperationModelV1, responseWire("model-local", "stop", "complete", nil))
		var record normalizedBodyV1
		if json.Unmarshal(body, &record) != nil {
			t.Fatal("decode fixture")
		}
		record.Usage = usage.Clone()
		body, _ = encodeNormalizedBodyV1(record, normalizedBodyLimitV1)
		o.Usage = usage.Clone()
		rebindRetainedData(t, &r, &o, body)
		out, err := DecodeRetainedNormalizedDataV1(p, r, o, body)
		got, ue := out.Usage.OperationUsage()
		if err != nil || ue != nil || !reflect.DeepEqual(got, usage) {
			t.Fatalf("usage provenance/optional quantity lost: decode=%v usage=%v", err, ue)
		}
		if usage.ExceedsReservation(r.Operation.Debit) != got.ExceedsReservation(r.Operation.Debit) {
			t.Fatal("overrun erased")
		}
	}
}

func TestRetainedNormalizedToolShapeWithoutInventedSchemaAuthority(t *testing.T) {
	p, r, o, body := retainedNormalizedFixture(t, domain.AttemptOperationModelV1, toolWire("next-call", "lookup", `{"query":"docs"}`))
	for _, tc := range []struct{ name, old, replacement string }{
		{"text-and-call", `"text":""`, `"text":"answer"`}, {"empty-id", `"id":"next-call"`, `"id":""`},
		{"arguments-array", `"arguments":{"query":"docs"}`, `"arguments":[]`},
		{"arguments-noncanonical", `"arguments":{"query":"docs"}`, `"arguments":{ "query":"docs"}`},
		{"tool-final", `"stop":"tool_call"`, `"stop":"final"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := normalizedReplace(t, body, tc.old, tc.replacement)
			co := o.Clone()
			co.ResponseBytes, co.ResponseDigest = uint64(len(bad)), normalizedSHA(bad)
			retainedNormalizedDenied(t, p, r, co, bad)
		})
	}
	// Without the originating request this structurally valid DATA cannot prove
	// catalog/schema/history membership. The old decoder still rejects it.
	bad := normalizedReplace(t, body, `"name":"lookup"`, `"name":"foreign"`)
	o.ResponseBytes, o.ResponseDigest = uint64(len(bad)), normalizedSHA(bad)
	out, err := DecodeRetainedNormalizedDataV1(p, r, o, bad)
	if err != nil || out.Call == nil || out.Call.Name != "foreign" {
		t.Fatal("structural retained DATA incorrectly claims request catalog authority", err)
	}
	f := normalizedFreeze(t, p, p.Limits, testRequest())
	normalizedDenied(t, p, f.Body(), bad, r.Operation.Kind)
}

func TestRetainedNormalizedExactCapacityAndCompactionSummary(t *testing.T) {
	p, r, o, base := retainedNormalizedFixture(t, domain.AttemptOperationModelV1, responseWire("model-local", "stop", "x", nil))
	f := normalizedFreeze(t, p, p.Limits, testRequest())
	// ASCII bytes have no escaping expansion. Compute the exact envelope cost
	// independently of the bounded encoder's capacity decision.
	n := int(normalizedBodyLimitV1) - len(base) + 1
	body := normalizedRecord(t, f, responseWire("model-local", "stop", strings.Repeat("x", n), nil), domain.AttemptOperationModelV1)
	if len(body) != int(normalizedBodyLimitV1) {
		t.Fatalf("exact normalized envelope bytes=%d want=%d", len(body), normalizedBodyLimitV1)
	}
	rebindRetainedData(t, &r, &o, body)
	if _, err := DecodeRetainedNormalizedDataV1(p, r, o, body); err != nil {
		t.Fatal("exact normalized envelope capacity rejected", err)
	}
	over := normalizedReplace(t, body, `"text":"x`, `"text":"xx`)
	co := o.Clone()
	co.ResponseBytes, co.ResponseDigest = uint64(len(over)), normalizedSHA(over)
	retainedNormalizedDenied(t, p, r, co, over)

	cp, cr, co, summary := retainedNormalizedFixture(t, domain.AttemptOperationCompactionV1, responseWire("model-local", "stop", strings.Repeat("s", 16<<10), nil))
	if _, err := DecodeRetainedNormalizedDataV1(cp, cr, co, summary); err != nil {
		t.Fatal("exact summary boundary rejected", err)
	}
	bad := normalizedReplace(t, summary, `"text":"s`, `"text":"ss`)
	co.ResponseBytes, co.ResponseDigest = uint64(len(bad)), normalizedSHA(bad)
	retainedNormalizedDenied(t, cp, cr, co, bad)
	cr.Operation.SummaryBytes--
	rebindRetainedData(t, &cr, &co, summary)
	retainedNormalizedDenied(t, cp, cr, co, summary)
}

func FuzzRetainedNormalizedDataV1(f *testing.F) {
	p, r, o, body := retainedNormalizedFixture(f, domain.AttemptOperationModelV1, responseWire("model-local", "stop", "seed", nil))
	f.Add(body)
	f.Add([]byte(`{"version":1}`))
	f.Add([]byte(`{"text":"\ud800"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		co := o.Clone()
		co.ResponseBytes, co.ResponseDigest = uint64(len(data)), normalizedSHA(data)
		out, err := DecodeRetainedNormalizedDataV1(p, r, co, data)
		if err != nil && !reflect.DeepEqual(out, ResponseV1{}) {
			t.Fatal("error retained usable material")
		}
	})
}
