package mcpadmission

import (
	"bytes"
	"encoding/json"
	"gitcode.com/urandon/sessionless/internal/domain"
	"strings"
	"testing"
	"time"
)

// Synthetic truthful usage DATA is not a provider receipt, price qualification
// or permission to dispatch. Unknown money must remain unknown after encoding.
func normalizedToolFixture(t *testing.T) (CallV1, domain.AttemptOperationV1, []byte) {
	t.Helper()
	g, _, _ := grantFixture(t)
	h := strings.Repeat("a", 64)
	c := CallV1{grant: g, id: "lookup-1", name: "search_openai_docs", args: []byte(`{"query":"docs"}`), model: h}
	req, e := c.Request()
	if e != nil {
		t.Fatal(e)
	}
	op := domain.AttemptOperationV1{Version: 1, Scope: domain.AttemptOperationScopeV1{TenantID: g.TenantID, OwnerUserID: g.OwnerUserID, SessionID: "session", RunID: "run", AttemptID: "attempt", LeaseID: "lease", FenceToken: 1, PhysicalInvocationClaimID: "claim", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(h), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(h), HarnessBindingDigest: domain.HarnessBindingDigestV2(h), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(h)}, Sequence: 2, Kind: domain.AttemptOperationMCPToolV1, CallID: c.ID(), RequestRef: "request", RequestDigest: normalizedDigest(req.Body()), PredecessorObservationDigest: domain.AttemptOperationObservationDigestV1(h), ModelResponseDigest: c.ModelResponseDigest(), ArgumentsDigest: c.ArgumentsDigest(), ToolGrantDigest: c.GrantDigest(), ExternalChargeEvidenceDigest: h, Deadline: g.IssuedAt.Add(time.Second), CleanupDuration: time.Second, RequestBytes: uint64(len(req.Body())), ResultBytes: 65536, Currency: "RUB", PriceRevision: "synthetic", AdmissionCostCeilingDigest: domain.AdmissionCostCeilingDigestV2(h), Debit: domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, BodyBytes: 70000, ResponseBytes: 65536, ActiveDuration: time.Second, MCPOutboundMessages: 4, MCPCatalogPages: 1, MCPInboundFrames: 3, MCPInboundBytes: 65536}}
	if e = op.Validate(); e != nil {
		t.Fatal(e)
	}
	return c, op, req.Body()
}
func TestMCPNormalizedKnownResultAndUnknownMoneyPreserved(t *testing.T) {
	c, op, request := normalizedToolFixture(t)
	r, _ := c.Request()
	tool, e := r.DecodeToolResultV1([]byte(`{"jsonrpc":"2.0","id":"lookup-1","result":{"content":[{"type":"text","text":"answer"}],"isError":false}}`))
	if e != nil {
		t.Fatal(e)
	}
	usage := domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
	body, e := EncodeToolResultV1(c, op, tool, usage)
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := DecodeToolResultV1(c.grant, op, request, body)
	if e != nil || decoded.CallID() != c.ID() || decoded.Content()[0] != "answer" || decoded.Usage().AmountMicrounits != nil {
		t.Fatal("roundtrip or invented money", e)
	}
	content := decoded.Content()
	content[0] = "changed"
	if decoded.Content()[0] != "answer" {
		t.Fatal("result aliases caller")
	}
	for _, field := range []string{"call_id", "request_digest", "model_response_digest", "operation_digest", "schema"} {
		var v map[string]json.RawMessage
		if json.Unmarshal(body, &v) != nil {
			t.Fatal("fixture")
		}
		old := append([]byte(`"`+field+`":`), v[field]...)
		if bytes.Count(body, old) != 1 {
			t.Fatal("noncausal mutation")
		}
		b := bytes.Replace(body, old, []byte(`"`+field+`":"altered"`), 1)
		if _, e = DecodeToolResultV1(c.grant, op, request, b); e == nil {
			t.Fatal("drift accepted", field)
		}
	}
	changed := op.Clone()
	changed.ToolGrantDigest = strings.Repeat("b", 64)
	if _, e = DecodeToolResultV1(c.grant, changed, request, body); e == nil {
		t.Fatal("cross grant accepted")
	}
	if _, e = DecodeToolResultV1(c.grant, op, append(bytes.Clone(request), ' '), body); e == nil {
		t.Fatal("request not exact")
	}
	if _, e = BindConfiguredCallV1(domain.ConfiguredInferenceProfileV1{}, c.grant, domain.AttemptOperationReservationV1{}, domain.AttemptOperationObservationV1{}, nil, nil); e == nil {
		t.Fatal("unsealed call admitted")
	}
}

func TestMCPNormalizedSealedSuccessAndDomainErrorExactUsage(t *testing.T) {
	for _, isError := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "domain_error"}[isError], func(t *testing.T) {
			c, op, request := normalizedToolFixture(t)
			req, _ := c.Request()
			wire, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": c.ID(), "result": map[string]any{"content": []any{map[string]any{"type": "text", "text": "known public result"}}, "isError": isError}})
			if e != nil {
				t.Fatal(e)
			}
			result, e := req.DecodeToolResultV1(wire)
			if e != nil {
				t.Fatal(e)
			}
			zero := uint64(0)
			usage := domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: &zero}
			body, e := EncodeToolResultV1(c, op, result, usage)
			if e != nil {
				t.Fatal(e)
			}
			od, _ := op.Digest()
			aggregate := op.Debit
			aggregate.Operations = 2
			aggregate.ModelCalls = 1
			r := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: od, ReservedAt: c.grant.IssuedAt, AggregateDebit: aggregate, RegisteredAllocationDigest: domain.RegisteredAllocationDigestV2(strings.Repeat("a", 64)), RegisteredProxyDigest: domain.RegisteredProxyAttestationDigestV2(strings.Repeat("b", 64))}
			rd, e := r.Digest()
			if e != nil {
				t.Fatal(e)
			}
			o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: od, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: "response", ResponseDigest: normalizedDigest(body), ResponseBytes: uint64(len(body)), ObservedAt: r.ReservedAt, Usage: usage}
			if isError {
				o.Reason = domain.AttemptOperationReasonServiceErrorV1
			}
			v, e := DecodeSealedToolResultDataV1(r, o, body)
			if e != nil || v.IsError() != isError || v.Usage().AmountMicrounits == nil {
				t.Fatal("known result not correlated", e)
			}
			if _, e = DecodeToolResultV1(c.grant, op, request, body); e != nil {
				t.Fatal(e)
			}
			wrong := o.Clone()
			wrong.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
			if _, e = DecodeSealedToolResultDataV1(r, wrong, body); e == nil {
				t.Fatal("usage mismatch")
			}
			wrong = o.Clone()
			if isError {
				wrong.Reason = domain.AttemptOperationReasonNoneV1
			} else {
				wrong.Reason = domain.AttemptOperationReasonServiceErrorV1
			}
			if _, e = DecodeSealedToolResultDataV1(r, wrong, body); e == nil {
				t.Fatal("domain error reason mismatch")
			}
		})
	}
}
