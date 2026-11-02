package ydbstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestLocalFailureCapsuleV3StrictUnknownMCPAbsence(t *testing.T) {
	// Pure codec DATA: no valid parent, issuer, original owner or authority.
	c := localFailureCapsule{Schema: "sessionless.terminal-failure-custody.v3", Version: 3, Stop: domain.MCPOutcomeUnknownV3, Cleanup: "joined_retained", Accounting: ports.OwnedOperationFailureAccountingV1{Version: 3}, Operations: make([]localFailureOperationV1, 2)}
	c.Operations[0].Reservation.Operation.Kind = domain.AttemptOperationModelV1
	c.Operations[0].Observation.Outcome, c.Operations[0].Observation.Reason = domain.AttemptOperationCompletedV1, domain.AttemptOperationReasonNoneV1
	c.Operations[0].Normalized = json.RawMessage(`{}`)
	c.Operations[1].Reservation.Operation.Kind = domain.AttemptOperationMCPToolV1
	c.Operations[1].Observation.Outcome, c.Operations[1].Observation.Reason = domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonTransportUnknownV1
	c.Operations[1].Observation.Usage.State = domain.AttemptOperationUsageUnknownV1
	c.Operations[1].Normalized = json.RawMessage(`null`)
	body, err := encodeLocalFailureCapsule(c, domain.MaxTerminalCapsuleBytesV1)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(body)
	decoded, err := decodeLocalFailureCapsule(body)
	clearLocalFailureNormalized(decoded)
	if err != nil {
		t.Fatal("strict F3 wire DATA", err)
	}
	if _, err := localFailurePlan(domain.Run{}, domain.ProductLimits{}, c); err == nil {
		t.Fatal("codec DATA became original producer")
	}
	for _, raw := range []string{
		strings.Replace(string(body), `,"normalized":null`, ``, 1),
		strings.Replace(string(body), `"normalized":null`, `"normalized":{}`, 1),
		strings.Replace(string(body), `"normalized":null`, `"normalized":"null"`, 1),
		strings.Replace(string(body), `"normalized":{}`, `"normalized":null`, 1),
		strings.Replace(string(body), `"normalized":null`, `"normalized":null,"extra":0`, 1),
		strings.Replace(string(body), `"schema":"sessionless.terminal-failure-custody.v3"`, `"schema":"sessionless.terminal-failure-custody.v2"`, 1),
		strings.Replace(string(body), `"stop":"mcp_outcome_unknown"`, `"stop":"model_outcome_unknown"`, 1),
		strings.Replace(string(body), `"version":3`, `"version":2`, 1),
		strings.ReplaceAll(string(body), `"response_ref":""`, `"response_ref":"invented"`),
	} {
		if bytes.Equal(body, []byte(raw)) {
			t.Fatal("mutation failed to change DATA")
		}
		out, err := decodeLocalFailureCapsule([]byte(raw))
		clearLocalFailureNormalized(out)
		if err == nil {
			t.Fatal("ambiguous F3 response/version accepted")
		}
	}
	n := uint64(len(body))
	if got, err := localFailureProspectiveCapsuleBytes(c, 4, n); err != nil || got != n {
		t.Fatal("F3 exact absent-body serializer fit", got, n, err)
	}
	if _, err := localFailureProspectiveCapsuleBytes(c, 4, n-1); err == nil {
		t.Fatal("F3 capsule one byte over admitted")
	}
}

func TestLocalFailureV3ProspectiveFullCanonicalAndKeyInventory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Actual original preparation supplies the complete escaped MODEL
		// request/normalized call. Future reservation/maxima below are LENGTH
		// DATA only: never validated, authenticated, persisted or sent.
		f, _ := localPreparedModelFixture(t, 0)
		model, err := f.entry.PrepareModel(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		recordPreparedLocalToolCall(t, f, model.operation, model.frozen, [2]string{"escaped-call", "<>&\"\\\n\u2028кириллица"})
		m, err := f.entry.PrepareMCP(t.Context())
		if err != nil || m == nil {
			t.Fatal("actual escaped original request/call preparation", err)
		}
		ops := m.source.state.operations
		pair, err := localTerminalCanonicalPairsUpperV2(f.actual.config, ops, m.operation)
		if err != nil || len(pair) != 2 {
			t.Fatal("actual canonical call size", err)
		}
		defer func() {
			for _, event := range pair {
				clear(event.Payload)
			}
		}()
		d := strings.Repeat("0", 64)
		clear(pair[1].Payload)
		pair[1].Payload, err = canonicaloutput.EncodeMCPUnknownToolOutcomeV1(domain.AttemptOperationDigestV1(d), domain.AttemptOperationReservationDigestV1(d), domain.AttemptOperationObservationDigestV1(d))
		if err != nil {
			t.Fatal(err)
		}
		run := m.source.state.snapshot.Job.Run
		plan, err := canonicaloutput.EncodeMCPOutcomeUnknownFailureV3(run.ID, m.source.state.snapshot.Job.Job.Limits, pair)
		if err != nil {
			t.Fatal(err)
		}
		defer plan.Release()
		future := localTerminalFutureMaterial(m.operation, 4)
		future.observation.Outcome, future.observation.Reason = domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonTransportUnknownV1
		future.observation.ResponseRef, future.observation.ResponseDigest, future.observation.ResponseBytes = "", "", 0
		future.observation.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
		pd, err := f.actual.config.Digest()
		if err != nil {
			t.Fatal(err)
		}
		prefixRow, err := ports.BuildOwnedOperationFailureRowV1(ops[0].Reservation, ops[0].Observation, f.entry.parent, f.entry.source)
		if err != nil {
			t.Fatal("complete known prefix row", err)
		}
		spending, _ := f.entry.source.Spending[2].Digest()
		futureRow := ports.OwnedOperationFailureRowV1{Sequence: m.operation.Sequence, Kind: domain.AttemptOperationMCPToolV1, OperationDigest: domain.AttemptOperationDigestV1(d), ReservationDigest: domain.AttemptOperationReservationDigestV1(d), ObservationDigest: domain.AttemptOperationObservationDigestV1(d), Outcome: future.observation.Outcome, Reason: future.observation.Reason, Reserved: future.reservation.Operation.Debit, Usage: future.observation.Usage, MCPSpendingDigest: spending}
		c := localFailureCapsule{Schema: "sessionless.terminal-failure-custody.v3", Version: 3, Parent: f.entry.parent.Clone(), Source: f.entry.source.Clone(), ProfileDigest: pd, FrontierDigest: d, Stop: domain.MCPOutcomeUnknownV3, Cleanup: "joined_retained", CapturedAt: localTerminalSizeTime, Accounting: ports.OwnedOperationFailureAccountingV1{Version: 3, Scope: m.operation.Scope, AccountingDigest: d, AggregateDebit: future.reservation.AggregateDebit, ComponentAmounts: future.reservation.ComponentAmounts, Operations: []ports.OwnedOperationFailureRowV1{prefixRow, futureRow}}, Operations: []localFailureOperationV1{{Reservation: ops[0].Reservation, Observation: ops[0].Observation, Normalized: json.RawMessage(ops[0].Response)}, {Reservation: future.reservation, Observation: future.observation, Normalized: json.RawMessage(`null`)}}, Derived: run.Clone().DerivedContext}
		for _, event := range plan.Events() {
			draft, err := canonicaloutput.PlanEventDraftV1(run, event.ID, event.Kind, event.Payload, localTerminalSizeTime)
			if err != nil {
				t.Fatal(err)
			}
			c.Events = append(c.Events, localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText})
		}
		bodies, err := f.entry.terminalOutputs.SnapshotRetainedBodies(t.Context())
		if err != nil || len(bodies) == 0 || len(c.Events) != 3 {
			t.Fatal("complete original/planned body inventory", err)
		}
		prefix := bodies[0].Key[:strings.LastIndex(bodies[0].Key, "/")+1]
		for i := range 24 {
			c.Bodies = append(c.Bodies, domain.BlobRef{TenantID: run.TenantID, Key: prefix + fmt.Sprintf("%02d-", i) + strings.Repeat("x", 79), Size: 1 << 20, SHA256: d})
		}
		before := f.canonical.calls
		body, err := encodeLocalFailureCapsule(c, domain.MaxTerminalCapsuleBytesV1)
		if err != nil {
			t.Fatal(err)
		}
		defer clear(body)
		n := uint64(len(body))
		if got, err := localFailureProspectiveCapsuleBytes(c, 4, n); err != nil || got != n {
			t.Fatal("full F3 request/canonical/24-key serializer exact capacity", got, n, err)
		}
		if _, err := localFailureProspectiveCapsuleBytes(c, 4, n-1); err == nil {
			t.Fatal("full F3 inventory one byte over admitted")
		}
		if f.canonical.calls != before || f.entry.failure.Load() != nil || f.entry.terminal.Load() != nil || f.entry.terminalSelection.Load() != 0 {
			t.Fatal("length-only capacity check performed I/O or fabricated a terminal owner")
		}
	})
}

func TestLocalFailureSemanticArgumentNormalization(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := localPreparedModelFixture(t, 0)
		profile := f.actual.config
		frozen, err := configuredinference.FreezeRequestV1(profile, profile.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "normalization DATA only"}}, Tools: []configuredinference.ToolV1{{Name: "shape", Parameters: []byte(`{"type":"object","properties":{"a":{"type":"array","items":{"type":"integer"},"minItems":0,"maxItems":2},"b":{"type":"string"}},"required":["a","b"],"additionalProperties":false}`)}}, MaxOutputTokens: 1})
		if err != nil {
			t.Fatal(err)
		}
		var digests []string
		for _, arguments := range []string{`{"b":"x","a":[1,2]}`, ` { "a" : [1, 2], "b" : "x" } `, `{"a":[2,1],"b":"x"}`, `{"a":[1,2],"b":" x"}`} {
			wire, err := json.Marshal(map[string]any{"model": profile.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "fresh-call-id", "type": "function", "function": map[string]any{"name": "shape", "arguments": arguments}}}}, "finish_reason": "tool_calls"}}})
			if err != nil {
				t.Fatal(err)
			}
			normalized, err := frozen.NormalizeResponseBodyV1(wire, domain.AttemptOperationModelV1, 64<<10)
			if err != nil {
				t.Fatal("actual configured argument normalizer", err)
			}
			decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(profile, frozen.Body(), normalized, domain.AttemptOperationModelV1)
			clear(normalized)
			if err != nil || decoded.Call == nil {
				t.Fatal("actual normalized argument decoder", err)
			}
			digests = append(digests, rawLocalFailureHash(decoded.Call.Arguments))
			clear(decoded.Call.Arguments)
		}
		if digests[0] != digests[1] || digests[0] == digests[2] || digests[0] == digests[3] {
			t.Fatal("semantic guard argument normalization conflates array/string changes or admits whitespace/key-order escape")
		}
	})
}

func TestLocalFailureSemanticGuardGrantRevisionAndDistinctQuery(t *testing.T) {
	for _, mode := range []string{"same_query", "grant_revision", "distinct_query", "distinct_resource"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// Exercise the actual original preparation and its guard. Only the
				// unresolved classification below is explicit test DATA; it grants
				// no Source authentication, reservation, send or writer authority.
				f, _ := localPreparedModelFixture(t, 0)
				model, err := f.entry.PrepareModel(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				query := "public docs"
				if mode == "distinct_query" {
					query = "different bounded query"
				}
				recordPreparedLocalToolCall(t, f, model.operation, model.frozen, [2]string{"new-call-id", query})
				prepared, err := f.entry.PrepareMCP(t.Context())
				if err != nil || prepared == nil {
					t.Fatal("actual current original preparation", err)
				}
				grant := prepared.grant
				prior := localUnresolvedMCP{tenant: grant.TenantID, owner: grant.OwnerUserID, session: f.entry.parent.Scope.SessionID, resource: grant.ResourceID, name: prepared.call.Name(), arguments: rawLocalFailureHash([]byte(`{"query":"public docs"}`))}
				if mode == "grant_revision" {
					grant.ResourceRevision++
					grant.ProfileRevision++
					grant.RevocationGeneration++
					if grant.Validate() != nil {
						t.Fatal("changed grant must remain valid DATA")
					}
				}
				if mode == "distinct_resource" {
					prior.resource = "other-original-resource"
				}
				prepared.source.state.history.unresolvedMCP = []localUnresolvedMCP{prior}
				denied := prepared.source.denyUnresolvedMCP(prepared.call, grant) != nil
				if denied != (mode == "same_query" || mode == "grant_revision") {
					t.Fatal("semantic guard key changed by wrapper/revision or denied different bounded work", mode, denied)
				}
			})
		})
	}
}
