package ydbstore

import (
	"encoding/json"
	"testing"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func recordPreparedLocalToolCall(t *testing.T, f *compactionSourceFixture, op domain.AttemptOperationV1, frozen *configuredinference.FrozenRequestV1) {
	t.Helper()
	installPreparedFutureProxy(t, f, op)
	r, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: op})
	if err != nil || r.Capability == nil || r.Status != ports.AttemptOperationReservedV1 {
		t.Fatalf("actual prepared model Reserve: %v", err)
	}
	if err := f.store.AuthorizeAttemptOperationBeforeSend(t.Context(), r.Capability.Clone()); err != nil {
		t.Fatalf("actual prepared model BeforeSend: %v", err)
	}
	argumentBytes, err := json.Marshal(map[string]string{"query": "public docs"})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"model": f.actual.config.ModelID, "choices": []any{map[string]any{
		"index": 0, "message": map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{
			"id": "mcp-call-1", "type": "function", "function": map[string]any{"name": "search_openai_docs", "arguments": string(argumentBytes)},
		}}}, "finish_reason": "tool_calls",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := frozen.NormalizeResponseBodyV1(wire, op.Kind, op.ResultBytes)
	if err != nil {
		t.Fatalf("normalize exact configured tool call: %v", err)
	}
	defer clear(body)
	ref, err := f.entry.bodies.Write(t.Context(), body)
	if err != nil {
		t.Fatal(err)
	}
	rd, _ := r.Reservation.Digest()
	observation := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: r.Reservation.OperationDigest, ReservationDigest: rd,
		Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: ref.Ref,
		ResponseDigest: ref.Digest, ResponseBytes: ref.Bytes, ObservedAt: f.parent.graph.at,
		Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
	observed, err := f.bootstrap.operate.AuthenticateAttemptOperationObservation(r.ObservationGrant, observation)
	if err != nil {
		t.Fatalf("authenticate original model observation: %v", err)
	}
	if err := f.store.RecordAttemptOperationObservation(t.Context(), ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: r.ObservationGrant, Observation: observed}); err != nil {
		t.Fatalf("record original model observation: %v", err)
	}
}

func TestLocalPreparedMCPOriginalToolCallAndUnknownUsageFence(t *testing.T) {
	f, _ := localPreparedModelFixture(t, 0)
	model, err := f.entry.PrepareModel(t.Context())
	if err != nil || model == nil {
		t.Fatalf("original MODEL preparation: %v", err)
	}
	recordPreparedLocalToolCall(t, f, model.operation, model.frozen)
	prepared, err := f.entry.PrepareMCP(t.Context())
	if err != nil || prepared == nil {
		t.Fatalf("original durable tool call did not prepare MCP DATA: %v", err)
	}
	op, err := prepared.Operation()
	call, callErr := prepared.Call()
	request, requestErr := prepared.Request()
	if err != nil || callErr != nil || requestErr != nil || op.Kind != domain.AttemptOperationMCPToolV1 ||
		op.CallID != "mcp-call-1" || call.ID() != "mcp-call-1" || call.Name() != "search_openai_docs" || request.ID() != call.ID() {
		t.Fatalf("prepared MCP was not derived from exact original MODEL call: operationErr=%v callErr=%v requestErr=%v", err, callErr, requestErr)
	}
	spending := f.entry.source.Spending[2]
	if op.Sequence != 2 || op.ToolGrantDigest != spending.ScopedGrantDigest || op.ExternalChargeEvidenceDigest == "" ||
		op.Debit.AmountMicrounits != *spending.MaxAmountMicrounits || op.Debit.MCPOutboundMessages != 4 || op.Debit.MCPCatalogPages != 1 ||
		op.Debit.MCPInboundFrames == 0 || op.Debit.MCPInboundBytes == 0 || op.Currency != f.entry.parent.Authority.AdmissionCostCeiling.Currency ||
		op.PriceRevision != f.entry.parent.Authority.AdmissionCostCeiling.PriceRevision {
		t.Fatal("prepared MCP operation did not preserve the original scoped grant, exact finite price reservation, protocol debit, and parent envelope")
	}
	if modelObservation := prepared.source.state.operations[0].Observation; modelObservation.Usage.State != domain.AttemptOperationUsageUnknownV1 {
		t.Fatal("test no longer represents unknown original model usage")
	}
	if next, err := f.entry.PrepareModel(t.Context()); err == nil || next != nil {
		t.Fatal("pending MCP result or unknown MCP cost permitted a successor MODEL")
	}
}

func TestLocalPreparedMCPFinalModelIsVerifiedNoop(t *testing.T) {
	f, _ := localPreparedModelFixture(t, 0)
	model, err := f.entry.PrepareModel(t.Context())
	if err != nil || model == nil {
		t.Fatalf("original MODEL preparation: %v", err)
	}
	recordPreparedLocalResponse(t, f, model.operation, model.frozen, "final original answer")
	prepared, err := f.entry.PrepareMCP(t.Context())
	if err != nil || prepared != nil || f.entry.preparedMCP.Load() != nil {
		t.Fatalf("final MODEL must produce verified no-op, not an MCP holder: prepared=%t err=%v", prepared != nil, err)
	}
}

func TestLocalPreparedMCPHolderCopyAndForeignEntryDenied(t *testing.T) {
	f, _ := localPreparedModelFixture(t, 0)
	model, err := f.entry.PrepareModel(t.Context())
	if err != nil || model == nil {
		t.Fatalf("original MODEL preparation: %v", err)
	}
	recordPreparedLocalToolCall(t, f, model.operation, model.frozen)
	prepared, err := f.entry.PrepareMCP(t.Context())
	if err != nil || prepared == nil {
		t.Fatalf("original MCP preparation: %v", err)
	}
	copy := *prepared
	if _, err := copy.Operation(); err == nil {
		t.Fatal("copied prepared MCP holder retained original authority")
	}
	foreign, _ := localPreparedModelFixture(t, 0)
	if prepared.originalForPhase(foreign.entry, localCompactionReserve) {
		t.Fatal("original prepared MCP holder crossed to a foreign invocation entry")
	}
}

func TestLocalPreparedModelInstalledToolMatchesCanonicalJSON(t *testing.T) {
	installed := []configuredinference.ToolV1{{Name: "lookup", Description: "exact", Parameters: []byte(`{"type":"object"}`)}}
	var raw []json.RawMessage
	if !installedMCPToolsMatch(raw, installed) {
		t.Fatal("tool-free original history should remain valid")
	}
	raw = []json.RawMessage{json.RawMessage(`{"type":"function","function":{"name":"lookup","description":"exact","parameters":{"type":"object"}}}`)}
	if !installedMCPToolsMatch(raw, installed) {
		t.Fatal("exact installed tool declaration rejected")
	}
	raw[0] = json.RawMessage(`{"type":"function","function":{"name":"lookup","description":"foreign","parameters":{"type":"object"}}}`)
	if installedMCPToolsMatch(raw, installed) {
		t.Fatal("foreign tool description accepted")
	}
}
