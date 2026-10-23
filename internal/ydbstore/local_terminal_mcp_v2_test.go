package ydbstore_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localloopback"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Actual constructor-created Source/Store/keeper/bridge, prepared holders,
// issuer/body custody, physical erasure and canonical finalization are joined.
// SQL, proxy rows, funding documents and external responses are scripted DATA.
// No network/model/MCP call, physical TLS or native YDB proof is claimed.
func TestLocalTerminalMCPV2OriginalPublicationCleanupAndReplay(t *testing.T) {
	completedOriginalMCPTerminalV2Fixture(t)
}

type originalMCPTerminalV2Owners struct {
	entry  *ydbstore.LocalInvocationEntryV1
	store  *ydbstore.Store
	keeper *localloopback.KeeperV1
	bridge *serverlessharness.BoundedOwnedAllocationPreparationV1
}

func newOriginalMCPTerminalV2Owners(t *testing.T, seed *ydbstore.LocalTerminalPublicationSessionConnectedSeed, blobs ports.BlobStore) originalMCPTerminalV2Owners {
	t.Helper()
	bootstrap, err := ydbstore.NewLocalOwnedStoreBootstrapV1(seed.Source, seed.Prepare, seed.Operate)
	if err != nil {
		t.Fatal("original bootstrap", err)
	}
	keeper, err := localloopback.NewKeeperV1(seed.Source, seed.Operate, bootstrap)
	if err != nil {
		t.Fatal("original keeper", err)
	}
	t.Cleanup(func() {
		if err := keeper.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := bootstrap.CreateStore(seed.DB, seed.Options, keeper)
	if err != nil {
		t.Fatal("original Store", err)
	}
	bridge, err := serverlessharness.NewBoundedOwnedLocalAllocationPreparationV1(seed.Prepare, seed.Backend, seed.Substrate, seed.Source, bootstrap)
	if err != nil {
		t.Fatal("original bridge", err)
	}
	t.Cleanup(func() {
		if err := bridge.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := bootstrap.Complete(bridge); err != nil {
		t.Fatal("original tuple completion", err)
	}
	entry, err := bootstrap.StartLocalInvocation(t.Context(), seed.Request, blobs)
	if err != nil {
		t.Fatal("original admitted entry", err)
	}
	return originalMCPTerminalV2Owners{entry: entry, store: store, keeper: keeper, bridge: bridge}
}

type originalMCPTerminalV2Fixture struct {
	owners     originalMCPTerminalV2Owners
	seed       *ydbstore.LocalTerminalPublicationSessionConnectedSeed
	completion *ydbstore.LocalTerminalCompletionSQLFixture
	blobs      *cleanupSessionBlobs
	output     *ydbstore.LocalPreparedOutputV1
	captured   *ydbstore.LocalCompactionSourceV1
	completed  ydbstore.LocalTerminalCompletionSnapshot
	capsule    []byte
}

func completedOriginalMCPTerminalV2Fixture(t *testing.T, limits ...ydbstore.LocalCompactionConnectedLimits) *originalMCPTerminalV2Fixture {
	t.Helper()
	blobs := &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}}
	seed := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, "http://127.0.0.1:49183/v1", blobs, limits...)
	fixture := seed.InstallTerminalCompletionSQL(t)
	blobs.seed = seed
	owners := newOriginalMCPTerminalV2Owners(t, seed, blobs)
	entry, store, keeper := owners.entry, owners.store, owners.keeper
	parent, err := entry.Parent()
	if err != nil {
		t.Fatal(err)
	}
	source, err := entry.Source()
	if err != nil {
		t.Fatal(err)
	}
	bodies, err := entry.Bodies()
	if err != nil {
		t.Fatal(err)
	}
	record := func(op domain.AttemptOperationV1, normalized []byte) ports.AuthenticatedAttemptOperationObservationV1 {
		t.Helper()
		seed.InstallTerminalMCPV2ProxyData(t, entry, op)
		r, err := store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: op})
		if err != nil || r.Capability == nil || r.Status != ports.AttemptOperationReservedV1 {
			t.Fatalf("original %s/%d reserve: %v", op.Kind, op.Sequence, err)
		}
		if err := store.AuthorizeAttemptOperationBeforeSend(t.Context(), r.Capability.Clone()); err != nil {
			t.Fatalf("original %s/%d before-send: %v", op.Kind, op.Sequence, err)
		}
		ref, err := bodies.Write(t.Context(), normalized)
		if err != nil {
			t.Fatal(err)
		}
		rd, _ := r.Reservation.Digest()
		o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: r.Reservation.OperationDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: ref.Ref, ResponseDigest: ref.Digest, ResponseBytes: ref.Bytes, ObservedAt: time.Now().UTC().Truncate(time.Microsecond), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
		auth, err := seed.Operate.AuthenticateAttemptOperationObservation(r.ObservationGrant, o)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.RecordAttemptOperationObservation(t.Context(), ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: r.ObservationGrant, Observation: auth}); err != nil {
			t.Fatalf("original %s/%d record: %v", op.Kind, op.Sequence, err)
		}
		read, err := store.ReadAttemptOperationObservation(t.Context(), r.ObservationGrant)
		if err != nil || !reflect.DeepEqual(read, auth) {
			t.Fatal("exact original normalized observation readback", err)
		}
		return read
	}
	model, err := entry.PrepareModel(t.Context())
	if err != nil {
		t.Fatal("original MODEL1 preparation", err)
	}
	op, err := model.Operation()
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := model.FrozenRequest()
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"model": seed.Profile.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "terminal-mcp-call", "type": "function", "function": map[string]any{"name": "search_openai_docs", "arguments": `{"query":"public docs"}`}}}}, "finish_reason": "tool_calls"}}})
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := frozen.NormalizeResponseBodyV1(wire, op.Kind, op.ResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	record(op, normalized)
	clear(normalized)
	mcp, err := entry.PrepareMCP(t.Context())
	if err != nil || mcp == nil {
		t.Fatal("original MCP2 preparation", err)
	}
	mcpOp, err := mcp.Operation()
	if err != nil {
		t.Fatal(err)
	}
	request, err := mcp.Request()
	if err != nil {
		t.Fatal(err)
	}
	call, err := mcp.Call()
	if err != nil {
		t.Fatal(err)
	}
	wire, err = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.ID(), "result": map[string]any{"content": []any{map[string]any{"type": "text", "text": "original terminal MCP result"}}, "isError": false}})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := request.DecodeToolResultV1(wire)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err = mcpadmission.EncodeToolResultV1(call, mcpOp, parsed, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1})
	if err != nil {
		t.Fatal(err)
	}
	mcpObserved := record(mcpOp, normalized)
	clear(normalized)
	model, err = entry.PrepareModel(t.Context())
	if err != nil || model == nil {
		t.Fatal("genuine original MODEL3 after MCP2", err)
	}
	op, err = model.Operation()
	if err != nil || op.Sequence != 3 {
		t.Fatal("original MODEL3 sequence", err)
	}
	frozen, err = model.FrozenRequest()
	if err != nil {
		t.Fatal(err)
	}
	wire, err = json.Marshal(map[string]any{"model": seed.Profile.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": "final answer using original MCP result"}, "finish_reason": "stop"}}})
	if err != nil {
		t.Fatal(err)
	}
	normalized, err = frozen.NormalizeResponseBodyV1(wire, op.Kind, op.ResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	record(op, normalized)
	clear(normalized)
	if again, err := entry.PrepareMCP(t.Context()); err != nil || again != nil {
		t.Fatal("final MODEL must not reserve another MCP", err)
	}
	currentSource, err := entry.Source()
	if err != nil || !reflect.DeepEqual(currentSource, source) {
		t.Fatal("original source funding/envelopes changed during genuine continuation", err)
	}
	h, err := entry.ClaimTerminal(t.Context())
	if err != nil {
		t.Fatal("original terminal claim", err)
	}
	deadline, err := h.Deadline()
	if err != nil {
		t.Fatal(err)
	}
	if err := keeper.SealTerminal(deadline); err != nil {
		t.Fatal("genuine keeper terminal seal", err)
	}
	captured, err := h.Capture(t.Context())
	if err != nil {
		t.Fatal("original three-operation terminal capture", err)
	}
	t.Cleanup(captured.Clear)
	output, err := h.PrepareOutput(t.Context())
	if err != nil {
		t.Fatal("original capsuleV2 output preparation", err)
	}
	t.Cleanup(output.Clear)
	plan, err := output.Plan()
	if err != nil {
		t.Fatal(err)
	}
	blobs.stateMu.Lock()
	blobs.plan = plan
	blobs.stateMu.Unlock()
	if err := output.Publish(t.Context()); err != nil {
		t.Fatal("original plan registration and physical output publication", err)
	}
	blobs.mu.Lock()
	capsule := bytes.Clone(blobs.objects[string(source.Scope.TenantID)+"\x00"+plan.Capsule.Key])
	blobs.mu.Unlock()
	defer clear(capsule)
	var retained struct {
		Schema     string                                      `json:"schema"`
		Accounting ports.OwnedOperationAccountingDispositionV3 `json:"accounting_disposition"`
		Events     []struct {
			ID      domain.SessionEventID   `json:"id"`
			Kind    domain.SessionEventKind `json:"kind"`
			Payload domain.BlobRef          `json:"payload"`
		} `json:"events"`
	}
	if err := json.Unmarshal(capsule, &retained); err != nil || retained.Schema != "sessionless.terminal-custody.v2" || int64(len(capsule)) != plan.Capsule.Size || retained.Accounting.ValidateFor(parent, source) != nil || len(retained.Accounting.Operations) != 3 || len(retained.Events) != 3 {
		t.Fatal("actual retained capsuleV2/V3 shape and exact bound", err)
	}
	row := retained.Accounting.Operations[1]
	if row.Disposition != ports.OwnedOperationUnknownFullReservedV3 || row.Reserved != mcpOp.Debit || row.Usage.AmountMicrounits != nil || row.Usage.State != domain.AttemptOperationUsageUnknownV1 || !reflect.DeepEqual(row.Usage, mcpObserved.Observation.Usage) || retained.Accounting.ComponentAmounts[2] != *source.Spending[2].MaxAmountMicrounits || retained.Accounting.AggregateDebit.ToolDispatches != 1 || retained.Accounting.AggregateDebit.MCPCatalogPages != 1 {
		t.Fatal("actual retained MCP money is not UNKNOWN with full original nonrefundable debit")
	}
	blobs.beforeDelete = func() {
		original, current := seed.TerminalCleanupAllocation(t)
		if current.State != domain.RegisteredPreparationRetiredV1 || current.ValidateTransitionFrom(original) != nil {
			t.Error("physical Delete before original confirmed retirement")
		}
	}
	cleanup, err := output.FinishCleanup(t.Context())
	if err != nil || cleanup == nil {
		t.Fatal("actual keeper/bridge stop, retirement and namespace erasure", err)
	}
	blobs.mu.Lock()
	for key := range blobs.objects {
		if strings.Contains(key, "/attempt-operations-v1/") {
			t.Error("ordinary body survived genuine erasure", key)
		}
	}
	blobs.mu.Unlock()
	fixture.ObserveCompletion()
	if err := cleanup.Complete(t.Context()); err != nil {
		t.Fatalf("original canonical finalizer: %v trace=%+v diagnostic=%+v", err, fixture.Trace(), fixture.DiagnoseCleanup(t, cleanup))
	}
	completed := fixture.Snapshot(t)
	if completed.Run.Status != domain.RunSucceeded || completed.Attempt.Status != domain.AttemptSucceeded || completed.Reservation.Status != domain.ReservationCommitted || len(completed.Events) != 3 || completed.Finalizations != 1 || completed.LeasePresent || completed.ActiveRuns != 0 {
		t.Fatalf("actual canonical/scheduler completion missing: run=%s events=%d finalizations=%d", completed.Run.Status, len(completed.Events), completed.Finalizations)
	}
	for i, kind := range []domain.SessionEventKind{domain.SessionEventToolCall, domain.SessionEventToolResult, domain.SessionEventAssistantMessage} {
		if completed.Events[i].Kind != kind || completed.Events[i].ID != retained.Events[i].ID || completed.Events[i].Payload != retained.Events[i].Payload {
			t.Fatal("canonical pair+answer missing/reordered/replaced", i)
		}
		blobs.mu.Lock()
		body := bytes.Clone(blobs.objects[string(source.Scope.TenantID)+"\x00"+completed.Events[i].Payload.Key])
		blobs.mu.Unlock()
		var envelope struct {
			Schema   string                    `json:"schema"`
			CallID   string                    `json:"call_id"`
			ToolName string                    `json:"tool_name"`
			Content  json.RawMessage           `json:"content"`
			Summary  string                    `json:"summary"`
			Manifest domain.ArtifactManifestID `json:"artifact_manifest_id"`
		}
		err := json.Unmarshal(body, &envelope)
		clear(body)
		if err != nil {
			t.Fatal("canonical physical payload absent or malformed after erasure", i, err)
		}
		if i == 2 {
			if envelope.Schema != "sessionless.assistant-message.v1" || envelope.Summary != "final answer using original MCP result" || envelope.Manifest != completed.Manifest.ID {
				t.Fatal("final canonical answer or manifest changed")
			}
			continue
		}
		var content struct {
			CallID    string            `json:"call_id"`
			Name      string            `json:"name"`
			Arguments map[string]string `json:"arguments"`
			IsError   bool              `json:"is_error"`
			Content   []string          `json:"content"`
		}
		if err := json.Unmarshal(envelope.Content, &content); err != nil || envelope.Schema != "sessionless.tool-event.v1" || envelope.CallID != call.ID() || envelope.ToolName != call.Name() || content.CallID != call.ID() {
			t.Fatal("canonical physical tool pair correlation changed", i, err)
		}
		if i == 0 && (content.Name != call.Name() || content.Arguments["query"] != "public docs") || i == 1 && (content.IsError || !reflect.DeepEqual(content.Content, []string{"original terminal MCP result"})) {
			t.Fatal("canonical physical call arguments/result bytes changed", i)
		}
	}
	ad, err := retained.Accounting.Digest()
	if err != nil || completed.Run.TerminalCustody == nil || completed.Run.TerminalCustody.Correspondence == nil || completed.Run.TerminalCustody.Correspondence.AccountingDispositionDigest != ad || completed.Run.TerminalCustody.LocalEvidence == nil || completed.Run.TerminalCustody.LocalEvidence.Validate() != nil || completed.Run.TerminalCustody.LocalEvidence.AccountingDispositionDigest != ad || blobs.deletes == 0 || blobs.absence != blobs.deletes {
		t.Fatal("exact actual V3 accounting not joined to canonical evidence", err)
	}
	stats, puts, deletes := seed.Stats(), blobs.capsulePuts+blobs.eventPuts, blobs.deletes
	if err := cleanup.Complete(t.Context()); err != nil {
		t.Fatal("exact original read-only replay", err)
	}
	if !reflect.DeepEqual(completed, fixture.Snapshot(t)) || seed.Stats() != stats || blobs.capsulePuts+blobs.eventPuts != puts || blobs.deletes != deletes {
		t.Fatal("canonical replay repeated original effects")
	}
	retainedCapsule := bytes.Clone(capsule)
	t.Cleanup(func() { clear(retainedCapsule) })
	return &originalMCPTerminalV2Fixture{owners: owners, seed: seed, completion: fixture, blobs: blobs, output: output, captured: captured, completed: completed, capsule: retainedCapsule}
}
