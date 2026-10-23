//go:build sessionless_mcp_offline

package ydbstore_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func offlineChainBody(t *testing.T, b *sessionCompactionBlobs, ref, digest string, size uint64) []byte {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for key, body := range b.objects {
		if !strings.HasSuffix(key, "/"+ref) {
			continue
		}
		h := sha256.Sum256(body)
		if uint64(len(body)) != size || hex.EncodeToString(h[:]) != digest {
			t.Fatal("exact durable operation body length/hash mismatch")
		}
		return bytes.Clone(body)
	}
	t.Fatal("exact durable operation body absent")
	return nil
}

func assertOfflineMCPChain(t *testing.T, seed *ydbstore.LocalSessionConnectedSeed, blobs *sessionCompactionBlobs, result providerloopsession.ModelStepResultV1, terminal *providerloopsession.PreparedLocalTerminalV1, wires []localLoopbackPeerResult, grant mcpadmission.MCPToolGrantV1, mcp *offlineMCPPeer) ([]domain.AttemptOperationReservationV1, []domain.AttemptOperationObservationV1) {
	t.Helper()
	if terminal == nil || !result.Model.SendAttempted || result.Tool == nil || !result.Tool.SendAttempted || result.Compaction != nil {
		t.Fatalf("original finite MODEL/MCP/MODEL terminal missing: terminal=%t model=%t tool=%t compaction=%t", terminal != nil, result.Model.SendAttempted, result.Tool != nil, result.Compaction != nil)
	}
	reservations, observed := seed.MCPOfflineOperationReadback(t)
	if len(reservations) != 3 || len(observed) != 3 {
		t.Fatalf("original durable operations=%d observations=%d want=3", len(reservations), len(observed))
	}
	if !reflect.DeepEqual(result.Model.Observation.Reservation, reservations[2]) || !reflect.DeepEqual(result.Model.Observation.Observation, observed[2]) || seed.Operate.VerifyAttemptOperationObservation(result.Model.Observation) != nil ||
		!reflect.DeepEqual(result.Tool.Observation.Reservation, reservations[1]) || !reflect.DeepEqual(result.Tool.Observation.Observation, observed[1]) || seed.Operate.VerifyAttemptOperationObservation(result.Tool.Observation) != nil {
		t.Fatal("returned final MODEL3/MCP2 not the exact authenticated durable operations")
	}
	for i, kind := range []domain.AttemptOperationKindV1{domain.AttemptOperationModelV1, domain.AttemptOperationMCPToolV1, domain.AttemptOperationModelV1} {
		r, o := reservations[i], observed[i]
		if r.Operation.Kind != kind || r.Operation.Sequence != uint64(i+1) || r.Operation.Scope != reservations[0].Operation.Scope || r.RegisteredAllocationDigest != reservations[0].RegisteredAllocationDigest || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.ValidateForReservation(r) != nil {
			t.Fatalf("original MODEL1/MCP2/MODEL3 chain row%d kind/scope/allocation/outcome mismatch", i+1)
		}
		if i > 0 {
			pred, _ := observed[i-1].Digest()
			total, err := reservations[i-1].AggregateDebit.Add(r.Operation.Debit)
			if err != nil || r.Operation.PredecessorObservationDigest != pred || total != r.AggregateDebit {
				t.Fatalf("original full-debit predecessor chain row%d changed: %v", i+1, err)
			}
		}
	}
	if reservations[2].AggregateDebit.Operations != 3 || reservations[2].AggregateDebit.ModelCalls != 2 || reservations[2].AggregateDebit.ToolDispatches != 1 || reservations[2].AggregateDebit.MCPCatalogPages != 1 {
		t.Fatal("same finite engine did not debit exact 3 operations/2 models/1 tool/1 catalog")
	}
	tool := observed[1]
	if tool.Usage.State != domain.AttemptOperationUsageUnknownV1 || tool.Usage.AmountMicrounits != nil || tool.Usage.InputTokens != nil || tool.Usage.OutputTokens != nil {
		t.Fatal("known durable MCP result fabricated measured money/tokens")
	}
	request := offlineChainBody(t, blobs, reservations[1].Operation.RequestRef, reservations[1].Operation.RequestDigest, reservations[1].Operation.RequestBytes)
	defer clear(request)
	normalized := offlineChainBody(t, blobs, tool.ResponseRef, tool.ResponseDigest, tool.ResponseBytes)
	defer clear(normalized)
	decoded, err := mcpadmission.DecodeSealedToolResultDataV1(reservations[1], tool, normalized)
	if err != nil || decoded.IsError() || !reflect.DeepEqual(decoded.Content(), []string{"offline public docs result"}) || decoded.Usage().AmountMicrounits != nil || decoded.ModelResponseDigest() != observed[0].ResponseDigest {
		t.Fatal("durable normalized MCP result/unknown usage/MODEL1 link mismatch", err)
	}
	if _, err := mcpadmission.DecodeToolResultV1(grant, reservations[1].Operation, request, normalized); err != nil {
		t.Fatal("exact installed grant/request/result linkage", err)
	}
	mcp.mu.Lock()
	same := bytes.Equal(request, mcp.bodies[3])
	methods := append([]string(nil), mcp.methods...)
	mcp.mu.Unlock()
	if !same || !reflect.DeepEqual(methods, []string{"initialize", "notifications/initialized", "tools/list", "tools/call"}) {
		t.Fatal("four actual MCP protocol messages did not include exact prepared call")
	}
	for i, sequence := range []int{0, 2} {
		request := offlineChainBody(t, blobs, reservations[sequence].Operation.RequestRef, reservations[sequence].Operation.RequestDigest, reservations[sequence].Operation.RequestBytes)
		equal := bytes.Equal(request, wires[i].body)
		clear(request)
		if !equal {
			t.Fatalf("MODEL%d frozen body differed from actual original socket bytes", i+1)
		}
	}
	assertOfflinePairOnce(t, wires[0].body, 0, string(reservations[0].Operation.Scope.RunID))
	assertOfflinePairOnce(t, wires[1].body, 1, string(reservations[0].Operation.Scope.RunID))
	return reservations, observed
}

// Historical tool frames are DATA inside the frozen history, not new native
// executable calls. Count exact roles/payloads in both actual MODEL requests.
func assertOfflinePairOnce(t *testing.T, body []byte, pairs int, run string) {
	t.Helper()
	var request struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &request); err != nil || len(request.Messages) != 2 {
		t.Fatal("actual MODEL frozen history request", err)
	}
	type frame struct {
		Role     string `json:"original_role"`
		Payload  string `json:"original_payload"`
		Run      string `json:"run_id"`
		Sequence uint64 `json:"sequence"`
	}
	var history struct {
		Protected []frame `json:"protected_originals"`
		Units     []frame `json:"original_units"`
	}
	if err := json.Unmarshal([]byte(request.Messages[1].Content), &history); err != nil {
		t.Fatal("actual MODEL history DATA grammar", err)
	}
	call, result := 0, 0
	callPosition, resultPosition := -1, -1
	for position, f := range append(history.Protected, history.Units...) {
		var p struct {
			CallID    string            `json:"call_id"`
			Name      string            `json:"name"`
			Arguments map[string]string `json:"arguments"`
			IsError   bool              `json:"is_error"`
			Content   []string          `json:"content"`
		}
		if json.Unmarshal([]byte(f.Payload), &p) != nil || p.CallID != "offline-call-1" {
			continue
		}
		if f.Run != run {
			t.Fatal("historical exact call identity moved to another run")
		}
		switch f.Role {
		case "tool_call":
			call++
			callPosition = position
			if f.Sequence != 1 || p.Name != "search_openai_docs" || p.Arguments["query"] != "public docs" {
				t.Fatal("historical call payload changed")
			}
		case "tool_result":
			result++
			resultPosition = position
			if f.Sequence != 2 || p.IsError || !reflect.DeepEqual(p.Content, []string{"offline public docs result"}) {
				t.Fatal("historical result payload changed")
			}
		}
	}
	if call != pairs || result != pairs || (pairs == 1 && resultPosition != callPosition+1) {
		t.Fatalf("actual frozen request tool frames call=%d result=%d want=%d each positions=%d/%d", call, result, pairs, callPosition, resultPosition)
	}
}

func finishOfflineMCPChain(t *testing.T, terminal *providerloopsession.PreparedLocalTerminalV1, seed *ydbstore.LocalTerminalPublicationSessionConnectedSeed, fixture *ydbstore.LocalTerminalCompletionSQLFixture, blobs *cleanupSessionBlobs, reservations []domain.AttemptOperationReservationV1, observed []domain.AttemptOperationObservationV1) {
	t.Helper()
	output, err := terminal.PrepareOutput(t.Context())
	if err != nil {
		t.Fatal("original joined runtime V2 terminal output", err)
	}
	plan, err := output.Plan()
	if err != nil {
		t.Fatal(err)
	}
	blobs.stateMu.Lock()
	blobs.plan = plan
	blobs.stateMu.Unlock()
	if err := terminal.PublishOutput(t.Context()); err != nil {
		t.Fatal("joined runtime canonical capsule/events publication", err)
	}
	blobs.mu.Lock()
	capsule := bytes.Clone(blobs.objects[string(seed.Scope.TenantID)+"\x00"+plan.Capsule.Key])
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
	if err := json.Unmarshal(capsule, &retained); err != nil || retained.Schema != "sessionless.terminal-custody.v2" || int64(len(capsule)) != plan.Capsule.Size || len(retained.Accounting.Operations) != 3 || len(retained.Events) != 3 || retained.Accounting.AggregateDebit != reservations[2].AggregateDebit {
		t.Fatal("joined runtime retained capsuleV2/full V3 accounting shape", err)
	}
	for i, row := range retained.Accounting.Operations {
		rd, _ := reservations[i].Digest()
		od, _ := observed[i].Digest()
		if row.Sequence != uint64(i+1) || row.Kind != reservations[i].Operation.Kind || row.OperationDigest != reservations[i].OperationDigest || row.ReservationDigest != rd || row.ObservationDigest != od || row.Reserved != reservations[i].Operation.Debit || !reflect.DeepEqual(row.Usage, observed[i].Usage) {
			t.Fatalf("retained V3 row%d not exact original durable operation", i+1)
		}
	}
	tool := retained.Accounting.Operations[1]
	if tool.Disposition != ports.OwnedOperationUnknownFullReservedV3 || tool.Usage.AmountMicrounits != nil || tool.Usage.State != domain.AttemptOperationUsageUnknownV1 || tool.MCPSpendingDigest == "" || retained.Accounting.ComponentAmounts[2] != tool.Reserved.AmountMicrounits {
		t.Fatal("retained known MCP result fabricated money/released full component debit")
	}
	cleanup, err := terminal.FinishCleanup(t.Context())
	if err != nil || cleanup == nil {
		t.Fatal("joined runtime stop/retire/physical erase", err)
	}
	fixture.ObserveCompletion()
	if err := cleanup.Complete(t.Context()); err != nil {
		t.Fatalf("joined runtime canonical complete: %v trace=%+v", err, fixture.Trace())
	}
	completed := fixture.Snapshot(t)
	if completed.Run.Status != domain.RunSucceeded || completed.Attempt.Status != domain.AttemptSucceeded || completed.Reservation.Status != domain.ReservationCommitted || len(completed.Events) != 3 || completed.Finalizations != 1 || completed.LeasePresent || completed.ActiveRuns != 0 {
		t.Fatalf("joined runtime canonical completion: run=%s events=%d finalizations=%d", completed.Run.Status, len(completed.Events), completed.Finalizations)
	}
	for i, kind := range []domain.SessionEventKind{domain.SessionEventToolCall, domain.SessionEventToolResult, domain.SessionEventAssistantMessage} {
		event := completed.Events[i]
		if event.Kind != kind || event.ID != retained.Events[i].ID || event.Payload != retained.Events[i].Payload {
			t.Fatalf("canonical pair/final order/capsule correspondence row%d", i+1)
		}
		blobs.mu.Lock()
		body := bytes.Clone(blobs.objects[string(seed.Scope.TenantID)+"\x00"+event.Payload.Key])
		blobs.mu.Unlock()
		hash := sha256.Sum256(body)
		if int64(len(body)) != event.Payload.Size || hex.EncodeToString(hash[:]) != event.Payload.SHA256 {
			t.Fatalf("canonical physical event%d length/hash after erase", i+1)
		}
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
			t.Fatal("canonical physical event grammar", err)
		}
		if i == 2 {
			if envelope.Schema != "sessionless.assistant-message.v1" || envelope.Summary != "offline joined final answer" || envelope.Manifest != completed.Manifest.ID {
				t.Fatal("canonical final MODEL3 answer was replaced")
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
		if json.Unmarshal(envelope.Content, &content) != nil || envelope.Schema != "sessionless.tool-event.v1" || envelope.CallID != "offline-call-1" || envelope.ToolName != "search_openai_docs" || content.CallID != "offline-call-1" || (i == 0 && (content.Name != "search_openai_docs" || content.Arguments["query"] != "public docs")) || (i == 1 && (content.IsError || !reflect.DeepEqual(content.Content, []string{"offline public docs result"}))) {
			t.Fatalf("canonical physical call/result correlation%d", i+1)
		}
	}
	ad, err := retained.Accounting.Digest()
	if err != nil || completed.Run.TerminalCustody == nil || completed.Run.TerminalCustody.Correspondence == nil || completed.Run.TerminalCustody.Correspondence.AccountingDispositionDigest != ad || completed.Run.TerminalCustody.LocalEvidence == nil || completed.Run.TerminalCustody.LocalEvidence.AccountingDispositionDigest != ad || blobs.deletes == 0 || blobs.absence != blobs.deletes {
		t.Fatal("joined runtime exact full accounting/canonical evidence/erase receipt", err)
	}
	blobs.mu.Lock()
	for key := range blobs.objects {
		if strings.Contains(key, "/attempt-operations-v1/") {
			t.Error("ordinary operation body survived actual erasure", key)
		}
	}
	blobs.mu.Unlock()
	before, puts, deletes := seed.Stats(), blobs.capsulePuts+blobs.eventPuts, blobs.deletes
	if err := cleanup.Complete(t.Context()); err != nil || !reflect.DeepEqual(completed, fixture.Snapshot(t)) || seed.Stats() != before || blobs.capsulePuts+blobs.eventPuts != puts || blobs.deletes != deletes {
		t.Fatal("canonical completed replay repeated physical effects", err)
	}
}

func testOfflineMCPChainCredentialed(t *testing.T) {
	t.Helper()
	peer := newOfflineModelChainPeer(t)
	blobs := &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}}
	original := ydbstore.NewCredentialedLocalCompactionSessionConnectedSeed(t, peer.endpoint(), func(domain.AttemptExecutionSourceScopeV1) ports.BlobStore { return blobs })
	seed := ydbstore.MCPOfflineTerminalSeed(t, original)
	fixture := seed.InstallTerminalCompletionSQL(t)
	blobs.seed = seed
	credentials, _ := newConnectedCredentialLifecycle(t, original)
	session, err := providerloopsession.NewCredentialedV1(t.Context(), providerloopsession.CredentialedConfigV1{Local: providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs}, Credentials: credentials})
	if err != nil {
		t.Fatal("original credentialed finite-engine fixture", err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error("credentialed joined runtime close", err)
		}
	})
	peer.configure(t, seed.Profile.ModelID)
	grant := seed.MCPOfflineInstalledGrant(t)
	mcp := newOfflineMCPPeer(t, grant.Descriptor, 4, nil, false)
	if err := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err != nil {
		t.Fatal(err)
	}
	before := seed.Stats()
	result, terminal, err := session.Run(t.Context(), seed.Request)
	if err != nil {
		var tool ports.AuthenticatedAttemptOperationObservationV1
		if result.Tool != nil {
			tool = result.Tool.Observation
		}
		t.Fatalf("original credentialed MODEL1/MCP2/MODEL3 engine: %v stats=%+v modelSeq=%d tool=(seq=%d outcome=%s reason=%s auth=%v) durable=%+v lifecycleError=%v", err, seed.Stats(), result.Model.Observation.Reservation.Operation.Sequence, tool.Reservation.Operation.Sequence, tool.Observation.Outcome, tool.Observation.Reason, seed.Operate.VerifyAttemptOperationObservation(tool), seed.PersistedCompactionObservations(t), credentials.callbackError)
	}
	mcp.wait(t)
	wires := peer.wait(t, true)
	reservations, observed := assertOfflineMCPChain(t, original, blobs.sessionCompactionBlobs, result, terminal, wires, grant, mcp)
	if credentials.issues != 1 || credentials.materializations != 1 || credentials.callbacks != 1 || credentials.releases != 1 || !credentials.borrowedZeroed || !credentials.materializeReturned || !credentials.releaseSawMaterializeReturned || !credentials.releaseSawBorrowedZeroed || !credentials.releaseHasDeadline {
		t.Fatal("MODEL1/MCP2/MODEL3 escaped one actual Issue/Materialize/disposal/Release")
	}
	stats := seed.Stats()
	if stats.Parent != before.Parent+1 || stats.Burn != before.Burn+1 || stats.Allocation != before.Allocation+1 || stats.Operation != before.Operation+3 || stats.Call != before.Call+3 {
		t.Fatalf("original one-entry MODEL/MCP/MODEL counters: before=%+v after=%+v", before, stats)
	}
	if err := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err == nil {
		t.Fatal("completed original credentialed session rewired")
	}
	again, next, err := session.Run(t.Context(), seed.Request)
	if err == nil || next != nil || again.Model.SendAttempted || seed.Stats() != stats || credentials.issues != 1 || credentials.materializations != 1 || credentials.releases != 1 {
		t.Fatal("completed credentialed engine replayed external effect/lifecycle")
	}
	finishOfflineMCPChain(t, terminal, seed, fixture, blobs, reservations, observed)
}
