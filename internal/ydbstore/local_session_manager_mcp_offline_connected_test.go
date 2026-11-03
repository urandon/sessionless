//go:build sessionless_mcp_offline

package ydbstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/worker"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// The CredentialedMCPOffline prefix intentionally selects the unchanged Make
// offline gate; this scenario is NOAUTH and must never read a credential. The
// real Manager, original worker entry, finite engine and canonical finalizer
// run against controlled HTTP/TLS peers and existing scripted SQL/storage.
// The original running lease is replayed: this is not fresh queued/Web
// admission, native YDB, a real model/MCP endpoint or a full #218 release proof.
func TestLocalSessionCredentialedMCPOfflineManagerChain(t *testing.T) {
	peer := newOfflineModelChainPeer(t)
	blobs := &localManagerBlobs{cleanupSessionBlobs: &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}}}
	seed := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, peer.endpoint(), blobs)
	fixture := seed.InstallManagerReadSQL(t)
	blobs.seed, blobs.seedTest = seed, t
	session, manager, queue, signals, retryError := offlineMCPManager(t, seed, blobs)
	peer.configure(t, seed.Profile.ModelID)
	grant := seed.MCPOfflineInstalledGrant(t)
	mcp := newOfflineMCPPeer(t, grant.Descriptor, 4, nil, false)
	if err := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err != nil {
		t.Fatal("original noauth network instrumentation", err)
	}
	queue.onAck = func() error {
		if _, err := session.WorkerState(); err == nil {
			return errors.New("ACK before original session close/join")
		}
		got := fixture.Snapshot(t)
		if got.Finalizations != 1 || got.LeasePresent || queue.wake != queue.ack+1 {
			return errors.New("ACK before canonical terminal/cleanup/projection wake")
		}
		return nil
	}
	before := seed.Stats()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	outcome, err := manager.RunOnce(ctx)
	got := fixture.Snapshot(t)
	if err != nil || outcome != worker.OutcomeCompleted || got.Run.Status != domain.RunSucceeded || got.Finalizations != 1 || len(got.Events) != 3 || queue.ack != 1 || queue.wake != 1 || queue.retry != 0 || queue.dead != 0 || *retryError != nil {
		t.Fatalf("Manager must finish MODEL1/MCP2/MODEL3 before terminal: outcome=%s err=%v retry=%v events=%d finals=%d queue=%+v stats=%+v", outcome, err, *retryError, len(got.Events), got.Finalizations, queue, seed.Stats())
	}
	mcp.wait(t)
	wires := peer.wait(t, false)
	assertOfflinePairOnce(t, wires[0].body, 0, string(seed.Scope.RunID))
	assertOfflinePairOnce(t, wires[1].body, 1, string(seed.Scope.RunID))
	reservations, observed := seed.MCPOfflineOperationReadback(t)
	if len(reservations) != 3 || len(observed) != 3 {
		t.Fatalf("Manager original durable operations=%d observations=%d want=3", len(reservations), len(observed))
	}
	for i, kind := range []domain.AttemptOperationKindV1{domain.AttemptOperationModelV1, domain.AttemptOperationMCPToolV1, domain.AttemptOperationModelV1} {
		r, o := reservations[i], observed[i]
		if r.Operation.Kind != kind || r.Operation.Sequence != uint64(i+1) || r.Operation.Scope != reservations[0].Operation.Scope || r.RegisteredAllocationDigest != reservations[0].RegisteredAllocationDigest || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.ValidateForReservation(r) != nil {
			t.Fatalf("Manager canonical operation%d kind/scope/allocation/outcome mismatch", i+1)
		}
		if i > 0 {
			predecessor, _ := observed[i-1].Digest()
			total, sumErr := reservations[i-1].AggregateDebit.Add(r.Operation.Debit)
			if sumErr != nil || r.Operation.PredecessorObservationDigest != predecessor || r.AggregateDebit != total {
				t.Fatalf("Manager operation%d original predecessor/debit chain: %v", i+1, sumErr)
			}
		}
	}
	stats := seed.Stats()
	// Allocation counts writes: one original creation plus its completed
	// retirement, not two allocations. All three reservations above retain
	// the same original RegisteredAllocationDigest.
	if stats.Parent != before.Parent+1 || stats.Burn != before.Burn+1 || stats.Allocation != before.Allocation+2 || stats.Operation != before.Operation+3 || stats.Call != before.Call+3 || stats.Observation != before.Observation+3 || seed.ProviderCredentialReadCount() != 0 {
		t.Fatalf("Manager duplicated parent/funding or missed original operations: before=%+v after=%+v credentialReads=%d", before, stats, seed.ProviderCredentialReadCount())
	}
	plan := got.Run.TerminalCustody.Plan
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
		t.Fatal("Manager retained typed capsuleV2/full accountingV3", err)
	}
	for i, row := range retained.Accounting.Operations {
		rd, _ := reservations[i].Digest()
		od, _ := observed[i].Digest()
		if row.Sequence != uint64(i+1) || row.Kind != reservations[i].Operation.Kind || row.OperationDigest != reservations[i].OperationDigest || row.ReservationDigest != rd || row.ObservationDigest != od || row.Reserved != reservations[i].Operation.Debit || !reflect.DeepEqual(row.Usage, observed[i].Usage) {
			t.Fatalf("Manager retained accounting row%d not exact durable observation", i+1)
		}
	}
	tool := retained.Accounting.Operations[1]
	if tool.Disposition != ports.OwnedOperationUnknownFullReservedV3 || tool.Usage.State != domain.AttemptOperationUsageUnknownV1 || tool.Usage.AmountMicrounits != nil || tool.MCPSpendingDigest == "" || retained.Accounting.ComponentAmounts[2] != tool.Reserved.AmountMicrounits {
		t.Fatal("Manager inferred measured MCP cost or released conservative reserved debit")
	}
	for i, kind := range []domain.SessionEventKind{domain.SessionEventToolCall, domain.SessionEventToolResult, domain.SessionEventAssistantMessage} {
		event := got.Events[i]
		if event.Kind != kind || event.ID != retained.Events[i].ID || event.Payload != retained.Events[i].Payload {
			t.Fatalf("Manager canonical ordered event%d differs from retained capsule", i+1)
		}
		blobs.mu.Lock()
		body := bytes.Clone(blobs.objects[string(seed.Scope.TenantID)+"\x00"+event.Payload.Key])
		blobs.mu.Unlock()
		hash := sha256.Sum256(body)
		if int64(len(body)) != event.Payload.Size || hex.EncodeToString(hash[:]) != event.Payload.SHA256 {
			t.Fatalf("Manager canonical physical event%d length/hash mismatch", i+1)
		}
		if i == 2 && !bytes.Contains(body, []byte(`"summary":"offline joined final answer"`)) {
			t.Error("Manager canonical assistant result did not come from final MODEL3")
		}
		clear(body)
	}
	ad, err := retained.Accounting.Digest()
	if err != nil || got.Run.TerminalCustody.Correspondence == nil || got.Run.TerminalCustody.Correspondence.AccountingDispositionDigest != ad || got.Run.TerminalCustody.LocalEvidence == nil || got.Run.TerminalCustody.LocalEvidence.AccountingDispositionDigest != ad || got.LeasePresent || got.ActiveRuns != 0 || got.Reservation.Status != domain.ReservationCommitted || got.Attempt.Status != domain.AttemptSucceeded {
		t.Fatal("Manager terminal ownership/accounting did not commit once", err)
	}
	if signals.count(opssignal.ManagedCleanupSuccess) != 1 || signals.count(opssignal.ManagedCanonicalSuccess) != 1 || signals.count(opssignal.ManagedCleanupFailure) != 0 {
		t.Fatal("Manager cleanup/canonical telemetry differs from actual completion")
	}
	blobs.cleanupMu.Lock()
	deletes, absence := blobs.deletes, blobs.absence
	blobs.cleanupMu.Unlock()
	if deletes == 0 || absence != deletes {
		t.Fatalf("Manager did not finish physical cleanup before ACK: deletes=%d absence=%d", deletes, absence)
	}
	blobs.mu.Lock()
	for key := range blobs.objects {
		if strings.Contains(key, "/attempt-operations-v1/") {
			t.Error("Manager ACK retained ordinary operation body", key)
		}
	}
	puts := blobs.capsulePuts + blobs.eventPuts
	blobs.mu.Unlock()
	// Duplicate delivery republishes the idempotent recovery wake before ACK;
	// it does not repeat any canonical event or original execution effect.
	if outcome, err := manager.RunOnce(ctx); err != nil || outcome != worker.OutcomeDuplicate || queue.ack != 2 || queue.wake != 2 || seed.Stats() != stats || !reflect.DeepEqual(got, fixture.Snapshot(t)) || seed.ProviderCredentialReadCount() != 0 {
		t.Fatal("Manager queue duplicate repeated original work or terminal publication", outcome, err)
	}
	blobs.cleanupMu.Lock()
	afterDeletes := blobs.deletes
	blobs.cleanupMu.Unlock()
	blobs.mu.Lock()
	afterPuts := blobs.capsulePuts + blobs.eventPuts
	blobs.mu.Unlock()
	if afterDeletes != deletes || afterPuts != puts {
		t.Fatal("Manager duplicate repeated physical publication/erasure")
	}
}

// A lost tools/call reply is accepted/unknown, not a known safe result. The
// original Manager must stop before MODEL3 and commit the exact authenticated
// UNKNOWN pair plus notice, retaining rather than erasing the original bodies.
func TestLocalSessionCredentialedMCPOfflineManagerLostReply(t *testing.T) {
	peer := newLocalLoopbackPeer(t, true)
	blobs := &localManagerBlobs{cleanupSessionBlobs: &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}}}
	storage := &offlineMCPLostReplyBlobs{localManagerBlobs: blobs}
	seed := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, peer.endpoint(), storage)
	fixture := seed.InstallManagerReadSQL(t)
	if err := fixture.SelectFailureCompletionFormat(); err != nil {
		t.Fatal("preselect closed failure storage before SQL activity", err)
	}
	blobs.seed, blobs.seedTest = seed, t
	var session *providerloopsession.LocalV1
	var mcp *offlineMCPPeer
	checkJoined := func(ctx context.Context) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := session.WorkerState(); err == nil {
			return errors.New("UNKNOWN projection wake before original session close")
		}
		original, retired := seed.TerminalCleanupAllocation(t)
		if retired.State != domain.RegisteredPreparationRetiredV1 || retired.ValidateTransitionFrom(original) != nil {
			return errors.New("UNKNOWN projection wake before actual allocation retirement")
		}
		// The original session's synchronous Close and genuine allocation
		// retirement are checked without waiting or changing state here. Remote
		// peer EOF/goroutine completion is observed separately after RunOnce;
		// waiting for it in this callback would not prove prior owned joining.
		return nil
	}
	session, manager, queue, signals, retryError := offlineMCPManager(t, seed, storage, checkJoined)
	response, err := json.Marshal(map[string]any{"model": seed.Profile.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "offline-call-1", "type": "function", "function": map[string]any{"name": "search_openai_docs", "arguments": `{"query":"public docs"}`}}}}, "finish_reason": "tool_calls"}}, "usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
	if err != nil {
		t.Fatal("original tool-requesting MODEL response", err)
	}
	peer.setResponse(response)
	clear(response)
	grant := seed.MCPOfflineInstalledGrant(t)
	mcp = newOfflineMCPPeer(t, grant.Descriptor, 4, nil, true)
	if err := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err != nil {
		t.Fatal("original lost-reply network instrumentation", err)
	}
	before := seed.Stats()
	beforePuts := storage.puts.Load() // excludes the seven original history setup PUTs
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	queue.onAck = func() error {
		got := fixture.Snapshot(t)
		if got.Run.Status != domain.RunFailed || got.Attempt.Status != domain.AttemptFailed || got.Reservation.Status != domain.ReservationReleased || got.Finalizations != 1 || got.LeasePresent || got.ActiveRuns != 0 || queue.wake != queue.ack+1 {
			return errors.New("UNKNOWN ACK before exact failed terminal/capacity release/projection wake")
		}
		return checkJoined(ctx)
	}
	outcome, err := manager.RunOnce(ctx)
	got := fixture.Snapshot(t)
	stats := seed.Stats()
	if err != nil || outcome != worker.OutcomeFailed || *retryError != nil || queue.retry != 0 || queue.ack != 1 || queue.wake != 1 || queue.dead != 0 || got.Finalizations != 1 || got.Commits != 1 || got.Run.Status != domain.RunFailed || got.Attempt.Status != domain.AttemptFailed || got.Reservation.Status != domain.ReservationReleased || got.LeasePresent || got.ActiveRuns != 0 || len(got.Events) != 3 || stats.Operation != before.Operation+2 || stats.Observation != before.Observation+2 {
		t.Fatalf("Manager lost reply must truthfully finalize original MODEL1/MCP2 UNKNOWN: outcome=%s err=%v retry=%v operations=%d observations=%d finals=%d queue=%+v trace=%+v", outcome, err, *retryError, stats.Operation, stats.Observation, got.Finalizations, queue, fixture.Trace())
	}
	mcp.wait(t)
	wire := peer.waitResult(t)
	if wire.err != nil || wire.request == nil || len(wire.extra) != 0 || wire.request.Header.Get("Authorization") != "" || wire.request.Header.Get("Cookie") != "" {
		t.Fatal("Manager lost-reply original noauth MODEL1 did not close", wire.err)
	}
	reservations, observed := seed.MCPOfflineOperationReadback(t)
	if len(reservations) != 2 || len(observed) != 2 || reservations[0].Operation.Kind != domain.AttemptOperationModelV1 || reservations[1].Operation.Kind != domain.AttemptOperationMCPToolV1 {
		t.Fatal("Manager lost reply dispatched another operation instead of original pair")
	}
	tool := observed[1]
	if observed[0].Outcome != domain.AttemptOperationCompletedV1 || observed[0].Reason != domain.AttemptOperationReasonNoneV1 || tool.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || tool.Reason != domain.AttemptOperationReasonTransportUnknownV1 || tool.Usage.State != domain.AttemptOperationUsageUnknownV1 || tool.Usage.InputTokens != nil || tool.Usage.OutputTokens != nil || tool.Usage.AmountMicrounits != nil || tool.ResponseRef != "" || tool.ResponseDigest != "" || tool.ResponseBytes != 0 || tool.ValidateForReservation(reservations[1]) != nil {
		t.Fatal("Manager lost reply fabricated known/successful MCP result")
	}
	if stats.Parent != before.Parent+1 || stats.Burn != before.Burn+1 || stats.Allocation != before.Allocation+2 || stats.Call != before.Call+2 || seed.ProviderCredentialReadCount() != 0 || signals.count(opssignal.ManagedCanonicalSuccess) != 0 || signals.count(opssignal.ManagedCleanupSuccess) != 0 {
		t.Fatalf("Manager unknown reply duplicated funding or emitted success: before=%+v after=%+v", before, stats)
	}
	if got.Run.TerminalFailure == nil || got.Run.TerminalFailure.CapsuleSchema != "sessionless.terminal-failure-custody.v3" || got.Run.TerminalFailure.Stop != domain.MCPOutcomeUnknownV3 || got.Run.TerminalFailure.Cleanup != "joined_retained" || got.Run.TerminalCustody == nil || got.Run.TerminalCustody.Correspondence != nil || got.Run.TerminalCustody.LocalEvidence != nil || got.Manifest.ID != "" || got.EvidenceRecord != "" || got.FinalStatus != string(domain.RunFailed) || !strings.HasPrefix(got.FinalDigest, "local_f1:") || len(got.FinalDigest) != 73 {
		t.Fatal("UNKNOWN terminal invented success or omitted strict V3 retained commitment")
	}
	modelRequest := offlineChainBody(t, blobs.sessionCompactionBlobs, reservations[0].Operation.RequestRef, reservations[0].Operation.RequestDigest, reservations[0].Operation.RequestBytes)
	defer clear(modelRequest)
	modelResponse := offlineChainBody(t, blobs.sessionCompactionBlobs, observed[0].ResponseRef, observed[0].ResponseDigest, observed[0].ResponseBytes)
	defer clear(modelResponse)
	bound, err := mcpadmission.BindConfiguredCallV1(seed.Profile, grant, reservations[0], observed[0], modelRequest, modelResponse)
	if err != nil || !bytes.Equal(wire.body, modelRequest) || bound.ID() != reservations[1].Operation.CallID || bound.ArgumentsDigest() != reservations[1].Operation.ArgumentsDigest {
		t.Fatal("UNKNOWN canonical source is not exact original durable MODEL/grant/call", err)
	}
	toolRequest := offlineChainBody(t, blobs.sessionCompactionBlobs, reservations[1].Operation.RequestRef, reservations[1].Operation.RequestDigest, reservations[1].Operation.RequestBytes)
	defer clear(toolRequest)
	mcp.mu.Lock()
	exactSend := len(mcp.bodies) == 4 && bytes.Equal(mcp.bodies[3], toolRequest)
	mcp.mu.Unlock()
	if !exactSend || reservations[1].Operation.ValidatePredecessor(reservations[0], observed[0]) != nil {
		t.Fatal("UNKNOWN tools/call was not exact original persisted successor")
	}
	rd, _ := reservations[1].Digest()
	od, _ := tool.Digest()
	unknown, err := json.Marshal(struct {
		Schema      string                                     `json:"schema"`
		Outcome     string                                     `json:"outcome"`
		Operation   domain.AttemptOperationDigestV1            `json:"operation_digest"`
		Reservation domain.AttemptOperationReservationDigestV1 `json:"reservation_digest"`
		Observation domain.AttemptOperationObservationDigestV1 `json:"observation_digest"`
	}{"sessionless.tool-outcome.v1", "accepted_outcome_unknown", reservations[1].OperationDigest, rd, od})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(unknown)
	call := []byte(`{"call_id":"offline-call-1","name":"search_openai_docs","arguments":{"query":"public docs"}}`)
	expected, err := canonicaloutput.EncodeCompletionV1(got.Run.ID, seed.WorkerPreflightState(t).Job.Limits, ports.ExecutionResult{ToolEvents: []ports.ExecutionToolEvent{{Kind: domain.SessionEventToolCall, CallID: bound.ID(), ToolName: bound.Name(), Payload: call}, {Kind: domain.SessionEventToolResult, CallID: bound.ID(), ToolName: bound.Name(), Payload: unknown}}}, "unused-success-manifest")
	if err != nil {
		t.Fatal(err)
	}
	defer expected.Release()
	notice, err := canonicaloutput.EncodeFailureV1(got.Run.ID, false, domain.MCPOutcomeUnknownV3)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(notice.Payload)
	wantEvents := append(expected.Events()[:2], notice)
	for i, event := range got.Events {
		blobs.mu.Lock()
		body := bytes.Clone(blobs.objects[string(event.TenantID)+"\x00"+event.Payload.Key])
		blobs.mu.Unlock()
		hash := sha256.Sum256(body)
		same := event.ID == wantEvents[i].ID && event.Kind == wantEvents[i].Kind && bytes.Equal(body, wantEvents[i].Payload) && int64(len(body)) == event.Payload.Size && hex.EncodeToString(hash[:]) == event.Payload.SHA256
		clear(body)
		if !same {
			t.Fatalf("UNKNOWN canonical event%d differs from exact call/five-field unknown/fixed notice", i)
		}
	}
	plan := got.Run.TerminalCustody.Plan
	blobs.mu.Lock()
	capsule := bytes.Clone(blobs.objects[string(seed.Scope.TenantID)+"\x00"+plan.Capsule.Key])
	ordinary := 0
	for key := range blobs.objects {
		if strings.Contains(key, "/attempt-operations-v1/") {
			ordinary++
		}
	}
	puts := storage.puts.Load()
	blobs.mu.Unlock()
	defer clear(capsule)
	var retained struct {
		Schema     string                                  `json:"schema"`
		Version    uint32                                  `json:"version"`
		Accounting ports.OwnedOperationFailureAccountingV3 `json:"accounting"`
		Bodies     []domain.BlobRef                        `json:"retained_bodies"`
	}
	hash := sha256.Sum256(capsule)
	if json.Unmarshal(capsule, &retained) != nil || retained.Schema != "sessionless.terminal-failure-custody.v3" || retained.Version != 3 || int64(len(capsule)) != plan.Capsule.Size || hex.EncodeToString(hash[:]) != plan.Capsule.SHA256 || len(retained.Accounting.Operations) != 2 || retained.Accounting.AggregateDebit != reservations[1].AggregateDebit || retained.Accounting.ComponentAmounts != reservations[1].ComponentAmounts || len(retained.Bodies) != 3 || ordinary != 3 {
		t.Fatal("UNKNOWN capsule lost complete full-reserved frontier or retained original namespace")
	}
	for i, row := range retained.Accounting.Operations {
		rd, _ := reservations[i].Digest()
		od, _ := observed[i].Digest()
		if row.Reserved != reservations[i].Operation.Debit || row.OperationDigest != reservations[i].OperationDigest || row.ReservationDigest != rd || row.ObservationDigest != od || row.Outcome != observed[i].Outcome || row.Reason != observed[i].Reason || !reflect.DeepEqual(row.Usage, observed[i].Usage) {
			t.Fatalf("UNKNOWN accounting row%d differs from actual full reservation/observation", i)
		}
	}
	aggregate, err := reservations[0].AggregateDebit.Add(reservations[1].Operation.Debit)
	accountingDigest, digestErr := retained.Accounting.Digest()
	if err != nil || digestErr != nil || aggregate != reservations[1].AggregateDebit || accountingDigest != got.Run.TerminalFailure.AccountingDigest || retained.Accounting.Operations[1].MCPSpendingDigest == "" || retained.Accounting.ComponentAmounts[2] != reservations[1].Operation.Debit.AmountMicrounits || reservations[1].Operation.Debit.AmountMicrounits != *seed.OriginalProfileReceipt().Profile.CostCeiling.MCPToolCost.MaxAmountMicrounits {
		t.Fatal("UNKNOWN result refunded/repriced the actual original reserved debit", err, digestErr)
	}
	blobs.stateMu.Lock()
	capsulePuts, eventPuts, storageFailure := blobs.capsulePuts, blobs.eventPuts, blobs.failure
	blobs.stateMu.Unlock()
	blobs.cleanupMu.Lock()
	deletes, absence := blobs.deletes, blobs.absence
	blobs.cleanupMu.Unlock()
	if capsulePuts != 1 || eventPuts != 3 || puts-beforePuts != 7 || storageFailure != "" || deletes != 0 || absence != 0 {
		t.Fatalf("UNKNOWN retained writes/erasure: capsule=%d events=%d setup_puts=%d actual_puts=%d delta=%d want_delta=7 storage_error=%q deletes=%d absence=%d", capsulePuts, eventPuts, beforePuts, puts, puts-beforePuts, storageFailure, deletes, absence)
	}
	if outcome, err := manager.RunOnce(ctx); err != nil || outcome != worker.OutcomeDuplicate || queue.retry != 0 || queue.ack != 2 || queue.wake != 2 || seed.Stats() != stats || !reflect.DeepEqual(got, fixture.Snapshot(t)) || seed.ProviderCredentialReadCount() != 0 {
		t.Fatal("Manager committed UNKNOWN duplicate repeated effects or canonical completion", outcome, err)
	}
	blobs.mu.Lock()
	afterPuts := storage.puts.Load()
	blobs.mu.Unlock()
	blobs.cleanupMu.Lock()
	afterDeletes, afterAbsence := blobs.deletes, blobs.absence
	blobs.cleanupMu.Unlock()
	if afterPuts != puts || afterDeletes != 0 || afterAbsence != 0 {
		t.Fatal("UNKNOWN duplicate repeated physical publication/erasure")
	}
	mcp.mu.Lock()
	frames := len(mcp.methods)
	mcp.mu.Unlock()
	if frames != 4 {
		t.Fatalf("Manager lost-reply queue replay changed actual MCP protocol frames=%d want=4", frames)
	}
}

type offlineMCPLostReplyBlobs struct {
	*localManagerBlobs
	puts atomic.Uint64
}

func (b *offlineMCPLostReplyBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	b.puts.Add(1)
	return b.localManagerBlobs.Put(ctx, tenant, key, r)
}

type offlineMCPCheckedWake struct {
	queue  *localManagerQueue
	before func(context.Context) error
}

func (w offlineMCPCheckedWake) PublishFrontendProjectionWake(ctx context.Context, tenant domain.TenantID, run domain.RunID, at time.Time) error {
	if err := w.before(ctx); err != nil {
		return err
	}
	return w.queue.PublishFrontendProjectionWake(ctx, tenant, run, at)
}

func offlineMCPManager(t *testing.T, seed *ydbstore.LocalTerminalPublicationSessionConnectedSeed, blobs ports.BlobStore, beforeWake ...func(context.Context) error) (*providerloopsession.LocalV1, *worker.Manager, *localManagerQueue, *localManagerOpsSink, *error) {
	t.Helper()
	if seed.Profile.Auth != domain.ConfiguredInferenceAuthNoneV1 || seed.ProviderCredentialReadCount() != 0 {
		t.Fatal("Manager noauth fixture unexpectedly acquired credentials")
	}
	session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal("original Manager local session", err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error("Manager session close", err)
		}
	})
	state, err := session.WorkerState()
	if err != nil {
		t.Fatal("original Manager worker facade", err)
	}
	queue := &localManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "offline-manager-message", Kind: queuecontract.KindDispatchRun, TenantID: seed.Scope.TenantID, SubjectID: string(seed.Scope.RunID), EnqueuedAt: localManagerClock{}.Now()}, ReceiptHandle: "offline-manager-receipt", DeliveryCount: 1}}
	signals := &localManagerOpsSink{}
	var projection ports.FrontendProjectionWakePublisher = queue
	if len(beforeWake) > 1 {
		t.Fatal("one projection observation only")
	}
	if len(beforeWake) == 1 {
		projection = offlineMCPCheckedWake{queue: queue, before: beforeWake[0]}
	}
	var retryError error
	manager, err := worker.New(worker.Config{ScratchRoot: t.TempDir(), WorkerID: seed.WorkerPreflightState(t).Attempt.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, LocalOwnedSession: session, DeliveryWakePublisher: queue, ProjectionWakePublisher: projection, PhysicalClaimGenerator: func(context.Context) (string, error) { return seed.Request.PhysicalInvocationClaimID, nil }, RetryObserver: func(err error) { retryError = err }, OpsSignals: signals}, localManagerClock{}, queue, state, blobs, nil)
	if err != nil {
		t.Fatal("original explicit Manager constructor", err)
	}
	return session, manager, queue, signals, &retryError
}
