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
	"net"
	"net/http"
	"net/netip"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/worker"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// The real original Store/issuer records and reads the observation; this test
// never authors one. SQL readback is correlation DATA, not a recreated MAC or
// producer. The MCP peer receives the exact tools/call and closes without a
// reply. Canonical failure is checked only after those causal facts and the
// actual MODEL/MCP transport joins have independently been established.
func TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailure(t *testing.T) {
	runUnknownMCPFailureWeb(t, unknownMCPWebOptions{})
}

func TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailureCommitLoss(t *testing.T) {
	for _, mode := range []string{"rollback", "ambiguous"} {
		t.Run(mode, func(t *testing.T) { runUnknownMCPFailureWeb(t, unknownMCPWebOptions{commitFault: mode}) })
	}
}

func TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailureIncompleteFrontier(t *testing.T) {
	for _, mode := range []string{"missing_readback", "missing_tail", "changed_tail"} {
		t.Run(mode, func(t *testing.T) { runUnknownMCPFailureWeb(t, unknownMCPWebOptions{observationFault: mode}) })
	}
}

func TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailureTaintedResponse(t *testing.T) {
	runUnknownMCPFailureWeb(t, unknownMCPWebOptions{taintedResponse: true})
}

func TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailureFreshHistory(t *testing.T) {
	runUnknownMCPFailureWeb(t, unknownMCPWebOptions{freshHistory: true})
}

func TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailureSemanticReplay(t *testing.T) {
	runUnknownMCPFailureWeb(t, unknownMCPWebOptions{semanticRetry: true})
}

func TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailureChangedHistory(t *testing.T) {
	for _, mode := range []string{"changed_notice", "missing_notice", "metadata_stripped", "marker_stripped"} {
		t.Run(mode, func(t *testing.T) {
			runUnknownMCPFailureWeb(t, unknownMCPWebOptions{freshHistory: true, historyFault: mode})
		})
	}
}

type unknownMCPWebOptions struct {
	commitFault, observationFault, historyFault  string
	taintedResponse, freshHistory, semanticRetry bool
}

// The peer performs an actual normalized response PUT, then loses its ACK.
// That fourth attempted ordinary key must stay reconciliation-only residue,
// not become an imaginary response or a truncated request-only F3 frontier.
type unknownMCPResponsePutBlobs struct {
	ports.BlobStore
	ordinary, uncertain int
}

func (b *unknownMCPResponsePutBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	ref, err := b.BlobStore.Put(ctx, tenant, key, r)
	if strings.Contains(key, "/attempt-operations-v1/") {
		b.ordinary++
		if b.ordinary == 4 && err == nil {
			b.uncertain++
			return domain.BlobRef{}, errors.New("controlled actual MCP response PUT lost acknowledgement")
		}
	}
	return ref, err
}

func runUnknownMCPFailureWeb(t *testing.T, options unknownMCPWebOptions) {
	t.Helper()
	wantModels := 1
	if options.freshHistory && options.historyFault == "" || options.semanticRetry {
		wantModels = 2
	}
	model := newOfflineModelChainPeer(t, wantModels)
	seed := ydbstore.NewLocalWebManagerSeed(t, model.endpoint())
	profile, err := seed.Source.Configuration()
	if err != nil {
		t.Fatal("original installed MODEL profile", err)
	}
	model.configure(t, profile.ModelID)
	if options.semanticRetry {
		// Fresh Run and call ID, equivalent whitespace in the actual MODEL
		// response. The normalizer—not a test-authored operation—selects args.
		body, err := json.Marshal(map[string]any{"model": profile.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "regenerated-unresolved-call", "type": "function", "function": map[string]any{"name": "search_openai_docs", "arguments": "  { \"query\" : \"public docs\" }  "}}}}, "finish_reason": "tool_calls"}}, "usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
		if err != nil {
			t.Fatal(err)
		}
		model.mu.Lock()
		clear(model.response[1])
		model.response[1] = body
		model.mu.Unlock()
	}
	if !seed.Empty() {
		t.Fatal("Web setup already contained invocation or spending facts")
	}
	blobs := &canonicalManagerBlobs{canonicalTerminalBlobs: &canonicalTerminalBlobs{sessionCompactionBlobs: newSessionCompactionBlobs(), owner: t}}
	h := newWebManagerHandler(t, seed, blobs, newWebManagerAuth(seed.Tenant, seed.Owner))
	cookie, csrf := webManagerLogin(t, h)
	path := "/api/web/v1/sessions/" + string(seed.Session) + "/messages"
	response := webManagerRequest(h, http.MethodPost, path, `{"text":"bounded public query with lost MCP reply","idempotency_key":"web-unknown-mcp"}`, cookie, csrf)
	if response.Code != http.StatusCreated {
		t.Fatalf("genuine authenticated Web admission: status=%d want=%d SQL=%s", response.Code, http.StatusCreated, seed.LastError())
	}
	originalStorage := seed.OriginalStorageDigest(t)
	managerSeed := seed.AdmitManager(t)
	if options.commitFault != "" {
		managerSeed.LoseNextTerminalCommit(t, options.commitFault)
	}
	if options.observationFault != "" {
		seed.SetMCPTailObservationFault(t, options.observationFault)
	}
	blobs.seed = managerSeed.LocalCanonicalTerminalSeed
	var storage ports.BlobStore = blobs
	tainted := &unknownMCPResponsePutBlobs{BlobStore: blobs}
	if options.taintedResponse {
		storage = tainted
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	invocation, err := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: managerSeed.Prepare, Operate: managerSeed.Operate, Backend: managerSeed.Backend, Substrate: managerSeed.Substrate, Blobs: storage})
	if err != nil {
		t.Fatal("inert original worker session", err)
	}
	t.Cleanup(func() {
		if err := invocation.Close(); err != nil {
			t.Error("original UNKNOWN MCP session cleanup/join", err)
		}
	})
	mcp := newOfflineMCPPeer(t, managerSeed.Grant.Descriptor, 4, nil, !options.taintedResponse)
	if err := invocation.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err != nil {
		t.Fatal("instance-scoped offline MCP instrumentation", err)
	}
	state, err := invocation.WorkerState()
	if err != nil {
		t.Fatal("original Manager Store facade", err)
	}
	q := &canonicalManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "web-unknown-mcp-delivery", Kind: queuecontract.KindDispatchRun, TenantID: seed.Tenant, SubjectID: string(managerSeed.Loaded.Run.ID), EnqueuedAt: managerSeed.Loaded.Job.CreatedAt}}}
	var retryError error
	manager, err := worker.New(worker.Config{ScratchRoot: t.TempDir(), WorkerID: managerSeed.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, LocalOwnedSession: invocation, ProjectionWakePublisher: q, DeliveryWakePublisher: q, RetryObserver: func(err error) { retryError = err }}, localManagerClock{}, q, state, storage, nil)
	if err != nil {
		t.Fatal("original Manager composition", err)
	}
	q.beforeAck = func() error {
		got := managerSeed.TerminalSnapshot(t)
		if got.Run.Status != domain.RunFailed || got.Attempt.Status != domain.AttemptFailed || got.Reservation.Status != domain.ReservationReleased || got.Finalizations != 1 || got.Run.TerminalFailure == nil || got.Run.TerminalFailure.Stop != "mcp_outcome_unknown" || got.Run.TerminalCustody == nil || got.Run.TerminalCustody.LocalEvidence != nil || got.Run.TerminalCustody.Correspondence != nil || got.LeasePresent || got.ActiveRuns != 0 || q.wake != q.ack+1 || blobs.deletes != 0 || blobs.absences != 0 {
			return errors.New("UNKNOWN MCP ACK before truthful failed terminal/joined-retained release/wake")
		}
		if _, err := invocation.WorkerState(); err == nil {
			return errors.New("UNKNOWN MCP ACK before original session close")
		}
		return nil
	}
	outcome, runErr := manager.RunOnce(ctx)
	if _, err := invocation.WorkerState(); err == nil {
		t.Fatal("original owner not closed before restoring read-only fault view")
	}
	seed.EndMCPTailObservationFault()
	mcp.wait(t)
	model.waitCompleted(t, 1)
	var wires []localLoopbackPeerResult
	if wantModels == 1 {
		wires = model.wait(t, false)
	}
	if model.completedCalls() != 1 || wantModels == 1 && len(wires) != 1 {
		t.Fatalf("lost MCP reply continued MODEL: completed=%d requests=%d want=1", model.completedCalls(), len(wires))
	}
	// These rows were produced solely by the actual Store/issuer flow above.
	// OperationReadback checks durable row/index/reservation/observation digests;
	// it does not manufacture or authenticate an observation for this assertion.
	reservations, observations := managerSeed.OperationReadback(t)
	if len(reservations) != 2 || len(observations) != 2 {
		t.Fatalf("actual MODEL/MCP complete persisted frontier: reservations=%d observations=%d want=2", len(reservations), len(observations))
	}
	parent := managerSeed.ManagerParent(t)
	for i, kind := range []domain.AttemptOperationKindV1{domain.AttemptOperationModelV1, domain.AttemptOperationMCPToolV1} {
		r, o := reservations[i], observations[i]
		if r.ValidateForParent(parent.Authority, parent.Reservation) != nil || o.ValidateForReservation(r) != nil || r.Operation.Kind != kind || r.Operation.Sequence != uint64(i+1) || r.Operation.Scope != reservations[0].Operation.Scope || r.RegisteredAllocationDigest != reservations[0].RegisteredAllocationDigest || r.Operation.Scope.TenantID != seed.Tenant || r.Operation.Scope.OwnerUserID != seed.Owner || r.Operation.Scope.SessionID != seed.Session || r.Operation.Scope.RunID != managerSeed.Loaded.Run.ID || r.Operation.Scope.AttemptID != managerSeed.Loaded.Attempt.ID {
			t.Fatalf("actual persisted frontier row=%d kind/scope/parent/allocation mismatch", i+1)
		}
	}
	known, unknown := observations[0], observations[1]
	wantReason := domain.AttemptOperationReasonTransportUnknownV1
	if options.taintedResponse {
		wantReason = domain.AttemptOperationReasonWorkerLostV1
	}
	if known.Outcome != domain.AttemptOperationCompletedV1 || known.Reason != domain.AttemptOperationReasonNoneV1 || unknown.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || unknown.Reason != wantReason || unknown.ResponseRef != "" || unknown.ResponseDigest != "" || unknown.ResponseBytes != 0 || unknown.Usage.State != domain.AttemptOperationUsageUnknownV1 || unknown.Usage.InputTokens != nil || unknown.Usage.OutputTokens != nil || unknown.Usage.AmountMicrounits != nil {
		t.Fatal("actual lost reply was not persisted as request-only UNKNOWN with no invented response or measured usage")
	}
	tool := reservations[1]
	total, debitErr := reservations[0].AggregateDebit.Add(tool.Operation.Debit)
	if debitErr != nil || tool.Operation.ValidatePredecessor(reservations[0], known) != nil || total != tool.AggregateDebit || tool.AggregateDebit.Operations != 2 || tool.AggregateDebit.ModelCalls != 1 || tool.AggregateDebit.ToolDispatches != 1 || tool.AggregateDebit.MCPCatalogPages != 1 || tool.Operation.Debit.AmountMicrounits != *parent.Authority.AdmissionCostCeiling.MCPToolCost.MaxAmountMicrounits || tool.ComponentAmounts[2] != tool.Operation.Debit.AmountMicrounits {
		t.Fatal("actual UNKNOWN MCP lost exact predecessor or full nonrefundable reserved debit")
	}
	modelRequest := offlineChainBody(t, blobs.sessionCompactionBlobs, reservations[0].Operation.RequestRef, reservations[0].Operation.RequestDigest, reservations[0].Operation.RequestBytes)
	defer clear(modelRequest)
	modelResponse := offlineChainBody(t, blobs.sessionCompactionBlobs, known.ResponseRef, known.ResponseDigest, known.ResponseBytes)
	defer clear(modelResponse)
	bound, err := mcpadmission.BindConfiguredCallV1(profile, managerSeed.Grant, reservations[0], known, modelRequest, modelResponse)
	if err != nil || len(wires) != 0 && !bytes.Equal(wires[0].body, modelRequest) || bound.ID() != tool.Operation.CallID || bound.Name() != "search_openai_docs" || bound.ArgumentsDigest() != tool.Operation.ArgumentsDigest || bound.ModelResponseDigest() != tool.Operation.ModelResponseDigest || bound.GrantDigest() != tool.Operation.ToolGrantDigest {
		t.Fatal("actual durable MODEL/request/grant/normalized call did not admit this exact MCP reservation", err)
	}
	request := offlineChainBody(t, blobs.sessionCompactionBlobs, tool.Operation.RequestRef, tool.Operation.RequestDigest, tool.Operation.RequestBytes)
	defer clear(request)
	prepared, err := bound.Request()
	if err != nil {
		t.Fatal("original bound tools/call request", err)
	}
	preparedBody := prepared.Body()
	defer clear(preparedBody)
	mcp.mu.Lock()
	sameCall := len(mcp.bodies) == 4 && bytes.Equal(request, mcp.bodies[3])
	mcp.mu.Unlock()
	if !sameCall || !bytes.Equal(request, preparedBody) {
		t.Fatal("lost-reply peer did not receive exact persisted/reserved tools/call bytes")
	}
	blobs.mu.Lock()
	ordinaryKeys := 0
	for key := range blobs.objects {
		if strings.Contains(key, "/attempt-operations-v1/") {
			ordinaryKeys++
		}
	}
	blobs.mu.Unlock()
	stats := managerSeed.Stats()
	wantOrdinary := 3
	if options.taintedResponse {
		wantOrdinary = 4
	}
	originalAllocation, currentAllocation := managerSeed.TerminalAllocation(t)
	allocationDigest, allocationErr := originalAllocation.Digest()
	if ordinaryKeys != wantOrdinary || stats.Parents != 1 || stats.Burns != 1 || originalAllocation.ValidateInitial() != nil || allocationErr != nil || allocationDigest != tool.RegisteredAllocationDigest || seed.OriginalStorageDigest(t) != originalStorage {
		t.Fatalf("unknown outcome invented response/reservation or changed original funding: ordinary_keys=%d want=%d parents=%d burns=%d allocations=%d", ordinaryKeys, wantOrdinary, stats.Parents, stats.Burns, stats.Allocations)
	}
	t.Logf("causal frontier verified: one actual MODEL; exact MCP tools/call sent; original Store/issuer persisted contiguous completed MODEL + response-absent UNKNOWN MCP reason=%s; full debit; both transports joined", wantReason)
	if options.observationFault != "" || options.taintedResponse || options.commitFault == "rollback" {
		got := managerSeed.TerminalSnapshot(t)
		if runErr != nil || retryError == nil || outcome != worker.OutcomeRetried || q.ack != 0 || q.retry != 1 || q.wake != 0 || got.Run.Status != domain.RunRunning || got.Run.TerminalFailure != nil || got.Finalizations != 0 || len(got.Events) != 0 || blobs.deletes != 0 || blobs.absences != 0 {
			t.Fatalf("uncertain MCP frontier was salvaged into canonical failure: outcome=%s err=%v retry=%v", outcome, runErr, retryError)
		}
		if options.observationFault != "" && seed.MCPTailObservationFaults() == 0 || options.taintedResponse && tainted.uncertain != 1 || options.commitFault == "rollback" && got.LostACKs != 1 {
			t.Fatal("selected original observation/PUT/commit fault was not actually exercised")
		}
		puts := blobs.puts
		if again, err := manager.RunOnce(ctx); err != nil || again != worker.OutcomeRetried || blobs.puts != puts || model.completedCalls() != 1 || !reflect.DeepEqual(got, managerSeed.TerminalSnapshot(t)) {
			t.Fatal("reconciliation-only redelivery replayed an effect or terminal write", again, err)
		}
		return
	}

	// This is the decisive OLD/RED boundary: current production rejects this
	// original authenticated UNKNOWN MCP holder instead of projecting F3.
	got := managerSeed.TerminalSnapshot(t)
	if runErr != nil || retryError != nil || outcome != worker.OutcomeFailed || q.ack != 1 || q.retry != 0 || q.wake != 1 || got.Run.Status != domain.RunFailed || got.Attempt.Status != domain.AttemptFailed || got.Run.TerminalFailure == nil || len(got.Events) != 3 || got.Finalizations != 1 {
		t.Fatalf("actual persisted UNKNOWN MCP lacks truthful canonical failure: outcome=%s run_error=%v retry_error=%v events=%d finalizations=%d terminal_failure=%t ack=%d retry=%d wake=%d", outcome, runErr, retryError, len(got.Events), got.Finalizations, got.Run.TerminalFailure != nil, q.ack, q.retry, q.wake)
	}
	if got.Events[0].Kind != domain.SessionEventToolCall || got.Events[1].Kind != domain.SessionEventToolResult || got.Events[2].Kind != domain.SessionEventSystemNotice || got.Manifest.ID != "" || got.EvidenceRecord != "" || got.Run.TerminalFailure.Version != 1 || got.Run.TerminalFailure.CapsuleSchema != "sessionless.terminal-failure-custody.v3" || got.Run.TerminalFailure.Stop != "mcp_outcome_unknown" || !strings.HasPrefix(got.FinalDigest, "local_f1:") || blobs.eventWrites != 3 || blobs.capsulePuts != 1 || blobs.deletes != 0 || blobs.absences != 0 {
		t.Fatal("UNKNOWN MCP failure changed original F3/capsule/notice slots or invented success/erasure")
	}
	if stats.Allocations != 2 || currentAllocation.State != domain.RegisteredPreparationRetiredV1 || currentAllocation.ValidateTransitionFrom(originalAllocation) != nil {
		t.Fatal("UNKNOWN MCP canonical failure lacks exact original allocation registration/retirement")
	}
	if options.commitFault == "ambiguous" && got.LostACKs != 1 {
		t.Fatal("actual committed F3 lost ACK was not exactly observed")
	}
	rd, _ := tool.Digest()
	od, _ := unknown.Digest()
	call := []byte(`{"call_id":"offline-call-1","name":"search_openai_docs","arguments":{"query":"public docs"}}`)
	result, err := json.Marshal(struct {
		Schema      string                                     `json:"schema"`
		Outcome     string                                     `json:"outcome"`
		Operation   domain.AttemptOperationDigestV1            `json:"operation_digest"`
		Reservation domain.AttemptOperationReservationDigestV1 `json:"reservation_digest"`
		Observation domain.AttemptOperationObservationDigestV1 `json:"observation_digest"`
	}{"sessionless.tool-outcome.v1", "accepted_outcome_unknown", tool.OperationDigest, rd, od})
	if err != nil {
		t.Fatal("expected closed UNKNOWN outcome DATA", err)
	}
	defer clear(result)
	expected, err := canonicaloutput.EncodeCompletionV1(got.Run.ID, managerSeed.Loaded.Job.Limits, ports.ExecutionResult{ToolEvents: []ports.ExecutionToolEvent{{Kind: domain.SessionEventToolCall, CallID: bound.ID(), ToolName: bound.Name(), Payload: call}, {Kind: domain.SessionEventToolResult, CallID: bound.ID(), ToolName: bound.Name(), Payload: result}}}, "unused-success-manifest")
	if err != nil {
		t.Fatal("expected unchanged canonical tool envelope", err)
	}
	defer expected.Release()
	notice, err := canonicaloutput.EncodeFailureV1(got.Run.ID, false, "mcp_outcome_unknown")
	if err != nil {
		t.Fatal("expected single fixed failure notice", err)
	}
	defer clear(notice.Payload)
	wantEvents := append(expected.Events()[:2], notice)
	for i, event := range got.Events {
		blobs.mu.Lock()
		payload := bytes.Clone(blobs.objects[string(event.TenantID)+"\x00"+event.Payload.Key])
		blobs.mu.Unlock()
		digest := sha256.Sum256(payload)
		same := event.ID == wantEvents[i].ID && bytes.Equal(payload, wantEvents[i].Payload) && hex.EncodeToString(digest[:]) == event.Payload.SHA256
		clear(payload)
		if !same {
			t.Fatalf("UNKNOWN canonical event=%d differs from exact call/closed outcome/sanitized notice", i)
		}
	}
	puts := blobs.puts
	if again, err := manager.RunOnce(ctx); err != nil || again != worker.OutcomeDuplicate || blobs.puts != puts || model.completedCalls() != 1 || !reflect.DeepEqual(got, managerSeed.TerminalSnapshot(t)) {
		t.Fatal("committed UNKNOWN MCP duplicate replayed an effect or canonical write", again, err)
	}
	for _, mode := range []string{"positive", "missing_observation", "missing_parent", "changed_marker", "changed_accounting", "missing_body"} {
		if err := seed.CheckKnownFailureDeletion(t, managerSeed, blobs.sessionCompactionBlobs, mode); err != nil {
			t.Fatal("genuine F3 retained deletion", mode, err)
		}
	}
	if options.freshHistory {
		if options.historyFault == "" {
			runNextAfterKnownFailure(t, seed, managerSeed, blobs, h, cookie, csrf, model)
		} else {
			runNextAfterKnownFailure(t, seed, managerSeed, blobs, h, cookie, csrf, model, options.historyFault)
		}
	}
	if options.semanticRetry {
		runNextUnresolvedMCPRetry(t, seed, managerSeed, blobs, h, cookie, csrf, model, mcp)
	}
}

func runNextUnresolvedMCPRetry(t *testing.T, seed *ydbstore.LocalWebManagerSeed, prior *ydbstore.LocalCanonicalManagerSeed, oldBlobs *canonicalManagerBlobs, h http.Handler, cookie, csrf *http.Cookie, model *offlineModelChainPeer, oldMCP *offlineMCPPeer) {
	t.Helper()
	old := prior.TerminalSnapshot(t)
	seed.BeginNextAfterKnownFailure(t, prior)
	path := "/api/web/v1/sessions/" + string(seed.Session) + "/messages"
	response := webManagerRequest(h, http.MethodPost, path, `{"text":"explicit new request must not replay unresolved tool","idempotency_key":"new-run-same-unresolved-tool"}`, cookie, csrf)
	if response.Code != http.StatusCreated {
		t.Fatalf("fresh semantic-replay Web admission status=%d SQL=%s", response.Code, seed.LastError())
	}
	next := seed.AdmitManager(t)
	blobs := &canonicalManagerBlobs{canonicalTerminalBlobs: &canonicalTerminalBlobs{sessionCompactionBlobs: oldBlobs.sessionCompactionBlobs, owner: t, seed: next.LocalCanonicalTerminalSeed}}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	invocation, err := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: next.Prepare, Operate: next.Operate, Backend: next.Backend, Substrate: next.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := invocation.Close(); err != nil {
			t.Error("fresh semantic-replay original owner join", err)
		}
	})
	var lookups, dials atomic.Int32
	if err := invocation.ConfigureMCPOfflineNetwork(func(context.Context, string) ([]netip.Addr, error) {
		lookups.Add(1)
		return nil, errors.New("semantic replay reached forbidden offline MCP preflight")
	}, func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("semantic replay reached forbidden offline MCP dial")
	}, oldMCP.roots); err != nil {
		t.Fatal(err)
	}
	state, err := invocation.WorkerState()
	if err != nil {
		t.Fatal(err)
	}
	q := &canonicalManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "new-run-unresolved-delivery", Kind: queuecontract.KindDispatchRun, TenantID: seed.Tenant, SubjectID: string(next.Loaded.Run.ID), EnqueuedAt: next.Loaded.Job.CreatedAt}}}
	var retried error
	manager, err := worker.New(worker.Config{ScratchRoot: t.TempDir(), WorkerID: next.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, LocalOwnedSession: invocation, ProjectionWakePublisher: q, DeliveryWakePublisher: q, RetryObserver: func(err error) { retried = err }}, localManagerClock{}, q, state, blobs, nil)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := manager.RunOnce(ctx)
	wires := model.wait(t, false)
	reservations, observations := next.OperationReadback(t)
	if err != nil || retried == nil || outcome != worker.OutcomeRetried || q.ack != 0 || q.retry != 1 || q.wake != 0 || len(wires) != 2 || model.completedCalls() != 2 || len(reservations) != 1 || len(observations) != 1 || reservations[0].Operation.Kind != domain.AttemptOperationModelV1 || observations[0].Outcome != domain.AttemptOperationCompletedV1 || blobs.puts != 2 || blobs.eventWrites != 0 || blobs.capsulePuts != 0 || lookups.Load() != 0 || dials.Load() != 0 {
		t.Fatalf("fresh call/Run or whitespace replay bypassed original unresolved guard: outcome=%s err=%v retry=%v operations=%d puts=%d DNS=%d dial=%d", outcome, err, retried, len(reservations), blobs.puts, lookups.Load(), dials.Load())
	}
	if reservations[0].Operation.Scope.RunID == old.Run.ID {
		t.Fatal("semantic test reused the original Run rather than a genuine new admission")
	}
	var wire globalClosureWire
	var history globalClosureHistory
	if json.Unmarshal(wires[1].body, &wire) != nil || len(wire.Messages) != 2 || json.Unmarshal([]byte(wire.Messages[1].Content), &history) != nil {
		t.Fatal("fresh semantic-replay MODEL history grammar")
	}
	for _, event := range old.Events {
		found := 0
		for _, frame := range append(history.Protected, history.Units...) {
			if frame.EventID == string(event.ID) {
				found++
				if frame.Kind != "canonical" || frame.Role != string(event.Kind) || frame.Digest != event.Payload.SHA256 {
					t.Fatal("UNKNOWN effect became completed operation context")
				}
			}
		}
		if found != 1 {
			t.Fatal("fresh semantic guard dropped/duplicated the original UNKNOWN canonical fact")
		}
	}
}
