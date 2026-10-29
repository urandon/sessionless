//go:build sessionless_mcp_offline

package ydbstore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// The unchanged offline Make selector includes this NOAUTH canonical lineage.
// MODEL uses literal HTTP; MCP uses the existing instance-only DNS/dial/CA
// overlay and numeric TLS listener. Its synthetic RemoteAddr is TEST DATA,
// not real public DNS/IP/TLS endpoint qualification. No OS network state changes.
func TestLocalSessionCredentialedMCPOfflineCanonicalWorkerToolChain(t *testing.T) {
	var mu sync.Mutex
	var wires [][]byte
	total, invalid := 0, 0
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		total++
		index := total
		mu.Unlock()
		defer r.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
		var request struct {
			Model string `json:"model"`
		}
		if err != nil || len(raw) > 1<<20 || r.Method != "POST" || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || json.Unmarshal(raw, &request) != nil || request.Model == "" || index > 2 {
			mu.Lock()
			invalid++
			mu.Unlock()
			http.Error(w, "invalid synthetic MODEL request", 400)
			return
		}
		mu.Lock()
		wires = append(wires, bytes.Clone(raw))
		mu.Unlock()
		message := map[string]any{"role": "assistant", "content": "canonical tool connected final answer"}
		finish := "stop"
		if index == 1 {
			message["content"] = nil
			message["tool_calls"] = []any{map[string]any{"id": "offline-call-1", "type": "function", "function": map[string]any{"name": "search_openai_docs", "arguments": `{"query":"public docs"}`}}}
			finish = "tool_calls"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"model": request.Model, "choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finish}}, "usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
	}))
	server.Config.ReadHeaderTimeout = 5 * time.Second
	server.Config.ReadTimeout, server.Config.WriteTimeout = 10*time.Second, 10*time.Second
	server.Config.MaxHeaderBytes = 16 << 10
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
	server.Start()
	seed := ydbstore.NewLocalCanonicalToolSeed(t, server.URL+"/v1")
	profile, err := seed.Source.Configuration()
	if err != nil || profile.Auth != domain.ConfiguredInferenceAuthNoneV1 || profile.BaseURL != server.URL+"/v1" {
		t.Fatal("same original sealed noauth Source", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	blobs := newSessionCompactionBlobs()
	ref, err := blobs.Put(ctx, seed.Payload.TenantID, seed.Payload.Key, bytes.NewReader(seed.Body))
	if err != nil || ref != seed.Payload {
		t.Fatal("same committed canonical bytes", err)
	}
	session, err := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	})
	mcp := newOfflineMCPPeer(t, seed.Grant.Descriptor, 4, nil, false)
	if err := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err != nil {
		t.Fatal("inert original instance offline wiring", err)
	}
	if err := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err == nil {
		t.Fatal("offline wiring reset")
	}
	state, err := session.WorkerState()
	if err != nil {
		t.Fatal(err)
	}
	if err := session.PreflightWorkerJob(ctx, seed.Loaded); err != nil {
		t.Fatal(err)
	}
	claim := seed.Claim
	claim.Now = seed.Now()
	claim.ExpiresAt = claim.Now.Add(30 * time.Second)
	lease, err := state.ClaimWorkerLease(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.StartWorkerJob(ctx, seed.Loaded, lease, seed.Now()); err != nil {
		t.Fatal(err)
	}
	fresh, err := state.(ports.AttemptEffectStoreV1).ReserveAttemptEffect(ctx, ports.ReserveAttemptEffectRequestV1{TenantID: claim.TenantID, RunID: claim.RunID, AttemptID: claim.AttemptID, LeaseID: lease.ID, FenceToken: lease.FenceToken, PhysicalInvocationClaimID: "physical-canonical-tool-connected"})
	if err != nil || fresh.Status != ports.AttemptEffectOwnedV1 {
		t.Fatal("same genuine fresh parent", err)
	}
	parent, err := session.EnterWorkerEffect(ctx, fresh)
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.RunWorkerLoop(ctx)
	if err != nil || !result.Model.SendAttempted || result.Tool == nil || !result.Tool.SendAttempted || result.Compaction != nil {
		t.Fatalf("actual canonical MODEL/MCP/MODEL: err=%v sent=%t tool=%t lastSQL=%s", err, result.Model.SendAttempted, result.Tool != nil, seed.LastSQL())
	}
	mcp.wait(t)
	mu.Lock()
	captured := append([][]byte(nil), wires...)
	requests, bad := total, invalid
	mu.Unlock()
	if requests != 2 || bad != 0 || len(captured) != 2 {
		t.Fatal("not exactly two valid original MODEL wires")
	}
	reservations, observed := seed.OperationReadback(t)
	if len(reservations) != 3 || len(observed) != 3 || !seed.ExactThreeEffects() {
		t.Fatal("not exact three durable operations/calls/proxies and original admitted hold")
	}
	if !reflect.DeepEqual(result.Model.Observation.Reservation, reservations[2]) || !reflect.DeepEqual(result.Model.Observation.Observation, observed[2]) || seed.Operate.VerifyAttemptOperationObservation(result.Model.Observation) != nil || !reflect.DeepEqual(result.Tool.Observation.Reservation, reservations[1]) || !reflect.DeepEqual(result.Tool.Observation.Observation, observed[1]) || seed.Operate.VerifyAttemptOperationObservation(result.Tool.Observation) != nil {
		t.Fatal("returned operations not original authenticated durable observations")
	}
	for i, kind := range []domain.AttemptOperationKindV1{domain.AttemptOperationModelV1, domain.AttemptOperationMCPToolV1, domain.AttemptOperationModelV1} {
		r, o := reservations[i], observed[i]
		if r.Operation.Kind != kind || r.Operation.Sequence != uint64(i+1) || r.Operation.Scope != reservations[0].Operation.Scope || r.RegisteredAllocationDigest != reservations[0].RegisteredAllocationDigest || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 {
			t.Fatal("original durable scope/kind/allocation mismatch")
		}
		if i > 0 {
			pred, _ := observed[i-1].Digest()
			sum, e := reservations[i-1].AggregateDebit.Add(r.Operation.Debit)
			if e != nil || r.Operation.PredecessorObservationDigest != pred || sum != r.AggregateDebit || r.ReservedAt.Before(observed[i-1].ObservedAt) {
				t.Fatal("successor preceded original durable observation/full debit")
			}
		}
	}
	debit := reservations[2].AggregateDebit
	if debit.Operations != 3 || debit.ModelCalls != 2 || debit.ToolDispatches != 1 || debit.MCPCatalogPages != 1 {
		t.Fatal("original finite aggregate debit changed")
	}
	if observed[1].Usage.State != domain.AttemptOperationUsageUnknownV1 || observed[1].Usage.AmountMicrounits != nil || observed[1].Usage.InputTokens != nil || observed[1].Usage.OutputTokens != nil {
		t.Fatal("durable tool result fabricated measured usage")
	}
	for i, position := range []int{0, 2} {
		r, o := reservations[position], observed[position]
		request := offlineChainBody(t, blobs, r.Operation.RequestRef, r.Operation.RequestDigest, r.Operation.RequestBytes)
		if !bytes.Equal(request, captured[i]) {
			t.Fatal("durable original MODEL body differs from wire")
		}
		clear(request)
		response := offlineChainBody(t, blobs, o.ResponseRef, o.ResponseDigest, o.ResponseBytes)
		decoded, e := configuredinference.DecodeNormalizedResponseBodyV1(profile, captured[i], response, r.Operation.Kind)
		clear(response)
		if e != nil || (i == 0 && (decoded.Stop != configuredinference.StopToolV1 || decoded.Call == nil)) || (i == 1 && (decoded.Stop != configuredinference.StopFinalV1 || decoded.Text != "canonical tool connected final answer")) {
			t.Fatal("durable normalized original MODEL response", e)
		}
		var wire globalClosureWire
		var history globalClosureHistory
		if json.Unmarshal(captured[i], &wire) != nil || len(wire.Messages) != 2 || json.Unmarshal([]byte(wire.Messages[1].Content), &history) != nil {
			t.Fatal("actual original wire history")
		}
		found := 0
		for _, frame := range append(history.Protected, history.Units...) {
			if frame.EventID == string(seed.Event.ID) {
				found++
				if frame.Kind != "canonical" || frame.Sequence != seed.Event.Sequence || frame.Digest != seed.Payload.SHA256 || frame.Payload != string(seed.Body) {
					t.Fatal("original canonical event changed across tool continuation")
				}
			}
		}
		if found != 1 {
			t.Fatal("original canonical event missing or duplicated")
		}
		assertOfflinePairOnce(t, captured[i], i, string(claim.RunID))
	}
	request := offlineChainBody(t, blobs, reservations[1].Operation.RequestRef, reservations[1].Operation.RequestDigest, reservations[1].Operation.RequestBytes)
	response := offlineChainBody(t, blobs, observed[1].ResponseRef, observed[1].ResponseDigest, observed[1].ResponseBytes)
	defer clear(request)
	defer clear(response)
	tool, e := mcpadmission.DecodeSealedToolResultDataV1(reservations[1], observed[1], response)
	if e != nil || tool.IsError() || !reflect.DeepEqual(tool.Content(), []string{"offline public docs result"}) || tool.ModelResponseDigest() != observed[0].ResponseDigest {
		t.Fatal("original durable tool/MODEL1 link", e)
	}
	if _, e := mcpadmission.DecodeToolResultV1(seed.Grant, reservations[1].Operation, request, response); e != nil {
		t.Fatal("original installed grant/request/result", e)
	}
	mcp.mu.Lock()
	same := bytes.Equal(request, mcp.bodies[3])
	methods := append([]string(nil), mcp.methods...)
	mcp.mu.Unlock()
	if !same || !reflect.DeepEqual(methods, []string{"initialize", "notifications/initialized", "tools/list", "tools/call"}) {
		t.Fatal("actual four MCP frames changed")
	}
	blobs.mu.Lock()
	objects, ordinary := len(blobs.objects), 0
	for key := range blobs.objects {
		if strings.Contains(key, "/attempt-operations-v1/") {
			ordinary++
		}
	}
	blobs.mu.Unlock()
	if objects != 7 || ordinary != 6 {
		t.Fatal("not exactly original canonical plus six immutable request/response objects")
	}
	afterProfile, e := seed.Source.Configuration()
	if e != nil || !reflect.DeepEqual(profile, afterProfile) {
		t.Fatal("original sealed Source configuration changed")
	}
	if parent.Authority.Lease != lease || seed.Stats().Parents != 1 || seed.Stats().Burns != 1 || seed.Stats().Allocations != 1 || seed.MaxCallbackSQL() > 32 {
		t.Fatal("original claimed entry/SQL budget widened")
	}
	t.Logf("canonical MODEL/MCP/MODEL max callback SQL=%d", seed.MaxCallbackSQL())
	if _, err := session.RunWorkerLoop(ctx); err == nil {
		t.Fatal("original worker loop repeated")
	}
	mu.Lock()
	afterRequests := total
	mu.Unlock()
	if afterRequests != requests || !seed.ExactThreeEffects() {
		t.Fatal("duplicate loop acquired another effect")
	}
}
