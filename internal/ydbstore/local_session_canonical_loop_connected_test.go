package ydbstore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Genuine canonical/queued/facade lineage through the normal worker engine.
// SQL/object storage and this literal HTTP peer are synthetic DATA, not native
// YDB, a real model, web authentication, Manager or terminal completion proof.
func TestLocalSessionCanonicalWorkerLoopDurableModelConnected(t *testing.T) {
	for _, fault := range []string{"none", "corrupt original payload", "missing original payload"} {
		t.Run(fault, func(t *testing.T) { runCanonicalWorkerModel(t, fault) })
	}
}

func runCanonicalWorkerModel(t *testing.T, fault string) {
	peer := &canonicalModelHTTPPeer{}
	server := httptest.NewUnstartedServer(peer)
	server.Config.ReadHeaderTimeout = 5 * time.Second
	server.Config.ReadTimeout, server.Config.WriteTimeout = 10*time.Second, 10*time.Second
	server.Config.MaxHeaderBytes = 16 << 10
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
	server.Start()
	seed := ydbstore.NewLocalCanonicalLoopSeed(t, server.URL+"/v1")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	blobs := newSessionCompactionBlobs()
	ref, err := blobs.Put(ctx, seed.Payload.TenantID, seed.Payload.Key, bytes.NewReader(seed.Body))
	if err != nil || ref != seed.Payload {
		t.Fatal("original committed canonical body custody", err)
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
	effects := state.(ports.AttemptEffectStoreV1)
	fresh, err := effects.ReserveAttemptEffect(ctx, ports.ReserveAttemptEffectRequestV1{TenantID: claim.TenantID, RunID: claim.RunID, AttemptID: claim.AttemptID, LeaseID: lease.ID, FenceToken: lease.FenceToken, PhysicalInvocationClaimID: "physical-local-canonical-loop"})
	if err != nil || fresh.Status != ports.AttemptEffectOwnedV1 {
		t.Fatal("fresh original parent", err)
	}
	if _, err := session.EnterWorkerEffect(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if fault != "none" {
		blobs.mu.Lock()
		key := string(seed.Payload.TenantID) + "\x00" + seed.Payload.Key
		if fault == "missing original payload" {
			delete(blobs.objects, key)
		} else {
			blobs.objects[key][0] ^= 1
		}
		blobs.mu.Unlock()
	}
	result, err := session.RunWorkerLoop(ctx)
	if fault != "none" {
		calls, _, _ := peer.snapshot()
		_, _, durable := seed.DurableModel()
		if err == nil || result.Model.SendAttempted || calls != 0 || durable || !seed.ModelEffectsAbsent() {
			t.Fatal("invalid canonical body acquired a model effect")
		}
		if stats := seed.Stats(); stats.Parents != 1 || stats.Burns != 1 || stats.Allocations != 1 {
			t.Fatal("invalid body changed original parent")
		}
		return
	}
	if err != nil || !result.Model.SendAttempted || result.Model.Observation.Observation.Outcome != domain.AttemptOperationCompletedV1 || result.Tool != nil || result.Compaction != nil {
		t.Fatalf("normal worker durable model: err=%v sent=%t", err, result.Model.SendAttempted)
	}
	r, o, ok := seed.DurableModel()
	if !ok || r.Operation.Kind != domain.AttemptOperationModelV1 || !reflect.DeepEqual(o, result.Model.Observation.Observation) || seed.Operate.VerifyAttemptOperationObservation(result.Model.Observation) != nil {
		t.Fatal("durable observation not original persisted model")
	}
	calls, invalid, captured := peer.snapshot()
	if calls != 1 || invalid != 0 || len(captured) != 1 || !bytes.Contains(captured[0], []byte("public canonical local text")) {
		t.Fatal("normal model omitted original canonical payload or sent twice")
	}
	var wire globalClosureWire
	var history globalClosureHistory
	if json.Unmarshal(captured[0], &wire) != nil || len(wire.Messages) != 2 || json.Unmarshal([]byte(wire.Messages[1].Content), &history) != nil || history.Schema != "sessionless.history.v1" || len(history.Protected) != 0 || len(history.Units) != 1 || history.Summary != "" {
		t.Fatal("canonical-only original history framing changed")
	}
	frame := history.Units[0]
	if frame.Kind != "canonical" || frame.EventID != string(seed.Event.ID) || frame.Sequence != seed.Event.Sequence || frame.Role != string(seed.Event.Kind) || frame.Digest != seed.Payload.SHA256 || frame.Payload != string(seed.Body) {
		t.Fatal("normal worker wire changed the actual committed event or raw payload")
	}
	adapter, err := operationbody.NewV1(r.Operation.Scope, seed.Loaded.Job.HarnessBinding.AgentLoopPolicy.Limits, blobs)
	if err != nil {
		t.Fatal(err)
	}
	body, err := adapter.ReadAttemptOperationBody(ctx, r.Operation.Scope, o.ResponseRef, o.ResponseBytes)
	if err != nil {
		t.Fatal("durable normalized body absent", err)
	}
	profile, err := seed.Source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(profile, captured[0], body, r.Operation.Kind)
	clear(body)
	if err != nil || decoded.Text != "canonical connected durable answer" || decoded.Stop != configuredinference.StopFinalV1 {
		t.Fatal("durable answer differs from actual controlled response", err)
	}
	if seed.MaxCallbackSQL() > 32 {
		t.Fatal("original SQL callback budget widened")
	}
	t.Logf("same canonical worker durable model: max callback SQL=%d", seed.MaxCallbackSQL())
	if stats := seed.Stats(); stats.Parents != 1 || stats.Burns != 1 || stats.Allocations != 1 {
		t.Fatalf("worker loop changed original entry: %+v", stats)
	}
	if _, err := session.RunWorkerLoop(ctx); err == nil {
		t.Fatal("normal worker loop entered twice")
	}
	afterCalls, afterInvalid, afterBodies := peer.snapshot()
	_, _, stillOriginal := seed.DurableModel()
	if afterCalls != calls || afterInvalid != invalid || len(afterBodies) != len(captured) || !stillOriginal {
		t.Fatal("duplicate worker loop acquired another effect")
	}
}

// Total entries and invalid requests are distinct from valid wire bodies.
// This handler is shared by the product scenario and separate in-memory oracle
// controls; invalid probes never enter or reset a product scenario's counters.
type canonicalModelHTTPPeer struct {
	mu             sync.Mutex
	total, invalid int
	bodies         [][]byte
}

func (p *canonicalModelHTTPPeer) snapshot() (int, int, [][]byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.total, p.invalid, append([][]byte(nil), p.bodies...)
}

func (p *canonicalModelHTTPPeer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	p.total++
	p.mu.Unlock()
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	var request struct {
		Model string `json:"model"`
	}
	if err != nil || len(raw) > 1<<20 || r.Method != "POST" || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || json.Unmarshal(raw, &request) != nil || request.Model == "" {
		p.mu.Lock()
		p.invalid++
		p.mu.Unlock()
		http.Error(w, "invalid synthetic request", 400)
		return
	}
	p.mu.Lock()
	p.bodies = append(p.bodies, bytes.Clone(raw))
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"model": request.Model, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": "canonical connected durable answer"}, "finish_reason": "stop"}}, "usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
}

type canonicalModelReadFailure struct{}

func (canonicalModelReadFailure) Read([]byte) (int, error) {
	return 0, errors.New("synthetic read failure")
}
func (canonicalModelReadFailure) Close() error { return nil }

func TestLocalSessionCanonicalModelHTTPOracleCountsInvalidEntries(t *testing.T) {
	for _, fault := range []string{"method", "path", "authorization", "cookie", "malformed", "oversize", "read failure"} {
		t.Run(fault, func(t *testing.T) {
			p := &canonicalModelHTTPPeer{}
			r := httptest.NewRequest("POST", "http://127.0.0.1/v1/chat/completions", bytes.NewBufferString(`{"model":"synthetic"}`))
			switch fault {
			case "method":
				r.Method = "GET"
			case "path":
				r.URL.Path = "/wrong"
			case "authorization":
				r.Header.Set("Authorization", "synthetic")
			case "cookie":
				r.Header.Set("Cookie", "synthetic")
			case "malformed":
				r.Body = io.NopCloser(bytes.NewBufferString(`{"model":`))
			case "oversize":
				r.Body = io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("x"), (1<<20)+1)))
			case "read failure":
				r.Body = canonicalModelReadFailure{}
			}
			w := httptest.NewRecorder()
			p.ServeHTTP(w, r)
			total, invalid, bodies := p.snapshot()
			if w.Code != 400 || total != 1 || invalid != 1 || len(bodies) != 0 {
				t.Fatalf("invalid entry invisible: total=%d invalid=%d valid=%d status=%d", total, invalid, len(bodies), w.Code)
			}
		})
	}
}
