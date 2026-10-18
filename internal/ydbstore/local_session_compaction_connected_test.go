package ydbstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Fake immutable STORAGE, not a body adapter or permission producer. Actual
// Source, original session, Store, issuer, bounded body custody and keeper are
// constructed by production factories. All fixture objects stay in memory.
type sessionCompactionBlobs struct {
	mu                            sync.Mutex
	objects                       map[string][]byte
	bytes                         uint64
	failRetained, corruptRetained bool
}

func newSessionCompactionBlobs() *sessionCompactionBlobs {
	return &sessionCompactionBlobs{objects: make(map[string][]byte)}
}
func (b *sessionCompactionBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	if ctx == nil || ctx.Err() != nil || tenant == "" || len(key) > 1024 {
		return domain.BlobRef{}, errors.New("bounded fake storage put denied")
	}
	raw, err := io.ReadAll(io.LimitReader(r, (1<<20)+1))
	if err != nil || len(raw) == 0 || len(raw) > 1<<20 {
		return domain.BlobRef{}, errors.New("bounded fake storage body denied")
	}
	sum := sha256.Sum256(raw)
	ref := domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(raw)), SHA256: hex.EncodeToString(sum[:])}
	if ref.Validate() != nil {
		return domain.BlobRef{}, errors.New("invalid fake storage reference")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failRetained && strings.Contains(key, "/derived-context-v1/") {
		return domain.BlobRef{}, errors.New("test retained PUT failed")
	}
	index := string(tenant) + "\x00" + key
	if old, exists := b.objects[index]; exists {
		if !bytes.Equal(old, raw) {
			return domain.BlobRef{}, errors.New("immutable storage replacement")
		}
		return ref, nil
	}
	if len(b.objects) >= 64 || b.bytes+uint64(len(raw)) > 4<<20 {
		return domain.BlobRef{}, errors.New("bounded fake storage inventory exhausted")
	}
	b.objects[index], b.bytes = bytes.Clone(raw), b.bytes+uint64(len(raw))
	return ref, nil
}
func (b *sessionCompactionBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if ctx == nil || ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil || ref.Size > 1<<20 {
		return nil, errors.New("bounded fake storage read denied")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	raw, found := b.objects[string(tenant)+"\x00"+ref.Key]
	if !found {
		return nil, errors.New("fake object missing")
	}
	copy := bytes.Clone(raw)
	if b.corruptRetained && strings.Contains(ref.Key, "/derived-context-v1/") && len(copy) > 0 {
		copy[0] ^= 1
	}
	return io.NopCloser(bytes.NewReader(copy)), nil
}
func (*sessionCompactionBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	return errors.New("unexpected deletion during live compaction step")
}

type sessionCompactionPeer struct {
	mu                                sync.Mutex
	kinds                             []string
	sizes                             []int
	failure                           string
	modelText                         string
	invalidSummary, partialGeneration bool
	waitForCancel                     bool
	generationStarted                 chan struct{}
}

func (p *sessionCompactionPeer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	var request struct {
		Model    string                           `json:"model"`
		Messages []struct{ Role, Content string } `json:"messages"`
	}
	if err != nil || len(raw) > 1<<20 || r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "" || json.Unmarshal(raw, &request) != nil || len(request.Messages) != 2 {
		p.mu.Lock()
		p.failure = "invalid actual request"
		p.mu.Unlock()
		http.Error(w, "invalid test request", http.StatusBadRequest)
		return
	}
	var history struct {
		Schema    string `json:"schema"`
		Protected []struct {
			SourceID string `json:"source_id"`
		} `json:"protected_originals"`
		Units []struct {
			SourceID string `json:"source_id"`
		} `json:"original_units"`
	}
	if json.Unmarshal([]byte(request.Messages[1].Content), &history) != nil {
		p.mu.Lock()
		p.failure = "invalid actual history"
		p.mu.Unlock()
		http.Error(w, "invalid history", http.StatusBadRequest)
		return
	}
	p.mu.Lock()
	p.kinds = append(p.kinds, history.Schema)
	p.sizes = append(p.sizes, len(raw))
	text := p.modelText
	invalidSummary, partialGeneration := p.invalidSummary, p.partialGeneration
	waitForCancel, generationStarted := p.waitForCancel, p.generationStarted
	p.mu.Unlock()
	if history.Schema == "sessionless.compaction-input.v1" {
		if waitForCancel {
			close(generationStarted)
			timer := time.NewTimer(10 * time.Second)
			defer timer.Stop()
			select {
			case <-r.Context().Done():
				return
			case <-timer.C:
				if r.Context().Err() != nil {
					return
				}
				p.mu.Lock()
				p.failure = "actual compaction socket did not close on cancellation"
				p.mu.Unlock()
				http.Error(w, "test cancellation timeout", http.StatusGatewayTimeout)
				return
			}
		}
		if len(history.Protected) == 0 || len(history.Units) == 0 {
			p.mu.Lock()
			p.failure = "missing actual compaction references"
			p.mu.Unlock()
			http.Error(w, "missing refs", http.StatusBadRequest)
			return
		}
		body, e := json.Marshal(map[string]any{"schema": "sessionless.compaction-summary.v1", "task": []any{map[string]any{"text": "Preserve original task and constraints", "source_ids": []string{history.Protected[0].SourceID}}}, "constraints": []any{}, "facts": []any{map[string]any{"text": "Completed original history", "source_ids": []string{history.Units[0].SourceID}}}, "unresolved": []any{}, "artifacts": []any{}, "omissions": []any{}})
		if e != nil {
			http.Error(w, "summary encode", http.StatusInternalServerError)
			return
		}
		text = string(body)
		if invalidSummary {
			text = `{"schema":"not-a-summary","task":[]}`
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if history.Schema == "sessionless.compaction-input.v1" && partialGeneration {
		w.Header().Set("Content-Length", "9999")
		_, _ = w.Write([]byte(`{"model":`))
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"model": request.Model, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text}, "finish_reason": "stop"}}})
}

func newCompactionFactorySession(t *testing.T, text string, limits ...ydbstore.LocalCompactionConnectedLimits) (*providerloopsession.LocalV1, *ydbstore.LocalSessionConnectedSeed, *sessionCompactionPeer, *sessionCompactionBlobs) {
	t.Helper()
	peer := &sessionCompactionPeer{modelText: text}
	server := httptest.NewUnstartedServer(peer)
	server.Config.ReadHeaderTimeout = 10 * time.Second
	server.Config.ReadTimeout = 20 * time.Second
	server.Config.WriteTimeout = 20 * time.Second
	server.Config.MaxHeaderBytes = 16 << 10
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
	server.Start()
	blobs := newSessionCompactionBlobs()
	seed := ydbstore.NewLocalCompactionSessionConnectedSeed(t, server.URL+"/v1", blobs, limits...)
	before := seed.Stats()
	session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil || seed.Stats() != before {
		t.Fatalf("original factory must be inert: %v", err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error("join original session", err)
		}
	})
	if _, err := session.Start(t.Context(), seed.Request); err != nil {
		t.Fatal("actual invocation entry", err)
	}
	return session, seed, peer, blobs
}

// Real local HTTP and production ownership; SCRIPTED SQL/storage/funding and
// synthetic installed calibration. No native YDB, real model or rollout proof.
func TestLocalSessionPreparedStepFittingFullViewNoCompaction(t *testing.T) {
	session, seed, peer, _ := newCompactionFactorySession(t, "bounded fake result")
	result, err := session.RunModelStep(t.Context())
	if err != nil || result.Compaction != nil || !result.Model.SendAttempted || result.Model.Observation.Observation.Outcome != domain.AttemptOperationCompletedV1 {
		t.Fatalf("actual full ordinary view must send without compaction: err=%v attempted=%t compaction=%t stats=%+v", err, result.Model.SendAttempted, result.Compaction != nil, seed.Stats())
	}
	if !reflect.DeepEqual(result.Model.Observation.Observation, seed.PersistedObservation(t)) || seed.Operate.VerifyAttemptOperationObservation(result.Model.Observation) != nil {
		t.Fatal("actual result is not the original authenticated durable observation")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.failure != "" || !reflect.DeepEqual(peer.kinds, []string{"sessionless.history.v1"}) {
		t.Fatalf("unexpected actual effects: kinds=%v failure=%s", peer.kinds, peer.failure)
	}
}

func TestLocalSessionPreparedStepTwoActualCompactions(t *testing.T) {
	session, seed, peer, blobs := newCompactionFactorySession(t, strings.Repeat("x", 4096), ydbstore.LocalCompactionConnectedLimits{InputTokens: 8192, NetworkOperation: 5 * time.Second})
	for step := 0; step < 3; step++ {
		result, err := session.RunModelStep(t.Context())
		if err != nil || !result.Model.SendAttempted || result.Model.Observation.Observation.Outcome != domain.AttemptOperationCompletedV1 {
			peer.mu.Lock()
			kinds, sizes, failure := append([]string(nil), peer.kinds...), append([]int(nil), peer.sizes...), peer.failure
			peer.mu.Unlock()
			t.Fatalf("step=%d actual compaction/model chain: err=%v attempted=%t compaction=%t stats=%+v kinds=%v sizes=%v peer=%s", step, err, result.Model.SendAttempted, result.Compaction != nil, seed.Stats(), kinds, sizes, failure)
		}
		if (result.Compaction != nil) != (step > 0) {
			t.Fatalf("step=%d unexpected compaction selection=%t", step, result.Compaction != nil)
		}
		if result.Compaction != nil && (result.Compaction.Observation.Observation.Outcome != domain.AttemptOperationCompletedV1 || !result.Compaction.SendAttempted) {
			t.Fatal("compaction lacks its own actual durable result")
		}
	}
	before := seed.Stats()
	blobs.mu.Lock()
	objects := len(blobs.objects)
	blobs.mu.Unlock()
	if result, err := session.RunModelStep(t.Context()); err == nil || result.Model.SendAttempted || result.Compaction != nil || seed.Stats() != before {
		t.Fatal("exhausted two-generation capacity acquired another effect", err)
	}
	blobs.mu.Lock()
	unchanged := len(blobs.objects) == objects
	blobs.mu.Unlock()
	if !unchanged {
		t.Fatal("third generation wrote new request or retained objects")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	want := []string{"sessionless.history.v1", "sessionless.compaction-input.v1", "sessionless.history.v1", "sessionless.compaction-input.v1", "sessionless.history.v1"}
	if peer.failure != "" || !reflect.DeepEqual(peer.kinds, want) {
		t.Fatalf("actual HTTP sequence=%v want=%v peer=%s", peer.kinds, want, peer.failure)
	}
	if stats := seed.Stats(); stats.Operation != 5 || stats.Observation != 5 || stats.Parent != 1 || stats.Burn != 1 || stats.Allocation != 1 {
		t.Fatalf("original ownership/accounting: %+v", stats)
	}
}

func TestLocalSessionPreparedStepFittingSuccessorNoCompaction(t *testing.T) {
	session, seed, peer, _ := newCompactionFactorySession(t, "short completed response")
	for sequence := uint64(1); sequence <= 2; sequence++ {
		result, err := session.RunModelStep(t.Context())
		if err != nil || result.Compaction != nil || !result.Model.SendAttempted || result.Model.Observation.Reservation.Operation.Sequence != sequence || result.Model.Observation.Observation.Outcome != domain.AttemptOperationCompletedV1 {
			t.Fatalf("original prepared MODEL successor sequence%d: err=%v attempted=%t compaction=%t stats=%+v", sequence, err, result.Model.SendAttempted, result.Compaction != nil, seed.Stats())
		}
	}
	before := seed.Stats()
	copySession := *session
	if _, err := copySession.InvokeFirstModel(t.Context(), domain.AttemptOperationV1{}, seed.Profile.Limits, sessionRequest()); err == nil || seed.Stats() != before {
		t.Fatal("prepared continuation permitted reentry through caller-authored legacy path")
	}
	if err := seed.Source.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := copySession.RunModelStep(t.Context()); err == nil || seed.Stats() != before {
		t.Fatal("source revocation permitted another prepared operation")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.failure != "" || !reflect.DeepEqual(peer.kinds, []string{"sessionless.history.v1", "sessionless.history.v1"}) || before.Operation != 2 || before.Observation != 2 || before.Parent != 1 || before.Allocation != 1 {
		t.Fatalf("same original session successor chain: kinds=%v stats=%+v failure=%s", peer.kinds, before, peer.failure)
	}
}

func TestLocalSessionPreparedStepFailedCompactionCannotSendSuccessor(t *testing.T) {
	for _, name := range []string{"invalid_summary", "retained_put", "corrupt_retained_read", "partial_reply"} {
		t.Run(name, func(t *testing.T) {
			session, seed, peer, blobs := newCompactionFactorySession(t, strings.Repeat("x", 4096), ydbstore.LocalCompactionConnectedLimits{InputTokens: 8192, NetworkOperation: 5 * time.Second})
			first, err := session.RunModelStep(t.Context())
			if err != nil || !first.Model.SendAttempted || first.Compaction != nil {
				t.Fatal("unchanged first MODEL positive", err)
			}
			peer.mu.Lock()
			peer.invalidSummary = name == "invalid_summary"
			peer.partialGeneration = name == "partial_reply"
			peer.mu.Unlock()
			blobs.mu.Lock()
			blobs.failRetained = name == "retained_put"
			blobs.corruptRetained = name == "corrupt_retained_read"
			blobs.mu.Unlock()
			result, err := session.RunModelStep(t.Context())
			if !errors.Is(err, providerloopsession.ErrIncomplete) || result.Model.SendAttempted || result.Compaction == nil || !result.Compaction.SendAttempted {
				t.Fatalf("failed compaction must deny actual successor: mode=%s err=%v model=%t compaction=%t stats=%+v", name, err, result.Model.SendAttempted, result.Compaction != nil, seed.Stats())
			}
			if name == "partial_reply" && result.Compaction.Observation.Observation.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 {
				t.Fatal("partial generation fabricated durable success")
			}
			before := seed.Stats()
			copySession := *session
			if _, err := copySession.RunModelStep(t.Context()); err == nil || seed.Stats() != before {
				t.Fatal("failed/ambiguous generation replayed through session copy")
			}
			if err := session.Close(); err != nil {
				t.Fatal("failed step original teardown", err)
			}
			peer.mu.Lock()
			defer peer.mu.Unlock()
			if peer.failure != "" || !reflect.DeepEqual(peer.kinds, []string{"sessionless.history.v1", "sessionless.compaction-input.v1"}) || before.Operation != 2 || before.Observation != 2 {
				t.Fatalf("failed compaction emitted extra HTTP or lost debit: modes=%v stats=%+v peer=%s", peer.kinds, before, peer.failure)
			}
		})
	}
}

func TestLocalSessionPreparedStepCancellationJoinsActualCompaction(t *testing.T) {
	session, seed, peer, _ := newCompactionFactorySession(t, strings.Repeat("x", 4096), ydbstore.LocalCompactionConnectedLimits{InputTokens: 8192, NetworkOperation: 5 * time.Second})
	if result, err := session.RunModelStep(t.Context()); err != nil || !result.Model.SendAttempted || result.Compaction != nil {
		t.Fatal("first ordinary positive", err)
	}
	started := make(chan struct{})
	peer.mu.Lock()
	peer.waitForCancel = true
	peer.generationStarted = started
	peer.mu.Unlock()
	call, cancel := context.WithCancel(t.Context())
	type outcome struct {
		result providerloopsession.ModelStepResultV1
		err    error
	}
	results := make(chan outcome, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-joined:
		case <-timer.C:
			select {
			case <-joined:
			default:
				t.Error("cancelled original model-step caller did not join")
			}
		}
	})
	go func() {
		defer close(joined)
		result, err := session.RunModelStep(call)
		results <- outcome{result, err}
	}()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-started:
	case <-timer.C:
		select {
		case <-started:
		default:
			t.Fatal("actual compaction request never reached owned peer")
		}
	}
	cancel() // observed actual request, not a timing guess before send.
	timer.Reset(10 * time.Second)
	var got outcome
	select {
	case got = <-results:
	case <-timer.C:
		select {
		case got = <-results:
		default:
			t.Fatal("original active compaction failed to join cancellation")
		}
	}
	if got.err == nil || got.result.Model.SendAttempted || got.result.Compaction == nil || !got.result.Compaction.SendAttempted || got.result.Compaction.Observation.Observation.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 {
		t.Fatalf("cancelled actual effect became success or successor: err=%v model=%t compaction=%t", got.err, got.result.Model.SendAttempted, got.result.Compaction != nil)
	}
	before := seed.Stats()
	copySession := *session
	if _, err := copySession.RunModelStep(t.Context()); err == nil || seed.Stats() != before {
		t.Fatal("cancelled original effect retried through copy")
	}
	if err := session.Close(); err != nil {
		t.Fatal("join cancelled session", err)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.failure != "" || !reflect.DeepEqual(peer.kinds, []string{"sessionless.history.v1", "sessionless.compaction-input.v1"}) {
		t.Fatalf("cancelled chain issued extra HTTP or failed socket join: kinds=%v failure=%s", peer.kinds, peer.failure)
	}
}
