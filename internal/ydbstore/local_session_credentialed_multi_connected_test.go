package ydbstore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// This is a bounded local scripted model, not model/MCP qualification. It sees
// real original keeper HTTP wires and never supplies a request or capability.
type credentialedMultiPeer struct {
	mu      sync.Mutex
	wires   [][]byte
	kinds   []string
	failure string
}

func (p *credentialedMultiPeer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	var request struct {
		Model    string                           `json:"model"`
		Messages []struct{ Role, Content string } `json:"messages"`
		Tools    []json.RawMessage                `json:"tools"`
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
	valid := e == nil && len(raw) <= 1<<20 && r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions" && r.Header.Get("Authorization") == "Bearer "+localSessionFixtureCredential && json.Unmarshal(raw, &request) == nil && len(request.Messages) == 2 && json.Unmarshal([]byte(request.Messages[1].Content), &history) == nil
	p.mu.Lock()
	index := len(p.wires)
	if !valid || index >= 2 || (index == 0 && (history.Schema != "sessionless.compaction-input.v1" || len(request.Tools) != 0 || len(history.Protected) == 0 || len(history.Units) == 0)) || (index == 1 && (history.Schema != "sessionless.history.v1" || len(request.Tools) != 1)) {
		p.failure = "unexpected bounded authenticated multi-operation wire"
		p.mu.Unlock()
		http.Error(w, "invalid scripted wire", http.StatusBadRequest)
		return
	}
	p.wires = append(p.wires, bytes.Clone(raw))
	p.kinds = append(p.kinds, history.Schema)
	p.mu.Unlock()
	text := "final answer after original compaction"
	if index == 0 {
		summary, e := json.Marshal(map[string]any{"schema": "sessionless.compaction-summary.v1", "task": []any{map[string]any{"text": "Preserve original task and constraints", "source_ids": []string{history.Protected[0].SourceID}}}, "constraints": []any{}, "facts": []any{map[string]any{"text": "Completed original history", "source_ids": []string{history.Units[0].SourceID}}}, "unresolved": []any{}, "artifacts": []any{}, "omissions": []any{}})
		if e != nil {
			http.Error(w, "summary", 500)
			return
		}
		text = string(summary)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"model": request.Model, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text}, "finish_reason": "stop"}}, "usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
}

func credentialedActualFrozen(t *testing.T, p domain.ConfiguredInferenceProfileV1, raw []byte) *configuredinference.FrozenRequestV1 {
	t.Helper()
	var wire struct {
		Model     string                           `json:"model"`
		MaxTokens uint64                           `json:"max_tokens"`
		Messages  []struct{ Role, Content string } `json:"messages"`
		Tools     []struct {
			Type     string `json:"type"`
			Function struct {
				Name, Description string
				Parameters        json.RawMessage `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
	}
	if json.Unmarshal(raw, &wire) != nil || wire.Model != p.ModelID {
		t.Fatal("actual received model wire")
	}
	request := configuredinference.RequestV1{MaxOutputTokens: wire.MaxTokens}
	for _, m := range wire.Messages {
		request.Messages = append(request.Messages, configuredinference.MessageV1{Role: configuredinference.RoleV1(m.Role), Text: m.Content})
	}
	for _, tool := range wire.Tools {
		if tool.Type != "function" {
			t.Fatal("actual native tool type")
		}
		request.Tools = append(request.Tools, configuredinference.ToolV1{Name: tool.Function.Name, Description: tool.Function.Description, Parameters: bytes.Clone(tool.Function.Parameters)})
	}
	f, e := configuredinference.FreezeRequestV1(p, p.Limits, request)
	if e != nil || !bytes.Equal(f.Body(), raw) {
		t.Fatal("actual wire did not roundtrip through original fixed codec", e)
	}
	return f
}

func TestLocalSessionCredentialedConnectedCompactionThenModelOneMaterialization(t *testing.T) {
	peer := &credentialedMultiPeer{}
	closed := make(chan struct{}, 4)
	server := httptest.NewUnstartedServer(peer)
	server.Config.ReadHeaderTimeout = 10 * time.Second
	server.Config.ReadTimeout = 20 * time.Second
	server.Config.WriteTimeout = 20 * time.Second
	server.Config.MaxHeaderBytes = 16 << 10
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			select {
			case closed <- struct{}{}:
			default:
			}
		}
	}
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
		peer.mu.Lock()
		defer peer.mu.Unlock()
		for _, body := range peer.wires {
			clear(body)
		}
	})
	server.Start()
	description, historical := strings.Repeat("d", 4096), strings.Repeat("h", 4096)
	endpoint := server.URL + "/v1"
	// Measure once using the exact installed inputs and original Source's
	// retained calibration; this creates NO runtime operation or credential use.
	measure, measureBlobs, measureCredentials, _, _ := newCredentialedConnectedSessionConfiguredAt(t, endpoint, historical, ydbstore.LocalCompactionConnectedLimits{MCPModelDescription: description, NetworkOperation: 5 * time.Second})
	beforeMeasure := measure.Stats()
	ordinary, generation, rebuilt := measure.LocalCompactionFixtureRequests(t, measureBlobs)
	ceilings := configuredinference.ContextFitCeilingsV1{InputTokens: measure.Profile.Limits.MaxInputTokens, OutputTokens: measure.Profile.Limits.MaxOutputTokens, TotalTokens: measure.Profile.ModelWindowTokens}
	ordinaryFit, e := measure.Source.FitContext(ordinary, ceilings)
	if e != nil {
		t.Fatal("measure full original ordinary DATA", e)
	}
	generationFit, e := measure.Source.FitContext(generation, ceilings)
	if e != nil {
		t.Fatal("measure original summarizer DATA", e)
	}
	rebuiltFit, e := measure.Source.FitContext(rebuilt, ceilings)
	if e != nil {
		t.Fatal("measure original rebuilt DATA", e)
	}
	// Pinned after the original Source measurement, not a search or widening
	// of any installed ceiling. A changed fixture must be deliberately remeasured.
	const input uint64 = 8884
	if input != max(generationFit.InputUpperBound, rebuiltFit.InputUpperBound)+64 {
		t.Fatalf("pinned pre-intake fixture must be remeasured: summary=%d rebuilt=%d chosen=%d", generationFit.InputUpperBound, rebuiltFit.InputUpperBound, input)
	}
	if input >= ordinaryFit.InputUpperBound {
		t.Fatalf("fixture must have causal fit interval: generation=%d rebuilt=%d chosen=%d ordinary=%d", generationFit.InputUpperBound, rebuiltFit.InputUpperBound, input, ordinaryFit.InputUpperBound)
	}
	if measure.Stats() != beforeMeasure || measureCredentials.issues != 0 || measureCredentials.materializations != 0 || measureCredentials.releases != 0 {
		t.Fatal("calibration DATA preparation produced runtime/credential effects")
	}
	// Fresh original intake: only narrow the declared ceiling before any seal.
	seed, blobs, credentials, secret, session := newCredentialedConnectedSessionConfiguredAt(t, endpoint, historical, ydbstore.LocalCompactionConnectedLimits{InputTokens: input, MCPModelDescription: description, NetworkOperation: 5 * time.Second})
	initial, summary, next := seed.LocalCompactionFixtureRequests(t, blobs)
	ceilings = configuredinference.ContextFitCeilingsV1{InputTokens: input, OutputTokens: seed.Profile.Limits.MaxOutputTokens, TotalTokens: seed.Profile.ModelWindowTokens}
	if _, e = seed.Source.FitContext(initial, ceilings); !errors.Is(e, configuredinference.ErrContextNoFit) {
		t.Fatal("fresh narrowed ordinary request must truthfully not fit", e)
	}
	for _, f := range []*configuredinference.FrozenRequestV1{summary, next} {
		if _, e = seed.Source.FitContext(f, ceilings); e != nil {
			t.Fatal("fresh summarizer/rebuilt request must truthfully fit", e)
		}
	}
	putsWithinScope := 0
	blobs.afterCompaction = func() {
		putsWithinScope++
		if credentials.issues != 1 || credentials.materializations != 1 || credentials.callbacks != 1 || credentials.materializeReturned || credentials.releases != 0 {
			t.Error("retained compaction publication escaped the one synchronous materialization")
		}
	}
	before := seed.Stats()
	result, terminal, e := session.Run(t.Context(), seed.Request)
	if e != nil || terminal == nil || result.Compaction == nil || !result.Compaction.SendAttempted || !result.Model.SendAttempted {
		t.Fatalf("one actual credentialed Run must compact then model: err=%v terminal=%t compaction=%t model=%t stats=%+v callback=%v", e, terminal != nil, result.Compaction != nil, result.Model.SendAttempted, seed.Stats(), credentials.callbackError)
	}
	if credentials.issues != 1 || credentials.materializations != 1 || credentials.callbacks != 1 || credentials.releases != 1 || !credentials.materializeReturned || !credentials.borrowedZeroed || !credentials.releaseSawMaterializeReturned || !credentials.releaseSawBorrowedZeroed || !credentials.releaseHasDeadline || putsWithinScope != 2 {
		t.Fatalf("one scope/clear/join/release ordering failed: issue=%d materialize=%d callback=%d release=%d retainedPuts=%d", credentials.issues, credentials.materializations, credentials.callbacks, credentials.releases, putsWithinScope)
	}
	if got := seed.Stats(); got.Parent != before.Parent+1 || got.Burn != before.Burn+1 || got.Allocation != before.Allocation+1 || got.Operation != before.Operation+2 || got.Call != before.Call+2 || got.Observation != before.Observation+2 {
		t.Fatalf("original Store cumulative effect counts: got=%+v before=%+v", got, before)
	}
	peer.mu.Lock()
	wires := make([][]byte, len(peer.wires))
	for i, body := range peer.wires {
		wires[i] = bytes.Clone(body)
	}
	kinds := append([]string(nil), peer.kinds...)
	failure := peer.failure
	peer.mu.Unlock()
	defer func() {
		for _, body := range wires {
			clear(body)
		}
	}()
	if failure != "" || !reflect.DeepEqual(kinds, []string{"sessionless.compaction-input.v1", "sessionless.history.v1"}) {
		t.Fatal("exact two authenticated owned sends", failure, kinds)
	}
	if len(wires[0]) != len(summary.Body()) || len(wires[1]) != len(next.Body()) {
		t.Fatalf("actual producer sizes differ from measured DATA: actual=(%d,%d) measured=(%d,%d)", len(wires[0]), len(wires[1]), len(summary.Body()), len(next.Body()))
	}
	compaction, model := result.Compaction.Observation, result.Model.Observation
	pred, _ := compaction.Observation.Digest()
	aggregate, e := compaction.Reservation.AggregateDebit.Add(model.Reservation.Operation.Debit)
	if e != nil || compaction.Reservation.Operation.Sequence != 1 || compaction.Reservation.Operation.Kind != domain.AttemptOperationCompactionV1 || model.Reservation.Operation.Sequence != 2 || model.Reservation.Operation.Kind != domain.AttemptOperationModelV1 || model.Reservation.Operation.PredecessorObservationDigest != pred || model.Reservation.Operation.Scope != compaction.Reservation.Operation.Scope || model.Reservation.AggregateDebit != aggregate || model.Reservation.RegisteredAllocationDigest != compaction.Reservation.RegisteredAllocationDigest || seed.Operate.VerifyAttemptOperationObservation(compaction) != nil || seed.Operate.VerifyAttemptOperationObservation(model) != nil || !reflect.DeepEqual(seed.PersistedObservation(t), model.Observation) {
		t.Fatal("actual authenticated operation chain/cumulative original budget was not preserved", e)
	}
	if observed := seed.PersistedCompactionObservations(t); !reflect.DeepEqual(observed, []domain.AttemptOperationObservationV1{compaction.Observation, model.Observation}) {
		t.Fatalf("both canonical operation observations must match their authenticated results: rows=%d", len(observed))
	}
	for i, body := range wires {
		f := credentialedActualFrozen(t, seed.Profile, body)
		fit, e := seed.Source.FitContext(f, ceilings)
		observation := []domain.AttemptOperationReservationV1{compaction.Reservation, model.Reservation}[i]
		if e != nil || fit.InputUpperBound > input || fit.InputUpperBound != observation.Operation.Debit.InputTokens || fit.OutputReserve != observation.Operation.Debit.OutputTokens {
			t.Fatalf("actual sent request%d did not use original bounded calibrated fit: fit=%+v err=%v", i, fit, e)
		}
	}
	actualNext := credentialedActualFrozen(t, seed.Profile, wires[1])
	manifests := 0
	blobs.history.mu.Lock()
	for _, body := range blobs.history.objects {
		var m struct {
			Schema string `json:"schema"`
			Core   struct {
				Next string `json:"next_request_digest"`
			} `json:"core"`
		}
		if json.Unmarshal(body, &m) == nil && m.Schema == "sessionless.compaction-manifest.v1" {
			manifests++
			if m.Core.Next != actualNext.Digest() {
				t.Error("actual MODEL differs from exact retained publication next-request digest")
			}
		}
	}
	blobs.history.mu.Unlock()
	if manifests != 1 {
		t.Fatalf("one retained original generation required, got=%d", manifests)
	}
	deadline, e := terminal.Deadline()
	if e != nil || !deadline.Equal(credentials.releaseDeadline) {
		t.Fatal("terminal/release cleanup deadline changed", e)
	}
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	for i := 0; i < 2; i++ {
		select {
		case <-closed:
		case <-ctx.Done():
			t.Fatal("actual owned HTTP connections did not close within original cleanup bound")
		}
	}
	if !bytes.Equal(secret, []byte(localSessionFixtureCredential)) {
		t.Fatal("fixture-owned secret changed instead of only borrowed material being cleared")
	}
	t.Logf("calibrated fit interval generation=%d rebuilt=%d <= chosen=%d < initial=%d; actual sends=2, credential lifecycle=1/1/1/1", generationFit.InputUpperBound, rebuiltFit.InputUpperBound, input, ordinaryFit.InputUpperBound)
}
