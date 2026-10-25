package ydbstore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// assertInstalledModelTools pins the exact closed synthetic grant declared by
// localSourceMCPDeclaration. It accepts neither an arbitrary caller tool nor
// a catalog-only name match; any fixture contract change must update this wire
// expectation explicitly.
func assertInstalledModelTools(t *testing.T, got []json.RawMessage) {
	t.Helper()
	limits := mcpwire.LimitsV1{MessageBytes: 64 << 10, AggregateBytes: 256 << 10, JSONDepth: 32, JSONMembers: 4096,
		SSELineBytes: 64 << 10, SSEEventBytes: 64 << 10, SSEEvents: 128, Tools: 1, DescriptorBytes: 32 << 10,
		ArgumentBytes: 4096, ResultTextBytes: 4096, ContentItems: 16}
	descriptor, err := mcpwire.ParseDescriptorV1([]byte(`{"annotations":{"readOnlyHint":true},"description":"synthetic descriptor, not a live catalog","inputSchema":{"additionalProperties":false,"properties":{"query":{"maxLength":30,"minLength":1,"type":"string"}},"required":["query"],"type":"object"},"name":"search_openai_docs"}`), limits)
	if err != nil {
		t.Fatal("retained installed MCP descriptor", err)
	}
	if len(got) != 1 {
		t.Fatalf("model request must carry exactly the one retained installed tool, got %d", len(got))
	}
	var tool struct {
		Type     string `json:"type"`
		Function struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			Parameters  json.RawMessage `json:"parameters"`
		} `json:"function"`
	}
	if err := json.Unmarshal(got[0], &tool); err != nil || tool.Type != "function" || tool.Function.Name != descriptor.Name() || tool.Function.Description != "synthetic installed docs lookup" ||
		!bytes.Equal(bytes.TrimSpace(tool.Function.Parameters), bytes.TrimSpace(descriptor.InputSchema())) {
		t.Fatalf("model tool differs from exact retained MCP grant: type=%q name=%q description=%q schema=%s err=%v", tool.Type, tool.Function.Name, tool.Function.Description, tool.Function.Parameters, err)
	}
}

type v1ConsumerBlobs struct {
	*globalClosureBlobs
	bad     domain.BlobRef
	missing bool
}

func (b *v1ConsumerBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if b.bad.Key != "" && ref == b.bad {
		if b.missing {
			return nil, errors.New("original canonical payload absent")
		}
		return io.NopCloser(strings.NewReader(`{"schema":"sessionless.assistant-message.v1","summary":"changed","artifact_manifest_id":"changed"}`)), nil
	}
	return b.globalClosureBlobs.Open(ctx, tenant, ref)
}

func newV1ConsumerInstallation(t *testing.T) (*ydbstore.LocalTerminalPublicationSessionConnectedSeed, *ydbstore.ManagedV1CommittedHistory, *sessionCompactionBlobs, *globalClosureStorageAudit, *globalClosurePeer) {
	t.Helper()
	peer := &sessionCompactionPeer{modelText: "new original local consumer response"}
	recorder := &globalClosurePeer{peer: peer}
	server := httptest.NewUnstartedServer(recorder)
	server.Config.ReadHeaderTimeout = 10 * time.Second
	server.Config.ReadTimeout, server.Config.WriteTimeout, server.Config.MaxHeaderBytes = 20*time.Second, 20*time.Second, 16<<10
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		for _, wire := range recorder.wires {
			clear(wire)
		}
	})
	server.Start()
	raw := newSessionCompactionBlobs()
	audit := &globalClosureStorageAudit{reads: make(map[domain.BlobRef]int)}
	storage := newGlobalClosureBlobs(raw, audit)
	// This provides only actual owned installation files/receipt. Its fixture
	// Run is never started and its fabricated legacy history is never imported.
	installation := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, server.URL+"/v1", storage)
	if err := installation.Source.Close(); err != nil {
		t.Fatal(err)
	}
	committed := ydbstore.NewManagedV1CommittedHistory(t, "managed-"+strings.ToLower(rand.Text()), storage)
	return installation, committed, raw, audit, recorder
}

func assertV1ConsumerWire(t *testing.T, recorder *globalClosurePeer, seed *ydbstore.LocalTerminalPublicationSessionConnectedSeed, raw *sessionCompactionBlobs, wantCalls int) {
	t.Helper()
	recorder.mu.Lock()
	wires := append([][]byte(nil), recorder.wires...)
	recorder.mu.Unlock()
	if len(wires) != wantCalls {
		t.Fatalf("actual local MODEL calls=%d want%d", len(wires), wantCalls)
	}
	var request globalClosureWire
	decodeGlobalClosureJSON(t, wires[len(wires)-1], &request)
	if request.Model != seed.Profile.ModelID || request.Stream || request.N != 1 || request.MaxTokens != min(uint64(2048), seed.Profile.Limits.MaxOutputTokens) || len(request.Messages) != 2 || request.Messages[0].Role != "system" || request.Messages[1].Role != "user" {
		t.Fatal("actual fresh MODEL wire shape")
	}
	assertInstalledModelTools(t, request.Tools)
	var got globalClosureHistory
	decodeGlobalClosureJSON(t, []byte(request.Messages[1].Content), &got)
	want := globalClosureHistory{Schema: "sessionless.history.v1"}
	seen := make(map[string]bool)
	for _, event := range seed.CanonicalSourceEvents(t) {
		key, err := compactioncontext.CanonicalSourceKeyV1(event)
		if err != nil || seen[key] {
			t.Fatal("original canonical source identity", err)
		}
		seen[key] = true
		want.Units = append(want.Units, globalClosureSourceFrame(t, globalClosureSource{SourceID: key, Canonical: &event}, seed.Profile, nil, raw))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("V1/localA0 must remain exact ordered canonical material once, without operation provenance or derived summary")
	}
	peer := recorder.peer
	peer.mu.Lock()
	failure := peer.failure
	kinds := append([]string(nil), peer.kinds...)
	peer.mu.Unlock()
	if failure != "" || len(kinds) != wantCalls {
		t.Fatal("owned peer actual requests", failure, kinds)
	}
	for _, kind := range kinds {
		if kind != "sessionless.history.v1" {
			t.Fatal("V1 consumer unexpectedly compacted/replayed", kind)
		}
	}
}

func v1ConsumerSession(t *testing.T, seed *ydbstore.LocalTerminalPublicationSessionConnectedSeed, blobs ports.BlobStore, sql *ydbstore.LocalTerminalCompletionSQLFixture) *providerloopsession.LocalV1 {
	t.Helper()
	s, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal("new V1 consumer factory", err, sql.Trace())
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.Start(t.Context(), seed.Request); err != nil {
		t.Fatal("original V1 consumer Start", err, sql.Trace(), seed.Stats())
	}
	return s
}

// Public V1 execution/composer/finalizer -> committed archive -> NEW original
// local Source/Store/factory/Run -> actual MODEL. SQL/funding/blob transports
// remain cooperative fixtures, not native YDB/Manager or a real model profile.
func TestLocalSessionManagedV1FreshConsumerConnected(t *testing.T) {
	installation, committed, raw, audit, recorder := newV1ConsumerInstallation(t)
	for _, fault := range []string{"positive", "missing_marker", "corrupt_marker", "missing_manifest", "corrupt_manifest", "missing_payload", "corrupt_payload", "missing_head", "corrupt_head", "missing_evidence", "corrupt_evidence", "locator_attempt", "evidence_claim"} {
		t.Run(fault, func(t *testing.T) {
			storage := &v1ConsumerBlobs{globalClosureBlobs: newGlobalClosureBlobs(raw, audit)}
			seed, history, sql := ydbstore.NewFreshRunAfterManagedV1Seed(t, installation.LocalSessionConnectedSeed, committed, storage, "consumer-"+strings.ToLower(rand.Text()))
			storage.seed = seed
			assertDistinctGlobalClosureOwners(t, installation, seed)
			if fault == "missing_payload" || fault == "corrupt_payload" {
				for _, event := range seed.CanonicalSourceEvents(t) {
					if event.RunID != nil && *event.RunID == committed.Run().ID {
						storage.bad = event.Payload
					}
				}
				if storage.bad.Key == "" {
					t.Fatal("actual managed terminal payload required")
				}
				storage.missing = fault == "missing_payload"
			} else if fault != "positive" {
				history.CorruptManagedV1(t, committed.Run().ID, fault)
			}
			session := v1ConsumerSession(t, seed, storage, sql)
			before := seed.Stats()
			recorder.mu.Lock()
			calls := len(recorder.wires)
			recorder.mu.Unlock()
			audit.mu.Lock()
			puts := audit.puts
			audit.mu.Unlock()
			result, err := session.RunModelStep(t.Context())
			if history.TTLReads != 0 {
				t.Fatal("fresh consumer attempted old operational state", fault, history.TTLReads)
			}
			if fault == "positive" {
				if err != nil || !result.Model.SendAttempted || result.Compaction != nil || history.LegacyHeadReads == 0 || history.LegacyEvidenceReads == 0 {
					t.Fatal("genuine managed V1 must join non-nil evidence before actual MODEL", err, history.LegacyHeadReads, history.LegacyEvidenceReads)
				}
				assertV1ConsumerWire(t, recorder, seed, raw, calls+1)
				return
			}
			recorder.mu.Lock()
			afterCalls := len(recorder.wires)
			recorder.mu.Unlock()
			audit.mu.Lock()
			afterPuts := audit.puts
			audit.mu.Unlock()
			if err == nil || result.Model.SendAttempted || result.Compaction != nil || seed.Stats() != before || afterCalls != calls || afterPuts != puts {
				t.Fatal("damaged V1 history authorized new MODEL/reservation/PUT", fault, err, before, seed.Stats(), calls, afterCalls, puts, afterPuts)
			}
		})
	}
}

// Both formats are produced genuinely: managed V1 first, then one actual local
// A0 terminal/retirement/erasure/finalization, then another fresh local B. No
// V1 row is given local custody, and no local row falls back to V1 validation.
func TestLocalSessionMixedManagedV1LocalV2FreshConsumerConnected(t *testing.T) {
	installation, committed, raw, audit, recorder := newV1ConsumerInstallation(t)
	aBlobs := newGlobalClosureBlobs(raw, audit)
	aSeed, aHistory, aSQL := ydbstore.NewFreshRunAfterManagedV1Seed(t, installation.LocalSessionConnectedSeed, committed, aBlobs, "mixed-a-"+strings.ToLower(rand.Text()))
	aBlobs.seed = aSeed
	a := v1ConsumerSession(t, aSeed, aBlobs, aSQL)
	result, err := a.RunModelStep(t.Context())
	if err != nil || !result.Model.SendAttempted || result.Compaction != nil {
		t.Fatal("actual local A0 consumer", err)
	}
	assertV1ConsumerWire(t, recorder, aSeed, raw, 1)
	h, p := prepareTerminalPublication(t, a, aBlobs.terminalSessionBlobs)
	if err := h.PublishOutput(t.Context()); err != nil {
		t.Fatal(err)
	}
	erased, err := h.FinishCleanup(t.Context())
	if err != nil {
		t.Fatal("actual A0 retirement/erasure", err)
	}
	if err := erased.Complete(t.Context()); err != nil {
		t.Fatal("actual A0 canonical success", err, aSQL.Trace())
	}
	aState := aSQL.Snapshot(t)
	if aState.Finalizations != 1 || aState.Run.TerminalCustody == nil || aState.Run.DerivedContext != nil || len(aState.Events) != 1 {
		t.Fatal("actual local A0 finalization")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := aSeed.Source.Close(); err != nil {
		t.Fatal(err)
	}
	p.Clear()
	aBlobs.retainErasedScope(t, aState.Run)
	bBlobs := newGlobalClosureBlobs(raw, audit)
	bSeed, bHistory, bSQL := ydbstore.NewFreshRunAfterTerminalSeed(t, aSeed, aSQL, bBlobs, "mixed-b-"+strings.ToLower(rand.Text()), aHistory)
	bBlobs.seed = bSeed
	assertDistinctGlobalClosureOwners(t, installation, aSeed, bSeed)
	b := v1ConsumerSession(t, bSeed, bBlobs, bSQL)
	result, err = b.RunModelStep(t.Context())
	if err != nil || !result.Model.SendAttempted || result.Compaction != nil || bHistory.DetailReads[aState.Run.ID] == 0 || bHistory.LegacyHeadReads == 0 || bHistory.LegacyEvidenceReads == 0 || bHistory.TTLReads != 0 {
		t.Fatal("mixed original formats not independently rejoined before MODEL", err, bHistory.DetailReads, bHistory.LegacyHeadReads, bHistory.LegacyEvidenceReads, bHistory.TTLReads)
	}
	assertV1ConsumerWire(t, recorder, bSeed, raw, 2)
	audit.mu.Lock()
	forbidden := audit.forbiddenReads
	capsuleReads := audit.reads[aState.Run.TerminalCustody.Plan.Capsule]
	audit.mu.Unlock()
	if forbidden != 0 || capsuleReads == 0 {
		t.Fatal("mixed consumer used erased bodies or omitted local capsule", forbidden, capsuleReads)
	}
}
