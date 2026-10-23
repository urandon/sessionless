package ydbstore_test

import (
	"bytes"
	"context"
	"crypto/rand"
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

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// This peer records the actual frozen wires, not a separately rebuilt request.
// The existing owned peer supplies deterministic responses; no real model runs.
type globalClosurePeer struct {
	peer  *sessionCompactionPeer
	mu    sync.Mutex
	wires [][]byte
}

func (p *globalClosurePeer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	_ = r.Body.Close()
	p.mu.Lock()
	if err != nil || len(body) > 1<<20 || len(p.wires) >= 8 {
		p.mu.Unlock()
		http.Error(w, "bounded chain peer", http.StatusBadRequest)
		return
	}
	p.wires = append(p.wires, bytes.Clone(body))
	p.mu.Unlock()
	r.Body = io.NopCloser(bytes.NewReader(body))
	p.peer.ServeHTTP(w, r)
}

type globalClosureStorageAudit struct {
	mu                   sync.Mutex
	erased               []string
	reads                map[domain.BlobRef]int
	puts, forbiddenReads int
}

type globalClosureBlobs struct {
	*cleanupSessionBlobs
	audit *globalClosureStorageAudit
}

func newGlobalClosureBlobs(raw *sessionCompactionBlobs, audit *globalClosureStorageAudit) *globalClosureBlobs {
	return &globalClosureBlobs{cleanupSessionBlobs: &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: raw}}, audit: audit}
}

func (b *globalClosureBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	b.audit.mu.Lock()
	b.audit.puts++
	b.audit.mu.Unlock()
	return b.cleanupSessionBlobs.Put(ctx, tenant, key, body)
}

func (b *globalClosureBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	b.audit.mu.Lock()
	for _, prefix := range b.audit.erased {
		if strings.HasPrefix(ref.Key, prefix) && strings.Contains(ref.Key, "/attempt-operations-v1/") {
			b.audit.forbiddenReads++
			b.audit.mu.Unlock()
			return nil, errors.New("erased producing Run body unavailable")
		}
	}
	b.audit.reads[ref]++
	b.audit.mu.Unlock()
	return b.cleanupSessionBlobs.Open(ctx, tenant, ref)
}

func (b *globalClosureBlobs) retainErasedScope(t *testing.T, run domain.Run) {
	t.Helper()
	prefix := domain.SessionRunObjectPrefix(run.TenantID, run.SessionID, run.ID)
	b.mu.Lock()
	for key := range b.objects {
		if strings.HasPrefix(key, string(run.TenantID)+"\x00"+prefix) && strings.Contains(key, "/attempt-operations-v1/") {
			b.mu.Unlock()
			t.Fatal("actual cleanup left ordinary body", run.ID)
		}
	}
	b.mu.Unlock()
	b.audit.mu.Lock()
	b.audit.erased = append(b.audit.erased, prefix)
	b.audit.mu.Unlock()
}

type globalClosureFrame struct {
	Kind     string `json:"kind"`
	SourceID string `json:"source_id"`
	Role     string `json:"original_role"`
	EventID  string `json:"event_id,omitempty"`
	RunID    string `json:"run_id,omitempty"`
	Sequence uint64 `json:"sequence"`
	Payload  string `json:"original_payload"`
	Digest   string `json:"original_digest"`
}

type globalClosureWire struct {
	Model     string `json:"model"`
	Stream    bool   `json:"stream"`
	N         uint64 `json:"n"`
	MaxTokens uint64 `json:"max_tokens"`
	Messages  []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
	Tools []json.RawMessage `json:"tools,omitempty"`
}

type globalClosureHistory struct {
	Schema    string               `json:"schema"`
	Protected []globalClosureFrame `json:"protected_originals"`
	Units     []globalClosureFrame `json:"original_units"`
	Summary   string               `json:"derived_summary"`
}

type globalClosureOperation struct {
	Reservation domain.AttemptOperationReservationV1 `json:"reservation"`
	Observation domain.AttemptOperationObservationV1 `json:"observation"`
	Normalized  json.RawMessage                      `json:"normalized_body"`
	Projection  string                               `json:"projection"`
	EventIDs    []domain.SessionEventID              `json:"event_ids"`
}

type globalClosureCapsule struct {
	Schema         string                         `json:"schema"`
	Scope          domain.AttemptOperationScopeV1 `json:"scope"`
	ProfileDigest  string                         `json:"profile_digest"`
	CodecVersion   uint32                         `json:"codec_version"`
	FrontierDigest string                         `json:"frontier_digest"`
	Operations     []globalClosureOperation       `json:"operations"`
	Derived        *domain.RunDerivedContextV1    `json:"derived_context"`
	Events         []json.RawMessage              `json:"events"`
	Manifest       domain.ArtifactManifest        `json:"public_manifest"`
}

type globalClosureSource struct {
	SourceID  string               `json:"source_id"`
	Canonical *domain.SessionEvent `json:"canonical"`
	Operation *struct {
		Reservation domain.AttemptOperationReservationV1 `json:"reservation"`
		Observation domain.AttemptOperationObservationV1 `json:"observation"`
	} `json:"operation"`
}

type globalClosureManifest struct {
	Schema  string          `json:"schema"`
	Core    json.RawMessage `json:"core"`
	Summary domain.BlobRef  `json:"summary"`
	Cut     struct {
		Sources   []globalClosureSource `json:"sources"`
		Cut       uint32                `json:"cut"`
		Protected []uint32              `json:"protected"`
		Covered   []uint32              `json:"covered"`
		Suffix    []uint32              `json:"suffix"`
	} `json:"cut"`
	Reservation   domain.AttemptOperationReservationV1 `json:"reservation"`
	Observation   domain.AttemptOperationObservationV1 `json:"observation"`
	GenerationFit json.RawMessage                      `json:"generation_fit"`
	NextFit       json.RawMessage                      `json:"next_fit"`
	Parent        *domain.DerivedContextPlanV1         `json:"parent"`
	Closure       uint32                               `json:"closure"`
}

func decodeGlobalClosureJSON(t *testing.T, body []byte, value any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		t.Fatal("actual chain JSON", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		t.Fatal("chain JSON trailing data", err)
	}
}

func globalClosureBlob(t *testing.T, raw *sessionCompactionBlobs, ref domain.BlobRef) []byte {
	t.Helper()
	r, err := raw.Open(t.Context(), ref.TenantID, ref)
	if err != nil {
		t.Fatal("inspect actual retained object", ref.Key, err)
	}
	body, readErr := io.ReadAll(io.LimitReader(r, ref.Size+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || int64(len(body)) != ref.Size {
		t.Fatal("exact retained read", readErr, closeErr)
	}
	return body
}

func globalClosureSession(t *testing.T, seed *ydbstore.LocalTerminalPublicationSessionConnectedSeed, blobs ports.BlobStore) *providerloopsession.LocalV1 {
	t.Helper()
	s, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal("new original chain factory", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.Start(t.Context(), seed.Request); err != nil {
		t.Fatal("original chain Start", err)
	}
	return s
}

func completeGlobalClosureRun(t *testing.T, session *providerloopsession.LocalV1, seed *ydbstore.LocalTerminalPublicationSessionConnectedSeed, sql *ydbstore.LocalTerminalCompletionSQLFixture, blobs *globalClosureBlobs) ydbstore.LocalTerminalCompletionSnapshot {
	t.Helper()
	h, p := prepareTerminalPublication(t, session, blobs.terminalSessionBlobs)
	if err := h.PublishOutput(t.Context()); err != nil {
		t.Fatal("actual chain publication", err)
	}
	erased, err := h.FinishCleanup(t.Context())
	if err != nil {
		t.Fatal("actual chain stop/retire/erase", err)
	}
	if err := erased.Complete(t.Context()); err != nil {
		t.Fatal("actual chain canonical completion", err, sql.Trace())
	}
	v := sql.Snapshot(t)
	if v.Finalizations != 1 || v.Run.Status != domain.RunSucceeded || len(v.Events) != 1 || v.Run.TerminalCustody == nil || v.Run.DerivedContext == nil || len(v.Run.DerivedContext.Inventory) != 1 {
		t.Fatal("actual one-generation terminal postimage", v.Run.ID, v.Finalizations)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := seed.Source.Close(); err != nil {
		t.Fatal(err)
	}
	p.Clear()
	blobs.retainErasedScope(t, v.Run)
	return v
}

func assertDistinctGlobalClosureOwners(t *testing.T, seeds ...*ydbstore.LocalTerminalPublicationSessionConnectedSeed) {
	t.Helper()
	for i, a := range seeds {
		for _, b := range seeds[i+1:] {
			if a.Source == b.Source || a.DB == b.DB || a.Prepare == b.Prepare || a.Operate == b.Operate || a.Request.RunID == b.Request.RunID || a.Request.AttemptID == b.Request.AttemptID || a.Request.LeaseID == b.Request.LeaseID || a.Request.PhysicalInvocationClaimID == b.Request.PhysicalInvocationClaimID {
				t.Fatal("fresh original owner/identity was reused", a.Scope.RunID, b.Scope.RunID)
			}
		}
	}
}

// Original A1 -> original B generation1/global closure2 -> B terminal/erasure ->
// new original C consumer. Real Go Source/Store/keepers/HTTP/writers, SCRIPTED
// SQL/funding and fake blob storage; not native YDB/S3/model or rollout proof.
func TestLocalSessionGlobalClosureAcrossErasedRunsConnected(t *testing.T) {
	peer := &sessionCompactionPeer{modelText: strings.Repeat("a", 4096)}
	recorder := &globalClosurePeer{peer: peer}
	server := httptest.NewUnstartedServer(recorder)
	server.Config.ReadHeaderTimeout = 10 * time.Second
	server.Config.ReadTimeout, server.Config.WriteTimeout, server.Config.MaxHeaderBytes = 20*time.Second, 20*time.Second, 16<<10
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		for _, b := range recorder.wires {
			clear(b)
		}
	})
	server.Start()
	raw := newSessionCompactionBlobs()
	audit := &globalClosureStorageAudit{reads: make(map[domain.BlobRef]int)}
	aBlobs := newGlobalClosureBlobs(raw, audit)
	aSeed := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, server.URL+"/v1", aBlobs, ydbstore.LocalCompactionConnectedLimits{InputTokens: 8192, NetworkOperation: 5 * time.Second})
	aSQL := aSeed.InstallTerminalCompletionSQL(t)
	aBlobs.seed = aSeed
	a := globalClosureSession(t, aSeed, aBlobs)
	runSteps := func(s *providerloopsession.LocalV1, firstText, lastText string) {
		t.Helper()
		peer.mu.Lock()
		peer.modelText = firstText
		peer.mu.Unlock()
		first, err := s.RunModelStep(t.Context())
		if err != nil || !first.Model.SendAttempted || first.Compaction != nil || first.Model.Observation.Reservation.Operation.Sequence != 1 {
			t.Fatal("first MODEL must actually fit", err)
		}
		peer.mu.Lock()
		peer.modelText = lastText
		peer.mu.Unlock()
		second, err := s.RunModelStep(t.Context())
		if err != nil || !second.Model.SendAttempted || second.Compaction == nil || !second.Compaction.SendAttempted || second.Compaction.Observation.Reservation.Operation.Sequence != 2 || second.Model.Observation.Reservation.Operation.Sequence != 3 {
			t.Fatal("actual one compaction plus MODEL successor", err)
		}
	}
	runSteps(a, strings.Repeat("a", 4096), "exact A terminal answer")
	aState := completeGlobalClosureRun(t, a, aSeed, aSQL, aBlobs)
	name := "chain-" + strings.ToLower(rand.Text())
	bBlobs := newGlobalClosureBlobs(raw, audit)
	bSeed, bHistory, bSQL := ydbstore.NewFreshRunAfterTerminalSeed(t, aSeed, aSQL, bBlobs, name+"-b", nil)
	bBlobs.seed = bSeed
	assertDistinctGlobalClosureOwners(t, aSeed, bSeed)
	b := globalClosureSession(t, bSeed, bBlobs)
	runSteps(b, strings.Repeat("b", 4096), "exact B terminal answer")
	bState := completeGlobalClosureRun(t, b, bSeed, bSQL, bBlobs)
	if bHistory.TTLReads != 0 || bHistory.DetailReads[aState.Run.ID] == 0 {
		t.Fatal("B did not rejoin A original durable history", bHistory.TTLReads)
	}
	aPlan, bPlan := aState.Run.DerivedContext.Inventory[0], bState.Run.DerivedContext.Inventory[0]
	var aManifest, bManifest globalClosureManifest
	decodeGlobalClosureJSON(t, globalClosureBlob(t, raw, aPlan.Manifest), &aManifest)
	decodeGlobalClosureJSON(t, globalClosureBlob(t, raw, bPlan.Manifest), &bManifest)
	if aPlan.Generation != 1 || bPlan.Generation != 1 || aManifest.Closure != 1 || aManifest.Parent != nil || bManifest.Closure != 2 || bManifest.Parent == nil || *bManifest.Parent != aPlan || aPlan.Scope.RunID == bPlan.Scope.RunID || aManifest.Summary != aPlan.Summary || bManifest.Summary != bPlan.Summary {
		t.Fatal("local generation numbers must not reset the global closure")
	}
	var aCapsule, bCapsule globalClosureCapsule
	decodeGlobalClosureJSON(t, globalClosureBlob(t, raw, aState.Run.TerminalCustody.Plan.Capsule), &aCapsule)
	decodeGlobalClosureJSON(t, globalClosureBlob(t, raw, bState.Run.TerminalCustody.Plan.Capsule), &bCapsule)
	for _, v := range []globalClosureCapsule{aCapsule, bCapsule} {
		if v.Schema != "sessionless.terminal-custody.v1" || v.CodecVersion != 1 || len(v.Operations) != 3 || len(v.Events) != 1 || v.Derived == nil || len(v.Derived.Inventory) != 1 {
			t.Fatal("complete original per-Run capsule")
		}
		for i, op := range v.Operations {
			want := []string{"retained_only", "compaction", "assistant"}[i]
			if op.Reservation.Operation.Scope != v.Scope || op.Reservation.Operation.Sequence != uint64(i+1) || op.Projection != want || (i < 2 && len(op.EventIDs) != 0) || (i == 2 && len(op.EventIDs) != 1) {
				t.Fatal("capsule copied/misprojected another Run operation", i)
			}
		}
	}
	cBlobs := newGlobalClosureBlobs(raw, audit)
	cSeed, cHistory, _ := ydbstore.NewFreshRunAfterTerminalSeed(t, bSeed, bSQL, cBlobs, name+"-c", bHistory)
	cBlobs.seed = cSeed
	assertDistinctGlobalClosureOwners(t, aSeed, bSeed, cSeed)
	c := globalClosureSession(t, cSeed, cBlobs)
	peer.mu.Lock()
	peer.modelText = strings.Repeat("c", 4096)
	peer.mu.Unlock()
	result, err := c.RunModelStep(t.Context())
	if err != nil || !result.Model.SendAttempted || result.Compaction != nil || result.Model.Observation.Reservation.Operation.Sequence != 1 {
		t.Fatal("new C must consume complete A1/B1 closure before MODEL", err)
	}
	if cHistory.TTLReads != 0 || cHistory.DetailReads[aState.Run.ID] == 0 || cHistory.DetailReads[bState.Run.ID] == 0 {
		t.Fatal("C omitted original producing Run or used TTL state", cHistory.DetailReads, cHistory.TTLReads)
	}
	recorder.mu.Lock()
	wires := append([][]byte(nil), recorder.wires...)
	recorder.mu.Unlock()
	if len(wires) != 7 {
		t.Fatal("actual chain request count", len(wires), "want 7")
	}
	for i, wire := range wires {
		var req globalClosureWire
		decodeGlobalClosureJSON(t, wire, &req)
		if req.Model != cSeed.Profile.ModelID || req.Stream || req.N != 1 || req.MaxTokens == 0 || len(req.Messages) != 2 || req.Messages[0].Role != "system" || req.Messages[1].Role != "user" {
			t.Fatal("actual chain request framing", i)
		}
		var history globalClosureHistory
		decodeGlobalClosureJSON(t, []byte(req.Messages[1].Content), &history)
		want := "sessionless.history.v1"
		if i == 1 || i == 4 {
			want = "sessionless.compaction-input.v1"
		}
		if history.Schema != want {
			t.Fatal("actual call sequence", i, history.Schema, want)
		}
		if want == "sessionless.compaction-input.v1" {
			if len(req.Tools) != 0 {
				t.Fatal("compaction request must not expose tools", i)
			}
		} else {
			assertInstalledModelTools(t, req.Tools)
		}
		if i == 6 {
			assertGlobalClosureWire(t, history, bManifest, bPlan, []globalClosureCapsule{aCapsule, bCapsule}, bState, cSeed, raw)
		}
	}
	audit.mu.Lock()
	beforePuts := audit.puts
	forbidden := audit.forbiddenReads
	for _, ref := range []domain.BlobRef{aState.Run.TerminalCustody.Plan.Capsule, bState.Run.TerminalCustody.Plan.Capsule, aPlan.Summary, aPlan.Manifest, bPlan.Summary, bPlan.Manifest} {
		if audit.reads[ref] == 0 {
			audit.mu.Unlock()
			t.Fatal("retained dependency was never read by original runtime", ref.Key)
		}
	}
	audit.mu.Unlock()
	if forbidden != 0 {
		t.Fatal("new owner reopened erased original body", forbidden)
	}
	before := cSeed.Stats()
	denied, err := c.RunModelStep(t.Context())
	audit.mu.Lock()
	afterPuts, forbidden := audit.puts, audit.forbiddenReads
	audit.mu.Unlock()
	recorder.mu.Lock()
	afterCalls := len(recorder.wires)
	recorder.mu.Unlock()
	if err == nil || denied.Model.SendAttempted || denied.Compaction != nil || cSeed.Stats() != before || afterCalls != 7 || afterPuts != beforePuts || forbidden != 0 || cHistory.TTLReads != 0 || cSeed.TerminalPublicationRun(t).DerivedContext != nil {
		t.Fatal("global closure3 authorized new effect", err, before, cSeed.Stats(), beforePuts, afterPuts, afterCalls)
	}
	peer.mu.Lock()
	failure := peer.failure
	peer.mu.Unlock()
	if failure != "" {
		t.Fatal("owned peer rejected actual request", failure)
	}
}

func assertGlobalClosureWire(t *testing.T, actual globalClosureHistory, manifest globalClosureManifest, plan domain.DerivedContextPlanV1, capsules []globalClosureCapsule, b ydbstore.LocalTerminalCompletionSnapshot, c *ydbstore.LocalTerminalPublicationSessionConnectedSeed, raw *sessionCompactionBlobs) {
	t.Helper()
	var summary struct {
		Schema  string          `json:"schema"`
		Core    json.RawMessage `json:"core"`
		Summary json.RawMessage `json:"summary"`
	}
	decodeGlobalClosureJSON(t, globalClosureBlob(t, raw, plan.Summary), &summary)
	if actual.Summary != string(summary.Summary) {
		t.Fatal("C must use exact B1 summary, not concatenate or rewrite ancestors")
	}
	covered, protected := make(map[string]bool), make(map[string]bool)
	for _, i := range manifest.Cut.Covered {
		if int(i) >= len(manifest.Cut.Sources) {
			t.Fatal("retained covered index")
		}
		covered[manifest.Cut.Sources[i].SourceID] = true
	}
	for _, i := range manifest.Cut.Protected {
		if int(i) >= len(manifest.Cut.Sources) {
			t.Fatal("retained protected index")
		}
		protected[manifest.Cut.Sources[i].SourceID] = true
	}
	want := globalClosureHistory{Schema: "sessionless.history.v1", Summary: string(summary.Summary)}
	for _, source := range manifest.Cut.Sources {
		if covered[source.SourceID] {
			continue
		}
		frame := globalClosureSourceFrame(t, source, c.Profile, capsules, raw)
		if protected[source.SourceID] {
			want.Protected = append(want.Protected, frame)
		} else {
			want.Units = append(want.Units, frame)
		}
	}
	// The final B MODEL is projected only by its committed canonical anchor;
	// C's new user event follows it. Neither can be covered by earlier B1.
	anchor := b.Events[0]
	key, err := compactioncontext.CanonicalSourceKeyV1(anchor)
	if err != nil {
		t.Fatal(err)
	}
	want.Units = append(want.Units, globalClosureSourceFrame(t, globalClosureSource{SourceID: key, Canonical: &anchor}, c.Profile, capsules, raw))
	var user domain.SessionEvent
	current := c.TerminalPublicationRun(t)
	for _, event := range c.CanonicalSourceEvents(t) {
		if event.ID == current.TriggerEventID {
			if user.ID != "" {
				t.Fatal("duplicate C canonical ingress")
			}
			user = event
		}
	}
	if user.ID == "" || user.Kind != domain.SessionEventUserMessage || user.Sequence != anchor.Sequence+1 {
		t.Fatal("C original stored canonical ingress absent")
	}
	userKey, err := compactioncontext.CanonicalSourceKeyV1(user)
	if err != nil {
		t.Fatal(err)
	}
	want.Units = append(want.Units, globalClosureSourceFrame(t, globalClosureSource{SourceID: userKey, Canonical: &user}, c.Profile, capsules, raw))
	if !reflect.DeepEqual(actual, want) {
		t.Fatal("C exact ordered source partition differs from original B1 cut/canonical anchors")
	}
	seen := make(map[string]bool)
	for _, frame := range append(append([]globalClosureFrame(nil), actual.Protected...), actual.Units...) {
		if frame.SourceID == "" || seen[frame.SourceID] || covered[frame.SourceID] {
			t.Fatal("duplicate or covered raw frame", frame.SourceID)
		}
		seen[frame.SourceID] = true
	}
}

func globalClosureSourceFrame(t *testing.T, source globalClosureSource, profile domain.ConfiguredInferenceProfileV1, capsules []globalClosureCapsule, raw *sessionCompactionBlobs) globalClosureFrame {
	t.Helper()
	if source.Canonical != nil {
		e := source.Canonical
		key, err := compactioncontext.CanonicalSourceKeyV1(*e)
		if err != nil || key != source.SourceID {
			t.Fatal("canonical source identity", err)
		}
		frame := globalClosureFrame{Kind: "canonical", SourceID: key, Role: string(e.Kind), EventID: string(e.ID), Sequence: e.Sequence, Payload: string(globalClosureBlob(t, raw, e.Payload)), Digest: e.Payload.SHA256}
		if e.RunID != nil {
			frame.RunID = string(*e.RunID)
		}
		return frame
	}
	if source.Operation == nil {
		t.Fatal("retained source has no original arm")
	}
	r, o := source.Operation.Reservation, source.Operation.Observation
	key, err := compactioncontext.OperationSourceKeyV1(r, o)
	if err != nil || key != source.SourceID {
		t.Fatal("operation source identity", err)
	}
	for _, capsule := range capsules {
		for _, op := range capsule.Operations {
			if reflect.DeepEqual(op.Reservation, r) && reflect.DeepEqual(op.Observation, o) {
				if op.Projection != "retained_only" {
					t.Fatal("terminal MODEL or compaction duplicated as ordinary operation")
				}
				decoded, err := configuredinference.DecodeRetainedNormalizedDataV1(profile, r, o, op.Normalized)
				if err != nil {
					t.Fatal("original retained normalized operation", err)
				}
				return globalClosureFrame{Kind: "operation", SourceID: key, Role: "assistant_message", RunID: string(r.Operation.Scope.RunID), Sequence: r.Operation.Sequence, Payload: decoded.Text, Digest: o.ResponseDigest}
			}
		}
	}
	t.Fatal("retained source missing its original producing capsule")
	return globalClosureFrame{}
}
