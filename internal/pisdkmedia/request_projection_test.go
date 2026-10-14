package pisdkmedia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func fixtureLedger(t *testing.T, l domain.AttachedPiSDKNativeLimitsV1) *codexexec.CanonicalMediaLedgerV1 {
	t.Helper()
	v, e := codexexec.NewCanonicalMediaLedgerV1(l)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func fixtureConfig() RequestDataV1 {
	limits := domain.AttachedPiSDKNativeLimitsV1{PreSendAllocationBytes: 64 << 20, ProcessMemoryBytes: 256 << 20, JSONNodes: 8 << 20, JSONDepth: 32, NativeRequestBytes: 1 << 20, RequestHeaderBytes: 4096, ResponseBodyBytes: 256 << 10, ResponseChunkBytes: 64 << 10, SSEEventBytes: 64 << 10, ResponseHeaderBytes: 8192, ResponseCopyWorkBytes: 4 << 20, ResponseAllocationBytes: 4 << 20}
	source := strings.Repeat("a", 64)
	return RequestDataV1{Profile: &domain.AttachedPiSDKNativeProfileV1{Sources: domain.AttachedPiSDKNativeSourcesV1{FinalSchemaDigest: source}, Model: domain.AttachedPiSDKNativeModelV1{ModelVendorID: "openai", ModelID: "synthetic", Endpoint: "https://synthetic.invalid/responses", MaxOutputTokens: 100}, Limits: limits},
		Model:        &ModelDataV1{API: "openai-responses", Provider: "openai", ID: "synthetic", Endpoint: "https://synthetic.invalid/responses", Vision: true, Reasoning: true, SupportsDeveloperRole: true, SupportsExplicitPromptCacheMode: true},
		SystemPrompt: "trusted <>&\u2028\u2029💬\n\t\"\\", MaxOutputTokens: 16, FinalSchema: []byte(`{"type":"object","properties":{"4294967295":{"type":"string"},"01":{"type":"string"},"4294967294":{"type":"string"},"__proto__":{"type":"string"},"answer":{"type":"string"},"artifacts":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"mime_type":{"type":"string"},"content":{"type":"string"}},"required":["name","content"],"additionalProperties":false}}},"required":["answer"],"additionalProperties":false}`),
		FinalSchemaSourceDigest: source, FinalSchemaName: "result", CredentialMaxBytes: 128, Headers: []HeaderDataV1{{"accept", "application/json"}, {"content-type", "application/json"}, {"user-agent", "Pi/synthetic (fixture)"}, {"x-stainless-lang", "js"}, {"x-stainless-package-version", "6.40.0"}, {"x-stainless-os", "MacOS"}, {"x-stainless-arch", "arm64"}, {"x-stainless-runtime", "node"}, {"x-stainless-runtime-version", "v24.19.0"}, {"x-stainless-retry-count", "0"}, {"x-stainless-timeout", "1"}}}
}
func fixtureBlob(key string, b []byte) domain.BlobRef {
	d := sha256.Sum256(b)
	return domain.BlobRef{TenantID: "tenant-body", Key: key, Size: int64(len(b)), SHA256: hex.EncodeToString(d[:])}
}
func fixtureImage(t *testing.T, mime string, w, h int) []byte {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.SetNRGBA(x, y, color.NRGBA{R: 200, G: 30, B: 80, A: 255})
		}
	}
	var out bytes.Buffer
	var e error
	if mime == "image/png" {
		e = png.Encode(&out, img)
	} else {
		e = jpeg.Encode(&out, img, &jpeg.Options{Quality: 85})
	}
	if e != nil {
		t.Fatal(e)
	}
	return out.Bytes()
}

// Complete synthetic original carrier, not production/native/source authority.
// Uses the real canonical snapshot/tail/association validators; no second parser.
func fixtureInput(t *testing.T, snapshot bool) (attachedworkerdaemon.InvocationIdentity, *attachedworkerdaemon.VerifiedAttachedInputV1) {
	t.Helper()
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	expires := now.Add(time.Hour)
	identity := attachedworkerdaemon.InvocationIdentity{TenantID: "tenant-body", OwnerUserID: "owner-body", WorkerID: "worker-body", RunID: "run-body", AttemptID: "attempt-body", LeaseID: "lease-body", FenceToken: 5}
	place := domain.ExecutionPlacementV2{Version: 2, Kind: domain.ExecutionPlacementAttachedWorker, FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: identity.OwnerUserID, WorkerID: identity.WorkerID, CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("8", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("9", 64))}
	pd, e := domain.ExecutionPlacementDigest(place)
	if e != nil {
		t.Fatal(e)
	}
	binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: identity.TenantID, OwnerUserID: identity.OwnerUserID, RunID: identity.RunID, AttemptID: identity.AttemptID,
		Backend:       domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendCodexExecV1, ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("1", 64), NativeProtocolVersion: "codex-jsonl.v1", BackendProfileDigest: strings.Repeat("2", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryFileV1},
		Resource:      domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-body", OwnerUserID: identity.OwnerUserID, Revision: 3, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 7},
		ModelVendorID: "openai", ModelID: "synthetic", InputDataClass: domain.ProviderDataPrivateV1, ProviderCatalogDigest: strings.Repeat("1", 64), ProviderRouteDigest: strings.Repeat("2", 64), PrivacyPolicyDigest: strings.Repeat("3", 64), CapabilityEvidenceDigest: strings.Repeat("4", 64), EffectivePolicyDigest: strings.Repeat("5", 64), ExecutionPlacementDigest: string(pd), EvidenceExpiresAt: &expires}
	job := domain.WorkerJob{TenantID: identity.TenantID, RunID: identity.RunID, AttemptID: identity.AttemptID, SessionID: "session-body", TriggerEventID: "event-body-4", ReservationID: "reservation-body", InputManifestID: "manifest-body", CredentialOwnerUserID: identity.OwnerUserID, ExecutionPlacementV2: place, HarnessBinding: binding, ContextWindow: &domain.SessionContextWindow{ThroughSequence: 4}, Limits: domain.ProductLimits{MaxTenantQueueDepth: 1, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 1, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 10, MaxArtifacts: 10, MaxToolEvents: 10, MaxToolEventBytes: 4096}}
	manifest := domain.ArtifactManifest{ID: job.InputManifestID, TenantID: job.TenantID, RunID: job.RunID, CreatedAt: now}
	proof := &sessioncontext.CanonicalProof{Input: domain.SessionContextInput{TenantID: job.TenantID, SessionID: job.SessionID}}
	bodies := [][]byte{[]byte(` {"huge":90071992547409931234567890123,"exponent":1.0000000000000000001e99} `), fixtureImage(t, "image/png", 3000, 1000), []byte("old quoted task\n"), fixtureImage(t, "image/jpeg", 24, 12)}
	names := []string{"same.json", "same.png", "same.json", "same.png"}
	mimes := []string{"application/json", "image/png", "text/plain", "image/jpeg"}
	var records []sessioncontext.EventPayload
	for seq := 1; seq <= 4; seq++ {
		id := domain.SessionEventID(fmt.Sprintf("event-body-%d", seq))
		event := domain.SessionEvent{ID: id, TenantID: job.TenantID, SessionID: job.SessionID, Sequence: uint64(seq), Kind: domain.SessionEventUserMessage, AuthorUserID: &job.CredentialOwnerUserID, RunID: &job.RunID, IdempotencyKey: domain.IdempotencyKey(fmt.Sprintf("body-key-%d", seq)), CreatedAt: now.Add(time.Duration(seq) * time.Second)}
		var payload []byte
		if seq == 3 {
			event.Kind = domain.SessionEventAssistantMessage
			event.AuthorUserID = nil
			payload = []byte(`{"schema":"sessionless.assistant-message.v1","summary":"historic assistant summary","artifact_manifest_id":"old-private-manifest"}`)
		} else {
			var refs []sessioncontext.AttachmentRef
			if seq <= 2 {
				for i := (seq - 1) * 2; i < seq*2; i++ {
					ref := sessioncontext.AttachmentRef{Name: names[i], MediaType: mimes[i], Blob: fixtureBlob(domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, id)+fmt.Sprintf("attachment-%d", i), bodies[i])}
					refs = append(refs, ref)
					qualified := ref
					qualified.Name = fmt.Sprintf("%020d-%02d-%s", seq, len(refs), ref.Name)
					proof.Attachments = append(proof.Attachments, sessioncontext.AttachmentPayload{AttachmentRef: qualified, Body: bodies[i]})
				}
			}
			text := fmt.Sprintf("history turn %d", seq)
			if seq == 4 {
				text = "newest sentinel"
			}
			payload, e = json.Marshal(struct {
				Version     uint32                         `json:"version"`
				Text        string                         `json:"text"`
				Attachments []sessioncontext.AttachmentRef `json:"attachments,omitempty"`
			}{1, text, refs})
			if e != nil {
				t.Fatal(e)
			}
		}
		event.Payload = fixtureBlob(domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, id)+"message.json", payload)
		records = append(records, sessioncontext.EventPayload{Event: event, Payload: payload})
	}
	if snapshot {
		compressed, history, e := sessioncontext.EncodeSnapshot(records[:2])
		if e != nil {
			t.Fatal(e)
		}
		version := uint64(1)
		proof.Input.Snapshot = &domain.SessionSnapshot{ID: "body-snapshot", TenantID: job.TenantID, SessionID: job.SessionID, Version: version, ThroughSequence: 2, EventCount: 2, FormatVersion: 1, Compression: "zstd", UncompressedSize: uint64(len(history)), Payload: fixtureBlob(domain.SessionSnapshotObjectKey(job.TenantID, job.SessionID, version), compressed), CreatedAt: now}
		proof.SnapshotBytes = compressed
		job.ContextWindow.AfterSequence = 2
		job.ContextWindow.SnapshotVersion = &version
		records = records[2:]
	}
	for _, r := range records {
		proof.Input.Events = append(proof.Input.Events, r.Event)
		proof.EventBodies = append(proof.EventBodies, r.Payload)
	}
	history, _, e := sessioncontext.ProjectCanonical(job, proof, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	digest, e := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if e != nil {
		t.Fatal(e)
	}
	fence, e := domain.NewAttachedWorkerFenceTokenV1(identity.TenantID, identity.OwnerUserID, identity.WorkerID, identity.RunID, identity.AttemptID, identity.LeaseID, identity.FenceToken)
	if e != nil {
		t.Fatal(e)
	}
	input := &attachedworkerdaemon.VerifiedAttachedInputV1{AcceptedAttempt: domain.AttachedWorkerAttemptV1{Version: 1, TenantID: identity.TenantID, OwnerUserID: identity.OwnerUserID, WorkerID: identity.WorkerID, ConnectionID: "connection-body", RunID: identity.RunID, AttemptID: identity.AttemptID, ReservationID: job.ReservationID, LeaseID: identity.LeaseID, LeaseGeneration: identity.FenceToken, FenceToken: fence, EnrollmentGeneration: 2, ConnectionGeneration: 3, ContextDigest: digest, CapabilityDigest: place.CapabilityDigest, PolicyDigest: place.PolicyDigest, State: domain.AttachedWorkerAttemptClaimed, PlatformAttemptSequence: 2, WorkerAttemptSequence: 1, CreatedAt: now, UpdatedAt: now, LeaseExpiresAt: expires, Revision: 7}, Job: job, Manifest: manifest, Context: history, CanonicalContext: proof}
	if e = input.ValidateFor(identity); e != nil {
		t.Fatalf("synthetic original fixture invalid: %v", e)
	}
	t.Cleanup(input.Clear)
	return identity, input
}
func TestCompleteProjectionSnapshotTailOriginalCohort(t *testing.T) {
	cfg := fixtureConfig()
	identity, tail := fixtureInput(t, false)
	_, snapshot := fixtureInput(t, true)
	a, e := CompileOriginalRequestV1(context.Background(), identity, tail, cfg, fixtureLedger(t, cfg.Profile.Limits))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(a.Clear)
	b, e := CompileOriginalRequestV1(context.Background(), identity, snapshot, cfg, fixtureLedger(t, cfg.Profile.Limits))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(b.Clear)
	if !bytes.Equal(a.Body(), b.Body()) {
		t.Fatal("snapshot/tail complete body differs")
	}
	body := a.Body()
	for _, text := range []string{"history turn 1", "history turn 2", "historic assistant summary", "newest sentinel", "90071992547409931234567890123", "1.0000000000000000001e99", "<>&  💬"} {
		if !bytes.Contains(body, []byte(text)) {
			t.Errorf("complete source content missing %q", text)
		}
	}
	if bytes.Contains(body, []byte("tenants/")) {
		t.Fatal("private storage coordinate leaked")
	}
	var native struct {
		Input []struct {
			Content json.RawMessage `json:"content"`
		} `json:"input"`
	}
	if json.Unmarshal(body, &native) != nil || len(native.Input) != 2 {
		t.Fatal("complete native array malformed")
	}
	var blocks []struct {
		Type, Text, Detail string
		ImageURL           string `json:"image_url"`
	}
	if json.Unmarshal(native.Input[1].Content, &blocks) != nil {
		t.Fatal("blocks")
	}
	images := 0
	for i, block := range blocks {
		if block.Type != "input_image" {
			continue
		}
		if i == 0 || blocks[i-1].Type != "input_text" || !strings.Contains(blocks[i-1].Text, "image_ordinal") {
			t.Fatal("image not adjacent to source label")
		}
		raw, e := base64.StdEncoding.DecodeString(block.ImageURL[strings.IndexByte(block.ImageURL, ',')+1:])
		if e != nil || !bytes.Equal(raw, tail.CanonicalContext.Attachments[images*2+1].Body) || block.Detail != "auto" {
			t.Fatal("original image transformed")
		}
		images++
	}
	if images != 2 {
		t.Fatalf("images=%d", images)
	}
	if a.Accounting().After.CopyBytes <= a.Accounting().Before.CopyBytes || a.Accounting().BodyNodes == 0 || a.Accounting().TypedBlocks < 8 {
		t.Fatal("full shared accounting missing")
	}
}
func TestCompleteProjectionEarlyAndLateBudgetBoundaries(t *testing.T) {
	cfg := fixtureConfig()
	identity, input := fixtureInput(t, false)
	tiny := cfg.Profile.Limits
	tiny.PreSendAllocationBytes = 1
	ledger := fixtureLedger(t, tiny)
	if p, e := CompileOriginalRequestV1(context.Background(), identity, input, cfg, ledger); e == nil || p != nil {
		t.Fatal("early prerequisite admitted")
	}
	if ledger.Accounting().CopyBytes != 0 {
		t.Fatal("failed metadata reservation unexpectedly charged")
	}
	p, e := CompileOriginalRequestV1(context.Background(), identity, input, cfg, fixtureLedger(t, cfg.Profile.Limits))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.Clear)
	limit := p.Accounting().After.CopyBytes - 1
	late := cfg.Profile.Limits
	late.PreSendAllocationBytes = limit
	ledger = fixtureLedger(t, late)
	if got, e := CompileOriginalRequestV1(context.Background(), identity, input, cfg, ledger); e == nil || got != nil {
		t.Fatal("combined first overflow admitted")
	}
	if ledger.Accounting().CopyBytes == 0 || ledger.Accounting().CopyBytes >= limit {
		t.Fatal("lawfully funded prerequisite work/refund boundary not retained")
	}
	good := fixtureLedger(t, cfg.Profile.Limits)
	restored, e := CompileOriginalRequestV1(context.Background(), identity, input, cfg, good)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(restored.Clear)
}
func TestCompleteProjectionOriginalSourceAndClosedConfigDenials(t *testing.T) {
	for _, name := range []string{"owner", "missing-schema", "model", "original-model", "floor", "header", "numeric-schema", "body-limit", "swapped-original"} {
		t.Run(name, func(t *testing.T) {
			cfg := fixtureConfig()
			id, input := fixtureInput(t, false)
			switch name {
			case "owner":
				id.OwnerUserID = "foreign"
			case "missing-schema":
				cfg.FinalSchema = nil
			case "model":
				cfg.Model.ID = "other"
			case "original-model":
				cfg.Model.ID = "other"
				cfg.Profile.Model.ModelID = "other"
			case "floor":
				cfg.MaxOutputTokens = 15
			case "header":
				cfg.Headers[0].Value = "text/event-stream"
			case "numeric-schema":
				cfg.FinalSchema = []byte(`{"minimum":0.1}`)
			case "body-limit":
				cfg.Profile.Limits.NativeRequestBytes = 1
			case "swapped-original":
				input.CanonicalContext.Attachments[1], input.CanonicalContext.Attachments[3] = input.CanonicalContext.Attachments[3], input.CanonicalContext.Attachments[1]
			}
			if p, e := CompileOriginalRequestV1(context.Background(), id, input, cfg, fixtureLedger(t, cfg.Profile.Limits)); e == nil || p != nil {
				t.Fatal("invalid original/config admitted")
			}
		})
	}
}

func TestCompleteProjectionJointResponseRemaindersAndMutation(t *testing.T) {
	cfg := fixtureConfig()
	id, input := fixtureInput(t, false)
	l := fixtureLedger(t, cfg.Profile.Limits)
	p, e := CompileOriginalRequestV1(context.Background(), id, input, cfg, l)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Clear()
	a := p.Accounting()
	limits := cfg.Profile.Limits
	o := limits.ResponseChunkBytes + 4*limits.ResponseHeaderBytes + 2*limits.ResponseBodyBytes + a.EchoBytes
	if a.ResponseAllocationBytes != min(limits.ResponseAllocationBytes-o, limits.ProcessMemoryBytes-limits.PreSendAllocationBytes-o) || a.ResponseCopyWorkBytes != limits.ResponseCopyWorkBytes-4*limits.ResponseHeaderBytes-2*limits.ResponseBodyBytes || a.ResponseJSONNodes != l.Remaining().Nodes || a.EchoNodes == 0 {
		t.Fatal("shared reduced response ledger differs")
	}
	m, e := p.OriginalMessages()
	if e != nil {
		t.Fatal(e)
	}
	m[0].Content[0].Text = "mutated"
	if _, e := p.OriginalMessages(); e == nil {
		t.Fatal("borrowed typed messages drift admitted")
	}
	charged := l.Accounting()
	p.Clear()
	if l.Accounting().CopyBytes != charged.CopyBytes || l.Accounting().Nodes != charged.Nodes {
		t.Fatal("clear refunded cumulative charges")
	}
	for _, name := range []string{"allocation", "copy", "nodes"} {
		t.Run(name, func(t *testing.T) {
			c := fixtureConfig()
			switch name {
			case "allocation":
				c.Profile.Limits.ResponseAllocationBytes = o
			case "copy":
				c.Profile.Limits.ResponseCopyWorkBytes = 4*limits.ResponseHeaderBytes + 4*limits.ResponseBodyBytes + limits.SSEEventBytes
			case "nodes":
				c.Profile.Limits.JSONNodes = a.After.Nodes
			}
			if got, e := CompileOriginalRequestV1(context.Background(), id, input, c, fixtureLedger(t, c.Profile.Limits)); e == nil || got != nil {
				t.Fatal("combined response first-over admitted")
			}
		})
	}
}

func TestCompleteProjectionConditionalSourceFields(t *testing.T) {
	for _, name := range []string{"off-default", "off-null", "effort", "cache-absent", "temperature"} {
		t.Run(name, func(t *testing.T) {
			cfg := fixtureConfig()
			id, input := fixtureInput(t, false)
			switch name {
			case "off-null":
				cfg.Model.ThinkingOffPresent = true
			case "effort":
				v := "low"
				cfg.ReasoningEffort = &v
			case "cache-absent":
				cfg.Model.SupportsExplicitPromptCacheMode = false
			case "temperature":
				v := float64(1)
				cfg.Temperature = &v
			}
			p, e := CompileOriginalRequestV1(context.Background(), id, input, cfg, fixtureLedger(t, cfg.Profile.Limits))
			if e != nil {
				t.Fatal(e)
			}
			defer p.Clear()
			b := string(p.Body())
			switch name {
			case "off-default":
				if !strings.Contains(b, `"reasoning":{"effort":"none"}`) {
					t.Fatal("source off default")
				}
			case "off-null":
				if strings.Contains(b, `"reasoning"`) {
					t.Fatal("explicit null became source default")
				}
			case "effort":
				if !strings.Contains(b, `"reasoning":{"effort":"low","summary":"auto"},"include":["reasoning.encrypted_content"]`) {
					t.Fatal("full source reasoning/include")
				}
			case "cache-absent":
				if strings.Contains(b, "prompt_cache_") {
					t.Fatal("undefined cache became explicit value")
				}
			case "temperature":
				if !strings.Contains(b, `"temperature":1`) {
					t.Fatal("explicit temperature lost")
				}
			}
		})
	}
}

func TestCompleteProjectionRetainedCallerCancellationAndOriginalMINs(t *testing.T) {
	cfg := fixtureConfig()
	id, input := fixtureInput(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	l := fixtureLedger(t, cfg.Profile.Limits)
	if p, e := CompileOriginalRequestV1(ctx, id, input, cfg, l); e == nil || p != nil || l.Accounting() != (codexexec.CanonicalMediaAccountingV1{}) {
		t.Fatal("cancelled original context did work")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	p, e := CompileOriginalRequestV1(ctx, id, input, cfg, fixtureLedger(t, cfg.Profile.Limits))
	if e != nil {
		t.Fatal(e)
	}
	defer p.Clear()
	cancel()
	if _, e := p.OriginalMessages(); e == nil {
		t.Fatal("caller lifetime renewed for typed body")
	}
	for _, name := range []string{"input", "context", "events"} {
		t.Run(name, func(t *testing.T) {
			_, v := fixtureInput(t, false)
			switch name {
			case "input":
				v.Job.Limits.MaxInputBytes = 1
			case "context":
				v.Job.Limits.MaxContextBytes = 1
			case "events":
				v.Job.Limits.MaxContextEvents = 3
			}
			if p, e := CompileOriginalRequestV1(context.Background(), id, v, cfg, fixtureLedger(t, cfg.Profile.Limits)); e == nil || p != nil {
				t.Fatal("original smaller MIN ignored")
			}
		})
	}
}

func TestCompleteProjectionStageAFundedPrerequisiteBoundary(t *testing.T) {
	cfg := fixtureConfig()
	id, input := fixtureInput(t, false)
	small := cfg.Profile.Limits
	small.PreSendAllocationBytes = 8 << 20
	l := fixtureLedger(t, small)
	if p, e := CompileOriginalRequestV1(context.Background(), id, input, cfg, l); e == nil || p != nil {
		t.Fatal("unfunded source prerequisites reached output")
	}
	// Only the bounded schema parse was funded; the failed joint Stage-A
	// reservation precedes source Clone/kernel. This is a source-order/control
	// oracle, not a fabricated observation of unavailable native allocation.
	if l.Accounting().CopyBytes != 4*uint64(len(cfg.FinalSchema)) {
		t.Fatal("failed K reservation debited or ignored")
	}
}

func TestCompleteProjectionProfileVendorIsNotTransportProvider(t *testing.T) {
	for _, vendor := range []string{"", "foreign"} {
		t.Run("deny-"+vendor, func(t *testing.T) {
			cfg := fixtureConfig()
			id, input := fixtureInput(t, false)
			cfg.Profile.Model.ModelVendorID = vendor
			if p, e := CompileOriginalRequestV1(context.Background(), id, input, cfg, fixtureLedger(t, cfg.Profile.Limits)); e == nil || p != nil {
				t.Fatal("empty/foreign original profile vendor admitted")
			}
		})
	}
	t.Run("distinct-provider-data", func(t *testing.T) {
		cfg := fixtureConfig()
		id, input := fixtureInput(t, false)
		cfg.Model.Provider = "openrouter"
		p, e := CompileOriginalRequestV1(context.Background(), id, input, cfg, fixtureLedger(t, cfg.Profile.Limits))
		if e != nil {
			t.Fatal("transport provider conflated with model vendor:", e)
		}
		defer p.Clear()
		if !bytes.Contains(p.Body(), []byte(`"model":"synthetic"`)) {
			t.Fatal("original model ID lost")
		}
		// Synthetic structural route DATA only, not profile validation/eligibility.
	})
}

func TestCompleteProjectionScalarEnvelopeBeforeFundedWork(t *testing.T) {
	for _, name := range []string{"system", "model-id", "endpoint", "provider", "effort", "summary", "thinking-off", "schema-name", "schema-source"} {
		t.Run(name, func(t *testing.T) {
			cfg := fixtureConfig()
			id, input := fixtureInput(t, false)
			cfg.Profile.Limits.NativeRequestBytes = 1024
			over := strings.Repeat("x", 1025)
			switch name {
			case "system":
				cfg.SystemPrompt = over
			case "model-id":
				cfg.Model.ID = over
				cfg.Profile.Model.ModelID = over
				input.Job.HarnessBinding.ModelID = over
			case "endpoint":
				cfg.Model.Endpoint = "https://" + over
				cfg.Profile.Model.Endpoint = cfg.Model.Endpoint
			case "provider":
				cfg.Model.Provider = over
			case "effort":
				cfg.ReasoningEffort = &over
			case "summary":
				v := "low"
				cfg.ReasoningEffort = &v
				cfg.ReasoningSummary = &over
			case "thinking-off":
				cfg.Model.ThinkingOff = &over
			case "schema-name":
				cfg.FinalSchemaName = strings.Repeat("n", 65)
			case "schema-source":
				cfg.FinalSchemaSourceDigest = strings.Repeat("a", 65)
				cfg.Profile.Sources.FinalSchemaDigest = cfg.FinalSchemaSourceDigest
			}
			l := fixtureLedger(t, cfg.Profile.Limits)
			p, e := CompileOriginalRequestV1(context.Background(), id, input, cfg, l)
			if e == nil || p != nil || l.Accounting() != (codexexec.CanonicalMediaAccountingV1{}) {
				t.Fatal("oversized borrowed scalar reached funded schema/Clone/emission")
			}
		})
	}
}

func TestCompleteProjectionExactScalarAndJointHeaderEnvelopes(t *testing.T) {
	cfg := fixtureConfig()
	cfg.Profile.Limits.NativeRequestBytes = 1024
	cfg.SystemPrompt = strings.Repeat("x", 1024)
	if _, e := checkData(cfg); e != nil {
		t.Fatal("legitimate scalar syntax boundary denied:", e)
	}
	cfg.SystemPrompt += "x"
	if _, e := checkData(cfg); e == nil {
		t.Fatal("first-over scalar admitted")
	}
	// A scalar syntax ceiling alone is not whole-request/source/model-fit proof.
	cfg = fixtureConfig()
	base, _, e := nonsecretHeaders(cfg, cfg.Profile.Limits)
	if e != nil {
		t.Fatal(e)
	}
	cfg.Headers[2].Value += strings.Repeat("u", int(cfg.Profile.Limits.RequestHeaderBytes-base))
	n, _, e := nonsecretHeaders(cfg, cfg.Profile.Limits)
	if e != nil || n != cfg.Profile.Limits.RequestHeaderBytes {
		t.Fatal("exact joint app/authorization header boundary")
	}
	cfg.Headers[2].Value += "u"
	if _, _, e := nonsecretHeaders(cfg, cfg.Profile.Limits); e == nil {
		t.Fatal("joint first-over header admitted")
	}
	cfg.Headers[2].Value += "\xff"
	if _, _, e := nonsecretHeaders(cfg, cfg.Profile.Limits); e == nil {
		t.Fatal("oversized invalid header admitted")
	}
	// Static source order establishes the last invalid suffix isn't traversed;
	// this is a finite first-over control, never a wall-clock timing oracle.
}
