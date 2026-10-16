//go:build pi_sdk_functional && (darwin || linux)

package providercredential_test

// Complete synthetic original cohort mirrored from pisdkmedia's canonical
// fixtureInputResource/initFixtureConfig. Both original images, all four
// events and snapshot+tail survive unchanged. These DATA fixtures do not grant
// Source, selected-profile fit, provider, credential or platform authority.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkmedia"
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
func fixtureConfig() pisdkmedia.RequestDataV1 {
	limits := domain.AttachedPiSDKNativeLimitsV1{PreSendAllocationBytes: 64 << 20, ProcessMemoryBytes: 256 << 20, JSONNodes: 8 << 20, JSONDepth: 32, NativeRequestBytes: 1 << 20, RequestHeaderBytes: 4096, ResponseBodyBytes: 256 << 10, ResponseChunkBytes: 64 << 10, SSEEventBytes: 64 << 10, ResponseHeaderBytes: 8192, ResponseCopyWorkBytes: 4 << 20, ResponseAllocationBytes: 4 << 20}
	source := strings.Repeat("a", 64)
	return pisdkmedia.RequestDataV1{Profile: &domain.AttachedPiSDKNativeProfileV1{Sources: domain.AttachedPiSDKNativeSourcesV1{FinalSchemaDigest: source}, Model: domain.AttachedPiSDKNativeModelV1{ModelVendorID: "openai", ModelID: "synthetic", Endpoint: "https://synthetic.invalid/responses", MaxOutputTokens: 100}, Limits: limits},
		Model:        &pisdkmedia.ModelDataV1{API: "openai-responses", Provider: "openai", ID: "synthetic", Endpoint: "https://synthetic.invalid/responses", Vision: true, Reasoning: true, SupportsDeveloperRole: true, SupportsExplicitPromptCacheMode: true},
		SystemPrompt: "trusted <>&\u2028\u2029💬\n\t\"\\", MaxOutputTokens: 16, FinalSchema: []byte(`{"type":"object","properties":{"4294967295":{"type":"string"},"01":{"type":"string"},"4294967294":{"type":"string"},"__proto__":{"type":"string"},"answer":{"type":"string"},"artifacts":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"mime_type":{"type":"string"},"content":{"type":"string"}},"required":["name","content"],"additionalProperties":false}}},"required":["answer"],"additionalProperties":false}`),
		FinalSchemaSourceDigest: source, FinalSchemaName: "result", CredentialMaxBytes: 128, Headers: []pisdkmedia.HeaderDataV1{{Name: "accept", Value: "application/json"}, {Name: "content-type", Value: "application/json"}, {Name: "user-agent", Value: "Pi/synthetic (fixture)"}, {Name: "x-stainless-lang", Value: "js"}, {Name: "x-stainless-package-version", Value: "6.40.0"}, {Name: "x-stainless-os", Value: "MacOS"}, {Name: "x-stainless-arch", Value: "arm64"}, {Name: "x-stainless-runtime", Value: "node"}, {Name: "x-stainless-runtime-version", Value: "v24.19.0"}, {Name: "x-stainless-retry-count", Value: "0"}, {Name: "x-stainless-timeout", Value: "1"}}}
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
	return fixtureInputResource(t, snapshot, false)
}

func fixtureInputResource(t *testing.T, snapshot, api bool) (attachedworkerdaemon.InvocationIdentity, *attachedworkerdaemon.VerifiedAttachedInputV1) {
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
	if api {
		binding.Resource.Kind, binding.Resource.ResourceID = domain.ProviderResourceAPIAccountV1, "api-account-body"
		binding.Backend.BackendKind = domain.HarnessBackendDirectOpenRouterV1
		binding.Backend.NativeProtocolVersion = "openrouter-http.v1"
	}
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

func initFixtureConfig(t *testing.T) pisdkmedia.RequestDataV1 {
	t.Helper()
	c := fixtureConfig()
	p := c.Profile
	p.Version, p.ProfileID, p.NativeProtocolVersion = 1, domain.AttachedPiSDKProfileIDV1, domain.AttachedPiSDKNativeProtocolV1
	p.Sources.PiSourceRevision = "6c4f360264397c59801f6da2bdac13e3b1fcbe91"
	p.Sources.OpenAISDKVersion = "6.40.0"
	p.Sources.OpenAISDKIntegrity = "sha512-MWtTjd/gQt4jpbji61NTgFWJLoY/PdRJ6wG9/ZDRMYNMlBKrCrSlkLI+KgHP1vR1qT6LKSAyAqIxno6lcK9JiA=="
	p.Sources.NodeVersion = "local-init-fixture"
	p.Model.ModelContextTokens, p.Model.MaxBilledInputTokens = 65536, 8192
	p.Platform.OS, p.Platform.Architecture = "local-fixture-os", "local-fixture-arch"
	for _, section := range []any{&p.Sources, &p.Model, &p.Platform} {
		v := reflect.ValueOf(section).Elem()
		for i := 0; i < v.NumField(); i++ {
			if strings.HasSuffix(v.Type().Field(i).Name, "Digest") {
				v.Field(i).SetString(strings.Repeat("a", 64))
			}
		}
	}
	l := &p.Limits
	l.CanonicalInputBytes, l.ProcessFrameBytes = 1<<20, 1<<20
	l.ProcessStdoutBytes, l.ProcessDiskBytes = 16<<20, 16<<20
	l.ProcessEvents, l.SSEEvents, l.SSELineBytes = 4096, 4096, l.SSEEventBytes
	l.RawFinalBytes, l.StructuredFinalBytes = 256<<10, 128<<10
	l.AnswerBytes, l.ArtifactBytes, l.ArtifactTotalBytes = 32<<10, 32<<10, 64<<10
	p.Behavior = domain.AttachedPiSDKNativeBehaviorV1{ProviderAPI: "openai-responses", ImageDerivation: "NoTransformOriginal", ImageDetail: "auto", Transport: "http-sse", LocalToolRegistry: "empty", MaxPhysicalSends: 1}
	if err := p.Validate(); err != nil {
		t.Fatal("structural fixture profile:", err)
	}
	return c
}

const initFixtureModel = `{"id":"synthetic","name":"structural init fixture","api":"openai-responses","provider":"openai","baseUrl":"https://synthetic.invalid","reasoning":true,"input":["text","image"],"cost":{"input":0.125,"output":1.5,"cacheRead":1.25e-2,"cacheWrite":0,"tiers":[{"inputTokensAbove":1000,"input":0.5,"output":2.5,"cacheRead":0.1,"cacheWrite":0}]},"contextWindow":65536,"maxTokens":100,"compat":{"supportsDeveloperRole":true,"supportsExplicitPromptCacheMode":true}}`
