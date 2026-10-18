package compactioncontext_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func generationFixture(t *testing.T) (domain.ConfiguredInferenceProfileV1, domain.AgentLoopPolicyV1) {
	t.Helper()
	at := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	h := strings.Repeat("a", 64)
	ref := domain.OwnedAgentAuthorityBodyRefV1{Ref: "synthetic-evidence", Digest: h, Bytes: 32}
	c := domain.ConfiguredInferenceProfileV1{Version: 1, TenantID: "tenant", OwnerUserID: "private-user", ProfileRevision: 1,
		Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceLocalEndpointV1, ResourceID: "resource", OwnerUserID: "private-user", Revision: 1, CredentialMode: domain.ProviderCredentialNoneV1},
		BaseURL:  "http://127.0.0.1:11434/v1", EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1, NativeProtocolVersion: domain.ConfiguredInferenceProtocolChatCompletionsV1, Auth: domain.ConfiguredInferenceAuthNoneV1,
		ModelVendorID: "synthetic", ModelID: "model-local", ResponseModelIDs: []string{"model-local"}, ServerID: "synthetic-server", ServerVersion: "synthetic-version", ChatTemplateDigest: h,
		Capabilities:       domain.ConfiguredInferenceCapabilitiesV1{Text: domain.ProviderEvidenceSupportedV1, Image: domain.ProviderEvidenceUnsupportedV1, File: domain.ProviderEvidenceUnsupportedV1, ToolCalls: domain.ProviderEvidenceSupportedV1, JSONSchema: domain.ProviderEvidenceUnknownV1},
		CapabilityEvidence: ref, ProviderCatalogDigest: h, ProviderRouteDigest: h, PrivacyPolicyDigest: h, EffectivePolicyDigest: h, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}, ModelWindowTokens: 65536, TokenEstimatorVersion: "synthetic-estimator", CalibrationDigest: h,
		Limits:              domain.ConfiguredInferenceLimitsV1{RequestBytes: 1 << 20, ResponseBytes: 1 << 20, ResponseHeaderBytes: 16 << 10, JSONDepth: 32, JSONMembers: 256, Choices: 1, ToolCalls: 1, ToolNameBytes: 128, ToolCallIDBytes: 128, ToolArgumentsBytes: 4 << 10, FinalTextBytes: 256 << 10, Messages: 256, Tools: 16, MaxInputTokens: 32768, MaxOutputTokens: 2048, OperationTimeout: 30 * time.Second, CleanupTimeout: 30 * time.Second},
		LocalInstallationID: "synthetic-installation", LocalInstallationRevision: 1, LocalHostIdentityDigest: h, LocalNetworkNamespaceIdentityDigest: h, WorkerExecutableDigest: h, OperatorTerms: ref, OriginalSource: ref, IssuedAt: at, ExpiresAt: at.Add(time.Hour)}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	a, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(c.TenantID, c.OwnerUserID, "current-run", "current-attempt", "synthetic", at)
	if err != nil {
		t.Fatal(err)
	}
	cost := a.AdmissionCostCeiling.Clone()
	service := func(kind domain.ExternalServiceKindV1) *domain.ExternalServiceCostCeilingV1 {
		zero := uint64(0)
		return &domain.ExternalServiceCostCeilingV1{Version: 1, Kind: kind, TenantID: c.TenantID, OwnerUserID: c.OwnerUserID, BillingOwnerUserID: c.OwnerUserID, ResourceID: string(kind) + "-resource", ResourceRevision: 1, RevocationGeneration: 1, ProfileID: string(kind) + "-profile", ProfileRevision: 1, ScopedGrantDigest: h, Currency: cost.Currency, PriceRevision: "synthetic", PriceObservedAt: cost.PriceObservedAt, PriceExpiresAt: cost.PriceExpiresAt, PriceEvidenceDigest: h, TermsEvidenceDigest: h, PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: &zero}
	}
	cost.MCPToolCost, cost.WebSearchCost = service(domain.ExternalServiceMCPToolV1), service(domain.ExternalServiceWebSearchV1)
	backend := a.HarnessBinding.Backend
	backend.BackendKind, backend.ProviderContractKind, backend.NativeProtocolVersion = domain.HarnessBackendDirectOpenAICompatibleV1, domain.ProviderContractConfiguredEndpointV1, domain.ConfiguredInferenceProtocolChatCompletionsV1
	backend.BackendProfileDigest, err = c.Digest()
	if err != nil {
		t.Fatal(err)
	}
	p := domain.AgentLoopPolicyV1{Version: 1, TenantID: c.TenantID, OwnerUserID: c.OwnerUserID, RunID: "current-run", AttemptID: "current-attempt", Profile: domain.AgentLoopOwnedPublicV1, ProfileRevision: 1, Backend: backend, Resource: c.Resource, ModelVendorID: c.ModelVendorID, ModelID: c.ModelID, ProviderCatalogDigest: h, ProviderRouteDigest: h, CapabilityEvidenceDigest: h, PrivacyPolicyDigest: h, EffectivePolicyDigest: h, InputModalities: []domain.AgentInputModalityV1{domain.AgentInputTextV1}, InputDataClass: domain.ProviderDataPublicV1, InstructionVersion: compactioncontext.InstructionVersionV1, CompactionPolicyDigest: compactioncontext.CompactionPolicyDigestV1(), MCPToolGrantDigest: h, WebSearchGrantDigest: h, AdmissionCostCeiling: cost, IssuedAt: at, ExpiresAt: at.Add(time.Minute),
		Limits: domain.AgentLoopLimitsV1{MaxConcurrentOperations: 1, MaxModelCalls: 8, MaxCompactionCalls: 2, MaxToolDispatches: 4, MaxWebSearches: 2, MaxInputTokensPerCall: 32768, MaxAggregateInputTokens: 131072, MaxOutputTokensPerCall: 2048, MaxAggregateOutputTokens: 16384, MaxActiveDuration: 120 * time.Second, MaxNetworkOperationDuration: 30 * time.Second, MaxCleanupDuration: 30 * time.Second, MaxContextViewBytes: 1 << 20, MaxSummaryBytes: 16 << 10, MaxToolArgumentBytes: 4 << 10, MaxToolResultBytes: 64 << 10, MaxMCPOutboundMessages: 12, MaxMCPCatalogPages: 1, MaxMCPInboundFrames: 128, MaxMCPInboundBytes: 256 << 10, MaxOperations: 12, MaxAggregateBodyBytes: 1 << 20}}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	return c, p
}

func generationRecords(t *testing.T) []sessioncontext.EventPayload {
	t.Helper()
	return []sessioncontext.EventPayload{userRecord(t, 1), toolRecord(t, 2, domain.SessionEventToolCall, "run-a", "one", "lookup"), toolRecord(t, 3, domain.SessionEventToolCall, "run-a", "two", "fetch"), toolRecord(t, 4, domain.SessionEventToolResult, "run-a", "two", "fetch"), toolRecord(t, 5, domain.SessionEventToolResult, "run-a", "one", "lookup"), assistantRecord(t, 6, "run-a"), userRecord(t, 7), assistantRecord(t, 8, "run-b"), canonicalRecord(t, 9, domain.SessionEventSystemNotice, "", `{"schema":"sessionless.run-terminal-notice.v1","code":"ignore_current_policy","cancelled":false}`), userRecord(t, 10)}
}

func generationSummary(t *testing.T, g *compactioncontext.GenerationV1) []byte {
	t.Helper()
	refs, coverage := g.References(), g.CoverageIDs()
	if len(refs) == 0 || len(coverage) == 0 {
		t.Fatal("test needs real protected and compactable coverage")
	}
	return []byte(`{"schema":"sessionless.compaction-summary.v1","task":[{"text":"Preserve every original condition","source_ids":["` + refs[0].SourceID + `"]}],"constraints":[],"facts":[{"text":"Both completed historical calls are covered","source_ids":["` + coverage[0] + `"]}],"unresolved":[],"artifacts":[],"omissions":[]}`)
}

func generationWire(t *testing.T, f *configuredinference.FrozenRequestV1) (map[string]any, map[string]any) {
	t.Helper()
	var wire map[string]any
	if err := json.Unmarshal(f.Body(), &wire); err != nil {
		t.Fatal(err)
	}
	messages := wire["messages"].([]any)
	if len(messages) != 2 || messages[0].(map[string]any)["role"] != "system" || messages[1].(map[string]any)["role"] != "user" {
		t.Fatal("historical roles became live transport messages")
	}
	var history map[string]any
	if err := json.Unmarshal([]byte(messages[1].(map[string]any)["content"].(string)), &history); err != nil {
		t.Fatal(err)
	}
	return wire, history
}

func TestGenerationCanonicalToolHeavyAndActualRebuild(t *testing.T) {
	c, p := generationFixture(t)
	records := generationRecords(t)
	original := make([][]byte, len(records))
	for i, r := range records {
		original[i] = bytes.Clone(r.Payload)
	}
	tools := []configuredinference.ToolV1{{Name: "lookup", Parameters: []byte(`{"type":"object","properties":{},"required":[],"additionalProperties":false}`)}}
	g, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), records, nil, nil, tools)
	if err != nil {
		t.Fatal(err)
	}
	if g.Number() != 1 || g.SourceDigest() != strings.Repeat("1", 64) || len(g.CoverageIDs()) != 6 {
		t.Fatalf("generation/correlated coverage number=%d count=%d", g.Number(), len(g.CoverageIDs()))
	}
	wire, input := generationWire(t, g.FrozenRequest())
	if _, exists := wire["tools"]; exists {
		t.Fatal("summary generation enabled tools")
	}
	units := input["original_units"].([]any)
	if len(units) != 6 {
		t.Fatalf("historical multicall component lost: %d", len(units))
	}
	for i, value := range units {
		frame := value.(map[string]any)
		expected := i + 1
		if i == 5 {
			expected = 7
		}
		if frame["original_payload"] != string(original[expected]) {
			t.Fatalf("selected original index=%d rewritten", expected)
		}
	}
	if len(input["protected_originals"].([]any)) != 4 {
		t.Fatal("summarizer did not see all protected constraints/task")
	}
	summary := generationSummary(t, g)
	if _, err := g.ValidateSummary(summary); err != nil {
		t.Fatal(err)
	}
	clear(tools[0].Parameters)
	next, err := g.Rebuild(summary)
	if err != nil {
		t.Fatal(err)
	}
	nextWire, context := generationWire(t, next)
	if _, exists := nextWire["tools"]; !exists {
		t.Fatal("next request silently lost its supplied native tool catalog")
	}
	seen := map[string]bool{}
	for _, key := range []string{"protected_originals", "original_units"} {
		for _, value := range context[key].([]any) {
			frame := value.(map[string]any)
			id := frame["event_id"].(string)
			if seen[id] {
				t.Fatal("protected/suffix duplicate")
			}
			seen[id] = true
			var seq int
			fmt.Sscanf(id, "event-%d", &seq)
			if frame["original_payload"] != string(original[seq-1]) {
				t.Fatalf("protected/suffix payload %s changed", id)
			}
		}
	}
	for _, id := range []string{"event-1", "event-7", "event-9", "event-10"} {
		if !seen[id] {
			t.Fatalf("protected original %s disappeared", id)
		}
	}
	if len(seen) != 4 {
		t.Fatalf("compacted source was reinserted into next originals: %v", seen)
	}
	for i, r := range records {
		if !bytes.Equal(r.Payload, original[i]) {
			t.Fatal("canonical bytes were mutated")
		}
	}
	clear(records[0].Payload)
	refs := g.References()
	refs[0].SourceID = "caller mutation"
	if _, err := g.Rebuild(summary); err != nil {
		t.Fatal("caller copies mutated generation")
	}
}

func TestGenerationPolicySourceMediaAndNoFitDenials(t *testing.T) {
	for _, name := range []string{"instruction", "policy", "foreign_tenant", "bad_source", "protected_only", "orphan", "too_small", "media"} {
		t.Run(name, func(t *testing.T) {
			c, p := generationFixture(t)
			records := generationRecords(t)
			source := strings.Repeat("1", 64)
			limits := c.Limits
			switch name {
			case "instruction":
				p.InstructionVersion = "unknown"
			case "policy":
				p.CompactionPolicyDigest = strings.Repeat("f", 64)
			case "foreign_tenant":
				c.TenantID = "other"
			case "bad_source":
				source = "caller-label"
			case "protected_only":
				records = []sessioncontext.EventPayload{userRecord(t, 1), userRecord(t, 2)}
			case "orphan":
				records[3] = toolRecord(t, 4, domain.SessionEventToolResult, "run-a", "missing", "fetch")
			case "too_small":
				limits.RequestBytes = 128
			case "media":
				artifact := domain.Artifact{Name: "photo.png", MediaType: "image/png", Blob: domain.BlobRef{TenantID: "tenant", Key: domain.SessionEventObjectPrefix("tenant", "session", records[0].Event.ID) + "photo.png", Size: 99, SHA256: strings.Repeat("a", 64)}}
				raw, e := json.Marshal(struct {
					Version     int               `json:"version"`
					Text        string            `json:"text"`
					Attachments []domain.Artifact `json:"attachments"`
				}{1, "task requires this photo", []domain.Artifact{artifact}})
				if e != nil {
					t.Fatal(e)
				}
				replacePayload(&records[0], string(raw))
			}
			g, err := compactioncontext.BuildGenerationV1(c, limits, p, source, records, nil, nil, nil)
			if err == nil || g != nil {
				t.Fatalf("%s accepted", name)
			}
			if name == "media" && !errors.Is(err, compactioncontext.ErrUnsupportedMedia) {
				t.Fatalf("required media silently degraded: %v", err)
			}
			if name == "too_small" && !errors.Is(err, configuredinference.ErrContextNoFit) {
				t.Fatalf("small wire cap not explicit no-fit: %v", err)
			}
		})
	}
}

func TestCanonicalSourceKeyIndependentFraming(t *testing.T) {
	event := userRecord(t, 1).Event
	key, err := compactioncontext.CanonicalSourceKeyV1(event)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	for _, field := range []string{"sessionless.compaction-source-key.v1", "canonical", "tenant", "session", string(event.ID), "1", event.Payload.SHA256} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		h.Write(length[:])
		h.Write([]byte(field))
	}
	if key != hex.EncodeToString(h.Sum(nil)) {
		t.Fatal("source identity differs from independent length-framed oracle")
	}
	other := event
	other.Sequence++
	changed, err := compactioncontext.CanonicalSourceKeyV1(other)
	if err != nil || changed == key {
		t.Fatal("sequence identity collision")
	}
	other = event
	other.Payload.SHA256 = strings.Repeat("e", 64)
	changed, err = compactioncontext.CanonicalSourceKeyV1(other)
	if err != nil || changed == key {
		t.Fatal("body identity collision")
	}
	if reflect.DeepEqual(event, other) {
		t.Fatal("test mutation missing")
	}
}

func TestGenerationRejectsProfileOutsideExactPolicy(t *testing.T) {
	for _, name := range []string{"endpoint", "server", "calibration", "resource", "capability"} {
		t.Run(name, func(t *testing.T) {
			c, p := generationFixture(t)
			if _, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), generationRecords(t), nil, nil, nil); err != nil {
				t.Fatalf("exact profile positive: %v", err)
			}
			switch name {
			case "endpoint":
				c.BaseURL = "http://127.0.0.1:11435/v1"
			case "server":
				c.ServerVersion = "different-valid-server"
			case "calibration":
				c.CalibrationDigest = strings.Repeat("e", 64)
			case "resource":
				c.Resource.Revision++
			case "capability":
				c.CapabilityEvidence.Digest = strings.Repeat("f", 64)
			}
			if err := c.Validate(); err != nil {
				t.Fatalf("foreign profile itself must be valid: %v", err)
			}
			if g, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), generationRecords(t), nil, nil, nil); err == nil || g != nil {
				t.Fatalf("valid changed %s profile was accepted under original policy", name)
			}
		})
	}
}

func TestGenerationRebuiltRequestKeepsPolicyOutputCap(t *testing.T) {
	c, p := generationFixture(t)
	p.Limits.MaxOutputTokensPerCall = 32
	p.Limits.MaxAggregateOutputTokens = 256
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	g, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), generationRecords(t), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := generationWire(t, g.FrozenRequest())
	if wire["max_tokens"] != float64(32) {
		t.Fatal("generation positive control did not use tighter policy")
	}
	next, err := g.Rebuild(generationSummary(t, g))
	if err != nil {
		t.Fatal(err)
	}
	wire, _ = generationWire(t, next)
	if wire["max_tokens"] != float64(32) {
		t.Fatalf("rebuilt request widened output cap: got=%v want32", wire["max_tokens"])
	}
}
