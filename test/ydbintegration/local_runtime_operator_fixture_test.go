//go:build ydbintegration

package ydbintegration

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

// This is a non-expiry native lifecycle proof, not a five-second database
// throughput benchmark. Use the existing admitted operation/cleanup maxima;
// production still enforces each original absolute deadline without renewal.
// The invocation must also cover pre-effect10 + active30 + cleanup30.
const (
	localRuntimeFixtureOperationBudget  = 30 * time.Second
	localRuntimeFixtureCleanupBudget    = 30 * time.Second
	localRuntimeFixtureInvocationBudget = 90 * time.Second
)

// Construct original, private operator declaration DATA only. Runtime identity
// is observed from this real Go executable; model/capability/review/service
// documents below are explicitly synthetic, not live qualification or prices.
// This never opens a database, installs a profile, publishes funding, reads a
// provider credential or contacts an endpoint. The caller owns real installation.
func newLocalRuntimeOperatorFiles(t *testing.T, ctx context.Context, tenant domain.TenantID, owner domain.UserID, endpoint string, at time.Time) (root, executable, receiptPath string) {
	t.Helper()
	if ctx == nil || ctx.Err() != nil || tenant.Validate() != nil || owner.Validate() != nil || at.IsZero() || at.Nanosecond()%1000 != 0 {
		t.Fatal("invalid original operator fixture scope/clock")
	}
	at, end := at.UTC(), at.UTC().Add(10*time.Minute)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	executable, err = os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := localoperator.InspectRuntime(ctx, executable)
	if err != nil {
		t.Fatal("inspect actual Go worker fixture", err)
	}
	connector, err := localoperator.LinkedConnectorIdentity(identity.WorkerExecutableDigest)
	if err != nil {
		t.Fatal(err)
	}
	// Reuse only immutable finite template DATA. No template resource or
	// invocation ID enters the manifest or any database/funding call.
	base, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(tenant, owner, "template-run", "template-attempt", "template-subscription", at)
	if err != nil {
		t.Fatal(err)
	}
	h := strings.Repeat("a", 64)
	scope := sha256.Sum256([]byte(string(tenant) + "\x00" + string(owner)))
	suffix := hex.EncodeToString(scope[:8])
	cost := base.AdmissionCostCeiling.Clone()
	cost.PriceObservedAt, cost.PriceExpiresAt = at, end
	cost.MaxPreEffectDurationPerDelivery = 10 * time.Second
	cost.MaxActiveDuration, cost.MaxCleanupAndReconcileDuration = 30*time.Second, localRuntimeFixtureCleanupBudget
	service := func(kind domain.ExternalServiceKindV1, amount uint64) *domain.ExternalServiceCostCeilingV1 {
		return &domain.ExternalServiceCostCeilingV1{Version: 1, Kind: kind, TenantID: tenant, OwnerUserID: owner, BillingOwnerUserID: owner, ResourceID: string(kind) + "-" + suffix, ResourceRevision: 1, RevocationGeneration: 1, ProfileID: string(kind) + "-synthetic", ProfileRevision: 1, ScopedGrantDigest: h, Currency: cost.Currency, PriceRevision: "synthetic-service-price.v1", PriceObservedAt: at, PriceExpiresAt: end, PriceEvidenceDigest: h, TermsEvidenceDigest: h, PriceState: domain.ExternalServicePriceKnownPaidV1, MaxAmountMicrounits: &amount}
	}
	cost.MCPToolCost, cost.WebSearchCost = service(domain.ExternalServiceMCPToolV1, 30), service(domain.ExternalServiceWebSearchV1, 20000)
	total := uint64(20030)
	cost.MaxTotalAmountMicrounits = &total
	substrate := base.SubstrateBinding
	substrate.Kind, substrate.Region, substrate.ImageDigest = domain.SubstrateLocalDevelopmentV1, "", ""
	substrate.ProfileEvidenceExpiresAt = end
	substrate.LocalHostIdentityDigest, substrate.LocalNetworkNamespaceIdentityDigest = identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	substrate.OuterHarnessArtifactDigest, substrate.EgressProxyArtifactDigest, substrate.EgressProxyIdentityDigest = identity.WorkerExecutableDigest, identity.WorkerExecutableDigest, connector
	substrate.Limits.InvocationTimeout, substrate.Limits.ExecutionTimeout, substrate.Limits.CleanupTimeout = localRuntimeFixtureInvocationBudget, 30*time.Second, localRuntimeFixtureCleanupBudget
	backend := base.HarnessBinding.Backend
	backend.BackendKind, backend.ProviderContractKind, backend.NativeProtocolVersion = domain.HarnessBackendDirectOpenAICompatibleV1, domain.ProviderContractConfiguredEndpointV1, domain.ConfiguredInferenceProtocolChatCompletionsV1
	backend.BackendProfileDigest = ""
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceLocalEndpointV1, OwnerUserID: owner, CredentialMode: domain.ProviderCredentialNoneV1}
	limits := domain.AgentLoopLimitsV1{MaxConcurrentOperations: 1, MaxModelCalls: 8, MaxCompactionCalls: 2, MaxToolDispatches: 4, MaxWebSearches: 2, MaxInputTokensPerCall: 32768, MaxAggregateInputTokens: 131072, MaxOutputTokensPerCall: 2048, MaxAggregateOutputTokens: 16384, MaxActiveDuration: 30 * time.Second, MaxNetworkOperationDuration: localRuntimeFixtureOperationBudget, MaxCleanupDuration: localRuntimeFixtureCleanupBudget, MaxContextViewBytes: 1 << 20, MaxSummaryBytes: 16 << 10, MaxToolArgumentBytes: 4 << 10, MaxToolResultBytes: 64 << 10, MaxMCPOutboundMessages: 12, MaxMCPCatalogPages: 1, MaxMCPInboundFrames: 128, MaxMCPInboundBytes: 256 << 10, MaxOperations: 12, MaxAggregateBodyBytes: 1 << 20}
	c := domain.ConfiguredInferenceProfileV1{Version: 1, TenantID: tenant, OwnerUserID: owner, Resource: resource, BaseURL: endpoint, EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1, NativeProtocolVersion: domain.ConfiguredInferenceProtocolChatCompletionsV1, Auth: domain.ConfiguredInferenceAuthNoneV1, ModelVendorID: "synthetic", ModelID: "local-runtime-synthetic", ResponseModelIDs: []string{"local-runtime-synthetic"}, ServerID: "synthetic-server", ServerVersion: "synthetic-v1", ChatTemplateDigest: h,
		Capabilities: domain.ConfiguredInferenceCapabilitiesV1{Text: domain.ProviderEvidenceSupportedV1, Image: domain.ProviderEvidenceUnsupportedV1, File: domain.ProviderEvidenceUnsupportedV1, ToolCalls: domain.ProviderEvidenceSupportedV1, JSONSchema: domain.ProviderEvidenceUnknownV1}, ProviderCatalogDigest: h, ProviderRouteDigest: h, PrivacyPolicyDigest: h, EffectivePolicyDigest: h, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}, ModelWindowTokens: 65536, TokenEstimatorVersion: "synthetic-affine.v1",
		Limits:                  domain.ConfiguredInferenceLimitsV1{RequestBytes: 1 << 20, ResponseBytes: 1 << 20, ResponseHeaderBytes: 16 << 10, JSONDepth: 32, JSONMembers: 256, Choices: 1, ToolCalls: 1, ToolNameBytes: 128, ToolCallIDBytes: 128, ToolArgumentsBytes: 4 << 10, FinalTextBytes: 256 << 10, Messages: 256, Tools: 16, MaxInputTokens: 32768, MaxOutputTokens: 2048, OperationTimeout: localRuntimeFixtureOperationBudget, CleanupTimeout: localRuntimeFixtureCleanupBudget},
		LocalHostIdentityDigest: identity.LocalHostIdentityDigest, LocalNetworkNamespaceIdentityDigest: identity.LocalNetworkNamespaceIdentityDigest, WorkerExecutableDigest: identity.WorkerExecutableDigest, IssuedAt: at, ExpiresAt: end}
	body := func(id string, value any) localoperator.EvidenceBodyV1 {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(raw)
		return localoperator.EvidenceBodyV1{Reference: domain.OwnedAgentAuthorityBodyRefV1{Ref: id, Digest: hex.EncodeToString(hash[:]), Bytes: uint64(len(raw))}, Body: raw}
	}
	terms := localoperator.InvoiceTermsV1{Version: 1, TenantID: tenant, OwnerUserID: owner, BaseURL: endpoint, ModelVendorID: c.ModelVendorID, ModelID: c.ModelID, Currency: cost.Currency, Revision: cost.PriceRevision, OwnerSuppliesModel: true, OwnerSuppliesHost: true, IssuedAt: at, ExpiresAt: end}
	invoice, capability, review := body("synthetic-invoice", terms), body("synthetic-capability", "synthetic text/tool declaration, not model qualification"), body("synthetic-review", "synthetic metadata, not independent execution acceptance")
	c.OperatorTerms, c.CapabilityEvidence = invoice.Reference, capability.Reference
	calibration := body("synthetic-calibration", map[string]any{"version": 1, "algorithm": "utf8_affine.v1", "input_domain": "chat_completions_text.v1", "tenant_id": tenant, "owner_user_id": owner, "model_vendor_id": c.ModelVendorID, "model_id": c.ModelID, "response_model_ids": c.ResponseModelIDs, "server_id": c.ServerID, "server_version": c.ServerVersion, "protocol": c.NativeProtocolVersion, "chat_template_digest": c.ChatTemplateDigest, "estimator_version": c.TokenEstimatorVersion, "capability_evidence_digest": capability.Reference.Digest, "model_window_tokens": c.ModelWindowTokens, "max_wire_bytes": c.Limits.RequestBytes, "max_messages": c.Limits.Messages, "max_tools": c.Limits.Tools, "byte_numerator": 1, "byte_denominator": 1, "fixed_tokens": 1, "message_tokens": 1, "tool_tokens": 1})
	c.CalibrationDigest = calibration.Reference.Digest
	p := domain.OwnedAgentProfileCurrentV1{Version: 1, Locator: domain.OwnedAgentProfileLocatorV1{TenantID: tenant, OwnerUserID: owner, Profile: domain.AgentLoopOwnedPublicV1, ResourceKind: resource.Kind, ModelVendorID: c.ModelVendorID, ModelID: c.ModelID, InputDataClass: domain.ProviderDataPublicV1}, Backend: backend, Resource: resource, Substrate: substrate, ReviewReceipt: review.Reference, ProviderCatalogDigest: h, ProviderRouteDigest: h, CapabilityEvidenceDigest: capability.Reference.Digest, PrivacyPolicyDigest: h, EffectivePolicyDigest: h, ProviderPriceDigest: invoice.Reference.Digest, ProviderTermsDigest: invoice.Reference.Digest, SubstratePriceDigest: invoice.Reference.Digest, SubstrateTermsDigest: invoice.Reference.Digest, SubstratePriceState: domain.ExternalServicePriceKnownFreeV1, BillingOwnerUserID: owner, InputModalities: []domain.AgentInputModalityV1{domain.AgentInputTextV1}, ModelWindowTokens: c.ModelWindowTokens, TokenEstimatorVersion: c.TokenEstimatorVersion, CalibrationDigest: c.CalibrationDigest, InstructionVersion: compactioncontext.InstructionVersionV1, CompactionPolicyDigest: compactioncontext.CompactionPolicyDigestV1(), Limits: limits, CostCeiling: cost, Window: domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: end}, UpdatedAt: at}
	m := localoperator.ManifestV1{Version: 1, Configuration: c, Profile: p, InvoiceTerms: terms, Evidence: []localoperator.EvidenceBodyV1{invoice, capability, review, calibration}}

	// Preserve the complete independently finite four-component declarations.
	// The final-only model does not send MCP/search requests or read their
	// credential locator. Synthetic reports never claim live vendor acceptance.
	mcp := mcpadmission.MCPToolGrantV1{Version: 1, TenantID: tenant, OwnerUserID: owner, BillingOwnerUserID: owner, ResourceID: cost.MCPToolCost.ResourceID, ResourceRevision: 1, RevocationGeneration: 1, ProfileID: cost.MCPToolCost.ProfileID, ProfileRevision: 1, DataPolicyDigest: h, DataClass: "public", ConsentMode: "owner_public_query", ConsentRevision: 1,
		Endpoint: "https://developers.openai.com/mcp", Transport: "streamable_http", ProtocolVersion: mcpwire.ProtocolVersionV1, SchemaValidator: domain.ConfiguredToolSchemaValidatorV1, ModelDescription: "Synthetic unused docs lookup; model returns a final answer",
		Descriptor: json.RawMessage("{\"annotations\":{\"readOnlyHint\":true},\"description\":\"synthetic descriptor, not a live catalog\",\"inputSchema\":{\"additionalProperties\":false,\"properties\":{\"query\":{\"maxLength\":30,\"minLength\":1,\"type\":\"string\"}},\"required\":[\"query\"],\"type\":\"object\"},\"name\":\"search_openai_docs\"}"),
		Limits:     mcpwire.LimitsV1{MessageBytes: 64 << 10, AggregateBytes: 256 << 10, JSONDepth: 32, JSONMembers: 4096, SSELineBytes: 64 << 10, SSEEventBytes: 64 << 10, SSEEvents: 128, Tools: 1, DescriptorBytes: 32 << 10, ArgumentBytes: 4 << 10, ResultTextBytes: 4096, ContentItems: 16}, IssuedAt: at, ExpiresAt: end}
	mcpBody := body("synthetic-mcp-grant", mcp)
	mcpPriceDoc, mcpTermsDoc := body("synthetic-mcp-price-document", "independent synthetic service price document"), body("synthetic-mcp-terms-document", "independent synthetic service terms document")
	mcpPrice := body("synthetic-mcp-price-report", mcpadmission.MCPPriceEvidenceV1{Version: 1, GrantDigest: mcpBody.Reference.Digest, Currency: cost.Currency, PriceRevision: cost.MCPToolCost.PriceRevision, PriceState: cost.MCPToolCost.PriceState, MaxAmountMicrounits: cost.MCPToolCost.MaxAmountMicrounits, SourceURL: "https://evidence.example.invalid/mcp-price", SourceBody: mcpPriceDoc.Reference, ObservedAt: at, ExpiresAt: end})
	mcpTerms := body("synthetic-mcp-terms-report", mcpadmission.MCPTermsEvidenceV1{Version: 1, GrantDigest: mcpBody.Reference.Digest, SourceURL: "https://evidence.example.invalid/mcp-terms", SourceBody: mcpTermsDoc.Reference, ObservedAt: at, ExpiresAt: end})
	m.Profile.MCPToolGrantDigest, m.Profile.CostCeiling.MCPToolCost.ScopedGrantDigest = mcpBody.Reference.Digest, mcpBody.Reference.Digest
	m.Profile.CostCeiling.MCPToolCost.PriceEvidenceDigest, m.Profile.CostCeiling.MCPToolCost.TermsEvidenceDigest = mcpPrice.Reference.Digest, mcpTerms.Reference.Digest
	if err := mcp.ValidateForCost(*m.Profile.CostCeiling.MCPToolCost, at); err != nil {
		t.Fatal("synthetic original MCP DATA correlation", err)
	}
	m.Evidence = append(m.Evidence, mcpBody, mcpPriceDoc, mcpTermsDoc, mcpPrice, mcpTerms)

	searchPriceDoc, searchTermsDoc := body("synthetic-search-price-document", "independent synthetic search price document"), body("synthetic-search-terms-document", "independent synthetic search terms document")
	searchPrice := body("synthetic-search-price-report", localoperator.OwnerAcceptedSearchPriceV2{Version: 2, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Origin: "https://evidence.example.invalid/search-price", ObservedAt: at, ExpiresAt: end, Payload: searchPriceDoc.Reference, PriceRevision: cost.WebSearchCost.PriceRevision, Currency: cost.Currency, Unit: "request", TariffMicrounits: 8000})
	searchTerms := body("synthetic-search-terms-report", localoperator.OwnerAcceptedSearchTermsV2{Version: 2, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Origin: "https://evidence.example.invalid/search-terms", ObservedAt: at, ExpiresAt: end, Payload: searchTermsDoc.Reference, DataPolicyDigest: h, CredentialAudience: domain.WebSearchCredentialAudienceV1})
	search := domain.WebSearchResourceGrantV2{Version: 2, TenantID: tenant, OwnerUserID: owner, BillingOwnerUserID: owner, ResourceID: cost.WebSearchCost.ResourceID, ResourceRevision: 1, RevocationGeneration: 1, ConsentRevision: 1, ProfileID: domain.WebSearchProfileV1, ProfileRevision: 1, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Endpoint: domain.WebSearchEndpointV1, Method: "POST", CredentialRef: "synthetic-search-locator-no-key", CredentialAudience: domain.WebSearchCredentialAudienceV1, DataPolicyDigest: h, APISchemaDigest: strings.Repeat("b", 64), AdapterArtifactDigest: strings.Repeat("c", 64), Currency: cost.Currency, PriceRevision: cost.WebSearchCost.PriceRevision, PriceEvidenceDigest: searchPrice.Reference.Digest, TermsEvidenceDigest: searchTerms.Reference.Digest, PerCallReservationMicrounits: 10000, AggregateReservationMicrounits: 20000, IssuedAt: at, ExpiresAt: end}
	searchBody := body("synthetic-search-grant", search)
	searchDigest, err := search.Digest()
	if err != nil {
		t.Fatal("synthetic search DATA", err)
	}
	m.Profile.WebSearchGrantDigest, m.Profile.WebSearchResourceGrant = searchDigest, &domain.WebSearchResourceGrantBodyV2{Version: 2, Body: searchBody.Reference}
	m.Profile.CostCeiling.WebSearchCost.ProfileID = search.ProfileID
	m.Profile.CostCeiling.WebSearchCost.ScopedGrantDigest = searchDigest
	m.Profile.CostCeiling.WebSearchCost.PriceEvidenceDigest, m.Profile.CostCeiling.WebSearchCost.TermsEvidenceDigest = searchPrice.Reference.Digest, searchTerms.Reference.Digest
	m.SearchExpenditure = &localoperator.OwnerLocalSearchExpenditureV2{Version: 2, TenantID: tenant, OwnerUserID: owner, BillingOwnerUserID: owner, ResourceID: search.ResourceID, ResourceRevision: 1, RevocationGeneration: 1, GrantDigest: searchDigest, GrantBody: searchBody.Reference, PriceEvidence: searchPrice.Reference, TermsEvidence: searchTerms.Reference, Currency: cost.Currency, PerCallReservationMicrounits: 10000, AggregateReservationMicrounits: 20000, IssuedAt: at, ExpiresAt: end}
	m.Evidence = append(m.Evidence, searchPriceDoc, searchTermsDoc, searchPrice, searchTerms, searchBody)
	policy, err := localRuntimeEgressPolicyDigest(m.Configuration)
	if err != nil {
		t.Fatal(err)
	}
	m.Profile.Substrate.EgressPolicyDigest = policy
	m.Profile.Substrate.AdmissionCostCeilingDigest, err = m.Profile.CostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	defer clear(key)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	keyHash := sha256.Sum256(key)
	m.KeyFingerprint = hex.EncodeToString(keyHash[:])
	raw, err := json.Marshal(m)
	if err != nil || len(raw) > 256<<10 {
		t.Fatal("bounded synthetic operator manifest", err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source.key"), key, 0o600); err != nil {
		t.Fatal(err)
	}
	return root, executable, filepath.Join(root, "receipt.json")
}

func localRuntimeEgressPolicyDigest(profile domain.ConfiguredInferenceProfileV1) (string, error) {
	policy, err := ports.BuildLocalLoopbackEgressPolicyV1(profile)
	if err != nil {
		return "", err
	}
	return policy.Digest()
}

func TestLocalRuntimeOperatorFixturePrivateTemplateAndRealIntake(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	at := time.Now().UTC().Truncate(time.Microsecond)
	root, executable, receipt := newLocalRuntimeOperatorFiles(t, ctx, "synthetic-template-tenant", "synthetic-template-owner", "http://127.0.0.1:35421/v1", at)
	if _, err := os.Stat(receipt); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("DATA builder installed a receipt", err)
	}
	for _, name := range []string{"manifest.json", "source.key"} {
		stat, err := os.Lstat(filepath.Join(root, name))
		if err != nil || !stat.Mode().IsRegular() || stat.Mode().Perm() != 0o600 {
			t.Fatal("operator fixture file permissions", name, err)
		}
	}
	stat, err := os.Stat(root)
	if err != nil || stat.Mode().Perm() != 0o700 {
		t.Fatal("operator fixture directory permissions", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	var manifest localoperator.ManifestV1
	if err != nil || json.Unmarshal(raw, &manifest) != nil || len(raw) > 256<<10 || len(manifest.Evidence) != 14 {
		t.Fatal("original bounded operator manifest", err)
	}
	if manifest.Configuration.Resource.ResourceID != "" || manifest.Configuration.Resource.Revision != 0 || manifest.Configuration.ProfileRevision != 0 || manifest.Configuration.LocalInstallationID != "" || manifest.Configuration.LocalInstallationRevision != 0 || manifest.Profile.Resource.ResourceID != "" || manifest.Profile.ProfileRevision != 0 || manifest.Profile.InstallationID != "" || manifest.Profile.InstalledManifest.Ref != "" || manifest.Profile.Backend.BackendProfileDigest != "" || !manifest.InvoiceTerms.IssuedAt.Equal(at) || !manifest.InvoiceTerms.ExpiresAt.After(at.Add(5*time.Minute)) {
		t.Fatal("DATA builder chose installed identity or shortened original finite terms")
	}
	configuration, profile := manifest.Configuration, manifest.Profile
	if configuration.Limits.OperationTimeout != localRuntimeFixtureOperationBudget || configuration.Limits.CleanupTimeout != localRuntimeFixtureCleanupBudget || profile.Limits.MaxNetworkOperationDuration != localRuntimeFixtureOperationBudget || profile.Limits.MaxCleanupDuration != localRuntimeFixtureCleanupBudget || profile.CostCeiling.MaxCleanupAndReconcileDuration != localRuntimeFixtureCleanupBudget || profile.Substrate.Limits.CleanupTimeout != localRuntimeFixtureCleanupBudget || profile.Substrate.Limits.InvocationTimeout != localRuntimeFixtureInvocationBudget {
		t.Fatalf("native non-expiry budget declarations disagree: operation=%s cleanup=%s profile_operation=%s profile_cleanup=%s cost_cleanup=%s substrate_cleanup=%s invocation=%s", configuration.Limits.OperationTimeout, configuration.Limits.CleanupTimeout, profile.Limits.MaxNetworkOperationDuration, profile.Limits.MaxCleanupDuration, profile.CostCeiling.MaxCleanupAndReconcileDuration, profile.Substrate.Limits.CleanupTimeout, profile.Substrate.Limits.InvocationTimeout)
	}
	minimumInvocation := profile.CostCeiling.MaxPreEffectDurationPerDelivery + profile.CostCeiling.MaxActiveDuration + profile.CostCeiling.MaxCleanupAndReconcileDuration
	if minimumInvocation > profile.Substrate.Limits.InvocationTimeout {
		t.Fatalf("native declaration loses original pre-effect/active/cleanup coverage: minimum=%s invocation=%s", minimumInvocation, profile.Substrate.Limits.InvocationTimeout)
	}
	source, err := localoperator.OpenFresh(ctx, root, executable)
	if err != nil {
		t.Fatal("real private Source intake rejected template", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Error(err)
		}
	})
	config, err := source.Configuration()
	if err != nil || config.TenantID != "synthetic-template-tenant" || config.OwnerUserID != "synthetic-template-owner" || config.Auth != domain.ConfiguredInferenceAuthNoneV1 || config.Resource.ResourceID == "" || !config.IssuedAt.Equal(at) || !config.ExpiresAt.Equal(at.Add(10*time.Minute)) || config.ValidateAt(at.Add(time.Second)) != nil {
		t.Fatal("Source did not retain original fixture scope/window", err)
	}
	// Validate the actual original Source-produced intent, not a fabricated
	// installed identity added to the deliberately uninstalled template above.
	// Reading this private intent performs no Store publication or SQL call.
	grant, err := source.Publication()
	if err != nil {
		t.Fatal("original Source publication intent", err)
	}
	if err := grant.Profile.CostCeiling.ValidateForSubstrate(grant.Profile.Substrate); err != nil {
		t.Fatal("original Source finite duration/cost coverage", err)
	}
	for _, delta := range []time.Duration{-time.Nanosecond, 0} {
		candidate := grant.Profile.Substrate
		candidate.Limits.InvocationTimeout = minimumInvocation + delta
		err := grant.Profile.CostCeiling.ValidateForSubstrate(candidate)
		if (err == nil) != (delta == 0) {
			t.Fatalf("pre-effect/active/cleanup coverage boundary delta=%s err=%v", delta, err)
		}
	}
	for _, field := range []string{"operation", "cleanup"} {
		candidate := config.Limits
		if field == "operation" {
			candidate.OperationTimeout++
		} else {
			candidate.CleanupTimeout++
		}
		if candidate.Validate() == nil {
			t.Fatalf("existing thirty-second %s maximum was relaxed", field)
		}
	}
}
