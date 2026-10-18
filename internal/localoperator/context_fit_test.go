package localoperator_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func fitDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func fitEvidence(id string, body []byte) localoperator.EvidenceBodyV1 {
	return localoperator.EvidenceBodyV1{Reference: domain.OwnedAgentAuthorityBodyRefV1{Ref: id, Digest: fitDigest(body), Bytes: uint64(len(body))}, Body: body}
}

func writeFitManifest(t *testing.T, root string, m localoperator.ManifestV1) {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// This is real private-file Source intake over synthetic declarations and
// executable BYTES. Nothing executes the file or contacts/qualifies a model.
func sourceFitFiles(t *testing.T) (string, string, localoperator.ManifestV1) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	exe := filepath.Join(root, "worker-fixture")
	if err := os.WriteFile(exe, []byte("synthetic never-executed worker bytes"), 0o700); err != nil {
		t.Fatal(err)
	}
	id, err := localoperator.InspectRuntime(t.Context(), exe)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Minute)
	expires := at.Add(time.Hour)
	const tenant domain.TenantID = "fit-tenant"
	const owner domain.UserID = "fit-owner"
	a, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(tenant, owner, "fit-run", "fit-attempt", "fit-subscription", at)
	if err != nil {
		t.Fatal(err)
	}
	h := strings.Repeat("a", 64)
	cost := a.AdmissionCostCeiling.Clone()
	service := func(kind domain.ExternalServiceKindV1) *domain.ExternalServiceCostCeilingV1 {
		zero := uint64(0)
		return &domain.ExternalServiceCostCeilingV1{Version: 1, Kind: kind, TenantID: tenant, OwnerUserID: owner, BillingOwnerUserID: owner, ResourceID: string(kind) + "-resource", ResourceRevision: 1, RevocationGeneration: 1, ProfileID: string(kind) + "-profile", ProfileRevision: 1, ScopedGrantDigest: h, Currency: cost.Currency, PriceRevision: "synthetic-price.v1", PriceObservedAt: cost.PriceObservedAt, PriceExpiresAt: cost.PriceExpiresAt, PriceEvidenceDigest: h, TermsEvidenceDigest: h, PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: &zero}
	}
	cost.MCPToolCost, cost.WebSearchCost = service(domain.ExternalServiceMCPToolV1), service(domain.ExternalServiceWebSearchV1)
	cd, err := cost.Digest()
	if err != nil {
		t.Fatal(err)
	}
	substrate := a.SubstrateBinding
	substrate.Kind, substrate.Region, substrate.ImageDigest = domain.SubstrateLocalDevelopmentV1, "", ""
	substrate.LocalHostIdentityDigest, substrate.LocalNetworkNamespaceIdentityDigest = id.LocalHostIdentityDigest, id.LocalNetworkNamespaceIdentityDigest
	substrate.OuterHarnessArtifactDigest, substrate.AdmissionCostCeilingDigest, substrate.ProfileEvidenceExpiresAt = id.WorkerExecutableDigest, cd, expires
	backend := a.HarnessBinding.Backend
	backend.BackendKind, backend.ProviderContractKind, backend.NativeProtocolVersion = domain.HarnessBackendDirectOpenAICompatibleV1, domain.ProviderContractConfiguredEndpointV1, domain.ConfiguredInferenceProtocolChatCompletionsV1
	backend.BackendProfileDigest = ""
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceLocalEndpointV1, OwnerUserID: owner, CredentialMode: domain.ProviderCredentialNoneV1}
	limits := domain.AgentLoopLimitsV1{MaxConcurrentOperations: 1, MaxModelCalls: 8, MaxCompactionCalls: 2, MaxToolDispatches: 4, MaxWebSearches: 2, MaxInputTokensPerCall: 32768, MaxAggregateInputTokens: 131072, MaxOutputTokensPerCall: 2048, MaxAggregateOutputTokens: 16384, MaxActiveDuration: 120 * time.Second, MaxNetworkOperationDuration: 30 * time.Second, MaxCleanupDuration: 30 * time.Second, MaxContextViewBytes: 1 << 20, MaxSummaryBytes: 16 << 10, MaxToolArgumentBytes: 4 << 10, MaxToolResultBytes: 64 << 10, MaxMCPOutboundMessages: 12, MaxMCPCatalogPages: 1, MaxMCPInboundFrames: 128, MaxMCPInboundBytes: 256 << 10, MaxOperations: 12, MaxAggregateBodyBytes: 1 << 20}
	c := domain.ConfiguredInferenceProfileV1{Version: 1, TenantID: tenant, OwnerUserID: owner, Resource: resource, BaseURL: "http://127.0.0.1:11434/v1", EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1, NativeProtocolVersion: domain.ConfiguredInferenceProtocolChatCompletionsV1, Auth: domain.ConfiguredInferenceAuthNoneV1, ModelVendorID: "synthetic", ModelID: "fit-model", ResponseModelIDs: []string{"fit-model"}, ServerID: "synthetic-server", ServerVersion: "synthetic-v1", ChatTemplateDigest: h, Capabilities: domain.ConfiguredInferenceCapabilitiesV1{Text: domain.ProviderEvidenceSupportedV1, Image: domain.ProviderEvidenceUnsupportedV1, File: domain.ProviderEvidenceUnsupportedV1, ToolCalls: domain.ProviderEvidenceSupportedV1, JSONSchema: domain.ProviderEvidenceUnknownV1}, ProviderCatalogDigest: h, ProviderRouteDigest: h, PrivacyPolicyDigest: h, EffectivePolicyDigest: h, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}, ModelWindowTokens: 65536, TokenEstimatorVersion: "synthetic-affine.v1", Limits: domain.ConfiguredInferenceLimitsV1{RequestBytes: 1 << 20, ResponseBytes: 1 << 20, ResponseHeaderBytes: 16 << 10, JSONDepth: 32, JSONMembers: 256, Choices: 1, ToolCalls: 1, ToolNameBytes: 128, ToolCallIDBytes: 128, ToolArgumentsBytes: 4 << 10, FinalTextBytes: 256 << 10, Messages: 256, Tools: 16, MaxInputTokens: 32768, MaxOutputTokens: 2048, OperationTimeout: 30 * time.Second, CleanupTimeout: 30 * time.Second}, LocalHostIdentityDigest: id.LocalHostIdentityDigest, LocalNetworkNamespaceIdentityDigest: id.LocalNetworkNamespaceIdentityDigest, WorkerExecutableDigest: id.WorkerExecutableDigest, IssuedAt: at, ExpiresAt: expires}
	terms := localoperator.InvoiceTermsV1{Version: 1, TenantID: tenant, OwnerUserID: owner, BaseURL: c.BaseURL, ModelVendorID: c.ModelVendorID, ModelID: c.ModelID, Currency: cost.Currency, Revision: cost.PriceRevision, OwnerSuppliesModel: true, OwnerSuppliesHost: true, IssuedAt: at, ExpiresAt: expires}
	termBody, err := json.Marshal(terms)
	if err != nil {
		t.Fatal(err)
	}
	invoice := fitEvidence("synthetic-invoice", termBody)
	capability := fitEvidence("synthetic-capability", []byte("synthetic capability declaration, not model qualification"))
	review := fitEvidence("synthetic-review", []byte("synthetic review fixture, not runtime acceptance"))
	c.OperatorTerms, c.CapabilityEvidence = invoice.Reference, capability.Reference
	calibrationBody, err := json.Marshal(map[string]any{"version": 1, "algorithm": "utf8_affine.v1", "input_domain": "chat_completions_text.v1", "tenant_id": tenant, "owner_user_id": owner, "model_vendor_id": c.ModelVendorID, "model_id": c.ModelID, "response_model_ids": c.ResponseModelIDs, "server_id": c.ServerID, "server_version": c.ServerVersion, "protocol": c.NativeProtocolVersion, "chat_template_digest": c.ChatTemplateDigest, "estimator_version": c.TokenEstimatorVersion, "capability_evidence_digest": capability.Reference.Digest, "model_window_tokens": c.ModelWindowTokens, "max_wire_bytes": c.Limits.RequestBytes, "max_messages": c.Limits.Messages, "max_tools": c.Limits.Tools, "byte_numerator": 3, "byte_denominator": 2, "fixed_tokens": 11, "message_tokens": 7, "tool_tokens": 13})
	if err != nil {
		t.Fatal(err)
	}
	calibration := fitEvidence("synthetic-calibration", calibrationBody)
	c.CalibrationDigest = calibration.Reference.Digest
	p := domain.OwnedAgentProfileCurrentV1{Version: 1, Locator: domain.OwnedAgentProfileLocatorV1{TenantID: tenant, OwnerUserID: owner, Profile: domain.AgentLoopOwnedPublicV1, ResourceKind: resource.Kind, ModelVendorID: c.ModelVendorID, ModelID: c.ModelID, InputDataClass: domain.ProviderDataPublicV1}, Backend: backend, Resource: resource, Substrate: substrate, ReviewReceipt: review.Reference, ProviderCatalogDigest: h, ProviderRouteDigest: h, CapabilityEvidenceDigest: capability.Reference.Digest, PrivacyPolicyDigest: h, EffectivePolicyDigest: h, ProviderPriceDigest: invoice.Reference.Digest, ProviderTermsDigest: invoice.Reference.Digest, SubstratePriceDigest: invoice.Reference.Digest, SubstrateTermsDigest: invoice.Reference.Digest, SubstratePriceState: domain.ExternalServicePriceKnownFreeV1, BillingOwnerUserID: owner, InputModalities: []domain.AgentInputModalityV1{domain.AgentInputTextV1}, ModelWindowTokens: c.ModelWindowTokens, TokenEstimatorVersion: c.TokenEstimatorVersion, CalibrationDigest: c.CalibrationDigest, InstructionVersion: "synthetic-instructions.v1", CompactionPolicyDigest: h, MCPToolGrantDigest: h, WebSearchGrantDigest: h, Limits: limits, CostCeiling: cost, Window: domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: expires}, UpdatedAt: at}
	key := bytes.Repeat([]byte{7}, 32)
	m := localoperator.ManifestV1{Version: 1, KeyFingerprint: fitDigest(key), Configuration: c, Profile: p, InvoiceTerms: terms, Evidence: []localoperator.EvidenceBodyV1{invoice, capability, review, calibration}}
	writeFitManifest(t, root, m)
	if err := os.WriteFile(filepath.Join(root, "source.key"), key, 0o600); err != nil {
		t.Fatal(err)
	}
	return root, exe, m
}

func openFitSource(t *testing.T, ctx context.Context, root, exe string) *localoperator.Source {
	t.Helper()
	s, err := localoperator.OpenFresh(ctx, root, exe)
	if err != nil {
		t.Fatalf("genuine synthetic file intake: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func freezeFitRequest(t *testing.T, s *localoperator.Source) *configuredinference.FrozenRequestV1 {
	t.Helper()
	p, err := s.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	f, err := configuredinference.FreezeRequestV1(p, p.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleSystemV1, Text: "trusted synthetic instruction"}, {Role: configuredinference.RoleUserV1, Text: "Проверка \\\" wire"}}, Tools: []configuredinference.ToolV1{{Name: "test_tool", Description: "synthetic tool", Parameters: []byte(`{"type":"object","properties":{},"required":[],"additionalProperties":false}`)}}, MaxOutputTokens: 32})
	if err != nil {
		t.Fatalf("actual freeze: %v", err)
	}
	return f
}

func fitCeilings() configuredinference.ContextFitCeilingsV1 {
	return configuredinference.ContextFitCeilingsV1{InputTokens: 32768, OutputTokens: 2048, TotalTokens: 65536}
}

func TestSourceContextFitRetainedBodyAndExactFrozenRequest(t *testing.T) {
	root, exe, m := sourceFitFiles(t)
	s := openFitSource(t, t.Context(), root, exe)
	f := freezeFitRequest(t, s)
	got, err := s.FitContext(f, fitCeilings())
	want := (uint64(len(f.Body()))*3+1)/2 + 11 + 2*7 + 13
	if err != nil || got.RequestDigest != f.Digest() || got.CalibrationDigest != m.Configuration.CalibrationDigest || got.InputUpperBound != want || got.OutputReserve != 32 || got.TotalUpperBound != want+32 {
		t.Fatalf("fit got=%+v want_input=%d err=%v", got, want, err)
	}
	// Caller snapshots and later files do not replace the original retained body.
	clear(f.Body())
	m.Evidence = m.Evidence[:3]
	writeFitManifest(t, root, m)
	again, err := s.FitContext(f, fitCeilings())
	if err != nil || again != got {
		t.Fatalf("retained source changed after caller/file mutation: got=%+v err=%v", again, err)
	}
	ceilings := fitCeilings()
	ceilings.InputTokens = want - 1
	if result, err := s.FitContext(f, ceilings); !errors.Is(err, configuredinference.ErrContextNoFit) || result != (configuredinference.ContextFitV1{}) {
		t.Fatalf("genuine no-fit result=%+v err=%v", result, err)
	}
}

func TestSourceContextFitMissingDuplicateForeignAndOversize(t *testing.T) {
	for _, name := range []string{"missing", "duplicate", "foreign", "oversize", "wrong_hash"} {
		t.Run(name, func(t *testing.T) {
			root, exe, m := sourceFitFiles(t)
			switch name {
			case "missing":
				m.Evidence = m.Evidence[:3]
			case "duplicate":
				duplicate := m.Evidence[3]
				duplicate.Reference.Ref = "another-calibration-reference"
				m.Evidence = append(m.Evidence, duplicate)
			case "foreign":
				m.Evidence[3] = fitEvidence("synthetic-calibration", []byte(strings.Replace(string(m.Evidence[3].Body), `"server_id":"synthetic-server"`, `"server_id":"foreign-server"`, 1)))
			case "oversize":
				m.Evidence[3] = fitEvidence("synthetic-calibration", bytes.Repeat([]byte{'x'}, configuredinference.MaxContextCalibrationBytesV1+1))
			case "wrong_hash":
				m.Evidence[3].Body = []byte("body does not match the retained reference")
			}
			if name == "foreign" || name == "oversize" {
				m.Configuration.CalibrationDigest = m.Evidence[3].Reference.Digest
				m.Profile.CalibrationDigest = m.Configuration.CalibrationDigest
			}
			writeFitManifest(t, root, m)
			if name == "wrong_hash" {
				if s, err := localoperator.OpenFresh(t.Context(), root, exe); !errors.Is(err, localoperator.ErrDenied) || s != nil {
					if s != nil {
						s.Close()
					}
					t.Fatalf("wrong hash accepted at file intake: %v", err)
				}
				return
			}
			s := openFitSource(t, t.Context(), root, exe) // legacy intake is intentionally still valid
			f := freezeFitRequest(t, s)
			ceilings := fitCeilings()
			ceilings.InputTokens = 1
			if got, err := s.FitContext(f, ceilings); !errors.Is(err, localoperator.ErrDenied) || got != (configuredinference.ContextFitV1{}) {
				t.Fatalf("%s became fit/no-fit: got=%+v err=%v", name, got, err)
			}
		})
	}
}

func TestSourceContextFitOriginalIdentityAndLifetime(t *testing.T) {
	root, exe, _ := sourceFitFiles(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := openFitSource(t, ctx, root, exe)
	// Fresh intake has not used Source.mu. Copy BEFORE the first Source method:
	// an already-used sync.Mutex must not be copied, even while quiescent.
	// The copy retains the original self identity, not a valid new Source.
	copied := new(localoperator.Source)
	reflect.ValueOf(copied).Elem().Set(reflect.ValueOf(s).Elem())
	f := freezeFitRequest(t, s)
	assertDenied := func(name string, source *localoperator.Source, request *configuredinference.FrozenRequestV1, ceilings configuredinference.ContextFitCeilingsV1) {
		t.Helper()
		if got, err := source.FitContext(request, ceilings); !errors.Is(err, localoperator.ErrDenied) || got != (configuredinference.ContextFitV1{}) {
			t.Fatalf("%s got=%+v err=%v", name, got, err)
		}
	}
	assertDenied("nil source", nil, f, fitCeilings())
	assertDenied("zero source", new(localoperator.Source), f, fitCeilings())
	assertDenied("copied source", copied, f, fitCeilings())
	if _, err := s.FitContext(f, fitCeilings()); err != nil {
		t.Fatalf("original source denied after rejected copy: %v", err)
	}
	assertDenied("nil request", s, nil, fitCeilings())
	assertDenied("zero ceilings", s, f, configuredinference.ContextFitCeilingsV1{})
	p, err := s.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	p.ServerVersion = "foreign-version"
	foreign, err := configuredinference.FreezeRequestV1(p, p.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "foreign"}}, MaxOutputTokens: 32})
	if err != nil {
		t.Fatal(err)
	}
	assertDenied("foreign frozen profile", s, foreign, fitCeilings())
	cancel()
	assertDenied("parent cancelled", s, f, fitCeilings())
	root2, exe2, _ := sourceFitFiles(t)
	s2 := openFitSource(t, t.Context(), root2, exe2)
	f2 := freezeFitRequest(t, s2)
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	assertDenied("closed", s2, f2, fitCeilings())
}

func TestSourceContextFitExclusiveExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root, exe, _ := sourceFitFiles(t)
		s := openFitSource(t, t.Context(), root, exe)
		f := freezeFitRequest(t, s)
		p, err := s.Configuration()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.FitContext(f, fitCeilings()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Until(p.ExpiresAt)) // advances the sole virtual clock to the exclusive boundary
		synctest.Wait()
		if got, err := s.FitContext(f, fitCeilings()); !errors.Is(err, localoperator.ErrDenied) || got != (configuredinference.ContextFitV1{}) {
			t.Fatalf("expired source got=%+v err=%v", got, err)
		}
	})
}
