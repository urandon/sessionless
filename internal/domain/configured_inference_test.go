package domain_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func configuredInferenceProfileFixture(t testing.TB) domain.ConfiguredInferenceProfileV1 {
	t.Helper()
	hash := strings.Repeat("1", 64)
	ref := domain.OwnedAgentAuthorityBodyRefV1{Ref: "synthetic-local-evidence", Digest: hash, Bytes: 32}
	v := domain.ConfiguredInferenceProfileV1{
		Version: 1, TenantID: "local-tenant", OwnerUserID: "local-owner", ProfileRevision: 1,
		Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceLocalEndpointV1, ResourceID: "synthetic-local-resource", OwnerUserID: "local-owner", Revision: 1, CredentialMode: domain.ProviderCredentialNoneV1},
		BaseURL:  "http://127.0.0.1:11434/v1", EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1,
		NativeProtocolVersion: domain.ConfiguredInferenceProtocolChatCompletionsV1, Auth: domain.ConfiguredInferenceAuthNoneV1,
		ModelVendorID: "synthetic-vendor", ModelID: "synthetic-model", ResponseModelIDs: []string{"synthetic-model"},
		ServerID: "synthetic-server", ServerVersion: "synthetic-version", ChatTemplateDigest: hash,
		Capabilities:       domain.ConfiguredInferenceCapabilitiesV1{Text: domain.ProviderEvidenceSupportedV1, Image: domain.ProviderEvidenceUnsupportedV1, File: domain.ProviderEvidenceUnsupportedV1, ToolCalls: domain.ProviderEvidenceSupportedV1, JSONSchema: domain.ProviderEvidenceUnknownV1},
		CapabilityEvidence: ref, ProviderCatalogDigest: hash, ProviderRouteDigest: hash, PrivacyPolicyDigest: hash, EffectivePolicyDigest: hash,
		AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}, ModelWindowTokens: 4096, TokenEstimatorVersion: "synthetic-estimator", CalibrationDigest: hash,
		Limits:              domain.ConfiguredInferenceLimitsV1{RequestBytes: 1 << 20, ResponseBytes: 1 << 20, ResponseHeaderBytes: 16 << 10, JSONDepth: 32, JSONMembers: 256, Choices: 1, ToolCalls: 1, ToolNameBytes: 128, ToolCallIDBytes: 128, ToolArgumentsBytes: 4 << 10, FinalTextBytes: 256 << 10, Messages: 256, Tools: 16, MaxInputTokens: 1024, MaxOutputTokens: 1024, OperationTimeout: 30 * time.Second, CleanupTimeout: 30 * time.Second},
		LocalInstallationID: "synthetic-installation", LocalInstallationRevision: 1, LocalHostIdentityDigest: hash, LocalNetworkNamespaceIdentityDigest: hash, WorkerExecutableDigest: hash,
		OperatorTerms: ref, OriginalSource: ref, IssuedAt: time.Unix(100, 0).UTC(), ExpiresAt: time.Unix(100, 0).UTC().Add(time.Hour),
	}
	if err := v.Validate(); err != nil {
		t.Fatalf("synthetic local profile invalid: %v", err)
	}
	return v
}

func TestConfiguredInferenceEndpointAcceptsOnlyCanonicalLiteralLoopback(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"http://127.0.0.1:1/v1", "http://127.0.0.1:65535/v1", "http://[::1]:11434/v1"} {
		if err := domain.ValidateConfiguredInferenceEndpointV1(value); err != nil {
			t.Errorf("canonical endpoint %q: %v", value, err)
		}
	}
	for _, value := range []string{
		"", "http://localhost:11434/v1", "https://127.0.0.1:11434/v1", "HTTP://127.0.0.1:11434/v1",
		"http://127.0.0.1/v1", "http://127.0.0.1:0/v1", "http://127.0.0.1:011434/v1", "http://127.0.0.1:01/v1", "http://127.0.0.1:65536/v1",
		"http://127.0.0.1:+1/v1", "http://127.0.0.1:-1/v1", "http://127.0.0.1:１２/v1", "http://127.0.0.1:1 /v1",
		"http://user@127.0.0.1:11434/v1", "http://127.0.0.1:11434@evil.test/v1", "http://127.0.0.1:11434/v1/",
		"http://127.0.0.1:11434/v1?x=1", "http://127.0.0.1:11434/v1#x", "http://127.0.0.1:11434/./v1", "http://127.0.0.1:11434/x/../v1",
		"http://127.0.0.1:11434/%76%31", "http://127.0.0.1:11434\\v1", " http://127.0.0.1:11434/v1", "http://127.0.0.1:11434/v1\n",
		"http://127.0.0.2:11434/v1", "http://127.1:11434/v1", "http://2130706433:11434/v1", "http://0x7f000001:11434/v1", "http://169.254.169.254:80/v1",
		"http://[0:0:0:0:0:0:0:1]:11434/v1", "http://[::ffff:127.0.0.1]:11434/v1", "http://[::1%25lo0]:11434/v1", "unix:///v1",
	} {
		if err := domain.ValidateConfiguredInferenceEndpointV1(value); err == nil {
			t.Errorf("hostile endpoint %q accepted without normalization", value)
		}
	}
}

func TestConfiguredInferenceAuthDoesNotConvertLocalResourceToFixture(t *testing.T) {
	t.Parallel()
	base := configuredInferenceProfileFixture(t)
	bearer := base.Clone()
	bearer.Auth = domain.ConfiguredInferenceAuthBearerV1
	bearer.Resource.CredentialMode = domain.ProviderCredentialInvocationV1
	bearer.Resource.CredentialGeneration = 1
	if err := bearer.Validate(); err != nil {
		t.Fatalf("explicit local bearer tuple rejected: %v", err)
	}
	for name, mutate := range map[string]func(*domain.ConfiguredInferenceProfileV1){
		"none with generation": func(v *domain.ConfiguredInferenceProfileV1) { v.Resource.CredentialGeneration = 1 },
		"none with invocation": func(v *domain.ConfiguredInferenceProfileV1) {
			v.Resource.CredentialMode = domain.ProviderCredentialInvocationV1
		},
		"bearer without generation": func(v *domain.ConfiguredInferenceProfileV1) { v.Auth = domain.ConfiguredInferenceAuthBearerV1 },
		"fixture identity": func(v *domain.ConfiguredInferenceProfileV1) {
			v.Resource.Kind = domain.ProviderResourceCredentiallessFixtureV1
		},
		"api account identity": func(v *domain.ConfiguredInferenceProfileV1) { v.Resource.Kind = domain.ProviderResourceAPIAccountV1 },
		"foreign owner":        func(v *domain.ConfiguredInferenceProfileV1) { v.Resource.OwnerUserID = "foreign-owner" },
		"ambient auth":         func(v *domain.ConfiguredInferenceProfileV1) { v.Auth = "environment" },
		"responses":            func(v *domain.ConfiguredInferenceProfileV1) { v.NativeProtocolVersion = "openai-responses.v1" },
		"stream": func(v *domain.ConfiguredInferenceProfileV1) {
			v.NativeProtocolVersion = "openai-chat-completions-stream.v1"
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := base.Clone()
			mutate(&v)
			if err := v.Validate(); err == nil {
				t.Error("unsupported profile accepted")
			}
		})
	}
}

func TestConfiguredInferenceBoundsAndOriginalExclusiveExpiry(t *testing.T) {
	t.Parallel()
	base := configuredInferenceProfileFixture(t)
	limitType := reflect.TypeOf(base.Limits)
	for i := 0; i < limitType.NumField(); i++ {
		t.Run("zero_limit_"+limitType.Field(i).Name, func(t *testing.T) {
			v := base.Clone()
			reflect.ValueOf(&v.Limits).Elem().Field(i).SetZero()
			if err := v.Validate(); err == nil {
				t.Error("zero limit accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*domain.ConfiguredInferenceProfileV1){
		"request":                       func(v *domain.ConfiguredInferenceProfileV1) { v.Limits.RequestBytes++ },
		"response":                      func(v *domain.ConfiguredInferenceProfileV1) { v.Limits.ResponseBytes++ },
		"headers":                       func(v *domain.ConfiguredInferenceProfileV1) { v.Limits.ResponseHeaderBytes++ },
		"nesting":                       func(v *domain.ConfiguredInferenceProfileV1) { v.Limits.JSONDepth++ },
		"members":                       func(v *domain.ConfiguredInferenceProfileV1) { v.Limits.JSONMembers++ },
		"choices":                       func(v *domain.ConfiguredInferenceProfileV1) { v.Limits.Choices++ },
		"multiple calls":                func(v *domain.ConfiguredInferenceProfileV1) { v.Limits.ToolCalls++ },
		"arguments":                     func(v *domain.ConfiguredInferenceProfileV1) { v.Limits.ToolArgumentsBytes++ },
		"output tokens":                 func(v *domain.ConfiguredInferenceProfileV1) { v.Limits.MaxOutputTokens = 2049 },
		"operation":                     func(v *domain.ConfiguredInferenceProfileV1) { v.Limits.OperationTimeout++ },
		"cleanup":                       func(v *domain.ConfiguredInferenceProfileV1) { v.Limits.CleanupTimeout = 0 },
		"unknown context fit":           func(v *domain.ConfiguredInferenceProfileV1) { v.ModelWindowTokens = 0 },
		"input output context overflow": func(v *domain.ConfiguredInferenceProfileV1) { v.ModelWindowTokens = 2047 },
		"missing evidence": func(v *domain.ConfiguredInferenceProfileV1) {
			v.CapabilityEvidence = domain.OwnedAgentAuthorityBodyRefV1{}
		},
		"unknown text":       func(v *domain.ConfiguredInferenceProfileV1) { v.Capabilities.Text = domain.ProviderEvidenceUnknownV1 },
		"invalid capability": func(v *domain.ConfiguredInferenceProfileV1) { v.Capabilities.ToolCalls = "enabled" },
		"model duplicates": func(v *domain.ConfiguredInferenceProfileV1) {
			v.ResponseModelIDs = append(v.ResponseModelIDs, v.ResponseModelIDs[0])
		},
		"model order":      func(v *domain.ConfiguredInferenceProfileV1) { v.ResponseModelIDs = []string{"z", "a"} },
		"missing source":   func(v *domain.ConfiguredInferenceProfileV1) { v.OriginalSource.Ref = "" },
		"submicrosecond":   func(v *domain.ConfiguredInferenceProfileV1) { v.ExpiresAt = v.ExpiresAt.Add(time.Nanosecond) },
		"unbounded window": func(v *domain.ConfiguredInferenceProfileV1) { v.ExpiresAt = v.IssuedAt.Add(25 * time.Hour) },
	} {
		t.Run(name, func(t *testing.T) {
			v := base.Clone()
			mutate(&v)
			if err := v.Validate(); err == nil {
				t.Error("unbounded or malformed profile accepted")
			}
		})
	}
	for _, at := range []time.Time{base.IssuedAt, base.ExpiresAt.Add(-time.Microsecond)} {
		if err := base.ValidateAt(at); err != nil {
			t.Errorf("valid original window instant %v: %v", at, err)
		}
	}
	for _, at := range []time.Time{{}, base.IssuedAt.Add(-time.Microsecond), base.ExpiresAt, base.ExpiresAt.Add(time.Microsecond)} {
		if err := base.ValidateAt(at); err == nil {
			t.Errorf("outside original window instant %v accepted", at)
		}
	}
}

func TestConfiguredInferenceCanonicalCodecCloneAndEveryFieldSealed(t *testing.T) {
	t.Parallel()
	base := configuredInferenceProfileFixture(t)
	raw, err := domain.EncodeConfiguredInferenceProfileV1(base)
	if err != nil {
		t.Fatal(err)
	}
	got, err := domain.DecodeConfiguredInferenceProfileV1(raw)
	if err != nil || !reflect.DeepEqual(got, base) {
		t.Fatalf("canonical round trip: equal=%t err=%v", reflect.DeepEqual(got, base), err)
	}
	digest, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	clone := base.Clone()
	clone.ResponseModelIDs[0] = "changed-model"
	clone.AllowedDataClasses[0] = domain.ProviderDataPrivateV1
	if base.ResponseModelIDs[0] != "synthetic-model" || base.AllowedDataClasses[0] != domain.ProviderDataPublicV1 {
		t.Fatal("clone aliases installation configuration")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != reflect.TypeOf(base).NumField() {
		t.Fatalf("encoded fields=%d want=%d", len(fields), reflect.TypeOf(base).NumField())
	}
	for name := range fields {
		t.Run("missing_"+name, func(t *testing.T) {
			var altered map[string]json.RawMessage
			if err := json.Unmarshal(raw, &altered); err != nil {
				t.Fatal(err)
			}
			delete(altered, name)
			b, err := json.Marshal(altered)
			if err != nil {
				t.Fatal(err)
			}
			if value, err := domain.DecodeConfiguredInferenceProfileV1(b); err == nil || !reflect.DeepEqual(value, domain.ConfiguredInferenceProfileV1{}) {
				t.Errorf("missing field yielded usable profile: err=%v", err)
			}
		})
	}
	for name, mutate := range map[string]func(*domain.ConfiguredInferenceProfileV1){
		"endpoint": func(v *domain.ConfiguredInferenceProfileV1) { v.BaseURL = "http://[::1]:11434/v1" },
		"model":    func(v *domain.ConfiguredInferenceProfileV1) { v.ModelID = "new-model" },
		"auth": func(v *domain.ConfiguredInferenceProfileV1) {
			v.Auth = domain.ConfiguredInferenceAuthBearerV1
			v.Resource.CredentialMode = domain.ProviderCredentialInvocationV1
			v.Resource.CredentialGeneration = 1
		},
		"source": func(v *domain.ConfiguredInferenceProfileV1) { v.OriginalSource.Digest = strings.Repeat("2", 64) },
		"terms":  func(v *domain.ConfiguredInferenceProfileV1) { v.OperatorTerms.Bytes++ },
		"host":   func(v *domain.ConfiguredInferenceProfileV1) { v.LocalHostIdentityDigest = strings.Repeat("2", 64) },
		"namespace": func(v *domain.ConfiguredInferenceProfileV1) {
			v.LocalNetworkNamespaceIdentityDigest = strings.Repeat("2", 64)
		},
		"worker":   func(v *domain.ConfiguredInferenceProfileV1) { v.WorkerExecutableDigest = strings.Repeat("2", 64) },
		"template": func(v *domain.ConfiguredInferenceProfileV1) { v.ChatTemplateDigest = strings.Repeat("2", 64) },
		"capability": func(v *domain.ConfiguredInferenceProfileV1) {
			v.Capabilities.ToolCalls = domain.ProviderEvidenceUnsupportedV1
		},
		"expiry": func(v *domain.ConfiguredInferenceProfileV1) { v.ExpiresAt = v.ExpiresAt.Add(-time.Minute) },
	} {
		t.Run("digest_"+name, func(t *testing.T) {
			v := base.Clone()
			mutate(&v)
			changed, err := v.Digest()
			if err != nil || changed == digest {
				t.Errorf("valid mutation not sealed: digest=%q err=%v", changed, err)
			}
		})
	}
	invalid := [][]byte{nil, []byte("null"), append(bytes.Clone(raw), '\n'), append(bytes.Clone(raw), []byte("{}")...), bytes.Repeat([]byte("x"), domain.OwnedAgentCurrentMaxRecordBytesV1+1), bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1), bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"secret":"forbidden"`), 1), bytes.Replace(raw, []byte(`"synthetic-model"`), []byte{'"', 0xff, '"'}, 1)}
	for i, b := range invalid {
		if value, err := domain.DecodeConfiguredInferenceProfileV1(b); err == nil || !reflect.DeepEqual(value, domain.ConfiguredInferenceProfileV1{}) {
			t.Errorf("malformed case %d returned usable profile: err=%v", i, err)
		}
	}
}

func configuredInferenceBindingFixture(t testing.TB, p domain.ConfiguredInferenceProfileV1) (domain.HarnessBindingV2, domain.SubstrateBindingV1) {
	t.Helper()
	managed := deterministicManagedAuthority(t, p.TenantID, p.OwnerUserID, "local-run", "local-attempt", p.IssuedAt)
	b, s := managed.HarnessBinding, managed.SubstrateBinding
	b.Profile, b.AgentLoopPolicy = domain.HarnessExecutionSingleTurnV2, nil
	b.InputDataClass = domain.ProviderDataPublicV1
	b.Resource, b.ModelVendorID, b.ModelID = p.Resource, p.ModelVendorID, p.ModelID
	b.Backend.BackendKind, b.Backend.ArtifactKind = domain.HarnessBackendDirectOpenAICompatibleV1, domain.HarnessArtifactEmbeddedProfileV1
	b.Backend.ProviderContractKind, b.Backend.NativeProtocolVersion = domain.ProviderContractConfiguredEndpointV1, p.NativeProtocolVersion
	b.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryNoneV1
	if p.Auth == domain.ConfiguredInferenceAuthBearerV1 {
		b.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryDirectV1
	}
	b.Backend.BackendProfileDigest, _ = p.Digest()
	b.ProviderCatalogDigest, b.ProviderRouteDigest, b.PrivacyPolicyDigest = p.ProviderCatalogDigest, p.ProviderRouteDigest, p.PrivacyPolicyDigest
	b.CapabilityEvidenceDigest, b.EffectivePolicyDigest = p.CapabilityEvidence.Digest, p.EffectivePolicyDigest
	expiry := p.ExpiresAt
	b.EvidenceExpiresAt = &expiry
	s.Kind, s.WorkloadMode, s.ImageDigest, s.Region = domain.SubstrateLocalDevelopmentV1, domain.SubstrateWorkloadInProcessDirectV1, "", ""
	s.OuterHarnessArtifactDigest, s.LocalInstallationID, s.LocalInstallationRevision = p.WorkerExecutableDigest, p.LocalInstallationID, p.LocalInstallationRevision
	s.LocalHostIdentityDigest, s.LocalNetworkNamespaceIdentityDigest, s.ProfileEvidenceExpiresAt = p.LocalHostIdentityDigest, p.LocalNetworkNamespaceIdentityDigest, p.ExpiresAt
	return b, s
}

func TestConfiguredInferenceBindingAndPlacementAreExact(t *testing.T) {
	t.Parallel()
	p := configuredInferenceProfileFixture(t)
	b, s := configuredInferenceBindingFixture(t, p)
	if err := p.ValidateForBinding(b, s, p.IssuedAt); err != nil {
		t.Fatalf("exact local association rejected: %v", err)
	}
	bearer := p.Clone()
	bearer.Auth = domain.ConfiguredInferenceAuthBearerV1
	bearer.Resource.CredentialMode = domain.ProviderCredentialInvocationV1
	bearer.Resource.CredentialGeneration = 2
	bb, bs := configuredInferenceBindingFixture(t, bearer)
	if err := bearer.ValidateForBinding(bb, bs, p.IssuedAt); err != nil {
		t.Fatalf("exact direct bearer association rejected: %v", err)
	}
	bb.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
	if err := bearer.ValidateForBinding(bb, bs, p.IssuedAt); err == nil {
		t.Error("bearer file-delivery conversion accepted")
	}
	for name, mutate := range map[string]func(*domain.HarnessBindingV2, *domain.SubstrateBindingV1){
		"model": func(b *domain.HarnessBindingV2, _ *domain.SubstrateBindingV1) { b.ModelID = "different-model" },
		"profile": func(b *domain.HarnessBindingV2, _ *domain.SubstrateBindingV1) {
			b.Backend.BackendProfileDigest = strings.Repeat("2", 64)
		},
		"owner":             func(b *domain.HarnessBindingV2, _ *domain.SubstrateBindingV1) { b.OwnerUserID = "different-owner" },
		"resource revision": func(b *domain.HarnessBindingV2, _ *domain.SubstrateBindingV1) { b.Resource.Revision++ },
		"evidence": func(b *domain.HarnessBindingV2, _ *domain.SubstrateBindingV1) {
			b.CapabilityEvidenceDigest = strings.Repeat("2", 64)
		},
		"fixture": func(b *domain.HarnessBindingV2, _ *domain.SubstrateBindingV1) {
			b.Resource.Kind = domain.ProviderResourceCredentiallessFixtureV1
		},
		"worker executable": func(_ *domain.HarnessBindingV2, s *domain.SubstrateBindingV1) {
			s.OuterHarnessArtifactDigest = strings.Repeat("2", 64)
		},
		"host": func(_ *domain.HarnessBindingV2, s *domain.SubstrateBindingV1) {
			s.LocalHostIdentityDigest = strings.Repeat("2", 64)
		},
		"namespace": func(_ *domain.HarnessBindingV2, s *domain.SubstrateBindingV1) {
			s.LocalNetworkNamespaceIdentityDigest = strings.Repeat("2", 64)
		},
		"installation": func(_ *domain.HarnessBindingV2, s *domain.SubstrateBindingV1) { s.LocalInstallationRevision++ },
		"fabricated image": func(_ *domain.HarnessBindingV2, s *domain.SubstrateBindingV1) {
			s.ImageDigest = strings.Repeat("2", 64)
		},
		"cloud placement": func(_ *domain.HarnessBindingV2, s *domain.SubstrateBindingV1) {
			s.Kind = domain.SubstrateYandexServerlessContainerV1
		},
		"extended expiry": func(b *domain.HarnessBindingV2, _ *domain.SubstrateBindingV1) {
			x := p.ExpiresAt.Add(time.Second)
			b.EvidenceExpiresAt = &x
		},
	} {
		t.Run(name, func(t *testing.T) {
			b2, s2 := b.Clone(), s
			mutate(&b2, &s2)
			if err := p.ValidateForBinding(b2, s2, p.IssuedAt); err == nil {
				t.Error("mismatched local association accepted")
			}
		})
	}
}
