package domain_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func ownedCurrentProfileFixture(t testing.TB) (domain.OwnedAgentProfileCurrentV1, time.Time) {
	t.Helper()
	at := time.Date(2026, 10, 11, 3, 0, 0, 1000, time.UTC)
	policy := agentLoopPolicyFixture()
	a := deterministicManagedAuthority(t, policy.TenantID, policy.OwnerUserID, policy.RunID, policy.AttemptID, at)
	cost := withOwnedServiceCostFixture(t, a.AdmissionCostCeiling, policy)
	substrate := a.SubstrateBinding
	substrate.AdmissionCostCeilingDigest, _ = cost.Digest()
	digest := strings.Repeat("a", 64)
	p := domain.OwnedAgentProfileCurrentV1{
		Version: 1, Locator: domain.OwnedAgentProfileLocatorV1{TenantID: policy.TenantID, OwnerUserID: policy.OwnerUserID, Profile: domain.AgentLoopOwnedPublicV1, BackendProfileDigest: a.HarnessBinding.Backend.BackendProfileDigest, ResourceKind: a.HarnessBinding.Resource.Kind, ResourceID: a.HarnessBinding.Resource.ResourceID, ModelVendorID: "fixture-vendor", ModelID: a.HarnessBinding.ModelID, InputDataClass: domain.ProviderDataPublicV1},
		ProfileRevision: 1, RevocationGeneration: 1, State: domain.OwnedAgentCurrentActiveV1,
		Backend: a.HarnessBinding.Backend, Resource: a.HarnessBinding.Resource, Substrate: substrate,
		InstallationID: "fixture-installation", InstallationRevision: 1,
		InstalledManifest: domain.OwnedAgentAuthorityBodyRefV1{Ref: "fixture-manifest", Digest: digest, Bytes: 123}, ReviewReceipt: domain.OwnedAgentAuthorityBodyRefV1{Ref: "reviewed-fixture", Digest: strings.Repeat("b", 64), Bytes: 456},
		ProviderCatalogDigest: digest, ProviderRouteDigest: digest, CapabilityEvidenceDigest: digest, PrivacyPolicyDigest: digest, EffectivePolicyDigest: digest, ProviderPriceDigest: digest, ProviderTermsDigest: digest, SubstratePriceDigest: strings.Repeat("c", 64), SubstrateTermsDigest: strings.Repeat("d", 64),
		SubstratePriceState: domain.ExternalServicePriceKnownFreeV1,
		BillingOwnerUserID:  cost.MCPToolCost.BillingOwnerUserID, InputModalities: []domain.AgentInputModalityV1{domain.AgentInputTextV1}, ModelWindowTokens: 65536, TokenEstimatorVersion: "fixture-estimator.v1", CalibrationDigest: digest, InstructionVersion: "fixture-instructions.v1", CompactionPolicyDigest: digest, MCPToolGrantDigest: policy.MCPToolGrantDigest, WebSearchGrantDigest: policy.WebSearchGrantDigest,
		Limits: policy.Limits, CostCeiling: cost, Window: domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: at.Add(time.Hour)}, UpdatedAt: at, MutationID: "install-fixture-1",
	}
	if err := p.ValidateInitial(); err != nil {
		t.Fatalf("valid fixture profile: %v", err)
	}
	if err := p.ValidateAt(at); err != nil {
		t.Fatalf("current fixture profile: %v", err)
	}
	return p, at
}

func bindOwnedSpendingSource(t testing.TB, v *domain.AttemptSpendingAuthorizationCurrentV1) {
	t.Helper()
	raw, err := v.SourceCanonicalBytes()
	if err != nil {
		t.Fatalf("encode independent fixture source: %v", err)
	}
	hash := sha256.Sum256(raw)
	v.SourceAuthorization = domain.OwnedAgentAuthorityBodyRefV1{Ref: "original-attempt-fixture-source", Digest: hex.EncodeToString(hash[:]), Bytes: uint64(len(raw))}
}

func ownedSpendingFixture(t testing.TB, p domain.OwnedAgentProfileCurrentV1, kind domain.AttemptSpendingComponentKindV1) domain.AttemptSpendingAuthorizationCurrentV1 {
	t.Helper()
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	v := domain.AttemptSpendingAuthorizationCurrentV1{
		Version: 1, Locator: domain.AttemptSpendingAuthorizationLocatorV1{TenantID: p.Locator.TenantID, AttemptID: "attempt-189", ComponentKind: kind, BillingOwnerUserID: p.BillingOwnerUserID}, RunID: "run-189", SessionID: "session-189", OwnerUserID: p.Locator.OwnerUserID,
		SourceKind: domain.AttemptSpendingFixtureSourceV1, SourceInstallationID: p.InstallationID, SourceInstallationRevision: p.InstallationRevision, SourceProfile: domain.OwnedAgentProfileReferenceV1{Locator: p.Locator, RevocationGeneration: p.RevocationGeneration, Digest: digest},
		ResourceID: p.Resource.ResourceID, ResourceRevision: p.Resource.Revision, ResourceGeneration: p.Resource.CredentialGeneration, ProfileID: string(p.Locator.Profile), ProfileRevision: p.ProfileRevision, ScopedGrantDigest: p.EffectivePolicyDigest, Currency: p.CostCeiling.Currency, PriceRevision: p.CostCeiling.PriceRevision, PriceEvidenceDigest: p.ProviderPriceDigest, TermsEvidenceDigest: p.ProviderTermsDigest, PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: new(uint64), RevocationGeneration: 1, State: domain.OwnedAgentCurrentActiveV1, Window: p.Window, UpdatedAt: p.UpdatedAt, MutationID: "publish-original-attempt",
	}
	switch kind {
	case domain.AttemptSpendingProviderV1:
		amount := *p.CostCeiling.MaxProviderAmountMicrounits
		v.MaxAmountMicrounits = &amount
		if p.CostCeiling.ProviderPriceState == domain.ProviderPriceKnownV1 {
			v.PriceState = domain.ExternalServicePriceKnownPaidV1
		}
	case domain.AttemptSpendingSubstrateV1:
		v.ResourceID, v.ResourceRevision, v.ResourceGeneration = p.Substrate.ProfileID, p.Substrate.ProfileRevision, p.RevocationGeneration
		v.ProfileID, v.ProfileRevision = p.Substrate.ProfileID, p.Substrate.ProfileRevision
		v.ScopedGrantDigest, v.PriceEvidenceDigest, v.TermsEvidenceDigest = p.Substrate.ProfileDigest, p.SubstratePriceDigest, p.SubstrateTermsDigest
		v.PriceState = p.SubstratePriceState
		amount := *p.CostCeiling.MaxSubstrateAmountMicrounits
		v.MaxAmountMicrounits = &amount
	case domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1:
		service := p.CostCeiling.MCPToolCost
		if kind == domain.AttemptSpendingWebSearchV1 {
			service = p.CostCeiling.WebSearchCost
		}
		v.ResourceID, v.ResourceRevision, v.ResourceGeneration, v.ProfileID, v.ProfileRevision = service.ResourceID, service.ResourceRevision, service.RevocationGeneration, service.ProfileID, service.ProfileRevision
		v.ScopedGrantDigest, v.PriceRevision, v.PriceEvidenceDigest, v.TermsEvidenceDigest, v.PriceState = service.ScopedGrantDigest, service.PriceRevision, service.PriceEvidenceDigest, service.TermsEvidenceDigest, service.PriceState
		amount := *service.MaxAmountMicrounits
		v.MaxAmountMicrounits = &amount
	}
	bindOwnedSpendingSource(t, &v)
	if err := v.ValidateInitial(); err != nil {
		t.Fatalf("valid %s source: %v", kind, err)
	}
	if err := v.ValidateAgainstProfile(p, p.Window.PublishedAt); err != nil {
		t.Fatalf("exact %s fixture source: %v", kind, err)
	}
	return v
}

func TestOwnedCurrentAllComponentsAreExactAndRevocable(t *testing.T) {
	t.Parallel()
	p, at := ownedCurrentProfileFixture(t)
	for _, kind := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1} {
		t.Run(string(kind), func(t *testing.T) {
			v := ownedSpendingFixture(t, p, kind)
			if err := v.ValidateAgainstProfile(p, at); err != nil {
				t.Fatal(err)
			}
			revoked := v.Clone()
			revoked.State = domain.OwnedAgentCurrentRevokedV1
			revoked.RevocationGeneration++
			revoked.UpdatedAt = at.Add(2 * time.Hour)
			revoked.MutationID = "revoke-original-attempt"
			if err := revoked.ValidateTransitionFrom(v); err != nil {
				t.Fatalf("historical revoke after expiry: %v", err)
			}
			if err := revoked.ValidateAt(revoked.UpdatedAt); err == nil {
				t.Error("revoked head authorized spending")
			}
			if err := revoked.ValidateTransitionFrom(revoked); err == nil {
				t.Error("exact replay interpreted as fresh transition")
			}
			for _, state := range []domain.OwnedAgentCurrentStateV1{domain.OwnedAgentCurrentDisabledV1, domain.OwnedAgentCurrentRevokedV1} {
				changed := p.Clone()
				changed.State = state
				changed.RevocationGeneration++
				changed.MutationID = "revoke-source"
				if err := changed.ValidateTransitionFrom(p); err != nil {
					t.Fatal(err)
				}
				if err := v.ValidateAgainstProfile(changed, at); err == nil {
					t.Errorf("%s source still authorized spending", state)
				}
			}
		})
	}
}

func TestOwnedCurrentSourceCannotBeReassignedOrExpanded(t *testing.T) {
	t.Parallel()
	p, at := ownedCurrentProfileFixture(t)
	base := ownedSpendingFixture(t, p, domain.AttemptSpendingWebSearchV1)
	changes := map[string]func(*domain.AttemptSpendingAuthorizationCurrentV1){
		"tenant":    func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.Locator.TenantID = "other-tenant" },
		"attempt":   func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.Locator.AttemptID = "other-attempt" },
		"run":       func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.RunID = "other-run" },
		"session":   func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.SessionID = "other-session" },
		"principal": func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.OwnerUserID = "other-owner" },
		"payer":     func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.Locator.BillingOwnerUserID = "other-payer" },
		"component": func(v *domain.AttemptSpendingAuthorizationCurrentV1) {
			v.Locator.ComponentKind = domain.AttemptSpendingMCPToolV1
		},
		"source_kind":       func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.SourceKind = "live_payer" },
		"source_generation": func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.SourceProfile.RevocationGeneration++ },
		"source_model":      func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.SourceProfile.Locator.ModelID = "other-model" },
		"source_digest": func(v *domain.AttemptSpendingAuthorizationCurrentV1) {
			v.SourceProfile.Digest = strings.Repeat("f", 64)
		},
		"resource":            func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.ResourceID = "other-resource" },
		"resource_revision":   func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.ResourceRevision++ },
		"resource_generation": func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.ResourceGeneration++ },
		"profile":             func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.ProfileID = "other-profile" },
		"profile_revision":    func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.ProfileRevision++ },
		"grant":               func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.ScopedGrantDigest = strings.Repeat("f", 64) },
		"price":               func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.PriceRevision = "different-price" },
		"terms":               func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.TermsEvidenceDigest = strings.Repeat("f", 64) },
		"amount":              func(v *domain.AttemptSpendingAuthorizationCurrentV1) { *v.MaxAmountMicrounits++ },
		"expiry": func(v *domain.AttemptSpendingAuthorizationCurrentV1) {
			v.Window.ExpiresAt = v.Window.ExpiresAt.Add(time.Microsecond)
		},
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			v := base.Clone()
			change(&v)
			if err := v.Validate(); err == nil {
				t.Fatal("changed original source accepted without new exact source bytes")
			}
			bindOwnedSpendingSource(t, &v) // Even new source bytes cannot rewrite an existing grant.
			v.State = domain.OwnedAgentCurrentRevokedV1
			v.RevocationGeneration++
			v.MutationID = "revoke-changed-source"
			if err := v.ValidateTransitionFrom(base); err == nil {
				t.Fatal("revoke allowed immutable source replacement")
			}
			if name != "attempt" && name != "run" && name != "session" {
				v.State = domain.OwnedAgentCurrentActiveV1
				if err := v.ValidateAgainstProfile(p, at); err == nil {
					t.Fatal("changed resource/profile/price/source matched original installation")
				}
			}
		})
	}
}

func TestOwnedCurrentPricePresenceAndIntegerPrecision(t *testing.T) {
	t.Parallel()
	p, at := ownedCurrentProfileFixture(t)
	base := ownedSpendingFixture(t, p, domain.AttemptSpendingWebSearchV1)
	for _, test := range []struct {
		name              string
		state             domain.ExternalServicePriceStateV1
		amount            *uint64
		valid, executable bool
	}{
		{"unknown_absent", domain.ExternalServicePriceUnknownV1, nil, true, false},
		{"unknown_zero", domain.ExternalServicePriceUnknownV1, new(uint64), false, false},
		{"free_absent", domain.ExternalServicePriceKnownFreeV1, nil, false, false},
		{"free_zero", domain.ExternalServicePriceKnownFreeV1, new(uint64), true, true},
		{"free_positive", domain.ExternalServicePriceKnownFreeV1, uint64Ptr(1), false, false},
		{"paid_zero", domain.ExternalServicePriceKnownPaidV1, new(uint64), false, false},
		{"paid_large", domain.ExternalServicePriceKnownPaidV1, uint64Ptr(1<<53 + 1), true, true},
		{"paid_max", domain.ExternalServicePriceKnownPaidV1, uint64Ptr(math.MaxUint64), true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			v := base.Clone()
			v.PriceState, v.MaxAmountMicrounits = test.state, test.amount
			bindOwnedSpendingSource(t, &v)
			if err := v.Validate(); (err == nil) != test.valid {
				t.Fatalf("valid got error %v, want %t", err, test.valid)
			}
			if err := v.ValidateAt(at); (err == nil) != test.executable {
				t.Errorf("freshness got error %v, want %t", err, test.executable)
			}
			if !test.valid {
				return
			}
			raw, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(v)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := domain.DecodeAttemptSpendingAuthorizationCurrentV1(raw)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, v.Clone()) {
				t.Error("integer/presence round trip changed the source")
			}
			if test.amount != nil && decoded.MaxAmountMicrounits == test.amount {
				t.Error("decoded amount aliases caller memory")
			}
		})
	}
}

func uint64Ptr(v uint64) *uint64 { return &v }

func TestOwnedCurrentWindowsAndProfileTransitions(t *testing.T) {
	t.Parallel()
	p, at := ownedCurrentProfileFixture(t)
	for _, test := range []struct {
		name  string
		at    time.Time
		valid bool
	}{{"before_publication", at.Add(-time.Microsecond), false}, {"publication", at, true}, {"before_expiry", p.Window.ExpiresAt.Add(-time.Microsecond), true}, {"expiry", p.Window.ExpiresAt, false}, {"other_zone", at.In(time.FixedZone("fixture", 3600)), true}, {"fine_precision", at.Add(time.Nanosecond), false}} {
		t.Run(test.name, func(t *testing.T) {
			if err := p.ValidateAt(test.at); (err == nil) != test.valid {
				t.Errorf("error %v, want valid=%t", err, test.valid)
			}
		})
	}
	revoked := p.Clone()
	revoked.State = domain.OwnedAgentCurrentRevokedV1
	revoked.RevocationGeneration++
	revoked.MutationID = "revoke-profile"
	revoked.UpdatedAt = at.Add(2 * time.Hour)
	if err := revoked.ValidateTransitionFrom(p); err != nil {
		t.Fatalf("historical revoke: %v", err)
	}
	reinstalled := revoked.Clone()
	reinstalled.State = domain.OwnedAgentCurrentActiveV1
	reinstalled.RevocationGeneration++
	reinstalled.MutationID = "old-startup"
	if err := reinstalled.ValidateTransitionFrom(revoked); err == nil {
		t.Error("old installation resurrected revoked source")
	}
	reinstalled.ProfileRevision++
	reinstalled.InstallationRevision++
	reinstalled.ReviewReceipt.Digest = strings.Repeat("e", 64)
	reinstalled.MutationID = "reviewed-new-installation"
	if err := reinstalled.ValidateTransitionFrom(revoked); err != nil {
		t.Fatalf("new reviewed structural installation: %v", err)
	}
	for _, mutate := range []func(*domain.OwnedAgentProfileCurrentV1){func(v *domain.OwnedAgentProfileCurrentV1) { v.RevocationGeneration += 2 }, func(v *domain.OwnedAgentProfileCurrentV1) { v.Locator.ModelID = "other-model" }, func(v *domain.OwnedAgentProfileCurrentV1) { v.UpdatedAt = at.Add(-time.Microsecond) }, func(v *domain.OwnedAgentProfileCurrentV1) {
		v.Window.ExpiresAt = v.Window.ExpiresAt.Add(time.Microsecond)
	}} {
		v := revoked.Clone()
		mutate(&v)
		if err := v.ValidateTransitionFrom(p); err == nil {
			t.Error("invalid/reassigned/expanded revoke accepted")
		}
	}
	overflow := p.Clone()
	overflow.RevocationGeneration = math.MaxUint64
	next := overflow.Clone()
	next.State = domain.OwnedAgentCurrentRevokedV1
	next.RevocationGeneration = 0
	next.MutationID = "overflow-revoke"
	if err := next.ValidateTransitionFrom(overflow); err == nil {
		t.Error("generation wrapped")
	}
	clone := p.Clone()
	clone.InputModalities[0] = domain.AgentInputFileV1
	*clone.CostCeiling.MCPToolCost.MaxAmountMicrounits++
	if p.InputModalities[0] != domain.AgentInputTextV1 || *p.CostCeiling.MCPToolCost.MaxAmountMicrounits != 30 {
		t.Error("clone aliases profile source")
	}
}

func TestOwnedCurrentCodecRejectsNonProducerBytes(t *testing.T) {
	t.Parallel()
	p, _ := ownedCurrentProfileFixture(t)
	v := ownedSpendingFixture(t, p, domain.AttemptSpendingWebSearchV1)
	profile, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
	if err != nil {
		t.Fatal(err)
	}
	spending, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(v)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range []struct {
		name   string
		raw    []byte
		decode func([]byte) bool
	}{
		{"profile", profile, func(raw []byte) bool {
			got, err := domain.DecodeOwnedAgentProfileCurrentV1(raw)
			if err != nil && !reflect.ValueOf(got).IsZero() {
				t.Error("profile decoder returned partial authority")
			}
			return err == nil
		}},
		{"spending", spending, func(raw []byte) bool {
			got, err := domain.DecodeAttemptSpendingAuthorizationCurrentV1(raw)
			if err != nil && !reflect.ValueOf(got).IsZero() {
				t.Error("spending decoder returned partial authority")
			}
			return err == nil
		}},
	} {
		t.Run(spec.name, func(t *testing.T) {
			if !spec.decode(spec.raw) {
				t.Fatal("exact producer bytes rejected")
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(spec.raw, &fields); err != nil {
				t.Fatal(err)
			}
			locatorMember := append([]byte(`"locator":`), fields["locator"]...)
			cases := map[string][]byte{
				"duplicate_version":  bytes.Replace(spec.raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
				"nested_duplicate":   bytes.Replace(spec.raw, []byte(`"tenant_id":"tenant-189"`), []byte(`"tenant_id":"tenant-189","tenant_id":"tenant-189"`), 1),
				"case_alias":         bytes.Replace(spec.raw, []byte(`"tenant_id"`), []byte(`"Tenant_ID"`), 1),
				"unknown_nested":     bytes.Replace(spec.raw, []byte(`"tenant_id"`), []byte(`"unknown":true,"tenant_id"`), 1),
				"missing_version":    bytes.Replace(spec.raw, []byte(`"version":1,`), nil, 1),
				"null_locator":       bytes.Replace(spec.raw, locatorMember, []byte(`"locator":null`), 1),
				"fine_timestamp":     bytes.Replace(spec.raw, []byte(`03:00:00.000001Z`), []byte(`03:00:00.000001001Z`), 1),
				"offset_timestamp":   bytes.Replace(spec.raw, []byte(`03:00:00.000001Z`), []byte(`04:00:00.000001+01:00`), 1),
				"oversized_token":    bytes.Replace(spec.raw, []byte(`tenant-189`), bytes.Repeat([]byte("a"), 4096), 1),
				"truncated":          bytes.Clone(spec.raw[:len(spec.raw)-1]),
				"deep_object":        []byte(`{"locator":` + strings.Repeat(`{"a":`, 300) + `null` + strings.Repeat(`}`, 301)),
				"negative_version":   bytes.Replace(spec.raw, []byte(`"version":1`), []byte(`"version":-1`), 1),
				"fractional_version": bytes.Replace(spec.raw, []byte(`"version":1`), []byte(`"version":1.0`), 1),
				"exponent_version":   bytes.Replace(spec.raw, []byte(`"version":1`), []byte(`"version":1e0`), 1),
				"overflow":           bytes.Replace(spec.raw, []byte(`"revocation_generation":1`), []byte(`"revocation_generation":18446744073709551616`), 1),
				"leading_whitespace": append([]byte(" "), spec.raw...),
				"trailing_object":    append(bytes.Clone(spec.raw), []byte(`{}`)...),
				"trailing_newline":   append(bytes.Clone(spec.raw), '\n'),
				"invalid_utf8":       append(bytes.Clone(spec.raw), 0xff),
				"cap_minus_one":      append(bytes.Clone(spec.raw), bytes.Repeat([]byte(" "), domain.OwnedAgentCurrentMaxRecordBytesV1-1-len(spec.raw))...),
				"cap":                append(bytes.Clone(spec.raw), bytes.Repeat([]byte(" "), domain.OwnedAgentCurrentMaxRecordBytesV1-len(spec.raw))...),
				"cap_plus_one":       bytes.Repeat([]byte(" "), domain.OwnedAgentCurrentMaxRecordBytesV1+1),
			}
			for name, raw := range cases {
				t.Run(name, func(t *testing.T) {
					if bytes.Equal(raw, spec.raw) {
						t.Fatal("counterexample did not change the producer bytes")
					}
					if spec.decode(raw) {
						t.Error("noncanonical/invalid bytes accepted")
					}
				})
			}
		})
	}
}

// Independent framing implementation, not a call to any production digest or
// codec. Literal vectors below also detect fixture/canonical-encoding drift.
func ownedCurrentReferenceDigest(t testing.TB, kind string, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(raw)))
	h := sha256.New()
	h.Write([]byte(kind))
	h.Write([]byte{0})
	h.Write(size[:])
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil))
}

func TestOwnedCurrentCanonicalGoldens(t *testing.T) {
	t.Parallel()
	p, _ := ownedCurrentProfileFixture(t)
	v := ownedSpendingFixture(t, p, domain.AttemptSpendingWebSearchV1)
	for _, spec := range []struct {
		name, kind, golden string
		value              any
		digest             func() (string, error)
	}{
		{"profile_locator", "sessionless.owned-agent-profile-locator.v1", "96aa94320f1a9456608e3c2f7ea18e0ec2f73ee0d2e047bbd4c479885ce194ff", p.Locator, p.Locator.Digest},
		{"spending_locator", "sessionless.attempt-spending-authorization-locator.v1", "1234da1996d1b537469f396bc15c86839c80aa415bf58113dddc3cb9e4915fd2", v.Locator, v.Locator.Digest},
		{"profile", "sessionless.owned-agent-profile-current.v1", "0465440f0caeece6d3e7e431b6333f92d920b2263ea7199df56910f522dd457d", p.Clone(), p.Digest},
		{"spending", "sessionless.attempt-spending-authorization-current.v1", "5e7de0b5780cb47ec6c7a874d8c5679dc76123e963351b510e90c36cfd49492c", v.Clone(), v.Digest},
	} {
		t.Run(spec.name, func(t *testing.T) {
			got, err := spec.digest()
			if err != nil {
				t.Fatal(err)
			}
			want := ownedCurrentReferenceDigest(t, spec.kind, spec.value)
			if got != want {
				t.Errorf("digest got %s, independently framed %s", got, want)
			}
			if want != spec.golden {
				t.Errorf("independent bytes/framing drift: got %s, literal vector %s", want, spec.golden)
			}
		})
	}
}

func TestOwnedCurrentProfileCounterexamples(t *testing.T) {
	t.Parallel()
	p, at := ownedCurrentProfileFixture(t)
	changes := map[string]func(*domain.OwnedAgentProfileCurrentV1){
		"version":               func(v *domain.OwnedAgentProfileCurrentV1) { v.Version = 0 },
		"generation":            func(v *domain.OwnedAgentProfileCurrentV1) { v.RevocationGeneration = 0 },
		"profile_revision":      func(v *domain.OwnedAgentProfileCurrentV1) { v.ProfileRevision = 0 },
		"installation":          func(v *domain.OwnedAgentProfileCurrentV1) { v.InstallationID = "" },
		"installation_revision": func(v *domain.OwnedAgentProfileCurrentV1) { v.InstallationRevision = 0 },
		"state":                 func(v *domain.OwnedAgentProfileCurrentV1) { v.State = "enabled" },
		"resource_owner":        func(v *domain.OwnedAgentProfileCurrentV1) { v.Resource.OwnerUserID = "other-owner" },
		"resource_id":           func(v *domain.OwnedAgentProfileCurrentV1) { v.Locator.ResourceID = "other-resource" },
		"backend_profile":       func(v *domain.OwnedAgentProfileCurrentV1) { v.Locator.BackendProfileDigest = strings.Repeat("f", 64) },
		"unsupported_profile":   func(v *domain.OwnedAgentProfileCurrentV1) { v.Locator.Profile = "owned_private" },
		"unsupported_class":     func(v *domain.OwnedAgentProfileCurrentV1) { v.Locator.InputDataClass = domain.ProviderDataPrivateV1 },
		"missing_vendor":        func(v *domain.OwnedAgentProfileCurrentV1) { v.Locator.ModelVendorID = "" },
		"large_model_id":        func(v *domain.OwnedAgentProfileCurrentV1) { v.Locator.ModelID = strings.Repeat("a", 257) },
		"manifest_missing":      func(v *domain.OwnedAgentProfileCurrentV1) { v.InstalledManifest.Ref = "" },
		"manifest_empty":        func(v *domain.OwnedAgentProfileCurrentV1) { v.InstalledManifest.Bytes = 0 },
		"manifest_too_large":    func(v *domain.OwnedAgentProfileCurrentV1) { v.InstalledManifest.Bytes = 256<<10 + 1 },
		"review_missing":        func(v *domain.OwnedAgentProfileCurrentV1) { v.ReviewReceipt.Digest = "" },
		"missing_price":         func(v *domain.OwnedAgentProfileCurrentV1) { v.SubstratePriceDigest = "" },
		"inferred_free":         func(v *domain.OwnedAgentProfileCurrentV1) { v.SubstratePriceState = "" },
		"paid_zero": func(v *domain.OwnedAgentProfileCurrentV1) {
			v.SubstratePriceState = domain.ExternalServicePriceKnownPaidV1
		},
		"missing_terms":    func(v *domain.OwnedAgentProfileCurrentV1) { v.ProviderTermsDigest = "" },
		"missing_modality": func(v *domain.OwnedAgentProfileCurrentV1) { v.InputModalities = nil },
		"duplicate_modality": func(v *domain.OwnedAgentProfileCurrentV1) {
			v.InputModalities = []domain.AgentInputModalityV1{domain.AgentInputTextV1, domain.AgentInputTextV1}
		},
		"unordered_modality": func(v *domain.OwnedAgentProfileCurrentV1) {
			v.InputModalities = []domain.AgentInputModalityV1{domain.AgentInputTextV1, domain.AgentInputFileV1}
		},
		"model_window":        func(v *domain.OwnedAgentProfileCurrentV1) { v.ModelWindowTokens = v.Limits.MaxInputTokensPerCall },
		"excess_model_window": func(v *domain.OwnedAgentProfileCurrentV1) { v.ModelWindowTokens = math.MaxUint64 },
		"limits":              func(v *domain.OwnedAgentProfileCurrentV1) { v.Limits.MaxInputTokensPerCall = math.MaxUint64 },
		"missing_mcp":         func(v *domain.OwnedAgentProfileCurrentV1) { v.CostCeiling.MCPToolCost = nil },
		"foreign_payer":       func(v *domain.OwnedAgentProfileCurrentV1) { v.BillingOwnerUserID = "other-payer" },
		"wrong_grant":         func(v *domain.OwnedAgentProfileCurrentV1) { v.WebSearchGrantDigest = strings.Repeat("f", 64) },
		"cost_substrate": func(v *domain.OwnedAgentProfileCurrentV1) {
			v.Substrate.AdmissionCostCeilingDigest = domain.AdmissionCostCeilingDigestV2(strings.Repeat("f", 64))
		},
		"fine_price_time":    func(v *domain.OwnedAgentProfileCurrentV1) { v.CostCeiling.PriceObservedAt = at.Add(time.Nanosecond) },
		"future_observation": func(v *domain.OwnedAgentProfileCurrentV1) { v.Window.ObservedAt = at.Add(time.Microsecond) },
		"price_expansion": func(v *domain.OwnedAgentProfileCurrentV1) {
			v.Window.ExpiresAt = v.CostCeiling.PriceExpiresAt.Add(time.Microsecond)
		},
		"noncanonical_time": func(v *domain.OwnedAgentProfileCurrentV1) { v.UpdatedAt = at.Add(time.Nanosecond) },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			v := p.Clone()
			change(&v)
			if err := v.Validate(); err == nil {
				t.Fatal("invalid profile accepted")
			}
			if raw, err := domain.EncodeOwnedAgentProfileCurrentV1(v); err == nil || raw != nil {
				t.Error("invalid profile encoded as authority")
			}
		})
	}
	// Current-only failures are distinct from invalid historical structure.
	for _, change := range []func(*domain.OwnedAgentProfileCurrentV1){
		func(v *domain.OwnedAgentProfileCurrentV1) { v.State = domain.OwnedAgentCurrentDisabledV1 },
		func(v *domain.OwnedAgentProfileCurrentV1) { v.UpdatedAt = at.Add(time.Microsecond) },
	} {
		v := p.Clone()
		change(&v)
		if err := v.Validate(); err != nil {
			t.Fatalf("historical structural control: %v", err)
		}
		if err := v.ValidateAt(at); err == nil {
			t.Error("inactive/future profile considered current")
		}
	}
}

// Walk every declared leaf, including nested optional pointers. Each one-field
// counterexample must either change the canonical digest or fail validation; a
// valid ignored field is forbidden. Reflection is only test inventory, not a
// production parser or approval mechanism.
func ownedCurrentLeafPaths(v reflect.Value, prefix []int) [][]int {
	if v.Type() == reflect.TypeOf(time.Time{}) || v.Kind() != reflect.Struct {
		return [][]int{append([]int(nil), prefix...)}
	}
	var paths [][]int
	for i := 0; i < v.NumField(); i++ {
		if v.Type().Field(i).PkgPath != "" {
			continue
		}
		path := append(append([]int(nil), prefix...), i)
		child := v.Field(i)
		if child.Kind() == reflect.Pointer && !child.IsNil() && child.Elem().Kind() == reflect.Struct {
			paths = append(paths, path) // Optional component presence is its own field.
			paths = append(paths, ownedCurrentLeafPaths(child.Elem(), path)...)
		} else {
			paths = append(paths, ownedCurrentLeafPaths(child, path)...)
		}
	}
	return paths
}

func ownedCurrentChangeLeaf(t testing.TB, value reflect.Value, path []int) string {
	t.Helper()
	var names []string
	for _, i := range path {
		if value.Kind() == reflect.Pointer {
			value = value.Elem()
		}
		names = append(names, value.Type().Field(i).Name)
		value = value.Field(i)
	}
	if value.Type() == reflect.TypeOf(time.Time{}) {
		value.Set(reflect.ValueOf(value.Interface().(time.Time).Add(time.Microsecond)))
		return strings.Join(names, ".")
	}
	switch value.Kind() {
	case reflect.String:
		old := value.String()
		if len(old) == 64 {
			replacement := byte('e')
			if old[0] == replacement {
				replacement = 'f'
			}
			value.SetString(string(replacement) + old[1:])
		} else {
			value.SetString(old + "x")
		}
	case reflect.Uint32, reflect.Uint64:
		value.SetUint(value.Uint() + 1)
	case reflect.Int64:
		value.SetInt(value.Int() + 1)
	case reflect.Pointer, reflect.Slice:
		value.SetZero()
	default:
		t.Fatalf("uncovered canonical leaf %s (%s)", strings.Join(names, "."), value.Kind())
	}
	return strings.Join(names, ".")
}

func TestOwnedCurrentEveryFieldIsSealed(t *testing.T) {
	t.Parallel()
	p, _ := ownedCurrentProfileFixture(t)
	// Exercise optional presence and every nested leaf with valid DATA. A nil
	// pointer changed to nil would be a no-op, not a sealing counterexample.
	body := resourceGrantRaw(t, resourceGrantFixture())
	sum := sha256.Sum256(body)
	p.WebSearchResourceGrant = &domain.WebSearchResourceGrantBodyV2{Version: 2, Body: domain.OwnedAgentAuthorityBodyRefV1{Ref: "original-search-grant", Digest: hex.EncodeToString(sum[:]), Bytes: uint64(len(body))}}
	v := ownedSpendingFixture(t, p, domain.AttemptSpendingWebSearchV1)
	for _, spec := range []struct {
		name   string
		base   any
		copy   func() any
		digest func(any) (string, error)
	}{
		{"profile", p, func() any { c := p.Clone(); return &c }, func(v any) (string, error) { return v.(*domain.OwnedAgentProfileCurrentV1).Digest() }},
		{"spending", v, func() any { c := v.Clone(); return &c }, func(v any) (string, error) { return v.(*domain.AttemptSpendingAuthorizationCurrentV1).Digest() }},
	} {
		t.Run(spec.name, func(t *testing.T) {
			original, err := spec.digest(spec.copy())
			if err != nil {
				t.Fatal(err)
			}
			paths := ownedCurrentLeafPaths(reflect.ValueOf(spec.base), nil)
			for _, path := range paths {
				copy := spec.copy()
				name := ownedCurrentChangeLeaf(t, reflect.ValueOf(copy).Elem(), path)
				t.Run(name, func(t *testing.T) {
					changed, err := spec.digest(copy)
					if err == nil && changed == original {
						t.Fatal("changed explicit canonical field was not sealed")
					}
				})
			}
		})
	}
}

func TestOwnedCurrentSourcePricingAndGenerations(t *testing.T) {
	t.Parallel()
	p, at := ownedCurrentProfileFixture(t)
	base := ownedSpendingFixture(t, p, domain.AttemptSpendingSubstrateV1)
	unknown := p.Clone()
	unknown.SubstratePriceState = domain.ExternalServicePriceUnknownV1
	unknown.CostCeiling.SubstratePriceState = domain.CostEvidenceUnknownV1
	unknown.CostCeiling.MaxSubstrateAmountMicrounits = nil
	unknown.CostCeiling.MaxTotalAmountMicrounits = nil
	unknown.Substrate.AdmissionCostCeilingDigest, _ = unknown.CostCeiling.Digest()
	if err := unknown.Validate(); err != nil {
		t.Fatalf("historical unknown-price structure: %v", err)
	}
	if err := unknown.ValidateAt(at); err == nil {
		t.Error("unknown substrate pricing became executable")
	}
	paid := p.Clone()
	paid.SubstratePriceState = domain.ExternalServicePriceKnownPaidV1
	paid.CostCeiling.MaxSubstrateAmountMicrounits = uint64Ptr(1)
	paid.CostCeiling.MaxTotalAmountMicrounits = uint64Ptr(71)
	paid.Substrate.AdmissionCostCeilingDigest, _ = paid.CostCeiling.Digest()
	if err := paid.ValidateAt(at); err != nil {
		t.Fatalf("explicit synthetic paid fixture: %v", err)
	}
	if err := ownedSpendingFixture(t, paid, domain.AttemptSpendingSubstrateV1).ValidateAgainstProfile(paid, at); err != nil {
		t.Fatalf("paid structural join: %v", err)
	}
	changed := p.Clone()
	changed.RevocationGeneration++ // Still active, but no longer the original source.
	if err := base.ValidateAgainstProfile(changed, at); err == nil {
		t.Error("still-active replacement generation accepted old source")
	}
	for _, change := range []func(*domain.AttemptSpendingAuthorizationCurrentV1){
		func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.RevocationGeneration += 2 },
		func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.RevocationGeneration = 1 },
		func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.State = domain.OwnedAgentCurrentActiveV1 },
		func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.MutationID = base.MutationID },
		func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.UpdatedAt = at.Add(-time.Microsecond) },
	} {
		v := base.Clone()
		v.State, v.RevocationGeneration, v.MutationID = domain.OwnedAgentCurrentRevokedV1, 2, "revoke-next"
		change(&v)
		if err := v.ValidateTransitionFrom(base); err == nil {
			t.Error("stale/skipped/expanded spending transition accepted")
		}
	}
	max := base.Clone()
	max.RevocationGeneration = math.MaxUint64
	wrapped := max.Clone()
	wrapped.State, wrapped.RevocationGeneration, wrapped.MutationID = domain.OwnedAgentCurrentRevokedV1, 0, "wrap"
	if err := wrapped.ValidateTransitionFrom(max); err == nil {
		t.Error("spending generation wrapped")
	}
}

func FuzzOwnedCurrentStrictCodecs(f *testing.F) {
	p, _ := ownedCurrentProfileFixture(f)
	v := ownedSpendingFixture(f, p, domain.AttemptSpendingWebSearchV1)
	profile, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
	if err != nil {
		f.Fatal(err)
	}
	spending, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(v)
	if err != nil {
		f.Fatal(err)
	}
	for _, raw := range [][]byte{profile, spending, {}, []byte(`null`), []byte(`{"version":1,"version":1}`), append(bytes.Clone(profile), '\n'), bytes.Repeat([]byte(" "), domain.OwnedAgentCurrentMaxRecordBytesV1+1)} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > domain.OwnedAgentCurrentMaxRecordBytesV1+1 {
			t.Skip("beyond the one-record input bound")
		}
		p, err := domain.DecodeOwnedAgentProfileCurrentV1(raw)
		if err != nil {
			if !reflect.ValueOf(p).IsZero() {
				t.Fatal("partial profile after decoder failure")
			}
		} else {
			encoded, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
			if err != nil || !bytes.Equal(raw, encoded) {
				t.Fatal("accepted noncanonical profile")
			}
		}
		v, err := domain.DecodeAttemptSpendingAuthorizationCurrentV1(raw)
		if err != nil {
			if !reflect.ValueOf(v).IsZero() {
				t.Fatal("partial spending head after decoder failure")
			}
		} else {
			encoded, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(v)
			if err != nil || !bytes.Equal(raw, encoded) {
				t.Fatal("accepted noncanonical spending head")
			}
		}
	})
}
