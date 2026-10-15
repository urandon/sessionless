package domain

import (
	"math"
	"strings"
	"testing"
	"time"
)

// Synthetic STRUCTURAL data only. A green pure validator does not install
// native/tariff evidence, issue owner consent or qualify a production model.
type piPolicyValidationFixture struct {
	p  AttachedPiSDKNativeProfileV1
	e  ProviderEvidenceBundleV1
	t  AttachedPiAPIInvocationPolicyV1
	r  AttachedPiAPIOwnerAuthorizationReceiptV1
	m  AttachedPiAPIInvocationPolicyMappingV1
	b  HarnessBindingV2
	at time.Time
}

func piPolicyValidationData(t testing.TB) piPolicyValidationFixture {
	t.Helper()
	p := piSDKFixtureProfile(t)
	b, _ := piSDKFixtureBinding(t, p)
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	start, end := at.Add(-time.Minute), at.Add(time.Hour)
	scope := ProviderEvidenceScopeV1{TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, Resource: b.Resource, Backend: b.Backend}
	e := ProviderEvidenceBundleV1{
		Catalog: ProviderCatalogObservationV1{Version: 1, Scope: scope, CatalogRevision: "synthetic-catalog", Models: []ProviderCatalogModelV1{{ModelVendorID: b.ModelVendorID, ModelID: b.ModelID, CatalogDigest: piSDKFixtureDigest("model-entry")}}, ObservedAt: start, ExpiresAt: end},
		Route: ProviderRoutePolicyV1{Version: 1, Scope: scope, State: ProviderEvidenceSupportedV1, PolicyID: "synthetic-route", Revision: 1, FallbackPolicy: ProviderFallbackDenyV1,
			Routes: []ProviderRouteV1{{BackendKind: HarnessBackendPiV1, ModelVendorID: b.ModelVendorID, ModelID: b.ModelID, TransportKind: ProviderTransportDirectAPIV1, TransportProvider: "synthetic-transport", UpstreamProviderID: "synthetic-upstream", EndpointID: "synthetic-endpoint", BillingKind: ProviderBillingAPIAccountV1, BillingAuthority: b.Resource.ResourceID}}, ObservedAt: start, ExpiresAt: end},
		Capability: ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: b.ModelVendorID, ModelID: b.ModelID, MaxContextTokens: ProviderEvidenceQuantityV1{State: ProviderEvidenceSupportedV1, Value: p.Model.ModelContextTokens}, ToolCalling: ProviderEvidenceUnsupportedV1, StructuredOutput: ProviderEvidenceSupportedV1, ObservedAt: start, ExpiresAt: end},
		Privacy:    ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: b.ModelVendorID, ModelID: b.ModelID, AllowedDataClasses: []ProviderDataClassV1{b.InputDataClass}, TrainingUse: ProviderEvidenceFalseV1, RetentionHours: ProviderEvidenceQuantityV1{State: ProviderEvidenceSupportedV1, Value: 1}, PolicyRevision: "synthetic-privacy", ObservedAt: start, ExpiresAt: end},
		Price:      ProviderPriceObservationV1{Version: 1, Scope: scope, State: ProviderEvidenceSupportedV1, ModelVendorID: b.ModelVendorID, ModelID: b.ModelID, Currency: "USD", InputMicrounitsPerMTokens: 1_000_001, OutputMicrounitsPerMTokens: 2_000_001, ObservedAt: start, ExpiresAt: end},
		Policy:     ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: "synthetic-go-policy", Revision: 1, DecisionOwner: "synthetic-only", EvidenceSource: "not-a-real-tariff", Verdict: ProviderPolicyGoV1, AllowedDataClasses: []ProviderDataClassV1{b.InputDataClass}, ObservedAt: start, ExpiresAt: end},
	}
	f := piPolicyValidationFixture{p: p, e: e, b: b, at: at,
		t: AttachedPiAPIInvocationPolicyV1{Version: 1, Scope: scope, ModelVendorID: b.ModelVendorID, ModelID: b.ModelID, InputDataClass: b.InputDataClass, Currency: "USD", MaxContextTokens: p.Model.ModelContextTokens, MaxOutputTokens: p.Model.MaxOutputTokens, MaxChargeMicrounits: 4098, NotBefore: start, ExpiresAt: end},
		r: AttachedPiAPIOwnerAuthorizationReceiptV1{Version: 1, IssuerID: "synthetic-not-registered", DecisionID: "synthetic-not-issued", TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, DecisionSourceID: "synthetic-not-authoritative", Decision: "authorize", IssuedAt: at, ExpiresAt: end, MembershipSecurityVersion: 3},
	}
	f.seal(t)
	if expiry, err := f.validate(); err != nil || expiry != end {
		t.Fatalf("synthetic complete P/E/T/R/M/binding control: expiry=%s want=%s err=%v", expiry, end, err)
	}
	return f
}

func (f *piPolicyValidationFixture) seal(t testing.TB) {
	t.Helper()
	for _, item := range []struct {
		digest func() (ProviderEvidenceDigestV1, error)
		fields []*string
	}{
		{f.e.Catalog.Digest, []*string{&f.b.ProviderCatalogDigest}},
		{f.e.Route.Digest, []*string{&f.e.Policy.RoutePolicyDigest, &f.b.ProviderRouteDigest}},
		{f.e.Capability.Digest, []*string{&f.e.Policy.CapabilityEvidenceDigest, &f.b.CapabilityEvidenceDigest}},
		{f.e.Privacy.Digest, []*string{&f.e.Policy.PrivacyEvidenceDigest, &f.b.PrivacyPolicyDigest}},
		{f.e.Price.Digest, []*string{&f.e.Policy.PriceObservationDigest, &f.t.PriceObservationDigest}},
		{func() (ProviderEvidenceDigestV1, error) { return f.e.Policy.Digest() }, []*string{&f.t.EffectivePolicyDigest, &f.b.EffectivePolicyDigest}},
	} {
		digest, err := item.digest()
		if err != nil {
			t.Fatalf("synthetic independent evidence reseal: %v", err)
		}
		for _, field := range item.fields {
			*field = string(digest)
		}
	}
	end := f.e.Catalog.ExpiresAt
	for _, expiry := range []time.Time{f.e.Route.ExpiresAt, f.e.Capability.ExpiresAt, f.e.Privacy.ExpiresAt, f.e.Price.ExpiresAt, f.e.Policy.ExpiresAt} {
		if expiry.Before(end) {
			end = expiry
		}
	}
	f.b.EvidenceExpiresAt = &end
	digest, err := f.t.Digest()
	if err != nil {
		t.Fatalf("synthetic T reseal: %v", err)
	}
	f.r.SubjectTermsDigest = digest
	rd, err := f.r.Digest()
	if err != nil {
		t.Fatalf("synthetic R reseal: %v", err)
	}
	f.m = AttachedPiAPIInvocationPolicyMappingV1{Version: 1, TermsDigest: digest, OwnerAuthorizationReceiptDigest: rd}
	f.b.AttachedPiAPIInvocationPolicyDigest, err = f.m.Digest()
	if err != nil {
		t.Fatalf("synthetic M reseal: %v", err)
	}
}

func (f piPolicyValidationFixture) validate() (time.Time, error) {
	return ValidateAttachedPiAPIInvocationPolicyV1(f.p, f.e, f.t, f.r, f.m, f.b, f.at)
}

func TestAttachedPiAPIValidationExactTermsAndIndependentRounding(t *testing.T) {
	t.Parallel()
	f := piPolicyValidationData(t)
	// ceil(2048*1000001/1000000) + ceil(1024*2000001/1000000)
	// is 2049+2049=4098. Rounding only after summing would give 4097.
	f.t.MaxChargeMicrounits = 4097
	f.seal(t)
	if expiry, err := f.validate(); err == nil || !expiry.IsZero() {
		t.Fatalf("underfunded independently rounded charge accepted: expiry=%s err=%v", expiry, err)
	}
	f.t.MaxChargeMicrounits = 4098
	f.seal(t)
	if _, err := f.validate(); err != nil {
		t.Fatalf("exact restored cap denied: %v", err)
	}
	// T/E/P can be checked before any R/M or invented Run exists.
	f.r, f.m, f.b = AttachedPiAPIOwnerAuthorizationReceiptV1{}, AttachedPiAPIInvocationPolicyMappingV1{}, HarnessBindingV2{}
	if expiry, err := ValidateAttachedPiAPITermsV1(f.p, f.e, f.t, f.at); err != nil || expiry != f.t.ExpiresAt {
		t.Fatalf("pre-consent exact terms denied: expiry=%s err=%v", expiry, err)
	}
}

func TestAttachedPiAPIValidationResealedEvidenceAndScopeFences(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*piPolicyValidationFixture)
	}{
		{"foreign_catalog_scope", func(f *piPolicyValidationFixture) { f.e.Catalog.Scope.TenantID = "foreign-tenant" }},
		{"foreign_price_model", func(f *piPolicyValidationFixture) { f.e.Price.ModelID = "foreign-model" }},
		{"catalog_missing_model", func(f *piPolicyValidationFixture) { f.e.Catalog.Models[0].ModelID = "other-model" }},
		{"route_missing_model", func(f *piPolicyValidationFixture) { f.e.Route.Routes[0].ModelID = "other-model" }},
		{"route_wrong_transport", func(f *piPolicyValidationFixture) { f.e.Route.Routes[0].TransportKind = ProviderTransportLocalCLIV1 }},
		{"unknown_price", func(f *piPolicyValidationFixture) {
			f.e.Price.State = ProviderEvidenceUnknownV1
			f.e.Price.Currency = ""
			f.e.Price.InputMicrounitsPerMTokens = 0
			f.e.Price.OutputMicrounitsPerMTokens = 0
		}},
		{"unknown_context", func(f *piPolicyValidationFixture) {
			f.e.Capability.MaxContextTokens = ProviderEvidenceQuantityV1{State: ProviderEvidenceUnknownV1}
		}},
		{"smaller_context", func(f *piPolicyValidationFixture) { f.e.Capability.MaxContextTokens.Value-- }},
		{"future_price", func(f *piPolicyValidationFixture) { f.e.Price.ObservedAt = f.at.Add(time.Second) }},
		{"expired_price", func(f *piPolicyValidationFixture) { f.e.Price.ExpiresAt = f.at }},
		{"currency", func(f *piPolicyValidationFixture) { f.e.Price.Currency = "EUR" }},
		{"rate_product_overflow", func(f *piPolicyValidationFixture) { f.e.Price.InputMicrounitsPerMTokens = math.MaxUint64 }},
		{"understated_rate", func(f *piPolicyValidationFixture) { f.e.Price.InputMicrounitsPerMTokens++; f.t.MaxChargeMicrounits = 1 }},
		{"unresolved_condition", func(f *piPolicyValidationFixture) { f.e.Policy.Verdict = ProviderPolicyConditionalV1 }},
		{"policy_private_only", func(f *piPolicyValidationFixture) {
			f.e.Policy.AllowedDataClasses = []ProviderDataClassV1{ProviderDataPrivateV1}
		}},
		{"privacy_private_only", func(f *piPolicyValidationFixture) {
			f.e.Privacy.AllowedDataClasses = []ProviderDataClassV1{ProviderDataPrivateV1}
		}},
		{"total_context", func(f *piPolicyValidationFixture) { f.t.MaxContextTokens-- }},
		{"output_cap", func(f *piPolicyValidationFixture) { f.t.MaxOutputTokens-- }},
		{"future_terms", func(f *piPolicyValidationFixture) {
			f.t.NotBefore = f.at.Add(time.Second)
			f.r.IssuedAt = f.t.NotBefore
		}},
		{"expired_terms", func(f *piPolicyValidationFixture) { f.t.ExpiresAt = f.at }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := piPolicyValidationData(t)
			tc.mutate(&f)
			f.seal(t) // not merely stale hashes: cross-scope/semantic cause remains
			if expiry, err := f.validate(); err == nil || !expiry.IsZero() {
				t.Fatalf("accepted resealed %s: expiry=%s err=%v", tc.name, expiry, err)
			}
		})
	}
}

func TestAttachedPiAPIValidationOriginalMappingAndReceiptFences(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*piPolicyValidationFixture)
	}{
		{"zero_time", func(f *piPolicyValidationFixture) { f.at = time.Time{} }},
		{"non_utc_time", func(f *piPolicyValidationFixture) { f.at = f.at.In(time.FixedZone("test-offset", 3600)) }},
		{"wrong_profile", func(f *piPolicyValidationFixture) { f.p.Model.ModelID = "other-native-model" }},
		{"foreign_binding_owner", func(f *piPolicyValidationFixture) { f.b.OwnerUserID = "other-owner" }},
		{"binding_model", func(f *piPolicyValidationFixture) { f.b.ModelID = "other-model" }},
		{"binding_data", func(f *piPolicyValidationFixture) { f.b.InputDataClass = ProviderDataPrivateV1 }},
		{"replaced_mapping", func(f *piPolicyValidationFixture) {
			f.b.AttachedPiAPIInvocationPolicyDigest = AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("e", 64))
		}},
		{"receipt_subject", func(f *piPolicyValidationFixture) {
			f.r.SubjectTermsDigest = ProviderEvidenceDigestV1(strings.Repeat("e", 64))
		}},
		{"receipt_future", func(f *piPolicyValidationFixture) { f.r.IssuedAt = f.at.Add(time.Second) }},
		{"receipt_before_terms", func(f *piPolicyValidationFixture) { f.r.IssuedAt = f.t.NotBefore.Add(-time.Second) }},
		{"receipt_expired", func(f *piPolicyValidationFixture) { f.r.ExpiresAt = f.at; f.r.IssuedAt = f.at.Add(-time.Second) }},
		{"wrong_price_digest", func(f *piPolicyValidationFixture) { f.t.PriceObservationDigest = strings.Repeat("e", 64) }},
		{"wrong_policy_price", func(f *piPolicyValidationFixture) { f.e.Policy.PriceObservationDigest = strings.Repeat("e", 64) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := piPolicyValidationData(t)
			tc.mutate(&f)
			if strings.HasPrefix(tc.name, "receipt_") && tc.name != "receipt_subject" {
				f.seal(t)
			}
			if expiry, err := f.validate(); err == nil || !expiry.IsZero() {
				t.Fatalf("accepted %s: expiry=%s err=%v", tc.name, expiry, err)
			}
		})
	}
}

func TestAttachedPiAPIValidationMinimumExclusiveExpiry(t *testing.T) {
	t.Parallel()
	for _, source := range []string{"catalog", "route", "capability", "privacy", "price", "policy", "terms", "receipt"} {
		t.Run(source, func(t *testing.T) {
			f := piPolicyValidationData(t)
			want := f.at.Add(time.Minute)
			fields := map[string]*time.Time{"catalog": &f.e.Catalog.ExpiresAt, "route": &f.e.Route.ExpiresAt, "capability": &f.e.Capability.ExpiresAt, "privacy": &f.e.Privacy.ExpiresAt, "price": &f.e.Price.ExpiresAt, "policy": &f.e.Policy.ExpiresAt, "terms": &f.t.ExpiresAt, "receipt": &f.r.ExpiresAt}
			*fields[source] = want
			f.seal(t)
			if expiry, err := f.validate(); err != nil || expiry != want {
				t.Fatalf("minimum %s expiry=%s want=%s err=%v", source, expiry, want, err)
			}
			f.at = want
			if expiry, err := f.validate(); err == nil || !expiry.IsZero() {
				t.Fatalf("accepted exclusive %s expiry=%s err=%v", source, expiry, err)
			}
		})
	}
}
