package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Pure structural fixture. No registered profile, authenticated owner source,
// issuer, API key, model, current mapping or permission is supplied by this.
func attachedPiAPIStructuralFixture(t testing.TB) (AttachedPiAPIInvocationPolicyV1, AttachedPiAPIOwnerAuthorizationReceiptV1, AttachedPiAPIInvocationPolicyMappingV1) {
	t.Helper()
	at := time.Date(2026, 10, 12, 12, 0, 0, 123456789, time.UTC)
	scope := ProviderEvidenceScopeV1{TenantID: "pi-structural-tenant", OwnerUserID: "pi-structural-owner",
		Resource: ProviderResourceBindingV1{Kind: ProviderResourceAPIAccountV1, ResourceID: "structural-api-account", OwnerUserID: "pi-structural-owner", Revision: 7, CredentialMode: ProviderCredentialInvocationV1, CredentialGeneration: 9},
		Backend:  HarnessBackendDescriptorV1{HarnessKind: HarnessKindSessionlessV1, HarnessVersion: "structural-version", BackendKind: HarnessBackendPiV1, ArtifactKind: HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("a", 64), NativeProtocolVersion: "structural-protocol", BackendProfileDigest: strings.Repeat("b", 64), ProviderContractKind: ProviderContractInvocationV1, CredentialDeliveryKind: ProviderCredentialDeliveryFileV1}}
	price := ProviderPriceObservationV1{Version: 1, Scope: scope, State: ProviderEvidenceSupportedV1, ModelVendorID: "fixture-vendor", ModelID: "fixture-model", Currency: "USD", InputMicrounitsPerMTokens: 3, OutputMicrounitsPerMTokens: 5, ObservedAt: at, ExpiresAt: at.Add(time.Hour)}
	priceDigest, err := price.Digest()
	if err != nil {
		t.Fatalf("structural price digest: %v", err)
	}
	policy := ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: "structural-policy", Revision: 2, DecisionOwner: "not-authenticated", EvidenceSource: "structural-only", Verdict: ProviderPolicyGoV1, AllowedDataClasses: []ProviderDataClassV1{ProviderDataPrivateV1}, CapabilityEvidenceDigest: strings.Repeat("c", 64), PrivacyEvidenceDigest: strings.Repeat("d", 64), PriceObservationDigest: string(priceDigest), RoutePolicyDigest: strings.Repeat("e", 64), ObservedAt: at, ExpiresAt: at.Add(time.Hour)}
	policyDigest, err := policy.Digest()
	if err != nil {
		t.Fatalf("structural policy digest: %v", err)
	}
	terms := AttachedPiAPIInvocationPolicyV1{Version: 1, Scope: scope, ModelVendorID: price.ModelVendorID, ModelID: price.ModelID, InputDataClass: ProviderDataPrivateV1, EffectivePolicyDigest: string(policyDigest), PriceObservationDigest: string(priceDigest), Currency: "USD", MaxContextTokens: 8192, MaxOutputTokens: 1024, MaxChargeMicrounits: 100, NotBefore: at, ExpiresAt: at.Add(time.Hour)}
	termsDigest, err := terms.Digest()
	if err != nil {
		t.Fatalf("structural terms digest: %v", err)
	}
	receipt := AttachedPiAPIOwnerAuthorizationReceiptV1{Version: 1, IssuerID: "structural-not-a-real-issuer", DecisionID: "structural-decision", TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, DecisionSourceID: "structural-not-an-authenticated-source", SubjectTermsDigest: termsDigest, Decision: "authorize", IssuedAt: at, ExpiresAt: at.Add(time.Hour), MembershipSecurityVersion: 11}
	receiptDigest, err := receipt.Digest()
	if err != nil {
		t.Fatalf("structural receipt digest: %v", err)
	}
	mapping := AttachedPiAPIInvocationPolicyMappingV1{Version: 1, TermsDigest: termsDigest, OwnerAuthorizationReceiptDigest: receiptDigest}
	if err := mapping.ValidateFor(terms, receipt); err != nil {
		t.Fatalf("structural T/R/M relationships: %v", err)
	}
	return terms, receipt, mapping
}

func TestAttachedPiAPIAcyclicCanonicalConstruction(t *testing.T) {
	t.Parallel()
	terms, receipt, mapping := attachedPiAPIStructuralFixture(t)
	for _, tc := range []struct {
		name, prefix string
		encode       func() ([]byte, error)
		digest       func() (string, error)
	}{
		{"terms", "sessionless.attached-pi-api-invocation-policy.v1", func() ([]byte, error) { return EncodeAttachedPiAPIInvocationPolicyV1(terms) }, func() (string, error) { d, e := terms.Digest(); return string(d), e }},
		{"receipt", "sessionless.attached-pi-api-owner-authorization-receipt.v1", func() ([]byte, error) { return EncodeAttachedPiAPIOwnerAuthorizationReceiptV1(receipt) }, func() (string, error) { d, e := receipt.Digest(); return string(d), e }},
		{"mapping", "sessionless.attached-pi-api-invocation-policy-mapping.v1", func() ([]byte, error) { return EncodeAttachedPiAPIInvocationPolicyMappingV1(mapping) }, func() (string, error) { d, e := mapping.Digest(); return string(d), e }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := tc.encode()
			if err != nil {
				t.Fatalf("encode %s: %v", tc.name, err)
			}
			want := sha256.Sum256(append([]byte(tc.prefix+"\x00"), raw...))
			got, err := tc.digest()
			if err != nil || got != hex.EncodeToString(want[:]) {
				t.Errorf("%s canonical domain-separated digest got=%q err=%v want=%x", tc.name, got, err, want)
			}
		})
	}
	tRaw, _ := EncodeAttachedPiAPIInvocationPolicyV1(terms)
	rRaw, _ := EncodeAttachedPiAPIOwnerAuthorizationReceiptV1(receipt)
	mRaw, _ := EncodeAttachedPiAPIInvocationPolicyMappingV1(mapping)
	gotT, eT := DecodeAttachedPiAPIInvocationPolicyV1(tRaw)
	gotR, eR := DecodeAttachedPiAPIOwnerAuthorizationReceiptV1(rRaw)
	gotM, eM := DecodeAttachedPiAPIInvocationPolicyMappingV1(mRaw)
	if eT != nil || eR != nil || eM != nil || gotT != terms || gotR != receipt || gotM != mapping {
		t.Fatalf("complete T/R/M round-trip errors=%v/%v/%v equality=%v/%v/%v", eT, eR, eM, gotT == terms, gotR == receipt, gotM == mapping)
	}
	// Their canonical field lists are closed: no receipt selfhash, mapping
	// reference in T/R, final binding/profile backreference, or permission flag.
	for _, value := range []any{terms, receipt, mapping} {
		typ := reflect.TypeOf(value)
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i).Name
			if field == "ReceiptDigest" || field == "MappingDigest" || field == "HarnessBindingDigest" || field == "Eligible" || field == "Authorized" {
				t.Errorf("%s contains forbidden backreference/grant field %s", typ.Name(), field)
			}
		}
	}
}

func TestAttachedPiAPICanonicalCodecRejectsNoncanonicalAndBackreferences(t *testing.T) {
	t.Parallel()
	terms, receipt, mapping := attachedPiAPIStructuralFixture(t)
	tRaw, _ := EncodeAttachedPiAPIInvocationPolicyV1(terms)
	rRaw, _ := EncodeAttachedPiAPIOwnerAuthorizationReceiptV1(receipt)
	mRaw, _ := EncodeAttachedPiAPIInvocationPolicyMappingV1(mapping)
	for _, codec := range []struct {
		name string
		raw  []byte
		deny func([]byte) bool
	}{
		{"terms", tRaw, func(raw []byte) bool {
			v, e := DecodeAttachedPiAPIInvocationPolicyV1(raw)
			return e != nil && v == (AttachedPiAPIInvocationPolicyV1{})
		}},
		{"receipt", rRaw, func(raw []byte) bool {
			v, e := DecodeAttachedPiAPIOwnerAuthorizationReceiptV1(raw)
			return e != nil && v == (AttachedPiAPIOwnerAuthorizationReceiptV1{})
		}},
		{"mapping", mRaw, func(raw []byte) bool {
			v, e := DecodeAttachedPiAPIInvocationPolicyMappingV1(raw)
			return e != nil && v == (AttachedPiAPIInvocationPolicyMappingV1{})
		}},
	} {
		for _, mutant := range []struct {
			name string
			raw  []byte
		}{
			{"duplicate", bytes.Replace(codec.raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)},
			{"case_alias", bytes.Replace(codec.raw, []byte(`"version"`), []byte(`"Version"`), 1)},
			{"omitted", bytes.Replace(codec.raw, []byte(`"version":1,`), nil, 1)},
			{"null", bytes.Replace(codec.raw, []byte(`"version":1`), []byte(`"version":null`), 1)},
			{"number_alias", bytes.Replace(codec.raw, []byte(`"version":1`), []byte(`"version":1.0`), 1)},
			{"trailing", append(bytes.Clone(codec.raw), []byte(`{}`)...)},
			{"whitespace", append([]byte(" "), codec.raw...)},
			{"invalid_utf8", append(bytes.Clone(codec.raw[:len(codec.raw)-1]), 0xff, '}')},
			{"metadata_oversize", bytes.Repeat([]byte{' '}, attachedPiAPICanonicalMetadataMaxBytesV1+1)},
			{"empty", nil},
		} {
			t.Run(codec.name+"/"+mutant.name, func(t *testing.T) {
				if !codec.deny(mutant.raw) {
					t.Errorf("%s/%s did not deny with ZERO result (bytes=%d)", codec.name, mutant.name, len(mutant.raw))
				}
				if codec.deny(codec.raw) {
					t.Errorf("%s/%s restored canonical control denied", codec.name, mutant.name)
				}
			})
		}
		for _, key := range []string{"receipt_digest", "mapping_digest", "harness_binding_digest", "owner_receipt_digest", "eligible", "legacy_policy_digest"} {
			t.Run(codec.name+"/"+key, func(t *testing.T) {
				raw := append(bytes.Clone(codec.raw[:len(codec.raw)-1]), []byte(`,"`+key+`":"`+strings.Repeat("a", 64)+`"}`)...)
				if !codec.deny(raw) {
					t.Errorf("%s accepted forbidden cyclic/alias/grant key %s", codec.name, key)
				}
			})
		}
	}
	for _, mutant := range []struct {
		name string
		raw  []byte
	}{
		{"nested_duplicate", bytes.Replace(tRaw, []byte(`"resource_id":"structural-api-account"`), []byte(`"resource_id":"structural-api-account","resource_id":"structural-api-account"`), 1)},
		{"nested_unknown", bytes.Replace(tRaw, []byte(`"backend":{`), []byte(`"backend":{"mapping_digest":"`+strings.Repeat("a", 64)+`",`), 1)},
		{"time_alias", bytes.Replace(tRaw, []byte(`2026-10-12T12:00:00.123456789Z`), []byte(`2026-10-12T12:00:00.123456789+00:00`), 1)},
	} {
		t.Run(mutant.name, func(t *testing.T) {
			v, err := DecodeAttachedPiAPIInvocationPolicyV1(mutant.raw)
			if err == nil || v != (AttachedPiAPIInvocationPolicyV1{}) {
				t.Errorf("%s: got partial/non-error result err=%v", mutant.name, err)
			}
		})
	}
}

func TestAttachedPiAPIStructuralInvalidFields(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*AttachedPiAPIInvocationPolicyV1)
	}{
		{"version", func(v *AttachedPiAPIInvocationPolicyV1) { v.Version = 2 }},
		{"owner_mismatch", func(v *AttachedPiAPIInvocationPolicyV1) { v.Scope.OwnerUserID = "foreign-owner" }},
		{"non_pi", func(v *AttachedPiAPIInvocationPolicyV1) { v.Scope.Backend.BackendKind = HarnessBackendCodexExecV1 }},
		{"subscription", func(v *AttachedPiAPIInvocationPolicyV1) { v.Scope.Resource.Kind = ProviderResourceSubscriptionV1 }},
		{"router", func(v *AttachedPiAPIInvocationPolicyV1) { v.Scope.Resource.Kind = ProviderResourceRouterAccountV1 }},
		{"environment", func(v *AttachedPiAPIInvocationPolicyV1) {
			v.Scope.Backend.CredentialDeliveryKind = ProviderCredentialDeliveryEnvironmentV1
		}},
		{"zero_generation", func(v *AttachedPiAPIInvocationPolicyV1) { v.Scope.Resource.CredentialGeneration = 0 }},
		{"zero_revision", func(v *AttachedPiAPIInvocationPolicyV1) { v.Scope.Resource.Revision = 0 }},
		{"unknown_data", func(v *AttachedPiAPIInvocationPolicyV1) { v.InputDataClass = "unknown" }},
		{"short_digest", func(v *AttachedPiAPIInvocationPolicyV1) { v.EffectivePolicyDigest = "a" }},
		{"uppercase_digest", func(v *AttachedPiAPIInvocationPolicyV1) { v.PriceObservationDigest = strings.Repeat("A", 64) }},
		{"invalid_currency", func(v *AttachedPiAPIInvocationPolicyV1) { v.Currency = "usd" }},
		{"zero_context", func(v *AttachedPiAPIInvocationPolicyV1) { v.MaxContextTokens = 0 }},
		{"zero_output", func(v *AttachedPiAPIInvocationPolicyV1) { v.MaxOutputTokens = 0 }},
		{"missing_time", func(v *AttachedPiAPIInvocationPolicyV1) { v.NotBefore = time.Time{} }},
		{"reversed_time", func(v *AttachedPiAPIInvocationPolicyV1) { v.ExpiresAt = v.NotBefore }},
		{"noncanonical_utc", func(v *AttachedPiAPIInvocationPolicyV1) { v.NotBefore = v.NotBefore.In(time.FixedZone("UTC-alias", 0)) }},
		{"model_token_overflow", func(v *AttachedPiAPIInvocationPolicyV1) { v.ModelID = strings.Repeat("m", 257) }},
		{"unsafe_model", func(v *AttachedPiAPIInvocationPolicyV1) { v.ModelVendorID = "fixture\nvendor" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, _, _ := attachedPiAPIStructuralFixture(t)
			tc.change(&v)
			if raw, err := EncodeAttachedPiAPIInvocationPolicyV1(v); err == nil || raw != nil {
				t.Errorf("%s encoded invalid structure: err=%v bytes=%d", tc.name, err, len(raw))
			}
			if digest, err := v.Digest(); err == nil || digest != "" {
				t.Errorf("%s hashed invalid structure: err=%v digest=%q", tc.name, err, digest)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*AttachedPiAPIOwnerAuthorizationReceiptV1)
	}{
		{"version", func(v *AttachedPiAPIOwnerAuthorizationReceiptV1) { v.Version = 2 }},
		{"empty_issuer", func(v *AttachedPiAPIOwnerAuthorizationReceiptV1) { v.IssuerID = "" }},
		{"invalid_source", func(v *AttachedPiAPIOwnerAuthorizationReceiptV1) { v.DecisionSourceID = "unsafe/source" }},
		{"subject", func(v *AttachedPiAPIOwnerAuthorizationReceiptV1) { v.SubjectTermsDigest = "missing" }},
		{"decision", func(v *AttachedPiAPIOwnerAuthorizationReceiptV1) { v.Decision = "eligible" }},
		{"security_version", func(v *AttachedPiAPIOwnerAuthorizationReceiptV1) { v.MembershipSecurityVersion = 0 }},
		{"expiry", func(v *AttachedPiAPIOwnerAuthorizationReceiptV1) { v.ExpiresAt = v.IssuedAt }},
	} {
		t.Run("receipt/"+tc.name, func(t *testing.T) {
			_, r, _ := attachedPiAPIStructuralFixture(t)
			tc.change(&r)
			if raw, err := EncodeAttachedPiAPIOwnerAuthorizationReceiptV1(r); err == nil || raw != nil {
				t.Errorf("receipt/%s encoded invalid structure: err=%v bytes=%d", tc.name, err, len(raw))
			}
		})
	}
}

func TestAttachedPiAPIStructuralLinksAndMutationDoNotChangeBaseEvidence(t *testing.T) {
	t.Parallel()
	terms, receipt, mapping := attachedPiAPIStructuralFixture(t)
	beforeScope, beforePolicy, beforePrice := terms.Scope, terms.EffectivePolicyDigest, terms.PriceObservationDigest
	for _, tc := range []struct {
		name   string
		change func(*AttachedPiAPIInvocationPolicyV1)
	}{
		{"cap", func(v *AttachedPiAPIInvocationPolicyV1) { v.MaxChargeMicrounits++ }},
		{"price", func(v *AttachedPiAPIInvocationPolicyV1) { v.PriceObservationDigest = strings.Repeat("f", 64) }},
		{"owner", func(v *AttachedPiAPIInvocationPolicyV1) {
			v.Scope.OwnerUserID = "other-owner"
			v.Scope.Resource.OwnerUserID = "other-owner"
		}},
		{"resource", func(v *AttachedPiAPIInvocationPolicyV1) { v.Scope.Resource.ResourceID = "other-api-account" }},
		{"model", func(v *AttachedPiAPIInvocationPolicyV1) { v.ModelID = "other-model" }},
		{"data", func(v *AttachedPiAPIInvocationPolicyV1) { v.InputDataClass = ProviderDataPublicV1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutated := terms
			tc.change(&mutated)
			if err := mapping.ValidateFor(mutated, receipt); err == nil {
				t.Errorf("%s: original structural T/R/M incorrectly matches changed terms", tc.name)
			}
			if err := mapping.ValidateFor(terms, receipt); err != nil {
				t.Errorf("%s restored original control: %v", tc.name, err)
			}
		})
	}
	for _, mutate := range []func(*AttachedPiAPIOwnerAuthorizationReceiptV1){
		func(r *AttachedPiAPIOwnerAuthorizationReceiptV1) { r.TenantID = "foreign-tenant" },
		func(r *AttachedPiAPIOwnerAuthorizationReceiptV1) { r.OwnerUserID = "foreign-owner" },
		func(r *AttachedPiAPIOwnerAuthorizationReceiptV1) {
			r.DecisionSourceID = "other-still-unauthenticated-source"
		},
	} {
		r := receipt
		mutate(&r)
		if err := mapping.ValidateFor(terms, r); err == nil {
			t.Errorf("original mapping accepted changed receipt")
		}
	}
	if terms.Scope != beforeScope || terms.EffectivePolicyDigest != beforePolicy || terms.PriceObservationDigest != beforePrice {
		t.Fatal("pure terms/receipt/mapping operations mutated original P/E")
	}
	// A fabricated but structurally matching receipt still passes this purely
	// structural checker. No live/owner issuer/source verifier exists here.
	if err := receipt.ValidateForTerms(terms); err != nil {
		t.Fatalf("explicit untrusted structural fixture rejected: %v", err)
	}
}

func TestAttachedPiAPIMappingAndDigestShape(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*AttachedPiAPIInvocationPolicyMappingV1)
	}{
		{"version", func(v *AttachedPiAPIInvocationPolicyMappingV1) { v.Version = 0 }},
		{"missing_terms", func(v *AttachedPiAPIInvocationPolicyMappingV1) { v.TermsDigest = "" }},
		{"uppercase_receipt", func(v *AttachedPiAPIInvocationPolicyMappingV1) {
			v.OwnerAuthorizationReceiptDigest = ProviderEvidenceDigestV1(strings.Repeat("A", 64))
		}},
		{"short_receipt", func(v *AttachedPiAPIInvocationPolicyMappingV1) { v.OwnerAuthorizationReceiptDigest = "a" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, v := attachedPiAPIStructuralFixture(t)
			tc.change(&v)
			if digest, err := v.Digest(); err == nil || digest != "" {
				t.Errorf("mapping/%s invalid digest=%q err=%v", tc.name, digest, err)
			}
		})
	}
	for _, value := range []string{"", "a", strings.Repeat("A", 64), strings.Repeat("0", 64)} {
		if err := AttachedPiAPIInvocationPolicyMappingDigestV1(value).Validate(); err == nil {
			t.Errorf("invalid or zero mapping digest accepted, len=%d", len(value))
		}
	}
	_, _, v := attachedPiAPIStructuralFixture(t)
	d, err := v.Digest()
	if err != nil || d.Validate() != nil {
		t.Errorf("restored structural mapping digest invalid=%q err=%v", d, err)
	}
}

func TestAttachedPiAPIMetadataMaximumKnownFieldBounds(t *testing.T) {
	t.Parallel()
	v, _, _ := attachedPiAPIStructuralFixture(t)
	v.Scope.TenantID, v.Scope.OwnerUserID = TenantID(strings.Repeat("t", maxOpaqueIDLength)), UserID(strings.Repeat("o", maxOpaqueIDLength))
	v.Scope.Resource.OwnerUserID, v.Scope.Resource.ResourceID = v.Scope.OwnerUserID, strings.Repeat("r", maxOpaqueIDLength)
	v.Scope.Backend.HarnessVersion, v.Scope.Backend.NativeProtocolVersion = strings.Repeat("<", 128), strings.Repeat(">", 128)
	v.ModelVendorID, v.ModelID = strings.Repeat("&", 128), strings.Repeat("<", 256)
	v.MaxContextTokens, v.MaxOutputTokens, v.MaxChargeMicrounits = math.MaxUint64, math.MaxUint64, math.MaxUint64
	raw, err := EncodeAttachedPiAPIInvocationPolicyV1(v)
	if err != nil {
		t.Fatalf("maximum existing field bounds/JSON escaping: %v", err)
	}
	got, err := DecodeAttachedPiAPIInvocationPolicyV1(raw)
	if err != nil || got != v {
		t.Errorf("maximum structural metadata bytes=%d limit=%d gotMatch=%v err=%v", len(raw), attachedPiAPICanonicalMetadataMaxBytesV1, got == v, err)
	}
}

func TestAttachedPiAPIMaximumChargeArithmetic(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                       string
		input, output, inputRate, outputRate, want uint64
		fail                                       bool
	}{
		{"independent_fractional_rounds", 1, 1, 1, 1, 2, false},
		{"exact_million", 1000000, 1000000, 7, 11, 18, false},
		{"below_exact_boundary", 999999, 0, 1, 0, 1, false},
		{"above_exact_boundary", 1000001, 0, 1, 0, 2, false},
		{"zero_rates_not_permission", math.MaxUint64, math.MaxUint64, 0, 0, 0, false},
		{"zero_tokens_not_permission", 0, 0, math.MaxUint64, math.MaxUint64, 0, false},
		{"maximum_product_no_ceil_add_overflow", math.MaxUint64, 0, 1, 0, math.MaxUint64/1000000 + 1, false},
		{"both_maximum_products_sum_reachable", math.MaxUint64, math.MaxUint64, 1, 1, 2 * (math.MaxUint64/1000000 + 1), false},
		{"input_multiply_overflow", math.MaxUint64, 1, 2, 1, 0, true},
		{"output_multiply_overflow", 1, math.MaxUint64, 1, 2, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CalculateAttachedPiAPIMaximumChargeV1(tc.input, tc.output, tc.inputRate, tc.outputRate)
			if tc.fail {
				if err == nil || got != 0 {
					t.Errorf("%s overflow got=%d err=%v; want zero/error", tc.name, got, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Errorf("%s I/O=%d/%d rates=%d/%d got=%d err=%v want=%d", tc.name, tc.input, tc.output, tc.inputRate, tc.outputRate, got, err, tc.want)
			}
			// Independent arbitrary-precision oracle, not the production helper.
			want := new(big.Int)
			for _, pair := range [][2]uint64{{tc.input, tc.inputRate}, {tc.output, tc.outputRate}} {
				product := new(big.Int).Mul(new(big.Int).SetUint64(pair[0]), new(big.Int).SetUint64(pair[1]))
				product.Add(product, big.NewInt(999999)).Quo(product, big.NewInt(1000000))
				want.Add(want, product)
			}
			if want.Cmp(new(big.Int).SetUint64(got)) != 0 {
				t.Errorf("%s independent ceil oracle=%s got=%d", tc.name, want, got)
			}
		})
	}
	terms, _, _ := attachedPiAPIStructuralFixture(t)
	terms.MaxChargeMicrounits = 0
	if err := terms.Validate(); err != nil {
		t.Errorf("explicit structural zero cap cannot establish free permission but is representable: %v", err)
	}
}

func TestAttachedPiAPIChargeDefensiveSumPrimitive(t *testing.T) {
	t.Parallel()
	// This is NOT reachable through the public two-component calculator after
	// uint64 products are checked and divided by 1e6. Test the defensive primitive
	// honestly, rather than fabricating an end-to-end sum-overflow case.
	if got, err := sumAttachedPiAPIChargeV1(math.MaxUint64, 1); err == nil || got != 0 {
		t.Errorf("defensive primitive overflow got=%d err=%v want zero/error", got, err)
	}
	if got, err := sumAttachedPiAPIChargeV1(math.MaxUint64-1, 1); err != nil || got != math.MaxUint64 {
		t.Errorf("defensive primitive exact boundary got=%d err=%v want=%d", got, err, uint64(math.MaxUint64))
	}
}

func FuzzAttachedPiAPICanonicalMetadataV1(f *testing.F) {
	terms, receipt, mapping := attachedPiAPIStructuralFixture(f)
	for _, v := range []any{terms, receipt, mapping} {
		raw, err := json.Marshal(v)
		if err != nil {
			f.Fatalf("seed marshal: %v", err)
		}
		f.Add(raw)
		f.Add(bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1))
		f.Add(append(bytes.Clone(raw[:len(raw)-1]), []byte(`,"mapping_digest":"`+strings.Repeat("a", 64)+`"}`)...))
	}
	f.Add([]byte{})
	f.Add([]byte{0xff})
	f.Add(bytes.Repeat([]byte{' '}, attachedPiAPICanonicalMetadataMaxBytesV1+1))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if v, err := DecodeAttachedPiAPIInvocationPolicyV1(raw); err == nil {
			encoded, e := EncodeAttachedPiAPIInvocationPolicyV1(v)
			if e != nil || !bytes.Equal(encoded, raw) {
				t.Fatalf("accepted noncanonical T len=%d err=%v", len(raw), e)
			}
		} else if v != (AttachedPiAPIInvocationPolicyV1{}) {
			t.Fatal("T failure returned partial record")
		}
		if v, err := DecodeAttachedPiAPIOwnerAuthorizationReceiptV1(raw); err == nil {
			encoded, e := EncodeAttachedPiAPIOwnerAuthorizationReceiptV1(v)
			if e != nil || !bytes.Equal(encoded, raw) {
				t.Fatalf("accepted noncanonical R len=%d err=%v", len(raw), e)
			}
		} else if v != (AttachedPiAPIOwnerAuthorizationReceiptV1{}) {
			t.Fatal("R failure returned partial record")
		}
		if v, err := DecodeAttachedPiAPIInvocationPolicyMappingV1(raw); err == nil {
			encoded, e := EncodeAttachedPiAPIInvocationPolicyMappingV1(v)
			if e != nil || !bytes.Equal(encoded, raw) {
				t.Fatalf("accepted noncanonical M len=%d err=%v", len(raw), e)
			}
		} else if v != (AttachedPiAPIInvocationPolicyMappingV1{}) {
			t.Fatal("M failure returned partial record")
		}
	})
}
