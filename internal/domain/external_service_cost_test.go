package domain_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func externalServiceCostFixture(t testing.TB) (domain.ExternalServiceCostCeilingV1, time.Time) {
	t.Helper()
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	amount := uint64(20_000)
	value := domain.ExternalServiceCostCeilingV1{
		Version: domain.ExternalServiceCostCeilingVersionV1, Kind: domain.ExternalServiceWebSearchV1,
		TenantID: "tenant-external", OwnerUserID: "owner-external", BillingOwnerUserID: "billing-external",
		ResourceID: "search-resource", ResourceRevision: 2, RevocationGeneration: 3,
		ProfileID: "public-web-basic", ProfileRevision: 4, ScopedGrantDigest: strings.Repeat("a", 64),
		Currency: "USD", PriceRevision: "price-5", PriceObservedAt: at, PriceExpiresAt: at.Add(time.Hour),
		PriceEvidenceDigest: strings.Repeat("b", 64), TermsEvidenceDigest: strings.Repeat("c", 64),
		PriceState: domain.ExternalServicePriceKnownPaidV1, MaxAmountMicrounits: &amount,
	}
	if err := value.ValidateAt(at); err != nil {
		t.Fatalf("valid external service fixture at %s: %v", at, err)
	}
	return value, at
}

func TestExternalServiceCostPriceStatesRemainDistinct(t *testing.T) {
	t.Parallel()
	paid, at := externalServiceCostFixture(t)
	free := paid.Clone()
	free.PriceState = domain.ExternalServicePriceKnownFreeV1
	*free.MaxAmountMicrounits = 0
	unknown := free.Clone()
	unknown.PriceState = domain.ExternalServicePriceUnknownV1
	unknown.MaxAmountMicrounits = nil
	seen := make(map[domain.ExternalServiceCostCeilingDigestV1]string)
	for _, test := range []struct {
		name       string
		value      domain.ExternalServiceCostCeilingV1
		executable bool
	}{
		{name: "known_paid", value: paid, executable: true},
		{name: "known_free", value: free, executable: true},
		{name: "unknown", value: unknown, executable: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.value.Validate(); err != nil {
				t.Fatalf("representable %s price: %v", test.name, err)
			}
			if err := test.value.ValidateAt(at); (err == nil) != test.executable {
				t.Errorf("%s executable at %s: got error %v, want executable=%t", test.name, at, err, test.executable)
			}
			digest, err := test.value.Digest()
			if err != nil {
				t.Fatalf("%s digest: %v", test.name, err)
			}
			if err := digest.Validate(); err != nil {
				t.Errorf("%s digest grammar: %v", test.name, err)
			}
			if prior, exists := seen[digest]; exists {
				t.Errorf("%s collapsed into %s digest %q", test.name, prior, digest)
			}
			seen[digest] = test.name
			encoded, err := json.Marshal(test.value)
			if err != nil {
				t.Fatalf("%s marshal: %v", test.name, err)
			}
			var restored domain.ExternalServiceCostCeilingV1
			if err := json.Unmarshal(encoded, &restored); err != nil {
				t.Fatalf("%s unmarshal: %v", test.name, err)
			}
			restoredDigest, err := restored.Digest()
			if err != nil || restoredDigest != digest {
				t.Errorf("%s round-trip digest got %q, %v; want %q", test.name, restoredDigest, err, digest)
			}
			if (restored.MaxAmountMicrounits == nil) != (test.value.MaxAmountMicrounits == nil) {
				t.Errorf("%s JSON changed explicit nil/zero reservation", test.name)
			}
		})
	}
}

func TestExternalServiceCostFreeRequiresIndependentEvidence(t *testing.T) {
	t.Parallel()
	base, at := externalServiceCostFixture(t)
	base.Kind, base.PriceState = domain.ExternalServiceMCPToolV1, domain.ExternalServicePriceKnownFreeV1
	*base.MaxAmountMicrounits = 0
	if err := base.ValidateAt(at); err != nil {
		t.Fatalf("explicit free MCP with independent evidence: %v", err)
	}
	for _, field := range []string{"PriceEvidenceDigest", "TermsEvidenceDigest"} {
		t.Run(field, func(t *testing.T) {
			candidate := base.Clone()
			reflect.ValueOf(&candidate).Elem().FieldByName(field).SetString("")
			if err := candidate.ValidateAt(at); err == nil {
				t.Errorf("explicit zero without %s authorized execution; anonymous access is not free-price proof", field)
			}
		})
	}
}

func TestExternalServiceCostRejectsInvalidStructure(t *testing.T) {
	t.Parallel()
	base, _ := externalServiceCostFixture(t)
	for _, test := range []struct {
		name   string
		mutate func(*domain.ExternalServiceCostCeilingV1)
	}{
		{"version", func(v *domain.ExternalServiceCostCeilingV1) { v.Version = 0 }},
		{"kind_missing", func(v *domain.ExternalServiceCostCeilingV1) { v.Kind = "" }},
		{"kind_model", func(v *domain.ExternalServiceCostCeilingV1) { v.Kind = "model" }},
		{"resource_revision", func(v *domain.ExternalServiceCostCeilingV1) { v.ResourceRevision = 0 }},
		{"revocation_generation", func(v *domain.ExternalServiceCostCeilingV1) { v.RevocationGeneration = 0 }},
		{"profile_revision", func(v *domain.ExternalServiceCostCeilingV1) { v.ProfileRevision = 0 }},
		{"currency_missing", func(v *domain.ExternalServiceCostCeilingV1) { v.Currency = "" }},
		{"currency_lowercase", func(v *domain.ExternalServiceCostCeilingV1) { v.Currency = "usd" }},
		{"currency_non_ascii", func(v *domain.ExternalServiceCostCeilingV1) { v.Currency = "USД" }},
		{"observation_missing", func(v *domain.ExternalServiceCostCeilingV1) { v.PriceObservedAt = time.Time{} }},
		{"expiry_missing", func(v *domain.ExternalServiceCostCeilingV1) { v.PriceExpiresAt = time.Time{} }},
		{"expiry_equal", func(v *domain.ExternalServiceCostCeilingV1) { v.PriceExpiresAt = v.PriceObservedAt }},
		{"expiry_before", func(v *domain.ExternalServiceCostCeilingV1) {
			v.PriceExpiresAt = v.PriceObservedAt.Add(-time.Nanosecond)
		}},
		{"observation_outside_canonical_range", func(v *domain.ExternalServiceCostCeilingV1) {
			v.PriceObservedAt = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
		}},
		{"expiry_outside_canonical_range", func(v *domain.ExternalServiceCostCeilingV1) {
			v.PriceExpiresAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		}},
		{"price_state_missing", func(v *domain.ExternalServiceCostCeilingV1) { v.PriceState = "" }},
		{"price_state_provider_alias", func(v *domain.ExternalServiceCostCeilingV1) { v.PriceState = "known" }},
		{"paid_nil", func(v *domain.ExternalServiceCostCeilingV1) { v.MaxAmountMicrounits = nil }},
		{"paid_zero", func(v *domain.ExternalServiceCostCeilingV1) { *v.MaxAmountMicrounits = 0 }},
		{"free_nil", func(v *domain.ExternalServiceCostCeilingV1) {
			v.PriceState = domain.ExternalServicePriceKnownFreeV1
			v.MaxAmountMicrounits = nil
		}},
		{"free_nonzero", func(v *domain.ExternalServiceCostCeilingV1) { v.PriceState = domain.ExternalServicePriceKnownFreeV1 }},
		{"unknown_nonzero", func(v *domain.ExternalServiceCostCeilingV1) { v.PriceState = domain.ExternalServicePriceUnknownV1 }},
		{"unknown_zero", func(v *domain.ExternalServiceCostCeilingV1) {
			v.PriceState = domain.ExternalServicePriceUnknownV1
			*v.MaxAmountMicrounits = 0
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := base.Clone()
			test.mutate(&candidate)
			assertExternalCostInvalid(t, candidate)
		})
	}
	for _, field := range []string{"TenantID", "OwnerUserID", "BillingOwnerUserID", "ResourceID", "ProfileID", "PriceRevision"} {
		for _, test := range []struct{ name, value string }{
			{"empty", ""}, {"leading_space", " id"}, {"trailing_space", "id "},
			{"slash", "id/path"}, {"unicode", "ид"}, {"newline", "id\n"},
			{"oversized", strings.Repeat("x", 161)},
		} {
			t.Run(field+"/"+test.name, func(t *testing.T) {
				candidate := base.Clone()
				reflect.ValueOf(&candidate).Elem().FieldByName(field).SetString(test.value)
				assertExternalCostInvalid(t, candidate)
			})
		}
	}
	for _, field := range []string{"ScopedGrantDigest", "PriceEvidenceDigest", "TermsEvidenceDigest"} {
		for _, test := range []struct{ name, value string }{
			{"empty", ""}, {"short", strings.Repeat("a", 63)}, {"long", strings.Repeat("a", 65)},
			{"nonhex", strings.Repeat("g", 64)}, {"uppercase", strings.Repeat("A", 64)},
			{"whitespace", " " + strings.Repeat("a", 64)}, {"oversized", strings.Repeat("a", 10_000)},
		} {
			t.Run(field+"/"+test.name, func(t *testing.T) {
				candidate := base.Clone()
				reflect.ValueOf(&candidate).Elem().FieldByName(field).SetString(test.value)
				assertExternalCostInvalid(t, candidate)
			})
		}
	}
	assertExternalCostInvalid(t, domain.ExternalServiceCostCeilingV1{})
}

func assertExternalCostInvalid(t testing.TB, value domain.ExternalServiceCostCeilingV1) {
	t.Helper()
	if err := value.Validate(); err == nil {
		t.Error("invalid external cost structure passed Validate")
	}
	if digest, err := value.Digest(); err == nil || digest != "" {
		t.Errorf("invalid external cost Digest got %q, %v; want empty digest and error", digest, err)
	}
	if err := value.ValidateAt(time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)); err == nil {
		t.Error("invalid external cost authorized execution")
	}
}

func TestExternalServiceCostPriceWindow(t *testing.T) {
	t.Parallel()
	value, at := externalServiceCostFixture(t)
	for _, test := range []struct {
		name  string
		now   time.Time
		valid bool
	}{
		{name: "zero", now: time.Time{}},
		{name: "future_observation", now: at.Add(-time.Nanosecond)},
		{name: "at_observation", now: at, valid: true},
		{name: "before_expiry", now: value.PriceExpiresAt.Add(-time.Nanosecond), valid: true},
		{name: "at_expiry", now: value.PriceExpiresAt},
		{name: "after_expiry", now: value.PriceExpiresAt.Add(time.Nanosecond)},
		{name: "same_instant_other_zone", now: at.In(time.FixedZone("test", 3600)), valid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := value.ValidateAt(test.now); (err == nil) != test.valid {
				t.Errorf("price window [%s, %s) at %s: error=%v; want valid=%t", at, value.PriceExpiresAt, test.now, err, test.valid)
			}
		})
	}
}

func TestExternalServiceCostExactOwnerGrantCurrency(t *testing.T) {
	t.Parallel()
	value, _ := externalServiceCostFixture(t)
	check := func(v domain.ExternalServiceCostCeilingV1) error {
		return value.ValidateForOwnerGrantCurrency(v.TenantID, v.OwnerUserID, v.BillingOwnerUserID, v.ScopedGrantDigest, v.Currency)
	}
	if err := check(value); err != nil {
		t.Fatalf("exact owner/grant/currency: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*domain.ExternalServiceCostCeilingV1)
	}{
		{"tenant", func(v *domain.ExternalServiceCostCeilingV1) { v.TenantID = "tenant-other" }},
		{"owner", func(v *domain.ExternalServiceCostCeilingV1) { v.OwnerUserID = "owner-other" }},
		{"billing_owner", func(v *domain.ExternalServiceCostCeilingV1) { v.BillingOwnerUserID = "billing-other" }},
		{"grant", func(v *domain.ExternalServiceCostCeilingV1) { v.ScopedGrantDigest = strings.Repeat("d", 64) }},
		{"currency", func(v *domain.ExternalServiceCostCeilingV1) { v.Currency = "EUR" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			other := value.Clone()
			test.mutate(&other)
			if err := check(other); err == nil {
				t.Errorf("divergent %s accepted by owner/grant/currency composition check", test.name)
			}
		})
	}
	invalid := value.Clone()
	invalid.MaxAmountMicrounits = nil
	if err := invalid.ValidateForOwnerGrantCurrency(value.TenantID, value.OwnerUserID, value.BillingOwnerUserID, value.ScopedGrantDigest, value.Currency); err == nil {
		t.Error("exact identity excused invalid reservation structure")
	}
}

func TestExternalServiceCostDigestBindsEveryField(t *testing.T) {
	t.Parallel()
	base, _ := externalServiceCostFixture(t)
	digest, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	// Independently calculated from the framed version-1 fixture transcript.
	// This also fences correlated state/amount mutations against field omission.
	const golden = "dfefeee69dcbb5fbc4e4bb9af0953f740d38cb53a6b49f79109761a9075219bf"
	if string(digest) != golden {
		t.Errorf("canonical fixture digest got %q, want %q", digest, golden)
	}
	mutations := map[string]func(*domain.ExternalServiceCostCeilingV1){
		"Version":              func(v *domain.ExternalServiceCostCeilingV1) { v.Version++ },
		"Kind":                 func(v *domain.ExternalServiceCostCeilingV1) { v.Kind = domain.ExternalServiceMCPToolV1 },
		"TenantID":             func(v *domain.ExternalServiceCostCeilingV1) { v.TenantID += "-other" },
		"OwnerUserID":          func(v *domain.ExternalServiceCostCeilingV1) { v.OwnerUserID += "-other" },
		"BillingOwnerUserID":   func(v *domain.ExternalServiceCostCeilingV1) { v.BillingOwnerUserID += "-other" },
		"ResourceID":           func(v *domain.ExternalServiceCostCeilingV1) { v.ResourceID += "-other" },
		"ResourceRevision":     func(v *domain.ExternalServiceCostCeilingV1) { v.ResourceRevision++ },
		"RevocationGeneration": func(v *domain.ExternalServiceCostCeilingV1) { v.RevocationGeneration++ },
		"ProfileID":            func(v *domain.ExternalServiceCostCeilingV1) { v.ProfileID += "-other" },
		"ProfileRevision":      func(v *domain.ExternalServiceCostCeilingV1) { v.ProfileRevision++ },
		"ScopedGrantDigest":    func(v *domain.ExternalServiceCostCeilingV1) { v.ScopedGrantDigest = strings.Repeat("d", 64) },
		"Currency":             func(v *domain.ExternalServiceCostCeilingV1) { v.Currency = "EUR" },
		"PriceRevision":        func(v *domain.ExternalServiceCostCeilingV1) { v.PriceRevision += "-other" },
		"PriceObservedAt": func(v *domain.ExternalServiceCostCeilingV1) {
			v.PriceObservedAt = v.PriceObservedAt.Add(time.Nanosecond)
		},
		"PriceExpiresAt":      func(v *domain.ExternalServiceCostCeilingV1) { v.PriceExpiresAt = v.PriceExpiresAt.Add(time.Nanosecond) },
		"PriceEvidenceDigest": func(v *domain.ExternalServiceCostCeilingV1) { v.PriceEvidenceDigest = strings.Repeat("e", 64) },
		"TermsEvidenceDigest": func(v *domain.ExternalServiceCostCeilingV1) { v.TermsEvidenceDigest = strings.Repeat("f", 64) },
		"PriceState": func(v *domain.ExternalServiceCostCeilingV1) {
			v.PriceState = domain.ExternalServicePriceUnknownV1
			v.MaxAmountMicrounits = nil
		},
		"MaxAmountMicrounits": func(v *domain.ExternalServiceCostCeilingV1) { *v.MaxAmountMicrounits++ },
	}
	typ := reflect.TypeOf(base)
	if len(mutations) != typ.NumField() {
		t.Fatalf("digest mutation coverage got %d fields, want all %d", len(mutations), typ.NumField())
	}
	for index := 0; index < typ.NumField(); index++ {
		name := typ.Field(index).Name
		mutate, ok := mutations[name]
		if !ok {
			t.Fatalf("missing digest mutation case for %s", name)
		}
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			mutate(&candidate)
			got, err := candidate.Digest()
			if name == "Version" {
				if err == nil || got != "" {
					t.Errorf("unsupported version digest got %q, %v; want rejection", got, err)
				}
				return
			}
			if err != nil || got == digest {
				t.Errorf("%s mutation digest got %q, %v; want valid digest distinct from %q", name, got, err, digest)
			}
		})
	}
	// Framed fields must not confuse the same unframed concatenation.
	left, right := base.Clone(), base.Clone()
	left.TenantID, left.OwnerUserID = "a", "bc"
	right.TenantID, right.OwnerUserID = "ab", "c"
	leftDigest, leftErr := left.Digest()
	rightDigest, rightErr := right.Digest()
	if leftErr != nil || rightErr != nil || leftDigest == rightDigest {
		t.Errorf("field framing digests left=%q (%v), right=%q (%v); want distinct valid digests", leftDigest, leftErr, rightDigest, rightErr)
	}
}

func TestExternalServiceCostCloneAndCanonicalBounds(t *testing.T) {
	t.Parallel()
	value, _ := externalServiceCostFixture(t)
	value.PriceObservedAt = value.PriceObservedAt.In(time.FixedZone("fixture", 3600))
	value.PriceExpiresAt = value.PriceExpiresAt.In(time.FixedZone("fixture", 3600))
	clone := value.Clone()
	if clone.MaxAmountMicrounits == value.MaxAmountMicrounits {
		t.Fatal("clone retained mutable amount pointer")
	}
	if clone.PriceObservedAt.Location() != time.UTC || clone.PriceExpiresAt.Location() != time.UTC {
		t.Error("clone timestamps were not UTC-normalized")
	}
	valueDigest, valueErr := value.Digest()
	cloneDigest, cloneErr := clone.Digest()
	if valueErr != nil || cloneErr != nil || valueDigest != cloneDigest {
		t.Errorf("UTC-normalized clone digest=%q (%v), source=%q (%v); want equal", cloneDigest, cloneErr, valueDigest, valueErr)
	}
	*clone.MaxAmountMicrounits++
	if *value.MaxAmountMicrounits != 20_000 {
		t.Errorf("clone amount mutation changed source to %d; want 20000", *value.MaxAmountMicrounits)
	}
	unknown := value.Clone()
	unknown.PriceState, unknown.MaxAmountMicrounits = domain.ExternalServicePriceUnknownV1, nil
	if unknown.Clone().MaxAmountMicrounits != nil {
		t.Error("clone invented numeric bound for unknown price")
	}
	bounded := value.Clone()
	bounded.ResourceID, bounded.ProfileID, bounded.PriceRevision = strings.Repeat("x", 160), strings.Repeat("y", 160), strings.Repeat("z", 160)
	*bounded.MaxAmountMicrounits = ^uint64(0)
	if err := bounded.Validate(); err != nil {
		t.Errorf("maximum bounded IDs/uint reservation rejected: %v", err)
	}
	for _, invalid := range []string{"", strings.Repeat("a", 63), strings.Repeat("A", 64), strings.Repeat("g", 64), strings.Repeat("a", 65)} {
		if err := domain.ExternalServiceCostCeilingDigestV1(invalid).Validate(); err == nil {
			t.Errorf("invalid component digest %q accepted", invalid)
		}
	}
}
