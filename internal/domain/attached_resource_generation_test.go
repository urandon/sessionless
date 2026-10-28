package domain_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func attachedGenerationBaseline(t *testing.T) domain.AttachedResourceGenerationBaselineV1 {
	t.Helper()
	bundle, binding := providerBundleFixture(t)
	binding.Backend.BackendKind = domain.HarnessBackendCodexExecV1
	binding.Backend.ArtifactKind = domain.HarnessArtifactExecutableV1
	binding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
	binding.Resource.Kind = domain.ProviderResourceSubscriptionV1
	binding.Resource.ResourceID = "subscription-generation"
	for _, scope := range []*domain.ProviderEvidenceScopeV1{&bundle.Catalog.Scope, &bundle.Route.Scope, &bundle.Capability.Scope, &bundle.Privacy.Scope, &bundle.Price.Scope, &bundle.Policy.Scope} {
		scope.Resource, scope.Backend = binding.Resource, binding.Backend
	}
	bundle.Route.Routes[0].BackendKind = binding.Backend.BackendKind
	bundle.Route.Routes[0].TransportKind = domain.ProviderTransportLocalCLIV1
	bundle.Route.Routes[0].BillingKind = domain.ProviderBillingSubscriptionV1
	bundle.Route.Routes[0].BillingAuthority = binding.Resource.ResourceID
	// Mixed evidence windows and named unknowns must survive projection.
	bundle.Capability.ToolCalling = domain.ProviderEvidenceUnknownV1
	bundle.Capability.StructuredOutput = domain.ProviderEvidenceUnsupportedV1
	bundle.Privacy.RetentionHours = domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}
	bundle.Privacy.ExpiresAt = bundle.Privacy.ExpiresAt.Add(-10 * time.Minute)
	expiry := bundle.Privacy.ExpiresAt
	binding.EvidenceExpiresAt = &expiry
	sealProviderBundleFixture(t, &bundle, &binding)
	binding.RunID, binding.AttemptID, binding.ExecutionPlacementDigest = "", "", ""
	baseline := domain.AttachedResourceGenerationBaselineV1{Version: 1,
		TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID,
		SubscriptionConnectionID: domain.SubscriptionConnectionID(binding.Resource.ResourceID), WorkerID: "worker-generation",
		EnrollmentGeneration: 5, BaselineRevision: binding.Resource.Revision,
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("c", 64)), GenerationOnlyDelegation: true,
		Binding: binding, Evidence: bundle}
	if err := baseline.Validate(); err != nil {
		t.Fatalf("complete generation baseline: %v", err)
	}
	return baseline
}

func TestAttachedResourceGenerationProjectionPreservesCompleteBaseline(t *testing.T) {
	t.Parallel()
	baseline := attachedGenerationBaseline(t)
	before := baseline.Clone()
	for _, generation := range []uint64{1, baseline.Binding.Resource.CredentialGeneration, 99, math.MaxUint64 - 1} {
		t.Run(fmt.Sprint(generation), func(t *testing.T) {
			binding, bundle, err := domain.ProjectAttachedResourceGenerationV1(baseline, generation)
			if err != nil {
				t.Fatalf("generation=%d projection: %v", generation, err)
			}
			wantBinding, wantBundle := baseline.Binding.Clone(), baseline.Evidence.Clone()
			wantBinding.Resource.CredentialGeneration = generation
			for _, scope := range []*domain.ProviderEvidenceScopeV1{&wantBundle.Catalog.Scope, &wantBundle.Route.Scope, &wantBundle.Capability.Scope, &wantBundle.Privacy.Scope, &wantBundle.Price.Scope, &wantBundle.Policy.Scope} {
				scope.Resource.CredentialGeneration = generation
			}
			sealProviderBundleFixture(t, &wantBundle, &wantBinding)
			if !reflect.DeepEqual(binding, wantBinding) || !reflect.DeepEqual(bundle, wantBundle) {
				t.Fatalf("generation=%d changed data outside generation/digest closure", generation)
			}
			if !reflect.DeepEqual(baseline, before) {
				t.Fatal("projection mutated original trusted baseline")
			}
			// The returned template must not contain invented attempt authority.
			if binding.RunID != "" || binding.AttemptID != "" || binding.ExecutionPlacementDigest != "" {
				t.Fatal("projection leaked constructor-only scope")
			}
			bundle.Catalog.Models[0].ModelID = "changed"
			bundle.Route.Routes[0].EndpointID = "changed"
			bundle.Privacy.AllowedDataClasses[0] = domain.ProviderDataPublicV1
			bundle.Policy.AllowedDataClasses[0] = domain.ProviderDataPublicV1
			*binding.EvidenceExpiresAt = time.Time{}
			if !reflect.DeepEqual(baseline, before) {
				t.Fatal("projected result aliases original baseline")
			}
		})
	}
	clone := baseline.Clone()
	clone.Evidence.Catalog.Models[0].ModelID = "clone-only"
	*clone.Binding.EvidenceExpiresAt = time.Time{}
	if !reflect.DeepEqual(baseline, before) {
		t.Fatal("baseline clone shares nested state")
	}
}

func TestAttachedResourceGenerationBaselineDeniesSubstitutions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*domain.AttachedResourceGenerationBaselineV1)
	}{
		{"version", func(b *domain.AttachedResourceGenerationBaselineV1) { b.Version++ }},
		{"delegation absent", func(b *domain.AttachedResourceGenerationBaselineV1) { b.GenerationOnlyDelegation = false }},
		{"tenant", func(b *domain.AttachedResourceGenerationBaselineV1) { b.TenantID = "foreign" }},
		{"owner", func(b *domain.AttachedResourceGenerationBaselineV1) { b.OwnerUserID = "foreign" }},
		{"resource", func(b *domain.AttachedResourceGenerationBaselineV1) { b.SubscriptionConnectionID = "foreign" }},
		{"empty worker", func(b *domain.AttachedResourceGenerationBaselineV1) { b.WorkerID = "" }},
		{"enrollment absent", func(b *domain.AttachedResourceGenerationBaselineV1) { b.EnrollmentGeneration = 0 }},
		{"revision", func(b *domain.AttachedResourceGenerationBaselineV1) { b.BaselineRevision++ }},
		{"capability", func(b *domain.AttachedResourceGenerationBaselineV1) { b.CapabilityDigest = "invalid" }},
		{"bound run", func(b *domain.AttachedResourceGenerationBaselineV1) { b.Binding.RunID = "run" }},
		{"bound attempt", func(b *domain.AttachedResourceGenerationBaselineV1) { b.Binding.AttemptID = "attempt" }},
		{"bound placement", func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.Binding.ExecutionPlacementDigest = strings.Repeat("d", 64)
		}},
		{"not subscription", func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.Binding.Resource.Kind = domain.ProviderResourceAPIAccountV1
		}},
		{"zero generation", func(b *domain.AttachedResourceGenerationBaselineV1) { b.Binding.Resource.CredentialGeneration = 0 }},
		{"reserved generation", func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.Binding.Resource.CredentialGeneration = math.MaxUint64
		}},
		{"model", func(b *domain.AttachedResourceGenerationBaselineV1) { b.Binding.ModelID = "foreign-model" }},
		{"backend", func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.Binding.Backend.ArtifactDigest = strings.Repeat("d", 64)
		}},
		{"catalog digest", func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.Binding.ProviderCatalogDigest = strings.Repeat("d", 64)
		}},
		{"policy digest", func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.Binding.EffectivePolicyDigest = strings.Repeat("d", 64)
		}},
		{"policy source", func(b *domain.AttachedResourceGenerationBaselineV1) { b.Evidence.Policy.EvidenceSource = "changed" }},
		{"expiry renewal", func(b *domain.AttachedResourceGenerationBaselineV1) {
			*b.Binding.EvidenceExpiresAt = b.Binding.EvidenceExpiresAt.Add(time.Hour)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := attachedGenerationBaseline(t)
			tc.mutate(&b)
			if err := b.Validate(); err == nil {
				t.Fatal("changed baseline accepted")
			}
			if binding, bundle, err := domain.ProjectAttachedResourceGenerationV1(b, 9); err == nil || !reflect.DeepEqual(binding, domain.HarnessBindingV2{}) || !reflect.DeepEqual(bundle, domain.ProviderEvidenceBundleV1{}) {
				t.Fatal("invalid baseline returned usable projected data")
			}
		})
	}
	for _, name := range []string{"catalog", "route", "capability", "privacy", "price", "policy"} {
		t.Run("scope/"+name, func(t *testing.T) {
			b := attachedGenerationBaseline(t)
			scopes := map[string]*domain.ProviderEvidenceScopeV1{"catalog": &b.Evidence.Catalog.Scope, "route": &b.Evidence.Route.Scope, "capability": &b.Evidence.Capability.Scope, "privacy": &b.Evidence.Privacy.Scope, "price": &b.Evidence.Price.Scope, "policy": &b.Evidence.Policy.Scope}
			scopes[name].Resource.CredentialGeneration++
			if err := b.Validate(); err == nil {
				t.Fatal("foreign evidence scope accepted")
			}
		})
	}
	for _, generation := range []uint64{0, math.MaxUint64} {
		if _, _, err := domain.ProjectAttachedResourceGenerationV1(attachedGenerationBaseline(t), generation); err == nil {
			t.Errorf("invalid active generation=%d accepted", generation)
		}
	}
}

func TestAttachedResourceGenerationBaselineEncodedBound(t *testing.T) {
	t.Parallel()
	b := attachedGenerationBaseline(t)
	for i := range 300 {
		b.Evidence.Catalog.Models = append(b.Evidence.Catalog.Models, domain.ProviderCatalogModelV1{ModelVendorID: "zz-vendor", ModelID: fmt.Sprintf("model-%03d-%s", i, strings.Repeat("m", 190)), CatalogDigest: strings.Repeat("a", 64)})
	}
	sort.Slice(b.Evidence.Catalog.Models, func(i, j int) bool {
		a, c := b.Evidence.Catalog.Models[i], b.Evidence.Catalog.Models[j]
		return a.ModelVendorID+"\x00"+a.ModelID < c.ModelVendorID+"\x00"+c.ModelID
	})
	sealProviderBundleFixture(t, &b.Evidence, &b.Binding)
	raw, err := json.Marshal(b)
	if err != nil || len(raw) <= domain.AttachedResourceGenerationBaselineMaxBytesV1 {
		t.Fatalf("oversize fixture: bytes=%d err=%v", len(raw), err)
	}
	if err := b.Validate(); err == nil || !strings.Contains(err.Error(), "bounded complete baseline") {
		t.Fatalf("complete oversized baseline accepted/wrong cause: %v", err)
	}
}

func TestAttachedResourceGenerationProjectionPreservesEmptyUnknownRoutes(t *testing.T) {
	t.Parallel()
	b := attachedGenerationBaseline(t)
	b.Evidence.Route.State = domain.ProviderEvidenceUnknownV1
	b.Evidence.Route.PolicyID, b.Evidence.Route.Revision = "", 0
	b.Evidence.Route.Routes = []domain.ProviderRouteV1{}
	sealProviderBundleFixture(t, &b.Evidence, &b.Binding)
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	if b.Clone().Evidence.Route.Routes == nil {
		t.Fatal("clone normalized explicit empty unknown routes to null")
	}
	binding, bundle, err := domain.ProjectAttachedResourceGenerationV1(b, b.Binding.Resource.CredentialGeneration)
	if err != nil || !reflect.DeepEqual(binding, b.Binding) || !reflect.DeepEqual(bundle, b.Evidence) {
		t.Fatalf("same-generation projection changed unknown route shape/digest: %v", err)
	}
}

func attachedGenerationHead() domain.AttachedResourceGenerationHeadV1 {
	return domain.AttachedResourceGenerationHeadV1{Version: 1, OwnerUserID: "owner", WorkerID: "worker", EnrollmentGeneration: 3, BaselineRevision: 7, CredentialGeneration: 1}
}

func attachedGenerationStringHead(t *testing.T, head domain.AttachedResourceGenerationHeadV1) []byte {
	t.Helper()
	return []byte(fmt.Sprintf(`{"version":1,"owner_user_id":"owner","worker_id":"worker","enrollment_generation":"%d","baseline_revision":"%d","credential_generation":"%d","revoked":%t}`,
		head.EnrollmentGeneration, head.BaselineRevision, head.CredentialGeneration, head.Revoked))
}

func TestAttachedResourceGenerationHeadCanonicalDecimalStrings(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"enrollment_generation", "baseline_revision", "credential_generation"} {
		for _, counter := range []uint64{1<<53 - 1, 1 << 53, 1<<53 + 1, math.MaxUint64} {
			t.Run(fmt.Sprintf("%s/%d", field, counter), func(t *testing.T) {
				head := attachedGenerationHead()
				switch field {
				case "enrollment_generation":
					head.EnrollmentGeneration = counter
				case "baseline_revision":
					head.BaselineRevision = counter
				case "credential_generation":
					head.CredentialGeneration = counter
					head.Revoked = counter == math.MaxUint64
				}
				want := attachedGenerationStringHead(t, head)
				for _, encode := range []struct {
					name string
					fn   func() ([]byte, error)
				}{
					{name: "named", fn: func() ([]byte, error) { return domain.EncodeAttachedResourceGenerationHeadV1(head) }},
					{name: "direct", fn: func() ([]byte, error) { return json.Marshal(head) }},
					{name: "nested", fn: func() ([]byte, error) {
						return json.Marshal(struct {
							Head domain.AttachedResourceGenerationHeadV1 `json:"head"`
						}{Head: head})
					}},
				} {
					raw, err := encode.fn()
					expected := want
					if encode.name == "nested" {
						expected = append(append([]byte(`{"head":`), want...), '}')
					}
					if err != nil || !bytes.Equal(raw, expected) {
						t.Errorf("%s encoded %s=%d as %s, want %s: %v", encode.name, field, counter, raw, expected, err)
					}
				}
				decoded, err := domain.DecodeAttachedResourceGenerationHeadV1(want)
				if err != nil || decoded != head {
					t.Errorf("named exact string roundtrip: got=%+v want=%+v err=%v", decoded, head, err)
				}
				var direct domain.AttachedResourceGenerationHeadV1
				if err := json.Unmarshal(want, &direct); err != nil || direct != head {
					t.Errorf("direct exact string roundtrip: got=%+v want=%+v err=%v", direct, head, err)
				}
				var nested struct {
					Head domain.AttachedResourceGenerationHeadV1 `json:"head"`
				}
				raw := append(append([]byte(`{"head":`), want...), '}')
				if err := json.Unmarshal(raw, &nested); err != nil || nested.Head != head {
					t.Errorf("nested exact string roundtrip: got=%+v want=%+v err=%v", nested.Head, head, err)
				}
			})
		}
	}
}

func TestAttachedResourceGenerationHeadRejectsNoncanonicalCountersEveryJSONPath(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"enrollment_generation", "baseline_revision", "credential_generation"} {
		for _, literal := range []string{`1`, `1.0`, `1e0`, `-1`, `null`, `true`, `""`, `"0"`, `"01"`, `"+1"`, `"-1"`, `" 1"`, `"1 "`, `"1.0"`, `"1e0"`, `"18446744073709551616"`, `"999999999999999999999"`, `"\u0031"`, `"1\n"`} {
			t.Run(field+"/"+literal, func(t *testing.T) {
				var members map[string]json.RawMessage
				if err := json.Unmarshal(attachedGenerationStringHead(t, attachedGenerationHead()), &members); err != nil {
					t.Fatal(err)
				}
				members[field] = json.RawMessage(literal)
				raw, err := json.Marshal(members)
				if err != nil {
					t.Fatal(err)
				}
				got, err := domain.DecodeAttachedResourceGenerationHeadV1(raw)
				if err == nil || got != (domain.AttachedResourceGenerationHeadV1{}) {
					t.Errorf("named decode accepted %s=%s: got=%+v err=%v", field, literal, got, err)
				}
				direct := attachedGenerationHead()
				if err := json.Unmarshal(raw, &direct); err == nil || direct != (domain.AttachedResourceGenerationHeadV1{}) {
					t.Errorf("direct decode accepted %s=%s/retained old authority: got=%+v err=%v", field, literal, direct, err)
				}
				nested := struct {
					Head domain.AttachedResourceGenerationHeadV1 `json:"head"`
				}{Head: attachedGenerationHead()}
				if err := json.Unmarshal(append(append([]byte(`{"head":`), raw...), '}'), &nested); err == nil || nested.Head != (domain.AttachedResourceGenerationHeadV1{}) {
					t.Errorf("nested decode accepted %s=%s/retained old authority: got=%+v err=%v", field, literal, nested.Head, err)
				}
			})
		}
	}
}

func TestAttachedResourceGenerationHeadZeroDenialCarrierIsNotDecodedAuthority(t *testing.T) {
	t.Parallel()
	carrier := struct {
		Status string                                  `json:"status"`
		Head   domain.AttachedResourceGenerationHeadV1 `json:"head"`
	}{Status: "denied"}
	raw, err := json.Marshal(carrier)
	if err != nil || !bytes.Contains(raw, []byte(`"credential_generation":"0"`)) {
		t.Fatalf("zero denial carrier cannot be bounded-serialized: raw=%s err=%v", raw, err)
	}
	if _, err := domain.EncodeAttachedResourceGenerationHeadV1(carrier.Head); err == nil {
		t.Error("zero denial carrier encoded as a valid head")
	}
	if err := json.Unmarshal(raw, &carrier); err == nil || carrier.Head != (domain.AttachedResourceGenerationHeadV1{}) {
		t.Errorf("zero denial carrier decoded as head authority: got=%+v err=%v", carrier.Head, err)
	}
}

func TestAttachedResourceGenerationHeadStrictCodec(t *testing.T) {
	t.Parallel()
	head := attachedGenerationHead()
	raw, err := domain.EncodeAttachedResourceGenerationHeadV1(head)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := domain.DecodeAttachedResourceGenerationHeadV1(raw)
	if err != nil || decoded != head {
		t.Fatalf("head roundtrip: got=%+v err=%v", decoded, err)
	}
	reordered := []byte(` { "revoked":false,"credential_generation":"1","baseline_revision":"7","enrollment_generation":"3","worker_id":"worker","owner_user_id":"owner","version":1 } `)
	if got, err := domain.DecodeAttachedResourceGenerationHeadV1(reordered); err != nil || got != head {
		t.Fatalf("JsonDocument normalization rejected: %v", err)
	}
	// Internal object padding is seen by direct and nested JSON methods too;
	// their enclosing JSON decoder owns any whitespace outside this value.
	exactBound := append(append(append([]byte(nil), raw[:len(raw)-1]...), bytes.Repeat([]byte(" "), domain.AttachedResourceGenerationMetadataMaxBytesV1-len(raw))...), '}')
	if got, err := domain.DecodeAttachedResourceGenerationHeadV1(exactBound); err != nil || got != head {
		t.Fatalf("inclusive 4KiB head bound rejected: %v", err)
	}
	var direct domain.AttachedResourceGenerationHeadV1
	if err := json.Unmarshal(exactBound, &direct); err != nil || direct != head {
		t.Fatalf("direct inclusive 4KiB head bound rejected: %v", err)
	}
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"duplicate", bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)},
		{"escaped duplicate", bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"\u0076ersion":1`), 1)},
		{"unknown", bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"secret":"no"`), 1)},
		{"case alias", bytes.Replace(raw, []byte(`"version"`), []byte(`"Version"`), 1)},
		{"missing revoked", bytes.Replace(raw, []byte(`,"revoked":false`), nil, 1)},
		{"null revoked", bytes.Replace(raw, []byte(`"revoked":false`), []byte(`"revoked":null`), 1)},
		{"null version", bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":null`), 1)},
		{"fraction counter", bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1.0`), 1)},
		{"negative counter", bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":-1`), 1)},
		{"unsupported", bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":2`), 1)},
		{"unicode loss", bytes.Replace(raw, []byte(`"owner"`), []byte(`"\ud800"`), 1)},
		{"trailing", append(append([]byte(nil), raw...), []byte(`{}`)...)},
		{"oversize", append(append(append([]byte(nil), raw[:len(raw)-1]...), bytes.Repeat([]byte(" "), domain.AttachedResourceGenerationMetadataMaxBytesV1)...), '}')},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := domain.DecodeAttachedResourceGenerationHeadV1(tc.raw); err == nil || got != (domain.AttachedResourceGenerationHeadV1{}) {
				t.Fatalf("invalid head yielded data=%+v err=%v", got, err)
			}
			var direct domain.AttachedResourceGenerationHeadV1
			if err := json.Unmarshal(tc.raw, &direct); err == nil || direct != (domain.AttachedResourceGenerationHeadV1{}) {
				t.Errorf("direct invalid head yielded data=%+v err=%v", direct, err)
			}
			var nested struct {
				Head domain.AttachedResourceGenerationHeadV1 `json:"head"`
			}
			if err := json.Unmarshal(append(append([]byte(`{"head":`), tc.raw...), '}'), &nested); err == nil || nested.Head != (domain.AttachedResourceGenerationHeadV1{}) {
				t.Errorf("nested invalid head yielded data=%+v err=%v", nested.Head, err)
			}
		})
	}
	for _, generation := range []uint64{0, math.MaxUint64} {
		bad := head
		bad.CredentialGeneration = generation
		if _, err := domain.EncodeAttachedResourceGenerationHeadV1(bad); err == nil {
			t.Errorf("active generation=%d encoded", generation)
		}
	}
	head.Revoked, head.CredentialGeneration = true, math.MaxUint64
	raw, err = domain.EncodeAttachedResourceGenerationHeadV1(head)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := domain.DecodeAttachedResourceGenerationHeadV1(raw); err != nil || got != head {
		t.Fatalf("final denial: %v", err)
	}
}

func FuzzAttachedResourceGenerationHeadStrictCodec(f *testing.F) {
	head := attachedGenerationHead()
	raw, err := domain.EncodeAttachedResourceGenerationHeadV1(head)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add([]byte(`{"version":1,"Version":1}`))
	f.Add([]byte(`{"version":1,"version":2}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		head, err := domain.DecodeAttachedResourceGenerationHeadV1(raw)
		if err != nil {
			if head != (domain.AttachedResourceGenerationHeadV1{}) {
				t.Fatal("failed decode returned partial authority")
			}
			return
		}
		encoded, err := domain.EncodeAttachedResourceGenerationHeadV1(head)
		if err != nil || len(encoded) > domain.AttachedResourceGenerationMetadataMaxBytesV1 {
			t.Fatalf("decoded head cannot be bounded-encoded: %v", err)
		}
		again, err := domain.DecodeAttachedResourceGenerationHeadV1(encoded)
		if err != nil || again != head {
			t.Fatalf("accepted head lost exact roundtrip: %v", err)
		}
	})
}

func TestAttachedResourceGenerationTransitions(t *testing.T) {
	t.Parallel()
	initial := attachedGenerationHead()
	active := initial
	active.CredentialGeneration = 10
	revoked := active
	revoked.CredentialGeneration, revoked.Revoked = 11, true
	for _, tc := range []struct {
		name       string
		current    *domain.AttachedResourceGenerationHeadV1
		expected   uint64
		generation uint64
		revoked    bool
		want       bool
	}{
		{"initial", nil, 0, 1, false, true}, {"initial active jump", nil, 0, 2, false, false},
		{"initial wrong expected", nil, 1, 1, false, false}, {"initial offline revoke", nil, 0, 9, true, true},
		{"initial final denial", nil, 0, math.MaxUint64, true, true},
		{"unchanged", &active, 10, 10, false, true}, {"duplicate successor", &active, 9, 10, false, true},
		{"duplicate foreign expected", &active, 7, 10, false, false}, {"successor", &active, 10, 11, false, true},
		{"active jump", &active, 10, 12, false, false}, {"stale active", &active, 9, 11, false, false},
		{"old active", &active, 10, 9, false, false}, {"revoke", &active, 10, 11, true, true},
		{"revoke lost intermediate ack", &active, 10, 14, true, true},
		{"revoke later local expected", &active, 13, 14, true, true},
		{"revoke nonadvancing", &active, 9, 10, true, false}, {"revoke future expected", &active, 14, 14, true, false},
		{"revoke duplicate original expected", &revoked, 10, 11, true, true},
		{"revoke unchanged", &revoked, 11, 11, true, true}, {"resurrection", &revoked, 11, 12, false, false},
		{"revoke samegen active", &revoked, 11, 11, false, false}, {"final active denied", &active, 10, math.MaxUint64, false, false},
		{"zero denied", &active, 10, 0, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := initial
			target.CredentialGeneration, target.Revoked = tc.generation, tc.revoked
			err := domain.ValidateAttachedResourceGenerationTransitionV1(tc.current, tc.expected, target)
			if (err == nil) != tc.want {
				t.Fatalf("current=%+v expected=%d target=%+v accepted=%t want=%t err=%v", tc.current, tc.expected, target, err == nil, tc.want, err)
			}
		})
	}
	for _, mutate := range []func(*domain.AttachedResourceGenerationHeadV1){
		func(h *domain.AttachedResourceGenerationHeadV1) { h.OwnerUserID = "foreign" },
		func(h *domain.AttachedResourceGenerationHeadV1) { h.WorkerID = "foreign" },
		func(h *domain.AttachedResourceGenerationHeadV1) { h.EnrollmentGeneration++ },
		func(h *domain.AttachedResourceGenerationHeadV1) { h.BaselineRevision++ },
	} {
		target := revoked
		mutate(&target)
		if err := domain.ValidateAttachedResourceGenerationTransitionV1(&active, 10, target); err == nil {
			t.Fatalf("foreign denial target accepted: %+v", target)
		}
	}
	last := initial
	last.CredentialGeneration = math.MaxUint64 - 1
	target := last
	target.CredentialGeneration = math.MaxUint64
	if domain.ValidateAttachedResourceGenerationTransitionV1(&last, last.CredentialGeneration, target) == nil {
		t.Fatal("reserved generation used for active successor")
	}
	target.Revoked = true
	if err := domain.ValidateAttachedResourceGenerationTransitionV1(&last, last.CredentialGeneration, target); err != nil {
		t.Fatalf("final revoke rejected: %v", err)
	}
	if domain.ValidateAttachedResourceGenerationTransitionV1(&target, math.MaxUint64, initial) == nil {
		t.Fatal("final generation wrapped to active one")
	}
	denial := initial
	denial.Revoked, denial.CredentialGeneration = true, 9
	if err := domain.ValidateAttachedResourceGenerationTransitionV1(nil, 0, denial); err != nil {
		t.Fatal(err)
	}
	if domain.ValidateAttachedResourceGenerationTransitionV1(&denial, 0, initial) == nil {
		t.Fatal("offline initial revoke did not fence delayed active initialization")
	}
}
