//go:build darwin || linux

package codexexec

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func nativePolicyFixtureV3(t *testing.T) (*nativeFrozenProfileV3, domain.ProviderEvidenceBundleV1, domain.HarnessBindingV2, time.Time) {
	t.Helper()
	old, bundle, binding, now := nativePolicyFixtureV2(t)
	profile, err := freezeNativeProfileV3(NativeProfileV3(old.profile))
	if err != nil {
		t.Fatal(err)
	}
	binding.Backend = profile.descriptor
	for _, scope := range []*domain.ProviderEvidenceScopeV1{&bundle.Catalog.Scope, &bundle.Route.Scope, &bundle.Capability.Scope, &bundle.Privacy.Scope, &bundle.Price.Scope, &bundle.Policy.Scope} {
		scope.Backend = profile.descriptor
	}
	bundle.Policy.PolicyID, bundle.Policy.Revision, bundle.Policy.EvidenceSource = NativeEnvelopePolicyIDV3, NativeEnvelopePolicyRevisionV3, "reviewed-native-envelope-v3"
	nativePolicyRebindV2(t, &bundle, &binding)
	if bundle.ValidateForBinding(binding) != nil {
		t.Fatal("V3 test metadata is not a complete internally consistent bundle")
	}
	return profile, bundle, binding, now
}

func TestNativePolicyV3ClosedSuccessorAndDistinctPolicyRoles(t *testing.T) {
	const semantics = "native-envelope-v3/r1;owner-local-openai-chatgpt-file;single-consumed-invocation;no-transport-or-native-replay;tools-denied;original-text-json-png-jpeg;context-fit/native-transformed-bytes/native-allocations/internal-sampling-count/actual-route/token-usage/price-cost=unknown;route-unknown-fallback-deny;price-unknown;full-evidence-scope-digests-min-expiry;unchanged-privacy-and-data-class;original-session-current;acknowledged-generation-exact-sealed-resource;trusted-baseline-generation-only-projection;per-attempt-full-evidence-no-renewal;dynamic-policy=sealed-effective-policy;static-installation-policy=descriptor-pinned;limits=min-profile-job-lease-caller-resource;no-cloud-custody"
	if NativeEnvelopePolicyIDV3 != "OAI-SUB-NATIVE-ENVELOPE-V3" || NativeEnvelopePolicyRevisionV3 != 1 || nativePolicySemanticsV3 != semantics {
		t.Fatal("V3 semantics changed without an explicit successor")
	}
	p, bundle, binding, now := nativePolicyFixtureV3(t)
	beforeBundle, beforeBinding := bundle.Clone(), binding.Clone()
	if err := validateNativeEnvelopeEvidenceV3(p, bundle, binding, now); err != nil {
		t.Fatalf("closed V3 metadata denied: %v", err)
	}
	static, err := p.profile.InstallationPolicyDigestV3()
	if err != nil || string(static) == binding.EffectivePolicyDigest {
		t.Fatalf("dynamic effective policy collapsed into static installation identity: %v", err)
	}
	if !reflect.DeepEqual(bundle, beforeBundle) || !reflect.DeepEqual(binding, beforeBinding) {
		t.Fatal("validator rewrote original dynamic evidence or sealed binding")
	}
	// New generation metadata changes dynamic evidence, not the installation.
	newBinding, next := binding.Clone(), bundle.Clone()
	newBinding.Resource.CredentialGeneration++
	for _, scope := range []*domain.ProviderEvidenceScopeV1{&next.Catalog.Scope, &next.Route.Scope, &next.Capability.Scope, &next.Privacy.Scope, &next.Price.Scope, &next.Policy.Scope} {
		scope.Resource = newBinding.Resource
	}
	nativePolicyRebindV2(t, &next, &newBinding)
	if next.ValidateForBinding(newBinding) != nil || validateNativeEnvelopeEvidenceV3(p, next, newBinding, now) != nil || newBinding.EffectivePolicyDigest == binding.EffectivePolicyDigest {
		t.Fatal("generation-bound dynamic policy did not change independently of static identity")
	}
	if after, err := p.profile.InstallationPolicyDigestV3(); err != nil || after != static {
		t.Fatalf("generation metadata changed static identity: %v", err)
	}
	wrong := binding.Clone()
	wrong.EffectivePolicyDigest = string(static)
	if validateNativeEnvelopeEvidenceV3(p, bundle, wrong, now) == nil {
		t.Fatal("static installation digest accepted in dynamic effective-policy role")
	}
}

func TestNativePolicyV3RejectsRehashedV2AndForeignSemantics(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*domain.ProviderEvidenceBundleV1)
	}{
		{name: "V2-policy", change: func(b *domain.ProviderEvidenceBundleV1) { b.Policy.PolicyID = NativeEnvelopePolicyIDV2 }},
		{name: "V2-source", change: func(b *domain.ProviderEvidenceBundleV1) { b.Policy.EvidenceSource = "reviewed-native-envelope-v2" }},
		{name: "revision", change: func(b *domain.ProviderEvidenceBundleV1) { b.Policy.Revision++ }},
		{name: "owner", change: func(b *domain.ProviderEvidenceBundleV1) { b.Policy.DecisionOwner = "caller-policy" }},
		{name: "source", change: func(b *domain.ProviderEvidenceBundleV1) { b.Policy.EvidenceSource = "unreviewed-native" }},
		{name: "go", change: func(b *domain.ProviderEvidenceBundleV1) { b.Policy.Verdict = domain.ProviderPolicyGoV1 }},
		{name: "unknown", change: func(b *domain.ProviderEvidenceBundleV1) { b.Policy.Verdict = domain.ProviderPolicyUnknownV1 }},
		{name: "known-price", change: func(b *domain.ProviderEvidenceBundleV1) {
			b.Price.State, b.Price.Currency = domain.ProviderEvidenceSupportedV1, "USD"
		}},
		{name: "tools", change: func(b *domain.ProviderEvidenceBundleV1) {
			b.Capability.ToolCalling = domain.ProviderEvidenceSupportedV1
		}},
		{name: "tools-unknown", change: func(b *domain.ProviderEvidenceBundleV1) { b.Capability.ToolCalling = domain.ProviderEvidenceUnknownV1 }},
		{name: "privacy", change: func(b *domain.ProviderEvidenceBundleV1) {
			b.Privacy.AllowedDataClasses = []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}
		}},
		{name: "known-route", change: func(b *domain.ProviderEvidenceBundleV1) {
			b.Route.State, b.Route.PolicyID, b.Route.Revision = domain.ProviderEvidenceSupportedV1, "exact-route", 1
			b.Route.Routes = []domain.ProviderRouteV1{{BackendKind: b.Route.Scope.Backend.BackendKind, ModelVendorID: ModelVendorIDV1, ModelID: b.Capability.ModelID, TransportKind: domain.ProviderTransportLocalCLIV1, TransportProvider: "openai", UpstreamProviderID: "openai", EndpointID: "chatgpt", BillingKind: domain.ProviderBillingSubscriptionV1, BillingAuthority: b.Route.Scope.Resource.ResourceID}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, bundle, binding, now := nativePolicyFixtureV3(t)
			test.change(&bundle)
			nativePolicyRebindV2(t, &bundle, &binding)
			if bundle.ValidateForBinding(binding) != nil {
				t.Fatal("negative must be structurally valid and rehashed before V3 semantic denial")
			}
			if validateNativeEnvelopeEvidenceV3(p, bundle, binding, now) == nil {
				t.Fatalf("rehashed %s silently qualified as V3", test.name)
			}
		})
	}
	p, bundle, binding, now := nativePolicyFixtureV3(t)
	old, err := freezeNativeProfileV2(p.profile.declarativeInputV2())
	if err != nil {
		t.Fatal(err)
	}
	binding.Backend = old.descriptor
	for _, scope := range []*domain.ProviderEvidenceScopeV1{&bundle.Catalog.Scope, &bundle.Route.Scope, &bundle.Capability.Scope, &bundle.Privacy.Scope, &bundle.Price.Scope, &bundle.Policy.Scope} {
		scope.Backend = old.descriptor
	}
	nativePolicyRebindV2(t, &bundle, &binding)
	if bundle.ValidateForBinding(binding) != nil || validateNativeEnvelopeEvidenceV3(p, bundle, binding, now) == nil || validateNativeEnvelopeEvidenceV2(old, bundle, binding, now) == nil {
		t.Fatal("V2 descriptor or V3 policy silently crossed profile qualification roles")
	}
}

func TestNativePolicyV3RejectsRehashedFrozenInstallationSubstitution(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*nativeFrozenProfileV3, domain.HarnessBindingV2)
	}{
		{name: "static-digest", change: func(p *nativeFrozenProfileV3, _ domain.HarnessBindingV2) { p.launch.StaticProfileDigest[0]++ }},
		{name: "socket", change: func(p *nativeFrozenProfileV3, _ domain.HarnessBindingV2) {
			p.launch.SocketDestination = "/conduit/foreign.sock"
		}},
		{name: "generated-catalog", change: func(p *nativeFrozenProfileV3, _ domain.HarnessBindingV2) { p.generated.catalog[0] ^= 1 }},
		{name: "dynamic-as-static-profile", change: func(p *nativeFrozenProfileV3, b domain.HarnessBindingV2) {
			raw, _ := hex.DecodeString(b.EffectivePolicyDigest) // fixture already validated the digest
			copy(p.launch.StaticProfileDigest[:], raw)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, bundle, binding, now := nativePolicyFixtureV3(t)
			test.change(p, binding)
			var err error
			p.launch.PolicyDigest, err = p.launch.InstallPolicyDigestV3()
			if err != nil {
				t.Fatalf("negative must retain a valid self-rehashed launch: %v", err)
			}
			sum := sha256.Sum256(append(append([]byte(NativeProfileVersionV3+"\x00"), p.launch.StaticProfileDigest[:]...), p.launch.PolicyDigest[:]...))
			p.descriptor.BackendProfileDigest = hex.EncodeToString(sum[:])
			binding.Backend = p.descriptor
			for _, scope := range []*domain.ProviderEvidenceScopeV1{&bundle.Catalog.Scope, &bundle.Route.Scope, &bundle.Capability.Scope, &bundle.Privacy.Scope, &bundle.Price.Scope, &bundle.Policy.Scope} {
				scope.Backend = p.descriptor
			}
			nativePolicyRebindV2(t, &bundle, &binding)
			if bundle.ValidateForBinding(binding) != nil || validateNativeEnvelopeEvidenceV3(p, bundle, binding, now) == nil {
				t.Fatalf("self-rehashed %s bypassed independent immutable installation derivation", test.name)
			}
		})
	}
	p, bundle, binding, now := nativePolicyFixtureV3(t)
	dynamic, err := hex.DecodeString(binding.EffectivePolicyDigest)
	if err != nil || len(dynamic) != sha256.Size {
		t.Fatalf("fixed dynamic-policy fixture digest invalid: %v", err)
	}
	copy(p.launch.PolicyDigest[:], dynamic)
	if validateNativeEnvelopeEvidenceV3(p, bundle, binding, now) == nil {
		t.Fatal("dynamic policy digest accepted as installation metadata")
	}
}

func TestNativePolicyV3MinimumExpiryNoFutureObservationsAndContextBounds(t *testing.T) {
	for i, name := range []string{"catalog", "route", "capability", "privacy", "price", "policy"} {
		t.Run(name, func(t *testing.T) {
			p, bundle, binding, now := nativePolicyFixtureV3(t)
			minimum := now.Add(time.Minute)
			*nativePolicyWindowsV2(&bundle)[i][1] = minimum
			nativePolicyRebindV2(t, &bundle, &binding)
			if bundle.ValidateForBinding(binding) != nil || validateNativeEnvelopeEvidenceV3(p, bundle, binding, now) != nil {
				t.Fatal("exact full-bundle minimum expiry denied")
			}
			if validateNativeEnvelopeEvidenceV3(p, bundle, binding, minimum) == nil {
				t.Fatal("exact original expiry accepted")
			}
			wrong := binding.Clone()
			expires := minimum.Add(time.Nanosecond)
			wrong.EvidenceExpiresAt = &expires
			if validateNativeEnvelopeEvidenceV3(p, bundle, wrong, now) == nil {
				t.Fatal("nonminimum evidence expiry accepted")
			}
			*nativePolicyWindowsV2(&bundle)[i][0] = now.Add(time.Second)
			nativePolicyRebindV2(t, &bundle, &binding)
			if bundle.ValidateForBinding(binding) != nil || validateNativeEnvelopeEvidenceV3(p, bundle, binding, now) == nil {
				t.Fatal("structurally valid future observation accepted")
			}
		})
	}
	for _, maximum := range []uint64{127999, 128000, 128001} {
		p, bundle, binding, now := nativePolicyFixtureV3(t)
		bundle.Capability.MaxContextTokens = domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceSupportedV1, Value: maximum}
		nativePolicyRebindV2(t, &bundle, &binding)
		if bundle.ValidateForBinding(binding) != nil || (validateNativeEnvelopeEvidenceV3(p, bundle, binding, now) == nil) != (maximum >= p.profile.DeclaredContextWindow) {
			t.Errorf("context declaration/model maximum mismatch: maximum=%d declared=%d", maximum, p.profile.DeclaredContextWindow)
		}
	}
	if validateNativeEnvelopeEvidenceV3(nil, domain.ProviderEvidenceBundleV1{}, domain.HarnessBindingV2{}, time.Time{}) == nil {
		t.Fatal("missing frozen profile accepted")
	}
}
