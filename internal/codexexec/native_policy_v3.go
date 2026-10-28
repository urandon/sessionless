package codexexec

import (
	"reflect"
	"slices"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	NativeEnvelopePolicyIDV3       = "OAI-SUB-NATIVE-ENVELOPE-V3"
	NativeEnvelopePolicyRevisionV3 = uint64(1)
	// This closed successor commits the generation/dynamic/static role split;
	// it grants none of those runtime facts by itself. They remain obligations
	// of the future original-Session/accepted-job/ACK composition.
	nativePolicySemanticsV3 = "native-envelope-v3/r1;owner-local-openai-chatgpt-file;single-consumed-invocation;no-transport-or-native-replay;tools-denied;original-text-json-png-jpeg;context-fit/native-transformed-bytes/native-allocations/internal-sampling-count/actual-route/token-usage/price-cost=unknown;route-unknown-fallback-deny;price-unknown;full-evidence-scope-digests-min-expiry;unchanged-privacy-and-data-class;original-session-current;acknowledged-generation-exact-sealed-resource;trusted-baseline-generation-only-projection;per-attempt-full-evidence-no-renewal;dynamic-policy=sealed-effective-policy;static-installation-policy=descriptor-pinned;limits=min-profile-job-lease-caller-resource;no-cloud-custody"
)

// This is pure consistency/semantic validation, never a provider, ACK,
// accepted-attempt or execution grant. V2 evidence does not qualify as V3.
func validateNativeEnvelopeEvidenceV3(profile *nativeFrozenProfileV3, bundle domain.ProviderEvidenceBundleV1, binding domain.HarnessBindingV2, now time.Time) error {
	if profile == nil || now.IsZero() {
		return ErrContract
	}
	// Recompute installation identity independently from the dynamic bundle.
	// A substituted/rehashed launch or descriptor cannot qualify itself.
	expected, err := freezeNativeProfileV3(profile.profile)
	if err != nil || profile.descriptor != expected.descriptor || !reflect.DeepEqual(profile.launch, expected.launch) || !reflect.DeepEqual(profile.generated, expected.generated) {
		return ErrContract
	}
	if binding.Backend != profile.descriptor || binding.Profile != domain.HarnessExecutionSingleTurnV2 ||
		binding.ModelVendorID != ModelVendorIDV1 || binding.ModelID != profile.profile.Backend.Model ||
		binding.Resource.Kind != domain.ProviderResourceSubscriptionV1 || binding.Resource.CredentialMode != domain.ProviderCredentialInvocationV1 ||
		bundle.ValidateForBinding(binding) != nil || binding.EvidenceExpiresAt == nil || !now.Before(*binding.EvidenceExpiresAt) {
		return ErrContract
	}
	policy := bundle.Policy
	if policy.PolicyID != NativeEnvelopePolicyIDV3 || policy.Revision != NativeEnvelopePolicyRevisionV3 ||
		policy.DecisionOwner != "sessionless-policy" || policy.EvidenceSource != "reviewed-native-envelope-v3" || policy.Verdict != domain.ProviderPolicyConditionalV1 ||
		bundle.Route.State != domain.ProviderEvidenceUnknownV1 || len(bundle.Route.Routes) != 0 || bundle.Route.FallbackPolicy != domain.ProviderFallbackDenyV1 ||
		bundle.Price.State != domain.ProviderEvidenceUnknownV1 || bundle.Capability.ToolCalling != domain.ProviderEvidenceUnsupportedV1 ||
		!slices.Contains(bundle.Privacy.AllowedDataClasses, binding.InputDataClass) {
		return ErrContract
	}
	if bundle.Capability.MaxContextTokens.State == domain.ProviderEvidenceSupportedV1 &&
		profile.profile.DeclaredContextWindow > bundle.Capability.MaxContextTokens.Value {
		return ErrContract
	}
	for _, observed := range []time.Time{bundle.Catalog.ObservedAt, bundle.Route.ObservedAt, bundle.Capability.ObservedAt,
		bundle.Privacy.ObservedAt, bundle.Price.ObservedAt, bundle.Policy.ObservedAt} {
		if observed.After(now) {
			return ErrContract
		}
	}
	return nil
}
