package codexexec

import (
	"context"
	"slices"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	NativeEnvelopePolicyIDV2       = "OAI-SUB-NATIVE-ENVELOPE-V2"
	NativeEnvelopePolicyRevisionV2 = uint64(1)
	// This closed semantics identity is part of the static backend descriptor.
	// Changing these rules requires a new policy/profile revision, not a mutable
	// local flag or reinterpretation of an already admitted evidence digest.
	nativePolicySemanticsV2 = "native-envelope-v2/r1;owner-local-openai-chatgpt-file;single-consumed-invocation;no-transport-or-native-replay;tools-denied;original-text-json-png-jpeg;context-fit/native-transformed-bytes/native-allocations/internal-sampling-count/actual-route/token-usage/price-cost=unknown;route-unknown-fallback-deny;price-unknown;full-evidence-scope-digests-min-expiry;unchanged-privacy-and-data-class;original-session-current;limits=min-profile-job-lease-caller-resource;no-cloud-custody"
	nativeEnvironmentABIV2  = "supervisor-replace-v1:HOME,TMPDIR,XDG_CONFIG_HOME,XDG_CACHE_HOME,XDG_DATA_HOME=fresh-owned-attempt;PATH=;LANG=C.UTF-8;LC_ALL=C.UTF-8;NO_COLOR=1;CODEX_HOME=sole-outer-materialization;native-bootstrap-v3:deadline=min-original,HTTP_PROXY/HTTPS_PROXY/http_proxy/https_proxy=fresh-owned-loopback-relay;no-ambient-env;exact-actual-bootstrap-env-inspected;native-relay-readiness-before-exec"
)

// NativeCredentialBindingReaderV2 is the existing canonical binding store's
// read-only metadata method. It is not an issuer and never reads secret bytes.
type NativeCredentialBindingReaderV2 interface {
	LoadCredentialBinding(context.Context, domain.TenantID, domain.SubscriptionConnectionID) (domain.CredentialBinding, bool, error)
}

// NativePreparedConfigV2 supplies the full existing immutable evidence bundle
// beside (not inside) the static profile identity: its Scope.Backend already
// contains that descriptor. The original server-sealed job and Session still
// authorize the exact binding. No shipped configuration selects this profile.
type NativePreparedConfigV2 struct {
	Profile            NativeProfileV2
	Evidence           domain.ProviderEvidenceBundleV1
	CredentialBindings NativeCredentialBindingReaderV2
	// Explicit selection of the bounded inline output composition. This does
	// not authorize an export or enable a profile; negotiated transport and the
	// original private completed owner are still required. False keeps V1's
	// denial of artifact-bearing success unchanged.
	InlineOutputV2 bool
}

func (c NativePreparedConfigV2) Clone() (NativePreparedConfigV2, error) {
	if c.CredentialBindings == nil || validateNativeBundleShapeV2(c.Evidence) != nil {
		return NativePreparedConfigV2{}, ErrContract
	}
	profile, err := freezeNativeProfileV2(c.Profile)
	if err != nil {
		return NativePreparedConfigV2{}, ErrContract
	}
	c.Profile = profile.profile
	c.Evidence = c.Evidence.Clone()
	return c, nil
}

func validateNativeBundleShapeV2(bundle domain.ProviderEvidenceBundleV1) error {
	for _, validate := range []func() error{bundle.Catalog.Validate, bundle.Route.Validate, bundle.Capability.Validate,
		bundle.Privacy.Validate, bundle.Price.Validate, bundle.Policy.Validate} {
		if validate() != nil {
			return ErrContract
		}
	}
	return nil
}

// Bundle structural consistency is necessary but insufficient. This branch
// accepts only the explicitly reviewed native-envelope semantics, not any
// conditional policy, a known-route V1 policy, or a caller matching digest.
func validateNativeEnvelopeEvidenceV2(profile *nativeFrozenProfileV2, bundle domain.ProviderEvidenceBundleV1, binding domain.HarnessBindingV2, now time.Time) error {
	if profile == nil || now.IsZero() || binding.Backend != profile.descriptor || binding.Profile != domain.HarnessExecutionSingleTurnV2 ||
		binding.ModelVendorID != ModelVendorIDV1 || binding.ModelID != profile.profile.Backend.Model ||
		binding.Resource.Kind != domain.ProviderResourceSubscriptionV1 || binding.Resource.CredentialMode != domain.ProviderCredentialInvocationV1 ||
		bundle.ValidateForBinding(binding) != nil || binding.EvidenceExpiresAt == nil || !now.Before(*binding.EvidenceExpiresAt) {
		return ErrContract
	}
	policy := bundle.Policy
	if policy.PolicyID != NativeEnvelopePolicyIDV2 || policy.Revision != NativeEnvelopePolicyRevisionV2 ||
		policy.DecisionOwner != "sessionless-policy" || policy.EvidenceSource != "reviewed-native-envelope-v2" || policy.Verdict != domain.ProviderPolicyConditionalV1 ||
		bundle.Route.State != domain.ProviderEvidenceUnknownV1 || len(bundle.Route.Routes) != 0 || bundle.Route.FallbackPolicy != domain.ProviderFallbackDenyV1 ||
		bundle.Price.State != domain.ProviderEvidenceUnknownV1 || bundle.Capability.ToolCalling != domain.ProviderEvidenceUnsupportedV1 ||
		!slices.Contains(bundle.Privacy.AllowedDataClasses, binding.InputDataClass) {
		return ErrContract
	}
	// A declared native window is not context-fit evidence. If the admitted
	// metadata does give a model maximum, do not silently exceed or erase it.
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

func nativeCredentialCurrentV2(ctx context.Context, reader NativeCredentialBindingReaderV2, identity AuthorityV1, now time.Time) error {
	binding, err := readNativeCredentialMetadataV2(ctx, reader, identity, now)
	if err != nil {
		return err
	}
	// Issue/launch/CONNECT always require the original sealed generation.
	// Receipt-only finalization must not turn this into a lower bound.
	if binding.Generation != identity.ProviderResource.CredentialGeneration {
		return ErrAuthorityUnavailable
	}
	return nil
}

// readNativeCredentialMetadataV2 reads exact active scope, not permission to
// execute or export. Each caller must separately compare its own generation:
// sealed N for execution, the original successful writeback for receipt only.
func readNativeCredentialMetadataV2(ctx context.Context, reader NativeCredentialBindingReaderV2, identity AuthorityV1, now time.Time) (domain.CredentialBinding, error) {
	if ctx == nil || ctx.Err() != nil || reader == nil || identity.Validate() != nil || now.IsZero() {
		return domain.CredentialBinding{}, ErrContract
	}
	resource := identity.ProviderResource
	binding, found, err := reader.LoadCredentialBinding(ctx, identity.TenantID, domain.SubscriptionConnectionID(resource.ResourceID))
	if ctx.Err() != nil || err != nil || !found || binding.Validate() != nil || binding.Revoked || binding.Entitlement != domain.EntitlementActive ||
		binding.TenantID != identity.TenantID || binding.OwnerUserID != identity.OwnerUserID || string(binding.SubscriptionConnectionID) != resource.ResourceID ||
		binding.Provider != "openai" || binding.AuthMode != "chatgpt" || binding.UpdatedAt.After(now) {
		return domain.CredentialBinding{}, ErrAuthorityUnavailable
	}
	return binding, nil
}
