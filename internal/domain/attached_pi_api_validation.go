package domain

import "time"

// ValidateAttachedPiAPITermsV1 checks the complete P/E/T relationship before R
// exists. MaxContextTokens is the total model context window, not an estimate
// of this request's input. The charge uses P's separate maximum billed input
// and output bounds, with independent upward rounding of both components.
//
// This is a PURE consistency check, not an authority producer. In particular,
// a matching context-accounting hash does not prove the billed token bound or
// that the two token rates cover vision, cache, reasoning and every surcharge.
// The server's trusted installation/tariff source must establish those facts
// before invoking this check. Unknown or unrepresented charges must not reach
// it as an apparently qualified P/E. No owner permission is created here.
func ValidateAttachedPiAPITermsV1(p AttachedPiSDKNativeProfileV1, e ProviderEvidenceBundleV1, t AttachedPiAPIInvocationPolicyV1, at time.Time) (time.Time, error) {
	deny := func(reason string) (time.Time, error) {
		return time.Time{}, attachedPiAPIPolicyError("terms", reason)
	}
	if at.IsZero() || at.Location() != time.UTC {
		return deny("requires explicit authoritative UTC time")
	}
	descriptor, err := p.Descriptor()
	if err != nil {
		return time.Time{}, err
	}
	if err := t.Validate(); err != nil {
		return time.Time{}, err
	}
	if t.Scope.Backend != descriptor || t.ModelVendorID != p.Model.ModelVendorID || t.ModelID != p.Model.ModelID ||
		t.MaxContextTokens != p.Model.ModelContextTokens || t.MaxOutputTokens != p.Model.MaxOutputTokens {
		return deny("must match exact immutable native profile, model and token bounds")
	}
	if at.Before(t.NotBefore) || !at.Before(t.ExpiresAt) {
		return deny("terms are not current")
	}
	for _, validate := range []func() error{e.Catalog.Validate, e.Route.Validate, e.Capability.Validate, e.Privacy.Validate, e.Price.Validate, e.Policy.Validate} {
		if err := validate(); err != nil {
			return time.Time{}, err
		}
	}
	// This phase has no Run, final binding or M yet. Check E's actual scopes
	// directly instead of inventing a synthetic job/binding to validate E.
	expires := t.ExpiresAt
	for _, item := range []struct {
		scope ProviderEvidenceScopeV1
		start time.Time
		end   time.Time
	}{
		{e.Catalog.Scope, e.Catalog.ObservedAt, e.Catalog.ExpiresAt},
		{e.Route.Scope, e.Route.ObservedAt, e.Route.ExpiresAt},
		{e.Capability.Scope, e.Capability.ObservedAt, e.Capability.ExpiresAt},
		{e.Privacy.Scope, e.Privacy.ObservedAt, e.Privacy.ExpiresAt},
		{e.Price.Scope, e.Price.ObservedAt, e.Price.ExpiresAt},
		{e.Policy.Scope, e.Policy.ObservedAt, e.Policy.ExpiresAt},
	} {
		if item.scope != t.Scope || at.Before(item.start) || !at.Before(item.end) {
			return deny("evidence scope or authoritative time differs")
		}
		if item.end.Before(expires) {
			expires = item.end
		}
	}
	for _, model := range []struct{ vendor, id string }{
		{e.Capability.ModelVendorID, e.Capability.ModelID},
		{e.Privacy.ModelVendorID, e.Privacy.ModelID},
		{e.Price.ModelVendorID, e.Price.ModelID},
	} {
		if model.vendor != t.ModelVendorID || model.id != t.ModelID {
			return deny("evidence model differs")
		}
	}
	found := false
	for _, model := range e.Catalog.Models {
		if model.ModelVendorID == t.ModelVendorID && model.ModelID == t.ModelID {
			found = true
		}
	}
	routeFound := false
	for _, route := range e.Route.Routes {
		if route.ModelVendorID == t.ModelVendorID && route.ModelID == t.ModelID && route.TransportKind == ProviderTransportDirectAPIV1 {
			routeFound = true
		}
	}
	if !found || !routeFound || e.Route.State != ProviderEvidenceSupportedV1 || e.Price.State != ProviderEvidenceSupportedV1 ||
		e.Capability.MaxContextTokens.State != ProviderEvidenceSupportedV1 || e.Capability.MaxContextTokens.Value < p.Model.ModelContextTokens {
		return deny("requires known catalog, route, price and sufficient context capacity")
	}
	// There is no condition-resolution record in the closed V1 source format.
	// An unresolved conditional must not be promoted by a caller's boolean.
	if e.Policy.Verdict != ProviderPolicyGoV1 || !attachedPiAPIAllowsClass(e.Policy.AllowedDataClasses, t.InputDataClass) ||
		!attachedPiAPIAllowsClass(e.Privacy.AllowedDataClasses, t.InputDataClass) {
		return deny("requires an effective Go policy and allowed data class")
	}
	capabilityDigest, _ := e.Capability.Digest()
	privacyDigest, _ := e.Privacy.Digest()
	priceDigest, _ := e.Price.Digest()
	routeDigest, _ := e.Route.Digest()
	policyDigest, _ := e.Policy.Digest()
	if e.Policy.CapabilityEvidenceDigest != string(capabilityDigest) || e.Policy.PrivacyEvidenceDigest != string(privacyDigest) ||
		e.Policy.PriceObservationDigest != string(priceDigest) || e.Policy.RoutePolicyDigest != string(routeDigest) ||
		t.EffectivePolicyDigest != string(policyDigest) || t.PriceObservationDigest != string(priceDigest) || t.Currency != e.Price.Currency {
		return deny("must seal the exact complete policy/price evidence and currency")
	}
	charge, err := CalculateAttachedPiAPIMaximumChargeV1(p.Model.MaxBilledInputTokens, p.Model.MaxOutputTokens,
		e.Price.InputMicrounitsPerMTokens, e.Price.OutputMicrounitsPerMTokens)
	if err != nil {
		return time.Time{}, err
	}
	if charge > t.MaxChargeMicrounits {
		return deny("conservative maximum charge exceeds explicit cap")
	}
	return expires, nil
}

// ValidateAttachedPiAPIInvocationPolicyV1 joins P/E/T/R/M to the ORIGINAL
// binding. Its result is only the exclusive minimum expiry, never a send
// capability or proof of current source/issuer/membership/resource authority.
// The same transaction must separately verify R against the immutable trusted
// issuer record and current M head, and recheck principal and API generation.
func ValidateAttachedPiAPIInvocationPolicyV1(p AttachedPiSDKNativeProfileV1, e ProviderEvidenceBundleV1, t AttachedPiAPIInvocationPolicyV1, r AttachedPiAPIOwnerAuthorizationReceiptV1, m AttachedPiAPIInvocationPolicyMappingV1, binding HarnessBindingV2, at time.Time) (time.Time, error) {
	expires, err := ValidateAttachedPiAPITermsV1(p, e, t, at)
	if err != nil {
		return time.Time{}, err
	}
	if err := binding.ValidateAt(at); err != nil {
		return time.Time{}, err
	}
	if err := e.ValidateForBinding(binding); err != nil {
		return time.Time{}, err
	}
	if err := m.ValidateFor(t, r); err != nil {
		return time.Time{}, err
	}
	digest, err := m.Digest()
	if err != nil {
		return time.Time{}, err
	}
	if binding.Profile != HarnessExecutionSingleTurnV2 || binding.AgentLoopPolicy != nil || binding.AttachedPiAPIInvocationPolicyDigest != digest ||
		binding.TenantID != t.Scope.TenantID || binding.OwnerUserID != t.Scope.OwnerUserID || binding.Resource != t.Scope.Resource || binding.Backend != t.Scope.Backend ||
		binding.ModelVendorID != t.ModelVendorID || binding.ModelID != t.ModelID || binding.InputDataClass != t.InputDataClass ||
		at.Before(r.IssuedAt) || !at.Before(r.ExpiresAt) || r.IssuedAt.Before(t.NotBefore) {
		return time.Time{}, attachedPiAPIPolicyError("binding", "requires exact original binding/mapping and current owner decision")
	}
	if r.ExpiresAt.Before(expires) {
		expires = r.ExpiresAt
	}
	return expires, nil
}

func attachedPiAPIAllowsClass(classes []ProviderDataClassV1, expected ProviderDataClassV1) bool {
	for _, class := range classes {
		if class == expected {
			return true
		}
	}
	return false
}
