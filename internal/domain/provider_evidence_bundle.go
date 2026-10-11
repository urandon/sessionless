package domain

import "time"

// ProviderEvidenceBundleV1 cross-validates the exact immutable provider evidence
// sealed by a binding. Structural consistency is not permission, conformance
// success, current freshness, or eligibility; those predicates remain separate.
type ProviderEvidenceBundleV1 struct {
	Catalog    ProviderCatalogObservationV1 `json:"catalog"`
	Route      ProviderRoutePolicyV1        `json:"route"`
	Capability ProviderCapabilityEvidenceV1 `json:"capability"`
	Privacy    ProviderPrivacyEvidenceV1    `json:"privacy"`
	Price      ProviderPriceObservationV1   `json:"price"`
	Policy     ProviderPolicyEvidenceV1     `json:"policy"`
}

func (bundle ProviderEvidenceBundleV1) Clone() ProviderEvidenceBundleV1 {
	bundle.Catalog = bundle.Catalog.Clone()
	bundle.Route = bundle.Route.Clone()
	bundle.Capability = bundle.Capability.Clone()
	bundle.Privacy = bundle.Privacy.Clone()
	bundle.Price = bundle.Price.Clone()
	bundle.Policy = bundle.Policy.Clone()
	return bundle
}

func (bundle ProviderEvidenceBundleV1) ValidateForBinding(binding HarnessBindingV2) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	if binding.Backend.ProviderContractKind == ProviderContractCredentiallessFixtureV1 {
		return ValidationError{Field: "provider_conformance.evidence_bundle", Reason: "must be absent for the credentialless fixture"}
	}
	for _, validate := range []func() error{
		bundle.Catalog.Validate, bundle.Route.Validate, bundle.Capability.Validate,
		bundle.Privacy.Validate, bundle.Price.Validate, bundle.Policy.Validate,
	} {
		if err := validate(); err != nil {
			return err
		}
	}
	expectedScope := ProviderEvidenceScopeV1{
		TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID,
		Resource: binding.Resource, Backend: binding.Backend,
	}
	for _, scope := range []ProviderEvidenceScopeV1{
		bundle.Catalog.Scope, bundle.Route.Scope, bundle.Capability.Scope,
		bundle.Privacy.Scope, bundle.Price.Scope, bundle.Policy.Scope,
	} {
		if scope != expectedScope {
			return ValidationError{Field: "provider_conformance.evidence_bundle.scope", Reason: "must match the sealed binding"}
		}
	}
	modelVendorID := binding.ModelVendorID
	if bundle.Capability.ModelVendorID != modelVendorID || bundle.Capability.ModelID != binding.ModelID ||
		bundle.Privacy.ModelID != binding.ModelID || bundle.Price.ModelID != binding.ModelID ||
		bundle.Privacy.ModelVendorID != modelVendorID || bundle.Price.ModelVendorID != modelVendorID {
		return ValidationError{Field: "provider_conformance.evidence_bundle.model_id", Reason: "must match the sealed binding"}
	}
	modelFound := false
	for _, model := range bundle.Catalog.Models {
		if model.ModelVendorID == modelVendorID && model.ModelID == binding.ModelID {
			modelFound = true
			break
		}
	}
	if !modelFound {
		return ValidationError{Field: "provider_conformance.evidence_bundle.catalog", Reason: "must contain the sealed model"}
	}
	if bundle.Policy.Verdict == ProviderPolicyGoV1 || bundle.Policy.Verdict == ProviderPolicyConditionalV1 {
		allowed := false
		for _, class := range bundle.Policy.AllowedDataClasses {
			if class == binding.InputDataClass {
				allowed = true
				break
			}
		}
		if !allowed {
			return ValidationError{Field: "provider_conformance.evidence_bundle.input_data_class", Reason: "is not allowed by the effective policy"}
		}
	}
	catalogDigest, _ := bundle.Catalog.Digest()
	routeDigest, _ := bundle.Route.Digest()
	capabilityDigest, _ := bundle.Capability.Digest()
	privacyDigest, _ := bundle.Privacy.Digest()
	priceDigest, _ := bundle.Price.Digest()
	policyDigest, _ := bundle.Policy.Digest()
	if string(catalogDigest) != binding.ProviderCatalogDigest || string(routeDigest) != binding.ProviderRouteDigest ||
		string(capabilityDigest) != binding.CapabilityEvidenceDigest || string(privacyDigest) != binding.PrivacyPolicyDigest ||
		string(policyDigest) != binding.EffectivePolicyDigest {
		return ValidationError{Field: "provider_conformance.evidence_bundle.digest", Reason: "must match the sealed binding"}
	}
	if bundle.Policy.CapabilityEvidenceDigest != string(capabilityDigest) || bundle.Policy.PrivacyEvidenceDigest != string(privacyDigest) ||
		bundle.Policy.PriceObservationDigest != string(priceDigest) || bundle.Policy.RoutePolicyDigest != string(routeDigest) {
		return ValidationError{Field: "provider_conformance.evidence_bundle.policy", Reason: "must reference the exact evidence bundle"}
	}
	if binding.EvidenceExpiresAt == nil {
		return ValidationError{Field: "provider_conformance.evidence_bundle.expiry", Reason: "provider fixture requires sealed evidence expiry"}
	}
	minimum := bundle.Catalog.ExpiresAt
	for _, expiry := range []time.Time{
		bundle.Route.ExpiresAt, bundle.Capability.ExpiresAt, bundle.Privacy.ExpiresAt,
		bundle.Price.ExpiresAt, bundle.Policy.ExpiresAt,
	} {
		if expiry.Before(minimum) {
			minimum = expiry
		}
	}
	if !binding.EvidenceExpiresAt.Equal(minimum) {
		return ValidationError{Field: "provider_conformance.evidence_bundle.expiry", Reason: "binding expiry must equal the minimum evidence expiry"}
	}
	return nil
}

func (bundle ProviderEvidenceBundleV1) ValidateExecutionEvidence(binding HarnessBindingV2, evidence ProviderExecutionEvidenceV1) error {
	if err := bundle.ValidateForBinding(binding); err != nil {
		return err
	}
	if err := evidence.ValidateForBinding(binding); err != nil {
		return err
	}
	if evidence.PolicyVerdict != bundle.Policy.Verdict {
		return ValidationError{Field: "provider_conformance.execution_evidence.policy_verdict", Reason: "must match the effective policy evidence"}
	}
	if evidence.RouteState == ProviderEvidenceUnknownV1 {
		if bundle.Route.State != ProviderEvidenceUnknownV1 {
			return ValidationError{Field: "provider_conformance.execution_evidence.route", Reason: "must not erase an admitted observed route"}
		}
		return nil
	}
	for _, route := range bundle.Route.Routes {
		if route.BackendKind == binding.Backend.BackendKind && route.ModelVendorID == evidence.ActualModelVendorID && route.ModelID == evidence.ActualModelID &&
			route.TransportKind == evidence.TransportKind && route.TransportProvider == evidence.TransportProvider && route.UpstreamProviderID == evidence.UpstreamProviderID &&
			route.EndpointID == evidence.EndpointID {
			return nil
		}
	}
	return ValidationError{Field: "provider_conformance.execution_evidence.route", Reason: "must match one exact admitted route"}
}
