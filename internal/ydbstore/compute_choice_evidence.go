package ydbstore

import (
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// This is an immutable structural provider-evidence projection, NOT an
// approved serving manifest, production profile or execution authority. The
// six canonical evidence contracts preserve unknown facts. They do not prove
// modality, monetary applicability/budget, terms conditions, residency/egress,
// conformance or whole-context provenance. Those remain required before any
// positive public eligibility projection. No route consumes these facts.
type computeChoiceProviderEvidence struct {
	template domain.HarnessBindingV2
	bundle   domain.ProviderEvidenceBundleV1
	material []byte
}

func (*computeChoiceProviderEvidence) MarshalJSON() ([]byte, error) {
	return nil, computechoice.ErrPrivateRegistrationNotSerializable
}

func newComputeChoiceProviderEvidence(template domain.HarnessBindingV2, bundle domain.ProviderEvidenceBundleV1) (*computeChoiceProviderEvidence, error) {
	if template.RunID != "" || template.AttemptID != "" || template.ExecutionPlacementDigest != "" || template.InputDataClass != domain.ProviderDataPrivateV1 ||
		template.Backend.ProviderContractKind != domain.ProviderContractInvocationV1 || template.Resource.Kind != domain.ProviderResourceSubscriptionV1 {
		return nil, errComputeChoiceSource
	}
	// Like the existing attached binder's constructor, validate only shape with
	// constructor-local scope. These placeholders are never stored as a Run,
	// dispatched, returned or promoted to admitted execution authority.
	validation := template.Clone()
	validation.RunID, validation.AttemptID = "choice-shape-run", "choice-shape-attempt"
	validation.ExecutionPlacementDigest = "0000000000000000000000000000000000000000000000000000000000000000"
	if bundle.ValidateForBinding(validation) != nil {
		return nil, errComputeChoiceSource
	}
	material, err := json.Marshal(struct {
		Template domain.HarnessBindingV2         `json:"template"`
		Bundle   domain.ProviderEvidenceBundleV1 `json:"bundle"`
	}{template, bundle})
	if err != nil || len(material) > computechoice.MaxOrdinaryRecordBytes {
		return nil, errComputeChoiceSource
	}
	return &computeChoiceProviderEvidence{template: template.Clone(), bundle: bundle.Clone(), material: material}, nil
}

type computeChoiceEvidenceWindow uint8

const (
	computeChoiceEvidenceMissing computeChoiceEvidenceWindow = iota
	computeChoiceEvidenceCurrent
	computeChoiceEvidenceExpired
	computeChoiceEvidenceFuture
)

// Independently named observations, deliberately without an eligible/ready or
// free/budget field. Supported provider price is NOT attached monetary budget.
type computeChoiceProviderSnapshot struct {
	window           computeChoiceEvidenceWindow
	observedAt       time.Time
	validUntil       time.Time
	routeObserved    bool
	privatePolicyGo  bool
	priceState       domain.ProviderEvidenceStateV1
	contextTokens    domain.ProviderEvidenceQuantityV1
	toolCalling      domain.ProviderEvidenceStateV1
	structuredOutput domain.ProviderEvidenceStateV1
}

func (computeChoiceProviderSnapshot) MarshalJSON() ([]byte, error) {
	return nil, computechoice.ErrPrivateRegistrationNotSerializable
}

func observeComputeChoiceProviderEvidence(query *computeChoiceQuery, auth computeChoiceAuth, pin computeChoiceAttachedPin, evidence *computeChoiceProviderEvidence) (computeChoiceProviderSnapshot, error) {
	if evidence == nil {
		return computeChoiceProviderSnapshot{}, nil
	}
	if query == nil || query.budget == nil || auth.at.IsZero() || auth.scope.Validate() != nil || evidence.template.TenantID != auth.scope.TenantID ||
		evidence.template.OwnerUserID != auth.scope.UserID || evidence.template.Resource.OwnerUserID != auth.scope.UserID || evidence.template.Resource.ResourceID != string(pin.subscriptionID) ||
		pin.tenantID != auth.scope.TenantID || pin.ownerUserID != auth.scope.UserID {
		return computeChoiceProviderSnapshot{}, errComputeChoiceSource
	}
	// Immutable trusted input still costs materialized work in EVERY attempt,
	// not only its constructor. It cannot bypass the shared retry byte budget.
	if err := query.budget.RecordMaterialized(computechoice.SourceOrdinary, int64(len(evidence.material))); err != nil {
		return computeChoiceProviderSnapshot{}, err
	}
	bundle := evidence.bundle
	result := computeChoiceProviderSnapshot{window: computeChoiceEvidenceCurrent,
		observedAt: bundle.Catalog.ObservedAt, validUntil: bundle.Catalog.ExpiresAt,
		priceState: bundle.Price.State, contextTokens: bundle.Capability.MaxContextTokens,
		toolCalling: bundle.Capability.ToolCalling, structuredOutput: bundle.Capability.StructuredOutput}
	for _, window := range [][2]time.Time{
		{bundle.Route.ObservedAt, bundle.Route.ExpiresAt}, {bundle.Capability.ObservedAt, bundle.Capability.ExpiresAt},
		{bundle.Privacy.ObservedAt, bundle.Privacy.ExpiresAt}, {bundle.Price.ObservedAt, bundle.Price.ExpiresAt}, {bundle.Policy.ObservedAt, bundle.Policy.ExpiresAt},
	} {
		if window[0].After(result.observedAt) {
			result.observedAt = window[0]
		}
		if window[1].Before(result.validUntil) {
			result.validUntil = window[1]
		}
	}
	if result.observedAt.After(auth.at) {
		result.window = computeChoiceEvidenceFuture
	} else if !auth.at.Before(result.validUntil) {
		result.window = computeChoiceEvidenceExpired
	}
	// No implicit first route. Unknown route or multiple exact model routes
	// remain unobserved until a reviewed exact selected tuple is supplied.
	if bundle.Route.State == domain.ProviderEvidenceSupportedV1 {
		matches := 0
		for _, route := range bundle.Route.Routes {
			if route.ModelVendorID == evidence.template.ModelVendorID && route.ModelID == evidence.template.ModelID {
				matches++
			}
		}
		result.routeObserved = matches == 1
	}
	// Conditional terms are not unconditional go. This records ONLY the two
	// explicit PRIVATE predicates, never satisfaction of unstated conditions.
	result.privatePolicyGo = result.window == computeChoiceEvidenceCurrent && bundle.Policy.Verdict == domain.ProviderPolicyGoV1 &&
		computeChoiceAllowsPrivate(bundle.Policy.AllowedDataClasses) && computeChoiceAllowsPrivate(bundle.Privacy.AllowedDataClasses)
	return result, nil
}

func computeChoiceAllowsPrivate(classes []domain.ProviderDataClassV1) bool {
	for _, class := range classes {
		if class == domain.ProviderDataPrivateV1 {
			return true
		}
	}
	return false
}
