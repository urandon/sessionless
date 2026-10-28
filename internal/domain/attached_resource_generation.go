package domain

import (
	"bytes"
	"encoding/json"
	"math"
	"slices"
	"strconv"

	"gitcode.com/urandon/sessionless/internal/strictjson"
)

const (
	AttachedResourceGenerationVersionV1          uint32 = 1
	AttachedResourceGenerationMetadataMaxBytesV1        = 4 << 10
	AttachedResourceGenerationBaselineMaxBytesV1        = 64 << 10
)

// AttachedResourceGenerationBaselineV1 is complete, operator-installed DATA.
// Its explicit delegation permits only generation/digest projection, not
// entitlement, activation, account continuity or renewed evidence validity.
// Binding is a scope-free template; original evidence bodies remain immutable.
type AttachedResourceGenerationBaselineV1 struct {
	Version                  uint32                         `json:"version"`
	TenantID                 TenantID                       `json:"tenant_id"`
	OwnerUserID              UserID                         `json:"owner_user_id"`
	SubscriptionConnectionID SubscriptionConnectionID       `json:"subscription_connection_id"`
	WorkerID                 AttachedWorkerID               `json:"worker_id"`
	EnrollmentGeneration     uint64                         `json:"enrollment_generation"`
	BaselineRevision         uint64                         `json:"baseline_revision"`
	CapabilityDigest         AttachedWorkerCapabilityDigest `json:"capability_digest"`
	GenerationOnlyDelegation bool                           `json:"generation_only_delegation"`
	Binding                  HarnessBindingV2               `json:"binding"`
	Evidence                 ProviderEvidenceBundleV1       `json:"evidence"`
}

func (baseline AttachedResourceGenerationBaselineV1) Clone() AttachedResourceGenerationBaselineV1 {
	baseline.Binding = baseline.Binding.Clone()
	// Preserve nil versus explicitly empty evidence collections too: they
	// have different existing JSON digests, although both may mean unknown.
	baseline.Evidence.Catalog.Models = slices.Clone(baseline.Evidence.Catalog.Models)
	baseline.Evidence.Route.Routes = slices.Clone(baseline.Evidence.Route.Routes)
	baseline.Evidence.Privacy.AllowedDataClasses = slices.Clone(baseline.Evidence.Privacy.AllowedDataClasses)
	baseline.Evidence.Policy.AllowedDataClasses = slices.Clone(baseline.Evidence.Policy.AllowedDataClasses)
	return baseline
}

func (baseline AttachedResourceGenerationBaselineV1) Validate() error {
	if baseline.Version != AttachedResourceGenerationVersionV1 || !baseline.GenerationOnlyDelegation ||
		baseline.EnrollmentGeneration == 0 || baseline.BaselineRevision == 0 {
		return attachedGenerationError("baseline", "requires an explicit version, enrollment, revision and generation-only delegation")
	}
	for _, validate := range []func() error{baseline.TenantID.Validate, baseline.OwnerUserID.Validate,
		baseline.SubscriptionConnectionID.Validate, baseline.WorkerID.Validate, baseline.CapabilityDigest.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	binding := baseline.Binding
	if binding.TenantID != baseline.TenantID || binding.OwnerUserID != baseline.OwnerUserID ||
		binding.Resource.OwnerUserID != baseline.OwnerUserID || binding.Resource.ResourceID != string(baseline.SubscriptionConnectionID) ||
		binding.Resource.Revision != baseline.BaselineRevision || binding.Resource.Kind != ProviderResourceSubscriptionV1 ||
		binding.Resource.CredentialMode != ProviderCredentialInvocationV1 || binding.Resource.CredentialGeneration == math.MaxUint64 ||
		binding.Profile != HarnessExecutionSingleTurnV2 || binding.RunID != "" || binding.AttemptID != "" || binding.ExecutionPlacementDigest != "" {
		return attachedGenerationError("baseline.binding", "requires an exact scope-free subscription binding")
	}
	// These constructor-only coordinates satisfy structural validation. They
	// are never returned, persisted, admitted or used to authorize execution.
	binding.RunID, binding.AttemptID = "generation-baseline-validation-run", "generation-baseline-validation-attempt"
	placement := ExecutionPlacementV2{Version: ExecutionPlacementVersionV2, Kind: ExecutionPlacementAttachedWorker,
		FallbackPolicy: ExecutionFallbackDenied, OwnerUserID: baseline.OwnerUserID, WorkerID: baseline.WorkerID,
		CapabilityDigest: baseline.CapabilityDigest, PolicyDigest: AttachedWorkerPolicyDigest(binding.EffectivePolicyDigest)}
	digest, err := ExecutionPlacementDigest(placement)
	if err != nil {
		return err
	}
	binding.ExecutionPlacementDigest = string(digest)
	if err := binding.ValidateForScope(baseline.TenantID, baseline.OwnerUserID, binding.RunID, binding.AttemptID, placement); err != nil {
		return err
	}
	if err := baseline.Evidence.ValidateForBinding(binding); err != nil {
		return err
	}
	raw, err := json.Marshal(baseline)
	if err != nil || len(raw) > AttachedResourceGenerationBaselineMaxBytesV1 {
		return attachedGenerationError("baseline", "exceeds the bounded complete baseline encoding")
	}
	return nil
}

// ProjectAttachedResourceGenerationV1 returns independent scope-free values.
// Consumers still need the original installed baseline, current authenticated
// head and their own admission checks. Projection has no clock or effects.
func ProjectAttachedResourceGenerationV1(baseline AttachedResourceGenerationBaselineV1, generation uint64) (HarnessBindingV2, ProviderEvidenceBundleV1, error) {
	if err := baseline.Validate(); err != nil {
		return HarnessBindingV2{}, ProviderEvidenceBundleV1{}, err
	}
	if generation == 0 || generation == math.MaxUint64 {
		return HarnessBindingV2{}, ProviderEvidenceBundleV1{}, attachedGenerationError("projection", "requires a positive active generation below the reserved final generation")
	}
	copy := baseline.Clone()
	binding, bundle := copy.Binding, copy.Evidence
	binding.Resource.CredentialGeneration = generation
	for _, scope := range []*ProviderEvidenceScopeV1{&bundle.Catalog.Scope, &bundle.Route.Scope, &bundle.Capability.Scope, &bundle.Privacy.Scope, &bundle.Price.Scope, &bundle.Policy.Scope} {
		scope.Resource.CredentialGeneration = generation
	}
	for _, item := range []struct {
		digest func() (ProviderEvidenceDigestV1, error)
		fields []*string
	}{
		{bundle.Catalog.Digest, []*string{&binding.ProviderCatalogDigest}},
		{bundle.Route.Digest, []*string{&binding.ProviderRouteDigest, &bundle.Policy.RoutePolicyDigest}},
		{bundle.Capability.Digest, []*string{&binding.CapabilityEvidenceDigest, &bundle.Policy.CapabilityEvidenceDigest}},
		{bundle.Privacy.Digest, []*string{&binding.PrivacyPolicyDigest, &bundle.Policy.PrivacyEvidenceDigest}},
		{bundle.Price.Digest, []*string{&bundle.Policy.PriceObservationDigest}},
		{func() (ProviderEvidenceDigestV1, error) { return bundle.Policy.Digest() }, []*string{&binding.EffectivePolicyDigest}},
	} {
		digest, err := item.digest()
		if err != nil {
			return HarnessBindingV2{}, ProviderEvidenceBundleV1{}, err
		}
		for _, field := range item.fields {
			*field = string(digest)
		}
	}
	// Validate the complete projected closure without modifying its real
	// scope-free representation or renewing the original validity window.
	projected := baseline
	projected.Binding, projected.Evidence = binding, bundle
	if err := projected.Validate(); err != nil {
		return HarnessBindingV2{}, ProviderEvidenceBundleV1{}, err
	}
	return binding, bundle, nil
}

// AttachedResourceGenerationHeadV1 contains exactly the seven persisted
// metadata fields. Tenant/resource scope comes from the original row keys.
// Only these three head counters use quoted canonical decimals; the existing
// baseline, authorization and expected-generation uint64 contracts are separate.
type AttachedResourceGenerationHeadV1 struct {
	Version              uint32           `json:"version"`
	OwnerUserID          UserID           `json:"owner_user_id"`
	WorkerID             AttachedWorkerID `json:"worker_id"`
	EnrollmentGeneration uint64           `json:"enrollment_generation,string"`
	BaselineRevision     uint64           `json:"baseline_revision,string"`
	CredentialGeneration uint64           `json:"credential_generation,string"`
	Revoked              bool             `json:"revoked"`
}

// This private wire type has no JSON methods, avoiding recursive dispatch while
// retaining the one head field/encoding shape in every nested JSON envelope.
type attachedResourceGenerationHeadJSONV1 AttachedResourceGenerationHeadV1

func (head AttachedResourceGenerationHeadV1) MarshalJSON() ([]byte, error) {
	// Internal denied/missing result carriers have a zero head and still need
	// bounded JSON serialization. That is not a valid or acknowledged head:
	// EncodeHead and every direct/nested decoder below require full validation.
	return json.Marshal(attachedResourceGenerationHeadJSONV1(head))
}

func (head *AttachedResourceGenerationHeadV1) UnmarshalJSON(raw []byte) error {
	if head == nil {
		return attachedGenerationError("head.codec", "requires a non-nil destination")
	}
	*head = AttachedResourceGenerationHeadV1{}
	decoded, err := DecodeAttachedResourceGenerationHeadV1(raw)
	if err != nil {
		return err
	}
	*head = decoded
	return nil
}

func (head AttachedResourceGenerationHeadV1) Validate() error {
	if head.Version != AttachedResourceGenerationVersionV1 || head.OwnerUserID.Validate() != nil || head.WorkerID.Validate() != nil ||
		head.EnrollmentGeneration == 0 || head.BaselineRevision == 0 || head.CredentialGeneration == 0 ||
		!head.Revoked && head.CredentialGeneration == math.MaxUint64 {
		return attachedGenerationError("head", "requires exact version, identity and positive bounded generations")
	}
	return nil
}

func EncodeAttachedResourceGenerationHeadV1(head AttachedResourceGenerationHeadV1) ([]byte, error) {
	if err := head.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(head)
	if err != nil || len(raw) > AttachedResourceGenerationMetadataMaxBytesV1 {
		return nil, attachedGenerationError("head", "cannot encode bounded metadata")
	}
	return raw, nil
}

func DecodeAttachedResourceGenerationHeadV1(raw []byte) (AttachedResourceGenerationHeadV1, error) {
	// JsonDocument storage may normalize whitespace or member order. Validate
	// exact schema, not one incidental JSON spelling. No aliases/defaults.
	if strictjson.ValidateDocument(raw, AttachedResourceGenerationMetadataMaxBytesV1, 2, 7) != nil {
		return AttachedResourceGenerationHeadV1{}, attachedGenerationError("head.codec", "requires strict bounded JSON")
	}
	var members map[string]json.RawMessage
	if json.Unmarshal(raw, &members) != nil || len(members) != 7 {
		return AttachedResourceGenerationHeadV1{}, attachedGenerationError("head.codec", "requires all seven exact fields")
	}
	for _, name := range []string{"version", "owner_user_id", "worker_id", "enrollment_generation", "baseline_revision", "credential_generation", "revoked"} {
		value, found := members[name]
		if !found || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return AttachedResourceGenerationHeadV1{}, attachedGenerationError("head.codec", "requires non-null exact fields")
		}
	}
	for _, name := range []string{"enrollment_generation", "baseline_revision", "credential_generation"} {
		if _, err := parseAttachedResourceGenerationHeadCounterV1(members[name]); err != nil {
			return AttachedResourceGenerationHeadV1{}, err
		}
	}
	var wire attachedResourceGenerationHeadJSONV1
	if json.Unmarshal(raw, &wire) != nil || AttachedResourceGenerationHeadV1(wire).Validate() != nil {
		return AttachedResourceGenerationHeadV1{}, attachedGenerationError("head.codec", "contains invalid metadata")
	}
	return AttachedResourceGenerationHeadV1(wire), nil
}

func parseAttachedResourceGenerationHeadCounterV1(raw []byte) (uint64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) < 3 || len(raw) > 22 || raw[0] != '"' || raw[len(raw)-1] != '"' || raw[1] < '1' || raw[1] > '9' {
		return 0, attachedGenerationError("head.codec", "requires quoted canonical positive uint64 counters")
	}
	for _, character := range raw[1 : len(raw)-1] {
		if character < '0' || character > '9' {
			return 0, attachedGenerationError("head.codec", "requires quoted canonical positive uint64 counters")
		}
	}
	counter, err := strconv.ParseUint(string(raw[1:len(raw)-1]), 10, 64)
	if err != nil {
		return 0, attachedGenerationError("head.codec", "requires quoted canonical positive uint64 counters")
	}
	return counter, nil
}

// ValidateAttachedResourceGenerationTransitionV1 validates DATA CAS semantics
// only. The original transaction must separately authorize the baseline,
// membership, ownership, current connection/capability and relevant expiry.
// An exact target already present is idempotent, not another mutation.
func ValidateAttachedResourceGenerationTransitionV1(current *AttachedResourceGenerationHeadV1, expected uint64, target AttachedResourceGenerationHeadV1) error {
	if target.Validate() != nil {
		return attachedGenerationError("transition", "target is invalid")
	}
	if current == nil {
		if expected != 0 || !target.Revoked && target.CredentialGeneration != 1 {
			return attachedGenerationError("transition", "initial publication requires expected zero and active generation one or explicit denial")
		}
		return nil
	}
	if current.Validate() != nil || current.OwnerUserID != target.OwnerUserID || current.WorkerID != target.WorkerID ||
		current.EnrollmentGeneration != target.EnrollmentGeneration || current.BaselineRevision != target.BaselineRevision {
		return attachedGenerationError("transition", "current and target must share the exact original scope")
	}
	if *current == target {
		if expected == current.CredentialGeneration || target.Revoked && expected < target.CredentialGeneration ||
			!target.Revoked && expected != math.MaxUint64 && expected+1 == target.CredentialGeneration {
			return nil
		}
		return attachedGenerationError("transition", "duplicate target has a conflicting expected generation")
	}
	if current.Revoked {
		return attachedGenerationError("transition", "revoked metadata cannot be resurrected or advanced")
	}
	if target.Revoked {
		if target.CredentialGeneration > current.CredentialGeneration && expected < target.CredentialGeneration {
			return nil
		}
		return attachedGenerationError("transition", "revoke must advance denial beyond the current generation")
	}
	if expected != current.CredentialGeneration || expected == math.MaxUint64 || target.CredentialGeneration != expected+1 {
		return attachedGenerationError("transition", "active publication requires the exact next generation")
	}
	return nil
}

func attachedGenerationError(field, reason string) error {
	return ValidationError{Field: "attached_resource_generation." + field, Reason: reason}
}
