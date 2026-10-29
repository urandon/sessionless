package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
)

// LocalCapacityAssociationV1 is a closed capacity identity, not entitlement,
// funding, current profile authority or permission to execute. Only the trusted
// admission path derives it from the original authenticated installation.
type LocalCapacityAssociationV1 struct {
	Version     uint32                 `json:"version"`
	Kind        ProviderResourceKindV1 `json:"kind"`
	TenantID    TenantID               `json:"tenant_id"`
	OwnerUserID UserID                 `json:"owner_id"`
	ResourceID  string                 `json:"resource_id"`
}

func (a LocalCapacityAssociationV1) Validate() error {
	if a.Version != 1 || a.Kind != ProviderResourceLocalEndpointV1 {
		return ValidationError{Field: "local_capacity_association", Reason: "requires version 1 and local_endpoint kind"}
	}
	if err := a.TenantID.Validate(); err != nil {
		return err
	}
	if err := a.OwnerUserID.Validate(); err != nil {
		return err
	}
	return ValidateOpaqueID("local_capacity_association.resource_id", a.ResourceID)
}

func (a LocalCapacityAssociationV1) ValidateForScope(tenant TenantID, owner UserID, resource string) error {
	if err := a.Validate(); err != nil {
		return err
	}
	if a.TenantID != tenant || a.OwnerUserID != owner || a.ResourceID != resource {
		return ValidationError{Field: "local_capacity_association.scope", Reason: "must match the original tenant, owner and resource"}
	}
	return nil
}

// ValidateForOwnedSource checks DATA correlation only. The serving adapter must
// still authenticate its original Source and join current rows in its own
// bounded transaction; a structurally valid projection grants no capacity.
func (a LocalCapacityAssociationV1) ValidateForOwnedSource(record string, placement ExecutionPlacementV2, binding HarnessBindingV2, substrate *SubstrateBindingV1, cost *AdmissionCostCeilingV2) error {
	deny := func() error {
		return ValidationError{Field: "local_capacity_association.source", Reason: "requires the exact configured local owned source"}
	}
	source, err := DecodeAttemptExecutionSourceV1([]byte(record))
	if err != nil {
		return deny()
	}
	if a.ValidateForScope(binding.TenantID, binding.OwnerUserID, binding.Resource.ResourceID) != nil ||
		binding.Profile != HarnessExecutionOwnedAgentV2 || placement.Kind != ExecutionPlacementManaged ||
		binding.Backend.BackendKind != HarnessBackendDirectOpenAICompatibleV1 || binding.Backend.ProviderContractKind != ProviderContractConfiguredEndpointV1 ||
		binding.Resource.Kind != ProviderResourceLocalEndpointV1 || substrate == nil || substrate.Kind != SubstrateLocalDevelopmentV1 || cost == nil ||
		binding.ValidateForScope(a.TenantID, a.OwnerUserID, binding.RunID, binding.AttemptID, placement) != nil ||
		ValidateOwnedExecutionSourceRecord(record, AttemptExecutionSourceScopeV1{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, SessionID: source.Scope.SessionID, RunID: binding.RunID, AttemptID: binding.AttemptID}, placement, binding, substrate, cost) != nil {
		return deny()
	}
	if source.Profile.Locator.TenantID != a.TenantID || source.Profile.Locator.OwnerUserID != a.OwnerUserID ||
		source.Profile.Locator.ResourceKind != a.Kind || source.Profile.Locator.ResourceID != a.ResourceID {
		return deny()
	}
	for _, spending := range source.Spending {
		if spending.SourceKind != AttemptSpendingOwnerLocalSourceV1 {
			return deny()
		}
	}
	return nil
}

func EncodeLocalCapacityAssociationV1(a LocalCapacityAssociationV1) ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	return encodeOwnedCurrent(a)
}

func DecodeLocalCapacityAssociationV1(raw []byte) (LocalCapacityAssociationV1, error) {
	type wire LocalCapacityAssociationV1
	var decoded wire
	if err := decodeOwnedCurrent(raw, &decoded); err != nil {
		return LocalCapacityAssociationV1{}, err
	}
	a := LocalCapacityAssociationV1(decoded)
	want, err := EncodeLocalCapacityAssociationV1(a)
	if err != nil || !bytes.Equal(raw, want) {
		return LocalCapacityAssociationV1{}, ValidationError{Field: "local_capacity_association.codec", Reason: "requires exact canonical producer bytes"}
	}
	return a, nil
}

// JSONDocument may reorder members or normalize whitespace in a Run/Quota/Job
// projection. Its nested value still has one closed structural schema. Exact
// byte order belongs only to the separately persisted String codec above.
func (a *LocalCapacityAssociationV1) UnmarshalJSON(raw []byte) error {
	if a == nil {
		return ValidationError{Field: "local_capacity_association", Reason: "requires a destination"}
	}
	if err := terminalJSONPreflight(raw, "local_capacity"); err != nil {
		return err
	}
	type wire LocalCapacityAssociationV1
	var decoded wire
	if err := decodeOwnedCurrent(raw, &decoded); err != nil {
		return err
	}
	value := LocalCapacityAssociationV1(decoded)
	if err := value.Validate(); err != nil {
		return err
	}
	*a = value
	return nil
}

func (a LocalCapacityAssociationV1) Digest() (string, error) {
	raw, err := EncodeLocalCapacityAssociationV1(a)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// CapacityKey addresses the existing physical slot/expiry columns. Its slash
// deliberately excludes it from the domain subscription identity grammar.
func (a LocalCapacityAssociationV1) CapacityKey() (string, error) {
	digest, err := a.Digest()
	if err != nil {
		return "", err
	}
	return "local-v1/" + digest, nil
}

func validateCapacityIdentity(tenant TenantID, subscription SubscriptionConnectionID, local *LocalCapacityAssociationV1) error {
	if local == nil {
		return subscription.Validate()
	}
	if subscription != "" {
		return ValidationError{Field: "capacity_identity", Reason: "local and subscription identities are mutually exclusive"}
	}
	if err := local.Validate(); err != nil {
		return err
	}
	return EnsureSameTenant(tenant, local.TenantID)
}
