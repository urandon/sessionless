package domain

import (
	"bytes"
	"time"
)

// This identity comes from canonical admission, not a frontend/model selector.
// A valid reservation ID is not evidence that capacity is actually held.
type AttemptExecutionAdmissionScopeV1 struct {
	TenantID      TenantID           `json:"tenant_id"`
	SessionID     SessionID          `json:"session_id"`
	OwnerUserID   UserID             `json:"owner_user_id"`
	RunID         RunID              `json:"run_id"`
	AttemptID     AttemptID          `json:"attempt_id"`
	ReservationID QuotaReservationID `json:"reservation_id"`
}

func (v AttemptExecutionAdmissionScopeV1) Validate() error {
	for _, check := range []func() error{v.TenantID.Validate, v.SessionID.Validate, v.OwnerUserID.Validate, v.RunID.Validate, v.AttemptID.Validate, v.ReservationID.Validate} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

// AttemptExecutionAdmissionV1 is the original immutable attachment, not current
// permission or a debit ledger. The canonical admission writer must create it
// with the WorkerJob in the SAME existing transaction, and later readers must
// load it by tenant/Attempt without reconstructing missing history from the Job.
// Spending snapshots preserve the exact original source, payer, component,
// price/terms, amount and window. Only these four references may be used later.
// The COMPOSITE String record is capped at 32 KiB by its strict codec; individual
// source-record bounds do not establish that the attachment itself fits.
type AttemptExecutionAdmissionV1 struct {
	Version                    uint32                                   `json:"version"`
	Scope                      AttemptExecutionAdmissionScopeV1         `json:"scope"`
	Profile                    OwnedAgentProfileReferenceV1             `json:"profile"`
	Spending                   [4]AttemptSpendingAuthorizationCurrentV1 `json:"spending"`
	AgentLoopPolicyDigest      AgentLoopPolicyDigestV1                  `json:"agent_loop_policy_digest"`
	HarnessBindingDigest       HarnessBindingDigestV2                   `json:"harness_binding_digest"`
	ExecutionPlacementDigest   string                                   `json:"execution_placement_digest"`
	SubstrateBindingDigest     SubstrateBindingDigestV1                 `json:"substrate_binding_digest"`
	AdmissionCostCeilingDigest AdmissionCostCeilingDigestV2             `json:"admission_cost_ceiling_digest"`
	Currency                   string                                   `json:"currency"`
	MaxTotalAmountMicrounits   uint64                                   `json:"max_total_amount_microunits"`
	AdmittedAt                 time.Time                                `json:"admitted_at"`
	ExpiresAt                  time.Time                                `json:"expires_at"`
}

func (v AttemptExecutionAdmissionV1) Clone() AttemptExecutionAdmissionV1 {
	for i := range v.Spending {
		v.Spending[i] = v.Spending[i].Clone()
	}
	v.AdmittedAt, v.ExpiresAt = v.AdmittedAt.UTC(), v.ExpiresAt.UTC()
	return v
}

func (v AttemptExecutionAdmissionV1) Validate() error {
	if v.Version != 1 || v.Scope.Validate() != nil || v.Profile.Validate() != nil ||
		v.Profile.Locator.TenantID != v.Scope.TenantID || v.Profile.Locator.OwnerUserID != v.Scope.OwnerUserID ||
		!validCurrencyV1(v.Currency) || !ownedCurrentTime(v.AdmittedAt) || !ownedCurrentTime(v.ExpiresAt) || !v.ExpiresAt.After(v.AdmittedAt) {
		return admissionError("identity", "requires exact scope, original profile and finite microsecond window")
	}
	for _, digest := range []string{string(v.AgentLoopPolicyDigest), string(v.HarnessBindingDigest), v.ExecutionPlacementDigest, string(v.SubstrateBindingDigest), string(v.AdmissionCostCeilingDigest)} {
		if err := ValidateSHA256Digest("attempt_execution_admission.digest", digest); err != nil {
			return err
		}
	}
	kinds := [4]AttemptSpendingComponentKindV1{AttemptSpendingProviderV1, AttemptSpendingSubstrateV1, AttemptSpendingMCPToolV1, AttemptSpendingWebSearchV1}
	var total uint64
	for i, s := range v.Spending {
		if s.ValidateAt(v.AdmittedAt) != nil || s.Locator.ComponentKind != kinds[i] || s.Locator.TenantID != v.Scope.TenantID ||
			s.Locator.AttemptID != v.Scope.AttemptID || s.RunID != v.Scope.RunID || s.SessionID != v.Scope.SessionID || s.OwnerUserID != v.Scope.OwnerUserID ||
			s.SourceProfile != v.Profile || s.Locator.BillingOwnerUserID != v.Spending[0].Locator.BillingOwnerUserID || s.Currency != v.Currency ||
			v.ExpiresAt.After(s.Window.ExpiresAt) || s.MaxAmountMicrounits == nil {
			return admissionError("spending", "requires exactly four ordered original same-scope/profile/payer known allocations")
		}
		if *s.MaxAmountMicrounits > ^uint64(0)-total {
			return admissionError("total", "component authorization sum must not overflow")
		}
		total += *s.MaxAmountMicrounits
	}
	if total != v.MaxTotalAmountMicrounits {
		return admissionError("total", "must equal the checked sum of original component allocations")
	}
	return nil
}

// This matches historical admission bytes to the admitted execution projection.
// It intentionally uses original admission time; fresh execution permission
// additionally requires transactional current profile/spending/source joins,
// canonical Session/Run/Attempt/capacity/lease and one physical claimant.
func (v AttemptExecutionAdmissionV1) ValidateForExecution(placement ExecutionPlacementV2, binding HarnessBindingV2, substrate SubstrateBindingV1, cost AdmissionCostCeilingV2) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if binding.Profile != HarnessExecutionOwnedAgentV2 || placement.Kind != ExecutionPlacementManaged ||
		binding.ValidateForScope(v.Scope.TenantID, v.Scope.OwnerUserID, v.Scope.RunID, v.Scope.AttemptID, placement) != nil ||
		binding.ValidateAdmissionCostCeiling(&cost) != nil || binding.ValidateAt(v.AdmittedAt) != nil ||
		cost.ValidateForSubstrate(substrate) != nil || cost.ValidateAt(v.AdmittedAt) != nil || substrate.ValidateAt(v.AdmittedAt) != nil {
		return admissionError("execution", "requires the exact admitted managed owned execution projection")
	}
	policy := binding.AgentLoopPolicy
	pd, _ := policy.Digest()
	bd, _ := binding.Digest()
	ld, _ := ExecutionPlacementDigest(placement)
	sd, _ := substrate.Digest()
	cd, _ := cost.Digest()
	if pd != v.AgentLoopPolicyDigest || bd != v.HarnessBindingDigest || string(ld) != v.ExecutionPlacementDigest ||
		sd != v.SubstrateBindingDigest || cd != v.AdmissionCostCeilingDigest || placement.SubstrateBindingDigest != string(sd) ||
		v.Currency != cost.Currency || cost.MaxTotalAmountMicrounits == nil || v.MaxTotalAmountMicrounits != *cost.MaxTotalAmountMicrounits ||
		v.ExpiresAt.After(policy.ExpiresAt) || v.ExpiresAt.After(substrate.ProfileEvidenceExpiresAt) || v.ExpiresAt.After(cost.PriceExpiresAt) {
		return admissionError("projection", "must preserve all completed digests, currency, aggregate and original windows")
	}
	l := v.Profile.Locator
	if l.BackendProfileDigest != binding.Backend.BackendProfileDigest || l.ResourceKind != binding.Resource.Kind || l.ResourceID != binding.Resource.ResourceID ||
		l.ModelVendorID != binding.ModelVendorID || l.ModelID != binding.ModelID || l.InputDataClass != binding.InputDataClass || l.Profile != policy.Profile {
		return admissionError("profile", "must preserve the original backend/resource/model/data/profile tuple")
	}
	amounts := [4]*uint64{cost.MaxProviderAmountMicrounits, cost.MaxSubstrateAmountMicrounits, cost.MCPToolCost.MaxAmountMicrounits, cost.WebSearchCost.MaxAmountMicrounits}
	for i, amount := range amounts {
		if amount == nil || *amount != *v.Spending[i].MaxAmountMicrounits {
			return admissionError("allocation", "each admitted component ceiling must equal its original authorized allocation")
		}
	}
	return nil
}

func (v AttemptExecutionAdmissionV1) Digest() (string, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	return ownedCurrentDigest("sessionless.attempt-execution-admission.v1", v.Clone())
}

// Exact original references are not enough on their own: all five current
// heads must still be active and unchanged in the SAME DB-time snapshot. This
// comparison never selects a replacement payer/package or renews admission.
func (v AttemptExecutionAdmissionV1) ValidateAgainstCurrentSources(profile OwnedAgentProfileCurrentV1, spending [4]AttemptSpendingAuthorizationCurrentV1, now time.Time) error {
	if v.Validate() != nil || !ownedCurrentTime(now) || now.Before(v.AdmittedAt) || !now.Before(v.ExpiresAt) || profile.ValidateAt(now) != nil {
		return admissionError("current", "requires original admission and current source windows")
	}
	pd, err := profile.Digest()
	if err != nil || profile.Locator != v.Profile.Locator || profile.RevocationGeneration != v.Profile.RevocationGeneration || pd != v.Profile.Digest ||
		v.Currency != profile.CostCeiling.Currency || v.MaxTotalAmountMicrounits != *profile.CostCeiling.MaxTotalAmountMicrounits || v.ExpiresAt.After(profile.Window.ExpiresAt) {
		return admissionError("current_profile", "requires the exact original current registration")
	}
	for i, s := range spending {
		want, err := v.Spending[i].Digest()
		got, gotErr := s.Digest()
		if err != nil || gotErr != nil || want != got || s.ValidateAgainstProfile(profile, now) != nil {
			return admissionError("current_spending", "requires every exact original active component/source")
		}
	}
	policy, err := ownedProfilePolicy(v.Scope, profile, v.AdmittedAt, v.ExpiresAt)
	if err != nil {
		return admissionError("current_projection", "cannot reconstruct the original profile projection")
	}
	pdPolicy, _ := policy.Digest()
	sd, _ := profile.Substrate.Digest()
	cd, _ := profile.CostCeiling.Digest()
	placement, err := ManagedExecutionPlacementV2(string(sd))
	if err != nil {
		return err
	}
	ld, _ := ExecutionPlacementDigest(placement)
	bd, err := ownedProfileBinding(v.Scope, profile, policy, placement).Digest()
	if err != nil || v.AgentLoopPolicyDigest != pdPolicy || v.SubstrateBindingDigest != sd || v.AdmissionCostCeilingDigest != cd ||
		v.ExecutionPlacementDigest != string(ld) || v.HarnessBindingDigest != bd {
		return admissionError("current_projection", "original completed projection must derive from the exact original source package")
	}
	return nil
}

func EncodeAttemptExecutionAdmissionV1(v AttemptExecutionAdmissionV1) ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return encodeOwnedCurrent(v.Clone())
}

func DecodeAttemptExecutionAdmissionV1(raw []byte) (AttemptExecutionAdmissionV1, error) {
	var v AttemptExecutionAdmissionV1
	if err := decodeOwnedCurrent(raw, &v); err != nil {
		return AttemptExecutionAdmissionV1{}, err
	}
	expected, err := EncodeAttemptExecutionAdmissionV1(v)
	if err != nil || !bytes.Equal(raw, expected) {
		return AttemptExecutionAdmissionV1{}, admissionError("codec", "requires exact bounded canonical producer bytes")
	}
	return v.Clone(), nil
}

// A pure composition result. It does not authenticate its supplied installation
// or payer, publish any row, hold capacity, prepare, mint or execute anything.
// The trusted issuer and canonical transaction remain responsible for those
// boundaries. Existing one-shot registrations remain unsupported for owned use.
type OwnedAgentExecutionPackageV1 struct {
	Admission AttemptExecutionAdmissionV1
	Placement ExecutionPlacementV2
	Binding   HarnessBindingV2
	Substrate SubstrateBindingV1
	Cost      AdmissionCostCeilingV2
}

// BuildOwnedAgentExecutionPackageV1 derives the complete acyclic package from
// ONE current profile and the original four allocations, never an independent
// caller-supplied policy/binding. Initial funding is credential-free fixture
// only; no fixture string can approve a live/foreign/platform funding source.
func BuildOwnedAgentExecutionPackageV1(scope AttemptExecutionAdmissionScopeV1, profile OwnedAgentProfileCurrentV1, spending [4]AttemptSpendingAuthorizationCurrentV1, at time.Time) (OwnedAgentExecutionPackageV1, error) {
	denied := func() (OwnedAgentExecutionPackageV1, error) {
		return OwnedAgentExecutionPackageV1{}, admissionError("source", "requires exact original current fixture sources")
	}
	if scope.Validate() != nil || !ownedCurrentTime(at) || profile.ValidateAt(at) != nil ||
		profile.Locator.TenantID != scope.TenantID || profile.Locator.OwnerUserID != scope.OwnerUserID {
		return denied()
	}
	expires := profile.Window.ExpiresAt
	for _, s := range spending {
		if s.ValidateAgainstProfile(profile, at) != nil || s.Locator.TenantID != scope.TenantID || s.Locator.AttemptID != scope.AttemptID ||
			s.RunID != scope.RunID || s.SessionID != scope.SessionID || s.OwnerUserID != scope.OwnerUserID {
			return denied()
		}
		if s.Window.ExpiresAt.Before(expires) {
			expires = s.Window.ExpiresAt
		}
	}
	policy, err := ownedProfilePolicy(scope, profile, at, expires)
	if err != nil {
		return denied()
	}
	sd, _ := profile.Substrate.Digest()
	placement, err := ManagedExecutionPlacementV2(string(sd))
	if err != nil {
		return denied()
	}
	ld, _ := ExecutionPlacementDigest(placement)
	binding := ownedProfileBinding(scope, profile, policy, placement)
	bd, err := binding.Digest()
	if err != nil {
		return denied()
	}
	pd, _ := policy.Digest()
	cd, _ := profile.CostCeiling.Digest()
	profileDigest, _ := profile.Digest()
	attachment := AttemptExecutionAdmissionV1{
		Version: 1, Scope: scope, Profile: OwnedAgentProfileReferenceV1{Locator: profile.Locator, RevocationGeneration: profile.RevocationGeneration, Digest: profileDigest},
		Spending: spending, AgentLoopPolicyDigest: pd, HarnessBindingDigest: bd, ExecutionPlacementDigest: string(ld), SubstrateBindingDigest: sd,
		AdmissionCostCeilingDigest: cd, Currency: profile.CostCeiling.Currency, MaxTotalAmountMicrounits: *profile.CostCeiling.MaxTotalAmountMicrounits,
		AdmittedAt: at.UTC(), ExpiresAt: expires.UTC(),
	}.Clone()
	if attachment.ValidateForExecution(placement, binding, profile.Substrate, profile.CostCeiling) != nil ||
		attachment.ValidateAgainstCurrentSources(profile, spending, at) != nil {
		return denied()
	}
	if _, err := EncodeAttemptExecutionAdmissionV1(attachment); err != nil {
		return denied()
	}
	return OwnedAgentExecutionPackageV1{Admission: attachment, Placement: placement, Binding: binding.Clone(), Substrate: profile.Substrate, Cost: profile.CostCeiling.Clone()}, nil
}

func admissionError(field, reason string) error {
	return ValidationError{Field: "attempt_execution_admission." + field, Reason: reason}
}

func ownedProfilePolicy(scope AttemptExecutionAdmissionScopeV1, profile OwnedAgentProfileCurrentV1, at, expires time.Time) (AgentLoopPolicyV1, error) {
	return BuildAgentLoopPolicyV1(AgentLoopPolicyV1{
		Version: 1, TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, RunID: scope.RunID, AttemptID: scope.AttemptID,
		Profile: profile.Locator.Profile, ProfileRevision: profile.ProfileRevision, Backend: profile.Backend, Resource: profile.Resource,
		ModelVendorID: profile.Locator.ModelVendorID, ModelID: profile.Locator.ModelID, ProviderCatalogDigest: profile.ProviderCatalogDigest,
		ProviderRouteDigest: profile.ProviderRouteDigest, CapabilityEvidenceDigest: profile.CapabilityEvidenceDigest,
		PrivacyPolicyDigest: profile.PrivacyPolicyDigest, EffectivePolicyDigest: profile.EffectivePolicyDigest,
		InputModalities: profile.InputModalities, InputDataClass: profile.Locator.InputDataClass, InstructionVersion: profile.InstructionVersion,
		CompactionPolicyDigest: profile.CompactionPolicyDigest, MCPToolGrantDigest: profile.MCPToolGrantDigest, WebSearchGrantDigest: profile.WebSearchGrantDigest,
		Limits: profile.Limits, AdmissionCostCeiling: profile.CostCeiling, IssuedAt: at.UTC(), ExpiresAt: expires.UTC(),
	})
}

func ownedProfileBinding(scope AttemptExecutionAdmissionScopeV1, profile OwnedAgentProfileCurrentV1, policy AgentLoopPolicyV1, placement ExecutionPlacementV2) HarnessBindingV2 {
	ld, _ := ExecutionPlacementDigest(placement)
	return HarnessBindingV2{
		Version: 2, Profile: HarnessExecutionOwnedAgentV2, AgentLoopPolicy: &policy,
		TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, RunID: scope.RunID, AttemptID: scope.AttemptID,
		Backend: profile.Backend, Resource: profile.Resource, ModelVendorID: profile.Locator.ModelVendorID, ModelID: profile.Locator.ModelID,
		InputDataClass: profile.Locator.InputDataClass, ProviderCatalogDigest: profile.ProviderCatalogDigest, ProviderRouteDigest: profile.ProviderRouteDigest,
		CapabilityEvidenceDigest: profile.CapabilityEvidenceDigest, PrivacyPolicyDigest: profile.PrivacyPolicyDigest,
		EffectivePolicyDigest: profile.EffectivePolicyDigest, ExecutionPlacementDigest: string(ld),
	}
}
