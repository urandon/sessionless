package domain

import (
	"bytes"
	"time"
)

// This original identity is fixed at canonical ingress, before scheduler
// admission. It contains NO reservation: that authority does not yet exist.
type AttemptExecutionSourceScopeV1 struct {
	TenantID    TenantID  `json:"tenant_id"`
	SessionID   SessionID `json:"session_id"`
	OwnerUserID UserID    `json:"owner_user_id"`
	RunID       RunID     `json:"run_id"`
	AttemptID   AttemptID `json:"attempt_id"`
}

func (v AttemptExecutionSourceScopeV1) Validate() error {
	for _, check := range []func() error{v.TenantID.Validate, v.SessionID.Validate, v.OwnerUserID.Validate, v.RunID.Validate, v.AttemptID.Validate} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

// AttemptExecutionSourceV1 fixes the original ingress source package, not
// admission or current permission. It preserves the original issued window,
// four exact funding snapshots and completed execution digests. Its String
// codec caps the entire package at 32 KiB. Canonical ingress must match its
// Session against the actually authorized frontend binding in the SAME Tx.
type AttemptExecutionSourceV1 struct {
	Version                    uint32                                   `json:"version"`
	Scope                      AttemptExecutionSourceScopeV1            `json:"scope"`
	Profile                    OwnedAgentProfileReferenceV1             `json:"profile"`
	Spending                   [4]AttemptSpendingAuthorizationCurrentV1 `json:"spending"`
	AgentLoopPolicyDigest      AgentLoopPolicyDigestV1                  `json:"agent_loop_policy_digest"`
	HarnessBindingDigest       HarnessBindingDigestV2                   `json:"harness_binding_digest"`
	ExecutionPlacementDigest   string                                   `json:"execution_placement_digest"`
	SubstrateBindingDigest     SubstrateBindingDigestV1                 `json:"substrate_binding_digest"`
	AdmissionCostCeilingDigest AdmissionCostCeilingDigestV2             `json:"admission_cost_ceiling_digest"`
	Currency                   string                                   `json:"currency"`
	MaxTotalAmountMicrounits   uint64                                   `json:"max_total_amount_microunits"`
	IssuedAt                   time.Time                                `json:"issued_at"`
	ExpiresAt                  time.Time                                `json:"expires_at"`
}

func (v AttemptExecutionSourceV1) Clone() AttemptExecutionSourceV1 {
	for i := range v.Spending {
		v.Spending[i] = v.Spending[i].Clone()
	}
	v.IssuedAt, v.ExpiresAt = v.IssuedAt.UTC(), v.ExpiresAt.UTC()
	return v
}

func (v AttemptExecutionSourceV1) Validate() error {
	if v.Version != 1 || v.Scope.Validate() != nil || v.Profile.Validate() != nil ||
		v.Profile.Locator.TenantID != v.Scope.TenantID || v.Profile.Locator.OwnerUserID != v.Scope.OwnerUserID ||
		!validCurrencyV1(v.Currency) || !ownedCurrentTime(v.IssuedAt) || !ownedCurrentTime(v.ExpiresAt) || !v.ExpiresAt.After(v.IssuedAt) {
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
		if s.ValidateAt(v.IssuedAt) != nil || s.Locator.ComponentKind != kinds[i] || s.Locator.TenantID != v.Scope.TenantID ||
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
func (v AttemptExecutionSourceV1) ValidateForExecution(placement ExecutionPlacementV2, binding HarnessBindingV2, substrate SubstrateBindingV1, cost AdmissionCostCeilingV2) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if binding.Profile != HarnessExecutionOwnedAgentV2 || placement.Kind != ExecutionPlacementManaged ||
		binding.ValidateForScope(v.Scope.TenantID, v.Scope.OwnerUserID, v.Scope.RunID, v.Scope.AttemptID, placement) != nil ||
		binding.ValidateAdmissionCostCeiling(&cost) != nil || binding.ValidateAt(v.IssuedAt) != nil ||
		cost.ValidateForSubstrate(substrate) != nil || cost.ValidateAt(v.IssuedAt) != nil || substrate.ValidateAt(v.IssuedAt) != nil {
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

func (v AttemptExecutionSourceV1) Digest() (string, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	return ownedCurrentDigest("sessionless.attempt-execution-source.v1", v.Clone())
}

// Exact original references are not enough on their own: all five current
// heads must still be active and unchanged in the SAME DB-time snapshot. This
// comparison never selects a replacement payer/package or renews admission.
func (v AttemptExecutionSourceV1) ValidateAgainstCurrentSources(profile OwnedAgentProfileCurrentV1, spending [4]AttemptSpendingAuthorizationCurrentV1, now time.Time) error {
	if v.Validate() != nil || !ownedCurrentTime(now) || now.Before(v.IssuedAt) || !now.Before(v.ExpiresAt) || profile.ValidateAt(now) != nil {
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
	policy, err := ownedProfilePolicy(v.Scope, profile, v.IssuedAt, v.ExpiresAt)
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

func EncodeAttemptExecutionSourceV1(v AttemptExecutionSourceV1) ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return encodeOwnedCurrent(v.Clone())
}

func DecodeAttemptExecutionSourceV1(raw []byte) (AttemptExecutionSourceV1, error) {
	var v AttemptExecutionSourceV1
	if err := decodeOwnedCurrent(raw, &v); err != nil {
		return AttemptExecutionSourceV1{}, err
	}
	expected, err := EncodeAttemptExecutionSourceV1(v)
	if err != nil || !bytes.Equal(raw, expected) {
		return AttemptExecutionSourceV1{}, admissionError("codec", "requires exact bounded canonical producer bytes")
	}
	return v.Clone(), nil
}

// A pure composition result. It does not authenticate its supplied installation
// or payer, publish any row, hold capacity, prepare, mint or execute anything.
// The trusted issuer and canonical transaction remain responsible for those
// boundaries. Existing one-shot registrations remain unsupported for owned use.
type OwnedAgentExecutionPackageV1 struct {
	Source    AttemptExecutionSourceV1
	Placement ExecutionPlacementV2
	Binding   HarnessBindingV2
	Substrate SubstrateBindingV1
	Cost      AdmissionCostCeilingV2
}

// BuildOwnedAgentExecutionPackageV1 derives the complete acyclic package from
// ONE current profile and the original four allocations at original issuance,
// BEFORE a capacity reservation exists, never an independent
// caller-supplied policy/binding. Initial funding is credential-free fixture
// only; no fixture string can approve a live/foreign/platform funding source.
func BuildOwnedAgentExecutionPackageV1(scope AttemptExecutionSourceScopeV1, profile OwnedAgentProfileCurrentV1, spending [4]AttemptSpendingAuthorizationCurrentV1, at time.Time) (OwnedAgentExecutionPackageV1, error) {
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
	attachment := AttemptExecutionSourceV1{
		Version: 1, Scope: scope, Profile: OwnedAgentProfileReferenceV1{Locator: profile.Locator, RevocationGeneration: profile.RevocationGeneration, Digest: profileDigest},
		Spending: spending, AgentLoopPolicyDigest: pd, HarnessBindingDigest: bd, ExecutionPlacementDigest: string(ld), SubstrateBindingDigest: sd,
		AdmissionCostCeilingDigest: cd, Currency: profile.CostCeiling.Currency, MaxTotalAmountMicrounits: *profile.CostCeiling.MaxTotalAmountMicrounits,
		IssuedAt: at.UTC(), ExpiresAt: expires.UTC(),
	}.Clone()
	if attachment.ValidateForExecution(placement, binding, profile.Substrate, profile.CostCeiling) != nil ||
		attachment.ValidateAgainstCurrentSources(profile, spending, at) != nil {
		return denied()
	}
	if _, err := EncodeAttemptExecutionSourceV1(attachment); err != nil {
		return denied()
	}
	return OwnedAgentExecutionPackageV1{Source: attachment, Placement: placement, Binding: binding.Clone(), Substrate: profile.Substrate, Cost: profile.CostCeiling.Clone()}, nil
}

func admissionError(field, reason string) error {
	return ValidationError{Field: "attempt_execution_source." + field, Reason: reason}
}

func ownedProfilePolicy(scope AttemptExecutionSourceScopeV1, profile OwnedAgentProfileCurrentV1, at, expires time.Time) (AgentLoopPolicyV1, error) {
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

func ownedProfileBinding(scope AttemptExecutionSourceScopeV1, profile OwnedAgentProfileCurrentV1, policy AgentLoopPolicyV1, placement ExecutionPlacementV2) HarnessBindingV2 {
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

// AttemptExecutionAdmissionV1 is the create-once final attachment to the
// canonical WorkerJob. Only the existing dispatch transaction can establish
// the actual held reservation and its DB admission time. Source retains its
// original issued time and completed policy/binding: admission NEVER rebuilds
// those bytes, refreshes funding or selects another profile/payer.
//
// Structural validation is not held capacity, authentic installation, current
// same-Tx permission, a debit or a capability. Missing persisted history must
// deny; it may not be reconstructed from WorkerJob or current source heads.
type AttemptExecutionAdmissionV1 struct {
	Version       uint32                   `json:"version"`
	Source        AttemptExecutionSourceV1 `json:"source"`
	ReservationID QuotaReservationID       `json:"reservation_id"`
	AdmittedAt    time.Time                `json:"admitted_at"`
}

func (v AttemptExecutionAdmissionV1) Clone() AttemptExecutionAdmissionV1 {
	v.Source = v.Source.Clone()
	v.AdmittedAt = v.AdmittedAt.UTC()
	return v
}

func (v AttemptExecutionAdmissionV1) Validate() error {
	if v.Version != 1 || v.Source.Validate() != nil || v.ReservationID.Validate() != nil ||
		!ownedCurrentTime(v.AdmittedAt) || v.AdmittedAt.Before(v.Source.IssuedAt) || !v.AdmittedAt.Before(v.Source.ExpiresAt) {
		return ValidationError{Field: "attempt_execution_admission", Reason: "requires original source, actual reservation identity and finite DB admission time"}
	}
	return nil
}

func BuildAttemptExecutionAdmissionV1(source AttemptExecutionSourceV1, reservationID QuotaReservationID, admittedAt time.Time) (AttemptExecutionAdmissionV1, error) {
	v := AttemptExecutionAdmissionV1{Version: 1, Source: source.Clone(), ReservationID: reservationID, AdmittedAt: admittedAt.UTC()}
	// Check the supplied timestamp before UTC conversion, including precision.
	if !ownedCurrentTime(admittedAt) || v.Validate() != nil {
		return AttemptExecutionAdmissionV1{}, ValidationError{Field: "attempt_execution_admission", Reason: "invalid original source or final admission"}
	}
	if _, err := EncodeAttemptExecutionAdmissionV1(v); err != nil {
		return AttemptExecutionAdmissionV1{}, err
	}
	return v, nil
}

func (v AttemptExecutionAdmissionV1) ValidateForExecution(placement ExecutionPlacementV2, binding HarnessBindingV2, substrate SubstrateBindingV1, cost AdmissionCostCeilingV2) error {
	if err := v.Validate(); err != nil {
		return err
	}
	return v.Source.ValidateForExecution(placement, binding, substrate, cost)
}

func (v AttemptExecutionAdmissionV1) ValidateAgainstCurrentSources(profile OwnedAgentProfileCurrentV1, spending [4]AttemptSpendingAuthorizationCurrentV1, now time.Time) error {
	if v.Validate() != nil || !ownedCurrentTime(now) || now.Before(v.AdmittedAt) {
		return ValidationError{Field: "attempt_execution_admission.current", Reason: "requires original admission time"}
	}
	return v.Source.ValidateAgainstCurrentSources(profile, spending, now)
}

func (v AttemptExecutionAdmissionV1) Digest() (string, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	return ownedCurrentDigest("sessionless.attempt-execution-admission.v1", v.Clone())
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
		return AttemptExecutionAdmissionV1{}, ValidationError{Field: "attempt_execution_admission.codec", Reason: "requires exact bounded canonical producer bytes"}
	}
	return v.Clone(), nil
}

// Writer-first tagged source projection. Single-turn and attached execution
// exclude owned source bytes; owned execution requires the one original strict
// producer record. This is structural correlation, not source authentication.
func ValidateOwnedExecutionSourceRecord(record string, scope AttemptExecutionSourceScopeV1, placement ExecutionPlacementV2, binding HarnessBindingV2, substrate *SubstrateBindingV1, cost *AdmissionCostCeilingV2) error {
	if binding.Profile != HarnessExecutionOwnedAgentV2 {
		if record != "" {
			return ValidationError{Field: "owned_execution_source", Reason: "must be absent for non-owned execution"}
		}
		return nil
	}
	if substrate == nil || cost == nil {
		return ValidationError{Field: "owned_execution_source", Reason: "requires exact managed execution projection"}
	}
	source, err := DecodeAttemptExecutionSourceV1([]byte(record))
	if err != nil || source.Scope != scope || source.ValidateForExecution(placement, binding, *substrate, *cost) != nil {
		return ValidationError{Field: "owned_execution_source", Reason: "must preserve original canonical scope and execution projection"}
	}
	return nil
}
