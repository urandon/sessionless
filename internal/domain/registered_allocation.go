package domain

import (
	"bytes"
	"math"
	"time"
)

const RegisteredAllocationVersionV1 uint32 = 1

type RegisteredPreparationStateV1 string

const (
	RegisteredPreparationActiveV1  RegisteredPreparationStateV1 = "active"
	RegisteredPreparationRetiredV1 RegisteredPreparationStateV1 = "retired"
	RegisteredPreparationRevokedV1 RegisteredPreparationStateV1 = "revoked"
)

type RegisteredAllocationDigestV1 string
type RegisteredProxyAttestationDigestV1 string

// References preserve the exact original four heads, without embedding their
// bodies or introducing another spending/source authority.
type RegisteredSpendingReferenceV1 struct {
	Locator              AttemptSpendingAuthorizationLocatorV1 `json:"locator"`
	RevocationGeneration uint64                                `json:"revocation_generation"`
	Digest               string                                `json:"digest"`
	Window               OwnedAgentAuthorityWindowV1           `json:"window"`
}

// RegisteredAllocationV1 is a bounded structural child of the sole original
// outer claim. Authentic supervisor publication, current SQL state, installed
// approval and capability verification remain separate requirements. A valid
// record cannot enable an unsupported backend or authorize another preparation.
// PreparedAllocation is sealed by digest, NOT embedded: its legitimate child
// argv may be 64*4096 bytes and must never be truncated to fit this 32-KiB head.
type RegisteredAllocationV1 struct {
	Version                     uint32                           `json:"version"`
	Scope                       AttemptExecutionSourceScopeV1    `json:"scope"`
	Lease                       Lease                            `json:"lease"`
	ParentReservation           AttemptEffectReservationV1       `json:"parent_reservation"`
	ParentReservationDigest     AttemptEffectReservationDigestV1 `json:"parent_reservation_digest"`
	PreparedInvocationDigest    PreparedInvocationDigestV1       `json:"prepared_invocation_digest"`
	PreparedInvocationExpiresAt time.Time                        `json:"prepared_invocation_expires_at"`
	InvocationDeadline          time.Time                        `json:"invocation_deadline"`
	PreparedAllocationDigest    PreparedAllocationDigestV1       `json:"prepared_allocation_digest"`
	Backend                     HarnessBackendDescriptorV1       `json:"backend"`
	Substrate                   SubstrateBindingV1               `json:"substrate"`
	ObservedImageDigest         string                           `json:"observed_image_digest"`
	ObservedOuterHarnessDigest  string                           `json:"observed_outer_harness_digest"`
	ObservedProxyArtifactDigest string                           `json:"observed_proxy_artifact_digest"`
	ObservedProxyIdentityDigest string                           `json:"observed_proxy_identity_digest"`
	WorkloadMode                SubstrateWorkloadModeV1          `json:"workload_mode"`
	SourceDigest                string                           `json:"source_digest"`
	Profile                     OwnedAgentProfileReferenceV1     `json:"profile"`
	Spending                    [4]RegisteredSpendingReferenceV1 `json:"spending"`
	SourceIssuedAt              time.Time                        `json:"source_issued_at"`
	SourceExpiresAt             time.Time                        `json:"source_expires_at"`
	AllocationID                string                           `json:"allocation_id"`
	AllocationGeneration        uint64                           `json:"allocation_generation"`
	State                       RegisteredPreparationStateV1     `json:"state"`
	RevocationGeneration        uint64                           `json:"revocation_generation"`
	Window                      OwnedAgentAuthorityWindowV1      `json:"window"`
	UpdatedAt                   time.Time                        `json:"updated_at"`
}

func (v RegisteredAllocationV1) Clone() RegisteredAllocationV1 {
	v.ParentReservation = v.ParentReservation.Clone()
	v.Lease.AcquiredAt, v.Lease.ExpiresAt = v.Lease.AcquiredAt.UTC(), v.Lease.ExpiresAt.UTC()
	v.ParentReservation.ReservedAt = v.ParentReservation.ReservedAt.UTC()
	v.Substrate.ProfileEvidenceExpiresAt = v.Substrate.ProfileEvidenceExpiresAt.UTC()
	for i := range v.Spending {
		v.Spending[i].Window = v.Spending[i].Window.Clone()
	}
	v.SourceIssuedAt, v.SourceExpiresAt = v.SourceIssuedAt.UTC(), v.SourceExpiresAt.UTC()
	v.PreparedInvocationExpiresAt, v.InvocationDeadline = v.PreparedInvocationExpiresAt.UTC(), v.InvocationDeadline.UTC()
	v.Window, v.UpdatedAt = v.Window.Clone(), v.UpdatedAt.UTC()
	return v
}

func (v RegisteredAllocationV1) Validate() error {
	bad := func() error {
		return registeredPreparationError("allocation", "requires exact bounded original claim and observation")
	}
	if v.Version != 1 || v.Scope.Validate() != nil || validateServerlessLease(v.Lease) != nil || v.ParentReservation.Validate() != nil ||
		v.ParentReservationDigest.Validate() != nil || v.PreparedInvocationDigest.Validate() != nil || v.PreparedAllocationDigest.Validate() != nil ||
		v.Backend.Validate() != nil || v.Substrate.Validate() != nil || v.Profile.Validate() != nil ||
		ValidateOpaqueID("registered_allocation.allocation_id", v.AllocationID) != nil || v.AllocationGeneration == 0 ||
		ValidateSHA256Digest("registered_allocation.source_digest", v.SourceDigest) != nil ||
		!ownedCurrentTime(v.SourceIssuedAt) || !ownedCurrentTime(v.SourceExpiresAt) || !v.SourceExpiresAt.After(v.SourceIssuedAt) ||
		!ownedCurrentTime(v.PreparedInvocationExpiresAt) || !ownedCurrentTime(v.InvocationDeadline) ||
		!ownedCurrentTime(v.Lease.AcquiredAt) || !ownedCurrentTime(v.Lease.ExpiresAt) || !ownedCurrentTime(v.ParentReservation.ReservedAt) ||
		validateRegisteredLifecycle(v.State, v.RevocationGeneration, v.Window, v.UpdatedAt) != nil {
		return bad()
	}
	l, p := v.Lease, v.ParentReservation
	if l.TenantID != v.Scope.TenantID || l.RunID != v.Scope.RunID || l.AttemptID != v.Scope.AttemptID ||
		p.TenantID != l.TenantID || p.RunID != l.RunID || p.AttemptID != l.AttemptID || p.LeaseID != l.ID || p.FenceToken != l.FenceToken ||
		p.ReservedAt.Before(l.AcquiredAt) || !p.ReservedAt.Before(l.ExpiresAt) || v.Window.ObservedAt.Before(p.ReservedAt) ||
		v.Window.ObservedAt.Before(v.SourceIssuedAt) || v.Window.ExpiresAt.After(v.SourceExpiresAt) || v.Window.ExpiresAt.After(l.ExpiresAt) ||
		v.InvocationDeadline.After(l.ExpiresAt) || !v.InvocationDeadline.After(l.AcquiredAt) || v.PreparedInvocationExpiresAt.After(v.InvocationDeadline) ||
		!v.PreparedInvocationExpiresAt.After(p.ReservedAt) || v.Window.ExpiresAt.After(v.PreparedInvocationExpiresAt) || v.Window.ExpiresAt.After(v.InvocationDeadline) ||
		v.Profile.Locator.TenantID != v.Scope.TenantID || v.Profile.Locator.OwnerUserID != v.Scope.OwnerUserID ||
		v.Profile.Locator.BackendProfileDigest != v.Backend.BackendProfileDigest {
		return bad()
	}
	sd, err := v.Substrate.Digest()
	if err != nil || p.SubstrateBindingDigest != sd || p.AdmissionCostCeilingDigest != v.Substrate.AdmissionCostCeilingDigest ||
		v.ObservedImageDigest != v.Substrate.ImageDigest || v.ObservedOuterHarnessDigest != v.Substrate.OuterHarnessArtifactDigest ||
		v.ObservedProxyArtifactDigest != v.Substrate.EgressProxyArtifactDigest || v.ObservedProxyIdentityDigest != v.Substrate.EgressProxyIdentityDigest || v.WorkloadMode != v.Substrate.WorkloadMode ||
		v.Substrate.ValidateAt(v.Window.ObservedAt) != nil || !ownedCurrentTime(v.Substrate.ProfileEvidenceExpiresAt) || v.Window.ExpiresAt.After(v.Substrate.ProfileEvidenceExpiresAt) {
		return bad()
	}
	kinds := [4]AttemptSpendingComponentKindV1{AttemptSpendingProviderV1, AttemptSpendingSubstrateV1, AttemptSpendingMCPToolV1, AttemptSpendingWebSearchV1}
	for i, s := range v.Spending {
		if s.Locator.Validate() != nil || s.RevocationGeneration == 0 || ValidateSHA256Digest("registered_allocation.spending_digest", s.Digest) != nil ||
			s.Locator.TenantID != v.Scope.TenantID || s.Locator.AttemptID != v.Scope.AttemptID || s.Locator.ComponentKind != kinds[i] ||
			s.Locator.BillingOwnerUserID != v.Spending[0].Locator.BillingOwnerUserID || s.Window.ValidateAt(v.Window.ObservedAt) != nil ||
			v.Window.ExpiresAt.After(s.Window.ExpiresAt) {
			return bad()
		}
	}
	return nil
}

// The caller must supply the EXACT original authenticated prepared capability's
// digest. A domain type cannot verify its process-local MAC or confirmed commit.
func BuildRegisteredAllocationV1(authority ServerlessInvocationAuthorityV1, parent AttemptEffectReservationV1, prepared PreparedAllocationV1, preparedDigest PreparedInvocationDigestV1, preparedExpiresAt time.Time, source AttemptExecutionSourceV1, allocationID string, allocationGeneration uint64, window OwnedAgentAuthorityWindowV1) (RegisteredAllocationV1, error) {
	if window.Validate() != nil || authority.ValidateAt(window.ObservedAt) != nil || parent.ValidateForAuthority(authority) != nil ||
		prepared.ValidateForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend) != nil || preparedDigest.Validate() != nil ||
		!ownedCurrentTime(preparedExpiresAt) || preparedExpiresAt.After(authority.InvocationDeadline) || window.ExpiresAt.After(preparedExpiresAt) ||
		source.ValidateForExecution(authority.ExecutionPlacementV2, authority.HarnessBinding, authority.SubstrateBinding, authority.AdmissionCostCeiling) != nil ||
		window.ExpiresAt.After(authority.InvocationDeadline) || window.ExpiresAt.After(source.ExpiresAt) {
		return RegisteredAllocationV1{}, registeredPreparationError("allocation.builder", "requires original owned authority, parent and prepared observation")
	}
	pd, _ := parent.DigestForAuthority(authority)
	ad, _ := prepared.DigestForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend)
	sd, _ := source.Digest()
	v := RegisteredAllocationV1{Version: 1, Scope: source.Scope, Lease: authority.Lease, ParentReservation: parent.Clone(), ParentReservationDigest: pd,
		PreparedInvocationDigest: preparedDigest, PreparedAllocationDigest: ad, Backend: authority.HarnessBinding.Backend, Substrate: authority.SubstrateBinding,
		PreparedInvocationExpiresAt: preparedExpiresAt, InvocationDeadline: authority.InvocationDeadline,
		ObservedImageDigest: prepared.ObservedImageDigest, ObservedOuterHarnessDigest: prepared.ObservedOuterHarnessDigest,
		ObservedProxyArtifactDigest: prepared.ObservedProxyArtifactDigest, ObservedProxyIdentityDigest: prepared.ObservedProxyIdentityDigest, WorkloadMode: prepared.WorkloadMode,
		SourceDigest: sd, Profile: source.Profile, SourceIssuedAt: source.IssuedAt, SourceExpiresAt: source.ExpiresAt,
		AllocationID: allocationID, AllocationGeneration: allocationGeneration, State: RegisteredPreparationActiveV1, RevocationGeneration: 1, Window: window, UpdatedAt: window.PublishedAt}
	for i, s := range source.Spending {
		d, _ := s.Digest()
		v.Spending[i] = RegisteredSpendingReferenceV1{Locator: s.Locator, RevocationGeneration: s.RevocationGeneration, Digest: d, Window: s.Window.Clone()}
	}
	if _, err := EncodeRegisteredAllocationV1(v); err != nil {
		return RegisteredAllocationV1{}, err
	}
	return v.Clone(), nil
}

func (v RegisteredAllocationV1) ValidateForAuthority(authority ServerlessInvocationAuthorityV1, parent AttemptEffectReservationV1, prepared PreparedAllocationV1, preparedDigest PreparedInvocationDigestV1, preparedExpiresAt time.Time, source AttemptExecutionSourceV1) error {
	if err := v.Validate(); err != nil {
		return err
	}
	expected, err := BuildRegisteredAllocationV1(authority, parent, prepared, preparedDigest, preparedExpiresAt, source, v.AllocationID, v.AllocationGeneration, v.Window)
	if err != nil {
		return err
	}
	expected.State, expected.RevocationGeneration, expected.UpdatedAt = v.State, v.RevocationGeneration, v.UpdatedAt
	left, _ := EncodeRegisteredAllocationV1(expected)
	right, _ := EncodeRegisteredAllocationV1(v)
	if !bytes.Equal(left, right) {
		return registeredPreparationError("allocation.correlation", "requires every exact original authority and prepared field")
	}
	return nil
}

func (v RegisteredAllocationV1) ValidateInitial() error {
	if err := v.Validate(); err != nil {
		return err
	}
	return validateRegisteredInitial(v.State, v.RevocationGeneration, v.Window, v.UpdatedAt)
}
func (v RegisteredAllocationV1) ValidateAt(now time.Time) error {
	if err := v.Validate(); err != nil {
		return err
	}
	return validateRegisteredAt(v.State, v.Window, v.UpdatedAt, now)
}
func (v RegisteredAllocationV1) ValidateTransitionFrom(old RegisteredAllocationV1) error {
	if old.Validate() != nil || v.Validate() != nil || validateRegisteredTransition(old.State, v.State, old.RevocationGeneration, v.RevocationGeneration, old.UpdatedAt, v.UpdatedAt) != nil {
		return registeredPreparationError("allocation.transition", "requires monotonic retirement or revocation")
	}
	expected := old.Clone()
	expected.State, expected.RevocationGeneration, expected.UpdatedAt = v.State, v.RevocationGeneration, v.UpdatedAt
	left, _ := EncodeRegisteredAllocationV1(expected)
	right, _ := EncodeRegisteredAllocationV1(v)
	if !bytes.Equal(left, right) {
		return registeredPreparationError("allocation.transition", "cannot replace original preparation or widen authority")
	}
	return nil
}
func (v RegisteredAllocationV1) Digest() (RegisteredAllocationDigestV1, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	d, err := ownedCurrentDigest("sessionless.registered-allocation.v1", v.Clone())
	return RegisteredAllocationDigestV1(d), err
}
func (v RegisteredAllocationDigestV1) Validate() error {
	return validateSHA256("registered_allocation_digest", string(v))
}
func EncodeRegisteredAllocationV1(v RegisteredAllocationV1) ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return encodeOwnedCurrent(v.Clone())
}
func DecodeRegisteredAllocationV1(raw []byte) (RegisteredAllocationV1, error) {
	var v RegisteredAllocationV1
	if err := decodeOwnedCurrent(raw, &v); err != nil {
		return RegisteredAllocationV1{}, err
	}
	expected, err := EncodeRegisteredAllocationV1(v)
	if err != nil || !bytes.Equal(raw, expected) {
		return RegisteredAllocationV1{}, registeredPreparationError("allocation.codec", "requires exact bounded canonical producer bytes")
	}
	return v.Clone(), nil
}

type RegisteredProxyServiceKindV1 string

const (
	RegisteredProxyProviderV1  RegisteredProxyServiceKindV1 = "provider"
	RegisteredProxyMCPToolV1   RegisteredProxyServiceKindV1 = "mcp_tool"
	RegisteredProxyWebSearchV1 RegisteredProxyServiceKindV1 = "web_search"
)

// The actual trusted proxy bridge must verify the immutable policy/attestation
// bytes and existing fixed-endpoint enforcement BEFORE publication. Body refs
// and digests are not network policy approval or proof of DNS/TLS enforcement.
// No URL/endpoint selector or invented parallel endpoint validator lives here.
// In particular, the MCP/search producer bridge remains unimplemented.
type RegisteredProxyAttestationV1 struct {
	Version                    uint32                        `json:"version"`
	Scope                      AttemptExecutionSourceScopeV1 `json:"scope"`
	PhysicalInvocationClaimID  string                        `json:"physical_invocation_claim_id"`
	AllocationID               string                        `json:"allocation_id"`
	AllocationGeneration       uint64                        `json:"allocation_generation"`
	RegisteredAllocationDigest RegisteredAllocationDigestV1  `json:"registered_allocation_digest"`
	SourceDigest               string                        `json:"source_digest"`
	ServiceKind                RegisteredProxyServiceKindV1  `json:"service_kind"`
	TransportDigest            string                        `json:"transport_digest"`
	GrantDigest                string                        `json:"grant_digest"`
	PolicyDigest               string                        `json:"policy_digest"`
	ResolutionSetDigest        string                        `json:"resolution_set_digest"`
	ProxyArtifactDigest        string                        `json:"proxy_artifact_digest"`
	ProxyIdentityDigest        string                        `json:"proxy_identity_digest"`
	PolicyBody                 OwnedAgentAuthorityBodyRefV1  `json:"policy_body"`
	AttestationBody            OwnedAgentAuthorityBodyRefV1  `json:"attestation_body"`
	State                      RegisteredPreparationStateV1  `json:"state"`
	RevocationGeneration       uint64                        `json:"revocation_generation"`
	Window                     OwnedAgentAuthorityWindowV1   `json:"window"`
	UpdatedAt                  time.Time                     `json:"updated_at"`
}

func (v RegisteredProxyAttestationV1) Clone() RegisteredProxyAttestationV1 {
	v.Window, v.UpdatedAt = v.Window.Clone(), v.UpdatedAt.UTC()
	return v
}
func (v RegisteredProxyAttestationV1) Validate() error {
	if v.Version != 1 || v.Scope.Validate() != nil || ValidateOpaqueID("registered_proxy.claim", v.PhysicalInvocationClaimID) != nil ||
		ValidateOpaqueID("registered_proxy.allocation_id", v.AllocationID) != nil || v.AllocationGeneration == 0 || v.RegisteredAllocationDigest.Validate() != nil ||
		v.PolicyBody.Validate() != nil || v.AttestationBody.Validate() != nil || validateRegisteredLifecycle(v.State, v.RevocationGeneration, v.Window, v.UpdatedAt) != nil {
		return registeredPreparationError("proxy", "requires exact original allocation and bounded observation")
	}
	switch v.ServiceKind {
	case RegisteredProxyProviderV1, RegisteredProxyMCPToolV1, RegisteredProxyWebSearchV1:
	default:
		return registeredPreparationError("proxy.service_kind", "is unsupported")
	}
	for _, d := range []string{v.SourceDigest, v.TransportDigest, v.GrantDigest, v.PolicyDigest, v.ResolutionSetDigest, v.ProxyArtifactDigest, v.ProxyIdentityDigest} {
		if err := ValidateSHA256Digest("registered_proxy.digest", d); err != nil {
			return err
		}
	}
	return nil
}

func BuildRegisteredProxyAttestationV1(allocation RegisteredAllocationV1, source AttemptExecutionSourceV1, service RegisteredProxyServiceKindV1, transportDigest, policyDigest, resolutionSetDigest string, policyBody, attestationBody OwnedAgentAuthorityBodyRefV1, window OwnedAgentAuthorityWindowV1) (RegisteredProxyAttestationV1, error) {
	sd, err := source.Digest()
	if err != nil || allocation.ValidateAt(window.PublishedAt) != nil || window.Validate() != nil || allocation.SourceDigest != sd || allocation.Scope != source.Scope ||
		window.ObservedAt.Before(allocation.ParentReservation.ReservedAt) || window.ObservedAt.Before(allocation.Window.ObservedAt) || window.ExpiresAt.After(allocation.Window.ExpiresAt) {
		return RegisteredProxyAttestationV1{}, registeredPreparationError("proxy.builder", "requires the active exact original allocation and contained observation")
	}
	i := -1
	switch service {
	case RegisteredProxyProviderV1:
		i = 0
	case RegisteredProxyMCPToolV1:
		i = 2
	case RegisteredProxyWebSearchV1:
		i = 3
	}
	if i < 0 {
		return RegisteredProxyAttestationV1{}, registeredPreparationError("proxy.service_kind", "is unsupported")
	}
	if service == RegisteredProxyProviderV1 && policyDigest != allocation.Substrate.EgressPolicyDigest {
		return RegisteredProxyAttestationV1{}, registeredPreparationError("proxy.policy", "requires the existing sealed provider egress policy")
	}
	d, _ := allocation.Digest()
	v := RegisteredProxyAttestationV1{Version: 1, Scope: allocation.Scope, PhysicalInvocationClaimID: allocation.ParentReservation.PhysicalInvocationClaimID,
		AllocationID: allocation.AllocationID, AllocationGeneration: allocation.AllocationGeneration, RegisteredAllocationDigest: d, SourceDigest: sd,
		ServiceKind: service, TransportDigest: transportDigest, GrantDigest: source.Spending[i].ScopedGrantDigest,
		PolicyDigest: policyDigest, ResolutionSetDigest: resolutionSetDigest, ProxyArtifactDigest: allocation.ObservedProxyArtifactDigest, ProxyIdentityDigest: allocation.ObservedProxyIdentityDigest,
		PolicyBody: policyBody, AttestationBody: attestationBody, State: RegisteredPreparationActiveV1, RevocationGeneration: 1, Window: window, UpdatedAt: window.PublishedAt}
	if _, err := EncodeRegisteredProxyAttestationV1(v); err != nil {
		return RegisteredProxyAttestationV1{}, err
	}
	return v.Clone(), nil
}

// Historical correlation takes the exact retained original active allocation
// snapshot whose digest this observation sealed, not a later retired/revoked
// head. It is not a current permission check: fresh execution separately needs
// active unchanged allocation/proxy rows and the complete same-Tx authority.
// Never normalize a current tombstone into an invented historical snapshot.
func (v RegisteredProxyAttestationV1) ValidateForAllocation(allocation RegisteredAllocationV1, source AttemptExecutionSourceV1) error {
	if err := v.Validate(); err != nil {
		return err
	}
	expected, err := BuildRegisteredProxyAttestationV1(allocation, source, v.ServiceKind, v.TransportDigest, v.PolicyDigest, v.ResolutionSetDigest, v.PolicyBody, v.AttestationBody, v.Window)
	if err != nil {
		return err
	}
	expected.State, expected.RevocationGeneration, expected.UpdatedAt = v.State, v.RevocationGeneration, v.UpdatedAt
	left, _ := EncodeRegisteredProxyAttestationV1(expected)
	right, _ := EncodeRegisteredProxyAttestationV1(v)
	if !bytes.Equal(left, right) {
		return registeredPreparationError("proxy.correlation", "requires exact allocation, source and scoped service grant")
	}
	return nil
}
func (v RegisteredProxyAttestationV1) ValidateInitial() error {
	if err := v.Validate(); err != nil {
		return err
	}
	return validateRegisteredInitial(v.State, v.RevocationGeneration, v.Window, v.UpdatedAt)
}
func (v RegisteredProxyAttestationV1) ValidateAt(now time.Time) error {
	if err := v.Validate(); err != nil {
		return err
	}
	return validateRegisteredAt(v.State, v.Window, v.UpdatedAt, now)
}
func (v RegisteredProxyAttestationV1) ValidateTransitionFrom(old RegisteredProxyAttestationV1) error {
	if old.Validate() != nil || v.Validate() != nil || validateRegisteredTransition(old.State, v.State, old.RevocationGeneration, v.RevocationGeneration, old.UpdatedAt, v.UpdatedAt) != nil {
		return registeredPreparationError("proxy.transition", "requires monotonic retirement or revocation")
	}
	expected := old.Clone()
	expected.State, expected.RevocationGeneration, expected.UpdatedAt = v.State, v.RevocationGeneration, v.UpdatedAt
	left, _ := EncodeRegisteredProxyAttestationV1(expected)
	right, _ := EncodeRegisteredProxyAttestationV1(v)
	if !bytes.Equal(left, right) {
		return registeredPreparationError("proxy.transition", "cannot replace the observed endpoint body or widen authority")
	}
	return nil
}
func (v RegisteredProxyAttestationV1) Digest() (RegisteredProxyAttestationDigestV1, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	d, err := ownedCurrentDigest("sessionless.registered-proxy-attestation.v1", v.Clone())
	return RegisteredProxyAttestationDigestV1(d), err
}
func (v RegisteredProxyAttestationDigestV1) Validate() error {
	return validateSHA256("registered_proxy_attestation_digest", string(v))
}
func EncodeRegisteredProxyAttestationV1(v RegisteredProxyAttestationV1) ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return encodeOwnedCurrent(v.Clone())
}
func DecodeRegisteredProxyAttestationV1(raw []byte) (RegisteredProxyAttestationV1, error) {
	var v RegisteredProxyAttestationV1
	if err := decodeOwnedCurrent(raw, &v); err != nil {
		return RegisteredProxyAttestationV1{}, err
	}
	expected, err := EncodeRegisteredProxyAttestationV1(v)
	if err != nil || !bytes.Equal(raw, expected) {
		return RegisteredProxyAttestationV1{}, registeredPreparationError("proxy.codec", "requires exact bounded canonical producer bytes")
	}
	return v.Clone(), nil
}

func validateRegisteredLifecycle(state RegisteredPreparationStateV1, generation uint64, window OwnedAgentAuthorityWindowV1, updated time.Time) error {
	if (state != RegisteredPreparationActiveV1 && state != RegisteredPreparationRetiredV1 && state != RegisteredPreparationRevokedV1) || generation == 0 || window.Validate() != nil || !ownedCurrentTime(updated) || updated.Before(window.PublishedAt) ||
		(state == RegisteredPreparationActiveV1 && (generation != 1 || !updated.Equal(window.PublishedAt))) || (state != RegisteredPreparationActiveV1 && generation < 2) {
		return registeredPreparationError("lifecycle", "requires canonical nonrenewable state and window")
	}
	return nil
}
func validateRegisteredInitial(state RegisteredPreparationStateV1, generation uint64, window OwnedAgentAuthorityWindowV1, updated time.Time) error {
	if state != RegisteredPreparationActiveV1 || generation != 1 || !updated.Equal(window.PublishedAt) {
		return registeredPreparationError("initial", "requires original active generation one")
	}
	return nil
}
func validateRegisteredAt(state RegisteredPreparationStateV1, window OwnedAgentAuthorityWindowV1, updated, now time.Time) error {
	if state != RegisteredPreparationActiveV1 || window.ValidateAt(now) != nil || updated.After(now) {
		return registeredPreparationError("current", "requires active original observation window")
	}
	return nil
}
func validateRegisteredTransition(old, next RegisteredPreparationStateV1, oldGen, nextGen uint64, oldAt, nextAt time.Time) error {
	if oldGen == math.MaxUint64 || nextGen != oldGen+1 || nextAt.Before(oldAt) || next == RegisteredPreparationActiveV1 || old == RegisteredPreparationRevokedV1 ||
		(old == RegisteredPreparationRetiredV1 && next != RegisteredPreparationRevokedV1) {
		return registeredPreparationError("transition", "cannot renew or resurrect original observation")
	}
	return nil
}
func registeredPreparationError(field, reason string) error {
	return ValidationError{Field: "registered_preparation." + field, Reason: reason}
}
