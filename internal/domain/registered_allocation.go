package domain

import (
	"bytes"
	"math"
	"time"
)

const RegisteredAllocationVersionV2 uint32 = 2

type RegisteredPreparationStateV1 string

const (
	RegisteredPreparationActiveV1  RegisteredPreparationStateV1 = "active"
	RegisteredPreparationRetiredV1 RegisteredPreparationStateV1 = "retired"
	RegisteredPreparationRevokedV1 RegisteredPreparationStateV1 = "revoked"
)

type RegisteredAllocationDigestV2 string
type RegisteredProxyAttestationDigestV2 string

// References preserve the exact original four heads, without embedding their
// bodies or introducing another spending/source authority.
type RegisteredSpendingReferenceV1 struct {
	Locator              AttemptSpendingAuthorizationLocatorV1 `json:"locator"`
	RevocationGeneration uint64                                `json:"revocation_generation"`
	Digest               string                                `json:"digest"`
	Window               OwnedAgentAuthorityWindowV1           `json:"window"`
}

// RegisteredAllocationV2 is a bounded structural child of the sole original
// outer claim. Authentic supervisor publication, current SQL state, installed
// approval and capability verification remain separate requirements. A valid
// record cannot enable an unsupported backend or authorize another preparation.
// PreparedAllocation is sealed by digest, NOT embedded: its legitimate child
// argv may be 64*4096 bytes and must never be truncated to fit this 32-KiB head.
type RegisteredAllocationV2 struct {
	Version                     uint32                           `json:"version"`
	Scope                       AttemptExecutionSourceScopeV1    `json:"scope"`
	Lease                       Lease                            `json:"lease"`
	ParentReservation           AttemptEffectReservationV1       `json:"parent_reservation"`
	ParentReservationDigest     AttemptEffectReservationDigestV1 `json:"parent_reservation_digest"`
	PreparedInvocationDigest    PreparedInvocationDigestV1       `json:"prepared_invocation_digest"`
	PreparedInvocationExpiresAt time.Time                        `json:"prepared_invocation_expires_at"`
	InvocationDeadline          time.Time                        `json:"invocation_deadline"`
	PreparedAllocationDigest    PreparedAllocationDigestV2       `json:"prepared_allocation_digest"`
	Backend                     HarnessBackendDescriptorV1       `json:"backend"`
	Substrate                   SubstrateBindingV1               `json:"substrate"`
	ObservedImageDigest         string                           `json:"observed_image_digest"`
	ObservedOuterHarnessDigest  string                           `json:"observed_outer_harness_digest"`
	ObservedProxyArtifactDigest string                           `json:"observed_proxy_artifact_digest"`
	ObservedProxyIdentityDigest string                           `json:"observed_proxy_identity_digest"`
	WorkloadMode                SubstrateWorkloadModeV1          `json:"workload_mode"`
	LocalRuntime                *LocalRuntimeAttestationV2       `json:"local_runtime,omitempty"`
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

func (v RegisteredAllocationV2) Clone() RegisteredAllocationV2 {
	v.ParentReservation = v.ParentReservation.Clone()
	if v.LocalRuntime != nil {
		local := v.LocalRuntime.Clone()
		v.LocalRuntime = &local
	}
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

func (v RegisteredAllocationV2) Validate() error {
	bad := func() error {
		return registeredPreparationError("allocation", "requires exact bounded original claim and observation")
	}
	if v.Version != RegisteredAllocationVersionV2 || v.Scope.Validate() != nil || validateServerlessLease(v.Lease) != nil || v.ParentReservation.Validate() != nil ||
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
		!v.PreparedInvocationExpiresAt.After(p.ReservedAt) || !v.Window.PublishedAt.Before(v.PreparedInvocationExpiresAt) ||
		(v.Substrate.Kind != SubstrateLocalDevelopmentV1 && v.Window.ExpiresAt.After(v.PreparedInvocationExpiresAt)) || v.Window.ExpiresAt.After(v.InvocationDeadline) ||
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
	if v.Substrate.Kind == SubstrateLocalDevelopmentV1 {
		if v.LocalRuntime == nil || v.ObservedImageDigest != "" || v.WorkloadMode != SubstrateWorkloadInProcessDirectV1 || v.LocalRuntime.ValidateForBinding(v.Substrate, v.Backend) != nil {
			return bad()
		}
		// Unlike image child argv, every local preparation fact is retained.
		prepared := PreparedAllocationV2{Version: PreparedAllocationVersionV2, SubstrateBindingDigest: sd,
			ObservedOuterHarnessDigest: v.ObservedOuterHarnessDigest, ObservedProxyArtifactDigest: v.ObservedProxyArtifactDigest,
			ObservedProxyIdentityDigest: v.ObservedProxyIdentityDigest, WorkloadMode: v.WorkloadMode, LocalRuntime: v.LocalRuntime}
		digest, err := prepared.DigestForBinding(v.Substrate, v.Backend)
		if err != nil || digest != v.PreparedAllocationDigest {
			return bad()
		}
	} else if v.LocalRuntime != nil {
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

// LocalAllocationActiveDeadlineV2 derives the one original local allocation
// lifetime, not a prepared-capability or first-entry deadline. It uses the
// original parent reservation instant, never registration/retry time. These
// structural correlations do not authenticate the producer, current SQL heads
// or first entry; those retain their separate original once-only setup gates.
func LocalAllocationActiveDeadlineV2(authority ServerlessInvocationAuthorityV1, parent AttemptEffectReservationV1, source AttemptExecutionSourceV1) (time.Time, error) {
	bad := func() (time.Time, error) {
		return time.Time{}, registeredPreparationError("allocation.active_deadline", "requires exact original local authority/source and contained finite active bounds")
	}
	policy := authority.HarnessBinding.AgentLoopPolicy
	cost := authority.AdmissionCostCeiling
	if authority.SubstrateBinding.Kind != SubstrateLocalDevelopmentV1 ||
		authority.HarnessBinding.Backend.BackendKind != HarnessBackendDirectOpenAICompatibleV1 || authority.HarnessBinding.Backend.ProviderContractKind != ProviderContractConfiguredEndpointV1 ||
		authority.HarnessBinding.Resource.Kind != ProviderResourceLocalEndpointV1 || policy == nil || cost.MCPToolCost == nil || cost.WebSearchCost == nil ||
		authority.HarnessBinding.EvidenceExpiresAt == nil || authority.ValidateAt(parent.ReservedAt) != nil || parent.ValidateForAuthority(authority) != nil ||
		source.ValidateForExecution(authority.ExecutionPlacementV2, authority.HarnessBinding, authority.SubstrateBinding, cost) != nil || source.IssuedAt.After(parent.ReservedAt) {
		return bad()
	}
	deadline := parent.ReservedAt.Add(policy.Limits.MaxActiveDuration)
	for _, bound := range []time.Time{authority.InvocationDeadline, authority.Lease.ExpiresAt, source.ExpiresAt, authority.SubstrateBinding.ProfileEvidenceExpiresAt,
		policy.ExpiresAt, *authority.HarnessBinding.EvidenceExpiresAt, cost.PriceExpiresAt, cost.MCPToolCost.PriceExpiresAt, cost.WebSearchCost.PriceExpiresAt} {
		if bound.Before(deadline) {
			deadline = bound
		}
	}
	for _, spending := range source.Spending {
		if spending.ValidateAt(parent.ReservedAt) != nil {
			return bad()
		}
		if spending.Window.ExpiresAt.Before(deadline) {
			deadline = spending.Window.ExpiresAt
		}
	}
	if !ownedCurrentTime(deadline) || !deadline.After(parent.ReservedAt) {
		return bad()
	}
	return deadline.UTC(), nil
}

// The caller must supply the EXACT original authenticated prepared capability's
// digest. A domain type cannot verify its process-local MAC or confirmed commit.
func BuildRegisteredAllocationV2(authority ServerlessInvocationAuthorityV1, parent AttemptEffectReservationV1, prepared PreparedAllocationV2, preparedDigest PreparedInvocationDigestV1, preparedExpiresAt time.Time, source AttemptExecutionSourceV1, allocationID string, allocationGeneration uint64, window OwnedAgentAuthorityWindowV1) (RegisteredAllocationV2, error) {
	if window.Validate() != nil || authority.ValidateAt(window.ObservedAt) != nil || parent.ValidateForAuthority(authority) != nil ||
		prepared.ValidateForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend) != nil || preparedDigest.Validate() != nil ||
		!ownedCurrentTime(preparedExpiresAt) || preparedExpiresAt.After(authority.InvocationDeadline) || !window.PublishedAt.Before(preparedExpiresAt) ||
		source.ValidateForExecution(authority.ExecutionPlacementV2, authority.HarnessBinding, authority.SubstrateBinding, authority.AdmissionCostCeiling) != nil ||
		window.ExpiresAt.After(authority.InvocationDeadline) || window.ExpiresAt.After(source.ExpiresAt) {
		return RegisteredAllocationV2{}, registeredPreparationError("allocation.builder", "requires original owned authority, parent and prepared observation")
	}
	ceiling := preparedExpiresAt
	if authority.SubstrateBinding.Kind == SubstrateLocalDevelopmentV1 {
		var err error
		ceiling, err = LocalAllocationActiveDeadlineV2(authority, parent, source)
		if err != nil {
			return RegisteredAllocationV2{}, err
		}
	}
	if window.ExpiresAt.After(ceiling) {
		return RegisteredAllocationV2{}, registeredPreparationError("allocation.window", "must fit the original active lifetime; image paths retain their original setup cap")
	}
	pd, _ := parent.DigestForAuthority(authority)
	ad, _ := prepared.DigestForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend)
	sd, _ := source.Digest()
	v := RegisteredAllocationV2{Version: RegisteredAllocationVersionV2, Scope: source.Scope, Lease: authority.Lease, ParentReservation: parent.Clone(), ParentReservationDigest: pd,
		PreparedInvocationDigest: preparedDigest, PreparedAllocationDigest: ad, Backend: authority.HarnessBinding.Backend, Substrate: authority.SubstrateBinding,
		PreparedInvocationExpiresAt: preparedExpiresAt, InvocationDeadline: authority.InvocationDeadline,
		ObservedImageDigest: prepared.ObservedImageDigest, ObservedOuterHarnessDigest: prepared.ObservedOuterHarnessDigest,
		ObservedProxyArtifactDigest: prepared.ObservedProxyArtifactDigest, ObservedProxyIdentityDigest: prepared.ObservedProxyIdentityDigest, WorkloadMode: prepared.WorkloadMode,
		LocalRuntime: prepared.LocalRuntime,
		SourceDigest: sd, Profile: source.Profile, SourceIssuedAt: source.IssuedAt, SourceExpiresAt: source.ExpiresAt,
		AllocationID: allocationID, AllocationGeneration: allocationGeneration, State: RegisteredPreparationActiveV1, RevocationGeneration: 1, Window: window, UpdatedAt: window.PublishedAt}
	for i, s := range source.Spending {
		d, _ := s.Digest()
		v.Spending[i] = RegisteredSpendingReferenceV1{Locator: s.Locator, RevocationGeneration: s.RevocationGeneration, Digest: d, Window: s.Window.Clone()}
	}
	if _, err := EncodeRegisteredAllocationV2(v); err != nil {
		return RegisteredAllocationV2{}, err
	}
	return v.Clone(), nil
}

func (v RegisteredAllocationV2) ValidateForAuthority(authority ServerlessInvocationAuthorityV1, parent AttemptEffectReservationV1, prepared PreparedAllocationV2, preparedDigest PreparedInvocationDigestV1, preparedExpiresAt time.Time, source AttemptExecutionSourceV1) error {
	if err := v.Validate(); err != nil {
		return err
	}
	expected, err := BuildRegisteredAllocationV2(authority, parent, prepared, preparedDigest, preparedExpiresAt, source, v.AllocationID, v.AllocationGeneration, v.Window)
	if err != nil {
		return err
	}
	expected.State, expected.RevocationGeneration, expected.UpdatedAt = v.State, v.RevocationGeneration, v.UpdatedAt
	left, _ := EncodeRegisteredAllocationV2(expected)
	right, _ := EncodeRegisteredAllocationV2(v)
	if !bytes.Equal(left, right) {
		return registeredPreparationError("allocation.correlation", "requires every exact original authority and prepared field")
	}
	return nil
}

func (v RegisteredAllocationV2) ValidateInitial() error {
	if err := v.Validate(); err != nil {
		return err
	}
	return validateRegisteredInitial(v.State, v.RevocationGeneration, v.Window, v.UpdatedAt)
}
func (v RegisteredAllocationV2) ValidateAt(now time.Time) error {
	if err := v.Validate(); err != nil {
		return err
	}
	return validateRegisteredAt(v.State, v.Window, v.UpdatedAt, now)
}
func (v RegisteredAllocationV2) ValidateTransitionFrom(old RegisteredAllocationV2) error {
	if old.Validate() != nil || v.Validate() != nil || validateRegisteredTransition(old.State, v.State, old.RevocationGeneration, v.RevocationGeneration, old.UpdatedAt, v.UpdatedAt) != nil {
		return registeredPreparationError("allocation.transition", "requires monotonic retirement or revocation")
	}
	expected := old.Clone()
	expected.State, expected.RevocationGeneration, expected.UpdatedAt = v.State, v.RevocationGeneration, v.UpdatedAt
	left, _ := EncodeRegisteredAllocationV2(expected)
	right, _ := EncodeRegisteredAllocationV2(v)
	if !bytes.Equal(left, right) {
		return registeredPreparationError("allocation.transition", "cannot replace original preparation or widen authority")
	}
	return nil
}
func (v RegisteredAllocationV2) Digest() (RegisteredAllocationDigestV2, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	d, err := ownedCurrentDigest("sessionless.registered-allocation.v2", v.Clone())
	return RegisteredAllocationDigestV2(d), err
}
func (v RegisteredAllocationDigestV2) Validate() error {
	return validateSHA256("registered_allocation_digest", string(v))
}
func EncodeRegisteredAllocationV2(v RegisteredAllocationV2) ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return encodeOwnedCurrent(v.Clone())
}
func DecodeRegisteredAllocationV2(raw []byte) (RegisteredAllocationV2, error) {
	var v RegisteredAllocationV2
	if err := decodeOwnedCurrent(raw, &v); err != nil {
		return RegisteredAllocationV2{}, err
	}
	expected, err := EncodeRegisteredAllocationV2(v)
	if err != nil || !bytes.Equal(raw, expected) {
		return RegisteredAllocationV2{}, registeredPreparationError("allocation.codec", "requires exact bounded canonical producer bytes")
	}
	return v.Clone(), nil
}

type RegisteredProxyServiceKindV1 string

type RegisteredProxyTransportKindV2 string

const (
	RegisteredProxyTransportPinnedHTTPSV2       RegisteredProxyTransportKindV2 = "pinned_https.v1"
	RegisteredProxyTransportLocalLoopbackHTTPV2 RegisteredProxyTransportKindV2 = "local_loopback_http.v1"
)

type RegisteredPinnedHTTPSProxyV2 struct {
	ResolutionSetDigest string `json:"resolution_set_digest"`
	ProxyArtifactDigest string `json:"proxy_artifact_digest"`
	ProxyIdentityDigest string `json:"proxy_identity_digest"`
}

// RegisteredLocalLoopbackProxyV2 describes local observation DATA only. There
// is no DNS/TLS/image attestation here. Structural allocation correlation does
// not authenticate an actual retained-connector producer or grant a send.
type RegisteredLocalLoopbackProxyV2 struct {
	ConfiguredProfileDigest        string `json:"configured_profile_digest"`
	EndpointPolicy                 string `json:"endpoint_policy"`
	ConnectorArtifactDigest        string `json:"connector_artifact_digest"`
	InstallationID                 string `json:"installation_id"`
	InstallationRevision           uint64 `json:"installation_revision"`
	HostIdentityDigest             string `json:"host_identity_digest"`
	NetworkNamespaceIdentityDigest string `json:"network_namespace_identity_digest"`
	LiteralPeerDigest              string `json:"literal_peer_digest"`
}

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
type RegisteredProxyAttestationV2 struct {
	Version                    uint32                          `json:"version"`
	TransportKind              RegisteredProxyTransportKindV2  `json:"transport_kind"`
	OperationSequence          uint64                          `json:"operation_sequence"`
	OperationDigest            AttemptOperationDigestV1        `json:"operation_digest"`
	Scope                      AttemptExecutionSourceScopeV1   `json:"scope"`
	PhysicalInvocationClaimID  string                          `json:"physical_invocation_claim_id"`
	AllocationID               string                          `json:"allocation_id"`
	AllocationGeneration       uint64                          `json:"allocation_generation"`
	RegisteredAllocationDigest RegisteredAllocationDigestV2    `json:"registered_allocation_digest"`
	SourceDigest               string                          `json:"source_digest"`
	ServiceKind                RegisteredProxyServiceKindV1    `json:"service_kind"`
	TransportDigest            string                          `json:"transport_digest"`
	GrantDigest                string                          `json:"grant_digest"`
	PolicyDigest               string                          `json:"policy_digest"`
	PinnedHTTPS                *RegisteredPinnedHTTPSProxyV2   `json:"pinned_https,omitempty"`
	LocalLoopback              *RegisteredLocalLoopbackProxyV2 `json:"local_loopback,omitempty"`
	PolicyBody                 OwnedAgentAuthorityBodyRefV1    `json:"policy_body"`
	AttestationBody            OwnedAgentAuthorityBodyRefV1    `json:"attestation_body"`
	State                      RegisteredPreparationStateV1    `json:"state"`
	RevocationGeneration       uint64                          `json:"revocation_generation"`
	Window                     OwnedAgentAuthorityWindowV1     `json:"window"`
	UpdatedAt                  time.Time                       `json:"updated_at"`
}

func (v RegisteredProxyAttestationV2) Clone() RegisteredProxyAttestationV2 {
	v.Window, v.UpdatedAt = v.Window.Clone(), v.UpdatedAt.UTC()
	if v.PinnedHTTPS != nil {
		value := *v.PinnedHTTPS
		v.PinnedHTTPS = &value
	}
	if v.LocalLoopback != nil {
		value := *v.LocalLoopback
		v.LocalLoopback = &value
	}
	return v
}
func (v RegisteredProxyAttestationV2) Validate() error {
	if v.Version != 2 || v.OperationSequence == 0 || v.OperationSequence > 12 || v.OperationDigest.Validate() != nil || v.Scope.Validate() != nil || ValidateOpaqueID("registered_proxy.claim", v.PhysicalInvocationClaimID) != nil ||
		ValidateOpaqueID("registered_proxy.allocation_id", v.AllocationID) != nil || v.AllocationGeneration == 0 || v.RegisteredAllocationDigest.Validate() != nil ||
		v.PolicyBody.Validate() != nil || v.AttestationBody.Validate() != nil || validateRegisteredLifecycle(v.State, v.RevocationGeneration, v.Window, v.UpdatedAt) != nil {
		return registeredPreparationError("proxy", "requires exact original allocation and bounded observation")
	}
	switch v.ServiceKind {
	case RegisteredProxyProviderV1, RegisteredProxyMCPToolV1, RegisteredProxyWebSearchV1:
	default:
		return registeredPreparationError("proxy.service_kind", "is unsupported")
	}
	for _, d := range []string{v.SourceDigest, v.TransportDigest, v.GrantDigest, v.PolicyDigest} {
		if err := ValidateSHA256Digest("registered_proxy.digest", d); err != nil {
			return err
		}
	}
	switch v.TransportKind {
	case RegisteredProxyTransportPinnedHTTPSV2:
		if v.PinnedHTTPS == nil || v.LocalLoopback != nil {
			return registeredPreparationError("proxy.transport", "pinned HTTPS requires only its explicit HTTPS observations")
		}
		for _, digest := range []string{v.PinnedHTTPS.ResolutionSetDigest, v.PinnedHTTPS.ProxyArtifactDigest, v.PinnedHTTPS.ProxyIdentityDigest} {
			if err := ValidateSHA256Digest("registered_proxy.pinned_https", digest); err != nil {
				return err
			}
		}
	case RegisteredProxyTransportLocalLoopbackHTTPV2:
		p := v.LocalLoopback
		if p == nil || v.PinnedHTTPS != nil || v.ServiceKind != RegisteredProxyProviderV1 || p.EndpointPolicy != ConfiguredInferenceLoopbackPolicyV1 || p.InstallationRevision == 0 || ValidateOpaqueID("registered_proxy.local_installation", p.InstallationID) != nil || v.PolicyBody.Bytes > 16<<10 || v.AttestationBody.Bytes > 16<<10 {
			return registeredPreparationError("proxy.transport", "local loopback requires explicit local provider facts only")
		}
		for _, digest := range []string{p.ConfiguredProfileDigest, p.ConnectorArtifactDigest, p.HostIdentityDigest, p.NetworkNamespaceIdentityDigest, p.LiteralPeerDigest} {
			if err := ValidateSHA256Digest("registered_proxy.local_loopback", digest); err != nil {
				return err
			}
		}
	default:
		return registeredPreparationError("proxy.transport", "requires an explicit supported transport tag")
	}
	return nil
}

func BuildRegisteredPinnedHTTPSProxyAttestationV2(allocation RegisteredAllocationV2, source AttemptExecutionSourceV1, service RegisteredProxyServiceKindV1, transportDigest, policyDigest, resolutionSetDigest string, policyBody, attestationBody OwnedAgentAuthorityBodyRefV1, window OwnedAgentAuthorityWindowV1, sequence uint64, operationDigest AttemptOperationDigestV1) (RegisteredProxyAttestationV2, error) {
	if allocation.Substrate.Kind == SubstrateLocalDevelopmentV1 || allocation.LocalRuntime != nil {
		return RegisteredProxyAttestationV2{}, registeredPreparationError("proxy.local_allocation", "HTTPS publication cannot attest a local runtime; its actual retained loopback connector is required")
	}
	sd, err := source.Digest()
	if err != nil || allocation.ValidateAt(window.PublishedAt) != nil || window.Validate() != nil || allocation.SourceDigest != sd || allocation.Scope != source.Scope ||
		window.ObservedAt.Before(allocation.ParentReservation.ReservedAt) || window.ObservedAt.Before(allocation.Window.ObservedAt) || window.ExpiresAt.After(allocation.Window.ExpiresAt) {
		return RegisteredProxyAttestationV2{}, registeredPreparationError("proxy.builder", "requires the active exact original allocation and contained observation")
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
		return RegisteredProxyAttestationV2{}, registeredPreparationError("proxy.service_kind", "is unsupported")
	}
	if service == RegisteredProxyProviderV1 && policyDigest != allocation.Substrate.EgressPolicyDigest {
		return RegisteredProxyAttestationV2{}, registeredPreparationError("proxy.policy", "requires the existing sealed provider egress policy")
	}
	d, _ := allocation.Digest()
	v := RegisteredProxyAttestationV2{Version: 2, TransportKind: RegisteredProxyTransportPinnedHTTPSV2, OperationSequence: sequence, OperationDigest: operationDigest, Scope: allocation.Scope, PhysicalInvocationClaimID: allocation.ParentReservation.PhysicalInvocationClaimID,
		AllocationID: allocation.AllocationID, AllocationGeneration: allocation.AllocationGeneration, RegisteredAllocationDigest: d, SourceDigest: sd,
		ServiceKind: service, TransportDigest: transportDigest, GrantDigest: source.Spending[i].ScopedGrantDigest,
		PolicyDigest: policyDigest, PinnedHTTPS: &RegisteredPinnedHTTPSProxyV2{ResolutionSetDigest: resolutionSetDigest, ProxyArtifactDigest: allocation.ObservedProxyArtifactDigest, ProxyIdentityDigest: allocation.ObservedProxyIdentityDigest},
		PolicyBody: policyBody, AttestationBody: attestationBody, State: RegisteredPreparationActiveV1, RevocationGeneration: 1, Window: window, UpdatedAt: window.PublishedAt}
	if _, err := EncodeRegisteredProxyAttestationV2(v); err != nil {
		return RegisteredProxyAttestationV2{}, err
	}
	return v.Clone(), nil
}

// BuildRegisteredLocalLoopbackProxyAttestationV2 correlates bounded local
// observation DATA with the original active local allocation and source. The
// actual retained connector/peer authentication and current Store joins remain
// mandatory separately; a successful builder never grants network permission.
func BuildRegisteredLocalLoopbackProxyAttestationV2(allocation RegisteredAllocationV2, source AttemptExecutionSourceV1, transportDigest, policyDigest string, observation RegisteredLocalLoopbackProxyV2, policyBody, attestationBody OwnedAgentAuthorityBodyRefV1, window OwnedAgentAuthorityWindowV1, sequence uint64, operationDigest AttemptOperationDigestV1) (RegisteredProxyAttestationV2, error) {
	deny := func() (RegisteredProxyAttestationV2, error) {
		return RegisteredProxyAttestationV2{}, registeredPreparationError("proxy.local_builder", "requires exact original local runtime, source, policy and contained observation")
	}
	sd, err := source.Digest()
	if err != nil || allocation.ValidateAt(window.PublishedAt) != nil || window.Validate() != nil || allocation.Substrate.Kind != SubstrateLocalDevelopmentV1 || allocation.LocalRuntime == nil ||
		allocation.SourceDigest != sd || allocation.Scope != source.Scope || window.ObservedAt.Before(allocation.ParentReservation.ReservedAt) || window.ObservedAt.Before(allocation.Window.ObservedAt) || window.ObservedAt.Before(source.IssuedAt) || window.ExpiresAt.After(allocation.Window.ExpiresAt) || window.ExpiresAt.After(source.ExpiresAt) || policyDigest != allocation.Substrate.EgressPolicyDigest {
		return deny()
	}
	runtime := allocation.LocalRuntime
	if observation.ConfiguredProfileDigest != allocation.Backend.BackendProfileDigest || observation.ConfiguredProfileDigest != runtime.ConfiguredProfileDigest || observation.EndpointPolicy != ConfiguredInferenceLoopbackPolicyV1 ||
		observation.ConnectorArtifactDigest != allocation.ObservedProxyArtifactDigest || observation.ConnectorArtifactDigest != runtime.WorkerExecutableDigest || observation.InstallationID != runtime.InstallationID || observation.InstallationRevision != runtime.InstallationRevision || observation.HostIdentityDigest != runtime.HostIdentityDigest || observation.NetworkNamespaceIdentityDigest != runtime.NetworkNamespaceIdentityDigest {
		return deny()
	}
	d, err := allocation.Digest()
	if err != nil {
		return deny()
	}
	v := RegisteredProxyAttestationV2{Version: 2, TransportKind: RegisteredProxyTransportLocalLoopbackHTTPV2, OperationSequence: sequence, OperationDigest: operationDigest, Scope: allocation.Scope, PhysicalInvocationClaimID: allocation.ParentReservation.PhysicalInvocationClaimID,
		AllocationID: allocation.AllocationID, AllocationGeneration: allocation.AllocationGeneration, RegisteredAllocationDigest: d, SourceDigest: sd, ServiceKind: RegisteredProxyProviderV1, TransportDigest: transportDigest, GrantDigest: source.Spending[0].ScopedGrantDigest,
		PolicyDigest: policyDigest, LocalLoopback: &observation, PolicyBody: policyBody, AttestationBody: attestationBody, State: RegisteredPreparationActiveV1, RevocationGeneration: 1, Window: window, UpdatedAt: window.PublishedAt}
	if _, err := EncodeRegisteredProxyAttestationV2(v); err != nil {
		return RegisteredProxyAttestationV2{}, err
	}
	return v.Clone(), nil
}

// Historical correlation takes the exact retained original active allocation
// snapshot whose digest this observation sealed, not a later retired/revoked
// head. It is not a current permission check: fresh execution separately needs
// active unchanged allocation/proxy rows and the complete same-Tx authority.
// Never normalize a current tombstone into an invented historical snapshot.
func (v RegisteredProxyAttestationV2) ValidateForAllocation(allocation RegisteredAllocationV2, source AttemptExecutionSourceV1) error {
	if err := v.Validate(); err != nil {
		return err
	}
	var expected RegisteredProxyAttestationV2
	var err error
	switch v.TransportKind {
	case RegisteredProxyTransportPinnedHTTPSV2:
		expected, err = BuildRegisteredPinnedHTTPSProxyAttestationV2(allocation, source, v.ServiceKind, v.TransportDigest, v.PolicyDigest, v.PinnedHTTPS.ResolutionSetDigest, v.PolicyBody, v.AttestationBody, v.Window, v.OperationSequence, v.OperationDigest)
	case RegisteredProxyTransportLocalLoopbackHTTPV2:
		expected, err = BuildRegisteredLocalLoopbackProxyAttestationV2(allocation, source, v.TransportDigest, v.PolicyDigest, *v.LocalLoopback, v.PolicyBody, v.AttestationBody, v.Window, v.OperationSequence, v.OperationDigest)
	default:
		return registeredPreparationError("proxy.transport", "requires an explicit supported transport tag")
	}
	if err != nil {
		return err
	}
	expected.State, expected.RevocationGeneration, expected.UpdatedAt = v.State, v.RevocationGeneration, v.UpdatedAt
	left, _ := EncodeRegisteredProxyAttestationV2(expected)
	right, _ := EncodeRegisteredProxyAttestationV2(v)
	if !bytes.Equal(left, right) {
		return registeredPreparationError("proxy.correlation", "requires exact allocation, source and scoped service grant")
	}
	return nil
}
func (v RegisteredProxyAttestationV2) ValidateInitial() error {
	if err := v.Validate(); err != nil {
		return err
	}
	return validateRegisteredInitial(v.State, v.RevocationGeneration, v.Window, v.UpdatedAt)
}
func (v RegisteredProxyAttestationV2) ValidateAt(now time.Time) error {
	if err := v.Validate(); err != nil {
		return err
	}
	return validateRegisteredAt(v.State, v.Window, v.UpdatedAt, now)
}
func (v RegisteredProxyAttestationV2) ValidateTransitionFrom(old RegisteredProxyAttestationV2) error {
	if old.Validate() != nil || v.Validate() != nil || validateRegisteredTransition(old.State, v.State, old.RevocationGeneration, v.RevocationGeneration, old.UpdatedAt, v.UpdatedAt) != nil {
		return registeredPreparationError("proxy.transition", "requires monotonic retirement or revocation")
	}
	expected := old.Clone()
	expected.State, expected.RevocationGeneration, expected.UpdatedAt = v.State, v.RevocationGeneration, v.UpdatedAt
	left, _ := EncodeRegisteredProxyAttestationV2(expected)
	right, _ := EncodeRegisteredProxyAttestationV2(v)
	if !bytes.Equal(left, right) {
		return registeredPreparationError("proxy.transition", "cannot replace the observed endpoint body or widen authority")
	}
	return nil
}
func (v RegisteredProxyAttestationV2) Digest() (RegisteredProxyAttestationDigestV2, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	d, err := ownedCurrentDigest("sessionless.registered-proxy-attestation.v2", v.Clone())
	return RegisteredProxyAttestationDigestV2(d), err
}
func (v RegisteredProxyAttestationDigestV2) Validate() error {
	return validateSHA256("registered_proxy_attestation_digest", string(v))
}
func EncodeRegisteredProxyAttestationV2(v RegisteredProxyAttestationV2) ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return encodeOwnedCurrent(v.Clone())
}
func DecodeRegisteredProxyAttestationV2(raw []byte) (RegisteredProxyAttestationV2, error) {
	var v RegisteredProxyAttestationV2
	if err := decodeOwnedCurrent(raw, &v); err != nil {
		return RegisteredProxyAttestationV2{}, err
	}
	expected, err := EncodeRegisteredProxyAttestationV2(v)
	if err != nil || !bytes.Equal(raw, expected) {
		return RegisteredProxyAttestationV2{}, registeredPreparationError("proxy.codec", "requires exact bounded canonical producer bytes")
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
