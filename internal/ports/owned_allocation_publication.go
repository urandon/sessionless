package ports

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// OwnedAllocationPublicationV2 is an authenticated observation from the actual
// private preparation producer, NOT a prepared execution capability. There is
// deliberately no public signer, endpoint selector or source-approval boolean.
// Store independently requires the exact confirmed burn, retained Final/source
// and (for first publication) all current canonical authority in the same Tx.
type OwnedAllocationPublicationV2 struct {
	Authority                   domain.ServerlessInvocationAuthorityV1
	Reservation                 domain.AttemptEffectReservationV1
	Allocation                  domain.PreparedAllocationV2
	PreparedInvocationDigest    string
	PreparedInvocationExpiresAt time.Time
	AllocationID                string
	AllocationGeneration        uint64
	ObservedAt                  time.Time
	Authenticator               []byte
}

func (p OwnedAllocationPublicationV2) Clone() OwnedAllocationPublicationV2 {
	p.Authority, p.Reservation, p.Allocation = p.Authority.Clone(), p.Reservation.Clone(), p.Allocation.Clone()
	p.ObservedAt, p.PreparedInvocationExpiresAt = p.ObservedAt.UTC(), p.PreparedInvocationExpiresAt.UTC()
	p.Authenticator = append([]byte(nil), p.Authenticator...)
	return p
}

func (p OwnedAllocationPublicationV2) Validate() error {
	b, s, r := p.Authority.HarnessBinding.Backend, p.Authority.SubstrateBinding, p.Authority.HarnessBinding.Resource
	fixture := b.BackendKind == domain.HarnessBackendDeterministicFixtureV1 && s.Kind == domain.SubstrateDeterministicFixtureV1 &&
		r.Kind == domain.ProviderResourceCredentiallessFixtureV1 && r.CredentialMode == domain.ProviderCredentialNoneV1 && r.CredentialGeneration == 0 && p.Allocation.LocalRuntime == nil
	local := b.BackendKind == domain.HarnessBackendDirectOpenAICompatibleV1 && b.ProviderContractKind == domain.ProviderContractConfiguredEndpointV1 &&
		s.Kind == domain.SubstrateLocalDevelopmentV1 && r.Kind == domain.ProviderResourceLocalEndpointV1 && p.Allocation.LocalRuntime != nil
	if len(p.Authenticator) != sha256.Size || (!fixture && !local) || p.Authority.HarnessBinding.Profile != domain.HarnessExecutionOwnedAgentV2 ||
		s.WorkloadMode != domain.SubstrateWorkloadInProcessDirectV1 ||
		p.Authority.ValidateAt(p.ObservedAt) != nil || p.Reservation.ValidateForAuthority(p.Authority) != nil ||
		p.Allocation.ValidateForBinding(p.Authority.SubstrateBinding, p.Authority.HarnessBinding.Backend) != nil ||
		domain.ValidateSHA256Digest("allocation_publication.prepared_digest", p.PreparedInvocationDigest) != nil ||
		domain.ValidateOpaqueID("allocation_publication.allocation_id", p.AllocationID) != nil || p.AllocationGeneration != 1 {
		return domain.ErrMembershipDenied
	}
	for _, at := range []time.Time{p.ObservedAt, p.PreparedInvocationExpiresAt} {
		if at.IsZero() || at.Year() < 1 || at.Year() > 9999 || at.Nanosecond()%1000 != 0 {
			return domain.ErrMembershipDenied
		}
	}
	deadline := p.Reservation.ReservedAt.Add(p.Authority.AdmissionCostCeiling.MaxPreEffectDurationPerDelivery)
	if p.ObservedAt.Before(p.Reservation.ReservedAt) || !p.PreparedInvocationExpiresAt.After(p.ObservedAt) ||
		p.PreparedInvocationExpiresAt.After(p.Authority.InvocationDeadline) || p.PreparedInvocationExpiresAt.After(deadline) {
		return domain.ErrMembershipDenied
	}
	return nil
}

// IntentBytes binds every complete original authority/parent/prepared value and
// original producer identity/time. DB publication time is never signed by a
// caller, and canonical readback does not rebuild this intent with a new clock.
func (p OwnedAllocationPublicationV2) IntentBytes() ([]byte, error) {
	if p.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	a, _ := p.Authority.Digest()
	r, _ := p.Reservation.DigestForAuthority(p.Authority)
	b, _ := p.Allocation.DigestForBinding(p.Authority.SubstrateBinding, p.Authority.HarnessBinding.Backend)
	return json.Marshal(ownedAllocationIntentV2{"sessionless.owned-allocation-publication.v2", a, r, b, p.PreparedInvocationDigest,
		p.PreparedInvocationExpiresAt.UTC(), p.AllocationID, p.AllocationGeneration, p.ObservedAt.UTC()})
}

type ownedAllocationIntentV2 struct {
	Kind                        string
	Authority                   domain.ServerlessInvocationAuthorityDigestV1
	Reservation                 domain.AttemptEffectReservationDigestV1
	Allocation                  domain.PreparedAllocationDigestV2
	PreparedInvocationDigest    string
	PreparedInvocationExpiresAt time.Time
	AllocationID                string
	AllocationGeneration        uint64
	ObservedAt                  time.Time
}

// Reconstructs ONLY the original observation's canonical correlation digest,
// using the SAME intent codec. It cannot authenticate a producer or authorize a
// new publication. Store validates original retained parent/source separately.
func OwnedAllocationRegistrationIntentDigestV2(original domain.RegisteredAllocationV2) (string, error) {
	if original.ValidateInitial() != nil {
		return "", domain.ErrMembershipDenied
	}
	raw, err := json.Marshal(ownedAllocationIntentV2{"sessionless.owned-allocation-publication.v2",
		original.ParentReservation.InvocationAuthorityDigest, original.ParentReservationDigest, original.PreparedAllocationDigest,
		string(original.PreparedInvocationDigest), original.PreparedInvocationExpiresAt.UTC(), original.AllocationID,
		original.AllocationGeneration, original.Window.ObservedAt.UTC()})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (p OwnedAllocationPublicationV2) IntentDigest() (string, error) {
	raw, err := p.IntentBytes()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (OwnedAllocationPublicationV2) MarshalJSON() ([]byte, error) {
	return nil, errors.New("process-local allocation observation cannot be serialized")
}
func (*OwnedAllocationPublicationV2) UnmarshalJSON([]byte) error {
	return errors.New("process-local allocation observation cannot be deserialized")
}

// Bounded local verification only; installed by private trusted composition.
// Historical authenticity is not current permission. First publication/current
// readback still requires Store current joins; observation never authorizes IO.
type OwnedAllocationPublicationVerifierV2 interface {
	VerifyOwnedAllocationPublication(OwnedAllocationPublicationV2, time.Time) error
	VerifyOwnedAllocationPublicationCurrent(OwnedAllocationPublicationV2, time.Time) error
}

type OwnedAllocationRegistrationReceiptV2 struct {
	Original domain.RegisteredAllocationV2
	Current  domain.RegisteredAllocationV2
	Digest   string // exact current head, not the retained original active snapshot
	Replayed bool
}
