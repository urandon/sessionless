package ports

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// OwnedAllocationPublicationV1 is an authenticated observation from the actual
// private preparation producer, NOT a prepared execution capability. There is
// deliberately no public signer, endpoint selector or source-approval boolean.
// Store independently requires the exact confirmed burn, retained Final/source
// and (for first publication) all current canonical authority in the same Tx.
type OwnedAllocationPublicationV1 struct {
	Authority                   domain.ServerlessInvocationAuthorityV1
	Reservation                 domain.AttemptEffectReservationV1
	Allocation                  domain.PreparedAllocationV1
	PreparedInvocationDigest    string
	PreparedInvocationExpiresAt time.Time
	AllocationID                string
	AllocationGeneration        uint64
	ObservedAt                  time.Time
	Authenticator               []byte
}

func (p OwnedAllocationPublicationV1) Clone() OwnedAllocationPublicationV1 {
	p.Authority, p.Reservation, p.Allocation = p.Authority.Clone(), p.Reservation.Clone(), p.Allocation.Clone()
	p.ObservedAt, p.PreparedInvocationExpiresAt = p.ObservedAt.UTC(), p.PreparedInvocationExpiresAt.UTC()
	p.Authenticator = append([]byte(nil), p.Authenticator...)
	return p
}

func (p OwnedAllocationPublicationV1) Validate() error {
	if len(p.Authenticator) != sha256.Size || p.Authority.HarnessBinding.Profile != domain.HarnessExecutionOwnedAgentV2 ||
		p.Authority.HarnessBinding.Backend.BackendKind != domain.HarnessBackendDeterministicFixtureV1 ||
		p.Authority.SubstrateBinding.Kind != domain.SubstrateDeterministicFixtureV1 ||
		p.Authority.SubstrateBinding.WorkloadMode != domain.SubstrateWorkloadInProcessDirectV1 ||
		p.Authority.HarnessBinding.Resource.Kind != domain.ProviderResourceCredentiallessFixtureV1 ||
		p.Authority.HarnessBinding.Resource.CredentialMode != domain.ProviderCredentialNoneV1 ||
		p.Authority.HarnessBinding.Resource.CredentialGeneration != 0 ||
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
func (p OwnedAllocationPublicationV1) IntentBytes() ([]byte, error) {
	if p.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	a, _ := p.Authority.Digest()
	r, _ := p.Reservation.DigestForAuthority(p.Authority)
	b, _ := p.Allocation.DigestForBinding(p.Authority.SubstrateBinding, p.Authority.HarnessBinding.Backend)
	return json.Marshal(ownedAllocationIntentV1{"sessionless.owned-allocation-publication.v1", a, r, b, p.PreparedInvocationDigest,
		p.PreparedInvocationExpiresAt.UTC(), p.AllocationID, p.AllocationGeneration, p.ObservedAt.UTC()})
}

type ownedAllocationIntentV1 struct {
	Kind                        string
	Authority                   domain.ServerlessInvocationAuthorityDigestV1
	Reservation                 domain.AttemptEffectReservationDigestV1
	Allocation                  domain.PreparedAllocationDigestV1
	PreparedInvocationDigest    string
	PreparedInvocationExpiresAt time.Time
	AllocationID                string
	AllocationGeneration        uint64
	ObservedAt                  time.Time
}

// Reconstructs ONLY the original observation's canonical correlation digest,
// using the SAME intent codec. It cannot authenticate a producer or authorize a
// new publication. Store validates original retained parent/source separately.
func OwnedAllocationRegistrationIntentDigestV1(original domain.RegisteredAllocationV1) (string, error) {
	if original.ValidateInitial() != nil {
		return "", domain.ErrMembershipDenied
	}
	raw, err := json.Marshal(ownedAllocationIntentV1{"sessionless.owned-allocation-publication.v1",
		original.ParentReservation.InvocationAuthorityDigest, original.ParentReservationDigest, original.PreparedAllocationDigest,
		string(original.PreparedInvocationDigest), original.PreparedInvocationExpiresAt.UTC(), original.AllocationID,
		original.AllocationGeneration, original.Window.ObservedAt.UTC()})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (p OwnedAllocationPublicationV1) IntentDigest() (string, error) {
	raw, err := p.IntentBytes()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (OwnedAllocationPublicationV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("process-local allocation observation cannot be serialized")
}
func (*OwnedAllocationPublicationV1) UnmarshalJSON([]byte) error {
	return errors.New("process-local allocation observation cannot be deserialized")
}

// Bounded local verification only; installed by private trusted composition.
// Historical authenticity is not current permission. First publication/current
// readback still requires Store current joins; observation never authorizes IO.
type OwnedAllocationPublicationVerifierV1 interface {
	VerifyOwnedAllocationPublication(OwnedAllocationPublicationV1, time.Time) error
}

type OwnedAllocationRegistrationReceiptV1 struct {
	Original domain.RegisteredAllocationV1
	Current  domain.RegisteredAllocationV1
	Digest   string // exact current head, not the retained original active snapshot
	Replayed bool
}
