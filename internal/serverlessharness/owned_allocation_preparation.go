package serverlessharness

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// BoundedOwnedAllocationPreparationV1 binds the actual linked fixture substrate
// or a concrete authenticated local runtime. It never accepts a caller-supplied prepared token,
// arbitrary observation, proxy attestation, supervisor ID or generic signer.
// It is a private allocation-registration prerequisite, not an owned runtime.
type BoundedOwnedAllocationPreparationV1 struct {
	preparation *BoundedOwnedAttemptPreparationV1
	driver      *InProcessExecutionSubstrateV1
	local       *localExecutionAllocationV1
}

func NewBoundedOwnedAllocationPreparationV1(issuer *CapabilityIssuer, backend domain.HarnessBackendDescriptorV1,
	binding domain.SubstrateBindingV1, driver *InProcessExecutionSubstrateV1, burn OwnedAttemptPreparationBurnV1,
) (*BoundedOwnedAllocationPreparationV1, error) {
	if driver == nil || driver.issuer != issuer || driver.backend != backend {
		return nil, ErrInvalidPreparedInvocation
	}
	preparation, err := NewBoundedOwnedAttemptPreparationV1(issuer, backend, binding, driver, burn)
	if err != nil {
		return nil, err
	}
	return &BoundedOwnedAllocationPreparationV1{preparation: preparation, driver: driver}, nil
}

// Returns observation only. The executable capability remains private and is
// never dispatched. Lost publication ACK must retry/read this SAME intent (or
// use historical DB readback after restart), NEVER call preparation again.
func (bridge *BoundedOwnedAllocationPreparationV1) PrepareAllocationPublication(ctx context.Context, grant ports.AttemptEffectOwnershipGrantV1) (ports.OwnedAllocationPublicationV2, error) {
	if bridge == nil || bridge.preparation == nil || (bridge.driver == nil && bridge.local == nil) {
		return ports.OwnedAllocationPublicationV2{}, ErrInvalidPreparedInvocation
	}
	prepared, err := bridge.preparation.Prepare(ctx, grant.Clone())
	if err != nil {
		return ports.OwnedAllocationPublicationV2{}, err
	}
	var allocationID string
	if bridge.local != nil {
		allocationID, err = bridge.local.identity(prepared.Authority(), prepared.Allocation())
		if err == nil {
			err = bridge.local.retainCapability(prepared)
		}
	} else {
		var observed domain.SubstrateOperationObservationV1
		observed, err = bridge.driver.observation(prepared.Authority(), domain.SubstrateOperationObservedV1)
		allocationID = observed.PhysicalInvocationID
	}
	if err != nil || ctx.Err() != nil || bridge.preparation.issuer.Validate(prepared) != nil {
		if bridge.local != nil {
			bridge.local.close()
		}
		return ports.OwnedAllocationPublicationV2{}, ErrInvalidPreparedInvocation
	}
	p := ports.OwnedAllocationPublicationV2{Authority: prepared.Authority(), Reservation: prepared.Reservation(),
		Allocation: prepared.Allocation(), PreparedInvocationDigest: string(prepared.Digest()), PreparedInvocationExpiresAt: prepared.ExecuteDeadline(),
		AllocationID: allocationID, AllocationGeneration: 1, ObservedAt: prepared.issuedAt,
		Authenticator: make([]byte, sha256.Size)}
	if p.Validate() != nil {
		if bridge.local != nil {
			bridge.local.close()
		}
		return ports.OwnedAllocationPublicationV2{}, ErrInvalidPreparedInvocation
	}
	p.Authenticator, err = bridge.authenticate(p)
	if err != nil {
		if bridge.local != nil {
			bridge.local.close()
		}
		return ports.OwnedAllocationPublicationV2{}, err
	}
	return p.Clone(), nil
}

func (bridge *BoundedOwnedAllocationPreparationV1) authenticate(p ports.OwnedAllocationPublicationV2) ([]byte, error) {
	raw, err := p.IntentBytes()
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha256.New, bridge.preparation.issuer.key[:])
	writeFrame(mac, "sessionless.actual-owned-allocation-observation.v2")
	writeFrame(mac, string(raw))
	return mac.Sum(nil), nil
}

func (bridge *BoundedOwnedAllocationPreparationV1) VerifyOwnedAllocationPublication(p ports.OwnedAllocationPublicationV2, at time.Time) error {
	if bridge == nil || bridge.preparation == nil || (bridge.driver == nil && bridge.local == nil) || p.Validate() != nil ||
		at.IsZero() || at.Year() < 1 || at.Year() > 9999 || at.Nanosecond()%1000 != 0 || at.Before(p.ObservedAt) ||
		p.Authority.SubstrateBinding != bridge.preparation.binding || p.Authority.HarnessBinding.Backend != bridge.preparation.backend {
		return domain.ErrMembershipDenied
	}
	if bridge.local != nil {
		id, err := bridge.local.identity(p.Authority, p.Allocation)
		if err != nil || id != p.AllocationID {
			return domain.ErrMembershipDenied
		}
	} else if p.Allocation.LocalRuntime != nil || p.AllocationID != string(p.Authority.Lease.ID)+"-in-process" {
		return domain.ErrMembershipDenied
	}
	expected, err := bridge.authenticate(p)
	if err != nil || subtle.ConstantTimeCompare(expected, p.Authenticator) != 1 {
		return domain.ErrMembershipDenied
	}
	return nil // authentic retained observation, not fresh execution permission
}

func (bridge *BoundedOwnedAllocationPreparationV1) VerifyOwnedAllocationPublicationCurrent(p ports.OwnedAllocationPublicationV2, at time.Time) error {
	if bridge.VerifyOwnedAllocationPublication(p, at) != nil || !at.Before(p.PreparedInvocationExpiresAt) {
		return domain.ErrMembershipDenied
	}
	if bridge.local != nil {
		return bridge.local.current(p.Authority, at)
	}
	return nil
}

// Close retires only this owned local allocation, never the shared Source,
// worker process, model server or host. Historical MAC verification survives.
func (bridge *BoundedOwnedAllocationPreparationV1) Close() error {
	if bridge != nil && bridge.local != nil {
		bridge.local.close()
	}
	return nil
}

var _ ports.OwnedAllocationPublicationVerifierV2 = (*BoundedOwnedAllocationPreparationV1)(nil)
