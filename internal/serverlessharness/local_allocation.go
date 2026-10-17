package serverlessharness

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// One actual trusted local execution allocation. It is not a registry, worker
// process launcher, scheduler, transport or source-of-truth. The same issuer
// and confirmed durable preparation burn still precede its physical setup.
type localExecutionAllocationV1 struct {
	mu             sync.Mutex
	self           *localExecutionAllocationV1
	source         *localoperator.Source
	issuer         *CapabilityIssuer
	started        bool
	closed         bool
	id             string
	authority      domain.ServerlessInvocationAuthorityDigestV1
	prepared       domain.PreparedAllocationV2
	capability     PreparedInvocation
	entered        bool
	lifetime       context.Context
	activeDeadline time.Time
	cancel         context.CancelFunc
}

// NewBoundedOwnedLocalAllocationPreparationV1 fixes the concrete authenticated
// installer and linked Go runtime. There is no injected observer, generic
// signer, fixture relabelling, process execution or model/socket probe here.
func NewBoundedOwnedLocalAllocationPreparationV1(issuer *CapabilityIssuer, backend domain.HarnessBackendDescriptorV1, binding domain.SubstrateBindingV1,
	source *localoperator.Source, burn OwnedAttemptPreparationBurnV1,
) (*BoundedOwnedAllocationPreparationV1, error) {
	if issuer == nil || issuer.clock == nil || source == nil || burn == nil || backend.Validate() != nil || binding.Validate() != nil ||
		backend.BackendKind != domain.HarnessBackendDirectOpenAICompatibleV1 || backend.ProviderContractKind != domain.ProviderContractConfiguredEndpointV1 || binding.Kind != domain.SubstrateLocalDevelopmentV1 {
		return nil, ErrInvalidPreparedInvocation
	}
	c, err := source.Configuration()
	if err != nil {
		return nil, ErrInvalidPreparedInvocation
	}
	digest, err := c.Digest()
	if err != nil || digest != backend.BackendProfileDigest || binding.OuterHarnessArtifactDigest != c.WorkerExecutableDigest || binding.LocalInstallationID != c.LocalInstallationID || binding.LocalInstallationRevision != c.LocalInstallationRevision {
		return nil, ErrInvalidPreparedInvocation
	}
	local := &localExecutionAllocationV1{source: source, issuer: issuer}
	local.self = local
	preparation := &BoundedOwnedAttemptPreparationV1{issuer: issuer, backend: backend, binding: binding, local: local, burn: burn}
	return &BoundedOwnedAllocationPreparationV1{preparation: preparation, local: local}, nil
}

func (l *localExecutionAllocationV1) preflight(setup, invocation context.Context, a domain.ServerlessInvocationAuthorityV1, parent domain.AttemptEffectReservationV1) (domain.PreparedAllocationV2, error) {
	if l == nil || l.self != l || setup == nil || invocation == nil || setup.Err() != nil || invocation.Err() != nil {
		return domain.PreparedAllocationV2{}, ErrInvalidPreparedInvocation
	}
	l.mu.Lock()
	if l.started || l.closed {
		l.mu.Unlock()
		return domain.PreparedAllocationV2{}, ErrInvalidPreparedInvocation
	}
	l.started = true
	l.mu.Unlock()
	// Actual file/kernel inspection occurs only here, after the common
	// preparation path confirmed the durable burn. Never hold this mutex over
	// I/O: transaction-time verifiers must not wait for an observing syscall.
	at := l.issuer.clock().UTC().Truncate(time.Microsecond)
	runtime, err := l.source.InspectExecutionRuntime(setup, a, at)
	if err != nil {
		l.close()
		return domain.PreparedAllocationV2{}, ErrInvalidPreparedInvocation
	}
	binding := a.SubstrateBinding
	connector, err := localoperator.LinkedConnectorIdentity(runtime.WorkerExecutableDigest)
	if err != nil {
		l.close()
		return domain.PreparedAllocationV2{}, ErrInvalidPreparedInvocation
	}
	bd, _ := binding.Digest()
	p := domain.PreparedAllocationV2{Version: domain.PreparedAllocationVersionV2, SubstrateBindingDigest: bd,
		ObservedOuterHarnessDigest: runtime.WorkerExecutableDigest, ObservedProxyArtifactDigest: runtime.WorkerExecutableDigest,
		ObservedProxyIdentityDigest: connector, WorkloadMode: domain.SubstrateWorkloadInProcessDirectV1, LocalRuntime: &runtime}
	// Physical lifetime is the original active window, not the short setup /
	// first-entry capability window. Store further intersects source/spending
	// windows before registration; every operation still requires its own joins.
	deadline := parent.ReservedAt.Add(a.HarnessBinding.AgentLoopPolicy.Limits.MaxActiveDuration)
	for _, limit := range []time.Time{a.InvocationDeadline, a.Lease.ExpiresAt, binding.ProfileEvidenceExpiresAt, a.AdmissionCostCeiling.PriceExpiresAt} {
		if limit.Before(deadline) {
			deadline = limit
		}
	}
	c, err := l.source.Configuration()
	if err != nil || parent.ValidateForAuthority(a) != nil {
		l.close()
		return domain.PreparedAllocationV2{}, ErrInvalidPreparedInvocation
	}
	if c.ExpiresAt.Before(deadline) {
		deadline = c.ExpiresAt
	}
	remaining := deadline.Sub(l.issuer.clock().UTC())
	if setup.Err() != nil || invocation.Err() != nil || remaining <= 0 || p.ValidateForBinding(binding, a.HarnessBinding.Backend) != nil {
		l.close()
		return domain.PreparedAllocationV2{}, ErrInvalidPreparedInvocation
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		l.close()
		return domain.PreparedAllocationV2{}, ErrInvalidPreparedInvocation
	}
	ad, _ := a.Digest()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || setup.Err() != nil || invocation.Err() != nil {
		return domain.PreparedAllocationV2{}, ErrInvalidPreparedInvocation
	}
	l.lifetime, l.cancel = context.WithTimeout(invocation, remaining)
	l.activeDeadline = deadline
	l.id, l.authority, l.prepared = "local_"+hex.EncodeToString(nonce[:]), ad, p.Clone()
	return p.Clone(), nil
}

func (l *localExecutionAllocationV1) retainCapability(p PreparedInvocation) error {
	if l == nil || l.self != l || l.issuer.Validate(p) != nil {
		return ErrInvalidPreparedInvocation
	}
	if _, err := l.identity(p.Authority(), p.Allocation()); err != nil {
		return ErrInvalidPreparedInvocation
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.lifetime == nil || l.lifetime.Err() != nil || !l.capability.issuedAt.IsZero() {
		return ErrInvalidPreparedInvocation
	}
	l.capability = p
	return nil
}

// Enter consumes the ORIGINAL prepared capability once inside its setup
// deadline. It authorizes no network send; private normal composition must
// first confirm current canonical registration and every operation repeats its
// complete current-authority/one-use checks before external bytes.
func (bridge *BoundedOwnedAllocationPreparationV1) EnterOwnedLocalAllocation(ctx context.Context, proof ports.OwnedAllocationPublicationV2) error {
	if bridge == nil || bridge.local == nil || ctx == nil || ctx.Err() != nil || bridge.VerifyOwnedAllocationPublicationCurrent(proof, bridge.preparation.issuer.clock().UTC()) != nil {
		return ErrInvalidPreparedInvocation
	}
	l := bridge.local
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.entered || l.closed || l.lifetime == nil || l.lifetime.Err() != nil || l.issuer.Consume(l.capability) != nil {
		return ErrInvalidPreparedInvocation
	}
	l.entered = true
	if ctx.Err() != nil {
		l.closed = true
		l.cancel()
		return ErrInvalidPreparedInvocation
	}
	return nil
}

// identity authenticates correlation with the actual privately retained
// allocation, including after retirement. A matching declaration/digest alone
// cannot create this original handle or revive its lifetime.
func (l *localExecutionAllocationV1) identity(a domain.ServerlessInvocationAuthorityV1, p domain.PreparedAllocationV2) (string, error) {
	if l == nil || l.self != l {
		return "", domain.ErrMembershipDenied
	}
	ad, ae := a.Digest()
	pd, pe := p.DigestForBinding(a.SubstrateBinding, a.HarnessBinding.Backend)
	l.mu.Lock()
	defer l.mu.Unlock()
	expected, ee := l.prepared.DigestForBinding(a.SubstrateBinding, a.HarnessBinding.Backend)
	if ae != nil || pe != nil || ee != nil || ad != l.authority || pd != expected || l.id == "" {
		return "", domain.ErrMembershipDenied
	}
	return l.id, nil
}

func (l *localExecutionAllocationV1) current(a domain.ServerlessInvocationAuthorityV1, at time.Time) error {
	if l == nil || l.self != l || l.source.ValidateExecutionBinding(a, at) != nil {
		return domain.ErrMembershipDenied
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.lifetime == nil || l.lifetime.Err() != nil || !at.Before(l.activeDeadline) || !at.Before(a.InvocationDeadline) {
		return domain.ErrMembershipDenied
	}
	return nil
}

// VerifyEnteredLocalAllocation authenticates this original live allocation
// after the setup deadline, but cannot authenticate a database record or grant
// send permission. The actual connector must also join the original source,
// current Store registration and fresh per-operation capability. In particular
// this never consumes an expired prepared token or reconstructs an allocation.
func (bridge *BoundedOwnedAllocationPreparationV1) VerifyEnteredLocalAllocation(v domain.RegisteredAllocationV2, at time.Time) error {
	if bridge == nil || bridge.local == nil || v.LocalRuntime == nil || v.ValidateAt(at) != nil {
		return domain.ErrMembershipDenied
	}
	l := bridge.local
	l.mu.Lock()
	p := l.capability
	valid := l.entered && !l.closed && l.lifetime != nil && l.lifetime.Err() == nil && at.Before(l.activeDeadline) && !v.Window.ExpiresAt.After(l.activeDeadline) &&
		v.AllocationID == l.id && v.PreparedInvocationDigest == p.Digest() && v.PreparedAllocationDigest == p.allocationDigest && v.ParentReservationDigest == p.reservationDigest &&
		v.Backend == p.authority.HarnessBinding.Backend && v.Substrate == p.authority.SubstrateBinding
	l.mu.Unlock()
	if !valid || l.source.ValidateExecutionBinding(p.Authority(), at) != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (l *localExecutionAllocationV1) close() {
	if l == nil || l.self != l {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	if l.cancel != nil {
		l.cancel()
	}
}
