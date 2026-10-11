package serverlessegress

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"time"
)

// No arbitrary signer or caller observation enters this producer. Only the
// original object AFTER its actual TLS handshake issues the sealed carrier.
// Observation does not claim that durable setup accounting already happened.
func (p *PinnedHTTPSProxyV1) ObserveOwnedProxy(allocation domain.RegisteredAllocationV1, source domain.AttemptExecutionSourceV1) (ports.OwnedProxyPublicationV1, error) {
	if p == nil || p.self != p {
		return ports.OwnedProxyPublicationV1{}, ErrProxyAttestation
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now, valid := p.sampleLocked()
	ad, ae := allocation.Digest()
	sd, se := source.Digest()
	pd, pe := p.policy.Digest()
	if !valid || p.state != 2 || p.lifetime == nil || p.lifetime.Err() != nil || ae != nil || se != nil || pe != nil || allocation.ValidateAt(now) != nil || allocation.SourceDigest != sd || allocation.Scope != source.Scope || allocation.Substrate.EgressPolicyDigest != pd || allocation.ObservedProxyArtifactDigest != p.policy.ProxyArtifactDigest || allocation.ObservedProxyIdentityDigest != p.policy.ProxyIdentityDigest || p.observedAt.Before(allocation.Window.ObservedAt) || !now.Before(p.attestation.ExpiresAt) {
		return ports.OwnedProxyPublicationV1{}, ErrProxyAttestation
	}
	if p.publicationAllocation != "" && (p.publicationAllocation != ad || p.publicationSource != sd) {
		return ports.OwnedProxyPublicationV1{}, ErrProxyAttestation
	}
	p.publicationAllocation, p.publicationSource = ad, sd
	p.publicationScope, p.publicationLease, p.publicationFence, p.publicationClaim = source.Scope, allocation.Lease.ID, allocation.Lease.FenceToken, allocation.ParentReservation.PhysicalInvocationClaimID
	pb, _ := json.Marshal(p.policy)
	ab, _ := json.Marshal(p.attestation)
	sequence, service := p.operationBinding()
	v := ports.OwnedProxyPublicationV1{OperationSequence: sequence, ServiceKind: service, Scope: source.Scope, LeaseID: p.publicationLease, FenceToken: p.publicationFence, PhysicalInvocationClaimID: p.publicationClaim, RegisteredAllocationDigest: ad, SourceDigest: sd, ObservedAt: p.observedAt.UTC().Truncate(time.Microsecond), PolicyBody: pb, AttestationBody: ab, Authenticator: make([]byte, sha256.Size)}
	raw, err := v.IntentBytes()
	if err != nil {
		return ports.OwnedProxyPublicationV1{}, ErrProxyAttestation
	}
	mac := hmac.New(sha256.New, p.publicationKey[:])
	_, _ = mac.Write(raw)
	v.Authenticator = mac.Sum(nil)
	return v.Clone(), nil
}
func (p *PinnedHTTPSProxyV1) VerifyOwnedProxyPublication(v ports.OwnedProxyPublicationV1, at time.Time) error {
	if p == nil || p.self != p || v.Validate() != nil || at.IsZero() || at.Before(v.ObservedAt) {
		return ErrProxyAttestation
	}
	v = v.Clone()
	p.mu.Lock()
	defer p.mu.Unlock()
	pb, _ := json.Marshal(p.policy)
	ab, _ := json.Marshal(p.attestation)
	sequence, service := p.operationBinding()
	if v.OperationSequence != sequence || v.ServiceKind != service {
		return ErrProxyAttestation
	}
	if p.publicationAllocation != v.RegisteredAllocationDigest || p.publicationSource != v.SourceDigest || p.publicationScope != v.Scope || p.publicationLease != v.LeaseID || p.publicationFence != v.FenceToken || p.publicationClaim != v.PhysicalInvocationClaimID || p.observedAt.UTC().Truncate(time.Microsecond) != v.ObservedAt || subtle.ConstantTimeCompare(pb, v.PolicyBody) != 1 || subtle.ConstantTimeCompare(ab, v.AttestationBody) != 1 {
		return ErrProxyAttestation
	}
	raw, err := v.IntentBytes()
	if err != nil {
		return ErrProxyAttestation
	}
	mac := hmac.New(sha256.New, p.publicationKey[:])
	_, _ = mac.Write(raw)
	if subtle.ConstantTimeCompare(mac.Sum(nil), v.Authenticator) != 1 {
		return ErrProxyAttestation
	}
	return nil
}

var _ ports.OwnedProxyPublicationVerifierV1 = (*PinnedHTTPSProxyV1)(nil)

func (p *PinnedHTTPSProxyV1) operationBinding() (uint64, domain.RegisteredProxyServiceKindV1) {
	if p.deps.fixtureSequence != 0 {
		return p.deps.fixtureSequence, p.deps.fixtureService
	}
	return 1, domain.RegisteredProxyProviderV1
}
func ownedOperationService(kind domain.AttemptOperationKindV1) domain.RegisteredProxyServiceKindV1 {
	switch kind {
	case domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1:
		return domain.RegisteredProxyProviderV1
	case domain.AttemptOperationMCPToolV1:
		return domain.RegisteredProxyMCPToolV1
	case domain.AttemptOperationWebSearchV1:
		return domain.RegisteredProxyWebSearchV1
	}
	return ""
}

// The privately composed Store supplies this exact existing send gate. The
// concrete original connection is burned BEFORE the bounded current SQL join,
// and no payload is written unless that join consumes the authentic capability.
// This checkpoint supports only the tagged credential-free synthetic producer;
// it cannot enable a live route, credential materialization or provider parser.
func (p *PinnedHTTPSProxyV1) InvokeOwnedOperation(ctx context.Context, registered domain.RegisteredProxyAttestationV1, capability ports.AttemptOperationCapabilityV1, gate interface {
	AuthorizeAttemptOperationBeforeSend(context.Context, ports.AttemptOperationCapabilityV1) error
}, payload []byte) (ProxyResultV1, error) {
	denied := ProxyResultV1{Acceptance: domain.ProviderAcceptanceUnknownV1}
	if p == nil || p.self != p || gate == nil || registered.ValidateInitial() != nil || capability.Validate() != nil {
		return denied, ErrEgress
	}
	p.mu.Lock()
	policy, attestation := p.policy, p.attestation
	pb, _ := json.Marshal(policy)
	ab, _ := json.Marshal(attestation)
	pd, _ := policy.Digest()
	rd, _ := registered.Digest()
	op := capability.Reservation.Operation
	sequence, service := p.operationBinding()
	valid := p.deps.fixture && pinnedFixtureRoute(policy) && registered.RegisteredAllocationDigest == p.publicationAllocation && registered.SourceDigest == p.publicationSource && registered.Scope == p.publicationScope && registered.PhysicalInvocationClaimID == p.publicationClaim && registered.PolicyDigest == pd && registered.ResolutionSetDigest == attestation.ResolutionSetDigest && registered.PolicyBody == ports.OwnedProxyBodyReferenceV1("policy", pb) && registered.AttestationBody == ports.OwnedProxyBodyReferenceV1("attestation", ab) && registered.Window.ObservedAt.Equal(p.observedAt.UTC().Truncate(time.Microsecond)) && rd == capability.Reservation.RegisteredProxyDigest && registered.RegisteredAllocationDigest == capability.Reservation.RegisteredAllocationDigest && op.Scope.LeaseID == p.publicationLease && op.Scope.FenceToken == p.publicationFence && op.Scope.PhysicalInvocationClaimID == p.publicationClaim && op.Scope.TenantID == p.publicationScope.TenantID && op.Scope.SessionID == p.publicationScope.SessionID && op.Scope.OwnerUserID == p.publicationScope.OwnerUserID && op.Scope.RunID == p.publicationScope.RunID && op.Scope.AttemptID == p.publicationScope.AttemptID && op.Sequence == sequence && registered.ServiceKind == service && ownedOperationService(op.Kind) == service && uint64(len(payload)) == op.RequestBytes && op.RequestBytes <= policy.Endpoint.MaxRequestBytes && op.ResultBytes <= policy.Endpoint.MaxResponseBytes
	p.mu.Unlock()
	if !valid {
		return denied, ErrEgress
	}
	if ctx == nil {
		return denied, ErrEgress
	}
	ctx, cancel := context.WithDeadline(ctx, op.Deadline)
	defer cancel()
	body := bytes.Clone(payload)
	defer clear(body)
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != op.RequestDigest {
		return denied, ErrEgress
	}
	return p.invoke(ctx, ProxyInvocationV1{Policy: policy, Attestation: attestation, Payload: body, Materialization: ports.ProviderCredentialMaterializationV1{Kind: domain.ProviderCredentialDeliveryNoneV1}}, func(ctx context.Context) error {
		return gate.AuthorizeAttemptOperationBeforeSend(ctx, capability.Clone())
	})
}
