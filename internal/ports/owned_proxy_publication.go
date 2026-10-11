package ports

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"gitcode.com/urandon/sessionless/internal/domain"
	"time"
)

// Original concrete producer observation only, never a send capability.
// Store independently authenticates the retained parent/source/allocation and
// current authority. Structural fields or caller Facts methods prove none of
// that; only the privately installed original producer authenticates issuance.
type OwnedProxyPublicationV1 struct {
	OperationSequence          uint64
	ServiceKind                domain.RegisteredProxyServiceKindV1
	Scope                      domain.AttemptExecutionSourceScopeV1
	LeaseID                    domain.LeaseID
	FenceToken                 uint64
	PhysicalInvocationClaimID  string
	RegisteredAllocationDigest domain.RegisteredAllocationDigestV1
	SourceDigest               string
	ObservedAt                 time.Time
	PolicyBody                 []byte
	AttestationBody            []byte
	Authenticator              []byte
}

func (p OwnedProxyPublicationV1) Clone() OwnedProxyPublicationV1 {
	p.ObservedAt = p.ObservedAt.UTC()
	p.PolicyBody, p.AttestationBody, p.Authenticator = bytes.Clone(p.PolicyBody), bytes.Clone(p.AttestationBody), bytes.Clone(p.Authenticator)
	return p
}
func (p OwnedProxyPublicationV1) Bodies() (ServerlessEgressPolicyV1, ServerlessEgressProxyAttestationV1, error) {
	var policy ServerlessEgressPolicyV1
	var attestation ServerlessEgressProxyAttestationV1
	if len(p.PolicyBody) == 0 || len(p.PolicyBody) > 16<<10 || len(p.AttestationBody) == 0 || len(p.AttestationBody) > 16<<10 || json.Unmarshal(p.PolicyBody, &policy) != nil || json.Unmarshal(p.AttestationBody, &attestation) != nil {
		return policy, attestation, domain.ErrMembershipDenied
	}
	pb, _ := json.Marshal(policy)
	ab, _ := json.Marshal(attestation)
	if !bytes.Equal(pb, p.PolicyBody) || !bytes.Equal(ab, p.AttestationBody) || attestation.ValidateForPolicy(policy, p.ObservedAt, attestation.ExpiresAt) != nil {
		return policy, attestation, domain.ErrMembershipDenied
	}
	return policy, attestation, nil
}
func (p OwnedProxyPublicationV1) Validate() error {
	if p.OperationSequence == 0 || p.OperationSequence > 12 || (p.ServiceKind != domain.RegisteredProxyProviderV1 && p.ServiceKind != domain.RegisteredProxyMCPToolV1 && p.ServiceKind != domain.RegisteredProxyWebSearchV1) {
		return domain.ErrMembershipDenied
	}
	if p.Scope.Validate() != nil || p.LeaseID.Validate() != nil || p.FenceToken == 0 || domain.ValidateOpaqueID("proxy_publication.claim", p.PhysicalInvocationClaimID) != nil || p.RegisteredAllocationDigest.Validate() != nil || domain.ValidateSHA256Digest("proxy_publication.source", p.SourceDigest) != nil || p.ObservedAt.IsZero() || p.ObservedAt.Year() < 1 || p.ObservedAt.Year() > 9999 || p.ObservedAt.Nanosecond()%1000 != 0 || len(p.Authenticator) != sha256.Size {
		return domain.ErrMembershipDenied
	}
	_, _, err := p.Bodies()
	return err
}
func (p OwnedProxyPublicationV1) IntentBytes() ([]byte, error) {
	if p.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	return json.Marshal(struct {
		Kind        string
		Sequence    uint64
		Service     domain.RegisteredProxyServiceKindV1
		Scope       domain.AttemptExecutionSourceScopeV1
		LeaseID     domain.LeaseID
		Fence       uint64
		Claim       string
		Allocation  domain.RegisteredAllocationDigestV1
		Source      string
		ObservedAt  time.Time
		Policy      []byte
		Attestation []byte
	}{"sessionless.actual-owned-proxy-observation.v1", p.OperationSequence, p.ServiceKind, p.Scope, p.LeaseID, p.FenceToken, p.PhysicalInvocationClaimID, p.RegisteredAllocationDigest, p.SourceDigest, p.ObservedAt.UTC(), p.PolicyBody, p.AttestationBody})
}
func OwnedProxyBodyReferenceV1(kind string, body []byte) domain.OwnedAgentAuthorityBodyRefV1 {
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	return domain.OwnedAgentAuthorityBodyRefV1{Ref: "owned-proxy-" + kind + "-" + digest, Digest: digest, Bytes: uint64(len(body))}
}

type OwnedProxyPublicationVerifierV1 interface {
	VerifyOwnedProxyPublication(OwnedProxyPublicationV1, time.Time) error
}

func (OwnedProxyPublicationV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("process-local proxy observation cannot be serialized")
}
func (*OwnedProxyPublicationV1) UnmarshalJSON([]byte) error {
	return errors.New("process-local proxy observation cannot be deserialized")
}
