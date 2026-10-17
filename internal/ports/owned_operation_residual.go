package ports

import (
	"context"
	"encoding/json"
	"gitcode.com/urandon/sessionless/internal/domain"
	"time"
)

type OwnedOperationResidualRowV1 struct {
	Reservation domain.AttemptOperationReservationV1 `json:"reservation"`
	Observation domain.AttemptOperationObservationV1 `json:"observation"`
}
type OwnedOperationResidualProxyV1 struct {
	Sequence uint64                              `json:"sequence"`
	Original domain.RegisteredProxyAttestationV2 `json:"original"`
	Current  domain.RegisteredProxyAttestationV2 `json:"current"`
}
type OwnedOperationResidualBodyV1 struct {
	Ref    string `json:"ref"`
	Digest string `json:"digest"`
	Bytes  uint64 `json:"bytes"`
}

// Complete bounded inventory of the SAME canonical parent, not a new ledger
// or caller statement that cleanup succeeded. Historical bodies are immutable;
// original allocation retirement is necessary but never sufficient for erasure.
type OwnedOperationResidualInventoryV1 struct {
	Version            uint32                            `json:"version"`
	Parent             domain.OwnedAttemptEffectParentV1 `json:"parent"`
	Source             domain.AttemptExecutionSourceV1   `json:"source"`
	OriginalAllocation domain.RegisteredAllocationV1     `json:"original_allocation"`
	RetiredAllocation  domain.RegisteredAllocationV1     `json:"retired_allocation"`
	AccountingDigest   string                            `json:"accounting_digest"`
	AggregateDebit     domain.AttemptOperationDebitV1    `json:"aggregate_debit"`
	ComponentAmounts   [4]uint64                         `json:"component_amounts"`
	Operations         []OwnedOperationResidualRowV1     `json:"operations"`
	Proxies            []OwnedOperationResidualProxyV1   `json:"proxies"`
	Consents           []domain.WebSearchConsentStateV1  `json:"consents"`
	Bodies             []OwnedOperationResidualBodyV1    `json:"bodies"`
}

func (v OwnedOperationResidualInventoryV1) IntentBytes() ([]byte, error) {
	if v.Version != 1 || v.Parent.Validate() != nil || v.Source.Validate() != nil || v.OriginalAllocation.Validate() != nil || v.RetiredAllocation.Validate() != nil || v.RetiredAllocation.State == domain.RegisteredPreparationActiveV1 || domain.ValidateSHA256Digest("owned_residual.accounting", v.AccountingDigest) != nil || len(v.Operations) > 12 || len(v.Proxies) > 12 || len(v.Consents) > 2 || len(v.Bodies) > 28 {
		return nil, domain.ErrMembershipDenied
	}
	var total uint64
	for i, b := range v.Bodies {
		if b.Ref == "" || len(b.Ref) > 2048 || domain.ValidateSHA256Digest("owned_residual.body", b.Digest) != nil || b.Bytes == 0 || b.Bytes > 1<<20 || total > (1<<20)-b.Bytes {
			return nil, domain.ErrMembershipDenied
		}
		total += b.Bytes
		for j := 0; j < i; j++ {
			if v.Bodies[j].Ref == b.Ref {
				return nil, domain.ErrMembershipDenied
			}
		}
	}
	policy := v.Parent.Authority.HarnessBinding.AgentLoopPolicy
	sd, _ := v.Source.Digest()
	rd, _ := v.Parent.Reservation.DigestForAuthority(v.Parent.Authority)
	ad, _ := v.OriginalAllocation.Digest()
	if policy == nil || v.AggregateDebit.ValidateForPolicy(*policy) != nil || uint64(len(v.Operations)) != v.AggregateDebit.Operations || total > v.AggregateDebit.BodyBytes || v.Parent.Scope != v.Source.Scope || v.OriginalAllocation.Scope != v.Source.Scope || v.OriginalAllocation.SourceDigest != sd || v.OriginalAllocation.ParentReservationDigest != rd || v.OriginalAllocation.ValidateInitial() != nil || v.RetiredAllocation.ValidateTransitionFrom(v.OriginalAllocation) != nil || len(v.Proxies) < len(v.Operations) || len(v.Proxies) > len(v.Operations)+1 {
		return nil, domain.ErrMembershipDenied
	}
	scope := ownedResidualScope(v.Parent)
	findBody := func(ref, digest string, size uint64) bool {
		for _, b := range v.Bodies {
			if b.Ref == ref && b.Digest == digest && b.Bytes == size {
				return true
			}
		}
		return false
	}
	for i, peer := range v.Proxies {
		original, oe := peer.Original.Digest()
		current, ce := peer.Current.Digest()
		if peer.Sequence != uint64(i+1) || peer.Original.OperationSequence != peer.Sequence || oe != nil || ce != nil || original != current || peer.Original.ValidateForAllocation(v.OriginalAllocation, v.Source) != nil {
			return nil, domain.ErrMembershipDenied
		}
	}
	for i, row := range v.Operations {
		r, o := row.Reservation, row.Observation
		if r.ValidateForParent(v.Parent.Authority, v.Parent.Reservation) != nil || r.Operation.Scope != scope || r.Operation.Sequence != uint64(i+1) || r.RegisteredAllocationDigest != ad || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.State != domain.AttemptOperationUsageObservedV1 || o.Usage.AmountMicrounits == nil || *o.Usage.AmountMicrounits > r.Operation.Debit.AmountMicrounits || !findBody(r.Operation.RequestRef, r.Operation.RequestDigest, r.Operation.RequestBytes) || !findBody(o.ResponseRef, o.ResponseDigest, o.ResponseBytes) {
			return nil, domain.ErrMembershipDenied
		}
		if r.Operation.Kind == domain.AttemptOperationModelV1 || r.Operation.Kind == domain.AttemptOperationCompactionV1 {
			if o.Usage.InputTokens == nil || o.Usage.OutputTokens == nil || *o.Usage.InputTokens > r.Operation.Debit.InputTokens || *o.Usage.OutputTokens > r.Operation.Debit.OutputTokens {
				return nil, domain.ErrMembershipDenied
			}
		}
		proxyDigest, _ := v.Proxies[i].Original.Digest()
		service := domain.RegisteredProxyProviderV1
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			service = domain.RegisteredProxyMCPToolV1
		} else if r.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			service = domain.RegisteredProxyWebSearchV1
		}
		if r.RegisteredProxyDigest != proxyDigest || v.Proxies[i].Original.ServiceKind != service || v.Proxies[i].Original.OperationDigest != r.OperationDigest {
			return nil, domain.ErrMembershipDenied
		}
		if i > 0 && r.Operation.ValidatePredecessor(v.Operations[i-1].Reservation, v.Operations[i-1].Observation) != nil {
			return nil, domain.ErrMembershipDenied
		}
	}
	if len(v.Operations) > 0 {
		last := v.Operations[len(v.Operations)-1].Reservation
		if last.AggregateDebit != v.AggregateDebit || last.ComponentAmounts != v.ComponentAmounts {
			return nil, domain.ErrMembershipDenied
		}
	}
	for _, state := range v.Consents {
		request := state.Request
		if state.Validate() != nil || state.Status == domain.WebSearchConsentPendingV1 || request.Scope.TenantID != scope.TenantID || request.Scope.SessionID != scope.SessionID || request.Scope.OwnerUserID != scope.OwnerUserID || request.Scope.RunID != scope.RunID || request.Scope.AttemptID != scope.AttemptID || request.Scope.LeaseID != scope.LeaseID || request.Scope.FenceToken != scope.FenceToken || request.Scope.PhysicalInvocationClaimID != scope.PhysicalInvocationClaimID || request.Scope.EffectReservationDigest != string(scope.EffectReservationDigest) || request.Scope.InvocationAuthorityDigest != string(scope.InvocationAuthorityDigest) || request.Scope.HarnessBindingDigest != string(scope.HarnessBindingDigest) || request.Scope.PolicyDigest != string(scope.AgentLoopPolicyDigest) || !findBody(request.Arguments.Key, request.ArgumentsDigest, uint64(request.Arguments.Size)) {
			return nil, domain.ErrMembershipDenied
		}
	}
	body, err := json.Marshal(v)
	if err != nil || len(body) > 256<<10 {
		return nil, domain.ErrMembershipDenied
	}
	return body, nil
}

func ownedResidualScope(parent domain.OwnedAttemptEffectParentV1) domain.AttemptOperationScopeV1 {
	r := parent.Reservation
	rd, _ := r.DigestForAuthority(parent.Authority)
	pd, _ := parent.Authority.HarnessBinding.AgentLoopPolicy.Digest()
	return domain.AttemptOperationScopeV1{TenantID: r.TenantID, SessionID: parent.Scope.SessionID, OwnerUserID: parent.Scope.OwnerUserID, RunID: r.RunID, AttemptID: r.AttemptID, LeaseID: r.LeaseID, FenceToken: r.FenceToken, PhysicalInvocationClaimID: r.PhysicalInvocationClaimID, EffectReservationDigest: rd, InvocationAuthorityDigest: r.InvocationAuthorityDigest, HarnessBindingDigest: r.HarnessBindingDigest, AgentLoopPolicyDigest: pd}
}
func (v OwnedOperationResidualInventoryV1) Digest() (string, error) {
	body, err := v.IntentBytes()
	if err != nil {
		return "", err
	}
	return OwnedFixtureBytesDigestV1(body), nil
}

type OwnedOperationResidualReceiptV1 struct {
	Version         uint32                         `json:"version"`
	Scope           domain.AttemptOperationScopeV1 `json:"scope"`
	ParentDigest    string                         `json:"parent_digest"`
	InventoryDigest string                         `json:"inventory_digest"`
	NamespaceDigest string                         `json:"namespace_digest"`
	ClosedAt        time.Time                      `json:"closed_at"`
	DeletedBodies   uint64                         `json:"deleted_bodies"`
	DeletedBytes    uint64                         `json:"deleted_bytes"`
	Authenticator   []byte                         `json:"authenticator"`
}

func (v OwnedOperationResidualReceiptV1) IntentBytes() ([]byte, error) {
	if v.Version != 1 || v.Scope.Validate() != nil || domain.ValidateSHA256Digest("owned_residual.parent", v.ParentDigest) != nil || domain.ValidateSHA256Digest("owned_residual.inventory", v.InventoryDigest) != nil || domain.ValidateSHA256Digest("owned_residual.namespace", v.NamespaceDigest) != nil || v.ClosedAt.IsZero() || v.ClosedAt.Year() < 1 || v.ClosedAt.Year() > 9999 || v.ClosedAt.Nanosecond()%1000 != 0 || v.DeletedBodies > 28 || v.DeletedBytes > 1<<20 || len(v.Authenticator) != 32 {
		return nil, domain.ErrMembershipDenied
	}
	v.Authenticator = nil
	return json.Marshal(v)
}
func (v OwnedOperationResidualReceiptV1) Clone() OwnedOperationResidualReceiptV1 {
	v.Authenticator = append([]byte(nil), v.Authenticator...)
	return v
}

// Only privately installed original concrete custody may implement this: its
// receipt is issued AFTER actual original namespace close/delete/absence checks.
// No public signer, caller facts interface or boolean authorizes the Store.
type OwnedOperationResidualCustodyV1 interface {
	AttemptOperationBodyStoreV1
	CloseOwnedOperationNamespace(context.Context, OwnedOperationResidualInventoryV1) (OwnedOperationResidualReceiptV1, error)
	VerifyOwnedOperationResidualReceipt(OwnedOperationResidualReceiptV1) error
}
