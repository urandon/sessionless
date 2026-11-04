package ports

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strconv"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// V4 retains the existing physical inventory protocol with explicit paid
// SEARCH accounting and exact consent objects. This DATA is not an erasure
// receipt, current approval, observation authenticator or dispatch authority.
type OwnedOperationResidualInventoryV4 struct {
	Version                 uint32                                `json:"version"`
	Parent                  domain.OwnedAttemptEffectParentV1     `json:"parent"`
	Source                  domain.AttemptExecutionSourceV1       `json:"source"`
	OriginalAllocation      domain.RegisteredAllocationV2         `json:"original_allocation"`
	RetiredAllocation       domain.RegisteredAllocationV2         `json:"retired_allocation"`
	AccountingDisposition   OwnedOperationAccountingDispositionV4 `json:"accounting_disposition"`
	Operations              []OwnedOperationResidualRowV1         `json:"operations"`
	Proxies                 []OwnedOperationResidualProxyV1       `json:"proxies"`
	Consents                []domain.WebSearchConsentStateV1      `json:"consents"`
	Bodies                  []domain.BlobRef                      `json:"bodies"`
	PhysicalInventoryDigest string                                `json:"physical_inventory_digest"`
}

func (v OwnedOperationResidualInventoryV4) Validate() error {
	deny := func() error { return domain.ErrMembershipDenied }
	if v.Version != 4 || v.AccountingDisposition.ValidateFor(v.Parent, v.Source) != nil || v.OriginalAllocation.ValidateInitial() != nil || v.RetiredAllocation.State != domain.RegisteredPreparationRetiredV1 || v.RetiredAllocation.ValidateTransitionFrom(v.OriginalAllocation) != nil || len(v.Operations) < 3 || len(v.Operations) > 12 || len(v.Proxies) != len(v.Operations) || len(v.Bodies) == 0 || len(v.Bodies) > 24 || len(v.Operations) != len(v.AccountingDisposition.Operations) || domain.ValidateSHA256Digest("physical.inventory", v.PhysicalInventoryDigest) != nil {
		return deny()
	}
	scope := v.AccountingDisposition.Scope
	sd, _ := v.Source.Digest()
	rd, _ := v.Parent.Reservation.DigestForAuthority(v.Parent.Authority)
	ad, _ := v.OriginalAllocation.Digest()
	if v.OriginalAllocation.Scope != v.Source.Scope || v.OriginalAllocation.SourceDigest != sd || v.OriginalAllocation.ParentReservationDigest != rd || !reflect.DeepEqual(v.OriginalAllocation.ParentReservation, v.Parent.Reservation) || v.OriginalAllocation.Backend != v.Parent.Authority.HarnessBinding.Backend || !reflect.DeepEqual(v.OriginalAllocation.Substrate, v.Parent.Authority.SubstrateBinding) || !reflect.DeepEqual(v.OriginalAllocation.Lease, v.Parent.Authority.Lease) || v.OriginalAllocation.Profile != v.Source.Profile || len(v.OriginalAllocation.Spending) != len(v.Source.Spending) {
		return deny()
	}
	for i, s := range v.Source.Spending {
		digest, err := s.Digest()
		registered := v.OriginalAllocation.Spending[i]
		if err != nil || registered.Locator != s.Locator || registered.RevocationGeneration != s.RevocationGeneration || registered.Digest != digest || registered.Window != s.Window {
			return deny()
		}
	}
	encoded, err := json.Marshal(scope)
	if err != nil {
		return deny()
	}
	ns := sha256.Sum256(append([]byte("sessionless.operation-body-scope.v1\x00"), encoded...))
	prefix := domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempt-operations-v1/" + hex.EncodeToString(ns[:]) + "/"
	used := make([]bool, len(v.Bodies))
	var total uint64
	for i, b := range v.Bodies {
		if b.Validate() != nil || b.TenantID != scope.TenantID || b.Size <= 0 || uint64(b.Size) > (1<<20)-total {
			return deny()
		}
		total += uint64(b.Size)
		for j := 0; j < i; j++ {
			if v.Bodies[j].Key == b.Key {
				return deny()
			}
		}
	}
	if total > v.AccountingDisposition.AggregateDebit.BodyBytes {
		return deny()
	}
	find := func(ref domain.BlobRef) bool {
		for i, b := range v.Bodies {
			if b == ref {
				used[i] = true
				return true
			}
		}
		return false
	}
	ordinary := func(ref, digest string, size uint64) bool {
		if size == 0 || size > 1<<20 || ref != "opbody-v1."+strconv.FormatUint(size, 10)+"."+digest {
			return false
		}
		return find(domain.BlobRef{TenantID: scope.TenantID, Key: prefix + ref, Size: int64(size), SHA256: digest})
	}
	frontiers, err := v.consentFrontiers(find)
	if err != nil {
		return deny()
	}
	cumulative := domain.AttemptOperationDebitV1{BodyBytes: 8 << 10}
	amounts := [4]uint64{0, *v.Source.Spending[1].MaxAmountMicrounits, 0, 0}
	for i, item := range v.Operations {
		r, o := item.Reservation, item.Observation
		for j, frontier := range frontiers {
			if frontier == uint64(i) {
				cumulative, err = cumulative.Add(domain.AttemptOperationDebitV1{BodyBytes: (64 << 10) + uint64(v.Consents[j].Request.Arguments.Size)})
				if err != nil || v.Consents[j].Request.CreatedAt.After(r.ReservedAt) {
					return deny()
				}
			}
		}
		projected, err := BuildOwnedOperationAccountingRowV4(r, o, v.Parent, v.Source)
		if err != nil || !reflect.DeepEqual(projected, v.AccountingDisposition.Operations[i]) || r.RegisteredAllocationDigest != ad || !ordinary(r.Operation.RequestRef, r.Operation.RequestDigest, r.Operation.RequestBytes) || !ordinary(o.ResponseRef, o.ResponseDigest, o.ResponseBytes) {
			return deny()
		}
		cumulative, err = cumulative.Add(r.Operation.Debit)
		component := 0
		service := domain.RegisteredProxyProviderV1
		transport := domain.RegisteredProxyTransportLocalLoopbackHTTPV2
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			component, service, transport = 2, domain.RegisteredProxyMCPToolV1, domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1
		} else if r.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			component, service, transport = 3, domain.RegisteredProxyWebSearchV1, domain.RegisteredProxyTransportLocalSearchHTTPSV1
		}
		// ValidateFor proved the complete checked sums; reproduce each immutable
		// reservation frontier, including intervening consent BODY-only charges.
		amounts[component] += r.Operation.Debit.AmountMicrounits
		if err != nil || cumulative != r.AggregateDebit || amounts != r.ComponentAmounts {
			return deny()
		}
		peer := v.Proxies[i]
		original, oe := peer.Original.Digest()
		current, ce := peer.Current.Digest()
		if peer.Sequence != uint64(i+1) || oe != nil || ce != nil || original != current || peer.Original.ValidateForAllocation(v.OriginalAllocation, v.Source) != nil || peer.Original.ServiceKind != service || peer.Original.TransportKind != transport || peer.Original.OperationDigest != r.OperationDigest || peer.Original.OperationSequence != r.Operation.Sequence || r.RegisteredProxyDigest != original || r.ReservedAt.Before(peer.Original.Window.PublishedAt) || r.Operation.Deadline.After(peer.Original.Window.ExpiresAt) {
			return deny()
		}
		if component != 0 && peer.Original.GrantDigest != v.Source.Spending[component].ScopedGrantDigest {
			return deny()
		}
		if i == 0 {
			if r.Operation.PredecessorObservationDigest != "" || r.ReservedAt.Before(v.Parent.Reservation.ReservedAt) {
				return deny()
			}
		} else if r.Operation.ValidatePredecessor(v.Operations[i-1].Reservation, v.Operations[i-1].Observation) != nil || r.ReservedAt.Before(v.Operations[i-1].Observation.ObservedAt) {
			return deny()
		}
	}
	if cumulative != v.AccountingDisposition.AggregateDebit || amounts != v.AccountingDisposition.ComponentAmounts {
		return deny()
	}
	for _, found := range used {
		if !found {
			return deny()
		}
	}
	return nil
}

func (v OwnedOperationResidualInventoryV4) consentFrontiers(find func(domain.BlobRef) bool) ([]uint64, error) {
	deny := func() ([]uint64, error) { return nil, domain.ErrMembershipDenied }
	digest, err := OwnedSearchConsentStatesDigestV4(v.Consents)
	if err != nil || digest != v.AccountingDisposition.ConsentStatesDigest {
		return deny()
	}
	scope := v.AccountingDisposition.Scope
	frontiers := make([]uint64, len(v.Consents))
	var charged uint64
	searches := 0
	for i, state := range v.Consents {
		r, s := state.Request, state.Request.Scope
		if s.TenantID != scope.TenantID || s.SessionID != scope.SessionID || s.OwnerUserID != scope.OwnerUserID || s.RunID != scope.RunID || s.AttemptID != scope.AttemptID || s.LeaseID != scope.LeaseID || s.FenceToken != scope.FenceToken || s.PhysicalInvocationClaimID != scope.PhysicalInvocationClaimID || s.EffectReservationDigest != string(scope.EffectReservationDigest) || s.InvocationAuthorityDigest != string(scope.InvocationAuthorityDigest) || s.HarnessBindingDigest != string(scope.HarnessBindingDigest) || s.PolicyDigest != string(scope.AgentLoopPolicyDigest) || state.Decision == nil || state.Decision.Choice != domain.WebSearchConsentApproveV1 {
			return deny()
		}
		ref, err := domain.ParseAttemptOperationConsentArgumentsRefV1(scope, r.Arguments.Key, uint64(r.Arguments.Size))
		if err != nil || ref != r.Arguments || !find(ref) {
			return deny()
		}
		rd, _ := r.Digest()
		dd, _ := state.Decision.Digest()
		for j, item := range v.Operations {
			op := item.Reservation.Operation
			if op.Kind != domain.AttemptOperationWebSearchV1 || op.ConsentRequestDigest != string(rd) {
				continue
			}
			if j == 0 || frontiers[i] != 0 || v.Operations[j-1].Reservation.Operation.Kind != domain.AttemptOperationModelV1 || v.Operations[j-1].Observation.ResponseDigest != r.ModelResponseDigest || r.CreatedAt.Before(v.Operations[j-1].Observation.ObservedAt) || op.SearchWire == nil || op.SearchWire.ArgumentsBytes != uint64(r.Arguments.Size) || op.ModelResponseDigest != r.ModelResponseDigest || op.CallID != r.CallID || op.ArgumentsDigest != r.ArgumentsDigest || op.QueryDigest != r.QueryDigest || op.ToolGrantDigest != r.GrantDigest || op.ConsentDecisionDigest != string(dd) || op.ConsentDecisionRef != string(r.ID) || state.Decision.ObservedAt.After(item.Reservation.ReservedAt) || !item.Reservation.ReservedAt.Before(r.ExpiresAt) || op.Deadline.After(r.ExpiresAt) || state.Status != domain.WebSearchConsentApprovedV1 && state.UpdatedAt.Before(item.Reservation.ReservedAt) {
				return deny()
			}
			frontiers[i] = v.Operations[j-1].Reservation.Operation.Sequence
			searches++
		}
		if frontiers[i] == 0 || i > 0 && frontiers[i] <= frontiers[i-1] {
			return deny()
		}
		charged += (64 << 10) + uint64(r.Arguments.Size)
	}
	for _, item := range v.Operations {
		if item.Reservation.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			searches--
		}
	}
	if searches != 0 || charged != v.AccountingDisposition.ConsentReservedBytes {
		return deny()
	}
	return frontiers, nil
}

func (v OwnedOperationResidualInventoryV4) Clone() OwnedOperationResidualInventoryV4 {
	v.Parent, v.Source = v.Parent.Clone(), v.Source.Clone()
	v.OriginalAllocation, v.RetiredAllocation = v.OriginalAllocation.Clone(), v.RetiredAllocation.Clone()
	v.AccountingDisposition = v.AccountingDisposition.Clone()
	v.Operations = append([]OwnedOperationResidualRowV1(nil), v.Operations...)
	for i := range v.Operations {
		v.Operations[i].Reservation = v.Operations[i].Reservation.Clone()
		v.Operations[i].Observation = v.Operations[i].Observation.Clone()
	}
	v.Proxies = append([]OwnedOperationResidualProxyV1(nil), v.Proxies...)
	for i := range v.Proxies {
		v.Proxies[i].Original = v.Proxies[i].Original.Clone()
		v.Proxies[i].Current = v.Proxies[i].Current.Clone()
	}
	v.Consents = append([]domain.WebSearchConsentStateV1(nil), v.Consents...)
	for i := range v.Consents {
		v.Consents[i] = v.Consents[i].Clone()
	}
	v.Bodies = append([]domain.BlobRef(nil), v.Bodies...)
	return v
}

func (v OwnedOperationResidualInventoryV4) IntentBytes() ([]byte, error) {
	if v.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	b, err := json.Marshal(v)
	if err != nil || len(b) > 256<<10 {
		return nil, domain.ErrMembershipDenied
	}
	return b, nil
}

func (v OwnedOperationResidualInventoryV4) Digest() (string, error) {
	if v.Validate() != nil {
		return "", domain.ErrMembershipDenied
	}
	return residualHashV2("sessionless.operation-residual-inventory.v4\x00", v)
}
