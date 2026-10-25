package ports

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strconv"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// OwnedOperationResidualInventoryV3 is bounded historical DATA for the original
// public-MCP terminal producer. It cannot prove physical erasure, authenticate
// Source custody or replace the original keeper's nonserializable receipt.
// Search and generic fixture routes keep their existing strict representations.
type OwnedOperationResidualInventoryV3 struct {
	Version                 uint32                                `json:"version"`
	Parent                  domain.OwnedAttemptEffectParentV1     `json:"parent"`
	Source                  domain.AttemptExecutionSourceV1       `json:"source"`
	OriginalAllocation      domain.RegisteredAllocationV2         `json:"original_allocation"`
	RetiredAllocation       domain.RegisteredAllocationV2         `json:"retired_allocation"`
	AccountingDisposition   OwnedOperationAccountingDispositionV3 `json:"accounting_disposition"`
	Operations              []OwnedOperationResidualRowV1         `json:"operations"`
	Proxies                 []OwnedOperationResidualProxyV1       `json:"proxies"`
	Consents                []domain.WebSearchConsentStateV1      `json:"consents"`
	Bodies                  []domain.BlobRef                      `json:"bodies"`
	PhysicalInventoryDigest string                                `json:"physical_inventory_digest"`
}

func (v OwnedOperationResidualInventoryV3) Validate() error {
	deny := func() error { return domain.ErrMembershipDenied }
	if v.Version != 3 || v.AccountingDisposition.ValidateFor(v.Parent, v.Source) != nil || v.OriginalAllocation.ValidateInitial() != nil || v.RetiredAllocation.State != domain.RegisteredPreparationRetiredV1 || v.RetiredAllocation.ValidateTransitionFrom(v.OriginalAllocation) != nil || len(v.Operations) == 0 || len(v.Operations) > 12 || len(v.Proxies) != len(v.Operations) || len(v.Consents) != 0 || len(v.Bodies) > 24 || len(v.Bodies) == 0 || domain.ValidateSHA256Digest("physical.inventory", v.PhysicalInventoryDigest) != nil || len(v.Operations) != len(v.AccountingDisposition.Operations) {
		return deny()
	}
	scope := v.AccountingDisposition.Scope
	sd, _ := v.Source.Digest()
	rd, _ := v.Parent.Reservation.DigestForAuthority(v.Parent.Authority)
	ad, _ := v.OriginalAllocation.Digest()
	if v.OriginalAllocation.Scope != v.Source.Scope || v.OriginalAllocation.SourceDigest != sd || v.OriginalAllocation.ParentReservationDigest != rd ||
		!reflect.DeepEqual(v.OriginalAllocation.ParentReservation, v.Parent.Reservation) || v.OriginalAllocation.Backend != v.Parent.Authority.HarnessBinding.Backend ||
		!reflect.DeepEqual(v.OriginalAllocation.Substrate, v.Parent.Authority.SubstrateBinding) || !reflect.DeepEqual(v.OriginalAllocation.Lease, v.Parent.Authority.Lease) || v.OriginalAllocation.Profile != v.Source.Profile {
		return deny()
	}
	for index, spending := range v.Source.Spending {
		digest, err := spending.Digest()
		registered := v.OriginalAllocation.Spending[index]
		if err != nil || registered.Locator != spending.Locator || registered.RevocationGeneration != spending.RevocationGeneration || registered.Digest != digest || registered.Window != spending.Window {
			return deny()
		}
	}
	raw, err := json.Marshal(scope)
	if err != nil {
		return deny()
	}
	ns := sha256.Sum256(append([]byte("sessionless.operation-body-scope.v1\x00"), raw...))
	prefix := domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempt-operations-v1/" + hex.EncodeToString(ns[:]) + "/"
	var total uint64
	for index, b := range v.Bodies {
		if b.Validate() != nil || b.TenantID != scope.TenantID || b.Size <= 0 || uint64(b.Size) > (1<<20)-total || b.Key != prefix+"opbody-v1."+strconv.FormatInt(b.Size, 10)+"."+b.SHA256 {
			return deny()
		}
		total += uint64(b.Size)
		for j := 0; j < index; j++ {
			if v.Bodies[j].Key == b.Key {
				return deny()
			}
		}
	}
	if total > v.AccountingDisposition.AggregateDebit.BodyBytes {
		return deny()
	}
	find := func(ref, digest string, n uint64) bool {
		for _, b := range v.Bodies {
			if b.Key == prefix+ref && b.SHA256 == digest && uint64(b.Size) == n {
				return true
			}
		}
		return false
	}
	cumulative := domain.AttemptOperationDebitV1{}
	for _, cost := range []*domain.ExternalServiceCostCeilingV1{v.Parent.Authority.AdmissionCostCeiling.MCPToolCost, v.Parent.Authority.AdmissionCostCeiling.WebSearchCost} {
		if cost != nil {
			cumulative.BodyBytes += 4 << 10
		}
	}
	amounts := [4]uint64{0, *v.Source.Spending[1].MaxAmountMicrounits, 0, 0}
	for index, item := range v.Operations {
		r, o := item.Reservation, item.Observation
		projected, e := BuildOwnedOperationAccountingRowV3(r, o, v.Parent, v.Source)
		if e != nil || !reflect.DeepEqual(projected, v.AccountingDisposition.Operations[index]) || r.Operation.Sequence != uint64(index+1) || r.RegisteredAllocationDigest != ad || !find(r.Operation.RequestRef, r.Operation.RequestDigest, r.Operation.RequestBytes) || !find(o.ResponseRef, o.ResponseDigest, o.ResponseBytes) {
			return deny()
		}
		cumulative, e = cumulative.Add(r.Operation.Debit)
		component := 0
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			component = 2
		}
		// Disposition.ValidateFor already proved the complete checked sum fits;
		// reproduce every immutable intermediate reservation, not only its tail.
		amounts[component] += r.Operation.Debit.AmountMicrounits
		if e != nil || cumulative != r.AggregateDebit || amounts != r.ComponentAmounts {
			return deny()
		}
		peer := v.Proxies[index]
		pd, pe := peer.Original.Digest()
		current, ce := peer.Current.Digest()
		service := domain.RegisteredProxyProviderV1
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			service = domain.RegisteredProxyMCPToolV1
			if peer.Original.TransportKind != domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1 || peer.Original.LocalPublicMCPHTTPS == nil || peer.Original.GrantDigest != v.Source.Spending[2].ScopedGrantDigest {
				return deny()
			}
		} else if peer.Original.TransportKind != domain.RegisteredProxyTransportLocalLoopbackHTTPV2 {
			return deny()
		}
		if peer.Sequence != uint64(index+1) || pe != nil || ce != nil || pd != current || peer.Original.ValidateForAllocation(v.OriginalAllocation, v.Source) != nil || peer.Original.ServiceKind != service || peer.Original.OperationDigest != r.OperationDigest || peer.Original.OperationSequence != r.Operation.Sequence || r.RegisteredProxyDigest != pd {
			return deny()
		}
		if index == 0 {
			if r.Operation.PredecessorObservationDigest != "" || r.ReservedAt.Before(v.Parent.Reservation.ReservedAt) {
				return deny()
			}
		} else {
			previous := v.Operations[index-1]
			if r.Operation.ValidatePredecessor(previous.Reservation, previous.Observation) != nil || r.ReservedAt.Before(previous.Observation.ObservedAt) {
				return deny()
			}
		}
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 && (index == 0 || v.Operations[index-1].Observation.ResponseDigest != r.Operation.ModelResponseDigest) {
			return deny()
		}
	}
	last := v.Operations[len(v.Operations)-1].Reservation
	if last.AggregateDebit != v.AccountingDisposition.AggregateDebit || last.ComponentAmounts != v.AccountingDisposition.ComponentAmounts {
		return deny()
	}
	return nil
}

func (v OwnedOperationResidualInventoryV3) Clone() OwnedOperationResidualInventoryV3 {
	v.Parent = v.Parent.Clone()
	v.Source = v.Source.Clone()
	v.OriginalAllocation = v.OriginalAllocation.Clone()
	v.RetiredAllocation = v.RetiredAllocation.Clone()
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

func (v OwnedOperationResidualInventoryV3) IntentBytes() ([]byte, error) {
	if v.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	b, err := json.Marshal(v)
	if err != nil || len(b) > 256<<10 {
		return nil, domain.ErrMembershipDenied
	}
	return b, nil
}

func (v OwnedOperationResidualInventoryV3) Digest() (string, error) {
	b, err := v.IntentBytes()
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, _ = h.Write([]byte("sessionless.operation-residual-inventory.v3\x00"))
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil)), nil
}
