package ports

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type OwnedOperationAccountingKindV2 string

const (
	OwnedOperationMeasuredV2                 OwnedOperationAccountingKindV2 = "measured"
	OwnedOperationProviderReportedReservedV2 OwnedOperationAccountingKindV2 = "provider_reported_reserved"
	OwnedOperationUnknownReservedV2          OwnedOperationAccountingKindV2 = "unknown_reserved"
)

// Identity DATA only. The original producer must authenticate the actual
// owner-supplied zero-invoice terms; localhost/no-auth never establish this.
type OwnedLocalInvoiceIdentityV2 struct {
	ProfileDigest string    `json:"profile_digest"`
	TermsDigest   string    `json:"terms_digest"`
	Revision      string    `json:"revision"`
	Currency      string    `json:"currency"`
	IssuedAt      time.Time `json:"issued_at"`
	ExpiresAt     time.Time `json:"expires_at"`
}
type OwnedOperationAccountingRowV2 struct {
	Sequence          uint64                                     `json:"sequence"`
	Kind              domain.AttemptOperationKindV1              `json:"kind"`
	OperationDigest   domain.AttemptOperationDigestV1            `json:"operation_digest"`
	ReservationDigest domain.AttemptOperationReservationDigestV1 `json:"reservation_digest"`
	ObservationDigest domain.AttemptOperationObservationDigestV1 `json:"observation_digest"`
	Disposition       OwnedOperationAccountingKindV2             `json:"disposition"`
	Reserved          domain.AttemptOperationDebitV1             `json:"reserved"`
	Usage             domain.AttemptOperationUsageV1             `json:"usage"`
}
type OwnedOperationAccountingDispositionV2 struct {
	Version          uint32                          `json:"version"`
	Scope            domain.AttemptOperationScopeV1  `json:"scope"`
	AccountingDigest string                          `json:"accounting_digest"`
	AggregateDebit   domain.AttemptOperationDebitV1  `json:"aggregate_debit"`
	ComponentAmounts [4]uint64                       `json:"component_amounts"`
	ProviderInvoice  OwnedLocalInvoiceIdentityV2     `json:"provider_invoice"`
	Operations       []OwnedOperationAccountingRowV2 `json:"operations"`
}

// BuildOwnedOperationAccountingRowV2 projects exact validated DATA; it neither
// authenticates an observation nor supplies measurement/cleanup permission.
func BuildOwnedOperationAccountingRowV2(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1) (OwnedOperationAccountingRowV2, error) {
	k, e := accountingKindV2(r.Operation.Kind, o.Usage)
	if r.Validate() != nil || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(r.Operation.Debit) || e != nil {
		return OwnedOperationAccountingRowV2{}, domain.ErrMembershipDenied
	}
	rd, e := r.Digest()
	if e != nil {
		return OwnedOperationAccountingRowV2{}, domain.ErrMembershipDenied
	}
	od, e := o.Digest()
	if e != nil {
		return OwnedOperationAccountingRowV2{}, domain.ErrMembershipDenied
	}
	return OwnedOperationAccountingRowV2{Sequence: r.Operation.Sequence, Kind: r.Operation.Kind, OperationDigest: r.OperationDigest, ReservationDigest: rd, ObservationDigest: od, Disposition: k, Reserved: r.Operation.Debit, Usage: o.Usage.Clone()}, nil
}

func accountingKindV2(kind domain.AttemptOperationKindV1, u domain.AttemptOperationUsageV1) (OwnedOperationAccountingKindV2, error) {
	if u.Validate() != nil || (kind != domain.AttemptOperationModelV1 && kind != domain.AttemptOperationCompactionV1 && kind != domain.AttemptOperationMCPToolV1 && kind != domain.AttemptOperationWebSearchV1) {
		return "", domain.ErrMembershipDenied
	}
	model := kind == domain.AttemptOperationModelV1 || kind == domain.AttemptOperationCompactionV1
	switch u.State {
	case domain.AttemptOperationUsageUnknownV1:
		if model {
			return OwnedOperationUnknownReservedV2, nil
		}
	case domain.AttemptOperationUsageProviderReportedV1:
		if model {
			return OwnedOperationProviderReportedReservedV2, nil
		}
	case domain.AttemptOperationUsageObservedV1:
		if u.AmountMicrounits != nil && (!model || (u.InputTokens != nil && u.OutputTokens != nil)) {
			return OwnedOperationMeasuredV2, nil
		}
	}
	return "", domain.ErrMembershipDenied
}

func (v OwnedOperationAccountingDispositionV2) ValidateFor(parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) error {
	p := parent.Authority.HarnessBinding.AgentLoopPolicy
	if !v.contentValid() || parent.Validate() != nil || source.Validate() != nil || p == nil || v.Scope != ownedResidualScope(parent) || v.Scope.ValidateForParent(parent.Authority, parent.Reservation) != nil || parent.Scope != source.Scope || v.AggregateDebit.ValidateForPolicy(*p) != nil {
		return domain.ErrMembershipDenied
	}
	i := v.ProviderInvoice
	s := source.Spending[0]
	if parent.Authority.HarnessBinding.Resource.Kind != domain.ProviderResourceLocalEndpointV1 || parent.Authority.SubstrateBinding.Kind != domain.SubstrateLocalDevelopmentV1 || i.ProfileDigest != source.Profile.Digest || i.TermsDigest != s.PriceEvidenceDigest || i.TermsDigest != s.TermsEvidenceDigest || domain.ValidateSHA256Digest("invoice.terms", i.TermsDigest) != nil || i.Revision != s.PriceRevision || i.Currency != s.Currency || s.SourceKind != domain.AttemptSpendingOwnerLocalSourceV1 || s.PriceState != domain.ExternalServicePriceKnownFreeV1 || s.MaxAmountMicrounits == nil || *s.MaxAmountMicrounits != 0 || i.IssuedAt.IsZero() || !i.ExpiresAt.After(i.IssuedAt) || i.IssuedAt.After(source.IssuedAt) || source.ExpiresAt.After(i.ExpiresAt) {
		return domain.ErrMembershipDenied
	}
	var total, nonSubstrate uint64
	for index, amount := range v.ComponentAmounts {
		cap := source.Spending[index].MaxAmountMicrounits
		if cap == nil || amount > *cap || amount > math.MaxUint64-total {
			return domain.ErrMembershipDenied
		}
		total += amount
		if index != 1 {
			if amount > math.MaxUint64-nonSubstrate {
				return domain.ErrMembershipDenied
			}
			nonSubstrate += amount
		}
	}
	if source.Spending[1].MaxAmountMicrounits == nil || v.ComponentAmounts[1] != *source.Spending[1].MaxAmountMicrounits || nonSubstrate != v.AggregateDebit.AmountMicrounits || total > source.MaxTotalAmountMicrounits {
		return domain.ErrMembershipDenied
	}
	for index, row := range v.Operations {
		kind, e := accountingKindV2(row.Kind, row.Usage)
		if row.Sequence != uint64(index+1) || row.OperationDigest.Validate() != nil || row.ReservationDigest.Validate() != nil || row.ObservationDigest.Validate() != nil || row.Reserved.ValidateForPolicy(*p) != nil || row.Usage.ExceedsReservation(row.Reserved) || e != nil || row.Disposition != kind {
			return domain.ErrMembershipDenied
		}
		if (row.Kind == domain.AttemptOperationModelV1 || row.Kind == domain.AttemptOperationCompactionV1) && row.Reserved.AmountMicrounits != 0 {
			return domain.ErrMembershipDenied
		}
		if row.Kind != domain.AttemptOperationModelV1 && row.Kind != domain.AttemptOperationCompactionV1 && row.Kind != domain.AttemptOperationMCPToolV1 && row.Kind != domain.AttemptOperationWebSearchV1 {
			return domain.ErrMembershipDenied
		}
	}
	return nil
}
func (v OwnedOperationAccountingDispositionV2) Clone() OwnedOperationAccountingDispositionV2 {
	v.Operations = append([]OwnedOperationAccountingRowV2(nil), v.Operations...)
	for i := range v.Operations {
		v.Operations[i].Usage = v.Operations[i].Usage.Clone()
	}
	return v
}
func (v OwnedOperationAccountingDispositionV2) Digest() (string, error) {
	if !v.contentValid() {
		return "", domain.ErrMembershipDenied
	}
	return residualHashV2("sessionless.operation-accounting-disposition.v2\x00", v)
}

func (v OwnedOperationAccountingDispositionV2) contentValid() bool {
	i := v.ProviderInvoice
	if v.Version != 2 || v.Scope.Validate() != nil || len(v.Operations) == 0 || len(v.Operations) > 12 || uint64(len(v.Operations)) != v.AggregateDebit.Operations || domain.ValidateSHA256Digest("accounting.digest", v.AccountingDigest) != nil || domain.ValidateSHA256Digest("invoice.profile", i.ProfileDigest) != nil || domain.ValidateSHA256Digest("invoice.terms", i.TermsDigest) != nil || len(i.Revision) == 0 || len(i.Revision) > 160 || len(i.Currency) == 0 || len(i.Currency) > 16 || i.IssuedAt.IsZero() || i.IssuedAt.Location() != time.UTC || i.ExpiresAt.Location() != time.UTC || i.IssuedAt.Nanosecond()%1000 != 0 || i.ExpiresAt.Nanosecond()%1000 != 0 || !i.ExpiresAt.After(i.IssuedAt) {
		return false
	}
	for index, row := range v.Operations {
		kind, e := accountingKindV2(row.Kind, row.Usage)
		if row.Sequence != uint64(index+1) || row.OperationDigest.Validate() != nil || row.ReservationDigest.Validate() != nil || row.ObservationDigest.Validate() != nil || e != nil || row.Disposition != kind || row.Usage.ExceedsReservation(row.Reserved) {
			return false
		}
	}
	return true
}

// Explicit V2 DATA successor. It is not a receipt, signer, authority, or V1
// decoder fallback. The original owner verifies actual physical receipt and
// current/historical Store joins separately before a first canonical commit.
type OwnedOperationResidualInventoryV2 struct {
	Version                 uint32                                `json:"version"`
	Parent                  domain.OwnedAttemptEffectParentV1     `json:"parent"`
	Source                  domain.AttemptExecutionSourceV1       `json:"source"`
	OriginalAllocation      domain.RegisteredAllocationV2         `json:"original_allocation"`
	RetiredAllocation       domain.RegisteredAllocationV2         `json:"retired_allocation"`
	AccountingDisposition   OwnedOperationAccountingDispositionV2 `json:"accounting_disposition"`
	Operations              []OwnedOperationResidualRowV1         `json:"operations"`
	Proxies                 []OwnedOperationResidualProxyV1       `json:"proxies"`
	Consents                []domain.WebSearchConsentStateV1      `json:"consents"`
	Bodies                  []domain.BlobRef                      `json:"bodies"`
	PhysicalInventoryDigest string                                `json:"physical_inventory_digest"`
}

func (v OwnedOperationResidualInventoryV2) Validate() error {
	if v.Version != 2 || v.AccountingDisposition.ValidateFor(v.Parent, v.Source) != nil || v.OriginalAllocation.ValidateInitial() != nil || v.RetiredAllocation.State == domain.RegisteredPreparationActiveV1 || v.RetiredAllocation.ValidateTransitionFrom(v.OriginalAllocation) != nil || len(v.Operations) == 0 || len(v.Operations) > 12 || len(v.Proxies) != len(v.Operations) || len(v.Consents) > 2 || len(v.Bodies) > 24 || domain.ValidateSHA256Digest("physical.inventory", v.PhysicalInventoryDigest) != nil {
		return domain.ErrMembershipDenied
	}
	scope := v.AccountingDisposition.Scope
	sd, _ := v.Source.Digest()
	rd, _ := v.Parent.Reservation.DigestForAuthority(v.Parent.Authority)
	ad, _ := v.OriginalAllocation.Digest()
	if v.OriginalAllocation.Scope != v.Source.Scope || v.OriginalAllocation.SourceDigest != sd || v.OriginalAllocation.ParentReservationDigest != rd || len(v.AccountingDisposition.Operations) != len(v.Operations) {
		return domain.ErrMembershipDenied
	}
	raw, e := json.Marshal(scope)
	if e != nil {
		return domain.ErrMembershipDenied
	}
	ns := sha256.Sum256(append([]byte("sessionless.operation-body-scope.v1\x00"), raw...))
	prefix := domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempt-operations-v1/" + hex.EncodeToString(ns[:]) + "/"
	var total uint64
	for index, b := range v.Bodies {
		if b.Validate() != nil || b.TenantID != scope.TenantID || b.Size <= 0 || uint64(b.Size) > (1<<20)-total || !strings.HasPrefix(b.Key, prefix) {
			return domain.ErrMembershipDenied
		}
		total += uint64(b.Size)
		if b.Key != prefix+"opbody-v1."+strconv.FormatInt(b.Size, 10)+"."+b.SHA256 {
			return domain.ErrMembershipDenied
		}
		for j := 0; j < index; j++ {
			if v.Bodies[j].Key == b.Key {
				return domain.ErrMembershipDenied
			}
		}
	}
	if total > v.AccountingDisposition.AggregateDebit.BodyBytes {
		return domain.ErrMembershipDenied
	}
	find := func(ref, digest string, n uint64) bool {
		for _, b := range v.Bodies {
			if b.Key == prefix+ref && b.SHA256 == digest && uint64(b.Size) == n {
				return true
			}
		}
		return false
	}
	for index, row := range v.Operations {
		r, o := row.Reservation, row.Observation
		d := v.AccountingDisposition.Operations[index]
		od, _ := o.Digest()
		resd, _ := r.Digest()
		kind, ke := accountingKindV2(r.Operation.Kind, o.Usage)
		if r.ValidateForParent(v.Parent.Authority, v.Parent.Reservation) != nil || r.Operation.Scope != scope || r.Operation.Sequence != uint64(index+1) || r.RegisteredAllocationDigest != ad || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(r.Operation.Debit) || !find(r.Operation.RequestRef, r.Operation.RequestDigest, r.Operation.RequestBytes) || !find(o.ResponseRef, o.ResponseDigest, o.ResponseBytes) || ke != nil || d.Sequence != r.Operation.Sequence || d.Kind != r.Operation.Kind || d.OperationDigest != r.OperationDigest || d.ReservationDigest != resd || d.ObservationDigest != od || d.Disposition != kind || d.Reserved != r.Operation.Debit || !reflect.DeepEqual(d.Usage, o.Usage) {
			return domain.ErrMembershipDenied
		}
		peer := v.Proxies[index]
		original, oe := peer.Original.Digest()
		current, ce := peer.Current.Digest()
		service := domain.RegisteredProxyProviderV1
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			service = domain.RegisteredProxyMCPToolV1
		} else if r.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			service = domain.RegisteredProxyWebSearchV1
		}
		if peer.Sequence != r.Operation.Sequence || oe != nil || ce != nil || original != current || peer.Original.ValidateForAllocation(v.OriginalAllocation, v.Source) != nil || peer.Original.ServiceKind != service || peer.Original.OperationDigest != r.OperationDigest || peer.Original.OperationSequence != r.Operation.Sequence || r.RegisteredProxyDigest != original {
			return domain.ErrMembershipDenied
		}
		if index > 0 && r.Operation.ValidatePredecessor(v.Operations[index-1].Reservation, v.Operations[index-1].Observation) != nil {
			return domain.ErrMembershipDenied
		}
	}
	last := v.Operations[len(v.Operations)-1].Reservation
	if last.AggregateDebit != v.AccountingDisposition.AggregateDebit || last.ComponentAmounts != v.AccountingDisposition.ComponentAmounts {
		return domain.ErrMembershipDenied
	}
	for _, s := range v.Consents {
		r := s.Request
		if s.Validate() != nil || s.Status == domain.WebSearchConsentPendingV1 || r.Scope.TenantID != scope.TenantID || r.Scope.SessionID != scope.SessionID || r.Scope.OwnerUserID != scope.OwnerUserID || r.Scope.RunID != scope.RunID || r.Scope.AttemptID != scope.AttemptID || r.Scope.LeaseID != scope.LeaseID || r.Scope.FenceToken != scope.FenceToken || r.Scope.PhysicalInvocationClaimID != scope.PhysicalInvocationClaimID || r.Scope.EffectReservationDigest != string(scope.EffectReservationDigest) || r.Scope.InvocationAuthorityDigest != string(scope.InvocationAuthorityDigest) || r.Scope.HarnessBindingDigest != string(scope.HarnessBindingDigest) || r.Scope.PolicyDigest != string(scope.AgentLoopPolicyDigest) {
			return domain.ErrMembershipDenied
		}
		found := false
		for _, b := range v.Bodies {
			if b.Key == r.Arguments.Key && b.SHA256 == r.ArgumentsDigest && b.Size == r.Arguments.Size {
				found = true
			}
		}
		if !found {
			return domain.ErrMembershipDenied
		}
	}
	return nil
}
func (v OwnedOperationResidualInventoryV2) IntentBytes() ([]byte, error) {
	if v.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	b, e := json.Marshal(v)
	if e != nil || len(b) > 256<<10 {
		return nil, domain.ErrMembershipDenied
	}
	return b, nil
}
func (v OwnedOperationResidualInventoryV2) Digest() (string, error) {
	b, e := v.IntentBytes()
	if e != nil {
		return "", e
	}
	h := sha256.New()
	_, _ = h.Write([]byte("sessionless.operation-residual-inventory.v2\x00"))
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil)), nil
}
func residualHashV2(namespace string, v any) (string, error) {
	b, e := json.Marshal(v)
	if e != nil || len(b) > 256<<10 {
		return "", domain.ErrMembershipDenied
	}
	h := sha256.New()
	_, _ = h.Write([]byte(namespace))
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil)), nil
}
