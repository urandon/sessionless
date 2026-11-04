package ports

import (
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// V4 is the closed successful paid-search accounting DATA language. Unknown
// service USD retains the original nonrefundable reservation; it is not a bill,
// refund, serving capability or a reinterpretation of a V2/V3 disposition.
type OwnedOperationAccountingKindV4 string

const (
	OwnedOperationMeasuredV4                 OwnedOperationAccountingKindV4 = "measured"
	OwnedOperationProviderReportedReservedV4 OwnedOperationAccountingKindV4 = "provider_reported_reserved"
	OwnedOperationUnknownReservedV4          OwnedOperationAccountingKindV4 = "unknown_reserved"
)

type OwnedOperationAccountingRowV4 struct {
	Sequence           uint64                                     `json:"sequence"`
	Kind               domain.AttemptOperationKindV1              `json:"kind"`
	OperationDigest    domain.AttemptOperationDigestV1            `json:"operation_digest"`
	ReservationDigest  domain.AttemptOperationReservationDigestV1 `json:"reservation_digest"`
	ObservationDigest  domain.AttemptOperationObservationDigestV1 `json:"observation_digest"`
	Disposition        OwnedOperationAccountingKindV4             `json:"disposition"`
	Reserved           domain.AttemptOperationDebitV1             `json:"reserved"`
	Usage              domain.AttemptOperationUsageV1             `json:"usage"`
	ToolSpendingDigest string                                     `json:"tool_spending_digest,omitempty"`
}

type OwnedOperationAccountingDispositionV4 struct {
	Version              uint32                          `json:"version"`
	Scope                domain.AttemptOperationScopeV1  `json:"scope"`
	AccountingDigest     string                          `json:"accounting_digest"`
	AggregateDebit       domain.AttemptOperationDebitV1  `json:"aggregate_debit"`
	ComponentAmounts     [4]uint64                       `json:"component_amounts"`
	ProviderInvoice      OwnedLocalInvoiceIdentityV2     `json:"provider_invoice"`
	ConsentStatesDigest  string                          `json:"consent_states_digest"`
	ConsentReservedBytes uint64                          `json:"consent_reserved_bytes"`
	Operations           []OwnedOperationAccountingRowV4 `json:"operations"`
}

// BuildOwnedOperationAccountingRowV4 checks immutable correlation only. The
// original entry separately authenticates the actual observation, receipt,
// Source funding and physical custody before admitting any successor.
func BuildOwnedOperationAccountingRowV4(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) (OwnedOperationAccountingRowV4, error) {
	deny := func() (OwnedOperationAccountingRowV4, error) {
		return OwnedOperationAccountingRowV4{}, domain.ErrMembershipDenied
	}
	if !accountingSourceV3(parent, source) || r.ValidateForParent(parent.Authority, parent.Reservation) != nil || r.Operation.Scope != ownedResidualScope(parent) || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(r.Operation.Debit) {
		return deny()
	}
	kind, spending := OwnedOperationUnknownReservedV4, ""
	switch r.Operation.Kind {
	case domain.AttemptOperationWebSearchV1:
		if o.Usage.State != domain.AttemptOperationUsageUnknownV1 || !searchFailureReservationV4(r.Operation, r.ReservedAt, parent, source) {
			return deny()
		}
		spending, _ = source.Spending[3].Digest()
	case domain.AttemptOperationMCPToolV1:
		if o.Usage.State != domain.AttemptOperationUsageUnknownV1 || !mcpReservationV3(r.Operation, r.ReservedAt, parent, source) {
			return deny()
		}
		spending, _ = source.Spending[2].Digest()
	case domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1:
		if r.Operation.Debit.AmountMicrounits != 0 {
			return deny()
		}
		var err error
		kind, err = providerAccountingKindV4(o.Usage)
		if err != nil {
			return deny()
		}
	default:
		return deny()
	}
	rd, _ := r.Digest()
	od, _ := o.Digest()
	return OwnedOperationAccountingRowV4{Sequence: r.Operation.Sequence, Kind: r.Operation.Kind, OperationDigest: r.OperationDigest, ReservationDigest: rd, ObservationDigest: od, Disposition: kind, Reserved: r.Operation.Debit, Usage: o.Usage.Clone(), ToolSpendingDigest: spending}, nil
}

func providerAccountingKindV4(u domain.AttemptOperationUsageV1) (OwnedOperationAccountingKindV4, error) {
	if u.Validate() == nil {
		switch u.State {
		case domain.AttemptOperationUsageUnknownV1:
			return OwnedOperationUnknownReservedV4, nil
		case domain.AttemptOperationUsageProviderReportedV1:
			return OwnedOperationProviderReportedReservedV4, nil
		case domain.AttemptOperationUsageObservedV1:
			if u.AmountMicrounits != nil && u.InputTokens != nil && u.OutputTokens != nil {
				return OwnedOperationMeasuredV4, nil
			}
		}
	}
	return "", domain.ErrMembershipDenied
}

func (v OwnedOperationAccountingDispositionV4) contentValid() bool {
	i := v.ProviderInvoice
	if v.Version != 4 || v.Scope.Validate() != nil || len(v.Operations) < 3 || len(v.Operations) > 12 || uint64(len(v.Operations)) != v.AggregateDebit.Operations || domain.ValidateSHA256Digest("accounting", v.AccountingDigest) != nil || domain.ValidateSHA256Digest("consents", v.ConsentStatesDigest) != nil || v.ConsentReservedBytes < (64<<10)+1 || v.ConsentReservedBytes > 2*((64<<10)+(4<<10)) || domain.ValidateSHA256Digest("invoice.profile", i.ProfileDigest) != nil || domain.ValidateSHA256Digest("invoice.terms", i.TermsDigest) != nil || domain.ValidateOpaqueID("invoice.revision", i.Revision) != nil || len(i.Currency) != 3 || i.IssuedAt.IsZero() || i.IssuedAt != i.IssuedAt.UTC().Truncate(time.Microsecond) || i.ExpiresAt != i.ExpiresAt.UTC().Truncate(time.Microsecond) || !i.ExpiresAt.After(i.IssuedAt) {
		return false
	}
	for _, c := range i.Currency {
		if c < 'A' || c > 'Z' {
			return false
		}
	}
	searches, tools := 0, 0
	for index, row := range v.Operations {
		if row.Sequence != uint64(index+1) || row.OperationDigest.Validate() != nil || row.ReservationDigest.Validate() != nil || row.ObservationDigest.Validate() != nil || row.Usage.Validate() != nil || row.Usage.ExceedsReservation(row.Reserved) {
			return false
		}
		switch row.Kind {
		case domain.AttemptOperationMCPToolV1, domain.AttemptOperationWebSearchV1:
			if index == 0 || index == len(v.Operations)-1 || v.Operations[index-1].Kind != domain.AttemptOperationModelV1 || row.Disposition != OwnedOperationUnknownReservedV4 || row.Usage.State != domain.AttemptOperationUsageUnknownV1 || domain.ValidateSHA256Digest("tool.spending", row.ToolSpendingDigest) != nil {
				return false
			}
			tools++
			if row.Kind == domain.AttemptOperationWebSearchV1 {
				searches++
			}
		case domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1:
			kind, err := providerAccountingKindV4(row.Usage)
			if err != nil || row.Disposition != kind || row.ToolSpendingDigest != "" || row.Reserved.AmountMicrounits != 0 {
				return false
			}
		default:
			return false
		}
	}
	return searches > 0 && searches <= 2 && tools <= 4 && v.Operations[len(v.Operations)-1].Kind == domain.AttemptOperationModelV1
}

func (v OwnedOperationAccountingDispositionV4) ValidateFor(parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) error {
	if !v.contentValid() || !accountingSourceV3(parent, source) || v.Scope != ownedResidualScope(parent) || v.AggregateDebit.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil {
		return domain.ErrMembershipDenied
	}
	i, p := v.ProviderInvoice, source.Spending[0]
	if i.ProfileDigest != source.Profile.Digest || i.TermsDigest != p.PriceEvidenceDigest || i.TermsDigest != p.TermsEvidenceDigest || i.Revision != p.PriceRevision || i.Currency != source.Currency || p.PriceState != domain.ExternalServicePriceKnownFreeV1 || p.MaxAmountMicrounits == nil || *p.MaxAmountMicrounits != 0 || i.IssuedAt.After(source.IssuedAt) || source.ExpiresAt.After(i.ExpiresAt) || source.Spending[1].MaxAmountMicrounits == nil {
		return domain.ErrMembershipDenied
	}
	aggregate := domain.AttemptOperationDebitV1{BodyBytes: v.ConsentReservedBytes}
	for _, c := range []*domain.ExternalServiceCostCeilingV1{parent.Authority.AdmissionCostCeiling.MCPToolCost, parent.Authority.AdmissionCostCeiling.WebSearchCost} {
		if c != nil {
			aggregate.BodyBytes += 4 << 10
		}
	}
	amounts := [4]uint64{0, *source.Spending[1].MaxAmountMicrounits, 0, 0}
	for _, row := range v.Operations {
		if row.Reserved.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil {
			return domain.ErrMembershipDenied
		}
		index := 0
		if row.Kind == domain.AttemptOperationMCPToolV1 {
			index = 2
		} else if row.Kind == domain.AttemptOperationWebSearchV1 {
			index = 3
		}
		if index != 0 {
			spending, err := source.Spending[index].Digest()
			if err != nil || spending != row.ToolSpendingDigest || source.Spending[index].MaxAmountMicrounits == nil || index == 2 && row.Reserved.AmountMicrounits != *source.Spending[2].MaxAmountMicrounits || index == 3 && (*source.Spending[3].MaxAmountMicrounits != 20000 || row.Reserved.AmountMicrounits != 10000) {
				return domain.ErrMembershipDenied
			}
		}
		var err error
		aggregate, err = aggregate.Add(row.Reserved)
		if err != nil || amounts[index] > math.MaxUint64-row.Reserved.AmountMicrounits {
			return domain.ErrMembershipDenied
		}
		amounts[index] += row.Reserved.AmountMicrounits
	}
	var total uint64
	for index, amount := range amounts {
		if source.Spending[index].MaxAmountMicrounits == nil || amount > *source.Spending[index].MaxAmountMicrounits || amount > math.MaxUint64-total {
			return domain.ErrMembershipDenied
		}
		total += amount
	}
	if total > source.MaxTotalAmountMicrounits || aggregate != v.AggregateDebit || amounts != v.ComponentAmounts {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (v OwnedOperationAccountingDispositionV4) Clone() OwnedOperationAccountingDispositionV4 {
	v.Operations = append([]OwnedOperationAccountingRowV4(nil), v.Operations...)
	for i := range v.Operations {
		v.Operations[i].Usage = v.Operations[i].Usage.Clone()
	}
	return v
}

func (v OwnedOperationAccountingDispositionV4) Digest() (string, error) {
	if !v.contentValid() {
		return "", domain.ErrMembershipDenied
	}
	return residualHashV2("sessionless.operation-accounting-disposition.v4\x00", v)
}

// OwnedSearchConsentStatesDigestV4 commits the exact ordered nonpending
// historical states for successful V4 custody. It supplies no live approval.
// Failure V4 retains its already committed, distinct failure digest language.
func OwnedSearchConsentStatesDigestV4(states []domain.WebSearchConsentStateV1) (string, error) {
	if len(states) < 1 || len(states) > 2 {
		return "", domain.ErrMembershipDenied
	}
	for i, state := range states {
		if state.Validate() != nil || state.Status == domain.WebSearchConsentPendingV1 {
			return "", domain.ErrMembershipDenied
		}
		for j := 0; j < i; j++ {
			if states[j].Request.ID == state.Request.ID {
				return "", domain.ErrMembershipDenied
			}
		}
	}
	return residualHashV2("sessionless.success-consent-states.v4\x00", states)
}
