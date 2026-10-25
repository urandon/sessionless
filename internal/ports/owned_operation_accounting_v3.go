package ports

import (
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// V3 is DATA for the original public-MCP composer, never an eligibility
// witness, billing measurement or cleanup receipt. V2 remains unchanged.
type OwnedOperationAccountingKindV3 string

const OwnedOperationUnknownFullReservedV3 OwnedOperationAccountingKindV3 = "unknown_full_reserved"

type OwnedOperationAccountingRowV3 struct {
	Sequence          uint64                                     `json:"sequence"`
	Kind              domain.AttemptOperationKindV1              `json:"kind"`
	OperationDigest   domain.AttemptOperationDigestV1            `json:"operation_digest"`
	ReservationDigest domain.AttemptOperationReservationDigestV1 `json:"reservation_digest"`
	ObservationDigest domain.AttemptOperationObservationDigestV1 `json:"observation_digest"`
	Disposition       OwnedOperationAccountingKindV3             `json:"disposition"`
	Reserved          domain.AttemptOperationDebitV1             `json:"reserved"`
	Usage             domain.AttemptOperationUsageV1             `json:"usage"`
	// Exact original component identity includes payer, resource/profile,
	// generations, grant, price/terms, currency, revision and original window.
	MCPSpendingDigest string `json:"mcp_spending_digest,omitempty"`
}

type OwnedOperationAccountingDispositionV3 struct {
	Version          uint32                          `json:"version"`
	Scope            domain.AttemptOperationScopeV1  `json:"scope"`
	AccountingDigest string                          `json:"accounting_digest"`
	AggregateDebit   domain.AttemptOperationDebitV1  `json:"aggregate_debit"`
	ComponentAmounts [4]uint64                       `json:"component_amounts"`
	ProviderInvoice  OwnedLocalInvoiceIdentityV2     `json:"provider_invoice"`
	Operations       []OwnedOperationAccountingRowV3 `json:"operations"`
}

// BuildOwnedOperationAccountingRowV3 validates historical DATA correlations.
// The original producer separately authenticates actual Source/Store custody;
// possession of parent/source/digests here cannot grant an external effect.
func BuildOwnedOperationAccountingRowV3(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) (OwnedOperationAccountingRowV3, error) {
	deny := func() (OwnedOperationAccountingRowV3, error) {
		return OwnedOperationAccountingRowV3{}, domain.ErrMembershipDenied
	}
	if !accountingSourceV3(parent, source) || r.ValidateForParent(parent.Authority, parent.Reservation) != nil || r.Operation.Scope != ownedResidualScope(parent) || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(r.Operation.Debit) {
		return deny()
	}
	var kind OwnedOperationAccountingKindV3
	var spending string
	if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
		if o.Usage.State != domain.AttemptOperationUsageUnknownV1 || !mcpReservationV3(r.Operation, r.ReservedAt, parent, source) {
			return deny()
		}
		kind = OwnedOperationUnknownFullReservedV3
		spending, _ = source.Spending[2].Digest()
	} else {
		if r.Operation.Kind != domain.AttemptOperationModelV1 && r.Operation.Kind != domain.AttemptOperationCompactionV1 {
			return deny()
		}
		row, err := BuildOwnedOperationAccountingRowV2(r, o)
		if err != nil || r.Operation.Debit.AmountMicrounits != 0 {
			return deny()
		}
		kind = OwnedOperationAccountingKindV3(row.Disposition)
	}
	rd, _ := r.Digest()
	od, _ := o.Digest()
	return OwnedOperationAccountingRowV3{Sequence: r.Operation.Sequence, Kind: r.Operation.Kind, OperationDigest: r.OperationDigest, ReservationDigest: rd, ObservationDigest: od, Disposition: kind, Reserved: r.Operation.Debit, Usage: o.Usage.Clone(), MCPSpendingDigest: spending}, nil
}

func accountingSourceV3(parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) bool {
	a := parent.Authority
	return parent.Validate() == nil && source.ValidateForExecution(a.ExecutionPlacementV2, a.HarnessBinding, a.SubstrateBinding, a.AdmissionCostCeiling) == nil && parent.Scope == source.Scope &&
		a.HarnessBinding.Resource.Kind == domain.ProviderResourceLocalEndpointV1 && a.SubstrateBinding.Kind == domain.SubstrateLocalDevelopmentV1 && a.HarnessBinding.InputDataClass == domain.ProviderDataPublicV1 &&
		source.Spending[0].SourceKind == domain.AttemptSpendingOwnerLocalSourceV1 && source.Spending[2].SourceKind == domain.AttemptSpendingOwnerLocalSourceV1
}

func mcpReservationV3(op domain.AttemptOperationV1, at time.Time, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) bool {
	s := source.Spending[2]
	c := parent.Authority.AdmissionCostCeiling.MCPToolCost
	return c != nil && c.ValidateAt(at) == nil && s.ValidateAt(at) == nil && c.Kind == domain.ExternalServiceMCPToolV1 &&
		c.TenantID == source.Scope.TenantID && c.OwnerUserID == source.Scope.OwnerUserID && c.BillingOwnerUserID == s.Locator.BillingOwnerUserID &&
		c.ResourceID == s.ResourceID && c.ResourceRevision == s.ResourceRevision && c.RevocationGeneration == s.ResourceGeneration && c.ProfileID == s.ProfileID && c.ProfileRevision == s.ProfileRevision &&
		c.ScopedGrantDigest == s.ScopedGrantDigest && c.PriceEvidenceDigest == s.PriceEvidenceDigest && c.TermsEvidenceDigest == s.TermsEvidenceDigest && c.Currency == s.Currency && c.PriceRevision == s.PriceRevision && c.PriceState == s.PriceState &&
		c.MaxAmountMicrounits != nil && s.MaxAmountMicrounits != nil && *c.MaxAmountMicrounits == *s.MaxAmountMicrounits &&
		op.ToolGrantDigest == s.ScopedGrantDigest && op.ExternalChargeEvidenceDigest == s.PriceEvidenceDigest && op.Currency == s.Currency && op.Debit.AmountMicrounits == *s.MaxAmountMicrounits &&
		!op.Deadline.After(c.PriceExpiresAt) && !op.Deadline.After(s.Window.ExpiresAt) && !at.Before(source.IssuedAt) && op.Deadline.After(at) && !op.Deadline.After(source.ExpiresAt)
}

func (v OwnedOperationAccountingDispositionV3) ValidateFor(parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) error {
	if !v.contentValid() || !accountingSourceV3(parent, source) || v.Scope != ownedResidualScope(parent) || v.AggregateDebit.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil {
		return domain.ErrMembershipDenied
	}
	i, p := v.ProviderInvoice, source.Spending[0]
	if i.ProfileDigest != source.Profile.Digest || i.TermsDigest != p.PriceEvidenceDigest || i.TermsDigest != p.TermsEvidenceDigest || i.Revision != p.PriceRevision || i.Currency != source.Currency || p.PriceState != domain.ExternalServicePriceKnownFreeV1 || p.MaxAmountMicrounits == nil || *p.MaxAmountMicrounits != 0 || i.IssuedAt.After(source.IssuedAt) || source.ExpiresAt.After(i.ExpiresAt) {
		return domain.ErrMembershipDenied
	}
	var aggregate domain.AttemptOperationDebitV1
	for _, cost := range []*domain.ExternalServiceCostCeilingV1{parent.Authority.AdmissionCostCeiling.MCPToolCost, parent.Authority.AdmissionCostCeiling.WebSearchCost} {
		if cost != nil {
			aggregate.BodyBytes += 4 << 10
		}
	}
	amounts := [4]uint64{0, *source.Spending[1].MaxAmountMicrounits, 0, 0}
	spending, _ := source.Spending[2].Digest()
	for _, row := range v.Operations {
		if row.Reserved.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil {
			return domain.ErrMembershipDenied
		}
		index := 0
		if row.Kind == domain.AttemptOperationMCPToolV1 {
			index = 2
			if row.MCPSpendingDigest != spending || row.Reserved.AmountMicrounits != *source.Spending[2].MaxAmountMicrounits {
				return domain.ErrMembershipDenied
			}
		} else if row.Reserved.AmountMicrounits != 0 {
			return domain.ErrMembershipDenied
		}
		var err error
		aggregate, err = aggregate.Add(row.Reserved)
		if err != nil || row.Reserved.AmountMicrounits > math.MaxUint64-amounts[index] {
			return domain.ErrMembershipDenied
		}
		amounts[index] += row.Reserved.AmountMicrounits
	}
	var total uint64
	for index, amount := range amounts {
		if amount > *source.Spending[index].MaxAmountMicrounits || amount > math.MaxUint64-total {
			return domain.ErrMembershipDenied
		}
		total += amount
	}
	if total > source.MaxTotalAmountMicrounits || aggregate != v.AggregateDebit || amounts != v.ComponentAmounts {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (v OwnedOperationAccountingDispositionV3) contentValid() bool {
	i := v.ProviderInvoice
	if v.Version != 3 || v.Scope.Validate() != nil || len(v.Operations) == 0 || len(v.Operations) > 12 || uint64(len(v.Operations)) != v.AggregateDebit.Operations || domain.ValidateSHA256Digest("accounting", v.AccountingDigest) != nil || domain.ValidateSHA256Digest("invoice.profile", i.ProfileDigest) != nil || domain.ValidateSHA256Digest("invoice.terms", i.TermsDigest) != nil || domain.ValidateOpaqueID("invoice.revision", i.Revision) != nil || len(i.Currency) != 3 || i.IssuedAt.IsZero() || i.IssuedAt.Location() != time.UTC || i.ExpiresAt.Location() != time.UTC || i.IssuedAt.Nanosecond()%1000 != 0 || i.ExpiresAt.Nanosecond()%1000 != 0 || !i.ExpiresAt.After(i.IssuedAt) {
		return false
	}
	for _, c := range i.Currency {
		if c < 'A' || c > 'Z' {
			return false
		}
	}
	mcp := 0
	for index, row := range v.Operations {
		if row.Sequence != uint64(index+1) || row.OperationDigest.Validate() != nil || row.ReservationDigest.Validate() != nil || row.ObservationDigest.Validate() != nil || row.Usage.Validate() != nil || row.Usage.ExceedsReservation(row.Reserved) {
			return false
		}
		if row.Kind == domain.AttemptOperationMCPToolV1 {
			if index == 0 || index == len(v.Operations)-1 || v.Operations[index-1].Kind != domain.AttemptOperationModelV1 || row.Disposition != OwnedOperationUnknownFullReservedV3 || row.Usage.State != domain.AttemptOperationUsageUnknownV1 || domain.ValidateSHA256Digest("mcp.spending", row.MCPSpendingDigest) != nil {
				return false
			}
			mcp++
		} else {
			kind, err := accountingKindV2(row.Kind, row.Usage)
			if err != nil || (row.Kind != domain.AttemptOperationModelV1 && row.Kind != domain.AttemptOperationCompactionV1) || row.Disposition != OwnedOperationAccountingKindV3(kind) || row.MCPSpendingDigest != "" {
				return false
			}
		}
	}
	return mcp > 0 && v.Operations[len(v.Operations)-1].Kind == domain.AttemptOperationModelV1
}

func (v OwnedOperationAccountingDispositionV3) Clone() OwnedOperationAccountingDispositionV3 {
	v.Operations = append([]OwnedOperationAccountingRowV3(nil), v.Operations...)
	for i := range v.Operations {
		v.Operations[i].Usage = v.Operations[i].Usage.Clone()
	}
	return v
}

func (v OwnedOperationAccountingDispositionV3) Digest() (string, error) {
	if !v.contentValid() {
		return "", domain.ErrMembershipDenied
	}
	return residualHashV2("sessionless.operation-accounting-disposition.v3\x00", v)
}
