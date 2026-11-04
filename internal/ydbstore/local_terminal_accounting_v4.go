package ydbstore

import (
	"context"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// One successor owner for successful paid SEARCH, never a cast of the old
// MCP-only accounting language. Its exact actual consent/frontier facts travel
// through the same output/cleanup/finalizer owner and existing physical inventory.
type localTerminalAccountingV4 struct {
	self        *localTerminalAccountingV4
	owner       *LocalTerminalV1
	source      *LocalCompactionSourceV1
	profile     domain.OwnedAgentProfileCurrentV1
	invoice     ownedLocalInvoiceEvidence
	disposition ports.OwnedOperationAccountingDispositionV4
	operations  []ports.OwnedOperationResidualRowV1
	proxies     []ports.OwnedOperationResidualProxyV1
	consents    []domain.WebSearchConsentStateV1
}

func (*localTerminalAccountingV4) MarshalJSON() ([]byte, error) {
	return nil, domain.ErrMembershipDenied
}
func (*localTerminalAccountingV4) String() string {
	return "localTerminalAccountingV4{private:[redacted]}"
}

func prepareLocalTerminalAccountingV4(ctx context.Context, h *LocalTerminalV1, at time.Time) (*localTerminalAccountingV4, error) {
	deny := func() (*localTerminalAccountingV4, error) { return nil, domain.ErrMembershipDenied }
	if ctx == nil || ctx.Err() != nil || !h.original() || h.failed.Load() || h.source == nil || h.source.self != h.source || h.source.state == nil || h.source.state.cleared {
		return deny()
	}
	entry, s := h.entry, h.source
	if len(s.state.operations) < 3 || len(s.state.operations) > 12 || len(s.state.operations) != len(s.state.snapshot.Operations) || len(s.state.snapshot.ProxyDigests) != len(s.state.operations) || s.state.snapshot.Consents == nil {
		return deny()
	}
	m := entry.preparedModel.Load()
	last := s.state.operations[len(s.state.operations)-1]
	if m == nil || !m.originalForPhase(entry, localCompactionCompleted) || !reflect.DeepEqual(last.Reservation, m.reservation) || !reflect.DeepEqual(last.Observation, m.observation.Observation) || m.price == nil || m.price.local == nil || m.price.price != nil {
		return deny()
	}
	price, accounting := m.price, s.state.snapshot.Accounting
	if entry.owner.store.validateOwnedLocalInvoiceEvidence(price.local, price.profile, entry.source, last.Reservation.Operation, at) != nil || accounting.validate(entry.parent, entry.source, at) != nil || entry.revalidateMCPConservativeFunding(ctx, at) != nil {
		return deny()
	}
	consents := make([]domain.WebSearchConsentStateV1, len(s.state.snapshot.Consents.States))
	for i, state := range s.state.snapshot.Consents.States {
		consents[i] = state.Clone()
	}
	consentDigest, err := ports.OwnedSearchConsentStatesDigestV4(consents)
	if err != nil {
		return deny()
	}
	_, digest, err := accountingBytes(accounting)
	if err != nil {
		return deny()
	}
	terms := price.local.terms
	d := ports.OwnedOperationAccountingDispositionV4{Version: 4, Scope: ownedOperationScope(entry.parent), AccountingDigest: digest, AggregateDebit: accounting.Aggregate, ComponentAmounts: accounting.Amounts, ConsentStatesDigest: consentDigest, ProviderInvoice: ports.OwnedLocalInvoiceIdentityV2{ProfileDigest: entry.source.Profile.Digest, TermsDigest: price.local.digest, Revision: terms.Revision, Currency: terms.Currency, IssuedAt: terms.IssuedAt, ExpiresAt: terms.ExpiresAt}}
	for _, state := range consents {
		d.ConsentReservedBytes += ownedConsentMetadataReservationBytes + uint64(state.Request.Arguments.Size)
	}
	var copied uint64
	for _, item := range s.state.operations {
		copied += uint64(len(item.Request) + len(item.Response))
	}
	if chargeOwnedAuthorityBody(ctx, copied+ownedProviderPriceBodyBytes+2*ownedAuthorityHeadBytes) != nil {
		return deny()
	}
	baseline := ownedAttemptAccounting{Version: 1, ParentDigest: accounting.ParentDigest, SetupReservedAt: accounting.SetupReservedAt, UpdatedAt: accounting.SetupReservedAt}
	baseline.Aggregate.BodyBytes = ownedSetupBodyBytes(entry.parent)
	baseline.Amounts[1] = *entry.source.Spending[1].MaxAmountMicrounits
	var charged uint8
	var rows []ports.OwnedOperationResidualRowV1
	for i, item := range s.state.operations {
		r, o := item.Reservation, item.Observation
		if !reflect.DeepEqual(r, s.state.snapshot.Operations[i].reservation) || s.state.snapshot.Operations[i].observation == nil || !reflect.DeepEqual(o, *s.state.snapshot.Operations[i].observation) || r.Operation.Sequence != uint64(i+1) || r.Operation.Scope != d.Scope || !entry.originalObservationAllowsSuccessor(r, o) {
			return deny()
		}
		if i > 0 && chargeLocalConsentFrontier(&baseline, s.state.snapshot.Consents, s.state.snapshot.Operations[i-1], r.ReservedAt, &charged) != nil {
			return deny()
		}
		switch r.Operation.Kind {
		case domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1:
			if validateOwnedLocalInvoiceContent(price.local, price.profile, entry.source, r.Operation, r.ReservedAt) != nil {
				return deny()
			}
			decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(entry.owner.configuration, item.Request, item.Response, r.Operation.Kind)
			usage, ue := decoded.Usage.OperationUsage()
			if decoded.Call != nil {
				clear(decoded.Call.Arguments)
			}
			if err != nil || ue != nil || !reflect.DeepEqual(usage, o.Usage) {
				return deny()
			}
		case domain.AttemptOperationWebSearchV1:
			original := entry.findOriginalSearch(r.Operation.Sequence)
			if i == 0 || original == nil || !original.pending.grant.sourceCurrentAt(at) || !reflect.DeepEqual(original.pending.state, consentsForRequest(consents, original.pending.request.ID)) || entry.validateOriginalSearchMaterial(s.state.operations[i-1], item) != nil {
				return deny()
			}
		case domain.AttemptOperationMCPToolV1:
		default:
			return deny()
		}
		baseline, err = debitOwnedOperation(baseline, r.Operation, entry.parent, entry.source, r.ReservedAt)
		if err != nil || baseline.Aggregate != r.AggregateDebit || baseline.Amounts != r.ComponentAmounts {
			return deny()
		}
		row, err := ports.BuildOwnedOperationAccountingRowV4(r, o, entry.parent, entry.source)
		if err != nil {
			return deny()
		}
		d.Operations = append(d.Operations, row)
		rows = append(rows, ports.OwnedOperationResidualRowV1{Reservation: r.Clone(), Observation: o.Clone()})
	}
	if charged != uint8((1<<len(consents))-1) || !reflect.DeepEqual(baseline, accounting) || d.ValidateFor(entry.parent, entry.source) != nil {
		return deny()
	}
	proxies, err := readLocalTerminalAccountingProxies(ctx, h, len(rows))
	if err != nil || ctx.Err() != nil || !entry.owner.store.localInvoiceLifetimeCurrent(price.local) {
		return deny()
	}
	invoice := *price.local
	invoice.configuration = invoice.configuration.Clone()
	c := &localTerminalAccountingV4{owner: h, source: s, profile: price.profile.Clone(), invoice: invoice, disposition: d.Clone(), operations: rows, proxies: proxies, consents: consents}
	c.self = c
	return c, nil
}

func consentsForRequest(states []domain.WebSearchConsentStateV1, id domain.WebSearchConsentRequestIDV1) domain.WebSearchConsentStateV1 {
	for _, state := range states {
		if state.Request.ID == id {
			return state
		}
	}
	return domain.WebSearchConsentStateV1{}
}

func (c *localTerminalAccountingV4) historicalValid() bool {
	if c == nil || c.self != c || !c.owner.original() || c.owner.source != c.source || c.invoice.source != c.owner.entry.owner.source || c.disposition.ValidateFor(c.owner.entry.parent, c.owner.entry.source) != nil || len(c.operations) != len(c.disposition.Operations) {
		return false
	}
	digest, err := ports.OwnedSearchConsentStatesDigestV4(c.consents)
	if err != nil || digest != c.disposition.ConsentStatesDigest {
		return false
	}
	for i, row := range c.operations {
		projected, err := ports.BuildOwnedOperationAccountingRowV4(row.Reservation, row.Observation, c.owner.entry.parent, c.owner.entry.source)
		if err != nil || !reflect.DeepEqual(projected, c.disposition.Operations[i]) || !c.owner.entry.originalObservationAllowsSuccessor(row.Reservation, row.Observation) {
			return false
		}
		if (row.Reservation.Operation.Kind == domain.AttemptOperationModelV1 || row.Reservation.Operation.Kind == domain.AttemptOperationCompactionV1) && validateOwnedLocalInvoiceContent(&c.invoice, c.profile, c.owner.entry.source, row.Reservation.Operation, row.Reservation.ReservedAt) != nil {
			return false
		}
	}
	return true
}

func (c *localTerminalAccountingV4) Digest() (string, error) {
	if !c.historicalValid() {
		return "", domain.ErrMembershipDenied
	}
	return c.disposition.Digest()
}

func (c *localTerminalAccountingV4) Residual(retired domain.RegisteredAllocationV2, erased *operationbody.NamespaceErasureV1) (ports.OwnedOperationResidualInventoryV4, error) {
	if !c.historicalValid() || c.owner.entry.terminalOutputs.VerifyNamespaceErasure(erased) != nil {
		return ports.OwnedOperationResidualInventoryV4{}, domain.ErrMembershipDenied
	}
	bodies, err := erased.Bodies()
	if err != nil {
		return ports.OwnedOperationResidualInventoryV4{}, domain.ErrMembershipDenied
	}
	physical, err := erased.InventoryDigest()
	if err != nil {
		return ports.OwnedOperationResidualInventoryV4{}, domain.ErrMembershipDenied
	}
	v := ports.OwnedOperationResidualInventoryV4{Version: 4, Parent: c.owner.entry.parent.Clone(), Source: c.owner.entry.source.Clone(), OriginalAllocation: c.owner.entry.allocation.Clone(), RetiredAllocation: retired.Clone(), AccountingDisposition: c.disposition.Clone(), Operations: c.operations, Proxies: c.proxies, Consents: c.consents, Bodies: bodies, PhysicalInventoryDigest: physical}.Clone()
	if v.Validate() != nil {
		return ports.OwnedOperationResidualInventoryV4{}, domain.ErrMembershipDenied
	}
	return v, nil
}
