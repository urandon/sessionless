package ydbstore

import (
	"context"
	"errors"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// V3 is a distinct original producer, not a reinterpretation of the strict V2
// disposition. The private MCP witness authenticates unknown/full-reserved;
// MODEL rows still require the original local invoice, never MCP tariff DATA.
type localTerminalAccountingV3 struct {
	self        *localTerminalAccountingV3
	owner       *LocalTerminalV1
	source      *LocalCompactionSourceV1
	profile     domain.OwnedAgentProfileCurrentV1
	invoice     ownedLocalInvoiceEvidence
	disposition ports.OwnedOperationAccountingDispositionV3
	operations  []ports.OwnedOperationResidualRowV1
	proxies     []ports.OwnedOperationResidualProxyV1
}

func (*localTerminalAccountingV3) MarshalJSON() ([]byte, error) {
	return nil, errors.New("original terminal accounting cannot be serialized")
}
func (*localTerminalAccountingV3) String() string {
	return "localTerminalAccountingV3{private:[redacted]}"
}

// Caller holds original entry.preparationMu and source.state.mu. This function
// consumes the current original snapshot, not an independently supplied row or
// financial witness. The final transaction repeats all live Source/Store joins.
func prepareLocalTerminalAccountingV3(ctx context.Context, h *LocalTerminalV1, at time.Time) (*localTerminalAccountingV3, error) {
	deny := func() (*localTerminalAccountingV3, error) { return nil, domain.ErrMembershipDenied }
	if ctx == nil || ctx.Err() != nil || !h.original() || h.failed.Load() || h.source == nil || h.source.self != h.source || h.source.state == nil || h.source.state.cleared {
		return deny()
	}
	entry, s := h.entry, h.source
	store := entry.owner.store
	if len(s.state.operations) < 3 || len(s.state.operations) > 12 || len(s.state.operations) != len(s.state.snapshot.Operations) || len(s.state.snapshot.ProxyDigests) != len(s.state.operations) {
		return deny()
	}
	m := entry.preparedModel.Load()
	last := s.state.operations[len(s.state.operations)-1]
	if m == nil || m.self != m || m.entry != entry || !m.originalForPhase(entry, localCompactionCompleted) || !reflect.DeepEqual(last.Reservation, m.reservation) || !reflect.DeepEqual(last.Observation, m.observation.Observation) {
		return deny()
	}
	price := m.price
	if price == nil || price.local == nil || price.price != nil || store.validateOwnedLocalInvoiceEvidence(price.local, price.profile, entry.source, last.Reservation.Operation, at) != nil || entry.revalidateMCPConservativeFunding(ctx, at) != nil {
		return deny()
	}
	accounting := s.state.snapshot.Accounting
	if accounting.validate(entry.parent, entry.source, at) != nil {
		return deny()
	}
	_, digest, err := accountingBytes(accounting)
	if err != nil {
		return deny()
	}
	terms := price.local.terms
	d := ports.OwnedOperationAccountingDispositionV3{Version: 3, Scope: ownedOperationScope(entry.parent), AccountingDigest: digest, AggregateDebit: accounting.Aggregate, ComponentAmounts: accounting.Amounts,
		ProviderInvoice: ports.OwnedLocalInvoiceIdentityV2{ProfileDigest: entry.source.Profile.Digest, TermsDigest: price.local.digest, Revision: terms.Revision, Currency: terms.Currency, IssuedAt: terms.IssuedAt, ExpiresAt: terms.ExpiresAt}}
	var copied uint64
	for _, item := range s.state.operations {
		copied += uint64(len(item.Request) + len(item.Response))
	}
	if chargeOwnedAuthorityBody(ctx, copied+ownedProviderPriceBodyBytes) != nil {
		return deny()
	}
	baseline := ownedAttemptAccounting{Version: 1, ParentDigest: accounting.ParentDigest, SetupReservedAt: accounting.SetupReservedAt, UpdatedAt: accounting.SetupReservedAt}
	baseline.Aggregate.BodyBytes = ownedSetupBodyBytes(entry.parent)
	baseline.Amounts[1] = *entry.source.Spending[1].MaxAmountMicrounits
	rows := make([]ports.OwnedOperationResidualRowV1, 0, len(s.state.operations))
	for index, item := range s.state.operations {
		r, o := item.Reservation, item.Observation
		if !reflect.DeepEqual(r, s.state.snapshot.Operations[index].reservation) || s.state.snapshot.Operations[index].observation == nil || !reflect.DeepEqual(o, *s.state.snapshot.Operations[index].observation) ||
			r.Operation.Scope != d.Scope || r.Operation.Sequence != uint64(index+1) || !entry.originalObservationAllowsSuccessor(r, o) {
			return deny()
		}
		if r.Operation.Kind != domain.AttemptOperationMCPToolV1 {
			if validateOwnedLocalInvoiceContent(price.local, price.profile, entry.source, r.Operation, r.ReservedAt) != nil {
				return deny()
			}
			decoded, e := configuredinference.DecodeNormalizedResponseBodyV1(entry.owner.configuration, item.Request, item.Response, r.Operation.Kind)
			usage, ue := decoded.Usage.OperationUsage()
			if decoded.Call != nil {
				clear(decoded.Call.Arguments)
			}
			if e != nil || ue != nil || !reflect.DeepEqual(usage, o.Usage) {
				return deny()
			}
		}
		baseline, err = debitOwnedOperation(baseline, r.Operation, entry.parent, entry.source, r.ReservedAt)
		if err != nil || baseline.Aggregate != r.AggregateDebit || baseline.Amounts != r.ComponentAmounts {
			return deny()
		}
		row, err := ports.BuildOwnedOperationAccountingRowV3(r, o, entry.parent, entry.source)
		if err != nil {
			return deny()
		}
		d.Operations = append(d.Operations, row)
		rows = append(rows, ports.OwnedOperationResidualRowV1{Reservation: r.Clone(), Observation: o.Clone()})
	}
	if !reflect.DeepEqual(baseline, accounting) || d.ValidateFor(entry.parent, entry.source) != nil {
		return deny()
	}
	proxies, err := readLocalTerminalAccountingProxies(ctx, h, len(rows))
	if err != nil || ctx.Err() != nil || !store.localInvoiceLifetimeCurrent(price.local) {
		return deny()
	}
	invoice := *price.local
	invoice.configuration = invoice.configuration.Clone()
	c := &localTerminalAccountingV3{owner: h, source: s, profile: price.profile.Clone(), invoice: invoice, disposition: d.Clone(), operations: rows, proxies: proxies}
	c.self = c
	return c, nil
}

func (c *localTerminalAccountingV3) historicalValid() bool {
	if c == nil || c.self != c || !c.owner.original() || c.owner.source != c.source || c.invoice.source != c.owner.entry.owner.source || c.disposition.ValidateFor(c.owner.entry.parent, c.owner.entry.source) != nil || len(c.operations) != len(c.disposition.Operations) {
		return false
	}
	for i, row := range c.operations {
		projected, err := ports.BuildOwnedOperationAccountingRowV3(row.Reservation, row.Observation, c.owner.entry.parent, c.owner.entry.source)
		if err != nil || !reflect.DeepEqual(projected, c.disposition.Operations[i]) || !c.owner.entry.originalObservationAllowsSuccessor(row.Reservation, row.Observation) {
			return false
		}
		if row.Reservation.Operation.Kind != domain.AttemptOperationMCPToolV1 && validateOwnedLocalInvoiceContent(&c.invoice, c.profile, c.owner.entry.source, row.Reservation.Operation, row.Reservation.ReservedAt) != nil {
			return false
		}
	}
	return true
}

func (c *localTerminalAccountingV3) Digest() (string, error) {
	if !c.historicalValid() {
		return "", domain.ErrMembershipDenied
	}
	return c.disposition.Digest()
}

func (c *localTerminalAccountingV3) Residual(retired domain.RegisteredAllocationV2, erased *operationbody.NamespaceErasureV1) (ports.OwnedOperationResidualInventoryV3, error) {
	deny := func() (ports.OwnedOperationResidualInventoryV3, error) {
		return ports.OwnedOperationResidualInventoryV3{}, domain.ErrMembershipDenied
	}
	if !c.historicalValid() || c.owner.entry.terminalOutputs.VerifyNamespaceErasure(erased) != nil {
		return deny()
	}
	bodies, err := erased.Bodies()
	if err != nil {
		return deny()
	}
	physical, err := erased.InventoryDigest()
	if err != nil {
		return deny()
	}
	v := ports.OwnedOperationResidualInventoryV3{Version: 3, Parent: c.owner.entry.parent.Clone(), Source: c.owner.entry.source.Clone(), OriginalAllocation: c.owner.entry.allocation.Clone(), RetiredAllocation: retired.Clone(), AccountingDisposition: c.disposition.Clone(), Operations: c.operations, Proxies: c.proxies, Bodies: bodies, PhysicalInventoryDigest: physical}
	v = v.Clone()
	if v.Validate() != nil {
		return deny()
	}
	return v, nil
}
