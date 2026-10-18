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

// Captured only while the original source/frontier locks and entry barrier are
// held. No caller DATA/JSON or independently supplied invoice can create this.
type localTerminalAccountingV2 struct {
	self        *localTerminalAccountingV2
	owner       *LocalTerminalV1
	source      *LocalCompactionSourceV1
	profile     domain.OwnedAgentProfileCurrentV1
	invoice     ownedLocalInvoiceEvidence
	disposition ports.OwnedOperationAccountingDispositionV2
	operations  []ports.OwnedOperationResidualRowV1
	proxies     []ports.OwnedOperationResidualProxyV1
}

func (*localTerminalAccountingV2) MarshalJSON() ([]byte, error) {
	return nil, errors.New("original terminal accounting cannot be serialized")
}
func (*localTerminalAccountingV2) String() string {
	return "localTerminalAccountingV2{private:[redacted]}"
}

// The caller holds original entry.preparationMu and h.source.state.mu. This
// spends the EXISTING logical budget, never resets it or makes N history reads.
func prepareLocalTerminalAccounting(ctx context.Context, h *LocalTerminalV1, at time.Time, genericProfile domain.OwnedAgentProfileCurrentV1) (*localTerminalAccountingV2, error) {
	deny := func() (*localTerminalAccountingV2, error) { return nil, domain.ErrMembershipDenied }
	if ctx == nil || ctx.Err() != nil || !h.original() || h.failed.Load() || h.source == nil || h.source.self != h.source || h.source.state == nil || h.source.state.cleared {
		return deny()
	}
	entry := h.entry
	s := h.source
	store := entry.owner.store
	m := entry.preparedModel.Load()
	if len(s.state.operations) == 0 || len(s.state.operations) > 12 || len(s.state.operations) != len(s.state.snapshot.Operations) || len(s.state.snapshot.ProxyDigests) != len(s.state.operations) {
		return deny()
	}
	last := s.state.operations[len(s.state.operations)-1]
	var price *ownedProviderPriceEvidence
	if m != nil {
		if m.self != m || m.entry != entry || !m.originalForPhase(entry, localCompactionCompleted) || !reflect.DeepEqual(last.Reservation, m.reservation) || !reflect.DeepEqual(last.Observation, m.observation.Observation) {
			return deny()
		}
		price = m.price
	} else {
		// Preserve the original lower-level generic first-MODEL entry without
		// manufacturing a private preparedModel from its matching DATA. The
		// current installed profile was read in PrepareOutput's original join;
		// the same original live Source supplies the authenticated invoice now.
		digest, e := genericProfile.Digest()
		if len(s.state.operations) != 1 || last.Reservation.Operation.Sequence != 1 || last.Reservation.Operation.Kind != domain.AttemptOperationModelV1 ||
			e != nil || genericProfile.Locator != entry.source.Profile.Locator || genericProfile.RevocationGeneration != entry.source.Profile.RevocationGeneration || digest != entry.source.Profile.Digest {
			return deny()
		}
		price, e = store.readOwnedLocalInvoiceEvidence(ctx, genericProfile)
		if e != nil {
			return deny()
		}
	}
	if price == nil || price.local == nil || price.price != nil || store.validateOwnedLocalInvoiceEvidence(price.local, price.profile, entry.source, last.Reservation.Operation, at) != nil {
		return deny()
	}
	accounting := s.state.snapshot.Accounting
	if accounting.validate(entry.parent, entry.source, at) != nil {
		return deny()
	}
	_, digest, e := accountingBytes(accounting)
	if e != nil {
		return deny()
	}
	terms := price.local.terms
	d := ports.OwnedOperationAccountingDispositionV2{Version: 2, Scope: ownedOperationScope(entry.parent), AccountingDigest: digest, AggregateDebit: accounting.Aggregate, ComponentAmounts: accounting.Amounts, ProviderInvoice: ports.OwnedLocalInvoiceIdentityV2{ProfileDigest: entry.source.Profile.Digest, TermsDigest: price.local.digest, Revision: terms.Revision, Currency: terms.Currency, IssuedAt: terms.IssuedAt, ExpiresAt: terms.ExpiresAt}}
	var copied uint64
	for _, o := range s.state.operations {
		copied += uint64(len(o.Request) + len(o.Response))
	}
	if chargeOwnedAuthorityBody(ctx, copied+ownedProviderPriceBodyBytes) != nil {
		return deny()
	}
	baseline := ownedAttemptAccounting{Version: 1, ParentDigest: accounting.ParentDigest, SetupReservedAt: accounting.SetupReservedAt, UpdatedAt: accounting.SetupReservedAt}
	baseline.Aggregate.BodyBytes = ownedSetupBodyBytes(entry.parent)
	baseline.Amounts[1] = *entry.source.Spending[1].MaxAmountMicrounits
	rows := make([]ports.OwnedOperationResidualRowV1, 0, len(s.state.operations))
	for index, o := range s.state.operations {
		r, obs := o.Reservation, o.Observation
		if !reflect.DeepEqual(r, s.state.snapshot.Operations[index].reservation) || s.state.snapshot.Operations[index].observation == nil || !reflect.DeepEqual(obs, *s.state.snapshot.Operations[index].observation) || r.Operation.Kind != domain.AttemptOperationModelV1 && r.Operation.Kind != domain.AttemptOperationCompactionV1 || r.Operation.Sequence != uint64(index+1) || r.Operation.Scope != d.Scope || !ownedObservationAllowsSuccessor(r, obs) || validateOwnedLocalInvoiceContent(price.local, price.profile, entry.source, r.Operation, r.ReservedAt) != nil {
			return deny()
		}
		decoded, e := configuredinference.DecodeNormalizedResponseBodyV1(entry.owner.configuration, o.Request, o.Response, r.Operation.Kind)
		if e != nil {
			return deny()
		}
		usage, ue := decoded.Usage.OperationUsage()
		if decoded.Call != nil {
			clear(decoded.Call.Arguments)
		}
		if ue != nil || !reflect.DeepEqual(usage, obs.Usage) {
			return deny()
		}
		baseline, e = debitOwnedOperation(baseline, r.Operation, entry.parent, entry.source, r.ReservedAt)
		if e != nil || baseline.Aggregate != r.AggregateDebit || baseline.Amounts != r.ComponentAmounts {
			return deny()
		}
		row, e := ports.BuildOwnedOperationAccountingRowV2(r, obs)
		if e != nil {
			return deny()
		}
		d.Operations = append(d.Operations, row)
		rows = append(rows, ports.OwnedOperationResidualRowV1{Reservation: r.Clone(), Observation: obs.Clone()})
	}
	if !reflect.DeepEqual(baseline, accounting) || d.ValidateFor(entry.parent, entry.source) != nil {
		return deny()
	}
	var proxies []ports.OwnedOperationResidualProxyV1
	e = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		proxies = nil
		if _, e := s.rejoinRunLocked(ctx, q, nil, nil); e != nil {
			return e
		}
		first, e := readOwnedProxySnapshotAt(ctx, q, entry.parent, entry.source, entry.allocation, 1)
		if e != nil || first == nil {
			return q.budget.deny()
		}
		appendProxy := func(p *ownedProxySnapshot, seq uint64) error {
			if seq != uint64(len(proxies)+1) || seq > uint64(len(rows)) {
				return q.budget.deny()
			}
			pd, e := entry.compactionProxyDigest(p, s.state.snapshot.Operations[seq-1])
			if e != nil || pd != s.state.snapshot.ProxyDigests[seq-1] {
				return q.budget.deny()
			}
			proxies = append(proxies, ports.OwnedOperationResidualProxyV1{Sequence: seq, Original: p.original.Clone(), Current: p.current.Clone()})
			return nil
		}
		if appendProxy(first, 1) != nil {
			return q.budget.deny()
		}
		result, e := q.QueryContext(ctx, localCompactionProxiesSQL, entry.parent.Scope.TenantID, entry.parent.Reservation.AttemptID)
		if e != nil {
			return q.budget.deny()
		}
		defer result.Close()
		for result.Next() {
			var seq uint64
			var columns ownedProxyColumns
			dest := append([]any{&seq}, columns.destinations(2)...)
			n, e := scanOwnedCurrentRow(result, dest...)
			if q.budget.materialized(q.callback, n) != nil || e != nil || seq <= 1 || seq > uint64(len(rows)) {
				return q.budget.deny()
			}
			p, e := columns.decode(q, entry.parent, entry.source, entry.allocation, seq)
			if e != nil || p == nil || appendProxy(p, seq) != nil {
				return q.budget.deny()
			}
		}
		if result.Err() != nil || result.Close() != nil || len(proxies) != len(rows) {
			return q.budget.deny()
		}
		return nil
	})
	if e != nil || ctx.Err() != nil || !store.localInvoiceLifetimeCurrent(price.local) {
		return deny()
	}
	invoice := *price.local
	invoice.configuration = invoice.configuration.Clone()
	c := &localTerminalAccountingV2{owner: h, source: s, profile: price.profile.Clone(), invoice: invoice, disposition: d.Clone(), operations: rows, proxies: proxies}
	c.self = c
	return c, nil
}

func (c *localTerminalAccountingV2) historicalValid() bool {
	if c == nil || c.self != c || !c.owner.original() || c.owner.source != c.source || c.invoice.source != c.owner.entry.owner.source || c.disposition.ValidateFor(c.owner.entry.parent, c.owner.entry.source) != nil || len(c.operations) != len(c.disposition.Operations) {
		return false
	}
	for index, row := range c.operations {
		projected, err := ports.BuildOwnedOperationAccountingRowV2(row.Reservation, row.Observation)
		if err != nil || !reflect.DeepEqual(projected, c.disposition.Operations[index]) || validateOwnedLocalInvoiceContent(&c.invoice, c.profile, c.owner.entry.source, row.Reservation.Operation, row.Reservation.ReservedAt) != nil {
			return false
		}
	}
	return true
}
func (c *localTerminalAccountingV2) Digest() (string, error) {
	if !c.historicalValid() {
		return "", domain.ErrMembershipDenied
	}
	return c.disposition.Digest()
}
func (c *localTerminalAccountingV2) Residual(retired domain.RegisteredAllocationV2, erased *operationbody.NamespaceErasureV1) (ports.OwnedOperationResidualInventoryV2, error) {
	if !c.historicalValid() || c.owner.entry.terminalOutputs.VerifyNamespaceErasure(erased) != nil {
		return ports.OwnedOperationResidualInventoryV2{}, domain.ErrMembershipDenied
	}
	bodies, e := erased.Bodies()
	if e != nil {
		return ports.OwnedOperationResidualInventoryV2{}, domain.ErrMembershipDenied
	}
	physical, e := erased.InventoryDigest()
	if e != nil {
		return ports.OwnedOperationResidualInventoryV2{}, domain.ErrMembershipDenied
	}
	v := ports.OwnedOperationResidualInventoryV2{Version: 2, Parent: c.owner.entry.parent.Clone(), Source: c.owner.entry.source.Clone(), OriginalAllocation: c.owner.entry.allocation.Clone(), RetiredAllocation: retired.Clone(), AccountingDisposition: c.disposition.Clone(), Operations: append([]ports.OwnedOperationResidualRowV1(nil), c.operations...), Proxies: append([]ports.OwnedOperationResidualProxyV1(nil), c.proxies...), Bodies: bodies, PhysicalInventoryDigest: physical}
	for i := range v.Operations {
		v.Operations[i].Reservation = v.Operations[i].Reservation.Clone()
		v.Operations[i].Observation = v.Operations[i].Observation.Clone()
	}
	for i := range v.Proxies {
		v.Proxies[i].Original = v.Proxies[i].Original.Clone()
		v.Proxies[i].Current = v.Proxies[i].Current.Clone()
	}
	if v.Validate() != nil {
		return ports.OwnedOperationResidualInventoryV2{}, domain.ErrMembershipDenied
	}
	return v, nil
}
