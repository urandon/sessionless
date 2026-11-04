package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

// Tagged successor facts inside the existing original failure owner, not a new
// lifecycle or a cast of legacy accounting. No public error/stop DTO selects it.
type localFailureSearchV4 struct {
	stop       string
	pending    *localSearchConsentV1
	accounting ports.OwnedOperationFailureAccountingV4
	consents   []domain.WebSearchConsentStateV1
	catalogs   []localFailureModelCatalogV4
	capsule    localFailureCapsuleV4
}

func (e *LocalInvocationEntryV1) claimSearchFailureV4() (*LocalFailureV1, bool, error) {
	deny := func() (*LocalFailureV1, bool, error) { return nil, true, domain.ErrMembershipDenied }
	if !e.original() || e.preparedSearch == nil || e.pendingSearch == nil {
		return nil, false, nil
	}
	pending := e.pendingSearch.Load()
	search := e.preparedSearch.Load()
	if pending == nil && search == nil {
		return nil, false, nil
	}
	claim := func(source *LocalCompactionSourceV1, observed ports.AuthenticatedAttemptOperationObservationV1, stop string, p *localSearchConsentV1) (*LocalFailureV1, bool, error) {
		if source == nil || source.self != source || source.entry != e || source.state == nil || observed.Validate() != nil || e.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(observed.Clone()) != nil {
			return deny()
		}
		h, err := e.claimFailureHolder(source, observed, 4, &localFailureSearchV4{stop: stop, pending: p})
		if err != nil {
			return deny()
		}
		return h, true, nil
	}
	// Prefer the actual reserved MODEL2 failure over a known SEARCH prefix.
	if model := e.preparedModel.Load(); model != nil && model.self == model && model.entry == e && model.source != nil && model.source.state != nil {
		model.source.state.mu.Lock()
		o := model.observation
		valid := !model.source.state.cleared && o.Validate() == nil && reflect.DeepEqual(o.Reservation, model.reservation) && o.Reservation.Operation.Kind == domain.AttemptOperationModelV1 && (o.Observation.Outcome == domain.AttemptOperationRejectedBeforeSendV1 && o.Observation.Reason == domain.AttemptOperationReasonTransportUnknownV1 || o.Observation.Outcome == domain.AttemptOperationAcceptedOutcomeUnknownV1 && ports.FailureModelUnknownReasonV2(o.Observation.Reason))
		known := false
		for _, item := range model.source.state.operations {
			known = known || item.Reservation.Operation.Kind == domain.AttemptOperationWebSearchV1 && e.originalSearchObservationAllowsSuccessor(item.Reservation, item.Observation)
		}
		if valid && known {
			h, handled, err := claim(model.source, o, domain.ModelAfterSearchFailureV4, nil)
			model.source.state.mu.Unlock()
			return h, handled, err
		}
		model.source.state.mu.Unlock()
	}
	// A stopped decision has ZERO SEARCH reservation/observation for that call.
	// The completed original MODEL and actually appended consent BODY frontier
	// are authenticated during capture; no vendor result is manufactured.
	if pending != nil && pending.original() && pending.created && pending.waited && !pending.failed && pending.state.Validate() == nil && pending.state.Status != domain.WebSearchConsentPendingV1 && pending.state.Status != domain.WebSearchConsentApprovedV1 && (search == nil || search.operation.Sequence < pending.model.Operation.Sequence) {
		model := e.preparedModel.Load()
		if model == nil || model.self != model || model.entry != e || model.source == nil || model.source.state == nil {
			return deny()
		}
		model.source.state.mu.Lock()
		defer model.source.state.mu.Unlock()
		if model.source.state.cleared || !reflect.DeepEqual(model.reservation, pending.model) || !reflect.DeepEqual(model.observation.Observation, pending.observation) {
			return deny()
		}
		return claim(model.source, model.observation, domain.SearchConsentStoppedV4, pending)
	}
	if search == nil || search.self != search || search.entry != e || search.source == nil || search.source.state == nil {
		return deny()
	}
	search.source.state.mu.Lock()
	defer search.source.state.mu.Unlock()
	o := search.observation
	if search.source.state.cleared || !search.prepared || o.Validate() != nil || !reflect.DeepEqual(o.Reservation, search.reservation) || search.pending == nil {
		return deny()
	}
	stop := ""
	switch o.Observation.Outcome {
	case domain.AttemptOperationCompletedV1:
		if o.Observation.Reason == domain.AttemptOperationReasonServiceErrorV1 {
			stop = domain.SearchServiceErrorV4
		}
	case domain.AttemptOperationRejectedBeforeSendV1:
		if ports.FailureSearchRejectedReasonV4(o.Observation.Reason) {
			stop = domain.SearchRejectedBeforeSendV4
		}
	case domain.AttemptOperationAcceptedOutcomeUnknownV1:
		if ports.FailureSearchUnknownReasonV4(o.Observation.Reason) {
			stop = domain.SearchOutcomeUnknownV4
		}
	}
	if stop == "" {
		return deny()
	} // no reserved failure tail: reconcile-only
	return claim(search.source, o, stop, search.pending)
}

func (h *LocalFailureV1) authenticatedSearchRowV4(row ownedOperationRow, total uint64) bool {
	if !h.original() || h.version != 4 || h.search == nil || row.observation == nil || !domain.SearchFailureStopV4(h.search.stop) {
		return false
	}
	r, o := row.reservation, *row.observation
	if r.Operation.Sequence == total {
		return reflect.DeepEqual(r, h.terminal.Reservation) && reflect.DeepEqual(o, h.terminal.Observation) && h.entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(h.terminal.Clone()) == nil
	}
	if r.Operation.Kind == domain.AttemptOperationWebSearchV1 {
		return h.entry.originalSearchObservationAllowsSuccessor(r, o)
	}
	if model := h.entry.preparedModel.Load(); model != nil && reflect.DeepEqual(model.reservation, r) && reflect.DeepEqual(model.observation.Observation, o) {
		return h.entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(model.observation.Clone()) == nil
	}
	if mcp := h.entry.preparedMCP.Load(); mcp != nil && reflect.DeepEqual(mcp.reservation, r) && reflect.DeepEqual(mcp.observation.Observation, o) {
		return h.entry.originalObservationAllowsSuccessor(r, o)
	}
	if generation := h.entry.compactionGeneration.Load(); generation != nil && reflect.DeepEqual(generation.reservation, r) && reflect.DeepEqual(generation.observation.Observation, o) {
		return h.entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(generation.observation.Clone()) == nil
	}
	_, ok := h.entry.historicalOriginalObservation(r, o)
	return ok
}

func sameHistoricalConsentFactsV4(old, current domain.WebSearchConsentStateV1) bool {
	if old.Validate() != nil || current.Validate() != nil || !reflect.DeepEqual(old.Request, current.Request) || !reflect.DeepEqual(old.Decision, current.Decision) || current.UpdatedAt.Before(old.UpdatedAt) {
		return false
	}
	if reflect.DeepEqual(old, current) {
		return true
	}
	return old.Status == domain.WebSearchConsentApprovedV1 && current.Revision == old.Revision+1 && current.Status != domain.WebSearchConsentPendingV1 && current.Status != domain.WebSearchConsentApprovedV1 && current.Status != domain.WebSearchConsentDeniedV1
}

func (h *LocalFailureV1) captureSearchV4(ctx context.Context) error {
	if !h.original() || h.search == nil || h.version != 4 || h.source == nil || h.source.state == nil {
		return domain.ErrMembershipDenied
	}
	e := h.entry
	var snapshot localCompactionSnapshot
	if e.owner.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		var err error
		snapshot, err = h.readSnapshot(ctx, q)
		if err == nil {
			h.capturedAt = q.at
		}
		return err
	}) != nil || snapshot.Consents == nil || len(snapshot.Operations) == 0 {
		return domain.ErrMembershipDenied
	}
	state := h.source.state
	state.mu.Lock()
	if state.cleared || state.view == nil || len(snapshot.Operations) != len(state.snapshot.Operations)+1 || len(snapshot.ProxyDigests) != len(state.snapshot.ProxyDigests)+1 {
		state.mu.Unlock()
		return domain.ErrMembershipDenied
	}
	expected := state.snapshot
	priorView := state.view.Digest()
	current := snapshot
	last := snapshot.Operations[len(snapshot.Operations)-1]
	if last.observation == nil || !reflect.DeepEqual(last.reservation, h.terminal.Reservation) || !reflect.DeepEqual(*last.observation, h.terminal.Observation) {
		state.mu.Unlock()
		return domain.ErrMembershipDenied
	}
	next, err := debitOwnedOperation(expected.Accounting, last.reservation.Operation, e.parent, e.source, last.reservation.ReservedAt)
	if err != nil || next.Aggregate != last.reservation.AggregateDebit || next.Amounts != last.reservation.ComponentAmounts {
		state.mu.Unlock()
		return domain.ErrMembershipDenied
	}
	oldCount := 0
	if expected.Consents != nil {
		oldCount = len(expected.Consents.States)
	}
	newCount := oldCount
	if h.search.stop == domain.SearchConsentStoppedV4 {
		newCount++
	}
	if len(snapshot.Consents.States) != newCount {
		state.mu.Unlock()
		return domain.ErrMembershipDenied
	}
	for i := 0; i < oldCount; i++ {
		if !sameHistoricalConsentFactsV4(expected.Consents.States[i], snapshot.Consents.States[i]) {
			state.mu.Unlock()
			return domain.ErrMembershipDenied
		}
	}
	if h.search.stop == domain.SearchConsentStoppedV4 {
		p := h.search.pending
		actual := snapshot.Consents.States[newCount-1]
		if p == nil || !p.original() || !p.created || p.failed || !p.waited || !reflect.DeepEqual(actual, p.state) || !reflect.DeepEqual(last.reservation, p.model) || !reflect.DeepEqual(*last.observation, p.observation) || actual.Status == domain.WebSearchConsentApprovedV1 || actual.Status == domain.WebSearchConsentPendingV1 || actual.Request.CreatedAt.Before(last.observation.ObservedAt) {
			state.mu.Unlock()
			return domain.ErrMembershipDenied
		}
		next.Aggregate, err = next.Aggregate.Add(domain.AttemptOperationDebitV1{BodyBytes: ownedConsentMetadataReservationBytes + uint64(actual.Request.Arguments.Size)})
		if err != nil || snapshot.Accounting.UpdatedAt.Before(actual.Request.CreatedAt) {
			state.mu.Unlock()
			return domain.ErrMembershipDenied
		}
		// Consent append was AFTER the last operation reservation. Keep its
		// actual authenticated accounting timestamp rather than inventing one.
		next.UpdatedAt = snapshot.Accounting.UpdatedAt
	}
	current.Accounting = expected.Accounting
	current.Consents = expected.Consents
	current.Operations = current.Operations[:len(current.Operations)-1]
	current.ProxyDigests = current.ProxyDigests[:len(current.ProxyDigests)-1]
	if len(current.Operations) == 0 && len(expected.Operations) == 0 {
		// The first stopped consent leaves exactly the authenticated MODEL
		// above, but its original pre-MODEL source had nil empty inventories.
		// Match that representation after removing ONLY that proved tail.
		current.Operations = expected.Operations
	}
	if len(current.ProxyDigests) == 0 && len(expected.ProxyDigests) == 0 {
		current.ProxyDigests = expected.ProxyDigests
	}
	matched := reflect.DeepEqual(next, snapshot.Accounting) && reflect.DeepEqual(current, expected)
	state.mu.Unlock()
	if !matched {
		return domain.ErrMembershipDenied
	}
	view, err := e.materializeCompactionCanonical(ctx, snapshot)
	if err != nil {
		return err
	}
	equal := view.Digest() == priorView
	view.Clear()
	if !equal {
		return domain.ErrMembershipDenied
	}
	h.snapshot = snapshot
	h.run = snapshot.Job.Run.Clone()
	_, digest, err := accountingBytes(snapshot.Accounting)
	if err != nil {
		return err
	}
	a := ports.OwnedOperationFailureAccountingV4{Version: 4, Scope: ownedOperationScope(e.parent), AccountingDigest: digest, AggregateDebit: snapshot.Accounting.Aggregate, ComponentAmounts: snapshot.Accounting.Amounts}
	for _, state := range snapshot.Consents.States {
		h.search.consents = append(h.search.consents, state.Clone())
		a.ConsentReservedBytes += ownedConsentMetadataReservationBytes + uint64(state.Request.Arguments.Size)
	}
	a.ConsentStatesDigest, err = localFailureV4Digest(h.search.consents)
	if err != nil {
		return err
	}
	for i, row := range snapshot.Operations {
		if row.observation == nil || !h.authenticatedSearchRowV4(row, uint64(len(snapshot.Operations))) {
			return domain.ErrMembershipDenied
		}
		r, o := row.reservation, *row.observation
		if chargeOwnedAuthorityBody(ctx, 8*(r.Operation.RequestBytes+o.ResponseBytes)) != nil {
			return domain.ErrMembershipDenied
		}
		request, err := e.owner.store.readOwnedOperationBody(ctx, r.Operation.Scope, r.Operation.RequestRef, r.Operation.RequestDigest, r.Operation.RequestBytes, 1<<20)
		if err != nil {
			return err
		}
		var response []byte
		if o.Outcome == domain.AttemptOperationCompletedV1 {
			response, err = e.owner.store.readOwnedOperationBody(ctx, r.Operation.Scope, o.ResponseRef, o.ResponseDigest, o.ResponseBytes, min(r.Operation.ResultBytes, uint64(64<<10)))
			if err != nil {
				clear(request)
				return err
			}
		} else if o.ResponseRef != "" || o.ResponseDigest != "" || o.ResponseBytes != 0 {
			clear(request)
			return domain.ErrMembershipDenied
		}
		item := LocalCompactionOperationV1{Reservation: r.Clone(), Observation: o.Clone(), Request: request, Response: response}
		valid := false
		switch r.Operation.Kind {
		case domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1:
			var price *ownedProviderPriceEvidence
			if m := e.preparedModel.Load(); m != nil {
				price = m.price
			}
			valid = price != nil && price.local != nil && validateOwnedLocalInvoiceContent(price.local, price.profile, e.source, r.Operation, r.ReservedAt) == nil
			if o.Outcome == domain.AttemptOperationCompletedV1 {
				decoded, de := configuredinference.DecodeNormalizedResponseBodyV1(e.owner.configuration, request, response, r.Operation.Kind)
				usage, ue := decoded.Usage.OperationUsage()
				if decoded.Call != nil {
					clear(decoded.Call.Arguments)
				}
				valid = valid && de == nil && ue == nil && reflect.DeepEqual(usage, o.Usage)
			}
			if r.Operation.Kind == domain.AttemptOperationModelV1 {
				catalog, ce := compactioncontext.ReadModelCatalogV3(r.Operation.Sequence, request)
				_, te := compactioncontext.DecodeModelCatalogV3(catalog.Tools, e.owner.configuration.Limits)
				valid = valid && ce == nil && te == nil
				h.search.catalogs = append(h.search.catalogs, localFailureModelCatalogV4{Sequence: catalog.Sequence, Tools: catalog.Tools})
			}
		case domain.AttemptOperationMCPToolV1:
			if i > 0 {
				result, re := mcpadmission.DecodeSealedToolResultDataV1(r, o, response)
				previous := h.ops[i-1]
				valid = re == nil && !result.IsError() && result.ValidateConfiguredCallV1(e.owner.configuration, previous.Reservation, previous.Observation, previous.Request, previous.Response) == nil && e.originalObservationAllowsSuccessor(r, o)
			}
		case domain.AttemptOperationWebSearchV1:
			valid = i > 0 && h.validateOriginalSearchFailureMaterialV4(h.ops[i-1], item) == nil
		}
		if !valid {
			clear(request)
			clear(response)
			return domain.ErrMembershipDenied
		}
		h.ops = append(h.ops, item)
		projected, err := ports.BuildOwnedOperationFailureRowV4(r, o, e.parent, e.source)
		if err != nil {
			return err
		}
		a.Operations = append(a.Operations, projected)
	}
	if a.ValidateForStop(e.parent, e.source, h.search.stop) != nil {
		return domain.ErrMembershipDenied
	}
	h.search.accounting = a
	return nil
}

func (h *LocalFailureV1) validateOriginalSearchFailureMaterialV4(previous, item LocalCompactionOperationV1) error {
	e := h.entry
	r, o, op := item.Reservation, item.Observation, item.Reservation.Operation
	m := e.findOriginalSearch(op.Sequence)
	if m == nil || m.pending == nil || m.pending.grant == nil || !reflect.DeepEqual(m.operation, op) || !reflect.DeepEqual(m.reservation, r) || !reflect.DeepEqual(m.observation.Observation, o) || !reflect.DeepEqual(previous.Reservation, m.pending.model) || !reflect.DeepEqual(previous.Observation, m.pending.observation) || !m.exactRequest(item.Request) {
		return domain.ErrMembershipDenied
	}
	p := m.pending
	state := consentsForRequest(h.search.consents, p.request.ID)
	if state.Validate() != nil || state.Decision == nil || state.Decision.Choice != domain.WebSearchConsentApproveV1 || !sameHistoricalConsentFactsV4(p.state, state) || p.request.ValidateArguments(p.arguments) != nil {
		return domain.ErrMembershipDenied
	}
	cost := e.parent.Authority.AdmissionCostCeiling.WebSearchCost
	if cost == nil || p.grant.grant.ValidateCostAt(*cost, r.ReservedAt) != nil || state.Request.ConsentRevision != p.grant.grant.ConsentRevision || state.Request.RevocationGeneration != p.grant.grant.RevocationGeneration || op.Deadline.After(p.grant.grant.ExpiresAt) {
		return domain.ErrMembershipDenied
	}
	compiled, err := websearch.CompileV2(p.grant.grant, *cost, websearch.BindingV1{Scope: op.Scope, Sequence: op.Sequence, CallID: op.CallID, ModelResponseDigest: op.ModelResponseDigest, InitiatingUserID: p.request.InitiatingUserID, ParentCostCeilingDigestV2: op.AdmissionCostCeilingDigest, ParentPriceRevision: op.PriceRevision}, p.arguments, r.ReservedAt)
	if err != nil || compiled.ValidateOperation(op) != nil || compiled.ValidateConsentRequest(p.request) != nil {
		return domain.ErrMembershipDenied
	}
	wire := compiled.Body()
	defer clear(wire)
	if !bytes.Equal(wire, item.Request) {
		return domain.ErrMembershipDenied
	}
	decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(e.owner.configuration, previous.Request, previous.Response, domain.AttemptOperationModelV1)
	if decoded.Call != nil {
		defer clear(decoded.Call.Arguments)
	}
	if err != nil || decoded.Call == nil || decoded.Call.Name != "web_search" || decoded.Call.ID != op.CallID || !bytes.Equal(decoded.Call.Arguments, p.arguments) {
		return domain.ErrMembershipDenied
	}
	if o.Outcome == domain.AttemptOperationCompletedV1 {
		result, err := domain.DecodeWebSearchResultV1(item.Response)
		if err != nil || result.ValidateForOperation(op) != nil || result.ProfileRevision != p.grant.grant.ProfileRevision || result.RetrievedAt.Before(r.ReservedAt) || result.RetrievedAt.After(o.ObservedAt) || (result.Outcome == "known_service_error") != (o.Reason == domain.AttemptOperationReasonServiceErrorV1) {
			return domain.ErrMembershipDenied
		}
	}
	return nil
}

// The actual original stop owner, after synchronous keeper/bridge teardown and
// capture, uses the same strict V4 representation already mounted on history
// and deletion. No future observation or public failure DTO enters this path.
func (h *LocalFailureV1) prepareSearchFailureOutputV4(bodies []domain.BlobRef) error {
	if !h.original() || h.version != 4 || h.search == nil || len(h.ops) == 0 || h.snapshot.Consents == nil || !reflect.DeepEqual(h.search.consents, h.snapshot.Consents.States) {
		return domain.ErrMembershipDenied
	}
	e := h.entry
	pd, err := e.owner.configuration.Digest()
	if err != nil {
		return err
	}
	c := localFailureCapsuleV4{Schema: "sessionless.terminal-failure-custody.v4", Version: 4, Parent: e.parent.Clone(), Source: e.source.Clone(), ProfileDigest: pd, Stop: h.search.stop, Cleanup: "joined_retained", CapturedAt: h.capturedAt, Accounting: h.search.accounting, Consents: h.search.consents, ModelCatalogs: h.search.catalogs, Events: []localCapsuleEventV1{}, Bodies: bodies, Derived: h.snapshot.Job.Run.Clone().DerivedContext}
	for _, item := range h.ops {
		normalized := json.RawMessage(item.Response)
		if item.Observation.Outcome != domain.AttemptOperationCompletedV1 {
			normalized = json.RawMessage("null")
		}
		c.Operations = append(c.Operations, localFailureOperationV1{Reservation: item.Reservation.Clone(), Observation: item.Observation.Clone(), Normalized: normalized})
	}
	c.FrontierDigest, err = localFailureV4Frontier(c)
	if err != nil {
		return err
	}
	frontiers, err := localFailureV4ConsentFrontiers(c)
	if err != nil || validateFailureV4Frontiers(c, h.snapshot.Accounting, frontiers) != nil {
		return domain.ErrMembershipDenied
	}
	// Use the actual authenticated job limits, not the historical reader's
	// maximum language. Both raw and encoded pairs are bounded by this encoder.
	h.canonical, err = localFailureV4Plan(h.snapshot.Job.Run, c, h.snapshot.Job.Job.Limits)
	if err != nil {
		return err
	}
	h.run = h.snapshot.Job.Run.Clone()
	for _, event := range h.canonical.Events() {
		draft, err := canonicaloutput.PlanEventDraftV1(h.run, event.ID, event.Kind, event.Payload, h.capturedAt)
		if err != nil {
			return err
		}
		h.drafts = append(h.drafts, draft)
		c.Events = append(c.Events, localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText})
	}
	h.capsule, err = encodeLocalFailureCapsuleV4(c, e.terminalOutputs.CapsuleLimit())
	if err != nil {
		return err
	}
	sha := rawLocalFailureHash(h.capsule)
	key, err := domain.TerminalCustodyObjectKeyV1(ownedOperationScope(e.parent), sha)
	if err != nil {
		return err
	}
	h.plan = domain.TerminalCustodyPlanV1{Scope: ownedOperationScope(e.parent), FrontierDigest: c.FrontierDigest, Capsule: domain.BlobRef{TenantID: h.run.TenantID, Key: key, Size: int64(len(h.capsule)), SHA256: sha}}
	if h.plan.Validate() != nil {
		return domain.ErrMembershipDenied
	}
	h.metadata, err = localFailureV4Metadata(h.plan, c, "")
	if err != nil {
		return err
	}
	h.search.capsule = c
	h.prepared = true
	return nil
}
