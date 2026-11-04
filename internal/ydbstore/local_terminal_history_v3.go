package ydbstore

import (
	"bytes"
	"context"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func (s *LocalCompactionSourceV1) validateHistoricalCapsuleV3(ctx context.Context, v localTerminalHistorySnapshot, c localRetainedTerminalCapsuleV3) error {
	if s.state.view == nil || chargeOwnedAuthorityBody(ctx, s.state.view.Bytes()) != nil {
		return domain.ErrMembershipDenied
	}
	records := s.state.view.Records()
	defer func() {
		for _, record := range records {
			clear(record.Payload)
		}
	}()
	return validateHistoricalCapsuleDataV3(v, c, records)
}

// The original prepared output/finalizer authenticated grant, receipt and wire
// before erasure and sealed the exact capsule. This reader consumes that actual
// persisted parent/admission/evidence/accounting/seal; it never recovers an
// erased request, consults today's grant or fabricates a cleanup capability.
func validateHistoricalCapsuleDataV3(v localTerminalHistorySnapshot, c localRetainedTerminalCapsuleV3, records []sessioncontext.EventPayload) error {
	deny := func() error { return domain.ErrMembershipDenied }
	run, custody := v.Run, v.Run.TerminalCustody
	if run.Validate() != nil || run.Status != domain.RunSucceeded || run.CancellationRequestedAt != nil || custody == nil || custody.Correspondence == nil || custody.LocalEvidence == nil || c.Schema != "sessionless.terminal-custody.v3" || c.CodecVersion != 3 || c.Scope != custody.Plan.Scope || c.ProfileDigest != custody.LocalEvidence.ProfileDigest || c.FrontierDigest != custody.Plan.FrontierDigest || !domain.EqualRunDerivedContextV1(c.DerivedContext, run.DerivedContext) || !reflect.DeepEqual(c.PublicManifest, v.Manifest) || len(c.Events) < 3 || len(c.Events) > 9 || len(c.Operations) < 3 || len(c.Operations) > 12 || uint64(len(c.Operations)) != v.Accounting.Aggregate.Operations || c.Accounting.ValidateFor(v.Parent, v.Admission.Source) != nil {
		return deny()
	}
	_, actualAccounting, err := accountingBytes(v.Accounting)
	disposition, de := c.Accounting.Digest()
	if err != nil || de != nil || actualAccounting != c.Accounting.AccountingDigest || c.Accounting.AggregateDebit != v.Accounting.Aggregate || c.Accounting.ComponentAmounts != v.Accounting.Amounts || disposition != custody.Correspondence.AccountingDispositionDigest || disposition != custody.LocalEvidence.AccountingDispositionDigest || len(c.Accounting.Operations) != len(c.Operations) {
		return deny()
	}
	frontiers := make([]uint64, len(c.Consents))
	for i, state := range c.Consents {
		if state.Request.Scope != ownedConsentScope(v.Parent) {
			return deny()
		}
		rd, _ := state.Request.Digest()
		for j, item := range c.Operations {
			if item.Reservation.Operation.Kind == domain.AttemptOperationWebSearchV1 && item.Reservation.Operation.ConsentRequestDigest == string(rd) {
				if j == 0 || frontiers[i] != 0 || c.Operations[j-1].Reservation.Operation.Kind != domain.AttemptOperationModelV1 {
					return deny()
				}
				frontiers[i] = c.Operations[j-1].Reservation.Operation.Sequence
			}
		}
		if frontiers[i] == 0 || i > 0 && frontiers[i] <= frontiers[i-1] {
			return deny()
		}
	}
	baseline := v.Accounting
	baseline.Aggregate = domain.AttemptOperationDebitV1{BodyBytes: ownedSetupBodyBytes(v.Parent)}
	baseline.Amounts = [4]uint64{0, *v.Admission.Source.Spending[1].MaxAmountMicrounits, 0, 0}
	baseline.UpdatedAt = baseline.SetupReservedAt
	var charged uint64
	ops := make([]LocalCompactionOperationV1, len(c.Operations))
	for i, item := range c.Operations {
		r, o := item.Reservation, item.Observation
		for j, frontier := range frontiers {
			if frontier == uint64(i) {
				request := c.Consents[j].Request
				charge := ownedConsentMetadataReservationBytes + uint64(request.Arguments.Size)
				if request.CreatedAt.Before(baseline.UpdatedAt) || request.CreatedAt.After(r.ReservedAt) {
					return deny()
				}
				baseline.Aggregate, err = baseline.Aggregate.Add(domain.AttemptOperationDebitV1{BodyBytes: charge})
				if err != nil {
					return deny()
				}
				baseline.UpdatedAt = request.CreatedAt
				charged += charge
			}
		}
		row, err := ports.BuildOwnedOperationAccountingRowV4(r, o, v.Parent, v.Admission.Source)
		if err != nil || r.Operation.Scope != c.Scope || r.Operation.Sequence != uint64(i+1) || !reflect.DeepEqual(row, c.Accounting.Operations[i]) || r.ReservedAt.Before(baseline.UpdatedAt) || o.ObservedAt.After(custody.LocalEvidence.StoppedAt) || i > 0 && (r.Operation.ValidatePredecessor(c.Operations[i-1].Reservation, c.Operations[i-1].Observation) != nil || r.ReservedAt.Before(c.Operations[i-1].Observation.ObservedAt)) {
			return deny()
		}
		baseline.Aggregate, err = baseline.Aggregate.Add(r.Operation.Debit)
		component := 0
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			component = 2
		} else if r.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			component = 3
		}
		if err != nil || baseline.Amounts[component] > ^uint64(0)-r.Operation.Debit.AmountMicrounits {
			return deny()
		}
		baseline.Amounts[component] += r.Operation.Debit.AmountMicrounits
		baseline.UpdatedAt = r.ReservedAt
		if baseline.Aggregate != r.AggregateDebit || baseline.Amounts != r.ComponentAmounts {
			return deny()
		}
		ops[i] = LocalCompactionOperationV1{Reservation: r, Observation: o, Response: item.NormalizedBody}
	}
	if charged != c.Accounting.ConsentReservedBytes || !reflect.DeepEqual(baseline, v.Accounting) {
		return deny()
	}
	encoded, err := buildLocalCanonicalPlanV3(run, domain.ProductLimits{MaxToolEvents: 8, MaxToolEventBytes: domain.MaxTerminalCapsuleBytesV1}, c.ProfileDigest, nil, v.Admission.Source, ops, c.Consents, c.ModelCatalogs, v.Manifest.ID)
	if err != nil {
		return deny()
	}
	defer encoded.Release()
	events := encoded.Events()
	if len(events) != len(c.Events) || v.Anchor.Sequence < uint64(len(events)) || v.Anchor.ID != events[len(events)-1].ID || v.Anchor.Kind != domain.SessionEventAssistantMessage {
		return deny()
	}
	drafts := make([]domain.SessionEventDraft, len(events))
	byID := make(map[domain.SessionEventID]int, len(events))
	for i, event := range events {
		draft, err := canonicaloutput.PlanEventDraftV1(run, event.ID, event.Kind, event.Payload, v.Anchor.CreatedAt)
		draft.DisplayText = event.DisplayText
		if err != nil || c.Events[i] != (localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText}) {
			return deny()
		}
		drafts[i], byID[event.ID] = draft, i
	}
	seen := make(map[domain.SessionEventID]bool, len(events))
	seenTrigger := false
	for _, record := range records {
		if record.Event.RunID == nil || *record.Event.RunID != run.ID {
			continue
		}
		// The original user trigger is authenticated canonical INPUT, not an
		// output of this terminal capsule. Normal Web assigns it this RunID too.
		// Recognize only that exact original ID/scope/owner strictly before
		// the sealed output; other same-Run user/notice records still deny.
		if record.Event.ID == run.TriggerEventID {
			e := record.Event
			if seenTrigger || e.Validate() != nil || e.Kind != domain.SessionEventUserMessage || e.TenantID != run.TenantID || e.SessionID != run.SessionID || e.AuthorUserID == nil || *e.AuthorUserID != v.Admission.Source.Scope.OwnerUserID || e.Sequence >= v.Anchor.Sequence-uint64(len(events)-1) || e.CreatedAt.After(v.Anchor.CreatedAt) || int64(len(record.Payload)) != e.Payload.Size || rawLocalFailureHash(record.Payload) != e.Payload.SHA256 {
				return deny()
			}
			seenTrigger = true
			continue
		}
		i, exists := byID[record.Event.ID]
		if !exists || seen[record.Event.ID] {
			return deny()
		}
		draft, event := drafts[i], record.Event
		if event.TenantID != run.TenantID || event.SessionID != run.SessionID || event.Kind != draft.Kind || event.IdempotencyKey != draft.IdempotencyKey || event.Payload != draft.Payload || !event.CreatedAt.Equal(draft.CreatedAt) || event.Sequence != v.Anchor.Sequence-uint64(len(events)-1-i) || !bytes.Equal(record.Payload, events[i].Payload) || i == len(events)-1 && !reflect.DeepEqual(event, v.Anchor) {
			return deny()
		}
		seen[event.ID] = true
	}
	if len(seen) != len(events) {
		return deny()
	}
	digest, err := terminalCanonicalEventsDigestV1(drafts)
	seal, se := runFinalizationDigestV2(run.Status, &v.Manifest, drafts, &v.Evidence, run.DerivedContext, custody)
	if se == nil {
		seal, se = encodeLocalFinalizationMarkerV2(seal)
	}
	if err != nil || se != nil || digest != custody.Correspondence.CanonicalEventsDigest || seal != v.Seal {
		return deny()
	}
	return nil
}

func historicalCapsuleRunV3(c localRetainedTerminalCapsuleV3, anchor domain.SessionEventID) (compactioncontext.HistoricalRunV3, error) {
	value := compactioncontext.HistoricalRunV3{Version: 3, Scope: c.Scope, Anchor: anchor, ModelCatalogs: c.ModelCatalogs}
	for i, item := range c.Operations {
		value.Operations = append(value.Operations, compactioncontext.HistoricalOperationV1{Reservation: item.Reservation.Clone(), Observation: item.Observation.Clone(), Normalized: item.NormalizedBody, Projection: item.Projection})
		for _, id := range item.EventIDs {
			for _, event := range c.Events {
				if id == event.ID {
					value.Projections = append(value.Projections, compactioncontext.CanonicalProjectionV2{OperationSequence: item.Reservation.Operation.Sequence, EventID: event.ID, Kind: event.Kind, Payload: event.Payload})
				}
			}
		}
		r, o, row := item.Reservation, item.Observation, c.Accounting.Operations[i]
		witness := compactioncontext.ToolWitnessV3{Kind: r.Operation.Kind, Scope: c.Scope, Sequence: r.Operation.Sequence}
		switch r.Operation.Kind {
		case domain.AttemptOperationMCPToolV1:
			rd, _ := r.Digest()
			od, _ := o.Digest()
			witness.MCP = &compactioncontext.MCPConservativeDataV2{Version: 2, Scope: c.Scope, Sequence: r.Operation.Sequence, ReservationDigest: rd, ObservationDigest: od, Reserved: r.Operation.Debit, MCPSpendingDigest: row.ToolSpendingDigest}
		case domain.AttemptOperationWebSearchV1:
			typed, err := domain.DecodeWebSearchResultV1(item.NormalizedBody)
			if err != nil {
				return compactioncontext.HistoricalRunV3{}, domain.ErrMembershipDenied
			}
			witness.Search = &compactioncontext.SearchToolDataV3{Accounting: row, Result: typed}
		default:
			continue
		}
		value.Tools = append(value.Tools, witness)
	}
	return value, nil
}
