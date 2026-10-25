package ydbstore

import (
	"bytes"
	"context"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func localHistoricalCanonicalSegment(run domain.Run, anchor domain.SessionEvent, trigger uint64, records []sessioncontext.EventPayload) bool {
	if trigger == 0 || anchor.Sequence <= trigger || anchor.Sequence-trigger > 9 || (anchor.Sequence-trigger)%2 != 1 {
		return false
	}
	next := trigger + 1
	for _, record := range records {
		e := record.Event
		if e.Sequence <= trigger || e.Sequence >= anchor.Sequence {
			continue
		}
		kind := domain.SessionEventToolCall
		if (next-trigger)%2 == 0 {
			kind = domain.SessionEventToolResult
		}
		if e.Sequence != next || e.Kind != kind || e.RunID == nil || *e.RunID != run.ID || e.TenantID != run.TenantID || e.SessionID != run.SessionID || !e.CreatedAt.Equal(anchor.CreatedAt) {
			return false
		}
		next++
	}
	return next == anchor.Sequence
}

func (s *LocalCompactionSourceV1) validateHistoricalCapsuleV2(ctx context.Context, v localTerminalHistorySnapshot, c localRetainedTerminalCapsuleV2) error {
	if s.state.view == nil || chargeOwnedAuthorityBody(ctx, s.state.view.Bytes()) != nil {
		return domain.ErrMembershipDenied
	}
	records := s.state.view.Records()
	defer func() {
		for _, record := range records {
			clear(record.Payload)
		}
	}()
	return validateHistoricalCapsuleDataV2(v, c, records, &s.entry.owner.configuration)
}

// Historical DATA validation never restores the old private funding witness.
// Parent/admission/custody/marker and every actual canonical payload must have
// been read by the original bounded serving/deletion owner before this call.
func validateHistoricalCapsuleDataV2(v localTerminalHistorySnapshot, c localRetainedTerminalCapsuleV2, records []sessioncontext.EventPayload, profile *domain.ConfiguredInferenceProfileV1) error {
	deny := func() error { return domain.ErrMembershipDenied }
	run, custody := v.Run, v.Run.TerminalCustody
	if run.Validate() != nil || run.Status != domain.RunSucceeded || run.CancellationRequestedAt != nil || custody == nil || custody.Correspondence == nil || custody.LocalEvidence == nil ||
		c.Schema != "sessionless.terminal-custody.v2" || c.CodecVersion != 2 || c.Scope != custody.Plan.Scope || c.ProfileDigest != custody.LocalEvidence.ProfileDigest || c.FrontierDigest != custody.Plan.FrontierDigest ||
		!domain.EqualRunDerivedContextV1(c.DerivedContext, run.DerivedContext) || !reflect.DeepEqual(c.PublicManifest, v.Manifest) || len(c.Events) < 3 || len(c.Events) > 9 ||
		len(c.Operations) < 3 || len(c.Operations) > 12 || uint64(len(c.Operations)) != v.Accounting.Aggregate.Operations || c.Accounting.ValidateFor(v.Parent, v.Admission.Source) != nil {
		return deny()
	}
	_, ad, err := accountingBytes(v.Accounting)
	disposition, de := c.Accounting.Digest()
	if err != nil || de != nil || ad != c.Accounting.AccountingDigest || c.Accounting.AggregateDebit != v.Accounting.Aggregate || c.Accounting.ComponentAmounts != v.Accounting.Amounts ||
		disposition != custody.Correspondence.AccountingDispositionDigest || disposition != custody.LocalEvidence.AccountingDispositionDigest || len(c.Accounting.Operations) != len(c.Operations) {
		return deny()
	}
	ops := make([]LocalCompactionOperationV1, len(c.Operations))
	baseline := v.Accounting
	baseline.Aggregate = domain.AttemptOperationDebitV1{BodyBytes: ownedSetupBodyBytes(v.Parent)}
	baseline.Amounts = [4]uint64{0, *v.Admission.Source.Spending[1].MaxAmountMicrounits, 0, 0}
	baseline.UpdatedAt = baseline.SetupReservedAt
	for i, item := range c.Operations {
		r, o := item.Reservation, item.Observation
		row, err := ports.BuildOwnedOperationAccountingRowV3(r, o, v.Parent, v.Admission.Source)
		if err != nil || r.Operation.Scope != c.Scope || r.Operation.Sequence != uint64(i+1) || !reflect.DeepEqual(row, c.Accounting.Operations[i]) ||
			r.ReservedAt.Before(baseline.UpdatedAt) || o.ObservedAt.After(custody.LocalEvidence.StoppedAt) || i > 0 && r.Operation.ValidatePredecessor(c.Operations[i-1].Reservation, c.Operations[i-1].Observation) != nil {
			return deny()
		}
		baseline, err = debitOwnedOperation(baseline, r.Operation, v.Parent, v.Admission.Source, r.ReservedAt)
		if err != nil || baseline.Aggregate != r.AggregateDebit || baseline.Amounts != r.ComponentAmounts {
			return deny()
		}
		if profile != nil && r.Operation.Kind != domain.AttemptOperationMCPToolV1 {
			decoded, err := configuredinference.DecodeRetainedNormalizedDataV1(*profile, r, o, item.NormalizedBody)
			if decoded.Call != nil {
				clear(decoded.Call.Arguments)
			}
			if err != nil {
				return deny()
			}
		}
		ops[i] = LocalCompactionOperationV1{Reservation: r, Observation: o, Response: item.NormalizedBody}
	}
	if !reflect.DeepEqual(baseline, v.Accounting) {
		return deny()
	}
	// Finite historical wire-decoding ceilings, not a current product budget.
	// The original writer had to meet its lower admitted Job.Limits before I/O.
	encoded, err := buildLocalCanonicalPlanV2(run, domain.ProductLimits{MaxToolEvents: 8, MaxToolEventBytes: domain.MaxTerminalCapsuleBytesV1}, c.ProfileDigest, nil, ops, v.Manifest.ID)
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
	for i, e := range events {
		d, err := canonicaloutput.PlanEventDraftV1(run, e.ID, e.Kind, e.Payload, v.Anchor.CreatedAt)
		d.DisplayText = e.DisplayText
		if err != nil || c.Events[i] != (localCapsuleEventV1{ID: d.ID, Kind: d.Kind, IdempotencyKey: d.IdempotencyKey, Payload: d.Payload, DisplayText: d.DisplayText}) {
			return deny()
		}
		drafts[i], byID[e.ID] = d, i
	}
	seen := make(map[domain.SessionEventID]bool, len(events))
	for _, record := range records {
		if record.Event.RunID == nil || *record.Event.RunID != run.ID {
			continue
		}
		i, exists := byID[record.Event.ID]
		if !exists || seen[record.Event.ID] {
			return deny()
		}
		d, e := drafts[i], record.Event
		if e.TenantID != run.TenantID || e.SessionID != run.SessionID || e.Kind != d.Kind || e.IdempotencyKey != d.IdempotencyKey || e.Payload != d.Payload ||
			!e.CreatedAt.Equal(d.CreatedAt) || e.Sequence != v.Anchor.Sequence-uint64(len(events)-1-i) || !bytes.Equal(record.Payload, events[i].Payload) {
			return deny()
		}
		if i == len(events)-1 && !reflect.DeepEqual(e, v.Anchor) {
			return deny()
		}
		seen[e.ID] = true
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

func historicalCapsuleRunV2(c localRetainedTerminalCapsuleV2, anchor domain.SessionEventID) compactioncontext.HistoricalRunV2 {
	v := compactioncontext.HistoricalRunV2{Scope: c.Scope, Anchor: anchor}
	for i, item := range c.Operations {
		v.Operations = append(v.Operations, compactioncontext.HistoricalOperationV1{Reservation: item.Reservation.Clone(), Observation: item.Observation.Clone(), Normalized: item.NormalizedBody, Projection: item.Projection})
		for _, id := range item.EventIDs {
			for _, event := range c.Events {
				if id == event.ID {
					v.Projections = append(v.Projections, compactioncontext.CanonicalProjectionV2{OperationSequence: item.Reservation.Operation.Sequence, EventID: event.ID, Kind: event.Kind, Payload: event.Payload})
				}
			}
		}
		if item.Reservation.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			rd, _ := item.Reservation.Digest()
			od, _ := item.Observation.Digest()
			v.ConservativeMCP = append(v.ConservativeMCP, compactioncontext.MCPConservativeDataV2{Version: 2, Scope: c.Scope, Sequence: item.Reservation.Operation.Sequence, ReservationDigest: rd, ObservationDigest: od, Reserved: item.Reservation.Operation.Debit, MCPSpendingDigest: c.Accounting.Operations[i].MCPSpendingDigest})
		}
	}
	return v
}
