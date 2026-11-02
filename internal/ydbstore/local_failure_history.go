package ydbstore

import (
	"bytes"
	"context"
	"reflect"
	"strings"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

const localFailureHistorySQL = `SELECT
 SUBSTRING(p.owned_record,0,32769),SUBSTRING(CAST(p.owned_digest AS String),0,65),
 SUBSTRING(a.record,0,32769),SUBSTRING(CAST(a.digest AS String),0,65),
 SUBSTRING(CAST(f.terminal_status AS String),0,33),SUBSTRING(CAST(f.content_digest AS String),0,74),
 SUBSTRING(p.operation_accounting_record,0,32769),SUBSTRING(CAST(p.operation_accounting_digest AS String),0,65),
 SUBSTRING(p.allocation_original_record,0,32769),SUBSTRING(CAST(p.allocation_original_digest AS String),0,65),
 SUBSTRING(p.allocation_current_record,0,32769),SUBSTRING(CAST(p.allocation_current_digest AS String),0,65)
 FROM attempt_effect_reservations AS p
 INNER JOIN attempt_execution_admissions_v1 AS a ON a.tenant_id=p.tenant_id AND a.attempt_id=p.attempt_id
 INNER JOIN run_finalizations AS f ON f.tenant_id=p.tenant_id AND f.run_id=p.run_id
 WHERE p.tenant_id=$1 AND p.attempt_id=$2 AND p.kind=$3`

type localFailureHistoryV1 struct {
	Run               domain.Run
	Anchor            domain.SessionEvent
	Parent            domain.OwnedAttemptEffectParentV1
	Admission         domain.AttemptExecutionAdmissionV1
	Accounting        ownedAttemptAccounting
	Original, Retired domain.RegisteredAllocationV2
	Seal              string
}

func readHistoricalFailure(ctx context.Context, q *ownedAuthorityQuery, run domain.Run, anchor domain.SessionEvent) (localFailureHistoryV1, error) {
	deny := func() (localFailureHistoryV1, error) { return localFailureHistoryV1{}, q.budget.deny() }
	if run.Validate() != nil || run.TerminalFailure == nil || run.Status != domain.RunFailed || anchor.Kind != domain.SessionEventSystemNotice || anchor.RunID == nil || *anchor.RunID != run.ID || run.FinishedAt == nil || !run.FinishedAt.Equal(run.UpdatedAt) || run.UpdatedAt.After(q.at) {
		return deny()
	}
	scope := run.TerminalFailure.Scope
	var parentRaw, pd, admissionRaw, ad, status, marker, accountingRaw, accountingDigest, originalRaw, originalDigest, retiredRaw, retiredDigest string
	if q.scanMetadata(ctx, localFailureHistorySQL, []any{scope.TenantID, scope.AttemptID, domain.ProviderEffectTurnV1}, &parentRaw, &pd, &admissionRaw, &ad, &status, &marker, &accountingRaw, &accountingDigest, &originalRaw, &originalDigest, &retiredRaw, &retiredDigest) != nil {
		return deny()
	}
	v := localFailureHistoryV1{Run: run.Clone(), Anchor: anchor, Seal: marker}
	var err error
	v.Parent, err = domain.DecodeOwnedAttemptEffectParentV1([]byte(parentRaw))
	if err != nil || decodeOwnedEffectCanonicalJSON(admissionRaw, &v.Admission) != nil || decodeOwnedEffectCanonicalJSON(accountingRaw, &v.Accounting) != nil {
		return deny()
	}
	v.Original, err = domain.DecodeRegisteredAllocationV2([]byte(originalRaw))
	if err != nil {
		return deny()
	}
	v.Retired, err = domain.DecodeRegisteredAllocationV2([]byte(retiredRaw))
	if err != nil {
		return deny()
	}
	pActual, pe := v.Parent.Digest()
	aActual, ae := v.Admission.Digest()
	oActual, oe := v.Original.Digest()
	rActual, re := v.Retired.Digest()
	_, ac, ce := accountingBytes(v.Accounting)
	if pe != nil || ae != nil || oe != nil || re != nil || ce != nil || pActual != pd || aActual != ad || string(oActual) != originalDigest || string(rActual) != retiredDigest || ac != accountingDigest || v.Parent.ValidateForAdmission(v.Admission) != nil || ownedOperationScope(v.Parent) != scope || v.Accounting.validate(v.Parent, v.Admission.Source, q.at) != nil || v.Original.ValidateInitial() != nil || v.Retired.State != domain.RegisteredPreparationRetiredV1 || v.Retired.ValidateTransitionFrom(v.Original) != nil || v.Retired.UpdatedAt.After(q.at) || retiredDigest != run.TerminalFailure.RetiredAllocationDigest || status != string(domain.RunFailed) {
		return deny()
	}
	prepared := domain.PreparedAllocationV2{Version: domain.PreparedAllocationVersionV2, SubstrateBindingDigest: v.Original.ParentReservation.SubstrateBindingDigest, ObservedOuterHarnessDigest: v.Original.ObservedOuterHarnessDigest, ObservedProxyArtifactDigest: v.Original.ObservedProxyArtifactDigest, ObservedProxyIdentityDigest: v.Original.ObservedProxyIdentityDigest, WorkloadMode: v.Original.WorkloadMode, LocalRuntime: v.Original.LocalRuntime}
	if v.Original.LocalRuntime == nil || v.Original.ValidateForAuthority(v.Parent.Authority, v.Parent.Reservation, prepared, v.Original.PreparedInvocationDigest, v.Parent.PreparationExpiresAt, v.Admission.Source) != nil || v.Retired.UpdatedAt.Before(run.TerminalFailure.CapturedAt) || v.Retired.UpdatedAt.After(run.UpdatedAt) {
		return deny()
	}
	if _, err := decodeLocalFailureMarker(marker); err != nil {
		return deny()
	}
	return v, nil
}

func (s *LocalCompactionSourceV1) readFailureHistories(ctx context.Context, q *ownedAuthorityQuery, records []sessioncontext.EventPayload, ids []domain.RunID, byRun map[domain.RunID]domain.Run, markers, statuses map[domain.RunID]string) ([]localFailureHistoryV1, error) {
	var out []localFailureHistoryV1
	for _, id := range ids {
		run, found := byRun[id]
		if !found {
			return nil, q.budget.deny()
		}
		marked := strings.HasPrefix(markers[id], localFailureFinalizationPrefix)
		stop := domain.KnownMCPServiceErrorV1
		if run.TerminalFailure != nil {
			stop = run.TerminalFailure.Stop
		}
		notice, err := canonicaloutput.EncodeFailureV1(id, false, stop)
		if err != nil {
			return nil, q.budget.deny()
		}
		unknownNotice, err := canonicaloutput.EncodeFailureV1(id, false, domain.ModelOutcomeUnknownV2)
		if err != nil {
			clear(notice.Payload)
			return nil, q.budget.deny()
		}
		var anchor domain.SessionEvent
		anchors, matching, closedNotice := 0, 0, 0
		for _, record := range records {
			e := record.Event
			if e.RunID != nil && *e.RunID == id && e.Kind == domain.SessionEventSystemNotice {
				anchors++
				anchor = e
				if bytes.Equal(notice.Payload, record.Payload) {
					matching++
				}
				if bytes.Equal(notice.Payload, record.Payload) || bytes.Equal(unknownNotice.Payload, record.Payload) {
					closedNotice++
				}
			}
		}
		clear(notice.Payload)
		clear(unknownNotice.Payload)
		if run.TerminalFailure == nil && !marked {
			// Ordinary legacy notices retain their old language. The newly closed
			// known-error notice cannot masquerade as one without F1 correspondence.
			if closedNotice != 0 {
				return nil, q.budget.deny()
			}
			continue
		}
		if run.TerminalFailure == nil || !marked || statuses[id] != string(domain.RunFailed) || anchors != 1 || matching != 1 || run.TerminalFailure.Scope.OwnerUserID != s.entry.parent.Scope.OwnerUserID || run.SessionID != s.entry.parent.Scope.SessionID {
			return nil, q.budget.deny()
		}
		v, err := readHistoricalFailure(ctx, q, run, anchor)
		if err != nil || v.Seal != markers[id] {
			return nil, q.budget.deny()
		}
		out = append(out, v)
	}
	return out, nil
}

func validateFailureHistoryData(v localFailureHistoryV1, c localFailureCapsule, records []sessioncontext.EventPayload) error {
	deny := func() error { return domain.ErrMembershipDenied }
	run := v.Run
	if run.TerminalFailure == nil || run.TerminalCustody == nil || !reflect.DeepEqual(c.Parent, v.Parent) || !reflect.DeepEqual(c.Source, v.Admission.Source) || !domain.EqualRunDerivedContextV1(c.Derived, run.DerivedContext) {
		return deny()
	}
	_, ad, err := accountingBytes(v.Accounting)
	if err != nil || ad != c.Accounting.AccountingDigest || v.Accounting.Aggregate != c.Accounting.AggregateDebit || v.Accounting.Amounts != c.Accounting.ComponentAmounts {
		return deny()
	}
	metadata, err := failureMetadata(run.TerminalCustody.Plan, c, run.TerminalFailure.RetiredAllocationDigest)
	if err != nil || metadata != *run.TerminalFailure {
		return deny()
	}
	p, err := localFailurePlan(run, domain.ProductLimits{MaxToolEvents: 8, MaxToolEventBytes: domain.MaxTerminalCapsuleBytesV1}, c)
	if err != nil {
		return err
	}
	defer p.Release()
	events := p.Events()
	if len(events) < 3 || len(events) > 9 || len(events)%2 != 1 || len(c.Events) != len(events) || events[len(events)-1].ID != v.Anchor.ID || v.Anchor.Sequence < uint64(len(events)) {
		return deny()
	}
	actual := make(map[domain.SessionEventID]sessioncontext.EventPayload)
	for _, r := range records {
		if r.Event.RunID != nil && *r.Event.RunID == run.ID && (r.Event.Kind == domain.SessionEventToolCall || r.Event.Kind == domain.SessionEventToolResult || r.Event.Kind == domain.SessionEventSystemNotice || r.Event.Kind == domain.SessionEventAssistantMessage) {
			if _, exists := actual[r.Event.ID]; exists {
				return deny()
			}
			actual[r.Event.ID] = r
		}
	}
	if len(actual) != len(events) {
		return deny()
	}
	for i, event := range events {
		r, ok := actual[event.ID]
		identity := c.Events[i]
		if !ok || identity.ID != event.ID || identity.Kind != event.Kind || identity.Payload != r.Event.Payload || identity.IdempotencyKey != r.Event.IdempotencyKey || r.Event.TenantID != run.TenantID || r.Event.SessionID != run.SessionID || r.Event.Kind != event.Kind || r.Event.Sequence != v.Anchor.Sequence-uint64(len(events)-1-i) || !r.Event.CreatedAt.Equal(v.Anchor.CreatedAt) || !bytes.Equal(r.Payload, event.Payload) {
			return deny()
		}
	}
	seal, err := localFailureSeal(run, c.Events)
	if err != nil || seal != v.Seal {
		return deny()
	}
	original, err := v.Original.Digest()
	if err != nil {
		return deny()
	}
	for _, op := range c.Operations {
		if op.Reservation.RegisteredAllocationDigest != original {
			return deny()
		}
		for _, required := range []struct {
			ref    string
			bytes  uint64
			digest string
		}{{op.Reservation.Operation.RequestRef, op.Reservation.Operation.RequestBytes, op.Reservation.Operation.RequestDigest}, {op.Observation.ResponseRef, op.Observation.ResponseBytes, op.Observation.ResponseDigest}} {
			// The V2 plan validator has already proved the exact UNKNOWN MODEL
			// tail and explicit normalized:null. It owns a request, no response.
			if required.ref == "" && c.Version == 2 && op.Observation.Outcome == domain.AttemptOperationAcceptedOutcomeUnknownV1 && required.bytes == 0 && required.digest == "" {
				continue
			}
			found := false
			for _, body := range c.Bodies {
				if strings.HasSuffix(body.Key, "/"+required.ref) && body.Size == int64(required.bytes) && body.SHA256 == required.digest {
					found = true
					break
				}
			}
			if !found {
				return deny()
			}
		}
	}
	return nil
}

func (s *LocalCompactionSourceV1) materializeFailureHistories(ctx context.Context) error {
	h := s.state.history
	if h == nil {
		return domain.ErrMembershipDenied
	}
	if h.failuresMaterialized {
		return nil
	}
	records := s.state.view.Records()
	defer func() {
		for _, r := range records {
			clear(r.Payload)
		}
	}()
	if chargeOwnedAuthorityBody(ctx, s.state.view.Bytes()) != nil {
		return domain.ErrMembershipDenied
	}
	for _, v := range h.failures {
		profile, err := s.entry.owner.configuration.Digest()
		if err != nil || profile != v.Parent.Authority.HarnessBinding.Backend.BackendProfileDigest {
			return domain.ErrMembershipDenied
		}
		body, err := s.entry.readCompactionBlob(ctx, v.Run.TerminalCustody.Plan.Capsule, domain.MaxTerminalCapsuleBytesV1)
		if err != nil {
			return err
		}
		c, err := decodeLocalFailureCapsule(body)
		clear(body)
		valid := err == nil && validateFailureHistoryData(v, c, records) == nil
		clearLocalFailureNormalized(c)
		if !valid {
			return domain.ErrMembershipDenied
		}
	}
	h.failuresMaterialized = true
	return nil
}
