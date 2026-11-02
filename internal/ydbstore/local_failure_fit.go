package ydbstore

import (
	"context"
	"encoding/json"
	"math"
	"strings"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

// Private length DATA, never signed, returned, persisted or accepted as an
// observation. The SAME closed F1 serializer counts its parent/source, complete
// frontier, full debit rows, retained-key inventory and pair/notice identities.
// This first arm is eligible only before a success candidate/batch exists, so
// there is no simultaneously writable late-success assistant residue.
func (entry *LocalInvocationEntryV1) knownFailureFitLocked(ctx context.Context, source *LocalCompactionSourceV1, op domain.AttemptOperationV1) error {
	if err := entry.failureFitLocked(ctx, source, op, false); err != nil {
		return err
	}
	if op.Kind == domain.AttemptOperationMCPToolV1 {
		return entry.failureFitLocked(ctx, source, op, true)
	}
	return nil
}

// Both initial failure alternatives use the SAME serializers and original
// inventory. They are selected before any terminal plan/batch can exist. No
// failure path may replace a success capsule or coexist with its batch PUTs.
func (entry *LocalInvocationEntryV1) failureFitLocked(ctx context.Context, source *LocalCompactionSourceV1, op domain.AttemptOperationV1, mcpUnknown bool) error {
	modelTail := op.Kind == domain.AttemptOperationModelV1 && localTerminalNeedsV2(source.state.operations, op)
	if op.Kind != domain.AttemptOperationMCPToolV1 && !modelTail {
		return nil
	}
	deny := func() error { return ErrTerminalCapacity }
	ops := source.state.operations
	if len(ops) == 0 || len(ops) >= 12 || op.Sequence != uint64(len(ops)+1) || source.state.snapshot.Job.Run.TerminalCustody != nil {
		return deny()
	}
	if !modelTail {
		previous := ops[len(ops)-1]
		decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(entry.owner.configuration, previous.Request, previous.Response, previous.Reservation.Operation.Kind)
		if decoded.Call != nil {
			defer clear(decoded.Call.Arguments)
		}
		if err != nil || decoded.Call == nil || decoded.Stop != configuredinference.StopToolV1 {
			return deny()
		}
	}
	d := strings.Repeat("0", 64)
	pair, err := localTerminalCanonicalPairsUpperV2(entry.owner.configuration, ops, op)
	if err != nil || len(pair) < 2 {
		return deny()
	}
	defer func() {
		for _, event := range pair {
			clear(event.Payload)
		}
	}()
	if !modelTail {
		result, err := localFailureToolResult(domain.AttemptOperationDigestV1(d), domain.AttemptOperationReservationDigestV1(d), domain.AttemptOperationObservationDigestV1(d), []string{})
		if mcpUnknown {
			result, err = canonicaloutput.EncodeMCPUnknownToolOutcomeV1(domain.AttemptOperationDigestV1(d), domain.AttemptOperationReservationDigestV1(d), domain.AttemptOperationObservationDigestV1(d))
		}
		if err != nil {
			return deny()
		}
		defer clear(result)
		clear(pair[len(pair)-1].Payload)
		pair[len(pair)-1].Payload = result
	}
	run := source.state.snapshot.Job.Run
	var plan *canonicaloutput.FailurePlanV1
	if modelTail {
		plan, err = canonicaloutput.EncodeModelAfterMCPFailureV2(run.ID, source.state.snapshot.Job.Job.Limits, pair)
	} else if mcpUnknown {
		plan, err = canonicaloutput.EncodeMCPOutcomeUnknownFailureV3(run.ID, source.state.snapshot.Job.Job.Limits, pair)
	} else {
		plan, err = canonicaloutput.EncodeKnownMCPServiceFailureV1(run.ID, source.state.snapshot.Job.Job.Limits, pair)
	}
	if err != nil {
		return deny()
	}
	defer plan.Release()
	n := min(op.ResultBytes, uint64(64<<10))
	if modelTail || mcpUnknown {
		n = 4
	} // exact explicit absent normalized response: null
	if n < 4 {
		return deny()
	}
	var encoded uint64
	c := localFailureCapsule{Schema: "sessionless.terminal-failure-custody.v1", Version: 1, Parent: entry.parent.Clone(), Source: entry.source.Clone(), FrontierDigest: d, Stop: domain.KnownMCPServiceErrorV1, Cleanup: "joined_retained", CapturedAt: localTerminalSizeTime, Derived: run.Clone().DerivedContext}
	if modelTail {
		c.Schema, c.Version, c.Stop = "sessionless.terminal-failure-custody.v2", 2, domain.ModelOutcomeUnknownV2
	} else if mcpUnknown {
		c.Schema, c.Version, c.Stop = "sessionless.terminal-failure-custody.v3", 3, domain.MCPOutcomeUnknownV3
	}
	c.ProfileDigest, err = entry.owner.configuration.Digest()
	if err != nil {
		return deny()
	}
	for i, event := range plan.Events() {
		draft, e := canonicaloutput.PlanEventDraftV1(run, event.ID, event.Kind, event.Payload, localTerminalSizeTime)
		if e != nil {
			return deny()
		}
		size := uint64(len(event.Payload))
		if i == len(pair)-1 && !modelTail && !mcpUnknown {
			size += n
		}
		if size > sessionmaterial.MaxPayloadBytesV1 {
			return deny()
		}
		draft.Payload.Size = int64(size)
		if i < len(pair) {
			encoded += size
		}
		c.Events = append(c.Events, localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText})
	}
	_, maxToolBytes := source.state.snapshot.Job.Job.Limits.EffectiveToolEventLimits()
	if encoded > maxToolBytes {
		return deny()
	}
	c.Accounting = ports.OwnedOperationFailureAccountingV1{Version: c.Version, Scope: op.Scope, AccountingDigest: d, AggregateDebit: localTerminalUpperDebit(), ComponentAmounts: [4]uint64{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64}}
	var materials []localCapsuleOperationMaterial
	for _, item := range ops {
		materials = append(materials, localCapsuleOperationMaterial{reservation: item.Reservation, observation: item.Observation})
		c.Operations = append(c.Operations, localFailureOperationV1{Reservation: item.Reservation, Observation: item.Observation, Normalized: json.RawMessage(item.Response)})
	}
	future := localTerminalFutureMaterial(op, n)
	future.observation.Reason = domain.AttemptOperationReasonServiceErrorV1
	future.observation.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
	if modelTail || mcpUnknown {
		future.observation.Outcome, future.observation.Reason = domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonTransportUnknownV1
		future.observation.ResponseRef, future.observation.ResponseDigest, future.observation.ResponseBytes = "", "", 0
	}
	if modelTail {
		number := uint64(math.MaxUint64)
		// LENGTH only: longest state plus every possible metric, matching the
		// existing upper accounting rows. This is never a valid/authenticated
		// usage observation or an invented measured bill.
		future.observation.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: &number, OutputTokens: &number, AmountMicrounits: &number}
	}
	materials = append(materials, future)
	c.Operations = append(c.Operations, localFailureOperationV1{Reservation: future.reservation, Observation: future.observation, Normalized: json.RawMessage(`null`)})
	upper := localTerminalAccountingUpperV3(entry.source, op.Scope, materials)
	for i, row := range upper.Operations {
		reason := domain.AttemptOperationReasonNoneV1
		if i == len(materials)-1 {
			reason = domain.AttemptOperationReasonServiceErrorV1
			if modelTail || mcpUnknown {
				reason = domain.AttemptOperationReasonTransportUnknownV1
			}
		}
		outcome := domain.AttemptOperationCompletedV1
		if (modelTail || mcpUnknown) && i == len(materials)-1 {
			outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
		}
		c.Accounting.Operations = append(c.Accounting.Operations, ports.OwnedOperationFailureRowV1{Sequence: row.Sequence, Kind: row.Kind, OperationDigest: row.OperationDigest, ReservationDigest: row.ReservationDigest, ObservationDigest: row.ObservationDigest, Outcome: outcome, Reason: reason, Reserved: row.Reserved, Usage: row.Usage, MCPSpendingDigest: row.MCPSpendingDigest})
	}
	bodies, err := entry.terminalOutputs.SnapshotRetainedBodies(ctx)
	if err != nil || len(bodies) == 0 {
		return deny()
	}
	prefix := bodies[0].Key[:strings.LastIndex(bodies[0].Key, "/")+1]
	// Count all 24 original attempted-key slots, including uncertain writes;
	// no eventual absence/refund is assumed by prospective admission.
	for range 24 {
		c.Bodies = append(c.Bodies, domain.BlobRef{TenantID: op.Scope.TenantID, Key: prefix + strings.Repeat("x", 82), Size: 1 << 20, SHA256: d})
	}
	cap := entry.terminalOutputs.CapsuleLimit()
	if _, err := localFailureProspectiveCapsuleBytes(c, n, cap); err != nil {
		return deny()
	}
	key, err := domain.TerminalCustodyObjectKeyV1(op.Scope, d)
	if err != nil {
		return deny()
	}
	run = run.Clone()
	run.Status = domain.RunFailed
	run.UpdatedAt = localTerminalSizeTime
	run.FinishedAt = &localTerminalSizeTime
	p := domain.TerminalCustodyPlanV1{Scope: op.Scope, FrontierDigest: d, Capsule: domain.BlobRef{TenantID: run.TenantID, Key: key, Size: int64(cap), SHA256: d}}
	run.TerminalCustody = &domain.RunTerminalCustodyV1{Version: 1, Plan: p}
	run.TerminalFailure = &domain.RunTerminalFailureV1{Version: 1, Scope: op.Scope, ParentDigest: d, Plan: p, CapsuleSchema: c.Schema, AccountingDigest: d, EventsDigest: d, RetainedBodiesDigest: d, RetiredAllocationDigest: d, CapturedAt: localTerminalSizeTime, Stop: c.Stop, Cleanup: c.Cleanup}
	raw, err := json.Marshal(run)
	if err != nil || len(raw) > ownedAuthorityHeadBytes {
		return deny()
	}
	return nil
}

// Count the exact closed serializer plus a scalar bound for the canonical
// normalized future response. The private placeholder is exactly JSON null;
// it is never decoded, authenticated, persisted or returned as an observation.
func localFailureProspectiveCapsuleBytes(c localFailureCapsule, futureBytes, cap uint64) (uint64, error) {
	if len(c.Operations) == 0 || futureBytes < 4 || futureBytes > 64<<10 || string(c.Operations[len(c.Operations)-1].Normalized) != "null" {
		return 0, ErrTerminalCapacity
	}
	body, err := encodeLocalFailureCapsule(c, cap)
	if err != nil {
		return 0, ErrTerminalCapacity
	}
	size := uint64(len(body)) + futureBytes - 4
	clear(body)
	if size > cap {
		return 0, ErrTerminalCapacity
	}
	return size, nil
}
