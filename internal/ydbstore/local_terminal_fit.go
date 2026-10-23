package ydbstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// ErrTerminalCapacity is NOT model context overflow and must not cause another
// model/compaction attempt. No prospective DATA below grants an effect.
var ErrTerminalCapacity = errors.New("local terminal capacity unavailable")

var localTerminalSizeTime = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)

// Numeric ceilings are for JSON LENGTH ONLY. These values are never validated
// as observations, signed, returned as receipts, or written to any Store.
func localTerminalUpperDebit() domain.AttemptOperationDebitV1 {
	return domain.AttemptOperationDebitV1{Operations: math.MaxUint64, ModelCalls: math.MaxUint64, CompactionCalls: math.MaxUint64, ToolDispatches: math.MaxUint64, WebSearches: math.MaxUint64, InputTokens: math.MaxUint64, OutputTokens: math.MaxUint64, BodyBytes: math.MaxUint64, MCPOutboundMessages: math.MaxUint64, MCPCatalogPages: math.MaxUint64, MCPInboundFrames: math.MaxUint64, MCPInboundBytes: math.MaxUint64, ResponseBytes: math.MaxUint64, AmountMicrounits: math.MaxUint64, ActiveDuration: time.Duration(math.MaxInt64)}
}

func localTerminalFutureMaterial(op domain.AttemptOperationV1, bytes uint64) localCapsuleOperationMaterial {
	digest := strings.Repeat("0", 64)
	// Original body refs have a bounded decimal size and raw digest; 82 is the
	// actual parser's maximum, not an unbounded opaque-ID allowance.
	op.RequestRef = strings.Repeat("x", 82)
	op.RequestDigest = digest
	op.AdmissionCostCeilingDigest = domain.AdmissionCostCeilingDigestV2(digest)
	if op.Sequence > 1 {
		op.PredecessorObservationDigest = domain.AttemptOperationObservationDigestV1(digest)
	}
	op.Deadline, op.CleanupDuration = localTerminalSizeTime, time.Duration(math.MaxInt64)
	op.Debit = localTerminalUpperDebit()
	op.RequestBytes, op.ContextBytes, op.SummaryBytes = math.MaxUint64, math.MaxUint64, math.MaxUint64
	reservation := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: domain.AttemptOperationDigestV1(digest), ReservedAt: localTerminalSizeTime, AggregateDebit: localTerminalUpperDebit(), RegisteredAllocationDigest: domain.RegisteredAllocationDigestV2(digest), RegisteredProxyDigest: domain.RegisteredProxyAttestationDigestV2(digest), ComponentAmounts: [4]uint64{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64}}
	n := uint64(math.MaxUint64)
	observation := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: domain.AttemptOperationDigestV1(digest), ReservationDigest: domain.AttemptOperationReservationDigestV1(digest), Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: strings.Repeat("x", 82), ResponseDigest: digest, ResponseBytes: bytes, ObservedAt: localTerminalSizeTime, Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: &n, OutputTokens: &n}}
	return localCapsuleOperationMaterial{reservation: reservation, observation: observation, bytes: bytes}
}

func localTerminalDerivedUpper(run domain.Run, op domain.AttemptOperationV1) (*domain.RunDerivedContextV1, error) {
	if run.DerivedContext != nil && run.DerivedContext.ValidateForRun(run) != nil {
		return nil, domain.ErrMembershipDenied
	}
	if op.Kind != domain.AttemptOperationCompactionV1 {
		return run.DerivedContext, nil
	}
	metadata := domain.RunDerivedContextV1{Version: 1}
	if run.DerivedContext != nil {
		metadata = run.DerivedContext.Clone()
	}
	if len(metadata.Inventory) >= 2 {
		return nil, ErrTerminalCapacity
	}
	d := strings.Repeat("0", 64)
	generation := uint64(len(metadata.Inventory) + 1)
	summary, err := domain.DerivedContextObjectKeyV1(op.Scope, generation, "summary", d)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	manifest, err := domain.DerivedContextObjectKeyV1(op.Scope, generation, "manifest", d)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	metadata.Inventory = append(metadata.Inventory, domain.DerivedContextPlanV1{Version: 1, Scope: op.Scope, Generation: generation, OperationSequence: op.Sequence, ReservationDigest: domain.AttemptOperationReservationDigestV1(d), ObservationDigest: domain.AttemptOperationObservationDigestV1(d), NormalizedResponseDigest: d, SourceViewDigest: d, Summary: domain.BlobRef{TenantID: op.Scope.TenantID, Key: summary, Size: 32 << 10, SHA256: d}, Manifest: domain.BlobRef{TenantID: op.Scope.TenantID, Key: manifest, Size: 128 << 10, SHA256: d}})
	metadata.Head = &domain.DerivedContextHeadV1{Version: 1, PlanDigest: d}
	return &metadata, nil // length-only head digest is not a selected-head proof
}

// Count the SAME closed capsule layout. The future raw normalized body is a
// scalar length, never allocated fake output. Final display uses exactly the
// same json.Marshal string escaping as configuredinference's normalized codec,
// so its encoded string cannot exceed the containing normalized body's bytes.
func localTerminalProspectiveBytes(run domain.Run, profile string, ops []LocalCompactionOperationV1, op domain.AttemptOperationV1, followResultBytes, cap uint64) (uint64, error) {
	needsSuccessor := op.Kind == domain.AttemptOperationCompactionV1 || op.Kind == domain.AttemptOperationMCPToolV1
	if len(ops) >= 12 || (needsSuccessor && len(ops) >= 11) || op.Sequence != uint64(len(ops)+1) || (op.Kind != domain.AttemptOperationModelV1 && op.Kind != domain.AttemptOperationCompactionV1 && op.Kind != domain.AttemptOperationMCPToolV1) {
		return 0, domain.ErrMembershipDenied
	}
	materials := make([]localCapsuleOperationMaterial, 0, len(ops)+2)
	for _, o := range ops {
		materials = append(materials, localCapsuleOperationMaterial{reservation: o.Reservation, observation: o.Observation, normalized: o.Response, bytes: uint64(len(o.Response))})
	}
	n := min(op.ResultBytes, uint64(64<<10))
	if n == 0 {
		return 0, ErrTerminalCapacity
	}
	materials = append(materials, localTerminalFutureMaterial(op, n))
	if needsSuccessor {
		follow := op.Clone()
		follow.Kind, follow.Sequence, follow.CallID = domain.AttemptOperationModelV1, op.Sequence+1, "model-"+strings.Repeat("0", 24)
		follow.ModelResponseDigest, follow.ArgumentsDigest, follow.ToolGrantDigest, follow.ExternalChargeEvidenceDigest = "", "", "", ""
		follow.QueryDigest, follow.ConsentRequestDigest, follow.ConsentDecisionDigest, follow.ConsentDecisionRef = "", "", "", ""
		follow.PredecessorObservationDigest = domain.AttemptOperationObservationDigestV1(strings.Repeat("0", 64))
		n = min(followResultBytes, uint64(64<<10))
		if n == 0 {
			return 0, ErrTerminalCapacity
		}
		follow.ResultBytes = n
		materials = append(materials, localTerminalFutureMaterial(follow, n))
	}
	derived, err := localTerminalDerivedUpper(run, op)
	if err != nil {
		return 0, err
	}
	manifest := domain.ArtifactManifest{ID: canonicaloutput.ManifestIDV1(run.ID), TenantID: run.TenantID, RunID: run.ID, CreatedAt: localTerminalSizeTime}
	encoded, err := canonicaloutput.EncodeCompletionV1(run.ID, domain.ProductLimits{}, ports.ExecutionResult{Summary: "x"}, manifest.ID)
	if err != nil {
		return 0, domain.ErrMembershipDenied
	}
	defer encoded.Release()
	e := encoded.Events()[0]
	// Actual assistant payload = exact envelope syntax + escaped summary. The
	// one-byte "x" string occupies three bytes in the original envelope.
	payloadSize := uint64(len(e.Payload)) - 3 + n
	draft, err := canonicaloutput.PlanEventDraftV1(run, e.ID, e.Kind, e.Payload, run.CreatedAt)
	if err != nil {
		return 0, domain.ErrMembershipDenied
	}
	draft.Payload.Size = int64(payloadSize)
	events := []localCapsuleEventV1{{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload}}
	if err := localTerminalRunUpperFits(run, op.Scope, derived, e.ID, cap); err != nil {
		return 0, err
	}
	sink := localCapsuleSink{limit: cap}
	if err := encodeLocalTerminalCapsuleTo(&sink, op.Scope, profile, strings.Repeat("0", 64), materials, derived, events, n, manifest); err != nil {
		return 0, err
	}
	return sink.size, nil
}

// Only JSON size is inspected; this private hypothetical shape is never
// persisted, signed, returned, or accepted as evidence by a real producer.
func localTerminalRunUpperFits(run domain.Run, scope domain.AttemptOperationScopeV1, derived *domain.RunDerivedContextV1, assistant domain.SessionEventID, cap uint64) error {
	d := strings.Repeat("0", 64)
	key, err := domain.TerminalCustodyObjectKeyV1(scope, d)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	ref := domain.BlobRef{TenantID: scope.TenantID, Key: key, Size: int64(cap), SHA256: d}
	run = run.Clone()
	run.Status, run.DerivedContext = domain.RunSucceeded, derived
	run.CreatedAt, run.UpdatedAt = localTerminalSizeTime, localTerminalSizeTime
	run.StartedAt, run.FinishedAt = &localTerminalSizeTime, &localTerminalSizeTime
	run.TerminalCustody = &domain.RunTerminalCustodyV1{Version: 1, Plan: domain.TerminalCustodyPlanV1{Scope: scope, Capsule: ref, FrontierDigest: d}, Correspondence: &domain.TerminalCorrespondenceV1{Version: 1, CapsuleDigest: d, TerminalAssistantEventID: assistant, CanonicalEventsDigest: d, DerivedContextDigest: d, AccountingDispositionDigest: d}, LocalEvidence: &domain.LocalExecutionEvidenceV1{Version: 1, Scope: scope, ParentDigest: d, SourceDigest: d, ProfileDigest: d, OriginalAllocationDigest: d, RetiredAllocationDigest: d, FrontierDigest: d, Capsule: ref, ResidualInventoryDigest: d, ResidualReceiptDigest: d, AccountingDispositionDigest: d, StoppedAt: localTerminalSizeTime, RetiredAt: localTerminalSizeTime, ErasedAt: localTerminalSizeTime, EvidenceDigest: d}}
	body, err := json.Marshal(run)
	if err != nil || len(body) > ownedAuthorityHeadBytes {
		return ErrTerminalCapacity
	}
	return nil
}

func chargeLocalTerminalFit(ctx context.Context, q *ownedAuthorityQuery, n uint64) error {
	if ctx == nil || ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	if q != nil {
		return q.budget.materialized(q.callback, n)
	}
	return chargeOwnedAuthorityBody(ctx, n)
}

func (entry *LocalInvocationEntryV1) terminalFitSourceLocked(ctx context.Context, q *ownedAuthorityQuery, source *LocalCompactionSourceV1, op domain.AttemptOperationV1, program *compactioncontext.GenerationV1) error {
	if !entry.original() || source == nil || source.self != source || source.entry != entry || source.state == nil || source.state.cleared || entry.terminalOutputs == nil {
		return domain.ErrMembershipDenied
	}
	for i, o := range source.state.operations {
		if chargeLocalTerminalFit(ctx, q, uint64(len(o.Request)+len(o.Response))) != nil {
			return domain.ErrMembershipDenied
		}
		if o.Reservation.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			if _, err := mcpadmission.DecodeSealedToolResultDataV1(o.Reservation, o.Observation, o.Response); err != nil {
				return domain.ErrMembershipDenied
			}
			continue
		}
		decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(entry.owner.configuration, o.Request, o.Response, o.Reservation.Operation.Kind)
		if err != nil {
			if decoded.Call != nil {
				clear(decoded.Call.Arguments)
			}
			return domain.ErrMembershipDenied
		}
		pendingForMCP := op.Kind == domain.AttemptOperationMCPToolV1 && i == len(source.state.operations)-1 && decoded.Stop == configuredinference.StopToolV1 && decoded.Call != nil
		completedMCP := false
		if decoded.Stop == configuredinference.StopToolV1 && decoded.Call != nil && i+1 < len(source.state.operations) {
			next := source.state.operations[i+1]
			if next.Reservation.Operation.Kind == domain.AttemptOperationMCPToolV1 {
				result, e := mcpadmission.DecodeSealedToolResultDataV1(next.Reservation, next.Observation, next.Response)
				args := sha256.Sum256(decoded.Call.Arguments)
				completedMCP = e == nil && !result.IsError() && decoded.Call.ID == result.CallID() && decoded.Call.Name == result.Name() && hex.EncodeToString(args[:]) == result.ArgumentsDigest() && o.Observation.ResponseDigest == result.ModelResponseDigest() &&
					result.ValidateConfiguredCallV1(entry.owner.configuration, o.Reservation, o.Observation, o.Request, o.Response) == nil && entry.originalObservationAllowsSuccessor(next.Reservation, next.Observation)
			}
		}
		if pendingForMCP {
			argsDigest := sha256.Sum256(decoded.Call.Arguments)
			responseDigest := o.Observation.ResponseDigest
			matched := decoded.Call.ID == op.CallID && hex.EncodeToString(argsDigest[:]) == op.ArgumentsDigest && responseDigest == op.ModelResponseDigest
			clear(decoded.Call.Arguments)
			if !matched {
				return domain.ErrMembershipDenied
			}
		} else if !completedMCP && (decoded.Stop != configuredinference.StopFinalV1 || decoded.Call != nil) {
			if decoded.Call != nil {
				clear(decoded.Call.Arguments)
			}
			return domain.ErrMembershipDenied
		}
		if completedMCP {
			clear(decoded.Call.Arguments)
		}
	}
	if op.Kind == domain.AttemptOperationCompactionV1 {
		if err := entry.terminalSuccessorBudgetLocked(ctx, q, source, op, program); err != nil {
			return err
		}
	}
	pd, err := entry.owner.configuration.Digest()
	if err != nil {
		return domain.ErrMembershipDenied
	}
	if chargeLocalTerminalFit(ctx, q, entry.terminalOutputs.CapsuleLimit()+ownedAuthorityHeadBytes) != nil {
		return domain.ErrMembershipDenied
	}
	if localTerminalNeedsV2(source.state.operations, op) {
		// The V2 counter decodes the original bodies again to derive exact
		// canonical pairs. Charge that additional bounded materialization to
		// the SAME callback/call budget; never reset it for prospective work.
		for _, item := range source.state.operations {
			if chargeLocalTerminalFit(ctx, q, uint64(len(item.Request)+len(item.Response))) != nil {
				return domain.ErrMembershipDenied
			}
		}
		if op.Kind == domain.AttemptOperationMCPToolV1 {
			if err := entry.terminalMCPSuccessorBudgetV2(ctx, source, op); err != nil {
				return err
			}
		}
		_, err = localTerminalProspectiveBytesV2(source.state.snapshot.Job.Run, source.state.snapshot.Job.Job.Limits, pd, entry.owner.configuration, entry.source, source.state.operations, op, entry.owner.configuration.Limits.ResponseBytes, entry.terminalOutputs.CapsuleLimit())
	} else {
		_, err = localTerminalProspectiveBytes(source.state.snapshot.Job.Run, pd, source.state.operations, op, entry.owner.configuration.Limits.ResponseBytes, entry.terminalOutputs.CapsuleLimit())
	}
	return err
}

// Original completed frontier and actual pending generation are already joined.
// This arithmetic reserves feasibility, not output quality or a guarantee that
// any arbitrary summary will pass the mandatory later Rebuild/Fit check.
func (entry *LocalInvocationEntryV1) terminalSuccessorBudgetLocked(ctx context.Context, q *ownedAuthorityQuery, source *LocalCompactionSourceV1, op domain.AttemptOperationV1, program *compactioncontext.GenerationV1) error {
	if program == nil || program.SourceDigest() != source.state.digest {
		return domain.ErrMembershipDenied
	}
	policy := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if policy == nil {
		return domain.ErrMembershipDenied
	}
	limits, cfg := policy.Limits, entry.owner.configuration
	// Three bounded SAME-renderer materializations, one retained calibration
	// decode and worst summary text. Inside callbacks the SAME original budget
	// uses its materialized method; outside callbacks no budget is reset.
	if chargeLocalTerminalFit(ctx, q, 3*cfg.Limits.RequestBytes+configuredinference.MaxContextCalibrationBytesV1+configuredinference.MaxCompactionSummaryBytesV1) != nil {
		return domain.ErrMembershipDenied
	}
	if err := entry.terminalMinimumSummaryFormat(op, program); err != nil {
		return err
	}
	used := source.state.snapshot.Accounting.Aggregate
	next, err := used.Add(op.Debit)
	if err != nil || next.Operations >= limits.MaxOperations || next.ModelCalls >= limits.MaxModelCalls || next.InputTokens >= limits.MaxAggregateInputTokens || next.OutputTokens >= limits.MaxAggregateOutputTokens || next.BodyBytes >= entry.localOperationBodyLimit() || next.ActiveDuration >= limits.MaxActiveDuration {
		return ErrTerminalCapacity
	}
	input := min(cfg.Limits.MaxInputTokens, limits.MaxInputTokensPerCall, limits.MaxAggregateInputTokens-next.InputTokens)
	output := program.SuccessorOutputReserve()
	if input <= 1 || output == 0 || output > min(cfg.Limits.MaxOutputTokens, limits.MaxOutputTokensPerCall, limits.MaxAggregateOutputTokens-next.OutputTokens) || output >= cfg.ModelWindowTokens {
		return ErrTerminalCapacity
	}
	minimum, err := program.MinimumSuccessorRequest()
	if err != nil {
		return ErrTerminalCapacity
	}
	if _, err := entry.owner.source.FitContext(minimum, configuredinference.ContextFitCeilingsV1{InputTokens: input, OutputTokens: output, TotalTokens: cfg.ModelWindowTokens}); err != nil {
		if errors.Is(err, configuredinference.ErrContextNoFit) {
			return ErrTerminalCapacity
		}
		return domain.ErrMembershipDenied
	}
	// Every ACCEPTED calibration has numerator>=denominator, fixed>=1 and
	// nonnegative message/tool terms (configuredinference/context_fit.go).
	// Consequently FitRequest(bytes,...) >= bytes+1, even at overflow (denied).
	// No Source coefficient, guessed tokenizer or reported usage is substituted.
	maxWire := min(input, cfg.ModelWindowTokens-output) - 1
	requestBytes, err := program.SuccessorRequestUpperBytes(min(op.SummaryBytes, uint64(configuredinference.MaxCompactionSummaryBytesV1)), maxWire)
	if err != nil {
		return ErrTerminalCapacity
	}
	resultBytes := min(uint64(64<<10), cfg.Limits.ResponseBytes)
	if resultBytes == 0 || requestBytes > entry.localOperationBodyLimit()-next.BodyBytes || resultBytes > entry.localOperationBodyLimit()-next.BodyBytes-requestBytes {
		return ErrTerminalCapacity
	}
	pairs := uint64(1)
	if metadata := source.state.snapshot.Job.Run.DerivedContext; metadata != nil {
		pairs += uint64(len(metadata.Inventory))
	}
	if pairs > 2 || 1+2*pairs > uint64(source.state.snapshot.Job.Job.Limits.MaxArtifacts) || operationbody.LocalTerminalCapsuleBytesV1+operationbody.RetainedPairBytesV1*pairs > entry.parent.Authority.SubstrateBinding.Limits.ArtifactBytes {
		return ErrTerminalCapacity
	}
	duration := min(cfg.Limits.OperationTimeout, limits.MaxNetworkOperationDuration, limits.MaxActiveDuration-next.ActiveDuration)
	end := op.Deadline.Add(duration)
	for _, bound := range []time.Time{cfg.ExpiresAt, policy.ExpiresAt, entry.parent.Authority.InvocationDeadline, entry.parent.Authority.Lease.ExpiresAt, entry.parent.Authority.AdmissionCostCeiling.PriceExpiresAt, entry.allocation.Window.ExpiresAt, entry.parent.Reservation.ReservedAt.Add(limits.MaxActiveDuration)} {
		if bound.Before(end) {
			end = bound
		}
	}
	life, err := entry.owner.source.ExecutionLifetime()
	if err != nil || life.Err() != nil || entry.invocationContext.Err() != nil {
		return domain.ErrMembershipDenied
	}
	for _, original := range []context.Context{life, entry.invocationContext} {
		if bound, ok := original.Deadline(); ok && bound.Before(end) {
			end = bound
		}
	}
	if !end.After(op.Deadline) {
		return ErrTerminalCapacity
	}
	return nil
}

// Necessary format feasibility only. This synthetic minimum response is DATA
// for the ORIGINAL frozen codec; it is never stored, signed, returned as an
// observation, or substituted for the actual model's normalized response.
func (entry *LocalInvocationEntryV1) terminalMinimumSummaryFormat(op domain.AttemptOperationV1, program *compactioncontext.GenerationV1) error {
	minimum, err := program.MinimumSummaryData()
	if err != nil {
		return ErrTerminalCapacity
	}
	defer clear(minimum)
	if uint64(len(minimum)) > op.SummaryBytes || uint64(len(minimum)) > entry.owner.configuration.Limits.FinalTextBytes {
		return ErrTerminalCapacity
	}
	bound, err := entry.owner.source.BoundCompactionSummary(string(minimum))
	if errors.Is(err, configuredinference.ErrContextNoFit) {
		return ErrTerminalCapacity
	}
	if err != nil {
		return domain.ErrMembershipDenied
	}
	if bound > min(uint64(2048), op.Debit.OutputTokens, entry.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxOutputTokensPerCall) {
		return ErrTerminalCapacity
	}
	model := ""
	var encodedBytes int
	for _, candidate := range entry.owner.configuration.ResponseModelIDs {
		raw, e := json.Marshal(candidate)
		if e != nil {
			return domain.ErrMembershipDenied
		}
		if model == "" || len(raw) < encodedBytes {
			model, encodedBytes = candidate, len(raw)
		}
	}
	if model == "" {
		return domain.ErrMembershipDenied
	}
	wire, err := json.Marshal(map[string]any{"model": model, "choices": []any{map[string]any{"index": 0, "finish_reason": "stop", "message": map[string]any{"role": "assistant", "content": string(minimum)}}}})
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer clear(wire)
	normalized, err := program.FrozenRequest().NormalizeResponseBodyV1(wire, domain.AttemptOperationCompactionV1, min(op.ResultBytes, uint64(64<<10)))
	clear(normalized)
	if err != nil {
		return ErrTerminalCapacity
	}
	return nil
}

// First-operation admission deliberately needs no canonical context window.
// It joins the actual current parent/job/accounting in the original bounded
// transaction. The generic Store path repeats this check with the actual op.
func (entry *LocalInvocationEntryV1) CheckFirstModelTerminalFit(ctx context.Context) error {
	if !entry.original() {
		return domain.ErrMembershipDenied
	}
	unlock, err := entry.beginLocalMutation(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	call, finish, err := entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return err
	}
	defer finish()
	call, cancel := ownedAuthorityCallContext(call)
	defer cancel()
	err = entry.owner.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		return entry.checkFirstModelTerminalFitTx(ctx, q, nil)
	})
	if ctx.Err() != nil || call.Err() != nil || entry.invocationContext.Err() != nil {
		return domain.ErrMembershipDenied
	}
	return err
}

func (entry *LocalInvocationEntryV1) checkFirstModelTerminalFitTx(ctx context.Context, q *ownedAuthorityQuery, actual *domain.AttemptOperationV1) error {
	if !entry.original() || entry.terminalOutputs == nil {
		return domain.ErrMembershipDenied
	}
	request := ports.ReserveAttemptEffectRequestV1{TenantID: entry.parent.Scope.TenantID, RunID: entry.parent.Scope.RunID, AttemptID: entry.parent.Scope.AttemptID, LeaseID: entry.parent.Reservation.LeaseID, FenceToken: entry.parent.Reservation.FenceToken, PhysicalInvocationClaimID: entry.parent.Reservation.PhysicalInvocationClaimID, UpstreamIdempotencyKeyDigest: entry.parent.Reservation.UpstreamIdempotencyKeyDigest}
	_, parent, source, burn, err := entry.owner.store.operationParent(ctx, q, ownedOperationScope(entry.parent))
	if err != nil || parent.Scope != entry.parent.Scope {
		return q.budget.deny()
	}
	job, err := loadOwnedEffectWorkerJob(ctx, q, request)
	if err != nil {
		return err
	}
	accounting, err := readOwnedAttemptAccounting(ctx, q, parent, source)
	if err != nil || accounting == nil || !accounting.SetupReservedAt.Equal(burn) || job.Run.TerminalCustody != nil || job.Run.DerivedContext != nil {
		return q.budget.deny()
	}
	if actual == nil && accounting.Aggregate.Operations != 0 {
		return q.budget.deny()
	}
	if actual != nil && (actual.Sequence != 1 || actual.Kind != domain.AttemptOperationModelV1 || actual.Scope != ownedOperationScope(entry.parent) || accounting.Aggregate.Operations > 1) {
		return q.budget.deny()
	}
	op := domain.AttemptOperationV1{Version: 1, Scope: ownedOperationScope(entry.parent), Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "model-" + strings.Repeat("0", 24), ResultBytes: min(uint64(64<<10), entry.owner.configuration.Limits.ResponseBytes), Currency: entry.parent.Authority.AdmissionCostCeiling.Currency, PriceRevision: entry.parent.Authority.AdmissionCostCeiling.PriceRevision}
	if actual != nil {
		op = actual.Clone()
	}
	pd, err := entry.owner.configuration.Digest()
	if err != nil {
		return q.budget.deny()
	}
	if chargeLocalTerminalFit(ctx, q, entry.terminalOutputs.CapsuleLimit()+ownedAuthorityHeadBytes) != nil {
		return domain.ErrMembershipDenied
	}
	_, err = localTerminalProspectiveBytes(job.Run, pd, nil, op, 0, entry.terminalOutputs.CapsuleLimit())
	return err
}

// The explicit preinstalled-body constructor has no capsule owner: it retains
// its old operation contract, never a terminal-custody eligibility claim.
func (store *Store) checkLocalGenericTerminalFit(ctx context.Context, q *ownedAuthorityQuery, op domain.AttemptOperationV1) error {
	if store.localBootstrap == nil {
		return nil
	}
	entry := store.localBootstrap.entry.Load()
	if entry == nil && store.localBootstrap.bodySlot == nil {
		return nil
	}
	return entry.checkFirstModelTerminalFitTx(ctx, q, &op)
}
