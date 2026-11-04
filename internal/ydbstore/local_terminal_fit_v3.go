package ydbstore

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

func localTerminalNeedsV3(ops []LocalCompactionOperationV1, future domain.AttemptOperationV1) bool {
	if future.Kind == domain.AttemptOperationWebSearchV1 {
		return true
	}
	for _, item := range ops {
		if item.Reservation.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			return true
		}
	}
	return false
}

// Private numeric LENGTH maxima only, never a billed usage/disposition, real
// observation or producer witness. SEARCH retains the distinct unknown arm.
func localTerminalAccountingUpperV4(source domain.AttemptExecutionSourceV1, scope domain.AttemptOperationScopeV1, materials []localCapsuleOperationMaterial) ports.OwnedOperationAccountingDispositionV4 {
	d, n := strings.Repeat("0", 64), uint64(math.MaxUint64)
	v := ports.OwnedOperationAccountingDispositionV4{Version: 4, Scope: scope, AccountingDigest: d, AggregateDebit: localTerminalUpperDebit(), ComponentAmounts: [4]uint64{n, n, n, n}, ConsentStatesDigest: d, ConsentReservedBytes: n, ProviderInvoice: ports.OwnedLocalInvoiceIdentityV2{ProfileDigest: source.Profile.Digest, TermsDigest: source.Spending[0].TermsEvidenceDigest, Revision: source.Spending[0].PriceRevision, Currency: source.Currency, IssuedAt: localTerminalSizeTime, ExpiresAt: localTerminalSizeTime}}
	for _, item := range materials {
		op := item.reservation.Operation
		row := ports.OwnedOperationAccountingRowV4{Sequence: op.Sequence, Kind: op.Kind, OperationDigest: domain.AttemptOperationDigestV1(d), ReservationDigest: domain.AttemptOperationReservationDigestV1(d), ObservationDigest: domain.AttemptOperationObservationDigestV1(d), Disposition: ports.OwnedOperationProviderReportedReservedV4, Reserved: localTerminalUpperDebit(), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: &n, OutputTokens: &n, AmountMicrounits: &n}}
		if op.Kind == domain.AttemptOperationMCPToolV1 || op.Kind == domain.AttemptOperationWebSearchV1 {
			index := 2
			if op.Kind == domain.AttemptOperationWebSearchV1 {
				index = 3
			}
			row.Disposition, row.Usage = ports.OwnedOperationUnknownReservedV4, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
			row.ToolSpendingDigest, _ = source.Spending[index].Digest()
		}
		v.Operations = append(v.Operations, row)
	}
	return v
}

func localTerminalCanonicalPairsUpperV3(configuration domain.ConfiguredInferenceProfileV1, source domain.AttemptExecutionSourceV1, ops []LocalCompactionOperationV1, future domain.AttemptOperationV1, consents []domain.WebSearchConsentStateV1) ([]ports.ExecutionToolEvent, error) {
	var tools []ports.ExecutionToolEvent
	var pending *configuredinference.FunctionCallV1
	var model LocalCompactionOperationV1
	ok := false
	defer func() {
		if pending != nil {
			clear(pending.Arguments)
		}
		if !ok {
			for _, tool := range tools {
				clear(tool.Payload)
			}
		}
	}()
	for i, item := range ops {
		r, o := item.Reservation, item.Observation
		if r.Operation.Sequence != uint64(i+1) || r.Operation.Scope != future.Scope || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || !localTerminalMaterialBytesV3(item.Request, r.Operation.RequestBytes, r.Operation.RequestDigest) || !localTerminalMaterialBytesV3(item.Response, o.ResponseBytes, o.ResponseDigest) || i > 0 && r.Operation.ValidatePredecessor(ops[i-1].Reservation, ops[i-1].Observation) != nil {
			return nil, domain.ErrMembershipDenied
		}
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 || r.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			if pending == nil || i == 0 {
				return nil, domain.ErrMembershipDenied
			}
			var pair []ports.ExecutionToolEvent
			var err error
			if r.Operation.Kind == domain.AttemptOperationWebSearchV1 {
				pair, _, err = localSearchCanonicalToolEventsV3(model, item, pending, source, consents)
			} else {
				result, e := mcpadmission.DecodeSealedToolResultDataV1(r, o, item.Response)
				if e != nil || result.IsError() || result.ValidateConfiguredCallV1(configuration, model.Reservation, model.Observation, model.Request, model.Response) != nil {
					return nil, domain.ErrMembershipDenied
				}
				pair, err = localMCPCanonicalToolEvents(pending.ID, pending.Name, pending.Arguments, result)
			}
			if err != nil {
				return nil, err
			}
			tools = append(tools, pair...)
			clear(pending.Arguments)
			pending = nil
			continue
		}
		if pending != nil || r.Operation.Kind != domain.AttemptOperationModelV1 && r.Operation.Kind != domain.AttemptOperationCompactionV1 {
			return nil, domain.ErrMembershipDenied
		}
		decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(configuration, item.Request, item.Response, r.Operation.Kind)
		usage, ue := decoded.Usage.OperationUsage()
		if err != nil || ue != nil || !reflect.DeepEqual(usage, o.Usage) {
			if decoded.Call != nil {
				clear(decoded.Call.Arguments)
			}
			return nil, domain.ErrMembershipDenied
		}
		if decoded.Call != nil {
			if decoded.Stop != configuredinference.StopToolV1 || r.Operation.Kind != domain.AttemptOperationModelV1 {
				clear(decoded.Call.Arguments)
				return nil, domain.ErrMembershipDenied
			}
			pending, model = decoded.Call, item
		} else if decoded.Stop != configuredinference.StopFinalV1 {
			return nil, domain.ErrMembershipDenied
		}
	}
	if future.Kind == domain.AttemptOperationWebSearchV1 || future.Kind == domain.AttemptOperationMCPToolV1 {
		if pending == nil || future.CallID != pending.ID || future.ArgumentsDigest != rawLocalFailureHash(pending.Arguments) || future.ModelResponseDigest != model.Observation.ResponseDigest {
			return nil, domain.ErrMembershipDenied
		}
		var call, result []byte
		var err error
		if future.Kind == domain.AttemptOperationWebSearchV1 {
			if pending.Name != "web_search" {
				return nil, domain.ErrMembershipDenied
			}
			call, err = json.Marshal(struct {
				ID        string `json:"call_id"`
				Name      string `json:"name"`
				Arguments string `json:"arguments_digest"`
				Query     string `json:"query_digest"`
			}{pending.ID, pending.Name, future.ArgumentsDigest, future.QueryDigest})
			if err == nil {
				// Exact canonical envelope with empty arrays is LENGTH framing,
				// not a domain vendor result or an authenticated future success.
				result, err = json.Marshal(struct {
					Schema    string                          `json:"schema"`
					Outcome   string                          `json:"outcome"`
					Operation domain.AttemptOperationDigestV1 `json:"operation_digest"`
					Sources   []domain.WebSearchSourceV1      `json:"sources"`
					Usage     domain.WebSearchUsageV1         `json:"usage"`
					Warnings  []domain.WebSearchWarningV1     `json:"warnings"`
				}{"sessionless.search-result.v4", "known_results", domain.AttemptOperationDigestV1(strings.Repeat("0", 64)), []domain.WebSearchSourceV1{}, domain.WebSearchUsageV1{State: "unknown"}, []domain.WebSearchWarningV1{}})
			}
		} else {
			call, err = json.Marshal(struct {
				ID        string          `json:"call_id"`
				Name      string          `json:"name"`
				Arguments json.RawMessage `json:"arguments"`
			}{pending.ID, pending.Name, pending.Arguments})
			if err == nil {
				result, err = json.Marshal(struct {
					ID      string   `json:"call_id"`
					Error   bool     `json:"is_error"`
					Content []string `json:"content"`
				}{pending.ID, false, []string{}})
			}
		}
		if err != nil {
			clear(call)
			clear(result)
			return nil, domain.ErrMembershipDenied
		}
		tools = append(tools, ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: pending.ID, ToolName: pending.Name, Payload: call}, ports.ExecutionToolEvent{Kind: domain.SessionEventToolResult, CallID: pending.ID, ToolName: pending.Name, Payload: result})
	} else if pending != nil {
		return nil, domain.ErrMembershipDenied
	}
	ok = true
	return tools, nil
}

// Both actual emission and this private scalar-length count use the SAME V3
// capsule encoder. No future result/body, bill, seal or receipt is persisted.
func localTerminalProspectiveBytesV3(run domain.Run, product domain.ProductLimits, configuration domain.ConfiguredInferenceProfileV1, source domain.AttemptExecutionSourceV1, ops []LocalCompactionOperationV1, future domain.AttemptOperationV1, consents []domain.WebSearchConsentStateV1, followResultBytes, cap uint64) (uint64, error) {
	if source.Validate() != nil || len(source.Spending) != 4 || future.Validate() != nil || !localTerminalNeedsV3(ops, future) || future.Kind == domain.AttemptOperationCompactionV1 || future.Sequence != uint64(len(ops)+1) || len(ops) >= 12 || future.Scope.RunID != run.ID || future.Scope.TenantID != run.TenantID || future.Scope.SessionID != run.SessionID {
		return 0, domain.ErrMembershipDenied
	}
	needsSuccessor := future.Kind == domain.AttemptOperationWebSearchV1 || future.Kind == domain.AttemptOperationMCPToolV1
	if needsSuccessor && len(ops) >= 11 {
		return 0, ErrTerminalCapacity
	}
	pd, err := configuration.Digest()
	if err != nil || product.Validate() != nil {
		return 0, domain.ErrMembershipDenied
	}
	if _, err := ports.OwnedSearchConsentStatesDigestV4(consents); err != nil {
		return 0, err
	}
	var materials []localCapsuleOperationMaterial
	var catalogs []compactioncontext.ModelCatalogV3
	for _, item := range ops {
		materials = append(materials, localCapsuleOperationMaterial{reservation: item.Reservation, observation: item.Observation, normalized: item.Response, bytes: uint64(len(item.Response))})
		if item.Reservation.Operation.Kind == domain.AttemptOperationModelV1 {
			catalog, err := compactioncontext.ReadModelCatalogV3(item.Reservation.Operation.Sequence, item.Request)
			if err != nil {
				return 0, err
			}
			catalogs = append(catalogs, catalog)
		}
	}
	n := min(future.ResultBytes, uint64(64<<10))
	if n == 0 {
		return 0, ErrTerminalCapacity
	}
	materials = append(materials, localTerminalFutureMaterial(future, n))
	if future.Kind == domain.AttemptOperationModelV1 {
		if len(catalogs) == 0 {
			return 0, domain.ErrMembershipDenied
		}
		catalogs = append(catalogs, compactioncontext.ModelCatalogV3{Sequence: future.Sequence, Tools: catalogs[len(catalogs)-1].Tools})
	}
	if needsSuccessor {
		follow := future.Clone()
		follow.Kind, follow.Sequence, follow.CallID = domain.AttemptOperationModelV1, future.Sequence+1, "model-"+strings.Repeat("0", 24)
		follow.ModelResponseDigest, follow.ArgumentsDigest, follow.ToolGrantDigest, follow.ExternalChargeEvidenceDigest = "", "", "", ""
		follow.QueryDigest, follow.ConsentRequestDigest, follow.ConsentDecisionDigest, follow.ConsentDecisionRef, follow.SearchWire = "", "", "", "", nil
		n = min(followResultBytes, uint64(64<<10))
		if n == 0 || len(catalogs) == 0 {
			return 0, ErrTerminalCapacity
		}
		follow.ResultBytes = n
		materials = append(materials, localTerminalFutureMaterial(follow, n))
		catalogs = append(catalogs, compactioncontext.ModelCatalogV3{Sequence: follow.Sequence, Tools: catalogs[len(catalogs)-1].Tools})
	}
	pairs, err := localTerminalCanonicalPairsUpperV3(configuration, source, ops, future, consents)
	if err != nil {
		return 0, err
	}
	defer func() {
		for _, pair := range pairs {
			clear(pair.Payload)
		}
	}()
	manifest := domain.ArtifactManifest{ID: canonicaloutput.ManifestIDV1(run.ID), TenantID: run.TenantID, RunID: run.ID, CreatedAt: localTerminalSizeTime}
	encoded, err := canonicaloutput.EncodeCompletionV1(run.ID, product, ports.ExecutionResult{Summary: "x", ToolEvents: pairs}, manifest.ID)
	if err != nil {
		return 0, ErrTerminalCapacity
	}
	defer encoded.Release()
	var events []localCapsuleEventV1
	var toolBytes uint64
	maxEvents, maxBytes := product.EffectiveToolEventLimits()
	for i, event := range encoded.Events() {
		draft, err := canonicaloutput.PlanEventDraftV1(run, event.ID, event.Kind, event.Payload, run.CreatedAt)
		if err != nil {
			return 0, err
		}
		upper := uint64(len(event.Payload))
		if i == len(pairs) {
			upper = upper - 3 + n
		} else if needsSuccessor && i == len(pairs)-1 {
			if future.Kind == domain.AttemptOperationWebSearchV1 {
				upper += 6 * future.ResultBytes
			} else {
				extra, err := localTerminalFutureResultExtraV2(future, pairs[i-1])
				if err != nil {
					return 0, err
				}
				upper += extra
			}
		}
		if upper > sessionmaterial.MaxPayloadBytesV1 || upper > math.MaxInt64 {
			return 0, ErrTerminalCapacity
		}
		if i < len(pairs) {
			if upper > maxBytes-toolBytes {
				return 0, ErrTerminalCapacity
			}
			toolBytes += upper
		}
		draft.Payload.Size = int64(upper)
		events = append(events, localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload})
	}
	if len(pairs) == 0 || uint64(len(pairs)) > uint64(maxEvents) {
		return 0, ErrTerminalCapacity
	}
	if localTerminalRunUpperFits(run, future.Scope, run.DerivedContext, events[len(events)-1].ID, cap) != nil {
		return 0, ErrTerminalCapacity
	}
	accounting := localTerminalAccountingUpperV4(source, future.Scope, materials)
	sink := localCapsuleSink{limit: cap}
	if err := encodeLocalTerminalCapsuleV3To(&sink, future.Scope, pd, strings.Repeat("0", 64), materials, run.DerivedContext, events, n, manifest, accounting, consents, catalogs); err != nil {
		return 0, err
	}
	return sink.size, nil
}

func (entry *LocalInvocationEntryV1) terminalSearchSuccessorBudgetV3(ctx context.Context, q *ownedAuthorityQuery, source *LocalCompactionSourceV1, future domain.AttemptOperationV1) error {
	policy := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if policy == nil || future.Kind != domain.AttemptOperationWebSearchV1 || source.state.history == nil {
		return domain.ErrMembershipDenied
	}
	limits, cfg := policy.Limits, entry.owner.configuration
	next, err := source.state.snapshot.Accounting.Aggregate.Add(future.Debit)
	if err != nil || next.Operations >= limits.MaxOperations || next.ModelCalls >= limits.MaxModelCalls || next.InputTokens >= limits.MaxAggregateInputTokens || next.OutputTokens >= limits.MaxAggregateOutputTokens || next.BodyBytes >= entry.localOperationBodyLimit() || next.ActiveDuration >= limits.MaxActiveDuration {
		return ErrTerminalCapacity
	}
	effective := cfg.Limits
	effective.MaxInputTokens = min(effective.MaxInputTokens, limits.MaxInputTokensPerCall, limits.MaxAggregateInputTokens-next.InputTokens)
	effective.MaxOutputTokens = min(uint64(2048), effective.MaxOutputTokens, limits.MaxOutputTokensPerCall, limits.MaxAggregateOutputTokens-next.OutputTokens)
	if effective.MaxInputTokens == 0 || effective.MaxOutputTokens == 0 {
		return ErrTerminalCapacity
	}
	// Prospectively cover actual record/catalog/decoder/renderer/calibration
	// copies before materializing them. Large legal contexts can honestly deny
	// the original four-MiB logical call; this does not create another allowance.
	copies := source.state.view.Bytes()
	for _, item := range source.state.operations {
		copies += uint64(len(item.Request) + len(item.Response))
	}
	if chargeLocalTerminalFit(ctx, q, 8*copies+36*(future.ResultBytes+512)+configuredinference.MaxContextCalibrationBytesV1+(32<<10)) != nil {
		return domain.ErrMembershipDenied
	}
	last := source.state.operations[len(source.state.operations)-1]
	catalog, err := compactioncontext.ReadModelCatalogV3(last.Reservation.Operation.Sequence, last.Request)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer clear(catalog.Tools)
	tools, err := compactioncontext.DecodeModelCatalogV3(catalog.Tools, cfg.Limits)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	data, _, err := entry.localContextDataV3(source)
	if err != nil {
		return err
	}
	if data.Version == 0 {
		data = compactioncontext.ContextDataV3{Version: 3, Historical: source.state.history.runsV3, HistoricalV2: source.state.history.runsV2, LegacyHistory: source.state.history.runs}
		mcp, err := entry.mcpConservativeDataFor(source.state.operations)
		if err != nil {
			return err
		}
		for i := range mcp {
			copy := mcp[i]
			data.Tools = append(data.Tools, compactioncontext.ToolWitnessV3{Kind: domain.AttemptOperationMCPToolV1, Scope: copy.Scope, Sequence: copy.Sequence, MCP: &copy})
		}
	}
	records := source.state.view.Records()
	defer func() {
		for _, record := range records {
			clear(record.Payload)
		}
	}()
	operations := make([]compactioncontext.CompletedOperationV1, len(source.state.operations))
	for i, item := range source.state.operations {
		operations[i] = compactioncontext.CompletedOperationV1{Reservation: item.Reservation, Observation: item.Observation, Request: item.Request, Response: item.Response}
	}
	// SEARCH-capable retained compaction is not implemented by this slice.
	// Never lower a V3 parent or no-fit result into the old V1/V2 writer.
	if source.state.snapshot.Job.Run.DerivedContext != nil {
		return domain.ErrMembershipDenied
	}
	upper, err := compactioncontext.BuildSearchSuccessorUpperV3(cfg, effective, *policy, source.state.digest, records, operations, nil, tools, data, future)
	if err != nil {
		return ErrTerminalCapacity
	}
	fit, err := entry.owner.source.FitContext(upper, configuredinference.ContextFitCeilingsV1{InputTokens: effective.MaxInputTokens, OutputTokens: effective.MaxOutputTokens, TotalTokens: cfg.ModelWindowTokens})
	if err != nil {
		return ErrTerminalCapacity
	}
	request := upper.Body()
	defer clear(request)
	result := min(uint64(64<<10), cfg.Limits.ResponseBytes)
	if fit.InputUpperBound > limits.MaxAggregateInputTokens-next.InputTokens || uint64(len(request)) > entry.localOperationBodyLimit()-next.BodyBytes || result > entry.localOperationBodyLimit()-next.BodyBytes-uint64(len(request)) {
		return ErrTerminalCapacity
	}
	duration := min(cfg.Limits.OperationTimeout, limits.MaxNetworkOperationDuration, limits.MaxActiveDuration-next.ActiveDuration)
	end := future.Deadline.Add(duration)
	for _, bound := range []time.Time{cfg.ExpiresAt, policy.ExpiresAt, entry.parent.Authority.InvocationDeadline, entry.parent.Authority.Lease.ExpiresAt, entry.parent.Authority.AdmissionCostCeiling.PriceExpiresAt, entry.allocation.Window.ExpiresAt, entry.parent.Reservation.ReservedAt.Add(limits.MaxActiveDuration)} {
		if bound.Before(end) {
			end = bound
		}
	}
	if bound, ok := entry.invocationContext.Deadline(); ok && bound.Before(end) {
		end = bound
	}
	if !end.After(future.Deadline) {
		return ErrTerminalCapacity
	}
	return nil
}

// Mounted before request-body publication/reservation and again in the current
// original reserve/send gates. A V3 no-fit is DENY, never legacy compaction.
func (entry *LocalInvocationEntryV1) terminalSearchFitSourceLocked(ctx context.Context, q *ownedAuthorityQuery, source *LocalCompactionSourceV1, op domain.AttemptOperationV1, program *compactioncontext.GenerationV1) error {
	if program != nil || op.Kind == domain.AttemptOperationCompactionV1 || source.state.snapshot.Job.Run.DerivedContext != nil || source.state.snapshot.Consents == nil || len(source.state.operations) == 0 || source.state.history == nil || source.state.history.unresolvedSearchV4 {
		return domain.ErrMembershipDenied
	}
	for _, item := range source.state.operations {
		if !entry.originalObservationAllowsSuccessor(item.Reservation, item.Observation) || chargeLocalTerminalFit(ctx, q, 4*uint64(len(item.Request)+len(item.Response))) != nil {
			return domain.ErrMembershipDenied
		}
	}
	states := source.state.snapshot.Consents.States
	if op.Kind == domain.AttemptOperationWebSearchV1 {
		if err := entry.terminalSearchSuccessorBudgetV3(ctx, q, source, op); err != nil {
			return err
		}
	} else if op.Kind != domain.AttemptOperationModelV1 {
		// A post-SEARCH MCP failure is not one of the closed five V4 tails.
		// Do not dispatch an effect whose required retained tail cannot fit.
		return domain.ErrMembershipDenied
	}
	run, product, cap := source.state.snapshot.Job.Run, source.state.snapshot.Job.Job.Limits, entry.terminalOutputs.CapsuleLimit()
	stops := []string{domain.ModelAfterSearchFailureV4}
	if op.Kind == domain.AttemptOperationWebSearchV1 {
		stops = []string{domain.SearchServiceErrorV4, domain.SearchRejectedBeforeSendV4, domain.SearchOutcomeUnknownV4}
	}
	for _, stop := range stops {
		if chargeLocalTerminalFit(ctx, q, cap+ownedAuthorityHeadBytes) != nil {
			return domain.ErrMembershipDenied
		}
		if _, err := localSearchFailureProspectiveBytesV4(run, product, entry.owner.configuration, entry.parent, entry.source, source.state.operations, op, states, stop, cap); err != nil {
			return err
		}
	}
	if chargeLocalTerminalFit(ctx, q, cap+ownedAuthorityHeadBytes) != nil {
		return domain.ErrMembershipDenied
	}
	_, err := localTerminalProspectiveBytesV3(run, product, entry.owner.configuration, entry.source, source.state.operations, op, states, entry.owner.configuration.Limits.ResponseBytes, cap)
	return err
}

// Consent append has a BODY-only debit after the last MODEL reservation. Admit
// its truthful notice-only tail BEFORE writing even the argument body. These
// hypothetical lengths never create a consent, SEARCH row or serving witness.
func (entry *LocalInvocationEntryV1) searchConsentStopFitLocked(ctx context.Context, source *LocalCompactionSourceV1, request domain.WebSearchConsentRequestV1) error {
	if !entry.original() || source == nil || source.self != source || source.entry != entry || source.state == nil || source.state.cleared || entry.terminalOutputs == nil || request.Validate() != nil || len(source.state.operations) == 0 {
		return domain.ErrMembershipDenied
	}
	states := []domain.WebSearchConsentStateV1{}
	if source.state.snapshot.Consents != nil {
		states = append(states, source.state.snapshot.Consents.States...)
	}
	states = append(states, domain.WebSearchConsentStateV1{Request: request})
	var copies uint64
	for _, item := range source.state.operations {
		copies += 4 * uint64(len(item.Request)+len(item.Response))
	}
	cap := entry.terminalOutputs.CapsuleLimit()
	if chargeLocalTerminalFit(ctx, nil, copies+cap+ownedAuthorityHeadBytes) != nil {
		return domain.ErrMembershipDenied
	}
	last := source.state.operations[len(source.state.operations)-1]
	future := domain.AttemptOperationV1{Scope: ownedOperationScope(entry.parent), Sequence: last.Reservation.Operation.Sequence + 1, Kind: domain.AttemptOperationWebSearchV1, CallID: request.CallID, ModelResponseDigest: request.ModelResponseDigest, ArgumentsDigest: request.ArgumentsDigest, QueryDigest: request.QueryDigest}
	_, err := localSearchFailureProspectiveBytesV4(source.state.snapshot.Job.Run, source.state.snapshot.Job.Job.Limits, entry.owner.configuration, entry.parent, entry.source, source.state.operations, future, states, domain.SearchConsentStoppedV4, cap)
	return err
}
