package ydbstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

func localTerminalNeedsV2(ops []LocalCompactionOperationV1, op domain.AttemptOperationV1) bool {
	if op.Kind == domain.AttemptOperationMCPToolV1 {
		return true
	}
	for _, item := range ops {
		if item.Reservation.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			return true
		}
	}
	return false
}

// Count-only metadata: maxima below are never authenticated observations,
// measured money, a disposition receipt or persisted financial state. The same
// V2 wire serializer counts the complete truthful V3 layout. MCP's future arm
// remains explicitly unknown/full-reserved; its source identity is original.
func localTerminalAccountingUpperV3(source domain.AttemptExecutionSourceV1, scope domain.AttemptOperationScopeV1, ops []localCapsuleOperationMaterial) ports.OwnedOperationAccountingDispositionV3 {
	d := strings.Repeat("0", 64)
	spending, _ := source.Spending[2].Digest()
	upper := localTerminalUpperDebit()
	v := ports.OwnedOperationAccountingDispositionV3{Version: 3, Scope: scope, AccountingDigest: d, AggregateDebit: upper, ComponentAmounts: [4]uint64{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64},
		ProviderInvoice: ports.OwnedLocalInvoiceIdentityV2{ProfileDigest: source.Profile.Digest, TermsDigest: source.Spending[0].TermsEvidenceDigest, Revision: source.Spending[0].PriceRevision, Currency: source.Currency, IssuedAt: localTerminalSizeTime, ExpiresAt: localTerminalSizeTime}}
	n := uint64(math.MaxUint64)
	for _, item := range ops {
		op := item.reservation.Operation
		row := ports.OwnedOperationAccountingRowV3{Sequence: op.Sequence, Kind: op.Kind, OperationDigest: domain.AttemptOperationDigestV1(d), ReservationDigest: domain.AttemptOperationReservationDigestV1(d), ObservationDigest: domain.AttemptOperationObservationDigestV1(d), Reserved: upper,
			Disposition: ports.OwnedOperationAccountingKindV3(ports.OwnedOperationProviderReportedReservedV2), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: &n, OutputTokens: &n, AmountMicrounits: &n}}
		if op.Kind == domain.AttemptOperationMCPToolV1 {
			row.Disposition = ports.OwnedOperationUnknownFullReservedV3
			row.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
			row.MCPSpendingDigest = spending
		}
		v.Operations = append(v.Operations, row)
	}
	return v
}

// The actual input bodies are independently authenticated by the original
// source before this pure codec is called. A hypothetical future result is a
// scalar byte bound, not a normalized result or a completed-operation witness.
func localTerminalProspectiveBytesV2(run domain.Run, product domain.ProductLimits, profile string, configuration domain.ConfiguredInferenceProfileV1, source domain.AttemptExecutionSourceV1, ops []LocalCompactionOperationV1, op domain.AttemptOperationV1, followResultBytes, cap uint64) (uint64, error) {
	needsSuccessor := op.Kind == domain.AttemptOperationCompactionV1 || op.Kind == domain.AttemptOperationMCPToolV1
	if len(ops) >= 12 || (needsSuccessor && len(ops) >= 11) || op.Sequence != uint64(len(ops)+1) || !localTerminalNeedsV2(ops, op) || (op.Kind != domain.AttemptOperationModelV1 && op.Kind != domain.AttemptOperationCompactionV1 && op.Kind != domain.AttemptOperationMCPToolV1) || source.Validate() != nil || run.ID != op.Scope.RunID || run.TenantID != op.Scope.TenantID || run.SessionID != op.Scope.SessionID {
		return 0, domain.ErrMembershipDenied
	}
	pd, err := configuration.Digest()
	if err != nil || pd != profile || product.Validate() != nil || source.Scope.TenantID != op.Scope.TenantID || source.Scope.SessionID != op.Scope.SessionID || source.Scope.RunID != op.Scope.RunID || source.Scope.AttemptID != op.Scope.AttemptID || source.Scope.OwnerUserID != op.Scope.OwnerUserID || source.AgentLoopPolicyDigest != op.Scope.AgentLoopPolicyDigest {
		return 0, domain.ErrMembershipDenied
	}
	materials := make([]localCapsuleOperationMaterial, 0, len(ops)+2)
	for _, item := range ops {
		materials = append(materials, localCapsuleOperationMaterial{reservation: item.Reservation, observation: item.Observation, normalized: item.Response, bytes: uint64(len(item.Response))})
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
		n = min(followResultBytes, uint64(64<<10))
		if n == 0 {
			return 0, ErrTerminalCapacity
		}
		follow.ResultBytes = n
		materials = append(materials, localTerminalFutureMaterial(follow, n))
	}
	toolEvents, err := localTerminalCanonicalPairsUpperV2(configuration, ops, op)
	if err != nil {
		return 0, err
	}
	defer func() {
		for i := range toolEvents {
			clear(toolEvents[i].Payload)
		}
	}()
	manifest := domain.ArtifactManifest{ID: canonicaloutput.ManifestIDV1(run.ID), TenantID: run.TenantID, RunID: run.ID, CreatedAt: localTerminalSizeTime}
	encoded, err := canonicaloutput.EncodeCompletionV1(run.ID, product, ports.ExecutionResult{Summary: "x", ToolEvents: toolEvents}, manifest.ID)
	if err != nil {
		return 0, ErrTerminalCapacity
	}
	defer encoded.Release()
	events := make([]localCapsuleEventV1, 0, len(toolEvents)+1)
	var toolBytes uint64
	maxToolEvents, maxToolBytes := product.EffectiveToolEventLimits()
	for i, event := range encoded.Events() {
		draft, err := canonicaloutput.PlanEventDraftV1(run, event.ID, event.Kind, event.Payload, run.CreatedAt)
		if err != nil {
			return 0, domain.ErrMembershipDenied
		}
		upper := uint64(len(event.Payload))
		if i == len(toolEvents) {
			// The containing normalized MODEL body bounds its JSON-escaped
			// final display, including quotes, with unchanged string encoding.
			upper = upper - 3 + n
		} else if op.Kind == domain.AttemptOperationMCPToolV1 && i == len(toolEvents)-1 {
			// The complete normalized MCP result contains EVERY quoted text
			// string once. Canonical result content keeps those strings and
			// removes item wrappers. Adding the actual empty-result envelope
			// is a conservative bound under the SAME JSON encoder/escaping.
			extra, err := localTerminalFutureResultExtraV2(op, toolEvents[i-1])
			if err != nil {
				return 0, err
			}
			upper += extra
		}
		if upper > sessionmaterial.MaxPayloadBytesV1 || upper > math.MaxInt64 {
			return 0, ErrTerminalCapacity
		}
		draft.Payload.Size = int64(upper)
		if i < len(toolEvents) {
			if upper > maxToolBytes-toolBytes {
				return 0, ErrTerminalCapacity
			}
			toolBytes += upper
		}
		events = append(events, localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload})
	}
	if len(toolEvents) == 0 || uint64(len(toolEvents)) > uint64(maxToolEvents) {
		return 0, ErrTerminalCapacity
	}
	derived, err := localTerminalDerivedUpper(run, op)
	if err != nil {
		return 0, err
	}
	if err := localTerminalRunUpperFits(run, op.Scope, derived, events[len(events)-1].ID, cap); err != nil {
		return 0, err
	}
	accounting := localTerminalAccountingUpperV3(source, op.Scope, materials)
	sink := localCapsuleSink{limit: cap}
	if err := encodeLocalTerminalCapsuleV2To(&sink, op.Scope, profile, strings.Repeat("0", 64), materials, derived, events, n, manifest, accounting); err != nil {
		return 0, err
	}
	return sink.size, nil
}

func localTerminalFutureResultExtraV2(op domain.AttemptOperationV1, call ports.ExecutionToolEvent) (uint64, error) {
	var content struct {
		ID        string          `json:"call_id"`
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if json.Unmarshal(call.Payload, &content) != nil || content.ID != op.CallID || len(content.Arguments) == 0 {
		return 0, domain.ErrMembershipDenied
	}
	// Lower-bound the original normalized grammar using its exact known
	// identities and argument bytes. A real authenticated grant object is at
	// least {}, so this subtraction never assumes a smaller real envelope.
	// This deliberately invalid grant is private LENGTH DATA, not evidence.
	d := strings.Repeat("0", 64)
	minimum, err := json.Marshal(struct {
		Schema              string                         `json:"schema"`
		Grant               json.RawMessage                `json:"grant"`
		CallID              string                         `json:"call_id"`
		Arguments           json.RawMessage                `json:"arguments"`
		ModelResponseDigest string                         `json:"model_response_digest"`
		RequestDigest       string                         `json:"request_digest"`
		OperationDigest     string                         `json:"operation_digest"`
		Result              json.RawMessage                `json:"result"`
		Usage               domain.AttemptOperationUsageV1 `json:"usage"`
	}{"sessionless.mcp-tool-result.v1", json.RawMessage(`{}`), content.ID, content.Arguments, d, d, d, json.RawMessage(`{"content":[],"isError":false}`), domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}})
	defer clear(minimum)
	n := min(op.ResultBytes, uint64(64<<10))
	if err != nil || n < uint64(len(minimum)) {
		return 0, ErrTerminalCapacity
	}
	return n - uint64(len(minimum)), nil
}

func localTerminalCanonicalPairsUpperV2(profile domain.ConfiguredInferenceProfileV1, ops []LocalCompactionOperationV1, future domain.AttemptOperationV1) ([]ports.ExecutionToolEvent, error) {
	var tools []ports.ExecutionToolEvent
	var pending *configuredinference.FunctionCallV1
	var model LocalCompactionOperationV1
	ok := false
	defer func() {
		if pending != nil {
			clear(pending.Arguments)
		}
		if !ok {
			for i := range tools {
				clear(tools[i].Payload)
			}
		}
	}()
	for i, item := range ops {
		if item.Reservation.Operation.Scope != future.Scope || item.Reservation.Operation.Sequence != uint64(i+1) || item.Reservation.Validate() != nil || item.Observation.ValidateForReservation(item.Reservation) != nil || item.Observation.Outcome != domain.AttemptOperationCompletedV1 || item.Observation.Reason != domain.AttemptOperationReasonNoneV1 || item.Observation.Usage.ExceedsReservation(item.Reservation.Operation.Debit) {
			return nil, domain.ErrMembershipDenied
		}
		requestHash, responseHash := sha256.Sum256(item.Request), sha256.Sum256(item.Response)
		if uint64(len(item.Request)) != item.Reservation.Operation.RequestBytes || uint64(len(item.Response)) != item.Observation.ResponseBytes || hex.EncodeToString(requestHash[:]) != item.Reservation.Operation.RequestDigest || hex.EncodeToString(responseHash[:]) != item.Observation.ResponseDigest || (i > 0 && item.Reservation.Operation.ValidatePredecessor(ops[i-1].Reservation, ops[i-1].Observation) != nil) {
			return nil, domain.ErrMembershipDenied
		}
		if item.Reservation.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			result, err := mcpadmission.DecodeSealedToolResultDataV1(item.Reservation, item.Observation, item.Response)
			if err != nil || result.IsError() || pending == nil || result.ValidateConfiguredCallV1(profile, model.Reservation, model.Observation, model.Request, model.Response) != nil {
				return nil, domain.ErrMembershipDenied
			}
			pair, err := localMCPCanonicalToolEvents(pending.ID, pending.Name, pending.Arguments, result)
			if err != nil {
				return nil, err
			}
			tools = append(tools, pair...)
			clear(pending.Arguments)
			pending = nil
			continue
		}
		if pending != nil || (item.Reservation.Operation.Kind != domain.AttemptOperationModelV1 && item.Reservation.Operation.Kind != domain.AttemptOperationCompactionV1) {
			return nil, domain.ErrMembershipDenied
		}
		decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(profile, item.Request, item.Response, item.Reservation.Operation.Kind)
		if err != nil {
			if decoded.Call != nil {
				clear(decoded.Call.Arguments)
			}
			return nil, domain.ErrMembershipDenied
		}
		if decoded.Call != nil {
			pending, model = decoded.Call, item
			if decoded.Stop != configuredinference.StopToolV1 || item.Reservation.Operation.Kind != domain.AttemptOperationModelV1 {
				return nil, domain.ErrMembershipDenied
			}
		} else if decoded.Stop != configuredinference.StopFinalV1 {
			return nil, domain.ErrMembershipDenied
		}
	}
	if future.Kind == domain.AttemptOperationMCPToolV1 {
		if pending == nil || future.CallID != pending.ID || future.ModelResponseDigest != model.Observation.ResponseDigest || future.ToolGrantDigest == "" {
			return nil, domain.ErrMembershipDenied
		}
		args := sha256.Sum256(pending.Arguments)
		if future.ArgumentsDigest != hex.EncodeToString(args[:]) {
			return nil, domain.ErrMembershipDenied
		}
		callPayload, err := json.Marshal(struct {
			ID        string          `json:"call_id"`
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}{pending.ID, pending.Name, pending.Arguments})
		if err != nil {
			return nil, domain.ErrMembershipDenied
		}
		resultPayload, err := json.Marshal(struct {
			ID      string   `json:"call_id"`
			IsError bool     `json:"is_error"`
			Content []string `json:"content"`
		}{pending.ID, false, []string{}})
		if err != nil {
			clear(callPayload)
			return nil, domain.ErrMembershipDenied
		}
		tools = append(tools, ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: pending.ID, ToolName: pending.Name, Payload: callPayload}, ports.ExecutionToolEvent{Kind: domain.SessionEventToolResult, CallID: pending.ID, ToolName: pending.Name, Payload: resultPayload})
	} else if pending != nil {
		return nil, domain.ErrMembershipDenied
	}
	ok = true
	return tools, nil
}

// Before tools/call, reserve feasibility of one unchanged final MODEL within
// the original remaining accounting/body/token/time envelopes. This is not a
// claim that arbitrary future content fits the mandatory actual MODEL2 codec.
func (entry *LocalInvocationEntryV1) terminalMCPSuccessorBudgetV2(ctx context.Context, source *LocalCompactionSourceV1, op domain.AttemptOperationV1) error {
	policy := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if policy == nil || op.Kind != domain.AttemptOperationMCPToolV1 {
		return domain.ErrMembershipDenied
	}
	limits, cfg := policy.Limits, entry.owner.configuration
	next, err := source.state.snapshot.Accounting.Aggregate.Add(op.Debit)
	if err != nil || next.Operations >= limits.MaxOperations || next.ModelCalls >= limits.MaxModelCalls || next.InputTokens >= limits.MaxAggregateInputTokens || next.OutputTokens >= limits.MaxAggregateOutputTokens || next.BodyBytes >= entry.localOperationBodyLimit() || next.ActiveDuration >= limits.MaxActiveDuration {
		return ErrTerminalCapacity
	}
	input := min(cfg.Limits.MaxInputTokens, limits.MaxInputTokensPerCall, limits.MaxAggregateInputTokens-next.InputTokens)
	output := min(uint64(2048), cfg.Limits.MaxOutputTokens, limits.MaxOutputTokensPerCall)
	if input <= 1 || output == 0 || output > limits.MaxAggregateOutputTokens-next.OutputTokens || output >= cfg.ModelWindowTokens {
		return ErrTerminalCapacity
	}
	request := min(cfg.Limits.RequestBytes, input-1, cfg.ModelWindowTokens-output-1)
	result := min(uint64(64<<10), cfg.Limits.ResponseBytes)
	if request == 0 || result == 0 || request > entry.localOperationBodyLimit()-next.BodyBytes || result > entry.localOperationBodyLimit()-next.BodyBytes-request {
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
	if err != nil || life.Err() != nil || ctx == nil || ctx.Err() != nil || entry.invocationContext.Err() != nil {
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
