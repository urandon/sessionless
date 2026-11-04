package compactioncontext

import (
	"bytes"
	"encoding/json"
	"strings"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/strictjson"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

type HistoricalRunV3 struct {
	Version       uint32
	Scope         domain.AttemptOperationScopeV1
	Anchor        domain.SessionEventID
	Operations    []HistoricalOperationV1
	Tools         []ToolWitnessV3
	Projections   []CanonicalProjectionV2
	ModelCatalogs []ModelCatalogV3
}

// V3 projection provenance remains transient in this in-fit slice. No old
// retained codec serializes it, and no retained-V3 writer exists here.
type projectedOperationV3 struct {
	Reservation domain.AttemptOperationReservationV1
	Observation domain.AttemptOperationObservationV1
	Witness     *ToolWitnessV3
}

func (route *contextRouteV3) historical(profile domain.ConfiguredInferenceProfileV1, old HistoricalRunV3, records []sessioncontext.EventPayload) ([]historicalFrameV1, []retainedSourceV1, error) {
	deny := func() ([]historicalFrameV1, []retainedSourceV1, error) { return nil, nil, ErrCanonical }
	if old.Version != 3 || old.Scope.Validate() != nil || old.Scope.TenantID != profile.TenantID || old.Scope.OwnerUserID != profile.OwnerUserID || len(records) == 0 || old.Scope.SessionID != records[0].Event.SessionID || old.Anchor.Validate() != nil || len(old.Operations) < 3 || len(old.Operations) > 12 || len(old.Projections) < 3 || len(old.Projections) > 9 || len(old.Projections)%2 != 1 || len(old.Tools) < 1 || len(old.Tools) > 4 || len(old.ModelCatalogs) < 2 || len(old.ModelCatalogs) > 8 {
		return deny()
	}
	witnesses := make(map[uint64]ToolWitnessV3)
	for _, witness := range old.Tools {
		if !witness.validShape() || witness.Scope != old.Scope {
			return deny()
		}
		if _, exists := witnesses[witness.Sequence]; exists {
			return deny()
		}
		witnesses[witness.Sequence] = witness
	}
	projections := make(map[uint64]CanonicalProjectionV2)
	projectionIDs := make(map[domain.SessionEventID]bool)
	for i, projection := range old.Projections {
		if projection.OperationSequence == 0 || projection.OperationSequence > uint64(len(old.Operations)) || projection.EventID.Validate() != nil || projection.Payload.Validate() != nil || projection.Payload.TenantID != old.Scope.TenantID || projectionIDs[projection.EventID] || i > 0 && projection.OperationSequence <= old.Projections[i-1].OperationSequence {
			return deny()
		}
		projections[projection.OperationSequence] = projection
		projectionIDs[projection.EventID] = true
	}
	var frames []historicalFrameV1
	var sources []retainedSourceV1
	var pending *configuredinference.FunctionCallV1
	var pendingObservation domain.AttemptOperationObservationV1
	var pendingTool *ports.ExecutionToolEvent
	defer func() {
		if pending != nil {
			clear(pending.Arguments)
		}
	}()
	seenCalls := map[string]bool{}
	var tools []ports.ExecutionToolEvent
	defer func() {
		for _, event := range tools {
			clear(event.Payload)
		}
	}()
	finalText, catalogIndex, searches := "", 0, 0
	for i, item := range old.Operations {
		r, o := item.Reservation, item.Observation
		if r.Validate() != nil || o.ValidateForReservation(r) != nil || r.Operation.Scope != old.Scope || r.Operation.Sequence != uint64(i+1) || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(r.Operation.Debit) || len(item.Normalized) == 0 || len(item.Normalized) > 64<<10 || uint64(len(item.Normalized)) != o.ResponseBytes || retainedDigest(item.Normalized) != o.ResponseDigest || i > 0 && r.Operation.ValidatePredecessor(old.Operations[i-1].Reservation, old.Operations[i-1].Observation) != nil {
			return deny()
		}
		projection, mapped := projections[r.Operation.Sequence]
		var witness *ToolWitnessV3
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 || r.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			if pending == nil || pendingTool == nil || i == len(old.Operations)-1 || item.Projection != "tool_result" || !mapped || projection.Kind != domain.SessionEventToolResult {
				return deny()
			}
			data, found := witnesses[r.Operation.Sequence]
			if !found || data.Kind != r.Operation.Kind {
				return deny()
			}
			if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
				result, err := mcpadmission.DecodeSealedToolResultDataV1(r, o, item.Normalized)
				if err != nil || result.IsError() || !toolPairMatches(pending, pendingObservation, result) || data.MCP == nil || !data.MCP.matches(r, o) || !historicalMCPCatalogV3(profile, item.Normalized, old.Operations[i-1].Reservation.Operation.Sequence, old.ModelCatalogs) {
					return deny()
				}
				tools = append(tools, ports.ExecutionToolEvent{Kind: domain.SessionEventToolResult, CallID: result.CallID(), ToolName: result.Name(), Payload: []byte(toolResultPayload(result))})
			} else {
				if !data.matchesSearch(r, o, item.Normalized) {
					return deny()
				}
				pair, err := searchCanonicalPairV3(profile, pending, pendingObservation, r, data.Search.Result)
				if err != nil || !bytes.Equal(pair[0].Payload, pendingTool.Payload) {
					return deny()
				}
				clear(pair[0].Payload)
				tools = append(tools, pair[1])
				searches++
			}
			copy := data
			witness = &copy
			delete(witnesses, r.Operation.Sequence)
			clear(pending.Arguments)
			pending, pendingTool = nil, nil
		} else {
			if pending != nil {
				return deny()
			}
			response, err := configuredinference.DecodeRetainedNormalizedDataV1(profile, r, o, item.Normalized)
			if response.Call != nil {
				pending = response.Call // Clear on every subsequent denial path.
			}
			if err != nil || response.Stop != configuredinference.StopFinalV1 && (response.Stop != configuredinference.StopToolV1 || response.Call == nil) {
				return deny()
			}
			var catalog []configuredinference.ToolV1
			if r.Operation.Kind == domain.AttemptOperationModelV1 {
				if catalogIndex >= len(old.ModelCatalogs) || old.ModelCatalogs[catalogIndex].Sequence != r.Operation.Sequence {
					return deny()
				}
				catalog, err = DecodeModelCatalogV3(old.ModelCatalogs[catalogIndex].Tools, profile.Limits)
				catalogIndex++
				if err != nil {
					return deny()
				}
			}
			switch {
			case i == len(old.Operations)-1:
				if item.Projection != "assistant" || r.Operation.Kind != domain.AttemptOperationModelV1 || response.Call != nil || !mapped || projection.Kind != domain.SessionEventAssistantMessage || projection.EventID != old.Anchor {
					return deny()
				}
				finalText = response.Text
			case r.Operation.Kind == domain.AttemptOperationCompactionV1:
				if item.Projection != "compaction" || response.Call != nil || mapped {
					return deny()
				}
			case r.Operation.Kind == domain.AttemptOperationModelV1:
				if response.Call != nil {
					pending, pendingObservation = response.Call, o
					advertised := false
					for _, tool := range catalog {
						advertised = advertised || tool.Name == pending.Name
					}
					if !advertised || item.Projection != "tool_call" || !mapped || projection.Kind != domain.SessionEventToolCall || seenCalls[pending.ID] {
						return deny()
					}
					seenCalls[pending.ID] = true
					payload := []byte(toolCallPayload(pending))
					if pending.Name == "web_search" {
						clear(payload)
						payload = searchCallPayloadV3(pending, old.Operations[i+1].Reservation.Operation)
					}
					tools = append(tools, ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: pending.ID, ToolName: pending.Name, Payload: payload})
					pendingTool = &tools[len(tools)-1]
				} else {
					if item.Projection != "retained_only" || mapped {
						return deny()
					}
					key, err := OperationSourceKeyV1(r, o)
					if err != nil {
						return deny()
					}
					frames = append(frames, historicalFrameV1{Kind: "operation", SourceID: key, Role: "assistant_message", RunID: string(old.Scope.RunID), Sequence: r.Operation.Sequence, Payload: response.Text, Digest: o.ResponseDigest})
					sources = append(sources, retainedSourceV1{SourceID: key, Operation: &retainedOperationV1{Reservation: r.Clone(), Observation: o.Clone()}})
				}
			default:
				return deny()
			}
		}
		if mapped {
			if _, exists := route.projected[projection.EventID]; exists {
				return deny()
			}
			route.projected[projection.EventID] = projectedOperationV3{Reservation: r.Clone(), Observation: o.Clone(), Witness: witness}
			delete(projections, r.Operation.Sequence)
		}
	}
	if pending != nil || len(witnesses) != 0 || len(projections) != 0 || searches < 1 || searches > 2 || catalogIndex != len(old.ModelCatalogs) {
		return deny()
	}
	encoded, err := canonicaloutput.EncodeCompletionV1(old.Scope.RunID, domain.ProductLimits{MaxToolEvents: 8, MaxToolEventBytes: MaxBytesV1}, ports.ExecutionResult{Summary: finalText, ToolEvents: tools}, canonicaloutput.ManifestIDV1(old.Scope.RunID))
	if err != nil {
		return deny()
	}
	defer encoded.Release()
	expected := encoded.Events()
	if len(expected) != len(old.Projections) {
		return deny()
	}
	byID := make(map[domain.SessionEventID]sessioncontext.EventPayload)
	for _, record := range records {
		if record.Event.RunID != nil && *record.Event.RunID == old.Scope.RunID && (record.Event.Kind == domain.SessionEventToolCall || record.Event.Kind == domain.SessionEventToolResult || record.Event.Kind == domain.SessionEventAssistantMessage) {
			if !projectionIDs[record.Event.ID] {
				return deny()
			}
			if _, duplicate := byID[record.Event.ID]; duplicate {
				return deny()
			}
			byID[record.Event.ID] = record
		}
	}
	var previous uint64
	for i, event := range expected {
		mapping := old.Projections[i]
		record, exists := byID[mapping.EventID]
		provenance := route.projected[mapping.EventID]
		expectedKey := domain.SessionEventObjectPrefix(old.Scope.TenantID, old.Scope.SessionID, event.ID) + "payloads/sha256/" + retainedDigest(event.Payload) + ".json"
		if !exists || event.ID != mapping.EventID || event.Kind != mapping.Kind || record.Event.Kind != event.Kind || record.Event.Payload != mapping.Payload || mapping.Payload.Key != expectedKey || mapping.Payload.SHA256 != retainedDigest(event.Payload) || mapping.Payload.Size != int64(len(event.Payload)) || !bytes.Equal(record.Payload, event.Payload) || record.Event.Sequence <= previous || record.Event.CreatedAt.Before(provenance.Observation.ObservedAt) {
			return deny()
		}
		previous = record.Event.Sequence
	}
	return frames, sources, nil
}

func historicalMCPCatalogV3(profile domain.ConfiguredInferenceProfileV1, normalized []byte, sequence uint64, catalogs []ModelCatalogV3) bool {
	var envelope struct {
		Grant json.RawMessage `json:"grant"`
	}
	if json.Unmarshal(normalized, &envelope) != nil {
		return false
	}
	grant, err := mcpadmission.ParseMCPToolGrantV1(envelope.Grant)
	if err != nil {
		return false
	}
	descriptor, err := mcpwire.ParseDescriptorV1(grant.Descriptor, grant.Limits)
	if err != nil {
		return false
	}
	schema := descriptor.InputSchema()
	defer clear(schema)
	for _, catalog := range catalogs {
		if catalog.Sequence != sequence {
			continue
		}
		tools, err := DecodeModelCatalogV3(catalog.Tools, profile.Limits)
		if err != nil {
			return false
		}
		for _, tool := range tools {
			if tool.Name == descriptor.Name() {
				return tool.Description == grant.ModelDescription && bytes.Equal(tool.Parameters, schema)
			}
		}
	}
	return false
}

func searchCallPayloadV3(call *configuredinference.FunctionCallV1, op domain.AttemptOperationV1) []byte {
	body, _ := json.Marshal(struct {
		CallID          string `json:"call_id"`
		Name            string `json:"name"`
		ArgumentsDigest string `json:"arguments_digest"`
		QueryDigest     string `json:"query_digest"`
	}{call.ID, call.Name, op.ArgumentsDigest, op.QueryDigest})
	return body
}

func searchCanonicalPairV3(profile domain.ConfiguredInferenceProfileV1, call *configuredinference.FunctionCallV1, model domain.AttemptOperationObservationV1, reservation domain.AttemptOperationReservationV1, result domain.WebSearchResultV1) ([]ports.ExecutionToolEvent, error) {
	op := reservation.Operation
	if call == nil || call.Name != "web_search" || call.ID != op.CallID || op.SearchWire == nil || uint64(len(call.Arguments)) != op.SearchWire.ArgumentsBytes || retainedDigest(call.Arguments) != op.ArgumentsDigest || model.ResponseDigest != op.ModelResponseDigest || result.ValidateForOperation(op) != nil {
		return nil, ErrCanonical
	}
	// Reuse the actual canonical schema compiler, not another argument schema.
	if _, err := configuredinference.FreezeRequestV1(profile, profile.Limits, configuredinference.RequestV1{MaxOutputTokens: 1, Tools: []configuredinference.ToolV1{websearch.ModelToolV1()}, Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleAssistantV1, Call: call}, {Role: configuredinference.RoleToolV1, ToolCallID: call.ID, Text: "untrusted historical DATA"}}}); err != nil {
		return nil, ErrCanonical
	}
	value, err := strictjson.Parse(call.Arguments, domain.WebSearchArgumentsBytesV1, 32, 256)
	arguments, ok := value.(map[string]any)
	if err != nil || !ok {
		return nil, ErrCanonical
	}
	query, ok := arguments["query"].(string)
	if !ok || strings.TrimSpace(query) == "" || len(query) > domain.WebSearchQueryBytesV1 || retainedDigest([]byte(query)) != op.QueryDigest {
		return nil, ErrCanonical
	}
	resultBody, err := json.Marshal(struct {
		Schema    string                          `json:"schema"`
		Outcome   string                          `json:"outcome"`
		Operation domain.AttemptOperationDigestV1 `json:"operation_digest"`
		Sources   []domain.WebSearchSourceV1      `json:"sources"`
		Usage     domain.WebSearchUsageV1         `json:"usage"`
		Warnings  []domain.WebSearchWarningV1     `json:"warnings"`
	}{"sessionless.search-result.v4", result.Outcome, result.OperationDigest, result.Sources, result.Usage, result.Warnings})
	if err != nil {
		return nil, ErrCanonical
	}
	return []ports.ExecutionToolEvent{{Kind: domain.SessionEventToolCall, CallID: call.ID, ToolName: call.Name, Payload: searchCallPayloadV3(call, op)}, {Kind: domain.SessionEventToolResult, CallID: call.ID, ToolName: call.Name, Payload: resultBody}}, nil
}
