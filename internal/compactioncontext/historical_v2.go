package compactioncontext

import (
	"bytes"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// CanonicalProjectionV2 maps an original typed operation to exactly one
// canonical event. It is sealed-capsule DATA; the original reader authenticates
// the capsule/correspondence/marker and immutable actual records separately.
type CanonicalProjectionV2 struct {
	OperationSequence uint64                  `json:"operation_sequence"`
	EventID           domain.SessionEventID   `json:"event_id"`
	Kind              domain.SessionEventKind `json:"kind"`
	Payload           domain.BlobRef          `json:"payload"`
}

type HistoricalRunV2 struct {
	Scope           domain.AttemptOperationScopeV1
	Anchor          domain.SessionEventID
	Operations      []HistoricalOperationV1
	ConservativeMCP []MCPConservativeDataV2
	Projections     []CanonicalProjectionV2
}

// A canonical source keeps its exact operation provenance even though the
// already projected operation frame is not rendered a second time.
type projectedOperationV2 struct {
	Reservation  domain.AttemptOperationReservationV1 `json:"reservation"`
	Observation  domain.AttemptOperationObservationV1 `json:"observation"`
	Conservative *MCPConservativeDataV2               `json:"conservative,omitempty"`
}

func (route *contextRouteV2) historical(profile domain.ConfiguredInferenceProfileV1, old HistoricalRunV2, records []sessioncontext.EventPayload) ([]historicalFrameV1, []retainedSourceV1, error) {
	deny := func() ([]historicalFrameV1, []retainedSourceV1, error) { return nil, nil, ErrCanonical }
	if old.Scope.Validate() != nil || old.Scope.TenantID != profile.TenantID || old.Scope.OwnerUserID != profile.OwnerUserID || old.Scope.SessionID != records[0].Event.SessionID || old.Anchor.Validate() != nil || len(old.Operations) < 3 || len(old.Operations) > 12 || len(old.Projections) < 3 || len(old.Projections) > 9 || len(old.Projections)%2 != 1 || len(old.ConservativeMCP) > 4 {
		return deny()
	}
	conservative := make(map[uint64]MCPConservativeDataV2, len(old.ConservativeMCP))
	for _, row := range old.ConservativeMCP {
		if _, exists := conservative[row.Sequence]; exists || row.Scope != old.Scope || !row.validShape() {
			return deny()
		}
		conservative[row.Sequence] = row
	}
	projectionBySequence := make(map[uint64]CanonicalProjectionV2, len(old.Projections))
	projectionIDs := make(map[domain.SessionEventID]bool, len(old.Projections))
	for i, projection := range old.Projections {
		if projection.OperationSequence == 0 || projection.OperationSequence > uint64(len(old.Operations)) || projection.EventID.Validate() != nil || projection.Payload.Validate() != nil || projection.Payload.TenantID != old.Scope.TenantID || projectionIDs[projection.EventID] || (i > 0 && projection.OperationSequence <= old.Projections[i-1].OperationSequence) {
			return deny()
		}
		if _, exists := projectionBySequence[projection.OperationSequence]; exists {
			return deny()
		}
		projectionBySequence[projection.OperationSequence] = projection
		projectionIDs[projection.EventID] = true
	}
	var frames []historicalFrameV1
	var sources []retainedSourceV1
	var pending *configuredinference.FunctionCallV1
	var pendingObservation domain.AttemptOperationObservationV1
	usedCalls := make(map[string]bool)
	var tools []ports.ExecutionToolEvent
	defer func() {
		for i := range tools {
			clear(tools[i].Payload)
		}
	}()
	finalText := ""
	for i, item := range old.Operations {
		r, o := item.Reservation, item.Observation
		if r.Validate() != nil || o.ValidateForReservation(r) != nil || r.Operation.Scope != old.Scope || r.Operation.Sequence != uint64(i+1) || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(r.Operation.Debit) || (i > 0 && r.Operation.ValidatePredecessor(old.Operations[i-1].Reservation, old.Operations[i-1].Observation) != nil) {
			return deny()
		}
		projection, mapped := projectionBySequence[r.Operation.Sequence]
		var conservativeData *MCPConservativeDataV2
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			result, err := mcpadmission.DecodeSealedToolResultDataV1(r, o, item.Normalized)
			if err != nil || result.IsError() || pending == nil || i == len(old.Operations)-1 || item.Projection != "tool_result" || !mapped || projection.Kind != domain.SessionEventToolResult || !toolPairMatches(pending, pendingObservation, result) {
				return deny()
			}
			if row, ok := conservative[r.Operation.Sequence]; ok {
				if !row.matches(r, o) {
					return deny()
				}
				conservativeData = &row
				delete(conservative, r.Operation.Sequence)
			} else if !toolUsageContinuable(o) {
				return deny()
			}
			tools = append(tools, ports.ExecutionToolEvent{Kind: domain.SessionEventToolResult, CallID: result.CallID(), ToolName: result.Name(), Payload: []byte(toolResultPayload(result))})
			pending = nil
		} else {
			if pending != nil {
				return deny()
			}
			response, err := configuredinference.DecodeRetainedNormalizedDataV1(profile, r, o, item.Normalized)
			if err != nil || (response.Stop != configuredinference.StopFinalV1 && (response.Stop != configuredinference.StopToolV1 || response.Call == nil)) {
				return deny()
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
					if item.Projection != "tool_call" || !mapped || projection.Kind != domain.SessionEventToolCall || usedCalls[response.Call.ID] {
						return deny()
					}
					usedCalls[response.Call.ID] = true
					pending, pendingObservation = response.Call, o
					tools = append(tools, ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: response.Call.ID, ToolName: response.Call.Name, Payload: []byte(toolCallPayload(response.Call))})
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
			route.projected[projection.EventID] = projectedOperationV2{Reservation: r.Clone(), Observation: o.Clone(), Conservative: conservativeData}
			delete(projectionBySequence, r.Operation.Sequence)
		}
	}
	if pending != nil || len(conservative) != 0 || len(projectionBySequence) != 0 || len(tools) == 0 {
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
	byID := make(map[domain.SessionEventID]sessioncontext.EventPayload, len(expected))
	for _, record := range records {
		if record.Event.RunID != nil && *record.Event.RunID == old.Scope.RunID && (record.Event.Kind == domain.SessionEventToolCall || record.Event.Kind == domain.SessionEventToolResult || record.Event.Kind == domain.SessionEventAssistantMessage) {
			if !projectionIDs[record.Event.ID] {
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

func (v *projectedOperationV2) validFor(event domain.SessionEvent) bool {
	if v == nil || event.RunID == nil {
		return false
	}
	r, o := v.Reservation, v.Observation
	if r.Validate() != nil || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(r.Operation.Debit) || r.Operation.Scope.RunID != *event.RunID || r.Operation.Scope.TenantID != event.TenantID || r.Operation.Scope.SessionID != event.SessionID || event.CreatedAt.Before(o.ObservedAt) {
		return false
	}
	if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
		return event.Kind == domain.SessionEventToolResult && ((v.Conservative == nil && toolUsageContinuable(o)) || (v.Conservative != nil && v.Conservative.matches(r, o)))
	}
	return v.Conservative == nil && r.Operation.Kind == domain.AttemptOperationModelV1 && (event.Kind == domain.SessionEventToolCall || event.Kind == domain.SessionEventAssistantMessage)
}
