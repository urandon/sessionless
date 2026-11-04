package ydbstore

import (
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This explicit successor projection admits actual SEARCH pairs without
// widening the immutable MCP-only V2 language or changing its wire bytes.
func localCapsuleProjectionsV3(ops []localCapsuleOperationMaterial, events []localCapsuleEventV1) ([]localCapsuleProjectionV2, error) {
	deny := func() ([]localCapsuleProjectionV2, error) { return nil, domain.ErrMembershipDenied }
	if len(ops) < 3 || len(ops) > 12 || len(events) < 3 || len(events) > 9 || len(events)%2 != 1 || ops[len(ops)-1].reservation.Operation.Kind != domain.AttemptOperationModelV1 || events[len(events)-1].Kind != domain.SessionEventAssistantMessage {
		return deny()
	}
	seen := map[domain.SessionEventID]bool{}
	for _, event := range events {
		if event.ID == "" || seen[event.ID] {
			return deny()
		}
		seen[event.ID] = true
	}
	projections := make([]localCapsuleProjectionV2, len(ops))
	for i := range projections {
		projections[i] = localCapsuleProjectionV2{kind: "retained_only", events: []domain.SessionEventID{}}
	}
	next, searches := 0, 0
	for i, item := range ops {
		op := item.reservation.Operation
		if op.Sequence != uint64(i+1) || op.Scope != ops[0].reservation.Operation.Scope {
			return deny()
		}
		switch op.Kind {
		case domain.AttemptOperationModelV1:
		case domain.AttemptOperationCompactionV1:
			projections[i].kind = "compaction"
		case domain.AttemptOperationMCPToolV1, domain.AttemptOperationWebSearchV1:
			if i == 0 || i == len(ops)-1 || ops[i-1].reservation.Operation.Kind != domain.AttemptOperationModelV1 || next+2 >= len(events) || events[next].Kind != domain.SessionEventToolCall || events[next+1].Kind != domain.SessionEventToolResult {
				return deny()
			}
			projections[i-1] = localCapsuleProjectionV2{kind: "tool_call", events: []domain.SessionEventID{events[next].ID}}
			projections[i] = localCapsuleProjectionV2{kind: "tool_result", events: []domain.SessionEventID{events[next+1].ID}}
			next += 2
			if op.Kind == domain.AttemptOperationWebSearchV1 {
				searches++
			}
		default:
			return deny()
		}
	}
	if searches < 1 || searches > 2 || next != len(events)-1 {
		return deny()
	}
	projections[len(ops)-1] = localCapsuleProjectionV2{kind: "assistant", events: []domain.SessionEventID{events[next].ID}}
	return projections, nil
}

// The exact same bounded encoder is used for emitted bytes and prospective
// fit. The counting arm never persists a hypothetical observation or result.
func encodeLocalTerminalCapsuleV3To(sink *localCapsuleSink, scope domain.AttemptOperationScopeV1, profile, frontier string, ops []localCapsuleOperationMaterial, derived *domain.RunDerivedContextV1, events []localCapsuleEventV1, finalDisplayUpper uint64, manifest domain.ArtifactManifest, accounting ports.OwnedOperationAccountingDispositionV4, consents []domain.WebSearchConsentStateV1, catalogs []compactioncontext.ModelCatalogV3) error {
	if sink == nil || sink.limit == 0 || sink.limit > domain.MaxTerminalCapsuleBytesV1 || accounting.Version != 4 || accounting.Scope != scope || len(accounting.Operations) != len(ops) || sink.emit && finalDisplayUpper != 0 || len(consents) < 1 || len(consents) > 2 || len(catalogs) < 2 || len(catalogs) > 8 {
		return domain.ErrMembershipDenied
	}
	projections, err := localCapsuleProjectionsV3(ops, events)
	if err != nil || ops[0].reservation.Operation.Scope != scope {
		return domain.ErrMembershipDenied
	}
	add, value := sink.add, sink.value
	add([]byte(`{"schema":"sessionless.terminal-custody.v3","scope":`))
	value(scope)
	add([]byte(`,"profile_digest":`))
	value(profile)
	add([]byte(`,"codec_version":3,"frontier_digest":`))
	value(frontier)
	add([]byte(`,"operations":[`))
	var normalized uint64
	for i, item := range ops {
		if item.bytes == 0 || item.bytes > 64<<10 || item.bytes > (384<<10)-normalized || sink.emit && uint64(len(item.normalized)) != item.bytes {
			return domain.ErrMembershipDenied
		}
		normalized += item.bytes
		if i > 0 {
			add([]byte(","))
		}
		add([]byte(`{"reservation":`))
		value(item.reservation)
		add([]byte(`,"observation":`))
		value(item.observation)
		add([]byte(`,"normalized_body":`))
		if sink.emit {
			add(item.normalized)
		} else {
			sink.count(item.bytes)
		}
		add([]byte(`,"projection":`))
		value(projections[i].kind)
		add([]byte(`,"event_ids":`))
		value(projections[i].events)
		add([]byte("}"))
	}
	add([]byte(`],"derived_context":`))
	value(derived)
	add([]byte(`,"events":[`))
	for i, event := range events {
		if i > 0 {
			add([]byte(","))
		}
		add([]byte(`{"id":`))
		value(event.ID)
		add([]byte(`,"kind":`))
		value(event.Kind)
		add([]byte(`,"idempotency_key":`))
		value(event.IdempotencyKey)
		add([]byte(`,"payload":`))
		value(event.Payload)
		add([]byte(`,"display_text":`))
		if i == len(events)-1 && finalDisplayUpper != 0 {
			sink.count(finalDisplayUpper)
		} else {
			value(event.DisplayText)
		}
		add([]byte("}"))
	}
	add([]byte(`],"public_manifest":`))
	value(manifest)
	add([]byte(`,"accounting_disposition":`))
	value(accounting)
	add([]byte(`,"consents":`))
	value(consents)
	add([]byte(`,"model_catalogs":`))
	value(catalogs)
	add([]byte("}"))
	if sink.failed {
		return ErrTerminalCapacity
	}
	return nil
}
