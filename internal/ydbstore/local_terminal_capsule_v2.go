package ydbstore

import (
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// These are wire correspondences only, not continuation or publication
// authority. The original composer must authenticate the normalized call/result
// bodies and current Source/Store before supplying this closed plan.
type localCapsuleProjectionV2 struct {
	kind   string
	events []domain.SessionEventID
}

// localCapsuleProjectionsV2 fixes the operation -> canonical-event map. Every
// MCP is immediately preceded by its MODEL and followed by another operation;
// the last operation is the final MODEL. Non-tool MODEL/COMPACTION records stay
// retained-only. IDs cannot be reused, omitted, reordered or supplied as extras.
func localCapsuleProjectionsV2(ops []localCapsuleOperationMaterial, events []localCapsuleEventV1) ([]localCapsuleProjectionV2, error) {
	deny := func() ([]localCapsuleProjectionV2, error) { return nil, domain.ErrMembershipDenied }
	if len(ops) < 3 || len(ops) > 12 || len(events) < 3 || len(events) > 25 || len(events)%2 != 1 || ops[len(ops)-1].reservation.Operation.Kind != domain.AttemptOperationModelV1 || events[len(events)-1].Kind != domain.SessionEventAssistantMessage {
		return deny()
	}
	seen := make(map[domain.SessionEventID]bool, len(events))
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
	next := 0
	for i, item := range ops {
		op := item.reservation.Operation
		if op.Sequence != uint64(i+1) || op.Scope != ops[0].reservation.Operation.Scope {
			return deny()
		}
		switch op.Kind {
		case domain.AttemptOperationCompactionV1:
			projections[i].kind = "compaction"
		case domain.AttemptOperationModelV1:
			// A following MCP owns this MODEL's call projection below.
		case domain.AttemptOperationMCPToolV1:
			if i == 0 || i == len(ops)-1 || ops[i-1].reservation.Operation.Kind != domain.AttemptOperationModelV1 || next+2 >= len(events) || events[next].Kind != domain.SessionEventToolCall || events[next+1].Kind != domain.SessionEventToolResult {
				return deny()
			}
			projections[i-1] = localCapsuleProjectionV2{kind: "tool_call", events: []domain.SessionEventID{events[next].ID}}
			projections[i] = localCapsuleProjectionV2{kind: "tool_result", events: []domain.SessionEventID{events[next+1].ID}}
			next += 2
		default:
			return deny()
		}
	}
	if next == 0 || next != len(events)-1 {
		return deny()
	}
	projections[len(ops)-1] = localCapsuleProjectionV2{kind: "assistant", events: []domain.SessionEventID{events[next].ID}}
	return projections, nil
}

// The emitting and prospective arms share the exact same encoder. No fake
// future observation is persisted or accepted here as an authenticated receipt.
func encodeLocalTerminalCapsuleV2To(sink *localCapsuleSink, scope domain.AttemptOperationScopeV1, profile, frontier string, ops []localCapsuleOperationMaterial, derived *domain.RunDerivedContextV1, events []localCapsuleEventV1, finalDisplayUpper uint64, manifest domain.ArtifactManifest, accounting ports.OwnedOperationAccountingDispositionV3) error {
	if len(ops) == 0 || ops[0].reservation.Operation.Scope != scope {
		return domain.ErrMembershipDenied
	}
	projections, err := localCapsuleProjectionsV2(ops, events)
	if err != nil {
		return err
	}
	return encodeLocalTerminalCapsuleVersionTo(sink, scope, profile, frontier, ops, derived, events, finalDisplayUpper, manifest, 2, projections, &accounting)
}
