package ydbstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Wire arithmetic DATA only. These deliberately do not construct a Source,
// authenticated observation, current financial witness or executable writer.
func terminalCapsuleV2WireData() ([]localCapsuleOperationMaterial, []localCapsuleEventV1) {
	ops := make([]localCapsuleOperationMaterial, 3)
	for i, kind := range []domain.AttemptOperationKindV1{domain.AttemptOperationModelV1, domain.AttemptOperationMCPToolV1, domain.AttemptOperationModelV1} {
		raw := []byte(`{"text":"bounded result"}`)
		ops[i] = localCapsuleOperationMaterial{reservation: domain.AttemptOperationReservationV1{Operation: domain.AttemptOperationV1{Sequence: uint64(i + 1), Kind: kind}}, normalized: raw, bytes: uint64(len(raw))}
	}
	events := []localCapsuleEventV1{
		{ID: "tool-call", Kind: domain.SessionEventToolCall},
		{ID: "tool-result", Kind: domain.SessionEventToolResult},
		{ID: "assistant", Kind: domain.SessionEventAssistantMessage, DisplayText: "\"\\\n<>&\u2028🙂"},
	}
	return ops, events
}

func TestLocalTerminalCapsuleV2CountProjectionAndBoundary(t *testing.T) {
	ops, events := terminalCapsuleV2WireData()
	accounting := ports.OwnedOperationAccountingDispositionV3{Version: 3, Operations: make([]ports.OwnedOperationAccountingRowV3, len(ops))} // wire DATA only
	const cap = domain.MaxTerminalCapsuleBytesV1
	emit := localCapsuleSink{limit: cap, emit: true}
	if err := encodeLocalTerminalCapsuleV2To(&emit, domain.AttemptOperationScopeV1{}, "profile", "frontier", ops, nil, events, 0, domain.ArtifactManifest{}, accounting); err != nil {
		t.Fatal(err)
	}
	var wire localRetainedTerminalCapsuleV1 // same JSON fields, different explicit version
	if err := json.Unmarshal(emit.body, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Schema != "sessionless.terminal-custody.v2" || wire.CodecVersion != 2 || len(wire.Operations) != 3 || !reflect.DeepEqual(wire.Events, events) {
		t.Fatal("wrong version/event wire")
	}
	for i, projection := range []string{"tool_call", "tool_result", "assistant"} {
		if wire.Operations[i].Projection != projection || !reflect.DeepEqual(wire.Operations[i].EventIDs, []domain.SessionEventID{events[i].ID}) || !bytes.Equal(wire.Operations[i].NormalizedBody, ops[i].normalized) {
			t.Fatalf("operation %d correspondence changed", i)
		}
	}
	for _, upper := range []bool{false, true} {
		count := localCapsuleSink{limit: cap}
		var display uint64
		if upper {
			raw, err := json.Marshal(events[len(events)-1].DisplayText)
			if err != nil {
				t.Fatal(err)
			}
			display = uint64(len(raw))
		}
		if err := encodeLocalTerminalCapsuleV2To(&count, domain.AttemptOperationScopeV1{}, "profile", "frontier", ops, nil, events, display, domain.ArtifactManifest{}, accounting); err != nil || count.size != uint64(len(emit.body)) || count.body != nil {
			t.Fatalf("count/emit mismatch upper=%t size=%d err=%v", upper, count.size, err)
		}
	}
	for _, delta := range []uint64{0, 1} {
		sink := localCapsuleSink{limit: uint64(len(emit.body)) - delta, emit: true}
		err := encodeLocalTerminalCapsuleV2To(&sink, domain.AttemptOperationScopeV1{}, "profile", "frontier", ops, nil, events, 0, domain.ArtifactManifest{}, accounting)
		if delta == 0 && (err != nil || !bytes.Equal(sink.body, emit.body)) || delta == 1 && !errors.Is(err, ErrTerminalCapacity) {
			t.Fatalf("exact boundary delta=%d err=%v", delta, err)
		}
	}
	legacy := localCapsuleSink{limit: cap, emit: true}
	if err := encodeLocalTerminalCapsuleTo(&legacy, domain.AttemptOperationScopeV1{}, "profile", "frontier", ops, nil, events, 0, domain.ArtifactManifest{}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(legacy.body), `{"schema":"sessionless.terminal-custody.v1","scope":`) || json.Unmarshal(legacy.body, &wire) != nil || wire.CodecVersion != 1 {
		t.Fatal("V1 was silently upgraded")
	}
	for i, item := range wire.Operations {
		if item.Projection == "tool_call" || item.Projection == "tool_result" || i != len(wire.Operations)-1 && len(item.EventIDs) != 0 {
			t.Fatal("V1 acquired V2 operation projections")
		}
	}
	// Frozen V1 wire schema is an independent full-byte reference; do not
	// construct it through either versioned encoder or its projection helper.
	wantV1 := localRetainedTerminalCapsuleV1{Schema: "sessionless.terminal-custody.v1", ProfileDigest: "profile", CodecVersion: 1, FrontierDigest: "frontier", Events: events}
	for i, op := range ops {
		projection := "retained_only"
		ids := []domain.SessionEventID{}
		if i == len(ops)-1 {
			projection, ids = "assistant", []domain.SessionEventID{events[len(events)-1].ID}
		}
		wantV1.Operations = append(wantV1.Operations, localRetainedTerminalOperationV1{Reservation: op.reservation, Observation: op.observation, NormalizedBody: op.normalized, Projection: projection, EventIDs: ids})
	}
	wantBytes, err := json.Marshal(wantV1)
	if err != nil || !bytes.Equal(legacy.body, wantBytes) {
		t.Fatal("full V1 wire differs from frozen schema", err)
	}
}

func TestLocalTerminalCapsuleV2RejectsIncompleteProjectionBeforeEmission(t *testing.T) {
	for _, name := range []string{"missing_pair", "reordered_pair", "duplicate_id", "extra_event", "last_not_model", "missing_model", "sequence_gap", "scope_swap", "search"} {
		t.Run(name, func(t *testing.T) {
			ops, events := terminalCapsuleV2WireData()
			switch name {
			case "missing_pair":
				events = events[2:]
			case "reordered_pair":
				events[0], events[1] = events[1], events[0]
			case "duplicate_id":
				events[1].ID = events[0].ID
			case "extra_event":
				events = append(events[:2], append([]localCapsuleEventV1{{ID: "extra-call", Kind: domain.SessionEventToolCall}, {ID: "extra-result", Kind: domain.SessionEventToolResult}}, events[2:]...)...)
			case "last_not_model":
				ops[2].reservation.Operation.Kind = domain.AttemptOperationCompactionV1
			case "missing_model":
				ops[0].reservation.Operation.Kind = domain.AttemptOperationCompactionV1
			case "sequence_gap":
				ops[1].reservation.Operation.Sequence++
			case "scope_swap":
				ops[1].reservation.Operation.Scope.TenantID = "other"
			case "search":
				ops[1].reservation.Operation.Kind = domain.AttemptOperationWebSearchV1
			}
			sink := localCapsuleSink{limit: domain.MaxTerminalCapsuleBytesV1, emit: true}
			err := encodeLocalTerminalCapsuleV2To(&sink, domain.AttemptOperationScopeV1{}, "profile", "frontier", ops, nil, events, 0, domain.ArtifactManifest{}, ports.OwnedOperationAccountingDispositionV3{Version: 3, Operations: make([]ports.OwnedOperationAccountingRowV3, len(ops))})
			if !errors.Is(err, domain.ErrMembershipDenied) || sink.size != 0 || len(sink.body) != 0 {
				t.Fatalf("invalid map emitted bytes: %d err=%v", sink.size, err)
			}
		})
	}
}
