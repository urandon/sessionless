package ydbstore

import (
	"fmt"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// Shape DATA only: the caller still must authenticate Run/anchor, immutable
// capsule/accounting/actual bodies and canonical correspondence. These values
// do not grant history restoration, continuation or a fabricated terminal seal.
func localMCPSegmentShapeV2(pairs int) (domain.Run, domain.SessionEvent, uint64, []sessioncontext.EventPayload) {
	at := time.Date(2026, 10, 23, 11, 12, 13, 0, time.UTC)
	run := domain.Run{ID: "shape-run", TenantID: "shape-tenant", SessionID: "shape-session"}
	trigger := uint64(7)
	anchor := domain.SessionEvent{ID: "shape-assistant", TenantID: run.TenantID, SessionID: run.SessionID, Kind: domain.SessionEventAssistantMessage, Sequence: trigger + uint64(2*pairs) + 1, CreatedAt: at}
	var records []sessioncontext.EventPayload
	for i := 0; i < 2*pairs; i++ {
		id := run.ID
		kind := domain.SessionEventToolCall
		if i%2 == 1 {
			kind = domain.SessionEventToolResult
		}
		records = append(records, sessioncontext.EventPayload{Event: domain.SessionEvent{ID: domain.SessionEventID(fmt.Sprintf("shape-tool-%d", i)), TenantID: run.TenantID, SessionID: run.SessionID, RunID: &id, Kind: kind, Sequence: trigger + uint64(i) + 1, CreatedAt: at}})
	}
	return run, anchor, trigger, records
}

func TestLocalMCPCanonicalSegmentV2BoundedPositiveShape(t *testing.T) {
	for _, pairs := range []int{0, 1, 4} {
		t.Run(fmt.Sprintf("pairs_%d", pairs), func(t *testing.T) {
			run, anchor, trigger, records := localMCPSegmentShapeV2(pairs)
			// The interval gate intentionally ignores unrelated records outside
			// the segment; the enclosing canonical source authenticates those.
			records = append([]sessioncontext.EventPayload{{Event: domain.SessionEvent{Sequence: 1, Kind: domain.SessionEventSystemNotice}}}, records...)
			records = append(records, sessioncontext.EventPayload{Event: domain.SessionEvent{Sequence: anchor.Sequence + 1, Kind: domain.SessionEventUserMessage}})
			if !localHistoricalCanonicalSegment(run, anchor, trigger, records) {
				t.Fatal("exact bounded same-run alternating segment denied", pairs)
			}
		})
	}
}

func TestLocalMCPCanonicalSegmentV2RejectsForeignInterleavedOrIncompleteShape(t *testing.T) {
	for _, name := range []string{"foreign_run", "nil_run", "foreign_tenant", "foreign_session", "user", "system", "interleaved_user", "missing_call", "missing_result", "reordered", "reversed_pair", "duplicate_sequence", "gap", "wrong_time_before", "wrong_time_after", "anchor_wrong_time", "zero_trigger", "backward_anchor", "even_distance", "too_many_pairs"} {
		t.Run(name, func(t *testing.T) {
			run, anchor, trigger, records := localMCPSegmentShapeV2(2)
			switch name {
			case "foreign_run":
				id := domain.RunID("foreign-run")
				records[0].Event.RunID = &id
			case "nil_run":
				records[0].Event.RunID = nil
			case "foreign_tenant":
				records[0].Event.TenantID = "foreign-tenant"
			case "foreign_session":
				records[0].Event.SessionID = "foreign-session"
			case "user":
				records[0].Event.Kind = domain.SessionEventUserMessage
			case "system":
				records[0].Event.Kind = domain.SessionEventSystemNotice
			case "interleaved_user":
				records[2].Event.Kind = domain.SessionEventUserMessage
				records[2].Event.RunID = nil
			case "missing_call":
				records = records[1:]
			case "missing_result":
				records = records[:len(records)-1]
			case "reordered":
				records[0], records[1] = records[1], records[0]
			case "reversed_pair":
				records[0].Event.Kind, records[1].Event.Kind = domain.SessionEventToolResult, domain.SessionEventToolCall
			case "duplicate_sequence":
				records[1].Event.Sequence = records[0].Event.Sequence
			case "gap":
				records[1].Event.Sequence++
			case "wrong_time_before":
				records[0].Event.CreatedAt = anchor.CreatedAt.Add(-time.Microsecond)
			case "wrong_time_after":
				records[0].Event.CreatedAt = anchor.CreatedAt.Add(time.Microsecond)
			case "anchor_wrong_time":
				anchor.CreatedAt = anchor.CreatedAt.Add(time.Microsecond)
			case "zero_trigger":
				trigger = 0
			case "backward_anchor":
				anchor.Sequence = trigger
			case "even_distance":
				anchor.Sequence--
			case "too_many_pairs":
				run, anchor, trigger, records = localMCPSegmentShapeV2(5)
			}
			if localHistoricalCanonicalSegment(run, anchor, trigger, records) {
				t.Fatal("foreign/interleaved/incomplete shape accepted", name)
			}
		})
	}
}
