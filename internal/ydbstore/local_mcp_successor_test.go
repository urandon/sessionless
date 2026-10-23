package ydbstore

import (
	"encoding/json"
	"reflect"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Original private Source, prepared holders, Store reserve/BeforeSend,
// normalized bodies, issuer and durable readback are production. SQL, proxy
// attestation and external response bytes are scripted DATA; this is not a
// credentialed runtime, socket, terminal cleanup or canonical publication test.
func TestLocalMCPSuccessorOriginalModelAndFinalNoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, mcp := completedOriginalMCPFixture(t)
		sourceBefore := f.entry.source.Clone()
		mcpReservation, mcpObserved := mcp.reservation.Clone(), mcp.observation.Clone()
		next, err := f.entry.PrepareModel(t.Context())
		if err != nil || next == nil {
			t.Fatalf("genuine MODEL3 preparation after durable original MCP: model=%t err=%v", next != nil, err)
		}
		op, err := next.Operation()
		predecessor, _ := mcpObserved.Observation.Digest()
		if err != nil || op.Kind != domain.AttemptOperationModelV1 || op.Sequence != 3 || op.PredecessorObservationDigest != predecessor {
			t.Fatalf("MODEL3 original predecessor binding: seq=%d kind=%s err=%v", op.Sequence, op.Kind, err)
		}
		body := next.frozen.Body()
		defer clear(body)
		assertOriginalMCPNextRequestPair(t, body, string(op.Scope.RunID), mcp.call.ID())
		final := recordPreparedLocalResponse(t, f, op, next.frozen, "original result consumed in final answer")
		if !reflect.DeepEqual(final, next.observation) {
			t.Fatal("MODEL3 original holder did not retain exact final observation readback")
		}
		if prepared, err := f.entry.PrepareMCP(t.Context()); err != nil || prepared != nil || f.entry.preparedMCP.Load() != mcp {
			t.Fatalf("final MODEL3 must be verified no-op without replacing MCP holder: prepared=%t err=%v", prepared != nil, err)
		}
		if !reflect.DeepEqual(sourceBefore, f.entry.source) || !reflect.DeepEqual(mcpReservation, mcp.reservation) || !reflect.DeepEqual(mcpObserved, mcp.observation) || mcpObserved.Observation.Usage.State != domain.AttemptOperationUsageUnknownV1 || mcpObserved.Observation.Usage.AmountMicrounits != nil {
			t.Fatal("successor altered original Source money/reservation or manufactured measured MCP money")
		}
		source, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatal("read actual durable completed MODEL/MCP/MODEL frontier", err)
		}
		defer source.Clear()
		if len(source.state.operations) != 3 || source.state.snapshot.Accounting.Aggregate.Operations != 3 || source.state.snapshot.Accounting.Aggregate.ToolDispatches != 1 || source.state.snapshot.Accounting.Aggregate.MCPCatalogPages != 1 {
			t.Fatal("final no-op reserved additional MCP or changed exact original three-operation frontier")
		}
		var reserved uint64
		for _, operation := range source.state.operations {
			reserved += operation.Reservation.Operation.Debit.AmountMicrounits
		}
		if source.state.snapshot.Accounting.Aggregate.AmountMicrounits != reserved || !reflect.DeepEqual(source.state.operations[1].Observation, mcpObserved.Observation) {
			t.Fatal("original full MCP debit refunded or durable UNKNOWN observation rewritten")
		}
	})
}

func assertOriginalMCPNextRequestPair(t *testing.T, body []byte, run, call string) {
	t.Helper()
	var wire struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal("decode genuine frozen next MODEL request", err)
	}
	var history struct {
		Units []struct {
			Role     string `json:"original_role"`
			Run      string `json:"run_id"`
			Sequence uint64 `json:"sequence"`
			Payload  string `json:"original_payload"`
		} `json:"original_units"`
	}
	if len(wire.Messages) != 2 || wire.Messages[1].Role != "user" {
		t.Fatal("genuine ordinary request lost original bounded history envelope")
	}
	if err := json.Unmarshal([]byte(wire.Messages[1].Content), &history); err != nil {
		t.Fatal("decode genuine original history units", err)
	}
	counts := [2]int{}
	positions := [2]int{-1, -1}
	for index, frame := range history.Units {
		if frame.Run != run || frame.Sequence > 2 || frame.Sequence == 0 {
			continue
		}
		var payload struct {
			CallID    string            `json:"call_id"`
			Name      string            `json:"name"`
			Arguments map[string]string `json:"arguments"`
			IsError   bool              `json:"is_error"`
			Content   []string          `json:"content"`
		}
		if err := json.Unmarshal([]byte(frame.Payload), &payload); err != nil || payload.CallID != call {
			t.Fatal("genuine history pair call identity", err, frame.Sequence)
		}
		if frame.Sequence == 1 {
			if frame.Role != "tool_call" || payload.Name != "search_openai_docs" || payload.Arguments["query"] != "public docs" {
				t.Fatal("genuine MODEL call arguments omitted/replaced")
			}
		} else if frame.Role != "tool_result" || payload.IsError || !reflect.DeepEqual(payload.Content, []string{"bounded original result"}) {
			t.Fatal("genuine durable MCP result omitted/replaced")
		}
		counts[frame.Sequence-1]++
		positions[frame.Sequence-1] = index
	}
	if counts != [2]int{1, 1} || positions[1] != positions[0]+1 {
		t.Fatalf("genuine original pair duplicated/separated/missing: counts=%v positions=%v", counts, positions)
	}
}
