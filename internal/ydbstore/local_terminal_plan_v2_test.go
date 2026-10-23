package ydbstore

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// MODEL1/MCP use the original prepared Store fixture. MODEL3 below is closed
// codec DATA only, deliberately not a reserved/executed original operation or
// a proof of MODEL2 terminal feasibility. No socket/provider call is made.
func terminalPlanV2Fixture(t *testing.T) (*compactionSourceFixture, domain.Run, []LocalCompactionOperationV1) {
	t.Helper()
	f, m := completedOriginalMCPFixture(t)
	model := m.source.state.operations[0]
	model.Request, model.Response = bytes.Clone(model.Request), bytes.Clone(model.Response)
	toolBody, err := f.store.readOwnedOperationBody(t.Context(), m.operation.Scope, m.observation.Observation.ResponseRef, m.observation.Observation.ResponseDigest, m.observation.Observation.ResponseBytes, m.operation.ResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	tool := LocalCompactionOperationV1{Reservation: m.reservation.Clone(), Observation: m.observation.Observation.Clone(), Request: m.request.Body(), Response: toolBody}
	r := model.Reservation.Clone()
	r.Operation.Sequence, r.Operation.CallID = 3, "codec-model-3"
	r.Operation.PredecessorObservationDigest, _ = tool.Observation.Digest()
	r.OperationDigest, err = r.Operation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r.AggregateDebit, err = tool.Reservation.AggregateDebit.Add(r.Operation.Debit)
	if err != nil {
		t.Fatal(err)
	}
	r.ComponentAmounts = tool.Reservation.ComponentAmounts
	r.ReservedAt = tool.Observation.ObservedAt
	frozen := f.entry.preparedModel.Load().frozen
	wire, err := json.Marshal(map[string]any{"model": f.actual.config.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": "final <answer>"}, "finish_reason": "stop"}}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := frozen.NormalizeResponseBodyV1(wire, r.Operation.Kind, r.Operation.ResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	o := model.Observation.Clone()
	o.OperationDigest, o.ReservationDigest = r.OperationDigest, ""
	o.ReservationDigest, _ = r.Digest()
	o.ResponseRef, o.ResponseBytes, o.ResponseDigest = "codec-final-response", uint64(len(body)), ports.OwnedFixtureBytesDigestV1(body)
	o.ObservedAt = r.ReservedAt
	final := LocalCompactionOperationV1{Reservation: r, Observation: o, Request: bytes.Clone(model.Request), Response: body}
	ops := []LocalCompactionOperationV1{model, tool, final}
	t.Cleanup(func() {
		for _, item := range ops {
			clear(item.Request)
			clear(item.Response)
		}
	})
	return f, m.source.state.snapshot.Job.Run, ops
}

func TestLocalTerminalPlanV2CurrentAndSealedCorrespondence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, run, ops := terminalPlanV2Fixture(t)
		pd, _ := f.actual.config.Digest()
		manifest := canonicaloutput.ManifestIDV1(run.ID)
		limits := domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 4096} // codec DATA cap, not an installed admission
		current, err := buildLocalCanonicalPlanV2(run, limits, pd, &f.actual.config, ops, manifest)
		if err != nil {
			t.Fatal("current typed correspondence", err)
		}
		defer current.Release()
		want := current.Events()
		if len(want) != 3 || want[0].Kind != domain.SessionEventToolCall || want[1].Kind != domain.SessionEventToolResult || want[2].Kind != domain.SessionEventAssistantMessage || want[2].DisplayText != "final <answer>" {
			t.Fatal("canonical pair/answer order changed")
		}
		for i := range ops {
			clear(ops[i].Request)
			ops[i].Request = nil
		}
		sealed, err := buildLocalCanonicalPlanV2(run, limits, pd, nil, ops, manifest)
		if err != nil {
			t.Fatal("request-free sealed correspondence", err)
		}
		defer sealed.Release()
		if !reflect.DeepEqual(want, sealed.Events()) {
			t.Fatal("historical DATA produced different canonical bytes/IDs")
		}
		if invalid, err := buildLocalCanonicalPlanV2(run, limits, pd, &f.actual.config, ops, manifest); err == nil || invalid != nil {
			t.Fatal("erased requests accepted as current original schema proof")
		}
	})
}

func TestLocalTerminalPlanV2DeniesCorruptionAndCapacity(t *testing.T) {
	for _, name := range []string{"unpaired", "wrong_result", "final_tool", "wrong_run", "event_capacity", "byte_capacity", "profile"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, run, ops := terminalPlanV2Fixture(t)
				pd, _ := f.actual.config.Digest()
				limits := domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 4096}
				switch name {
				case "unpaired":
					ops = append(ops[:1], ops[2])
				case "wrong_result":
					ops[1].Response[0] = ' '
				case "final_tool":
					ops[2].Response = bytes.Clone(ops[0].Response)
				case "wrong_run":
					run.ID = "other-run"
				case "event_capacity":
					limits.MaxToolEvents = 1
				case "byte_capacity":
					limits.MaxToolEventBytes = 1
				case "profile":
					pd = "not-a-profile"
				}
				plan, err := buildLocalCanonicalPlanV2(run, limits, pd, &f.actual.config, ops, canonicaloutput.ManifestIDV1(run.ID))
				if err == nil || plan != nil {
					t.Fatal("corrupt/unbounded typed plan accepted")
				}
			})
		})
	}
}
