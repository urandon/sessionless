package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
	"strings"
	"testing"
	"testing/synctest"
)

// Genuine original MODEL/MCP fixture, with a synthetic final normalized codec
// record. All future maxima and resulting counters are LENGTH DATA, never a
// reservation, observation, actual invoice, physical publication or cloud E2E.
func terminalFitV2ActualCapsule(t *testing.T, f *compactionSourceFixture, run domain.Run, ops []LocalCompactionOperationV1) []byte {
	t.Helper()
	pd, _ := f.actual.config.Digest()
	manifest := domain.ArtifactManifest{ID: canonicaloutput.ManifestIDV1(run.ID), TenantID: run.TenantID, RunID: run.ID, CreatedAt: f.parent.graph.at}
	encoded, err := buildLocalCanonicalPlanV2(run, f.entry.preparedMCP.Load().source.state.snapshot.Job.Job.Limits, pd, &f.actual.config, ops, manifest.ID)
	if err != nil {
		t.Fatal("actual canonical codec DATA", err)
	}
	defer encoded.Release()
	var events []localCapsuleEventV1
	for _, event := range encoded.Events() {
		draft, err := canonicaloutput.PlanEventDraftV1(run, event.ID, event.Kind, event.Payload, run.CreatedAt)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: event.DisplayText})
	}
	last := ops[len(ops)-1].Reservation
	disposition := ports.OwnedOperationAccountingDispositionV3{Version: 3, Scope: last.Operation.Scope, AccountingDigest: strings.Repeat("a", 64), AggregateDebit: last.AggregateDebit, ComponentAmounts: last.ComponentAmounts,
		ProviderInvoice: ports.OwnedLocalInvoiceIdentityV2{ProfileDigest: f.entry.source.Profile.Digest, TermsDigest: f.entry.source.Spending[0].TermsEvidenceDigest, Revision: f.entry.source.Spending[0].PriceRevision, Currency: f.entry.source.Currency, IssuedAt: f.entry.source.IssuedAt, ExpiresAt: f.entry.source.ExpiresAt}}
	var materials []localCapsuleOperationMaterial
	for _, item := range ops {
		row, err := ports.BuildOwnedOperationAccountingRowV3(item.Reservation, item.Observation, f.entry.parent, f.entry.source)
		if err != nil {
			t.Fatal("auth-correlation DATA for actual V3 shape", err)
		}
		disposition.Operations = append(disposition.Operations, row)
		materials = append(materials, localCapsuleOperationMaterial{reservation: item.Reservation, observation: item.Observation, normalized: item.Response, bytes: uint64(len(item.Response))})
	}
	emit := localCapsuleSink{limit: domain.MaxTerminalCapsuleBytesV1, emit: true}
	if err := encodeLocalTerminalCapsuleV2To(&emit, last.Operation.Scope, pd, strings.Repeat("a", 64), materials, run.DerivedContext, events, 0, manifest, disposition); err != nil {
		t.Fatal("actual V2 layout", err)
	}
	count := localCapsuleSink{limit: domain.MaxTerminalCapsuleBytesV1}
	if err := encodeLocalTerminalCapsuleV2To(&count, last.Operation.Scope, pd, strings.Repeat("a", 64), materials, run.DerivedContext, events, 0, manifest, disposition); err != nil || count.size != uint64(len(emit.body)) || count.body != nil {
		t.Fatalf("same V2 serializer emitter/count mismatch: count=%d bytes=%d err=%v", count.size, len(emit.body), err)
	}
	return emit.body
}

func TestLocalTerminalFitV2SameSerializerDominatesActualAndExactBoundary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, run, ops := terminalPlanV2Fixture(t)
		actual := terminalFitV2ActualCapsule(t, f, run, ops)
		defer clear(actual)
		pd, _ := f.actual.config.Digest()
		product := f.entry.preparedMCP.Load().source.state.snapshot.Job.Job.Limits
		before := f.canonical.calls
		for _, i := range []int{1, 2} {
			n, err := localTerminalProspectiveBytesV2(run, product, pd, f.actual.config, f.entry.source, ops[:i], ops[i].Reservation.Operation, f.actual.config.Limits.ResponseBytes, domain.MaxTerminalCapsuleBytesV1)
			if err != nil || n < uint64(len(actual)) {
				t.Fatalf("complete V2 prospective upper: op=%d upper=%d actual=%d err=%v", i+1, n, len(actual), err)
			}
			if n > f.entry.terminalOutputs.CapsuleLimit() {
				t.Fatalf("original precharged capsule cap cannot represent admitted fixture: upper=%d cap=%d", n, f.entry.terminalOutputs.CapsuleLimit())
			}
			exact, err := localTerminalProspectiveBytesV2(run, product, pd, f.actual.config, f.entry.source, ops[:i], ops[i].Reservation.Operation, f.actual.config.Limits.ResponseBytes, n)
			if err != nil || exact != n {
				t.Fatalf("exact prospective boundary: op=%d got=%d want=%d err=%v", i+1, exact, n, err)
			}
			if _, err := localTerminalProspectiveBytesV2(run, product, pd, f.actual.config, f.entry.source, ops[:i], ops[i].Reservation.Operation, f.actual.config.Limits.ResponseBytes, n-1); !errors.Is(err, ErrTerminalCapacity) {
				t.Fatalf("first-over prospective cap: op=%d err=%v", i+1, err)
			}
		}
		if f.canonical.calls != before {
			t.Error("pure prospective count performed body I/O")
		}
		if !bytes.Contains(actual, []byte(`"accounting_disposition":{"version":3`)) || !bytes.Contains(actual, []byte(`"disposition":"unknown_full_reserved"`)) {
			t.Error("actual capsule did not retain complete truthful V3 accounting")
		}
	})
}

func TestLocalTerminalFitV2OriginalCanonicalToolBudgetBeforeSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, run, ops := terminalPlanV2Fixture(t)
		pd, _ := f.actual.config.Digest()
		product := f.entry.preparedMCP.Load().source.state.snapshot.Job.Job.Limits
		pending := ops[1].Reservation.Operation
		tools, err := localTerminalCanonicalPairsUpperV2(f.actual.config, ops[:1], pending)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			for _, e := range tools {
				clear(e.Payload)
			}
		}()
		encoded, err := canonicaloutput.EncodeCompletionV1(run.ID, product, ports.ExecutionResult{Summary: "x", ToolEvents: tools}, canonicaloutput.ManifestIDV1(run.ID))
		if err != nil {
			t.Fatal(err)
		}
		defer encoded.Release()
		var exact uint64
		for i, e := range encoded.Events() {
			if i < len(tools) {
				exact += uint64(len(e.Payload))
			}
		}
		extra, err := localTerminalFutureResultExtraV2(pending, tools[0])
		if err != nil {
			t.Fatal(err)
		}
		exact += extra
		for _, mode := range []string{"exact", "one_byte_short", "one_event_short", "disabled"} {
			limits := product
			limits.MaxToolEvents, limits.MaxToolEventBytes = 2, exact
			switch mode {
			case "one_byte_short":
				limits.MaxToolEventBytes--
			case "one_event_short":
				limits.MaxToolEvents = 1
			case "disabled":
				limits.MaxToolEvents = 0
				limits.MaxToolEventBytes = 0
				limits.MaxTurns = 0
			}
			_, err := localTerminalProspectiveBytesV2(run, limits, pd, f.actual.config, f.entry.source, ops[:1], pending, f.actual.config.Limits.ResponseBytes, domain.MaxTerminalCapsuleBytesV1)
			if (err == nil) != (mode == "exact") {
				t.Errorf("canonical tool count/byte admission: mode=%s bytes=%d err=%v", mode, exact, err)
			}
		}
	})
}

func TestLocalTerminalFitV2RejectsDriftingSourceAndPair(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, originalRun, original := terminalPlanV2Fixture(t)
		product := f.entry.preparedMCP.Load().source.state.snapshot.Job.Job.Limits
		for _, mode := range []string{"profile", "scope", "source", "model_body", "result_body", "result_usage", "unpaired", "future_args", "future_call", "future_scope"} {
			pd, _ := f.actual.config.Digest()
			run := originalRun.Clone()
			ops := make([]LocalCompactionOperationV1, len(original))
			for i, item := range original {
				ops[i] = LocalCompactionOperationV1{Reservation: item.Reservation.Clone(), Observation: item.Observation.Clone(), Request: bytes.Clone(item.Request), Response: bytes.Clone(item.Response)}
			}
			func() {
				defer func() {
					for _, item := range ops {
						clear(item.Request)
						clear(item.Response)
					}
				}()
				source := f.entry.source.Clone()
				future := ops[2].Reservation.Operation.Clone()
				prefix := ops[:2]
				switch mode {
				case "profile":
					pd = strings.Repeat("b", 64)
				case "scope":
					run.ID = "different-run"
				case "source":
					source.Scope.OwnerUserID = "different-owner"
				case "model_body":
					prefix[0].Response[0] ^= 1
				case "result_body":
					prefix[1].Response[0] ^= 1
				case "result_usage":
					prefix[1].Observation.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: new(uint64(1))}
				case "unpaired":
					prefix = prefix[1:]
				case "future_args":
					prefix = ops[:1]
					future = ops[1].Reservation.Operation.Clone()
					future.ArgumentsDigest = strings.Repeat("b", 64)
				case "future_call":
					prefix = ops[:1]
					future = ops[1].Reservation.Operation.Clone()
					future.CallID = "other-call"
				case "future_scope":
					future.Scope.FenceToken++
				}
				before := f.canonical.calls
				if _, err := localTerminalProspectiveBytesV2(run, product, pd, f.actual.config, source, prefix, future, f.actual.config.Limits.ResponseBytes, domain.MaxTerminalCapsuleBytesV1); err == nil {
					t.Fatalf("drifting future/pair admitted: %s", mode)
				}
				if before != f.canonical.calls {
					t.Error("denied prospective DATA read/wrote body storage")
				}
			}()
		}
	})
}

func TestLocalTerminalFitV2OriginalCompletedPairRequiresPrivateWitness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, m := completedOriginalMCPFixture(t)
		primeOriginalMCPConservative(t, f)
		source, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer source.Clear()
		previous := source.state.operations[len(source.state.operations)-1]
		future := source.state.operations[0].Reservation.Operation.Clone()
		future.Sequence = 3
		future.CallID = "future-model"
		future.PredecessorObservationDigest, _ = previous.Observation.Digest()
		ctx, finish := ownedAuthorityCallContext(t.Context())
		defer finish()
		if err := f.entry.terminalFitSourceLocked(ctx, nil, source, future, nil); err != nil {
			t.Fatal("original paired current fit", err)
		}
		w := m.conservative.Load()
		m.conservative.Store(nil)
		if err := f.entry.terminalFitSourceLocked(ctx, nil, source, future, nil); err == nil {
			t.Error("unknown monetary MCP admitted after private witness removed")
		}
		m.conservative.Store(w)
		if ownedObservationAllowsSuccessor(m.reservation, m.observation.Observation) {
			t.Error("generic measured-money gate weakened")
		}
	})
}

func TestLocalTerminalFitV2SuccessorBudgetOriginalCeilings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, m := completedOriginalMCPFixture(t)
		s := m.source
		original := s.state.snapshot.Accounting.Aggregate
		limits := f.entry.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits
		for _, mode := range []string{"positive", "operations", "models", "input", "output", "bodies", "duration", "cancelled"} {
			func() {
				s.state.snapshot.Accounting.Aggregate = original
				defer func() { s.state.snapshot.Accounting.Aggregate = original }()
				switch mode {
				case "operations":
					s.state.snapshot.Accounting.Aggregate.Operations = limits.MaxOperations - 1
				case "models":
					s.state.snapshot.Accounting.Aggregate.ModelCalls = limits.MaxModelCalls
				case "input":
					s.state.snapshot.Accounting.Aggregate.InputTokens = limits.MaxAggregateInputTokens
				case "output":
					s.state.snapshot.Accounting.Aggregate.OutputTokens = limits.MaxAggregateOutputTokens
				case "bodies":
					s.state.snapshot.Accounting.Aggregate.BodyBytes = f.entry.localOperationBodyLimit() - m.operation.Debit.BodyBytes
				case "duration":
					s.state.snapshot.Accounting.Aggregate.ActiveDuration = limits.MaxActiveDuration - m.operation.Debit.ActiveDuration
				}
				ctx := t.Context()
				if mode == "cancelled" {
					c, cancel := context.WithCancel(ctx)
					cancel()
					ctx = c
				}
				err := f.entry.terminalMCPSuccessorBudgetV2(ctx, s, m.operation)
				s.state.snapshot.Accounting.Aggregate = original
				if (err == nil) != (mode == "positive") {
					t.Errorf("same original successor capacity: mode=%s err=%v", mode, err)
				}
			}()
		}
	})
}

func TestLocalTerminalFitV2FutureResultEscapingBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, run, ops := terminalPlanV2Fixture(t)
		future := ops[1].Reservation.Operation
		tools, err := localTerminalCanonicalPairsUpperV2(f.actual.config, ops[:1], future)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			for _, e := range tools {
				clear(e.Payload)
			}
		}()
		product := f.entry.preparedMCP.Load().source.state.snapshot.Job.Job.Limits
		minimal, err := canonicaloutput.EncodeCompletionV1(run.ID, product, ports.ExecutionResult{Summary: "x", ToolEvents: tools}, canonicaloutput.ManifestIDV1(run.ID))
		if err != nil {
			t.Fatal(err)
		}
		defer minimal.Release()
		extra, err := localTerminalFutureResultExtraV2(future, tools[0])
		if err != nil {
			t.Fatal(err)
		}
		upper := uint64(len(minimal.Events()[1].Payload)) + extra
		for i, text := range []string{"", `"\\` + "\n<>&\u2028🙂", strings.Repeat("<", 128), strings.Repeat("<", 1024)} {
			m := f.entry.preparedMCP.Load()
			wire, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": future.CallID, "result": map[string]any{"content": []any{map[string]any{"type": "text", "text": text}}, "isError": false}})
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := m.request.DecodeToolResultV1(wire)
			clear(wire)
			if err != nil {
				t.Fatalf("bounded actual text-only result DATA: case=%d err=%v", i, err)
			}
			normalized, err := mcpadmission.EncodeToolResultV1(m.call, future, parsed, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1})
			clear(normalized)
			if i == 3 {
				if err == nil {
					t.Error("oversize escaped normalized result unexpectedly admitted")
				}
				continue
			}
			if err != nil {
				t.Fatalf("admitted normalized result DATA: case=%d err=%v", i, err)
			}
			content, err := json.Marshal(struct {
				ID      string   `json:"call_id"`
				IsError bool     `json:"is_error"`
				Content []string `json:"content"`
			}{future.CallID, false, []string{text}})
			if err != nil {
				t.Fatal(err)
			}
			events := append([]ports.ExecutionToolEvent(nil), tools...)
			events[1].Payload = content
			actual, err := canonicaloutput.EncodeCompletionV1(run.ID, product, ports.ExecutionResult{Summary: "x", ToolEvents: events}, canonicaloutput.ManifestIDV1(run.ID))
			if err != nil {
				t.Fatal(err)
			}
			if uint64(len(actual.Events()[1].Payload)) > upper {
				t.Errorf("future canonical escaping exceeds exact grammar bound: got=%d upper=%d", len(actual.Events()[1].Payload), upper)
			}
			actual.Release()
			clear(content)
		}
	})
}
