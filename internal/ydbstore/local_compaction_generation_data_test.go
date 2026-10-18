package ydbstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
)

// These tests exercise the pure generation program with canonical records from
// an actual original-entry source fixture and actual Freeze/Normalize bytes.
// The compiled-policy clone, reservations, parent and plan below are DATA ONLY:
// they are not the fixture Source's installed policy/issuer authorization, an
// original generation factory, retained manifest closure, Store head or runtime.
type generationDataFixture struct {
	f      *compactionSourceFixture
	source *LocalCompactionSourceV1
	policy domain.AgentLoopPolicyV1
}

func newGenerationDataFixture(t *testing.T) *generationDataFixture {
	t.Helper()
	f := newCompactionSourceFixture(t, true, true)
	source, err := f.entry.CaptureCompactionSource(t.Context())
	if err != nil {
		t.Fatalf("actual canonical original source: %v", err)
	}
	t.Cleanup(source.Clear)
	policy := f.entry.parent.Authority.HarnessBinding.AgentLoopPolicy.Clone()
	policy.InstructionVersion, policy.CompactionPolicyDigest = compactioncontext.InstructionVersionV1, compactioncontext.CompactionPolicyDigestV1()
	if err := policy.Validate(); err != nil {
		t.Fatalf("compiled policy DATA: %v", err)
	}
	return &generationDataFixture{f: f, source: source, policy: policy}
}

func dataDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (f *generationDataFixture) operation(t *testing.T, previous []compactioncontext.CompletedOperationV1, frozen *configuredinference.FrozenRequestV1, kind domain.AttemptOperationKindV1, text string) compactioncontext.CompletedOperationV1 {
	t.Helper()
	op := f.f.operation(t, uint64(len(previous))+1)
	policyDigest, err := f.policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	op.Scope.AgentLoopPolicyDigest = policyDigest
	op.Kind, op.ContextBytes, op.SummaryBytes = kind, uint64(len(frozen.Body())), 0
	op.Debit.CompactionCalls, op.Debit.InputTokens = 0, f.policy.Limits.MaxInputTokensPerCall
	var requestWire struct {
		MaxTokens uint64 `json:"max_tokens"`
	}
	if json.Unmarshal(frozen.Body(), &requestWire) != nil || requestWire.MaxTokens == 0 {
		t.Fatal("frozen output limit")
	}
	op.Debit.OutputTokens = requestWire.MaxTokens
	if kind == domain.AttemptOperationCompactionV1 {
		op.SummaryBytes, op.Debit.CompactionCalls = f.policy.Limits.MaxSummaryBytes, 1
	}
	request, err := f.f.entry.bodies.Write(t.Context(), frozen.Body())
	if err != nil {
		t.Fatalf("owned body adapter request DATA: %v", err)
	}
	op.RequestRef, op.RequestDigest, op.RequestBytes = request.Ref, request.Digest, request.Bytes
	op.ResultBytes, op.Debit.ResponseBytes = 64<<10, 64<<10
	op.Debit.BodyBytes = op.RequestBytes + op.ResultBytes
	if kind == domain.AttemptOperationCompactionV1 {
		op.Debit.BodyBytes += operationbody.RetainedPairBytesV1
	}
	op.PredecessorObservationDigest = ""
	if len(previous) > 0 {
		op.PredecessorObservationDigest, err = previous[len(previous)-1].Observation.Digest()
		if err != nil {
			t.Fatal(err)
		}
	}
	opDigest, err := op.Digest()
	if err != nil {
		t.Fatalf("complete operation DATA: %v", err)
	}
	debit := op.Debit
	if len(previous) > 0 {
		debit, err = previous[len(previous)-1].Reservation.AggregateDebit.Add(op.Debit)
		if err != nil {
			t.Fatal(err)
		}
	}
	allocation, err := f.f.entry.allocation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: opDigest, ReservedAt: f.f.parent.graph.at, AggregateDebit: debit, RegisteredAllocationDigest: allocation, RegisteredProxyDigest: domain.RegisteredProxyAttestationDigestV2(strings.Repeat("a", 64))}
	if r.Validate() != nil {
		t.Fatal("structurally valid reservation DATA required")
	}
	rd, err := r.Digest()
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"model": f.f.actual.config.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text}, "finish_reason": "stop"}}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := frozen.NormalizeResponseBodyV1(wire, kind, op.ResultBytes)
	if err != nil {
		t.Fatalf("actual normalized %s response: %v", kind, err)
	}
	ref, err := f.f.entry.bodies.Write(t.Context(), response)
	if err != nil {
		t.Fatalf("owned body adapter response DATA: %v", err)
	}
	decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(f.f.actual.config, frozen.Body(), response, kind)
	if err != nil || decoded.Call != nil || decoded.Stop != configuredinference.StopFinalV1 {
		t.Fatalf("causal completed tool-free response: %v", err)
	}
	usage, err := decoded.Usage.OperationUsage()
	if err != nil {
		t.Fatal(err)
	}
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: opDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: ref.Ref, ResponseDigest: ref.Digest, ResponseBytes: ref.Bytes, ObservedAt: r.ReservedAt, Usage: usage}
	if err := o.ValidateForReservation(r); err != nil {
		t.Fatalf("causal completed observation DATA: %v", err)
	}
	return compactioncontext.CompletedOperationV1{Reservation: r, Observation: o, Request: frozen.Body(), Response: response}
}

func (f *generationDataFixture) build(ops []compactioncontext.CompletedOperationV1, parent *compactioncontext.ParentV1, tools []configuredinference.ToolV1) (*compactioncontext.GenerationV1, error) {
	return compactioncontext.BuildGenerationV1(f.f.actual.config, f.f.actual.config.Limits, f.policy, f.source.Digest(), f.source.Records(), ops, parent, tools)
}

func summaryData(t *testing.T, g *compactioncontext.GenerationV1) []byte {
	t.Helper()
	refs, coverage := g.References(), g.CoverageIDs()
	if len(refs) == 0 || len(coverage) == 0 {
		t.Fatal("summary requires actual reference inventory")
	}
	raw := []byte(fmt.Sprintf(`{"schema":"sessionless.compaction-summary.v1","task":[{"text":"Retain original constraints","source_ids":[%q]}],"constraints":[],"facts":[{"text":"Historical tool pairs remain correlated","source_ids":[%q]}],"unresolved":[],"artifacts":[],"omissions":[]}`, refs[0].SourceID, coverage[0]))
	if _, err := g.ValidateSummary(raw); err != nil {
		t.Fatalf("strict summary DATA: %v", err)
	}
	return raw
}

func generationDataHistory(t *testing.T, request *configuredinference.FrozenRequestV1) map[string]json.RawMessage {
	t.Helper()
	var wire struct {
		Messages []struct{ Role, Content string }
		Tools    []json.RawMessage
	}
	if err := json.Unmarshal(request.Body(), &wire); err != nil || len(wire.Messages) != 2 || wire.Messages[0].Role != "system" || wire.Messages[1].Role != "user" {
		t.Fatalf("closed model wire: %v", err)
	}
	var history map[string]json.RawMessage
	if err := json.Unmarshal([]byte(wire.Messages[1].Content), &history); err != nil {
		t.Fatal(err)
	}
	return history
}

func TestLocalCompactionGenerationDataCompletedModelAndSecondParent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newGenerationDataFixture(t)
		firstModel := f.operation(t, nil, f.f.frozen, domain.AttemptOperationModelV1, "first completed model result")
		ops := []compactioncontext.CompletedOperationV1{firstModel}
		tools := []configuredinference.ToolV1{{Name: "lookup", Parameters: []byte(`{"type":"object","properties":{},"required":[],"additionalProperties":false}`)}}
		first, err := f.build(ops, nil, tools)
		if err != nil {
			t.Fatalf("original canonical tool-heavy plus normalized model DATA: %v", err)
		}
		if first.Number() != 1 || len(first.CoverageIDs()) != 5 {
			t.Fatalf("first original tool span coverage=%v", first.CoverageIDs())
		}
		summary := summaryData(t, first)
		compaction := f.operation(t, ops, first.FrozenRequest(), domain.AttemptOperationCompactionV1, string(summary))
		ops = append(ops, compaction)
		rd, _ := compaction.Reservation.Digest()
		od, _ := compaction.Observation.Digest()
		plan := domain.DerivedContextPlanV1{Version: 1, Scope: compaction.Reservation.Operation.Scope, Generation: 1, OperationSequence: compaction.Reservation.Operation.Sequence, ReservationDigest: rd, ObservationDigest: od, NormalizedResponseDigest: compaction.Observation.ResponseDigest, SourceViewDigest: first.SourceDigest()}
		manifest := []byte(`{"fixture":"typed plan DATA only, not retained closure"}`)
		for i, body := range [][]byte{summary, manifest} {
			role := "summary"
			if i == 1 {
				role = "manifest"
			}
			ref := domain.BlobRef{TenantID: plan.Scope.TenantID, Size: int64(len(body)), SHA256: dataDigest(body)}
			ref.Key, err = domain.DerivedContextObjectKeyV1(plan.Scope, plan.Generation, role, ref.SHA256)
			if err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				plan.Summary = ref
			} else {
				plan.Manifest = ref
			}
		}
		if err := plan.Validate(); err != nil {
			t.Fatalf("exact typed parent plan DATA: %v", err)
		}
		parent := &compactioncontext.ParentV1{Plan: plan, Summary: bytes.Clone(summary), CoveredSourceIDs: first.CoverageIDs(), CompatibilityDigest: first.CompatibilityDigest()}
		for _, text := range []string{"model after first summary", "exact final original suffix"} {
			ops = append(ops, f.operation(t, ops, f.f.frozen, domain.AttemptOperationModelV1, text))
		}
		second, err := f.build(ops, parent, tools)
		if err != nil {
			t.Fatalf("exact parent and additional normalized models: %v", err)
		}
		for _, name := range []string{"different_summary", "different_observation", "duplicate_coverage", "foreign_coverage"} {
			bad := *parent
			bad.Summary = bytes.Clone(parent.Summary)
			bad.CoveredSourceIDs = append([]string(nil), parent.CoveredSourceIDs...)
			switch name {
			case "different_summary":
				bad.Summary = []byte(strings.Replace(string(summary), "Retain original constraints", "Different derived statement", 1))
			case "different_observation":
				bad.Plan.ObservationDigest = domain.AttemptOperationObservationDigestV1(strings.Repeat("b", 64))
			case "duplicate_coverage":
				bad.CoveredSourceIDs = append(bad.CoveredSourceIDs, bad.CoveredSourceIDs[0])
			case "foreign_coverage":
				bad.CoveredSourceIDs[0] = strings.Repeat("f", 64)
			}
			if bad.Plan.Validate() != nil {
				t.Fatalf("%s must retain a structurally valid parent plan", name)
			}
			if name == "different_summary" {
				if _, err := first.ValidateSummary(bad.Summary); err != nil {
					t.Fatalf("causal altered parent summary must retain strict schema/citations: %v", err)
				}
			}
			if got, err := f.build(ops, &bad, tools); !errors.Is(err, compactioncontext.ErrCanonical) || got != nil {
				t.Fatalf("%s parent DATA accepted: %v", name, err)
			}
		}
		if second.Number() != 2 || len(second.CoverageIDs()) != len(first.CoverageIDs())+2 {
			t.Fatalf("second flattened coverage number=%d ids=%v", second.Number(), second.CoverageIDs())
		}
		counts := make(map[string]int)
		for _, id := range second.CoverageIDs() {
			counts[id]++
		}
		for _, id := range first.CoverageIDs() {
			if counts[id] != 1 {
				t.Fatalf("old coverage %s repeated/lost: %d", id, counts[id])
			}
		}
		for id, count := range counts {
			if count != 1 {
				t.Fatalf("duplicate flattened source %s: %d", id, count)
			}
		}
		history := generationDataHistory(t, second.FrozenRequest())
		var parentSummary string
		if json.Unmarshal(history["derived_summary"], &parentSummary) != nil || parentSummary != string(summary) {
			t.Fatal("parent summary not kept in its separate untrusted DATA field")
		}
		var frames []struct {
			Kind, SourceID string
			Payload        string `json:"original_payload"`
		}
		if err := json.Unmarshal(history["original_units"], &frames); err != nil {
			t.Fatal(err)
		}
		if len(frames) != 2 || frames[0].Payload != "first completed model result" || frames[1].Payload != "model after first summary" {
			t.Fatalf("parent compaction replayed as ordinary message or original model lost: %+v", frames)
		}
		for _, frame := range frames {
			if frame.Payload == string(summary) {
				t.Fatal("parent compaction duplicated as ordinary original")
			}
		}
		next, err := second.Rebuild(summaryData(t, second))
		if err != nil {
			t.Fatal(err)
		}
		var nextWire struct{ Tools []json.RawMessage }
		if json.Unmarshal(next.Body(), &nextWire) != nil || len(nextWire.Tools) != 1 {
			t.Fatal("actual next frozen request lost original native catalog DATA")
		}
		var suffix []struct {
			Payload string `json:"original_payload"`
		}
		if json.Unmarshal(generationDataHistory(t, next)["original_units"], &suffix) != nil || len(suffix) != 1 || suffix[0].Payload != "exact final original suffix" {
			t.Fatalf("exact original suffix lost/duplicated: %+v", suffix)
		}
	})
}

func TestLocalCompactionGenerationDataOperationDenials(t *testing.T) {
	for _, name := range []string{"malformed_response", "pending_call", "unknown_effect", "policy_digest"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newGenerationDataFixture(t)
				model := f.operation(t, nil, f.f.frozen, domain.AttemptOperationModelV1, "known completed model result")
				if _, err := f.build([]compactioncontext.CompletedOperationV1{model}, nil, nil); err != nil {
					t.Fatalf("causal positive baseline: %v", err)
				}
				switch name {
				case "malformed_response":
					model.Response = []byte(`{"not":"normalized model body"}`)
				case "unknown_effect":
					model.Observation.Outcome, model.Observation.Reason = domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonTransportUnknownV1
					model.Observation.ResponseRef, model.Observation.ResponseDigest, model.Observation.ResponseBytes = "", "", 0
					model.Response = nil
				case "policy_digest":
					model.Reservation.Operation.Scope.AgentLoopPolicyDigest = domain.AgentLoopPolicyDigestV1(strings.Repeat("b", 64))
					model.Reservation.OperationDigest, _ = model.Reservation.Operation.Digest()
					model.Observation.OperationDigest = model.Reservation.OperationDigest
					model.Observation.ReservationDigest, _ = model.Reservation.Digest()
				case "pending_call":
					frozen, err := configuredinference.FreezeRequestV1(f.f.actual.config, f.f.actual.config.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "synthetic pending work"}}, Tools: []configuredinference.ToolV1{{Name: "lookup", Parameters: []byte(`{"type":"object","properties":{},"required":[],"additionalProperties":false}`)}}, MaxOutputTokens: 1})
					if err != nil {
						t.Fatal(err)
					}
					model = f.operation(t, nil, frozen, domain.AttemptOperationModelV1, "temporary baseline")
					wire := []byte(fmt.Sprintf(`{"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"pending","type":"function","function":{"name":"lookup","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`, f.f.actual.config.ModelID))
					model.Response, err = frozen.NormalizeResponseBodyV1(wire, domain.AttemptOperationModelV1, model.Reservation.Operation.ResultBytes)
					if err != nil {
						t.Fatalf("actual wellformed pending normalized call: %v", err)
					}
					decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(f.f.actual.config, model.Request, model.Response, domain.AttemptOperationModelV1)
					if err != nil || decoded.Call == nil {
						t.Fatal("pending fixture needs a genuine normalized declared call")
					}
					clear(decoded.Call.Arguments)
				}
				if name == "malformed_response" || name == "pending_call" {
					ref, err := f.f.entry.bodies.Write(t.Context(), model.Response)
					if err != nil {
						t.Fatalf("causal mutated response body storage: %v", err)
					}
					model.Observation.ResponseRef, model.Observation.ResponseBytes, model.Observation.ResponseDigest = ref.Ref, ref.Bytes, ref.Digest
				}
				if model.Reservation.Validate() != nil || model.Observation.ValidateForReservation(model.Reservation) != nil {
					t.Fatal("negative must retain structurally valid reservation/observation, not fail unrelated integrity checks")
				}
				if got, err := f.build([]compactioncontext.CompletedOperationV1{model}, nil, nil); !errors.Is(err, compactioncontext.ErrCanonical) || got != nil {
					t.Fatalf("%s generation accepted: err=%v", name, err)
				}
			})
		})
	}
}
