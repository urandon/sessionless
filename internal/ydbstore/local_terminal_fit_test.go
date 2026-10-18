package ydbstore

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
)

// Codec/arithmetic DATA tests. Genuine producer fixtures authenticate their
// inputs, but neither the counter nor any hypothetical future row is evidence.
func TestLocalTerminalFitSameEncoderCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _, h := terminalOutputFixture(t)
		p, err := h.PrepareOutput(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Clear)
		pd, err := f.actual.config.Digest()
		if err != nil {
			t.Fatal(err)
		}
		ops := h.source.state.operations
		materials := make([]localCapsuleOperationMaterial, len(ops))
		for i, o := range ops {
			materials[i] = localCapsuleOperationMaterial{reservation: o.Reservation, observation: o.Observation, normalized: o.Response, bytes: uint64(len(o.Response))}
		}
		d := p.drafts[0]
		events := []localCapsuleEventV1{{ID: d.ID, Kind: d.Kind, IdempotencyKey: d.IdempotencyKey, Payload: d.Payload, DisplayText: d.DisplayText}}
		for _, text := range []string{d.DisplayText, "\"\\\n<>&\u2028🙂", strings.Repeat("<", 1024)} {
			events[0].DisplayText = text
			actual, err := encodeLocalTerminalCapsule(p.plan.Scope, pd, h.source.state.digest, ops, nil, events, p.manifest, domain.MaxTerminalCapsuleBytesV1)
			if err != nil {
				t.Fatal(err)
			}
			sink := localCapsuleSink{limit: domain.MaxTerminalCapsuleBytesV1}
			if err := encodeLocalTerminalCapsuleTo(&sink, p.plan.Scope, pd, h.source.state.digest, materials, nil, events, 0, p.manifest); err != nil {
				t.Fatal(err)
			}
			if sink.size != uint64(len(actual)) || sink.body != nil {
				t.Fatalf("count/emitter mismatch: count=%d encoded=%d allocated=%t", sink.size, len(actual), sink.body != nil)
			}
			clear(actual)
		}
	})
}

func TestLocalTerminalFitOriginalCompactionLifetime(t *testing.T) {
	for _, duration := range []time.Duration{5 * time.Second, 20 * time.Second} {
		name := "original_deadline_has_successor_interval"
		if duration == 5*time.Second {
			name = "current_operation_exhausts_original_deadline"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithDeadline(t.Context(), time.Now().Add(duration))
				t.Cleanup(cancel)
				f, source := terminalFitGenerationSource(t, localRuntimeActualPrebuild{sourceParent: parent, networkOperation: 5 * time.Second}, false)
				before := f.canonical.calls
				g, err := prepareLocalCompactionGeneration(t.Context(), source)
				if duration == 5*time.Second {
					if g != nil || !errors.Is(err, ErrTerminalCapacity) || f.canonical.calls != before {
						t.Fatalf("original Source deadline renewed: handle=%t err=%v I/O=%d", g != nil, err, f.canonical.calls-before)
					}
				} else if err != nil || g == nil {
					t.Fatal("original future interval positive", err)
				}
			})
		})
	}
}

func TestLocalTerminalFitOriginalCompactionBodyReserve(t *testing.T) {
	for _, over := range []bool{false, true} {
		name := "exact_pending_and_successor_body"
		if over {
			name = "pending_fits_successor_first_over"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				baseline, source := terminalFitGenerationSource(t, localRuntimeActualPrebuild{networkOperation: 5 * time.Second}, false)
				g, err := prepareLocalCompactionGeneration(t.Context(), source)
				if err != nil {
					t.Fatal("baseline original generation", err)
				}
				limits, cfg := baseline.entry.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits, baseline.actual.config
				next, err := source.state.snapshot.Accounting.Aggregate.Add(g.operation.Debit)
				if err != nil {
					t.Fatal(err)
				}
				input := min(cfg.Limits.MaxInputTokens, limits.MaxInputTokensPerCall, limits.MaxAggregateInputTokens-next.InputTokens)
				wire, err := g.program.SuccessorRequestUpperBytes(g.operation.SummaryBytes, min(input, cfg.ModelWindowTokens-g.program.SuccessorOutputReserve())-1)
				if err != nil {
					t.Fatal(err)
				}
				bodyLimit := operationbody.LocalTerminalCapsuleBytesV1 + next.BodyBytes + wire + min(uint64(64<<10), cfg.Limits.ResponseBytes)
				if over {
					bodyLimit--
				}
				f, source := terminalFitGenerationSource(t, localRuntimeActualPrebuild{networkOperation: 5 * time.Second, bodyBytes: bodyLimit}, false)
				before := f.canonical.calls
				prepared, err := prepareLocalCompactionGeneration(t.Context(), source)
				if over {
					if prepared != nil || !errors.Is(err, ErrTerminalCapacity) || f.canonical.calls != before {
						t.Fatalf("successor body first-over performed preparation: handle=%t err=%v I/O=%d", prepared != nil, err, f.canonical.calls-before)
					}
				} else if err != nil || prepared == nil {
					t.Fatal("exact pending+successor body reserve", err)
				}
			})
		})
	}
}

func terminalFitGenerationSource(t *testing.T, prebuild localRuntimeActualPrebuild, prefix bool) (*compactionSourceFixture, *LocalCompactionSourceV1) {
	t.Helper()
	prebuild.compiledCompaction = true
	f := newCompactionSourceFixtureWithPrebuild(t, true, prebuild, true)
	f.store.ownedProviderPriceCustody = localInvoiceRejectStandardCustody{}
	if prefix {
		op := f.operation(t, 1)
		op.Kind, op.SummaryBytes, op.Debit.CompactionCalls = domain.AttemptOperationModelV1, 0, 0
		f.seedCompleted(t, op, 1)
	}
	source, err := f.entry.CaptureCompactionSource(t.Context())
	if err != nil {
		t.Fatal("original source", err)
	}
	t.Cleanup(source.Clear)
	return f, source
}

func TestLocalTerminalFitOriginalCompactionSuccessorModelCalls(t *testing.T) {
	for _, count := range []uint64{2, 3} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, source := terminalFitGenerationSource(t, localRuntimeActualPrebuild{modelCalls: count}, true)
				before := f.canonical.calls
				g, err := prepareLocalCompactionGeneration(t.Context(), source)
				if count == 2 {
					if g != nil || !errors.Is(err, ErrTerminalCapacity) || f.canonical.calls != before || f.entry.compactionGeneration.Load() != nil {
						t.Fatalf("current compaction consumes last MODEL slot: handle=%t err=%v I/O=%d", g != nil, err, f.canonical.calls-before)
					}
				} else if err != nil || g == nil {
					t.Fatal("original compaction plus successor positive", err)
				}
			})
		})
	}
}

func TestLocalTerminalFitOriginalCompactionArtifactBeforeBody(t *testing.T) {
	for _, over := range []bool{false, true} {
		name := "exact_pair_and_capsule"
		if over {
			name = "first_over_before_body"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				limit := operationbody.LocalTerminalCapsuleBytesV1 + operationbody.RetainedPairBytesV1
				if over {
					limit--
				}
				f, source := terminalFitGenerationSource(t, localRuntimeActualPrebuild{artifactBytes: limit}, false)
				before := f.canonical.calls
				g, err := prepareLocalCompactionGeneration(t.Context(), source)
				if over {
					if g != nil || !errors.Is(err, ErrTerminalCapacity) || f.canonical.calls != before || f.entry.compactionGeneration.Load() != nil {
						t.Fatalf("pair capacity failed after body effects: handle=%t err=%v I/O=%d", g != nil, err, f.canonical.calls-before)
					}
				} else if err != nil || g == nil {
					t.Fatal("original exact capsule+pair positive", err)
				}
			})
		})
	}
}

func TestLocalTerminalFitOriginalCompactionOutputReserve(t *testing.T) {
	for _, tokens := range []uint64{2049, 4096} {
		name := "exact_two_calls_positive"
		if tokens == 2049 {
			name = "one_remaining_token_cannot_shrink_fixed_successor"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, source := terminalFitGenerationSource(t, localRuntimeActualPrebuild{aggregateOutputTokens: tokens}, false)
				before := f.canonical.calls
				g, err := prepareLocalCompactionGeneration(t.Context(), source)
				if tokens == 2049 {
					if g != nil || !errors.Is(err, ErrTerminalCapacity) || f.canonical.calls != before || f.entry.compactionGeneration.Load() != nil {
						t.Fatalf("fixed successor output silently narrowed: handle=%t err=%v I/O=%d", g != nil, err, f.canonical.calls-before)
					}
				} else if err != nil || g == nil || g.program.SuccessorOutputReserve() != 2048 {
					t.Fatal("exact two fixed output reserves", err)
				}
			})
		})
	}
}

func TestLocalTerminalFitOriginalMinimumSummaryOutput(t *testing.T) {
	var minimumOutput uint64
	synctest.Test(t, func(t *testing.T) {
		f, source := terminalFitGenerationSource(t, localRuntimeActualPrebuild{networkOperation: 5 * time.Second}, false)
		g, err := prepareLocalCompactionGeneration(t.Context(), source)
		if err != nil {
			t.Fatal(err)
		}
		minimum, err := g.program.MinimumSuccessorRequest()
		if err != nil {
			t.Fatal(err)
		}
		var wire struct {
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if json.Unmarshal(minimum.Body(), &wire) != nil || len(wire.Messages) != 2 {
			t.Fatal("minimum request DATA")
		}
		var history struct {
			Summary string `json:"derived_summary"`
		}
		if json.Unmarshal([]byte(wire.Messages[1].Content), &history) != nil {
			t.Fatal("minimum summary DATA")
		}
		minimumOutput, err = f.actual.source.BoundCompactionSummary(history.Summary)
		if err != nil || minimumOutput < 2 {
			t.Fatal("actual retained minimum output bound", err)
		}
	})
	for _, over := range []bool{false, true} {
		name := "actual_minimum_output_exact"
		if over {
			name = "actual_minimum_output_first_over"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				output := minimumOutput
				if over {
					output--
				}
				f, source := terminalFitGenerationSource(t, localRuntimeActualPrebuild{networkOperation: 5 * time.Second, outputTokens: output}, false)
				before := f.canonical.calls
				g, err := prepareLocalCompactionGeneration(t.Context(), source)
				if over {
					if g != nil || !errors.Is(err, ErrTerminalCapacity) || f.canonical.calls != before {
						t.Fatalf("summary cannot satisfy its original output reserve: bound=%d reserve=%d handle=%t err=%v I/O=%d", minimumOutput, output, g != nil, err, f.canonical.calls-before)
					}
				} else if err != nil || g == nil {
					t.Fatalf("minimum output exact=%d: %v", minimumOutput, err)
				}
			})
		})
	}
}

func TestLocalTerminalFitOriginalMinimumSummaryBytes(t *testing.T) {
	var minimumBytes, responseBytes uint64
	synctest.Test(t, func(t *testing.T) {
		f, source := terminalFitGenerationSource(t, localRuntimeActualPrebuild{networkOperation: 5 * time.Second, finalTextBytes: 224}, false)
		g, err := prepareLocalCompactionGeneration(t.Context(), source)
		if err != nil {
			t.Fatal(err)
		}
		minimum, err := g.program.MinimumSummaryData()
		if err != nil {
			t.Fatal(err)
		}
		minimumBytes = uint64(len(minimum))
		wire, err := json.Marshal(map[string]any{"model": f.actual.config.ResponseModelIDs[0], "choices": []any{map[string]any{"index": 0, "finish_reason": "stop", "message": map[string]any{"role": "assistant", "content": string(minimum)}}}})
		if err != nil {
			t.Fatal(err)
		}
		normalized, err := g.frozen.NormalizeResponseBodyV1(wire, domain.AttemptOperationCompactionV1, 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		// ResponseBytes and its dependent ToolArgumentsBytes declaration change
		// decimal widths in the retained envelope. Derive their bounded exact
		// fixed point; no guessed framing overhead or incompatible old digest.
		n := uint64(len(normalized))
		width := func(n uint64) int { return len(strconv.FormatUint(n, 10)) }
		responseBytes = n
		stable := false
		for i := 0; i < 8; i++ {
			candidate := uint64(int(n) + width(responseBytes) - width(f.actual.config.Limits.ResponseBytes) + width(min(f.actual.config.Limits.ToolArgumentsBytes, responseBytes)) - width(f.actual.config.Limits.ToolArgumentsBytes))
			if candidate == responseBytes {
				stable = true
				break
			}
			responseBytes = candidate
		}
		if !stable || responseBytes < minimumBytes {
			t.Fatal("normalized minimum fixed point failed", responseBytes)
		}
		clear(normalized)
		clear(wire)
		clear(minimum)
	})
	for _, tc := range []struct {
		name           string
		response, over bool
	}{{name: "final_text_exact"}, {name: "final_text_first_over", over: true}, {name: "normalized_response_exact", response: true}, {name: "normalized_response_first_over", response: true, over: true}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				prebuild := localRuntimeActualPrebuild{networkOperation: 5 * time.Second, finalTextBytes: minimumBytes}
				if tc.response {
					prebuild.responseBytes = responseBytes
					if tc.over {
						prebuild.responseBytes--
					}
				} else if tc.over {
					prebuild.finalTextBytes--
				}
				f, source := terminalFitGenerationSource(t, prebuild, false)
				before := f.canonical.calls
				g, err := prepareLocalCompactionGeneration(t.Context(), source)
				if tc.over {
					if g != nil || !errors.Is(err, ErrTerminalCapacity) || f.canonical.calls != before {
						t.Fatalf("minimum format first-over reachedbody: min=%d response=%d handle=%t err=%v I/O=%d", minimumBytes, responseBytes, g != nil, err, f.canonical.calls-before)
					}
				} else if err != nil || g == nil {
					t.Fatalf("exact format original positive: min=%d response=%d err=%v", minimumBytes, responseBytes, err)
				}
			})
		})
	}
}

func TestLocalTerminalFitUpperDominatesAndFirstOver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _, h := terminalOutputFixture(t)
		p, err := h.PrepareOutput(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Clear)
		op := h.source.state.operations[0].Reservation.Operation
		pd, err := f.actual.config.Digest()
		if err != nil {
			t.Fatal(err)
		}
		n, err := localTerminalProspectiveBytes(h.source.state.snapshot.Job.Run, pd, nil, op, 0, domain.MaxTerminalCapsuleBytesV1)
		if err != nil || n < uint64(len(p.capsule)) {
			t.Fatalf("upper=%d actual=%d err=%v", n, len(p.capsule), err)
		}
		if got, err := localTerminalProspectiveBytes(h.source.state.snapshot.Job.Run, pd, nil, op, 0, n); err != nil || got != n {
			t.Fatalf("exact prospective capacity: got=%d want=%d err=%v", got, n, err)
		}
		if _, err := localTerminalProspectiveBytes(h.source.state.snapshot.Job.Run, pd, nil, op, 0, n-1); !errors.Is(err, ErrTerminalCapacity) {
			t.Fatalf("first-over=%d: %v", n-1, err)
		}
		if errors.Is(ErrTerminalCapacity, domain.ErrMembershipDenied) {
			t.Fatal("capacity became generic authority error")
		}
	})
}

func TestLocalTerminalFitCompactionIncludesSuccessor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _, h := terminalOutputFixture(t)
		original := h.source.state.operations[0].Reservation.Operation
		op := original.Clone()
		op.Kind = domain.AttemptOperationCompactionV1
		op.ResultBytes = 4096 // A smaller summarizer must NOT shrink the next MODEL.
		pd, err := f.actual.config.Digest()
		if err != nil {
			t.Fatal(err)
		}
		run := h.source.state.snapshot.Job.Run
		small, err := localTerminalProspectiveBytes(run, pd, nil, op, 4096, domain.MaxTerminalCapsuleBytesV1)
		if err != nil {
			t.Fatal(err)
		}
		full, err := localTerminalProspectiveBytes(run, pd, nil, op, 64<<10, domain.MaxTerminalCapsuleBytesV1)
		if err != nil || full <= small {
			t.Fatalf("mandatory MODEL absent: small=%d full=%d err=%v", small, full, err)
		}
		if _, err := localTerminalProspectiveBytes(run, pd, nil, op, 64<<10, small); !errors.Is(err, ErrTerminalCapacity) {
			t.Fatalf("summarizer-only capacity admitted larger successor: %v", err)
		}
	})
}

func TestLocalTerminalFitOriginalPreBodyChecks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := localPreparedModelFixture(t, 0)
		before := f.canonical.calls
		if err := f.entry.CheckFirstModelTerminalFit(t.Context()); err != nil {
			t.Fatal("original pre-body positive", err)
		}
		if f.canonical.calls != before {
			t.Fatal("capacity precheck performed body I/O")
		}
		copy := *f.entry
		if err := copy.CheckFirstModelTerminalFit(t.Context()); err == nil {
			t.Fatal("copied entry gained original fit authority")
		}
		if err := f.actual.source.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.entry.CheckFirstModelTerminalFit(t.Context()); err == nil {
			t.Fatal("closed original Source passed pre-body check")
		}
	})
}
