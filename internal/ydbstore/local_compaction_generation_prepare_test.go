package ydbstore

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Actual pre-intake installed compiled bundle/calibration BODY, original Source,
// profile publication/receipt, bootstrap, entry, invoice reader, current Store
// transactions and body adapter. SQL/isolation/funding and calibration truth
// remain SCRIPTED DATA, not model qualification, generation/send/head proof.
func prepareGenerationFixture(t *testing.T) (*compactionSourceFixture, *LocalCompactionSourceV1) {
	t.Helper()
	f := newCompactionSourceFixtureWithPrebuild(t, true, localRuntimeActualPrebuild{compiledCompaction: true}, true)
	f.store.ownedProviderPriceCustody = localInvoiceRejectStandardCustody{}
	source, err := f.entry.CaptureCompactionSource(t.Context())
	if err != nil {
		t.Fatalf("compiled original source capture: %v", err)
	}
	t.Cleanup(source.Clear)
	return f, source
}

func TestLocalCompactionGenerationPrepareOriginalFactory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, source := prepareGenerationFixture(t)
		if f.entry.parent.Authority.HarnessBinding.AgentLoopPolicy.InstructionVersion != compactioncontext.InstructionVersionV1 || f.actual.profile.CompactionPolicyDigest != compactioncontext.CompactionPolicyDigestV1() {
			t.Fatal("compiled bundle was not installed before original Source/authority sealing")
		}
		before := f.canonical.calls
		g, err := prepareLocalCompactionGeneration(t.Context(), source)
		if err != nil {
			t.Fatalf("genuine original compiled factory: %v statements=%d max=%d", err, f.queries.statements, f.queries.maxStatements)
		}
		if g == nil || g.self != g || f.entry.compactionGeneration.Load() != g || g.source != source || g.entry != f.entry || g.program == nil || g.retained == nil || g.retained.pair == nil || g.retained.source != source || g.retained.expected != nil || g.retained.uploaded.Load() != nil {
			t.Fatal("factory lost original owner/physical pair or invented publication")
		}
		if g.program.Number() != 1 || g.program.SourceDigest() != source.Digest() || g.frozen.Digest() != g.program.FrozenRequest().Digest() || g.fit.RequestDigest != g.frozen.Digest() || g.fit.CalibrationDigest != f.actual.config.CalibrationDigest || g.fit.InputUpperBound == 0 {
			t.Fatal("generation/real retained calibration result not joined")
		}
		if g.price == nil || g.price.local == nil || g.price.price != nil || g.price.local.source != f.actual.source || g.price.local.digest != f.actual.profile.ProviderPriceDigest || g.operation.Debit.AmountMicrounits != 0 {
			t.Fatal("actual local invoice evidence not retained")
		}
		if g.operation.ValidateForParent(f.entry.parent.Authority, f.entry.parent.Reservation) != nil || g.operation.Kind != domain.AttemptOperationCompactionV1 || g.operation.Sequence != 1 || g.operation.Debit.InputTokens != g.fit.InputUpperBound || g.operation.Debit.OutputTokens != g.fit.OutputReserve {
			t.Fatal("canonical operation/fit debit not derived from original parent")
		}
		body, err := f.entry.bodies.ReadAttemptOperationBody(t.Context(), g.operation.Scope, g.operation.RequestRef, g.operation.RequestBytes)
		if err != nil || !bytes.Equal(body, g.frozen.Body()) {
			t.Fatalf("actual stored request differs from original frozen wire: %v", err)
		}
		clear(body)
		if f.queries.statements > 32 || f.queries.maxStatements > 32 || f.sql.registrationWrites != 0 || f.canonical.retainedPuts != 0 || f.canonical.calls <= before {
			t.Fatal("factory exceeded query budget or published/generated retained artifacts")
		}
		t.Logf("genuine prepare final callback statements=%d maximum=%d", f.queries.statements, f.queries.maxStatements)
		before = f.canonical.calls
		if retry, err := prepareLocalCompactionGeneration(t.Context(), source); !errors.Is(err, domain.ErrMembershipDenied) || retry != nil || f.canonical.calls != before {
			t.Fatal("wrapper replay repeated physical preparation")
		}
		if _, err := f.entry.bodies.ReserveRetainedPair(t.Context(), g.operation.Sequence, 1); err == nil {
			t.Fatal("retained pair was not prepaid before generation")
		}
	})
}

func TestLocalCompactionGenerationPrepareCompletedModelPrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, empty := prepareGenerationFixture(t)
		empty.Clear()
		op := f.operation(t, 1)
		op.Kind, op.SummaryBytes, op.Debit.CompactionCalls = domain.AttemptOperationModelV1, 0, 0
		_, observed, _, _ := f.seedCompleted(t, op, 1)
		source, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatalf("original durable MODEL capture: %v", err)
		}
		defer source.Clear()
		g, err := prepareLocalCompactionGeneration(t.Context(), source)
		if err != nil {
			t.Fatalf("original MODEL prefix generation preparation: %v", err)
		}
		predecessor, err := observed.Observation.Digest()
		if err != nil {
			t.Fatal(err)
		}
		if g.operation.Sequence != 2 || g.operation.PredecessorObservationDigest != predecessor || g.program.Number() != 1 || g.operation.Scope != op.Scope || g.operation.ValidateForParent(f.entry.parent.Authority, f.entry.parent.Reservation) != nil {
			t.Fatal("factory omitted exact durable predecessor/scope")
		}
		if len(source.Operations()) != 1 || f.sql.registrationWrites != 0 || f.canonical.retainedPuts != 0 {
			t.Fatal("preparation mutated source history or published a head")
		}
	})
}

func TestLocalCompactionGenerationPrepareConcurrentOriginalSources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, first := prepareGenerationFixture(t)
		second, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer second.Clear()
		start := make(chan struct{})
		type result struct {
			g   *localCompactionGenerationV1
			err error
		}
		results := make(chan result, 2)
		var joined sync.WaitGroup
		for _, source := range []*LocalCompactionSourceV1{first, second} {
			joined.Add(1)
			go func(s *LocalCompactionSourceV1) {
				defer joined.Done()
				<-start
				g, err := prepareLocalCompactionGeneration(t.Context(), s)
				results <- result{g, err}
			}(source)
		}
		close(start)
		joined.Wait()
		close(results)
		wins := 0
		for result := range results {
			if result.err == nil && result.g != nil {
				wins++
			} else if !errors.Is(result.err, domain.ErrMembershipDenied) || result.g != nil {
				t.Fatalf("concurrent original preparation result=%v", result.err)
			}
		}
		if wins != 1 || f.entry.compactionGeneration.Load() == nil || f.entry.compactionGeneration.Load().retained == nil {
			t.Fatalf("original entry slot winners=%d", wins)
		}
	})
}

func TestLocalCompactionGenerationPrepareDenialsBeforeWrites(t *testing.T) {
	for _, name := range []string{"copied_source", "cleared_source", "closed_owner", "cancelled", "short_caller", "changed_run", "existing_inventory"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, source := prepareGenerationFixture(t)
				ctx := t.Context()
				switch name {
				case "copied_source":
					copied := *source
					source = &copied
				case "cleared_source":
					source.Clear()
				case "closed_owner":
					if err := f.actual.source.Close(); err != nil {
						t.Fatal(err)
					}
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "short_caller":
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, f.parent.graph.at.Add(time.Millisecond))
					defer cancel()
				case "changed_run":
					var run domain.Run
					if json.Unmarshal([]byte(f.parent.graph.rows["runs"][0].(string)), &run) != nil {
						t.Fatal("run fixture")
					}
					run.UpdatedAt = run.UpdatedAt.Add(time.Nanosecond)
					ratedFixturePut(t, f.parent.graph.ratedSQLFixture, "runs", run)
				case "existing_inventory":
					op := f.operation(t, 1)
					plan, _, _, _ := f.seedCompleted(t, op, 1)
					var run domain.Run
					if json.Unmarshal([]byte(f.parent.graph.rows["runs"][0].(string)), &run) != nil {
						t.Fatal("run fixture")
					}
					run.DerivedContext = &domain.RunDerivedContextV1{Version: 1, Inventory: []domain.DerivedContextPlanV1{plan}}
					if run.Validate() != nil {
						t.Fatal("existing valid inventory DATA")
					}
					ratedFixturePut(t, f.parent.graph.ratedSQLFixture, "runs", run)
					source.Clear()
					var err error
					source, err = f.entry.CaptureCompactionSource(t.Context())
					if err != nil {
						t.Fatalf("causal source with inventory must still be capturable: %v", err)
					}
					defer source.Clear()
				}
				before := f.canonical.calls
				if g, err := prepareLocalCompactionGeneration(ctx, source); !errors.Is(err, domain.ErrMembershipDenied) || g != nil || f.canonical.calls != before {
					t.Fatalf("%s factory accepted/wrote: err=%v calls=%d want=%d", name, err, f.canonical.calls, before)
				}
			})
		})
	}
}

func TestLocalCompactionGenerationPrepareLegacyAndOriginalCatalogDeny(t *testing.T) {
	for _, catalog := range []bool{false, true} {
		name := "legacy_missing_bundle_and_body"
		if catalog {
			name = "actual_original_catalog"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newCompactionSourceFixtureWithPrebuild(t, true, localRuntimeActualPrebuild{compiledCompaction: catalog}, true)
				if catalog {
					frozen, err := configuredinference.FreezeRequestV1(f.actual.config, f.actual.config.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "original catalog request"}}, Tools: []configuredinference.ToolV1{{Name: "lookup", Parameters: []byte(`{"type":"object","properties":{},"required":[],"additionalProperties":false}`)}}, MaxOutputTokens: 1})
					if err != nil {
						t.Fatal(err)
					}
					ref, err := f.entry.bodies.Write(t.Context(), frozen.Body())
					if err != nil {
						t.Fatal(err)
					}
					f.frozen, f.request = frozen, ref
					op := f.operation(t, 1)
					op.Kind, op.SummaryBytes, op.Debit.CompactionCalls = domain.AttemptOperationModelV1, 0, 0
					f.seedCompleted(t, op, 1)
				}
				source, err := f.entry.CaptureCompactionSource(t.Context())
				if err != nil {
					t.Fatalf("causal original source: %v", err)
				}
				defer source.Clear()
				before := f.canonical.calls
				if g, err := prepareLocalCompactionGeneration(t.Context(), source); !errors.Is(err, domain.ErrMembershipDenied) || g != nil || f.canonical.calls != before {
					t.Fatalf("%s silently admitted/stripped: %v", name, err)
				}
			})
		})
	}
}

func TestLocalCompactionGenerationPrepareNoFitAndWriteTombstone(t *testing.T) {
	for _, name := range []string{"no_fit", "ambiguous_request", "pair_already_spent"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, source := prepareGenerationFixture(t)
				want := domain.ErrMembershipDenied
				switch name {
				case "no_fit":
					source.Clear()
					last := &f.records[len(f.records)-1]
					raw, _ := json.Marshal(map[string]any{"version": 1, "text": strings.Repeat("x", 40000)})
					ref, err := f.canonical.Put(t.Context(), last.Event.TenantID, last.Event.Payload.Key, bytes.NewReader(raw))
					if err != nil {
						t.Fatal(err)
					}
					last.Payload, last.Event.Payload = raw, ref
					row, _ := json.Marshal(last.Event)
					f.queries.events[len(f.queries.events)-1] = []driver.Value{string(row)}
					source, err = f.entry.CaptureCompactionSource(t.Context())
					if err != nil {
						t.Fatalf("large protected original must remain valid source: %v", err)
					}
					defer source.Clear()
					want = configuredinference.ErrContextNoFit
				case "ambiguous_request":
					f.canonical.ambiguousOrdinary = true
				case "pair_already_spent":
					if _, err := f.entry.bodies.ReserveRetainedPair(t.Context(), 1, 1); err != nil {
						t.Fatal(err)
					}
				}
				before := f.canonical.calls
				if g, err := prepareLocalCompactionGeneration(t.Context(), source); !errors.Is(err, want) || g != nil {
					t.Fatalf("%s result=%v want=%v", name, err, want)
				}
				if name == "no_fit" {
					if f.canonical.calls != before || f.entry.compactionGeneration.Load() != nil {
						t.Fatal("no-fit produced physical request or claimed slot")
					}
					return
				}
				if f.entry.compactionGeneration.Load() == nil || f.entry.compactionGeneration.Load().retained != nil {
					t.Fatal("failed physical preparation did not retain an unusable original tombstone")
				}
				before = f.canonical.calls
				if g, err := prepareLocalCompactionGeneration(t.Context(), source); !errors.Is(err, domain.ErrMembershipDenied) || g != nil || f.canonical.calls != before {
					t.Fatal("failed preparation retried physical request/pair")
				}
			})
		})
	}
}

func TestLocalCompactionGenerationPrepareSourceCloseBeforePUT(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Public cancellation-cause propagation makes the old asynchronous
		// composition fail deterministically even if its callback happens to run
		// before observation. No sleep/Wait/scheduler ordering is the oracle.
		cause := errors.New("owned original Source lifetime revoked at PUT barrier")
		parent, revoke := context.WithCancelCause(t.Context())
		defer revoke(context.Canceled)
		f := newCompactionSourceFixtureWithPrebuild(t, true, localRuntimeActualPrebuild{compiledCompaction: true, sourceParent: parent}, true)
		source, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer source.Clear()
		objects := len(f.canonical.objects)
		var observedErr, observedCause, closeErr error
		entered := false
		f.canonical.beforeOrdinary = func(call context.Context) {
			entered = true
			revoke(cause)
			closeErr = f.actual.source.Close()
			// AFTER actual Close returned and BEFORE fake storage may write.
			observedErr, observedCause = call.Err(), context.Cause(call)
		}
		g, err := prepareLocalCompactionGeneration(t.Context(), source)
		if !entered || closeErr != nil || !errors.Is(err, domain.ErrMembershipDenied) || g != nil {
			t.Fatalf("original pre-PUT barrier entered=%t close=%v prepare=%v", entered, closeErr, err)
		}
		if !errors.Is(observedErr, context.Canceled) || !errors.Is(observedCause, cause) {
			t.Fatalf("PUT context was not synchronously descended from original Source: err=%v cause=%v want=%v", observedErr, observedCause, cause)
		}
		if len(f.canonical.objects) != objects || f.canonical.retainedPuts != 0 {
			t.Fatal("fresh body storage write occurred after original Source.Close returned")
		}
	})
}

func TestLocalCompactionLifetimePreservesCallerCancellationCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := prepareGenerationFixture(t)
		caller, cancel := context.WithCancelCause(t.Context())
		defer cancel(context.Canceled)
		call, finish, err := f.entry.localCompactionLifetimeContext(caller)
		if err != nil {
			t.Fatal(err)
		}
		defer finish()
		cause := errors.New("original caller stopped compaction")
		cancel(cause)
		synctest.Wait() // Quiesce the independent caller AfterFunc, not a sleep.
		if !errors.Is(call.Err(), context.Canceled) || !errors.Is(context.Cause(call), cause) {
			t.Fatalf("caller cancellation lost cause: err=%v cause=%v want=%v", call.Err(), context.Cause(call), cause)
		}
		life, err := f.actual.source.ExecutionLifetime()
		if err != nil || life.Err() != nil {
			t.Fatal("one caller cancellation revoked the original shared Source")
		}
	})
}

func TestLocalCompactionGenerationPreparePreservesOriginalBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, source := prepareGenerationFixture(t)
		type callerValueKey struct{}
		marker := new(int)
		original, cancel := ownedAuthorityCallContext(context.WithValue(t.Context(), callerValueKey{}, marker))
		defer cancel()
		budget := original.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		root, finish, err := f.entry.localCompactionLifetimeContext(original)
		if err != nil {
			t.Fatal(err)
		}
		defer finish()
		if root.Value(ownedAuthorityCallContextKey{}) != budget {
			t.Fatal("Source-rooted helper replaced original logical budget identity")
		}
		if root.Value(callerValueKey{}) != marker {
			t.Fatal("Source-rooted helper discarded caller values")
		}
		call, end := ownedAuthorityCallContext(root)
		defer end()
		if call.Value(ownedAuthorityCallContextKey{}) != budget {
			t.Fatal("downstream helper allocated a replacement budget")
		}
		if call.Value(callerValueKey{}) != marker {
			t.Fatal("downstream helper discarded caller values")
		}
		if err := chargeOwnedAuthorityBody(original, ownedAuthorityMaxBytes+1); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatal("budget exhaustion fixture")
		}
		reads, writes := f.queries.rangeReads, f.canonical.calls
		if g, err := prepareLocalCompactionGeneration(original, source); !errors.Is(err, domain.ErrMembershipDenied) || g != nil || f.queries.rangeReads != reads || f.canonical.calls != writes || f.entry.compactionGeneration.Load() != nil {
			t.Fatalf("exhausted original budget was reset: err=%v reads=%d/%d writes=%d/%d", err, f.queries.rangeReads, reads, f.canonical.calls, writes)
		}
	})
}
