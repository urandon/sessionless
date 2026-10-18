package ydbstore

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Production original Source/entry/Store/issuer/body owners; scripted SQL and
// model response DATA. No Keeper seal, socket join, erasure or terminal success
// is claimed by an entry capture. Those remain separate original-owner phases.
func TestLocalTerminalOriginalClaimAndBoundedBusy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := localPreparedModelFixture(t, 0)
		copyEntry := *f.entry // entry contains shared pointers, not copied mutexes
		cancelled, cancel := context.WithCancel(t.Context())
		cancel()
		for _, tc := range []struct {
			entry *LocalInvocationEntryV1
			ctx   context.Context
		}{{nil, t.Context()}, {&copyEntry, t.Context()}, {f.entry, nil}, {f.entry, cancelled}} {
			if h, err := tc.entry.ClaimTerminal(tc.ctx); h != nil || !errors.Is(err, domain.ErrMembershipDenied) {
				t.Fatalf("invalid original claim: handle=%t err=%v", h != nil, err)
			}
		}
		if f.entry.terminal.Load() != nil {
			t.Fatal("invalid claim consumed original slot")
		}
		unlock, err := f.entry.beginLocalMutation(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer unlock()
		at := time.Now()
		h, err := f.entry.ClaimTerminal(t.Context())
		if h == nil || !errors.Is(err, domain.ErrMembershipDenied) || !h.failed.Load() || time.Now() != at {
			t.Fatalf("busy claim queued/renewed/succeeded: handle=%t elapsed=%v err=%v", h != nil, time.Since(at), err)
		}
		d, err := h.Deadline()
		if err != nil || !d.After(at) {
			t.Fatal("failed original owner lost cleanup deadline", err)
		}
		if source, err := h.Capture(t.Context()); source != nil || err == nil {
			t.Fatal("busy/failed claim captured frontier")
		}
		if again, err := f.entry.ClaimTerminal(context.Background()); again != nil || err == nil {
			t.Fatal("busy claim reset after retry")
		}
		if _, err := f.entry.Parent(); err != nil {
			t.Fatal("terminal latch invalidated historical original identity", err)
		}
	})
}

func TestLocalTerminalConcurrentClaimOneOriginalOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := localPreparedModelFixture(t, 0)
		type result struct {
			h   *LocalTerminalV1
			err error
		}
		start, done := make(chan struct{}), make(chan result, 2)
		for range 2 {
			go func() { <-start; h, err := f.entry.ClaimTerminal(t.Context()); done <- result{h, err} }()
		}
		close(start)
		synctest.Wait()
		var winner *LocalTerminalV1
		for range 2 {
			r := <-done
			if r.err == nil {
				if r.h == nil || winner != nil {
					t.Fatal("more than one original terminal owner")
				}
				winner = r.h
			} else if r.h != nil {
				t.Fatal("loser acquired another cleanup/capture owner")
			}
		}
		if winner == nil || f.entry.terminal.Load() != winner {
			t.Fatal("original claim missing")
		}
		deadline, _ := winner.Deadline()
		time.Sleep(time.Second) // virtual elapsed cleanup allowance, not readiness
		if again, err := f.entry.ClaimTerminal(t.Context()); again != nil || err == nil {
			t.Fatal("repeated claim renewed capture")
		}
		if got, _ := winner.Deadline(); got != deadline {
			t.Fatal("cleanup deadline renewed")
		}
	})
}

func TestLocalTerminalCapturesActualCompletedFrontierOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, engine := localPreparedModelFixture(t, 0)
		m, err := f.entry.PrepareModel(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		observed := recordPreparedLocalResponse(t, f, m.operation, m.frozen, "complete original terminal answer")
		before := cloneGenerationRows(f.queries.events)
		h, err := f.entry.ClaimTerminal(t.Context())
		if err != nil {
			t.Fatal("original idle entry claim", err)
		}
		copyHandle := &LocalTerminalV1{self: h, entry: f.entry, deadline: h.deadline}
		if _, err := copyHandle.Deadline(); err == nil {
			t.Fatal("copy established original identity")
		}
		if source, err := copyHandle.Capture(t.Context()); source != nil || err == nil {
			t.Fatal("copy captured original frontier")
		}
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		source, err := h.Capture(ctx)
		if err != nil || source == nil {
			t.Fatal("complete original frontier capture", err)
		}
		defer source.Clear()
		ops := source.Operations()
		defer func() {
			for i := range ops {
				clear(ops[i].Request)
				clear(ops[i].Response)
			}
		}()
		if len(ops) != 1 || !reflect.DeepEqual(ops[0].Observation, observed.Observation) || len(ops[0].Response) == 0 || len(ops[0].Request) == 0 {
			t.Fatal("terminal capture substituted or omitted original normalized material")
		}
		budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		if budget.failed || budget.callbacks != 2 || engine.maxStatements > 32 {
			t.Fatalf("terminal capture reset snapshot budget: callbacks=%d statements=%d", budget.callbacks, engine.maxStatements)
		}
		if again, err := h.Capture(context.Background()); again != nil || err == nil {
			t.Fatal("copied caller context renewed capture")
		}
		if m2, err := f.entry.PrepareModel(t.Context()); m2 != nil || err == nil {
			t.Fatal("terminal entry prepared another MODEL")
		}
		if g, err := f.entry.PrepareCompaction(t.Context()); g != nil || err == nil {
			t.Fatal("terminal entry prepared compaction")
		}
		if !reflect.DeepEqual(before, f.queries.events) || engine.writes != 1 {
			t.Fatal("capture changed canonical source or operation accounting")
		}
		if _, err := f.entry.Source(); err != nil {
			t.Fatal("historical source disabled", err)
		}
	})
}

func TestLocalTerminalPreparedEffectsDenyWithoutGenericFallback(t *testing.T) {
	for _, kind := range []domain.AttemptOperationKindV1{domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1} {
		for _, phase := range []string{"reserve", "before_send"} {
			t.Run(string(kind)+"/"+phase, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f, engine := localPreparedModelFixture(t, 0)
					var op domain.AttemptOperationV1
					if kind == domain.AttemptOperationModelV1 {
						m, err := f.entry.PrepareModel(t.Context())
						if err != nil {
							t.Fatal(err)
						}
						op = m.operation
					} else {
						g, err := f.entry.PrepareCompaction(t.Context())
						if err != nil {
							t.Fatal(err)
						}
						op = g.operation
					}
					installPreparedFutureProxy(t, f, op)
					var reserved ports.ReserveAttemptOperationResultV1
					if phase == "before_send" {
						var err error
						reserved, err = f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: op})
						if err != nil || reserved.Capability == nil {
							t.Fatal("valid preterminal reservation required", err)
						}
					}
					h, err := f.entry.ClaimTerminal(t.Context())
					if err != nil || h == nil {
						t.Fatal(err)
					}
					writes, bodies := engine.writes, len(f.canonical.objects)
					if _, err := f.store.prepareOwnedOperationEvidence(t.Context(), op); !errors.Is(err, domain.ErrMembershipDenied) {
						t.Fatal("terminal denial fell through to generic evidence", err)
					}
					if phase == "reserve" {
						if result, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: op}); err == nil || result.Capability != nil {
							t.Fatal("prepared handle bypassed terminal reserve gate")
						}
					} else if err := f.store.AuthorizeAttemptOperationBeforeSend(t.Context(), reserved.Capability.Clone()); !errors.Is(err, domain.ErrMembershipDenied) {
						t.Fatal("previously prepared send bypassed terminal gate", err)
					}
					if engine.writes != writes || len(f.canonical.objects) != bodies {
						t.Fatal("terminal denial performed another effect/body write")
					}
					if source, err := h.Capture(t.Context()); source != nil || err == nil {
						t.Fatal("empty or unresolved frontier captured successfully")
					}
				})
			})
		}
	}
}

func TestLocalTerminalRetainedPublicationAndPreparationDeny(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, g, engine := compactionPublicationFixture(t)
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		g.source.state.mu.Lock()
		retained, _, err := g.completedRetainedLocked(ctx)
		g.source.state.mu.Unlock()
		if err != nil || retained == nil {
			t.Fatal("valid original preterminal publication material", err)
		}
		summary, manifest := retained.SummaryBody(), retained.ManifestBody()
		defer clear(summary)
		defer clear(manifest)
		if _, err := f.entry.ClaimTerminal(t.Context()); err != nil {
			t.Fatal(err)
		}
		puts, heads := f.canonical.retainedPuts, engine.headWrites
		if err := g.retained.Upload(t.Context(), retained.Plan(), g.observation, summary, manifest); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatal("prepared retained upload bypassed terminal gate", err)
		}
		if next, err := g.Publish(t.Context()); next != nil || err == nil {
			t.Fatal("prepared head publication bypassed terminal gate")
		}
		if next, err := g.PrepareNextModel(t.Context()); next != nil || err == nil {
			t.Fatal("next MODEL preparation bypassed terminal gate")
		}
		if f.canonical.retainedPuts != puts || engine.headWrites != heads {
			t.Fatal("terminal publication denial performed PUT/head write")
		}
	})
	synctest.Test(t, func(t *testing.T) {
		f := newCompactionSourceFixture(t, true, true)
		source, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer source.Clear()
		op := f.operation(t, 1)
		if _, err := f.entry.ClaimTerminal(t.Context()); err != nil {
			t.Fatal(err)
		}
		if h, err := source.PrepareRetainedCompaction(t.Context(), op, 1); h != nil || err == nil {
			t.Fatal("captured source prepared retained pair after terminal latch")
		}
		if h, err := f.entry.PrepareRetainedCompaction(t.Context(), op, 1); h != nil || err == nil {
			t.Fatal("entry prepared retained pair after terminal latch")
		}
	})
}

// The original explicit first-op API remains supported without a prepared
// MODEL/generation. Pause its REAL issuer, not a replacement Store callback.
// SQL/proxy rows are scripted; this does not assert a socket or token-fit proof.
func TestLocalTerminalGenericLocalIssuerOverlap(t *testing.T) {
	for _, phase := range []string{"mint", "consume"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var countdown atomic.Int32
				entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				releaseOnce := sync.OnceFunc(func() { close(release) })
				barrier := func() {
					if countdown.Load() > 0 && countdown.Add(-1) == 0 {
						close(entered)
						<-release
					}
				}
				f, _ := localPreparedModelFixture(t, 0, barrier)
				op := f.operation(t, 1)
				op.Kind, op.SummaryBytes, op.Debit.CompactionCalls = domain.AttemptOperationModelV1, 0, 0
				if op.ValidateForParent(f.entry.parent.Authority, f.entry.parent.Reservation) != nil {
					t.Fatal("valid original generic operation required")
				}
				installPreparedFutureProxy(t, f, op)
				if f.entry.preparedModel.Load() != nil || f.entry.compactionGeneration.Load() != nil {
					t.Fatal("generic test accidentally uses prepared owner")
				}
				var reserved ports.ReserveAttemptOperationResultV1
				var err error
				if phase == "consume" {
					reserved, err = f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: op})
					if err != nil || reserved.Capability == nil {
						t.Fatal("actual generic reserve positive required", err)
					}
					countdown.Store(1)
				} else {
					countdown.Store(2)
				}
				t.Cleanup(func() {
					releaseOnce()
					synctest.Wait()
					select {
					case <-done:
					default:
						t.Error("original generic issuer did not join")
					}
				})
				go func() {
					defer close(done)
					if phase == "mint" {
						reserved, err = f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: op})
					} else {
						err = f.store.AuthorizeAttemptOperationBeforeSend(t.Context(), reserved.Capability.Clone())
					}
				}()
				synctest.Wait()
				select {
				case <-entered:
				default:
					t.Fatalf("actual generic %s never reached original issuer (err=%v)", phase, err)
				}
				h, claimErr := f.entry.ClaimTerminal(t.Context())
				if h == nil || !errors.Is(claimErr, domain.ErrMembershipDenied) || !h.failed.Load() {
					t.Fatalf("generic %s raced successful terminal claim: handle=%t err=%v", phase, h != nil, claimErr)
				}
				releaseOnce()
				synctest.Wait()
				select {
				case <-done:
				default:
					t.Fatal("original admitted generic issuer did not finish")
				}
				if err != nil || reserved.Capability == nil {
					t.Fatal("previously admitted generic operation lost positive control", err)
				}
				if source, err := h.Capture(t.Context()); source != nil || err == nil {
					t.Fatal("failed busy generic claim recovered success")
				}
				if _, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: op}); !errors.Is(err, domain.ErrMembershipDenied) {
					t.Fatal("sealed generic owner admitted a new reserve", err)
				}
			})
		})
	}
}
