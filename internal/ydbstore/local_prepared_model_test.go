package ydbstore

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Original private Source, compiled calibration BODY, entry, Store, issuers,
// operation bodies and retained publisher are production. Proxy/SQL isolation,
// external funding and model response bytes are scripted, not native SQL or
// actual socket/model/qualification evidence. No private prepared owner is made
// by the fixture: the public original entry methods must construct every one.
func localPreparedModelFixture(t *testing.T, input uint64, barrier ...func()) (*compactionSourceFixture, *compactionPublicationSQL) {
	t.Helper()
	if len(barrier) > 1 {
		t.Fatal("at most one original issuer clock barrier")
	}
	prebuild := localRuntimeActualPrebuild{compiledCompaction: true, networkOperation: 5 * time.Second, inputTokens: input}
	if len(barrier) == 1 {
		prebuild.operationClockBarrier = barrier[0]
	}
	return localPreparedModelFixtureWithPrebuild(t, prebuild)
}

func localPreparedModelFixtureWithPrebuild(t *testing.T, prebuild localRuntimeActualPrebuild) (*compactionSourceFixture, *compactionPublicationSQL) {
	t.Helper()
	f := newCompactionSourceFixtureWithPrebuild(t, true, prebuild, true)
	f.store.ownedProviderPriceCustody = localInvoiceRejectStandardCustody{}
	engine := &compactionPublicationSQL{compactionEffectSQL: &compactionEffectSQL{compactionSourceSQL: f.queries}}
	db := sql.OpenDB(compactionPublicationConnector{engine})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store.db = db
	return f, engine
}

func recordPreparedLocalResponse(t *testing.T, f *compactionSourceFixture, op domain.AttemptOperationV1, frozen *configuredinference.FrozenRequestV1, text string) ports.AuthenticatedAttemptOperationObservationV1 {
	t.Helper()
	installPreparedFutureProxy(t, f, op)
	r, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: op})
	if err != nil || r.Capability == nil || r.Status != ports.AttemptOperationReservedV1 {
		t.Fatalf("actual prepared %s seq%d Reserve: %v", op.Kind, op.Sequence, err)
	}
	if err := f.store.AuthorizeAttemptOperationBeforeSend(t.Context(), r.Capability.Clone()); err != nil {
		t.Fatalf("actual prepared %s seq%d BeforeSend: %v", op.Kind, op.Sequence, err)
	}
	wire, err := json.Marshal(map[string]any{"model": f.actual.config.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text}, "finish_reason": "stop"}}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := frozen.NormalizeResponseBodyV1(wire, op.Kind, op.ResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(body)
	ref, err := f.entry.bodies.Write(t.Context(), body)
	if err != nil {
		t.Fatal(err)
	}
	rd, _ := r.Reservation.Digest()
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: r.Reservation.OperationDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: ref.Ref, ResponseDigest: ref.Digest, ResponseBytes: ref.Bytes, ObservedAt: f.parent.graph.at, Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
	observed, err := f.bootstrap.operate.AuthenticateAttemptOperationObservation(r.ObservationGrant, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordAttemptOperationObservation(t.Context(), ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: r.ObservationGrant, Observation: observed}); err != nil {
		t.Fatalf("actual %s seq%d Record: %v", op.Kind, op.Sequence, err)
	}
	read, err := f.store.ReadAttemptOperationObservation(t.Context(), r.ObservationGrant)
	if err != nil || !reflect.DeepEqual(read, observed) {
		t.Fatalf("actual %s seq%d Read: %v", op.Kind, op.Sequence, err)
	}
	return read
}

func actualPreparedSummary(t *testing.T, g *localCompactionGenerationV1) string {
	t.Helper()
	refs := g.program.References()
	if len(refs) == 0 {
		t.Fatal("original reference table")
	}
	body, err := json.Marshal(map[string]any{"schema": "sessionless.compaction-summary.v1", "task": []any{map[string]any{"text": "Keep the original task", "source_ids": []string{refs[0].SourceID}}}, "constraints": []any{}, "facts": []any{}, "unresolved": []any{}, "artifacts": []any{}, "omissions": []any{}})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestLocalPreparedModelOriginalFullViewAndEffects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, engine := localPreparedModelFixture(t, 0)
		canonical := cloneGenerationRows(f.queries.events)
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		m, err := f.entry.PrepareModel(ctx)
		if err != nil || m == nil {
			t.Fatalf("original fullview preparation: %v callbacks=%d max=%d", err, ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget).callbacks, engine.maxStatements)
		}
		budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		if budget.failed || budget.callbacks != 3 || budget.statements > 96 || engine.maxStatements > 32 {
			t.Fatalf("original preparation budget callbacks=%d statements=%d failed=%t", budget.callbacks, budget.statements, budget.failed)
		}
		op, err := m.Operation()
		if err != nil || op.Sequence != 1 || op.Kind != domain.AttemptOperationModelV1 || m.fit.RequestDigest != m.frozen.Digest() || op.RequestDigest == m.frozen.Digest() {
			t.Fatal("raw/typed original MODEL binding", err)
		}
		if engine.writes != 0 || engine.headWrites != 0 || f.canonical.retainedPuts != 0 || f.entry.compactionGeneration.Load() != nil {
			t.Fatal("ordinary preparation generated/reserved/published")
		}
		observed := recordPreparedLocalResponse(t, f, op, m.frozen, "first genuine completed MODEL DATA")
		if !reflect.DeepEqual(observed, m.observation) {
			t.Fatal("actual authenticated read not retained")
		}
		if _, err := f.entry.PrepareModel(t.Context()); err != nil {
			t.Fatalf("next tool-free MODEL uses exact typed non-fixture history: %v", err)
		}
		if !reflect.DeepEqual(canonical, f.queries.events) {
			t.Fatal("canonical source mutated")
		}
	})
}

func TestLocalPreparedModelCurrentRunBeforeSendAndLostCommit(t *testing.T) {
	for _, name := range []string{"changed_run", "lost_rollback", "lost_persist"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, engine := localPreparedModelFixture(t, 0)
				m, err := f.entry.PrepareModel(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				installPreparedFutureProxy(t, f, m.operation)
				if name != "changed_run" {
					engine.loseCommit = name[len("lost_"):]
				}
				r, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: m.operation})
				if name == "changed_run" {
					if err != nil || r.Capability == nil {
						t.Fatal("live original reserve", err)
					}
					run := publicationRun(t, f)
					run.CreatedAt = run.CreatedAt.Add(-time.Microsecond)
					if run.Validate() != nil || run.CreatedAt.After(f.parent.graph.at) || run.UpdatedAt.After(f.parent.graph.at) {
						t.Fatal("causal structurally valid changed Run")
					}
					ratedFixturePut(t, f.parent.graph.ratedSQLFixture, "runs", run)
					if err := f.store.AuthorizeAttemptOperationBeforeSend(t.Context(), r.Capability.Clone()); !errors.Is(err, domain.ErrMembershipDenied) {
						t.Fatal("changed Run allowed before-send", err)
					}
					if err := f.bootstrap.operate.ConsumeAttemptOperationCapability(r.Capability.Clone()); err != nil {
						t.Fatal("denial consumed original capability", err)
					}
				} else {
					if err == nil || r.Capability != nil || !m.reservationAttempted || m.reservation.Version != 0 || engine.writes != 1 || engine.lostAcks != 1 {
						t.Fatal("lost ACK became capability or retried write", err)
					}
					if again, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: m.operation}); err == nil || again.Capability != nil || engine.writes != 1 {
						t.Fatal("fresh call retried ambiguous MODEL")
					}
				}
			})
		})
	}
}

func TestLocalPreparedModelNoFitDoesNotClaimOrWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, engine := localPreparedModelFixture(t, 100)
		before := len(f.canonical.objects)
		m, err := f.entry.PrepareModel(t.Context())
		if !errors.Is(err, configuredinference.ErrContextNoFit) || m != nil || f.entry.preparedModel.Load() != nil || engine.writes != 0 || len(f.canonical.objects) != before {
			t.Fatalf("genuine Source nofit lost/noeffects: %v", err)
		}
	})
}

func TestLocalPreparedModelTwoCompactionsOriginalChain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, engine := localPreparedModelFixture(t, 0)
		canonical := cloneGenerationRows(f.queries.events)
		g1, err := f.entry.PrepareCompaction(t.Context())
		if err != nil {
			t.Fatal("original entry generation1", err)
		}
		recordPreparedLocalResponse(t, f, g1.operation, g1.frozen, actualPreparedSummary(t, g1))
		next1, err := g1.Publish(t.Context())
		if err != nil {
			t.Fatal("generation1 actual publication", err)
		}
		m1, err := g1.PrepareNextModel(t.Context())
		if err != nil || m1 == nil {
			t.Fatal("original committed head next MODEL", err)
		}
		if next1.Digest() != m1.frozen.Digest() || !bytes.Equal(next1.Body(), m1.frozen.Body()) {
			t.Fatal("next MODEL replaced published frozen input")
		}
		recordPreparedLocalResponse(t, f, m1.operation, m1.frozen, "completed ordinary model two")
		m2, err := f.entry.PrepareModel(t.Context())
		if err != nil {
			t.Fatal("new closed MODEL frontier", err)
		}
		recordPreparedLocalResponse(t, f, m2.operation, m2.frozen, "completed ordinary model three")
		if _, err := readPublishedLocalCompaction(t.Context(), g1); err == nil {
			t.Fatal("old generation stale frontier accepted newer MODELs")
		}
		g2, err := f.entry.PrepareCompaction(t.Context())
		if err != nil {
			t.Fatal("new closed frontier generation2", err)
		}
		if g2 == g1 || g2.program.Number() != 2 || g2.parentRetained == nil || f.entry.compactionGeneration.Load() != g2 || g2.source == g1.source {
			t.Fatal("generation2 did not replace exact confirmed original owner")
		}
		recordPreparedLocalResponse(t, f, g2.operation, g2.frozen, actualPreparedSummary(t, g2))
		next2, err := g2.Publish(t.Context())
		if err != nil {
			t.Fatal("generation2 actual publication", err)
		}
		m3, err := g2.PrepareNextModel(t.Context())
		if err != nil || m3 == nil {
			t.Fatal("generation2 committed head next MODEL", err)
		}
		if next2.Digest() != m3.frozen.Digest() || !bytes.Equal(next2.Body(), m3.frozen.Body()) {
			t.Fatal("generation2 published input was replaced")
		}
		recordPreparedLocalResponse(t, f, m3.operation, m3.frozen, "completed ordinary model five")
		run := publicationRun(t, f)
		if run.DerivedContext == nil || len(run.DerivedContext.Inventory) != 2 || run.DerivedContext.Head == nil || engine.writes != 5 || f.canonical.retainedPuts != 4 || !reflect.DeepEqual(canonical, f.queries.events) {
			t.Fatal("two-generation chain lost inventory/operations/canonical bytes")
		}
		objects := len(f.canonical.objects)
		if g, err := f.entry.PrepareCompaction(t.Context()); err == nil || g != nil || len(f.canonical.objects) != objects {
			t.Fatal("third generation acquired physical capacity")
		}
	})
}
