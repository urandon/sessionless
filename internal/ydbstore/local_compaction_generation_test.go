package ydbstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Real original Source/bootstrap/entry/Store and body adapter; phase rows and
// private prepared state below are SCRIPTED DATA. There is intentionally no
// production generation constructor/effect hook here, no inference, fit proof,
// structured-summary validation or head publication claim.
type generationPhaseFixture struct {
	f                   *compactionSourceFixture
	g                   *localCompactionGenerationV1
	plan                domain.DerivedContextPlanV1
	observed            ports.AuthenticatedAttemptOperationObservationV1
	summary, manifest   []byte
	completedRows       [][]driver.Value
	completedAccounting []driver.Value
}

func newGenerationPhaseFixture(t *testing.T, prefix bool) *generationPhaseFixture {
	t.Helper()
	f := newCompactionSourceFixture(t, true, true)
	sequence := uint64(1)
	var predecessor domain.AttemptOperationObservationDigestV1
	if prefix {
		op := f.operation(t, 1)
		op.Kind, op.SummaryBytes, op.Debit.CompactionCalls = domain.AttemptOperationModelV1, 0, 0
		_, observed, _, _ := f.seedCompleted(t, op, 1)
		predecessor, _ = observed.Observation.Digest()
		sequence++
	}
	source, err := f.entry.CaptureCompactionSource(t.Context())
	if err != nil {
		t.Fatalf("original source capture: %v", err)
	}
	t.Cleanup(source.Clear)
	op := f.operation(t, sequence)
	op.PredecessorObservationDigest = predecessor
	retained, err := source.PrepareRetainedCompaction(t.Context(), op, 1)
	if err != nil {
		t.Fatalf("original source retained preparation: %v", err)
	}
	g := &localCompactionGenerationV1{entry: f.entry, source: source, retained: retained, frozen: f.frozen, operation: op}
	g.self = g
	if !f.entry.compactionGeneration.CompareAndSwap(nil, g) {
		t.Fatal("fixture original generation slot occupied")
	}
	plan, observed, summary, manifest := f.seedCompleted(t, op, 1)
	plan.SourceViewDigest = source.Digest()
	g.reservation, g.observation = observed.Reservation, observed
	result := &generationPhaseFixture{f: f, g: g, plan: plan, observed: observed, summary: summary, manifest: manifest, completedRows: cloneGenerationRows(f.queries.operations), completedAccounting: append([]driver.Value(nil), f.parent.sql.accountingRow...)}
	return result
}

func cloneGenerationRows(rows [][]driver.Value) [][]driver.Value {
	copyRows := make([][]driver.Value, len(rows))
	for i := range rows {
		copyRows[i] = append([]driver.Value(nil), rows[i]...)
	}
	return copyRows
}

func (f *generationPhaseFixture) stage(t *testing.T, phase localCompactionGenerationPhase) {
	t.Helper()
	f.f.queries.operations = cloneGenerationRows(f.completedRows)
	f.f.parent.sql.accountingRow = append([]driver.Value(nil), f.completedAccounting...)
	switch phase {
	case localCompactionReserve:
		f.f.queries.operations = f.f.queries.operations[:len(f.f.queries.operations)-1]
		raw, digest, err := accountingBytes(f.g.source.state.snapshot.Accounting)
		if err != nil {
			t.Fatal(err)
		}
		f.f.parent.sql.accountingRow = []driver.Value{uint64(1), uint64(1), string(raw), digest}
	case localCompactionBeforeSend:
		tail := f.f.queries.operations[len(f.f.queries.operations)-1]
		tail[5], tail[6], tail[7], tail[8] = uint64(0), uint64(0), "", ""
	}
}

func generationJoin(t *testing.T, g *localCompactionGenerationV1, phase localCompactionGenerationPhase) (domain.Run, error) {
	t.Helper()
	call, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	g.source.state.mu.Lock()
	defer g.source.state.mu.Unlock()
	var run domain.Run
	err := g.entry.owner.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		var err error
		run, err = g.rejoinLocked(ctx, q, phase)
		return err
	})
	return run, err
}

func TestLocalCompactionGenerationPrivatePhaseJoins(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		name := "zero_prefix"
		if prefix {
			name = "completed_model_prefix"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newGenerationPhaseFixture(t, prefix)
				for _, phase := range []localCompactionGenerationPhase{localCompactionReserve, localCompactionBeforeSend, localCompactionCompleted} {
					f.stage(t, phase)
					run, err := generationJoin(t, f.g, phase)
					if err != nil || !reflect.DeepEqual(run, f.g.source.state.snapshot.Job.Run) {
						t.Fatalf("phase=%d exact original join: run=%s err=%v statements=%d", phase, run.ID, err, f.f.queries.statements)
					}
					if f.f.queries.statements > 32 || f.f.queries.maxStatements > 32 {
						t.Fatalf("phase=%d exceeded statement budget: current=%d max=%d", phase, f.f.queries.statements, f.f.queries.maxStatements)
					}
					t.Logf("private phase=%d statements=%d max=%d", phase, f.f.queries.statements, f.f.queries.maxStatements)
				}
				if _, err := generationJoin(t, f.g, localCompactionPostRegistration); !errors.Is(err, domain.ErrMembershipDenied) {
					t.Fatal("completed rows without actual retained upload became postregistration evidence")
				}
				if err := f.g.retained.Upload(t.Context(), f.plan, f.observed, f.summary, f.manifest); err != nil {
					t.Fatalf("original registered retained write: %v", err)
				}
				uploaded := f.g.retained.uploaded.Load()
				if uploaded == nil || !reflect.DeepEqual(uploaded.plan, f.plan) || len(uploaded.postimage.Inventory) != 1 || uploaded.postimage.Head != nil || f.f.canonical.retainedPuts != 2 {
					t.Fatal("successful upload did not capture exact inventory-only postimage")
				}
				run, err := generationJoin(t, f.g, localCompactionPostRegistration)
				if err != nil || !domain.EqualRunDerivedContextV1(run.DerivedContext, &uploaded.postimage) {
					t.Fatalf("registered exact postimage join: %v statements=%d", err, f.f.queries.statements)
				}
				if _, err := generationJoin(t, f.g, localCompactionCompleted); !errors.Is(err, domain.ErrMembershipDenied) {
					t.Fatal("registered metadata silently accepted as old preimage")
				}
			})
		})
	}
}

func TestLocalCompactionGenerationDoesNotRelaxCapture(t *testing.T) {
	for _, phase := range []localCompactionGenerationPhase{localCompactionReserve, localCompactionBeforeSend} {
		name := "future_proxy"
		if phase == localCompactionBeforeSend {
			name = "unobserved_reservation"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newGenerationPhaseFixture(t, false)
				f.stage(t, phase)
				if _, err := generationJoin(t, f.g, phase); err != nil {
					t.Fatalf("causal valid private phase: %v", err)
				}
				if source, err := f.f.entry.CaptureCompactionSource(t.Context()); !errors.Is(err, domain.ErrMembershipDenied) || source != nil {
					if source != nil {
						source.Clear()
					}
					t.Fatal("normal Capture accepted phase-only future/pending state")
				}
			})
		})
	}
}

func TestLocalCompactionGenerationPhaseDenials(t *testing.T) {
	for _, name := range []string{"unknown_phase", "copied_handle", "foreign_slot", "cleared_source", "observed_before_send", "unobserved_completed", "wrong_reservation", "wrong_proxy", "revoked_tail_proxy", "changed_run", "trailing_operation", "trailing_proxy"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newGenerationPhaseFixture(t, true)
				phase := localCompactionBeforeSend
				f.stage(t, phase)
				if _, err := generationJoin(t, f.g, phase); err != nil {
					t.Fatalf("causal valid before-send shape: %v", err)
				}
				switch name {
				case "unknown_phase":
					phase = 99
				case "copied_handle":
					copied := *f.g
					f.g = &copied
				case "foreign_slot":
					f.f.entry.compactionGeneration.Store(new(localCompactionGenerationV1))
				case "cleared_source":
					f.g.source.Clear()
				case "observed_before_send":
					f.stage(t, localCompactionCompleted)
				case "unobserved_completed":
					phase = localCompactionCompleted
				case "wrong_reservation":
					f.g.reservation.RegisteredProxyDigest = domain.RegisteredProxyAttestationDigestV2("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
				case "wrong_proxy":
					row := f.f.queries.proxies[len(f.f.queries.proxies)-1]
					row[8] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				case "revoked_tail_proxy":
					row := f.f.queries.proxies[len(f.f.queries.proxies)-1]
					var original, current domain.RegisteredProxyAttestationV2
					if json.Unmarshal([]byte(row[7].(string)), &original) != nil {
						t.Fatal("original tail proxy")
					}
					current = original.Clone()
					current.State, current.RevocationGeneration = domain.RegisteredPreparationRevokedV1, original.RevocationGeneration+1
					if current.ValidateTransitionFrom(original) != nil {
						t.Fatal("causal revoked current proxy must be a valid transition")
					}
					raw, err := domain.EncodeRegisteredProxyAttestationV2(current)
					if err != nil {
						t.Fatal(err)
					}
					digest, err := current.Digest()
					if err != nil {
						t.Fatal(err)
					}
					row[9], row[10] = string(raw), string(digest)
				case "changed_run":
					var run domain.Run
					if json.Unmarshal([]byte(f.f.parent.graph.rows["runs"][0].(string)), &run) != nil {
						t.Fatal("run fixture")
					}
					run.UpdatedAt = run.UpdatedAt.Add(1)
					ratedFixturePut(t, f.f.parent.graph.ratedSQLFixture, "runs", run)
				case "trailing_operation":
					f.f.queries.operations = append(f.f.queries.operations, append([]driver.Value(nil), f.f.queries.operations[len(f.f.queries.operations)-1]...))
				case "trailing_proxy":
					f.f.queries.proxies = append(f.f.queries.proxies, append([]driver.Value(nil), f.f.queries.proxies[len(f.f.queries.proxies)-1]...))
				}
				if run, err := generationJoin(t, f.g, phase); !errors.Is(err, domain.ErrMembershipDenied) || run != (domain.Run{}) {
					t.Fatalf("%s accepted phase=%d run=%s err=%v", name, phase, run.ID, err)
				}
				if f.f.canonical.retainedPuts != 0 || f.f.sql.registrationWrites != 0 {
					t.Fatal("read-only phase check caused mutation")
				}
			})
		})
	}
}

func TestLocalCompactionGenerationAmbiguousUploadHasNoSuccessState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newGenerationPhaseFixture(t, false)
		f.f.canonical.ambiguous = true
		if err := f.g.retained.Upload(t.Context(), f.plan, f.observed, f.summary, f.manifest); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatalf("ambiguous PUT result=%v", err)
		}
		if f.g.retained.uploaded.Load() != nil {
			t.Fatal("ambiguous PUT produced successful-upload state")
		}
		if _, err := generationJoin(t, f.g, localCompactionPostRegistration); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatal("ambiguous pair became usable postregistration state")
		}
	})
}
