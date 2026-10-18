package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// SQL commit/rollback and a lost head acknowledgement are scripted. The actual
// original factory, Store/issuer, bodies, retained pair, typed codec, head
// publisher and next-request reader execute production code. No socket/model,
// native YDB, gen2 runtime, terminal custody or fresh-Run proof is claimed.
type compactionPublicationSQL struct {
	*compactionEffectSQL
	headWrites int
	loseHead   string
}
type compactionPublicationConnector struct{ f *compactionPublicationSQL }

func (c compactionPublicationConnector) Connect(context.Context) (driver.Conn, error) {
	return c.f, nil
}
func (c compactionPublicationConnector) Driver() driver.Driver {
	return compactionEffectConnector{c.f.compactionEffectSQL}.Driver()
}

type compactionPublicationTx struct {
	driver.Tx
	f          *compactionPublicationSQL
	beforeRun  []driver.Value
	beforeHead int
	done       bool
}

func (f *compactionPublicationSQL) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := f.compactionEffectSQL.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &compactionPublicationTx{Tx: tx, f: f, beforeRun: append([]driver.Value(nil), f.f.parent.graph.rows["runs"]...), beforeHead: f.headWrites}, nil
}
func (f *compactionPublicationSQL) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if statement == localRetainedRunUpdateSQL {
		var run domain.Run
		if len(args) != 3 || json.Unmarshal([]byte(args[0].Value.(string)), &run) != nil {
			return nil, errors.New("publication scripted exact Run payload")
		}
		if run.DerivedContext != nil && run.DerivedContext.Head != nil {
			f.headWrites++
		}
	}
	return f.compactionEffectSQL.ExecContext(ctx, statement, args)
}
func (tx *compactionPublicationTx) Commit() error {
	head := tx.f.headWrites > tx.beforeHead
	if head && tx.f.loseHead == "rollback" {
		tx.f.loseHead = ""
		_ = tx.Rollback()
		return retry.RetryableError(errors.New("owned head ACK lost without persistence"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	tx.done = true
	if head && tx.f.loseHead == "persist" {
		tx.f.loseHead = ""
		return retry.RetryableError(errors.New("owned persisted head ACK lost"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	return nil
}
func (tx *compactionPublicationTx) Rollback() error {
	if !tx.done {
		tx.f.f.parent.graph.rows["runs"] = append([]driver.Value(nil), tx.beforeRun...)
	}
	tx.done = true
	return tx.Tx.Rollback()
}

func compactionPublicationFixture(t *testing.T) (*compactionSourceFixture, *localCompactionGenerationV1, *compactionPublicationSQL) {
	t.Helper()
	f, g, grant, _ := recordedCompactionEffect(t)
	if _, err := f.store.ReadAttemptOperationObservation(t.Context(), grant); err != nil {
		t.Fatalf("actual durable read before publication: %v", err)
	}
	engine := &compactionPublicationSQL{compactionEffectSQL: &compactionEffectSQL{compactionSourceSQL: f.queries}}
	db := sql.OpenDB(compactionPublicationConnector{engine})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store.db = db // same original Store, extended declared SQL commit model only
	return f, g, engine
}

func publicationRun(t *testing.T, f *compactionSourceFixture) domain.Run {
	t.Helper()
	var run domain.Run
	if json.Unmarshal([]byte(f.parent.graph.rows["runs"][0].(string)), &run) != nil || run.Validate() != nil {
		t.Fatal("valid original Run row required")
	}
	return run
}

func TestLocalCompactionPublicationActualHeadAndNextRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, g, engine := compactionPublicationFixture(t)
		canonical := cloneGenerationRows(f.queries.events)
		before := publicationRun(t, f)
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		next, err := publishLocalCompaction(ctx, g)
		if err != nil || next == nil {
			t.Fatalf("original completed generation -> typed upload/head/next: %v writes=%d heads=%d puts=%d statements=%d/%d", err, f.sql.registrationWrites, engine.headWrites, f.canonical.retainedPuts, engine.statements, engine.maxStatements)
		}
		after := publicationRun(t, f)
		if after.DerivedContext == nil || after.DerivedContext.Head == nil || len(after.DerivedContext.Inventory) != 1 || g.publication == nil || !domain.EqualRunDerivedContextV1(after.DerivedContext, &g.publication.postimage) || engine.headWrites != 1 || f.sql.registrationWrites != 2 || f.canonical.retainedPuts != 2 || f.canonical.premature {
			t.Fatal("head did not preserve registered inventory and original write/readback order")
		}
		base := after.Clone()
		base.DerivedContext = before.DerivedContext
		if !reflect.DeepEqual(base, before) || !reflect.DeepEqual(canonical, f.queries.events) {
			t.Fatal("publication changed base Run or canonical source")
		}
		budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		if budget.failed || budget.callbacks != 3 || budget.statements > 96 || engine.maxStatements > 32 || engine.statements > 32 {
			t.Fatalf("original logical limits changed/exceeded: callbacks=%d statements=%d bytes=%d failed=%t", budget.callbacks, budget.statements, budget.bytes, budget.failed)
		}
		t.Logf("publication callbacks=%d statements=%d last=%d max=%d bytes=%d", budget.callbacks, budget.statements, engine.statements, engine.maxStatements, budget.bytes)
		again, err := readPublishedLocalCompaction(t.Context(), g)
		if err != nil || again == nil || !bytes.Equal(next.Body(), again.Body()) || next.Digest() != again.Digest() {
			t.Fatalf("actual committed-head/typed-object reread: %v", err)
		}
		if repeated, err := publishLocalCompaction(t.Context(), g); err == nil || repeated != nil || engine.headWrites != 1 || f.canonical.retainedPuts != 2 {
			t.Fatal("publication retry recovered physical ownership")
		}
	})
}

func TestLocalCompactionPublicationLostHeadAcknowledgement(t *testing.T) {
	for _, mode := range []string{"rollback", "persist"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, g, engine := compactionPublicationFixture(t)
				engine.loseHead = mode
				next, err := publishLocalCompaction(t.Context(), g)
				if err == nil || next != nil || g.publication != nil || engine.headWrites != 1 || f.canonical.retainedPuts != 2 || !g.publicationAttempted {
					t.Fatalf("%s ambiguous head became confirmed/retried: err=%v heads=%d puts=%d", mode, err, engine.headWrites, f.canonical.retainedPuts)
				}
				run := publicationRun(t, f)
				if run.DerivedContext == nil || len(run.DerivedContext.Inventory) != 1 || (run.DerivedContext.Head != nil) != (mode == "persist") {
					t.Fatal("scripted persistence did not retain exact cleanup inventory/head outcome")
				}
				if again, err := publishLocalCompaction(t.Context(), g); err == nil || again != nil || engine.headWrites != 1 || f.canonical.retainedPuts != 2 {
					t.Fatal("fresh call retried an ambiguous head")
				}
				if again, err := readPublishedLocalCompaction(t.Context(), g); err == nil || again != nil {
					t.Fatal("durable intended head reminted original confirmed publication")
				}
			})
		})
	}
}

func TestLocalCompactionPublicationReadbackAndCurrentStateDenials(t *testing.T) {
	for _, name := range []string{"changed_run_during_retained_read", "corrupt_manifest", "close_failure", "source_closed"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, g, engine := compactionPublicationFixture(t)
				entered := false
				var barrier func()
				barrier = func() {
					if f.canonical.retainedPuts < 2 {
						f.canonical.beforeOpen = barrier
						return
					}
					entered = true
					switch name {
					case "changed_run_during_retained_read":
						run := publicationRun(t, f)
						// Keep every time <= the authoritative DB clock. A future
						// UpdatedAt would be rejected by generic admission before
						// testing the exact captured-source comparison itself.
						run.CreatedAt = run.CreatedAt.Add(-time.Microsecond)
						if run.Validate() != nil {
							t.Fatal("causal changed Run must remain valid")
						}
						ratedFixturePut(t, f.parent.graph.ratedSQLFixture, "runs", run)
					case "corrupt_manifest":
						run := publicationRun(t, f)
						f.canonical.objects[run.DerivedContext.Inventory[0].Manifest.Key] = []byte("corrupt")
					case "close_failure":
						f.canonical.badClose = true
					case "source_closed":
						if err := f.actual.source.Close(); err != nil {
							t.Fatal(err)
						}
					}
				}
				f.canonical.beforeOpen = barrier
				if next, err := publishLocalCompaction(t.Context(), g); err == nil || next != nil || !entered || engine.headWrites != 0 || g.publication != nil {
					t.Fatalf("%s reached head: err=%v entered=%t heads=%d", name, err, entered, engine.headWrites)
				}
				run := publicationRun(t, f)
				if run.DerivedContext == nil || len(run.DerivedContext.Inventory) != 1 || run.DerivedContext.Head != nil {
					t.Fatal("failed publication hid residue or installed a head")
				}
			})
		})
	}
}

func TestLocalRetainedContextSynchronouslyInheritsSource(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cause := errors.New("owned original Source revoked before retained I/O")
		parent, revoke := context.WithCancelCause(t.Context())
		defer revoke(context.Canceled)
		f := newCompactionSourceFixtureWithPrebuild(t, true, localRuntimeActualPrebuild{sourceParent: parent}, true)
		call, finish, err := f.entry.retainedContext(t.Context(), f.entry.compactionSourceControl())
		if err != nil {
			t.Fatal(err)
		}
		defer finish()
		revoke(cause)
		if err := f.actual.source.Close(); err != nil {
			t.Fatal(err)
		}
		// The original cancellation cause, not callback scheduling, proves the
		// context is synchronously descended from Source after Close returns.
		if !errors.Is(call.Err(), context.Canceled) || !errors.Is(context.Cause(call), cause) {
			t.Fatalf("retained I/O not synchronously Source-rooted: err=%v cause=%v", call.Err(), context.Cause(call))
		}
		before := f.canonical.calls
		if _, err := f.entry.bodies.Write(call, []byte("must not reach storage")); err == nil || f.canonical.calls != before {
			t.Fatal("closed original Source permitted fresh retained-context body I/O")
		}
	})
}

func TestLocalCompactionPublishedReaderRejoinsHeadAfterBlobIO(t *testing.T) {
	for _, changed := range []bool{false, true} {
		name := "unchanged_control"
		if changed {
			name = "head_changed_during_retained_open"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, g, engine := compactionPublicationFixture(t)
				original, err := publishLocalCompaction(t.Context(), g)
				if err != nil || original == nil || g.publication == nil {
					t.Fatalf("causal confirmed publication: %v", err)
				}
				heads, puts := engine.headWrites, f.canonical.retainedPuts
				entered := false
				f.canonical.beforeOpen = func() {
					entered = true
					if changed {
						run := publicationRun(t, f)
						run.DerivedContext.Head = nil
						if run.Validate() != nil {
							t.Fatal("head removal must remain structurally valid")
						}
						ratedFixturePut(t, f.parent.graph.ratedSQLFixture, "runs", run)
					}
				}
				next, err := readPublishedLocalCompaction(t.Context(), g)
				if !entered || engine.headWrites != heads || f.canonical.retainedPuts != puts {
					t.Fatal("committed-head read missed actual I/O barrier or mutated storage")
				}
				if changed {
					if err == nil || next != nil {
						t.Fatalf("stale committed-head consumer returned next request: err=%v next=%t", err, next != nil)
					}
				} else if err != nil || next == nil || next.Digest() != original.Digest() {
					t.Fatalf("unchanged committed-head consumer: %v", err)
				}
			})
		})
	}
}
