package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// SCRIPTED SQL commit/readback faults, not real YDB isolation. All authority
// queries delegate unchanged to the original Store fixture. This wrapper only
// models the dedicated terminal Run UPDATE and its transaction outcome.
type terminalPublicationSQL struct {
	base                       *compactionPublicationSQL
	mu                         sync.Mutex
	writes, commits, readbacks int
	lost                       string
	corruptPostimage           bool
}
type terminalPublicationConnector struct{ f *terminalPublicationSQL }
type terminalPublicationConn struct {
	driver.Conn
	f *terminalPublicationSQL
}
type terminalPublicationTx struct {
	driver.Tx
	f            *terminalPublicationSQL
	writesBefore int
	done         bool
}

func (c terminalPublicationConnector) Driver() driver.Driver {
	return compactionPublicationConnector{c.f.base}.Driver()
}
func (c terminalPublicationConnector) Connect(ctx context.Context) (driver.Conn, error) {
	cx, err := (compactionPublicationConnector{c.f.base}).Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &terminalPublicationConn{Conn: cx, f: c.f}, nil
}
func (c *terminalPublicationConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	c.f.mu.Lock()
	before := c.f.writes
	c.f.mu.Unlock()
	return &terminalPublicationTx{Tx: tx, f: c.f, writesBefore: before}, nil
}
func (c *terminalPublicationConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	c.f.mu.Lock()
	if statement == boundedRunPayloadSQL && c.f.commits > 0 {
		c.f.readbacks++
		if c.f.corruptPostimage {
			c.f.corruptPostimage = false
			var run domain.Run
			if json.Unmarshal([]byte(c.f.base.f.parent.graph.rows["runs"][0].(string)), &run) != nil {
				c.f.mu.Unlock()
				return nil, errors.New("invalid fixture postimage")
			}
			run.CreatedAt = run.CreatedAt.Add(-1)
			raw, _ := json.Marshal(run)
			c.f.base.f.parent.graph.rows["runs"] = []driver.Value{string(raw)}
		}
	}
	c.f.mu.Unlock()
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, statement, args)
}
func (c *terminalPublicationConn) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if statement != localRetainedRunUpdateSQL {
		return c.Conn.(driver.ExecerContext).ExecContext(ctx, statement, args)
	}
	var old, next domain.Run
	if len(args) != 3 || json.Unmarshal([]byte(c.f.base.f.parent.graph.rows["runs"][0].(string)), &old) != nil || json.Unmarshal([]byte(args[0].Value.(string)), &next) != nil {
		return nil, errors.New("scripted terminal exact Run update")
	}
	if old.TerminalCustody == nil && next.TerminalCustody == nil {
		// The SAME existing SQL also carries retained compaction inventory/head
		// writes. Their original driver owns all validation and commit semantics.
		return c.Conn.(driver.ExecerContext).ExecContext(ctx, statement, args)
	}
	if next.TerminalCustody == nil || next.TerminalCustody.Correspondence != nil || next.TerminalCustody.LocalEvidence != nil || next.Validate() != nil || len(args[0].Value.(string)) > 32<<10 || args[1].Value != string(old.TenantID) || args[2].Value != string(old.ID) {
		return nil, errors.New("scripted terminal exact bounded plan update")
	}
	base := next.Clone()
	base.TerminalCustody = old.TerminalCustody
	if old.TerminalCustody != nil || !reflect.DeepEqual(base, old) {
		return nil, errors.New("terminal update changed existing Run")
	}
	c.f.mu.Lock()
	c.f.writes++
	c.f.mu.Unlock()
	c.f.base.f.parent.graph.rows["runs"] = []driver.Value{args[0].Value}
	return driver.RowsAffected(1), nil
}
func (tx *terminalPublicationTx) Commit() error {
	tx.f.mu.Lock()
	mutated := tx.f.writes > tx.writesBefore
	lost := tx.f.lost
	if mutated {
		tx.f.lost = ""
	}
	tx.f.mu.Unlock()
	if mutated && lost == "rollback" {
		_ = tx.Rollback()
		return retry.RetryableError(errors.New("terminal ACK lost without persistence"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	tx.done = true
	if mutated {
		tx.f.mu.Lock()
		tx.f.commits++
		tx.f.mu.Unlock()
	}
	if mutated && lost == "persist" {
		return retry.RetryableError(errors.New("terminal ACK lost after persistence"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	return nil
}
func (tx *terminalPublicationTx) Rollback() error {
	if tx.done {
		return nil
	}
	tx.done = true
	return tx.Tx.Rollback()
}

func installTerminalPublicationSQL(t *testing.T, engine *compactionPublicationSQL) (*sql.DB, *terminalPublicationSQL) {
	t.Helper()
	f := &terminalPublicationSQL{base: engine}
	db := sql.OpenDB(terminalPublicationConnector{f})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db, f
}

// TEST ONLY: dependencies installed BEFORE actual NewLocalV1 construction. No
// Store, entry, keeper, plan or private owner is injected into the runtime.
type LocalTerminalPublicationSessionConnectedSeed struct {
	*LocalSessionConnectedSeed
	publication *terminalPublicationSQL
}

func NewLocalTerminalPublicationSessionConnectedSeed(t *testing.T, endpoint string, blobs ports.BlobStore, limits ...LocalCompactionConnectedLimits) *LocalTerminalPublicationSessionConnectedSeed {
	t.Helper()
	s := NewLocalCompactionSessionConnectedSeed(t, endpoint, blobs, limits...)
	db, f := installTerminalPublicationSQL(t, s.compaction)
	s.DB = db
	return &LocalTerminalPublicationSessionConnectedSeed{LocalSessionConnectedSeed: s, publication: f}
}
func (s *LocalTerminalPublicationSessionConnectedSeed) TerminalPublicationRun(t *testing.T) domain.Run {
	t.Helper()
	s.publication.mu.Lock()
	defer s.publication.mu.Unlock()
	var run domain.Run
	if json.Unmarshal([]byte(s.parent.graph.rows["runs"][0].(string)), &run) != nil || run.Validate() != nil {
		t.Fatal("invalid terminal publication Run DATA")
	}
	return run.Clone()
}
func (s *LocalTerminalPublicationSessionConnectedSeed) TerminalPublicationCounts() (writes, commits, readbacks int) {
	s.publication.mu.Lock()
	defer s.publication.mu.Unlock()
	return s.publication.writes, s.publication.commits, s.publication.readbacks
}

func terminalPublicationFixture(t *testing.T) (*compactionSourceFixture, *terminalPublicationSQL, *LocalPreparedOutputV1) {
	t.Helper()
	f, engine, h := terminalOutputFixture(t)
	db, sql := installTerminalPublicationSQL(t, engine)
	f.store.db = db
	p, err := h.PrepareOutput(t.Context())
	if err != nil {
		t.Fatal("original PrepareOutput", err)
	}
	t.Cleanup(p.Clear)
	return f, sql, p
}

func TestLocalTerminalPublicationOriginalPlanBeforeObjects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, sql, p := terminalPublicationFixture(t)
		before := publicationRun(t, f)
		puts := f.canonical.calls
		f.canonical.beforeOrdinary = func(ctx context.Context) {
			if ctx.Err() != nil || sql.commits != 1 || sql.readbacks == 0 {
				t.Error("capsule PUT before confirmed commit and independent readback", sql.commits, sql.readbacks)
			}
			run := publicationRun(t, f)
			if run.TerminalCustody == nil || run.TerminalCustody.Plan != p.plan {
				t.Error("missing exact persisted original plan")
			}
			if _, ok := f.canonical.objects[p.drafts[0].Payload.Key]; ok {
				t.Error("event published before capsule")
			}
			f.canonical.beforeOrdinary = func(context.Context) {
				if _, ok := f.canonical.objects[p.plan.Capsule.Key]; !ok {
					t.Error("capsule not first")
				}
			}
		}
		if err := p.Publish(t.Context()); err != nil {
			t.Fatal("original publication", err, sql.writes, sql.commits, sql.readbacks)
		}
		after := publicationRun(t, f)
		after.TerminalCustody = nil
		if !reflect.DeepEqual(before, after) || !p.published || sql.writes != 1 || sql.commits != 1 || sql.readbacks < 2 || f.canonical.calls <= puts {
			t.Fatal("publication postimage/state", sql.writes, sql.commits, sql.readbacks, p.published)
		}
		if !localRetainedBytes(p.plan.Capsule, f.canonical.objects[p.plan.Capsule.Key]) || !localRetainedBytes(p.drafts[0].Payload, f.canonical.objects[p.drafts[0].Payload.Key]) {
			t.Error("immutable exact original output bytes")
		}
		calls := f.canonical.calls
		if p.Publish(t.Context()) == nil || f.canonical.calls != calls {
			t.Error("original publication replay effects")
		}
		copy := *p
		if copy.Publish(t.Context()) == nil || f.canonical.calls != calls {
			t.Error("copied publisher effects")
		}
		if f.queries.maxStatements > 32 || f.queries.statements > 32 {
			t.Error("statement cap", f.queries.maxStatements, f.queries.statements)
		}
	})
}

func TestLocalTerminalPublicationDeniesUnknownRegistration(t *testing.T) {
	for _, mode := range []string{"rollback", "persist"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, sql, p := terminalPublicationFixture(t)
				sql.lost = mode
				calls := f.canonical.calls
				if p.Publish(t.Context()) == nil || p.published || sql.writes != 1 || f.canonical.calls != calls {
					t.Fatal("unknown registration performed PUT or retry", mode, sql.writes, f.canonical.calls-calls)
				}
				run := publicationRun(t, f)
				if (run.TerminalCustody != nil) != (mode == "persist") {
					t.Error("scripted lost ACK persistence", mode)
				}
				if p.Publish(t.Context()) == nil || sql.writes != 1 || f.canonical.calls != calls {
					t.Error("uncertain registration replay")
				}
			})
		})
	}
}

func TestLocalTerminalPublicationDeniesStaleAndForeignPostimages(t *testing.T) {
	for _, mode := range []string{"stale_run", "foreign_plan", "postcommit_changed_run"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, sql, p := terminalPublicationFixture(t)
				run := publicationRun(t, f)
				switch mode {
				case "stale_run":
					run.CreatedAt = run.CreatedAt.Add(-1)
				case "foreign_plan":
					plan := p.plan
					plan.FrontierDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
					run.TerminalCustody = &domain.RunTerminalCustodyV1{Version: 1, Plan: plan}
				case "postcommit_changed_run":
					sql.corruptPostimage = true
				}
				if run.Validate() != nil {
					t.Fatal("causal valid current Run required")
				}
				raw, _ := json.Marshal(run)
				f.parent.graph.rows["runs"] = []driver.Value{string(raw)}
				calls := f.canonical.calls
				if p.Publish(t.Context()) == nil || p.published || f.canonical.calls != calls {
					t.Fatal("stale/foreign readback reached PUT", mode, sql.writes)
				}
				want := 0
				if mode == "postcommit_changed_run" {
					want = 1
				}
				if sql.writes != want {
					t.Error("registration writes", sql.writes, want)
				}
			})
		})
	}
}

func TestLocalTerminalPublicationStorageFailuresBurnOwner(t *testing.T) {
	for _, mode := range []string{"capsule_put_ambiguous", "capsule_readback_corrupt", "event_readback_corrupt"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, sql, p := terminalPublicationFixture(t)
				switch mode {
				case "capsule_put_ambiguous":
					f.canonical.ambiguousOrdinary = true
				case "capsule_readback_corrupt":
					f.canonical.beforeOrdinary = func(context.Context) {
						f.canonical.beforeOpen = func() { f.canonical.objects[p.plan.Capsule.Key] = []byte("corrupt") }
					}
				case "event_readback_corrupt":
					f.canonical.beforeOrdinary = func(context.Context) {
						f.canonical.beforeOrdinary = func(context.Context) {
							f.canonical.beforeOpen = func() { f.canonical.objects[p.drafts[0].Payload.Key] = []byte("corrupt") }
						}
					}
				}
				if p.Publish(t.Context()) == nil || p.published || sql.writes != 1 || publicationRun(t, f).TerminalCustody == nil {
					t.Fatal("failed storage became publication or lost residue", mode)
				}
				calls := f.canonical.calls
				if p.Publish(t.Context()) == nil || f.canonical.calls != calls || sql.writes != 1 {
					t.Error("failed original owner retried storage")
				}
				if mode != "event_readback_corrupt" {
					if _, ok := f.canonical.objects[p.drafts[0].Payload.Key]; ok {
						t.Error("event PUT followed failed capsule")
					}
				}
			})
		})
	}
}

func TestLocalTerminalPublicationDirectWriterCancellationAndJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, sql, p := terminalPublicationFixture(t)
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		call, cancel := context.WithCancel(t.Context())
		// A deliberately uncooperative owned PUT lets the test distinguish actual
		// completion from cancellation or a free session gate. Cleanup always joins.
		t.Cleanup(func() {
			cancel()
			unblock()
			synctest.Wait()
			select {
			case <-done:
			default:
				t.Error("direct publisher did not join cleanup")
			}
		})
		f.canonical.beforeOrdinary = func(ctx context.Context) {
			close(entered)
			<-release
			if ctx.Err() == nil {
				t.Error("Clear did not revoke actual original PUT context")
			}
		}
		go func() { done <- p.Publish(call) }()
		synctest.Wait()
		select {
		case <-entered:
		default:
			t.Fatal("original publication did not reach owned capsule PUT", sql.writes, sql.commits, sql.readbacks)
		}
		copy := *p
		if copy.StopAndJoin(t.Context()) == nil {
			t.Error("copied owner joined original writer")
		}
		p.Clear()
		if !localRetainedBytes(p.plan.Capsule, p.capsule) {
			t.Error("Clear erased still-live writer buffers")
		}
		join, finish := context.WithTimeout(t.Context(), time.Second)
		defer finish()
		if p.StopAndJoin(join) == nil {
			t.Error("paused writer was reported joined")
		}
		if !localRetainedBytes(p.plan.Capsule, p.capsule) {
			t.Error("join timeout erased live buffers")
		}
		unblock()
		synctest.Wait()
		select {
		case err := <-done:
			if err == nil {
				t.Error("revoked actual writer published success")
			}
			// Preserve the completed result for registered cleanup's join check.
			done <- err
		default:
			t.Fatal("released writer did not finish")
		}
		if p.StopAndJoin(t.Context()) != nil || p.capsule != nil || p.canonical != nil {
			t.Error("completed revoked owner did not release owned buffers")
		}
		if p.Publish(t.Context()) == nil || sql.writes != 1 {
			t.Error("revoked original owner restarted publication")
		}
		if _, ok := f.canonical.objects[p.plan.Capsule.Key]; ok {
			t.Error("cancelled capsule PUT persisted")
		}
	})
}

func TestLocalTerminalPublicationEmptyJoinRevokes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, sql, p := terminalPublicationFixture(t)
		calls := f.canonical.calls
		if p.StopAndJoin(t.Context()) != nil {
			t.Fatal("unused original owner join")
		}
		if p.Publish(t.Context()) == nil || f.canonical.calls != calls || sql.writes != 0 {
			t.Error("empty join permitted a later publisher")
		}
	})
}
