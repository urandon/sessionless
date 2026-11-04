package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// Only the existing scripted graph transaction is wrapped. Blocking before
// its actual restore reproduces database/sql's cancellation rollback window;
// this is not native SQL isolation or a substitute for the connected tests.
type canonicalObserverSQL struct {
	*ratedSQLFixture
	graph           *managedAdmissionGraphFixture
	rollbackEntered chan struct{}
	releaseRollback chan struct{}
	rollbackResult  chan error
	release         sync.Once
}

func (f *canonicalObserverSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *canonicalObserverSQL) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	inner, err := f.graph.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return canonicalObserverTx{Tx: inner, fixture: f}, nil
}

type canonicalObserverTx struct {
	driver.Tx
	fixture *canonicalObserverSQL
}

func (tx canonicalObserverTx) Rollback() error {
	close(tx.fixture.rollbackEntered)
	<-tx.fixture.releaseRollback
	err := tx.Tx.Rollback()
	tx.fixture.rollbackResult <- err
	return err
}

func newCanonicalObserverSeed(t *testing.T) (*LocalCanonicalLoopSeed, *canonicalObserverSQL) {
	t.Helper()
	rated := &ratedSQLFixture{rows: map[string][]driver.Value{}, writes: 5}
	graph := &managedAdmissionGraphFixture{canonicalBudgetFixture: &canonicalBudgetFixture{ratedSQLFixture: rated}}
	base := &localCanonicalJoinSQL{localCancellationSQL: &localCancellationSQL{ownedFinalScript: &ownedFinalScript{managedAdmissionGraphFixture: graph}}}
	parent := &ownedAttemptEffectParentSQLFixture{inserts: 2, burnUpdates: 3}
	alloc := &allocationSQLFixture{writes: 4}
	peer := &canonicalSessionSQL{base: base, parent: parent, alloc: alloc}
	f := &canonicalObserverSQL{ratedSQLFixture: rated, graph: graph, rollbackEntered: make(chan struct{}), releaseRollback: make(chan struct{}), rollbackResult: make(chan error, 1)}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		f.release.Do(func() { close(f.releaseRollback) })
		if err := db.Close(); err != nil {
			t.Errorf("observer fixture database close: %v", err)
		}
	})
	seed := &LocalCanonicalSessionSeed{DB: db, peer: peer, owner: t}
	return &LocalCanonicalLoopSeed{LocalCanonicalSessionSeed: seed, loop: &canonicalLoopSQL{canonicalSessionSQL: peer, operations: &loopbackOperationSQL{localProxySQL: &localProxySQL{}}}}, f
}

func TestLocalSessionFixtureObserversWaitForRollback(t *testing.T) {
	for _, method := range []string{"Stats", "ModelEffectsAbsent"} {
		t.Run(method, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				seed, f := newCanonicalObserverSeed(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				tx, err := seed.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal("begin actual scripted graph transaction:", err)
				}
				// This speculative row is replaced by managedGraphTx.restore, not
				// by an observer-specific lock or synthetic snapshot callback.
				f.rows["leases"] = []driver.Value{"speculative lease"}
				cancel()
				<-f.rollbackEntered
				var completed atomic.Bool
				done := make(chan struct{})
				var stats LocalCanonicalSessionStats
				var absent bool
				go func() {
					defer close(done)
					if method == "Stats" {
						stats = seed.Stats()
					} else {
						absent = seed.ModelEffectsAbsent()
					}
					completed.Store(true)
				}()
				// Both the rollback and the observer's database/sql pool wait
				// are channel-blocked. Quiescence, not select scheduling, proves
				// that an unguarded observer would already have completed.
				synctest.Wait()
				if completed.Load() {
					t.Errorf("%s completed before the actual rollback restore", method)
				}
				f.release.Do(func() { close(f.releaseRollback) })
				if err := <-f.rollbackResult; err != nil {
					t.Errorf("actual graph rollback: %v", err)
				}
				<-done
				if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
					t.Errorf("cancelled transaction terminal result = %v, want ErrTxDone", err)
				}
				if method == "Stats" {
					want := LocalCanonicalSessionStats{Parents: 2, Burns: 3, Allocations: 4, Writes: 5}
					if stats != want {
						t.Errorf("restored Stats = %+v, want %+v", stats, want)
					}
				} else if !absent {
					t.Error("restored ModelEffectsAbsent = false, want true")
				}
			})
		})
	}
}

func TestLocalSessionFixtureObserverConnectionErrors(t *testing.T) {
	t.Run("invalid_pool", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			seed, _ := newCanonicalObserverSeed(t)
			seed.DB.SetMaxOpenConns(2)
			conn, err := canonicalFixtureObserverConnection(t.Context(), seed.DB)
			if err == nil || conn != nil {
				t.Fatalf("non-sole pool observer = (%v, %v), want explicit error and no connection", conn, err)
			}
			conn, err = canonicalFixtureObserverConnection(t.Context(), nil)
			if err == nil || conn != nil {
				t.Fatalf("nil pool observer = (%v, %v), want explicit error and no connection", conn, err)
			}
		})
	})
	t.Run("closed_database", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			seed, _ := newCanonicalObserverSeed(t)
			if err := seed.DB.Close(); err != nil {
				t.Fatal("close database:", err)
			}
			conn, err := canonicalFixtureObserverConnection(t.Context(), seed.DB)
			if err == nil || conn != nil {
				t.Fatalf("closed database observer = (%v, %v), want explicit error and no connection", conn, err)
			}
		})
	})
	t.Run("acquisition_deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			seed, _ := newCanonicalObserverSeed(t)
			held, err := seed.DB.Conn(t.Context())
			if err != nil {
				t.Fatal("occupy sole connection:", err)
			}
			t.Cleanup(func() {
				if err := held.Close(); err != nil {
					t.Error("held connection close:", err)
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			conn, err := canonicalFixtureObserverConnection(ctx, seed.DB)
			if !errors.Is(err, context.DeadlineExceeded) || conn != nil {
				t.Fatalf("occupied observer = (%v, %v), want DeadlineExceeded and no connection", conn, err)
			}
		})
	})
}
