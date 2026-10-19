package ydbstore_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// The original Source/factory/Store/current reader is real. SQL, full source
// verification and external funding are SCRIPTED, not runtime qualification.
// No lease/effect/body/model/terminal operation is invoked by these tests.
func newWorkerPreflightSession(t *testing.T, caller context.Context, cancelAtSources bool) (*providerloopsession.LocalV1, *ydbstore.LocalSessionConnectedSeed, *connectedOperationBlobs, *workerPreflightQueryHook) {
	t.Helper()
	seed := ydbstore.NewLocalSessionConnectedSeed(t, "http://127.0.0.1:18899/v1")
	seed.DeclareWorkerPreflightWebOrigin(t)
	blobs := newConnectedOperationBlobs(seed.Scope)
	var hook *workerPreflightQueryHook
	if cancelAtSources {
		held, ok := caller.(*heldTerminalCaller)
		if !ok {
			t.Fatal("test cancellation must use actual raw factory caller")
		}
		hook = &workerPreflightQueryHook{driver: seed.DB.Driver(), caller: held}
		seed.DB = sql.OpenDB(hook)
		seed.DB.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = seed.DB.Close() })
	}
	session, err := providerloopsession.NewLocalV1(caller, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB,
		StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	})
	return session, seed, blobs, hook
}

func assertPreflightNoEffects(t *testing.T, seed *ydbstore.LocalSessionConnectedSeed, blobs *connectedOperationBlobs, initial ydbstore.LocalWorkerPreflightReads) {
	t.Helper()
	if seed.Stats() != (ydbstore.LocalSessionConnectedStats{}) {
		t.Fatalf("readonly preflight mutated parent/burn/allocation/operation: %+v", seed.Stats())
	}
	after := seed.WorkerPreflightReads()
	if after.Writes != initial.Writes || after.Lease != initial.Lease {
		t.Fatalf("readonly preflight wrote or joined a fabricated lease: before=%+v after=%+v", initial, after)
	}
	if puts, opens, closes, deletes := blobs.counts(); puts != 0 || opens != 0 || closes != 0 || deletes != 0 {
		t.Fatalf("readonly preflight touched body storage: %d/%d/%d/%d", puts, opens, closes, deletes)
	}
}

func TestLocalSessionWorkerPreflightOriginalPositiveReadOnly(t *testing.T) {
	session, seed, blobs, _ := newWorkerPreflightSession(t, t.Context(), false)
	expected := seed.WorkerPreflightState(t)
	before := seed.WorkerPreflightReads()
	for i := 0; i < 2; i++ {
		if err := session.PreflightWorkerJob(t.Context(), expected); err != nil {
			t.Fatalf("original readonly preflight %d: %v", i, err)
		}
	}
	after := seed.WorkerPreflightReads()
	if after.Final-before.Final != 2 || after.Principal-before.Principal != 2 || after.Sources-before.Sources != 2 || after.Verifier-before.Verifier != 2 {
		t.Fatalf("repeat did not perform actual current joins: before=%+v after=%+v", before, after)
	}
	if !reflect.DeepEqual(expected, seed.WorkerPreflightState(t)) {
		t.Fatal("read-only preflight changed canonical rows")
	}
	assertPreflightNoEffects(t, seed, blobs, before)
}

func TestLocalSessionWorkerPreflightFactoryCancelAtCurrentRead(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		name := "live_control"
		if cancel {
			name = "raw_factory_cancel_callback_held"
		}
		t.Run(name, func(t *testing.T) {
			caller := newHeldTerminalCaller(t.Context())
			t.Cleanup(caller.release)
			session, seed, blobs, hook := newWorkerPreflightSession(t, caller, true)
			hook.cancel = cancel
			before := seed.WorkerPreflightReads()
			methodCtx := t.Context() // remains live; only ORIGINAL factory cancels
			err := session.PreflightWorkerJob(methodCtx, seed.WorkerPreflightState(t))
			if hook.sources != 1 || methodCtx.Err() != nil || (caller.Err() != nil) != cancel {
				t.Fatal("causal current-query cancellation did not fire at its intended boundary")
			}
			assertPreflightNoEffects(t, seed, blobs, before)
			if _, sourceErr := seed.Source.Configuration(); sourceErr != nil {
				t.Fatal("test canceled borrowed Source instead of raw factory caller")
			}
			if cancel && err == nil {
				t.Fatal("preflight returned readiness after original factory canceled during SQL, before callback delivery")
			}
			if !cancel && err != nil {
				t.Fatal("uncanceled current-read positive", err)
			}
		})
	}
}

func TestLocalSessionWorkerPreflightAuthorityTimestamps(t *testing.T) {
	for _, kind := range []string{"run", "attempt", "reservation"} {
		for _, future := range []bool{false, true} {
			name := kind + "/current_control"
			if future {
				name = kind + "/future_updated_at"
			}
			t.Run(name, func(t *testing.T) {
				session, seed, blobs, _ := newWorkerPreflightSession(t, t.Context(), false)
				at := seed.FreezeWorkerPreflightDBClock()
				updated := at
				if future {
					updated = updated.Add(time.Microsecond)
				}
				seed.SetWorkerPreflightTimestamp(t, kind, updated)
				expected := seed.WorkerPreflightState(t) // SAME valid DATA as actual row
				before := seed.WorkerPreflightReads()
				err := session.PreflightWorkerJob(t.Context(), expected)
				after := seed.WorkerPreflightReads()
				if after.Final-before.Final != 1 || after.Sources-before.Sources != 1 || after.Verifier-before.Verifier != 1 {
					t.Fatalf("timestamp mutant was masked before original Final/current reads: before=%+v after=%+v", before, after)
				}
				assertPreflightNoEffects(t, seed, blobs, before)
				if future && err == nil {
					t.Fatal("future UpdatedAt passed actual current DB authority sample", kind)
				}
				if !future && err != nil {
					t.Fatal("current timestamp positive", kind, err)
				}
			})
		}
	}
}

// Wrap only actual query return timing. Every SQL row/transaction and all
// authority decisions still come from the unchanged strict original driver.
type workerPreflightQueryHook struct {
	driver  driver.Driver
	caller  *heldTerminalCaller
	cancel  bool
	sources int
}

func (h *workerPreflightQueryHook) Driver() driver.Driver { return h.driver }
func (h *workerPreflightQueryHook) Connect(context.Context) (driver.Conn, error) {
	c, err := h.driver.Open("")
	if err != nil {
		return nil, err
	}
	return workerPreflightHookConn{Conn: c, hook: h}, nil
}

type workerPreflightHookConn struct {
	driver.Conn
	hook *workerPreflightQueryHook
}

func (c workerPreflightHookConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
}
func (c workerPreflightHookConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c workerPreflightHookConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err == nil && strings.Contains(query, "FROM owned_agent_profile_current_v1 AS p") && strings.Contains(query, "INNER JOIN attempt_spending_authorization_current_v1 AS s3") {
		c.hook.sources++
		if c.hook.cancel {
			c.hook.caller.cancel()
		}
	}
	return rows, err
}
