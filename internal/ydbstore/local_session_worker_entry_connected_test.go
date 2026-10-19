package ydbstore_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http/httptest"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Actual original Source/factory/bootstrap/Store/entry. SQL, funding and the
// complete source verifier remain scripted. No Manager registration, native
// YDB, model HTTP, terminal evidence or full #218 is proved by this foundation.
func newWorkerEntrySession(t *testing.T, endpoint string, commitFault ...string) (*providerloopsession.LocalV1, *ydbstore.LocalSessionConnectedSeed, *connectedOperationBlobs, ports.WorkerStateStore, ports.AttemptEffectStoreV1) {
	t.Helper()
	seed := ydbstore.NewLocalSessionConnectedSeed(t, endpoint)
	if len(commitFault) > 1 {
		t.Fatal("one original first-commit fault only")
	}
	if len(commitFault) == 1 {
		// Storage fault only, installed BEFORE the actual factory. Every SQL
		// query and authority decision still goes to the original strict driver.
		fault := &workerEntryCommitFault{driver: seed.DB.Driver(), mode: commitFault[0]}
		seed.DB = sql.OpenDB(fault)
		seed.DB.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = seed.DB.Close() })
	}
	blobs := newConnectedOperationBlobs(seed.Scope)
	before := seed.Stats()
	session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options,
		Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	state, err := session.WorkerState()
	if err != nil {
		t.Fatal(err)
	}
	effects, ok := state.(ports.AttemptEffectStoreV1)
	if !ok {
		t.Fatal("original facade lost effect port")
	}
	if _, fakeV1 := state.(serverlessharness.ExecutionPreparerV1); fakeV1 {
		t.Fatal("state facade masquerades as a V1 execution preparer")
	}
	if again, err := session.WorkerState(); err != nil || again != state || seed.Stats() != before {
		t.Fatal("factory facade reconstruction caused effects or changed Store owner")
	}
	return session, seed, blobs, state, effects
}

func assertWorkerEntryCounts(t *testing.T, seed *ydbstore.LocalSessionConnectedSeed, entered bool) {
	t.Helper()
	s := seed.Stats()
	want := 0
	if entered {
		want = 1
	}
	if s.Parent != 1 || s.Burn != want || s.Allocation != want || s.Proxy != 0 || s.Operation != 0 || s.Call != 0 || s.Observation != 0 {
		t.Fatalf("original reserve/entry/effect counters: %+v", s)
	}
}

func assertWorkerEntryWritable(t *testing.T, session *providerloopsession.LocalV1, seed *ydbstore.LocalSessionConnectedSeed, blobs *connectedOperationBlobs) {
	t.Helper()
	if _, err := session.WriteModelRequest(t.Context(), seed.Profile.Limits, sessionRequest()); err != nil {
		t.Fatalf("rejected loser damaged actual winner body owner: %v", err)
	}
	puts, opens, closes, deletes := blobs.counts()
	if puts != 1 || opens != closes || deletes != 0 {
		t.Fatalf("actual winner physical body control: %d/%d/%d/%d", puts, opens, closes, deletes)
	}
	assertWorkerEntryCounts(t, seed, true)
}

func TestLocalSessionWorkerEntrySingleReserveAndMutableLosers(t *testing.T) {
	session, seed, blobs, state, effects := newWorkerEntrySession(t, "http://127.0.0.1:18899/v1")
	if requested, err := state.CancellationRequested(t.Context(), seed.Request.TenantID, seed.Request.RunID); err != nil || requested {
		t.Fatalf("SAME Store state delegation: requested=%t err=%v", requested, err)
	}
	fresh, err := effects.ReserveAttemptEffect(t.Context(), seed.Request)
	if err != nil || fresh.Status != ports.AttemptEffectOwnedV1 {
		t.Fatalf("original worker reserve: %v", err)
	}
	assertWorkerEntryCounts(t, seed, false)
	// Status is valid mutable DATA, not freshness. This loser must not consume
	// the bootstrap's private fresh slot or cancel its retained caller.
	changed := fresh
	changed.Status = ports.AttemptEffectReplayedV1
	if err := changed.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := session.EnterWorkerEffect(t.Context(), changed); err == nil {
		t.Fatal("mutable replay status minted entry")
	}
	copyBeforeEntry := *session
	if _, err := copyBeforeEntry.Start(t.Context(), seed.Request); err == nil {
		t.Fatal("direct Start entered the worker-reserved invocation")
	}
	// Copying the private facade value must not create a second effect owner.
	copyValue := reflect.New(reflect.TypeOf(state).Elem())
	copyValue.Elem().Set(reflect.ValueOf(state).Elem())
	if _, err := copyValue.Interface().(ports.AttemptEffectStoreV1).ReserveAttemptEffect(t.Context(), seed.Request); err == nil {
		t.Fatal("copied facade minted reservation")
	}
	observed, err := effects.ReserveAttemptEffect(t.Context(), seed.Request)
	if err != nil || observed.Status != ports.AttemptEffectReconcileOnlyV1 || observed.Grant != nil {
		t.Fatalf("repeated reserve not observation-only: %v", err)
	}
	if _, err := session.EnterWorkerEffect(t.Context(), observed); err == nil {
		t.Fatal("historical observation became a fresh entry")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := session.EnterWorkerEffect(canceled, fresh); err == nil {
		t.Fatal("canceled handoff entered original slot")
	}
	parent, err := session.EnterWorkerEffect(t.Context(), fresh)
	if err != nil || parent.Reservation.RunID != seed.Request.RunID {
		t.Fatalf("original confirmed-fresh handoff: %v", err)
	}
	copySession := *session
	if _, err := copySession.EnterWorkerEffect(t.Context(), fresh); err == nil {
		t.Fatal("copied wrapper reentered original slot")
	}
	assertWorkerEntryWritable(t, session, seed, blobs)
}

func TestLocalSessionWorkerEntryForeignResultDoesNotCloseEitherWinner(t *testing.T) {
	a, as, ab, _, ae := newWorkerEntrySession(t, "http://127.0.0.1:18899/v1")
	b, bs, bb, _, be := newWorkerEntrySession(t, "http://127.0.0.1:18898/v1")
	ar, err := ae.ReserveAttemptEffect(t.Context(), as.Request)
	if err != nil {
		t.Fatal(err)
	}
	br, err := be.ReserveAttemptEffect(t.Context(), bs.Request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.EnterWorkerEffect(t.Context(), ar); err == nil {
		t.Fatal("foreign original result entered another factory")
	}
	if _, err := a.EnterWorkerEffect(t.Context(), ar); err != nil {
		t.Fatal(err)
	}
	if _, err := b.EnterWorkerEffect(t.Context(), br); err != nil {
		t.Fatal(err)
	}
	if _, err := b.EnterWorkerEffect(t.Context(), ar); err == nil {
		t.Fatal("foreign loser replaced already entered owner")
	}
	assertWorkerEntryWritable(t, a, as, ab)
	assertWorkerEntryWritable(t, b, bs, bb)
}

func TestLocalSessionWorkerEntryConcurrentCopiesOneWinner(t *testing.T) {
	session, seed, blobs, _, effects := newWorkerEntrySession(t, "http://127.0.0.1:18899/v1")
	fresh, err := effects.ReserveAttemptEffect(t.Context(), seed.Request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	start := make(chan struct{})
	done := make(chan error, 2)
	joined := make(chan struct{})
	// Cleanup is armed before either goroutine can own the entry. A failure
	// still releases both waiters and joins the actual calls under one bound.
	t.Cleanup(func() {
		cancel()
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-joined:
		case <-timer.C:
			select {
			case <-joined:
			default:
				t.Error("worker handoff calls did not join")
			}
		}
	})
	copySession := *session
	for _, owner := range []*providerloopsession.LocalV1{session, &copySession} {
		go func() {
			<-start
			_, err := owner.EnterWorkerEffect(ctx, fresh)
			done <- err
		}()
	}
	close(start)
	results := make(chan [2]error, 1)
	go func() { results <- [2]error{<-done, <-done}; close(joined) }()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	var outcome [2]error
	select {
	case outcome = <-results:
	case <-timer.C:
		select {
		case outcome = <-results:
		default:
			t.Fatal("original concurrent handoff did not complete")
		}
	}
	if (outcome[0] == nil) == (outcome[1] == nil) {
		t.Fatal("concurrent original wrappers did not have exactly one winner")
	}
	assertWorkerEntryWritable(t, session, seed, blobs)
}

func TestLocalSessionWorkerEntryOriginalCallerLifetime(t *testing.T) {
	for _, mode := range []string{"canceled_before_handoff", "deadline_before_handoff", "cancel_after_handoff", "deadline_after_handoff"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				session, seed, blobs, _, effects := newWorkerEntrySession(t, "http://127.0.0.1:18899/v1")
				deadline := time.Now().Add(3 * time.Second)
				original, cancel := context.WithDeadline(t.Context(), deadline)
				t.Cleanup(cancel)
				fresh, err := effects.ReserveAttemptEffect(original, seed.Request)
				if err != nil {
					t.Fatal(err)
				}
				beforeHandoff := mode == "canceled_before_handoff" || mode == "deadline_before_handoff"
				if beforeHandoff {
					if mode == "canceled_before_handoff" {
						cancel()
					} else {
						time.Sleep(time.Until(deadline)) // Actual deadline expiry in the fake-time bubble.
					}
				}
				_, err = session.EnterWorkerEffect(t.Context(), fresh) // Different, live handoff caller.
				if beforeHandoff {
					if err == nil {
						t.Fatal("handoff renewed dead original reserve caller")
					}
					assertWorkerEntryCounts(t, seed, false)
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if mode == "cancel_after_handoff" {
					cancel()
				} else {
					time.Sleep(time.Until(deadline))
				}
				if _, err := session.WriteModelRequest(t.Context(), seed.Profile.Limits, sessionRequest()); err == nil {
					t.Fatal("installed entry outlived original reserve caller")
				}
				if puts, opens, _, deletes := blobs.counts(); puts != 0 || opens != 0 || deletes != 0 {
					t.Fatal("dead original invocation touched physical body store")
				}
				assertWorkerEntryCounts(t, seed, true)
			})
		})
	}
}

func TestLocalSessionWorkerEntryDirectStartRemainsExclusive(t *testing.T) {
	session, seed, blobs, _, effects := newWorkerEntrySession(t, "http://127.0.0.1:18899/v1")
	if _, err := session.Start(t.Context(), seed.Request); err != nil {
		t.Fatal(err)
	}
	before := seed.Stats()
	if _, err := effects.ReserveAttemptEffect(t.Context(), seed.Request); err == nil {
		t.Fatal("worker facade reserved after direct entry")
	}
	if _, err := session.EnterWorkerEffect(t.Context(), ports.ReserveAttemptEffectResultV1{}); err == nil || seed.Stats() != before {
		t.Fatal("worker route replaced direct owner")
	}
	assertWorkerEntryWritable(t, session, seed, blobs)
	if _, err := session.WorkerState(); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := session.WorkerState(); err == nil {
		t.Fatal("closed factory returned an available worker facade")
	}
	if _, err := seed.Source.Configuration(); err != nil {
		t.Fatal("session closed borrowed Source")
	}
}

func TestLocalSessionWorkerEntrySourceRevokedBeforeHandoff(t *testing.T) {
	session, seed, blobs, _, effects := newWorkerEntrySession(t, "http://127.0.0.1:18899/v1")
	fresh, err := effects.ReserveAttemptEffect(t.Context(), seed.Request)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Source.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := session.EnterWorkerEffect(t.Context(), fresh); err == nil {
		t.Fatal("revoked original Source admitted worker entry")
	}
	assertWorkerEntryCounts(t, seed, false)
	if puts, opens, _, deletes := blobs.counts(); puts != 0 || opens != 0 || deletes != 0 {
		t.Fatal("revoked original Source caused body I/O")
	}
}

func TestLocalSessionWorkerEntryWinningHandoffCancelBeforeCallback(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "live_control"
		if canceled {
			name = "canceled_callback_held"
		}
		t.Run(name, func(t *testing.T) {
			session, seed, blobs, _, effects := newWorkerEntrySession(t, "http://127.0.0.1:18899/v1")
			original, cancelOriginal := context.WithCancel(t.Context())
			t.Cleanup(cancelOriginal)
			fresh, err := effects.ReserveAttemptEffect(original, seed.Request)
			if err != nil {
				t.Fatal(err)
			}
			handoff := newHeldTerminalCaller(t.Context())
			t.Cleanup(handoff.release)
			if _, err := session.EnterWorkerEffect(handoff, fresh); err != nil {
				t.Fatal(err)
			}
			if canceled {
				handoff.cancel()
			}
			if original.Err() != nil {
				t.Fatal("handoff test canceled original reserve caller")
			}
			if !canceled {
				assertWorkerEntryWritable(t, session, seed, blobs)
				return
			}
			_, err = session.WriteModelRequest(t.Context(), seed.Profile.Limits, sessionRequest())
			puts, opens, _, deletes := blobs.counts()
			if err == nil || puts != 0 || opens != 0 || deletes != 0 {
				t.Fatalf("canceled winning handoff admitted body I/O before callback: err=%v puts=%d opens=%d deletes=%d", err, puts, opens, deletes)
			}
			assertWorkerEntryCounts(t, seed, true)
		})
	}
}

func TestLocalSessionWorkerEntryHandoffCancelBeforeTerminalPhase(t *testing.T) {
	for _, phase := range []string{"plan", "publish", "direct_publish", "cleanup"} {
		for _, canceledCaller := range []string{"live", "handoff", "reserve", "factory"} {
			canceled := canceledCaller != "live"
			name := phase + "/" + canceledCaller
			t.Run(name, func(t *testing.T) {
				peer := &sessionCompactionPeer{modelText: "bounded worker terminal control"}
				server := httptest.NewUnstartedServer(peer)
				server.Config.ReadHeaderTimeout = 10 * time.Second
				server.Config.ReadTimeout = 20 * time.Second
				server.Config.WriteTimeout = 20 * time.Second
				server.Config.MaxHeaderBytes = 16 << 10
				t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
				server.Start()
				blobs := &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}}
				seed := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, server.URL+"/v1", blobs)
				blobs.seed = seed
				blobs.sql = seed.InstallTerminalCleanupSQL(t)
				factory := newHeldTerminalCaller(t.Context())
				reserve := newHeldTerminalCaller(t.Context())
				t.Cleanup(factory.release)
				t.Cleanup(reserve.release)
				session, err := providerloopsession.NewLocalV1(factory, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options,
					Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := session.Close(); err != nil {
						t.Error(err)
					}
				})
				state, err := session.WorkerState()
				if err != nil {
					t.Fatal(err)
				}
				fresh, err := state.(ports.AttemptEffectStoreV1).ReserveAttemptEffect(reserve, seed.Request)
				if err != nil {
					t.Fatal(err)
				}
				handoff := newHeldTerminalCaller(t.Context())
				t.Cleanup(handoff.release)
				if _, err := session.EnterWorkerEffect(handoff, fresh); err != nil {
					t.Fatal(err)
				}
				if result, err := session.RunModelStep(t.Context()); err != nil || !result.Model.SendAttempted || result.Compaction != nil {
					t.Fatal("original worker MODEL before terminal control", err)
				}
				h, err := session.PrepareTerminal(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				var direct *ydbstore.LocalPreparedOutputV1
				if phase != "plan" {
					p, err := h.PrepareOutput(t.Context())
					if err != nil {
						t.Fatal(err)
					}
					direct = p
					blobs.plan, err = p.Plan()
					if err != nil {
						t.Fatal(err)
					}
				}
				if phase == "cleanup" {
					if err := h.PublishOutput(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				switch canceledCaller {
				case "handoff":
					handoff.cancel()
				case "reserve":
					reserve.cancel()
				case "factory":
					factory.cancel()
				}
				if phase == "plan" {
					p, err := h.PrepareOutput(t.Context())
					if canceled && (err == nil || p != nil) {
						t.Fatal("canceled winning handoff admitted fresh terminal planning")
					}
					if !canceled && (err != nil || p == nil) {
						t.Fatal("live worker terminal planning", err)
					}
				} else if phase == "cleanup" {
					result, err := h.FinishCleanup(t.Context())
					if err != nil || result == nil {
						t.Fatalf("caller cancellation blocked independent cleanup: result=%t err=%v", result != nil, err)
					}
					blobs.cleanupMu.Lock()
					deletes, absence := blobs.deletes, blobs.absence
					blobs.cleanupMu.Unlock()
					if deletes == 0 || absence != deletes {
						t.Fatalf("independent cleanup physical proof: deletes=%d absence=%d", deletes, absence)
					}
				} else {
					var err error
					if phase == "direct_publish" {
						err = direct.Publish(t.Context())
					} else {
						err = h.PublishOutput(t.Context())
					}
					if canceled && err == nil {
						t.Error("canceled winning handoff admitted fresh terminal publication")
					}
					if !canceled && err != nil {
						t.Fatal("live worker terminal publication", err)
					}
					writes, commits, _ := seed.TerminalPublicationCounts()
					blobs.stateMu.Lock()
					capsules, events, failure := blobs.capsulePuts, blobs.eventPuts, blobs.failure
					blobs.stateMu.Unlock()
					if canceled && (writes != 0 || commits != 0 || capsules != 0 || events != 0) {
						t.Fatalf("canceled terminal phase caused fresh registration/PUT: %d/%d/%d/%d", writes, commits, capsules, events)
					}
					if !canceled && (writes != 1 || commits != 1 || capsules != 1 || events == 0 || failure != "") {
						t.Fatalf("live terminal publication control: %d/%d/%d/%d failure=%q", writes, commits, capsules, events, failure)
					}
				}
			})
		}
	}
}

// No SQL simulator: only the original first transaction's actual Commit or
// Rollback is selected, followed by a lost acknowledgement to the caller.
type workerEntryCommitFault struct {
	driver driver.Driver
	mode   string
	used   bool // the original database/sql pool has exactly one connection
}

func (f *workerEntryCommitFault) Driver() driver.Driver { return f.driver }
func (f *workerEntryCommitFault) Connect(context.Context) (driver.Conn, error) {
	c, err := f.driver.Open("")
	if err != nil {
		return nil, err
	}
	return workerEntryFaultConn{Conn: c, fault: f}, nil
}

type workerEntryFaultConn struct {
	driver.Conn
	fault *workerEntryCommitFault
}

func (c workerEntryFaultConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c workerEntryFaultConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c workerEntryFaultConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	if c.fault.used {
		return tx, nil
	}
	c.fault.used = true
	return workerEntryFaultTx{Tx: tx, mode: c.fault.mode}, nil
}

type workerEntryFaultTx struct {
	driver.Tx
	mode string
}

func (tx workerEntryFaultTx) Commit() error {
	var err error
	if tx.mode == "persisted" {
		err = tx.Tx.Commit()
	} else {
		err = tx.Tx.Rollback()
	}
	if err != nil {
		return err
	}
	return errors.New("owned fixture lost original commit acknowledgement")
}

func TestLocalSessionWorkerEntryLostCommitNeverRecoversFreshness(t *testing.T) {
	for _, mode := range []string{"persisted", "not_persisted"} {
		t.Run(mode, func(t *testing.T) {
			session, seed, blobs, _, effects := newWorkerEntrySession(t, "http://127.0.0.1:18899/v1", mode)
			result, err := effects.ReserveAttemptEffect(t.Context(), seed.Request)
			if err == nil || !reflect.DeepEqual(result, ports.ReserveAttemptEffectResultV1{}) {
				t.Fatal("lost commit gained a fresh result")
			}
			result, err = effects.ReserveAttemptEffect(t.Context(), seed.Request)
			if mode == "persisted" {
				if err != nil || result.Status != ports.AttemptEffectReconcileOnlyV1 || result.Grant != nil {
					t.Fatal("persisted uncertainty did not remain observation-only")
				}
			} else if err == nil || !reflect.DeepEqual(result, ports.ReserveAttemptEffectResultV1{}) {
				t.Fatal("absent uncertain commit became freshness or not_found permission")
			}
			if _, err := session.EnterWorkerEffect(t.Context(), result); err == nil {
				t.Fatal("uncertain/recovered DATA became original entry")
			}
			assertWorkerEntryCounts(t, seed, false)
			if puts, opens, _, deletes := blobs.counts(); puts != 0 || opens != 0 || deletes != 0 {
				t.Fatal("uncertain original reservation touched blob storage")
			}
		})
	}
}
