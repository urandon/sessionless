package ydbstore_test

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/deterministicharness"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/worker"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Reads the actual registered plan at the physical PUT boundary; no prepared
// handle, authority callback or caller-authored capsule enters production.
type localManagerBlobs struct {
	*cleanupSessionBlobs
	seedTest *testing.T
}

func (b *localManagerBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	if strings.Contains(key, "/terminal-custody-v1/") {
		// The fixture's scripted registration owner has committed before this
		// call; the base double checks its independently read-back plan.
		b.stateMu.Lock()
		b.plan = b.seed.TerminalPublicationRun(b.seedTest).TerminalCustody.Plan
		b.stateMu.Unlock()
	}
	return b.cleanupSessionBlobs.Put(ctx, tenant, key, r)
}

type localManagerQueue struct {
	message                ports.ReceivedMessage
	ack, retry, dead, wake int
	onAck                  func() error
}

func (*localManagerQueue) Publish(context.Context, queuecontract.Envelope) error {
	return errors.New("unexpected publish")
}
func (q *localManagerQueue) Receive(context.Context) (ports.ReceivedMessage, error) {
	return q.message, nil
}
func (q *localManagerQueue) Ack(context.Context, string) error {
	if q.onAck != nil {
		if err := q.onAck(); err != nil {
			return err
		}
	}
	q.ack++
	return nil
}
func (q *localManagerQueue) Retry(context.Context, string, time.Duration) error {
	q.retry++
	return nil
}
func (q *localManagerQueue) DeadLetter(context.Context, string, string) error { q.dead++; return nil }
func (q *localManagerQueue) PublishFrontendProjectionWake(context.Context, domain.TenantID, domain.RunID, time.Time) error {
	q.wake++
	return nil
}
func (*localManagerQueue) PublishTelegramDeliveryWake(context.Context, domain.TenantID, domain.TelegramDeliveryID, time.Time) error {
	return errors.New("local canonical job must not use Telegram completion")
}

type localManagerClock struct{}

func (localManagerClock) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// The sink deliberately fails after recording: telemetry delivery cannot alter
// the actual original cleanup, canonical transaction or queue decision.
type localManagerOpsSink struct {
	mu      sync.Mutex
	records []opssignal.Record
}

func (sink *localManagerOpsSink) Observe(_ context.Context, record opssignal.Record) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.records = append(sink.records, record)
	return errors.New("discarded observation")
}

func (sink *localManagerOpsSink) count(signal opssignal.SignalID) int {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	count := 0
	for _, record := range sink.records {
		if record.SignalID == signal {
			count++
		}
	}
	return count
}

// A real Context/AfterFunc parent, not a worker callback or permission. With no
// parent Values, Go uses this public AfterFunc contract for cancellation wiring.
// Once armed inside physical cleanup, stopping the execution child's original
// parent subscription witnesses that the watchdog ACTUALLY cancelled that
// child. A database-read notification alone would race watchdog.err assignment.
type localManagerCaller struct {
	context.Context
	mu               sync.Mutex
	armed, observed  bool
	executionStopped chan struct{}
}

func (*localManagerCaller) Value(any) any { return nil }
func (c *localManagerCaller) AfterFunc(f func()) func() bool {
	stop := context.AfterFunc(c.Context, f)
	return func() bool {
		stopped := stop()
		if stopped {
			c.mu.Lock()
			if c.armed && !c.observed {
				c.observed = true
				close(c.executionStopped)
			}
			c.mu.Unlock()
		}
		return stopped
	}
}

func (c *localManagerCaller) observeExecutionCancellation() {
	c.mu.Lock()
	c.armed = true
	c.mu.Unlock()
}

// Actual Manager, original session/factory/Store/keeper/bridge and controlled
// HTTP peer. The initial running lease is replayed, not queued admission.
// SQL/source verification/funding/storage are fixtures; this is NOT Web ingress,
// browser/re-login, a typed tool loop, native YDB, real-model or cloud proof.
func TestLocalSessionManagerCompletionConnected(t *testing.T) {
	for _, mode := range []string{"success", "late_cancel", "watchdog_cancel_during_cleanup", "delete_failure", "lost_terminal_ack", "lost_terminal_ack_rollback"} {
		t.Run(mode, func(t *testing.T) {
			peer := &sessionCompactionPeer{modelText: "original manager answer"}
			server := httptest.NewUnstartedServer(peer)
			server.Config.ReadHeaderTimeout, server.Config.ReadTimeout, server.Config.WriteTimeout = 10*time.Second, 20*time.Second, 20*time.Second
			server.Config.MaxHeaderBytes = 16 << 10
			t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
			server.Start()
			caller, cancelCaller := context.WithTimeout(t.Context(), 2*time.Minute)
			t.Cleanup(cancelCaller)
			var watchdogCaller *localManagerCaller
			var cleanupReady chan struct{}
			if mode == "watchdog_cancel_during_cleanup" {
				watchdogCaller = &localManagerCaller{Context: caller, executionStopped: make(chan struct{})}
				caller = watchdogCaller
				cleanupReady = make(chan struct{})
			}
			blobs := &localManagerBlobs{cleanupSessionBlobs: &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}}}
			seed := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, server.URL+"/v1", blobs)
			fixture := seed.InstallManagerReadSQL(t, cleanupReady)
			blobs.seed, blobs.seedTest = seed, t
			if mode == "delete_failure" {
				blobs.mode = "delete_failure"
			}
			if mode == "late_cancel" {
				var once sync.Once
				blobs.beforeDelete = func() { once.Do(func() { fixture.RequestCancellation(t) }) }
			}
			if mode == "watchdog_cancel_during_cleanup" {
				var once sync.Once
				blobs.beforeDelete = func() {
					once.Do(func() {
						// The original keeper/allocation is stopped and retired,
						// but original physical erasure is still in progress.
						fixture.RequestCancellation(t)
						watchdogCaller.observeExecutionCancellation()
						close(cleanupReady)
						wait, cancelWait := context.WithTimeout(caller, 10*time.Second)
						defer cancelWait()
						select {
						case <-watchdogCaller.executionStopped:
						case <-wait.Done():
							t.Fatal("actual watchdog did not cancel execution during cleanup", wait.Err())
						}
					})
				}
			}
			if mode == "lost_terminal_ack" {
				fixture.LoseNextCompletionACK(t, "ambiguous")
			}
			if mode == "lost_terminal_ack_rollback" {
				fixture.LoseNextCompletionACK(t, "rollback")
			}
			session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
			if err != nil {
				t.Fatal("original local factory", err)
			}
			t.Cleanup(func() { _ = session.Close() })
			state, err := session.WorkerState()
			if err != nil {
				t.Fatal(err)
			}
			q := &localManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "local-manager-message", Kind: queuecontract.KindDispatchRun, TenantID: seed.Scope.TenantID, SubjectID: string(seed.Scope.RunID), EnqueuedAt: localManagerClock{}.Now()}, ReceiptHandle: "local-manager-receipt", DeliveryCount: 1}}
			q.onAck = func() error {
				if _, err := session.WorkerState(); err == nil {
					return errors.New("ACK before original session close/join")
				}
				got := fixture.Snapshot(t)
				if got.Finalizations != 1 || got.LeasePresent || q.wake != q.ack+1 {
					return errors.New("ACK before canonical terminal or projection wake")
				}
				return nil
			}
			var observed error
			signals := &localManagerOpsSink{}
			configuration := worker.Config{ScratchRoot: t.TempDir(), WorkerID: seed.WorkerPreflightState(t).Attempt.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, LocalOwnedSession: session, DeliveryWakePublisher: q, ProjectionWakePublisher: q, PhysicalClaimGenerator: func(context.Context) (string, error) { return seed.Request.PhysicalInvocationClaimID, nil }, RetryObserver: func(err error) { observed = err }}
			configuration.OpsSignals = signals
			if watchdogCaller != nil {
				configuration.LeaseWatchdogInterval = time.Millisecond
			}
			before := seed.Stats()
			// Delegation is not identity. A wrapper around even the same Store
			// cannot replace the factory's original reservation facade.
			if _, err := worker.New(configuration, localManagerClock{}, q, struct{ ports.WorkerStateStore }{state}, blobs, nil); err == nil || seed.Stats() != before {
				t.Fatal("foreign/wrapped state acquired local registration or effects", err)
			}
			unselected := configuration
			unselected.LocalOwnedSession = nil
			if _, err := worker.New(unselected, localManagerClock{}, q, state, blobs, nil); err == nil || seed.Stats() != before {
				t.Fatal("local route enabled without explicit concrete selection", err)
			}
			mixed := configuration
			mixed.ExecutionPreparer = (*serverlessharness.ExactExecutionPreparerV1)(nil)
			if _, err := worker.New(mixed, localManagerClock{}, q, state, blobs, nil); err == nil || seed.Stats() != before {
				t.Fatal("mixed V1/local execution preparers accepted", err)
			}
			if _, err := worker.New(configuration, localManagerClock{}, q, state, blobs, (*deterministicharness.Driver)(nil)); err == nil || seed.Stats() != before {
				t.Fatal("mixed V1 harness/local execution accepted", err)
			}
			manager, err := worker.New(configuration, localManagerClock{}, q, state, blobs, nil)
			if err != nil {
				t.Fatal("explicit concrete local manager", err)
			}
			outcome, err := manager.RunOnce(caller)
			got := fixture.Snapshot(t)
			if mode == "delete_failure" {
				if signals.count(opssignal.ManagedCleanupFailure) != 1 || signals.count(opssignal.ManagedCleanupSuccess) != 0 || signals.count(opssignal.ManagedCanonicalSuccess) != 0 {
					t.Fatal("partial cleanup emitted false success or lost failure", signals.records)
				}
				stats := seed.Stats()
				blobs.cleanupMu.Lock()
				deletes := blobs.deletes
				blobs.cleanupMu.Unlock()
				if err != nil || outcome != worker.OutcomeRetried || observed == nil || q.ack != 0 || q.retry != 1 || q.wake != 0 || got.Finalizations != 0 || got.Run.Status.Terminal() || stats.Parent != 1 || stats.Burn != 1 || stats.Observation != 1 || deletes != 1 {
					t.Fatalf("partial erasure became canonical/ACK: outcome=%s err=%v observed=%v queue=%+v run=%s finals=%d", outcome, err, observed, q, got.Run.Status, got.Finalizations)
				}
				if outcome, err = manager.RunOnce(t.Context()); err != nil || outcome != worker.OutcomeRetried || q.retry != 2 || q.ack != 0 || seed.Stats() != stats || !reflect.DeepEqual(got, fixture.Snapshot(t)) {
					t.Fatal("partial cleanup retry re-entered execution or changed canonical state", outcome, err)
				}
				blobs.cleanupMu.Lock()
				afterDeletes := blobs.deletes
				blobs.cleanupMu.Unlock()
				peer.mu.Lock()
				calls := len(peer.kinds)
				peer.mu.Unlock()
				if afterDeletes != deletes || calls != 1 {
					t.Fatal("retry repeated erasure or model send", afterDeletes, calls)
				}
				return
			}
			wantOutcome, wantStatus := worker.OutcomeCompleted, domain.RunSucceeded
			if mode == "late_cancel" || mode == "watchdog_cancel_during_cleanup" {
				wantOutcome, wantStatus = worker.OutcomeCancelled, domain.RunCancelled
			}
			if err != nil || outcome != wantOutcome || got.Run.Status != wantStatus || got.Finalizations != 1 || len(got.Events) != 1 || q.ack != 1 || q.retry != 0 || q.dead != 0 {
				t.Fatalf("actual manager terminal: outcome=%s want=%s err=%v observed=%v queue=%+v run=%s finals=%d trace=%+v", outcome, wantOutcome, err, observed, q, got.Run.Status, got.Finalizations, fixture.Trace())
			}
			if (mode == "late_cancel" || mode == "watchdog_cancel_during_cleanup") && (got.Events[0].Kind != domain.SessionEventSystemNotice || got.Run.TerminalCustody.LocalEvidence != nil) {
				t.Fatal("late cancellation published candidate assistant/local evidence")
			}
			if strings.HasPrefix(mode, "lost_terminal_ack") && got.LostACKs != 1 {
				t.Fatal("fault missed actual terminal commit")
			}
			wantSuccess := 1
			if wantStatus == domain.RunCancelled {
				wantSuccess = 0
			}
			if signals.count(opssignal.ManagedCleanupSuccess) != 1 || signals.count(opssignal.ManagedCanonicalSuccess) != wantSuccess || signals.count(opssignal.ManagedCanonicalUnknown) != 0 || signals.count(opssignal.ManagedCleanupFailure) != 0 {
				t.Fatal("observations differ from actual terminal/cleanup readback", signals.records)
			}
			statsBeforeReplay := seed.Stats()
			if statsBeforeReplay.Parent != 1 || statsBeforeReplay.Burn != 1 || statsBeforeReplay.Operation != 1 || statsBeforeReplay.Observation != 1 {
				t.Fatal("Manager did not use one original reservation and durable operation", statsBeforeReplay)
			}
			blobs.mu.Lock()
			ordinaryLeft := 0
			for key := range blobs.objects {
				if strings.Contains(key, "/attempt-operations-v1/") {
					ordinaryLeft++
				}
			}
			blobs.mu.Unlock()
			if ordinaryLeft != 0 {
				t.Fatal("ACK left ordinary operation-body objects", ordinaryLeft)
			}
			lifetime, sourceErr := seed.Source.ExecutionLifetime()
			if sourceErr != nil || lifetime.Err() != nil {
				t.Fatal("Manager closed the borrowed operator Source", sourceErr)
			}
			completedBefore, stats := got, seed.Stats()
			if outcome, err = manager.RunOnce(t.Context()); err != nil || outcome != worker.OutcomeDuplicate || q.ack != 2 || !reflect.DeepEqual(completedBefore, fixture.Snapshot(t)) || seed.Stats() != stats {
				t.Fatal("queue replay repeated original local effects", outcome, err)
			}
			peer.mu.Lock()
			calls := len(peer.kinds)
			peer.mu.Unlock()
			if calls != 1 {
				t.Fatal("Manager/replay dispatched more than one original model", calls)
			}
		})
	}
}
