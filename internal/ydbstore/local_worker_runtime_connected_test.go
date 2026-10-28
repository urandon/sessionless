package ydbstore_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/worker"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

var errRuntimeEmpty = errors.New("controlled runtime queue empty")

// Only a typed-nil rejected dependency; this never supplies a usable lifecycle.
type localRuntimeNilCredentials struct{ ports.CredentialLifecycle }

// The public Context cancellation contract observes invocation lifetime only;
// it is not a Source, factory, approval callback or execution capability.
type localRuntimeCaller struct {
	context.Context
	mu              sync.Mutex
	active, started int
}

func (*localRuntimeCaller) Value(any) any { return nil }
func (c *localRuntimeCaller) AfterFunc(f func()) func() bool {
	c.mu.Lock()
	c.active++
	c.started++
	c.mu.Unlock()
	var once sync.Once
	finish := func() { once.Do(func() { c.mu.Lock(); c.active--; c.mu.Unlock() }) }
	stop := context.AfterFunc(c.Context, func() { defer finish(); f() })
	return func() bool {
		stopped := stop()
		if stopped {
			finish()
		}
		return stopped
	}
}
func (c *localRuntimeCaller) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active, c.started
}

type localRuntimeQueue struct {
	*localManagerQueue
	empty    bool
	receives int
}

func (q *localRuntimeQueue) Receive(ctx context.Context) (ports.ReceivedMessage, error) {
	q.receives++
	if q.empty {
		return ports.ReceivedMessage{}, errRuntimeEmpty
	}
	return q.localManagerQueue.Receive(ctx)
}

// Actual Runtime -> fresh original session/Store -> Manager -> canonical
// terminal, with a SAME borrowed Source and DB across deliveries. The existing
// SQL seed begins admitted/running with a replayed lease: this is NOT fresh
// canonical Web/queued admission, actual command startup, a real model/MCP,
// native YDB or full #218 proof. The separately tagged Manager MCP test covers
// the finite tool loop; this test intentionally uses one model-only answer.
func TestLocalSessionRuntimeOwnsEachRunOnceConnected(t *testing.T) {
	peer := &sessionCompactionPeer{modelText: "runtime-owned original answer"}
	server := httptest.NewUnstartedServer(peer)
	server.Config.ReadHeaderTimeout, server.Config.ReadTimeout, server.Config.WriteTimeout = 10*time.Second, 20*time.Second, 20*time.Second
	server.Config.MaxHeaderBytes = 16 << 10
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
	server.Start()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	caller := &localRuntimeCaller{Context: ctx}
	blobs := &localManagerBlobs{cleanupSessionBlobs: &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}}}
	seed := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, server.URL+"/v1", blobs)
	fixture := seed.InstallManagerReadSQL(t)
	blobs.seed, blobs.seedTest = seed, t
	queue := &localRuntimeQueue{localManagerQueue: &localManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1,
		MessageID: "runtime-original-message", Kind: queuecontract.KindDispatchRun, TenantID: seed.Scope.TenantID,
		SubjectID: string(seed.Scope.RunID), EnqueuedAt: localManagerClock{}.Now()}, ReceiptHandle: "runtime-original-receipt", DeliveryCount: 1}}, empty: true}
	var observed error
	config := worker.LocalRuntimeConfig{Source: seed.Source, Receipt: seed.OriginalProfileReceipt(), DB: seed.DB, Blobs: blobs,
		Worker: worker.Config{ScratchRoot: t.TempDir(), WorkerID: seed.WorkerPreflightState(t).Attempt.WorkerID, LeaseTTL: time.Hour,
			LeaseWatchdogInterval: 20 * time.Minute, DeliveryWakePublisher: queue, ProjectionWakePublisher: queue,
			PhysicalClaimGenerator: func(context.Context) (string, error) { return seed.Request.PhysicalInvocationClaimID, nil },
			RetryObserver:          func(err error) { observed = err }}}
	before := seed.Stats()
	for _, fault := range []string{"nil source", "zero source", "nil DB", "nil blob", "typed nil blob", "nil clock", "typed nil clock", "receipt digest", "foreign receipt", "session", "typed nil preparer", "credentials", "typed nil credentials"} {
		t.Run("constructor/"+fault, func(t *testing.T) {
			bad := config
			bad.Receipt.Profile = bad.Receipt.Profile.Clone()
			var clock ports.Clock = localManagerClock{}
			switch fault {
			case "nil source":
				bad.Source = nil
			case "zero source":
				bad.Source = new(localoperator.Source)
			case "nil DB":
				bad.DB = nil
			case "nil blob":
				bad.Blobs = nil
			case "typed nil blob":
				bad.Blobs = (*localManagerBlobs)(nil)
			case "nil clock":
				clock = nil
			case "typed nil clock":
				clock = (*localManagerClock)(nil)
			case "receipt digest":
				bad.Receipt.Digest = "wrong-digest"
			case "foreign receipt":
				bad.Receipt.Profile.Resource.ResourceID = "foreign-constructor-resource"
				bad.Receipt.Digest, _ = bad.Receipt.Profile.Digest()
			case "session":
				bad.Worker.LocalOwnedSession = new(providerloopsession.LocalV1)
			case "typed nil preparer":
				bad.Worker.ExecutionPreparer = (*serverlessharness.ExactExecutionPreparerV1)(nil)
			case "credentials":
				bad.Worker.CredentialMode = worker.CredentialRequired
			case "typed nil credentials":
				bad.Worker.CredentialLifecycle = (*localRuntimeNilCredentials)(nil)
			}
			if got, err := worker.NewLocalRuntime(bad, clock); !errors.Is(err, worker.ErrLocalRuntime) || got != nil || seed.Stats() != before || queue.receives != 0 {
				t.Fatalf("%s constructor accepted mixed/foreign owners: runtime=%t err=%v", fault, got != nil, err)
			}
		})
	}
	credentialed := ydbstore.NewCredentialedLocalSessionConnectedSeed(t, server.URL+"/v1")
	badAuth := config
	badAuth.Source, badAuth.Receipt = credentialed.Source, credentialed.OriginalProfileReceipt()
	credentialedBefore := credentialed.Stats()
	if got, err := worker.NewLocalRuntime(badAuth, localManagerClock{}); !errors.Is(err, worker.ErrLocalRuntime) || got != nil || seed.Stats() != before || credentialed.Stats() != credentialedBefore || credentialed.ProviderCredentialReadCount() != 0 || queue.receives != 0 {
		t.Fatal("noauth runtime promoted a genuine credentialed Source or read credentials", err)
	}
	runtime, err := worker.NewLocalRuntime(config, localManagerClock{})
	if err != nil || seed.Stats() != before || queue.receives != 0 {
		t.Fatalf("inert runtime construction: err=%v stats=%+v receives=%d", err, seed.Stats(), queue.receives)
	}
	canceled, cancelInvalid := context.WithCancel(ctx)
	cancelInvalid()
	for _, test := range []struct {
		name    string
		runtime *worker.LocalRuntime
		ctx     context.Context
		queue   ports.Queue
	}{
		{name: "nil runtime", ctx: ctx, queue: queue},
		{name: "zero runtime", runtime: new(worker.LocalRuntime), ctx: ctx, queue: queue},
		{name: "nil context", runtime: runtime, queue: queue},
		{name: "typed nil context", runtime: runtime, ctx: (*localRuntimeCaller)(nil), queue: queue},
		{name: "cancelled context", runtime: runtime, ctx: canceled, queue: queue},
		{name: "nil queue", runtime: runtime, ctx: ctx},
		{name: "typed nil queue", runtime: runtime, ctx: ctx, queue: (*localRuntimeQueue)(nil)},
	} {
		t.Run("invocation/"+test.name, func(t *testing.T) {
			if outcome, err := test.runtime.RunOnce(test.ctx, test.queue); !errors.Is(err, worker.ErrLocalRuntime) || outcome != "" || seed.Stats() != before || queue.receives != 0 {
				t.Fatalf("%s invocation reached effects/queue: outcome=%s err=%v receives=%d", test.name, outcome, err, queue.receives)
			}
		})
	}
	// Mutate both scalar and slice-backed caller receipt DATA after construction.
	// Neither the retained runtime nor the original fixture Source may change.
	original := seed.OriginalProfileReceipt()
	config.Receipt.Profile.Resource.ResourceID = "foreign-caller-resource"
	if len(config.Receipt.Profile.InputModalities) == 0 {
		t.Fatal("fixture must have explicit input modalities")
	}
	config.Receipt.Profile.InputModalities[0] = "foreign-caller-modality"
	if !reflect.DeepEqual(original, seed.OriginalProfileReceipt()) {
		t.Fatal("caller receipt mutation rewrote original Source receipt")
	}
	for i := range 2 {
		_, startedBefore := caller.counts()
		if outcome, err := runtime.RunOnce(caller, queue); !errors.Is(err, errRuntimeEmpty) || outcome != "" || seed.Stats() != before || queue.receives != i+1 {
			t.Fatalf("empty invocation%d repeated effects: outcome=%s err=%v receives=%d stats=%+v", i, outcome, err, queue.receives, seed.Stats())
		}
		if active, started := caller.counts(); active != 0 || started <= startedBefore {
			t.Fatalf("empty invocation%d did not create/join fresh caller lifetime: active=%d started=%d before=%d", i, active, started, startedBefore)
		}
	}
	if _, err := seed.Source.Configuration(); err != nil {
		t.Fatal("early invocation closed borrowed Source", err)
	}
	queue.empty = false
	queue.onAck = func() error {
		got := fixture.Snapshot(t)
		if got.Finalizations != 1 || got.LeasePresent || queue.wake != queue.ack+1 {
			return errors.New("runtime ACK before canonical completion and projection wake")
		}
		return nil
	}
	if outcome, err := runtime.RunOnce(caller, queue); err != nil || outcome != worker.OutcomeCompleted {
		t.Fatalf("actual fresh runtime -> Manager terminal: outcome=%s err=%v retry=%v", outcome, err, observed)
	}
	first, stats := fixture.Snapshot(t), seed.Stats()
	if first.Finalizations != 1 || first.Run.Status != domain.RunSucceeded || first.Attempt.Status != domain.AttemptSucceeded || first.Reservation.Status != domain.ReservationCommitted || first.LeasePresent || first.ActiveRuns != 0 || len(first.Events) != 1 || first.Events[0].Kind != domain.SessionEventAssistantMessage || queue.ack != 1 || queue.wake != 1 || queue.retry != 0 || queue.dead != 0 || observed != nil || stats.Parent != before.Parent+1 || stats.Burn != before.Burn+1 || stats.Operation != before.Operation+1 || stats.Observation != before.Observation+1 || seed.ProviderCredentialReadCount() != 0 {
		t.Fatalf("runtime canonical completion/counters: run=%s finalizations=%d events=%d queue=%+v before=%+v after=%+v", first.Run.Status, first.Finalizations, len(first.Events), queue, before, stats)
	}
	blobs.cleanupMu.Lock()
	deletes, absence := blobs.deletes, blobs.absence
	blobs.cleanupMu.Unlock()
	blobs.stateMu.Lock()
	puts := blobs.capsulePuts + blobs.eventPuts
	blobs.stateMu.Unlock()
	if deletes == 0 || deletes != absence {
		t.Fatalf("runtime ACK preceded completed physical erasure: deletes=%d absence=%d", deletes, absence)
	}
	// The fresh delivery uses current canonical terminal readback, not the
	// consumed prior session. Recovery wake is intentionally republished.
	if outcome, err := runtime.RunOnce(caller, queue); err != nil || outcome != worker.OutcomeDuplicate || seed.Stats() != stats || !reflect.DeepEqual(first, fixture.Snapshot(t)) || queue.ack != 2 || queue.wake != 2 {
		t.Fatalf("fresh duplicate repeated original execution: outcome=%s err=%v stats=%+v", outcome, err, seed.Stats())
	}
	blobs.cleanupMu.Lock()
	afterDeletes := blobs.deletes
	blobs.cleanupMu.Unlock()
	blobs.stateMu.Lock()
	afterPuts := blobs.capsulePuts + blobs.eventPuts
	blobs.stateMu.Unlock()
	peer.mu.Lock()
	calls := len(peer.kinds)
	peer.mu.Unlock()
	if afterDeletes != deletes || afterPuts != puts || calls != 1 || seed.ProviderCredentialReadCount() != 0 {
		t.Fatalf("fresh duplicate repeated physical effects: calls=%d puts=%d/%d deletes=%d/%d", calls, puts, afterPuts, deletes, afterDeletes)
	}
	if active, _ := caller.counts(); active != 0 {
		t.Fatalf("completed/duplicate invocations retained caller cancellation hooks=%d", active)
	}
	if _, err := seed.Source.Configuration(); err != nil {
		t.Fatal("completed invocation closed borrowed Source", err)
	}
	if err := seed.DB.PingContext(ctx); err != nil {
		t.Fatal("invocation closed borrowed DB", err)
	}
	if err := seed.Source.Close(); err != nil {
		t.Fatal("original Source close", err)
	}
	if _, err := seed.Source.Configuration(); !errors.Is(err, localoperator.ErrDenied) {
		t.Fatal("causal Source close failed to retire installation", err)
	}
	receives := queue.receives
	if outcome, err := runtime.RunOnce(ctx, queue); !errors.Is(err, worker.ErrLocalRuntime) || outcome != "" || seed.Stats() != stats || queue.receives != receives || queue.ack != 2 {
		t.Fatalf("closed Source caused fallback/effect/queue read: outcome=%s err=%v stats=%+v", outcome, err, seed.Stats())
	}
}
