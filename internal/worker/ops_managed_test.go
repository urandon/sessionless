package worker_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
	"gitcode.com/urandon/sessionless/internal/worker"
)

type managedOpsSink struct {
	mu      sync.Mutex
	records []opssignal.Record
	err     error
	observe func(context.Context, opssignal.Record)
}

func (s *managedOpsSink) Observe(ctx context.Context, r opssignal.Record) error {
	if s.observe != nil {
		s.observe(ctx, r)
	}
	s.mu.Lock()
	s.records = append(s.records, r)
	s.mu.Unlock()
	return s.err
}
func (s *managedOpsSink) signals() []opssignal.SignalID {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := []opssignal.SignalID{}
	for _, r := range s.records {
		result = append(result, r.SignalID)
	}
	return result
}
func assertManagedSignals(t *testing.T, sink *managedOpsSink, want ...opssignal.SignalID) {
	t.Helper()
	got := sink.signals()
	if len(got) != len(want) {
		t.Fatalf("managed signal count: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("managed signals: got=%v want=%v", got, want)
		}
	}
}

type managedOpsHarness struct {
	calls int
	cause error
}

func (*managedOpsHarness) Preflight(context.Context, ports.ExecutionIdentity) error { return nil }
func (h *managedOpsHarness) Execute(context.Context, ports.ExecutionRequest, ports.ExecutionEventSink) (ports.ExecutionResult, error) {
	h.calls++
	return ports.ExecutionResult{Summary: "managed observation fixture"}, h.cause
}
func (*managedOpsHarness) Cancel(context.Context, ports.ExecutionIdentity) error { return nil }

type managedOpsStore struct {
	*workerState
	completionErr error
	loads         int
}

func (s *managedOpsStore) LoadWorkerJob(ctx context.Context, tenant domain.TenantID, run domain.RunID) (ports.WorkerJobState, bool, error) {
	s.loads++
	return s.workerState.LoadWorkerJob(ctx, tenant, run)
}
func (s *managedOpsStore) CompleteWorkerJob(ctx context.Context, c ports.WorkerCompletion) error {
	if s.completionErr != nil {
		return s.completionErr
	}
	return s.workerState.CompleteWorkerJob(ctx, c)
}

func managedOpsFixture(t *testing.T) (*memoryBlobs, *managedOpsStore, *testkit.MemoryQueue, ports.WorkerJobState) {
	t.Helper()
	blobs := newMemoryBlobs()
	base := newWorkerState()
	loaded := workerFixture(t, context.Background(), blobs, "tenant-managed-ops", workerTestTime)
	loaded.Job.Origin = &domain.FrontendEventOrigin{BindingID: "managed-ops-binding", BindingRevision: 1, Frontend: "synthetic", ExternalConversationID: "managed-ops-conversation", ExternalEventID: "managed-ops-event"}
	loaded.Job.DeliveryChat = domain.TelegramChatRef{}
	loaded.Job.ReplyToMessageID = 0
	base.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
	return blobs, &managedOpsStore{workerState: base}, testkit.NewMemoryQueue(), loaded
}
func newManagedOpsManager(t *testing.T, c worker.Config, queue ports.Queue, state ports.WorkerStateStore, blobs ports.BlobStore, harness ports.HarnessDriver) *worker.Manager {
	t.Helper()
	c.ScratchRoot = t.TempDir()
	c.WorkerID = "managed-ops-worker"
	c.LeaseTTL = time.Hour
	c.LeaseWatchdogInterval = 20 * time.Minute
	if c.DeliveryWakePublisher == nil {
		c.DeliveryWakePublisher = newDeliveryWakePublisher(t)
	}
	m, err := newTestWorker(c, testkit.NewFakeClock(workerTestTime), queue, state, blobs, harness)
	if err != nil {
		t.Fatalf("new actual worker manager: %v", err)
	}
	return m
}

func TestOpsManagedActualExecutionFailureAndExpectedDenials(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		cause error
		want  bool
	}{{"unexpected", errors.New("private-provider-error-sentinel"), true}, {"quota", &domain.ClassifiedError{Kind: domain.ErrorQuotaBlocked}, false}, {"policy", &domain.ClassifiedError{Kind: domain.ErrorPolicyDenied}, false}, {"reauth", &domain.ClassifiedError{Kind: domain.ErrorReauthRequired}, false}, {"membership", domain.ErrMembershipDenied, false}, {"validation", domain.ValidationError{Field: "private-field", Reason: "private-reason"}, false}, {"canceled", context.Canceled, false}} {
		t.Run(tc.name, func(t *testing.T) {
			blobs, state, queue, loaded := managedOpsFixture(t)
			sink := &managedOpsSink{err: errors.New("private-sink-error")}
			harness := &managedOpsHarness{cause: tc.cause}
			m := newManagedOpsManager(t, worker.Config{OpsSignals: sink}, queue, state, blobs, harness)
			publishWorkerMessage(t, context.Background(), queue, loaded.Run.TenantID, loaded.Run.ID)
			_, _ = m.RunOnce(context.Background())
			if harness.calls != 1 || state.completions != 0 || len(state.effects) != 1 {
				t.Fatalf("observation changed effect ownership: sends=%d completions=%d effects=%d", harness.calls, state.completions, len(state.effects))
			}
			if tc.want {
				assertManagedSignals(t, sink, opssignal.ManagedExecutionFailure)
			} else {
				assertManagedSignals(t, sink)
			}
		})
	}
}

type managedOpsFailArtifact struct {
	ports.BlobStore
	puts int
}

func (b *managedOpsFailArtifact) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	if strings.Contains(key, "/artifacts/sha256/") {
		b.puts++
		return domain.BlobRef{}, errors.New("private-object-error")
	}
	return b.BlobStore.Put(ctx, tenant, key, r)
}

func TestOpsManagedActualPublicationAndScratchFailure(t *testing.T) {
	t.Parallel()
	t.Run("artifact_publication", func(t *testing.T) {
		blobs, state, queue, loaded := managedOpsFixture(t)
		sink := &managedOpsSink{}
		failing := &managedOpsFailArtifact{BlobStore: blobs}
		m := newManagedOpsManager(t, worker.Config{OpsSignals: sink}, queue, state, failing, outputHarness{})
		publishWorkerMessage(t, context.Background(), queue, loaded.Run.TenantID, loaded.Run.ID)
		outcome, err := m.RunOnce(context.Background())
		if err != nil || outcome != worker.OutcomeFailed || failing.puts != 1 || state.completions != 0 {
			t.Fatalf("publication actual boundary: outcome=%s err=%v puts=%d completions=%d", outcome, err, failing.puts, state.completions)
		}
		assertManagedSignals(t, sink, opssignal.ManagedPublicationFailure)
	})
	t.Run("scratch_cleanup", func(t *testing.T) {
		blobs, state, queue, loaded := managedOpsFixture(t)
		sink := &managedOpsSink{}
		calls := 0
		m := newManagedOpsManager(t, worker.Config{OpsSignals: sink, ScratchRemoveAll: func(string) error { calls++; return errors.New("private-scratch-error") }}, queue, state, blobs, &managedOpsHarness{})
		publishWorkerMessage(t, context.Background(), queue, loaded.Run.TenantID, loaded.Run.ID)
		outcome, err := m.RunOnce(context.Background())
		if err != nil || outcome != worker.OutcomeFailed || calls != 1 || state.completions != 0 {
			t.Fatalf("scratch owner boundary: outcome=%s err=%v calls=%d completions=%d", outcome, err, calls, state.completions)
		}
		assertManagedSignals(t, sink, opssignal.ManagedCleanupFailure)
	})
}

func TestOpsManagedCommitErrorUnknownAndNilCommitNotSuccess(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"commit_error", "successful_commit"} {
		t.Run(mode, func(t *testing.T) {
			blobs, state, queue, loaded := managedOpsFixture(t)
			sink := &managedOpsSink{err: errors.New("dropped observation")}
			harness := &managedOpsHarness{}
			if mode == "commit_error" {
				state.completionErr = errors.New("private-store-commit-error")
			}
			m := newManagedOpsManager(t, worker.Config{OpsSignals: sink}, queue, state, blobs, harness)
			publishWorkerMessage(t, context.Background(), queue, loaded.Run.TenantID, loaded.Run.ID)
			outcome, err := m.RunOnce(context.Background())
			if mode == "commit_error" {
				if err != nil || outcome != worker.OutcomeRetried || state.completions != 0 {
					t.Fatalf("commit failure business outcome: outcome=%s err=%v completions=%d", outcome, err, state.completions)
				}
				assertManagedSignals(t, sink, opssignal.ManagedCanonicalUnknown)
			} else {
				if err != nil || outcome != worker.OutcomeCompleted || state.completions != 1 {
					t.Fatalf("commit business success: outcome=%s err=%v completions=%d", outcome, err, state.completions)
				}
				assertManagedSignals(t, sink)
			}
			if harness.calls != 1 || state.loads != 1 || len(state.effects) != 1 {
				t.Fatalf("observation added read/send/reservation: sends=%d reads=%d effects=%d", harness.calls, state.loads, len(state.effects))
			}
		})
	}
}

func TestOpsManagedExistingTerminalReadbackScopeAndDuplicate(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"succeeded", "failed", "cancelled", "foreign_tenant", "foreign_job", "foreign_attempt", "running_attempt", "legacy", "attached"} {
		t.Run(mode, func(t *testing.T) {
			blobs, state, queue, loaded := managedOpsFixture(t)
			loaded.Run.Status = domain.RunSucceeded
			loaded.Attempt.Status = domain.AttemptSucceeded
			key := jobKey(loaded.Run.TenantID, loaded.Run.ID)
			requestTenant, requestRun := loaded.Run.TenantID, loaded.Run.ID
			switch mode {
			case "failed":
				loaded.Run.Status = domain.RunFailed
				loaded.Attempt.Status = domain.AttemptFailed
			case "cancelled":
				loaded.Run.Status = domain.RunCancelled
				loaded.Attempt.Status = domain.AttemptCancelled
			case "foreign_tenant":
				loaded.Run.TenantID = "foreign-tenant"
			case "foreign_job":
				loaded.Job.RunID = "foreign-run"
			case "foreign_attempt":
				loaded.Attempt.ID = "foreign-attempt"
			case "running_attempt":
				loaded.Attempt.Status = domain.AttemptRunning
			case "legacy":
				loaded.Job.Origin = nil
			case "attached":
				loaded.Job.ExecutionPlacementV2.Kind = domain.ExecutionPlacementAttachedWorker
			}
			state.jobs[key] = loaded
			sink := &managedOpsSink{err: errors.New("ignored sink failure")}
			harness := &managedOpsHarness{}
			m := newManagedOpsManager(t, worker.Config{OpsSignals: sink}, queue, state, blobs, harness)
			for i := 0; i < 2; i++ {
				publishWorkerMessage(t, context.Background(), queue, requestTenant, requestRun)
				outcome, err := m.RunOnce(context.Background())
				if err != nil || outcome != worker.OutcomeDuplicate {
					t.Fatalf("terminal redelivery %d: outcome=%s err=%v", i, outcome, err)
				}
			}
			if mode == "succeeded" {
				assertManagedSignals(t, sink, opssignal.ManagedCanonicalSuccess, opssignal.ManagedCanonicalSuccess)
			} else if strings.HasPrefix(mode, "foreign_") || mode == "running_attempt" {
				assertManagedSignals(t, sink, opssignal.ManagedCanonicalUnknown, opssignal.ManagedCanonicalUnknown)
			} else {
				assertManagedSignals(t, sink)
			}
			if harness.calls != 0 || state.completions != 0 || len(state.effects) != 0 || state.loads != 2 {
				t.Fatalf("terminal observation replayed effects or added reads: sends=%d commits=%d reservations=%d reads=%d", harness.calls, state.completions, len(state.effects), state.loads)
			}
		})
	}
}

func TestOpsManagedLegacyFailureDoesNotClaimManagedCoverage(t *testing.T) {
	t.Parallel()
	blobs, state, queue, loaded := managedOpsFixture(t)
	loaded.Job.Origin = nil
	state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
	sink := &managedOpsSink{}
	m := newManagedOpsManager(t, worker.Config{OpsSignals: sink}, queue, state, blobs, &managedOpsHarness{cause: errors.New("fixture-error")})
	publishWorkerMessage(t, context.Background(), queue, loaded.Run.TenantID, loaded.Run.ID)
	_, _ = m.RunOnce(context.Background())
	assertManagedSignals(t, sink)
}

func TestOpsManagedDelayedSinkPreservesOriginalContextAndNoExtraEffects(t *testing.T) {
	t.Parallel()
	blobs, state, queue, loaded := managedOpsFixture(t)
	loaded.Run.Status = domain.RunSucceeded
	loaded.Attempt.Status = domain.AttemptSucceeded
	state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	sink := &managedOpsSink{observe: func(got context.Context, _ opssignal.Record) {
		if got != ctx {
			t.Error("telemetry replaced original context")
		}
		close(entered)
		<-release
	}, err: errors.New("drop after delay")}
	harness := &managedOpsHarness{}
	m := newManagedOpsManager(t, worker.Config{OpsSignals: sink}, queue, state, blobs, harness)
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	done := runWorkerOnce(m, ctx)
	waitForSignal(t, t.Context(), entered, "managed observation sink entered")
	cancel()
	once.Do(func() { close(release) })
	result := waitForWorkerResult(t, t.Context(), done)
	if !errors.Is(result.err, context.Canceled) || harness.calls != 0 || state.loads != 1 || len(state.effects) != 0 {
		t.Fatalf("delayed/drop signal bypassed cancellation or repeated effects: result=%+v sends=%d reads=%d effects=%d", result, harness.calls, state.loads, len(state.effects))
	}
	assertManagedSignals(t, sink, opssignal.ManagedCanonicalSuccess)
}

type managedOpsDeadlineHarness struct {
	calls        int
	cancels      int
	deadline     time.Time
	authority    context.Context
	rawTimeout   bool
	collapse     bool
	cancelParent context.CancelFunc
}

func (*managedOpsDeadlineHarness) Preflight(context.Context, ports.ExecutionIdentity) error {
	return nil
}
func (h *managedOpsDeadlineHarness) Execute(ctx context.Context, _ ports.ExecutionRequest, _ ports.ExecutionEventSink) (ports.ExecutionResult, error) {
	h.calls++
	h.deadline, _ = ctx.Deadline()
	h.authority = ctx
	if h.rawTimeout {
		return ports.ExecutionResult{}, context.DeadlineExceeded
	}
	<-ctx.Done()
	if h.cancelParent != nil {
		h.cancelParent()
	}
	if h.collapse {
		return ports.ExecutionResult{}, domain.ErrMembershipDenied
	}
	return ports.ExecutionResult{}, ctx.Err()
}
func (h *managedOpsDeadlineHarness) Cancel(context.Context, ports.ExecutionIdentity) error {
	h.cancels++
	return nil
}

func TestOpsManagedRawTimeoutWithLiveAuthorityAtActualRunOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		blobs, state, queue, loaded := managedOpsFixture(t)
		loaded.Job.Limits.MaxRuntime = time.Minute
		state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
		parent, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		parentDeadline, _ := parent.Deadline()
		harness := &managedOpsDeadlineHarness{rawTimeout: true}
		sink := &managedOpsSink{err: errors.New("drop nested timeout observation"), observe: func(got context.Context, r opssignal.Record) {
			if got != harness.authority || got.Err() != nil {
				t.Errorf("nested timeout observation replaced or expired original authority: same=%t err=%v", got == harness.authority, got.Err())
			}
			if r.Stage != opssignal.Execution || r.ErrorClass != opssignal.Unexpected || r.DeadlineState != opssignal.Within {
				t.Errorf("nested timeout observation metadata: got=%+v", r)
			}
		}}
		m := newManagedOpsManager(t, worker.Config{OpsSignals: sink}, queue, state, blobs, harness)
		publishWorkerMessage(t, parent, queue, loaded.Run.TenantID, loaded.Run.ID)
		started := time.Now()
		outcome, err := m.RunOnce(parent)
		if err != nil || outcome != worker.OutcomeRetried || state.failures != 0 || state.completions != 0 {
			t.Fatalf("nested timeout business retry changed: outcome=%s err=%v failures=%d completions=%d", outcome, err, state.failures, state.completions)
		}
		if harness.calls != 1 || harness.cancels != 1 || state.loads != 1 || len(state.effects) != 1 {
			t.Fatalf("nested timeout observation added effect/read/reservation: sends=%d cancels=%d loads=%d effects=%d", harness.calls, harness.cancels, state.loads, len(state.effects))
		}
		if want := started.Add(loaded.Job.Limits.MaxRuntime); harness.deadline != want {
			t.Errorf("original execution deadline changed: got=%v want=%v", harness.deadline, want)
		}
		if after, _ := parent.Deadline(); after != parentDeadline || parent.Err() != nil {
			t.Errorf("nested timeout altered original parent: deadline=%v want=%v err=%v", after, parentDeadline, parent.Err())
		}
		assertManagedSignals(t, sink, opssignal.ManagedExecutionFailure)
	})
}

func TestOpsManagedOriginalRuntimeDeadlineAtActualRunOnce(t *testing.T) {
	// All timers, contexts and goroutines live in the same synthetic-time
	// bubble. The harness waits for the original execution context, not a test
	// sleep or fabricated deadline error. SQL, model and cloud calls are absent.
	for _, mode := range []string{"deadline_error", "collapsed_membership", "canceled_parent_drops"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				blobs, state, queue, loaded := managedOpsFixture(t)
				loaded.Job.Limits.MaxRuntime = time.Minute
				state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
				parent, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				parentDeadline, ok := parent.Deadline()
				if !ok {
					t.Fatal("fixture parent deadline missing")
				}
				sink := &managedOpsSink{err: errors.New("drop deadline observation"), observe: func(got context.Context, r opssignal.Record) {
					if got != parent {
						t.Error("deadline observation detached or replaced the original parent")
					}
					if r.Stage != opssignal.OriginalDeadline || r.ErrorClass != opssignal.DeadlineExceeded || r.DeadlineState != opssignal.Within {
						t.Errorf("deadline observation metadata: got=%+v", r)
					}
				}}
				harness := &managedOpsDeadlineHarness{collapse: mode != "deadline_error"}
				if mode == "canceled_parent_drops" {
					harness.cancelParent = cancel
				}
				m := newManagedOpsManager(t, worker.Config{OpsSignals: sink}, queue, state, blobs, harness)
				publishWorkerMessage(t, parent, queue, loaded.Run.TenantID, loaded.Run.ID)
				started := time.Now()
				outcome, err := m.RunOnce(parent)
				if err != nil || outcome != worker.OutcomeFailed || state.failures != 1 || state.completions != 0 {
					t.Fatalf("actual original-timeout business outcome: outcome=%s err=%v failures=%d completions=%d", outcome, err, state.failures, state.completions)
				}
				if harness.calls != 1 || harness.cancels != 1 || state.loads != 1 || len(state.effects) != 1 {
					t.Fatalf("deadline observation added effect/read/reservation: sends=%d cancels=%d loads=%d effects=%d", harness.calls, harness.cancels, state.loads, len(state.effects))
				}
				if want := started.Add(loaded.Job.Limits.MaxRuntime); harness.deadline != want {
					t.Errorf("original execution deadline changed: got=%v want=%v", harness.deadline, want)
				}
				if after, _ := parent.Deadline(); after != parentDeadline {
					t.Errorf("parent deadline/lifetime altered: got_deadline=%v want=%v err=%v", after, parentDeadline, parent.Err())
				}
				if mode == "canceled_parent_drops" {
					if !errors.Is(parent.Err(), context.Canceled) {
						t.Fatalf("original parent cancellation not preserved: %v", parent.Err())
					}
					assertManagedSignals(t, sink)
				} else {
					if parent.Err() != nil {
						t.Fatalf("original parent expired before its deadline: %v", parent.Err())
					}
					assertManagedSignals(t, sink, opssignal.ManagedDeadlineExceeded)
				}
			})
		})
	}
}
