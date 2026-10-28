package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
	"gitcode.com/urandon/sessionless/internal/worker"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

var errWorkerLocalExecution = errors.New("owner-local worker runtime unavailable")

type workerLocalExecutionConfig struct {
	enabled                           bool
	installation, executable, receipt string
}

func workerLocalExecutionFromEnv(getenv func(string) string) (workerLocalExecutionConfig, error) {
	if getenv == nil {
		return workerLocalExecutionConfig{}, errWorkerLocalExecution
	}
	c := workerLocalExecutionConfig{installation: getenv("WEB_LOCAL_INSTALLATION_ROOT"), executable: getenv("WEB_LOCAL_WORKER_EXECUTABLE"), receipt: getenv("WEB_LOCAL_PROFILE_RECEIPT_FILE")}
	switch getenv("WORKER_LOCAL_EXECUTION_ENABLED") {
	case "", "false":
		if c.installation != "" || c.executable != "" || c.receipt != "" {
			return workerLocalExecutionConfig{}, errWorkerLocalExecution
		}
		return c, nil
	case "true":
		c.enabled = true
	default:
		return workerLocalExecutionConfig{}, errWorkerLocalExecution
	}
	attached := getenv("WEB_ATTACHED_EXECUTION_ENABLED")
	if getenv("SESSIONLESS_ENVIRONMENT") != "local" || (attached != "" && attached != "false") || getenv("WEB_ATTACHED_RESOURCE_PINS") != "" {
		return workerLocalExecutionConfig{}, errWorkerLocalExecution
	}
	for _, path := range []string{c.installation, c.executable, c.receipt} {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
			return workerLocalExecutionConfig{}, errWorkerLocalExecution
		}
	}
	return c, nil
}

// Process-owned installation; concrete LocalRuntime borrows it for each fresh
// invocation. The worker never installs, publishes a new profile or reconstructs
// a missing receipt. Per-invocation cleanup must not close this shared Source.
type workerLocalExecution struct {
	source   *localoperator.Source
	receipt  ports.OwnedProfilePublicationReceiptV1
	once     sync.Once
	closeErr error
}

func openWorkerLocalExecution(ctx context.Context, c workerLocalExecutionConfig) (*workerLocalExecution, error) {
	if !c.enabled {
		return nil, nil
	}
	if ctx == nil || ctx.Err() != nil {
		return nil, errWorkerLocalExecution
	}
	receipt, err := localoperator.ReadProfileReceipt(ctx, c.receipt)
	if err != nil {
		return nil, errWorkerLocalExecution
	}
	source, err := localoperator.OpenExisting(ctx, c.installation, c.executable, receipt)
	if err != nil {
		return nil, errWorkerLocalExecution
	}
	profile, err := source.Configuration()
	if err != nil || profile.Auth != domain.ConfiguredInferenceAuthNoneV1 {
		_ = source.Close()
		return nil, errWorkerLocalExecution
	}
	return &workerLocalExecution{source: source, receipt: receipt}, nil
}

func (e *workerLocalExecution) close() error {
	if e == nil {
		return nil
	}
	e.once.Do(func() { e.closeErr = e.source.Close() })
	return e.closeErr
}

func (e *workerLocalExecution) storeOptions() ydbstore.Options {
	if e == nil {
		return ydbstore.Options{}
	}
	return ydbstore.Options{OwnedProfilePublicationVerifier: e.source, AttemptSpendingPublicationVerifier: e.source, OwnedAttemptEffectSourceVerifier: e.source}
}

func workerRuntimeError(local bool, stage string, cause error) error {
	if cause == nil {
		return nil
	}
	if local {
		if errors.Is(cause, worker.ErrLocalRuntime) {
			return errWorkerLocalExecution // incomplete invocation cleanup is not ordinary retry control flow
		}
		safe := []error{errWorkerLocalExecution}
		for _, marker := range []error{context.Canceled, ports.ErrQueueEmpty, ports.ErrTriggerQueueEmpty, ports.ErrQueueRetryRequested, ports.ErrQueueRedrivePending, domain.ErrMembershipDenied} {
			if errors.Is(cause, marker) {
				safe = append(safe, marker)
			}
		}
		if kind, ok := domain.ClassifyError(cause); ok && (kind == domain.ErrorQuotaBlocked || kind == domain.ErrorPolicyDenied || kind == domain.ErrorReauthRequired) {
			safe = append(safe, &domain.ClassifiedError{Kind: kind, Code: "local_worker_expected_outcome"})
		}
		if len(safe) == 1 {
			return errWorkerLocalExecution
		}
		return errors.Join(safe...)
	}
	return fmt.Errorf("%s: %w", stage, cause)
}

var errWorkerTriggerDrain = errors.New("worker HTTP invocation drain incomplete")

// Process-local request ownership only: this gate issues no execution authority.
// It tracks callback completion, including the original RunOnce deferred Close,
// without leaving a waiting goroutine behind when a bounded drain fails.
type workerTriggerDrain struct {
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	active   int
	stopping bool
	done     chan struct{}
}

func newWorkerTriggerDrain(ctx context.Context) *workerTriggerDrain {
	owned, cancel := context.WithCancel(ctx)
	return &workerTriggerDrain{ctx: owned, cancel: cancel, done: make(chan struct{})}
}

func (d *workerTriggerDrain) begin(request context.Context) (context.Context, func(), error) {
	if d == nil || request == nil {
		return nil, nil, errWorkerTriggerDrain
	}
	if err := request.Err(); err != nil {
		return nil, nil, err
	}
	d.mu.Lock()
	if d.stopping || d.ctx.Err() != nil {
		d.mu.Unlock()
		return nil, nil, context.Canceled
	}
	d.active++
	d.mu.Unlock()
	linked, cancel := context.WithCancel(request)
	stop := context.AfterFunc(d.ctx, cancel)
	var once sync.Once
	finish := func() {
		once.Do(func() {
			stop()
			cancel()
			d.mu.Lock()
			d.active--
			if d.stopping && d.active == 0 {
				close(d.done)
			}
			d.mu.Unlock()
		})
	}
	return linked, finish, nil
}

func (d *workerTriggerDrain) close(ctx context.Context) error {
	if d == nil {
		return nil
	}
	if ctx == nil {
		return errWorkerTriggerDrain
	}
	d.mu.Lock()
	if !d.stopping {
		d.stopping = true
		if d.active == 0 {
			close(d.done)
		}
	}
	d.mu.Unlock()
	d.cancel()
	select {
	case <-d.done:
		return nil
	case <-ctx.Done():
		select {
		case <-d.done:
			return nil
		default:
			return errWorkerTriggerDrain
		}
	}
}

// A real empty receive is ordinary control flow, but it must not hide a joined
// invocation-cleanup error returned by the original local runtime.
func workerQueueEmpty(local bool, cause error) bool {
	return errors.Is(cause, sqsqueue.ErrNoMessage) && (!local || !errors.Is(cause, worker.ErrLocalRuntime))
}
