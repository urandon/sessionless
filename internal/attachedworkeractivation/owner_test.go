package attachedworkeractivation

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

type ownedRuntimeFixture struct {
	started chan struct{}
	stop    chan struct{}
	once    sync.Once
	drains  atomic.Int32
	closes  atomic.Int32
}

func (fixture *ownedRuntimeFixture) Run(ctx context.Context) error {
	close(fixture.started)
	select {
	case <-fixture.stop:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (fixture *ownedRuntimeFixture) Drain(context.Context) error {
	fixture.drains.Add(1)
	return nil
}

func (fixture *ownedRuntimeFixture) Close(context.Context) error {
	fixture.closes.Add(1)
	fixture.once.Do(func() { close(fixture.stop) })
	return nil
}

func (*ownedRuntimeFixture) Status() attachedworkerdaemon.Status {
	return attachedworkerdaemon.Status{State: attachedworkerdaemon.DaemonRunning}
}

func TestOwnerRunsOneRuntimeAndReportsContentFreeLifecycle(t *testing.T) {
	fixture := &ownedRuntimeFixture{started: make(chan struct{}), stop: make(chan struct{})}
	owner, err := newOwner(fixture, testProfile())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- owner.Run(context.Background()) }()
	select {
	case <-fixture.started:
	case <-time.After(time.Second):
		t.Fatal("runtime did not start")
	}
	if err := owner.Run(context.Background()); !errors.Is(err, ErrAlreadyRun) {
		t.Fatalf("duplicate run error = %v", err)
	}
	if err := owner.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := owner.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not stop")
	}
	result := owner.Result()
	if result.Code != "synthetic_stopped" || result.RuntimeOwnership != "released" ||
		result.ServerConnectionState != "closed" || result.DaemonState != "running" ||
		result.CredentialAction != "denied" || fixture.drains.Load() != 1 || fixture.closes.Load() != 1 {
		t.Fatalf("unexpected owned lifecycle: %+v", result)
	}
}

type readinessRuntimeFixture struct {
	ready   chan struct{}
	done    chan struct{}
	running atomic.Bool
}

func (fixture *readinessRuntimeFixture) Run(ctx context.Context) error {
	select {
	case <-fixture.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	fixture.running.Store(true)
	select {
	case <-fixture.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*readinessRuntimeFixture) Drain(context.Context) error { return nil }
func (*readinessRuntimeFixture) Close(context.Context) error { return nil }
func (fixture *readinessRuntimeFixture) Status() attachedworkerdaemon.Status {
	if fixture.running.Load() {
		return attachedworkerdaemon.Status{State: attachedworkerdaemon.DaemonRunning}
	}
	return attachedworkerdaemon.Status{State: attachedworkerdaemon.DaemonStopped}
}

func TestOwnerWaitReadyDoesNotExposeUnstartedOrExitedRuntime(t *testing.T) {
	fixture := &readinessRuntimeFixture{ready: make(chan struct{}), done: make(chan struct{})}
	owner, err := newOwner(fixture, testProfile())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- owner.Run(ctx) }()
	notReady, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	if err := owner.WaitReady(notReady); !errors.Is(err, context.Canceled) {
		t.Fatalf("unstarted readiness = %v", err)
	}
	close(fixture.ready)
	readyCtx, cancelReady := context.WithTimeout(context.Background(), time.Second)
	defer cancelReady()
	if err := owner.WaitReady(readyCtx); err != nil {
		t.Fatalf("running readiness = %v", err)
	}
	close(fixture.done)
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not exit")
	}
	if err := owner.WaitReady(readyCtx); !errors.Is(err, ErrInvalidProfile) {
		t.Fatalf("exited runtime readiness = %v", err)
	}
}
