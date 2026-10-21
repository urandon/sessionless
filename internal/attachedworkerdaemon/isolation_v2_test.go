//go:build darwin || linux

package attachedworkerdaemon

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestNativeCleanupBudgetIsOneWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget := nativeCleanupBudget(10*time.Second, 6*time.Second)
		bound := budget.remaining
		// No cleanup time is consumed by the earlier active phase.
		time.Sleep(time.Hour)
		if got := bound(); got != 6*time.Second {
			t.Fatalf("first bound = %v", got)
		}
		time.Sleep(7 * time.Second)
		if got := bound(); got != 3*time.Second {
			t.Fatalf("remaining bound = %v, want 3s", got)
		}
		time.Sleep(3 * time.Second)
		if got := bound(); got != 0 {
			t.Fatalf("expired budget replenished: %v", got)
		}
		if !budget.expired() {
			t.Fatal("expired original cleanup deadline was lost")
		}
	})
}

type nativeBudgetBoundary struct {
	*fixtureBoundary
	envelope LaunchEnvelopeV2
	alive    func(context.Context) (bool, error)
	graceful func(context.Context) error
	force    func(context.Context) error
	release  func(context.Context) error
}

func (b *nativeBudgetBoundary) AttestedWorkloadV2() WorkloadAttestationV2 {
	return WorkloadAttestationV2{Native: b.attestation, Bundle: b.envelope.Native.Clone(), Socket: b.envelope.Socket}
}
func (b *nativeBudgetBoundary) Alive(ctx context.Context) (bool, error) {
	if b.alive != nil {
		return b.alive(ctx)
	}
	return false, nil
}
func (b *nativeBudgetBoundary) GracefulStop(ctx context.Context) error {
	if b.graceful != nil {
		return b.graceful(ctx)
	}
	return nil
}
func (b *nativeBudgetBoundary) ForceStop(ctx context.Context) error {
	if b.force != nil {
		return b.force(ctx)
	}
	return nil
}
func (b *nativeBudgetBoundary) Release(ctx context.Context) error {
	if b.release != nil {
		return b.release(ctx)
	}
	return nil
}

type nativeBudgetLauncher struct {
	prepare func(context.Context, LaunchEnvelopeV2) (IsolationBoundaryV2, error)
}

// A passive, inert lifecycle fixture; it grants no network or Session authority.
type nativeBudgetConduit struct{}

func (*nativeBudgetConduit) Start(_ context.Context, socket *NativeSocketV2) error {
	_, err := socket.Path()
	return err
}
func (*nativeBudgetConduit) Stop(context.Context) error { return nil }
func (*nativeBudgetConduit) Alive() bool                { return false }

func (*nativeBudgetLauncher) ProfileV2() IsolationProfileV2 {
	return IsolationProfileV2{Version: 2, Name: "native-budget-fixture", FilesystemReadBoundary: true,
		FilesystemWriteBoundary: true, ProcessBoundary: true, DiskBytesBounded: true, Network: NativeConnectUnixV2}
}
func (l *nativeBudgetLauncher) PrepareV2(ctx context.Context, envelope LaunchEnvelopeV2) (IsolationBoundaryV2, error) {
	return l.prepare(ctx, envelope)
}

func nativeBudgetFixture(t *testing.T, launcher *nativeBudgetLauncher, timeout time.Duration) (*Supervisor, AttemptSpec, NativeLaunchV2) {
	t.Helper()
	// TMPDIR is task-owned by the driver. Short names also exercise Darwin's
	// finite Unix socket locator without moving any artifact off workspace.
	root, err := os.MkdirTemp("", "n-")
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := DigestExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := NewSupervisorV2(SupervisorConfigV2{Base: SupervisorConfig{ScratchRoot: root,
		Timeout: timeout, TerminationGrace: 100 * time.Millisecond,
		AllowedEnvironmentNames: []string{helperEnabled, helperMode}}, Launcher: launcher})
	if err != nil {
		t.Fatal(err)
	}
	spec := AttemptSpec{Executable: executable, ExecutableDigest: digest,
		Arguments:   []string{"-test.run=TestSupervisorHelperProcess"},
		Environment: []EnvironmentVariable{{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "protocol-terminal"}}}
	artifact := NativeArtifactV2{Executable: executable, Digest: digest}
	bundle := NativeLaunchV2{Version: 2, Bootstrap: artifact, Relay: artifact, Companion: artifact,
		BootstrapArguments: []string{"bootstrap"}, RelayArguments: []string{"relay"}, CompanionArguments: []string{"companion"},
		SocketDestination: "/conduit", ProviderHosts: []string{"provider.example"}, ScopeDigest: digest, PolicyDigest: digest,
		Limits: NativeConduitLimitsV2{Connections: 1, InboundBytes: 1024, OutboundBytes: 1024,
			CompanionMemoryBytes: 1024, CompanionGoroutines: 1, Active: time.Second, Cleanup: time.Second}}
	return supervisor, spec, bundle
}

func TestNativeActiveBudgetCoversPreparationAndPreventsLateStart(t *testing.T) {
	for _, limit := range []string{"bundle", "supervisor", "parent"} {
		t.Run(limit, func(t *testing.T) {
			launcher := &nativeBudgetLauncher{}
			supervisor, spec, bundle := nativeBudgetFixture(t, launcher, 3*time.Second)
			bundle.Limits.Active = 3 * time.Second
			ctx := t.Context()
			cap := time.Second
			switch limit {
			case "bundle":
				bundle.Limits.Active = cap
			case "supervisor":
				supervisor.timeout = cap
			case "parent":
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, cap)
				defer cancel()
			}
			var boundary *nativeBudgetBoundary
			var observed time.Time
			var observedAt time.Time
			released := make(chan bool, 1)
			launcher.prepare = func(ctx context.Context, envelope LaunchEnvelopeV2) (IsolationBoundaryV2, error) {
				observedAt = time.Now()
				observed, _ = ctx.Deadline()
				base, err := (&fixtureLauncher{}).Prepare(ctx, envelope.Workload)
				if err != nil {
					return nil, err
				}
				boundary = &nativeBudgetBoundary{fixtureBoundary: base.(*fixtureBoundary), envelope: envelope,
					release: func(ctx context.Context) error { released <- ctx.Err() == nil; return nil }}
				<-ctx.Done()
				// A late successful Prepare must not renew active authority.
				return boundary, nil
			}
			result, err := supervisor.RunNativeV2(ctx, spec, bundle, &nativeBudgetConduit{})
			if !errors.Is(err, context.DeadlineExceeded) || !result.Deadline {
				t.Fatalf("late preparation: result=%+v err=%v", result, err)
			}
			if observed.IsZero() || observed.After(observedAt.Add(cap)) {
				t.Fatalf("preparation deadline exceeds %s cap", limit)
			}
			if boundary == nil || boundary.command.Process != nil {
				t.Fatal("expired preparation started a process")
			}
			if !<-released || !result.BoundaryReleased || !result.CleanupSucceeded {
				t.Fatalf("active expiry erased independent cleanup window: %+v", result)
			}
		})
	}
}

func TestNativePartialPreparationRetainsCleanupFailure(t *testing.T) {
	launcher := &nativeBudgetLauncher{}
	supervisor, spec, bundle := nativeBudgetFixture(t, launcher, time.Second)
	released := make(chan struct{})
	launcher.prepare = func(ctx context.Context, envelope LaunchEnvelopeV2) (IsolationBoundaryV2, error) {
		base, err := (&fixtureLauncher{}).Prepare(ctx, envelope.Workload)
		if err != nil {
			return nil, err
		}
		return &nativeBudgetBoundary{fixtureBoundary: base.(*fixtureBoundary), envelope: envelope,
			release: func(context.Context) error { close(released); return errors.New("fixture release failure") }}, errors.New("partial preparation")
	}
	result, err := supervisor.RunNativeV2(t.Context(), spec, bundle, &nativeBudgetConduit{})
	if err == nil || result.CleanupSucceeded || result.BoundaryReleased {
		t.Fatalf("partial owner failure certified clean: result=%+v err=%v", result, err)
	}
	select {
	case <-released:
	default:
		t.Fatal("partial owner was discarded without Release")
	}
}

func TestNativeCleanupExpiryCannotCertifyLateNilRelease(t *testing.T) {
	launcher := &nativeBudgetLauncher{}
	supervisor, spec, bundle := nativeBudgetFixture(t, launcher, 3*time.Second)
	bundle.Limits.Cleanup = 50 * time.Millisecond
	released := make(chan struct{})
	launcher.prepare = func(ctx context.Context, envelope LaunchEnvelopeV2) (IsolationBoundaryV2, error) {
		base, err := (&fixtureLauncher{}).Prepare(ctx, envelope.Workload)
		if err != nil {
			return nil, err
		}
		return &nativeBudgetBoundary{fixtureBoundary: base.(*fixtureBoundary), envelope: envelope,
			release: func(ctx context.Context) error {
				<-ctx.Done()
				close(released)
				return nil // late success is not timely cleanup evidence
			}}, errors.New("partial preparation")
	}
	result, err := supervisor.RunNativeV2(t.Context(), spec, bundle, &nativeBudgetConduit{})
	if !errors.Is(err, errNativeCleanupDeadline) || result.CleanupSucceeded {
		t.Fatalf("expired cleanup certified success: result=%+v err=%v", result, err)
	}
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("original Release owner was not joined")
	}
}

// Real helper processes stay outside synctest. The marker is observed from
// the supervisor's actual output reader, not inferred from Prepare or Start.
func TestNativeActiveBudgetStopsRunningHelper(t *testing.T) {
	launcher := &nativeBudgetLauncher{}
	supervisor, spec, bundle := nativeBudgetFixture(t, launcher, 10*time.Second)
	spec.Arguments = []string{"-test.run=^TestNativeBudgetHelperProcess$"}
	bundle.Limits.Active = time.Second
	ready := make(chan struct{})
	var once sync.Once
	supervisor.outputReader = func(stream string, source io.Reader) io.Reader {
		if stream != "stdout" {
			return source
		}
		return &nativeReadyReader{Reader: source, ready: func() { once.Do(func() { close(ready) }) }}
	}
	launcher.prepare = func(ctx context.Context, envelope LaunchEnvelopeV2) (IsolationBoundaryV2, error) {
		base, err := (&fixtureLauncher{}).Prepare(ctx, envelope.Workload)
		if err != nil {
			return nil, err
		}
		return &nativeBudgetBoundary{fixtureBoundary: base.(*fixtureBoundary), envelope: envelope}, nil
	}
	parent, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	type outcome struct {
		result AttemptResult
		err    error
	}
	finished := make(chan struct{})
	outcomes := make(chan outcome, 1)
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("owned native helper supervisor did not join cleanup")
		}
	})
	go func() {
		defer close(finished)
		result, err := supervisor.RunNativeV2(parent, spec, bundle, &nativeBudgetConduit{})
		outcomes <- outcome{result, err}
	}()
	select {
	case <-ready:
	case <-parent.Done():
		t.Fatal("native helper never acknowledged readiness")
	}
	var got outcome
	select {
	case got = <-outcomes:
	case <-parent.Done():
		t.Fatal("admitted active budget failed to stop the running helper")
	}
	if !got.result.Deadline || got.result.Cancelled || parent.Err() != nil ||
		!got.result.DescendantsReaped || !got.result.BoundaryReleased || !got.result.CleanupSucceeded {
		t.Fatalf("native active deadline/cleanup evidence: result=%+v err=%v", got.result, got.err)
	}
}

type nativeReadyReader struct {
	io.Reader
	ready func()
}

func (r *nativeReadyReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.ready()
	}
	return n, err
}

func TestNativeBudgetHelperProcess(t *testing.T) {
	if os.Getenv(helperEnabled) != "1" {
		return
	}
	_, _ = os.Stdout.Write([]byte("native-ready\n"))
	// A finite backstop, not a test synchronization sleep. The actual native
	// deadline must terminate this process well before the backstop expires.
	<-time.NewTimer(10 * time.Second).C
	os.Exit(97)
}

func TestNativeCleanupDeadlineSpansStopReleaseAndReaders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget := nativeCleanupBudget(10*time.Second, 6*time.Second)
		bound := budget.remaining
		boundary := &nativeBudgetBoundary{
			alive:    func(context.Context) (bool, error) { return true, nil },
			graceful: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			force:    func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		started := time.Now()
		if err := teardownIsolationBoundaryWithBudget(boundary, bound); err == nil {
			t.Fatal("timed out boundary was certified stopped")
		}
		released := make(chan bool, 1)
		_ = callBoundary(bound(), func(ctx context.Context) error {
			released <- errors.Is(ctx.Err(), context.DeadlineExceeded)
			return ctx.Err()
		})
		synctest.Wait()
		if !<-released {
			t.Fatal("Release received a new cleanup window")
		}
		done := make(chan struct{})
		complete, err := finishOutputReaderWithBudget(done, nil, bound, "fixture")
		if complete || err == nil {
			t.Fatal("unfinished reader was certified complete")
		}
		close(done)
		if elapsed := time.Since(started); elapsed != 10*time.Second {
			t.Fatalf("stop/release/readers reset total cleanup deadline: %v", elapsed)
		}
		if !waitChannel(done, 0) {
			t.Fatal("already completed reader lost to expired budget")
		}
	})
}

func TestNativeSocketOriginalScopeAndSubstitution(t *testing.T) {
	root, err := os.MkdirTemp("", "s-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	scope, policy := ExecutableDigest{1}, ExecutableDigest{2}
	socket, err := newNativeSocketV2(root, scope, policy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = socket.close() })
	if !socket.MatchesPolicyV2(scope, policy) || socket.MatchesPolicyV2(ExecutableDigest{3}, policy) {
		t.Fatal("original handle scope mismatch")
	}
	copy := &NativeSocketV2{self: socket, listener: socket.listener, file: socket.file, scope: scope, policy: policy}
	if copy.MatchesPolicyV2(scope, policy) {
		t.Fatal("caller-shaped handle matched original")
	}
	path, err := socket.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replacement.Close() })
	if socket.MatchesPolicyV2(scope, policy) {
		t.Fatal("substituted socket path accepted")
	}
	if _, err := socket.AcceptConnectionV2(); err == nil {
		t.Fatal("substituted socket accepted a connection")
	}
	if err := socket.close(); err == nil {
		t.Fatal("substitution cleanup certified clean")
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatal("original cleanup unlinked replacement")
	}
}
