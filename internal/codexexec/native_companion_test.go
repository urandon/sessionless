//go:build darwin || linux

package codexexec

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

type companionPeerConn struct{ net.Conn }

func (companionPeerConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("8.8.8.8"), Port: 443}
}

type companionPrepareFixture struct {
	ready chan *attachedworkerdaemon.NativeSocketV2
}

func (*companionPrepareFixture) ProfileV2() attachedworkerdaemon.IsolationProfileV2 {
	return (&nativeConstructorLauncher{}).ProfileV2()
}
func (l *companionPrepareFixture) PrepareV2(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV2) (attachedworkerdaemon.IsolationBoundaryV2, error) {
	l.ready <- e.Socket
	<-ctx.Done()
	return nil, ctx.Err() // no native process or OCI engine is started
}

// Real Session/claim, admitted policy and original supervisor Unix listener;
// only DNS/TCP are private in-memory fixtures. No provider request is sent.
func companionFixture(t *testing.T, deps nativeCompanionDependencies) (*nativeCompanion, string) {
	t.Helper()
	f := newNativeConstructorFixture(t)
	root, err := os.MkdirTemp("", "c-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	launcher := &companionPrepareFixture{ready: make(chan *attachedworkerdaemon.NativeSocketV2, 1)}
	sup, err := attachedworkerdaemon.NewSupervisorV2(attachedworkerdaemon.SupervisorConfigV2{
		Base: attachedworkerdaemon.SupervisorConfig{ScratchRoot: root, Timeout: 10 * time.Second, TerminationGrace: 200 * time.Millisecond}, Launcher: launcher})
	if err != nil {
		t.Fatal(err)
	}
	f.config.Supervisor = sup
	entry, err := NewNativePreparedProcessBoundaryV2(t.Context(), f.config, f.session, f.input, f.bundle)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	c, err := entry.native.newCompanion(ctx, deps)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := c.Stop(cleanup); errors.Is(err, context.DeadlineExceeded) {
			t.Error("companion did not join cleanup")
		}
		select {
		case <-finished:
		case <-cleanup.Done():
			t.Error("owned supervisor did not finish")
		}
	})
	go func() {
		_, err := sup.RunNativeV2(ctx, attachedworkerdaemon.AttemptSpec{
			Executable: f.config.Executable, ExecutableDigest: f.config.ExecutableDigest, Arguments: f.config.Arguments}, entry.native.bundle, c)
		finished <- err
	}()
	var socket *attachedworkerdaemon.NativeSocketV2
	select {
	case socket = <-launcher.ready:
	case err := <-finished:
		t.Fatalf("before socket readiness: %v", err)
	case <-ctx.Done():
		t.Fatal("socket readiness timeout")
	}
	path, err := socket.Path()
	if err != nil {
		t.Fatal(err)
	}
	return c, path
}

func companionClient(t *testing.T, path string) net.Conn {
	t.Helper()
	client, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return client
}
func companionLookup(context.Context, string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
}

const companionCONNECT = "CONNECT provider.example:443 HTTP/1.1\r\nHost: provider.example:443\r\n\r\n"

func TestNativeCompanionOpaqueTunnelPreservesPrefetchedBytes(t *testing.T) {
	peerDone := make(chan error, 1)
	var dials atomic.Int32
	c, path := companionFixture(t, nativeCompanionDependencies{lookup: companionLookup,
		dial: func(_ context.Context, network, target string) (net.Conn, error) {
			if network != "tcp4" || target != "8.8.8.8:443" {
				return nil, errors.New("not pinned literal dial")
			}
			dials.Add(1)
			a, b := net.Pipe()
			go func() {
				defer b.Close()
				buffer := make([]byte, len("opaque-request"))
				_, err := io.ReadFull(b, buffer)
				if err == nil && string(buffer) != "opaque-request" {
					err = errors.New("prefetched bytes changed")
				}
				if err == nil {
					_, err = io.WriteString(b, "opaque-reply")
				}
				peerDone <- err
			}()
			return companionPeerConn{a}, nil
		}})
	client := companionClient(t, path)
	if _, err := io.WriteString(client, companionCONNECT+"opaque-request"); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(client)
	line, err := r.ReadString('\n')
	if err != nil || line != "HTTP/1.1 200 Connection Established\r\n" {
		t.Fatalf("CONNECT status=%q err=%v", line, err)
	}
	if line, err = r.ReadString('\n'); err != nil || line != "\r\n" {
		t.Fatalf("CONNECT terminator=%q err=%v", line, err)
	}
	reply := make([]byte, len("opaque-reply"))
	if _, err := io.ReadFull(r, reply); err != nil || string(reply) != "opaque-reply" {
		t.Fatalf("opaque reply mismatch: %v", err)
	}
	select {
	case err := <-peerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("fake peer did not finish")
	}
	if dials.Load() != 1 {
		t.Fatal("tunnel redialed")
	}
	ctx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := c.Stop(ctx); err != nil {
		t.Fatalf("clean opaque tunnel stop: %v", err)
	}
	if c.Alive() {
		t.Fatal("stopped companion remains alive")
	}
}

func TestNativeCompanionRejectsHostAndMixedDNSBeforeDial(t *testing.T) {
	for _, kind := range []string{"foreign-host", "mixed-private", "wrong-peer"} {
		t.Run(kind, func(t *testing.T) {
			var lookups, dials atomic.Int32
			c, path := companionFixture(t, nativeCompanionDependencies{
				lookup: func(context.Context, string) ([]netip.Addr, error) {
					lookups.Add(1)
					if kind == "mixed-private" {
						return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("127.0.0.1")}, nil
					}
					return companionLookup(t.Context(), "")
				},
				dial: func(context.Context, string, string) (net.Conn, error) {
					dials.Add(1)
					a, b := net.Pipe()
					_ = b.Close()
					return a, nil // RemoteAddr is not a pinned TCP peer
				}})
			client := companionClient(t, path)
			host := "provider.example"
			if kind == "foreign-host" {
				host = "foreign.example"
			}
			_, _ = io.WriteString(client, "CONNECT "+host+":443 HTTP/1.1\r\nHost: "+host+":443\r\n\r\n")
			if n, _ := client.Read(make([]byte, 1)); n != 0 {
				t.Fatal("denied connection received success")
			}
			select {
			case <-c.ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("denial did not retire companion")
			}
			ctx, stop := context.WithTimeout(t.Context(), time.Second)
			defer stop()
			if err := c.Stop(ctx); err == nil {
				t.Fatal("denied conduit certified clean")
			}
			if kind != "wrong-peer" && dials.Load() != 0 {
				t.Fatal("denied target dialed")
			}
			if kind == "foreign-host" && lookups.Load() != 0 {
				t.Fatal("foreign target resolved")
			}
		})
	}
}

func TestNativeCompanionSessionCancelRejectsLateDialAndJoinsOriginalPeer(t *testing.T) {
	dialStarted, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	latePeer := make(chan net.Conn, 1)
	c, path := companionFixture(t, nativeCompanionDependencies{lookup: companionLookup,
		dial: func(context.Context, string, string) (net.Conn, error) {
			close(dialStarted)
			<-release
			a, b := net.Pipe()
			latePeer <- b
			return companionPeerConn{a}, nil
		}})
	client := companionClient(t, path)
	_, _ = io.WriteString(client, companionCONNECT)
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("owned dial not reached")
	}
	if err := c.owner.child.Close(); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithCancel(t.Context())
	cancel()
	if err := c.Stop(short); err == nil || !c.Alive() {
		t.Fatal("unjoined late dial certified stopped")
	}
	unblock()
	var peer net.Conn
	select {
	case peer = <-latePeer:
	case <-time.After(time.Second):
		t.Fatal("late peer not returned")
	}
	defer peer.Close()
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := peer.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatal("late connection was retained")
	}
	ctx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := c.Stop(ctx); err != nil || c.Alive() {
		t.Fatalf("late peer did not join: %v", err)
	}
}

func TestNativeCompanionInstallPolicyBindsHostsAndLimits(t *testing.T) {
	f := newNativeConstructorFixture(t)
	for _, mutate := range []func(*attachedworkerdaemon.NativeLaunchV2){
		func(v *attachedworkerdaemon.NativeLaunchV2) { v.ProviderHosts = []string{"foreign.example"} },
		func(v *attachedworkerdaemon.NativeLaunchV2) { v.Limits.OutboundBytes++ },
	} {
		bundle := f.bundle.Clone()
		mutate(&bundle)
		if _, err := NewNativePreparedProcessBoundaryV2(t.Context(), f.config, f.session, f.input, bundle); err == nil {
			t.Fatal("unadmitted network policy reconstructed from caller bundle")
		}
	}
	if _, err := NewNativePreparedProcessBoundaryV2(t.Context(), f.config, f.session, f.input, f.bundle); err != nil {
		t.Fatalf("invalid loser retired admitted original: %v", err)
	}
}

func TestNativeCompanionAggregateByteBudgetCannotOverspend(t *testing.T) {
	peerDone := make(chan int64, 1)
	c, path := companionFixture(t, nativeCompanionDependencies{lookup: companionLookup,
		dial: func(context.Context, string, string) (net.Conn, error) {
			a, b := net.Pipe()
			go func() { defer b.Close(); n, _ := io.Copy(io.Discard, b); peerDone <- n }()
			return companionPeerConn{a}, nil
		}})
	client := companionClient(t, path)
	_, _ = io.WriteString(client, companionCONNECT)
	r := bufio.NewReader(client)
	_, _ = r.ReadString('\n')
	_, _ = r.ReadString('\n')
	_, _ = io.WriteString(client, strings.Repeat("x", 80<<10))
	select {
	case <-c.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("byte exhaustion did not retire companion")
	}
	ctx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := c.Stop(ctx); !errors.Is(err, errNativeConduitLimit) {
		t.Fatalf("byte exhaustion cause: %v", err)
	}
	select {
	case bytes := <-peerDone:
		if bytes <= 0 || uint64(bytes) > c.owner.bundle.Limits.OutboundBytes {
			t.Fatalf("aggregate byte cap: got=%d", bytes)
		}
	case <-time.After(time.Second):
		t.Fatal("bounded fake peer did not join")
	}
}

func TestNativeCompanionAggregateConnectionLimitAndNoRemint(t *testing.T) {
	var dials atomic.Int32
	peers := make(chan struct{}, 2)
	c, path := companionFixture(t, nativeCompanionDependencies{lookup: companionLookup,
		dial: func(context.Context, string, string) (net.Conn, error) {
			dials.Add(1)
			a, b := net.Pipe()
			go func() { defer b.Close(); _, _ = io.Copy(io.Discard, b); peers <- struct{}{} }()
			return companionPeerConn{a}, nil
		}})
	for range 2 {
		client := companionClient(t, path)
		if _, err := io.WriteString(client, companionCONNECT); err != nil {
			t.Fatal(err)
		}
		r := bufio.NewReader(client)
		line, err := r.ReadString('\n')
		if err != nil || line != "HTTP/1.1 200 Connection Established\r\n" {
			t.Fatalf("admitted tunnel status=%q err=%v", line, err)
		}
		if line, err = r.ReadString('\n'); err != nil || line != "\r\n" {
			t.Fatal("missing CONNECT terminator")
		}
	}
	// A third accepted connection consumes no additional DNS/TCP operation.
	client := companionClient(t, path)
	_, _ = io.WriteString(client, companionCONNECT)
	select {
	case <-c.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("connection limit did not retire original")
	}
	ctx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := c.Stop(ctx); !errors.Is(err, errNativeConduitLimit) {
		t.Fatalf("connection exhaustion cause: %v", err)
	}
	if dials.Load() != 2 {
		t.Fatalf("total dial count=%d, want 2", dials.Load())
	}
	for range 2 {
		select {
		case <-peers:
		case <-ctx.Done():
			t.Fatal("owned fake peer not joined")
		}
	}
	if _, err := c.owner.newCompanion(ctx, nativeCompanionDependencies{lookup: companionLookup, dial: func(context.Context, string, string) (net.Conn, error) {
		t.Error("remint dialed")
		return nil, errors.New("forbidden")
	}}); err == nil {
		t.Fatal("retired original counters replenished")
	}
}

func TestNativeCompanionSessionCancelDuringDNSCannotDial(t *testing.T) {
	lookupStarted := make(chan struct{})
	var dials atomic.Int32
	c, path := companionFixture(t, nativeCompanionDependencies{
		lookup: func(ctx context.Context, _ string) ([]netip.Addr, error) {
			close(lookupStarted)
			<-ctx.Done()
			return companionLookup(ctx, "") // late successful resolver result
		},
		dial: func(context.Context, string, string) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("must not dial")
		},
	})
	client := companionClient(t, path)
	_, _ = io.WriteString(client, companionCONNECT)
	select {
	case <-lookupStarted:
	case <-time.After(time.Second):
		t.Fatal("resolver not reached")
	}
	if err := c.owner.child.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := c.Stop(ctx); err != nil || c.Alive() {
		t.Fatalf("revoked resolver did not join: %v", err)
	}
	if dials.Load() != 0 {
		t.Fatal("revoked original authority dialed")
	}
}

type companionProcessBoundary struct {
	*fixtureIsolationBoundary
	envelope attachedworkerdaemon.LaunchEnvelopeV2
}

func (b *companionProcessBoundary) AttestedWorkloadV2() attachedworkerdaemon.WorkloadAttestationV2 {
	return attachedworkerdaemon.WorkloadAttestationV2{Native: b.AttestedWorkload(), Bundle: b.envelope.Native.Clone(), Socket: b.envelope.Socket}
}

type companionProcessLauncher struct {
	ready chan *companionProcessBoundary
}

func (*companionProcessLauncher) ProfileV2() attachedworkerdaemon.IsolationProfileV2 {
	return (&nativeConstructorLauncher{}).ProfileV2()
}

func (l *companionProcessLauncher) PrepareV2(_ context.Context, e attachedworkerdaemon.LaunchEnvelopeV2) (attachedworkerdaemon.IsolationBoundaryV2, error) {
	command := exec.Command(e.Workload.Executable, e.Workload.Arguments...)
	command.Dir, command.Env = e.Workload.Directory, append([]string(nil), e.Workload.Environment...)
	b := &companionProcessBoundary{fixtureIsolationBoundary: &fixtureIsolationBoundary{command: command,
		attestation: attachedworkerdaemon.WorkloadAttestation{Executable: e.Workload.Executable,
			ExecutableDigest: e.Workload.ExecutableDigest, Arguments: append([]string(nil), e.Workload.Arguments...)}}, envelope: e}
	l.ready <- b
	return b, nil
}

type companionProcessResult struct {
	observed attachedworkerdaemon.AttemptResult
	err      error
}

type companionProcessRun struct {
	companion *nativeCompanion
	parent    context.Context
	boundary  *companionProcessBoundary
	finished  <-chan companionProcessResult
	path      string
	pid       int
	joined    bool
}

// The owned child has no descendants and blocks on opening its own FIFO. It
// emits a marker only after exec; cancellation must reach the real supervisor
// wait/TERM/reap path, not just an inert PrepareV2 or companion.Done signal.
func startCompanionProcess(t *testing.T, timeout time.Duration, deps nativeCompanionDependencies) *companionProcessRun {
	t.Helper()
	root := canonicalTempDir(t)
	fifo, marker := filepath.Join(root, "blocked"), filepath.Join(root, "ready")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	path := writePreparedCodexFixture(t, "#!/bin/sh\nprintf '%s' \"$$\" >'"+marker+"'\nIFS= read -r line <'"+fifo+"'\nexit 89\n")
	f := newNativeConstructorFixtureWithExecutable(t, path)
	launcher := &companionProcessLauncher{ready: make(chan *companionProcessBoundary, 1)}
	// Short workspace-owned names stay below Darwin's Unix socket path bound.
	scratch, err := os.MkdirTemp("", "cp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(scratch) })
	scratch, err = filepath.EvalSymlinks(scratch)
	if err != nil {
		t.Fatal(err)
	}
	sup, err := attachedworkerdaemon.NewSupervisorV2(attachedworkerdaemon.SupervisorConfigV2{
		Base: attachedworkerdaemon.SupervisorConfig{ScratchRoot: scratch, Timeout: timeout, TerminationGrace: 200 * time.Millisecond,
			MaxStdoutBytes: 4096, MaxStderrBytes: 4096}, Launcher: launcher})
	if err != nil {
		t.Fatal(err)
	}
	f.config.Supervisor = sup
	entry, err := NewNativePreparedProcessBoundaryV2(t.Context(), f.config, f.session, f.input, f.bundle)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = entry.native.child.Close() })
	parent, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	c, err := entry.native.newCompanion(parent, deps)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	finished := make(chan companionProcessResult, 1)
	run := &companionProcessRun{companion: c, parent: parent, finished: finished}
	t.Cleanup(func() {
		cancel()
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		_ = c.Stop(cleanup)
		if !run.joined {
			select {
			case <-finished:
			case <-cleanup.Done():
				t.Error("owned blocked process did not join cleanup")
			}
		}
	})
	go func() {
		observed, err := entry.native.run(parent, attachedworkerdaemon.AttemptSpec{
			Executable: f.config.Executable, ExecutableDigest: f.config.ExecutableDigest, Arguments: f.config.Arguments},
			ProcessInvocationV1{Authority: f.config.Authority})
		finished <- companionProcessResult{observed: observed, err: err}
	}()
	select {
	case run.boundary = <-launcher.ready:
	case result := <-finished:
		run.joined = true
		t.Fatalf("before owned preparation: %+v, %v", result.observed, result.err)
	case <-parent.Done():
		t.Fatal("owned preparation did not become ready")
	}
	run.path, err = run.boundary.envelope.Socket.Path()
	if err != nil {
		t.Fatal(err)
	}
	readiness := time.NewTimer(3 * time.Second)
	poll := time.NewTicker(5 * time.Millisecond)
	defer readiness.Stop()
	defer poll.Stop()
	readMarker := func() bool {
		if raw, err := os.ReadFile(marker); err == nil && len(raw) != 0 {
			run.pid, err = strconv.Atoi(string(raw))
			if err != nil || run.pid <= 0 {
				t.Fatalf("owned process marker invalid: %q, %v", raw, err)
			}
			return true
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("observe owned process marker: %v", err)
		}
		return false
	}
	for {
		if readMarker() {
			return run
		}
		select {
		case result := <-finished:
			run.joined = true
			t.Fatalf("before blocked helper readiness: %+v, %v", result.observed, result.err)
		case <-poll.C:
		case <-readiness.C:
			if readMarker() {
				return run
			}
			select {
			case result := <-finished:
				run.joined = true
				t.Fatalf("helper exited before readiness: %+v, %v", result.observed, result.err)
			default:
			}
			t.Fatal("owned helper never reached blocked-process readiness")
		}
	}
}

func (run *companionProcessRun) await(t *testing.T) companionProcessResult {
	t.Helper()
	limit := time.NewTimer(3 * time.Second)
	defer limit.Stop()
	var result companionProcessResult
	select {
	case result = <-run.finished:
	case <-limit.C:
		select {
		case result = <-run.finished:
		default:
			t.Fatal("native run did not stop after its conduit lost authority; outer parent is still live")
		}
	}
	run.joined = true
	if run.parent.Err() != nil {
		t.Fatalf("outer parent expired before causal native stop: %v", run.parent.Err())
	}
	if !result.observed.TermSent || !result.observed.DescendantsReaped {
		t.Fatalf("actual helper stop/reap absent: %+v, %v", result.observed, result.err)
	}
	if state := run.boundary.command.ProcessState; state == nil || state.Pid() != run.pid {
		t.Fatalf("owned helper PID %d was not waited: %v", run.pid, state)
	}
	if err := syscall.Kill(run.pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("owned helper PID %d remains observable after Wait: %v", run.pid, err)
	}
	return result
}

func companionReceive[T any](t *testing.T, ch <-chan T, operation string) T {
	t.Helper()
	limit := time.NewTimer(3 * time.Second)
	defer limit.Stop()
	select {
	case got := <-ch:
		return got
	case <-limit.C:
		select {
		case got := <-ch:
			return got
		default:
			t.Fatalf("%s did not complete within owned test bound", operation)
			var zero T
			return zero
		}
	}
}

func TestNativeCompanionForeignCONNECTCancelsAndReapsRunningWorkload(t *testing.T) {
	var lookups, dials atomic.Int32
	run := startCompanionProcess(t, 10*time.Second, nativeCompanionDependencies{
		lookup: func(context.Context, string) ([]netip.Addr, error) {
			lookups.Add(1)
			return nil, errors.New("foreign host must not resolve")
		},
		dial: func(context.Context, string, string) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("foreign host must not dial")
		}})
	client := companionClient(t, run.path)
	if _, err := io.WriteString(client, "CONNECT foreign.example:443 HTTP/1.1\r\nHost: foreign.example:443\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	result := run.await(t)
	if !result.observed.Cancelled || result.observed.Deadline || result.err == nil {
		t.Fatalf("foreign CONNECT did not cause cancellation: %+v, %v", result.observed, result.err)
	}
	if lookups.Load() != 0 || dials.Load() != 0 {
		t.Fatalf("foreign CONNECT reached DNS/dial: lookups=%d dials=%d", lookups.Load(), dials.Load())
	}
}

type companionDeadlinePeer struct {
	companionPeerConn
	deadline chan time.Time
}

func (p *companionDeadlinePeer) SetDeadline(deadline time.Time) error {
	p.deadline <- deadline
	return p.Conn.SetDeadline(deadline)
}

func TestNativeCompanionUsesActualSupervisorDeadlineForDNSDialAndConnections(t *testing.T) {
	lookupContext, dialContext := make(chan context.Context, 1), make(chan context.Context, 1)
	peerDeadline, peerClosed := make(chan time.Time, 1), make(chan error, 1)
	run := startCompanionProcess(t, 2*time.Second, nativeCompanionDependencies{
		lookup: func(ctx context.Context, host string) ([]netip.Addr, error) {
			lookupContext <- ctx
			return companionLookup(ctx, host)
		},
		dial: func(ctx context.Context, network, target string) (net.Conn, error) {
			dialContext <- ctx
			a, b := net.Pipe()
			joined := make(chan struct{})
			t.Cleanup(func() {
				_ = a.Close()
				_ = b.Close()
				companionReceive(t, joined, "owned fake peer cleanup")
			})
			go func() {
				defer close(joined)
				defer b.Close()
				_, err := b.Read(make([]byte, 1))
				peerClosed <- err
			}()
			return &companionDeadlinePeer{companionPeerConn: companionPeerConn{a}, deadline: peerDeadline}, nil
		}})
	client := companionClient(t, run.path)
	if _, err := io.WriteString(client, companionCONNECT); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(client)
	if status, err := reader.ReadString('\n'); err != nil || status != "HTTP/1.1 200 Connection Established\r\n" {
		t.Fatalf("owned CONNECT not ready after preparation: %q, %v", status, err)
	}
	if end, err := reader.ReadString('\n'); err != nil || end != "\r\n" {
		t.Fatalf("owned CONNECT terminator: %q, %v", end, err)
	}
	lookup := companionReceive(t, lookupContext, "owned DNS context")
	dial := companionReceive(t, dialContext, "owned dial context")
	actualDeadline, ok := lookup.Deadline()
	ownerDeadline, ownerOK := run.companion.ctx.Deadline()
	dialDeadline, dialOK := dial.Deadline()
	if !ok || !ownerOK || !dialOK || !actualDeadline.Before(ownerDeadline) || !actualDeadline.Equal(dialDeadline) {
		t.Fatalf("DNS/dial did not receive tightened active authority: lookup=%v dial=%v owner=%v", actualDeadline, dialDeadline, ownerDeadline)
	}
	if got := companionReceive(t, peerDeadline, "pinned connection deadline"); !got.Equal(actualDeadline) {
		t.Fatalf("owned peer deadline=%v, want actual supervisor deadline=%v", got, actualDeadline)
	}
	companionReceive(t, lookup.Done(), "actual active deadline")
	// A peer deadline and the active timer expire together. Socket IO can win
	// that scheduling race and cancel the shared context first; the real-time
	// authority is the original deadline, not which cancellation cause wins.
	if time.Now().Before(actualDeadline) {
		t.Fatalf("DNS context ended before actual active deadline: %v", lookup.Err())
	}
	if dial.Err() == nil || run.parent.Err() != nil {
		t.Fatalf("dial did not expire under still-live outer parent: dial=%v parent=%v", dial.Err(), run.parent.Err())
	}
	result := run.await(t)
	if !result.observed.Deadline && !result.observed.Cancelled {
		t.Fatalf("actual active expiry not observed: %+v, %v", result.observed, result.err)
	}
	if err := companionReceive(t, peerClosed, "original pinned connection closure"); err == nil {
		t.Fatal("fake peer read succeeded without payload instead of seeing connection closure")
	}
	if n, err := reader.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatalf("workload connection remained open: n=%d err=%v", n, err)
	}
}
