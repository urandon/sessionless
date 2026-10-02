package attachedworkerservice

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerforeground"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
)

type fixtureOwner struct {
	mu          sync.Mutex
	drained     int
	stopped     int
	drainErr    error
	shutdownErr error
	result      attachedworkerforeground.ResultV1
}

func (owner *fixtureOwner) Result() attachedworkerforeground.ResultV1 {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return owner.result
}

func (owner *fixtureOwner) Drain(context.Context) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.drained++
	owner.result.DaemonState = "draining"
	return owner.drainErr
}

func (owner *fixtureOwner) Shutdown(context.Context) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.stopped++
	owner.result.RuntimeOwnership = "released"
	return owner.shutdownErr
}

func TestShutdownFailureStillRetiresControlOwner(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("Unix-domain control is supported only on Darwin and Linux")
	}
	directory := filepath.Join(shortTempDir(t), "control")
	owner := &fixtureOwner{drainErr: errors.New("injected drain failure"), result: attachedworkerforeground.ResultV1{
		Version: 1, ManifestRevision: 7, Code: attachedworkerforeground.CodeFeatureDisabled,
		FeatureState: "disabled", RuntimeOwnership: "acquired", DaemonState: "not_started",
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, directory, owner, func(context.Context) (attachedworkerlocal.DoctorResultV1, error) {
			return attachedworkerlocal.DoctorResultV1{}, nil
		})
	}()
	clientCtx, clientCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer clientCancel()
	for {
		if _, err := Call(clientCtx, directory, ActionStatus, 0); err == nil {
			break
		}
		if clientCtx.Err() != nil {
			t.Fatal(clientCtx.Err())
		}
		select {
		case <-time.After(time.Millisecond):
		case <-clientCtx.Done():
		}
	}
	response, err := Call(clientCtx, directory, ActionShutdown, 7)
	if err != nil || response.Code != "shutdown_unknown" {
		t.Fatalf("shutdown response=%+v err=%v", response, err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("serve reported clean exit after failed shutdown")
		}
	case <-clientCtx.Done():
		t.Fatal(clientCtx.Err())
	}
	if _, err := os.Lstat(filepath.Join(directory, SocketFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control endpoint remains after ambiguous cleanup: %v", err)
	}
}

func TestLostMutationResponseIsAmbiguous(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("Unix-domain control is supported only on Darwin and Linux")
	}
	directory := filepath.Join(shortTempDir(t), "control")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(directory, SocketFileName))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(filepath.Join(directory, SocketFileName), 0o600); err != nil {
		t.Fatal(err)
	}
	seen := make(chan struct{})
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			var request RequestV1
			_ = decode(connection, &request)
			_ = connection.Close()
		}
		close(seen)
	}()
	_, err = Call(context.Background(), directory, ActionShutdown, 7)
	if !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("lost mutation response=%v, want ambiguity", err)
	}
	<-seen
}

func TestControlOwnerLifecycle(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("Unix-domain control is supported only on Darwin and Linux")
	}
	stateRoot := filepath.Join(shortTempDir(t), "state")
	controlDir, err := Directory(stateRoot)
	if err != nil {
		t.Fatalf("derive control directory: %v", err)
	}
	owner := &fixtureOwner{result: attachedworkerforeground.ResultV1{
		Version: 1, ManifestRevision: 7, Code: attachedworkerforeground.CodeFeatureDisabled,
		FeatureState: "disabled", RuntimeOwnership: "acquired", DaemonState: "not_started",
		ServerConnectionState: "unknown", NetworkAction: "not_attempted",
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var serveErr error
	go func() {
		serveErr = Serve(ctx, controlDir, owner, func(context.Context) (attachedworkerlocal.DoctorResultV1, error) {
			return attachedworkerlocal.DoctorResultV1{Version: 1, Code: attachedworkerlocal.CodeOK,
				DaemonObservation: "observed_local", ServerConnectionState: "unknown"}, nil
		})
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
			if serveErr != nil {
				t.Errorf("cleanup service: %v", serveErr)
			}
		case <-time.After(2 * time.Second):
			t.Error("cleanup service did not stop within 2s")
		}
	})
	clientCtx, clientCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer clientCancel()
	var status ResponseV1
	for {
		status, err = Call(clientCtx, controlDir, ActionStatus, 0)
		if err == nil || clientCtx.Err() != nil {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-clientCtx.Done():
		}
	}
	if err != nil {
		t.Fatalf("read live status before deadline: %v", err)
	}
	if !status.Valid() || status.Foreground == nil || status.Foreground.DaemonState != "not_started" ||
		status.Foreground.ServerConnectionState != "unknown" || status.Foreground.NetworkAction != "not_attempted" {
		t.Fatalf("live status claimed unproved authority: %+v", status)
	}
	info, err := os.Lstat(controlDir)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("control directory mode: info=%v err=%v", info, err)
	}
	socket, err := os.Lstat(filepath.Join(controlDir, SocketFileName))
	if err != nil || socket.Mode()&os.ModeSocket == 0 || socket.Mode().Perm() != 0o600 {
		t.Fatalf("control socket mode: info=%v err=%v", socket, err)
	}
	doctor, err := Call(clientCtx, controlDir, ActionDoctor, 0)
	if err != nil || doctor.Doctor == nil || doctor.Doctor.ServerConnectionState != "unknown" {
		t.Fatalf("live doctor: response=%+v err=%v", doctor, err)
	}
	conflict, err := Call(clientCtx, controlDir, ActionDrain, 6)
	if err != nil || conflict.Code != "revision_conflict" {
		t.Fatalf("stale drain was not fenced: response=%+v err=%v", conflict, err)
	}
	drain, err := Call(clientCtx, controlDir, ActionDrain, 7)
	if err != nil || drain.Foreground == nil || drain.Foreground.DaemonState != "draining" {
		t.Fatalf("drain owner: response=%+v err=%v", drain, err)
	}
	stopped, err := Call(clientCtx, controlDir, ActionShutdown, 7)
	if err != nil || stopped.Code != "ok" || stopped.Foreground == nil || stopped.Foreground.RuntimeOwnership != "released" {
		t.Fatalf("shutdown owner: response=%+v err=%v", stopped, err)
	}
	select {
	case <-done:
		if serveErr != nil {
			t.Fatalf("service exit: %v", serveErr)
		}
	case <-clientCtx.Done():
		t.Fatalf("service exit exceeded deadline: %v", clientCtx.Err())
	}
	owner.mu.Lock()
	deferredDrain, stoppedCount := owner.drained, owner.stopped
	owner.mu.Unlock()
	if deferredDrain < 1 || stoppedCount != 1 {
		t.Fatalf("owner lifecycle: drain=%d shutdown=%d, want drain>=1 shutdown=1", deferredDrain, stoppedCount)
	}
	if _, err := os.Lstat(filepath.Join(controlDir, SocketFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control socket remains after shutdown: %v", err)
	}
}

func TestControlDirectoryRejectsAmbientOrUnsafeState(t *testing.T) {
	root := t.TempDir()
	for _, candidate := range []string{"", "relative", root + "/../state"} {
		if _, err := Directory(candidate); err == nil {
			t.Errorf("unsafe state root %q accepted", candidate)
		}
	}
	private := filepath.Join(root, "private")
	if err := os.Mkdir(private, 0o755); err != nil {
		t.Fatalf("create unsafe control dir: %v", err)
	}
	if _, err := socketPath(private, true); !errors.Is(err, ErrInvalid) {
		t.Fatalf("world-readable control directory accepted: %v", err)
	}
	if _, err := Call(context.Background(), private, Action("arbitrary"), 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown control action accepted: %v", err)
	}
}

func TestControlStaleSocketRecovery(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("Unix-domain control is supported only on Darwin and Linux")
	}
	directory := filepath.Join(shortTempDir(t), "control")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("create control directory: %v", err)
	}
	path := filepath.Join(directory, SocketFileName)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("create stale socket: %v", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("pin stale socket mode: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close stale listener: %v", err)
	}
	// Go may unlink a Unix socket on Close by default. Preserve one stale
	// socket exactly as a crash would leave it.
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		listener, err = net.Listen("unix", path)
		if err != nil {
			t.Fatalf("recreate stale socket: %v", err)
		}
		listener.(*net.UnixListener).SetUnlinkOnClose(false)
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatalf("pin recreated socket mode: %v", err)
		}
		if err := listener.Close(); err != nil {
			t.Fatalf("close recreated listener: %v", err)
		}
	}
	got, err := socketPath(directory, true)
	if err != nil || got != path {
		t.Fatalf("reconcile stale control socket: path=%q err=%v", got, err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale socket remains: %v", err)
	}
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	// Darwin's sockaddr_un limit is shorter than testing.T.TempDir's
	// machine-specific path. This unique directory is owned by this test.
	base := "/tmp"
	if runtime.GOOS == "darwin" {
		base = "/private/tmp"
	}
	directory, err := os.MkdirTemp(base, "aw137-")
	if err != nil {
		t.Fatalf("create short private test directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Errorf("remove short private test directory %q: %v", directory, err)
		}
	})
	return directory
}
