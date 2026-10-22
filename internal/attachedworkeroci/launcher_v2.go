//go:build darwin || linux

package attachedworkeroci

import (
	"context"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/nativeharnessbootstrap"
)

// NativeLauncher is an explicit, default-unselected Linux-rootless successor.
// It intentionally does not implement the V1 IsolationLauncher interface.
// Source/inspect checks are not the real UID/controller/native proof in #220.
type NativeLauncher struct{ base *Launcher }

func NewNativeLauncher(ctx context.Context, config Config) (*NativeLauncher, error) {
	if ctx == nil || ctx.Err() != nil || validateDeclarativeCommon(config) != nil || config.UserID != 0 || config.GroupID != 0 || config.Boundary != BoundaryLinuxRootless || os.Geteuid() == 0 || os.Getegid() == 0 {
		return nil, ErrConfig
	}
	hostOS := config.hostOS
	if hostOS == "" {
		hostOS = runtime.GOOS
	}
	if hostOS != "linux" {
		return nil, ErrUnsupported
	}
	// This first tuple keeps owner-only socket/auth permissions. A rootless
	// daemon socket owned by another host UID is not a same-user installation.
	endpoint := config.Host[len("unix://"):]
	if !ownedObject(endpoint, os.ModeSocket, 0) {
		return nil, ErrConfig
	}
	base, err := newLauncherValidated(ctx, config)
	if err != nil {
		return nil, err
	}
	var info infoResponse
	if err := base.client.runJSON(ctx, &info, "info", "--format", "{{json .}}"); err != nil || info.ID != config.EngineID || !hasNamedSecurityOption(info.SecurityOptions, "rootless") {
		return nil, ErrUnsupported
	}
	return &NativeLauncher{base: base}, nil
}

func ownedObject(path string, kind os.FileMode, permissions os.FileMode) bool {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeType != kind || info.Mode()&os.ModeSymlink != 0 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid()) && (permissions == 0 || info.Mode().Perm() == permissions)
}

func (l *NativeLauncher) ProfileV2() attachedworkerdaemon.IsolationProfileV2 {
	if l == nil || l.base == nil {
		return attachedworkerdaemon.IsolationProfileV2{}
	}
	return attachedworkerdaemon.IsolationProfileV2{Version: 2, Name: nativeProfileName, FilesystemReadBoundary: true, FilesystemWriteBoundary: true, ProcessBoundary: true, DiskBytesBounded: true, Network: attachedworkerdaemon.NativeConnectUnixV2}
}

func (l *NativeLauncher) PrepareV2(ctx context.Context, envelope attachedworkerdaemon.LaunchEnvelopeV2) (attachedworkerdaemon.IsolationBoundaryV2, error) {
	if l == nil || l.base == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrConfig
	}
	deadline, ok := ctx.Deadline()
	if !ok || !time.Now().Before(deadline) {
		return nil, ErrConfig
	}
	envelope.Native = envelope.Native.Clone()
	if envelope.Native.Validate() != nil || nativeharnessbootstrap.ValidateArguments(envelope.Workload, envelope.Native) != nil || !envelope.Socket.MatchesPolicyV2(envelope.Native.ScopeDigest, envelope.Native.PolicyDigest) {
		return nil, ErrConfig
	}
	install, err := envelope.Native.InstallPolicyDigestV2()
	if err != nil || install != envelope.Native.PolicyDigest {
		return nil, ErrConfig
	}
	if envelope.Workload.Executable == envelope.Native.Bootstrap.Executable || envelope.Workload.Executable == envelope.Native.Relay.Executable {
		return nil, ErrConfig
	}
	socket, err := envelope.Socket.Path()
	if err != nil || !ownedObject(socket, os.ModeSocket, 0600) || !ownedObject(filepath.Dir(socket), os.ModeDir, 0700) {
		return nil, ErrConfig
	}
	destination := envelope.Native.SocketDestination
	for _, root := range append(append([]string(nil), envelope.Workload.ReadRoots...), envelope.Workload.WriteRoots...) {
		if pathWithin(destination, root) || pathWithin(root, destination) {
			return nil, ErrConfig
		}
	}
	for _, artifact := range []attachedworkerdaemon.NativeArtifactV2{envelope.Native.Bootstrap, envelope.Native.Relay, envelope.Native.Companion, {Executable: envelope.Workload.Executable, Digest: envelope.Workload.ExecutableDigest}} {
		if _, err := verifyPinnedExecutable(artifact.Executable, hex.EncodeToString(artifact.Digest[:])); err != nil {
			return nil, ErrConfig
		}
		if destination == artifact.Executable {
			return nil, ErrConfig
		}
	}
	inner := envelope.Workload
	inner.Executable, inner.ExecutableDigest = envelope.Native.Bootstrap.Executable, envelope.Native.Bootstrap.Digest
	inner.Arguments = append([]string(nil), envelope.Native.BootstrapArguments...)
	inner.Environment, err = nativeharnessbootstrap.BootstrapEnvironment(envelope.Workload.Environment, deadline)
	if err != nil {
		return nil, ErrConfig
	}
	if !time.Now().Before(deadline) || ctx.Err() != nil {
		return nil, ErrConfig
	}
	container := &nativeContainerEnvelope{workload: envelope.Workload, relay: envelope.Native.Relay, socket: envelope.Socket, socketDestination: envelope.Native.SocketDestination, profile: nativeProfileName}
	b, err := l.base.prepareContainer(ctx, envelope.Workload, inner, container)
	if b == nil {
		return nil, err
	}
	if err != nil {
		return &nativeBoundary{base: b, envelope: envelope}, err
	}
	if !envelope.Socket.MatchesPolicyV2(envelope.Native.ScopeDigest, envelope.Native.PolicyDigest) {
		b.nativePrepared = false
		return &nativeBoundary{base: b, envelope: envelope}, ErrConfig
	}
	return &nativeBoundary{base: b, envelope: envelope}, nil
}

// This boundary delegates the existing container/credential lifecycle. The
// supervisor's nativeBoundaryV2 joins it with the original host companion.
type nativeBoundary struct {
	base     *boundary
	envelope attachedworkerdaemon.LaunchEnvelopeV2
}

func (b *nativeBoundary) Command() *exec.Cmd { return b.base.Command() }
func (b *nativeBoundary) AttestedWorkloadV2() attachedworkerdaemon.WorkloadAttestationV2 {
	return attachedworkerdaemon.WorkloadAttestationV2{Native: b.base.AttestedWorkload(), Bundle: b.envelope.Native.Clone(), Socket: b.envelope.Socket}
}
func (b *nativeBoundary) GracefulStop(ctx context.Context) error  { return b.base.GracefulStop(ctx) }
func (b *nativeBoundary) ForceStop(ctx context.Context) error     { return b.base.ForceStop(ctx) }
func (b *nativeBoundary) Alive(ctx context.Context) (bool, error) { return b.base.Alive(ctx) }
func (b *nativeBoundary) Release(ctx context.Context) error       { return b.base.Release(ctx) }
func (l *NativeLauncher) Reconcile(ctx context.Context) (int, error) {
	if l == nil || l.base == nil {
		return 0, ErrConfig
	}
	return l.base.reconcileProfile(ctx, nativeProfileName)
}
