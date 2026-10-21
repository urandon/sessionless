package attachedworkerdaemon

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/nativeconnect"
)

type NetworkPolicyV2 string

const (
	NetworkDeniedV2     NetworkPolicyV2 = "denied"
	NativeConnectUnixV2 NetworkPolicyV2 = "native_connect_unix.v1"
)

// IsolationProfileV2 never reports effective conduit egress as NetworkDenied.
// Trusted implementation review and exact-engine proof remain separate gates.
type IsolationProfileV2 struct {
	Version                                                                            uint32
	Name                                                                               string
	FilesystemReadBoundary, FilesystemWriteBoundary, ProcessBoundary, DiskBytesBounded bool
	Network                                                                            NetworkPolicyV2
}

func (p IsolationProfileV2) Validate() error {
	if p.Version != 2 || p.Name == "" || !p.FilesystemReadBoundary || !p.FilesystemWriteBoundary || !p.ProcessBoundary || !p.DiskBytesBounded ||
		(p.Network != NetworkDeniedV2 && p.Network != NativeConnectUnixV2) {
		return ErrIsolationUnsupported
	}
	return nil
}

type NativeArtifactV2 struct {
	Executable string
	Digest     ExecutableDigest
}

// NativeLaunchV2 is a frozen install envelope, not accepted-attempt authority.
// The native process remains distinct from every pinned bootstrap/relay member.
type NativeLaunchV2 struct {
	Version                                                uint32
	Bootstrap, Relay, Companion                            NativeArtifactV2
	BootstrapArguments, RelayArguments, CompanionArguments []string
	SocketDestination                                      string
	ProviderHosts                                          []string
	ScopeDigest                                            ExecutableDigest
	PolicyDigest                                           ExecutableDigest
	Limits                                                 NativeConduitLimitsV2
}

type NativeConduitLimitsV2 struct {
	Connections, InboundBytes, OutboundBytes, CompanionMemoryBytes, CompanionGoroutines uint64
	Active, Cleanup                                                                     time.Duration
}

// nativeCleanupWindow starts only when cleanup first needs to wait. All native
// teardown, release and reader joins share this original ceiling, independent
// of active-context cancellation. Per-operation grace may tighten it, never
// replenish it. OS close/remove calls still have their ordinary synchronous
// semantics; an exhausted budget cannot certify complete cleanup.
type nativeCleanupWindow struct {
	total, grace time.Duration
	deadline     time.Time
}

func nativeCleanupBudget(total, grace time.Duration) *nativeCleanupWindow {
	return &nativeCleanupWindow{total: total, grace: grace}
}

func (b *nativeCleanupWindow) remaining() time.Duration {
	if b.deadline.IsZero() {
		b.deadline = time.Now().Add(b.total)
	}
	return max(0, min(b.grace, time.Until(b.deadline)))
}

func (b *nativeCleanupWindow) expired() bool {
	return !b.deadline.IsZero() && !time.Now().Before(b.deadline)
}

func (v NativeLaunchV2) Clone() NativeLaunchV2 {
	v.BootstrapArguments = append([]string(nil), v.BootstrapArguments...)
	v.RelayArguments = append([]string(nil), v.RelayArguments...)
	v.CompanionArguments = append([]string(nil), v.CompanionArguments...)
	v.ProviderHosts = append([]string(nil), v.ProviderHosts...)
	return v
}

func (v NativeLaunchV2) Validate() error {
	if v.ScopeDigest == (ExecutableDigest{}) || v.PolicyDigest == (ExecutableDigest{}) {
		return ErrSupervisorConfig
	}
	return v.validateInstall()
}

// InstallPolicyDigestV2 binds the concrete origins, limits, every artifact,
// argv and socket destination to the admitted attached placement policy.
// Dynamic attempt scope is deliberately excluded; it is checked separately.
func (v NativeLaunchV2) InstallPolicyDigestV2() (ExecutableDigest, error) {
	if v.validateInstall() != nil {
		return ExecutableDigest{}, ErrSupervisorConfig
	}
	v.ScopeDigest, v.PolicyDigest = ExecutableDigest{}, ExecutableDigest{}
	raw, err := json.Marshal(v)
	if err != nil {
		return ExecutableDigest{}, ErrSupervisorConfig
	}
	return ExecutableDigest(sha256.Sum256(append([]byte("sessionless.native-connect.install.v2\x00"), raw...))), nil
}

func (v NativeLaunchV2) validateInstall() error {
	if v.Version != 2 || nativeconnect.ValidateHosts(v.ProviderHosts) != nil ||
		!filepath.IsAbs(v.SocketDestination) || filepath.Clean(v.SocketDestination) != v.SocketDestination || v.SocketDestination == "/" {
		return ErrSupervisorConfig
	}
	if v.Limits.Connections == 0 || v.Limits.InboundBytes == 0 || v.Limits.OutboundBytes == 0 ||
		v.Limits.CompanionMemoryBytes == 0 || v.Limits.CompanionGoroutines == 0 || v.Limits.Active <= 0 ||
		v.Limits.Cleanup <= 0 || v.Limits.Cleanup > maxSupervisorGrace {
		return ErrSupervisorConfig
	}
	for _, a := range []NativeArtifactV2{v.Bootstrap, v.Relay, v.Companion} {
		p, err := validateRegularPath(a.Executable)
		if err != nil || p != a.Executable || a.Digest == (ExecutableDigest{}) {
			return ErrSupervisorConfig
		}
	}
	for _, args := range [][]string{v.BootstrapArguments, v.RelayArguments, v.CompanionArguments} {
		if len(args) == 0 || len(args) > 128 {
			return ErrSupervisorConfig
		}
		for _, arg := range args {
			if len(arg) > 4096 {
				return ErrSupervisorConfig
			}
			for _, b := range []byte(arg) {
				if b == 0 {
					return ErrSupervisorConfig
				}
			}
		}
	}
	return nil
}

// NativeSocketV2 is an original descriptor/inode owner, not path metadata.
// Only its original listener admits connections for the sealed attempt policy;
// the trusted companion still checks current Session authority for each one.
type NativeSocketV2 struct {
	self            *NativeSocketV2
	mu              sync.Mutex
	listener        *net.UnixListener
	file            *os.File
	path, directory string
	identity        os.FileInfo
	fdIdentity      os.FileInfo
	closed          bool
	acceptStopped   bool
	scope, policy   ExecutableDigest
}

func newNativeSocketV2(root string, scope, policy ExecutableDigest) (*NativeSocketV2, error) {
	if scope == (ExecutableDigest{}) || policy == (ExecutableDigest{}) {
		return nil, ErrSupervisorConfig
	}
	// The socket is private and never selected by this basename. Keep the
	// host locator short enough for the supported Unix sockaddr path limits.
	dir, err := os.MkdirTemp(root, ".nc-")
	if err != nil {
		return nil, ErrSupervisorConfig
	}
	path := filepath.Join(dir, "s")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		_ = os.Remove(dir)
		return nil, ErrSupervisorConfig
	}
	listener.SetUnlinkOnClose(false)
	file, err := listener.File()
	if err != nil {
		_ = listener.Close()
		_ = os.Remove(path)
		_ = os.Remove(dir)
		return nil, ErrSupervisorConfig
	}
	info, err := os.Lstat(path)
	fd, fdErr := file.Stat()
	if err != nil || fdErr != nil || info.Mode()&os.ModeSocket == 0 || fd.Mode()&os.ModeSocket == 0 || os.Chmod(path, 0600) != nil {
		_ = file.Close()
		_ = listener.Close()
		_ = os.Remove(path)
		_ = os.Remove(dir)
		return nil, ErrSupervisorConfig
	}
	v := &NativeSocketV2{listener: listener, file: file, path: path, directory: dir, identity: info, fdIdentity: fd, scope: scope, policy: policy}
	v.self = v
	return v, nil
}

func (v *NativeSocketV2) original() bool {
	if v == nil || v.self != v {
		return false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.acceptStopped || v.listener == nil || v.file == nil {
		return false
	}
	actual, err := os.Lstat(v.path)
	fd, fe := v.file.Stat()
	// A Linux socket FD uses sockfs identity, distinct from its filesystem inode.
	// Retain/check both original identities; do not equate these namespaces.
	return err == nil && fe == nil && actual.Mode()&os.ModeSocket != 0 && os.SameFile(actual, v.identity) && os.SameFile(fd, v.fdIdentity)
}

// Path is only an exact mount locator. It never grants a connection or proves
// UID mapping, socket reachability, HTTP/TLS or engine enforcement.
func (v *NativeSocketV2) Path() (string, error) {
	if !v.original() {
		return "", ErrSupervisorConfig
	}
	return v.path, nil
}

// MatchesPolicyV2 compares the sealed original handle, not path metadata or
// caller readiness. A different Session's valid companion cannot borrow it.
func (v *NativeSocketV2) MatchesPolicyV2(scope, policy ExecutableDigest) bool {
	return v.original() && v.scope == scope && v.policy == policy
}

// AcceptConnectionV2 uses only this original listener. The pathname is never
// reopened or used to construct connection permission. Recheck both original
// identities after a blocked accept before handing the connection to its owner.
func (v *NativeSocketV2) AcceptConnectionV2() (net.Conn, error) {
	if !v.original() {
		return nil, ErrSupervisorConfig
	}
	conn, err := v.listener.AcceptUnix()
	if err != nil {
		return nil, err
	}
	if !v.original() {
		_ = conn.Close()
		return nil, ErrSupervisorConfig
	}
	return conn, nil
}

func (v *NativeSocketV2) StopAcceptingV2() error {
	if v == nil || v.self != v {
		return ErrSupervisorConfig
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.acceptStopped || v.closed {
		return nil
	}
	v.acceptStopped = true
	err := v.listener.Close()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (v *NativeSocketV2) close() error {
	if v == nil || v.self != v {
		return ErrSupervisorConfig
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.closed = true
	var err error
	if !v.acceptStopped {
		err = v.listener.Close()
	}
	v.acceptStopped = true
	if e := v.file.Close(); err == nil {
		err = e
	}
	// Never unlink a substituted path or an unowned directory recursively.
	if actual, e := os.Lstat(v.path); e == nil && os.SameFile(actual, v.identity) {
		if e = os.Remove(v.path); err == nil {
			err = e
		}
	} else if e == nil || !os.IsNotExist(e) {
		return ErrSupervisorConfig
	}
	if e := os.Remove(v.directory); err == nil {
		err = e
	}
	return err
}

type LaunchEnvelopeV2 struct {
	Workload LaunchSpec
	Native   NativeLaunchV2
	Socket   *NativeSocketV2
}

type WorkloadAttestationV2 struct {
	Native WorkloadAttestation
	Bundle NativeLaunchV2
	Socket *NativeSocketV2
}

// Existing lifecycle operations must cover native+relay+companion and accepted
// connections together. No v1 attestation can represent the V2 bundle.
type IsolationBoundaryV2 interface {
	Command() *exec.Cmd
	AttestedWorkloadV2() WorkloadAttestationV2
	GracefulStop(context.Context) error
	ForceStop(context.Context) error
	Alive(context.Context) (bool, error)
	Release(context.Context) error
}

type IsolationLauncherV2 interface {
	ProfileV2() IsolationProfileV2
	PrepareV2(context.Context, LaunchEnvelopeV2) (IsolationBoundaryV2, error)
}

type SupervisorConfigV2 struct {
	Base     SupervisorConfig
	Launcher IsolationLauncherV2
}

// NativeConduitV2 is a passive lifecycle port under the existing supervisor,
// not an authorization callback. The prepared Codex owner supplies its genuine
// Session-bound companion; the port itself grants no policy or model authority.
type NativeConduitV2 interface {
	Start(context.Context, *NativeSocketV2) error
	Stop(context.Context) error
	Alive() bool
}

func NewSupervisorV2(c SupervisorConfigV2) (*Supervisor, error) {
	if c.Base.Launcher != nil || c.Launcher == nil {
		return nil, ErrIsolationUnsupported
	}
	profile := c.Launcher.ProfileV2()
	if profile.Validate() != nil {
		return nil, ErrIsolationUnsupported
	}
	v, err := newSupervisor(c.Base, profile.Name)
	if err != nil {
		return nil, err
	}
	v.nativeLauncher = c.Launcher
	v.nativeProfile = profile
	return v, nil
}

func (v *Supervisor) NativeV2() bool { return v != nil && v.launcher == nil && v.nativeLauncher != nil }

// RunNativeV2 shares the original process/pipe/stop/release owner. This explicit
// entry accepts no V1 conversion and cannot be called through ProcessRunner.Run.
// No shipped command or runtime registry constructs this disabled successor.
func (v *Supervisor) RunNativeV2(ctx context.Context, spec AttemptSpec, bundle NativeLaunchV2, companion NativeConduitV2) (AttemptResult, error) {
	if !v.NativeV2() || v.nativeProfile.Network != NativeConnectUnixV2 || bundle.Validate() != nil || companion == nil {
		return AttemptResult{}, ErrIsolationUnsupported
	}
	frozen := bundle.Clone()
	return v.run(ctx, spec, &frozen, companion)
}

// This adapter is private lifecycle reuse only. It deliberately has no usable
// V1 executable attestation and is never accepted by a V1 constructor.
type nativeBoundaryV2 struct {
	IsolationBoundaryV2
	conduit NativeConduitV2
}

func (*nativeBoundaryV2) AttestedWorkload() WorkloadAttestation { return WorkloadAttestation{} }

func (b *nativeBoundaryV2) GracefulStop(ctx context.Context) error {
	return errors.Join(b.conduit.Stop(ctx), b.IsolationBoundaryV2.GracefulStop(ctx))
}
func (b *nativeBoundaryV2) ForceStop(ctx context.Context) error {
	return errors.Join(b.conduit.Stop(ctx), b.IsolationBoundaryV2.ForceStop(ctx))
}
func (b *nativeBoundaryV2) Alive(ctx context.Context) (bool, error) {
	alive, err := b.IsolationBoundaryV2.Alive(ctx)
	return alive || b.conduit.Alive(), err
}
func (b *nativeBoundaryV2) Release(ctx context.Context) error {
	return errors.Join(b.conduit.Stop(ctx), b.IsolationBoundaryV2.Release(ctx))
}
