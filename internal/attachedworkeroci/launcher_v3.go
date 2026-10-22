//go:build darwin || linux

package attachedworkeroci

import (
	"context"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/nativeharnessbootstrap"
)

const nativeProfileNameV3 = "sessionless.oci.docker.native-input.v3"

// NativeLauncherV3 retains the existing rootless engine/container/credential
// lifecycle. Its explicit constructor accepts no V2 installation conversion.
// No shipped runtime registry selects this still-unqualified profile.
type NativeLauncherV3 struct{ base *Launcher }

func NewNativeLauncherV3(ctx context.Context, config Config) (*NativeLauncherV3, error) {
	base, err := NewNativeLauncher(ctx, config)
	if err != nil {
		return nil, err
	}
	return &NativeLauncherV3{base: base.base}, nil
}

func (l *NativeLauncherV3) ProfileV3() attachedworkerdaemon.IsolationProfileV3 {
	if l == nil || l.base == nil {
		return attachedworkerdaemon.IsolationProfileV3{}
	}
	return attachedworkerdaemon.IsolationProfileV3{Version: 3, Name: nativeProfileNameV3, FilesystemReadBoundary: true, FilesystemWriteBoundary: true, ProcessBoundary: true, DiskBytesBounded: true, Network: attachedworkerdaemon.NativeConnectUnixV2}
}

func (l *NativeLauncherV3) PrepareV3(ctx context.Context, envelope attachedworkerdaemon.LaunchEnvelopeV3) (attachedworkerdaemon.IsolationBoundaryV3, error) {
	if l == nil || l.base == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrConfig
	}
	deadline, ok := ctx.Deadline()
	if !ok || !time.Now().Before(deadline) {
		return nil, ErrConfig
	}
	envelope.Native = envelope.Native.Clone()
	native := envelope.Native
	if nativeharnessbootstrap.ValidateInstallationV3(native) != nil || envelope.Execution.ValidateV3(ctx, native) != nil || !envelope.Socket.MatchesPolicyV2(native.ScopeDigest, native.PolicyDigest) || !envelope.Socket.MatchesExecutionV3(envelope.Execution) {
		return nil, ErrConfig
	}
	policy, err := native.InstallPolicyDigestV3()
	if err != nil || policy != native.PolicyDigest {
		return nil, ErrConfig
	}
	descriptor := envelope.Execution.DescriptorV3()
	workload := envelope.Workload
	if workload.Executable != native.Workload.Executable || workload.ExecutableDigest != native.Workload.Digest || !slices.Equal(workload.Arguments, native.NativeArguments[descriptor.Variant]) {
		return nil, ErrConfig
	}
	socket, err := envelope.Socket.Path()
	if err != nil || !ownedObject(socket, os.ModeSocket, 0600) || !ownedObject(filepath.Dir(socket), os.ModeDir, 0700) {
		return nil, ErrConfig
	}
	mappings := envelope.Execution.MappingsV3()
	for _, artifact := range []attachedworkerdaemon.NativeArtifactV2{native.Bootstrap, native.Relay, native.Companion, native.Workload} {
		if _, err := verifyPinnedExecutable(artifact.Executable, hex.EncodeToString(artifact.Digest[:])); err != nil {
			return nil, ErrConfig
		}
	}
	if nativeInputPathsV3(workload, native, mappings) != nil {
		return nil, ErrConfig
	}
	inner := workload
	inner.Executable, inner.ExecutableDigest = native.Bootstrap.Executable, native.Bootstrap.Digest
	inner.Arguments = envelope.Execution.ArgumentsV3()
	inner.Environment, err = nativeharnessbootstrap.BootstrapEnvironment(workload.Environment, deadline)
	if err != nil || ctx.Err() != nil || !time.Now().Before(deadline) || envelope.Execution.ValidateV3(ctx, native) != nil {
		return nil, ErrConfig
	}
	container := &nativeContainerEnvelope{workload: workload, relay: native.Relay, socket: envelope.Socket, socketDestination: native.SocketDestination, profile: nativeProfileNameV3, inputs: mappings}
	b, err := l.base.prepareContainer(ctx, workload, inner, container)
	if b == nil {
		return nil, err
	}
	owner := &nativeBoundaryV3{base: b, envelope: envelope}
	if err != nil {
		return owner, err
	}
	if ctx.Err() != nil || !time.Now().Before(deadline) || envelope.Execution.ValidateV3(ctx, native) != nil || !envelope.Socket.MatchesPolicyV2(native.ScopeDigest, native.PolicyDigest) || !envelope.Socket.MatchesExecutionV3(envelope.Execution) {
		b.nativePrepared = false
		return owner, ErrConfig
	}
	return owner, nil
}

func nativeInputPathsV3(workload attachedworkerdaemon.LaunchSpec, native attachedworkerdaemon.NativeLaunchV3, mappings []attachedworkerdaemon.NativeInputMappingV3) error {
	if len(workload.WriteRoots) != 1 || len(workload.WriteFiles) != 1 {
		return ErrConfig
	}
	// V3 adds files, not a second same-path mount of the whole media/history
	// directory. Only the existing attempt root and exact credential home may
	// remain read roots; the latter keeps its separately fenced auth writeback.
	for _, root := range workload.ReadRoots {
		if root != workload.WriteRoots[0] && root != filepath.Dir(workload.WriteFiles[0]) {
			return ErrConfig
		}
	}
	artifacts := []string{native.Bootstrap.Executable, native.Relay.Executable, native.Companion.Executable, native.Workload.Executable}
	protected := append(append([]string(nil), workload.ReadRoots...), workload.WriteRoots...)
	protected = append(protected, workload.WriteFiles...)
	for _, path := range protected {
		if pathWithin(native.SocketDestination, path) || pathWithin(path, native.SocketDestination) {
			return ErrConfig
		}
	}
	for _, artifact := range artifacts {
		if pathWithin(artifact, native.SocketDestination) || pathWithin(native.SocketDestination, artifact) {
			return ErrConfig
		}
	}
	for i, mapping := range mappings {
		for _, path := range append(append([]string(nil), protected...), artifacts...) {
			if pathWithin(mapping.Destination, path) || pathWithin(path, mapping.Destination) {
				return ErrConfig
			}
		}
		if pathWithin(mapping.Destination, native.SocketDestination) || pathWithin(native.SocketDestination, mapping.Destination) {
			return ErrConfig
		}
		for _, file := range workload.WriteFiles {
			if pathWithin(mapping.Source, filepath.Dir(file)) {
				return ErrConfig
			}
		}
		for _, artifact := range artifacts {
			if mapping.Source == artifact {
				return ErrConfig
			}
		}
		for _, previous := range mappings[:i] {
			if mapping.Source == previous.Source || pathWithin(mapping.Destination, previous.Destination) || pathWithin(previous.Destination, mapping.Destination) {
				return ErrConfig
			}
		}
	}
	return nil
}

type nativeBoundaryV3 struct {
	base     *boundary
	envelope attachedworkerdaemon.LaunchEnvelopeV3
}

func (b *nativeBoundaryV3) Command() *exec.Cmd { return b.base.Command() }
func (b *nativeBoundaryV3) AttestedWorkloadV3() attachedworkerdaemon.WorkloadAttestationV3 {
	return attachedworkerdaemon.WorkloadAttestationV3{Native: b.base.AttestedWorkload(), Bundle: b.envelope.Native.Clone(), Execution: b.envelope.Execution, ExecutionDigest: b.envelope.Execution.DigestV3(), Socket: b.envelope.Socket}
}
func (b *nativeBoundaryV3) GracefulStop(ctx context.Context) error  { return b.base.GracefulStop(ctx) }
func (b *nativeBoundaryV3) ForceStop(ctx context.Context) error     { return b.base.ForceStop(ctx) }
func (b *nativeBoundaryV3) Alive(ctx context.Context) (bool, error) { return b.base.Alive(ctx) }
func (b *nativeBoundaryV3) Release(ctx context.Context) error       { return b.base.Release(ctx) }
func (l *NativeLauncherV3) Reconcile(ctx context.Context) (int, error) {
	if l == nil || l.base == nil {
		return 0, ErrConfig
	}
	return l.base.reconcileProfile(ctx, nativeProfileNameV3)
}
