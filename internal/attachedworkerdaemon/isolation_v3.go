package attachedworkerdaemon

import (
	"context"
	"errors"
	"os/exec"
	"slices"
)

type IsolationProfileV3 struct {
	Version                                                                            uint32
	Name                                                                               string
	FilesystemReadBoundary, FilesystemWriteBoundary, ProcessBoundary, DiskBytesBounded bool
	Network                                                                            NetworkPolicyV2
}

func (p IsolationProfileV3) Validate() error {
	if p.Version != 3 || p.Name == "" || !p.FilesystemReadBoundary || !p.FilesystemWriteBoundary || !p.ProcessBoundary || !p.DiskBytesBounded || p.Network != NativeConnectUnixV2 {
		return ErrIsolationUnsupported
	}
	return nil
}

type LaunchEnvelopeV3 struct {
	Workload  LaunchSpec
	Native    NativeLaunchV3
	Execution *NativeExecutionV3
	Socket    *NativeSocketV2
}

type WorkloadAttestationV3 struct {
	Native          WorkloadAttestation
	Bundle          NativeLaunchV3
	Execution       *NativeExecutionV3
	ExecutionDigest ExecutableDigest
	Socket          *NativeSocketV2
}

type IsolationBoundaryV3 interface {
	Command() *exec.Cmd
	AttestedWorkloadV3() WorkloadAttestationV3
	GracefulStop(context.Context) error
	ForceStop(context.Context) error
	Alive(context.Context) (bool, error)
	Release(context.Context) error
}

type IsolationLauncherV3 interface {
	ProfileV3() IsolationProfileV3
	PrepareV3(context.Context, LaunchEnvelopeV3) (IsolationBoundaryV3, error)
}

type SupervisorConfigV3 struct {
	Base     SupervisorConfig
	Launcher IsolationLauncherV3
}

func NewSupervisorV3(config SupervisorConfigV3) (*Supervisor, error) {
	if config.Base.Launcher != nil || config.Launcher == nil {
		return nil, ErrIsolationUnsupported
	}
	profile := config.Launcher.ProfileV3()
	if profile.Validate() != nil {
		return nil, ErrIsolationUnsupported
	}
	v, err := newSupervisor(config.Base, profile.Name)
	if err != nil {
		return nil, err
	}
	v.nativeLauncherV3 = config.Launcher
	v.nativeProfileV3 = profile
	return v, nil
}

func (v *Supervisor) NativeV3() bool {
	return v != nil && v.launcher == nil && v.nativeLauncher == nil && v.nativeLauncherV3 != nil
}

// Reuse the original listener/inode owner and retain this execution's exact
// readset before exposing the socket to companion/launcher. A matching scope
// and installation digest alone cannot substitute another valid readset.
func newNativeSocketV3(root string, scope, policy ExecutableDigest, execution *NativeExecutionV3) (*NativeSocketV2, error) {
	if execution == nil || execution.self != execution || execution.bundle.ScopeDigest != scope || execution.bundle.PolicyDigest != policy || execution.DigestV3() == (ExecutableDigest{}) {
		return nil, ErrSupervisorConfig
	}
	v, err := newNativeSocketV2(root, scope, policy)
	if err != nil {
		return nil, err
	}
	v.executionV3 = execution
	v.executionDigestV3 = execution.DigestV3()
	return v, nil
}

func (v *NativeSocketV2) MatchesExecutionV3(execution *NativeExecutionV3) bool {
	return v.original() && execution != nil && execution.self == execution && v.executionV3 == execution && v.executionDigestV3 == execution.DigestV3() && v.executionDigestV3 != (ExecutableDigest{})
}

// nativeLaunchPlan is private lowering of exactly one versioned launch entry.
// It introduces no runner or lifecycle: both versions use supervisor.run's
// original process/pipe/stop/release/credential/cleanup owner.
type nativeLaunchPlan struct {
	v2                          *NativeLaunchV2
	v3                          *NativeLaunchV3
	execution                   *NativeExecutionV3
	Bootstrap, Relay, Companion NativeArtifactV2
	ScopeDigest, PolicyDigest   ExecutableDigest
	Limits                      NativeConduitLimitsV2
}

func (v *Supervisor) RunNativeV3(ctx context.Context, spec AttemptSpec, bundle NativeLaunchV3, execution *NativeExecutionV3, companion NativeConduitV2) (AttemptResult, error) {
	if !v.NativeV3() || companion == nil || execution.ValidateV3(ctx, bundle) != nil {
		return AttemptResult{}, ErrIsolationUnsupported
	}
	d := execution.DescriptorV3()
	if spec.Executable != bundle.Workload.Executable || spec.ExecutableDigest != bundle.Workload.Digest || !slices.Equal(spec.Arguments, bundle.NativeArguments[d.Variant]) {
		return AttemptResult{}, ErrSupervisorConfig
	}
	frozen := bundle.Clone()
	plan := &nativeLaunchPlan{v3: &frozen, execution: execution, Bootstrap: frozen.Bootstrap, Relay: frozen.Relay, Companion: frozen.Companion, ScopeDigest: frozen.ScopeDigest, PolicyDigest: frozen.PolicyDigest, Limits: frozen.Limits}
	return v.run(ctx, spec, plan, companion)
}

type nativeBoundaryV3 struct {
	IsolationBoundaryV3
	conduit NativeConduitV2
}

func (*nativeBoundaryV3) AttestedWorkload() WorkloadAttestation { return WorkloadAttestation{} }
func (b *nativeBoundaryV3) GracefulStop(ctx context.Context) error {
	return errors.Join(b.conduit.Stop(ctx), b.IsolationBoundaryV3.GracefulStop(ctx))
}
func (b *nativeBoundaryV3) ForceStop(ctx context.Context) error {
	return errors.Join(b.conduit.Stop(ctx), b.IsolationBoundaryV3.ForceStop(ctx))
}
func (b *nativeBoundaryV3) Alive(ctx context.Context) (bool, error) {
	alive, err := b.IsolationBoundaryV3.Alive(ctx)
	return alive || b.conduit.Alive(), err
}
func (b *nativeBoundaryV3) Release(ctx context.Context) error {
	if err := b.conduit.Stop(ctx); err != nil {
		return err
	}
	return b.IsolationBoundaryV3.Release(ctx)
}
