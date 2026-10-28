package attachedworkerstack

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/ownerlocalcredential"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// NewNativeV3 explicitly selects the disabled native-envelope composition.
// Installation verification/reconciliation is not permission for a provider
// turn: the same original Session and prepared entry still own admission.
func NewNativeV3(ctx context.Context, manifest attachedworkerlocal.ManifestV1, config Config, credentials ports.CredentialLifecycle) (*Stack, error) {
	return newNativeStackV3(ctx, manifest, config, credentials, defaultNativeDependenciesV3())
}

type nativeReconcilingLauncherV3 interface {
	attachedworkerdaemon.IsolationLauncherV3
	Reconcile(context.Context) (int, error)
}

// Private typed seams exercise construction without an engine. Neither the
// production API nor the prepared entry accepts an arbitrary runner factory.
type nativeDependenciesV3 struct {
	hostOS, hostArchitecture string
	digest                   func(string) (attachedworkerdaemon.ExecutableDigest, error)
	newLauncher              func(context.Context, attachedworkeroci.Config) (nativeReconcilingLauncherV3, error)
	newSupervisor            func(attachedworkerdaemon.SupervisorConfigV3) (*attachedworkerdaemon.Supervisor, error)
}

func newNativeStackV3(ctx context.Context, manifest attachedworkerlocal.ManifestV1, config Config, credentials ports.CredentialLifecycle, deps nativeDependenciesV3) (*Stack, error) {
	return newNativeStackCompositionV3(ctx, manifest, config, credentials, deps, nativeStackFixedV3)
}

type nativeStackChoiceV3 uint8

const (
	nativeStackFixedV3 nativeStackChoiceV3 = iota + 1
	nativeStackGenerationV3
)

// One installation path, with two closed prepared constructors. The common
// artifact/platform checks do not qualify V2 evidence as generation-aware V3.
func newNativeStackCompositionV3(ctx context.Context, manifest attachedworkerlocal.ManifestV1, config Config, credentials ports.CredentialLifecycle, deps nativeDependenciesV3, choice nativeStackChoiceV3) (*Stack, error) {
	if ctx == nil || credentials == nil || config.PreparedCodex != nil || config.PreparedCodexMedia != nil ||
		manifest.Validate() != nil || manifest.Lifecycle != attachedworkerlocal.LifecycleActive || deps.digest == nil || deps.newLauncher == nil || deps.newSupervisor == nil {
		return nil, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var fixed *codexexec.NativePreparedConfigV2
	var generation *codexexec.NativePreparedConfigV3
	var backend codexexec.Config
	var platform attachedworkerlocal.OCIConfigV1
	var launch attachedworkerdaemon.NativeLaunchV3
	var operatingSystem, architecture string
	var arguments []string
	var launcherConfig attachedworkeroci.Config
	var inline bool
	var err error
	switch choice {
	case nativeStackFixedV3:
		if config.PreparedCodexNative == nil || config.PreparedCodexNativeGeneration != nil {
			return nil, ErrInvalidConfiguration
		}
		owned, cloneErr := config.PreparedCodexNative.Clone()
		if cloneErr != nil {
			return nil, ErrInvalidConfiguration
		}
		fixed = &owned
		p := owned.Profile
		backend, platform, launch, operatingSystem, architecture, inline = p.Backend, p.Platform, p.Launch, p.OperatingSystem, p.Architecture, owned.InlineOutputV2
		arguments, err = p.ArgumentsV2()
		if err == nil {
			launcherConfig, err = p.NativeLauncherConfigV3()
		}
	case nativeStackGenerationV3:
		if config.PreparedCodexNativeGeneration == nil || config.PreparedCodexNative != nil {
			return nil, ErrInvalidConfiguration
		}
		lifecycle, ok := credentials.(*ownerlocalcredential.Lifecycle)
		if !ok || lifecycle == nil || lifecycle != config.PreparedCodexNativeGeneration.Lifecycle {
			return nil, ErrInvalidConfiguration
		}
		owned, cloneErr := config.PreparedCodexNativeGeneration.Clone()
		if cloneErr != nil || owned.Baseline.TenantID != manifest.TenantID || owned.Baseline.OwnerUserID != manifest.OwnerUserID ||
			owned.Baseline.WorkerID != manifest.WorkerID || owned.Baseline.EnrollmentGeneration != manifest.EnrollmentGeneration {
			return nil, ErrInvalidConfiguration
		}
		generation = &owned
		p := owned.Profile
		backend, platform, launch, operatingSystem, architecture, inline = p.Backend, p.Platform, p.Launch, p.OperatingSystem, p.Architecture, owned.InlineOutputV2
		arguments, err = p.ArgumentsV3()
		if err == nil {
			launcherConfig, err = p.NativeLauncherConfigV3()
		}
	default:
		return nil, ErrInvalidConfiguration
	}
	if err != nil || !backend.Enabled {
		return nil, ErrInvalidConfiguration
	}
	if platform != manifest.OCI || manifest.OCI.Boundary != attachedworkerlocal.BoundaryLinuxRootlessNativeV3 ||
		backend.Executable != manifest.Harness.Executable || fmt.Sprintf("%x", backend.ExecutableDigest) != manifest.Harness.SHA256 {
		return nil, ErrArtifactMismatch
	}
	if !slices.Equal(arguments, manifest.Harness.Arguments) {
		return nil, ErrArtifactMismatch
	}
	if deps.hostOS != "linux" || operatingSystem != deps.hostOS || architecture != deps.hostArchitecture {
		return nil, ErrIsolationUnsupported
	}
	// The actual supervisor bounds may be tighter, never wider than the frozen
	// installation. No ambient variables or extra model-visible environment.
	if config.Timeout <= 0 || config.Timeout > launch.Limits.Active || config.TerminationGrace <= 0 ||
		config.TerminationGrace > launch.Limits.Cleanup || config.MaxStdoutBytes < 0 || config.MaxStdoutBytes > 16<<20 ||
		config.MaxStderrBytes < 0 || config.MaxStderrBytes > 1<<20 || !slices.Equal(config.AllowedEnvironmentNames, []string{"CODEX_HOME"}) {
		return nil, ErrInvalidConfiguration
	}
	root, err := privateCanonicalDirectory(config.ScratchRoot)
	if err != nil || root != config.ScratchRoot {
		return nil, ErrInvalidConfiguration
	}
	engineDigest, err := strictExecutableDigest(manifest.OCI.DockerPath, deps.digest)
	if err != nil || !digestMatches(engineDigest, manifest.OCI.DockerSHA256) {
		return nil, ErrArtifactMismatch
	}
	harnessDigest, err := strictExecutableDigest(manifest.Harness.Executable, deps.digest)
	if err != nil || !digestMatches(harnessDigest, manifest.Harness.SHA256) {
		return nil, ErrArtifactMismatch
	}
	for _, artifact := range []attachedworkerdaemon.NativeArtifactV2{launch.Bootstrap, launch.Relay, launch.Companion} {
		actual, err := strictExecutableDigest(artifact.Executable, deps.digest)
		if err != nil || actual != artifact.Digest {
			return nil, ErrArtifactMismatch
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	launcher, err := deps.newLauncher(ctx, launcherConfig)
	if err != nil {
		if errors.Is(err, attachedworkeroci.ErrUnsupported) {
			return nil, ErrIsolationUnsupported
		}
		return nil, errors.Join(ErrInvalidConfiguration, err)
	}
	if launcher == nil || launcher.ProfileV3().Validate() != nil {
		return nil, ErrIsolationUnsupported
	}
	residue, err := launcher.Reconcile(ctx)
	if err != nil || residue < 0 {
		return nil, errors.Join(ErrReconciliationRequired, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	supervisor, err := deps.newSupervisor(attachedworkerdaemon.SupervisorConfigV3{Base: attachedworkerdaemon.SupervisorConfig{
		ScratchRoot: root, Timeout: config.Timeout, TerminationGrace: config.TerminationGrace,
		MaxStdoutBytes: config.MaxStdoutBytes, MaxStderrBytes: config.MaxStderrBytes,
		AllowedEnvironmentNames: slices.Clone(config.AllowedEnvironmentNames), AllowedReadRoots: slices.Clone(config.AllowedReadRoots),
	}, Launcher: launcher})
	if err != nil || supervisor == nil || !supervisor.NativeV3() {
		return nil, errors.Join(ErrInvalidConfiguration, err)
	}
	var prepared *codexexec.PreparedAttachedWorkerRunnerV1
	outer := attachedworkerdaemon.InvocationRunnerConfig{CredentialFinalizeGrace: config.CredentialFinalizeGrace}
	if generation != nil {
		prepared, err = codexexec.NewNativePreparedAttachedWorkerRunnerV3(*generation, root, supervisor, credentials, outer)
	} else {
		prepared, err = codexexec.NewNativePreparedAttachedWorkerRunnerV2(*fixed, root, supervisor, credentials, outer)
	}
	if err != nil || prepared == nil {
		return nil, errors.Join(ErrInvalidConfiguration, err)
	}
	clock := backend.Now
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &Stack{runner: &pinnedRunner{delegate: prepared, executable: manifest.Harness.Executable, digest: harnessDigest, arguments: slices.Clone(arguments)},
		prepared: prepared, nativePrepared: true, nativeGeneration: generation, inlineOutputV2: inline, allowedEnvironmentNames: slices.Clone(config.AllowedEnvironmentNames), now: clock,
		nativeOperatingSystem: operatingSystem, nativeArchitecture: architecture, nativeHarnessVersion: backend.ExecutableVersion,
		scope: preparedStackScope{tenant: manifest.TenantID, owner: manifest.OwnerUserID, worker: manifest.WorkerID, enrollment: manifest.EnrollmentGeneration, connection: manifest.ConnectionGeneration},
		evidence: EvidenceV1{Version: evidenceVersionV1, ManifestRevision: manifest.Revision, Boundary: manifest.OCI.Boundary,
			IsolationProfile: launcher.ProfileV3().Name, ReconciledResidue: residue}}, nil
}

func defaultNativeDependenciesV3() nativeDependenciesV3 {
	return nativeDependenciesV3{hostOS: runtime.GOOS, hostArchitecture: runtime.GOARCH, digest: attachedworkerdaemon.DigestExecutable,
		newLauncher: newNativeLauncherV3, newSupervisor: attachedworkerdaemon.NewSupervisorV3}
}
