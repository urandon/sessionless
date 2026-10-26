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
	if ctx == nil || credentials == nil || config.PreparedCodexNative == nil || config.PreparedCodex != nil || config.PreparedCodexMedia != nil ||
		manifest.Validate() != nil || manifest.Lifecycle != attachedworkerlocal.LifecycleActive || deps.digest == nil || deps.newLauncher == nil || deps.newSupervisor == nil {
		return nil, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	native, err := config.PreparedCodexNative.Clone()
	if err != nil || !native.Profile.Backend.Enabled {
		return nil, ErrInvalidConfiguration
	}
	profile := native.Profile
	if profile.Platform != manifest.OCI || manifest.OCI.Boundary != attachedworkerlocal.BoundaryLinuxRootlessNativeV3 ||
		profile.Backend.Executable != manifest.Harness.Executable || fmt.Sprintf("%x", profile.Backend.ExecutableDigest) != manifest.Harness.SHA256 {
		return nil, ErrArtifactMismatch
	}
	arguments, err := profile.ArgumentsV2()
	if err != nil || !slices.Equal(arguments, manifest.Harness.Arguments) {
		return nil, ErrArtifactMismatch
	}
	if deps.hostOS != "linux" || profile.OperatingSystem != deps.hostOS || profile.Architecture != deps.hostArchitecture {
		return nil, ErrIsolationUnsupported
	}
	// The actual supervisor bounds may be tighter, never wider than the frozen
	// installation. No ambient variables or extra model-visible environment.
	if config.Timeout <= 0 || config.Timeout > profile.Launch.Limits.Active || config.TerminationGrace <= 0 ||
		config.TerminationGrace > profile.Launch.Limits.Cleanup || config.MaxStdoutBytes < 0 || config.MaxStdoutBytes > 16<<20 ||
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
	for _, artifact := range []attachedworkerdaemon.NativeArtifactV2{profile.Launch.Bootstrap, profile.Launch.Relay, profile.Launch.Companion} {
		actual, err := strictExecutableDigest(artifact.Executable, deps.digest)
		if err != nil || actual != artifact.Digest {
			return nil, ErrArtifactMismatch
		}
	}
	launcherConfig, err := profile.NativeLauncherConfigV3()
	if err != nil {
		return nil, ErrInvalidConfiguration
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
	prepared, err := codexexec.NewNativePreparedAttachedWorkerRunnerV2(native, root, supervisor, credentials,
		attachedworkerdaemon.InvocationRunnerConfig{CredentialFinalizeGrace: config.CredentialFinalizeGrace})
	if err != nil || prepared == nil {
		return nil, errors.Join(ErrInvalidConfiguration, err)
	}
	clock := profile.Backend.Now
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &Stack{runner: &pinnedRunner{delegate: prepared, executable: manifest.Harness.Executable, digest: harnessDigest, arguments: slices.Clone(arguments)},
		prepared: prepared, nativePrepared: true, inlineOutputV2: native.InlineOutputV2, allowedEnvironmentNames: slices.Clone(config.AllowedEnvironmentNames), now: clock,
		nativeOperatingSystem: profile.OperatingSystem, nativeArchitecture: profile.Architecture, nativeHarnessVersion: profile.Backend.ExecutableVersion,
		scope: preparedStackScope{tenant: manifest.TenantID, owner: manifest.OwnerUserID, worker: manifest.WorkerID, enrollment: manifest.EnrollmentGeneration, connection: manifest.ConnectionGeneration},
		evidence: EvidenceV1{Version: evidenceVersionV1, ManifestRevision: manifest.Revision, Boundary: manifest.OCI.Boundary,
			IsolationProfile: launcher.ProfileV3().Name, ReconciledResidue: residue}}, nil
}

func defaultNativeDependenciesV3() nativeDependenciesV3 {
	return nativeDependenciesV3{hostOS: runtime.GOOS, hostArchitecture: runtime.GOARCH, digest: attachedworkerdaemon.DigestExecutable,
		newLauncher: newNativeLauncherV3, newSupervisor: attachedworkerdaemon.NewSupervisorV3}
}
