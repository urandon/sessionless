package codexexec

import (
	"context"
	"os"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegenerationsession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ownerlocalcredential"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// NativePreparedConfigV3 is the explicit generation-aware successor, not a
// mutable global Evidence bundle or a promotion of V2 qualification. Lifecycle
// and GenerationOwner must be the actual concrete owners used by this runner
// and connected Session. This configuration grants no real profile activation.
type NativePreparedConfigV3 struct {
	Profile         NativeProfileV3
	Baseline        domain.AttachedResourceGenerationBaselineV1
	GenerationOwner *attachedresourcegenerationsession.OwnerV1
	Lifecycle       *ownerlocalcredential.Lifecycle
	InlineOutputV2  bool
}

func (c NativePreparedConfigV3) Clone() (NativePreparedConfigV3, error) {
	if c.GenerationOwner == nil || c.Lifecycle == nil || c.Baseline.Validate() != nil {
		return NativePreparedConfigV3{}, ErrContract
	}
	p, err := freezeNativeProfileV3(c.Profile)
	if err != nil || c.Baseline.Binding.Backend != p.descriptor {
		return NativePreparedConfigV3{}, ErrContract
	}
	c.Profile, c.Baseline = p.profile, c.Baseline.Clone()
	return c, nil
}

func NewNativePreparedAttachedWorkerRunnerV3(config NativePreparedConfigV3, scratch string, supervisor *attachedworkerdaemon.Supervisor,
	credentials ports.CredentialLifecycle, outerConfig attachedworkerdaemon.InvocationRunnerConfig) (*PreparedAttachedWorkerRunnerV1, error) {
	lifecycle, ok := credentials.(*ownerlocalcredential.Lifecycle)
	if !ok || lifecycle == nil || lifecycle != config.Lifecycle || supervisor == nil || !supervisor.NativeV3() {
		return nil, ErrContract
	}
	owned, err := config.Clone()
	if err != nil {
		return nil, err
	}
	profile, err := freezeNativeProfileV3(owned.Profile)
	if err != nil {
		return nil, err
	}
	owner := &PreparedAttachedWorkerRunnerV1{config: owned.Profile.Backend, scratch: scratch, supervisor: supervisor,
		nativeConfigV3: &owned, nativeProfileV3: profile}
	owner.nativeIdentityV2 = owner
	outer, err := attachedworkerdaemon.NewPreparedInvocationRunner(outerConfig, &preparedExecutor{owner: owner}, lifecycle)
	if err != nil {
		return nil, err
	}
	owner.outer = outer
	return owner, nil
}

func (owner *PreparedAttachedWorkerRunnerV1) BindNativeSessionV3(session *attachedworkersession.Session) error {
	if owner == nil || session == nil || owner.nativeIdentityV2 != owner {
		return ErrContract
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.nativeConfigV3 == nil || owner.nativeConfigV2 != nil || owner.closed || owner.preparing || owner.slot != nil || owner.nativeSessionV2 != nil {
		return ErrAuthorityUnavailable
	}
	snapshot := session.Snapshot()
	b := owner.nativeConfigV3.Baseline
	if snapshot.State != attachedworkersession.StateReady || snapshot.TenantID != b.TenantID || snapshot.OwnerUserID != b.OwnerUserID ||
		snapshot.WorkerID != b.WorkerID || snapshot.EnrollmentGeneration != b.EnrollmentGeneration || snapshot.CapabilityDigest != b.CapabilityDigest ||
		snapshot.AuthenticationExpires == nil || !owner.nativeNowV2().Before(*snapshot.AuthenticationExpires) {
		return ErrAuthorityUnavailable
	}
	owner.nativeSessionV2 = session
	return nil
}

func (owner *PreparedAttachedWorkerRunnerV1) nativeSelected() bool {
	return owner != nil && (owner.nativeConfigV2 != nil || owner.nativeConfigV3 != nil)
}
func (owner *PreparedAttachedWorkerRunnerV1) nativeLaunch() attachedworkerdaemon.NativeLaunchV3 {
	if owner.nativeConfigV3 != nil && owner.nativeConfigV2 == nil && owner.nativeProfileV3 != nil {
		return owner.nativeProfileV3.launch
	}
	if owner.nativeConfigV2 != nil && owner.nativeConfigV3 == nil && owner.nativeProfileV2 != nil {
		return owner.nativeProfileV2.launch
	}
	return attachedworkerdaemon.NativeLaunchV3{}
}
func (owner *PreparedAttachedWorkerRunnerV1) nativeDescriptor() domain.HarnessBackendDescriptorV1 {
	if owner.nativeConfigV3 != nil && owner.nativeConfigV2 == nil && owner.nativeProfileV3 != nil {
		return owner.nativeProfileV3.descriptor
	}
	if owner.nativeConfigV2 != nil && owner.nativeConfigV3 == nil && owner.nativeProfileV2 != nil {
		return owner.nativeProfileV2.descriptor
	}
	return domain.HarnessBackendDescriptorV1{}
}
func (owner *PreparedAttachedWorkerRunnerV1) nativeInlineSelected() bool {
	return owner != nil && (owner.nativeConfigV3 != nil && owner.nativeConfigV2 == nil && owner.nativeConfigV3.InlineOutputV2 ||
		owner.nativeConfigV2 != nil && owner.nativeConfigV3 == nil && owner.nativeConfigV2.InlineOutputV2)
}

func (owner *PreparedAttachedWorkerRunnerV1) checkNativeGenerationV3(ctx context.Context, session *attachedworkersession.Session, resource domain.ProviderResourceBindingV1) error {
	if owner == nil || owner.nativeIdentityV2 != owner || owner.nativeConfigV3 == nil || owner.nativeConfigV2 != nil || session == nil {
		return ErrContract
	}
	c := owner.nativeConfigV3
	if c.GenerationOwner.CheckNewResourceForSessionV1(ctx, session, c.Lifecycle, c.Baseline, resource) != nil {
		return ErrAuthorityUnavailable
	}
	return nil
}

func (owner *PreparedAttachedWorkerRunnerV1) qualifyNativeGenerationV3(ctx context.Context, input *attachedworkerdaemon.VerifiedAttachedInputV1, session *attachedworkersession.Session) (domain.ProviderEvidenceBundleV1, error) {
	zero := domain.ProviderEvidenceBundleV1{}
	if input == nil || owner.checkNativeGenerationV3(ctx, session, input.Job.HarnessBinding.Resource) != nil {
		return zero, ErrAuthorityUnavailable
	}
	bundle, err := projectNativeGenerationEvidenceV3(owner.nativeProfileV3, owner.nativeConfigV3.Baseline, input, owner.nativeNowV2())
	if err != nil || verifyNativeGenerationArtifactsV3(ctx, owner.nativeProfileV3) != nil {
		return zero, ErrContract
	}
	return bundle, nil
}

func verifyNativeGenerationArtifactsV3(ctx context.Context, p *nativeFrozenProfileV3) error {
	if ctx == nil || ctx.Err() != nil || p == nil {
		return ErrContract
	}
	for _, artifact := range []attachedworkerdaemon.NativeArtifactV2{p.launch.Bootstrap, p.launch.Relay, p.launch.Companion, p.launch.Workload} {
		info, err := os.Lstat(artifact.Executable)
		if ctx.Err() != nil || err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 128<<20 || info.Mode().Perm()&0o022 != 0 || info.Mode().Perm()&0o111 == 0 {
			return ErrContract
		}
		actual, err := attachedworkerdaemon.DigestExecutable(artifact.Executable)
		if ctx.Err() != nil || err != nil || actual != artifact.Digest {
			return ErrContract
		}
	}
	return nil
}

func (owner *PreparedAttachedWorkerRunnerV1) nativeGenerationDriverV3(ctx context.Context, input *attachedworkerdaemon.VerifiedAttachedInputV1,
	session *attachedworkersession.Session, evidence domain.ProviderEvidenceBundleV1) (*Driver, error) {
	resolver, err := NewAcceptedAuthorityResolverV1(input.AcceptedAttempt, input.Job.HarnessBinding.Resource, owner.config.Now)
	if err != nil {
		return nil, err
	}
	boundary, err := newNativeGenerationPreparedProcessBoundaryV3(ctx, PreparedProcessBoundaryConfigV1{Supervisor: owner.supervisor, Authority: resolver.authority,
		Executable: owner.config.Executable, ExecutableDigest: owner.config.ExecutableDigest, Arguments: owner.nativeProfileV3.launch.NativeArguments[0], Now: owner.config.Now}, session, input, *owner.nativeConfigV3, evidence)
	if err != nil {
		return nil, err
	}
	driver, err := NewDriver(owner.config, resolver, boundary)
	if err != nil {
		return nil, err
	}
	driver.nativeProfileV3, driver.nativeInlineOutputV2 = boundary.native.profileV3, owner.nativeConfigV3.InlineOutputV2
	driver.descriptor = driver.nativeProfileV3.descriptor
	return driver, nil
}

func (driver *Driver) nativeSelected() bool {
	return driver != nil && (driver.nativeProfileV2 != nil || driver.nativeProfileV3 != nil)
}
func (driver *Driver) nativeLaunch() attachedworkerdaemon.NativeLaunchV3 {
	if driver.nativeProfileV3 != nil && driver.nativeProfileV2 == nil {
		return driver.nativeProfileV3.launch
	}
	if driver.nativeProfileV2 != nil && driver.nativeProfileV3 == nil {
		return driver.nativeProfileV2.launch
	}
	return attachedworkerdaemon.NativeLaunchV3{}
}
func (driver *Driver) sameNativeProfile(v *nativeConduitOwner) bool {
	return v != nil && v.fullNativeProfile() && (driver.nativeProfileV3 != nil && driver.nativeProfileV2 == nil && v.profileV3 == driver.nativeProfileV3 ||
		driver.nativeProfileV2 != nil && driver.nativeProfileV3 == nil && v.profileV2 == driver.nativeProfileV2)
}
func (driver *Driver) verifyNativeStage(ctx context.Context, s *nativeInputStageV2) error {
	if driver.nativeProfileV3 != nil && driver.nativeProfileV2 == nil {
		return s.verifyV3(ctx, driver.nativeProfileV3, driver.media)
	}
	if driver.nativeProfileV2 != nil && driver.nativeProfileV3 == nil {
		return s.verify(ctx, driver.nativeProfileV2, driver.media)
	}
	return ErrContract
}
