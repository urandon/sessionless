package codexexec

import (
	"context"
	"os"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// NewNativePreparedAttachedWorkerRunnerV2 selects a full profile, not another
// execution lifecycle. The original InvocationRunner remains the sole issuer,
// materializer, writeback and release owner. No shipped configuration selects it.
func NewNativePreparedAttachedWorkerRunnerV2(config NativePreparedConfigV2, scratch string, supervisor *attachedworkerdaemon.Supervisor,
	credentials ports.CredentialLifecycle, outerConfig attachedworkerdaemon.InvocationRunnerConfig) (*PreparedAttachedWorkerRunnerV1, error) {
	if supervisor == nil || !supervisor.NativeV3() || credentials == nil {
		return nil, ErrContract
	}
	owned, err := config.Clone()
	if err != nil {
		return nil, err
	}
	profile, err := freezeNativeProfileV2(owned.Profile)
	if err != nil {
		return nil, err
	}
	owner := &PreparedAttachedWorkerRunnerV1{config: owned.Profile.Backend, scratch: scratch, supervisor: supervisor,
		nativeConfigV2: &owned, nativeProfileV2: profile}
	owner.nativeIdentityV2 = owner
	outer, err := attachedworkerdaemon.NewPreparedInvocationRunner(outerConfig, &preparedExecutor{owner: owner}, credentials)
	if err != nil {
		return nil, err
	}
	owner.outer = outer
	return owner, nil
}

// BindNativeSessionV2 retains the actual authenticated Session passed by the
// existing Stack. A matching snapshot is not a replacement execution child.
func (owner *PreparedAttachedWorkerRunnerV1) BindNativeSessionV2(session *attachedworkersession.Session) error {
	if owner == nil || session == nil || owner.nativeIdentityV2 != owner {
		return ErrContract
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.nativeConfigV2 == nil || owner.closed || owner.preparing || owner.slot != nil || owner.nativeSessionV2 != nil {
		return ErrAuthorityUnavailable
	}
	snapshot := session.Snapshot()
	scope := owner.nativeConfigV2.Evidence.Policy.Scope
	if snapshot.State != attachedworkersession.StateReady || snapshot.TenantID != scope.TenantID || snapshot.OwnerUserID != scope.OwnerUserID ||
		snapshot.AuthenticationExpires == nil || !owner.nativeNowV2().Before(*snapshot.AuthenticationExpires) {
		return ErrAuthorityUnavailable
	}
	owner.nativeSessionV2 = session
	return nil
}

func (owner *PreparedAttachedWorkerRunnerV1) nativeNowV2() time.Time {
	if owner.config.Now != nil {
		return owner.config.Now().UTC()
	}
	return time.Now().UTC()
}

// The non-renewable original offer starts the admitted runtime. Preparation,
// Issue and process launch must not give the same job a new runtime window.
// The lease has an additional finalization allowance; it is not this deadline.
func acceptedNativeRuntimeDeadlineV2(input *attachedworkerdaemon.VerifiedAttachedInputV1, now time.Time) (time.Time, error) {
	if input == nil || input.AcceptedAttempt.CreatedAt.IsZero() || input.Job.Limits.MaxRuntime <= 0 || now.IsZero() {
		return time.Time{}, ErrContract
	}
	deadline := input.AcceptedAttempt.CreatedAt.Add(input.Job.Limits.MaxRuntime).UTC()
	if !now.Before(deadline) {
		return time.Time{}, ErrAuthorityUnavailable
	}
	return deadline, nil
}

func verifyNativeArtifactsV2(ctx context.Context, profile *nativeFrozenProfileV2) error {
	if ctx == nil || ctx.Err() != nil || profile == nil {
		return ErrContract
	}
	for _, artifact := range []attachedworkerdaemon.NativeArtifactV2{profile.launch.Bootstrap, profile.launch.Relay, profile.launch.Companion, profile.launch.Workload} {
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

func (owner *PreparedAttachedWorkerRunnerV1) nativeDriverV2(ctx context.Context, input *attachedworkerdaemon.VerifiedAttachedInputV1, session *attachedworkersession.Session) (*Driver, error) {
	resolver, err := NewAcceptedAuthorityResolverV1(input.AcceptedAttempt, input.Job.HarnessBinding.Resource, owner.config.Now)
	if err != nil {
		return nil, err
	}
	boundary, err := NewNativePreparedProcessBoundaryV3(ctx, PreparedProcessBoundaryConfigV1{Supervisor: owner.supervisor, Authority: resolver.authority,
		Executable: owner.config.Executable, ExecutableDigest: owner.config.ExecutableDigest,
		Arguments: owner.nativeProfileV2.launch.NativeArguments[0], Now: owner.config.Now}, session, input, *owner.nativeConfigV2)
	if err != nil {
		return nil, err
	}
	driver, err := NewDriver(owner.config, resolver, boundary)
	if err != nil {
		return nil, err
	}
	driver.nativeProfileV2 = boundary.native.profileV2
	driver.descriptor = driver.nativeProfileV2.descriptor
	return driver, nil
}
