package codexexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// nativeConduitOwner is a child of the actual connected Session, not a caller
// predicate or another execution reservation. Only the existing prepared
// boundary's consumed entry may invoke it. No shipped profile constructs it.
type nativeConduitOwner struct {
	self         *nativeConduitOwner
	child        *attachedworkersession.NativeAttemptV2
	authority    AuthorityV1
	bundle       attachedworkerdaemon.NativeLaunchV2
	supervisor   *attachedworkerdaemon.Supervisor
	attempt      domain.AttachedWorkerAttemptV1
	mu           sync.Mutex
	companion    *nativeCompanion
	hostIdentity os.FileInfo
	// V3 is selected only by the explicit full-profile constructor. It retains
	// the same child, prepared consumed entry, companion and counters.
	profileV2            *nativeFrozenProfileV2
	profileV3            *nativeFrozenProfileV3
	generationConfigV3   *NativePreparedConfigV3
	generationSessionV3  *attachedworkersession.Session
	bundleV3             *attachedworkerdaemon.NativeLaunchV3
	evidenceV2           domain.ProviderEvidenceBundleV1
	credentialBindingsV2 NativeCredentialBindingReaderV2
	stageV2              *nativeInputStageV2
	executionV3          *attachedworkerdaemon.NativeExecutionV3
	bindingV2            domain.HarnessBindingV2
	nowV2                func() time.Time
	runtimeDeadlineV2    time.Time
}

func newNativeConduitOwner(ctx context.Context, session *attachedworkersession.Session, input *attachedworkerdaemon.VerifiedAttachedInputV1,
	authority AuthorityV1, supervisor *attachedworkerdaemon.Supervisor, bundle attachedworkerdaemon.NativeLaunchV2) (*nativeConduitOwner, error) {
	if ctx == nil || ctx.Err() != nil || session == nil || input == nil || authority.Validate() != nil || supervisor == nil || !supervisor.NativeV2() {
		return nil, ErrContract
	}
	hostIdentity, err := verifyNativeCompanionHost(bundle.Companion)
	if err != nil {
		return nil, ErrContract
	}
	a := input.AcceptedAttempt
	identity := attachedworkerdaemon.InvocationIdentity{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, WorkerID: a.WorkerID,
		RunID: a.RunID, AttemptID: a.AttemptID, LeaseID: a.LeaseID, FenceToken: a.LeaseGeneration}
	if input.ValidateFor(identity) != nil {
		return nil, ErrContract
	}
	resolver, err := NewAcceptedAuthorityResolverV1(a, input.Job.HarnessBinding.Resource, nil)
	if err != nil || resolver.authority != authority {
		return nil, ErrContract
	}
	// These source associations are derived here, never taken from bundle data.
	raw, err := json.Marshal(struct {
		Version  uint32
		Attempt  domain.AttachedWorkerAttemptV1
		Resource domain.ProviderResourceBindingV1
	}{2, a, input.Job.HarnessBinding.Resource})
	if err != nil {
		return nil, ErrContract
	}
	bundle.ScopeDigest = attachedworkerdaemon.ExecutableDigest(sha256.Sum256(raw))
	policy, err := hex.DecodeString(string(a.PolicyDigest))
	if err != nil || len(policy) != sha256.Size {
		return nil, ErrContract
	}
	installPolicy, err := bundle.InstallPolicyDigestV2()
	if err != nil || !bytes.Equal(installPolicy[:], policy) {
		return nil, ErrContract
	}
	bundle.PolicyDigest = installPolicy
	if bundle.Validate() != nil {
		return nil, ErrContract
	}
	child, err := session.BorrowNativeAttemptV2(ctx, a, input.Job, input.Manifest)
	if err != nil {
		return nil, ErrAuthorityUnavailable
	}
	v := &nativeConduitOwner{child: child, authority: authority, bundle: bundle.Clone(), supervisor: supervisor, attempt: a, hostIdentity: hostIdentity}
	v.self = v
	return v, nil
}

func (v *nativeConduitOwner) run(ctx context.Context, spec attachedworkerdaemon.AttemptSpec, invocation ProcessInvocationV1) (attachedworkerdaemon.AttemptResult, error) {
	if v == nil || v.self != v || v.child == nil || invocation.Authority != v.authority || v.child.Current() != nil {
		return attachedworkerdaemon.AttemptResult{}, ErrAuthorityUnavailable
	}
	life, err := v.child.Lifetime()
	if err != nil {
		return attachedworkerdaemon.AttemptResult{}, ErrAuthorityUnavailable
	}
	call, cancel := context.WithTimeout(ctx, v.conduitLimits().Active)
	stop := context.AfterFunc(life, cancel)
	// The original prepared entry retires the child after its terminal
	// cancellation/current-authority arbitration. Retiring here would make a
	// normal return indistinguishable from authority lost before completion.
	defer func() { stop(); cancel() }()
	if life.Err() != nil || v.child.Current() != nil {
		return attachedworkerdaemon.AttemptResult{}, ErrAuthorityUnavailable
	}
	companion, err := v.newCompanion(call, nativeCompanionNetwork())
	if err != nil {
		return attachedworkerdaemon.AttemptResult{}, err
	}
	// The supervisor owns the same companion through preparation, stop and
	// release, inside its original aggregate cleanup window.
	if v.bundleV3 != nil {
		if v.stageV2 == nil || v.executionV3 == nil || v.executionV3.ValidateV3(companion.ctx, *v.bundleV3) != nil ||
			v.currentEnvelopeV2(companion.ctx, v.nativeNowV2()) != nil {
			return attachedworkerdaemon.AttemptResult{}, ErrContract
		}
		v.stageV2.dispatched = true
		actual, runErr := v.supervisor.RunNativeV3(companion.ctx, spec, *v.bundleV3, v.executionV3, companion)
		v.stageV2.quiesced = actual.DescendantsReaped && actual.BoundaryReleased && actual.CleanupSucceeded
		// Sample full eligibility after the actual original stop/release, while
		// the SAME active/caller/job deadline still bounds the metadata read.
		// No fresh timeout, child or grant is created. A lost entitlement cannot
		// publish success; acceptance stays unknown/observed, consumption stays.
		if runErr == nil {
			if err := v.currentEnvelopeV2(call, v.nativeNowV2()); err != nil {
				runErr = errors.Join(ErrAuthorityUnavailable, err)
			}
		}
		return actual, runErr
	}
	return v.supervisor.RunNativeV2(companion.ctx, spec, v.bundle, companion)
}
