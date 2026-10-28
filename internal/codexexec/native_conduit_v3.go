package codexexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// NewNativePreparedProcessBoundaryV3 is an explicit entry to the SAME prepared
// boundary and Session child. It accepts no caller-created execution readset;
// that is derived after this original entry consumes its invocation.
func NewNativePreparedProcessBoundaryV3(ctx context.Context, config PreparedProcessBoundaryConfigV1,
	session *attachedworkersession.Session, input *attachedworkerdaemon.VerifiedAttachedInputV1,
	native NativePreparedConfigV2) (*PreparedProcessBoundaryV1, error) {
	if config.Supervisor == nil || !config.Supervisor.NativeV3() {
		return nil, ErrContract
	}
	profile, err := freezeNativeProfileV2(native.Profile)
	if err != nil || config.Executable != profile.profile.Backend.Executable || config.ExecutableDigest != profile.profile.Backend.ExecutableDigest ||
		!slices.Equal(config.Arguments, profile.launch.NativeArguments[0]) {
		return nil, ErrContract
	}
	boundary, err := newPreparedProcessBoundary(config)
	if err != nil {
		return nil, err
	}
	owner, err := newNativeConduitOwnerV3(ctx, session, input, config.Authority, config.Supervisor, profile, native, boundary.now().UTC())
	if err != nil {
		return nil, err
	}
	boundary.native, boundary.nativeIdentity = owner, boundary
	bound, err := owner.child.BindPreparedEntryV2(boundary)
	if err != nil {
		return nil, ErrAuthorityUnavailable
	}
	original, ok := bound.(*PreparedProcessBoundaryV1)
	if !ok || original == nil || original.nativeIdentity != original || original.native == nil || original.native.profileV2 == nil ||
		original.native.self != original.native || original.native.child != owner.child || original.supervisor != boundary.supervisor ||
		original.authority != boundary.authority || original.executable != boundary.executable || original.digest != boundary.digest ||
		!slices.Equal(original.arguments, boundary.arguments) || original.native.profileV2.descriptor != profile.descriptor ||
		!reflect.DeepEqual(original.native.bundleV3, owner.bundleV3) || !reflect.DeepEqual(original.native.evidenceV2, owner.evidenceV2) || owner.child.Current() != nil {
		return nil, ErrAuthorityUnavailable
	}
	return original, nil
}

func newNativeConduitOwnerV3(ctx context.Context, session *attachedworkersession.Session, input *attachedworkerdaemon.VerifiedAttachedInputV1,
	authority AuthorityV1, supervisor *attachedworkerdaemon.Supervisor, profile *nativeFrozenProfileV2, config NativePreparedConfigV2, now time.Time) (*nativeConduitOwner, error) {
	if ctx == nil || ctx.Err() != nil || session == nil || input == nil || authority.Validate() != nil || supervisor == nil || !supervisor.NativeV3() || profile == nil {
		return nil, ErrContract
	}
	a := input.AcceptedAttempt
	identity := attachedworkerdaemon.InvocationIdentity{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, WorkerID: a.WorkerID,
		RunID: a.RunID, AttemptID: a.AttemptID, LeaseID: a.LeaseID, FenceToken: a.LeaseGeneration}
	if input.ValidateFor(identity) != nil || validateNativeEnvelopeEvidenceV2(profile, config.Evidence, input.Job.HarnessBinding, now) != nil ||
		nativeCredentialCurrentV2(ctx, config.CredentialBindings, authority, now) != nil {
		return nil, ErrContract
	}
	runtimeDeadline, err := acceptedNativeRuntimeDeadlineV2(input, now)
	if err != nil {
		return nil, err
	}
	resolver, err := NewAcceptedAuthorityResolverV1(a, input.Job.HarnessBinding.Resource, nil)
	if err != nil || resolver.authority != authority {
		return nil, ErrContract
	}
	bundle := profile.launch.Clone()
	if bundle.StaticProfileDigest == (attachedworkerdaemon.ExecutableDigest{}) {
		return nil, ErrContract
	}
	host, err := verifyNativeCompanionHost(bundle.Companion)
	if err != nil {
		return nil, ErrContract
	}
	raw, err := json.Marshal(struct {
		Version  uint32
		Attempt  domain.AttachedWorkerAttemptV1
		Resource domain.ProviderResourceBindingV1
	}{3, a, input.Job.HarnessBinding.Resource})
	if err != nil {
		return nil, ErrContract
	}
	bundle.ScopeDigest = attachedworkerdaemon.ExecutableDigest(sha256.Sum256(raw))
	policy, err := hex.DecodeString(string(a.PolicyDigest))
	actual, policyErr := bundle.InstallPolicyDigestV3()
	if err != nil || len(policy) != sha256.Size || policyErr != nil || !bytes.Equal(policy, actual[:]) || actual != bundle.PolicyDigest || bundle.Validate() != nil {
		return nil, ErrContract
	}
	child, err := session.BorrowNativeAttemptV2(ctx, a, input.Job, input.Manifest)
	if err != nil {
		return nil, ErrAuthorityUnavailable
	}
	v := &nativeConduitOwner{child: child, authority: authority, supervisor: supervisor, attempt: a, hostIdentity: host,
		profileV2: profile, bundleV3: &bundle, evidenceV2: config.Evidence.Clone(), credentialBindingsV2: config.CredentialBindings,
		bindingV2: input.Job.HarnessBinding.Clone(), nowV2: config.Profile.Backend.Now, runtimeDeadlineV2: runtimeDeadline}
	v.self = v
	return v, nil
}

func (v *nativeConduitOwner) nativeNowV2() time.Time {
	if v.nowV2 != nil {
		return v.nowV2().UTC()
	}
	return time.Now().UTC()
}

func (v *nativeConduitOwner) currentEnvelopeV2(ctx context.Context, now time.Time) error {
	if v == nil || v.self != v {
		return ErrAuthorityUnavailable
	}
	if v.bundleV3 == nil {
		return nil
	} // Existing V2 authority contract is unchanged.
	if ctx == nil || ctx.Err() != nil || v.child == nil || v.child.Current() != nil ||
		v.runtimeDeadlineV2.IsZero() || !now.Before(v.runtimeDeadlineV2) ||
		v.validateNativeEvidence(now) != nil || v.checkGenerationInput(ctx) != nil ||
		nativeCredentialCurrentV2(ctx, v.credentialBindingsV2, v.authority, now) != nil || ctx.Err() != nil {
		return ErrAuthorityUnavailable
	}
	return nil
}

func (v *nativeConduitOwner) conduitLimits() attachedworkerdaemon.NativeConduitLimitsV2 {
	if v.bundleV3 != nil {
		return v.bundleV3.Limits
	}
	return v.bundle.Limits
}
func (v *nativeConduitOwner) conduitCompanion() attachedworkerdaemon.NativeArtifactV2 {
	if v.bundleV3 != nil {
		return v.bundleV3.Companion
	}
	return v.bundle.Companion
}
func (v *nativeConduitOwner) conduitHosts() []string {
	if v.bundleV3 != nil {
		return v.bundleV3.ProviderHosts
	}
	return v.bundle.ProviderHosts
}
func (v *nativeConduitOwner) conduitPolicyValid() bool {
	if v.bundleV3 != nil {
		h, err := v.bundleV3.InstallPolicyDigestV3()
		return err == nil && h == v.bundleV3.PolicyDigest && v.fullNativeProfile() && v.bundleV3.StaticProfileDigest == v.nativeLaunch().StaticProfileDigest
	}
	h, err := v.bundle.InstallPolicyDigestV2()
	return err == nil && h == v.bundle.PolicyDigest
}
func (v *nativeConduitOwner) conduitSocketMatches(socket *attachedworkerdaemon.NativeSocketV2) bool {
	if v.bundleV3 != nil {
		return socket.MatchesPolicyV2(v.bundleV3.ScopeDigest, v.bundleV3.PolicyDigest) && socket.MatchesExecutionV3(v.executionV3)
	}
	return socket.MatchesPolicyV2(v.bundle.ScopeDigest, v.bundle.PolicyDigest)
}
