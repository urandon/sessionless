package codexexec

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"slices"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Only the explicit generation-aware runner calls this closed successor.
// It borrows the SAME original NativeAttempt/prepared entry; no lifecycle or
// reservation is introduced. Evidence was privately projected before Issue.
func newNativeGenerationPreparedProcessBoundaryV3(ctx context.Context, config PreparedProcessBoundaryConfigV1,
	session *attachedworkersession.Session, input *attachedworkerdaemon.VerifiedAttachedInputV1,
	native NativePreparedConfigV3, evidence domain.ProviderEvidenceBundleV1) (*PreparedProcessBoundaryV1, error) {
	if ctx == nil || ctx.Err() != nil || input == nil || session == nil || config.Supervisor == nil || !config.Supervisor.NativeV3() {
		return nil, ErrContract
	}
	owned, err := native.Clone()
	if err != nil {
		return nil, err
	}
	profile, err := freezeNativeProfileV3(owned.Profile)
	if err != nil || config.Executable != profile.profile.Backend.Executable || config.ExecutableDigest != profile.profile.Backend.ExecutableDigest ||
		!slices.Equal(config.Arguments, profile.launch.NativeArguments[0]) {
		return nil, ErrContract
	}
	boundary, err := newPreparedProcessBoundary(config)
	if err != nil {
		return nil, err
	}
	now := boundary.now().UTC()
	projected, err := projectNativeGenerationEvidenceV3(profile, owned.Baseline, input, now)
	if err != nil || !reflect.DeepEqual(evidence, projected) ||
		owned.GenerationOwner.CheckNewResourceForSessionV1(ctx, session, owned.Lifecycle, owned.Baseline, input.Job.HarnessBinding.Resource) != nil ||
		nativeCredentialCurrentV2(ctx, owned.Lifecycle, config.Authority, now) != nil {
		return nil, ErrAuthorityUnavailable
	}
	a := input.AcceptedAttempt
	resolver, err := NewAcceptedAuthorityResolverV1(a, input.Job.HarnessBinding.Resource, nil)
	if err != nil || resolver.authority != config.Authority {
		return nil, ErrContract
	}
	deadline, err := acceptedNativeRuntimeDeadlineV2(input, now)
	if err != nil {
		return nil, err
	}
	bundle := profile.launch.Clone()
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
	// Accepted dynamic policy stays sealed in authority/binding. Installation
	// policy is independently recomputed from the descriptor-pinned launch.
	static, err := bundle.InstallPolicyDigestV3()
	if err != nil || static != bundle.PolicyDigest || bundle.Validate() != nil {
		return nil, ErrContract
	}
	child, err := session.BorrowNativeAttemptV2(ctx, a, input.Job, input.Manifest)
	if err != nil {
		return nil, ErrAuthorityUnavailable
	}
	v := &nativeConduitOwner{child: child, authority: config.Authority, supervisor: config.Supervisor, attempt: a, hostIdentity: host,
		profileV3: profile, bundleV3: &bundle, evidenceV2: projected, credentialBindingsV2: owned.Lifecycle,
		bindingV2: input.Job.HarnessBinding.Clone(), nowV2: owned.Profile.Backend.Now, runtimeDeadlineV2: deadline,
		generationConfigV3: &owned, generationSessionV3: session}
	v.self = v
	boundary.native, boundary.nativeIdentity = v, boundary
	bound, err := child.BindPreparedEntryV2(boundary)
	if err != nil {
		return nil, ErrAuthorityUnavailable
	}
	original, ok := bound.(*PreparedProcessBoundaryV1)
	if !ok || original == nil || original.nativeIdentity != original || original.native == nil || original.native.self != original.native ||
		original.native.child != child || original.supervisor != boundary.supervisor || original.authority != boundary.authority ||
		original.executable != boundary.executable || original.digest != boundary.digest || !slices.Equal(original.arguments, boundary.arguments) ||
		original.native.profileV3 == nil || original.native.profileV2 != nil || original.native.profileV3.descriptor != profile.descriptor ||
		original.native.generationSessionV3 != session || original.native.generationConfigV3 == nil ||
		original.native.generationConfigV3.GenerationOwner != owned.GenerationOwner || original.native.generationConfigV3.Lifecycle != owned.Lifecycle ||
		!reflect.DeepEqual(original.native.generationConfigV3.Baseline, owned.Baseline) || !reflect.DeepEqual(original.native.bundleV3, &bundle) ||
		!reflect.DeepEqual(original.native.evidenceV2, projected) || child.Current() != nil {
		return nil, ErrAuthorityUnavailable
	}
	return original, nil
}

func (v *nativeConduitOwner) fullNativeProfile() bool {
	return v != nil && (v.profileV2 != nil) != (v.profileV3 != nil)
}
func (v *nativeConduitOwner) nativeDescriptor() domain.HarnessBackendDescriptorV1 {
	if !v.fullNativeProfile() {
		return domain.HarnessBackendDescriptorV1{}
	}
	if v.profileV3 != nil {
		return v.profileV3.descriptor
	}
	return v.profileV2.descriptor
}
func (v *nativeConduitOwner) nativeLaunch() attachedworkerdaemon.NativeLaunchV3 {
	if !v.fullNativeProfile() {
		return attachedworkerdaemon.NativeLaunchV3{}
	}
	if v.profileV3 != nil {
		return v.profileV3.launch
	}
	return v.profileV2.launch
}
func (v *nativeConduitOwner) validateNativeEvidence(now time.Time) error {
	if !v.fullNativeProfile() {
		return ErrContract
	}
	if v.profileV3 != nil {
		return validateNativeEnvelopeEvidenceV3(v.profileV3, v.evidenceV2, v.bindingV2, now)
	}
	return validateNativeEnvelopeEvidenceV2(v.profileV2, v.evidenceV2, v.bindingV2, now)
}
func (v *nativeConduitOwner) verifyNativeStage(ctx context.Context, s *nativeInputStageV2, media *mediaInput) error {
	if !v.fullNativeProfile() || s == nil {
		return ErrContract
	}
	if v.profileV3 != nil {
		return s.verifyV3(ctx, v.profileV3, media)
	}
	return s.verify(ctx, v.profileV2, media)
}
func (v *nativeConduitOwner) checkGenerationInput(ctx context.Context) error {
	if v.profileV3 == nil {
		return nil
	} // original V2 contract remains exact
	c := v.generationConfigV3
	if c == nil || v.generationSessionV3 == nil || c.Lifecycle == nil || c.GenerationOwner == nil ||
		c.GenerationOwner.CheckNewResourceForSessionV1(ctx, v.generationSessionV3, c.Lifecycle, c.Baseline, v.authority.ProviderResource) != nil {
		return ErrAuthorityUnavailable
	}
	return nil
}
