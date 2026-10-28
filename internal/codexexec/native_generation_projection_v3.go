package codexexec

import (
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// projectNativeGenerationEvidenceV3 is private consistency checking, NOT an
// accepted-attempt or credential grant. The normal runner must separately
// prove its original Session/ACK/SAME lifecycle before Issue and retain this
// independent bundle in that original attempt's slot. Never use it to rewrite
// a job, refresh evidence validity or authorize an old-N terminal operation.
func projectNativeGenerationEvidenceV3(profile *nativeFrozenProfileV3, baseline domain.AttachedResourceGenerationBaselineV1,
	input *attachedworkerdaemon.VerifiedAttachedInputV1, now time.Time) (domain.ProviderEvidenceBundleV1, error) {
	zero := domain.ProviderEvidenceBundleV1{}
	if profile == nil || input == nil || now.IsZero() || baseline.Validate() != nil {
		return zero, ErrContract
	}
	a := input.AcceptedAttempt
	identity := attachedworkerdaemon.InvocationIdentity{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, WorkerID: a.WorkerID,
		RunID: a.RunID, AttemptID: a.AttemptID, LeaseID: a.LeaseID, FenceToken: a.LeaseGeneration}
	if input.ValidateFor(identity) != nil || baseline.TenantID != a.TenantID || baseline.OwnerUserID != a.OwnerUserID ||
		baseline.WorkerID != a.WorkerID || baseline.EnrollmentGeneration != a.EnrollmentGeneration || baseline.CapabilityDigest != a.CapabilityDigest ||
		baseline.SubscriptionConnectionID != domain.SubscriptionConnectionID(input.Job.HarnessBinding.Resource.ResourceID) ||
		baseline.BaselineRevision != input.Job.HarnessBinding.Resource.Revision || baseline.Binding.Backend != profile.descriptor ||
		!now.Before(a.LeaseExpiresAt) {
		return zero, ErrContract
	}
	if _, err := acceptedNativeRuntimeDeadlineV2(input, now); err != nil {
		return zero, ErrContract
	}
	binding, bundle, err := domain.ProjectAttachedResourceGenerationV1(baseline, input.Job.HarnessBinding.Resource.CredentialGeneration)
	if err != nil {
		return zero, ErrContract
	}
	placement := domain.ExecutionPlacementV2{Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: baseline.OwnerUserID, WorkerID: baseline.WorkerID,
		CapabilityDigest: baseline.CapabilityDigest, PolicyDigest: domain.AttachedWorkerPolicyDigest(binding.EffectivePolicyDigest)}
	digest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		return zero, ErrContract
	}
	binding.RunID, binding.AttemptID, binding.ExecutionPlacementDigest = a.RunID, a.AttemptID, string(digest)
	// Compare ALL sealed fields, not only the dynamic policy or six digests.
	// This includes the exact original minimum expiry and resource revision.
	if placement != input.Job.ExecutionPlacementV2 || a.PolicyDigest != placement.PolicyDigest ||
		!reflect.DeepEqual(binding, input.Job.HarnessBinding) ||
		validateNativeEnvelopeEvidenceV3(profile, bundle, binding, now) != nil {
		return zero, ErrContract
	}
	// Common Project already cloned the full baseline, preserving nil/empty
	// collection distinctions that participate in the existing evidence hashes.
	return bundle, nil
}
