package domain

import "time"

// ServerlessInvocationAuthorityV1 is assembled from canonical state only
// after the lease is claimed. Queue delivery fields are not authority.
type ServerlessInvocationAuthorityV1 struct {
	Version               uint32                 `json:"version"`
	HarnessBinding        HarnessBindingV1       `json:"harness_binding"`
	ExecutionPlacementV2  ExecutionPlacementV2   `json:"execution_placement"`
	SubstrateBinding      SubstrateBindingV1     `json:"substrate_binding"`
	AdmissionCostCeiling  AdmissionCostCeilingV1 `json:"admission_cost_ceiling"`
	Lease                 Lease                  `json:"lease"`
	ContextManifestDigest string                 `json:"context_manifest_digest"`
	InputManifestDigest   string                 `json:"input_manifest_digest"`
	InvocationDeadline    time.Time              `json:"invocation_deadline"`
}

func (value ServerlessInvocationAuthorityV1) Clone() ServerlessInvocationAuthorityV1 {
	clone := value
	clone.HarnessBinding = value.HarnessBinding.Clone()
	clone.AdmissionCostCeiling = value.AdmissionCostCeiling.Clone()
	return clone
}

func (value ServerlessInvocationAuthorityV1) Validate() error {
	if value.Version != ServerlessInvocationAuthorityVersionV1 {
		return ValidationError{Field: "serverless_invocation_authority.version", Reason: "must equal 1"}
	}
	if err := value.ExecutionPlacementV2.Validate(); err != nil {
		return err
	}
	if value.ExecutionPlacementV2.Version != ExecutionPlacementVersionV2 || value.ExecutionPlacementV2.Kind != ExecutionPlacementManaged {
		return ValidationError{Field: "serverless_invocation_authority.execution_placement", Reason: "must be explicit managed version 2"}
	}
	if err := value.SubstrateBinding.Validate(); err != nil {
		return err
	}
	substrateDigest, err := value.SubstrateBinding.Digest()
	if err != nil {
		return err
	}
	if value.ExecutionPlacementV2.SubstrateBindingDigest != string(substrateDigest) {
		return ValidationError{Field: "serverless_invocation_authority.execution_placement", Reason: "must seal the exact substrate binding"}
	}
	if err := value.AdmissionCostCeiling.Validate(); err != nil {
		return err
	}
	costDigest, err := value.AdmissionCostCeiling.Digest()
	if err != nil {
		return err
	}
	if value.SubstrateBinding.AdmissionCostCeilingDigest != costDigest {
		return ValidationError{Field: "serverless_invocation_authority.admission_cost_ceiling", Reason: "must match the sealed substrate cost authority"}
	}
	if err := value.HarnessBinding.ValidateForScope(value.Lease.TenantID, value.HarnessBinding.OwnerUserID, value.Lease.RunID, value.Lease.AttemptID, value.ExecutionPlacementV2); err != nil {
		return err
	}
	if err := validateServerlessLease(value.Lease); err != nil {
		return err
	}
	if value.HarnessBinding.TenantID != value.Lease.TenantID || value.HarnessBinding.RunID != value.Lease.RunID || value.HarnessBinding.AttemptID != value.Lease.AttemptID {
		return ValidationError{Field: "serverless_invocation_authority.lease", Reason: "must match the sealed harness scope"}
	}
	if err := validateDigestToken("serverless_invocation_authority.context_manifest_digest", value.ContextManifestDigest); err != nil {
		return err
	}
	if err := validateDigestToken("serverless_invocation_authority.input_manifest_digest", value.InputManifestDigest); err != nil {
		return err
	}
	if value.InvocationDeadline.IsZero() || !value.InvocationDeadline.After(value.Lease.AcquiredAt) || value.InvocationDeadline.After(value.Lease.ExpiresAt) {
		return ValidationError{Field: "serverless_invocation_authority.invocation_deadline", Reason: "must fit inside the exact lease"}
	}
	return nil
}

func (value ServerlessInvocationAuthorityV1) ValidateAt(now time.Time) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if now.IsZero() || !value.Lease.ActiveAt(now.UTC()) || !now.UTC().Before(value.InvocationDeadline.UTC()) {
		return ValidationError{Field: "serverless_invocation_authority", Reason: "lease and invocation deadline must be active"}
	}
	if err := value.HarnessBinding.ValidateAt(now.UTC()); err != nil {
		return err
	}
	if err := value.SubstrateBinding.ValidateAt(now.UTC()); err != nil {
		return err
	}
	return value.AdmissionCostCeiling.ValidateAt(now.UTC())
}

func (value ServerlessInvocationAuthorityV1) Digest() (ServerlessInvocationAuthorityDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	harnessDigest, _ := value.HarnessBinding.Digest()
	placementDigest, _ := ExecutionPlacementDigest(value.ExecutionPlacementV2)
	substrateDigest, _ := value.SubstrateBinding.Digest()
	costDigest, _ := value.AdmissionCostCeiling.Digest()
	digest := newServerlessDigest("sessionless.serverless-invocation-authority.v1")
	digest.uint(uint64(value.Version))
	for _, field := range []string{string(harnessDigest), string(placementDigest), string(substrateDigest), string(costDigest), string(value.Lease.ID), string(value.Lease.TenantID), string(value.Lease.RunID), string(value.Lease.AttemptID), value.Lease.WorkerID} {
		digest.str(field)
	}
	digest.uint(value.Lease.FenceToken)
	digest.instant(value.Lease.AcquiredAt)
	digest.instant(value.Lease.ExpiresAt)
	digest.str(value.ContextManifestDigest)
	digest.str(value.InputManifestDigest)
	digest.instant(value.InvocationDeadline)
	return ServerlessInvocationAuthorityDigestV1(digest.sum()), nil
}

func (digest ServerlessInvocationAuthorityDigestV1) Validate() error {
	return validateSHA256("serverless_invocation_authority_digest", string(digest))
}

type ChildProcessAttestationV1 struct {
	ExecutableDigest     string   `json:"executable_digest"`
	ExactArgv            []string `json:"exact_argv"`
	NativeProtocol       string   `json:"native_protocol"`
	BackendProfileDigest string   `json:"backend_profile_digest"`
}

type InProcessAttestationV1 struct {
	LinkedBackendProfileDigest string `json:"linked_backend_profile_digest"`
}

type PreparedAllocationV1 struct {
	Version                     uint32                     `json:"version"`
	SubstrateBindingDigest      SubstrateBindingDigestV1   `json:"substrate_binding_digest"`
	ObservedImageDigest         string                     `json:"observed_image_digest"`
	ObservedOuterHarnessDigest  string                     `json:"observed_outer_harness_digest"`
	ObservedProxyArtifactDigest string                     `json:"observed_proxy_artifact_digest"`
	ObservedProxyIdentityDigest string                     `json:"observed_proxy_identity_digest"`
	WorkloadMode                SubstrateWorkloadModeV1    `json:"workload_mode"`
	ChildProcess                *ChildProcessAttestationV1 `json:"child_process,omitempty"`
	InProcess                   *InProcessAttestationV1    `json:"in_process,omitempty"`
}

func (value PreparedAllocationV1) Clone() PreparedAllocationV1 {
	clone := value
	if value.ChildProcess != nil {
		child := *value.ChildProcess
		child.ExactArgv = append([]string(nil), value.ChildProcess.ExactArgv...)
		clone.ChildProcess = &child
	}
	if value.InProcess != nil {
		inProcess := *value.InProcess
		clone.InProcess = &inProcess
	}
	return clone
}

func (value PreparedAllocationV1) ValidateForBinding(binding SubstrateBindingV1, backend HarnessBackendDescriptorV1) error {
	if value.Version != PreparedAllocationVersionV1 {
		return ValidationError{Field: "prepared_allocation.version", Reason: "must equal 1"}
	}
	if err := binding.Validate(); err != nil {
		return err
	}
	if err := backend.Validate(); err != nil {
		return err
	}
	bindingDigest, err := binding.Digest()
	if err != nil {
		return err
	}
	if value.SubstrateBindingDigest != bindingDigest || value.ObservedImageDigest != binding.ImageDigest || value.ObservedOuterHarnessDigest != binding.OuterHarnessArtifactDigest || value.ObservedProxyArtifactDigest != binding.EgressProxyArtifactDigest || value.ObservedProxyIdentityDigest != binding.EgressProxyIdentityDigest || value.WorkloadMode != binding.WorkloadMode {
		return ValidationError{Field: "prepared_allocation", Reason: "must exact-match the sealed substrate and proxy attestation"}
	}
	for field, observed := range map[string]string{
		"observed_image_digest": value.ObservedImageDigest, "observed_outer_harness_digest": value.ObservedOuterHarnessDigest,
		"observed_proxy_artifact_digest": value.ObservedProxyArtifactDigest, "observed_proxy_identity_digest": value.ObservedProxyIdentityDigest,
	} {
		if err := validateSHA256("prepared_allocation."+field, observed); err != nil {
			return err
		}
	}
	switch value.WorkloadMode {
	case SubstrateWorkloadChildProcessV1:
		if value.ChildProcess == nil || value.InProcess != nil {
			return ValidationError{Field: "prepared_allocation.workload", Reason: "child-process mode requires exactly one child attestation"}
		}
		if value.ChildProcess.ExecutableDigest != backend.ArtifactDigest || value.ChildProcess.BackendProfileDigest != backend.BackendProfileDigest || value.ChildProcess.NativeProtocol != backend.NativeProtocolVersion {
			return ValidationError{Field: "prepared_allocation.child_process", Reason: "must exact-match the sealed backend"}
		}
		if len(value.ChildProcess.ExactArgv) == 0 || len(value.ChildProcess.ExactArgv) > 64 {
			return ValidationError{Field: "prepared_allocation.child_process.exact_argv", Reason: "must be non-empty and bounded"}
		}
		for _, argument := range value.ChildProcess.ExactArgv {
			if argument == "" || len(argument) > 4096 {
				return ValidationError{Field: "prepared_allocation.child_process.exact_argv", Reason: "contains an empty or oversized argument"}
			}
		}
	case SubstrateWorkloadInProcessDirectV1:
		if value.InProcess == nil || value.ChildProcess != nil || value.InProcess.LinkedBackendProfileDigest != backend.BackendProfileDigest {
			return ValidationError{Field: "prepared_allocation.in_process", Reason: "must exact-match the linked backend profile"}
		}
	default:
		return ValidationError{Field: "prepared_allocation.workload_mode", Reason: "is unsupported"}
	}
	return nil
}

func (value PreparedAllocationV1) DigestForBinding(binding SubstrateBindingV1, backend HarnessBackendDescriptorV1) (PreparedAllocationDigestV1, error) {
	if err := value.ValidateForBinding(binding, backend); err != nil {
		return "", err
	}
	digest := newServerlessDigest("sessionless.prepared-allocation.v1")
	digest.uint(uint64(value.Version))
	for _, field := range []string{string(value.SubstrateBindingDigest), value.ObservedImageDigest, value.ObservedOuterHarnessDigest, value.ObservedProxyArtifactDigest, value.ObservedProxyIdentityDigest, string(value.WorkloadMode)} {
		digest.str(field)
	}
	if value.ChildProcess == nil {
		digest.uint(0)
	} else {
		digest.uint(1)
		digest.str(value.ChildProcess.ExecutableDigest)
		digest.uint(uint64(len(value.ChildProcess.ExactArgv)))
		for _, argument := range value.ChildProcess.ExactArgv {
			digest.str(argument)
		}
		digest.str(value.ChildProcess.NativeProtocol)
		digest.str(value.ChildProcess.BackendProfileDigest)
	}
	if value.InProcess == nil {
		digest.uint(0)
	} else {
		digest.uint(1)
		digest.str(value.InProcess.LinkedBackendProfileDigest)
	}
	return PreparedAllocationDigestV1(digest.sum()), nil
}

func (digest PreparedAllocationDigestV1) Validate() error {
	return validateSHA256("prepared_allocation_digest", string(digest))
}

// AttemptEffectReservationV1 is the durable one-way effect fence. Once it
// exists, any different physical invocation is reconcile-only.
type AttemptEffectReservationV1 struct {
	Version                      uint32                                `json:"version"`
	TenantID                     TenantID                              `json:"tenant_id"`
	RunID                        RunID                                 `json:"run_id"`
	AttemptID                    AttemptID                             `json:"attempt_id"`
	LeaseID                      LeaseID                               `json:"lease_id"`
	FenceToken                   uint64                                `json:"fence_token"`
	PhysicalInvocationClaimID    string                                `json:"physical_invocation_claim_id"`
	EffectSequence               uint64                                `json:"effect_sequence"`
	InvocationAuthorityDigest    ServerlessInvocationAuthorityDigestV1 `json:"invocation_authority_digest"`
	HarnessBindingDigest         HarnessBindingDigestV1                `json:"harness_binding_digest"`
	SubstrateBindingDigest       SubstrateBindingDigestV1              `json:"substrate_binding_digest"`
	AdmissionCostCeilingDigest   AdmissionCostCeilingDigestV1          `json:"admission_cost_ceiling_digest"`
	Kind                         ProviderEffectKindV1                  `json:"kind"`
	UpstreamIdempotencyKeyDigest *string                               `json:"upstream_idempotency_key_digest,omitempty"`
	ReservedAt                   time.Time                             `json:"reserved_at"`
}

func BuildAttemptEffectReservationV1(authority ServerlessInvocationAuthorityV1, physicalClaimID string, upstreamDigest *string, at time.Time) (AttemptEffectReservationV1, error) {
	if err := authority.ValidateAt(at); err != nil {
		return AttemptEffectReservationV1{}, err
	}
	if err := ValidateOpaqueID("attempt_effect_reservation.physical_invocation_claim_id", physicalClaimID); err != nil {
		return AttemptEffectReservationV1{}, err
	}
	harnessDigest, _ := authority.HarnessBinding.Digest()
	substrateDigest, _ := authority.SubstrateBinding.Digest()
	costDigest, _ := authority.AdmissionCostCeiling.Digest()
	authorityDigest, _ := authority.Digest()
	result := AttemptEffectReservationV1{
		Version: AttemptEffectReservationVersionV1, TenantID: authority.Lease.TenantID, RunID: authority.Lease.RunID,
		AttemptID: authority.Lease.AttemptID, LeaseID: authority.Lease.ID, FenceToken: authority.Lease.FenceToken,
		PhysicalInvocationClaimID: physicalClaimID, EffectSequence: ServerlessProviderEffectSequenceV1,
		InvocationAuthorityDigest: authorityDigest,
		HarnessBindingDigest:      harnessDigest, SubstrateBindingDigest: substrateDigest, AdmissionCostCeilingDigest: costDigest,
		Kind: ProviderEffectTurnV1, UpstreamIdempotencyKeyDigest: cloneString(upstreamDigest), ReservedAt: at.UTC(),
	}
	if err := result.ValidateForAuthority(authority); err != nil {
		return AttemptEffectReservationV1{}, err
	}
	return result, nil
}

func (value AttemptEffectReservationV1) Clone() AttemptEffectReservationV1 {
	clone := value
	clone.UpstreamIdempotencyKeyDigest = cloneString(value.UpstreamIdempotencyKeyDigest)
	return clone
}

func (value AttemptEffectReservationV1) ValidateForAuthority(authority ServerlessInvocationAuthorityV1) error {
	if value.Version != AttemptEffectReservationVersionV1 || value.Kind != ProviderEffectTurnV1 || value.EffectSequence != ServerlessProviderEffectSequenceV1 {
		return ValidationError{Field: "attempt_effect_reservation", Reason: "must be the version 1 provider-turn effect fence"}
	}
	if err := authority.Validate(); err != nil {
		return err
	}
	if value.TenantID != authority.Lease.TenantID || value.RunID != authority.Lease.RunID || value.AttemptID != authority.Lease.AttemptID || value.LeaseID != authority.Lease.ID || value.FenceToken != authority.Lease.FenceToken {
		return ValidationError{Field: "attempt_effect_reservation.scope", Reason: "must exact-match the lease and fence"}
	}
	if err := ValidateOpaqueID("attempt_effect_reservation.physical_invocation_claim_id", value.PhysicalInvocationClaimID); err != nil {
		return err
	}
	harnessDigest, _ := authority.HarnessBinding.Digest()
	substrateDigest, _ := authority.SubstrateBinding.Digest()
	costDigest, _ := authority.AdmissionCostCeiling.Digest()
	authorityDigest, _ := authority.Digest()
	if value.InvocationAuthorityDigest != authorityDigest || value.HarnessBindingDigest != harnessDigest || value.SubstrateBindingDigest != substrateDigest || value.AdmissionCostCeilingDigest != costDigest {
		return ValidationError{Field: "attempt_effect_reservation.authority", Reason: "must exact-match all admitted authority digests"}
	}
	if value.UpstreamIdempotencyKeyDigest != nil {
		if err := validateSHA256("attempt_effect_reservation.upstream_idempotency_key_digest", *value.UpstreamIdempotencyKeyDigest); err != nil {
			return err
		}
	}
	if value.ReservedAt.IsZero() || value.ReservedAt.Before(authority.Lease.AcquiredAt) || !value.ReservedAt.Before(authority.InvocationDeadline) || !value.ReservedAt.Before(authority.Lease.ExpiresAt) {
		return ValidationError{Field: "attempt_effect_reservation.reserved_at", Reason: "must be inside the active authority window"}
	}
	return nil
}

func (value AttemptEffectReservationV1) DigestForAuthority(authority ServerlessInvocationAuthorityV1) (AttemptEffectReservationDigestV1, error) {
	if err := value.ValidateForAuthority(authority); err != nil {
		return "", err
	}
	digest := newServerlessDigest("sessionless.attempt-effect-reservation.v1")
	digest.uint(uint64(value.Version))
	for _, field := range []string{string(value.TenantID), string(value.RunID), string(value.AttemptID), string(value.LeaseID), value.PhysicalInvocationClaimID, string(value.InvocationAuthorityDigest), string(value.HarnessBindingDigest), string(value.SubstrateBindingDigest), string(value.AdmissionCostCeilingDigest), string(value.Kind)} {
		digest.str(field)
	}
	digest.uint(value.FenceToken)
	digest.uint(value.EffectSequence)
	if value.UpstreamIdempotencyKeyDigest == nil {
		digest.uint(0)
	} else {
		digest.uint(1)
		digest.str(*value.UpstreamIdempotencyKeyDigest)
	}
	digest.instant(value.ReservedAt)
	return AttemptEffectReservationDigestV1(digest.sum()), nil
}

func (digest AttemptEffectReservationDigestV1) Validate() error {
	return validateSHA256("attempt_effect_reservation_digest", string(digest))
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
