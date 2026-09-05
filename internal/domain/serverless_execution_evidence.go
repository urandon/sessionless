package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"
)

const SubstrateExecutionEvidenceVersionV1 uint32 = 1

type (
	PreparedInvocationDigestV1         string
	SubstrateExecutionEvidenceDigestV1 string
	SubstrateAllocationStateV1         string
	SubstrateProcessStateV1            string
	CredentialFinalizationStateV1      string
	SubstrateCleanupStateV1            string
	SubstrateEgressStateV1             string
	SubstrateAttestationStateV1        string
	SubstrateProxyAttestationStateV1   string
	SubstrateCancellationRequestV1     string
	SubstrateCancellationSignalV1      string
	SubstrateResourceKindV1            string
	SubstrateResourceStateV1           string
	SubstrateResourceUnitV1            string
	SubstrateResourceProvenanceV1      string
	SubstrateExecutionFailureCodeV1    string
)

const (
	SubstrateAllocationUnknownV1  SubstrateAllocationStateV1 = "unknown"
	SubstrateAllocationStartedV1  SubstrateAllocationStateV1 = "started"
	SubstrateAllocationRejectedV1 SubstrateAllocationStateV1 = "rejected"

	SubstrateProcessNotApplicableV1 SubstrateProcessStateV1 = "not_applicable"
	SubstrateProcessNotStartedV1    SubstrateProcessStateV1 = "not_started"
	SubstrateProcessRunningV1       SubstrateProcessStateV1 = "running"
	SubstrateProcessStoppedV1       SubstrateProcessStateV1 = "stopped"
	SubstrateProcessStopUnknownV1   SubstrateProcessStateV1 = "stop_unknown"

	CredentialFinalizationNotRequiredV1 CredentialFinalizationStateV1 = "not_required"
	CredentialFinalizationVerifiedV1    CredentialFinalizationStateV1 = "verified"
	CredentialFinalizationFailedV1      CredentialFinalizationStateV1 = "failed"
	CredentialFinalizationUnknownV1     CredentialFinalizationStateV1 = "unknown"

	SubstrateCleanupNotRequiredV1 SubstrateCleanupStateV1 = "not_required"
	SubstrateCleanupVerifiedV1    SubstrateCleanupStateV1 = "verified"
	SubstrateCleanupFailedV1      SubstrateCleanupStateV1 = "failed"
	SubstrateCleanupUnknownV1     SubstrateCleanupStateV1 = "unknown"

	SubstrateEgressNotAttemptedV1   SubstrateEgressStateV1 = "not_attempted"
	SubstrateEgressPolicyEnforcedV1 SubstrateEgressStateV1 = "policy_enforced"
	SubstrateEgressDeniedV1         SubstrateEgressStateV1 = "denied"
	SubstrateEgressUnknownV1        SubstrateEgressStateV1 = "unknown"

	SubstrateAttestationVerifiedV1 SubstrateAttestationStateV1 = "verified"
	SubstrateAttestationMismatchV1 SubstrateAttestationStateV1 = "mismatch"
	SubstrateAttestationUnknownV1  SubstrateAttestationStateV1 = "unknown"

	SubstrateProxyAttestationNotRequiredV1 SubstrateProxyAttestationStateV1 = "not_required"
	SubstrateProxyAttestationVerifiedV1    SubstrateProxyAttestationStateV1 = "verified"
	SubstrateProxyAttestationMismatchV1    SubstrateProxyAttestationStateV1 = "mismatch"
	SubstrateProxyAttestationUnknownV1     SubstrateProxyAttestationStateV1 = "unknown"

	SubstrateCancellationRequestNoneV1     SubstrateCancellationRequestV1 = "none"
	SubstrateCancellationRequestObservedV1 SubstrateCancellationRequestV1 = "observed"

	SubstrateCancellationSignalNotRequiredV1  SubstrateCancellationSignalV1 = "not_required"
	SubstrateCancellationSignalNotSentV1      SubstrateCancellationSignalV1 = "not_sent"
	SubstrateCancellationSignalSentV1         SubstrateCancellationSignalV1 = "sent"
	SubstrateCancellationSignalAcknowledgedV1 SubstrateCancellationSignalV1 = "acknowledged"
	SubstrateCancellationSignalUnknownV1      SubstrateCancellationSignalV1 = "unknown"

	SubstrateResourceCPUTimeV1       SubstrateResourceKindV1 = "cpu_time"
	SubstrateResourceMemoryPeakV1    SubstrateResourceKindV1 = "memory_peak"
	SubstrateResourceScratchPeakV1   SubstrateResourceKindV1 = "scratch_peak"
	SubstrateResourceIngressBytesV1  SubstrateResourceKindV1 = "ingress_bytes"
	SubstrateResourceEgressBytesV1   SubstrateResourceKindV1 = "egress_bytes"
	SubstrateResourceLogBytesV1      SubstrateResourceKindV1 = "log_bytes"
	SubstrateResourceEvidenceBytesV1 SubstrateResourceKindV1 = "evidence_bytes"

	SubstrateResourceUnknownV1  SubstrateResourceStateV1 = "unknown"
	SubstrateResourceObservedV1 SubstrateResourceStateV1 = "observed"

	SubstrateResourceUnitNanosecondsV1 SubstrateResourceUnitV1 = "nanoseconds"
	SubstrateResourceUnitBytesV1       SubstrateResourceUnitV1 = "bytes"

	SubstrateResourceProvenanceUnknownV1           SubstrateResourceProvenanceV1 = "unknown"
	SubstrateResourceProvenanceSubstrateReportedV1 SubstrateResourceProvenanceV1 = "substrate_reported"
	SubstrateResourceProvenanceHarnessMeasuredV1   SubstrateResourceProvenanceV1 = "harness_measured"
	SubstrateResourceProvenanceProxyMeasuredV1     SubstrateResourceProvenanceV1 = "proxy_measured"

	SubstrateExecutionFailureNoneV1                     SubstrateExecutionFailureCodeV1 = "none"
	SubstrateExecutionFailureAuthorityDeniedV1          SubstrateExecutionFailureCodeV1 = "authority_denied"
	SubstrateExecutionFailureProfileDisabledV1          SubstrateExecutionFailureCodeV1 = "profile_disabled"
	SubstrateExecutionFailureProfileExpiredV1           SubstrateExecutionFailureCodeV1 = "profile_expired"
	SubstrateExecutionFailureAttestationMismatchV1      SubstrateExecutionFailureCodeV1 = "attestation_mismatch"
	SubstrateExecutionFailureLeaseLostV1                SubstrateExecutionFailureCodeV1 = "lease_lost"
	SubstrateExecutionFailureCancelledV1                SubstrateExecutionFailureCodeV1 = "cancelled"
	SubstrateExecutionFailureProcessFailedV1            SubstrateExecutionFailureCodeV1 = "process_failed"
	SubstrateExecutionFailureOutputBoundExceededV1      SubstrateExecutionFailureCodeV1 = "output_bound_exceeded"
	SubstrateExecutionFailureEgressDeniedV1             SubstrateExecutionFailureCodeV1 = "egress_denied"
	SubstrateExecutionFailureProviderFailedV1           SubstrateExecutionFailureCodeV1 = "provider_failed"
	SubstrateExecutionFailureAcceptedOutcomeUnknownV1   SubstrateExecutionFailureCodeV1 = "accepted_outcome_unknown"
	SubstrateExecutionFailureCredentialFinalizeFailedV1 SubstrateExecutionFailureCodeV1 = "credential_finalize_failed"
	SubstrateExecutionFailureCleanupFailedV1            SubstrateExecutionFailureCodeV1 = "cleanup_failed"
	SubstrateExecutionFailureBackendFailedV1            SubstrateExecutionFailureCodeV1 = "backend_failed"
)

// SubstrateCancellationEvidenceV1 deliberately stops at substrate/backend
// observations. Canonical cancellation request and terminal commit remain in
// their existing authorities.
type SubstrateCancellationEvidenceV1 struct {
	Request       SubstrateCancellationRequestV1 `json:"request"`
	BackendSignal SubstrateCancellationSignalV1  `json:"backend_signal"`
}

// SubstrateResourceObservationV1 is content-free. Unknown is represented by
// an absent quantity, absent unit/time, and unknown provenance; an observed
// zero is represented by a non-nil zero quantity with exact metadata.
type SubstrateResourceObservationV1 struct {
	Kind       SubstrateResourceKindV1       `json:"kind"`
	State      SubstrateResourceStateV1      `json:"state"`
	Quantity   *uint64                       `json:"quantity,omitempty"`
	Unit       SubstrateResourceUnitV1       `json:"unit,omitempty"`
	Provenance SubstrateResourceProvenanceV1 `json:"provenance"`
	ObservedAt *time.Time                    `json:"observed_at,omitempty"`
}

func (value SubstrateResourceObservationV1) Clone() SubstrateResourceObservationV1 {
	clone := value
	clone.Quantity = cloneUint64(value.Quantity)
	if value.ObservedAt != nil {
		observedAt := value.ObservedAt.UTC()
		clone.ObservedAt = &observedAt
	}
	return clone
}

// SubstrateExecutionEvidenceV1 is a public-safe, content-free observation of
// one exact physical invocation. It is not attempt state and cannot assert a
// canonical terminal commit.
type SubstrateExecutionEvidenceV1 struct {
	Version                    uint32                                `json:"version"`
	InvocationAuthorityDigest  ServerlessInvocationAuthorityDigestV1 `json:"invocation_authority_digest"`
	SubstrateBindingDigest     SubstrateBindingDigestV1              `json:"substrate_binding_digest"`
	AdmissionCostCeilingDigest AdmissionCostCeilingDigestV1          `json:"admission_cost_ceiling_digest"`
	PreparedInvocationDigest   PreparedInvocationDigestV1            `json:"prepared_invocation_digest"`
	EffectReservationDigest    AttemptEffectReservationDigestV1      `json:"effect_reservation_digest"`
	PreparedAllocationDigest   PreparedAllocationDigestV1            `json:"prepared_allocation_digest"`
	PhysicalInvocationClaimID  string                                `json:"physical_invocation_claim_id"`
	Allocation                 SubstrateAllocationStateV1            `json:"allocation"`
	Process                    SubstrateProcessStateV1               `json:"process"`
	CredentialFinalization     CredentialFinalizationStateV1         `json:"credential_finalization"`
	Cleanup                    SubstrateCleanupStateV1               `json:"cleanup"`
	Egress                     SubstrateEgressStateV1                `json:"egress"`
	ImageAttestation           SubstrateAttestationStateV1           `json:"image_attestation"`
	BackendAttestation         SubstrateAttestationStateV1           `json:"backend_attestation"`
	ProxyAttestation           SubstrateProxyAttestationStateV1      `json:"proxy_attestation"`
	Cancellation               SubstrateCancellationEvidenceV1       `json:"cancellation"`
	ProviderEvidence           *ProviderExecutionEvidenceV1          `json:"provider_evidence,omitempty"`
	ResourceObservations       []SubstrateResourceObservationV1      `json:"resource_observations"`
	FailureCode                SubstrateExecutionFailureCodeV1       `json:"failure_code"`
	SecondaryFailureCodes      []SubstrateExecutionFailureCodeV1     `json:"secondary_failure_codes"`
	EvidenceDigest             SubstrateExecutionEvidenceDigestV1    `json:"evidence_digest"`
}

func (value SubstrateExecutionEvidenceV1) Clone() SubstrateExecutionEvidenceV1 {
	clone := value
	if value.ProviderEvidence != nil {
		provider := value.ProviderEvidence.Clone()
		clone.ProviderEvidence = &provider
	}
	clone.ResourceObservations = make([]SubstrateResourceObservationV1, len(value.ResourceObservations))
	for index := range value.ResourceObservations {
		clone.ResourceObservations[index] = value.ResourceObservations[index].Clone()
	}
	clone.SecondaryFailureCodes = append([]SubstrateExecutionFailureCodeV1(nil), value.SecondaryFailureCodes...)
	return clone
}

// SealForAuthority fills only fields derivable from the exact admitted
// authority and its already-issued capability inputs. The caller supplies the
// opaque prepared-capability digest because the domain package cannot mint or
// reconstruct that process-local capability.
func (value SubstrateExecutionEvidenceV1) SealForAuthority(
	authority ServerlessInvocationAuthorityV1,
	reservation AttemptEffectReservationV1,
	allocation PreparedAllocationV1,
	preparedDigest PreparedInvocationDigestV1,
) (SubstrateExecutionEvidenceV1, error) {
	if err := authority.Validate(); err != nil {
		return SubstrateExecutionEvidenceV1{}, err
	}
	if err := reservation.ValidateForAuthority(authority); err != nil {
		return SubstrateExecutionEvidenceV1{}, err
	}
	if err := allocation.ValidateForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend); err != nil {
		return SubstrateExecutionEvidenceV1{}, err
	}
	if err := preparedDigest.Validate(); err != nil {
		return SubstrateExecutionEvidenceV1{}, err
	}
	value.Version = SubstrateExecutionEvidenceVersionV1
	value.InvocationAuthorityDigest, _ = authority.Digest()
	value.SubstrateBindingDigest, _ = authority.SubstrateBinding.Digest()
	value.AdmissionCostCeilingDigest, _ = authority.AdmissionCostCeiling.Digest()
	value.PreparedInvocationDigest = preparedDigest
	value.EffectReservationDigest, _ = reservation.DigestForAuthority(authority)
	value.PreparedAllocationDigest, _ = allocation.DigestForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend)
	value.PhysicalInvocationClaimID = reservation.PhysicalInvocationClaimID
	value.EvidenceDigest = ""
	if err := value.validateForAuthority(authority); err != nil {
		return SubstrateExecutionEvidenceV1{}, err
	}
	digest, err := value.digest()
	if err != nil {
		return SubstrateExecutionEvidenceV1{}, err
	}
	value.EvidenceDigest = digest
	return value, nil
}

func (value SubstrateExecutionEvidenceV1) ValidateForAuthority(
	authority ServerlessInvocationAuthorityV1,
	reservation AttemptEffectReservationV1,
	allocation PreparedAllocationV1,
	preparedDigest PreparedInvocationDigestV1,
) error {
	if err := authority.Validate(); err != nil {
		return err
	}
	if err := reservation.ValidateForAuthority(authority); err != nil {
		return err
	}
	if err := allocation.ValidateForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend); err != nil {
		return err
	}
	if err := preparedDigest.Validate(); err != nil {
		return err
	}
	authorityDigest, _ := authority.Digest()
	substrateDigest, _ := authority.SubstrateBinding.Digest()
	costDigest, _ := authority.AdmissionCostCeiling.Digest()
	reservationDigest, _ := reservation.DigestForAuthority(authority)
	allocationDigest, _ := allocation.DigestForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend)
	if value.InvocationAuthorityDigest != authorityDigest || value.SubstrateBindingDigest != substrateDigest ||
		value.AdmissionCostCeilingDigest != costDigest || value.PreparedInvocationDigest != preparedDigest ||
		value.EffectReservationDigest != reservationDigest || value.PreparedAllocationDigest != allocationDigest ||
		value.PhysicalInvocationClaimID != reservation.PhysicalInvocationClaimID {
		return ValidationError{Field: "substrate_execution_evidence.authority", Reason: "must exact-match authority, reservation, capability, allocation, cost, and physical claim"}
	}
	if err := value.validateForAuthority(authority); err != nil {
		return err
	}
	if err := value.EvidenceDigest.Validate(); err != nil {
		return err
	}
	expected, err := value.digest()
	if err != nil {
		return err
	}
	if value.EvidenceDigest != expected {
		return ValidationError{Field: "substrate_execution_evidence.evidence_digest", Reason: "does not match canonical evidence"}
	}
	return nil
}

// ValidateForPersistedAuthority validates a sealed observation against the
// durable authority and effect reservation available to the state store. The
// exact PreparedAllocation was already authenticated and checked by the
// process-local registry; only its sealed digest crosses this boundary.
func (value SubstrateExecutionEvidenceV1) ValidateForPersistedAuthority(
	authority ServerlessInvocationAuthorityV1,
	reservation AttemptEffectReservationV1,
) error {
	if err := authority.Validate(); err != nil {
		return err
	}
	if err := reservation.ValidateForAuthority(authority); err != nil {
		return err
	}
	authorityDigest, _ := authority.Digest()
	substrateDigest, _ := authority.SubstrateBinding.Digest()
	costDigest, _ := authority.AdmissionCostCeiling.Digest()
	reservationDigest, _ := reservation.DigestForAuthority(authority)
	if value.InvocationAuthorityDigest != authorityDigest || value.SubstrateBindingDigest != substrateDigest ||
		value.AdmissionCostCeilingDigest != costDigest || value.EffectReservationDigest != reservationDigest ||
		value.PhysicalInvocationClaimID != reservation.PhysicalInvocationClaimID {
		return ValidationError{Field: "substrate_execution_evidence.authority", Reason: "must exact-match persisted authority, reservation, cost, and physical claim"}
	}
	if err := value.PreparedInvocationDigest.Validate(); err != nil {
		return err
	}
	if err := value.PreparedAllocationDigest.Validate(); err != nil {
		return err
	}
	if err := value.validateForAuthority(authority); err != nil {
		return err
	}
	if err := value.EvidenceDigest.Validate(); err != nil {
		return err
	}
	expected, err := value.digest()
	if err != nil {
		return err
	}
	if value.EvidenceDigest != expected {
		return ValidationError{Field: "substrate_execution_evidence.evidence_digest", Reason: "does not match canonical evidence"}
	}
	return nil
}

// DigestForAuthority derives the canonical digest after validating every
// content field against the exact authority chain. It deliberately ignores a
// previously attached EvidenceDigest so callers can seal an otherwise exact
// observation without accepting a caller-supplied digest.
func (value SubstrateExecutionEvidenceV1) DigestForAuthority(
	authority ServerlessInvocationAuthorityV1,
	reservation AttemptEffectReservationV1,
	allocation PreparedAllocationV1,
	preparedDigest PreparedInvocationDigestV1,
) (SubstrateExecutionEvidenceDigestV1, error) {
	copy := value.Clone()
	digest, err := copy.digest()
	if err != nil {
		return "", err
	}
	copy.EvidenceDigest = digest
	if err := copy.ValidateForAuthority(authority, reservation, allocation, preparedDigest); err != nil {
		return "", err
	}
	return digest, nil
}

func (value SubstrateExecutionEvidenceV1) validateForAuthority(authority ServerlessInvocationAuthorityV1) error {
	if value.Version != SubstrateExecutionEvidenceVersionV1 {
		return ValidationError{Field: "substrate_execution_evidence.version", Reason: "must equal 1"}
	}
	for _, digest := range []interface{ Validate() error }{
		value.InvocationAuthorityDigest, value.SubstrateBindingDigest, value.AdmissionCostCeilingDigest,
		value.PreparedInvocationDigest, value.EffectReservationDigest, value.PreparedAllocationDigest,
	} {
		if err := digest.Validate(); err != nil {
			return err
		}
	}
	if err := ValidateOpaqueID("substrate_execution_evidence.physical_invocation_claim_id", value.PhysicalInvocationClaimID); err != nil {
		return err
	}
	if err := validateSubstrateEnums(value); err != nil {
		return err
	}
	if err := validateSubstrateCancellation(value.Cancellation); err != nil {
		return err
	}
	if err := validateSubstrateResources(value.ResourceObservations); err != nil {
		return err
	}
	if value.ProviderEvidence != nil {
		if err := value.ProviderEvidence.ValidateForBinding(authority.HarnessBinding); err != nil {
			return err
		}
	}
	if err := validateSubstrateAttestationAndProcess(value, authority); err != nil {
		return err
	}
	if err := validateSubstrateProviderCompletion(value); err != nil {
		return err
	}
	return validateSubstrateFailures(value)
}

func validateSubstrateEnums(value SubstrateExecutionEvidenceV1) error {
	switch value.Allocation {
	case SubstrateAllocationUnknownV1, SubstrateAllocationStartedV1, SubstrateAllocationRejectedV1:
	default:
		return ValidationError{Field: "substrate_execution_evidence.allocation", Reason: "is unsupported"}
	}
	switch value.Process {
	case SubstrateProcessNotApplicableV1, SubstrateProcessNotStartedV1, SubstrateProcessRunningV1, SubstrateProcessStoppedV1, SubstrateProcessStopUnknownV1:
	default:
		return ValidationError{Field: "substrate_execution_evidence.process", Reason: "is unsupported"}
	}
	switch value.CredentialFinalization {
	case CredentialFinalizationNotRequiredV1, CredentialFinalizationVerifiedV1, CredentialFinalizationFailedV1, CredentialFinalizationUnknownV1:
	default:
		return ValidationError{Field: "substrate_execution_evidence.credential_finalization", Reason: "is unsupported"}
	}
	switch value.Cleanup {
	case SubstrateCleanupNotRequiredV1, SubstrateCleanupVerifiedV1, SubstrateCleanupFailedV1, SubstrateCleanupUnknownV1:
	default:
		return ValidationError{Field: "substrate_execution_evidence.cleanup", Reason: "is unsupported"}
	}
	switch value.Egress {
	case SubstrateEgressNotAttemptedV1, SubstrateEgressPolicyEnforcedV1, SubstrateEgressDeniedV1, SubstrateEgressUnknownV1:
	default:
		return ValidationError{Field: "substrate_execution_evidence.egress", Reason: "is unsupported"}
	}
	if !value.ImageAttestation.Valid() || !value.BackendAttestation.Valid() || !value.ProxyAttestation.Valid() {
		return ValidationError{Field: "substrate_execution_evidence.attestation", Reason: "contains an unsupported state"}
	}
	if !value.FailureCode.Valid() {
		return ValidationError{Field: "substrate_execution_evidence.failure_code", Reason: "is unsupported"}
	}
	return nil
}

func validateSubstrateCancellation(value SubstrateCancellationEvidenceV1) error {
	switch value.Request {
	case SubstrateCancellationRequestNoneV1:
		if value.BackendSignal != SubstrateCancellationSignalNotRequiredV1 && value.BackendSignal != SubstrateCancellationSignalNotSentV1 {
			return ValidationError{Field: "substrate_execution_evidence.cancellation", Reason: "a signal cannot precede an observed request"}
		}
	case SubstrateCancellationRequestObservedV1:
		switch value.BackendSignal {
		case SubstrateCancellationSignalNotRequiredV1, SubstrateCancellationSignalNotSentV1, SubstrateCancellationSignalSentV1, SubstrateCancellationSignalAcknowledgedV1, SubstrateCancellationSignalUnknownV1:
		default:
			return ValidationError{Field: "substrate_execution_evidence.cancellation.backend_signal", Reason: "is unsupported"}
		}
	default:
		return ValidationError{Field: "substrate_execution_evidence.cancellation.request", Reason: "is unsupported"}
	}
	return nil
}

func validateSubstrateResources(values []SubstrateResourceObservationV1) error {
	expectedKinds := []SubstrateResourceKindV1{
		SubstrateResourceCPUTimeV1, SubstrateResourceEgressBytesV1, SubstrateResourceEvidenceBytesV1,
		SubstrateResourceIngressBytesV1, SubstrateResourceLogBytesV1, SubstrateResourceMemoryPeakV1, SubstrateResourceScratchPeakV1,
	}
	if len(values) != len(expectedKinds) {
		return ValidationError{Field: "substrate_execution_evidence.resource_observations", Reason: "must contain each closed resource kind exactly once"}
	}
	for index, value := range values {
		if value.Kind != expectedKinds[index] {
			return ValidationError{Field: "substrate_execution_evidence.resource_observations", Reason: "must be unique and sorted by kind"}
		}
		expectedUnit := SubstrateResourceUnitBytesV1
		if value.Kind == SubstrateResourceCPUTimeV1 {
			expectedUnit = SubstrateResourceUnitNanosecondsV1
		}
		switch value.State {
		case SubstrateResourceUnknownV1:
			if value.Quantity != nil || value.Unit != "" || value.Provenance != SubstrateResourceProvenanceUnknownV1 || value.ObservedAt != nil {
				return ValidationError{Field: "substrate_execution_evidence.resource_observations", Reason: "unknown observation must not invent quantity, unit, provenance, or time"}
			}
		case SubstrateResourceObservedV1:
			if value.Quantity == nil || value.Unit != expectedUnit || value.ObservedAt == nil || value.ObservedAt.IsZero() {
				return ValidationError{Field: "substrate_execution_evidence.resource_observations", Reason: "observed quantity requires its exact unit and time"}
			}
			switch value.Provenance {
			case SubstrateResourceProvenanceSubstrateReportedV1, SubstrateResourceProvenanceHarnessMeasuredV1, SubstrateResourceProvenanceProxyMeasuredV1:
			default:
				return ValidationError{Field: "substrate_execution_evidence.resource_observations", Reason: "observed quantity requires closed provenance"}
			}
		default:
			return ValidationError{Field: "substrate_execution_evidence.resource_observations", Reason: "contains an unsupported state"}
		}
	}
	return nil
}

func validateSubstrateAttestationAndProcess(value SubstrateExecutionEvidenceV1, authority ServerlessInvocationAuthorityV1) error {
	if value.Allocation == SubstrateAllocationRejectedV1 {
		if value.Process != SubstrateProcessNotStartedV1 || value.Egress != SubstrateEgressNotAttemptedV1 || value.ProviderEvidence != nil ||
			value.CredentialFinalization != CredentialFinalizationNotRequiredV1 || value.Cleanup != SubstrateCleanupNotRequiredV1 {
			return ValidationError{Field: "substrate_execution_evidence.allocation", Reason: "rejected allocation cannot claim process, egress, provider, credential, or cleanup effects"}
		}
	}
	if value.Allocation == SubstrateAllocationUnknownV1 {
		if value.Process != SubstrateProcessNotStartedV1 && value.Process != SubstrateProcessStopUnknownV1 {
			return ValidationError{Field: "substrate_execution_evidence.allocation", Reason: "unknown allocation cannot claim a known process lifecycle"}
		}
		if value.ProviderEvidence != nil {
			return ValidationError{Field: "substrate_execution_evidence.allocation", Reason: "unknown allocation cannot claim provider evidence"}
		}
		if value.CredentialFinalization != CredentialFinalizationUnknownV1 || value.Cleanup != SubstrateCleanupUnknownV1 ||
			value.Egress != SubstrateEgressUnknownV1 || value.ImageAttestation != SubstrateAttestationUnknownV1 ||
			value.BackendAttestation != SubstrateAttestationUnknownV1 || value.ProxyAttestation != SubstrateProxyAttestationUnknownV1 {
			return ValidationError{Field: "substrate_execution_evidence.allocation", Reason: "unknown allocation must preserve unknown effect and attestation facts"}
		}
	}
	if value.Allocation == SubstrateAllocationStartedV1 {
		switch authority.SubstrateBinding.WorkloadMode {
		case SubstrateWorkloadChildProcessV1:
			if value.Process == SubstrateProcessNotApplicableV1 || value.Process == SubstrateProcessNotStartedV1 {
				return ValidationError{Field: "substrate_execution_evidence.process", Reason: "started child-process allocation requires a process observation"}
			}
		case SubstrateWorkloadInProcessDirectV1:
			if value.Process != SubstrateProcessNotApplicableV1 {
				return ValidationError{Field: "substrate_execution_evidence.process", Reason: "in-process allocation has no child process lifecycle"}
			}
		}
	}
	if value.Cleanup == SubstrateCleanupVerifiedV1 &&
		(value.Process != SubstrateProcessStoppedV1 && value.Process != SubstrateProcessNotApplicableV1 ||
			value.CredentialFinalization != CredentialFinalizationVerifiedV1 && value.CredentialFinalization != CredentialFinalizationNotRequiredV1) {
		return ValidationError{Field: "substrate_execution_evidence.cleanup", Reason: "verified cleanup requires stopped/not-applicable process and independently finalized/not-required credentials"}
	}
	if authority.HarnessBinding.Backend.CredentialDeliveryKind == ProviderCredentialDeliveryNoneV1 {
		if value.CredentialFinalization != CredentialFinalizationNotRequiredV1 && value.Allocation != SubstrateAllocationUnknownV1 {
			return ValidationError{Field: "substrate_execution_evidence.credential_finalization", Reason: "credentialless backend requires not_required"}
		}
	} else if value.Allocation == SubstrateAllocationStartedV1 && value.CredentialFinalization == CredentialFinalizationNotRequiredV1 {
		return ValidationError{Field: "substrate_execution_evidence.credential_finalization", Reason: "credential-bearing started work must report verified, failed, or unknown finalization"}
	}
	if value.Egress == SubstrateEgressPolicyEnforcedV1 &&
		(value.ProxyAttestation != SubstrateProxyAttestationVerifiedV1 || value.ImageAttestation != SubstrateAttestationVerifiedV1 || value.BackendAttestation != SubstrateAttestationVerifiedV1) {
		return ValidationError{Field: "substrate_execution_evidence.egress", Reason: "enforced provider egress requires verified image, backend, and proxy attestation"}
	}
	if value.Egress == SubstrateEgressDeniedV1 && value.ProxyAttestation != SubstrateProxyAttestationVerifiedV1 {
		return ValidationError{Field: "substrate_execution_evidence.egress", Reason: "denied egress requires the verified enforcing proxy"}
	}
	if value.ProviderEvidence != nil && (value.Allocation != SubstrateAllocationStartedV1 || value.Egress != SubstrateEgressPolicyEnforcedV1) {
		return ValidationError{Field: "substrate_execution_evidence.provider_evidence", Reason: "provider evidence requires a started, attested, policy-enforced allocation"}
	}
	mismatch := value.ImageAttestation == SubstrateAttestationMismatchV1 || value.BackendAttestation == SubstrateAttestationMismatchV1 || value.ProxyAttestation == SubstrateProxyAttestationMismatchV1
	if mismatch {
		if value.Allocation != SubstrateAllocationRejectedV1 || value.ProviderEvidence != nil || value.Egress != SubstrateEgressNotAttemptedV1 || value.Process != SubstrateProcessNotStartedV1 {
			return ValidationError{Field: "substrate_execution_evidence.attestation", Reason: "mismatch must fail before process, network, or provider effects"}
		}
	}
	return nil
}

func validateSubstrateProviderCompletion(value SubstrateExecutionEvidenceV1) error {
	if value.ProviderEvidence == nil || value.ProviderEvidence.FinishClass != ProviderFinishCompletedV1 {
		return nil
	}
	provider := value.ProviderEvidence
	if provider.AcceptanceClass != ProviderAcceptanceAcceptedV1 || provider.RouteState != ProviderEvidenceSupportedV1 ||
		(provider.PolicyVerdict != ProviderPolicyGoV1 && provider.PolicyVerdict != ProviderPolicyConditionalV1) ||
		value.Allocation != SubstrateAllocationStartedV1 || value.Egress != SubstrateEgressPolicyEnforcedV1 ||
		value.ImageAttestation != SubstrateAttestationVerifiedV1 || value.BackendAttestation != SubstrateAttestationVerifiedV1 ||
		value.ProxyAttestation != SubstrateProxyAttestationVerifiedV1 ||
		(value.Process != SubstrateProcessStoppedV1 && value.Process != SubstrateProcessNotApplicableV1) {
		return ValidationError{Field: "substrate_execution_evidence.provider_evidence", Reason: "completed provider work requires accepted policy, exact route, enforced egress, verified attestations, and a stopped/not-applicable process"}
	}
	return nil
}

func validateSubstrateFailures(value SubstrateExecutionEvidenceV1) error {
	seen := map[SubstrateExecutionFailureCodeV1]struct{}{value.FailureCode: {}}
	if value.FailureCode == SubstrateExecutionFailureNoneV1 && len(value.SecondaryFailureCodes) != 0 {
		return ValidationError{Field: "substrate_execution_evidence.secondary_failure_codes", Reason: "success cannot contain secondary failures"}
	}
	if !sort.SliceIsSorted(value.SecondaryFailureCodes, func(left, right int) bool {
		return value.SecondaryFailureCodes[left] < value.SecondaryFailureCodes[right]
	}) {
		return ValidationError{Field: "substrate_execution_evidence.secondary_failure_codes", Reason: "must be sorted"}
	}
	for _, code := range value.SecondaryFailureCodes {
		if !code.Valid() || code == SubstrateExecutionFailureNoneV1 {
			return ValidationError{Field: "substrate_execution_evidence.secondary_failure_codes", Reason: "contains an unsupported code"}
		}
		if _, exists := seen[code]; exists {
			return ValidationError{Field: "substrate_execution_evidence.secondary_failure_codes", Reason: "must be unique and exclude the primary code"}
		}
		seen[code] = struct{}{}
	}
	for code := range seen {
		if err := validateSubstrateFailure(value, code); err != nil {
			return err
		}
	}
	return nil
}

func validateSubstrateFailure(value SubstrateExecutionEvidenceV1, code SubstrateExecutionFailureCodeV1) error {
	preEffectRejected := value.Allocation == SubstrateAllocationRejectedV1 && value.Process == SubstrateProcessNotStartedV1 && value.Egress == SubstrateEgressNotAttemptedV1 && value.ProviderEvidence == nil
	switch code {
	case SubstrateExecutionFailureNoneV1:
		if value.ProviderEvidence == nil || value.ProviderEvidence.FinishClass != ProviderFinishCompletedV1 || value.Allocation != SubstrateAllocationStartedV1 ||
			(value.CredentialFinalization != CredentialFinalizationVerifiedV1 && value.CredentialFinalization != CredentialFinalizationNotRequiredV1) ||
			(value.Cleanup != SubstrateCleanupVerifiedV1 && value.Cleanup != SubstrateCleanupNotRequiredV1) {
			return incompatibleSubstrateFailure(code)
		}
	case SubstrateExecutionFailureAuthorityDeniedV1, SubstrateExecutionFailureProfileDisabledV1, SubstrateExecutionFailureProfileExpiredV1:
		if !preEffectRejected {
			return incompatibleSubstrateFailure(code)
		}
	case SubstrateExecutionFailureAttestationMismatchV1:
		if !preEffectRejected || value.ImageAttestation != SubstrateAttestationMismatchV1 && value.BackendAttestation != SubstrateAttestationMismatchV1 && value.ProxyAttestation != SubstrateProxyAttestationMismatchV1 {
			return incompatibleSubstrateFailure(code)
		}
	case SubstrateExecutionFailureLeaseLostV1:
		if value.Allocation == SubstrateAllocationRejectedV1 {
			return incompatibleSubstrateFailure(code)
		}
	case SubstrateExecutionFailureCancelledV1:
		if value.Cancellation.Request != SubstrateCancellationRequestObservedV1 || value.ProviderEvidence != nil && value.ProviderEvidence.FinishClass != ProviderFinishCancelledV1 && value.ProviderEvidence.FinishClass != ProviderFinishUnknownV1 {
			return incompatibleSubstrateFailure(code)
		}
	case SubstrateExecutionFailureProcessFailedV1:
		if value.Allocation != SubstrateAllocationStartedV1 || value.Process != SubstrateProcessStoppedV1 && value.Process != SubstrateProcessStopUnknownV1 {
			return incompatibleSubstrateFailure(code)
		}
	case SubstrateExecutionFailureOutputBoundExceededV1:
		if value.Allocation != SubstrateAllocationStartedV1 || value.ProviderEvidence != nil && value.ProviderEvidence.FinishClass == ProviderFinishCompletedV1 {
			return incompatibleSubstrateFailure(code)
		}
	case SubstrateExecutionFailureEgressDeniedV1:
		if value.Egress != SubstrateEgressDeniedV1 || value.ProviderEvidence != nil {
			return incompatibleSubstrateFailure(code)
		}
	case SubstrateExecutionFailureProviderFailedV1:
		if value.ProviderEvidence == nil || value.ProviderEvidence.FinishClass != ProviderFinishFailedV1 || value.ProviderEvidence.FailureCode != ProviderExecutionFailureProviderFailedV1 {
			return incompatibleSubstrateFailure(code)
		}
	case SubstrateExecutionFailureAcceptedOutcomeUnknownV1:
		if value.Allocation != SubstrateAllocationStartedV1 || value.ProviderEvidence == nil || value.ProviderEvidence.AcceptanceClass == ProviderAcceptancePreAcceptanceV1 ||
			value.ProviderEvidence.FinishClass != ProviderFinishUnknownV1 || value.ProviderEvidence.FailureCode != ProviderExecutionFailureAcceptedUnknownV1 ||
			value.Egress != SubstrateEgressPolicyEnforcedV1 || value.ImageAttestation != SubstrateAttestationVerifiedV1 ||
			value.BackendAttestation != SubstrateAttestationVerifiedV1 || value.ProxyAttestation != SubstrateProxyAttestationVerifiedV1 {
			return incompatibleSubstrateFailure(code)
		}
	case SubstrateExecutionFailureCredentialFinalizeFailedV1:
		if value.Allocation != SubstrateAllocationStartedV1 || value.CredentialFinalization != CredentialFinalizationFailedV1 {
			return incompatibleSubstrateFailure(code)
		}
	case SubstrateExecutionFailureCleanupFailedV1:
		if value.Allocation != SubstrateAllocationStartedV1 || value.Cleanup != SubstrateCleanupFailedV1 {
			return incompatibleSubstrateFailure(code)
		}
	case SubstrateExecutionFailureBackendFailedV1:
		if value.ProviderEvidence != nil && value.ProviderEvidence.FailureCode != ProviderExecutionFailureBackendV1 {
			return incompatibleSubstrateFailure(code)
		}
	default:
		return incompatibleSubstrateFailure(code)
	}
	return nil
}

func incompatibleSubstrateFailure(code SubstrateExecutionFailureCodeV1) error {
	return ValidationError{Field: "substrate_execution_evidence.failure_code", Reason: "is incompatible with observation: " + string(code)}
}

func (value SubstrateAttestationStateV1) Valid() bool {
	return value == SubstrateAttestationVerifiedV1 || value == SubstrateAttestationMismatchV1 || value == SubstrateAttestationUnknownV1
}

func (value SubstrateProxyAttestationStateV1) Valid() bool {
	return value == SubstrateProxyAttestationNotRequiredV1 || value == SubstrateProxyAttestationVerifiedV1 || value == SubstrateProxyAttestationMismatchV1 || value == SubstrateProxyAttestationUnknownV1
}

func (value SubstrateExecutionFailureCodeV1) Valid() bool {
	switch value {
	case SubstrateExecutionFailureNoneV1, SubstrateExecutionFailureAuthorityDeniedV1, SubstrateExecutionFailureProfileDisabledV1,
		SubstrateExecutionFailureProfileExpiredV1, SubstrateExecutionFailureAttestationMismatchV1, SubstrateExecutionFailureLeaseLostV1,
		SubstrateExecutionFailureCancelledV1, SubstrateExecutionFailureProcessFailedV1, SubstrateExecutionFailureOutputBoundExceededV1,
		SubstrateExecutionFailureEgressDeniedV1, SubstrateExecutionFailureProviderFailedV1, SubstrateExecutionFailureAcceptedOutcomeUnknownV1,
		SubstrateExecutionFailureCredentialFinalizeFailedV1, SubstrateExecutionFailureCleanupFailedV1, SubstrateExecutionFailureBackendFailedV1:
		return true
	default:
		return false
	}
}

func (digest PreparedInvocationDigestV1) Validate() error {
	return validateSHA256("prepared_invocation_digest", string(digest))
}

func (digest SubstrateExecutionEvidenceDigestV1) Validate() error {
	return validateSHA256("substrate_execution_evidence_digest", string(digest))
}

func (value SubstrateExecutionEvidenceV1) digest() (SubstrateExecutionEvidenceDigestV1, error) {
	copy := value.Clone()
	copy.EvidenceDigest = ""
	encoded, err := json.Marshal(copy)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	hash.Write([]byte("sessionless.substrate-execution-evidence.v1"))
	hash.Write([]byte{0})
	hash.Write(encoded)
	return SubstrateExecutionEvidenceDigestV1(hex.EncodeToString(hash.Sum(nil))), nil
}
