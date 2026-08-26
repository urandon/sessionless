package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"time"
)

const (
	SubstrateBindingVersionV1              uint32 = 1
	AdmissionCostCeilingVersionV1          uint32 = 1
	ServerlessInvocationAuthorityVersionV1 uint32 = 1
	PreparedAllocationVersionV1            uint32 = 1
	AttemptEffectReservationVersionV1      uint32 = 1
	ServerlessProviderEffectSequenceV1     uint64 = 1
	maxServerlessDeliveriesV1                     = 100
	maxServerlessInvocationV1                     = 24 * time.Hour
)

type (
	SubstrateKindV1                       string
	SubstrateWorkloadModeV1               string
	SubstrateBindingDigestV1              string
	AdmissionCostCeilingDigestV1          string
	ServerlessInvocationAuthorityDigestV1 string
	PreparedAllocationDigestV1            string
	AttemptEffectReservationDigestV1      string
	ProviderEffectKindV1                  string
	CostEvidenceStateV1                   string
	ProviderPriceStateV1                  string
)

const (
	SubstrateYandexServerlessContainerV1 SubstrateKindV1 = "yandex_serverless_container"
	SubstrateCloudRunJobV1               SubstrateKindV1 = "cloud_run_job"
	SubstrateAzureDynamicSessionV1       SubstrateKindV1 = "azure_dynamic_session"
	SubstrateManagedSandboxV1            SubstrateKindV1 = "managed_sandbox"
	SubstrateDeterministicFixtureV1      SubstrateKindV1 = "deterministic_fixture"

	SubstrateWorkloadChildProcessV1    SubstrateWorkloadModeV1 = "child_process"
	SubstrateWorkloadInProcessDirectV1 SubstrateWorkloadModeV1 = "in_process_direct"

	ProviderEffectTurnV1 ProviderEffectKindV1 = "provider_turn"

	CostEvidenceUnknownV1 CostEvidenceStateV1 = "unknown"
	CostEvidenceKnownV1   CostEvidenceStateV1 = "known"

	ProviderPriceUnknownV1   ProviderPriceStateV1 = "unknown"
	ProviderPriceKnownFreeV1 ProviderPriceStateV1 = "known_free"
	ProviderPriceKnownV1     ProviderPriceStateV1 = "known"
)

// SubstrateLimitsV1 is the immutable resource and byte budget of one exact
// substrate profile. It is authority, not an observation of actual use.
type SubstrateLimitsV1 struct {
	InvocationTimeout time.Duration `json:"invocation_timeout"`
	ExecutionTimeout  time.Duration `json:"execution_timeout"`
	CleanupTimeout    time.Duration `json:"cleanup_timeout"`
	CPUMillis         uint64        `json:"cpu_millis"`
	MemoryBytes       uint64        `json:"memory_bytes"`
	ScratchBytes      uint64        `json:"scratch_bytes"`
	StdoutBytes       uint64        `json:"stdout_bytes"`
	StderrBytes       uint64        `json:"stderr_bytes"`
	NativeEventCount  uint64        `json:"native_event_count"`
	ArtifactBytes     uint64        `json:"artifact_bytes"`
}

func (limits SubstrateLimitsV1) Validate() error {
	if limits.InvocationTimeout <= 0 || limits.InvocationTimeout > maxServerlessInvocationV1 {
		return ValidationError{Field: "substrate_binding.limits.invocation_timeout", Reason: "must be positive and bounded"}
	}
	if limits.ExecutionTimeout <= 0 || limits.CleanupTimeout <= 0 || limits.ExecutionTimeout+limits.CleanupTimeout >= limits.InvocationTimeout {
		return ValidationError{Field: "substrate_binding.limits", Reason: "execution and cleanup must fit strictly inside invocation timeout"}
	}
	for field, value := range map[string]uint64{
		"cpu_millis": limits.CPUMillis, "memory_bytes": limits.MemoryBytes, "scratch_bytes": limits.ScratchBytes,
		"stdout_bytes": limits.StdoutBytes, "stderr_bytes": limits.StderrBytes, "native_event_count": limits.NativeEventCount,
		"artifact_bytes": limits.ArtifactBytes,
	} {
		if value == 0 {
			return ValidationError{Field: "substrate_binding.limits." + field, Reason: "must be positive"}
		}
	}
	return nil
}

// AdmissionCostCeilingV1 is the pre-effect cost and redelivery authority.
// Unknown prices are representable but ValidateAt rejects them for admission.
type AdmissionCostCeilingV1 struct {
	Version                         uint32               `json:"version"`
	Currency                        string               `json:"currency"`
	PriceRevision                   string               `json:"price_revision"`
	PriceObservedAt                 time.Time            `json:"price_observed_at"`
	PriceExpiresAt                  time.Time            `json:"price_expires_at"`
	MaxDeliveries                   uint32               `json:"max_deliveries"`
	MaxPreEffectDurationPerDelivery time.Duration        `json:"max_pre_effect_duration_per_delivery"`
	MaxActiveDuration               time.Duration        `json:"max_active_duration"`
	MaxCleanupAndReconcileDuration  time.Duration        `json:"max_cleanup_and_reconcile_duration"`
	ConfiguredMemoryBytes           uint64               `json:"configured_memory_bytes"`
	ConfiguredVCPUMillis            uint64               `json:"configured_vcpu_millis"`
	MaxIngressBytes                 uint64               `json:"max_ingress_bytes"`
	MaxEgressBytes                  uint64               `json:"max_egress_bytes"`
	MaxLogBytes                     uint64               `json:"max_log_bytes"`
	MaxEvidenceBytes                uint64               `json:"max_evidence_bytes"`
	SubstratePriceState             CostEvidenceStateV1  `json:"substrate_price_state"`
	ProviderPriceState              ProviderPriceStateV1 `json:"provider_price_state"`
	MaxSubstrateAmountMicrounits    *uint64              `json:"max_substrate_amount_microunits,omitempty"`
	MaxProviderAmountMicrounits     *uint64              `json:"max_provider_amount_microunits,omitempty"`
	MaxTotalAmountMicrounits        *uint64              `json:"max_total_amount_microunits,omitempty"`
}

func (value AdmissionCostCeilingV1) Clone() AdmissionCostCeilingV1 {
	clone := value
	clone.MaxSubstrateAmountMicrounits = cloneUint64(value.MaxSubstrateAmountMicrounits)
	clone.MaxProviderAmountMicrounits = cloneUint64(value.MaxProviderAmountMicrounits)
	clone.MaxTotalAmountMicrounits = cloneUint64(value.MaxTotalAmountMicrounits)
	return clone
}

func (value AdmissionCostCeilingV1) Validate() error {
	if value.Version != AdmissionCostCeilingVersionV1 {
		return ValidationError{Field: "admission_cost_ceiling.version", Reason: "must equal 1"}
	}
	if !validCurrencyV1(value.Currency) {
		return ValidationError{Field: "admission_cost_ceiling.currency", Reason: "must be three uppercase ASCII letters"}
	}
	if err := validateProviderToken("admission_cost_ceiling.price_revision", value.PriceRevision, 128); err != nil {
		return err
	}
	if value.PriceObservedAt.IsZero() || !value.PriceExpiresAt.After(value.PriceObservedAt) {
		return ValidationError{Field: "admission_cost_ceiling.price_expires_at", Reason: "must be after a non-zero observation time"}
	}
	if value.MaxDeliveries == 0 || value.MaxDeliveries > maxServerlessDeliveriesV1 {
		return ValidationError{Field: "admission_cost_ceiling.max_deliveries", Reason: "must be positive and bounded"}
	}
	for field, duration := range map[string]time.Duration{
		"max_pre_effect_duration_per_delivery": value.MaxPreEffectDurationPerDelivery,
		"max_active_duration":                  value.MaxActiveDuration,
		"max_cleanup_and_reconcile_duration":   value.MaxCleanupAndReconcileDuration,
	} {
		if duration <= 0 || duration > maxServerlessInvocationV1 {
			return ValidationError{Field: "admission_cost_ceiling." + field, Reason: "must be positive and bounded"}
		}
	}
	for field, quantity := range map[string]uint64{
		"configured_memory_bytes": value.ConfiguredMemoryBytes, "configured_vcpu_millis": value.ConfiguredVCPUMillis,
		"max_ingress_bytes": value.MaxIngressBytes, "max_egress_bytes": value.MaxEgressBytes,
		"max_log_bytes": value.MaxLogBytes, "max_evidence_bytes": value.MaxEvidenceBytes,
	} {
		if quantity == 0 {
			return ValidationError{Field: "admission_cost_ceiling." + field, Reason: "must be positive"}
		}
	}
	if err := validateCostAmounts(value); err != nil {
		return err
	}
	return nil
}

func (value AdmissionCostCeilingV1) ValidateAt(now time.Time) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if now.IsZero() || !now.UTC().Before(value.PriceExpiresAt.UTC()) {
		return ValidationError{Field: "admission_cost_ceiling.price_expires_at", Reason: "must be later than authoritative admission time"}
	}
	if value.SubstratePriceState != CostEvidenceKnownV1 || value.ProviderPriceState == ProviderPriceUnknownV1 || value.MaxTotalAmountMicrounits == nil {
		return ValidationError{Field: "admission_cost_ceiling", Reason: "unknown price cannot authorize execution"}
	}
	return nil
}

func (value AdmissionCostCeilingV1) Digest() (AdmissionCostCeilingDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	digest := newServerlessDigest("sessionless.admission-cost-ceiling.v1")
	digest.uint(uint64(value.Version))
	digest.str(value.Currency)
	digest.str(value.PriceRevision)
	digest.instant(value.PriceObservedAt)
	digest.instant(value.PriceExpiresAt)
	digest.uint(uint64(value.MaxDeliveries))
	digest.duration(value.MaxPreEffectDurationPerDelivery)
	digest.duration(value.MaxActiveDuration)
	digest.duration(value.MaxCleanupAndReconcileDuration)
	for _, quantity := range []uint64{value.ConfiguredMemoryBytes, value.ConfiguredVCPUMillis, value.MaxIngressBytes, value.MaxEgressBytes, value.MaxLogBytes, value.MaxEvidenceBytes} {
		digest.uint(quantity)
	}
	digest.str(string(value.SubstratePriceState))
	digest.str(string(value.ProviderPriceState))
	digest.optionalUint(value.MaxSubstrateAmountMicrounits)
	digest.optionalUint(value.MaxProviderAmountMicrounits)
	digest.optionalUint(value.MaxTotalAmountMicrounits)
	return AdmissionCostCeilingDigestV1(digest.sum()), nil
}

// SubstrateBindingV1 is one immutable, server-owned runtime profile. It does
// not select a provider and has no fallback semantics.
type SubstrateBindingV1 struct {
	Version                    uint32                       `json:"version"`
	Kind                       SubstrateKindV1              `json:"kind"`
	ProfileID                  string                       `json:"profile_id"`
	ProfileRevision            uint64                       `json:"profile_revision"`
	ProfileDigest              string                       `json:"profile_digest"`
	ProfileEvidenceExpiresAt   time.Time                    `json:"profile_evidence_expires_at"`
	Region                     string                       `json:"region"`
	ImageDigest                string                       `json:"image_digest"`
	OuterHarnessArtifactDigest string                       `json:"outer_harness_artifact_digest"`
	WorkloadMode               SubstrateWorkloadModeV1      `json:"workload_mode"`
	IsolationProfileDigest     string                       `json:"isolation_profile_digest"`
	EgressPolicyDigest         string                       `json:"egress_policy_digest"`
	CleanupPolicyDigest        string                       `json:"cleanup_policy_digest"`
	EgressProxyArtifactDigest  string                       `json:"egress_proxy_artifact_digest"`
	EgressProxyIdentityDigest  string                       `json:"egress_proxy_identity_digest"`
	AdmissionCostCeilingDigest AdmissionCostCeilingDigestV1 `json:"admission_cost_ceiling_digest"`
	Limits                     SubstrateLimitsV1            `json:"limits"`
}

func (value SubstrateBindingV1) Validate() error {
	if value.Version != SubstrateBindingVersionV1 {
		return ValidationError{Field: "substrate_binding.version", Reason: "must equal 1"}
	}
	switch value.Kind {
	case SubstrateYandexServerlessContainerV1, SubstrateCloudRunJobV1, SubstrateAzureDynamicSessionV1, SubstrateManagedSandboxV1, SubstrateDeterministicFixtureV1:
	default:
		return ValidationError{Field: "substrate_binding.kind", Reason: "is unsupported"}
	}
	if err := ValidateOpaqueID("substrate_binding.profile_id", value.ProfileID); err != nil {
		return err
	}
	if value.ProfileRevision == 0 {
		return ValidationError{Field: "substrate_binding.profile_revision", Reason: "must be positive"}
	}
	if value.ProfileEvidenceExpiresAt.IsZero() {
		return ValidationError{Field: "substrate_binding.profile_evidence_expires_at", Reason: "must not be zero"}
	}
	if err := validateProviderToken("substrate_binding.region", value.Region, 128); err != nil {
		return err
	}
	for field, digest := range map[string]string{
		"profile_digest": value.ProfileDigest, "image_digest": value.ImageDigest,
		"outer_harness_artifact_digest": value.OuterHarnessArtifactDigest,
		"isolation_profile_digest":      value.IsolationProfileDigest, "egress_policy_digest": value.EgressPolicyDigest,
		"cleanup_policy_digest": value.CleanupPolicyDigest, "egress_proxy_artifact_digest": value.EgressProxyArtifactDigest,
		"egress_proxy_identity_digest": value.EgressProxyIdentityDigest,
	} {
		if err := validateSHA256("substrate_binding."+field, digest); err != nil {
			return err
		}
	}
	if err := value.AdmissionCostCeilingDigest.Validate(); err != nil {
		return err
	}
	if value.WorkloadMode != SubstrateWorkloadChildProcessV1 && value.WorkloadMode != SubstrateWorkloadInProcessDirectV1 {
		return ValidationError{Field: "substrate_binding.workload_mode", Reason: "is unsupported"}
	}
	return value.Limits.Validate()
}

func (value SubstrateBindingV1) ValidateAt(now time.Time) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if now.IsZero() || !now.UTC().Before(value.ProfileEvidenceExpiresAt.UTC()) {
		return ValidationError{Field: "substrate_binding.profile_evidence_expires_at", Reason: "must be later than authoritative execution time"}
	}
	return nil
}

func (value SubstrateBindingV1) Digest() (SubstrateBindingDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	digest := newServerlessDigest("sessionless.substrate-binding.v1")
	digest.uint(uint64(value.Version))
	digest.str(string(value.Kind))
	digest.str(value.ProfileID)
	digest.uint(value.ProfileRevision)
	for _, field := range []string{value.ProfileDigest, value.Region, value.ImageDigest, value.OuterHarnessArtifactDigest, string(value.WorkloadMode), value.IsolationProfileDigest, value.EgressPolicyDigest, value.CleanupPolicyDigest, value.EgressProxyArtifactDigest, value.EgressProxyIdentityDigest, string(value.AdmissionCostCeilingDigest)} {
		digest.str(field)
	}
	digest.instant(value.ProfileEvidenceExpiresAt)
	digest.duration(value.Limits.InvocationTimeout)
	digest.duration(value.Limits.ExecutionTimeout)
	digest.duration(value.Limits.CleanupTimeout)
	for _, quantity := range []uint64{value.Limits.CPUMillis, value.Limits.MemoryBytes, value.Limits.ScratchBytes, value.Limits.StdoutBytes, value.Limits.StderrBytes, value.Limits.NativeEventCount, value.Limits.ArtifactBytes} {
		digest.uint(quantity)
	}
	return SubstrateBindingDigestV1(digest.sum()), nil
}

func (digest SubstrateBindingDigestV1) Validate() error {
	return validateSHA256("substrate_binding_digest", string(digest))
}

func (digest AdmissionCostCeilingDigestV1) Validate() error {
	return validateSHA256("admission_cost_ceiling_digest", string(digest))
}

// ValidateExecutionAuthorityProjection enforces the tagged placement union.
// Managed V2 always carries the exact substrate and cost authority; attached
// workers carry neither and remain governed by their existing protocol.
func ValidateExecutionAuthorityProjection(placement ExecutionPlacementV2, substrate *SubstrateBindingV1, cost *AdmissionCostCeilingV1) error {
	if err := placement.Validate(); err != nil {
		return err
	}
	switch placement.Kind {
	case ExecutionPlacementManaged:
		if substrate == nil || cost == nil {
			return ValidationError{Field: "execution_authority", Reason: "managed placement requires substrate and cost authority"}
		}
		if err := substrate.Validate(); err != nil {
			return err
		}
		if err := cost.Validate(); err != nil {
			return err
		}
		substrateDigest, _ := substrate.Digest()
		costDigest, _ := cost.Digest()
		if placement.SubstrateBindingDigest != string(substrateDigest) || substrate.AdmissionCostCeilingDigest != costDigest {
			return ValidationError{Field: "execution_authority", Reason: "placement, substrate and cost digests must form one exact chain"}
		}
	case ExecutionPlacementAttachedWorker:
		if substrate != nil || cost != nil {
			return ValidationError{Field: "execution_authority", Reason: "attached-worker placement must not contain serverless authority"}
		}
	default:
		return ValidationError{Field: "execution_authority", Reason: "placement kind is unsupported"}
	}
	return nil
}

type serverlessDigest struct {
	hash       [sha256.Size]byte
	transcript []byte
}

func newServerlessDigest(domain string) *serverlessDigest {
	return &serverlessDigest{transcript: append([]byte(domain), 0)}
}
func (digest *serverlessDigest) bytes(value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	digest.transcript = append(digest.transcript, size[:]...)
	digest.transcript = append(digest.transcript, value...)
}
func (digest *serverlessDigest) str(value string) { digest.bytes([]byte(value)) }
func (digest *serverlessDigest) uint(value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	digest.bytes(encoded[:])
}
func (digest *serverlessDigest) duration(value time.Duration) { digest.uint(uint64(value)) }
func (digest *serverlessDigest) instant(value time.Time) {
	digest.str(value.UTC().Format(time.RFC3339Nano))
}
func (digest *serverlessDigest) optionalUint(value *uint64) {
	if value == nil {
		digest.uint(0)
		return
	}
	digest.uint(1)
	digest.uint(*value)
}
func (digest *serverlessDigest) sum() string {
	sum := sha256.Sum256(digest.transcript)
	digest.hash = sum
	return hex.EncodeToString(sum[:])
}

func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func validCurrencyV1(value string) bool {
	if len(value) != 3 {
		return false
	}
	for _, character := range value {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}

func validateCostAmounts(value AdmissionCostCeilingV1) error {
	switch value.SubstratePriceState {
	case CostEvidenceUnknownV1:
		if value.MaxSubstrateAmountMicrounits != nil || value.MaxTotalAmountMicrounits != nil {
			return ValidationError{Field: "admission_cost_ceiling.substrate_price_state", Reason: "unknown price must not carry numeric ceilings"}
		}
	case CostEvidenceKnownV1:
		if value.MaxSubstrateAmountMicrounits == nil {
			return ValidationError{Field: "admission_cost_ceiling.max_substrate_amount_microunits", Reason: "is required for known substrate price"}
		}
	default:
		return ValidationError{Field: "admission_cost_ceiling.substrate_price_state", Reason: "is unsupported"}
	}
	switch value.ProviderPriceState {
	case ProviderPriceUnknownV1:
		if value.MaxProviderAmountMicrounits != nil || value.MaxTotalAmountMicrounits != nil {
			return ValidationError{Field: "admission_cost_ceiling.provider_price_state", Reason: "unknown price must not carry numeric ceilings"}
		}
	case ProviderPriceKnownFreeV1:
		if value.MaxProviderAmountMicrounits == nil || *value.MaxProviderAmountMicrounits != 0 {
			return ValidationError{Field: "admission_cost_ceiling.max_provider_amount_microunits", Reason: "known free requires an explicit zero"}
		}
	case ProviderPriceKnownV1:
		if value.MaxProviderAmountMicrounits == nil {
			return ValidationError{Field: "admission_cost_ceiling.max_provider_amount_microunits", Reason: "is required for known provider price"}
		}
	default:
		return ValidationError{Field: "admission_cost_ceiling.provider_price_state", Reason: "is unsupported"}
	}
	if value.SubstratePriceState == CostEvidenceKnownV1 && value.ProviderPriceState != ProviderPriceUnknownV1 {
		if value.MaxTotalAmountMicrounits == nil {
			return ValidationError{Field: "admission_cost_ceiling.max_total_amount_microunits", Reason: "is required when all prices are known"}
		}
		if *value.MaxSubstrateAmountMicrounits > ^uint64(0)-*value.MaxProviderAmountMicrounits || *value.MaxTotalAmountMicrounits != *value.MaxSubstrateAmountMicrounits+*value.MaxProviderAmountMicrounits {
			return ValidationError{Field: "admission_cost_ceiling.max_total_amount_microunits", Reason: "must equal the bounded substrate and provider ceilings without overflow"}
		}
	}
	return nil
}

func validateServerlessLease(lease Lease) error {
	for _, validate := range []func() error{lease.ID.Validate, lease.TenantID.Validate, lease.RunID.Validate, lease.AttemptID.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if err := ValidateOpaqueID("lease.worker_id", lease.WorkerID); err != nil {
		return err
	}
	if lease.FenceToken == 0 {
		return ValidationError{Field: "lease.fence_token", Reason: "must be positive"}
	}
	if lease.AcquiredAt.IsZero() || !lease.ExpiresAt.After(lease.AcquiredAt) {
		return ValidationError{Field: "lease.expires_at", Reason: "must be after a non-zero acquired_at"}
	}
	return nil
}

func validateDigestToken(field, value string) error {
	if strings.TrimSpace(value) != value {
		return ValidationError{Field: field, Reason: "must be canonical"}
	}
	return validateSHA256(field, value)
}

// ValidateSHA256Digest exposes the canonical lowercase SHA-256 grammar to
// ports without duplicating the repository digest contract.
func ValidateSHA256Digest(field, value string) error {
	return validateSHA256(field, value)
}
