// Package harnessconformance provides bounded, credential-free fixtures for
// the production Sessionless harness registry. It is a test/evidence layer,
// not a scheduler, router, policy evaluator, or provider retry loop.
package harnessconformance

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

const VersionV1 uint32 = 1

type (
	OperationV1            string
	RegistryContractV1     string
	BackendProtocolStateV1 string
)

const (
	OperationPreflightV1 OperationV1 = "preflight"
	OperationExecuteV1   OperationV1 = "execute"
	OperationCancelV1    OperationV1 = "cancel"

	RegistryContractPassV1 RegistryContractV1 = "pass"
	RegistryContractNoGoV1 RegistryContractV1 = "no_go"

	BackendProtocolSupportedV1   BackendProtocolStateV1 = "supported"
	BackendProtocolUnsupportedV1 BackendProtocolStateV1 = "unsupported"
	BackendProtocolSkippedV1     BackendProtocolStateV1 = "skipped"
)

type ExpectedV1 struct {
	RegistryContract RegistryContractV1             `json:"registry_contract"`
	BackendProtocol  BackendProtocolStateV1         `json:"backend_protocol"`
	FailureCode      sessionlessharness.FailureCode `json:"failure_code,omitempty"`
}

type FixtureV1 struct {
	Version              uint32                           `json:"version"`
	FixtureID            string                           `json:"fixture_id"`
	Placement            domain.ExecutionPlacementV2      `json:"execution_placement"`
	Binding              domain.HarnessBindingV2          `json:"binding"`
	SubstrateBinding     domain.SubstrateBindingV1        `json:"substrate_binding"`
	AdmissionCostCeiling domain.AdmissionCostCeilingV2    `json:"admission_cost_ceiling"`
	EvidenceBundle       *domain.ProviderEvidenceBundleV1 `json:"evidence_bundle,omitempty"`
	Operation            OperationV1                      `json:"operation"`
	Expected             ExpectedV1                       `json:"expected"`
}

func (fixture FixtureV1) Clone() FixtureV1 {
	clone := fixture
	clone.Binding = fixture.Binding.Clone()
	clone.AdmissionCostCeiling = fixture.AdmissionCostCeiling.Clone()
	if fixture.EvidenceBundle != nil {
		bundle := fixture.EvidenceBundle.Clone()
		clone.EvidenceBundle = &bundle
	}
	return clone
}

func (fixture FixtureV1) Validate() error {
	if fixture.Version != VersionV1 {
		return domain.ValidationError{Field: "provider_conformance.version", Reason: "must equal 1"}
	}
	if err := domain.ValidateOpaqueID("provider_conformance.fixture_id", fixture.FixtureID); err != nil {
		return err
	}
	if err := fixture.Binding.Validate(); err != nil {
		return err
	}
	if err := fixture.Binding.ValidateForScope(fixture.Binding.TenantID, fixture.Binding.OwnerUserID, fixture.Binding.RunID, fixture.Binding.AttemptID, fixture.Placement); err != nil {
		return err
	}
	substrate, cost := fixtureExecutionAuthority(fixture)
	if err := domain.ValidateExecutionAuthorityProjection(fixture.Placement, substrate, cost); err != nil {
		return err
	}
	if err := fixture.Binding.ValidateAdmissionCostCeiling(cost); err != nil {
		return err
	}
	switch fixture.Operation {
	case OperationPreflightV1, OperationExecuteV1, OperationCancelV1:
	default:
		return domain.ValidationError{Field: "provider_conformance.operation", Reason: "is unsupported"}
	}
	if fixture.Binding.Backend.ProviderContractKind == domain.ProviderContractCredentiallessFixtureV1 {
		if fixture.EvidenceBundle != nil {
			return domain.ValidationError{Field: "provider_conformance.evidence_bundle", Reason: "must be absent for the credentialless fixture"}
		}
	} else {
		if fixture.EvidenceBundle == nil {
			return domain.ValidationError{Field: "provider_conformance.evidence_bundle", Reason: "is required for provider invocation"}
		}
		if err := fixture.EvidenceBundle.ValidateForBinding(fixture.Binding); err != nil {
			return err
		}
	}
	if fixture.Expected.RegistryContract != RegistryContractPassV1 && fixture.Expected.RegistryContract != RegistryContractNoGoV1 {
		return domain.ValidationError{Field: "provider_conformance.expected.registry_contract", Reason: "is unsupported"}
	}
	if fixture.Expected.BackendProtocol != BackendProtocolSupportedV1 && fixture.Expected.BackendProtocol != BackendProtocolUnsupportedV1 && fixture.Expected.BackendProtocol != BackendProtocolSkippedV1 {
		return domain.ValidationError{Field: "provider_conformance.expected.backend_protocol", Reason: "is unsupported"}
	}
	if fixture.Expected.RegistryContract == RegistryContractPassV1 {
		if fixture.Expected.FailureCode != "" {
			return domain.ValidationError{Field: "provider_conformance.expected.failure_code", Reason: "must be absent for pass"}
		}
	} else if fixture.Expected.FailureCode == "" {
		return domain.ValidationError{Field: "provider_conformance.expected.failure_code", Reason: "is required for no_go"}
	} else if !fixture.Expected.FailureCode.Valid() {
		return domain.ValidationError{Field: "provider_conformance.expected.failure_code", Reason: "is not in the closed harness taxonomy"}
	}
	return nil
}

func fixtureExecutionAuthority(fixture FixtureV1) (*domain.SubstrateBindingV1, *domain.AdmissionCostCeilingV2) {
	if fixture.Placement.Kind == domain.ExecutionPlacementAttachedWorker {
		if fixture.SubstrateBinding != (domain.SubstrateBindingV1{}) ||
			fixture.AdmissionCostCeiling != (domain.AdmissionCostCeilingV2{}) {
			return &fixture.SubstrateBinding, &fixture.AdmissionCostCeiling
		}
		return nil, nil
	}
	return &fixture.SubstrateBinding, &fixture.AdmissionCostCeiling
}

func (fixture FixtureV1) Digest() (domain.ProviderEvidenceDigestV1, error) {
	if err := fixture.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(fixture)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	hash.Write([]byte("sessionless.provider-conformance-fixture.v1"))
	hash.Write([]byte{0})
	hash.Write(encoded)
	return domain.ProviderEvidenceDigestV1(hex.EncodeToString(hash.Sum(nil))), nil
}

type CheckV1 struct {
	Code   string `json:"code"`
	Passed bool   `json:"passed"`
}

type SideEffectsV1 struct {
	ValidatorCalls             uint64 `json:"validator_calls"`
	DriverPreflights           uint64 `json:"driver_preflights"`
	DriverExecutes             uint64 `json:"driver_executes"`
	DriverCancels              uint64 `json:"driver_cancels"`
	CredentialReads            uint64 `json:"credential_reads"`
	CredentialMaterializations uint64 `json:"credential_materializations"`
	ProcessStarts              uint64 `json:"process_starts"`
	NetworkStarts              uint64 `json:"network_starts"`
	Retries                    uint64 `json:"retries"`
}

type ResultV1 struct {
	Version                         uint32                          `json:"version"`
	FixtureID                       string                          `json:"fixture_id"`
	FixtureDigest                   domain.ProviderEvidenceDigestV1 `json:"fixture_digest"`
	BindingDigest                   domain.HarnessBindingDigestV2   `json:"binding_digest"`
	RegistryContract                RegistryContractV1              `json:"registry_contract"`
	BackendProtocol                 BackendProtocolStateV1          `json:"backend_protocol"`
	FailureCode                     sessionlessharness.FailureCode  `json:"failure_code,omitempty"`
	Checks                          []CheckV1                       `json:"checks"`
	SideEffects                     SideEffectsV1                   `json:"side_effects"`
	ProviderExecutionEvidenceDigest domain.ProviderEvidenceDigestV1 `json:"provider_execution_evidence_digest,omitempty"`
	EvidenceDigest                  domain.ProviderEvidenceDigestV1 `json:"evidence_digest"`
}

func (result ResultV1) Clone() ResultV1 {
	result.Checks = append([]CheckV1(nil), result.Checks...)
	return result
}

func (result ResultV1) Validate() error {
	if result.Version != VersionV1 {
		return domain.ValidationError{Field: "provider_conformance_result.version", Reason: "must equal 1"}
	}
	if err := domain.ValidateOpaqueID("provider_conformance_result.fixture_id", result.FixtureID); err != nil {
		return err
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "fixture_digest", value: string(result.FixtureDigest)},
		{name: "binding_digest", value: string(result.BindingDigest)},
		{name: "evidence_digest", value: string(result.EvidenceDigest)},
	} {
		if len(field.value) != sha256.Size*2 || field.value != strings.ToLower(field.value) {
			return domain.ValidationError{Field: "provider_conformance_result." + field.name, Reason: "must be a lowercase SHA-256 digest"}
		}
		if _, err := hex.DecodeString(field.value); err != nil {
			return err
		}
	}
	if result.ProviderExecutionEvidenceDigest != "" {
		if _, err := hex.DecodeString(string(result.ProviderExecutionEvidenceDigest)); err != nil || len(result.ProviderExecutionEvidenceDigest) != sha256.Size*2 || string(result.ProviderExecutionEvidenceDigest) != strings.ToLower(string(result.ProviderExecutionEvidenceDigest)) {
			return domain.ValidationError{Field: "provider_conformance_result.provider_execution_evidence_digest", Reason: "must be a lowercase SHA-256 digest"}
		}
	}
	if result.RegistryContract != RegistryContractPassV1 && result.RegistryContract != RegistryContractNoGoV1 {
		return domain.ValidationError{Field: "provider_conformance_result.registry_contract", Reason: "is unsupported"}
	}
	if result.BackendProtocol != BackendProtocolSupportedV1 && result.BackendProtocol != BackendProtocolUnsupportedV1 && result.BackendProtocol != BackendProtocolSkippedV1 {
		return domain.ValidationError{Field: "provider_conformance_result.backend_protocol", Reason: "is unsupported"}
	}
	if result.RegistryContract == RegistryContractPassV1 && result.FailureCode != "" {
		return domain.ValidationError{Field: "provider_conformance_result.failure_code", Reason: "must be absent for pass"}
	}
	if result.RegistryContract == RegistryContractNoGoV1 && result.FailureCode == "" {
		return domain.ValidationError{Field: "provider_conformance_result.failure_code", Reason: "is required for no_go"}
	}
	if result.FailureCode != "" && !result.FailureCode.Valid() {
		return domain.ValidationError{Field: "provider_conformance_result.failure_code", Reason: "is not in the closed harness taxonomy"}
	}
	if len(result.Checks) == 0 || len(result.Checks) > 32 {
		return domain.ValidationError{Field: "provider_conformance_result.checks", Reason: "must contain one to 32 checks"}
	}
	previous := ""
	for _, check := range result.Checks {
		if err := domain.ValidateOpaqueID("provider_conformance_result.check.code", check.Code); err != nil {
			return err
		}
		if check.Code <= previous {
			return domain.ValidationError{Field: "provider_conformance_result.checks", Reason: "must be sorted and unique"}
		}
		previous = check.Code
		if !check.Passed {
			return domain.ValidationError{Field: "provider_conformance_result.checks", Reason: "all conformance checks must pass"}
		}
	}
	expected, err := result.digest()
	if err != nil {
		return err
	}
	if result.EvidenceDigest != expected {
		return domain.ValidationError{Field: "provider_conformance_result.evidence_digest", Reason: "does not match canonical result"}
	}
	return nil
}

func (result ResultV1) seal() (ResultV1, error) {
	result = result.Clone()
	sort.Slice(result.Checks, func(i, j int) bool { return result.Checks[i].Code < result.Checks[j].Code })
	result.EvidenceDigest = ""
	digest, err := result.digest()
	if err != nil {
		return ResultV1{}, err
	}
	result.EvidenceDigest = digest
	if err := result.Validate(); err != nil {
		return ResultV1{}, err
	}
	return result, nil
}

func (result ResultV1) digest() (domain.ProviderEvidenceDigestV1, error) {
	copy := result
	copy.EvidenceDigest = ""
	encoded, err := json.Marshal(copy)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	hash.Write([]byte("sessionless.provider-conformance-result.v1"))
	hash.Write([]byte{0})
	hash.Write(encoded)
	return domain.ProviderEvidenceDigestV1(hex.EncodeToString(hash.Sum(nil))), nil
}

func closedFailureCode(err error) sessionlessharness.FailureCode {
	var classified *domain.ClassifiedError
	if errors.As(err, &classified) && classified != nil {
		code := sessionlessharness.FailureCode(classified.Code)
		if code.Valid() {
			return code
		}
	}
	return sessionlessharness.FailureHarnessBackendFailed
}
