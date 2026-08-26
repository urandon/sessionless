package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

const ProviderExecutionEvidenceVersionV1 uint32 = 1

type (
	ProviderAcceptanceClassV1      string
	ProviderFinishClassV1          string
	ProviderUsageProvenanceV1      string
	ProviderExecutionFailureCodeV1 string
)

const (
	ProviderAcceptanceUnknownV1       ProviderAcceptanceClassV1 = "unknown"
	ProviderAcceptancePreAcceptanceV1 ProviderAcceptanceClassV1 = "pre_acceptance"
	ProviderAcceptanceAcceptedV1      ProviderAcceptanceClassV1 = "accepted"

	ProviderFinishUnknownV1   ProviderFinishClassV1 = "unknown"
	ProviderFinishCompletedV1 ProviderFinishClassV1 = "completed"
	ProviderFinishFailedV1    ProviderFinishClassV1 = "failed"
	ProviderFinishCancelledV1 ProviderFinishClassV1 = "cancelled"

	ProviderUsageUnknownV1          ProviderUsageProvenanceV1 = "unknown"
	ProviderUsageProviderReportedV1 ProviderUsageProvenanceV1 = "provider_reported"
	ProviderUsageHarnessMeasuredV1  ProviderUsageProvenanceV1 = "harness_measured"

	ProviderExecutionFailurePreAcceptanceV1      ProviderExecutionFailureCodeV1 = "pre_acceptance_failure"
	ProviderExecutionFailureAcceptedUnknownV1    ProviderExecutionFailureCodeV1 = "accepted_outcome_unknown"
	ProviderExecutionFailureProviderFailedV1     ProviderExecutionFailureCodeV1 = "provider_failed"
	ProviderExecutionFailureCancelledV1          ProviderExecutionFailureCodeV1 = "cancelled"
	ProviderExecutionFailureProtocolDriftV1      ProviderExecutionFailureCodeV1 = "protocol_drift"
	ProviderExecutionFailureTeardownV1           ProviderExecutionFailureCodeV1 = "teardown_failed"
	ProviderExecutionFailureCredentialFinalizeV1 ProviderExecutionFailureCodeV1 = "credential_finalization_failed"
	ProviderExecutionFailurePolicyDeniedV1       ProviderExecutionFailureCodeV1 = "policy_denied"
	ProviderExecutionFailureBackendV1            ProviderExecutionFailureCodeV1 = "backend_failed"
)

// ProviderExecutionEvidenceV1 is the bounded, provider-neutral terminal
// observation returned by a harness backend. It never contains prompts,
// provider bodies, raw frames, stdout/stderr, credentials, or local paths.
// Unknown route and usage remain explicit instead of being inferred.
type ProviderExecutionEvidenceV1 struct {
	Version               uint32                         `json:"version"`
	BindingDigest         HarnessBindingDigestV1         `json:"binding_digest"`
	AcceptanceClass       ProviderAcceptanceClassV1      `json:"acceptance_class"`
	FinishClass           ProviderFinishClassV1          `json:"finish_class"`
	RouteState            ProviderEvidenceStateV1        `json:"route_state"`
	ActualModelVendorID   string                         `json:"actual_model_vendor_id,omitempty"`
	ActualModelID         string                         `json:"actual_model_id,omitempty"`
	TransportKind         ProviderTransportKindV1        `json:"transport_kind,omitempty"`
	TransportProvider     string                         `json:"transport_provider,omitempty"`
	UpstreamProviderID    string                         `json:"upstream_provider_id,omitempty"`
	EndpointID            string                         `json:"endpoint_id,omitempty"`
	InputDataClass        ProviderDataClassV1            `json:"input_data_class"`
	EffectivePolicyDigest string                         `json:"effective_policy_digest"`
	PolicyVerdict         ProviderPolicyVerdictV1        `json:"policy_verdict"`
	UsageProvenance       ProviderUsageProvenanceV1      `json:"usage_provenance"`
	InputTokens           *uint64                        `json:"input_tokens,omitempty"`
	OutputTokens          *uint64                        `json:"output_tokens,omitempty"`
	FailureCode           ProviderExecutionFailureCodeV1 `json:"failure_code,omitempty"`
	EvidenceDigest        ProviderEvidenceDigestV1       `json:"evidence_digest"`
}

func (value ProviderExecutionEvidenceV1) Clone() ProviderExecutionEvidenceV1 {
	clone := value
	if value.InputTokens != nil {
		input := *value.InputTokens
		clone.InputTokens = &input
	}
	if value.OutputTokens != nil {
		output := *value.OutputTokens
		clone.OutputTokens = &output
	}
	return clone
}

func (value ProviderExecutionEvidenceV1) SealForBinding(binding HarnessBindingV1) (ProviderExecutionEvidenceV1, error) {
	if err := binding.Validate(); err != nil {
		return ProviderExecutionEvidenceV1{}, err
	}
	bindingDigest, err := binding.Digest()
	if err != nil {
		return ProviderExecutionEvidenceV1{}, err
	}
	value.Version = ProviderExecutionEvidenceVersionV1
	value.BindingDigest = bindingDigest
	value.InputDataClass = binding.InputDataClass
	value.EffectivePolicyDigest = binding.EffectivePolicyDigest
	value.EvidenceDigest = ""
	if err := value.validateIntrinsic(); err != nil {
		return ProviderExecutionEvidenceV1{}, err
	}
	digest, err := value.digest()
	if err != nil {
		return ProviderExecutionEvidenceV1{}, err
	}
	value.EvidenceDigest = digest
	return value, nil
}

func (value ProviderExecutionEvidenceV1) ValidateForBinding(binding HarnessBindingV1) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	bindingDigest, err := binding.Digest()
	if err != nil {
		return err
	}
	if value.BindingDigest != bindingDigest || value.InputDataClass != binding.InputDataClass || value.EffectivePolicyDigest != binding.EffectivePolicyDigest {
		return ValidationError{Field: "provider_execution_evidence.authority", Reason: "must match the sealed harness binding"}
	}
	if value.RouteState == ProviderEvidenceSupportedV1 && (value.ActualModelVendorID != binding.ModelVendorID || value.ActualModelID != binding.ModelID) {
		return ValidationError{Field: "provider_execution_evidence.actual_model", Reason: "must match the sealed model vendor and model"}
	}
	if err := value.validateIntrinsic(); err != nil {
		return err
	}
	if err := validateSHA256("provider_execution_evidence.evidence_digest", string(value.EvidenceDigest)); err != nil {
		return err
	}
	expected, err := value.digest()
	if err != nil {
		return err
	}
	if value.EvidenceDigest != expected {
		return ValidationError{Field: "provider_execution_evidence.evidence_digest", Reason: "does not match canonical evidence"}
	}
	return nil
}

func (value ProviderExecutionEvidenceV1) validateIntrinsic() error {
	if value.Version != ProviderExecutionEvidenceVersionV1 {
		return ValidationError{Field: "provider_execution_evidence.version", Reason: "must equal 1"}
	}
	if err := value.BindingDigest.Validate(); err != nil {
		return err
	}
	switch value.AcceptanceClass {
	case ProviderAcceptanceUnknownV1, ProviderAcceptancePreAcceptanceV1, ProviderAcceptanceAcceptedV1:
	default:
		return ValidationError{Field: "provider_execution_evidence.acceptance_class", Reason: "is unsupported"}
	}
	switch value.FinishClass {
	case ProviderFinishUnknownV1, ProviderFinishCompletedV1, ProviderFinishFailedV1, ProviderFinishCancelledV1:
	default:
		return ValidationError{Field: "provider_execution_evidence.finish_class", Reason: "is unsupported"}
	}
	if value.FinishClass == ProviderFinishCompletedV1 && value.AcceptanceClass != ProviderAcceptanceAcceptedV1 {
		return ValidationError{Field: "provider_execution_evidence", Reason: "completed execution requires accepted provider work"}
	}
	if value.InputDataClass != ProviderDataPublicV1 && value.InputDataClass != ProviderDataExternallyShareableV1 && value.InputDataClass != ProviderDataPrivateV1 {
		return ValidationError{Field: "provider_execution_evidence.input_data_class", Reason: "is unsupported"}
	}
	if err := validateSHA256("provider_execution_evidence.effective_policy_digest", value.EffectivePolicyDigest); err != nil {
		return err
	}
	switch value.PolicyVerdict {
	case ProviderPolicyUnknownV1, ProviderPolicyGoV1, ProviderPolicyConditionalV1, ProviderPolicyNoGoV1:
	default:
		return ValidationError{Field: "provider_execution_evidence.policy_verdict", Reason: "is unsupported"}
	}
	if value.FinishClass == ProviderFinishCompletedV1 && value.PolicyVerdict != ProviderPolicyGoV1 && value.PolicyVerdict != ProviderPolicyConditionalV1 {
		return ValidationError{Field: "provider_execution_evidence.policy_verdict", Reason: "completed execution requires go or conditional policy"}
	}
	if value.RouteState == ProviderEvidenceUnknownV1 {
		if value.ActualModelVendorID != "" || value.ActualModelID != "" || value.TransportKind != "" || value.TransportProvider != "" || value.UpstreamProviderID != "" || value.EndpointID != "" {
			return ValidationError{Field: "provider_execution_evidence.route", Reason: "unknown route must not invent observed identifiers"}
		}
	} else if value.RouteState == ProviderEvidenceSupportedV1 {
		if err := validateProviderToken("provider_execution_evidence.actual_model_vendor_id", value.ActualModelVendorID, 128); err != nil {
			return err
		}
		if err := validateProviderToken("provider_execution_evidence.actual_model_id", value.ActualModelID, 256); err != nil {
			return err
		}
		switch value.TransportKind {
		case ProviderTransportLocalCLIV1, ProviderTransportDirectAPIV1, ProviderTransportRouterAPIV1:
		default:
			return ValidationError{Field: "provider_execution_evidence.transport_kind", Reason: "is unsupported"}
		}
		for _, field := range []struct {
			name  string
			value string
		}{
			{name: "transport_provider", value: value.TransportProvider},
			{name: "upstream_provider_id", value: value.UpstreamProviderID},
			{name: "endpoint_id", value: value.EndpointID},
		} {
			if err := validateProviderToken("provider_execution_evidence."+field.name, field.value, 128); err != nil {
				return err
			}
		}
	} else {
		return ValidationError{Field: "provider_execution_evidence.route_state", Reason: "must be unknown or supported"}
	}
	switch value.UsageProvenance {
	case ProviderUsageUnknownV1:
		if value.InputTokens != nil || value.OutputTokens != nil {
			return ValidationError{Field: "provider_execution_evidence.usage", Reason: "unknown usage must not invent quantities"}
		}
	case ProviderUsageProviderReportedV1, ProviderUsageHarnessMeasuredV1:
		if value.InputTokens == nil || value.OutputTokens == nil {
			return ValidationError{Field: "provider_execution_evidence.usage", Reason: "known usage requires explicit input and output quantities"}
		}
	default:
		return ValidationError{Field: "provider_execution_evidence.usage_provenance", Reason: "is unsupported"}
	}
	if !validProviderExecutionLifecycle(value.AcceptanceClass, value.FinishClass, value.FailureCode) {
		return ValidationError{Field: "provider_execution_evidence.lifecycle", Reason: "acceptance, finish, and failure code are incompatible"}
	}
	return nil
}

func validProviderExecutionLifecycle(acceptance ProviderAcceptanceClassV1, finish ProviderFinishClassV1, failure ProviderExecutionFailureCodeV1) bool {
	switch acceptance {
	case ProviderAcceptancePreAcceptanceV1:
		return finish == ProviderFinishFailedV1 && failure == ProviderExecutionFailurePreAcceptanceV1
	case ProviderAcceptanceAcceptedV1:
		switch finish {
		case ProviderFinishCompletedV1:
			return failure == ""
		case ProviderFinishFailedV1:
			return failure == ProviderExecutionFailureProviderFailedV1 || failure == ProviderExecutionFailureProtocolDriftV1 ||
				failure == ProviderExecutionFailureTeardownV1 || failure == ProviderExecutionFailureCredentialFinalizeV1 ||
				failure == ProviderExecutionFailureBackendV1
		case ProviderFinishCancelledV1:
			return failure == ProviderExecutionFailureCancelledV1
		case ProviderFinishUnknownV1:
			return failure == ProviderExecutionFailureAcceptedUnknownV1
		}
	case ProviderAcceptanceUnknownV1:
		switch finish {
		case ProviderFinishFailedV1:
			return failure == ProviderExecutionFailurePolicyDeniedV1 || failure == ProviderExecutionFailureProtocolDriftV1 || failure == ProviderExecutionFailureBackendV1
		case ProviderFinishCancelledV1:
			return failure == ProviderExecutionFailureCancelledV1
		case ProviderFinishUnknownV1:
			return failure == ProviderExecutionFailureAcceptedUnknownV1
		}
	}
	return false
}

func (code ProviderExecutionFailureCodeV1) Valid() bool {
	switch code {
	case ProviderExecutionFailurePreAcceptanceV1, ProviderExecutionFailureAcceptedUnknownV1,
		ProviderExecutionFailureProviderFailedV1, ProviderExecutionFailureCancelledV1,
		ProviderExecutionFailureProtocolDriftV1, ProviderExecutionFailureTeardownV1,
		ProviderExecutionFailureCredentialFinalizeV1, ProviderExecutionFailurePolicyDeniedV1,
		ProviderExecutionFailureBackendV1:
		return true
	default:
		return false
	}
}

func (value ProviderExecutionEvidenceV1) digest() (ProviderEvidenceDigestV1, error) {
	copy := value.Clone()
	copy.EvidenceDigest = ""
	encoded, err := json.Marshal(copy)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	hash.Write([]byte("sessionless.provider-execution-evidence.v1"))
	hash.Write([]byte{0})
	hash.Write(encoded)
	return ProviderEvidenceDigestV1(hex.EncodeToString(hash.Sum(nil))), nil
}
