package configuredinference

import (
	"bytes"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// SealedRetainedTextV1 is historical DATA, not a dispatch-capable ResponseV1.
// The original model allow-list, calibration and profile-relative effective
// limits were verified before erasure and are attested by the terminal seal.
type SealedRetainedTextV1 struct{ Text string }

// DecodeSealedRetainedTextV1 checks the closed original normalized encoding and
// exact original reservation/observation/profile identity without reconstructing
// the expired installation. The caller MUST verify the original terminal seal.
// Unsupported tool material is denied, never converted to a model response.
func DecodeSealedRetainedTextV1(profileDigest string, r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, body []byte) (SealedRetainedTextV1, error) {
	op := r.Operation
	if domain.ValidateSHA256Digest("retained.profile", profileDigest) != nil || r.Validate() != nil || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(op.Debit) || (op.Kind != domain.AttemptOperationModelV1 && op.Kind != domain.AttemptOperationCompactionV1) || len(body) == 0 || uint64(len(body)) > normalizedBodyLimitV1 || o.ResponseBytes != uint64(len(body)) || o.ResponseDigest != rawDigest(body) {
		return SealedRetainedTextV1{}, ErrContract
	}
	if _, err := strictJSON(body, normalizedBodyLimitV1, 64, 4096); err != nil {
		return SealedRetainedTextV1{}, ErrContract
	}
	var v normalizedBodyV1
	if json.Unmarshal(body, &v) != nil || v.Version != 1 || v.Kind != "configured_model_response" || v.ProfileDigest != profileDigest || v.OperationKind != op.Kind || v.RequestDigest != op.RequestDigest || v.Limits.Validate() != nil || op.RequestBytes > v.Limits.RequestBytes || v.Stop != StopFinalV1 || v.Call != nil || !validString(v.Text, v.Limits.FinalTextBytes, false) || !validString(v.ReportedModel, 256, false) || v.Usage.Validate() != nil || v.Usage.AmountMicrounits != nil || (v.Usage.State != domain.AttemptOperationUsageUnknownV1 && v.Usage.State != domain.AttemptOperationUsageProviderReportedV1) || (op.Kind == domain.AttemptOperationCompactionV1 && (len(v.Text) > 16<<10 || uint64(len(v.Text)) > op.SummaryBytes)) {
		return SealedRetainedTextV1{}, ErrContract
	}
	actual, _ := json.Marshal(v.Usage)
	expected, _ := json.Marshal(o.Usage)
	canonical, err := encodeNormalizedBodyV1(v, normalizedBodyLimitV1)
	defer clear(canonical)
	if err != nil || !bytes.Equal(actual, expected) || !bytes.Equal(canonical, body) {
		return SealedRetainedTextV1{}, ErrContract
	}
	return SealedRetainedTextV1{Text: v.Text}, nil
}

// DecodeRetainedNormalizedDataV1 reads the existing normalized representation
// after the original request has been erased. It verifies bounded DATA joins,
// not the observation's issuer, original schema/history, current permission or
// successful custody. The original producer must validate the request-dependent
// codec before erasure; its successful terminal seal preserves that derivation.
// In particular a returned Call does NOT establish catalog membership, schema
// acceptance, unused call identity or permission to dispatch a tool.
//
// The caller authenticates the complete capsule/frontier and terminal seal.
// This function does no I/O and never restores a request or falls back to a
// native/tool codec. Reported overruns remain evidence, not continuation credit.
func DecodeRetainedNormalizedDataV1(profile domain.ConfiguredInferenceProfileV1, reservation domain.AttemptOperationReservationV1, observation domain.AttemptOperationObservationV1, body []byte) (ResponseV1, error) {
	op := reservation.Operation
	if len(body) == 0 || uint64(len(body)) > normalizedBodyLimitV1 || profile.Validate() != nil ||
		reservation.Validate() != nil || observation.ValidateForReservation(reservation) != nil ||
		(op.Kind != domain.AttemptOperationModelV1 && op.Kind != domain.AttemptOperationCompactionV1) ||
		profile.TenantID != op.Scope.TenantID || profile.OwnerUserID != op.Scope.OwnerUserID ||
		observation.Outcome != domain.AttemptOperationCompletedV1 || observation.Reason != domain.AttemptOperationReasonNoneV1 ||
		observation.ResponseBytes != uint64(len(body)) || observation.ResponseDigest != rawDigest(body) {
		return ResponseV1{}, ErrContract
	}
	if _, err := strictJSON(body, normalizedBodyLimitV1, 64, 4096); err != nil {
		return ResponseV1{}, ErrContract
	}
	var value normalizedBodyV1
	pd, _ := profile.Digest()
	if json.Unmarshal(body, &value) != nil || value.Version != 1 || value.Kind != "configured_model_response" ||
		value.OperationKind != op.Kind || value.ProfileDigest != pd || value.RequestDigest != op.RequestDigest ||
		!limitsWithin(value.Limits, profile.Limits) || op.RequestBytes > value.Limits.RequestBytes ||
		!validString(value.ReportedModel, 256, false) || value.Usage.Validate() != nil || value.Usage.AmountMicrounits != nil ||
		(value.Usage.State != domain.AttemptOperationUsageUnknownV1 && value.Usage.State != domain.AttemptOperationUsageProviderReportedV1) {
		return ResponseV1{}, ErrContract
	}
	allowed := false
	for _, model := range profile.ResponseModelIDs {
		allowed = allowed || model == value.ReportedModel
	}
	expectedUsage, _ := json.Marshal(observation.Usage)
	actualUsage, _ := json.Marshal(value.Usage)
	if !allowed || !bytes.Equal(expectedUsage, actualUsage) {
		return ResponseV1{}, ErrContract
	}
	// Do not compare codec token ceilings with the exact operation debit: the
	// original request can select a smaller output limit, and calibrated input
	// fit is not the configured maximum. That original proof is seal-attested.
	switch value.Stop {
	case StopFinalV1:
		if value.Call != nil || !validString(value.Text, value.Limits.FinalTextBytes, false) ||
			(op.Kind == domain.AttemptOperationCompactionV1 && (len(value.Text) > 16<<10 || uint64(len(value.Text)) > op.SummaryBytes)) {
			return ResponseV1{}, ErrContract
		}
	case StopToolV1:
		if op.Kind != domain.AttemptOperationModelV1 || profile.Capabilities.ToolCalls != domain.ProviderEvidenceSupportedV1 || value.Text != "" || value.Call == nil ||
			!validString(value.Call.ID, value.Limits.ToolCallIDBytes, false) || !validString(value.Call.Name, value.Limits.ToolNameBytes, false) {
			return ResponseV1{}, ErrContract
		}
		args, err := strictJSON(value.Call.Arguments, value.Limits.ToolArgumentsBytes, value.Limits.JSONDepth, value.Limits.JSONMembers)
		if err != nil {
			return ResponseV1{}, ErrContract
		}
		if _, object := args.(map[string]any); !object {
			return ResponseV1{}, ErrContract
		}
		canonical, err := json.Marshal(args)
		if err != nil || !bytes.Equal(canonical, value.Call.Arguments) {
			clear(canonical)
			return ResponseV1{}, ErrContract
		}
		clear(canonical)
	default:
		return ResponseV1{}, ErrContract
	}
	canonical, err := encodeNormalizedBodyV1(value, normalizedBodyLimitV1)
	if err != nil || !bytes.Equal(canonical, body) {
		clear(canonical)
		return ResponseV1{}, ErrContract
	}
	clear(canonical)
	out := ResponseV1{Stop: value.Stop, Text: value.Text, ConfiguredModel: profile.ModelID, ReportedModel: value.ReportedModel,
		ModelProvenance: ModelReportedV1, Usage: UsageV1{Provenance: UsageUnknownV1}}
	if value.Usage.State == domain.AttemptOperationUsageProviderReportedV1 {
		usage := value.Usage.Clone()
		out.Usage = UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens}
	}
	if value.Call != nil {
		out.Call = &FunctionCallV1{ID: value.Call.ID, Name: value.Call.Name, Arguments: bytes.Clone(value.Call.Arguments)}
	}
	return out, nil
}
