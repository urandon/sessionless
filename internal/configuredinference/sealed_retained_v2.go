package configuredinference

import (
	"bytes"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// SealedRetainedResponseV2 is request-free historical DATA. A tool call here
// cannot establish catalog membership, schema qualification, current funding or
// permission to execute. The caller must authenticate the original V2 capsule
// and complete terminal seal. The strict text-only V1 decoder is unchanged.
type SealedRetainedResponseV2 struct {
	Stop StopV1
	Text string
	Call *FunctionCallV1
}

func (SealedRetainedResponseV2) String() string {
	return "SealedRetainedResponseV2{historical:[redacted]}"
}
func (v SealedRetainedResponseV2) GoString() string { return v.String() }

func DecodeSealedRetainedResponseV2(profileDigest string, r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, body []byte) (SealedRetainedResponseV2, error) {
	deny := func() (SealedRetainedResponseV2, error) { return SealedRetainedResponseV2{}, ErrContract }
	op := r.Operation
	if domain.ValidateSHA256Digest("retained.profile", profileDigest) != nil || r.Validate() != nil || o.ValidateForReservation(r) != nil ||
		o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(op.Debit) ||
		(op.Kind != domain.AttemptOperationModelV1 && op.Kind != domain.AttemptOperationCompactionV1) || len(body) == 0 || uint64(len(body)) > normalizedBodyLimitV1 || o.ResponseBytes != uint64(len(body)) || o.ResponseDigest != rawDigest(body) {
		return deny()
	}
	if _, err := strictJSON(body, normalizedBodyLimitV1, 64, 4096); err != nil {
		return deny()
	}
	var v normalizedBodyV1
	if json.Unmarshal(body, &v) != nil || v.Version != 1 || v.Kind != "configured_model_response" || v.ProfileDigest != profileDigest ||
		v.OperationKind != op.Kind || v.RequestDigest != op.RequestDigest || v.Limits.Validate() != nil || op.RequestBytes > v.Limits.RequestBytes ||
		!validString(v.ReportedModel, 256, false) || v.Usage.Validate() != nil || v.Usage.AmountMicrounits != nil ||
		(v.Usage.State != domain.AttemptOperationUsageUnknownV1 && v.Usage.State != domain.AttemptOperationUsageProviderReportedV1) {
		return deny()
	}
	switch v.Stop {
	case StopFinalV1:
		if v.Call != nil || !validString(v.Text, v.Limits.FinalTextBytes, false) || op.Kind == domain.AttemptOperationCompactionV1 && (len(v.Text) > 16<<10 || uint64(len(v.Text)) > op.SummaryBytes) {
			return deny()
		}
	case StopToolV1:
		if op.Kind != domain.AttemptOperationModelV1 || v.Text != "" || v.Call == nil || !validString(v.Call.ID, v.Limits.ToolCallIDBytes, false) || !validString(v.Call.Name, v.Limits.ToolNameBytes, false) {
			return deny()
		}
		args, err := strictJSON(v.Call.Arguments, v.Limits.ToolArgumentsBytes, v.Limits.JSONDepth, v.Limits.JSONMembers)
		if err != nil {
			return deny()
		}
		if _, object := args.(map[string]any); !object {
			return deny()
		}
		canonical, err := json.Marshal(args)
		defer clear(canonical)
		if err != nil || !bytes.Equal(canonical, v.Call.Arguments) {
			return deny()
		}
	default:
		return deny()
	}
	actual, _ := json.Marshal(v.Usage)
	expected, _ := json.Marshal(o.Usage)
	canonical, err := encodeNormalizedBodyV1(v, normalizedBodyLimitV1)
	defer clear(canonical)
	if err != nil || !bytes.Equal(actual, expected) || !bytes.Equal(canonical, body) {
		return deny()
	}
	out := SealedRetainedResponseV2{Stop: v.Stop, Text: v.Text}
	if v.Call != nil {
		out.Call = &FunctionCallV1{ID: v.Call.ID, Name: v.Call.Name, Arguments: bytes.Clone(v.Call.Arguments)}
	}
	return out, nil
}
