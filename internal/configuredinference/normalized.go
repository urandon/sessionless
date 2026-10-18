package configuredinference

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// A normalized body is bounded DATA for the existing operation store, not a
// transport receipt, tool grant, measured bill or permission to continue. Its
// effective limits are explicit: they cannot be recovered from the wire alone.
const normalizedBodyLimitV1 uint64 = 64 << 10

type normalizedCallV1 struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type normalizedBodyV1 struct {
	Version       uint32                             `json:"version"`
	Kind          string                             `json:"kind"`
	OperationKind domain.AttemptOperationKindV1      `json:"operation_kind"`
	ProfileDigest string                             `json:"profile_digest"`
	RequestDigest string                             `json:"request_digest"`
	Limits        domain.ConfiguredInferenceLimitsV1 `json:"limits"`
	Stop          StopV1                             `json:"stop"`
	Text          string                             `json:"text"`
	Call          *normalizedCallV1                  `json:"call"`
	ReportedModel string                             `json:"reported_model"`
	Usage         domain.AttemptOperationUsageV1     `json:"usage"`
}

func rawDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// NormalizeResponseBodyV1 starts with the original frozen decoder, never a
// caller-built ResponseV1. The cap is the already reserved normalized-body
// slot, distinct from the HTTP response ceiling; envelope overhead counts too.
// Non-success stops remain on the existing error/unknown observation path.
func (f *FrozenRequestV1) NormalizeResponseBodyV1(raw []byte, kind domain.AttemptOperationKindV1, cap uint64) ([]byte, error) {
	if cap == 0 || cap > normalizedBodyLimitV1 || (kind != domain.AttemptOperationModelV1 && kind != domain.AttemptOperationCompactionV1) {
		return nil, ErrContract
	}
	response, err := f.DecodeResponse(raw)
	if err != nil || (response.Stop != StopFinalV1 && response.Stop != StopToolV1) || (kind == domain.AttemptOperationCompactionV1 && (response.Stop != StopFinalV1 || len(response.Text) > 16<<10)) {
		return nil, ErrContract
	}
	usage, err := response.Usage.OperationUsage()
	if err != nil {
		return nil, ErrContract
	}
	profileDigest, err := f.profile.Digest()
	if err != nil {
		return nil, ErrContract
	}
	value := normalizedBodyV1{Version: 1, Kind: "configured_model_response", OperationKind: kind,
		ProfileDigest: profileDigest, RequestDigest: rawDigest(f.body), Limits: f.limits,
		Stop: response.Stop, Text: response.Text, ReportedModel: response.ReportedModel, Usage: usage}
	if response.Call != nil {
		defer clear(response.Call.Arguments)
		args, err := strictJSON(response.Call.Arguments, f.limits.ToolArgumentsBytes, f.limits.JSONDepth, f.limits.JSONMembers)
		if err != nil {
			return nil, ErrContract
		}
		canonical, err := json.Marshal(args)
		if err != nil {
			return nil, ErrContract
		}
		value.Call = &normalizedCallV1{ID: response.Call.ID, Name: response.Call.Name, Arguments: canonical}
		defer clear(canonical)
	}
	return encodeNormalizedBodyV1(value, cap)
}

// DecodeNormalizedResponseBodyV1 revalidates persisted DATA using the original
// verified request and fixed trusted profile. It performs no I/O/current check
// and does not prove those bytes came from a real provider. The authenticated
// operation observation and original transport/session own that evidence.
func DecodeNormalizedResponseBodyV1(profile domain.ConfiguredInferenceProfileV1, request, body []byte, kind domain.AttemptOperationKindV1) (ResponseV1, error) {
	if profile.Validate() != nil || len(request) == 0 || uint64(len(request)) > profile.Limits.RequestBytes || len(body) == 0 || uint64(len(body)) > normalizedBodyLimitV1 {
		return ResponseV1{}, ErrContract
	}
	// The bounded record wrapper adds nesting to the original argument object.
	// Its lexical pass still rejects duplicates/invalid UTF8/surrogates/numbers;
	// the restored frozen validator applies the original tighter argument limits.
	if _, err := strictJSON(body, normalizedBodyLimitV1, 64, 4096); err != nil {
		return ResponseV1{}, ErrContract
	}
	var value normalizedBodyV1
	pd, _ := profile.Digest()
	if json.Unmarshal(body, &value) != nil || value.Version != 1 || value.Kind != "configured_model_response" || value.OperationKind != kind ||
		(kind != domain.AttemptOperationModelV1 && kind != domain.AttemptOperationCompactionV1) || value.ProfileDigest != pd || value.RequestDigest != rawDigest(request) || !limitsWithin(value.Limits, profile.Limits) {
		return ResponseV1{}, ErrContract
	}
	frozen, err := restoreRequestV1(profile, value.Limits, request)
	if err != nil || !validString(value.ReportedModel, 256, false) || value.Usage.Validate() != nil || value.Usage.AmountMicrounits != nil ||
		(value.Usage.State != domain.AttemptOperationUsageUnknownV1 && value.Usage.State != domain.AttemptOperationUsageProviderReportedV1) {
		return ResponseV1{}, ErrContract
	}
	allowedModel := false
	for _, model := range profile.ResponseModelIDs {
		allowedModel = allowedModel || model == value.ReportedModel
	}
	if !allowedModel {
		return ResponseV1{}, ErrContract
	}
	out := ResponseV1{Stop: value.Stop, Text: value.Text, ConfiguredModel: profile.ModelID, ReportedModel: value.ReportedModel,
		ModelProvenance: ModelReportedV1, Usage: UsageV1{Provenance: UsageUnknownV1}}
	if value.Usage.State == domain.AttemptOperationUsageProviderReportedV1 {
		usage := value.Usage.Clone()
		out.Usage = UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens}
	}
	switch value.Stop {
	case StopFinalV1:
		if value.Call != nil || !validString(value.Text, value.Limits.FinalTextBytes, false) ||
			(kind == domain.AttemptOperationCompactionV1 && len(value.Text) > 16<<10) {
			return ResponseV1{}, ErrContract
		}
	case StopToolV1:
		if kind != domain.AttemptOperationModelV1 || value.Text != "" || value.Call == nil {
			return ResponseV1{}, ErrContract
		}
		call := FunctionCallV1{ID: value.Call.ID, Name: value.Call.Name, Arguments: value.Call.Arguments}
		args, err := strictJSON(call.Arguments, value.Limits.ToolArgumentsBytes, value.Limits.JSONDepth, value.Limits.JSONMembers)
		_, repeated := frozen.priorIDs[call.ID]
		if err != nil || repeated || !frozen.validCall(call) {
			return ResponseV1{}, ErrContract
		}
		canonical, err := json.Marshal(args)
		if err != nil || !bytes.Equal(canonical, call.Arguments) {
			return ResponseV1{}, ErrContract
		}
		call.Arguments = bytes.Clone(call.Arguments)
		out.Call = &call
	default:
		return ResponseV1{}, ErrContract
	}
	canonical, err := encodeNormalizedBodyV1(value, normalizedBodyLimitV1)
	if err != nil || !bytes.Equal(canonical, body) {
		return ResponseV1{}, ErrContract
	}
	return out, nil
}

// Use the same bounded string builder as the wire codec: an escaped value
// exceeding the remaining slot is rejected before its encoded allocation.
func encodeNormalizedBodyV1(v normalizedBodyV1, cap uint64) ([]byte, error) {
	b := bodyBuilder{limit: cap}
	b.add(`{"version":1,"kind":"configured_model_response","operation_kind":`)
	b.text(string(v.OperationKind))
	b.add(`,"profile_digest":`)
	b.text(v.ProfileDigest)
	b.add(`,"request_digest":`)
	b.text(v.RequestDigest)
	b.add(`,"limits":`)
	limits, err := json.Marshal(v.Limits)
	if err != nil {
		return nil, ErrContract
	}
	b.raw(limits)
	b.add(`,"stop":`)
	b.text(string(v.Stop))
	b.add(`,"text":`)
	b.text(v.Text)
	b.add(`,"call":`)
	if v.Call == nil {
		b.add("null")
	} else {
		b.add(`{"id":`)
		b.text(v.Call.ID)
		b.add(`,"name":`)
		b.text(v.Call.Name)
		b.add(`,"arguments":`)
		b.raw(v.Call.Arguments)
		b.add("}")
	}
	b.add(`,"reported_model":`)
	b.text(v.ReportedModel)
	b.add(`,"usage":`)
	usage, err := json.Marshal(v.Usage)
	if err != nil {
		clear(b.b)
		return nil, ErrContract
	}
	b.raw(usage)
	b.add("}")
	if b.failed {
		clear(b.b)
		return nil, ErrContract
	}
	return b.b, nil
}

// Restore only this codec's exact wire shape, including the ORIGINAL schema
// bytes. Re-marshalling a generic schema would discard accepted whitespace/key
// ordering and reject genuine frozen requests. No permissive decoder fallback.
func restoreRequestV1(profile domain.ConfiguredInferenceProfileV1, limits domain.ConfiguredInferenceLimitsV1, body []byte) (*FrozenRequestV1, error) {
	if !limitsWithin(limits, profile.Limits) {
		return nil, ErrContract
	}
	if _, err := strictJSON(body, limits.RequestBytes, limits.JSONDepth, limits.JSONMembers); err != nil {
		return nil, ErrContract
	}
	type wireFunction struct {
		Name, Description string
		Arguments         string
	}
	type wireCall struct {
		ID, Type string
		Function wireFunction
	}
	var wire struct {
		Model     string
		Stream    bool
		N         uint64
		MaxTokens uint64 `json:"max_tokens"`
		Messages  []struct {
			Role       RoleV1
			Content    *string
			ToolCalls  []wireCall `json:"tool_calls"`
			ToolCallID string     `json:"tool_call_id"`
		}
		Tools []json.RawMessage
	}
	if json.Unmarshal(body, &wire) != nil || wire.Model != profile.ModelID || wire.Stream || wire.N != 1 || uint64(len(wire.Messages)) > limits.Messages || uint64(len(wire.Tools)) > limits.Tools {
		return nil, ErrContract
	}
	request := RequestV1{MaxOutputTokens: wire.MaxTokens}
	for _, m := range wire.Messages {
		v := MessageV1{Role: m.Role, ToolCallID: m.ToolCallID}
		if len(m.ToolCalls) != 0 {
			if m.Content != nil || len(m.ToolCalls) != 1 || m.ToolCalls[0].Type != "function" {
				return nil, ErrContract
			}
			c := m.ToolCalls[0]
			v.Call = &FunctionCallV1{ID: c.ID, Name: c.Function.Name, Arguments: []byte(c.Function.Arguments)}
		} else {
			if m.Content == nil {
				return nil, ErrContract
			}
			v.Text = *m.Content
		}
		request.Messages = append(request.Messages, v)
	}
	for _, raw := range wire.Tools {
		var tool struct {
			Type     string
			Function wireFunction
		}
		if json.Unmarshal(raw, &tool) != nil || tool.Type != "function" || !validString(tool.Function.Name, limits.ToolNameBytes, false) || !validString(tool.Function.Description, limits.RequestBytes, true) {
			return nil, ErrContract
		}
		name, _ := json.Marshal(tool.Function.Name)
		description, _ := json.Marshal(tool.Function.Description)
		prefix := append([]byte(`{"type":"function","function":{"name":`), name...)
		prefix = append(prefix, []byte(`,"description":`)...)
		prefix = append(prefix, description...)
		prefix = append(prefix, []byte(`,"parameters":`)...)
		if !bytes.HasPrefix(raw, prefix) || !bytes.HasSuffix(raw, []byte("}}")) || len(raw) <= len(prefix)+2 {
			return nil, ErrContract
		}
		request.Tools = append(request.Tools, ToolV1{Name: tool.Function.Name, Description: tool.Function.Description,
			Parameters: bytes.Clone(raw[len(prefix) : len(raw)-2])})
	}
	frozen, err := FreezeRequestV1(profile, limits, request)
	if err != nil || !bytes.Equal(frozen.body, body) {
		return nil, ErrContract
	}
	return frozen, nil
}
