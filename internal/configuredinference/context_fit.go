package configuredinference

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const MaxContextCalibrationBytesV1 = 16 << 10

const contextCalibrationAlgorithmV1 = "utf8_affine.v1"
const contextCalibrationDomainV1 = "chat_completions_text.v1"

var ErrContextNoFit = errors.New("configured inference context does not fit")

// ContextFitCeilingsV1 is DATA from an original operation's remaining ceilings,
// not authority or an independent budget. Zero never means unlimited. The
// consumer still joins these values to its original reservation/current state.
type ContextFitCeilingsV1 struct {
	InputTokens, OutputTokens, TotalTokens uint64
}

// ContextFitV1 reports a conservative declaration-based calculation, never a
// measured token count, model qualification, current-source grant or send permit.
type ContextFitV1 struct {
	RequestDigest, CalibrationDigest                string
	InputUpperBound, OutputReserve, TotalUpperBound uint64
}

type contextCalibrationDocumentV1 struct {
	Version                  uint32          `json:"version"`
	Algorithm                string          `json:"algorithm"`
	InputDomain              string          `json:"input_domain"`
	TenantID                 domain.TenantID `json:"tenant_id"`
	OwnerUserID              domain.UserID   `json:"owner_user_id"`
	ModelVendorID            string          `json:"model_vendor_id"`
	ModelID                  string          `json:"model_id"`
	ResponseModelIDs         []string        `json:"response_model_ids"`
	ServerID                 string          `json:"server_id"`
	ServerVersion            string          `json:"server_version"`
	Protocol                 string          `json:"protocol"`
	ChatTemplateDigest       string          `json:"chat_template_digest"`
	EstimatorVersion         string          `json:"estimator_version"`
	CapabilityEvidenceDigest string          `json:"capability_evidence_digest"`
	ModelWindowTokens        uint64          `json:"model_window_tokens"`
	MaxWireBytes             uint64          `json:"max_wire_bytes"`
	MaxMessages              uint64          `json:"max_messages"`
	MaxTools                 uint64          `json:"max_tools"`
	ByteNumerator            uint64          `json:"byte_numerator"`
	ByteDenominator          uint64          `json:"byte_denominator"`
	FixedTokens              uint64          `json:"fixed_tokens"`
	MessageTokens            uint64          `json:"message_tokens"`
	ToolTokens               uint64          `json:"tool_tokens"`
}

// ContextCalibrationV1 owns a strictly decoded immutable declaration. This
// pure decoder authenticates neither its caller nor model behavior. The real
// Source must select/reverify the exact retained body, and the original session
// must rejoin authority and operation ceilings before using any fit result.
type ContextCalibrationV1 struct {
	document      contextCalibrationDocumentV1
	profileDigest string
	bodyDigest    string
}

func (ContextCalibrationV1) String() string               { return "ContextCalibrationV1{private:[redacted]}" }
func (v ContextCalibrationV1) GoString() string           { return v.String() }
func (v ContextCalibrationV1) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, v.String()) }
func (ContextCalibrationV1) MarshalJSON() ([]byte, error) { return nil, ErrContract }

// DecodeContextCalibrationV1 executes bounded strict DATA validation. A matching
// retained declaration is necessary but does not prove its bound universally;
// exact-profile qualification must cover normalization, tokenizer and template
// expansion over this closed request AND visible-output domain.
func DecodeContextCalibrationV1(profile domain.ConfiguredInferenceProfileV1, body []byte) (*ContextCalibrationV1, error) {
	if profile.Validate() != nil {
		return nil, ErrContract
	}
	value, err := strictJSON(body, MaxContextCalibrationBytesV1, 4, 32)
	if err != nil {
		return nil, ErrContract
	}
	fields, ok := object(value)
	keys := []string{"version", "algorithm", "input_domain", "tenant_id", "owner_user_id", "model_vendor_id", "model_id", "response_model_ids", "server_id", "server_version", "protocol", "chat_template_digest", "estimator_version", "capability_evidence_digest", "model_window_tokens", "max_wire_bytes", "max_messages", "max_tools", "byte_numerator", "byte_denominator", "fixed_tokens", "message_tokens", "tool_tokens"}
	if !ok || len(fields) != len(keys) || !exactKeys(fields, keys...) {
		return nil, ErrContract
	}
	for _, key := range []string{"version", "model_window_tokens", "max_wire_bytes", "max_messages", "max_tools", "byte_numerator", "byte_denominator", "fixed_tokens", "message_tokens", "tool_tokens"} {
		if _, ok := uintValue(fields[key]); !ok {
			return nil, ErrContract
		}
	}
	var d contextCalibrationDocumentV1
	if json.Unmarshal(body, &d) != nil || d.Version != 1 || d.Algorithm != contextCalibrationAlgorithmV1 || d.InputDomain != contextCalibrationDomainV1 ||
		d.TenantID != profile.TenantID || d.OwnerUserID != profile.OwnerUserID || d.ModelVendorID != profile.ModelVendorID || d.ModelID != profile.ModelID ||
		!reflect.DeepEqual(d.ResponseModelIDs, profile.ResponseModelIDs) || d.ServerID != profile.ServerID || d.ServerVersion != profile.ServerVersion ||
		d.Protocol != profile.NativeProtocolVersion || d.ChatTemplateDigest != profile.ChatTemplateDigest || d.EstimatorVersion != profile.TokenEstimatorVersion ||
		d.CapabilityEvidenceDigest != profile.CapabilityEvidence.Digest || d.ModelWindowTokens != profile.ModelWindowTokens ||
		d.MaxWireBytes == 0 || d.MaxWireBytes > profile.Limits.RequestBytes || d.MaxMessages == 0 || d.MaxMessages > profile.Limits.Messages || d.MaxTools > profile.Limits.Tools ||
		d.ByteDenominator == 0 || d.ByteNumerator < d.ByteDenominator || d.FixedTokens == 0 {
		return nil, ErrContract
	}
	for _, coefficient := range []uint64{d.ByteNumerator, d.ByteDenominator, d.FixedTokens, d.MessageTokens, d.ToolTokens} {
		if coefficient > 1<<20 {
			return nil, ErrContract
		}
	}
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	if digest != profile.CalibrationDigest {
		return nil, ErrContract
	}
	profileDigest, err := profile.Digest()
	if err != nil {
		return nil, ErrContract
	}
	return &ContextCalibrationV1{document: d, profileDigest: profileDigest, bodyDigest: digest}, nil
}

// FitRequest executes the fixed estimator over the exact original frozen wire,
// including JSON escaping/schema/instruction bytes. No caller count, body,
// coefficient or reported usage can replace it. A result is DATA, not a grant.
func (v *ContextCalibrationV1) FitRequest(frozen *FrozenRequestV1, ceilings ContextFitCeilingsV1) (ContextFitV1, error) {
	deny := func(err error) (ContextFitV1, error) { return ContextFitV1{}, err }
	if v == nil || v.profileDigest == "" || frozen == nil || len(frozen.body) == 0 || ceilings.InputTokens == 0 || ceilings.OutputTokens == 0 || ceilings.TotalTokens == 0 {
		return deny(ErrContract)
	}
	pd, err := frozen.profile.Digest()
	if err != nil || pd != v.profileDigest || frozen.profile.CalibrationDigest != v.bodyDigest || !limitsWithin(frozen.limits, frozen.profile.Limits) {
		return deny(ErrContract)
	}
	fd, err := requestDigest(frozen.profile, frozen.limits, frozen.body)
	if err != nil || fd != frozen.digest {
		return deny(ErrContract)
	}
	value, err := strictJSON(frozen.body, frozen.limits.RequestBytes, frozen.limits.JSONDepth, frozen.limits.JSONMembers)
	if err != nil {
		return deny(ErrContract)
	}
	fields, ok := object(value)
	if !ok {
		return deny(ErrContract)
	}
	messages, ok := fields["messages"].([]any)
	output, outputOK := uintValue(fields["max_tokens"])
	if !ok || len(messages) == 0 || !outputOK || output == 0 {
		return deny(ErrContract)
	}
	var tools []any
	if raw, exists := fields["tools"]; exists {
		tools, ok = raw.([]any)
		if !ok {
			return deny(ErrContract)
		}
	}
	d := v.document
	if uint64(len(frozen.body)) > d.MaxWireBytes || uint64(len(messages)) > d.MaxMessages || uint64(len(tools)) > d.MaxTools {
		return deny(ErrContextNoFit)
	}
	bound, ok := v.bound(uint64(len(frozen.body)), uint64(len(messages)), uint64(len(tools)))
	if !ok || output > math.MaxUint64-bound {
		return deny(ErrContextNoFit)
	}
	total := bound + output
	if bound > min(ceilings.InputTokens, frozen.limits.MaxInputTokens) || output > min(ceilings.OutputTokens, frozen.limits.MaxOutputTokens) || total > min(ceilings.TotalTokens, d.ModelWindowTokens) {
		return deny(ErrContextNoFit)
	}
	return ContextFitV1{RequestDigest: frozen.digest, CalibrationDigest: v.bodyDigest, InputUpperBound: bound, OutputReserve: output, TotalUpperBound: total}, nil
}

// BoundSummaryText applies the independently qualified visible-output bound.
// Neither provider max_tokens nor reported usage replaces this local check.
func (v *ContextCalibrationV1) BoundSummaryText(text string) (uint64, error) {
	if v == nil || v.profileDigest == "" || len(text) == 0 || len(text) > 16<<10 || !utf8.ValidString(text) {
		return 0, ErrContract
	}
	bound, ok := v.bound(uint64(len(text)), 0, 0)
	if !ok || bound > 2048 {
		return 0, ErrContextNoFit
	}
	return bound, nil
}

func (v *ContextCalibrationV1) bound(bytes, messages, tools uint64) (uint64, bool) {
	d := v.document
	if d.ByteDenominator == 0 || d.ByteNumerator == 0 || bytes > math.MaxUint64/d.ByteNumerator {
		return 0, false
	}
	product := bytes * d.ByteNumerator
	result := product / d.ByteDenominator
	if product%d.ByteDenominator != 0 {
		result++ // remainder implies quotient < MaxUint64
	}
	for _, term := range [][2]uint64{{1, d.FixedTokens}, {messages, d.MessageTokens}, {tools, d.ToolTokens}} {
		if term[0] != 0 && term[1] > math.MaxUint64/term[0] {
			return 0, false
		}
		amount := term[0] * term[1]
		if amount > math.MaxUint64-result {
			return 0, false
		}
		result += amount
	}
	return result, true
}
