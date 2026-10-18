// Package configuredinference contains pure, nonstream Chat Completions DATA
// codecs. Valid bytes, a profile digest, and a frozen request grant no authority
// to send, dispatch tools, publish events, or claim model qualification. The
// real #190/#191 composer must prove calibrated context fit, current authority,
// and all remaining operation budgets before any transport effect.
package configuredinference

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"

	"gitcode.com/urandon/sessionless/internal/domain"
)

var ErrContract = errors.New("configured inference codec contract is invalid")

type RoleV1 string

const (
	RoleSystemV1    RoleV1 = "system"
	RoleUserV1      RoleV1 = "user"
	RoleAssistantV1 RoleV1 = "assistant"
	RoleToolV1      RoleV1 = "tool"
)

type FunctionCallV1 struct {
	ID        string `json:"-"`
	Name      string `json:"-"`
	Arguments []byte `json:"-"`
}

type MessageV1 struct {
	Role       RoleV1
	Text       string          `json:"-"`
	Call       *FunctionCallV1 `json:"-"`
	ToolCallID string          `json:"-"`
}

type ToolV1 struct {
	Name        string `json:"-"`
	Description string `json:"-"`
	Parameters  []byte `json:"-"`
}

type RequestV1 struct {
	Messages        []MessageV1 `json:"-"`
	Tools           []ToolV1    `json:"-"`
	MaxOutputTokens uint64
}

type StopV1 string

const (
	StopFinalV1             StopV1 = "final"
	StopToolV1              StopV1 = "tool_call"
	StopLengthV1            StopV1 = "length"
	StopFilteredV1          StopV1 = "content_filter"
	StopRefusalV1           StopV1 = "refusal"
	StopServiceErrorV1      StopV1 = "service_error"
	StopUnsupportedFinishV1 StopV1 = "unsupported_finish"
	StopUnverifiedModelV1   StopV1 = "unverified_model"
)

type ModelProvenanceV1 string

const (
	ModelReportedV1   ModelProvenanceV1 = "provider_reported"
	ModelUnverifiedV1 ModelProvenanceV1 = "unverified"
)

type UsageProvenanceV1 string

const (
	UsageUnknownV1          UsageProvenanceV1 = "unknown"
	UsageProviderReportedV1 UsageProvenanceV1 = "provider_reported"
)

// UsageV1 is an unverified provider report, NOT a measured observation,
// independent token count, refund or another budget. OperationUsage preserves
// its explicit provenance in the durable domain record without upgrading it.
type UsageV1 struct {
	Provenance   UsageProvenanceV1
	InputTokens  *uint64
	OutputTokens *uint64
	TotalTokens  *uint64
}

type ResponseV1 struct {
	Stop            StopV1
	Text            string          `json:"-"`
	Call            *FunctionCallV1 `json:"-"`
	ConfiguredModel string          `json:"-"`
	ReportedModel   string          `json:"-"`
	ModelProvenance ModelProvenanceV1
	Usage           UsageV1
}

// FrozenRequestV1 owns bounded copies of wire bytes and the exact tool schemas
// and history identities used to decode its response. It is not a capability.
type FrozenRequestV1 struct {
	profile  domain.ConfiguredInferenceProfileV1
	limits   domain.ConfiguredInferenceLimitsV1
	body     []byte
	digest   string
	tools    map[string]*schemaV1
	priorIDs map[string]struct{}
}

func (v *FrozenRequestV1) Body() []byte {
	if v == nil {
		return nil
	}
	return append([]byte(nil), v.body...)
}
func (v *FrozenRequestV1) Digest() string {
	if v == nil {
		return ""
	}
	return v.digest
}

func (FunctionCallV1) String() string       { return "ConfiguredFunctionCallV1{content:[redacted]}" }
func (v FunctionCallV1) GoString() string   { return v.String() }
func (MessageV1) String() string            { return "ConfiguredMessageV1{content:[redacted]}" }
func (v MessageV1) GoString() string        { return v.String() }
func (ToolV1) String() string               { return "ConfiguredToolV1{content:[redacted]}" }
func (v ToolV1) GoString() string           { return v.String() }
func (RequestV1) String() string            { return "ConfiguredRequestV1{content:[redacted]}" }
func (v RequestV1) GoString() string        { return v.String() }
func (ResponseV1) String() string           { return "ConfiguredResponseV1{content:[redacted]}" }
func (v ResponseV1) GoString() string       { return v.String() }
func (*FrozenRequestV1) String() string     { return "ConfiguredFrozenRequestV1{content:[redacted]}" }
func (v *FrozenRequestV1) GoString() string { return v.String() }

func limitsWithin(v, p domain.ConfiguredInferenceLimitsV1) bool {
	if v.Validate() != nil {
		return false
	}
	for _, pair := range [][2]uint64{
		{v.RequestBytes, p.RequestBytes}, {v.ResponseBytes, p.ResponseBytes}, {v.ResponseHeaderBytes, p.ResponseHeaderBytes},
		{v.JSONDepth, p.JSONDepth}, {v.JSONMembers, p.JSONMembers}, {v.Choices, p.Choices}, {v.ToolCalls, p.ToolCalls},
		{v.ToolNameBytes, p.ToolNameBytes}, {v.ToolCallIDBytes, p.ToolCallIDBytes}, {v.ToolArgumentsBytes, p.ToolArgumentsBytes},
		{v.FinalTextBytes, p.FinalTextBytes}, {v.Messages, p.Messages}, {v.Tools, p.Tools},
		{v.MaxInputTokens, p.MaxInputTokens}, {v.MaxOutputTokens, p.MaxOutputTokens},
	} {
		if pair[0] > pair[1] {
			return false
		}
	}
	return v.OperationTimeout <= p.OperationTimeout && v.CleanupTimeout <= p.CleanupTimeout
}

func frame(h hash.Hash, b []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(b)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(b)
}
func requestDigest(profile domain.ConfiguredInferenceProfileV1, limits domain.ConfiguredInferenceLimitsV1, body []byte) (string, error) {
	p, err := profile.Digest()
	if err != nil {
		return "", ErrContract
	}
	l, err := json.Marshal(limits)
	if err != nil {
		return "", ErrContract
	}
	h := sha256.New()
	frame(h, []byte("sessionless.configured-inference-request.v1"))
	frame(h, []byte(p))
	frame(h, l)
	frame(h, body)
	return hex.EncodeToString(h.Sum(nil)), nil
}
