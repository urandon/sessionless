package mcpadmission

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

// CallV1 is immutable correlated DATA, never a prepared operation or permission.
type CallV1 struct {
	grant           MCPToolGrantV1
	id, name, model string
	args            []byte
}

func (CallV1) String() string            { return "MCPCallV1{content:[redacted]}" }
func (c CallV1) GoString() string        { return c.String() }
func (c CallV1) ID() string              { return c.id }
func (c CallV1) Name() string            { return c.name }
func (c CallV1) Arguments() []byte       { return bytes.Clone(c.args) }
func (c CallV1) ArgumentsDigest() string { return normalizedDigest(c.args) }
func (c CallV1) GrantDigest() string     { d, _ := c.grant.Digest(); return d }
func (c CallV1) DescriptorDigest() string {
	d, e := mcpwire.ParseDescriptorV1(c.grant.Descriptor, c.grant.Limits)
	if e != nil {
		return ""
	}
	return d.Digest()
}
func (c CallV1) ModelResponseDigest() string { return c.model }
func (c CallV1) Request() (mcpwire.RequestV1, error) {
	d, e := mcpwire.ParseDescriptorV1(c.grant.Descriptor, c.grant.Limits)
	if e != nil || c.name != d.Name() || domain.ValidateSHA256Digest("mcp.model", c.model) != nil {
		return mcpwire.RequestV1{}, ErrGrant
	}
	r, e := mcpwire.NewToolCallRequestV1(c.id, d, c.args, c.grant.Limits)
	if e != nil || len(r.Body()) > 4096 {
		return mcpwire.RequestV1{}, ErrGrant
	}
	return r, nil
}
func normalizedDigest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// BindConfiguredCallV1 reuses the configured codec's original request/schema
// validator, then checks the exact installed descriptor through that SAME
// validator. The synthetic pair below is validation DATA, not a dispatch path.
func BindConfiguredCallV1(p domain.ConfiguredInferenceProfileV1, g MCPToolGrantV1, r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, request, response []byte) (CallV1, error) {
	if g.Validate() != nil || r.Validate() != nil || o.ValidateForReservation(r) != nil || r.Operation.Kind != domain.AttemptOperationModelV1 || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(r.Operation.Debit) || p.TenantID != r.Operation.Scope.TenantID || p.OwnerUserID != r.Operation.Scope.OwnerUserID || g.TenantID != r.Operation.Scope.TenantID || g.OwnerUserID != r.Operation.Scope.OwnerUserID || r.ReservedAt.Before(g.IssuedAt) || !r.ReservedAt.Before(g.ExpiresAt) || r.Operation.Deadline.After(g.ExpiresAt) || r.Operation.RequestDigest != normalizedDigest(request) || r.Operation.RequestBytes != uint64(len(request)) || o.ResponseDigest != normalizedDigest(response) || o.ResponseBytes != uint64(len(response)) {
		return CallV1{}, ErrGrant
	}
	v, e := configuredinference.DecodeNormalizedResponseBodyV1(p, request, response, r.Operation.Kind)
	u, ue := v.Usage.OperationUsage()
	if e != nil || ue != nil || !reflect.DeepEqual(u, o.Usage) || v.Stop != configuredinference.StopToolV1 || v.Call == nil {
		return CallV1{}, ErrGrant
	}
	d, e := mcpwire.ParseDescriptorV1(g.Descriptor, g.Limits)
	if e != nil || v.Call.Name != d.Name() {
		return CallV1{}, ErrGrant
	}
	var original struct {
		Tools []struct {
			Function struct {
				Name        string          `json:"name"`
				Description string          `json:"description"`
				Parameters  json.RawMessage `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
	}
	if json.Unmarshal(request, &original) != nil {
		return CallV1{}, ErrGrant
	}
	exact := false
	for _, tool := range original.Tools {
		if tool.Function.Name == d.Name() {
			exact = tool.Function.Description == g.ModelDescription && bytes.Equal(tool.Function.Parameters, d.InputSchema())
		}
	}
	if !exact {
		return CallV1{}, ErrGrant
	}
	_, e = configuredinference.FreezeRequestV1(p, p.Limits, configuredinference.RequestV1{MaxOutputTokens: 1, Tools: []configuredinference.ToolV1{{Name: d.Name(), Description: g.ModelDescription, Parameters: d.InputSchema()}}, Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleAssistantV1, Call: v.Call}, {Role: configuredinference.RoleToolV1, ToolCallID: v.Call.ID, Text: "validated DATA"}}})
	if e != nil {
		return CallV1{}, ErrGrant
	}
	// Own the full installed body; caller mutation cannot rebind later request.
	gb, _ := json.Marshal(g)
	owned, e := ParseMCPToolGrantV1(gb)
	if e != nil {
		return CallV1{}, ErrGrant
	}
	c := CallV1{grant: owned, id: v.Call.ID, name: v.Call.Name, args: bytes.Clone(v.Call.Arguments), model: o.ResponseDigest}
	if _, e = c.Request(); e != nil {
		return CallV1{}, ErrGrant
	}
	return c, nil
}

type normalizedToolV1 struct {
	Schema              string                         `json:"schema"`
	Grant               MCPToolGrantV1                 `json:"grant"`
	CallID              string                         `json:"call_id"`
	Arguments           json.RawMessage                `json:"arguments"`
	ModelResponseDigest string                         `json:"model_response_digest"`
	RequestDigest       string                         `json:"request_digest"`
	OperationDigest     string                         `json:"operation_digest"`
	Result              json.RawMessage                `json:"result"`
	Usage               domain.AttemptOperationUsageV1 `json:"usage"`
}

// ResultV1 is known text-only normalized DATA. Usage is preserved, not inferred
// from the grant's price declaration or a successful transport response.
type ResultV1 struct {
	call    CallV1
	request string
	result  mcpwire.ToolResultV1
	usage   domain.AttemptOperationUsageV1
}

func (ResultV1) String() string     { return "MCPResultV1{content:[redacted]}" }
func (r ResultV1) GoString() string { return r.String() }
func (r ResultV1) IsError() bool    { return r.result.IsError }
func (r ResultV1) Content() []string {
	v := make([]string, len(r.result.Content))
	for i, c := range r.result.Content {
		v[i] = c.Text
	}
	return v
}
func (r ResultV1) Usage() domain.AttemptOperationUsageV1 { return r.usage.Clone() }
func (r ResultV1) CallID() string                        { return r.call.ID() }
func (r ResultV1) Name() string                          { return r.call.Name() }
func (r ResultV1) ArgumentsDigest() string               { return r.call.ArgumentsDigest() }
func (r ResultV1) GrantDigest() string                   { return r.call.GrantDigest() }
func (r ResultV1) DescriptorDigest() string              { return r.call.DescriptorDigest() }
func (r ResultV1) ModelResponseDigest() string           { return r.call.ModelResponseDigest() }
func (r ResultV1) RequestDigest() string                 { return r.request }

// ValidateConfiguredCallV1 repeats the full original MODEL/schema join for a
// current source. Retained request-free DATA cannot perform this check again.
func (r ResultV1) ValidateConfiguredCallV1(p domain.ConfiguredInferenceProfileV1, modelR domain.AttemptOperationReservationV1, modelO domain.AttemptOperationObservationV1, request, response []byte) error {
	c, e := BindConfiguredCallV1(p, r.call.grant, modelR, modelO, request, response)
	if e != nil || c.ID() != r.CallID() || c.Name() != r.Name() || c.ArgumentsDigest() != r.ArgumentsDigest() || c.GrantDigest() != r.GrantDigest() || c.ModelResponseDigest() != r.ModelResponseDigest() {
		return ErrGrant
	}
	return nil
}

func callMatchesOperation(c CallV1, op domain.AttemptOperationV1, request []byte) bool {
	return op.Validate() == nil && op.Kind == domain.AttemptOperationMCPToolV1 && !op.Deadline.Before(c.grant.IssuedAt) && !op.Deadline.After(c.grant.ExpiresAt) && op.Scope.TenantID == c.grant.TenantID && op.Scope.OwnerUserID == c.grant.OwnerUserID && op.CallID == c.ID() && op.ModelResponseDigest == c.ModelResponseDigest() && op.ArgumentsDigest == c.ArgumentsDigest() && op.ToolGrantDigest == c.GrantDigest() && op.RequestDigest == normalizedDigest(request) && op.RequestBytes == uint64(len(request))
}
func EncodeToolResultV1(c CallV1, op domain.AttemptOperationV1, result mcpwire.ToolResultV1, usage domain.AttemptOperationUsageV1) ([]byte, error) {
	r, e := c.Request()
	if e != nil || !callMatchesOperation(c, op, r.Body()) || usage.Validate() != nil || usage.State == domain.AttemptOperationUsageProviderReportedV1 {
		return nil, ErrGrant
	}
	checked, e := r.ValidateToolResultV1(result)
	if e != nil {
		return nil, ErrGrant
	}
	od, e := op.Digest()
	if e != nil {
		return nil, ErrGrant
	}
	v := normalizedToolV1{Schema: "sessionless.mcp-tool-result.v1", Grant: c.grant, CallID: c.id, Arguments: bytes.Clone(c.args), ModelResponseDigest: c.model, RequestDigest: op.RequestDigest, OperationDigest: string(od), Result: checked.Body(), Usage: usage.Clone()}
	b, e := json.Marshal(v)
	if e != nil || uint64(len(b)) > min(op.ResultBytes, uint64(64<<10)) {
		return nil, ErrGrant
	}
	if _, e = strictjson.Parse(b, 64<<10, 64, 4096); e != nil {
		return nil, ErrGrant
	}
	return b, nil
}
func decodeToolData(op domain.AttemptOperationV1, body []byte) (ResultV1, error) {
	if op.Validate() != nil || op.Kind != domain.AttemptOperationMCPToolV1 || uint64(len(body)) > min(op.ResultBytes, uint64(64<<10)) {
		return ResultV1{}, ErrGrant
	}
	if _, e := strictjson.Parse(body, 64<<10, 64, 4096); e != nil {
		return ResultV1{}, ErrGrant
	}
	var v normalizedToolV1
	if json.Unmarshal(body, &v) != nil {
		return ResultV1{}, ErrGrant
	}
	canonical, e := json.Marshal(v)
	od, oe := op.Digest()
	if e != nil || oe != nil || !bytes.Equal(canonical, body) || v.Schema != "sessionless.mcp-tool-result.v1" || v.Grant.Validate() != nil || v.OperationDigest != string(od) || v.Usage.Validate() != nil || v.Usage.State == domain.AttemptOperationUsageProviderReportedV1 {
		return ResultV1{}, ErrGrant
	}
	d, e := mcpwire.ParseDescriptorV1(v.Grant.Descriptor, v.Grant.Limits)
	if e != nil {
		return ResultV1{}, ErrGrant
	}
	c := CallV1{grant: v.Grant, id: v.CallID, name: d.Name(), args: bytes.Clone(v.Arguments), model: v.ModelResponseDigest}
	r, e := c.Request()
	if e != nil || !callMatchesOperation(c, op, r.Body()) || v.RequestDigest != op.RequestDigest {
		return ResultV1{}, ErrGrant
	}
	id, _ := json.Marshal(v.CallID)
	wire := append([]byte(`{"jsonrpc":"2.0","id":`), id...)
	wire = append(wire, `,"result":`...)
	wire = append(wire, v.Result...)
	wire = append(wire, '}')
	result, e := r.DecodeToolResultV1(wire)
	if e != nil || !bytes.Equal(result.Body(), v.Result) {
		return ResultV1{}, ErrGrant
	}
	return ResultV1{call: c, request: v.RequestDigest, result: result, usage: v.Usage.Clone()}, nil
}
func DecodeToolResultV1(g MCPToolGrantV1, op domain.AttemptOperationV1, request, body []byte) (ResultV1, error) {
	v, e := decodeToolData(op, body)
	gd, ge := g.Digest()
	if e != nil || ge != nil || gd != v.GrantDigest() || normalizedDigest(request) != op.RequestDigest {
		return ResultV1{}, ErrGrant
	}
	r, re := v.call.Request()
	if re != nil || !bytes.Equal(request, r.Body()) {
		return ResultV1{}, ErrGrant
	}
	return v, nil
}

// DecodeSealedToolResultDataV1 checks request-free historical DATA. It does not
// restore a current grant, schema qualification, receipt issuer or dispatch.
func DecodeSealedToolResultDataV1(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, body []byte) (ResultV1, error) {
	if r.Validate() != nil || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.ResponseBytes != uint64(len(body)) || o.ResponseDigest != normalizedDigest(body) || o.Usage.ExceedsReservation(r.Operation.Debit) {
		return ResultV1{}, ErrGrant
	}
	v, e := decodeToolData(r.Operation, body)
	if e != nil || r.ReservedAt.Before(v.call.grant.IssuedAt) || !r.ReservedAt.Before(v.call.grant.ExpiresAt) || !reflect.DeepEqual(v.Usage(), o.Usage) || (v.IsError() && o.Reason != domain.AttemptOperationReasonServiceErrorV1) || (!v.IsError() && o.Reason != domain.AttemptOperationReasonNoneV1) {
		return ResultV1{}, ErrGrant
	}
	return v, nil
}
