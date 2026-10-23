package compactioncontext

import (
	"encoding/json"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
)

// Tool frames are untrusted historical DATA under the existing instruction
// bundle, not executable native calls or a reconstructed permission.
func toolCallPayload(c *configuredinference.FunctionCallV1) string {
	b, _ := json.Marshal(struct {
		ID        string          `json:"call_id"`
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}{c.ID, c.Name, c.Arguments})
	return string(b)
}
func toolResultPayload(r mcpadmission.ResultV1) string {
	b, _ := json.Marshal(struct {
		ID      string   `json:"call_id"`
		IsError bool     `json:"is_error"`
		Content []string `json:"content"`
	}{r.CallID(), r.IsError(), r.Content()})
	return string(b)
}
func toolPairMatches(call *configuredinference.FunctionCallV1, model domain.AttemptOperationObservationV1, result mcpadmission.ResultV1) bool {
	return call != nil && call.ID == result.CallID() && call.Name == result.Name() && retainedDigest(call.Arguments) == result.ArgumentsDigest() && model.ResponseDigest == result.ModelResponseDigest()
}
func toolUsageContinuable(o domain.AttemptOperationObservationV1) bool {
	return o.Usage.State == domain.AttemptOperationUsageObservedV1 && o.Usage.AmountMicrounits != nil
}
