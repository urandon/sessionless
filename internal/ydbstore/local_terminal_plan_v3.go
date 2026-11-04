package ydbstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This correspondence program consumes only authenticated original material
// at its mounted callers. A nil profile is sealed historical DATA, never a
// reconstructed MODEL request or current tool/consent authority.
func buildLocalCanonicalPlanV3(run domain.Run, limits domain.ProductLimits, profileDigest string, profile *domain.ConfiguredInferenceProfileV1, source domain.AttemptExecutionSourceV1, ops []LocalCompactionOperationV1, consents []domain.WebSearchConsentStateV1, catalogs []compactioncontext.ModelCatalogV3, manifest domain.ArtifactManifestID) (*canonicaloutput.PlanV1, error) {
	deny := func() (*canonicaloutput.PlanV1, error) { return nil, domain.ErrMembershipDenied }
	if len(ops) < 3 || len(ops) > 12 || source.Validate() != nil || len(source.Spending) != 4 || source.Scope.RunID != run.ID || source.Scope.TenantID != run.TenantID || source.Scope.SessionID != run.SessionID || domain.ValidateSHA256Digest("terminal.profile", profileDigest) != nil {
		return deny()
	}
	if profile != nil {
		d, err := profile.Digest()
		if err != nil || d != profileDigest {
			return deny()
		}
	}
	if _, err := ports.OwnedSearchConsentStatesDigestV4(consents); err != nil {
		return deny()
	}
	retained := make([]localRetainedTerminalOperationV1, len(ops))
	for i, item := range ops {
		retained[i] = localRetainedTerminalOperationV1{Reservation: item.Reservation, Observation: item.Observation, NormalizedBody: item.Response}
	}
	if localSuccessCatalogsV3(profileDigest, retained, catalogs) != nil {
		return deny()
	}
	var result ports.ExecutionResult
	defer func() {
		for _, event := range result.ToolEvents {
			clear(event.Payload)
		}
	}()
	var pending *configuredinference.FunctionCallV1
	defer func() {
		if pending != nil {
			clear(pending.Arguments)
		}
	}()
	seenCalls := map[string]bool{}
	usedConsents := map[domain.WebSearchConsentRequestIDV1]bool{}
	var normalized uint64
	searches := 0
	for i, item := range ops {
		r, o := item.Reservation, item.Observation
		op := r.Operation
		if op.Scope != ops[0].Reservation.Operation.Scope || op.Scope.RunID != run.ID || op.Scope.TenantID != run.TenantID || op.Scope.SessionID != run.SessionID || op.Sequence != uint64(i+1) || r.Validate() != nil || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(op.Debit) || len(item.Response) == 0 || len(item.Response) > 64<<10 || uint64(len(item.Response)) > (384<<10)-normalized || i > 0 && op.ValidatePredecessor(ops[i-1].Reservation, ops[i-1].Observation) != nil {
			return deny()
		}
		normalized += uint64(len(item.Response))
		if !localTerminalMaterialBytesV3(item.Response, o.ResponseBytes, o.ResponseDigest) || profile != nil && !localTerminalMaterialBytesV3(item.Request, op.RequestBytes, op.RequestDigest) {
			return deny()
		}
		if op.Kind == domain.AttemptOperationMCPToolV1 || op.Kind == domain.AttemptOperationWebSearchV1 {
			if i == 0 || i == len(ops)-1 || pending == nil || ops[i-1].Reservation.Operation.Kind != domain.AttemptOperationModelV1 {
				return deny()
			}
			var pair []ports.ExecutionToolEvent
			var err error
			if op.Kind == domain.AttemptOperationMCPToolV1 {
				tool, decodeErr := mcpadmission.DecodeSealedToolResultDataV1(r, o, item.Response)
				if decodeErr != nil || tool.IsError() || tool.CallID() != pending.ID || tool.Name() != pending.Name || tool.ModelResponseDigest() != ops[i-1].Observation.ResponseDigest || rawLocalFailureHash(pending.Arguments) != tool.ArgumentsDigest() || profile != nil && tool.ValidateConfiguredCallV1(*profile, ops[i-1].Reservation, ops[i-1].Observation, ops[i-1].Request, ops[i-1].Response) != nil || !localMCPResultCatalogV3(item.Response, ops[i-1].Reservation.Operation.Sequence, catalogs) {
					return deny()
				}
				pair, err = localMCPCanonicalToolEvents(pending.ID, pending.Name, pending.Arguments, tool)
			} else {
				var id domain.WebSearchConsentRequestIDV1
				pair, id, err = localSearchCanonicalToolEventsV3(ops[i-1], item, pending, source, consents)
				if err != nil || usedConsents[id] {
					return deny()
				}
				usedConsents[id] = true
				searches++
			}
			if err != nil {
				return deny()
			}
			result.ToolEvents = append(result.ToolEvents, pair...)
			clear(pending.Arguments)
			pending = nil
			continue
		}
		if pending != nil || op.Kind != domain.AttemptOperationModelV1 && op.Kind != domain.AttemptOperationCompactionV1 {
			return deny()
		}
		var decoded configuredinference.SealedRetainedResponseV2
		if profile == nil {
			var err error
			decoded, err = configuredinference.DecodeSealedRetainedResponseV2(profileDigest, r, o, item.Response)
			if err != nil {
				return deny()
			}
		} else {
			value, err := configuredinference.DecodeNormalizedResponseBodyV1(*profile, item.Request, item.Response, op.Kind)
			usage, ue := value.Usage.OperationUsage()
			if err != nil || ue != nil || !reflect.DeepEqual(usage, o.Usage) {
				if value.Call != nil {
					clear(value.Call.Arguments)
				}
				return deny()
			}
			decoded = configuredinference.SealedRetainedResponseV2{Stop: value.Stop, Text: value.Text, Call: value.Call}
			if op.Kind == domain.AttemptOperationModelV1 {
				actual, err := compactioncontext.ReadModelCatalogV3(op.Sequence, item.Request)
				matched := false
				for _, catalog := range catalogs {
					matched = matched || catalog.Sequence == op.Sequence && bytes.Equal(catalog.Tools, actual.Tools)
				}
				clear(actual.Tools)
				if err != nil || !matched {
					if decoded.Call != nil {
						clear(decoded.Call.Arguments)
					}
					return deny()
				}
			}
		}
		if decoded.Call != nil {
			pending = decoded.Call
			if decoded.Stop != configuredinference.StopToolV1 || op.Kind != domain.AttemptOperationModelV1 || seenCalls[pending.ID] || i == len(ops)-1 || ops[i+1].Reservation.Operation.Kind != domain.AttemptOperationMCPToolV1 && ops[i+1].Reservation.Operation.Kind != domain.AttemptOperationWebSearchV1 {
				return deny()
			}
			seenCalls[pending.ID] = true
		} else if decoded.Stop != configuredinference.StopFinalV1 {
			return deny()
		}
		if i == len(ops)-1 {
			if op.Kind != domain.AttemptOperationModelV1 || pending != nil {
				return deny()
			}
			result.Summary = decoded.Text
		}
	}
	if pending != nil || searches == 0 || searches != len(consents) {
		return deny()
	}
	return canonicaloutput.EncodeCompletionV1(run.ID, limits, result, manifest)
}

func localTerminalMaterialBytesV3(body []byte, size uint64, digest string) bool {
	hash := sha256.Sum256(body)
	return uint64(len(body)) == size && hex.EncodeToString(hash[:]) == digest
}

func localMCPResultCatalogV3(normalized []byte, modelSequence uint64, catalogs []compactioncontext.ModelCatalogV3) bool {
	var envelope struct {
		Grant json.RawMessage `json:"grant"`
	}
	if json.Unmarshal(normalized, &envelope) != nil {
		return false
	}
	grant, err := mcpadmission.ParseMCPToolGrantV1(envelope.Grant)
	if err != nil {
		return false
	}
	descriptor, err := mcpwire.ParseDescriptorV1(grant.Descriptor, grant.Limits)
	if err != nil {
		return false
	}
	for _, catalog := range catalogs {
		if catalog.Sequence != modelSequence {
			continue
		}
		var tools []localCatalogToolV1
		if json.Unmarshal(catalog.Tools, &tools) != nil {
			return false
		}
		for _, tool := range tools {
			if tool.Function.Name == descriptor.Name() {
				return tool.Function.Description == grant.ModelDescription && bytes.Equal(tool.Function.Parameters, descriptor.InputSchema())
			}
		}
	}
	return false
}

func localSearchCanonicalToolEventsV3(previous, item LocalCompactionOperationV1, call *configuredinference.FunctionCallV1, source domain.AttemptExecutionSourceV1, consents []domain.WebSearchConsentStateV1) ([]ports.ExecutionToolEvent, domain.WebSearchConsentRequestIDV1, error) {
	deny := func() ([]ports.ExecutionToolEvent, domain.WebSearchConsentRequestIDV1, error) {
		return nil, "", domain.ErrMembershipDenied
	}
	r, o, op := item.Reservation, item.Observation, item.Reservation.Operation
	if call == nil || call.Name != "web_search" || op.SearchWire == nil || op.SearchWire.ArgumentsBytes != uint64(len(call.Arguments)) || rawLocalFailureHash(call.Arguments) != op.ArgumentsDigest || len(source.Spending) != 4 {
		return deny()
	}
	var id domain.WebSearchConsentRequestIDV1
	for _, state := range consents {
		request := state.Request
		rd, _ := request.Digest()
		if string(rd) != op.ConsentRequestDigest {
			continue
		}
		if state.Validate() != nil || state.Status == domain.WebSearchConsentPendingV1 || state.Decision == nil || state.Decision.Choice != domain.WebSearchConsentApproveV1 || request.ValidateArguments(call.Arguments) != nil || request.Arguments.Size != int64(len(call.Arguments)) {
			return deny()
		}
		dd, err := state.Decision.Digest()
		if err != nil || string(dd) != op.ConsentDecisionDigest || string(request.ID) != op.ConsentDecisionRef || request.CallID != call.ID || call.ID != op.CallID || request.ModelResponseDigest != previous.Observation.ResponseDigest || request.ModelResponseDigest != op.ModelResponseDigest || request.ArgumentsDigest != op.ArgumentsDigest || request.QueryDigest != op.QueryDigest || request.GrantDigest != op.ToolGrantDigest || request.CreatedAt.Before(previous.Observation.ObservedAt) || state.Decision.ObservedAt.After(r.ReservedAt) || !r.ReservedAt.Before(request.ExpiresAt) || op.Deadline.After(request.ExpiresAt) || state.Status != domain.WebSearchConsentApprovedV1 && state.UpdatedAt.Before(r.ReservedAt) || id != "" {
			return deny()
		}
		id = request.ID
	}
	if id == "" {
		return deny()
	}
	typed, err := domain.DecodeWebSearchResultV1(item.Response)
	if err != nil || typed.ValidateForOperation(op) != nil || typed.ProfileRevision != source.Spending[3].ProfileRevision || typed.RetrievedAt.Before(r.ReservedAt) || typed.RetrievedAt.After(o.ObservedAt) || typed.Outcome != "known_results" && typed.Outcome != "known_empty" {
		return deny()
	}
	callBody, err := json.Marshal(struct {
		CallID          string `json:"call_id"`
		Name            string `json:"name"`
		ArgumentsDigest string `json:"arguments_digest"`
		QueryDigest     string `json:"query_digest"`
	}{call.ID, call.Name, op.ArgumentsDigest, op.QueryDigest})
	if err != nil {
		return deny()
	}
	resultBody, err := json.Marshal(struct {
		Schema    string                          `json:"schema"`
		Outcome   string                          `json:"outcome"`
		Operation domain.AttemptOperationDigestV1 `json:"operation_digest"`
		Sources   []domain.WebSearchSourceV1      `json:"sources"`
		Usage     domain.WebSearchUsageV1         `json:"usage"`
		Warnings  []domain.WebSearchWarningV1     `json:"warnings"`
	}{"sessionless.search-result.v4", typed.Outcome, typed.OperationDigest, typed.Sources, typed.Usage, typed.Warnings})
	if err != nil {
		clear(callBody)
		return deny()
	}
	return []ports.ExecutionToolEvent{{Kind: domain.SessionEventToolCall, CallID: call.ID, ToolName: call.Name, Payload: callBody}, {Kind: domain.SessionEventToolResult, CallID: call.ID, ToolName: call.Name, Payload: resultBody}}, id, nil
}
