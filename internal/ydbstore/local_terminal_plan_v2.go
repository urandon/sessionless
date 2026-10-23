package ydbstore

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// buildLocalCanonicalPlanV2 is a pure, bounded correspondence codec, not an
// authority producer. Current callers must rejoin the original Source/Store,
// issuer and conservative funding witness. Historical callers must authenticate
// the exact V2 capsule and successful terminal seal before decoding DATA.
// A nil profile never reconstructs a current request or grants tool execution.
func buildLocalCanonicalPlanV2(run domain.Run, limits domain.ProductLimits, profileDigest string, profile *domain.ConfiguredInferenceProfileV1, ops []LocalCompactionOperationV1, manifest domain.ArtifactManifestID) (*canonicaloutput.PlanV1, error) {
	deny := func() (*canonicaloutput.PlanV1, error) { return nil, domain.ErrMembershipDenied }
	if len(ops) < 3 || len(ops) > 12 || domain.ValidateSHA256Digest("terminal.profile", profileDigest) != nil {
		return deny()
	}
	if profile != nil {
		d, err := profile.Digest()
		if err != nil || d != profileDigest {
			return deny()
		}
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
	seenCalls := make(map[string]bool)
	var normalized uint64
	for i, item := range ops {
		r, o := item.Reservation, item.Observation
		op := r.Operation
		if op.Scope != ops[0].Reservation.Operation.Scope || op.Scope.RunID != run.ID || op.Scope.TenantID != run.TenantID || op.Scope.SessionID != run.SessionID ||
			op.Sequence != uint64(i+1) || r.Validate() != nil || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(op.Debit) ||
			len(item.Response) == 0 || len(item.Response) > 64<<10 || uint64(len(item.Response)) > (384<<10)-normalized ||
			i > 0 && op.ValidatePredecessor(ops[i-1].Reservation, ops[i-1].Observation) != nil {
			return deny()
		}
		normalized += uint64(len(item.Response))
		sum := sha256.Sum256(item.Response)
		if uint64(len(item.Response)) != o.ResponseBytes || hex.EncodeToString(sum[:]) != o.ResponseDigest {
			return deny()
		}
		if profile != nil {
			request := sha256.Sum256(item.Request)
			if uint64(len(item.Request)) != op.RequestBytes || hex.EncodeToString(request[:]) != op.RequestDigest {
				return deny()
			}
		}
		if op.Kind == domain.AttemptOperationMCPToolV1 {
			if i == 0 || i == len(ops)-1 || pending == nil || ops[i-1].Reservation.Operation.Kind != domain.AttemptOperationModelV1 {
				return deny()
			}
			tool, err := mcpadmission.DecodeSealedToolResultDataV1(r, o, item.Response)
			if err != nil || tool.IsError() || tool.CallID() != pending.ID || tool.Name() != pending.Name || tool.ModelResponseDigest() != ops[i-1].Observation.ResponseDigest {
				return deny()
			}
			arguments := sha256.Sum256(pending.Arguments)
			if hex.EncodeToString(arguments[:]) != tool.ArgumentsDigest() || profile != nil && tool.ValidateConfiguredCallV1(*profile, ops[i-1].Reservation, ops[i-1].Observation, ops[i-1].Request, ops[i-1].Response) != nil {
				return deny()
			}
			pair, err := localMCPCanonicalToolEvents(pending.ID, pending.Name, pending.Arguments, tool)
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
			v, err := configuredinference.DecodeNormalizedResponseBodyV1(*profile, item.Request, item.Response, op.Kind)
			usage, ue := v.Usage.OperationUsage()
			if err != nil || ue != nil || !reflect.DeepEqual(usage, o.Usage) {
				if v.Call != nil {
					clear(v.Call.Arguments)
				}
				return deny()
			}
			decoded = configuredinference.SealedRetainedResponseV2{Stop: v.Stop, Text: v.Text, Call: v.Call}
		}
		if decoded.Call != nil {
			pending = decoded.Call // retain until paired, or clear on every denial
			if decoded.Stop != configuredinference.StopToolV1 || op.Kind != domain.AttemptOperationModelV1 || seenCalls[pending.ID] || i == len(ops)-1 || ops[i+1].Reservation.Operation.Kind != domain.AttemptOperationMCPToolV1 {
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
	if pending != nil || len(result.ToolEvents) == 0 {
		return deny()
	}
	return canonicaloutput.EncodeCompletionV1(run.ID, limits, result, manifest)
}
