package providerloopsession

import (
	"bytes"
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// One finite engine stays inside the original synchronous Materialize callback.
// Nothing accepts a caller engine, tool operation, successor or secret-bearing
// callback. Store preparation/observations, not this counter, authorize a step.
func (s *localState) runBoundedEngine(caller, call context.Context) (result ModelStepResultV1, err error) {
	policy := s.parent.Authority.HarnessBinding.AgentLoopPolicy
	if policy == nil || policy.Validate() != nil {
		return result, ErrSession
	}
	for i := uint64(0); i < policy.Limits.MaxModelCalls; i++ {
		step, e := s.runModelStep(caller, call)
		result.Model = step.Model
		if step.Compaction != nil {
			result.Compaction = step.Compaction
		}
		if e != nil {
			return result, e
		}
		prepared, e := s.entry.PrepareMCP(call)
		if e != nil {
			return result, ErrIncomplete
		}
		if prepared == nil {
			return result, nil
		} // original verified durable final MODEL
		op, e := prepared.Operation()
		if e != nil {
			return result, ErrIncomplete
		}
		request, e := prepared.Request()
		if e != nil {
			return result, ErrIncomplete
		}
		linked, e := prepared.Call()
		if e != nil {
			return result, ErrIncomplete
		}
		tool, e := s.invokeMCP(caller, call, op, request, linked)
		result.Tool = &tool
		if e != nil {
			return result, e
		}
		// The next ORIGINAL prepared MODEL revalidates the same Source/Store
		// conservative witness and full nonrefundable MCP reservation. A known
		// result is not measured money or a caller-granted continuation flag.
	}
	return result, ErrIncomplete
}

func (s *localState) invokeMCP(caller, call context.Context, op domain.AttemptOperationV1, request mcpwire.RequestV1, linked mcpadmission.CallV1) (result ModelResultV1, err error) {
	if op.Kind != domain.AttemptOperationMCPToolV1 || op.ValidateForParent(s.parent.Authority, s.parent.Reservation) != nil || op.Scope.SessionID != s.source.Scope.SessionID || op.Deadline.After(s.allocation.Window.ExpiresAt) {
		return result, ErrSession
	}
	if d, ok := caller.Deadline(); ok && d.Before(op.Deadline) {
		return result, ErrSession
	}
	payload := request.Body()
	defer clear(payload)
	stored, e := s.bodies.ReadAttemptOperationBody(call, op.Scope, op.RequestRef, op.RequestBytes)
	if e != nil {
		return result, ErrSession
	}
	equal := bytes.Equal(payload, stored)
	clear(stored)
	if !equal {
		return result, ErrSession
	}
	conn, e := s.keeper.PreflightMCP(call, s.allocation, s.source, op, request)
	if e != nil {
		return result, ErrSession
	}
	defer func() {
		if conn.Close() != nil {
			result.Observation = ports.AuthenticatedAttemptOperationObservationV1{}
			err = ErrIncomplete
		}
	}()
	proof, e := conn.Publication()
	if e != nil {
		return result, ErrSession
	}
	r := s.parent.Reservation
	parent := ports.ReserveAttemptEffectRequestV1{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID, LeaseID: r.LeaseID, FenceToken: r.FenceToken, PhysicalInvocationClaimID: r.PhysicalInvocationClaimID, UpstreamIdempotencyKeyDigest: r.UpstreamIdempotencyKeyDigest}
	registered, replayed, e := s.store.PublishOwnedProxy(call, parent, proof)
	if e != nil || replayed {
		return result, ErrSession
	}
	reserved, e := s.store.ReserveAttemptOperation(call, ports.ReserveAttemptOperationRequestV1{Operation: op})
	if e != nil || reserved.Status != ports.AttemptOperationReservedV1 || reserved.Capability == nil || reserved.Validate() != nil {
		return result, ErrSession
	}
	transport, invokeErr := conn.Invoke(call, registered, reserved.Capability.Clone(), payload)
	result.SendAttempted = transport.SendAttempted
	defer clear(transport.Body)
	// The admitted endpoint/price declaration is not a measured service bill.
	// This transport currently has no authoritative measured-cost channel. Keep
	// the known result durable and its actual monetary usage UNKNOWN. Only the
	// original Source/Store conservative funding witness may admit a successor;
	// a completed transport result alone is never financial authority.
	usage := domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
	if invokeErr != nil {
		outcome := domain.AttemptOperationRejectedBeforeSendV1
		if result.SendAttempted {
			outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
		}
		result.Observation, _ = s.record(reserved.ObservationGrant, outcome, domain.AttemptOperationReasonTransportUnknownV1, operationbody.ReferenceV1{}, usage)
		return result, ErrIncomplete
	}
	parsed, e := request.DecodeToolResultV1(transport.Body)
	if e != nil {
		result.Observation, _ = s.record(reserved.ObservationGrant, domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonMalformedV1, operationbody.ReferenceV1{}, usage)
		return result, ErrIncomplete
	}
	normalized, e := mcpadmission.EncodeToolResultV1(linked, op, parsed, usage)
	defer clear(normalized)
	if e != nil {
		result.Observation, _ = s.record(reserved.ObservationGrant, domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonMalformedV1, operationbody.ReferenceV1{}, usage)
		return result, ErrIncomplete
	}
	ref, e := s.bodies.Write(call, normalized)
	if e != nil {
		result.Observation, _ = s.record(reserved.ObservationGrant, domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonWorkerLostV1, operationbody.ReferenceV1{}, usage)
		return result, ErrIncomplete
	}
	reason := domain.AttemptOperationReasonNoneV1
	if parsed.IsError {
		reason = domain.AttemptOperationReasonServiceErrorV1
	}
	result.Observation, e = s.record(reserved.ObservationGrant, domain.AttemptOperationCompletedV1, reason, ref, usage)
	if e != nil || call.Err() != nil || !time.Now().Before(op.Deadline) {
		return result, ErrIncomplete
	}
	if parsed.IsError {
		return result, ErrIncomplete
	}
	// Actual close/join below must succeed before the shared engine can prepare
	// MODEL2. Its original Store route, not this return value, authenticates the
	// unknown/full-reserved financial witness and matched terminal feasibility.
	return result, nil
}
