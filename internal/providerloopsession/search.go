package providerloopsession

import (
	"bytes"
	"context"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

// Called only by the fixed engine with DATA from the original prepared holder.
// The distinct one-shot SEARCH borrow encloses actual transport construction,
// send/read, durable observation, Close and socket join. No connection or key
// escapes that synchronous callback. Whole-invocation End remains engine-exit
// only; this method never starts or renews it.
func (s *localState) invokeSearch(caller, call context.Context, reservation ports.ReserveAttemptOperationRequestV1, frozen *websearch.FrozenRequestV2, issue ports.SearchCredentialIssueRequestV1, cleanupDeadline time.Time) (result ModelResultV1, err error) {
	op := reservation.Operation
	digest, digestErr := op.Digest()
	if s.searchCredentials == nil || s.searchCredentials.ValidateConstructor() != nil || caller == nil || call == nil || call.Err() != nil || reservation.Validate() != nil || op.Kind != domain.AttemptOperationWebSearchV1 || op.SearchWire == nil || digestErr != nil || op.ValidateForParent(s.parent.Authority, s.parent.Reservation) != nil || frozen == nil || frozen.ValidateOperation(op) != nil || issue.Validate() != nil || issue.Correlation.Scope != op.Scope || issue.Correlation.OperationDigest != digest || string(issue.Correlation.ConsentRequestDigest) != op.ConsentRequestDigest || string(issue.Correlation.ConsentDecisionDigest) != op.ConsentDecisionDigest || !issue.ExpiresAt.Equal(op.Deadline) || !cleanupDeadline.After(op.Deadline) || cleanupDeadline.After(op.Deadline.Add(op.CleanupDuration)) {
		return result, ErrSession
	}
	active, cancel := context.WithDeadline(call, op.Deadline)
	defer cancel()
	cleanup, finish := context.WithDeadline(context.WithoutCancel(caller), cleanupDeadline)
	defer finish()
	payload := frozen.Body()
	defer clear(payload)
	stored, e := s.bodies.ReadAttemptOperationBody(active, op.Scope, op.RequestRef, op.RequestBytes)
	equal := bytes.Equal(stored, payload)
	clear(stored)
	if e != nil || !equal {
		return result, ErrSession
	}
	handle, e := s.searchCredentials.IssueSearchCredential(active, issue)
	if e != nil || handle.Validate() != nil || handle.Correlation != issue.Correlation || handle.Declaration != issue.Declaration || !handle.Credential.ExpiresAt.Equal(issue.ExpiresAt) {
		return result, ErrIncomplete
	}
	defer func() {
		if s.searchCredentials.ReleaseSearchCredential(cleanup, handle) != nil || cleanup.Err() != nil {
			err = ErrIncomplete
		}
	}()
	materializeErr := s.searchCredentials.MaterializeSearchCredential(active, handle, func(delivery ports.ProviderCredentialMaterializationV1, borrowed []byte) (callbackErr error) {
		if delivery.Validate() != nil || delivery.Kind != domain.ProviderCredentialDeliveryDirectV1 || delivery.FilePath != "" || delivery.EnvironmentName != "" || active.Err() != nil {
			return ErrSession
		}
		conn, e := s.keeper.PreflightSearch(active, s.allocation, s.source, op, frozen, handle, borrowed)
		if e != nil {
			return ErrIncomplete
		}
		defer func() {
			if conn.Close() != nil {
				callbackErr = ErrIncomplete
			}
		}()
		proof, e := conn.Publication()
		if e != nil {
			return ErrSession
		}
		r := s.parent.Reservation
		parent := ports.ReserveAttemptEffectRequestV1{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID, LeaseID: r.LeaseID, FenceToken: r.FenceToken, PhysicalInvocationClaimID: r.PhysicalInvocationClaimID, UpstreamIdempotencyKeyDigest: r.UpstreamIdempotencyKeyDigest}
		registered, replayed, e := s.store.PublishOwnedProxy(active, parent, proof)
		if e != nil || replayed {
			return ErrIncomplete
		}
		reserved, e := s.store.ReserveAttemptOperation(active, reservation)
		if e != nil || reserved.Status != ports.AttemptOperationReservedV1 || reserved.Capability == nil || reserved.Validate() != nil {
			return ErrIncomplete
		}
		transport, invokeErr := conn.InvokeSearch(active, registered, reserved.Capability.Clone(), payload)
		result.SendAttempted = transport.SendAttempted
		defer clear(transport.Body)
		usage := domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
		record := func(outcome domain.AttemptOperationOutcomeV1, reason domain.AttemptOperationReasonV1, ref operationbody.ReferenceV1) error {
			var err error
			result.Observation, err = s.recordWithin(cleanup, reserved.ObservationGrant, outcome, reason, ref, usage)
			return err
		}
		if invokeErr != nil {
			outcome := domain.AttemptOperationRejectedBeforeSendV1
			if transport.SendAttempted {
				outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
			}
			_ = record(outcome, domain.AttemptOperationReasonTransportUnknownV1, operationbody.ReferenceV1{})
			return ErrIncomplete
		}
		typed, e := frozen.DecodeResponseV2(active, transport.Metadata, bytes.NewReader(transport.Body), transport.ObservedAt)
		if e != nil {
			reason := domain.AttemptOperationReasonMalformedV1
			if errors.Is(e, websearch.ErrOverflow) {
				reason = domain.AttemptOperationReasonOverflowV1
			}
			_ = record(domain.AttemptOperationAcceptedOutcomeUnknownV1, reason, operationbody.ReferenceV1{})
			return ErrIncomplete
		}
		normalized, e := domain.EncodeWebSearchResultV1(typed)
		defer clear(normalized)
		if e != nil || uint64(len(normalized)) > op.ResultBytes {
			_ = record(domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonOverflowV1, operationbody.ReferenceV1{})
			return ErrIncomplete
		}
		ref, e := s.bodies.Write(active, normalized)
		if e != nil {
			_ = record(domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonWorkerLostV1, operationbody.ReferenceV1{})
			return ErrIncomplete
		}
		reason := domain.AttemptOperationReasonNoneV1
		if typed.Outcome == "known_service_error" {
			reason = domain.AttemptOperationReasonServiceErrorV1
		}
		if record(domain.AttemptOperationCompletedV1, reason, ref) != nil || active.Err() != nil || !time.Now().Before(op.Deadline) || reason == domain.AttemptOperationReasonServiceErrorV1 {
			return ErrIncomplete
		}
		return nil
	})
	if materializeErr != nil || active.Err() != nil {
		return result, ErrIncomplete
	}
	return result, nil
}
