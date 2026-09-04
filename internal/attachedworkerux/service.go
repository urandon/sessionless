package attachedworkerux

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrInvalidRequest = errors.New("attached worker UX request is invalid")
	ErrNotFound       = errors.New("attached worker was not found")
	ErrBackend        = errors.New("attached worker UX backend is unavailable")
)

type Clock func() time.Time

type Service struct {
	store ports.AttachedWorkerUXReadStore
	now   Clock
}

func NewService(store ports.AttachedWorkerUXReadStore, now Clock) (*Service, error) {
	if store == nil {
		return nil, ErrInvalidRequest
	}
	if now == nil {
		now = time.Now
	}
	return &Service{store: store, now: now}, nil
}

func (service *Service) Get(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
) (AttachedWorkerUXReadModelV1, error) {
	if ctx == nil || tenantID.Validate() != nil || ownerUserID.Validate() != nil || workerID.Validate() != nil {
		return AttachedWorkerUXReadModelV1{}, ErrInvalidRequest
	}
	worker, found, err := service.store.LoadAttachedWorker(ctx, tenantID, ownerUserID, workerID)
	if err != nil {
		return AttachedWorkerUXReadModelV1{}, ErrBackend
	}
	if !found {
		return AttachedWorkerUXReadModelV1{}, ErrNotFound
	}
	if worker.TenantID != tenantID || worker.OwnerUserID != ownerUserID || worker.ID != workerID {
		return AttachedWorkerUXReadModelV1{}, ErrBackend
	}
	return service.reduce(ctx, worker, canonicalNow(service.now()), true)
}

func (service *Service) List(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	afterWorkerID domain.AttachedWorkerID,
	limit uint64,
) (AttachedWorkerListV1, error) {
	if ctx == nil || tenantID.Validate() != nil || ownerUserID.Validate() != nil || limit == 0 || limit > MaxListLimitV1 {
		return AttachedWorkerListV1{}, ErrInvalidRequest
	}
	if afterWorkerID != "" && afterWorkerID.Validate() != nil {
		return AttachedWorkerListV1{}, ErrInvalidRequest
	}
	workers, err := service.store.ListAttachedWorkers(ctx, tenantID, ownerUserID, afterWorkerID, limit+1)
	if err != nil {
		return AttachedWorkerListV1{}, ErrBackend
	}
	if uint64(len(workers)) > limit+1 {
		return AttachedWorkerListV1{}, ErrBackend
	}
	evaluatedAt := canonicalNow(service.now())
	hasMore := uint64(len(workers)) > limit
	if hasMore {
		workers = workers[:limit]
	}
	result := AttachedWorkerListV1{
		Version: ReadModelVersionV1, EvaluatedAt: evaluatedAt,
		Items: make([]AttachedWorkerSummaryV1, 0, len(workers)), HasMore: hasMore,
	}
	previousWorkerID := afterWorkerID
	for _, worker := range workers {
		if worker.TenantID != tenantID || worker.OwnerUserID != ownerUserID || worker.ID <= previousWorkerID {
			return AttachedWorkerListV1{}, ErrBackend
		}
		detail, err := service.reduce(ctx, worker, evaluatedAt, false)
		if err != nil {
			return AttachedWorkerListV1{}, err
		}
		result.Items = append(result.Items, AttachedWorkerSummaryV1{
			EvaluatedAt: detail.EvaluatedAt, Worker: detail.Worker, Connectivity: detail.Connectivity,
			ExecutionState:      detail.Execution.State,
			ObservationWarnings: append([]ReasonCodeV1(nil), detail.ObservationWarnings...),
		})
		previousWorkerID = worker.ID
	}
	if hasMore && len(result.Items) > 0 {
		result.NextWorkerID = result.Items[len(result.Items)-1].Worker.WorkerID
	}
	return result, nil
}

func (service *Service) Diagnostics(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
) (AttachedWorkerDiagnosticsV1, error) {
	detail, err := service.Get(ctx, tenantID, ownerUserID, workerID)
	if err != nil {
		return AttachedWorkerDiagnosticsV1{}, err
	}
	return AttachedWorkerDiagnosticsV1{
		Version: ReadModelVersionV1, EvaluatedAt: detail.EvaluatedAt, WorkerID: detail.Worker.WorkerID,
		Facts: []DiagnosticFactV1{
			{Cohort: "identity", Code: "desired_state", State: detail.Worker.DesiredState},
			{Cohort: "identity", Code: "observed_state", State: detail.Worker.ObservedState},
			{Cohort: "connectivity", Code: "connection_state", State: detail.Connectivity.State, ObservedAt: detail.Connectivity.LastContactAt, Freshness: detail.Connectivity.Freshness},
			{Cohort: "readiness", Code: "daemon_state", State: detail.Readiness.DaemonObservation.State, Freshness: detail.Readiness.DaemonObservation.Freshness},
			{Cohort: "readiness", Code: "isolation_verification", State: detail.Readiness.Isolation.VerificationState},
			{Cohort: "eligibility", Code: "admission_preview", State: detail.AdmissionPreview.State},
			{Cohort: "execution", Code: "attempt_state", State: detail.Execution.State},
			{Cohort: "execution", Code: "cancel_ack", State: detail.Execution.CancelAcknowledgement.State},
			{Cohort: "execution", Code: "worker_terminal", State: detail.Execution.WorkerTerminal.State},
			{Cohort: "execution", Code: "canonical_terminal", State: detail.Execution.CanonicalTerminal.State},
			{Cohort: "governance", Code: "remote_erase", State: detail.Governance.RemoteErase},
		},
		Warnings: append([]ReasonCodeV1(nil), detail.ObservationWarnings...),
	}, nil
}

func (service *Service) reduce(ctx context.Context, worker domain.AttachedWorker, evaluatedAt time.Time, includeExecutionOccurrences bool) (AttachedWorkerUXReadModelV1, error) {
	if worker.Validate() != nil {
		return AttachedWorkerUXReadModelV1{}, ErrBackend
	}
	result := AttachedWorkerUXReadModelV1{
		Version: ReadModelVersionV1, EvaluatedAt: evaluatedAt,
		Worker: WorkerV1{
			WorkerID: string(worker.ID), DisplayName: worker.DisplayName, Revision: worker.Revision,
			EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: worker.ConnectionGeneration,
			DesiredState: string(worker.DesiredState), ObservedState: string(worker.ObservedState),
			CreatedAt: worker.CreatedAt, UpdatedAt: worker.UpdatedAt, RevokedAt: worker.RevokedAt,
		},
		Identity: IdentityV1{Algorithm: "ed25519", Fingerprint: fingerprintBytes(worker.IdentityPublicKey), EnrollmentState: "consumed"},
		Readiness: ReadinessV1{
			DaemonObservation: DaemonObservationV1{State: "unknown", Source: "unavailable", Freshness: FreshnessUnknown},
			LastDaemonFailure: LastFailureV1{State: "unknown"}, CredentialState: "unknown",
			Isolation: IsolationV1{ConfigurationState: "unsupported", AdvertisedEvidence: []string{}, VerificationState: "unsupported"},
		},
		Connectivity:     ConnectivityV1{State: "unknown", Freshness: FreshnessUnknown, LastFailure: LastFailureV1{State: "unknown"}},
		Capability:       CapabilityV1{State: "unknown", Harness: HarnessV1{}, IsolationEvidence: []string{}, Features: []string{}},
		AdmissionPreview: AdmissionPreviewV1{State: "not_evaluated"},
		Resource:         ResourceV1{State: "unknown", CredentialState: "unknown", EntitlementState: "unknown", Quota: QuotaObservationV1{State: "unknown"}},
		Execution:        emptyExecution(),
		Governance:       GovernanceV1{AdmissionControl: "unavailable", RemoteErase: "not_requested", AvailableActions: disabledActions()},
	}
	connection, connectionFound, err := service.store.LoadAttachedWorkerConnection(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil {
		return AttachedWorkerUXReadModelV1{}, ErrBackend
	}
	if connectionFound {
		if connection.Validate() != nil || connection.TenantID != worker.TenantID || connection.OwnerUserID != worker.OwnerUserID || connection.WorkerID != worker.ID {
			return AttachedWorkerUXReadModelV1{}, ErrBackend
		}
		result.Connectivity = reduceConnectivity(connection, evaluatedAt)
		manifest, found, err := service.store.LoadAttachedWorkerCapabilityManifest(
			ctx, worker.TenantID, worker.OwnerUserID, worker.ID, connection.CapabilityDigest,
		)
		if err != nil {
			return AttachedWorkerUXReadModelV1{}, ErrBackend
		}
		if found {
			if manifest.Validate() != nil || manifest.TenantID != worker.TenantID || manifest.OwnerUserID != worker.OwnerUserID || manifest.WorkerID != worker.ID {
				return AttachedWorkerUXReadModelV1{}, ErrBackend
			}
			capability, ok := reduceCapability(worker, connection, manifest)
			if !ok {
				return AttachedWorkerUXReadModelV1{}, ErrBackend
			}
			result.Capability = capability
			result.Readiness.Isolation.AdvertisedEvidence = append([]string(nil), capability.IsolationEvidence...)
		}
	}
	attempt, attemptFound, err := service.store.LoadAttachedWorkerAttempt(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil {
		return AttachedWorkerUXReadModelV1{}, ErrBackend
	}
	if attemptFound {
		if attempt.Validate() != nil || attempt.TenantID != worker.TenantID || attempt.OwnerUserID != worker.OwnerUserID || attempt.WorkerID != worker.ID {
			return AttachedWorkerUXReadModelV1{}, ErrBackend
		}
		if !includeExecutionOccurrences {
			result.Execution.State = string(attempt.State)
		} else {
			messages, err := service.store.ListAttachedWorkerAttemptMessages(
				ctx, worker.TenantID, worker.OwnerUserID, worker.ID, attempt.AttemptID,
			)
			if err != nil {
				return AttachedWorkerUXReadModelV1{}, ErrBackend
			}
			times, ok := executionOccurrenceTimes(attempt, messages)
			if !ok {
				return AttachedWorkerUXReadModelV1{}, ErrBackend
			}
			result.Execution = reduceExecution(attempt, times)
		}
	}
	if worker.DesiredState == domain.AttachedWorkerDesiredRevoked {
		result.Governance.RemoteErase = "unknown"
	}
	result.ObservationWarnings = warnings(worker, connection, connectionFound, result.Capability, attempt, attemptFound, evaluatedAt)
	return result, nil
}

func reduceConnectivity(connection domain.AttachedWorkerConnection, evaluatedAt time.Time) ConnectivityV1 {
	freshness := FreshnessUnknown
	if !connection.LastCheckpointAt.IsZero() && !connection.PresenceExpiresAt.IsZero() {
		freshness = FreshnessFresh
		if !evaluatedAt.Before(connection.PresenceExpiresAt) {
			freshness = FreshnessExpired
		}
	}
	return ConnectivityV1{
		ConnectionID: string(connection.ID), State: string(connection.State), ConnectedAt: connection.ConnectedAt,
		LastContactAt: connection.LastCheckpointAt, PresenceExpiresAt: connection.PresenceExpiresAt,
		AuthenticationExpiresAt: connection.AuthExpiresAt, Freshness: freshness,
		LastFailure: LastFailureV1{State: "unknown"},
	}
}

func reduceCapability(worker domain.AttachedWorker, connection domain.AttachedWorkerConnection, stored domain.AttachedWorkerCapabilityManifest) (CapabilityV1, bool) {
	var manifest attachedworkerprotocol.CapabilityManifestV1
	decoder := json.NewDecoder(bytes.NewReader(stored.ManifestPayload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&manifest) != nil || decoder.Decode(&struct{}{}) != io.EOF || manifest.Validate() != nil {
		return CapabilityV1{}, false
	}
	digest, err := attachedworkerprotocol.ManifestDigestV1(manifest)
	if err != nil || hex.EncodeToString(digest) != string(stored.Digest) || manifest.WorkerID != string(worker.ID) ||
		manifest.EnrollmentGeneration != worker.EnrollmentGeneration || manifest.Revision != stored.ManifestRevision ||
		stored.EnrollmentGeneration != worker.EnrollmentGeneration || stored.EnrollmentGeneration != connection.EnrollmentGeneration ||
		stored.ManifestRevision != connection.ManifestRevision || stored.ProtocolVersion != connection.ProtocolVersion ||
		stored.IdentityKeyDigest != connection.ManifestIdentityKey || stored.Digest != connection.CapabilityDigest {
		return CapabilityV1{}, false
	}
	isolation := make([]string, 0, len(manifest.IsolationEvidence))
	for _, value := range manifest.IsolationEvidence {
		isolation = append(isolation, string(value))
	}
	features := make([]string, 0, len(manifest.Features))
	for _, value := range manifest.Features {
		features = append(features, string(value))
	}
	return CapabilityV1{
		State: "advertised", ManifestRevision: manifest.Revision, DigestFingerprint: fingerprintDigest(string(stored.Digest)),
		OperatingSystem: manifest.OperatingSystem, Architecture: manifest.Architecture, BuildID: manifest.BuildID,
		Harness:           HarnessV1{Name: manifest.HarnessName, Version: manifest.HarnessVersion, Surface: string(manifest.HarnessSurface)},
		IsolationEvidence: isolation, Features: features, MaxConcurrentAttempts: manifest.MaxConcurrentAttempts,
		ObservedAt: connection.ManifestObservedAt,
	}, true
}

func emptyExecution() ExecutionV1 {
	return ExecutionV1{
		State: "none", CancelRequest: CancelRequestV1{State: "none"},
		CancelAcknowledgement: CancelAcknowledgementV1{State: "none"},
		ProcessObservation:    ProcessObservationV1{State: "unknown", Source: "unavailable", Freshness: FreshnessUnknown},
		WorkerTerminal:        WorkerTerminalV1{State: "none"}, CanonicalTerminal: CanonicalTerminalV1{State: "none"},
	}
}

type executionTimes struct {
	cancelRequestedAt    time.Time
	cancelAcknowledgedAt time.Time
	terminalReceivedAt   time.Time
	terminalCommittedAt  time.Time
}

func executionOccurrenceTimes(attempt domain.AttachedWorkerAttemptV1, messages []domain.AttachedWorkerAttemptMessageV1) (executionTimes, bool) {
	var result executionTimes
	for _, message := range messages {
		if message.Validate() != nil || message.TenantID != attempt.TenantID || message.OwnerUserID != attempt.OwnerUserID ||
			message.WorkerID != attempt.WorkerID || message.AttemptID != attempt.AttemptID ||
			message.ConnectionGeneration != attempt.ConnectionGeneration || message.CreatedAt.Before(attempt.CreatedAt) ||
			message.CreatedAt.After(attempt.UpdatedAt) {
			return executionTimes{}, false
		}
		frame, ok := decodeOccurrenceFrame(message)
		if !ok || frame.WorkerID != string(attempt.WorkerID) || frame.EnrollmentGeneration != attempt.EnrollmentGeneration ||
			frame.ConnectionGeneration != attempt.ConnectionGeneration {
			return executionTimes{}, false
		}
		switch message.Kind {
		case domain.AttachedWorkerAttemptMessageCancelRequested:
			if !result.cancelRequestedAt.IsZero() || message.Direction != domain.AttachedWorkerAttemptPlatformToWorker ||
				attempt.CancelRevision == 0 || !message.OperationDeadline.Equal(attempt.CancelDeadline) || frame.Cancel == nil ||
				frame.Cancel.CancelRevision != attempt.CancelRevision || !bindingMatchesAttempt(frame.Cancel.Binding, attempt) {
				return executionTimes{}, false
			}
			result.cancelRequestedAt = message.CreatedAt
		case domain.AttachedWorkerAttemptMessageCancelAcknowledged:
			if !result.cancelAcknowledgedAt.IsZero() || message.Direction != domain.AttachedWorkerAttemptWorkerToPlatform ||
				attempt.CancelRevision == 0 || frame.CancelAck == nil || frame.CancelAck.CancelRevision != attempt.CancelRevision ||
				!bindingMatchesAttempt(frame.CancelAck.Binding, attempt) {
				return executionTimes{}, false
			}
			result.cancelAcknowledgedAt = message.CreatedAt
		case domain.AttachedWorkerAttemptMessageTerminal:
			if message.Direction != domain.AttachedWorkerAttemptWorkerToPlatform || frame.Terminal == nil ||
				message.MaterializationReservationID != attempt.ReservationID || message.ExecutionConnectionID != attempt.ConnectionID ||
				!bindingMatchesAttempt(frame.Terminal.Binding, attempt) {
				return executionTimes{}, false
			}
			if frame.Terminal.TerminalSequence == attempt.TerminalSequence && string(frame.Terminal.Status) == string(attempt.TerminalStatus) &&
				terminalResultMatchesStatus(frame.Terminal.Result, attempt.TerminalStatus) &&
				digestBytesMatch(frame.Terminal.EvidenceDigest, string(attempt.TerminalEvidenceDigest)) {
				if !result.terminalReceivedAt.IsZero() {
					return executionTimes{}, false
				}
				result.terminalReceivedAt = message.CreatedAt
			}
		case domain.AttachedWorkerAttemptMessageTerminalCommitted:
			if !result.terminalCommittedAt.IsZero() || message.Direction != domain.AttachedWorkerAttemptPlatformToWorker ||
				attempt.TerminalSequence == 0 || frame.TerminalAck == nil ||
				message.MaterializationReservationID != attempt.ReservationID || message.ExecutionConnectionID != attempt.ConnectionID ||
				frame.TerminalAck.TerminalSequence != attempt.TerminalSequence || string(frame.TerminalAck.Status) != string(attempt.TerminalStatus) ||
				!terminalResultMatchesStatus(frame.TerminalAck.Result, attempt.TerminalStatus) ||
				!digestBytesMatch(frame.TerminalAck.EvidenceDigest, string(attempt.TerminalEvidenceDigest)) ||
				!bindingMatchesAttempt(frame.TerminalAck.Binding, attempt) {
				return executionTimes{}, false
			}
			result.terminalCommittedAt = message.CreatedAt
		}
	}
	if attempt.CancelRevision > 0 && result.cancelRequestedAt.IsZero() {
		return executionTimes{}, false
	}
	if !result.cancelAcknowledgedAt.IsZero() && result.cancelAcknowledgedAt.Before(result.cancelRequestedAt) {
		return executionTimes{}, false
	}
	if !result.terminalCommittedAt.IsZero() && attempt.CancelRevision > 0 && result.terminalCommittedAt.Before(result.cancelRequestedAt) {
		return executionTimes{}, false
	}
	if !result.terminalCommittedAt.IsZero() && (result.terminalReceivedAt.IsZero() || result.terminalCommittedAt.Before(result.terminalReceivedAt)) {
		return executionTimes{}, false
	}
	if !result.terminalCommittedAt.IsZero() && !result.cancelAcknowledgedAt.IsZero() && result.terminalCommittedAt.Before(result.cancelAcknowledgedAt) {
		return executionTimes{}, false
	}
	if attempt.State == domain.AttachedWorkerAttemptCancelAcknowledged && result.cancelAcknowledgedAt.IsZero() {
		return executionTimes{}, false
	}
	if attempt.TerminalSequence > 0 && (attempt.State == domain.AttachedWorkerAttemptTerminalCommitted || attempt.State == domain.AttachedWorkerAttemptRetired) && result.terminalCommittedAt.IsZero() {
		return executionTimes{}, false
	}
	return result, true
}

func decodeOccurrenceFrame(message domain.AttachedWorkerAttemptMessageV1) (attachedworkerprotocol.FrameV1, bool) {
	batch, err := attachedworkerprotocol.DecodeBatchV1(message.Payload)
	if err != nil || len(batch.Frames) != 1 {
		return attachedworkerprotocol.FrameV1{}, false
	}
	frame := batch.Frames[0]
	wantKind, attemptSequence, ok := occurrenceFrameIdentity(frame)
	if !ok || wantKind != message.Kind || frame.Sequence != message.EnvelopeSequence ||
		frame.ConnectionGeneration != message.ConnectionGeneration || attemptSequence != message.AttemptSequence {
		return attachedworkerprotocol.FrameV1{}, false
	}
	fingerprint, err := attachedworkerprotocol.AttemptFrameFingerprintV1(frame)
	if err != nil || hex.EncodeToString(fingerprint) != string(message.Fingerprint) {
		return attachedworkerprotocol.FrameV1{}, false
	}
	return frame, true
}

func occurrenceFrameIdentity(frame attachedworkerprotocol.FrameV1) (domain.AttachedWorkerAttemptMessageKind, uint64, bool) {
	switch frame.Kind {
	case attachedworkerprotocol.MessageLeaseOffer:
		if frame.LeaseOffer != nil {
			return domain.AttachedWorkerAttemptMessageLeaseOffered, frame.LeaseOffer.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageLeaseClaim:
		if frame.LeaseClaim != nil {
			return domain.AttachedWorkerAttemptMessageLeaseClaim, frame.LeaseClaim.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageLeaseAccepted:
		if frame.LeaseAccepted != nil {
			return domain.AttachedWorkerAttemptMessageLeaseAccepted, frame.LeaseAccepted.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageProgress:
		if frame.Progress != nil {
			return domain.AttachedWorkerAttemptMessageProgress, frame.Progress.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageCancel:
		if frame.Cancel != nil {
			return domain.AttachedWorkerAttemptMessageCancelRequested, frame.Cancel.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageCancelAck:
		if frame.CancelAck != nil {
			return domain.AttachedWorkerAttemptMessageCancelAcknowledged, frame.CancelAck.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageTerminal:
		if frame.Terminal != nil {
			return domain.AttachedWorkerAttemptMessageTerminal, frame.Terminal.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageTerminalAck:
		if frame.TerminalAck != nil {
			return domain.AttachedWorkerAttemptMessageTerminalCommitted, frame.TerminalAck.AttemptSequence, true
		}
	}
	return "", 0, false
}

func bindingMatchesAttempt(binding attachedworkerprotocol.AttemptBindingV1, attempt domain.AttachedWorkerAttemptV1) bool {
	return binding.RunID == string(attempt.RunID) && binding.AttemptID == string(attempt.AttemptID) &&
		binding.LeaseID == string(attempt.LeaseID) && binding.LeaseGeneration == attempt.LeaseGeneration &&
		binding.FenceToken == string(attempt.FenceToken) && binding.ExpiresAtUnixMicro == attempt.LeaseExpiresAt.UnixMicro() &&
		digestBytesMatch(binding.ContextDigest, string(attempt.ContextDigest)) &&
		digestBytesMatch(binding.CapabilityDigest, string(attempt.CapabilityDigest)) &&
		digestBytesMatch(binding.PolicyDigest, string(attempt.PolicyDigest))
}

func digestBytesMatch(value []byte, encoded string) bool {
	want, err := hex.DecodeString(encoded)
	return err == nil && bytes.Equal(value, want)
}

func terminalResultMatchesStatus(result attachedworkerprotocol.TerminalResult, status domain.AttachedWorkerTerminalStatus) bool {
	switch status {
	case domain.AttachedWorkerTerminalSucceeded:
		return result == attachedworkerprotocol.TerminalResultCompleted
	case domain.AttachedWorkerTerminalFailed:
		return result == attachedworkerprotocol.TerminalResultFailed
	case domain.AttachedWorkerTerminalCancelled:
		return result == attachedworkerprotocol.TerminalResultCancelled
	default:
		return false
	}
}

func reduceExecution(attempt domain.AttachedWorkerAttemptV1, times executionTimes) ExecutionV1 {
	result := emptyExecution()
	result.State = string(attempt.State)
	result.RunID, result.AttemptID, result.LeaseID = string(attempt.RunID), string(attempt.AttemptID), string(attempt.LeaseID)
	result.LeaseGeneration, result.FenceFingerprint = attempt.LeaseGeneration, fingerprintOpaqueString(string(attempt.FenceToken))
	result.LeaseExpiresAt = attempt.LeaseExpiresAt
	result.ProcessObservation.AttemptID = string(attempt.AttemptID)
	result.ProcessObservation.LeaseGeneration = attempt.LeaseGeneration
	result.ProcessObservation.FenceFingerprint = result.FenceFingerprint
	if attempt.CancelRevision > 0 {
		result.CancelRequest = CancelRequestV1{State: "requested", Revision: attempt.CancelRevision, RequestedAt: times.cancelRequestedAt, AckDeadline: attempt.CancelDeadline}
		result.CancelAcknowledgement = CancelAcknowledgementV1{State: "pending", Revision: attempt.CancelRevision}
		if !times.cancelAcknowledgedAt.IsZero() {
			result.CancelAcknowledgement.State = "acknowledged"
			result.CancelAcknowledgement.AcknowledgedAt = times.cancelAcknowledgedAt
		} else if attempt.State != domain.AttachedWorkerAttemptCancelRequested && attempt.State != domain.AttachedWorkerAttemptCancelledBeforeClaim {
			// Later terminal/retired/fenced heads retain the cancel revision but do
			// not prove that a distinct CancelAck was ever durably observed.
			result.CancelAcknowledgement.State = "unknown"
		}
	}
	if attempt.TerminalSequence > 0 {
		result.WorkerTerminal = WorkerTerminalV1{
			State: "received", Sequence: attempt.TerminalSequence, Status: string(attempt.TerminalStatus),
			EvidenceFingerprint: fingerprintDigest(string(attempt.TerminalEvidenceDigest)),
		}
		if attempt.State == domain.AttachedWorkerAttemptTerminalCommitted || attempt.State == domain.AttachedWorkerAttemptRetired {
			result.CanonicalTerminal = CanonicalTerminalV1{State: "committed", CommittedAt: times.terminalCommittedAt, Sequence: attempt.TerminalSequence, Status: string(attempt.TerminalStatus)}
		}
	}
	return result
}

func warnings(
	worker domain.AttachedWorker,
	connection domain.AttachedWorkerConnection,
	connectionFound bool,
	capability CapabilityV1,
	attempt domain.AttachedWorkerAttemptV1,
	attemptFound bool,
	evaluatedAt time.Time,
) []ReasonCodeV1 {
	set := map[ReasonCodeV1]struct{}{
		ReasonIsolationUnsupported: {}, ReasonQuotaUnknown: {}, ReasonEntitlementUnknown: {}, ReasonControlContractUnavailable: {},
	}
	if worker.DesiredState != domain.AttachedWorkerDesiredActive {
		set[ReasonWorkerNotActive] = struct{}{}
	}
	if worker.DesiredState == domain.AttachedWorkerDesiredRevoked {
		set[ReasonWorkerRevoked] = struct{}{}
	}
	if worker.DesiredState == domain.AttachedWorkerDesiredDrain {
		set[ReasonWorkerDraining] = struct{}{}
	}
	if worker.ObservedState == domain.AttachedWorkerObservedOffline {
		set[ReasonWorkerOffline] = struct{}{}
	}
	if connectionFound {
		switch connection.State {
		case domain.AttachedWorkerConnectionAttaching:
			set[ReasonConnectionAttaching] = struct{}{}
		case domain.AttachedWorkerConnectionSuperseded:
			set[ReasonConnectionSuperseded] = struct{}{}
		}
		if !connection.PresenceExpiresAt.IsZero() && !evaluatedAt.Before(connection.PresenceExpiresAt) {
			set[ReasonPresenceExpired] = struct{}{}
		}
		if !evaluatedAt.Before(connection.AuthExpiresAt) {
			set[ReasonAuthenticationExpired] = struct{}{}
		}
	}
	if capability.State != "advertised" {
		set[ReasonCapabilityMissing] = struct{}{}
	}
	if attemptFound {
		if attempt.State == domain.AttachedWorkerAttemptFencedUnknown {
			set[ReasonAttemptAmbiguous] = struct{}{}
		} else if attempt.State != domain.AttachedWorkerAttemptRetired {
			set[ReasonAttemptActive] = struct{}{}
		}
	}
	result := make([]ReasonCodeV1, 0, len(set))
	for code := range set {
		result = append(result, code)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func disabledActions() []AvailableActionV1 {
	codes := []ActionCodeV1{
		ActionRename, ActionRotateIdentity, ActionPauseAdmission, ActionResumeAdmission,
		ActionDrain, ActionRevoke, ActionRequestCancel, ActionReconnectRemediation,
		ActionReauthRemediation, ActionCheckUpdate, ActionLogout, ActionUninstallPlan,
	}
	result := make([]AvailableActionV1, 0, len(codes))
	for _, code := range codes {
		result = append(result, AvailableActionV1{Code: code, Enabled: false, ReasonCode: ActionUnavailableControlContract})
	}
	return result
}

func fingerprintBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return fingerprintDigest(hex.EncodeToString(digest[:]))
}

func fingerprintOpaqueString(value string) string {
	digest := sha256.Sum256([]byte(value))
	return fingerprintDigest(hex.EncodeToString(digest[:]))
}

func fingerprintDigest(value string) string {
	if len(value) > 12 {
		value = value[:12]
	}
	if value == "" {
		return ""
	}
	return "sha256:" + value
}

func canonicalNow(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}
