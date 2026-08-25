package ydbstore

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestValidateAttachedWorkerAttemptOfferRequiresCanonicalLease(t *testing.T) {
	t.Parallel()
	leaseID, err := domain.NewAttachedWorkerLeaseIDV1("tenant-1", "run-1", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	request := ports.AttachedWorkerAttemptOffer{
		TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1",
		RunID: "run-1", AttemptID: "attempt-1", ReservationID: "reservation-1",
		LeaseID: leaseID, LeaseTTL: time.Minute,
	}
	if err := validateAttachedWorkerAttemptOffer(request); err != nil {
		t.Fatalf("canonical offer rejected: %v", err)
	}
	request.LeaseID = "lease-divergent"
	if err := validateAttachedWorkerAttemptOffer(request); err == nil {
		t.Fatal("divergent retry lease ID was accepted")
	}
	request.LeaseID = leaseID
	request.LeaseTTL = domain.AttachedWorkerLeaseMaximumTTLV1 + time.Nanosecond
	if err := validateAttachedWorkerAttemptOffer(request); err == nil {
		t.Fatal("overlarge fixed lease was accepted")
	}
}

func TestAttachedWorkerOfferEligibilityUsesPersistedAuthority(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	limits := domain.ProductLimits{
		MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute,
		MaxTurns: 4, MaxInputBytes: 1024, MaxContextBytes: 2048,
		MaxContextEvents: 16, MaxArtifacts: 4, MaxToolEvents: 8, MaxToolEventBytes: 4096,
	}
	ttl, err := domain.AttachedWorkerLeaseTTLForLimitsV1(limits)
	if err != nil {
		t.Fatal(err)
	}
	capability := domain.DigestAttachedWorkerCapability([]byte("capability"))
	placement := domain.ExecutionPlacementV1{
		Version: domain.ExecutionPlacementVersionV1, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: "owner-1", WorkerID: "worker-1",
		CapabilityDigest: capability, PolicyDigest: domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy"))),
	}
	request := ports.AttachedWorkerAttemptOffer{
		TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1",
		RunID: "run-1", AttemptID: "attempt-1", ReservationID: "reservation-1", LeaseTTL: ttl,
	}
	worker := domain.AttachedWorker{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, ID: request.WorkerID,
		EnrollmentGeneration: 2, ConnectionGeneration: 3,
		DesiredState: domain.AttachedWorkerDesiredActive, ObservedState: domain.AttachedWorkerObservedOnline,
	}
	connection := domain.AttachedWorkerConnection{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		EnrollmentGeneration: 2, ConnectionGeneration: 3, CapabilityDigest: capability,
		State: domain.AttachedWorkerConnectionOnline, AuthExpiresAt: at.Add(2 * ttl), PresenceExpiresAt: at.Add(2 * ttl),
	}
	loaded := ports.WorkerJobState{
		Job:         domain.WorkerJob{ExecutionPlacement: placement, Limits: limits},
		Run:         domain.Run{ID: request.RunID, Status: domain.RunQueued},
		Attempt:     domain.Attempt{ID: request.AttemptID, Status: domain.AttemptCreated},
		Reservation: domain.QuotaReservation{ID: request.ReservationID, Status: domain.ReservationHeld, ExpiresAt: at.Add(time.Minute)},
	}
	expiresAt := at.Add(ttl)
	if !attachedWorkerOfferEligible(request, worker, connection, loaded, placement, at, expiresAt) {
		t.Fatal("authoritative eligible worker was denied")
	}

	tests := map[string]func(*domain.AttachedWorker, *domain.AttachedWorkerConnection, *ports.WorkerJobState, *ports.AttachedWorkerAttemptOffer){
		"caller TTL divergence": func(_ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *ports.WorkerJobState, request *ports.AttachedWorkerAttemptOffer) {
			request.LeaseTTL++
		},
		"placement owner": func(_ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, loaded *ports.WorkerJobState, _ *ports.AttachedWorkerAttemptOffer) {
			loaded.Job.ExecutionPlacement.OwnerUserID = "owner-2"
		},
		"worker generation": func(worker *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *ports.WorkerJobState, _ *ports.AttachedWorkerAttemptOffer) {
			worker.ConnectionGeneration++
		},
		"capability pin": func(_ *domain.AttachedWorker, connection *domain.AttachedWorkerConnection, _ *ports.WorkerJobState, _ *ports.AttachedWorkerAttemptOffer) {
			connection.CapabilityDigest = domain.DigestAttachedWorkerCapability([]byte("other"))
		},
		"exact auth expiry": func(_ *domain.AttachedWorker, connection *domain.AttachedWorkerConnection, _ *ports.WorkerJobState, _ *ports.AttachedWorkerAttemptOffer) {
			connection.AuthExpiresAt = expiresAt
		},
		"desired drain": func(worker *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *ports.WorkerJobState, _ *ports.AttachedWorkerAttemptOffer) {
			worker.DesiredState = domain.AttachedWorkerDesiredDrain
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidateWorker, candidateConnection, candidateLoaded, candidateRequest := worker, connection, loaded, request
			mutate(&candidateWorker, &candidateConnection, &candidateLoaded, &candidateRequest)
			if attachedWorkerOfferEligible(candidateRequest, candidateWorker, candidateConnection, candidateLoaded, candidateLoaded.Job.ExecutionPlacement, at, expiresAt) {
				t.Fatal("divergent authority was accepted")
			}
		})
	}
}

func TestAttachedWorkerExecutionAuthorityIsDenyFirst(t *testing.T) {
	t.Parallel()
	worker := domain.AttachedWorker{
		TenantID: "tenant-1", OwnerUserID: "owner-1", ID: "worker-1",
		EnrollmentGeneration: 2, ConnectionGeneration: 3, DesiredState: domain.AttachedWorkerDesiredActive,
	}
	connection := domain.AttachedWorkerConnection{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		EnrollmentGeneration: 2, ConnectionGeneration: 3, State: domain.AttachedWorkerConnectionOnline,
	}
	if !attachedWorkerExecutionAuthorityCurrent(worker, connection) {
		t.Fatal("current execution authority was denied")
	}
	for name, mutate := range map[string]func(*domain.AttachedWorker, *domain.AttachedWorkerConnection){
		"revoked": func(worker *domain.AttachedWorker, _ *domain.AttachedWorkerConnection) {
			worker.DesiredState = domain.AttachedWorkerDesiredRevoked
		},
		"rotated enrollment": func(worker *domain.AttachedWorker, _ *domain.AttachedWorkerConnection) {
			worker.EnrollmentGeneration++
		},
		"new connection generation": func(worker *domain.AttachedWorker, _ *domain.AttachedWorkerConnection) {
			worker.ConnectionGeneration++
		},
		"cross owner": func(_ *domain.AttachedWorker, connection *domain.AttachedWorkerConnection) {
			connection.OwnerUserID = "owner-2"
		},
		"draining": func(_ *domain.AttachedWorker, connection *domain.AttachedWorkerConnection) {
			connection.State = domain.AttachedWorkerConnectionDraining
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidateWorker, candidateConnection := worker, connection
			mutate(&candidateWorker, &candidateConnection)
			if attachedWorkerExecutionAuthorityCurrent(candidateWorker, candidateConnection) {
				t.Fatal("stale or revoked execution authority was accepted")
			}
		})
	}
}

func TestAttachedWorkerProtocolProjectionDoesNotClaimRevokingAsRevoked(t *testing.T) {
	t.Parallel()
	if attachedWorkerProtocolStateMatches(domain.AttachedWorkerConnectionRevoked, attachedworkerprotocol.ConnectionRevoking) {
		t.Fatal("revoking protocol state was projected as durably revoked")
	}
	if !attachedWorkerProtocolStateMatches(domain.AttachedWorkerConnectionRevoked, attachedworkerprotocol.ConnectionRevoked) {
		t.Fatal("terminal revoked protocol state was rejected")
	}
}

func TestTerminalMaterializationMustMatchSignedTerminalStatus(t *testing.T) {
	t.Parallel()
	completion := &ports.WorkerCompletion{}
	failure := &ports.WorkerFailure{}
	cancelled := &ports.WorkerFailure{Cancelled: true}
	tests := []struct {
		name   string
		status domain.AttachedWorkerTerminalStatus
		value  ports.AttachedWorkerTerminalMaterialization
		valid  bool
	}{
		{name: "success completion", status: domain.AttachedWorkerTerminalSucceeded, value: ports.AttachedWorkerTerminalMaterialization{Completion: completion}, valid: true},
		{name: "failed failure", status: domain.AttachedWorkerTerminalFailed, value: ports.AttachedWorkerTerminalMaterialization{Failure: failure}, valid: true},
		{name: "cancelled failure", status: domain.AttachedWorkerTerminalCancelled, value: ports.AttachedWorkerTerminalMaterialization{Failure: cancelled}, valid: true},
		{name: "success as failure", status: domain.AttachedWorkerTerminalSucceeded, value: ports.AttachedWorkerTerminalMaterialization{Failure: failure}},
		{name: "failed as completion", status: domain.AttachedWorkerTerminalFailed, value: ports.AttachedWorkerTerminalMaterialization{Completion: completion}},
		{name: "failed as cancelled", status: domain.AttachedWorkerTerminalFailed, value: ports.AttachedWorkerTerminalMaterialization{Failure: cancelled}},
		{name: "cancelled as failed", status: domain.AttachedWorkerTerminalCancelled, value: ports.AttachedWorkerTerminalMaterialization{Failure: failure}},
		{name: "both", status: domain.AttachedWorkerTerminalSucceeded, value: ports.AttachedWorkerTerminalMaterialization{Completion: completion, Failure: failure}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateAttachedWorkerTerminalMaterializationStatus(test.status, test.value)
			if (err == nil) != test.valid {
				t.Fatalf("validation error = %v, valid=%t", err, test.valid)
			}
		})
	}
}

func TestTerminalMaterializationDigestBindsCanonicalOutput(t *testing.T) {
	t.Parallel()
	completion := ports.AttachedWorkerTerminalMaterialization{Completion: &ports.WorkerCompletion{
		Manifest: domain.ArtifactManifest{ID: "manifest-1"},
		Events:   []domain.SessionEventDraft{{ID: "event-1", Kind: domain.SessionEventAssistantMessage, IdempotencyKey: "idem-1", DisplayText: "answer"}},
	}}
	digest, err := attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalSucceeded, completion)
	if err != nil || digest.Validate() != nil {
		t.Fatalf("canonical digest = %q, %v", digest, err)
	}
	mutated := completion
	mutated.Completion = &ports.WorkerCompletion{Manifest: completion.Completion.Manifest, Events: append([]domain.SessionEventDraft(nil), completion.Completion.Events...)}
	mutated.Completion.Events[0].DisplayText = "different answer"
	other, err := attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalSucceeded, mutated)
	if err != nil {
		t.Fatal(err)
	}
	if other == digest {
		t.Fatal("divergent canonical output retained the signed terminal digest")
	}
	if _, err := attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalFailed, completion); err == nil {
		t.Fatal("status-substituted materialization was accepted")
	}
}

func TestTerminalMaterializationBindingRejectsEveryDivergentLocator(t *testing.T) {
	t.Parallel()
	attempt := domain.AttachedWorkerAttemptV1{
		TenantID: "tenant-1", RunID: "run-1", AttemptID: "attempt-1", ReservationID: "reservation-1",
		LeaseID: "lease-1", LeaseGeneration: 7,
	}
	completion := ports.WorkerCompletion{
		TenantID: attempt.TenantID, RunID: attempt.RunID, AttemptID: attempt.AttemptID, ReservationID: attempt.ReservationID,
		LeaseID: attempt.LeaseID, Fence: attempt.LeaseGeneration,
	}
	if err := validateAttachedWorkerTerminalMaterializationBinding(attempt, ports.AttachedWorkerTerminalMaterialization{Completion: &completion}); err != nil {
		t.Fatalf("exact completion binding rejected: %v", err)
	}
	completionMutations := []func(*ports.WorkerCompletion){
		func(value *ports.WorkerCompletion) { value.TenantID = "tenant-2" },
		func(value *ports.WorkerCompletion) { value.RunID = "run-2" },
		func(value *ports.WorkerCompletion) { value.AttemptID = "attempt-2" },
		func(value *ports.WorkerCompletion) { value.ReservationID = "reservation-2" },
		func(value *ports.WorkerCompletion) { value.LeaseID = "lease-2" },
		func(value *ports.WorkerCompletion) { value.Fence++ },
	}
	for index, mutate := range completionMutations {
		value := completion
		mutate(&value)
		if err := validateAttachedWorkerTerminalMaterializationBinding(attempt, ports.AttachedWorkerTerminalMaterialization{Completion: &value}); err == nil {
			t.Fatalf("completion locator mutation %d accepted", index)
		}
	}

	failure := ports.WorkerFailure{
		TenantID: attempt.TenantID, RunID: attempt.RunID, AttemptID: attempt.AttemptID, ReservationID: attempt.ReservationID,
		LeaseID: attempt.LeaseID, Fence: attempt.LeaseGeneration,
	}
	if err := validateAttachedWorkerTerminalMaterializationBinding(attempt, ports.AttachedWorkerTerminalMaterialization{Failure: &failure}); err != nil {
		t.Fatalf("exact failure binding rejected: %v", err)
	}
	failureMutations := []func(*ports.WorkerFailure){
		func(value *ports.WorkerFailure) { value.TenantID = "tenant-2" },
		func(value *ports.WorkerFailure) { value.RunID = "run-2" },
		func(value *ports.WorkerFailure) { value.AttemptID = "attempt-2" },
		func(value *ports.WorkerFailure) { value.ReservationID = "reservation-2" },
		func(value *ports.WorkerFailure) { value.LeaseID = "lease-2" },
		func(value *ports.WorkerFailure) { value.Fence++ },
	}
	for index, mutate := range failureMutations {
		value := failure
		mutate(&value)
		if err := validateAttachedWorkerTerminalMaterializationBinding(attempt, ports.AttachedWorkerTerminalMaterialization{Failure: &value}); err == nil {
			t.Fatalf("failure locator mutation %d accepted", index)
		}
	}
}

func TestRetiredAttemptBindingRequiresEveryImmutableAuthorityField(t *testing.T) {
	t.Parallel()
	base := domain.AttachedWorkerAttemptV1{
		TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1", ConnectionID: "connection-1",
		RunID: "run-1", AttemptID: "attempt-1", ReservationID: "reservation-1", LeaseID: "lease-1",
		LeaseGeneration: 7, FenceToken: "fence-1", EnrollmentGeneration: 2, ConnectionGeneration: 3,
		ContextDigest: domain.AttachedWorkerContextDigest(strings.Repeat("a", 64)), CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("b", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("c", 64)),
		PlatformAttemptSequence: 4, WorkerAttemptSequence: 5, TerminalSequence: 1,
		TerminalStatus: domain.AttachedWorkerTerminalSucceeded, TerminalEvidenceDigest: domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("d", 64)),
		LeaseExpiresAt: time.Unix(200, 0).UTC(), State: domain.AttachedWorkerAttemptRetired,
	}
	if !sameAttachedWorkerRetiredAttemptBinding(base, base) {
		t.Fatal("exact retired binding rejected")
	}
	mutations := []func(*domain.AttachedWorkerAttemptV1){
		func(value *domain.AttachedWorkerAttemptV1) { value.TenantID = "tenant-2" },
		func(value *domain.AttachedWorkerAttemptV1) { value.OwnerUserID = "owner-2" },
		func(value *domain.AttachedWorkerAttemptV1) { value.WorkerID = "worker-2" },
		func(value *domain.AttachedWorkerAttemptV1) { value.ConnectionID = "connection-2" },
		func(value *domain.AttachedWorkerAttemptV1) { value.RunID = "run-2" },
		func(value *domain.AttachedWorkerAttemptV1) { value.AttemptID = "attempt-2" },
		func(value *domain.AttachedWorkerAttemptV1) { value.ReservationID = "reservation-2" },
		func(value *domain.AttachedWorkerAttemptV1) { value.LeaseID = "lease-2" },
		func(value *domain.AttachedWorkerAttemptV1) { value.LeaseGeneration++ },
		func(value *domain.AttachedWorkerAttemptV1) { value.FenceToken = "fence-2" },
		func(value *domain.AttachedWorkerAttemptV1) { value.EnrollmentGeneration++ },
		func(value *domain.AttachedWorkerAttemptV1) { value.ConnectionGeneration++ },
		func(value *domain.AttachedWorkerAttemptV1) {
			value.ContextDigest = domain.AttachedWorkerContextDigest(strings.Repeat("e", 64))
		},
		func(value *domain.AttachedWorkerAttemptV1) {
			value.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(strings.Repeat("e", 64))
		},
		func(value *domain.AttachedWorkerAttemptV1) {
			value.PolicyDigest = domain.AttachedWorkerPolicyDigest(strings.Repeat("e", 64))
		},
		func(value *domain.AttachedWorkerAttemptV1) { value.PlatformAttemptSequence++ },
		func(value *domain.AttachedWorkerAttemptV1) { value.WorkerAttemptSequence++ },
		func(value *domain.AttachedWorkerAttemptV1) { value.TerminalSequence++ },
		func(value *domain.AttachedWorkerAttemptV1) {
			value.TerminalStatus = domain.AttachedWorkerTerminalFailed
		},
		func(value *domain.AttachedWorkerAttemptV1) {
			value.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("e", 64))
		},
		func(value *domain.AttachedWorkerAttemptV1) {
			value.LeaseExpiresAt = value.LeaseExpiresAt.Add(time.Microsecond)
		},
	}
	for index, mutate := range mutations {
		value := base
		mutate(&value)
		if sameAttachedWorkerRetiredAttemptBinding(value, base) {
			t.Fatalf("retired authority mutation %d accepted", index)
		}
	}
}

func TestCrossedTerminalCannotReplaceDurableCancellation(t *testing.T) {
	t.Parallel()
	base := domain.AttachedWorkerAttemptV1{
		State: domain.AttachedWorkerAttemptCancelRequested, CancelRevision: 1,
		CancelDeadline: time.Unix(200, 0).UTC(),
	}
	crossed := attachedworkerprotocol.TerminalV1{
		TerminalSequence: 1, Status: attachedworkerprotocol.TerminalSucceeded,
		Result: attachedworkerprotocol.TerminalResultCompleted, EvidenceDigest: make([]byte, 32),
	}
	next := base
	if err := applyAttachedWorkerTerminalTransition(&next, base.State, crossed, attachedworkerprotocol.AttemptCancelRequested); err != nil {
		t.Fatal(err)
	}
	if next.State != domain.AttachedWorkerAttemptCancelRequested || next.CancelDeadline != base.CancelDeadline || next.TerminalSequence != 0 {
		t.Fatalf("crossed terminal replaced cancellation: %#v", next)
	}

	base.State = domain.AttachedWorkerAttemptCancelAcknowledged
	next = base
	if err := applyAttachedWorkerTerminalTransition(&next, base.State, crossed, attachedworkerprotocol.AttemptCancelAcked); err != nil {
		t.Fatal(err)
	}
	if next.State != domain.AttachedWorkerAttemptCancelAcknowledged || next.TerminalSequence != 0 {
		t.Fatalf("crossed terminal replaced acknowledged cancellation: %#v", next)
	}

	cancelled := crossed
	cancelled.Status = attachedworkerprotocol.TerminalCancelled
	cancelled.Result = attachedworkerprotocol.TerminalResultCancelled
	next = base
	if err := applyAttachedWorkerTerminalTransition(&next, base.State, cancelled, attachedworkerprotocol.AttemptTerminalPending); err != nil {
		t.Fatal(err)
	}
	if next.State != domain.AttachedWorkerAttemptTerminalPending || next.TerminalStatus != domain.AttachedWorkerTerminalCancelled {
		t.Fatalf("cancelled terminal was not made pending: %#v", next)
	}
}

func TestCancellationDeadlinePlanDoesNotLeavePreClaimPoisonWork(t *testing.T) {
	t.Parallel()
	previous := domain.AttachedWorkerAttemptV1{
		TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1", AttemptID: "attempt-1",
		LeaseGeneration: 4, LeaseExpiresAt: time.Unix(100, 0).UTC(), Revision: 7,
		State: domain.AttachedWorkerAttemptOffered,
	}
	next := previous
	next.State, next.Revision, next.CancelDeadline = domain.AttachedWorkerAttemptCancelledBeforeClaim, 8, time.Unix(90, 0).UTC()
	plan, err := planAttachedWorkerCancellationDeadlines(previous, next)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.DeleteLease || plan.Cancel == nil || plan.Lease.AttemptRevision != previous.Revision ||
		plan.Cancel.AttemptRevision != next.Revision || !plan.Cancel.DeadlineAt.Equal(next.CancelDeadline) {
		t.Fatalf("pre-claim cancellation plan = %#v", plan)
	}
	if _, err := retireUnacknowledgedPreClaimCancellation(next, next.CancelDeadline.Add(-time.Microsecond)); err == nil {
		t.Fatal("pre-claim cancellation retired before deadline")
	}
	retired, err := retireUnacknowledgedPreClaimCancellation(next, next.CancelDeadline)
	if err != nil || retired.State != domain.AttachedWorkerAttemptRetired || retired.Revision != next.Revision+1 {
		t.Fatalf("pre-claim cancellation retirement = %#v, %v", retired, err)
	}

	previous.State = domain.AttachedWorkerAttemptClaimed
	next.State = domain.AttachedWorkerAttemptCancelRequested
	plan, err = planAttachedWorkerCancellationDeadlines(previous, next)
	if err != nil {
		t.Fatal(err)
	}
	if plan.DeleteLease || plan.Cancel == nil || plan.Lease.AttemptRevision != next.Revision || plan.Cancel.AttemptRevision != next.Revision {
		t.Fatalf("claimed cancellation plan = %#v", plan)
	}
}

func TestDeadlineCursorRepresentsEveryRawPhysicalKey(t *testing.T) {
	t.Parallel()
	if err := validateAttachedWorkerAttemptDeadlineCursor(ports.AttachedWorkerAttemptDeadlineCursor{Present: true}); err != nil {
		t.Fatalf("present all-zero raw cursor rejected: %v", err)
	}
	if err := validateAttachedWorkerAttemptDeadlineCursor(ports.AttachedWorkerAttemptDeadlineCursor{TenantID: "raw"}); err == nil {
		t.Fatal("absent cursor with routing data accepted")
	}
}

func TestCancellationFrameExpiryIsExclusiveAndStoreAuthoritative(t *testing.T) {
	t.Parallel()
	deadline := time.Unix(200, 0).UTC()
	attempt := domain.AttachedWorkerAttemptV1{State: domain.AttachedWorkerAttemptCancelRequested, CancelDeadline: deadline}
	if attachedWorkerCancellationFrameExpired(attempt, deadline.Add(-time.Microsecond)) {
		t.Fatal("frame expired before acknowledgement deadline")
	}
	if !attachedWorkerCancellationFrameExpired(attempt, deadline) {
		t.Fatal("frame accepted at exclusive acknowledgement deadline")
	}
	attempt.State = domain.AttachedWorkerAttemptCancelAcknowledged
	if attachedWorkerCancellationFrameExpired(attempt, deadline) {
		t.Fatal("acknowledged cancellation retained obsolete acknowledgement deadline")
	}
}

func TestPreClaimCancelAckPreservesKnownNoExecutionState(t *testing.T) {
	t.Parallel()
	if got := attachedWorkerStateAfterCancelAck(domain.AttachedWorkerAttemptCancelledBeforeClaim); got != domain.AttachedWorkerAttemptCancelledBeforeClaim {
		t.Fatalf("pre-claim cancel ack state = %s", got)
	}
	if got := attachedWorkerStateAfterCancelAck(domain.AttachedWorkerAttemptCancelRequested); got != domain.AttachedWorkerAttemptCancelAcknowledged {
		t.Fatalf("claimed cancel ack state = %s", got)
	}
}

func TestCancellationReplaySurvivesLaterCompatibleStates(t *testing.T) {
	t.Parallel()
	for _, state := range []domain.AttachedWorkerAttemptState{
		domain.AttachedWorkerAttemptCancelRequested,
		domain.AttachedWorkerAttemptCancelledBeforeClaim,
		domain.AttachedWorkerAttemptCancelAcknowledged,
		domain.AttachedWorkerAttemptTerminalPending,
		domain.AttachedWorkerAttemptTerminalCommitted,
		domain.AttachedWorkerAttemptFencedUnknown,
		domain.AttachedWorkerAttemptRetired,
	} {
		if !attachedWorkerCancellationReplayState(state) {
			t.Fatalf("cancellation replay rejected for %s", state)
		}
	}
	for _, state := range []domain.AttachedWorkerAttemptState{
		domain.AttachedWorkerAttemptOffered,
		domain.AttachedWorkerAttemptClaimed,
	} {
		if attachedWorkerCancellationReplayState(state) {
			t.Fatalf("non-cancelled state accepted replay for %s", state)
		}
	}
}

func TestProgressRemainsAdvisoryAcrossCancellation(t *testing.T) {
	t.Parallel()
	for _, state := range []domain.AttachedWorkerAttemptState{
		domain.AttachedWorkerAttemptClaimed,
		domain.AttachedWorkerAttemptCancelRequested,
		domain.AttachedWorkerAttemptCancelAcknowledged,
	} {
		if !attachedWorkerProgressAllowed(state) {
			t.Fatalf("progress rejected for %s", state)
		}
	}
	for _, state := range []domain.AttachedWorkerAttemptState{
		domain.AttachedWorkerAttemptOffered,
		domain.AttachedWorkerAttemptTerminalPending,
		domain.AttachedWorkerAttemptFencedUnknown,
	} {
		if attachedWorkerProgressAllowed(state) {
			t.Fatalf("progress accepted for %s", state)
		}
	}
}

func TestLeaseClaimReplayReturnsOriginalAcceptanceAfterLaterFrames(t *testing.T) {
	t.Parallel()
	inbound := domain.AttachedWorkerAttemptMessageV1{Kind: domain.AttachedWorkerAttemptMessageLeaseClaim, AttemptSequence: 1}
	if got := attachedWorkerReplayOutboundSequence(inbound, 7); got != 2 {
		t.Fatalf("claim replay outbound sequence = %d", got)
	}
	inbound.Kind = domain.AttachedWorkerAttemptMessageTerminal
	if got := attachedWorkerReplayOutboundSequence(inbound, 7); got != 7 {
		t.Fatalf("terminal replay outbound sequence = %d", got)
	}
}

func TestTerminalAckObservationRetiresAttemptForNextOffer(t *testing.T) {
	t.Parallel()
	attempt := domain.AttachedWorkerAttemptV1{
		State: domain.AttachedWorkerAttemptTerminalCommitted, Revision: 9,
		UpdatedAt: time.Unix(100, 0).UTC(), TerminalEvidenceDigest: "signed-evidence",
	}
	next, err := retireAttachedWorkerCommittedAttempt(attempt, time.Unix(101, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if next.State != domain.AttachedWorkerAttemptRetired || next.Revision != 10 || next.TerminalEvidenceDigest != attempt.TerminalEvidenceDigest {
		t.Fatalf("retired attempt = %#v", next)
	}
	if _, err := retireAttachedWorkerCommittedAttempt(next, time.Unix(102, 0).UTC()); err == nil {
		t.Fatal("attempt retired twice")
	}
}

func TestExistingCancelIsFencedWithoutDivergentProtocolFrame(t *testing.T) {
	t.Parallel()
	if !fenceAttachedWorkerAttemptWithoutProtocolMutation(domain.AttachedWorkerAttemptCancelRequested) {
		t.Fatal("committed CancelRequested was not recognized")
	}
	if !fenceAttachedWorkerAttemptWithoutProtocolMutation(domain.AttachedWorkerAttemptCancelAcknowledged) {
		t.Fatal("acknowledged cancellation was not recognized")
	}
	for _, state := range []domain.AttachedWorkerAttemptState{
		domain.AttachedWorkerAttemptOffered,
		domain.AttachedWorkerAttemptClaimed,
		domain.AttachedWorkerAttemptTerminalPending,
	} {
		if fenceAttachedWorkerAttemptWithoutProtocolMutation(state) {
			t.Fatalf("state %q incorrectly bypassed the protocol fence transition", state)
		}
	}
	attempt := domain.AttachedWorkerAttemptV1{
		State: domain.AttachedWorkerAttemptCancelRequested, Revision: 9,
		PlatformAttemptSequence: 4, CancelRevision: 1,
	}
	next, err := fenceAttachedWorkerCommittedCancel(attempt, time.Unix(123, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if next.State != domain.AttachedWorkerAttemptFencedUnknown || next.Revision != 10 ||
		next.PlatformAttemptSequence != attempt.PlatformAttemptSequence || next.CancelRevision != attempt.CancelRevision {
		t.Fatalf("fenced committed cancel = %#v", next)
	}
	attempt.State = domain.AttachedWorkerAttemptCancelAcknowledged
	if next, err = fenceAttachedWorkerCommittedCancel(attempt, time.Unix(124, 0).UTC()); err != nil ||
		next.State != domain.AttachedWorkerAttemptFencedUnknown || next.CancelRevision != attempt.CancelRevision {
		t.Fatalf("fenced acknowledged cancel = %#v, %v", next, err)
	}
}

func TestAttachedWorkerPollAuthorityAndPendingSelection(t *testing.T) {
	t.Parallel()
	at := time.Unix(100, 0).UTC()
	secret := domain.DigestAttachedWorkerConnectionSecret([]byte("poll-secret"))
	request := ports.AttachedWorkerAttemptPoll{
		TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1",
		ConnectionID: "connection-1", PresentedSecretDigest: secret,
	}
	worker := domain.AttachedWorker{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, ID: request.WorkerID,
		EnrollmentGeneration: 2, ConnectionGeneration: 3, DesiredState: domain.AttachedWorkerDesiredActive,
	}
	connection := domain.AttachedWorkerConnection{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		ID: request.ConnectionID, EnrollmentGeneration: 2, ConnectionGeneration: 3,
		SecretDigest: secret, State: domain.AttachedWorkerConnectionOnline,
		AuthExpiresAt: at.Add(time.Hour), PresenceExpiresAt: at.Add(time.Minute),
	}
	if !attachedWorkerAttemptPollAuthorized(request, at, worker, connection) {
		t.Fatal("current authorized poll was denied")
	}
	wrongBearer := request
	wrongBearer.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("wrong"))
	if attachedWorkerAttemptPollAuthorized(wrongBearer, at, worker, connection) {
		t.Fatal("wrong bearer poll was authorized")
	}
	revoked := worker
	revoked.DesiredState = domain.AttachedWorkerDesiredRevoked
	if attachedWorkerAttemptPollAuthorized(request, at, revoked, connection) {
		t.Fatal("revoked worker poll was authorized")
	}
	stale := connection
	stale.ConnectionGeneration++
	if attachedWorkerAttemptPollAuthorized(request, at, worker, stale) {
		t.Fatal("stale connection generation was authorized")
	}

	want := map[domain.AttachedWorkerAttemptState]domain.AttachedWorkerAttemptMessageKind{
		domain.AttachedWorkerAttemptOffered:              domain.AttachedWorkerAttemptMessageLeaseOffered,
		domain.AttachedWorkerAttemptCancelRequested:      domain.AttachedWorkerAttemptMessageCancelRequested,
		domain.AttachedWorkerAttemptCancelledBeforeClaim: domain.AttachedWorkerAttemptMessageCancelRequested,
		domain.AttachedWorkerAttemptFencedUnknown:        domain.AttachedWorkerAttemptMessageCancelRequested,
		domain.AttachedWorkerAttemptTerminalCommitted:    domain.AttachedWorkerAttemptMessageTerminalCommitted,
		domain.AttachedWorkerAttemptClaimed:              "",
	}
	for state, kind := range want {
		if got := pendingAttachedWorkerAttemptMessageKind(state); got != kind {
			t.Errorf("state %q pending kind = %q, want %q", state, got, kind)
		}
	}
	if !attachedWorkerPollFramePending(4, 5) || attachedWorkerPollFramePending(5, 5) || attachedWorkerPollFramePending(6, 5) {
		t.Fatal("poll frame ACK suppression is not monotonic")
	}
}

func TestMalformedDurableAttemptMessageConflicts(t *testing.T) {
	t.Parallel()
	message := domain.AttachedWorkerAttemptMessageV1{
		Version:  domain.AttachedWorkerAttemptMessageVersionV1,
		TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1", AttemptID: "attempt-1",
		Direction: domain.AttachedWorkerAttemptPlatformToWorker, AttemptSequence: 1,
		ConnectionGeneration: 1, EnvelopeSequence: 1, Kind: domain.AttachedWorkerAttemptMessageLeaseOffered,
		Fingerprint: domain.AttachedWorkerAttemptMessageFingerprint(strings.Repeat("a", 64)),
		Payload:     []byte(`{"not":"a canonical batch"}`), CreatedAt: time.Unix(1, 0).UTC(),
	}
	if _, _, err := decodeAttachedWorkerAttemptFrame(message); err == nil {
		t.Fatal("malformed durable attempt message was accepted")
	}
}
