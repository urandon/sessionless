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
	if !plan.DeleteLease || plan.Cancel != nil || plan.Lease.AttemptRevision != previous.Revision {
		t.Fatalf("pre-claim cancellation plan = %#v", plan)
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

func TestExistingCancelIsFencedWithoutDivergentProtocolFrame(t *testing.T) {
	t.Parallel()
	if !fenceAttachedWorkerAttemptWithoutProtocolMutation(domain.AttachedWorkerAttemptCancelRequested) {
		t.Fatal("committed CancelRequested was not recognized")
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
