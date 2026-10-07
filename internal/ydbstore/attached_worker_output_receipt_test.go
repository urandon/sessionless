package ydbstore

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestAttachedWorkerOutputReceiptStatusAdmissible(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, time.October, 7, 9, 0, 0, 0, time.UTC)
	attempt := domain.AttachedWorkerAttemptV1{
		State:          domain.AttachedWorkerAttemptClaimed,
		LeaseExpiresAt: base.Add(time.Minute),
	}
	for _, tc := range []struct {
		name           string
		state          domain.AttachedWorkerAttemptState
		status         domain.AttachedWorkerTerminalStatus
		cancelRevision uint64
		cancelDeadline time.Time
		at             time.Time
		want           bool
	}{
		{name: "claimed success", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded, at: base, want: true},
		{name: "claimed failure", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalFailed, at: base, want: true},
		{name: "claimed cancelled", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalCancelled, at: base},
		{name: "claimed invalid status", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalStatus("unknown"), at: base},
		{name: "claimed at zero time", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded},
		{name: "claimed at lease expiry", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded, at: base.Add(time.Minute)},
		{name: "claimed with cancel revision", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded, cancelRevision: 1, at: base},
		{name: "claimed with cancel deadline", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded, cancelDeadline: base.Add(10 * time.Second), at: base},
		{name: "cancel requested before ack deadline", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(5 * time.Second), want: true},
		{name: "cancel requested at ack deadline", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(10 * time.Second)},
		{name: "cancel requested without revision", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalCancelled, cancelDeadline: base.Add(10 * time.Second), at: base.Add(5 * time.Second)},
		{name: "cancel requested without deadline", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, at: base.Add(5 * time.Second)},
		{name: "cancel requested cannot succeed", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalSucceeded, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(5 * time.Second)},
		{name: "cancel acknowledged after ack deadline", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(30 * time.Second), want: true},
		{name: "cancel acknowledged cannot succeed", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalSucceeded, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(30 * time.Second)},
		{name: "cancel acknowledged without revision", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalCancelled, cancelDeadline: base.Add(10 * time.Second), at: base.Add(30 * time.Second)},
		{name: "cancel acknowledged without deadline", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, at: base.Add(30 * time.Second)},
		{name: "expired lease", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(time.Minute)},
		{name: "fenced", state: domain.AttachedWorkerAttemptFencedUnknown, status: domain.AttachedWorkerTerminalFailed, at: base},
		{name: "terminal pending", state: domain.AttachedWorkerAttemptTerminalPending, status: domain.AttachedWorkerTerminalSucceeded, at: base},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := attempt
			candidate.State = tc.state
			candidate.CancelRevision = tc.cancelRevision
			candidate.CancelDeadline = tc.cancelDeadline
			if got := attachedWorkerOutputReceiptStatusAdmissible(candidate, tc.status, tc.at); got != tc.want {
				t.Errorf("receipt admissible state=%s status=%s at=%s: got %t, want %t", tc.state, tc.status, tc.at.Format(time.RFC3339), got, tc.want)
			}
		})
	}
}

func TestAttachedWorkerOutputReceiptRequiresCurrentBearerAndExactOwnerHead(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.October, 7, 10, 0, 0, 0, time.UTC)
	secret := domain.DigestAttachedWorkerConnectionSecret([]byte("receipt-secret"))
	capability := domain.DigestAttachedWorkerCapability([]byte("receipt-capability"))
	contextDigest := domain.AttachedWorkerContextDigest(domain.DigestAttachedWorkerCapability([]byte("receipt-context")))
	policy := domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("receipt-policy")))
	fence, err := domain.NewAttachedWorkerFenceTokenV1("tenant-a", "owner-a", "worker-collision", "run-a", "attempt-a", "lease-a", 5)
	if err != nil {
		t.Fatal(err)
	}
	request := ports.AttachedWorkerSealedInputAuthorization{
		TenantID: "tenant-a", OwnerUserID: "owner-a", WorkerID: "worker-collision",
		ConnectionID: "connection-a", PresentedSecretDigest: secret,
		EnrollmentGeneration: 2, ConnectionGeneration: 3,
		RunID: "run-a", AttemptID: "attempt-a", AttemptSequence: 1,
		LeaseID: "lease-a", LeaseGeneration: 5, FenceToken: fence,
		LeaseExpiresAtUnixMicro: at.Add(time.Minute).UnixMicro(),
		ContextDigest:           contextDigest, CapabilityDigest: capability, PolicyDigest: policy,
	}
	worker := domain.AttachedWorker{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, ID: request.WorkerID,
		DesiredState:         domain.AttachedWorkerDesiredActive,
		EnrollmentGeneration: 2, ConnectionGeneration: 3,
	}
	connection := domain.AttachedWorkerConnection{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		ID: request.ConnectionID, SecretDigest: secret, State: domain.AttachedWorkerConnectionOnline,
		EnrollmentGeneration: 2, ConnectionGeneration: 3,
		AuthExpiresAt: at.Add(time.Hour), PresenceExpiresAt: at.Add(time.Minute),
	}
	attempt := domain.AttachedWorkerAttemptV1{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		ConnectionID: request.ConnectionID, EnrollmentGeneration: 2, ConnectionGeneration: 3,
		ExecutionConnectionID: request.ConnectionID, ExecutionConnectionGeneration: 3,
		RunID: request.RunID, AttemptID: request.AttemptID, LeaseID: request.LeaseID,
		LeaseGeneration: 5, FenceToken: fence, LeaseExpiresAt: at.Add(time.Minute),
		ContextDigest: contextDigest, CapabilityDigest: capability, PolicyDigest: policy,
		State: domain.AttachedWorkerAttemptClaimed, Revision: 7,
	}
	if err := validateAttachedWorkerSealedInputAuthorization(request); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		status domain.AttachedWorkerTerminalStatus
		change func(*ports.AttachedWorkerSealedInputAuthorization, *domain.AttachedWorker, *domain.AttachedWorkerConnection, *domain.AttachedWorkerAttemptV1)
		want   bool
	}{
		{name: "current success", status: domain.AttachedWorkerTerminalSucceeded, want: true},
		{name: "foreign owner", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			r.OwnerUserID = "owner-b"
		}},
		{name: "wrong bearer", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			r.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("stolen"))
		}},
		{name: "rotated connection", status: domain.AttachedWorkerTerminalSucceeded, change: func(_ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, c *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			c.ID = "connection-b"
		}},
		{name: "rebound current connection cannot originate output", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, w *domain.AttachedWorker, c *domain.AttachedWorkerConnection, a *domain.AttachedWorkerAttemptV1) {
			r.ConnectionID, c.ID, a.ConnectionID = "connection-b", "connection-b", "connection-b"
			r.ConnectionGeneration, w.ConnectionGeneration, c.ConnectionGeneration, a.ConnectionGeneration = 4, 4, 4, 4
		}},
		{name: "stale generation", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			r.ConnectionGeneration++
		}},
		{name: "stale fence", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			r.LeaseGeneration++
		}},
		{name: "capability bait and switch", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			r.CapabilityDigest = domain.DigestAttachedWorkerCapability([]byte("changed"))
		}},
		{name: "revoked", status: domain.AttachedWorkerTerminalSucceeded, change: func(_ *ports.AttachedWorkerSealedInputAuthorization, w *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			w.DesiredState = domain.AttachedWorkerDesiredRevoked
		}},
		{name: "cancel cannot become success", status: domain.AttachedWorkerTerminalSucceeded, change: func(_ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, a *domain.AttachedWorkerAttemptV1) {
			a.State, a.CancelRevision, a.CancelDeadline = domain.AttachedWorkerAttemptCancelAcknowledged, 1, at.Add(time.Second)
		}},
		{name: "cancelled after acknowledged deadline", status: domain.AttachedWorkerTerminalCancelled, change: func(_ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, a *domain.AttachedWorkerAttemptV1) {
			a.State, a.CancelRevision, a.CancelDeadline = domain.AttachedWorkerAttemptCancelAcknowledged, 1, at.Add(-time.Second)
		}, want: true},
		{name: "fenced", status: domain.AttachedWorkerTerminalFailed, change: func(_ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, a *domain.AttachedWorkerAttemptV1) {
			a.State = domain.AttachedWorkerAttemptFencedUnknown
		}},
		{name: "attempt changed", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, a *domain.AttachedWorkerAttemptV1) {
			r.ExpectedAttemptRevision = a.Revision
			a.Revision++
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r, w, c, a := request, worker, connection, attempt
			if tc.change != nil {
				tc.change(&r, &w, &c, &a)
			}
			if got := attachedWorkerOutputReceiptAuthorized(r, tc.status, at, w, c, a); got != tc.want {
				t.Errorf("receipt authority status=%s state=%s revision=%d: got %t, want %t", tc.status, a.State, a.Revision, got, tc.want)
			}
		})
	}
}
