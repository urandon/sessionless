package runexplanation_test

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
)

func attachedFixture(t *testing.T) runexplanation.Snapshot {
	t.Helper()
	s := fixture(t, true)
	at := s.ReadAt
	fence, err := domain.NewAttachedWorkerFenceTokenV1(s.Run.TenantID, s.Head.Placement.OwnerUserID, s.Head.Placement.WorkerID, s.Run.ID, s.Attempt.ID, "lease-private", 7)
	if err != nil {
		t.Fatalf("fixture fence: %v", err)
	}
	s.Lease = &domain.Lease{ID: "lease-private", TenantID: s.Run.TenantID, RunID: s.Run.ID, AttemptID: s.Attempt.ID, WorkerID: string(s.Head.Placement.WorkerID), FenceToken: 7, AcquiredAt: at, ExpiresAt: at.Add(time.Minute)}
	s.Attached = &domain.AttachedWorkerAttemptV1{Version: domain.AttachedWorkerAttemptVersionV1, TenantID: s.Run.TenantID, OwnerUserID: s.Head.Placement.OwnerUserID, WorkerID: s.Head.Placement.WorkerID, ConnectionID: "connection-private", RunID: s.Run.ID, AttemptID: s.Attempt.ID, ReservationID: "reservation-private", LeaseID: s.Lease.ID, LeaseGeneration: 7, FenceToken: fence, EnrollmentGeneration: 2, ConnectionGeneration: 3, ContextDigest: domain.AttachedWorkerContextDigest(domain.DigestAttachedWorkerCapability([]byte("context"))), CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("capability")), PolicyDigest: domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy"))), State: domain.AttachedWorkerAttemptOffered, PlatformAttemptSequence: 1, LeaseExpiresAt: s.Lease.ExpiresAt, CreatedAt: at, UpdatedAt: at, Revision: 1}
	s.Worker = &domain.AttachedWorker{TenantID: s.Run.TenantID, OwnerUserID: s.Head.Placement.OwnerUserID, ID: s.Head.Placement.WorkerID, EnrollmentGeneration: 2, ConnectionGeneration: 3, DesiredState: domain.AttachedWorkerDesiredActive, ObservedState: domain.AttachedWorkerObservedOnline, Revision: 1, CreatedAt: at, UpdatedAt: at}
	s.Connection = &domain.AttachedWorkerConnection{TenantID: s.Run.TenantID, OwnerUserID: s.Head.Placement.OwnerUserID, WorkerID: s.Head.Placement.WorkerID, ID: s.Attached.ConnectionID, EnrollmentGeneration: 2, ConnectionGeneration: 3, State: domain.AttachedWorkerConnectionOnline, Revision: 1, ConnectedAt: at}
	if err := s.Attached.Validate(); err != nil {
		t.Fatalf("fixture attached head: %v", err)
	}
	return s
}

func TestAttachedLeaseBoundaryAndDurableReceipts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		delta time.Duration
		want  runexplanation.Freshness
	}{{"before", time.Minute - time.Nanosecond, runexplanation.WithinLease}, {"at", time.Minute, runexplanation.Expired}, {"after", time.Minute + time.Nanosecond, runexplanation.Expired}} {
		t.Run(tc.name, func(t *testing.T) {
			s := attachedFixture(t)
			s.ReadAt = s.ReadAt.Add(tc.delta)
			got := projection(t, s).Attached
			if got.Availability != runexplanation.Recorded || got.Freshness != tc.want || got.ValidUntil == nil || !got.ValidUntil.Equal(s.Lease.ExpiresAt) {
				t.Errorf("read=%v expiry=%v got=%+v want=%s", s.ReadAt, s.Lease.ExpiresAt, got, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		state domain.AttachedWorkerAttemptState
		fact  runexplanation.AttachedFact
	}{{"fenced", domain.AttachedWorkerAttemptFencedUnknown, runexplanation.FencedOutcomeUnknown}, {"retired", domain.AttachedWorkerAttemptRetired, runexplanation.ReceiptRetired}, {"committed", domain.AttachedWorkerAttemptTerminalCommitted, runexplanation.TerminalCommitRecorded}} {
		t.Run(tc.name, func(t *testing.T) {
			s := attachedFixture(t)
			s.Attached.State = tc.state
			s.Attached.WorkerAttemptSequence = 1
			if tc.state != domain.AttachedWorkerAttemptFencedUnknown {
				s.Attached.TerminalSequence = 1
				s.Attached.TerminalStatus = domain.AttachedWorkerTerminalFailed
				s.Attached.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(domain.DigestAttachedWorkerCapability([]byte("terminal")))
			}
			if tc.state == domain.AttachedWorkerAttemptTerminalCommitted {
				transition(t, &s, domain.RunFailed, time.Second)
				if err := s.Attempt.Transition(domain.AttemptFailed, s.ReadAt); err != nil {
					t.Fatal(err)
				}
				s.Attached.UpdatedAt = s.ReadAt
			}
			s.ReadAt = s.ReadAt.Add(24 * time.Hour)
			got := projection(t, s).Attached
			if got.Availability != runexplanation.Recorded || got.Fact != tc.fact || got.Freshness != runexplanation.Durable || got.ValidUntil != nil {
				t.Errorf("old receipt got=%+v want fact=%s durable without expiry", got, tc.fact)
			}
		})
	}
}

func TestAttachedExactJoinAndUnsupportedStates(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*runexplanation.Snapshot)
	}{
		{"missing_lease", func(s *runexplanation.Snapshot) { s.Lease = nil }},
		{"missing_current_worker", func(s *runexplanation.Snapshot) { s.Worker = nil }},
		{"missing_current_connection", func(s *runexplanation.Snapshot) { s.Connection = nil }},
		{"replacement_run", func(s *runexplanation.Snapshot) { s.Attached.RunID = "replacement-run" }},
		{"replacement_attempt", func(s *runexplanation.Snapshot) { s.Attached.AttemptID = "replacement-attempt" }},
		{"different_lease", func(s *runexplanation.Snapshot) { s.Lease.ID = "different-lease" }},
		{"numeric_generation", func(s *runexplanation.Snapshot) { s.Lease.FenceToken++ }},
		{"lease_expiry", func(s *runexplanation.Snapshot) { s.Lease.ExpiresAt = s.Lease.ExpiresAt.Add(time.Second) }},
		{"lease_worker", func(s *runexplanation.Snapshot) { s.Lease.WorkerID = "different-worker" }},
		{"current_enrollment", func(s *runexplanation.Snapshot) { s.Worker.EnrollmentGeneration++ }},
		{"current_connection_generation", func(s *runexplanation.Snapshot) { s.Worker.ConnectionGeneration++ }},
		{"connection_binding", func(s *runexplanation.Snapshot) { s.Connection.ID = "replacement-connection" }},
		{"connection_enrollment", func(s *runexplanation.Snapshot) { s.Connection.EnrollmentGeneration++ }},
		{"connection_generation", func(s *runexplanation.Snapshot) { s.Connection.ConnectionGeneration++ }},
		{"foreign_owner", func(s *runexplanation.Snapshot) { s.Connection.OwnerUserID = "replacement-owner" }},
		{"future_observation", func(s *runexplanation.Snapshot) { s.Attached.UpdatedAt = s.ReadAt.Add(time.Second) }},
		{"future_worker", func(s *runexplanation.Snapshot) { s.Worker.UpdatedAt = s.ReadAt.Add(time.Second) }},
		{"future_connection", func(s *runexplanation.Snapshot) { s.Connection.ConnectedAt = s.ReadAt.Add(time.Second) }},
		{"cancel_requested", func(s *runexplanation.Snapshot) {
			s.Attached.State = domain.AttachedWorkerAttemptCancelRequested
			s.Attached.WorkerAttemptSequence = 1
			s.Attached.CancelRevision = 1
			s.Attached.CancelDeadline = s.ReadAt.Add(time.Second)
		}},
		{"future_state", func(s *runexplanation.Snapshot) { s.Attached.State = "future-private-state" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := attachedFixture(t)
			tc.mutate(&s)
			got := projection(t, s)
			if got.Attached != (runexplanation.AttachedObservation{Availability: runexplanation.Unknown}) || got.Coverage.Operational != runexplanation.Unknown || got.Attempt.Availability != runexplanation.Recorded {
				t.Errorf("mismatch must preserve canonical Attempt and omit operational evidence: got=%+v", got)
			}
		})
	}
}

func TestAttachedSupportedLeaseFactsAndManagedUnknown(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		state domain.AttachedWorkerAttemptState
		fact  runexplanation.AttachedFact
	}{{"offered", domain.AttachedWorkerAttemptOffered, runexplanation.OfferRecorded}, {"claimed", domain.AttachedWorkerAttemptClaimed, runexplanation.ClaimRecorded}, {"pending", domain.AttachedWorkerAttemptTerminalPending, runexplanation.TerminalCandidateRecorded}} {
		t.Run(tc.name, func(t *testing.T) {
			s := attachedFixture(t)
			s.Attached.State = tc.state
			if tc.state != domain.AttachedWorkerAttemptOffered {
				s.Attached.WorkerAttemptSequence = 1
			}
			if tc.state == domain.AttachedWorkerAttemptTerminalPending {
				s.Attached.TerminalSequence = 1
				s.Attached.TerminalStatus = domain.AttachedWorkerTerminalFailed
				s.Attached.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(domain.DigestAttachedWorkerCapability([]byte("terminal")))
			}
			got := projection(t, s).Attached
			if got.Fact != tc.fact || got.Freshness != runexplanation.WithinLease {
				t.Errorf("state=%s got=%+v want=%s", tc.state, got, tc.fact)
			}
		})
	}
	s := attachedFixture(t)
	s.Head.Placement = runexplanation.PlacementSelector{Kind: domain.ExecutionPlacementManaged}
	if got := projection(t, s).Attached; got.Availability != runexplanation.Unknown || got.Fact != "" {
		t.Errorf("managed remote acceptance inferred got=%+v", got)
	}
}
