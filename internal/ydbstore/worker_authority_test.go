package ydbstore

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func currentWorkerAuthorityFixture(t *testing.T) ports.WorkerJobState {
	t.Helper()
	at := time.Unix(20, 0).UTC()
	managed, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2("tenant-test", "user-test", "run-test", "attempt-test", "subscription-test", at)
	if err != nil {
		t.Fatal(err)
	}
	run := domain.Run{ID: "run-test", TenantID: "tenant-test", SessionID: "session-test", TriggerEventID: "event-test", SubscriptionConnectionID: "subscription-test", Status: domain.RunRunning, IdempotencyKey: "idempotency-test", CreatedAt: at, UpdatedAt: at}
	job := domain.WorkerJob{
		TenantID: run.TenantID, RunID: run.ID, SessionID: run.SessionID, TriggerEventID: run.TriggerEventID,
		AttemptID: "attempt-test", ReservationID: "reservation-test", InputManifestID: "manifest-test",
		CredentialOwnerUserID: "user-test", ExecutionPlacementV2: managed.ExecutionPlacementV2,
		HarnessBinding: managed.HarnessBinding, SubstrateBinding: &managed.SubstrateBinding, AdmissionCostCeiling: &managed.AdmissionCostCeiling,
		ContextSnapshot: domain.BlobRef{TenantID: run.TenantID, Key: domain.SessionObjectPrefix(run.TenantID, run.SessionID) + "context", SHA256: strings.Repeat("a", 64), Size: 10},
		Limits:          domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxArtifacts: 8},
		DeliveryChat:    domain.TelegramChatRef{TenantID: run.TenantID, ChatID: 1}, ReplyToMessageID: 1, CreatedAt: at,
	}
	state := ports.WorkerJobState{
		Run: run, Job: job,
		Attempt:       domain.Attempt{ID: job.AttemptID, TenantID: run.TenantID, RunID: run.ID, Number: 1, Status: domain.AttemptRunning, WorkerID: "worker-test", CreatedAt: at, UpdatedAt: at},
		Reservation:   domain.QuotaReservation{ID: job.ReservationID, TenantID: run.TenantID, RunID: run.ID, SubscriptionConnectionID: run.SubscriptionConnectionID, Status: domain.ReservationHeld, CapacityUnits: 1, HeldAt: at, ExpiresAt: at.Add(time.Hour), UpdatedAt: at},
		InputManifest: domain.ArtifactManifest{ID: job.InputManifestID, TenantID: run.TenantID, RunID: run.ID, CreatedAt: at},
	}
	if err := job.ValidateForRun(run); err != nil {
		t.Fatalf("complete fixture job: %v", err)
	}
	if err := validateLoadedWorkerJob(state); err != nil {
		t.Fatalf("complete fixture aggregate: %v", err)
	}
	return state
}

// This exercises the production aggregate validator used by both public load
// and same-transaction effect admission, not a duplicate test-only validator.
func TestLoadedWorkerJobBindsCanonicalRunAndExecutionAuthority(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*ports.WorkerJobState)
	}{
		{"job_tenant", func(s *ports.WorkerJobState) { s.Job.TenantID = "tenant-other" }},
		{"job_run", func(s *ports.WorkerJobState) { s.Job.RunID = "run-other" }},
		{"job_session", func(s *ports.WorkerJobState) { s.Job.SessionID = "session-other" }},
		{"job_trigger", func(s *ports.WorkerJobState) { s.Job.TriggerEventID = "event-other" }},
		{"credential_owner", func(s *ports.WorkerJobState) { s.Job.CredentialOwnerUserID = "user-other" }},
		{"placement_scope", func(s *ports.WorkerJobState) {
			s.Job.ExecutionPlacementV2.SubstrateBindingDigest = strings.Repeat("b", 64)
		}},
		{"harness_scope", func(s *ports.WorkerJobState) { s.Job.HarnessBinding.AttemptID = "attempt-other" }},
		{"admitted_cost", func(s *ports.WorkerJobState) { s.Job.AdmissionCostCeiling.PriceRevision = "price-other" }},
		{"context_tenant", func(s *ports.WorkerJobState) { s.Job.ContextSnapshot.TenantID = "tenant-other" }},
		{"job_created_before_run", func(s *ports.WorkerJobState) { s.Job.CreatedAt = s.Run.CreatedAt.Add(-time.Second) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := currentWorkerAuthorityFixture(t)
			c.mutate(&s)
			if err := validateLoadedWorkerJob(s); err == nil {
				t.Fatalf("case=%s: inconsistent durable job accepted for Run=%s Session=%s", c.name, s.Run.ID, s.Run.SessionID)
			}
		})
	}
}

func TestCurrentServerlessLeaseRequiresExactCanonicalWorkerAndClock(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*ports.WorkerJobState, *domain.Lease, *ports.ReserveAttemptEffectRequestV1, *time.Time)
	}{
		{"valid", func(*ports.WorkerJobState, *domain.Lease, *ports.ReserveAttemptEffectRequestV1, *time.Time) {}},
		{"wrong_worker", func(_ *ports.WorkerJobState, l *domain.Lease, _ *ports.ReserveAttemptEffectRequestV1, _ *time.Time) {
			l.WorkerID = "worker-other"
		}},
		{"missing_attempt_worker", func(s *ports.WorkerJobState, _ *domain.Lease, _ *ports.ReserveAttemptEffectRequestV1, _ *time.Time) {
			s.Attempt.WorkerID = ""
		}},
		{"foreign_tenant_request", func(_ *ports.WorkerJobState, _ *domain.Lease, r *ports.ReserveAttemptEffectRequestV1, _ *time.Time) {
			r.TenantID = "tenant-other"
		}},
		{"foreign_run_request", func(_ *ports.WorkerJobState, _ *domain.Lease, r *ports.ReserveAttemptEffectRequestV1, _ *time.Time) {
			r.RunID = "run-other"
		}},
		{"foreign_attempt_request", func(_ *ports.WorkerJobState, _ *domain.Lease, r *ports.ReserveAttemptEffectRequestV1, _ *time.Time) {
			r.AttemptID = "attempt-other"
		}},
		{"different_lease", func(_ *ports.WorkerJobState, _ *domain.Lease, r *ports.ReserveAttemptEffectRequestV1, _ *time.Time) {
			r.LeaseID = "lease-other"
		}},
		{"stale_fence", func(_ *ports.WorkerJobState, _ *domain.Lease, r *ports.ReserveAttemptEffectRequestV1, _ *time.Time) {
			r.FenceToken++
		}},
		{"expiry_boundary", func(_ *ports.WorkerJobState, l *domain.Lease, _ *ports.ReserveAttemptEffectRequestV1, at *time.Time) {
			*at = l.ExpiresAt
		}},
		{"before_acquisition", func(_ *ports.WorkerJobState, l *domain.Lease, _ *ports.ReserveAttemptEffectRequestV1, at *time.Time) {
			l.AcquiredAt = at.Add(time.Microsecond)
		}},
		{"zero_clock", func(_ *ports.WorkerJobState, _ *domain.Lease, _ *ports.ReserveAttemptEffectRequestV1, at *time.Time) {
			*at = time.Time{}
		}},
		{"future_run", func(s *ports.WorkerJobState, _ *domain.Lease, _ *ports.ReserveAttemptEffectRequestV1, at *time.Time) {
			s.Run.UpdatedAt = at.Add(time.Microsecond)
		}},
		{"future_attempt", func(s *ports.WorkerJobState, _ *domain.Lease, _ *ports.ReserveAttemptEffectRequestV1, at *time.Time) {
			s.Attempt.UpdatedAt = at.Add(time.Microsecond)
		}},
		{"future_job", func(s *ports.WorkerJobState, _ *domain.Lease, _ *ports.ReserveAttemptEffectRequestV1, at *time.Time) {
			s.Job.CreatedAt = at.Add(time.Microsecond)
		}},
		{"cancellation", func(s *ports.WorkerJobState, _ *domain.Lease, _ *ports.ReserveAttemptEffectRequestV1, at *time.Time) {
			s.Run.CancellationRequestedAt = at
		}},
		{"nonrunning_run", func(s *ports.WorkerJobState, _ *domain.Lease, _ *ports.ReserveAttemptEffectRequestV1, _ *time.Time) {
			s.Run.Status = domain.RunQueued
		}},
		{"nonrunning_attempt", func(s *ports.WorkerJobState, _ *domain.Lease, _ *ports.ReserveAttemptEffectRequestV1, _ *time.Time) {
			s.Attempt.Status = domain.AttemptCreated
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := currentWorkerAuthorityFixture(t)
			at := s.Run.CreatedAt.Add(time.Second)
			lease := domain.Lease{ID: "lease-test", TenantID: s.Run.TenantID, RunID: s.Run.ID, AttemptID: s.Attempt.ID, WorkerID: s.Attempt.WorkerID, FenceToken: 1, AcquiredAt: s.Run.CreatedAt, ExpiresAt: s.Run.CreatedAt.Add(time.Hour)}
			r := ports.ReserveAttemptEffectRequestV1{TenantID: s.Run.TenantID, RunID: s.Run.ID, AttemptID: s.Attempt.ID, LeaseID: lease.ID, FenceToken: lease.FenceToken, PhysicalInvocationClaimID: "claim-test"}
			c.mutate(&s, &lease, &r, &at)
			err := validateCurrentServerlessLease(s, lease, r, at)
			if (err == nil) != (c.name == "valid") {
				t.Fatalf("case=%s: current lease error=%v, want authorized=%t", c.name, err, c.name == "valid")
			}
		})
	}
}
