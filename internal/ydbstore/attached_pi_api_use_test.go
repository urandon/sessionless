package ydbstore

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestPiAPIUseExpiryClampsBeforeAdd(t *testing.T) {
	t.Parallel()
	for _, at := range []time.Time{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(9999, 12, 31, 23, 58, 50, 0, time.UTC)} {
		for _, tc := range []struct {
			name      string
			remaining uint64
			deadlines []time.Time
			want      time.Time
		}{
			{"caller_shorter", uint64(time.Second), []time.Time{at.Add(time.Minute)}, at.Add(time.Second)},
			{"server_shorter", math.MaxInt64, []time.Time{at.Add(time.Second)}, at.Add(time.Second)},
			{"minimum_server_source", uint64(time.Minute), []time.Time{at.Add(5 * time.Second), at.Add(time.Second), at.Add(3 * time.Second)}, at.Add(time.Second)},
			{"positive_nanosecond", 1, []time.Time{at.Add(time.Second)}, at.Add(time.Nanosecond)},
		} {
			t.Run(at.Format("2006")+"/"+tc.name, func(t *testing.T) {
				got, err := piAPIUseExpiry(at, tc.remaining, tc.deadlines...)
				if err != nil || got != tc.want {
					t.Fatalf("expiry=%s want=%s err=%v", got, tc.want, err)
				}
			})
		}
	}
}

func TestPiAPIUseExpiryRejectsUnknownOrExpiredBounds(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		at        time.Time
		remaining uint64
		deadlines []time.Time
	}{
		{"zero_clock", time.Time{}, 1, []time.Time{at.Add(time.Second)}},
		{"nonmicro_clock", at.Add(time.Nanosecond), 1, []time.Time{at.Add(time.Second)}},
		{"clock_zone_alias", at.In(time.FixedZone("synthetic", 3600)), 1, []time.Time{at.Add(time.Second)}},
		{"zero_allowance", at, 0, []time.Time{at.Add(time.Second)}},
		{"overflow_allowance", at, math.MaxInt64 + 1, []time.Time{at.Add(time.Second)}},
		{"missing_server_bound", at, 1, nil},
		{"zero_server_bound", at, 1, []time.Time{{}}},
		{"expired_server_bound", at, 1, []time.Time{at}},
		{"past_server_bound", at, 1, []time.Time{at.Add(-time.Nanosecond)}},
		{"invalid_server_year", at, 1, []time.Time{time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := piAPIUseExpiry(tc.at, tc.remaining, tc.deadlines...)
			if err == nil || !got.IsZero() {
				t.Fatalf("accepted %s: expiry=%s err=%v", tc.name, got, err)
			}
		})
	}
}

// These are coherent original-job DATA controls only. No real owner receipt,
// qualified native/tariff source, credential or native YDB is supplied here.
func piAPIOriginalJobData(t *testing.T) (ports.WorkerJobState, domain.Lease, domain.AttachedWorkerAttemptV1, ports.AttachedWorkerSealedInputAuthorization, domain.HarnessBindingDigestV2, time.Time) {
	t.Helper()
	s := currentWorkerAuthorityFixture(t)
	s.Job.Limits.MaxContextEvents, s.Job.Limits.MaxToolEvents, s.Job.Limits.MaxToolEventBytes = 32, 8, 4096
	base := s.Run.CreatedAt
	s.Run.StartedAt = &base
	p := domain.ExecutionPlacementV2{Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker, FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: s.Job.CredentialOwnerUserID, WorkerID: "worker-test", CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("b", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("c", 64))}
	s.Job.ExecutionPlacementV2 = p
	s.Job.SubstrateBinding, s.Job.AdmissionCostCeiling = nil, nil
	b := &s.Job.HarnessBinding
	b.Profile = domain.HarnessExecutionSingleTurnV2
	b.Backend = domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendPiV1, ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("d", 64), NativeProtocolVersion: domain.AttachedPiSDKNativeProtocolV1, BackendProfileDigest: strings.Repeat("e", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryFileV1}
	b.Resource = domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceAPIAccountV1, ResourceID: "synthetic-api-resource", OwnerUserID: s.Job.CredentialOwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	b.ModelVendorID, b.ModelID = "synthetic-vendor", "synthetic-model"
	b.InputDataClass = domain.ProviderDataPublicV1
	b.ProviderCatalogDigest, b.ProviderRouteDigest, b.PrivacyPolicyDigest, b.CapabilityEvidenceDigest, b.EffectivePolicyDigest = strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64), strings.Repeat("4", 64), strings.Repeat("5", 64)
	expires := base.Add(time.Minute)
	b.EvidenceExpiresAt = &expires
	b.AttachedPiAPIInvocationPolicyDigest = domain.AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("f", 64))
	pd, err := domain.ExecutionPlacementDigest(p)
	if err != nil {
		t.Fatal(err)
	}
	b.ExecutionPlacementDigest = string(pd)
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(s.Job, s.InputManifest)
	if err != nil {
		t.Fatalf("synthetic original context: %v", err)
	}
	lease := domain.Lease{ID: "synthetic-pi-lease", TenantID: s.Run.TenantID, RunID: s.Run.ID, AttemptID: s.Attempt.ID, WorkerID: string(p.WorkerID), FenceToken: 4, AcquiredAt: base, ExpiresAt: expires}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(s.Run.TenantID, s.Job.CredentialOwnerUserID, p.WorkerID, s.Run.ID, s.Attempt.ID, lease.ID, lease.FenceToken)
	if err != nil {
		t.Fatal(err)
	}
	head := domain.AttachedWorkerAttemptV1{TenantID: s.Run.TenantID, OwnerUserID: p.OwnerUserID, WorkerID: p.WorkerID, ConnectionID: "synthetic-connection", EnrollmentGeneration: 2, ConnectionGeneration: 3, RunID: s.Run.ID, AttemptID: s.Attempt.ID, ReservationID: s.Reservation.ID, LeaseID: lease.ID, LeaseGeneration: lease.FenceToken, FenceToken: fence, LeaseExpiresAt: expires, ContextDigest: contextDigest, CapabilityDigest: p.CapabilityDigest, PolicyDigest: p.PolicyDigest, State: domain.AttachedWorkerAttemptClaimed, Revision: 7, CreatedAt: base, UpdatedAt: base}
	auth := ports.AttachedWorkerSealedInputAuthorization{TenantID: head.TenantID, OwnerUserID: head.OwnerUserID, WorkerID: head.WorkerID, ConnectionID: head.ConnectionID, PresentedSecretDigest: domain.DigestAttachedWorkerConnectionSecret([]byte("synthetic-pi-phase")), EnrollmentGeneration: head.EnrollmentGeneration, ConnectionGeneration: head.ConnectionGeneration, RunID: head.RunID, AttemptID: head.AttemptID, AttemptSequence: 1, LeaseID: head.LeaseID, LeaseGeneration: head.LeaseGeneration, FenceToken: head.FenceToken, LeaseExpiresAtUnixMicro: expires.UnixMicro(), ContextDigest: head.ContextDigest, CapabilityDigest: head.CapabilityDigest, PolicyDigest: head.PolicyDigest, ExpectedAttemptRevision: head.Revision}
	digest, err := b.Digest()
	if err != nil {
		t.Fatalf("synthetic original binding: %v", err)
	}
	at := base.Add(time.Second)
	if !piAPIOriginalJobMatches(s, lease, head, auth, digest, b.AttachedPiAPIInvocationPolicyDigest, at) {
		t.Fatal("coherent original-job control denied")
	}
	return s, lease, head, auth, digest, at
}

func TestPiAPIOriginalJobRejectsReplacementAndStaleLifecycle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*ports.WorkerJobState, *domain.Lease, *ports.AttachedWorkerSealedInputAuthorization, *domain.HarnessBindingDigestV2, *time.Time)
	}{
		{"cancelled_run", func(s *ports.WorkerJobState, _ *domain.Lease, _ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.HarnessBindingDigestV2, at *time.Time) {
			s.Run.CancellationRequestedAt = at
		}},
		{"nonrunning_attempt", func(s *ports.WorkerJobState, _ *domain.Lease, _ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.HarnessBindingDigestV2, _ *time.Time) {
			s.Attempt.Status = domain.AttemptSucceeded
		}},
		{"changed_binding", func(_ *ports.WorkerJobState, _ *domain.Lease, _ *ports.AttachedWorkerSealedInputAuthorization, d *domain.HarnessBindingDigestV2, _ *time.Time) {
			*d = domain.HarnessBindingDigestV2(strings.Repeat("a", 64))
		}},
		{"changed_mapping", func(s *ports.WorkerJobState, _ *domain.Lease, _ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.HarnessBindingDigestV2, _ *time.Time) {
			s.Job.HarnessBinding.AttachedPiAPIInvocationPolicyDigest = domain.AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("a", 64))
		}},
		{"lease_worker", func(_ *ports.WorkerJobState, l *domain.Lease, _ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.HarnessBindingDigestV2, _ *time.Time) {
			l.WorkerID = "foreign-worker"
		}},
		{"attempt_worker", func(s *ports.WorkerJobState, _ *domain.Lease, _ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.HarnessBindingDigestV2, _ *time.Time) {
			s.Attempt.WorkerID = "foreign-worker"
		}},
		{"lease_generation", func(_ *ports.WorkerJobState, l *domain.Lease, _ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.HarnessBindingDigestV2, _ *time.Time) {
			l.FenceToken++
		}},
		{"lease_expired", func(_ *ports.WorkerJobState, l *domain.Lease, _ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.HarnessBindingDigestV2, at *time.Time) {
			*at = l.ExpiresAt
		}},
		{"original_context", func(_ *ports.WorkerJobState, _ *domain.Lease, a *ports.AttachedWorkerSealedInputAuthorization, _ *domain.HarnessBindingDigestV2, _ *time.Time) {
			a.ContextDigest = domain.AttachedWorkerContextDigest(strings.Repeat("a", 64))
		}},
		{"future_job", func(s *ports.WorkerJobState, _ *domain.Lease, _ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.HarnessBindingDigestV2, at *time.Time) {
			s.Job.CreatedAt = at.Add(time.Second)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, l, h, a, d, at := piAPIOriginalJobData(t)
			originalM := s.Job.HarnessBinding.AttachedPiAPIInvocationPolicyDigest
			tc.mutate(&s, &l, &a, &d, &at)
			if piAPIOriginalJobMatches(s, l, h, a, d, originalM, at) {
				t.Fatalf("accepted %s", tc.name)
			}
		})
	}
}

func TestPiAPIUseDefaultStoreCannotIssueAuthority(t *testing.T) {
	t.Parallel()
	for _, s := range []*Store{nil, {}} {
		got, err := s.AuthorizeAttachedWorkerPiAPIUse(context.Background(), ports.AttachedWorkerSealedInputAuthorization{}, "", "", 1, "physical_send")
		if err == nil || got.Status != ports.AttachedWorkerExecutionDenied || !got.CheckedAt.IsZero() || !got.UseExpiresAt.IsZero() {
			t.Fatalf("default store issued phase data: status=%s err=%v", got.Status, err)
		}
	}
}
