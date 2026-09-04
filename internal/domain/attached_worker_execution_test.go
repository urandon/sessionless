package domain_test

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func attachedContextJob(t testing.TB) domain.WorkerJob {
	t.Helper()
	blob := func(name, digest string) domain.BlobRef {
		return domain.BlobRef{TenantID: "tenant-1", Key: "tenants/tenant-1/" + name, Size: 10, SHA256: strings.Repeat(digest, 64)}
	}
	workspace, skills := blob("workspace.tar", "2"), blob("skills.tar", "3")
	job := domain.WorkerJob{
		TenantID: "tenant-1", RunID: "run-1", SessionID: "session-1", TriggerEventID: "event-1", AttemptID: "attempt-1",
		ReservationID: "reservation-1", InputManifestID: "manifest-1", ContextSnapshot: blob("context.json", "1"),
		WorkspaceSnapshot: &workspace, SkillBundle: &skills, AllowedMCPServers: []string{"filesystem", "search"}, CredentialOwnerUserID: "user-1",
		ExecutionPlacementV2: domain.ExecutionPlacementV2{Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
			FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: "user-1", WorkerID: "worker-1",
			CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("capability")), PolicyDigest: domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy")))},
		Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 10,
			MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 100, MaxArtifacts: 10, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18},
	}
	job.HarnessBinding = deterministicHarnessBindingForPlacement(
		t, job.TenantID, job.CredentialOwnerUserID, job.RunID, job.AttemptID,
		job.ExecutionPlacementV2, time.Unix(1, 0).UTC(),
	)
	return job
}

func attachedContextManifest(job domain.WorkerJob) domain.ArtifactManifest {
	return domain.ArtifactManifest{
		ID: job.InputManifestID, TenantID: job.TenantID, RunID: job.RunID,
		CreatedAt: time.Unix(10, 0).UTC(),
		Artifacts: []domain.Artifact{{
			Name: "input", MediaType: "application/json",
			Blob: domain.BlobRef{TenantID: job.TenantID, Key: "tenants/" + string(job.TenantID) + "/input.json", Size: 10, SHA256: strings.Repeat("a", 64)},
		}},
	}
}

func cloneAttachedContextJob(job domain.WorkerJob) domain.WorkerJob {
	job.AllowedMCPServers = append([]string(nil), job.AllowedMCPServers...)
	if job.ContextWindow != nil {
		value := *job.ContextWindow
		if value.SnapshotVersion != nil {
			version := *value.SnapshotVersion
			value.SnapshotVersion = &version
		}
		job.ContextWindow = &value
	}
	if job.WorkspaceSnapshot != nil {
		value := *job.WorkspaceSnapshot
		job.WorkspaceSnapshot = &value
	}
	if job.SkillBundle != nil {
		value := *job.SkillBundle
		job.SkillBundle = &value
	}
	return job
}

func TestAttachedWorkerJobContextDigestV1BindsEveryExecutionInput(t *testing.T) {
	t.Parallel()
	base := attachedContextJob(t)
	manifest := attachedContextManifest(base)
	digest, err := domain.AttachedWorkerJobContextDigestV1(base, manifest)
	if err != nil {
		t.Fatalf("base digest: %v", err)
	}
	mutations := map[string]func(*domain.WorkerJob){
		"tenant": func(job *domain.WorkerJob) {
			job.TenantID = "tenant-2"
			for _, blob := range []*domain.BlobRef{&job.ContextSnapshot, job.WorkspaceSnapshot, job.SkillBundle} {
				blob.TenantID = job.TenantID
				blob.Key = strings.Replace(blob.Key, "tenants/tenant-1/", "tenants/tenant-2/", 1)
			}
		},
		"run": func(job *domain.WorkerJob) { job.RunID = "run-2" }, "session": func(job *domain.WorkerJob) { job.SessionID = "session-2" },
		"trigger": func(job *domain.WorkerJob) { job.TriggerEventID = "event-2" }, "attempt": func(job *domain.WorkerJob) { job.AttemptID = "attempt-2" },
		"reservation": func(job *domain.WorkerJob) { job.ReservationID = "reservation-2" }, "manifest": func(job *domain.WorkerJob) { job.InputManifestID = "manifest-2" },
		"context key":  func(job *domain.WorkerJob) { job.ContextSnapshot.Key = "tenants/tenant-1/context-2.json" },
		"context size": func(job *domain.WorkerJob) { job.ContextSnapshot.Size++ }, "context digest": func(job *domain.WorkerJob) { job.ContextSnapshot.SHA256 = strings.Repeat("4", 64) },
		"context source": func(job *domain.WorkerJob) { job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 5} },
		"workspace":      func(job *domain.WorkerJob) { job.WorkspaceSnapshot.SHA256 = strings.Repeat("5", 64) }, "workspace presence": func(job *domain.WorkerJob) { job.WorkspaceSnapshot = nil },
		"skills": func(job *domain.WorkerJob) { job.SkillBundle.SHA256 = strings.Repeat("6", 64) }, "skills presence": func(job *domain.WorkerJob) { job.SkillBundle = nil },
		"mcp set": func(job *domain.WorkerJob) { job.AllowedMCPServers[0] = "browser" }, "credential owner": func(job *domain.WorkerJob) { job.CredentialOwnerUserID = "user-2" },
		"placement owner": func(job *domain.WorkerJob) { job.ExecutionPlacementV2.OwnerUserID = "user-2" }, "placement worker": func(job *domain.WorkerJob) { job.ExecutionPlacementV2.WorkerID = "worker-2" },
		"capability": func(job *domain.WorkerJob) {
			job.ExecutionPlacementV2.CapabilityDigest = domain.DigestAttachedWorkerCapability([]byte("capability-2"))
		},
		"policy": func(job *domain.WorkerJob) {
			job.ExecutionPlacementV2.PolicyDigest = domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy-2")))
		},
		"harness model": func(job *domain.WorkerJob) { job.HarnessBinding.ModelID = "deterministic-fixture-v2" },
		"queue limit":   func(job *domain.WorkerJob) { job.Limits.MaxTenantQueueDepth++ }, "active limit": func(job *domain.WorkerJob) { job.Limits.MaxActiveRuns++ },
		"runtime limit": func(job *domain.WorkerJob) { job.Limits.MaxRuntime++ }, "turn limit": func(job *domain.WorkerJob) { job.Limits.MaxTurns++ },
		"input limit": func(job *domain.WorkerJob) { job.Limits.MaxInputBytes++ }, "context limit": func(job *domain.WorkerJob) { job.Limits.MaxContextBytes++ },
		"context event limit": func(job *domain.WorkerJob) { job.Limits.MaxContextEvents++ }, "artifact limit": func(job *domain.WorkerJob) { job.Limits.MaxArtifacts++ },
		"tool event limit": func(job *domain.WorkerJob) { job.Limits.MaxToolEvents++ }, "tool byte limit": func(job *domain.WorkerJob) { job.Limits.MaxToolEventBytes++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := cloneAttachedContextJob(base)
			mutate(&candidate)
			candidateManifest := manifest
			candidateManifest.Artifacts = append([]domain.Artifact(nil), manifest.Artifacts...)
			if candidate.TenantID != base.TenantID {
				candidateManifest.TenantID = candidate.TenantID
				candidateManifest.Artifacts[0].Blob.TenantID = candidate.TenantID
				candidateManifest.Artifacts[0].Blob.Key = "tenants/" + string(candidate.TenantID) + "/input.json"
			}
			if candidate.RunID != base.RunID {
				candidateManifest.RunID = candidate.RunID
			}
			if candidate.InputManifestID != base.InputManifestID {
				candidateManifest.ID = candidate.InputManifestID
			}
			other, err := domain.AttachedWorkerJobContextDigestV1(candidate, candidateManifest)
			if err != nil {
				t.Fatalf("mutated digest: %v", err)
			}
			if other == digest {
				t.Fatal("security-relevant mutation did not change digest")
			}
		})
	}
}

func TestAttachedWorkerJobContextDigestV1SortsOnlySemanticMCPSet(t *testing.T) {
	t.Parallel()
	job := attachedContextJob(t)
	manifest := attachedContextManifest(job)
	first, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	job.AllowedMCPServers[0], job.AllowedMCPServers[1] = job.AllowedMCPServers[1], job.AllowedMCPServers[0]
	second, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("semantic MCP set order changed digest")
	}
	job.AllowedMCPServers = append(job.AllowedMCPServers, job.AllowedMCPServers[0])
	if _, err := domain.AttachedWorkerJobContextDigestV1(job, manifest); err == nil {
		t.Fatal("duplicate MCP server accepted")
	}
	job = attachedContextJob(t)
	job.ExecutionPlacementV2.FallbackPolicy = "managed"
	if _, err := domain.AttachedWorkerJobContextDigestV1(job, manifest); err == nil {
		t.Fatal("non-deny attached placement accepted")
	}
}

func TestAttachedWorkerJobContextDigestBindsManifestContent(t *testing.T) {
	t.Parallel()
	job := attachedContextJob(t)
	manifest := attachedContextManifest(job)
	first, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	mutated := manifest
	mutated.Artifacts = append([]domain.Artifact(nil), manifest.Artifacts...)
	mutated.Artifacts[0].Blob.SHA256 = strings.Repeat("b", 64)
	second, err := domain.AttachedWorkerJobContextDigestV1(job, mutated)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("same manifest ID with different artifact content retained context digest")
	}
	reordered := manifest
	reordered.Artifacts = append(reordered.Artifacts, domain.Artifact{
		Name: "alpha", MediaType: "text/plain",
		Blob: domain.BlobRef{TenantID: job.TenantID, Key: "tenants/tenant-1/alpha.txt", Size: 1, SHA256: strings.Repeat("c", 64)},
	})
	firstOrder, err := domain.AttachedWorkerJobContextDigestV1(job, reordered)
	if err != nil {
		t.Fatal(err)
	}
	reordered.Artifacts[0], reordered.Artifacts[1] = reordered.Artifacts[1], reordered.Artifacts[0]
	secondOrder, err := domain.AttachedWorkerJobContextDigestV1(job, reordered)
	if err != nil || firstOrder != secondOrder {
		t.Fatalf("manifest semantic set order changed digest: %v", err)
	}
}

func TestExecutionPlacementIsExplicitAndDenyOnly(t *testing.T) {
	t.Parallel()
	managed := deterministicManagedAuthority(t, "tenant-1", "user-1", "run-1", "attempt-1", time.Unix(1, 0).UTC()).ExecutionPlacementV2
	if err := managed.Validate(); err != nil {
		t.Fatalf("managed placement: %v", err)
	}
	capability := domain.DigestAttachedWorkerCapability([]byte("capability"))
	attached := domain.ExecutionPlacementV2{
		Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: "user-1", WorkerID: "worker-1",
		CapabilityDigest: capability, PolicyDigest: domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy"))),
	}
	if err := attached.Validate(); err != nil {
		t.Fatalf("attached placement: %v", err)
	}
	for name, mutate := range map[string]func(*domain.ExecutionPlacementV2){
		"zero":           func(value *domain.ExecutionPlacementV2) { *value = domain.ExecutionPlacementV2{} },
		"fallback":       func(value *domain.ExecutionPlacementV2) { value.FallbackPolicy = "managed" },
		"managed target": func(value *domain.ExecutionPlacementV2) { value.Kind = domain.ExecutionPlacementManaged },
		"missing owner":  func(value *domain.ExecutionPlacementV2) { value.OwnerUserID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := attached
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("invalid placement accepted")
			}
		})
	}
}

func validAttachedAttempt(t *testing.T) domain.AttachedWorkerAttemptV1 {
	t.Helper()
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	fence, err := domain.NewAttachedWorkerFenceTokenV1("tenant-1", "user-1", "worker-1", "run-1", "attempt-1", "lease-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	return domain.AttachedWorkerAttemptV1{
		Version: domain.AttachedWorkerAttemptVersionV1, TenantID: "tenant-1", OwnerUserID: "user-1", WorkerID: "worker-1",
		ConnectionID: "connection-1", RunID: "run-1", AttemptID: "attempt-1", ReservationID: "reservation-1", LeaseID: "lease-1",
		LeaseGeneration: 7, FenceToken: fence, EnrollmentGeneration: 2, ConnectionGeneration: 3,
		ContextDigest:    domain.AttachedWorkerContextDigest(domain.DigestAttachedWorkerCapability([]byte("context"))),
		CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("capability")),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy"))),
		State:            domain.AttachedWorkerAttemptOffered, PlatformAttemptSequence: 1,
		LeaseExpiresAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now, Revision: 1,
	}
}

func TestAttachedWorkerAttemptV1StrictStateEvidence(t *testing.T) {
	t.Parallel()
	attempt := validAttachedAttempt(t)
	if err := attempt.Validate(); err != nil {
		t.Fatalf("valid offered attempt: %v", err)
	}
	claimed := attempt
	claimed.State, claimed.WorkerAttemptSequence, claimed.Revision = domain.AttachedWorkerAttemptClaimed, 1, 2
	if err := claimed.Validate(); err != nil {
		t.Fatalf("valid claimed attempt: %v", err)
	}
	terminal := claimed
	terminal.State, terminal.TerminalSequence, terminal.TerminalStatus = domain.AttachedWorkerAttemptTerminalPending, 1, domain.AttachedWorkerTerminalSucceeded
	terminal.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(domain.DigestAttachedWorkerCapability([]byte("terminal")))
	if err := terminal.Validate(); err != nil {
		t.Fatalf("valid terminal attempt: %v", err)
	}
	for name, mutate := range map[string]func(*domain.AttachedWorkerAttemptV1){
		"renewed lease":    func(value *domain.AttachedWorkerAttemptV1) { value.LeaseGeneration++ },
		"offered claimed":  func(value *domain.AttachedWorkerAttemptV1) { value.WorkerAttemptSequence = 1 },
		"terminal partial": func(value *domain.AttachedWorkerAttemptV1) { value.TerminalSequence = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := attempt
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("invalid attempt accepted")
			}
		})
	}
}

func TestAttachedWorkerMessageFingerprintDirectionAndDeadlineCursorKey(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"kind":"lease_offered"}`)
	message := domain.AttachedWorkerAttemptMessageV1{
		Version: domain.AttachedWorkerAttemptMessageVersionV1, TenantID: "tenant-1", OwnerUserID: "user-1", WorkerID: "worker-1", AttemptID: "attempt-1",
		Direction: domain.AttachedWorkerAttemptPlatformToWorker, AttemptSequence: 1, ConnectionGeneration: 3, EnvelopeSequence: 3,
		Kind: domain.AttachedWorkerAttemptMessageLeaseOffered, Fingerprint: domain.AttachedWorkerAttemptMessageFingerprint(domain.DigestAttachedWorkerCapability([]byte("offer-fingerprint"))), Payload: payload, CreatedAt: now,
	}
	if err := message.Validate(); err != nil {
		t.Fatalf("valid message: %v", err)
	}
	tampered := message
	tampered.Fingerprint = "not-a-digest"
	if err := tampered.Validate(); err == nil {
		t.Fatal("malformed fingerprint accepted")
	}
	wrongDirection := message
	wrongDirection.Direction = domain.AttachedWorkerAttemptWorkerToPlatform
	if err := wrongDirection.Validate(); err == nil {
		t.Fatal("wrong direction accepted")
	}
	for name, item := range map[string]struct {
		kind      domain.AttachedWorkerAttemptMessageKind
		direction domain.AttachedWorkerAttemptDirection
	}{
		"lease claim":    {domain.AttachedWorkerAttemptMessageLeaseClaim, domain.AttachedWorkerAttemptWorkerToPlatform},
		"lease accepted": {domain.AttachedWorkerAttemptMessageLeaseAccepted, domain.AttachedWorkerAttemptPlatformToWorker},
		"progress":       {domain.AttachedWorkerAttemptMessageProgress, domain.AttachedWorkerAttemptWorkerToPlatform},
		"cancel ack":     {domain.AttachedWorkerAttemptMessageCancelAcknowledged, domain.AttachedWorkerAttemptWorkerToPlatform},
		"terminal":       {domain.AttachedWorkerAttemptMessageTerminal, domain.AttachedWorkerAttemptWorkerToPlatform},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := message
			candidate.Kind, candidate.Direction = item.kind, item.direction
			if candidate.Kind == domain.AttachedWorkerAttemptMessageCancelRequested {
				candidate.OperationDeadline = now.Add(time.Minute)
			}
			if candidate.Kind == domain.AttachedWorkerAttemptMessageTerminal {
				candidate.MaterializationReservationID = "reservation-1"
				candidate.ExecutionConnectionID = "connection-1"
			}
			if err := candidate.Validate(); err != nil {
				t.Fatalf("valid direction rejected: %v", err)
			}
			if item.direction == domain.AttachedWorkerAttemptPlatformToWorker {
				candidate.Direction = domain.AttachedWorkerAttemptWorkerToPlatform
			} else {
				candidate.Direction = domain.AttachedWorkerAttemptPlatformToWorker
			}
			if err := candidate.Validate(); err == nil {
				t.Fatal("opposite direction accepted")
			}
		})
	}
	bucket, err := domain.AttachedWorkerAttemptDeadlineBucketV1("tenant-1", "user-1", "worker-1", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	deadline := domain.AttachedWorkerAttemptDeadlineV1{Bucket: bucket, DeadlineAt: now, TenantID: "tenant-1", OwnerUserID: "user-1", WorkerID: "worker-1", AttemptID: "attempt-1", Kind: domain.AttachedWorkerDeadlineLeaseExpiry, LeaseGeneration: 7, AttemptRevision: 2}
	if err := deadline.Validate(); err != nil {
		t.Fatalf("valid deadline: %v", err)
	}
	deadline.Bucket = (bucket + 1) % domain.AttachedWorkerAttemptDeadlineBuckets
	if err := deadline.Validate(); err == nil {
		t.Fatal("wrong deadline bucket accepted")
	}
}

func TestAttachedWorkerFenceTokenIsScopedAndGenerationIsCanonical(t *testing.T) {
	t.Parallel()
	first, err := domain.NewAttachedWorkerFenceTokenV1("tenant-1", "user-1", "worker-1", "run-1", "attempt-1", "lease-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	replay, _ := domain.NewAttachedWorkerFenceTokenV1("tenant-1", "user-1", "worker-1", "run-1", "attempt-1", "lease-1", 7)
	otherOwner, _ := domain.NewAttachedWorkerFenceTokenV1("tenant-1", "user-2", "worker-1", "run-1", "attempt-1", "lease-1", 7)
	if first != replay || first == otherOwner {
		t.Fatal("fence token is not deterministic and scope-bound")
	}
	if generation, err := domain.AttachedWorkerLeaseGeneration(7); err != nil || generation != 7 {
		t.Fatalf("canonical generation: %d, %v", generation, err)
	}
	if _, err := domain.AttachedWorkerLeaseGeneration(0); err == nil {
		t.Fatal("zero fence accepted")
	}
}

func TestAttachedWorkerLeaseIDV1IsOpaqueStableAndScopeBound(t *testing.T) {
	t.Parallel()
	first, err := domain.NewAttachedWorkerLeaseIDV1("tenant-1", "run-1", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	replay, _ := domain.NewAttachedWorkerLeaseIDV1("tenant-1", "run-1", "attempt-1")
	if first != replay || !strings.HasPrefix(string(first), "lea_") || len(first) != len("lea_")+64 {
		t.Fatalf("invalid stable lease ID %q", first)
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("generated ID rejected: %v", err)
	}
	for name, scope := range map[string][3]string{
		"tenant": {"tenant-2", "run-1", "attempt-1"}, "run": {"tenant-1", "run-2", "attempt-1"}, "attempt": {"tenant-1", "run-1", "attempt-2"},
	} {
		t.Run(name, func(t *testing.T) {
			other, err := domain.NewAttachedWorkerLeaseIDV1(domain.TenantID(scope[0]), domain.RunID(scope[1]), domain.AttemptID(scope[2]))
			if err != nil {
				t.Fatal(err)
			}
			if other == first {
				t.Fatal("scope mutation did not change lease ID")
			}
		})
	}
	if _, err := domain.NewAttachedWorkerLeaseIDV1("invalid tenant", "run-1", "attempt-1"); err == nil {
		t.Fatal("invalid scope accepted")
	}
}

func TestAttachedWorkerLeaseTTLForLimitsV1IsFixedAndBounded(t *testing.T) {
	t.Parallel()
	limits := attachedContextJob(t).Limits
	limits.MaxRuntime = time.Minute
	ttl, err := domain.AttachedWorkerLeaseTTLForLimitsV1(limits)
	if err != nil || ttl != time.Minute+domain.AttachedWorkerLeaseFinalizationBudgetV1 {
		t.Fatalf("ttl=%s err=%v", ttl, err)
	}
	limits.MaxRuntime = domain.AttachedWorkerLeaseMaximumTTLV1 - domain.AttachedWorkerLeaseFinalizationBudgetV1
	if ttl, err := domain.AttachedWorkerLeaseTTLForLimitsV1(limits); err != nil || ttl != domain.AttachedWorkerLeaseMaximumTTLV1 {
		t.Fatalf("inclusive boundary ttl=%s err=%v", ttl, err)
	}
	limits.MaxRuntime++
	if _, err := domain.AttachedWorkerLeaseTTLForLimitsV1(limits); err == nil {
		t.Fatal("over-maximum lease accepted")
	}
	limits = attachedContextJob(t).Limits
	limits.MaxContextEvents = 0
	if _, err := domain.AttachedWorkerLeaseTTLForLimitsV1(limits); err == nil {
		t.Fatal("non-admission limits accepted")
	}
}
