//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestAttachedWorkerEnrollmentClaimIsAtomicAndFailClosed(t *testing.T) {
	store, _ := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-worker-claim"))
	ownerID := domain.UserID(uniqueID("owner-worker-claim"))

	enrollment, createAudit := attachedWorkerEnrollmentFixture("concurrent", tenantID, ownerID, now)
	if err := store.CreateAttachedWorkerEnrollment(ctx, enrollment, createAudit); err != nil {
		t.Fatal(err)
	}
	claim := attachedWorkerClaimFixture(enrollment, now.Add(time.Second), 1)
	otherOwnerID := domain.UserID(uniqueID("other-owner-worker-claim"))
	if _, found, err := store.LoadAttachedWorkerEnrollment(ctx, tenantID, otherOwnerID, enrollment.ID); err != nil || found {
		t.Fatalf("cross-owner enrollment load: found=%t err=%v", found, err)
	}
	wrongOwnerClaim := claim
	wrongOwnerClaim.OwnerUserID = otherOwnerID
	wrongOwnerClaim.Worker.OwnerUserID = otherOwnerID
	wrongOwnerClaim.Audit.OwnerUserID = otherOwnerID
	denied, err := store.ClaimAttachedWorkerEnrollment(ctx, wrongOwnerClaim)
	if err != nil || denied.Status != ports.AttachedWorkerDenied {
		t.Fatalf("cross-owner claim = %#v, %v; want denied", denied, err)
	}

	const contenders = 8
	start := make(chan struct{})
	results := make(chan ports.AttachedWorkerClaimResult, contenders)
	errs := make(chan error, contenders)
	var wait sync.WaitGroup
	for range contenders {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := store.ClaimAttachedWorkerEnrollment(ctx, claim)
			results <- result
			errs <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent claim: %v", err)
		}
	}
	claimed, consumed := 0, 0
	for result := range results {
		switch result.Status {
		case ports.AttachedWorkerClaimed:
			claimed++
		case ports.AttachedWorkerConsumed:
			consumed++
		default:
			t.Fatalf("unexpected concurrent claim status %q", result.Status)
		}
	}
	if claimed != 1 || consumed != contenders-1 {
		t.Fatalf("claim outcomes: claimed=%d consumed=%d", claimed, consumed)
	}
	replay, err := store.ClaimAttachedWorkerEnrollment(ctx, claim)
	if err != nil || replay.Status != ports.AttachedWorkerConsumed {
		t.Fatalf("replay = %#v, %v; want consumed", replay, err)
	}
	storedEnrollment, found, err := store.LoadAttachedWorkerEnrollment(ctx, tenantID, ownerID, enrollment.ID)
	if err != nil || !found || storedEnrollment.ConsumedAt.IsZero() || storedEnrollment.Revision != 2 {
		t.Fatalf("consumed enrollment = %#v, found=%t, err=%v", storedEnrollment, found, err)
	}
	if _, found, err := store.LoadAttachedWorker(ctx, tenantID, ownerID, enrollment.WorkerID); err != nil || !found {
		t.Fatalf("claimed worker: found=%t err=%v", found, err)
	}
	audits, err := store.ListAttachedWorkerAuditEvents(ctx, tenantID, ownerID, enrollment.WorkerID, 0, 10)
	if err != nil || len(audits) != 2 || audits[0].WorkerRevision != 0 || audits[1].WorkerRevision != 1 {
		t.Fatalf("claim audit = %#v, err=%v", audits, err)
	}

	tests := []struct {
		name   string
		adjust func(*domain.AttachedWorkerEnrollment, *ports.AttachedWorkerClaimMutation)
		want   ports.AttachedWorkerClaimStatus
	}{
		{
			name: "audience",
			adjust: func(_ *domain.AttachedWorkerEnrollment, claim *ports.AttachedWorkerClaimMutation) {
				claim.PresentedAudience = "worker://wrong"
			},
			want: ports.AttachedWorkerDenied,
		},
		{
			name: "digest",
			adjust: func(_ *domain.AttachedWorkerEnrollment, claim *ports.AttachedWorkerClaimMutation) {
				claim.PresentedDigest = domain.DigestWorkerBootstrap([]byte("wrong-secret"))
			},
			want: ports.AttachedWorkerDenied,
		},
		{
			name: "expiry boundary",
			adjust: func(enrollment *domain.AttachedWorkerEnrollment, claim *ports.AttachedWorkerClaimMutation) {
				claim.At = enrollment.ExpiresAt
				claim.Worker.CreatedAt, claim.Worker.UpdatedAt = claim.At, claim.At
				claim.Audit.OccurredAt = claim.At
			},
			want: ports.AttachedWorkerExpired,
		},
		{
			name: "revision",
			adjust: func(_ *domain.AttachedWorkerEnrollment, claim *ports.AttachedWorkerClaimMutation) {
				claim.ExpectedEnrollmentRevision++
			},
			want: ports.AttachedWorkerConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			enrollment, audit := attachedWorkerEnrollmentFixture(test.name, tenantID, ownerID, now.Add(2*time.Minute))
			if err := store.CreateAttachedWorkerEnrollment(ctx, enrollment, audit); err != nil {
				t.Fatal(err)
			}
			claim := attachedWorkerClaimFixture(enrollment, enrollment.CreatedAt.Add(time.Second), byte(len(test.name)+2))
			test.adjust(&enrollment, &claim)
			result, err := store.ClaimAttachedWorkerEnrollment(ctx, claim)
			if err != nil || result.Status != test.want {
				t.Fatalf("claim = %#v, %v; want %s", result, err, test.want)
			}
		})
	}

	badProofDerivedWorker := attachedWorkerClaimFixture(enrollment, now.Add(time.Second), 9)
	badProofDerivedWorker.Worker.IdentityPublicKey = []byte("not-an-ed25519-key")
	if _, err := store.ClaimAttachedWorkerEnrollment(ctx, badProofDerivedWorker); err == nil {
		t.Fatal("invalid proof-derived identity key was accepted")
	}
}

func TestAttachedWorkerCASRevocationOwnerScopeAndBoundedPagination(t *testing.T) {
	store, _ := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-worker-cas"))
	ownerID := domain.UserID(uniqueID("owner-worker-cas"))
	otherOwnerID := domain.UserID(uniqueID("other-owner-worker-cas"))

	var workers []domain.AttachedWorker
	for index, suffix := range []string{"a", "b", "c"} {
		enrollment, audit := attachedWorkerEnrollmentFixture(suffix, tenantID, ownerID, now.Add(time.Duration(index)*time.Minute))
		if err := store.CreateAttachedWorkerEnrollment(ctx, enrollment, audit); err != nil {
			t.Fatal(err)
		}
		claim := attachedWorkerClaimFixture(enrollment, enrollment.CreatedAt.Add(time.Second), byte(index+1))
		result, err := store.ClaimAttachedWorkerEnrollment(ctx, claim)
		if err != nil || result.Status != ports.AttachedWorkerClaimed {
			t.Fatalf("claim %s = %#v, %v", suffix, result, err)
		}
		workers = append(workers, result.Worker)
	}

	page, err := store.ListAttachedWorkers(ctx, tenantID, ownerID, "", 2)
	if err != nil || len(page) != 2 || page[0].ID >= page[1].ID {
		t.Fatalf("first worker page = %#v, %v", page, err)
	}
	next, err := store.ListAttachedWorkers(ctx, tenantID, ownerID, page[1].ID, 2)
	if err != nil || len(next) != 1 || next[0].ID <= page[1].ID {
		t.Fatalf("second worker page = %#v, %v", next, err)
	}
	if _, err := store.ListAttachedWorkers(ctx, tenantID, ownerID, "", 0); err == nil {
		t.Fatal("zero worker limit was accepted")
	}
	if _, err := store.ListAttachedWorkers(ctx, tenantID, ownerID, "", 101); err == nil {
		t.Fatal("overlarge worker limit was accepted")
	}

	current := workers[0]
	at := current.UpdatedAt.Add(time.Second)
	renamed := current
	renamed.DisplayName = "renamed worker"
	renamed.Revision++
	renamed.UpdatedAt = at
	renamedAudit := attachedWorkerMutationAudit(renamed, domain.AttachedWorkerAuditWorkerRenamed, at)
	swapped, err := store.CompareAndSwapAttachedWorker(ctx, ports.AttachedWorkerCASMutation{
		ExpectedRevision: current.Revision, Next: renamed, Audit: renamedAudit, At: at,
	})
	if err != nil || !swapped {
		t.Fatalf("rename CAS: swapped=%t err=%v", swapped, err)
	}
	stale, err := store.CompareAndSwapAttachedWorker(ctx, ports.AttachedWorkerCASMutation{
		ExpectedRevision: current.Revision, Next: renamed, Audit: renamedAudit, At: at,
	})
	if err != nil || stale {
		t.Fatalf("stale rename CAS: swapped=%t err=%v", stale, err)
	}

	current = renamed
	at = at.Add(time.Second)
	rotated := current
	rotated.IdentityPublicKey = bytes.Repeat([]byte{0x55}, ed25519.PublicKeySize)
	rotated.EnrollmentGeneration++
	rotated.Revision++
	rotated.UpdatedAt = at
	rotatedAudit := attachedWorkerMutationAudit(rotated, domain.AttachedWorkerAuditIdentityRotated, at)
	swapped, err = store.CompareAndSwapAttachedWorker(ctx, ports.AttachedWorkerCASMutation{
		ExpectedRevision: current.Revision, Next: rotated, Audit: rotatedAudit, At: at,
	})
	if err != nil || !swapped {
		t.Fatalf("identity CAS: swapped=%t err=%v", swapped, err)
	}

	current = rotated
	at = at.Add(time.Second)
	advanced := current
	advanced.ConnectionGeneration++
	advanced.Revision++
	advanced.UpdatedAt = at
	advancedAudit := attachedWorkerMutationAudit(advanced, domain.AttachedWorkerAuditConnectionGenerationAdvanced, at)
	swapped, err = store.CompareAndSwapAttachedWorker(ctx, ports.AttachedWorkerCASMutation{
		ExpectedRevision: current.Revision, Next: advanced, Audit: advancedAudit, At: at,
	})
	if err != nil || !swapped {
		t.Fatalf("connection CAS: swapped=%t err=%v", swapped, err)
	}
	wrongOwnerCASNext := advanced
	wrongOwnerCASNext.OwnerUserID = otherOwnerID
	wrongOwnerCASNext.DisplayName = "cross-owner rename"
	wrongOwnerCASNext.Revision++
	wrongOwnerCASNext.UpdatedAt = at.Add(time.Second)
	wrongOwnerCASAudit := attachedWorkerMutationAudit(wrongOwnerCASNext, domain.AttachedWorkerAuditWorkerRenamed, wrongOwnerCASNext.UpdatedAt)
	swapped, err = store.CompareAndSwapAttachedWorker(ctx, ports.AttachedWorkerCASMutation{
		ExpectedRevision: advanced.Revision, Next: wrongOwnerCASNext, Audit: wrongOwnerCASAudit, At: wrongOwnerCASNext.UpdatedAt,
	})
	if err != nil || swapped {
		t.Fatalf("cross-owner CAS: swapped=%t err=%v", swapped, err)
	}

	current = advanced
	at = at.Add(time.Second)
	revoked := current
	revoked.DesiredState = domain.AttachedWorkerDesiredRevoked
	revoked.EnrollmentGeneration++
	revoked.ConnectionGeneration++
	revoked.Revision++
	revoked.UpdatedAt, revoked.RevokedAt = at, at
	revokeAudit := attachedWorkerMutationAudit(revoked, domain.AttachedWorkerAuditWorkerRevoked, at)
	didRevoke, err := store.RevokeAttachedWorker(ctx, ports.AttachedWorkerRevokeMutation{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: current.ID,
		ExpectedRevision: current.Revision, Next: revoked, Audit: revokeAudit, At: at,
	})
	if err != nil || !didRevoke {
		t.Fatalf("revoke: revoked=%t err=%v", didRevoke, err)
	}
	stored, found, err := store.LoadAttachedWorker(ctx, tenantID, ownerID, current.ID)
	if err != nil || !found || stored.DesiredState != domain.AttachedWorkerDesiredRevoked ||
		stored.ObservedState != current.ObservedState || stored.EnrollmentGeneration != current.EnrollmentGeneration+1 ||
		stored.ConnectionGeneration != current.ConnectionGeneration+1 {
		t.Fatalf("deny-first revoked worker = %#v, found=%t, err=%v", stored, found, err)
	}

	if _, found, err := store.LoadAttachedWorker(ctx, tenantID, otherOwnerID, current.ID); err != nil || found {
		t.Fatalf("cross-owner load: found=%t err=%v", found, err)
	}
	if list, err := store.ListAttachedWorkers(ctx, tenantID, otherOwnerID, "", 10); err != nil || len(list) != 0 {
		t.Fatalf("cross-owner list = %#v, %v", list, err)
	}
	if list, err := store.ListAttachedWorkerAuditEvents(ctx, tenantID, otherOwnerID, current.ID, 0, 10); err != nil || len(list) != 0 {
		t.Fatalf("cross-owner audit = %#v, %v", list, err)
	}
	wrongOwnerNext := revoked
	wrongOwnerNext.OwnerUserID = otherOwnerID
	wrongOwnerNext.Revision++
	wrongOwnerNext.EnrollmentGeneration++
	wrongOwnerNext.ConnectionGeneration++
	wrongOwnerNext.UpdatedAt, wrongOwnerNext.RevokedAt = at.Add(time.Second), at.Add(time.Second)
	wrongOwnerAudit := attachedWorkerMutationAudit(wrongOwnerNext, domain.AttachedWorkerAuditWorkerRevoked, wrongOwnerNext.UpdatedAt)
	didRevoke, err = store.RevokeAttachedWorker(ctx, ports.AttachedWorkerRevokeMutation{
		TenantID: tenantID, OwnerUserID: otherOwnerID, WorkerID: current.ID,
		ExpectedRevision: revoked.Revision, Next: wrongOwnerNext, Audit: wrongOwnerAudit, At: wrongOwnerNext.UpdatedAt,
	})
	if err != nil || didRevoke {
		t.Fatalf("cross-owner revoke: revoked=%t err=%v", didRevoke, err)
	}

	audits, err := store.ListAttachedWorkerAuditEvents(ctx, tenantID, ownerID, current.ID, 0, 3)
	if err != nil || len(audits) != 3 || audits[0].WorkerRevision != 0 || audits[2].WorkerRevision != 2 {
		t.Fatalf("first audit page = %#v, %v", audits, err)
	}
	audits, err = store.ListAttachedWorkerAuditEvents(ctx, tenantID, ownerID, current.ID, 3, 10)
	if err != nil || len(audits) != 3 || audits[0].WorkerRevision != 3 || audits[2].WorkerRevision != 5 {
		t.Fatalf("second audit page = %#v, %v", audits, err)
	}
	if _, err := store.ListAttachedWorkerAuditEvents(ctx, tenantID, ownerID, current.ID, 0, 0); err == nil {
		t.Fatal("zero audit limit was accepted")
	}
	if _, err := store.ListAttachedWorkerAuditEvents(ctx, tenantID, ownerID, current.ID, 0, 101); err == nil {
		t.Fatal("overlarge audit limit was accepted")
	}
}

func attachedWorkerEnrollmentFixture(
	suffix string,
	tenantID domain.TenantID,
	ownerID domain.UserID,
	createdAt time.Time,
) (domain.AttachedWorkerEnrollment, domain.AttachedWorkerAuditEvent) {
	enrollment := domain.AttachedWorkerEnrollment{
		TenantID: tenantID, OwnerUserID: ownerID,
		ID:          domain.AttachedWorkerEnrollmentID(uniqueID("worker-enrollment-" + suffix)),
		WorkerID:    domain.AttachedWorkerID(uniqueID("worker-" + suffix)),
		DisplayName: "worker " + suffix, Audience: "worker://sessionless/test",
		BootstrapDigest: domain.DigestWorkerBootstrap([]byte("bootstrap-" + suffix)),
		CreatedAt:       createdAt, ExpiresAt: createdAt.Add(time.Minute), RetainUntil: createdAt.Add(time.Hour),
		Revision: 1,
	}
	audit := domain.AttachedWorkerAuditEvent{
		Version:  domain.AttachedWorkerAuditEventVersionV1,
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: enrollment.WorkerID, EnrollmentID: enrollment.ID,
		Action: domain.AttachedWorkerAuditEnrollmentCreated, OccurredAt: createdAt,
	}
	return enrollment, audit
}

func attachedWorkerClaimFixture(
	enrollment domain.AttachedWorkerEnrollment,
	at time.Time,
	keyByte byte,
) ports.AttachedWorkerClaimMutation {
	worker := domain.AttachedWorker{
		TenantID: enrollment.TenantID, OwnerUserID: enrollment.OwnerUserID, ID: enrollment.WorkerID,
		DisplayName: enrollment.DisplayName, IdentityPublicKey: bytes.Repeat([]byte{keyByte}, ed25519.PublicKeySize),
		EnrollmentGeneration: 1, DesiredState: domain.AttachedWorkerDesiredActive,
		ObservedState: domain.AttachedWorkerObservedOffline, Revision: 1, CreatedAt: at, UpdatedAt: at,
	}
	audit := domain.AttachedWorkerAuditEvent{
		Version:  domain.AttachedWorkerAuditEventVersionV1,
		TenantID: enrollment.TenantID, OwnerUserID: enrollment.OwnerUserID, WorkerID: enrollment.WorkerID,
		EnrollmentID: enrollment.ID, Action: domain.AttachedWorkerAuditEnrollmentClaimed,
		WorkerRevision: 1, EnrollmentGeneration: 1, OccurredAt: at,
	}
	return ports.AttachedWorkerClaimMutation{
		TenantID: enrollment.TenantID, OwnerUserID: enrollment.OwnerUserID, EnrollmentID: enrollment.ID,
		ExpectedEnrollmentRevision: enrollment.Revision, PresentedAudience: enrollment.Audience,
		PresentedDigest: enrollment.BootstrapDigest, Worker: worker, Audit: audit, At: at,
	}
}

func attachedWorkerMutationAudit(
	worker domain.AttachedWorker,
	action domain.AttachedWorkerAuditAction,
	at time.Time,
) domain.AttachedWorkerAuditEvent {
	return domain.AttachedWorkerAuditEvent{
		Version:  domain.AttachedWorkerAuditEventVersionV1,
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		Action: action, WorkerRevision: worker.Revision,
		EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: worker.ConnectionGeneration,
		OccurredAt: at,
	}
}
