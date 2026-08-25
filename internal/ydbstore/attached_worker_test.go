package ydbstore

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestAttachedWorkerTransactionClockIsAttemptScopedAndExpiryIsExclusive(t *testing.T) {
	expiresAt := time.Date(2026, 8, 25, 10, 0, 0, 123456000, time.UTC)
	times := []time.Time{expiresAt.Add(-time.Microsecond), expiresAt}
	index := 0
	store := &Store{attachedWorkerNow: func(context.Context, *sql.Tx) (time.Time, error) {
		at := times[index]
		index++
		return at, nil
	}}

	first, err := store.attachedWorkerTransactionTime(context.Background(), &stateTx{})
	if err != nil || attachedWorkerClaimExpired(first, expiresAt) {
		t.Fatalf("first attempt at %v: expired=%t err=%v", first, attachedWorkerClaimExpired(first, expiresAt), err)
	}
	retry, err := store.attachedWorkerTransactionTime(context.Background(), &stateTx{})
	if err != nil || !attachedWorkerClaimExpired(retry, expiresAt) {
		t.Fatalf("retry at exact expiry %v: expired=%t err=%v", retry, attachedWorkerClaimExpired(retry, expiresAt), err)
	}
}

func TestAttachedWorkerClaimAmbiguousCommitReplayRequiresExactPristineTarget(t *testing.T) {
	at := time.Date(2026, 8, 25, 10, 0, 0, 123456000, time.UTC)
	enrollment := attachedWorkerStoreEnrollmentFixture(at)
	mutation := ports.AttachedWorkerClaimMutation{
		TenantID: enrollment.TenantID, OwnerUserID: enrollment.OwnerUserID, EnrollmentID: enrollment.ID,
		ExpectedEnrollmentRevision: 1, PresentedAudience: enrollment.Audience,
		PresentedDigest:   enrollment.BootstrapDigest,
		IdentityPublicKey: bytes.Repeat([]byte{0x44}, ed25519.PublicKeySize),
	}
	enrollment.ConsumedAt = at
	enrollment.Revision = 2
	worker, audit := attachedWorkerClaimTarget(enrollment, mutation, at)
	if !sameAppliedAttachedWorkerClaim(enrollment, worker, audit, mutation) {
		t.Fatal("exact ambiguous-commit replay was not recognized")
	}

	subMicroAudit := audit
	subMicroAudit.OccurredAt = subMicroAudit.OccurredAt.Add(500 * time.Nanosecond)
	if !sameAppliedAttachedWorkerClaim(enrollment, worker, subMicroAudit, mutation) {
		t.Fatal("YDB sub-microsecond timestamp normalization broke exact replay")
	}

	renamed := worker
	renamed.DisplayName = "renamed"
	renamed.Revision = 2
	renamed.UpdatedAt = at.Add(time.Second)
	if sameAppliedAttachedWorkerClaim(enrollment, renamed, audit, mutation) {
		t.Fatal("replay after rename was accepted as the pristine claim")
	}

	wrongKey := mutation
	wrongKey.IdentityPublicKey = bytes.Repeat([]byte{0x55}, ed25519.PublicKeySize)
	if sameAppliedAttachedWorkerClaim(enrollment, worker, audit, wrongKey) {
		t.Fatal("replay with a different proved identity key was accepted")
	}
}

func TestAttachedWorkerMutationAmbiguousCommitReplayRejectsMismatchedTarget(t *testing.T) {
	at := time.Date(2026, 8, 25, 10, 0, 0, 123456000, time.UTC)
	enrollment := attachedWorkerStoreEnrollmentFixture(at)
	claim := ports.AttachedWorkerClaimMutation{
		TenantID: enrollment.TenantID, OwnerUserID: enrollment.OwnerUserID, EnrollmentID: enrollment.ID,
		ExpectedEnrollmentRevision: 1, PresentedAudience: enrollment.Audience,
		PresentedDigest:   enrollment.BootstrapDigest,
		IdentityPublicKey: bytes.Repeat([]byte{0x44}, ed25519.PublicKeySize),
	}
	worker, _ := attachedWorkerClaimTarget(enrollment, claim, at)
	next := worker
	next.DisplayName = "renamed"
	next.Revision = 2
	next.UpdatedAt = at.Add(time.Second)
	audit := domain.AttachedWorkerAuditEvent{
		Version:  domain.AttachedWorkerAuditEventVersionV1,
		TenantID: next.TenantID, OwnerUserID: next.OwnerUserID, WorkerID: next.ID,
		Action: domain.AttachedWorkerAuditWorkerRenamed, WorkerRevision: next.Revision,
		EnrollmentGeneration: next.EnrollmentGeneration, ConnectionGeneration: next.ConnectionGeneration,
		OccurredAt: next.UpdatedAt,
	}
	if !sameAppliedAttachedWorkerMutation(next, audit, next, audit) {
		t.Fatal("exact CAS/revoke ambiguous-commit target was not recognized")
	}
	mismatch := next
	mismatch.ConnectionGeneration++
	if sameAppliedAttachedWorkerMutation(next, audit, mismatch, audit) {
		t.Fatal("mismatched CAS/revoke target was accepted")
	}
}

func TestAttachedWorkerCreationReconciliationUsesYDBTimestampPrecision(t *testing.T) {
	at := time.Date(2026, 8, 25, 10, 0, 0, 123456100, time.UTC)
	left := attachedWorkerStoreEnrollmentFixture(at)
	right := left
	right.CreatedAt = right.CreatedAt.Add(700 * time.Nanosecond)
	right.ExpiresAt = right.ExpiresAt.Add(700 * time.Nanosecond)
	right.RetainUntil = right.RetainUntil.Add(700 * time.Nanosecond)
	if !sameAttachedWorkerEnrollment(left, right) {
		t.Fatal("sub-microsecond enrollment retry did not reconcile")
	}
	right.CreatedAt = right.CreatedAt.Add(time.Microsecond)
	if sameAttachedWorkerEnrollment(left, right) {
		t.Fatal("materially different enrollment creation target reconciled")
	}
}

func attachedWorkerStoreEnrollmentFixture(createdAt time.Time) domain.AttachedWorkerEnrollment {
	return domain.AttachedWorkerEnrollment{
		TenantID: "tenant-test", OwnerUserID: "owner-test",
		ID: "enrollment-test", WorkerID: "worker-test", DisplayName: "test worker",
		Audience: "worker://test", BootstrapDigest: domain.DigestWorkerBootstrap([]byte("secret")),
		CreatedAt: createdAt, ExpiresAt: createdAt.Add(time.Minute), RetainUntil: createdAt.Add(time.Hour), Revision: 1,
	}
}
