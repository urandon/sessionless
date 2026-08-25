//go:build ydbintegration

package ydbintegration

import (
	"context"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestAttachedWorkerAttemptDeadlinePaginationIsLosslessAndBounded(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	tenantID := domain.TenantID(uniqueID("tenant-attempt-deadline"))
	ownerID := domain.UserID(uniqueID("owner-attempt-deadline"))
	workerID := domain.AttachedWorkerID(uniqueID("worker-attempt-deadline"))
	attemptID := domain.AttemptID(uniqueID("attempt-deadline"))
	deadlineAt := time.Now().UTC().Truncate(time.Microsecond)
	bucket, err := domain.AttachedWorkerAttemptDeadlineBucketV1(tenantID, ownerID, workerID, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	for index, kind := range []domain.AttachedWorkerAttemptDeadlineKind{
		domain.AttachedWorkerDeadlineCancelAck,
		domain.AttachedWorkerDeadlineLeaseExpiry,
	} {
		if _, err := client.DB.ExecContext(ctx,
			`INSERT INTO attached_worker_attempt_deadlines_v1
			 (shard_bucket,deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind,
			  lease_generation,attempt_revision)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			bucket, deadlineAt, tenantID, ownerID, workerID, attemptID, kind, uint64(7), uint64(index+1),
		); err != nil {
			t.Fatal(err)
		}
	}

	first, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, ports.AttachedWorkerAttemptDeadlineCursor{}, 1)
	if err != nil || len(first) != 1 {
		t.Fatalf("first deadline page = %#v, %v", first, err)
	}
	cursor := ports.AttachedWorkerAttemptDeadlineCursor{
		DeadlineAt: first[0].DeadlineAt, TenantID: first[0].TenantID,
		OwnerUserID: first[0].OwnerUserID, WorkerID: first[0].WorkerID,
		AttemptID: first[0].AttemptID, Kind: first[0].Kind,
	}
	second, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, cursor, 1)
	if err != nil || len(second) != 1 || second[0].Kind == first[0].Kind {
		t.Fatalf("second deadline page = %#v after %#v, %v", second, first, err)
	}
	last := ports.AttachedWorkerAttemptDeadlineCursor{
		DeadlineAt: second[0].DeadlineAt, TenantID: second[0].TenantID,
		OwnerUserID: second[0].OwnerUserID, WorkerID: second[0].WorkerID,
		AttemptID: second[0].AttemptID, Kind: second[0].Kind,
	}
	done, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, last, 1)
	if err != nil || len(done) != 0 {
		t.Fatalf("terminal deadline page = %#v, %v", done, err)
	}
	if _, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, ports.AttachedWorkerAttemptDeadlineCursor{}, 101); err == nil {
		t.Fatal("overlarge deadline page was accepted")
	}
}
