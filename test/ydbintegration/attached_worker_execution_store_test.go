//go:build ydbintegration

package ydbintegration

import (
	"context"
	"fmt"
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
	if err != nil || len(first.Items) != 1 {
		t.Fatalf("first deadline page = %#v, %v", first, err)
	}
	cursor := first.NextCursor
	second, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, cursor, 1)
	if err != nil || len(second.Items) != 1 || second.Items[0].Kind == first.Items[0].Kind {
		t.Fatalf("second deadline page = %#v after %#v, %v", second, first, err)
	}
	last := second.NextCursor
	done, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, last, 1)
	if err != nil || len(done.Items) != 0 {
		t.Fatalf("terminal deadline page = %#v, %v", done, err)
	}
	if _, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, ports.AttachedWorkerAttemptDeadlineCursor{}, 101); err == nil {
		t.Fatal("overlarge deadline page was accepted")
	}
}

func TestAttachedWorkerAttemptDeadlinePageSkipsPoisonRowByRawKey(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	tenantID := domain.TenantID(uniqueID("tenant-attempt-poison"))
	ownerID := domain.UserID(uniqueID("owner-attempt-poison"))
	workerID := domain.AttachedWorkerID(uniqueID("worker-attempt-poison"))
	attemptID := domain.AttemptID(uniqueID("attempt-poison"))
	deadlineAt := time.Now().UTC().Truncate(time.Microsecond)
	bucket, err := domain.AttachedWorkerAttemptDeadlineBucketV1(tenantID, ownerID, workerID, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"aaa_invalid", string(domain.AttachedWorkerDeadlineLeaseExpiry)} {
		if _, err := client.DB.ExecContext(ctx,
			`INSERT INTO attached_worker_attempt_deadlines_v1
			 (shard_bucket,deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind,lease_generation,attempt_revision)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			bucket, deadlineAt, tenantID, ownerID, workerID, attemptID, kind, uint64(7), uint64(1)); err != nil {
			t.Fatal(err)
		}
	}
	poison, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, ports.AttachedWorkerAttemptDeadlineCursor{}, 1)
	if err != nil || len(poison.Items) != 0 || poison.SkippedInvalid != 1 || !poison.HasMore || poison.NextCursor.Kind != "aaa_invalid" {
		t.Fatalf("poison page = %#v, %v", poison, err)
	}
	valid, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, poison.NextCursor, 1)
	if err != nil || len(valid.Items) != 1 || valid.Items[0].Kind != domain.AttachedWorkerDeadlineLeaseExpiry {
		t.Fatalf("valid page after poison = %#v, %v", valid, err)
	}
}

func TestAttachedWorkerAttemptDeadlineCursorRepresentsEmptyRawComponents(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	deadlineAt := time.Now().UTC().Truncate(time.Microsecond)
	bucket := uint32(0)
	if _, err := client.DB.ExecContext(ctx,
		`INSERT INTO attached_worker_attempt_deadlines_v1
		 (shard_bucket,deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind,lease_generation,attempt_revision)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		bucket, deadlineAt, "", "", "", "", "", uint64(0), uint64(0)); err != nil {
		t.Fatal(err)
	}
	var tenantID domain.TenantID
	var ownerID domain.UserID
	var workerID domain.AttachedWorkerID
	var attemptID domain.AttemptID
	for candidate := 0; ; candidate++ {
		tenantID = domain.TenantID(uniqueID(fmt.Sprintf("tenant-after-empty-poison-%d", candidate)))
		ownerID = domain.UserID(uniqueID(fmt.Sprintf("owner-after-empty-poison-%d", candidate)))
		workerID = domain.AttachedWorkerID(uniqueID(fmt.Sprintf("worker-after-empty-poison-%d", candidate)))
		attemptID = domain.AttemptID(uniqueID(fmt.Sprintf("attempt-after-empty-poison-%d", candidate)))
		validBucket, err := domain.AttachedWorkerAttemptDeadlineBucketV1(tenantID, ownerID, workerID, attemptID)
		if err != nil {
			t.Fatal(err)
		}
		if validBucket == bucket {
			break
		}
	}
	if _, err := client.DB.ExecContext(ctx,
		`INSERT INTO attached_worker_attempt_deadlines_v1
		 (shard_bucket,deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind,lease_generation,attempt_revision)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		bucket, deadlineAt, tenantID, ownerID, workerID, attemptID, domain.AttachedWorkerDeadlineLeaseExpiry, uint64(1), uint64(1)); err != nil {
		t.Fatal(err)
	}
	poison, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, ports.AttachedWorkerAttemptDeadlineCursor{}, 1)
	if err != nil || !poison.NextCursor.Present || poison.NextCursor.TenantID != "" || poison.SkippedInvalid != 1 {
		t.Fatalf("empty poison page = %#v, %v", poison, err)
	}
	if _, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, poison.NextCursor, 1); err != nil {
		t.Fatalf("continue after empty poison: %v", err)
	}
}
