//go:build ydbintegration

package ydbintegration

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestAttachedWorkerSealedInputAuthorizationReadsClaimedHeadTransactionally(t *testing.T) {
	store, client, worker, connection, secretDigest, _, _, now := readyAttachedWorkerForDrain(t, "sealed-input")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	offer := attachedWorkerOfferForDrain(t, store, client, worker, connection, now, "sealed-input")
	batch, err := attachedworkerprotocol.DecodeBatchV1(offer.Outbound.Payload)
	if err != nil || len(batch.Frames) != 1 || batch.Frames[0].LeaseOffer == nil {
		t.Fatalf("decode offered attempt: frames=%d error=%v", len(batch.Frames), err)
	}
	offerFrame := batch.Frames[0]
	connection, found, err := store.LoadAttachedWorkerConnection(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("offered connection = %#v found=%t err=%v", connection, found, err)
	}
	request := ports.AttachedWorkerSealedInputAuthorization{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, PresentedSecretDigest: secretDigest,
		EnrollmentGeneration: connection.EnrollmentGeneration, ConnectionGeneration: connection.ConnectionGeneration,
		RunID: offer.Attempt.RunID, AttemptID: offer.Attempt.AttemptID, AttemptSequence: 1,
		LeaseID: offer.Attempt.LeaseID, LeaseGeneration: offer.Attempt.LeaseGeneration,
		FenceToken: offer.Attempt.FenceToken, LeaseExpiresAtUnixMicro: offer.Attempt.LeaseExpiresAt.UnixMicro(),
		ContextDigest: offer.Attempt.ContextDigest, CapabilityDigest: offer.Attempt.CapabilityDigest,
		PolicyDigest: offer.Attempt.PolicyDigest,
	}
	if result, err := store.AuthorizeAttachedWorkerSealedInput(ctx, request); err != nil || result.Status != ports.AttachedWorkerExecutionDenied {
		t.Fatalf("offered attempt authorized: result=%+v error=%v", result, err)
	}
	claimFrame := attachedworkerprotocol.FrameV1{
		Version:   offerFrame.Version,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, connection.WorkerSequence+1),
		WorkerID:  string(worker.ID), EnrollmentGeneration: connection.EnrollmentGeneration,
		ConnectionGeneration: connection.ConnectionGeneration, Sequence: connection.WorkerSequence + 1,
		Ack: offerFrame.Sequence, Kind: attachedworkerprotocol.MessageLeaseClaim,
		LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: offerFrame.LeaseOffer.Binding, AttemptSequence: 1},
	}
	claim, err := store.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, AttemptID: offer.Attempt.AttemptID, LeaseGeneration: offer.Attempt.LeaseGeneration,
		PresentedSecretDigest: secretDigest, InboundFrame: claimFrame,
	})
	if err != nil || claim.Status != ports.AttachedWorkerExecutionApplied || claim.Attempt.State != domain.AttachedWorkerAttemptClaimed {
		t.Fatalf("claim attempt: result=%+v error=%v", claim, err)
	}
	result, err := store.AuthorizeAttachedWorkerSealedInput(ctx, request)
	if err != nil || result.Status != ports.AttachedWorkerExecutionApplied || result.AttemptRevision != claim.Attempt.Revision {
		t.Fatalf("claimed head authorization: result=%+v error=%v want revision=%d", result, err, claim.Attempt.Revision)
	}
	for _, test := range []struct {
		name   string
		change func(*ports.AttachedWorkerSealedInputAuthorization)
	}{
		{name: "foreign owner", change: func(r *ports.AttachedWorkerSealedInputAuthorization) {
			r.OwnerUserID = domain.UserID(uniqueID("foreign-owner"))
		}},
		{name: "wrong bearer", change: func(r *ports.AttachedWorkerSealedInputAuthorization) {
			r.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("wrong bearer"))
		}},
		{name: "rotated connection", change: func(r *ports.AttachedWorkerSealedInputAuthorization) {
			r.ConnectionID = domain.AttachedWorkerConnectionID(uniqueID("foreign-connection"))
		}},
		{name: "mismatched generation", change: func(r *ports.AttachedWorkerSealedInputAuthorization) { r.ConnectionGeneration++ }},
		{name: "changed head revision", change: func(r *ports.AttachedWorkerSealedInputAuthorization) {
			r.ExpectedAttemptRevision = result.AttemptRevision + 1
		}},
		{name: "changed expiry", change: func(r *ports.AttachedWorkerSealedInputAuthorization) { r.LeaseExpiresAtUnixMicro++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := request
			test.change(&candidate)
			denied, err := store.AuthorizeAttachedWorkerSealedInput(ctx, candidate)
			if err != nil || denied.Status != ports.AttachedWorkerExecutionDenied || denied.AttemptRevision != 0 {
				t.Fatalf("divergent durable head authorized: result=%+v error=%v", denied, err)
			}
		})
	}
	worker, found, err = store.LoadAttachedWorker(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("worker before revoke = %#v found=%t err=%v", worker, found, err)
	}
	revoked := worker
	revoked.DesiredState = domain.AttachedWorkerDesiredRevoked
	revoked.EnrollmentGeneration++
	revoked.ConnectionGeneration++
	revoked.Revision++
	revoked.UpdatedAt = worker.UpdatedAt.Add(time.Microsecond).UTC().Truncate(time.Microsecond)
	revoked.RevokedAt = revoked.UpdatedAt
	revokeAudit := attachedWorkerMutationAudit(revoked, domain.AttachedWorkerAuditWorkerRevoked, revoked.UpdatedAt)
	didRevoke, err := store.RevokeAttachedWorker(ctx, ports.AttachedWorkerRevokeMutation{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ExpectedRevision: worker.Revision, Next: revoked, Audit: revokeAudit, At: revoked.UpdatedAt,
	})
	if err != nil || !didRevoke {
		t.Fatalf("revoke after claim = %t, %v", didRevoke, err)
	}
	if denied, err := store.AuthorizeAttachedWorkerSealedInput(ctx, request); err != nil || denied.Status != ports.AttachedWorkerExecutionDenied {
		t.Fatalf("revoked durable worker authorized: result=%+v error=%v", denied, err)
	}
}

func TestAttachedWorkerAttemptDeadlinePaginationIsLosslessAndBounded(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	tenantID := domain.TenantID("tenant-attempt-deadline-" + rand.Text())
	ownerID := domain.UserID(uniqueID("owner-attempt-deadline"))
	workerID := domain.AttachedWorkerID(uniqueID("worker-attempt-deadline"))
	attemptID := domain.AttemptID(uniqueID("attempt-deadline"))
	deadlineAt := time.Now().UTC().Truncate(time.Microsecond)
	bucket, err := domain.AttachedWorkerAttemptDeadlineBucketV1(tenantID, ownerID, workerID, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	start := ports.AttachedWorkerAttemptDeadlineCursor{Present: true, DeadlineAt: deadlineAt,
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: workerID, AttemptID: attemptID}
	cleanupAttemptDeadlineFixture(t, client.DB, bucket, start)
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

	first, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, start, 1)
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
	tenantID := domain.TenantID("tenant-attempt-poison-" + rand.Text())
	ownerID := domain.UserID(uniqueID("owner-attempt-poison"))
	workerID := domain.AttachedWorkerID(uniqueID("worker-attempt-poison"))
	attemptID := domain.AttemptID(uniqueID("attempt-poison"))
	deadlineAt := time.Now().UTC().Truncate(time.Microsecond)
	bucket, err := domain.AttachedWorkerAttemptDeadlineBucketV1(tenantID, ownerID, workerID, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	start := ports.AttachedWorkerAttemptDeadlineCursor{Present: true, DeadlineAt: deadlineAt,
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: workerID, AttemptID: attemptID}
	cleanupAttemptDeadlineFixture(t, client.DB, bucket, start)
	for _, kind := range []string{"aaa_invalid", string(domain.AttachedWorkerDeadlineLeaseExpiry)} {
		if _, err := client.DB.ExecContext(ctx,
			`INSERT INTO attached_worker_attempt_deadlines_v1
			 (shard_bucket,deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind,lease_generation,attempt_revision)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			bucket, deadlineAt, tenantID, ownerID, workerID, attemptID, kind, uint64(7), uint64(1)); err != nil {
			t.Fatal(err)
		}
	}
	// Buckets are shared with other fixtures. Seek immediately before our raw
	// key rather than assuming this shard contains no older unrelated row.
	poison, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, start, 1)
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
	cleanupAttemptDeadlineFixture(t, client.DB, bucket, ports.AttachedWorkerAttemptDeadlineCursor{DeadlineAt: deadlineAt})
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
	nonce := rand.Text()
	foundBucket := false
	for candidate := 0; candidate < 4096; candidate++ {
		tenantID = domain.TenantID(fmt.Sprintf("tenant-after-empty-poison-%s-%d", nonce, candidate))
		ownerID = domain.UserID(uniqueID(fmt.Sprintf("owner-after-empty-poison-%d", candidate)))
		workerID = domain.AttachedWorkerID(uniqueID(fmt.Sprintf("worker-after-empty-poison-%d", candidate)))
		attemptID = domain.AttemptID(uniqueID(fmt.Sprintf("attempt-after-empty-poison-%d", candidate)))
		validBucket, err := domain.AttachedWorkerAttemptDeadlineBucketV1(tenantID, ownerID, workerID, attemptID)
		if err != nil {
			t.Fatal(err)
		}
		if validBucket == bucket {
			foundBucket = true
			break
		}
	}
	if !foundBucket {
		t.Fatal("could not construct a bucket-zero fixture within 4096 candidates")
	}
	cleanupAttemptDeadlineFixture(t, client.DB, bucket, ports.AttachedWorkerAttemptDeadlineCursor{
		DeadlineAt: deadlineAt, TenantID: tenantID, OwnerUserID: ownerID, WorkerID: workerID, AttemptID: attemptID})
	if _, err := client.DB.ExecContext(ctx,
		`INSERT INTO attached_worker_attempt_deadlines_v1
		 (shard_bucket,deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind,lease_generation,attempt_revision)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		bucket, deadlineAt, tenantID, ownerID, workerID, attemptID, domain.AttachedWorkerDeadlineLeaseExpiry, uint64(1), uint64(1)); err != nil {
		t.Fatal(err)
	}
	// The initial query is shard-global. Keep its positive native coverage,
	// comparing the first physical key rather than assuming our fixture is first.
	expectedFirst := ports.AttachedWorkerAttemptDeadlineCursor{Present: true}
	if err := client.DB.QueryRowContext(ctx, `SELECT deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind
	 FROM attached_worker_attempt_deadlines_v1 WHERE shard_bucket=$1 AND deadline_at <= $2
	 ORDER BY deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind LIMIT 1`, bucket, deadlineAt).Scan(
		&expectedFirst.DeadlineAt, &expectedFirst.TenantID, &expectedFirst.OwnerUserID,
		&expectedFirst.WorkerID, &expectedFirst.AttemptID, &expectedFirst.Kind); err != nil {
		t.Fatalf("read initial physical deadline key: %v", err)
	}
	initial, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, ports.AttachedWorkerAttemptDeadlineCursor{}, 1)
	if err != nil || !initial.NextCursor.DeadlineAt.Equal(expectedFirst.DeadlineAt) {
		t.Fatalf("initial deadline page = %#v, expected first key %#v, error=%v", initial, expectedFirst, err)
	}
	expectedFirst.DeadlineAt = initial.NextCursor.DeadlineAt
	if initial.NextCursor != expectedFirst || uint64(len(initial.Items))+initial.SkippedInvalid != 1 || !initial.HasMore {
		t.Fatalf("initial deadline page = %#v, expected exactly first physical key %#v", initial, expectedFirst)
	}
	// The raw empty key itself must be returned, so seek from the immediately
	// preceding timestamp, not from the beginning of a shared shard. The suite
	// is serial: older fixtures cannot enter this one-microsecond query window.
	start := ports.AttachedWorkerAttemptDeadlineCursor{Present: true, DeadlineAt: deadlineAt.Add(-time.Microsecond)}
	poison, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, start, 1)
	if err != nil || !poison.NextCursor.Present || poison.NextCursor.TenantID != "" || poison.SkippedInvalid != 1 {
		t.Fatalf("empty poison page = %#v, %v", poison, err)
	}
	valid, err := store.ListDueAttachedWorkerAttemptDeadlines(ctx, bucket, deadlineAt, poison.NextCursor, 1)
	if err != nil || len(valid.Items) != 1 || valid.Items[0].TenantID != tenantID || valid.Items[0].Kind != domain.AttachedWorkerDeadlineLeaseExpiry {
		t.Fatalf("continue after empty poison: page=%#v error=%v", valid, err)
	}
}

func cleanupAttemptDeadlineFixture(t *testing.T, db *sql.DB, bucket uint32, key ports.AttachedWorkerAttemptDeadlineCursor) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, `DELETE FROM attached_worker_attempt_deadlines_v1
		 WHERE shard_bucket=$1 AND deadline_at=$2 AND tenant_id=$3 AND owner_user_id=$4 AND worker_id=$5 AND attempt_id=$6`,
			bucket, key.DeadlineAt, key.TenantID, key.OwnerUserID, key.WorkerID, key.AttemptID); err != nil {
			t.Errorf("cleanup owned deadline fixture bucket=%d key=%#v: %v", bucket, key, err)
		}
	})
}
