//go:build ydbintegration

package ydbintegration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbmigrate"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
	ydbmigrations "gitcode.com/urandon/sessionless/migrations/ydb"
)

var idSequence atomic.Uint64

func TestMigrationsAreRepeatable(t *testing.T) {
	connectionString := requireConnectionString(t)
	for run := 1; run <= 2; run++ {
		migrator, err := ydbmigrate.New(ydbmigrate.Config{
			ConnectionString: connectionString,
			OwnerID:          fmt.Sprintf("integration-migrate-%d-%d", run, time.Now().UnixNano()),
		}, ydbmigrations.Files)
		if err != nil {
			t.Fatal(err)
		}
		if err := migrator.Up(context.Background()); err != nil {
			t.Fatalf("migration run %d failed: %v", run, err)
		}
	}
}

func TestPartitioningContractMatchesYDBLocal(t *testing.T) {
	_, client := openStore(t)
	report, err := ydbpartition.Inspect(
		context.Background(),
		client.Table(),
		client.DatabasePath(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Valid {
		for _, table := range report.Tables {
			if !table.MatchesContract {
				t.Errorf("%s: %v", table.PhysicalTable, table.ContractViolations)
			}
		}
		t.FailNow()
	}
}

func TestConcurrentDuplicateTelegramUpdateCreatesOneRunAndOutbox(t *testing.T) {
	store, client := openStore(t)
	tenantID := domain.TenantID(uniqueID("tenant-dedupe"))
	now := time.Now().UTC().Truncate(time.Microsecond)

	const contenders = 8
	results := make(chan ydbstore.TelegramIngressResult, contenders)
	errs := make(chan error, contenders)
	var wait sync.WaitGroup
	for contender := 0; contender < contenders; contender++ {
		contender := contender
		wait.Add(1)
		go func() {
			defer wait.Done()
			request := ingressFixture(
				tenantID,
				fmt.Sprintf("run-%s-%d", tenantID, contender),
				int64(991337),
				now,
			)
			result, err := store.IngestTelegram(context.Background(), request)
			results <- result
			errs <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent ingress failed: %v", err)
		}
	}
	var canonical domain.RunID
	for result := range results {
		if canonical == "" {
			canonical = result.RunID
		}
		if result.RunID != canonical {
			t.Fatalf("duplicate update resolved to %q and %q", canonical, result.RunID)
		}
	}
	assertCount(t, client, "runs", tenantID, 1)
	assertCount(t, client, "attempts", tenantID, 1)
	assertCount(t, client, "dispatch_outbox", tenantID, 1)
	assertCount(t, client, "dispatch_ready", tenantID, 1)
	assertCount(t, client, "dispatch_ready_v2", tenantID, 1)
	assertCount(t, client, "telegram_updates", tenantID, 1)
	assertCount(t, client, "artifact_manifests", tenantID, 1)
}

func TestTelegramIdentityInitializationIsIdempotent(t *testing.T) {
	store, client := openStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-identity"))
	request := ports.TelegramIdentityRequest{
		TenantID: tenantID,
		Actor: domain.ActorRef{
			TenantID: tenantID, Frontend: domain.FrontendTelegram,
			ExternalID: "2001", ID: domain.ActorID(uniqueID("actor")),
		},
		Conversation: domain.ConversationRef{
			TenantID: tenantID, Frontend: domain.FrontendTelegram,
			ExternalID: "1001", ID: domain.ConversationID(uniqueID("conversation")),
		},
		SubscriptionConnectionID: domain.SubscriptionConnectionID(uniqueID("subscription")),
		Provider:                 "codex", ObservedAt: now,
	}
	for run := 0; run < 2; run++ {
		state, err := store.EnsureTelegramIdentity(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if state.LegacyContextRevision != 1 {
			t.Fatalf("legacy context revision = %d", state.LegacyContextRevision)
		}
	}
	assertCount(t, client, "tenants", tenantID, 1)
	assertCount(t, client, "actors", tenantID, 1)
	assertCount(t, client, "conversations", tenantID, 1)
	assertCount(t, client, "subscription_connections", tenantID, 1)
}

func TestConcurrentLeaseClaimHasExactlyOneWinner(t *testing.T) {
	store, client := openStore(t)
	tenantID := domain.TenantID(uniqueID("tenant-lease"))
	now := time.Now().UTC().Truncate(time.Microsecond)
	ingress := ingressFixture(tenantID, uniqueID("run"), 5512, now)
	if _, err := store.IngestTelegram(context.Background(), ingress); err != nil {
		t.Fatal(err)
	}

	claims := []ydbstore.LeaseClaim{
		{
			TenantID: tenantID, RunID: ingress.Run.ID, AttemptID: ingress.Attempt.ID,
			LeaseID: domain.LeaseID(uniqueID("lease-a")), WorkerID: "worker-a",
			Now: now.Add(time.Second), ExpiresAt: now.Add(time.Minute),
		},
		{
			TenantID: tenantID, RunID: ingress.Run.ID, AttemptID: ingress.Attempt.ID,
			LeaseID: domain.LeaseID(uniqueID("lease-b")), WorkerID: "worker-b",
			Now: now.Add(time.Second), ExpiresAt: now.Add(time.Minute),
		},
	}
	errs := make(chan error, len(claims))
	var wait sync.WaitGroup
	for _, claim := range claims {
		claim := claim
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := store.ClaimLease(context.Background(), claim)
			errs <- err
		}()
	}
	wait.Wait()
	close(errs)

	var winners, held int
	for err := range errs {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ydbstore.ErrLeaseHeld):
			held++
		default:
			t.Fatalf("unexpected lease result: %v", err)
		}
	}
	if winners != 1 || held != 1 {
		t.Fatalf("winners=%d held=%d, want exactly one of each", winners, held)
	}
	assertCount(t, client, "lease_heads", tenantID, 1)
	assertCount(t, client, "lease_expiry", tenantID, 1)
	assertCount(t, client, "lease_expiry_v2", tenantID, 1)
	bucket, err := ydbpartition.BucketV1(string(ingress.Run.ID))
	if err != nil {
		t.Fatal(err)
	}
	expired, err := store.ListExpiredLeasesByBucket(
		context.Background(),
		bucket,
		now.Add(2*time.Minute),
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 || expired[0].TenantID != tenantID {
		t.Fatalf("bucket lease result = %+v", expired)
	}
	var winningLeaseID domain.LeaseID
	var winningFence uint64
	if err := client.DB.QueryRowContext(context.Background(),
		`SELECT lease_id, fence_token FROM lease_heads
		 WHERE tenant_id = $1 AND run_id = $2`,
		tenantID, ingress.Run.ID,
	).Scan(&winningLeaseID, &winningFence); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := store.ClaimLease(context.Background(), ydbstore.LeaseClaim{
		TenantID: tenantID, RunID: ingress.Run.ID, AttemptID: ingress.Attempt.ID,
		LeaseID: winningLeaseID, WorkerID: "worker-recovered",
		Now: now.Add(2 * time.Minute), ExpiresAt: now.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.FenceToken != winningFence+1 || reclaimed.WorkerID != "worker-recovered" {
		t.Fatalf("reclaimed lease = %+v, previous fence = %d", reclaimed, winningFence)
	}
	assertCount(t, client, "lease_expiry", tenantID, 1)
	assertCount(t, client, "lease_expiry_v2", tenantID, 1)
}

func TestConcurrentSchedulerAdmissionReservesOneSubscriptionSlot(t *testing.T) {
	store, client := openStore(t)
	tenantID := domain.TenantID(uniqueID("tenant-scheduler"))
	now := time.Now().UTC().Truncate(time.Microsecond)
	connectionID := domain.SubscriptionConnectionID("subscription-" + string(tenantID))
	actor := domain.ActorRef{
		TenantID: tenantID, Frontend: domain.FrontendTelegram,
		ExternalID: "scheduler-user", ID: domain.ActorID(uniqueID("actor")),
	}
	conversation := domain.ConversationRef{
		TenantID: tenantID, Frontend: domain.FrontendTelegram,
		ExternalID: "scheduler-chat", ID: domain.ConversationID("conversation-" + string(tenantID)),
	}
	if _, err := store.EnsureTelegramIdentity(context.Background(), ports.TelegramIdentityRequest{
		TenantID: tenantID, Actor: actor, Conversation: conversation,
		SubscriptionConnectionID: connectionID, Provider: "codex",
		ObservedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB.ExecContext(context.Background(),
		`UPDATE subscription_connections
		 SET entitlement_state = $1, quota_state = $2, updated_at = $3
		 WHERE tenant_id = $4 AND subscription_connection_id = $5`,
		domain.EntitlementActive, domain.ProviderQuotaUnknown, now,
		tenantID, connectionID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB.ExecContext(context.Background(),
		`UPDATE subscription_scheduler_slots
		 SET state = $1, updated_at = $2
		 WHERE tenant_id = $3 AND subscription_connection_id = $4`,
		domain.SchedulerReady, now, tenantID, connectionID,
	); err != nil {
		t.Fatal(err)
	}

	first := ingressFixture(tenantID, uniqueID("run-scheduler-a"), 6101, now)
	second := ingressFixture(tenantID, uniqueID("run-scheduler-b"), 6102, now)
	for _, ingress := range []ydbstore.TelegramIngress{first, second} {
		if _, err := store.IngestTelegram(context.Background(), ingress); err != nil {
			t.Fatal(err)
		}
	}
	limits := domain.ProductLimits{
		MaxTenantQueueDepth: 8, MaxActiveRuns: 1,
		MaxRuntime: 15 * time.Minute, MaxTurns: 30,
		MaxInputBytes: 16 << 20, MaxContextBytes: 64 << 20,
		MaxArtifacts: 32,
	}
	requests := []ports.DispatchAdmissionRequest{
		admissionFixture(first, domain.QuotaReservationID(uniqueID("reservation-a")), now, limits),
		admissionFixture(second, domain.QuotaReservationID(uniqueID("reservation-b")), now, limits),
	}
	results := make(chan ports.DispatchAdmissionResult, len(requests))
	errs := make(chan error, len(requests))
	var wait sync.WaitGroup
	for _, request := range requests {
		request := request
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := store.AdmitDispatch(context.Background(), request)
			results <- result
			errs <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("scheduler admission failed: %v", err)
		}
	}
	var admitted int
	var admittedRequest ports.DispatchAdmissionRequest
	for result := range results {
		if result.Admitted {
			admitted++
			for _, request := range requests {
				if request.RunID == result.RunID {
					admittedRequest = request
				}
			}
		}
	}
	if admitted != 1 {
		t.Fatalf("admitted = %d, want exactly one", admitted)
	}
	assertCount(t, client, "quota_reservations", tenantID, 1)
	var queueDepth, activeRuns uint32
	if err := client.DB.QueryRowContext(context.Background(),
		`SELECT queue_depth, active_runs FROM tenant_scheduler_counters
		 WHERE tenant_id = $1`,
		tenantID,
	).Scan(&queueDepth, &activeRuns); err != nil {
		t.Fatal(err)
	}
	if queueDepth != 1 || activeRuns != 0 {
		t.Fatalf("scheduler counters = queue:%d active:%d", queueDepth, activeRuns)
	}

	bucket, err := ydbpartition.BucketV1(string(admittedRequest.ReservationID))
	if err != nil {
		t.Fatal(err)
	}
	expired, err := store.ListExpiredQuotaReservations(
		context.Background(), bucket, now.Add(2*time.Minute), 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 {
		t.Fatalf("expired reservations = %#v", expired)
	}
	didExpire, err := store.ExpireQuotaReservation(
		context.Background(), expired[0], now.Add(2*time.Minute),
	)
	if err != nil || !didExpire {
		t.Fatalf("expire result = %t, %v", didExpire, err)
	}
	didExpire, err = store.ExpireQuotaReservation(
		context.Background(), expired[0], now.Add(2*time.Minute),
	)
	if err != nil || didExpire {
		t.Fatalf("idempotent expire result = %t, %v", didExpire, err)
	}
	assertCount(t, client, "quota_expiry_v2", tenantID, 0)
}

func TestWorkerLifecycleCommitsResultAndClearsLeaseIndexes(t *testing.T) {
	store, client := openStore(t)
	tenantID := domain.TenantID(uniqueID("tenant-worker"))
	now := time.Now().UTC().Truncate(time.Microsecond)
	connectionID := domain.SubscriptionConnectionID("subscription-" + string(tenantID))
	actor := domain.ActorRef{
		TenantID: tenantID, Frontend: domain.FrontendTelegram,
		ExternalID: "worker-user", ID: domain.ActorID(uniqueID("actor")),
	}
	conversation := domain.ConversationRef{
		TenantID: tenantID, Frontend: domain.FrontendTelegram,
		ExternalID: "worker-chat", ID: domain.ConversationID("conversation-" + string(tenantID)),
	}
	if _, err := store.EnsureTelegramIdentity(context.Background(), ports.TelegramIdentityRequest{
		TenantID: tenantID, Actor: actor, Conversation: conversation,
		SubscriptionConnectionID: connectionID, Provider: "codex", ObservedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB.ExecContext(context.Background(),
		`UPDATE subscription_connections
		 SET entitlement_state = $1, quota_state = $2, updated_at = $3
		 WHERE tenant_id = $4 AND subscription_connection_id = $5`,
		domain.EntitlementActive, domain.ProviderQuotaUnknown, now,
		tenantID, connectionID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB.ExecContext(context.Background(),
		`UPDATE subscription_scheduler_slots
		 SET state = $1, updated_at = $2
		 WHERE tenant_id = $3 AND subscription_connection_id = $4`,
		domain.SchedulerReady, now, tenantID, connectionID,
	); err != nil {
		t.Fatal(err)
	}
	ingress := ingressFixture(tenantID, uniqueID("run-worker"), 6201, now)
	if _, err := store.IngestTelegram(context.Background(), ingress); err != nil {
		t.Fatal(err)
	}
	reservationID := domain.QuotaReservationID(uniqueID("reservation-worker"))
	admission, err := store.AdmitDispatch(
		context.Background(),
		admissionFixture(ingress, reservationID, now, domain.ProductLimits{
			MaxTenantQueueDepth: 8, MaxActiveRuns: 1,
			MaxRuntime: 15 * time.Minute, MaxTurns: 30,
			MaxInputBytes: 16 << 20, MaxContextBytes: 64 << 20, MaxArtifacts: 32,
		}),
	)
	if err != nil || !admission.Admitted {
		t.Fatalf("admission = %+v, %v", admission, err)
	}
	loaded, found, err := store.LoadWorkerJob(context.Background(), tenantID, ingress.Run.ID)
	if err != nil || !found {
		t.Fatalf("load worker job = found:%t error:%v", found, err)
	}
	if loaded.Checkpoint != nil || loaded.Job.ReservationID != reservationID {
		t.Fatalf("initial worker state = %+v", loaded)
	}
	lease, err := store.ClaimWorkerLease(context.Background(), ports.WorkerLeaseRequest{
		TenantID: tenantID, RunID: ingress.Run.ID, AttemptID: ingress.Attempt.ID,
		LeaseID: domain.LeaseID(uniqueID("lease-worker")), WorkerID: "worker-integration",
		Now: now.Add(2 * time.Second), ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartWorkerJob(context.Background(), loaded, lease, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	checkpoint := domain.Checkpoint{
		ID: domain.CheckpointID(uniqueID("checkpoint")), TenantID: tenantID,
		RunID: ingress.Run.ID, AttemptID: ingress.Attempt.ID, Sequence: 1,
		State: domain.BlobRef{
			TenantID: tenantID,
			Key:      domain.TenantObjectPrefix(tenantID) + "runs/checkpoint.json",
			Size:     2, SHA256: strings.Repeat("1", 64),
		},
		CreatedAt: now.Add(3 * time.Second),
	}
	if err := store.CommitWorkerEvent(context.Background(), ports.WorkerEventCommit{
		Checkpoint: checkpoint, LeaseID: lease.ID, Fence: lease.FenceToken,
		At: checkpoint.CreatedAt,
	}); err != nil {
		t.Fatal(err)
	}
	lease, err = store.RenewWorkerLease(
		context.Background(), tenantID, lease.ID, lease.FenceToken,
		now.Add(30*time.Second), now.Add(2*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	finishedAt := now.Add(40 * time.Second)
	manifest := domain.ArtifactManifest{
		ID:       domain.ArtifactManifestID(uniqueID("manifest-output")),
		TenantID: tenantID, RunID: ingress.Run.ID, CreatedAt: finishedAt,
	}
	delivery := domain.TelegramDeliveryOutbox{
		ID:       domain.TelegramDeliveryID(uniqueID("delivery-worker")),
		TenantID: tenantID, RunID: ingress.Run.ID,
		Chat: ingress.Dispatch.DeliveryChat, ReplyToMessageID: ingress.Dispatch.ReplyToMessageID,
		Text: "done", Status: domain.DeliveryPending,
		IdempotencyKey: domain.IdempotencyKey(uniqueID("delivery-key")),
		CreatedAt:      finishedAt, UpdatedAt: finishedAt,
	}
	if err := store.CompleteWorkerJob(context.Background(), ports.WorkerCompletion{
		TenantID: tenantID, RunID: ingress.Run.ID, AttemptID: ingress.Attempt.ID,
		ReservationID: reservationID, LeaseID: lease.ID, Fence: lease.FenceToken,
		At: finishedAt, Manifest: manifest, Delivery: delivery,
	}); err != nil {
		t.Fatal(err)
	}
	terminal, found, err := store.LoadWorkerJob(context.Background(), tenantID, ingress.Run.ID)
	if err != nil || !found {
		t.Fatalf("load terminal worker job = found:%t error:%v", found, err)
	}
	if terminal.Run.Status != domain.RunSucceeded || terminal.Attempt.Status != domain.AttemptSucceeded ||
		terminal.Reservation.Status != domain.ReservationCommitted ||
		terminal.Checkpoint == nil || terminal.Checkpoint.Sequence != 1 {
		t.Fatalf("terminal worker state = %+v", terminal)
	}
	assertCount(t, client, "worker_jobs", tenantID, 1)
	assertCount(t, client, "lease_heads", tenantID, 0)
	assertCount(t, client, "lease_expiry", tenantID, 0)
	assertCount(t, client, "lease_expiry_v2", tenantID, 0)
	assertCount(t, client, "telegram_delivery_outbox", tenantID, 1)
}

func TestTenantScopedReadsCannotCrossTenant(t *testing.T) {
	store, _ := openStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantA := domain.TenantID(uniqueID("tenant-a"))
	tenantB := domain.TenantID(uniqueID("tenant-b"))
	ingress := ingressFixture(tenantA, uniqueID("run-a"), 7711, now)
	if _, err := store.IngestTelegram(context.Background(), ingress); err != nil {
		t.Fatal(err)
	}
	err := store.Transact(context.Background(), tenantB, func(tx ports.StateTx) error {
		run, found, err := tx.GetRun(context.Background(), ingress.Run.ID)
		if err != nil {
			return err
		}
		if found || run.ID != "" {
			return errors.New("tenant B read tenant A run")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func admissionFixture(
	ingress ydbstore.TelegramIngress,
	reservationID domain.QuotaReservationID,
	now time.Time,
	limits domain.ProductLimits,
) ports.DispatchAdmissionRequest {
	return ports.DispatchAdmissionRequest{
		TenantID: ingress.TenantID, OutboxID: ingress.Dispatch.ID,
		RunID: ingress.Run.ID, AttemptID: ingress.Attempt.ID,
		ReservationID: reservationID, Now: now.Add(time.Second),
		HoldUntil: now.Add(time.Minute), Limits: limits,
		Workload: domain.WorkloadShape{
			Runtime: 5 * time.Minute, Turns: 1,
			InputBytes: 1024, ContextBytes: 2048, Artifacts: 1,
		},
	}
}

func TestQuotaAndDeliveryDualWriteBucketedReadyTables(t *testing.T) {
	store, client := openStore(t)
	tenantID := domain.TenantID(uniqueID("tenant-ready"))
	now := time.Now().UTC().Truncate(time.Microsecond)
	ingress := ingressFixture(tenantID, uniqueID("run-ready"), 8811, now)
	if _, err := store.IngestTelegram(context.Background(), ingress); err != nil {
		t.Fatal(err)
	}
	reservation := domain.QuotaReservation{
		ID:       domain.QuotaReservationID(uniqueID("reservation")),
		TenantID: tenantID, RunID: ingress.Run.ID,
		SubscriptionConnectionID: ingress.Run.SubscriptionConnectionID,
		Status:                   domain.ReservationHeld, CapacityUnits: 1,
		HeldAt: now, ExpiresAt: now.Add(5 * time.Minute), UpdatedAt: now,
	}
	delivery := domain.TelegramDeliveryOutbox{
		ID:       domain.TelegramDeliveryID(uniqueID("delivery")),
		TenantID: tenantID, RunID: ingress.Run.ID,
		Chat:             domain.TelegramChatRef{TenantID: tenantID, ChatID: 442211},
		ReplyToMessageID: 19,
		Payload: domain.BlobRef{
			TenantID: tenantID,
			Key:      domain.TenantObjectPrefix(tenantID) + "results/reply.json",
			SHA256:   strings.Repeat("00", 32),
		},
		Status:         domain.DeliveryPending,
		IdempotencyKey: domain.IdempotencyKey(uniqueID("delivery-key")),
		CreatedAt:      now, UpdatedAt: now,
	}
	err := store.Transact(context.Background(), tenantID, func(tx ports.StateTx) error {
		if err := tx.PutQuotaReservation(context.Background(), reservation); err != nil {
			return err
		}
		return tx.PutTelegramDeliveryOutbox(context.Background(), delivery)
	})
	if err != nil {
		t.Fatal(err)
	}
	assertCount(t, client, "quota_expiry", tenantID, 1)
	assertCount(t, client, "quota_expiry_v2", tenantID, 1)
	assertCount(t, client, "telegram_delivery_ready", tenantID, 1)
	assertCount(t, client, "telegram_delivery_ready_v2", tenantID, 1)
	bucket, err := ydbpartition.BucketV1(string(delivery.ID))
	if err != nil {
		t.Fatal(err)
	}
	ready, err := store.ListReadyTelegramDeliveries(
		context.Background(), bucket, now.Add(time.Second), 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 1 || ready[0].TenantID != tenantID || ready[0].DeliveryID != delivery.ID {
		t.Fatalf("ready deliveries = %+v", ready)
	}
	claimed, ok, err := store.ClaimTelegramDelivery(
		context.Background(), tenantID, delivery.ID, now.Add(time.Second),
	)
	if err != nil || !ok || claimed.Status != domain.DeliverySending ||
		claimed.AttemptCount != 1 {
		t.Fatalf("delivery claim = %+v, %t, %v", claimed, ok, err)
	}

	retryAt := now.Add(time.Minute)
	if err := store.TransitionTelegramDelivery(
		context.Background(), tenantID, delivery.ID,
		domain.DeliveryRetryWait, now.Add(2*time.Second), &retryAt,
	); err != nil {
		t.Fatal(err)
	}
	assertCount(t, client, "telegram_delivery_ready", tenantID, 1)
	assertCount(t, client, "telegram_delivery_ready_v2", tenantID, 1)
	readyAfterRetry, err := store.ListReadyTelegramDeliveries(
		context.Background(), bucket, now.Add(3*time.Minute), 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(readyAfterRetry) != 1 ||
		readyAfterRetry[0].TenantID != tenantID ||
		readyAfterRetry[0].DeliveryID != delivery.ID {
		t.Fatalf("ready deliveries after retry transition = %+v", readyAfterRetry)
	}

	if _, err := ydbpartition.BackfillReadyExpiryV2(
		context.Background(),
		client.DB,
		false,
	); err != nil {
		t.Fatal(err)
	}
	assertCount(t, client, "dispatch_ready_v2", tenantID, 1)
	assertCount(t, client, "quota_expiry_v2", tenantID, 1)
	assertCount(t, client, "telegram_delivery_ready_v2", tenantID, 1)
}

func TestTelegramCommandsAreAtomicIdempotentAndDoNotDispatchAIWork(t *testing.T) {
	store, client := openStore(t)
	tenantID := domain.TenantID(uniqueID("tenant-command"))
	now := time.Now().UTC().Truncate(time.Microsecond)
	actor := domain.ActorRef{
		TenantID: tenantID, Frontend: domain.FrontendTelegram,
		ExternalID: "5511", ID: domain.ActorID(uniqueID("actor")),
	}
	conversation := domain.ConversationRef{
		TenantID: tenantID, Frontend: domain.FrontendTelegram,
		ExternalID: "7711", ID: domain.ConversationID(uniqueID("conversation")),
	}
	connectionID := domain.SubscriptionConnectionID(uniqueID("subscription"))
	if _, err := store.EnsureTelegramIdentity(context.Background(), ports.TelegramIdentityRequest{
		TenantID: tenantID, Actor: actor, Conversation: conversation,
		SubscriptionConnectionID: connectionID, Provider: "codex",
		ObservedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	connect := commandFixture(
		tenantID, actor, conversation, connectionID,
		ports.TelegramCommandConnectCodex, 2001, now,
	)
	first, err := store.ExecuteTelegramCommand(context.Background(), connect)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.ExecuteTelegramCommand(context.Background(), connect)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || duplicate.Created || duplicate.RunID != first.RunID {
		t.Fatalf("connect dedup = %#v then %#v", first, duplicate)
	}

	status := commandFixture(
		tenantID, actor, conversation, connectionID,
		ports.TelegramCommandComputeStatus, 2002, now.Add(time.Second),
	)
	if _, err := store.ExecuteTelegramCommand(context.Background(), status); err != nil {
		t.Fatal(err)
	}
	claimedStatus, ok, err := store.ClaimTelegramDelivery(
		context.Background(), tenantID, status.DeliveryID, now.Add(2*time.Second),
	)
	if err != nil || !ok {
		t.Fatalf("claim status reply = %t, %v", ok, err)
	}
	if !strings.Contains(claimedStatus.Text, "reauthentication_required") ||
		claimedStatus.Payload.Key != "" {
		t.Fatalf("status reply = %#v", claimedStatus)
	}

	newContext := commandFixture(
		tenantID, actor, conversation, connectionID,
		ports.TelegramCommandNewContext, 2003, now.Add(3*time.Second),
	)
	if _, err := store.ExecuteTelegramCommand(context.Background(), newContext); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ExecuteTelegramCommand(context.Background(), newContext); err != nil {
		t.Fatal(err)
	}
	identity, err := store.EnsureTelegramIdentity(context.Background(), ports.TelegramIdentityRequest{
		TenantID: tenantID, Actor: actor, Conversation: conversation,
		SubscriptionConnectionID: connectionID, Provider: "codex",
		ObservedAt: now.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if identity.LegacyContextRevision != 2 {
		t.Fatalf("legacy context revision = %d, want 2", identity.LegacyContextRevision)
	}

	disconnect := commandFixture(
		tenantID, actor, conversation, connectionID,
		ports.TelegramCommandDisconnectCodex, 2004, now.Add(5*time.Second),
	)
	if _, err := store.ExecuteTelegramCommand(context.Background(), disconnect); err != nil {
		t.Fatal(err)
	}
	var entitlement, credentialRef string
	if err := client.DB.QueryRowContext(context.Background(),
		`SELECT entitlement_state, credential_ref FROM subscription_connections
		 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
		tenantID, connectionID,
	).Scan(&entitlement, &credentialRef); err != nil {
		t.Fatal(err)
	}
	if entitlement != string(domain.EntitlementDisconnected) || credentialRef != "" {
		t.Fatalf("disconnected state = %q, credential ref = %q", entitlement, credentialRef)
	}

	assertCount(t, client, "telegram_updates", tenantID, 4)
	assertCount(t, client, "runs", tenantID, 4)
	assertCount(t, client, "telegram_delivery_outbox", tenantID, 4)
	assertCount(t, client, "context_epochs", tenantID, 1)
	assertCount(t, client, "attempts", tenantID, 0)
	assertCount(t, client, "dispatch_outbox", tenantID, 0)
}

func openStore(t *testing.T) (*ydbstore.Store, *ydbclient.Client) {
	t.Helper()
	connectionString := requireConnectionString(t)
	migrator, err := ydbmigrate.New(ydbmigrate.Config{
		ConnectionString: connectionString,
		OwnerID:          uniqueID("integration-schema"),
	}, ydbmigrations.Files)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrator.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	client, err := ydbclient.Open(context.Background(), connectionString)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("close YDB client: %v", err)
		}
	})
	store, err := ydbstore.New(client.DB, ydbstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return store, client
}

func commandFixture(
	tenantID domain.TenantID,
	actor domain.ActorRef,
	conversation domain.ConversationRef,
	connectionID domain.SubscriptionConnectionID,
	kind ports.TelegramCommandKind,
	updateID int64,
	now time.Time,
) ports.TelegramCommandRequest {
	suffix := fmt.Sprintf("%d-%s", updateID, kind)
	return ports.TelegramCommandRequest{
		TenantID: tenantID, SourceID: "telegram-bot-primary",
		UpdateID: updateID, ExpireAt: now.Add(24 * time.Hour),
		Kind: kind, Provider: "codex", Actor: actor, Conversation: conversation,
		SubscriptionConnectionID: connectionID,
		RunID:                    domain.RunID("run-command-" + suffix),
		SessionID:                domain.SessionID("session-command-" + suffix),
		TriggerEventID:           domain.SessionEventID("event-command-" + suffix),
		DeliveryID:               domain.TelegramDeliveryID("delivery-command-" + suffix),
		Chat: domain.TelegramChatRef{
			TenantID: tenantID,
			ChatID:   7711,
		},
		ReplyToMessageID: updateID,
		IdempotencyKey:   domain.IdempotencyKey("telegram-command-" + suffix),
		RequestedAt:      now,
	}
}

func requireConnectionString(t *testing.T) string {
	t.Helper()
	value := os.Getenv("YDB_CONNECTION_STRING")
	if value == "" {
		t.Fatal("YDB_CONNECTION_STRING is required for ydbintegration tests")
	}
	return value
}

func ingressFixture(
	tenantID domain.TenantID,
	runID string,
	updateID int64,
	now time.Time,
) ydbstore.TelegramIngress {
	run := domain.Run{
		ID:                       domain.RunID(runID),
		TenantID:                 tenantID,
		SessionID:                domain.SessionID("session-" + runID),
		TriggerEventID:           domain.SessionEventID("event-" + runID),
		SubscriptionConnectionID: domain.SubscriptionConnectionID("subscription-" + string(tenantID)),
		Status:                   domain.RunCreated,
		IdempotencyKey:           domain.IdempotencyKey(fmt.Sprintf("telegram-%s-%d", tenantID, updateID)),
		CreatedAt:                now,
		UpdatedAt:                now,
	}
	attempt := domain.Attempt{
		ID: domain.AttemptID("attempt-" + runID), TenantID: tenantID, RunID: run.ID,
		Number: 1, Status: domain.AttemptCreated, CreatedAt: now, UpdatedAt: now,
	}
	dispatch := domain.DispatchOutbox{
		ID: domain.DispatchOutboxID("dispatch-" + runID), TenantID: tenantID,
		RunID: run.ID, AttemptID: attempt.ID, Status: domain.DispatchPending,
		InputManifestID: domain.ArtifactManifestID("manifest-" + runID),
		ContextSnapshot: domain.BlobRef{
			TenantID: tenantID, Key: "tenants/" + string(tenantID) + "/context/" + runID,
			Size: 1, SHA256: strings.Repeat("0", 64),
		},
		DeliveryChat:     domain.TelegramChatRef{TenantID: tenantID, ChatID: 442211},
		ReplyToMessageID: updateID,
		IdempotencyKey:   domain.IdempotencyKey("dispatch-key-" + runID),
		CreatedAt:        now, UpdatedAt: now,
	}
	manifest := domain.ArtifactManifest{
		ID: domain.ArtifactManifestID("manifest-" + runID), TenantID: tenantID,
		RunID: run.ID, CreatedAt: now,
	}
	return ydbstore.TelegramIngress{
		TenantID: tenantID, SourceID: "telegram-bot-primary", UpdateID: updateID,
		ExpireAt: now.Add(24 * time.Hour), Run: run, Attempt: attempt,
		InputManifest: manifest, Dispatch: dispatch,
	}
}

func uniqueID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, idSequence.Add(1))
}

func assertCount(
	t *testing.T,
	client *ydbclient.Client,
	table string,
	tenantID domain.TenantID,
	want uint64,
) {
	t.Helper()
	var got uint64
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE tenant_id = $1", table)
	if err := client.DB.QueryRowContext(context.Background(), query, tenantID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", table, got, want)
	}
}
