//go:build ydbintegration

package ydbintegration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbmigrate"
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
	assertCount(t, client, "telegram_updates", tenantID, 1)
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
		ID:       domain.RunID(runID),
		TenantID: tenantID,
		Conversation: domain.ConversationRef{
			TenantID: tenantID, Frontend: domain.FrontendTelegram,
			ExternalID: "442211", ID: domain.ConversationID("conversation-" + string(tenantID)),
		},
		SubscriptionConnectionID: domain.SubscriptionConnectionID("subscription-" + string(tenantID)),
		ContextEpoch:             domain.InitialContextEpoch,
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
		IdempotencyKey: domain.IdempotencyKey("dispatch-key-" + runID),
		CreatedAt:      now, UpdatedAt: now,
	}
	return ydbstore.TelegramIngress{
		TenantID: tenantID, SourceID: "telegram-bot-primary", UpdateID: updateID,
		ExpireAt: now.Add(24 * time.Hour), Run: run, Attempt: attempt, Dispatch: dispatch,
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
