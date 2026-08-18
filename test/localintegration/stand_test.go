//go:build localintegration

package localintegration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
	"gitcode.com/urandon/sessionless/internal/telegramingress"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
)

func TestLocalStandContracts(t *testing.T) {
	t.Run("YDB", testYDB)
	t.Run("S3 tenant isolation", testS3TenantIsolation)
	t.Run("SQS at least once and dead letter", testSQS)
	t.Run("Telegram capture", testTelegramFake)
	t.Run("Telegram webhook deduplication", testTelegramWebhook)
	t.Run("Telegram command state and durable replies", testTelegramCommands)
	t.Run("Subscription admission and dispatch", testSchedulerDispatch)
}

func testYDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := ydbclient.Open(ctx, envOrDefault(
		"YDB_CONNECTION_STRING",
		"grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric",
	))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("close YDB: %v", err)
		}
	})
	if err := client.DB.PingContext(ctx); err != nil {
		t.Fatalf("ping YDB: %v", err)
	}
}

func testS3TenantIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := s3store.New(ctx, s3store.Config{
		Endpoint:        envOrDefault("S3_ENDPOINT", "http://localhost:9000"),
		Region:          envOrDefault("S3_REGION", "us-east-1"),
		Bucket:          envOrDefault("S3_BUCKET", "sessionless-local"),
		AccessKeyID:     envOrDefault("S3_ACCESS_KEY_ID", "sessionless-local"),
		SecretAccessKey: envOrDefault("S3_SECRET_ACCESS_KEY", "sessionless-local-secret"),
		ForcePathStyle:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("synthetic tenant A fixture\n")
	ref, err := store.Put(ctx, "tenant-a", "fixtures/input.txt", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = store.Delete(context.Background(), "tenant-a", ref)
	})
	if ref.Key != "tenants/tenant-a/fixtures/input.txt" {
		t.Fatalf("key = %q", ref.Key)
	}

	reader, err := store.Open(ctx, "tenant-a", ref)
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read=%v close=%v", readErr, closeErr)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content = %q", got)
	}

	_, err = store.Open(ctx, "tenant-b", ref)
	var mismatch domain.TenantMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("cross-tenant open error = %v, want TenantMismatchError", err)
	}
	_, err = store.Put(ctx, "tenant-b", ref.Key, bytes.NewReader(content))
	if !errors.As(err, &mismatch) {
		t.Fatalf("cross-tenant put error = %v, want TenantMismatchError", err)
	}
}

func testSQS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	queue := openQueue(t, envOrDefault(
		"DISPATCH_QUEUE_URL",
		"http://localhost:9324/000000000000/sessionless-dispatch",
	), envOrDefault(
		"DEAD_LETTER_QUEUE_URL",
		"http://localhost:9324/000000000000/sessionless-dlq",
	))
	envelope := queuecontract.Envelope{
		Schema:     queuecontract.SchemaV1,
		MessageID:  domain.MessageID(fmt.Sprintf("local-contract-message-%d", time.Now().UTC().UnixNano())),
		Kind:       queuecontract.KindDispatchRun,
		TenantID:   "tenant-a",
		SubjectID:  fmt.Sprintf("run-local-contract-%d", time.Now().UTC().UnixNano()),
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(ctx, envelope); err != nil {
		t.Fatal(err)
	}
	first := receiveMatching(t, ctx, queue, func(message ports.ReceivedMessage) bool {
		return message.Envelope.MessageID == envelope.MessageID
	})
	if first.Envelope.MessageID != envelope.MessageID || first.DeliveryCount != 1 {
		t.Fatalf("first delivery = %#v", first)
	}
	if err := queue.Retry(ctx, first.ReceiptHandle, 0); err != nil {
		t.Fatal(err)
	}
	second := receiveMatching(t, ctx, queue, func(message ports.ReceivedMessage) bool {
		return message.Envelope.MessageID == envelope.MessageID
	})
	if second.Envelope.MessageID != envelope.MessageID || second.DeliveryCount < 2 {
		t.Fatalf("second delivery = %#v", second)
	}
	if err := queue.DeadLetter(ctx, second.ReceiptHandle, "contract_test"); err != nil {
		t.Fatal(err)
	}

	deadLetter := openQueue(t, envOrDefault(
		"DEAD_LETTER_QUEUE_URL",
		"http://localhost:9324/000000000000/sessionless-dlq",
	), "")
	moved := receiveMatching(t, ctx, deadLetter, func(message ports.ReceivedMessage) bool {
		return message.Envelope.MessageID == envelope.MessageID
	})
	if moved.Envelope.MessageID != envelope.MessageID {
		t.Fatalf("dead-letter envelope = %#v", moved.Envelope)
	}
	if err := deadLetter.Ack(ctx, moved.ReceiptHandle); err != nil {
		t.Fatal(err)
	}
}

func testTelegramFake(t *testing.T) {
	baseURL := envOrDefault("TELEGRAM_API_BASE_URL", "http://localhost:8081")
	token := envOrDefault("TELEGRAM_FAKE_TOKEN", "local-test-token")
	client := &http.Client{Timeout: 5 * time.Second}
	postJSON(t, client, baseURL+"/test/reset", []byte(`{}`))

	fixture, err := os.ReadFile(filepath.Join("..", "fixtures", "telegram", "text-message.json"))
	if err != nil {
		t.Fatal(err)
	}
	postJSON(t, client, baseURL+"/test/updates", fixture)
	postJSON(t, client, baseURL+"/test/updates", fixture)

	var updates struct {
		OK     bool              `json:"ok"`
		Result []json.RawMessage `json:"result"`
	}
	getJSON(t, client, baseURL+"/bot"+token+"/getUpdates", &updates)
	if !updates.OK || len(updates.Result) != 1 {
		t.Fatalf("updates = %#v", updates)
	}

	postJSON(t, client, baseURL+"/bot"+token+"/sendMessage", []byte(`{"chat_id":777001,"text":"synthetic reply"}`))
	var captures struct {
		OK     bool `json:"ok"`
		Result []struct {
			Method string `json:"method"`
			ChatID int64  `json:"chat_id"`
		} `json:"result"`
	}
	getJSON(t, client, baseURL+"/test/captures", &captures)
	if !captures.OK || len(captures.Result) != 1 {
		t.Fatalf("captures = %#v", captures)
	}
	if captures.Result[0].Method != "sendMessage" || captures.Result[0].ChatID != 777001 {
		t.Fatalf("capture = %#v", captures.Result[0])
	}
}

func testTelegramWebhook(t *testing.T) {
	baseURL := envOrDefault("SESSIONLESS_BASE_URL", "http://localhost:8080")
	secret := envOrDefault("TELEGRAM_WEBHOOK_SECRET", "local-webhook-secret")
	fixture, err := os.ReadFile(filepath.Join("..", "fixtures", "telegram", "text-message.json"))
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 2; attempt++ {
		request, err := http.NewRequest(
			http.MethodPost, baseURL+"/telegram/webhook", bytes.NewReader(fixture),
		)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("webhook attempt %d status = %d", attempt+1, response.StatusCode)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ydb, err := ydbclient.Open(ctx, envOrDefault(
		"YDB_CONNECTION_STRING",
		"grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric",
	))
	if err != nil {
		t.Fatal(err)
	}
	defer ydb.Close(context.Background())
	resolver, err := telegramingress.NewIdentityResolver([]byte(envOrDefault(
		"TELEGRAM_IDENTITY_HMAC_KEY",
		"sessionless-local-identity-key-0001",
	)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := resolver.ResolvePrivate(700000001, 700000001, "codex")
	if err != nil {
		t.Fatal(err)
	}
	var count uint64
	if err := ydb.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM frontend_ingress_idempotency
		 WHERE tenant_id = $1`,
		identity.Tenant,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("canonical Telegram ingress count = %d, want 1", count)
	}
}

func testTelegramCommands(t *testing.T) {
	controlURL := envOrDefault("SESSIONLESS_BASE_URL", "http://localhost:8080")
	telegramURL := envOrDefault("TELEGRAM_API_BASE_URL", "http://localhost:8081")
	secret := envOrDefault("TELEGRAM_WEBHOOK_SECRET", "local-webhook-secret")
	client := &http.Client{Timeout: 10 * time.Second}
	postJSON(t, client, telegramURL+"/test/reset", []byte(`{}`))

	baseUpdateID := int64(1_000_000_000) + time.Now().UTC().UnixMilli()%900_000_000
	chatID := baseUpdateID
	commands := []string{
		"/connect codex",
		"/compute status",
		"/new",
		"/compute disconnect codex",
	}
	for index, command := range commands {
		fixture, err := json.Marshal(map[string]any{
			"update_id": baseUpdateID + int64(index),
			"message": map[string]any{
				"message_id": baseUpdateID + int64(index),
				"from":       map[string]any{"id": chatID},
				"chat":       map[string]any{"id": chatID, "type": "private"},
				"date":       time.Now().UTC().Unix(),
				"text":       command,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		postWebhook(t, client, controlURL+"/telegram/webhook", secret, fixture)
	}

	var captures struct {
		OK     bool `json:"ok"`
		Result []struct {
			Method  string          `json:"method"`
			ChatID  int64           `json:"chat_id"`
			Request json.RawMessage `json:"request"`
		} `json:"result"`
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		getJSON(t, client, telegramURL+"/test/captures", &captures)
		if captures.OK && len(captures.Result) == len(commands) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("command reply captures = %#v", captures)
		}
		time.Sleep(250 * time.Millisecond)
	}
	var sawStatus, sawNewSession, sawDisconnect bool
	for _, capture := range captures.Result {
		if capture.Method != "sendMessage" || capture.ChatID != chatID {
			t.Fatalf("command capture = %#v", capture)
		}
		var payload struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(capture.Request, &payload); err != nil {
			t.Fatal(err)
		}
		sawStatus = sawStatus || strings.Contains(payload.Text, "reauthentication_required")
		sawNewSession = sawNewSession || strings.Contains(payload.Text, "new session was created")
		sawDisconnect = sawDisconnect || strings.Contains(payload.Text, "connection disconnected")
	}
	if !sawStatus || !sawNewSession || !sawDisconnect {
		t.Fatalf(
			"command replies missing: status=%t new_session=%t disconnect=%t",
			sawStatus, sawNewSession, sawDisconnect,
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ydb, err := ydbclient.Open(ctx, envOrDefault(
		"YDB_CONNECTION_STRING",
		"grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric",
	))
	if err != nil {
		t.Fatal(err)
	}
	defer ydb.Close(context.Background())
	resolver, err := telegramingress.NewIdentityResolver([]byte(envOrDefault(
		"TELEGRAM_IDENTITY_HMAC_KEY",
		"sessionless-local-identity-key-0001",
	)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := resolver.ResolvePrivate(chatID, chatID, "codex")
	if err != nil {
		t.Fatal(err)
	}
	var commandRuns, switchedBindings uint64
	if err := ydb.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM telegram_updates
		 WHERE tenant_id = $1 AND source_id = $2
		   AND update_id >= $3 AND update_id < $4`,
		identity.Tenant, "bot-primary",
		baseUpdateID, baseUpdateID+int64(len(commands)),
	).Scan(&commandRuns); err != nil {
		t.Fatal(err)
	}
	if commandRuns != uint64(len(commands)) {
		t.Fatalf("command update count = %d, want %d", commandRuns, len(commands))
	}
	if err := ydb.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM frontend_bindings
		 WHERE tenant_id = $1 AND frontend = $2
		   AND external_conversation_id = $3 AND revision = $4`,
		identity.Tenant, domain.FrontendTelegram,
		identity.Conversation.ExternalID, uint64(2),
	).Scan(&switchedBindings); err != nil {
		t.Fatal(err)
	}
	if switchedBindings != 1 {
		t.Fatalf("canonical switched binding count = %d, want 1", switchedBindings)
	}
	var entitlement string
	if err := ydb.DB.QueryRowContext(ctx,
		`SELECT entitlement_state FROM subscription_connections
		 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
		identity.Tenant, identity.SubscriptionConnection,
	).Scan(&entitlement); err != nil {
		t.Fatal(err)
	}
	if entitlement != string(domain.EntitlementDisconnected) {
		t.Fatalf("connection state = %q, want disconnected", entitlement)
	}
}

func testSchedulerDispatch(t *testing.T) {
	controlURL := envOrDefault("SESSIONLESS_BASE_URL", "http://localhost:8080")
	secret := envOrDefault("TELEGRAM_WEBHOOK_SECRET", "local-webhook-secret")
	updateID := int64(1_900_000_000) + time.Now().UTC().UnixMilli()%200_000_000
	chatID := int64(99101)
	fixture, err := json.Marshal(map[string]any{
		"update_id": updateID,
		"message": map[string]any{
			"message_id": updateID,
			"from":       map[string]any{"id": chatID},
			"chat":       map[string]any{"id": chatID, "type": "private"},
			"date":       time.Now().UTC().Unix(),
			"text":       "schedule this deterministic local workload",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	postWebhook(
		t,
		&http.Client{Timeout: 10 * time.Second},
		controlURL+"/telegram/webhook",
		secret,
		fixture,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ydb, err := ydbclient.Open(ctx, envOrDefault(
		"YDB_CONNECTION_STRING",
		"grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric",
	))
	if err != nil {
		t.Fatal(err)
	}
	defer ydb.Close(context.Background())
	resolver, err := telegramingress.NewIdentityResolver([]byte(envOrDefault(
		"TELEGRAM_IDENTITY_HMAC_KEY",
		"sessionless-local-identity-key-0001",
	)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := resolver.ResolvePrivate(chatID, chatID, "codex")
	if err != nil {
		t.Fatal(err)
	}
	runID := canonicalRunForExternalEvent(
		t, ctx, ydb.DB, identity.Tenant, fmt.Sprintf("bot-primary:%d", updateID),
	)
	execEventually(t, ctx, ydb.DB,
		`UPDATE subscription_connections
		 SET entitlement_state = $1, quota_state = $2, updated_at = CurrentUtcTimestamp()
		 WHERE tenant_id = $3 AND subscription_connection_id = $4`,
		domain.EntitlementActive, domain.ProviderQuotaUnknown,
		identity.Tenant, identity.SubscriptionConnection,
	)
	execEventually(t, ctx, ydb.DB,
		`UPDATE subscription_scheduler_slots
		 SET state = $1, blocked_until = $2, updated_at = CurrentUtcTimestamp()
		 WHERE tenant_id = $3 AND subscription_connection_id = $4`,
		domain.SchedulerReady, time.Unix(0, 0).UTC(),
		identity.Tenant, identity.SubscriptionConnection,
	)
	var outboxID domain.DispatchOutboxID
	if err := ydb.DB.QueryRowContext(ctx,
		`SELECT dispatch_outbox_id FROM dispatch_outbox
		 WHERE tenant_id = $1 AND run_id = $2`, identity.Tenant, runID,
	).Scan(&outboxID); err != nil {
		t.Fatal(err)
	}
	wakeQueue := openQueue(t, envOrDefault(
		"SCHEDULER_WAKE_QUEUE_URL",
		"http://localhost:9324/000000000000/sessionless-scheduler-wake",
	), envOrDefault(
		"DEAD_LETTER_QUEUE_URL",
		"http://localhost:9324/000000000000/sessionless-dlq",
	))
	wakePublisher, err := outboxwake.NewPublisher(wakeQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err := wakePublisher.PublishDispatchWake(ctx, identity.Tenant, outboxID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	queue := openQueue(t, envOrDefault(
		"DISPATCH_QUEUE_URL",
		"http://localhost:9324/000000000000/sessionless-dispatch",
	), envOrDefault(
		"DEAD_LETTER_QUEUE_URL",
		"http://localhost:9324/000000000000/sessionless-dlq",
	))
	message := receiveMatching(t, ctx, queue, func(message ports.ReceivedMessage) bool {
		return message.Envelope.Kind == queuecontract.KindDispatchRun &&
			message.Envelope.TenantID == identity.Tenant && message.Envelope.SubjectID == runID
	})
	if message.Envelope.Kind != queuecontract.KindDispatchRun ||
		message.Envelope.TenantID != identity.Tenant ||
		message.Envelope.SubjectID != runID {
		t.Fatalf("scheduler envelope = %#v", message.Envelope)
	}
	if err := queue.Ack(ctx, message.ReceiptHandle); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := ydb.DB.QueryRowContext(ctx,
		`SELECT status FROM runs WHERE tenant_id = $1 AND run_id = $2`,
		identity.Tenant, runID,
	).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.RunQueued) {
		t.Fatalf("run status = %q, want queued", status)
	}
}

func receiveMatching(
	t *testing.T,
	ctx context.Context,
	queue *sqsqueue.Queue,
	matches func(ports.ReceivedMessage) bool,
) ports.ReceivedMessage {
	t.Helper()
	for {
		message := receiveEventually(t, ctx, queue)
		if matches(message) {
			return message
		}
		if err := queue.Retry(ctx, message.ReceiptHandle, time.Second); err != nil {
			t.Fatal(err)
		}
	}
}

func execEventually(t *testing.T, ctx context.Context, db *sql.DB, query string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var err error
	for {
		if _, err = db.ExecContext(ctx, query, args...); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("YDB update did not stabilize: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func canonicalRunForExternalEvent(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	tenantID domain.TenantID,
	externalEventID string,
) string {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		`SELECT payload FROM dispatch_outbox WHERE tenant_id = $1`, tenantID,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			t.Fatal(err)
		}
		var outbox domain.DispatchOutbox
		if err := json.Unmarshal([]byte(payload), &outbox); err != nil {
			t.Fatal(err)
		}
		if outbox.Origin != nil && outbox.Origin.Frontend == domain.FrontendTelegram &&
			outbox.Origin.ExternalEventID == externalEventID {
			return string(outbox.RunID)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("canonical dispatch for external event %q not found", externalEventID)
	return ""
}

func postWebhook(
	t *testing.T,
	client *http.Client,
	url string,
	secret string,
	body []byte,
) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("webhook status = %d", response.StatusCode)
	}
}

func openQueue(t *testing.T, queueURL, deadLetterURL string) *sqsqueue.Queue {
	t.Helper()
	queue, err := sqsqueue.New(context.Background(), sqsqueue.Config{
		Endpoint:        envOrDefault("QUEUE_ENDPOINT", "http://localhost:9324"),
		Region:          envOrDefault("QUEUE_REGION", "us-east-1"),
		QueueURL:        queueURL,
		DeadLetterURL:   deadLetterURL,
		AccessKeyID:     envOrDefault("QUEUE_ACCESS_KEY_ID", "sessionless-local"),
		SecretAccessKey: envOrDefault("QUEUE_SECRET_ACCESS_KEY", "sessionless-local-secret"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

func receiveEventually(t *testing.T, ctx context.Context, queue *sqsqueue.Queue) ports.ReceivedMessage {
	t.Helper()
	for {
		message, err := queue.Receive(ctx)
		if err == nil {
			return message
		}
		if !errors.Is(err, sqsqueue.ErrNoMessage) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func postJSON(t *testing.T, client *http.Client, url string, body []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("POST %s: status=%d body=%s", url, response.StatusCode, data)
	}
}

func getJSON(t *testing.T, client *http.Client, url string, target any) {
	t.Helper()
	response, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("GET %s: status=%d body=%s", url, response.StatusCode, data)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatal(err)
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
