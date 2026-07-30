//go:build e2elocal

package e2e

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
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
	"gitcode.com/urandon/sessionless/internal/telegramingress"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const (
	webhookSecret  = "local-webhook-secret"
	identityKey    = "sessionless-local-identity-key-0001"
	telegramSource = "bot-primary"
)

type localSlice struct {
	t       *testing.T
	ctx     context.Context
	cancel  context.CancelFunc
	db      *sql.DB
	closeDB func()
	state   *ydbstore.Store
	blobs   *s3store.Store
	queue   *sqsqueue.Queue
	client  *http.Client

	controlURL  string
	telegramURL string
}

type runRef struct {
	UpdateID     int64
	MessageID    int64
	ChatID       int64
	TenantID     domain.TenantID
	Conversation domain.ConversationRef
	ConnectionID domain.SubscriptionConnectionID
	RunID        domain.RunID
}

type capture struct {
	MessageID int64           `json:"message_id"`
	Method    string          `json:"method"`
	ChatID    int64           `json:"chat_id"`
	Request   json.RawMessage `json:"request"`
}

func TestDeterministicLocalMultiUserSlice(t *testing.T) {
	if os.Getenv("SESSIONLESS_E2E") != "1" {
		t.Skip("set SESSIONLESS_E2E=1 and start the local stand")
	}
	slice := newLocalSlice(t)
	defer slice.close()
	slice.reset()

	base := time.Now().UTC().UnixMilli()
	userA := int64(881_001)
	userB := int64(881_002)

	t.Run("interleaved tenants, duplicate update, delivery retry and duplicate queue delivery", func(t *testing.T) {
		slice.injectTelegramFailure("sendMessage", 1, http.StatusTooManyRequests)

		// Deliberately deliver the larger update first, then repeat it. Ordering
		// across users is irrelevant; idempotency is scoped to the Bot API update.
		runA := slice.postMessage(base+2, userA, "tenant A deterministic request")
		runB := slice.postMessageWithDocument(
			base+1, userB, "tenant B deterministic request with a file",
			"e2e-file-b", "notes.txt", []byte("synthetic tenant B attachment\n"),
		)
		duplicate := slice.postMessage(base+2, userA, "tenant A duplicate delivery")
		if duplicate.RunID != runA.RunID {
			t.Fatalf("duplicate update run = %s, want %s", duplicate.RunID, runA.RunID)
		}

		slice.waitRunStatus(runA, domain.RunQuotaBlocked)
		slice.waitRunStatus(runB, domain.RunQuotaBlocked)
		slice.setConnectionReady(runA)
		slice.setConnectionReady(runB)
		slice.waitRunStatus(runA, domain.RunQueued)
		slice.waitRunStatus(runB, domain.RunQueued)

		slice.runWorker(nil)
		slice.runWorker(nil)
		slice.waitRunStatus(runA, domain.RunSucceeded)
		slice.waitRunStatus(runB, domain.RunSucceeded)
		slice.waitForChatMethods(map[int64]map[string]int{
			userA: {"sendMessage": 1, "sendDocument": 1},
			userB: {"sendMessage": 1, "sendDocument": 1},
		})

		slice.assertOneTelegramRun(runA)
		slice.assertOneTelegramRun(runB)
		slice.assertUsage(runA, 2)
		slice.assertUsage(runB, 2)
		slice.assertInputDocument(runB, "attachment-01-notes.txt")
		slice.assertTenantArtifacts(runA, runB)
		slice.assertDeliveryWasRetried(runA, runB)

		before := len(slice.captures())
		slice.publishDuplicate(runA)
		slice.runWorker(nil)
		time.Sleep(750 * time.Millisecond)
		if after := len(slice.captures()); after != before {
			t.Fatalf("duplicate terminal delivery produced captures: before=%d after=%d", before, after)
		}
	})

	t.Run("retry before the first checkpoint", func(t *testing.T) {
		run := slice.postMessage(base+10, userA, "retry before checkpoint")
		slice.setConnectionReady(run)
		slice.waitRunStatus(run, domain.RunQueued)
		slice.runWorker(map[string]string{
			"DETERMINISTIC_HARNESS_FAIL_BEFORE_FIRST_TURN": "true",
			"DETERMINISTIC_HARNESS_RETRYABLE_FAIL":         "true",
		})
		slice.assertCheckpointCount(run, 0)
		time.Sleep(1200 * time.Millisecond)
		slice.runWorker(nil)
		slice.waitRunStatus(run, domain.RunSucceeded)
		slice.assertCheckpointCount(run, 2)
	})

	t.Run("retry resumes after a durable checkpoint", func(t *testing.T) {
		run := slice.postMessage(base+11, userB, "retry after checkpoint")
		slice.setConnectionReady(run)
		slice.waitRunStatus(run, domain.RunQueued)
		slice.runWorker(map[string]string{
			"DETERMINISTIC_HARNESS_FAIL_AT_TURN":   "1",
			"DETERMINISTIC_HARNESS_RETRYABLE_FAIL": "true",
		})
		slice.assertCheckpointCount(run, 1)
		time.Sleep(1200 * time.Millisecond)
		slice.runWorker(nil)
		slice.waitRunStatus(run, domain.RunSucceeded)
		slice.assertCheckpointCount(run, 2)
	})

	t.Run("durable cancellation releases the run and replies in the same chat", func(t *testing.T) {
		run := slice.postMessage(base+12, userA, "cancel this run")
		slice.setConnectionReady(run)
		slice.waitRunStatus(run, domain.RunQueued)
		before := len(slice.capturesForChat(userA))
		slice.requestCancellation(run)
		slice.runWorker(nil)
		slice.waitRunStatus(run, domain.RunCancelled)
		slice.waitCaptureIncrease(userA, before)
	})

	t.Run("provider quota block recovers without API billing fallback", func(t *testing.T) {
		run := slice.postMessage(base+13, userB, "wait for provider reset")
		resetAt := time.Now().UTC().Add(10 * time.Minute)
		slice.setConnectionState(
			run,
			domain.EntitlementActive,
			domain.ProviderQuotaExhausted,
			domain.SchedulerBlockedUntilReset,
			&resetAt,
		)
		slice.waitRunStatus(run, domain.RunQuotaBlocked)
		slice.setConnectionReady(run)
		slice.waitRunStatus(run, domain.RunQueued)
		slice.runWorker(nil)
		slice.waitRunStatus(run, domain.RunSucceeded)
	})

	t.Run("admitted dispatch is republished after a queue outage", func(t *testing.T) {
		slice.compose("stop", "queue-local")
		run := slice.postMessage(base+14, userA, "repair dispatch publication gap")
		slice.setConnectionReady(run)
		slice.waitRunStatus(run, domain.RunQueued)
		slice.compose("start", "queue-local")
		slice.waitHTTP("http://127.0.0.1:9324/?Action=ListQueues&Version=2012-11-05")
		time.Sleep(500 * time.Millisecond)
		slice.runWorker(nil)
		slice.waitRunStatus(run, domain.RunSucceeded)
	})

	t.Run("explicit clean context advances the frontend context epoch", func(t *testing.T) {
		before := len(slice.capturesForChat(userA))
		command := slice.postMessage(base+15, userA, "/new")
		slice.waitRunStatus(command, domain.RunSucceeded)
		slice.waitCaptureIncrease(userA, before)
		var epoch uint64
		if err := slice.db.QueryRowContext(
			slice.ctx,
			`SELECT current_context_epoch FROM conversations
			 WHERE tenant_id = $1 AND conversation_id = $2`,
			command.TenantID, command.Conversation.ID,
		).Scan(&epoch); err != nil {
			t.Fatal(err)
		}
		if epoch == 0 {
			t.Fatal("explicit clean context did not advance the context epoch")
		}
	})
}

func newLocalSlice(t *testing.T) *localSlice {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	ydb, err := ydbclient.Open(ctx, envOrDefault(
		"YDB_CONNECTION_STRING",
		"grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric",
	))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	state, err := ydbstore.New(ydb.DB, ydbstore.Options{})
	if err != nil {
		_ = ydb.Close(context.Background())
		cancel()
		t.Fatal(err)
	}
	blobs, err := s3store.New(ctx, s3store.Config{
		Endpoint:        envOrDefault("S3_ENDPOINT", "http://localhost:9000"),
		Region:          envOrDefault("S3_REGION", "us-east-1"),
		Bucket:          envOrDefault("S3_BUCKET", "sessionless-local"),
		AccessKeyID:     envOrDefault("S3_ACCESS_KEY_ID", "sessionless-local"),
		SecretAccessKey: envOrDefault("S3_SECRET_ACCESS_KEY", "sessionless-local-secret"),
		ForcePathStyle:  true,
	})
	if err != nil {
		_ = ydb.Close(context.Background())
		cancel()
		t.Fatal(err)
	}
	queue, err := sqsqueue.New(ctx, sqsqueue.Config{
		Endpoint:        envOrDefault("QUEUE_ENDPOINT", "http://localhost:9324"),
		Region:          envOrDefault("QUEUE_REGION", "us-east-1"),
		QueueURL:        envOrDefault("DISPATCH_QUEUE_URL", "http://localhost:9324/000000000000/sessionless-dispatch"),
		DeadLetterURL:   envOrDefault("DEAD_LETTER_QUEUE_URL", "http://localhost:9324/000000000000/sessionless-dlq"),
		AccessKeyID:     envOrDefault("QUEUE_ACCESS_KEY_ID", "sessionless-local"),
		SecretAccessKey: envOrDefault("QUEUE_SECRET_ACCESS_KEY", "sessionless-local-secret"),
	})
	if err != nil {
		_ = ydb.Close(context.Background())
		cancel()
		t.Fatal(err)
	}
	return &localSlice{
		t: t, ctx: ctx, cancel: cancel, db: ydb.DB,
		closeDB: func() { _ = ydb.Close(context.Background()) },
		state:   state, blobs: blobs, queue: queue,
		client:      &http.Client{Timeout: 10 * time.Second},
		controlURL:  envOrDefault("SESSIONLESS_BASE_URL", "http://localhost:8080"),
		telegramURL: envOrDefault("TELEGRAM_API_BASE_URL", "http://localhost:8081"),
	}
}

func (slice *localSlice) close() {
	slice.closeDB()
	slice.cancel()
}

func (slice *localSlice) reset() {
	slice.postJSON(slice.telegramURL+"/test/reset", []byte(`{}`))
	for {
		message, err := slice.queue.Receive(slice.ctx)
		if errors.Is(err, sqsqueue.ErrNoMessage) {
			break
		}
		if err != nil {
			slice.t.Fatal(err)
		}
		if err := slice.queue.Ack(slice.ctx, message.ReceiptHandle); err != nil {
			slice.t.Fatal(err)
		}
	}
}

func (slice *localSlice) postMessage(updateID, chatID int64, text string) runRef {
	slice.t.Helper()
	return slice.postUpdate(updateID, chatID, text, nil)
}

func (slice *localSlice) postMessageWithDocument(
	updateID, chatID int64,
	text, fileID, name string,
	data []byte,
) runRef {
	slice.t.Helper()
	request, err := http.NewRequest(
		http.MethodPost,
		slice.telegramURL+"/test/files/"+fileID+"?name="+name,
		bytes.NewReader(data),
	)
	if err != nil {
		slice.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "text/plain")
	response, err := slice.client.Do(request)
	if err != nil {
		slice.t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		slice.t.Fatalf("seed Telegram file status = %d", response.StatusCode)
	}
	return slice.postUpdate(updateID, chatID, text, map[string]any{
		"file_id": fileID, "file_name": name, "mime_type": "text/plain",
	})
}

func (slice *localSlice) postUpdate(
	updateID, chatID int64,
	text string,
	document map[string]any,
) runRef {
	slice.t.Helper()
	message := map[string]any{
		"message_id": updateID,
		"from":       map[string]any{"id": chatID},
		"chat":       map[string]any{"id": chatID, "type": "private"},
		"date":       time.Now().UTC().Unix(),
		"text":       text,
	}
	if document != nil {
		message["document"] = document
	}
	body, err := json.Marshal(map[string]any{
		"update_id": updateID,
		"message":   message,
	})
	if err != nil {
		slice.t.Fatal(err)
	}
	request, err := http.NewRequest(
		http.MethodPost, slice.controlURL+"/telegram/webhook", bytes.NewReader(body),
	)
	if err != nil {
		slice.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", webhookSecret)
	response, err := slice.client.Do(request)
	if err != nil {
		slice.t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		slice.t.Fatalf("webhook status = %d", response.StatusCode)
	}

	resolver, err := telegramingress.NewIdentityResolver([]byte(identityKey))
	if err != nil {
		slice.t.Fatal(err)
	}
	identity, err := resolver.ResolvePrivate(chatID, chatID, "codex")
	if err != nil {
		slice.t.Fatal(err)
	}
	var runID domain.RunID
	if err := slice.db.QueryRowContext(
		slice.ctx,
		`SELECT run_id FROM telegram_updates
		 WHERE tenant_id = $1 AND source_id = $2 AND update_id = $3`,
		identity.Tenant, telegramSource, updateID,
	).Scan(&runID); err != nil {
		slice.t.Fatal(err)
	}
	ref := runRef{
		UpdateID: updateID, MessageID: updateID, ChatID: chatID,
		TenantID: identity.Tenant, Conversation: identity.Conversation,
		ConnectionID: identity.SubscriptionConnection, RunID: runID,
	}
	slice.t.Logf(
		"correlation update_id=%d tenant_id=%s run_id=%s chat_id=%d",
		ref.UpdateID, ref.TenantID, ref.RunID, ref.ChatID,
	)
	return ref
}

func (slice *localSlice) assertInputDocument(run runRef, name string) {
	slice.t.Helper()
	loaded, found, err := slice.state.LoadWorkerJob(slice.ctx, run.TenantID, run.RunID)
	if err != nil {
		slice.t.Fatal(err)
	}
	if !found {
		slice.t.Fatalf("worker job for run %s not found", run.RunID)
	}
	for _, artifact := range loaded.InputManifest.Artifacts {
		if artifact.Name == name {
			if !strings.HasPrefix(artifact.Blob.Key, domain.TenantObjectPrefix(run.TenantID)) {
				slice.t.Fatalf("input document key = %q", artifact.Blob.Key)
			}
			return
		}
	}
	slice.t.Fatalf("input document %q not found in worker manifest", name)
}

func (slice *localSlice) setConnectionReady(run runRef) {
	slice.setConnectionState(
		run,
		domain.EntitlementActive,
		domain.ProviderQuotaUnknown,
		domain.SchedulerReady,
		nil,
	)
}

func (slice *localSlice) setConnectionState(
	run runRef,
	entitlement domain.EntitlementState,
	quota domain.ProviderQuotaState,
	scheduler domain.SchedulerState,
	blockedUntil *time.Time,
) {
	slice.t.Helper()
	blocked := time.Unix(0, 0).UTC()
	if blockedUntil != nil {
		blocked = blockedUntil.UTC()
	}
	if _, err := slice.db.ExecContext(
		slice.ctx,
		`UPDATE subscription_connections
		 SET entitlement_state = $1, quota_state = $2, updated_at = CurrentUtcTimestamp()
		 WHERE tenant_id = $3 AND subscription_connection_id = $4`,
		entitlement, quota, run.TenantID, run.ConnectionID,
	); err != nil {
		slice.t.Fatal(err)
	}
	if _, err := slice.db.ExecContext(
		slice.ctx,
		`UPDATE subscription_scheduler_slots
		 SET state = $1, blocked_until = $2, updated_at = CurrentUtcTimestamp()
		 WHERE tenant_id = $3 AND subscription_connection_id = $4`,
		scheduler, blocked, run.TenantID, run.ConnectionID,
	); err != nil {
		slice.t.Fatal(err)
	}
}

func (slice *localSlice) waitRunStatus(run runRef, wanted domain.RunStatus) {
	slice.t.Helper()
	deadline := time.Now().Add(35 * time.Second)
	for {
		var status domain.RunStatus
		err := slice.db.QueryRowContext(
			slice.ctx,
			`SELECT status FROM runs WHERE tenant_id = $1 AND run_id = $2`,
			run.TenantID, run.RunID,
		).Scan(&status)
		if err == nil && status == wanted {
			return
		}
		if time.Now().After(deadline) {
			slice.t.Fatalf("run %s status = %q, want %q (last error: %v)", run.RunID, status, wanted, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (slice *localSlice) runWorker(overrides map[string]string) {
	slice.t.Helper()
	arguments := []string{
		"compose", "--project-name", "sessionless-dev", "--profile", "worker",
		"run", "--rm", "--no-deps",
		"-e", "WORKER_RETRY_DELAY=1s",
		"-e", "WORKER_QUEUE_WAIT=2s",
	}
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		arguments = append(arguments, "-e", key+"="+overrides[key])
	}
	arguments = append(arguments, "worker-runtime")
	command := exec.CommandContext(slice.ctx, "docker", arguments...)
	command.Dir = repositoryRoot(slice.t)
	output, err := command.CombinedOutput()
	if err != nil {
		slice.t.Fatalf("worker container failed: %v\n%s", err, output)
	}
	slice.t.Logf("worker invocation: %s", strings.TrimSpace(string(output)))
}

func (slice *localSlice) publishDuplicate(run runRef) {
	slice.t.Helper()
	if err := slice.queue.Publish(slice.ctx, queuecontract.Envelope{
		Schema: queuecontract.SchemaV1,
		MessageID: domain.MessageID(
			"msg-e2e-duplicate-" + strconv.FormatInt(run.UpdateID, 10),
		),
		Kind: queuecontract.KindDispatchRun, TenantID: run.TenantID,
		SubjectID: string(run.RunID), EnqueuedAt: time.Now().UTC(),
	}); err != nil {
		slice.t.Fatal(err)
	}
}

func (slice *localSlice) requestCancellation(ref runRef) {
	slice.t.Helper()
	var payload string
	if err := slice.db.QueryRowContext(
		slice.ctx,
		`SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2`,
		ref.TenantID, ref.RunID,
	).Scan(&payload); err != nil {
		slice.t.Fatal(err)
	}
	var run domain.Run
	if err := json.Unmarshal([]byte(payload), &run); err != nil {
		slice.t.Fatal(err)
	}
	now := time.Now().UTC()
	run.CancellationRequestedAt = &now
	run.UpdatedAt = now
	updated, err := json.Marshal(run)
	if err != nil {
		slice.t.Fatal(err)
	}
	if _, err := slice.db.ExecContext(
		slice.ctx,
		`UPDATE runs
		 SET updated_at = $1, payload = CAST($2 AS JsonDocument)
		 WHERE tenant_id = $3 AND run_id = $4`,
		now, string(updated), ref.TenantID, ref.RunID,
	); err != nil {
		slice.t.Fatal(err)
	}
}

func (slice *localSlice) assertOneTelegramRun(run runRef) {
	slice.t.Helper()
	var count uint64
	if err := slice.db.QueryRowContext(
		slice.ctx,
		`SELECT COUNT(*) FROM telegram_updates
		 WHERE tenant_id = $1 AND source_id = $2 AND update_id = $3`,
		run.TenantID, telegramSource, run.UpdateID,
	).Scan(&count); err != nil {
		slice.t.Fatal(err)
	}
	if count != 1 {
		slice.t.Fatalf("update %d rows = %d, want 1", run.UpdateID, count)
	}
}

func (slice *localSlice) assertCheckpointCount(run runRef, wanted uint64) {
	slice.t.Helper()
	var count uint64
	if err := slice.db.QueryRowContext(
		slice.ctx,
		`SELECT COUNT(*) FROM checkpoints WHERE tenant_id = $1 AND run_id = $2`,
		run.TenantID, run.RunID,
	).Scan(&count); err != nil {
		slice.t.Fatal(err)
	}
	if count != wanted {
		slice.t.Fatalf("run %s checkpoints = %d, want %d", run.RunID, count, wanted)
	}
}

func (slice *localSlice) assertUsage(run runRef, wanted uint64) {
	slice.t.Helper()
	var count uint64
	if err := slice.db.QueryRowContext(
		slice.ctx,
		`SELECT COUNT(*) FROM usage_observations WHERE tenant_id = $1 AND run_id = $2`,
		run.TenantID, run.RunID,
	).Scan(&count); err != nil {
		slice.t.Fatal(err)
	}
	if count != wanted {
		slice.t.Fatalf("run %s usage rows = %d, want %d", run.RunID, count, wanted)
	}
}

func (slice *localSlice) assertTenantArtifacts(runA, runB runRef) {
	slice.t.Helper()
	manifestA := slice.outputManifest(runA)
	manifestB := slice.outputManifest(runB)
	if len(manifestA.Artifacts) == 0 || len(manifestB.Artifacts) == 0 {
		slice.t.Fatal("terminal output manifests must contain artifacts")
	}
	for _, item := range manifestA.Artifacts {
		if !strings.HasPrefix(item.Blob.Key, domain.TenantObjectPrefix(runA.TenantID)) {
			slice.t.Fatalf("tenant A artifact key = %q", item.Blob.Key)
		}
	}
	for _, item := range manifestB.Artifacts {
		if !strings.HasPrefix(item.Blob.Key, domain.TenantObjectPrefix(runB.TenantID)) {
			slice.t.Fatalf("tenant B artifact key = %q", item.Blob.Key)
		}
	}
	_, err := slice.blobs.Open(slice.ctx, runB.TenantID, manifestA.Artifacts[0].Blob)
	var mismatch domain.TenantMismatchError
	if !errors.As(err, &mismatch) {
		slice.t.Fatalf("cross-tenant artifact open error = %v, want TenantMismatchError", err)
	}
}

func (slice *localSlice) outputManifest(run runRef) domain.ArtifactManifest {
	slice.t.Helper()
	delivery := slice.deliveryForRun(run)
	if delivery.ArtifactManifestID == nil {
		slice.t.Fatalf("run %s has no terminal artifact manifest", run.RunID)
	}
	manifest, found, err := slice.state.GetArtifactManifest(
		slice.ctx, run.TenantID, *delivery.ArtifactManifestID,
	)
	if err != nil {
		slice.t.Fatal(err)
	}
	if !found {
		slice.t.Fatalf("manifest %s not found", *delivery.ArtifactManifestID)
	}
	return manifest
}

func (slice *localSlice) assertDeliveryWasRetried(runs ...runRef) {
	slice.t.Helper()
	for _, run := range runs {
		if slice.deliveryForRun(run).AttemptCount >= 2 {
			return
		}
	}
	slice.t.Fatal("injected Telegram failure did not produce a durable retry")
}

func (slice *localSlice) deliveryForRun(run runRef) domain.TelegramDeliveryOutbox {
	slice.t.Helper()
	var payload string
	if err := slice.db.QueryRowContext(
		slice.ctx,
		`SELECT payload FROM telegram_delivery_outbox
		 WHERE tenant_id = $1 AND run_id = $2 LIMIT 1`,
		run.TenantID, run.RunID,
	).Scan(&payload); err != nil {
		slice.t.Fatal(err)
	}
	var delivery domain.TelegramDeliveryOutbox
	if err := json.Unmarshal([]byte(payload), &delivery); err != nil {
		slice.t.Fatal(err)
	}
	return delivery
}

func (slice *localSlice) injectTelegramFailure(method string, count, status int) {
	slice.t.Helper()
	body, err := json.Marshal(map[string]any{
		"method": method, "count": count, "status": status,
	})
	if err != nil {
		slice.t.Fatal(err)
	}
	slice.postJSON(slice.telegramURL+"/test/failures", body)
}

func (slice *localSlice) waitForChatMethods(wanted map[int64]map[string]int) {
	slice.t.Helper()
	deadline := time.Now().Add(35 * time.Second)
	for {
		got := make(map[int64]map[string]int)
		for _, item := range slice.captures() {
			if got[item.ChatID] == nil {
				got[item.ChatID] = make(map[string]int)
			}
			got[item.ChatID][item.Method]++
		}
		complete := true
		for chatID, methods := range wanted {
			for method, count := range methods {
				if got[chatID][method] < count {
					complete = false
				}
			}
		}
		if complete {
			return
		}
		if time.Now().After(deadline) {
			slice.t.Fatalf("Telegram captures = %#v, want %#v", got, wanted)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (slice *localSlice) waitCaptureIncrease(chatID int64, before int) {
	slice.t.Helper()
	deadline := time.Now().Add(35 * time.Second)
	for {
		if len(slice.capturesForChat(chatID)) > before {
			return
		}
		if time.Now().After(deadline) {
			slice.t.Fatalf("no new Telegram capture for chat %d", chatID)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (slice *localSlice) capturesForChat(chatID int64) []capture {
	all := slice.captures()
	result := make([]capture, 0)
	for _, item := range all {
		if item.ChatID == chatID {
			result = append(result, item)
		}
	}
	return result
}

func (slice *localSlice) captures() []capture {
	slice.t.Helper()
	var payload struct {
		OK     bool      `json:"ok"`
		Result []capture `json:"result"`
	}
	slice.getJSON(slice.telegramURL+"/test/captures", &payload)
	if !payload.OK {
		slice.t.Fatal("Telegram fake returned ok=false")
	}
	return payload.Result
}

func (slice *localSlice) compose(arguments ...string) {
	slice.t.Helper()
	args := append([]string{"compose", "--project-name", "sessionless-dev"}, arguments...)
	command := exec.CommandContext(slice.ctx, "docker", args...)
	command.Dir = repositoryRoot(slice.t)
	output, err := command.CombinedOutput()
	if err != nil {
		slice.t.Fatalf("docker compose %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
}

func (slice *localSlice) waitHTTP(url string) {
	slice.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		response, err := slice.client.Get(url)
		if err == nil {
			response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 500 {
				return
			}
		}
		if time.Now().After(deadline) {
			slice.t.Fatalf("%s did not become ready: %v", url, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (slice *localSlice) postJSON(url string, body []byte) {
	slice.t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		slice.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := slice.client.Do(request)
	if err != nil {
		slice.t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		slice.t.Fatalf("POST %s: status=%d body=%s", url, response.StatusCode, data)
	}
}

func (slice *localSlice) getJSON(url string, target any) {
	slice.t.Helper()
	response, err := slice.client.Get(url)
	if err != nil {
		slice.t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		slice.t.Fatalf("GET %s: status=%d body=%s", url, response.StatusCode, data)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		slice.t.Fatal(err)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(directory + "/go.mod"); err == nil {
			return directory
		}
		parent := directory[:strings.LastIndex(directory, "/")]
		if parent == "" || parent == directory {
			t.Fatal("repository root not found")
		}
		directory = parent
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func TestE2EPackageCompilesWithoutRuntimeEnvironment(t *testing.T) {
	if os.Getenv("SESSIONLESS_E2E") == "1" {
		return
	}
	// This test deliberately exercises only pure helpers so the tagged package
	// can be compiled in a preflight job without Docker or live adapters.
	if root := repositoryRoot(t); root == "" {
		t.Fatal("repository root is empty")
	}
	if _, err := strconv.ParseInt("42", 10, 64); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(domain.RunSucceeded) != "succeeded" {
		t.Fatal("unexpected domain status")
	}
}
