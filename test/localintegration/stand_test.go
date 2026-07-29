//go:build localintegration

package localintegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
)

func TestLocalStandContracts(t *testing.T) {
	t.Run("YDB", testYDB)
	t.Run("S3 tenant isolation", testS3TenantIsolation)
	t.Run("SQS at least once and dead letter", testSQS)
	t.Run("Telegram capture", testTelegramFake)
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
		MessageID:  "local-contract-message",
		Kind:       queuecontract.KindDispatchRun,
		TenantID:   "tenant-a",
		SubjectID:  "run-local-contract",
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(ctx, envelope); err != nil {
		t.Fatal(err)
	}
	first := receiveEventually(t, ctx, queue)
	if first.Envelope.MessageID != envelope.MessageID || first.DeliveryCount != 1 {
		t.Fatalf("first delivery = %#v", first)
	}
	if err := queue.Retry(ctx, first.ReceiptHandle, 0); err != nil {
		t.Fatal(err)
	}
	second := receiveEventually(t, ctx, queue)
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
	moved := receiveEventually(t, ctx, deadLetter)
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
