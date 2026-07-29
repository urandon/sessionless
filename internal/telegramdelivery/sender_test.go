package telegramdelivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

type senderStore struct {
	delivery    domain.TelegramDeliveryOutbox
	listed      bool
	transitions []domain.DeliveryStatus
}

func (store *senderStore) ListReadyTelegramDeliveries(
	_ context.Context,
	_ uint32,
	before time.Time,
	_ uint64,
) ([]ports.TelegramDeliveryReady, error) {
	if store.listed || (store.delivery.NextAttemptAt != nil && store.delivery.NextAttemptAt.After(before)) {
		return nil, nil
	}
	store.listed = true
	return []ports.TelegramDeliveryReady{{
		TenantID: store.delivery.TenantID, DeliveryID: store.delivery.ID,
	}}, nil
}

func (store *senderStore) ClaimTelegramDelivery(
	_ context.Context,
	_ domain.TenantID,
	_ domain.TelegramDeliveryID,
	at time.Time,
) (domain.TelegramDeliveryOutbox, bool, error) {
	if err := store.delivery.Transition(domain.DeliverySending, at, nil); err != nil {
		return domain.TelegramDeliveryOutbox{}, false, err
	}
	return store.delivery, true, nil
}

func (store *senderStore) TransitionTelegramDelivery(
	_ context.Context,
	_ domain.TenantID,
	_ domain.TelegramDeliveryID,
	to domain.DeliveryStatus,
	at time.Time,
	retryAt *time.Time,
) error {
	if err := store.delivery.Transition(to, at, retryAt); err != nil {
		return err
	}
	store.transitions = append(store.transitions, to)
	return nil
}

func (*senderStore) GetArtifactManifest(
	context.Context,
	domain.TenantID,
	domain.ArtifactManifestID,
) (domain.ArtifactManifest, bool, error) {
	return domain.ArtifactManifest{}, false, nil
}

type senderClient struct {
	err error
}

func (client *senderClient) Send(
	context.Context,
	ports.TelegramSendRequest,
) (ports.TelegramSendResult, error) {
	return ports.TelegramSendResult{}, client.err
}

func TestSenderRetriesThenMarksSent(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 29, 13, 0, 0, 0, time.UTC)
	tenantID := domain.TenantID("tenant-a")
	store := &senderStore{delivery: domain.TelegramDeliveryOutbox{
		ID: domain.TelegramDeliveryID("delivery-a"), TenantID: tenantID,
		RunID:            domain.RunID("run-a"),
		Chat:             domain.TelegramChatRef{TenantID: tenantID, ChatID: 10},
		ReplyToMessageID: 20,
		Payload: domain.BlobRef{
			TenantID: tenantID, Key: domain.TenantObjectPrefix(tenantID) + "reply.txt",
			SHA256: "0000000000000000000000000000000000000000000000000000000000000000",
		},
		Status: domain.DeliveryPending, IdempotencyKey: domain.IdempotencyKey("reply-a"),
		CreatedAt: now, UpdatedAt: now,
	}}
	clock := testkit.NewFakeClock(now.Add(time.Second))
	client := &senderClient{err: errors.New("rate limited")}
	sender, err := NewSender(Config{BaseBackoff: time.Second}, clock, store, client)
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := sender.RunOnce(context.Background()); err != nil || processed != 1 {
		t.Fatalf("first pass = %d, %v", processed, err)
	}
	if store.delivery.Status != domain.DeliveryRetryWait {
		t.Fatalf("status = %s, want retry_wait", store.delivery.Status)
	}
	clock.Advance(2 * time.Second)
	store.listed = false
	client.err = nil
	if processed, err := sender.RunOnce(context.Background()); err != nil || processed != 1 {
		t.Fatalf("second pass = %d, %v", processed, err)
	}
	if store.delivery.Status != domain.DeliverySent {
		t.Fatalf("status = %s, want sent", store.delivery.Status)
	}
}

var _ ports.TelegramDeliveryStore = (*senderStore)(nil)
var _ ports.TelegramClient = (*senderClient)(nil)
