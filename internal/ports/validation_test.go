package ports_test

import (
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var portTestTime = time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

func portTestBlob(tenant domain.TenantID) domain.BlobRef {
	return domain.BlobRef{
		TenantID: tenant,
		Key:      "tenants/" + string(tenant) + "/runs/run-1/context.json",
		Size:     1,
		SHA256:   "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
}

func TestExecutionRequestRejectsCrossTenantBlob(t *testing.T) {
	t.Parallel()

	request := ports.ExecutionRequest{
		TenantID:        "tenant-a",
		RunID:           "run-1",
		AttemptID:       "attempt-1",
		ContextSnapshot: portTestBlob("tenant-b"),
		Credential: ports.CredentialHandle{
			TenantID:                 "tenant-a",
			SubscriptionConnectionID: "subscription-1",
			Handle:                   "credential-1",
			ExpiresAt:                portTestTime.Add(time.Minute),
		},
	}
	err := request.Validate()
	var mismatch domain.TenantMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Validate() error = %v, want TenantMismatchError", err)
	}
}

func TestExecutionRequestAcceptsHarnessNeutralReferences(t *testing.T) {
	t.Parallel()

	request := ports.ExecutionRequest{
		TenantID:        "tenant-a",
		RunID:           "run-1",
		AttemptID:       "attempt-1",
		ContextSnapshot: portTestBlob("tenant-a"),
		InputArtifacts: []domain.Artifact{{
			Name:      "input.txt",
			MediaType: "text/plain",
			Blob:      portTestBlob("tenant-a"),
		}},
		Credential: ports.CredentialHandle{
			TenantID:                 "tenant-a",
			SubscriptionConnectionID: "subscription-1",
			Handle:                   "credential-1",
			ExpiresAt:                portTestTime.Add(time.Minute),
		},
		AllowedMCPServers: []string{"source-control", "docs"},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("valid execution request rejected: %v", err)
	}
	request.AllowedMCPServers = append(request.AllowedMCPServers, "docs")
	if err := request.Validate(); err == nil {
		t.Fatal("duplicate MCP server accepted")
	}
}

func TestCredentialHandleMustMatchRequest(t *testing.T) {
	t.Parallel()

	request := ports.CredentialRequest{
		TenantID:                 "tenant-a",
		SubscriptionConnectionID: "subscription-1",
		RunID:                    "run-1",
		AttemptID:                "attempt-1",
		WorkerID:                 "worker-1",
	}
	handle := ports.CredentialHandle{
		TenantID:                 "tenant-a",
		SubscriptionConnectionID: "subscription-2",
		Handle:                   "credential-1",
		ExpiresAt:                portTestTime.Add(time.Minute),
	}
	if err := handle.ValidateFor(request); err == nil {
		t.Fatal("credential handle for another subscription accepted")
	}
}

func TestTelegramSendRequestRejectsCrossTenantPayload(t *testing.T) {
	t.Parallel()

	request := ports.TelegramSendRequest{
		TenantID:         "tenant-a",
		DeliveryID:       "delivery-1",
		Chat:             domain.TelegramChatRef{TenantID: "tenant-a", ChatID: -1000123},
		ReplyToMessageID: 77,
		Payload:          portTestBlob("tenant-b"),
		IdempotencyKey:   "delivery-run-1",
	}
	if err := request.Validate(); err == nil {
		t.Fatal("cross-tenant Telegram payload accepted")
	}
}

func TestTelegramSendRequestAcceptsInlineTextOnly(t *testing.T) {
	t.Parallel()

	request := ports.TelegramSendRequest{
		TenantID:         "tenant-a",
		DeliveryID:       "delivery-inline",
		Chat:             domain.TelegramChatRef{TenantID: "tenant-a", ChatID: 123},
		ReplyToMessageID: 78,
		Text:             "command reply",
		IdempotencyKey:   "delivery-inline",
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("inline Telegram request rejected: %v", err)
	}
	request.Payload = portTestBlob("tenant-a")
	if err := request.Validate(); err == nil {
		t.Fatal("ambiguous Telegram request content accepted")
	}
}
