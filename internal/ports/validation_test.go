package ports_test

import (
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
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

func portTestHarnessBinding(tenant domain.TenantID, owner domain.UserID, run domain.RunID, attempt domain.AttemptID) domain.HarnessBindingV1 {
	binding, err := sessionlessharness.NewDeterministicFixtureBindingV1(
		tenant, owner, run, attempt, "subscription-1", domain.ManagedExecutionPlacementV1(), portTestTime,
	)
	if err != nil {
		panic(err)
	}
	return binding
}

func TestExecutionRequestRejectsCrossTenantBlob(t *testing.T) {
	t.Parallel()

	request := ports.ExecutionRequest{
		TenantID:           "tenant-a",
		OwnerUserID:        "user-1",
		RunID:              "run-1",
		SessionID:          "session-1",
		TriggerEventID:     "event-1",
		AttemptID:          "attempt-1",
		WorkDir:            "/tmp/sessionless-test",
		ContextSnapshot:    portTestBlob("tenant-b"),
		HarnessBinding:     portTestHarnessBinding("tenant-a", "user-1", "run-1", "attempt-1"),
		ExecutionPlacement: domain.ManagedExecutionPlacementV1(),
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
		OwnerUserID:     "user-1",
		RunID:           "run-1",
		SessionID:       "session-1",
		TriggerEventID:  "event-1",
		AttemptID:       "attempt-1",
		WorkDir:         "/tmp/sessionless-test",
		ContextSnapshot: portTestBlob("tenant-a"),
		InputArtifacts: []domain.Artifact{{
			Name:      "input.txt",
			MediaType: "text/plain",
			Blob:      portTestBlob("tenant-a"),
		}},
		Credential: ports.ProviderInvocationCredentialV1{
			HandleID:    "credential-1",
			TenantID:    "tenant-a",
			OwnerUserID: "user-1",
			RunID:       "run-1",
			AttemptID:   "attempt-1",
			WorkerID:    "worker-1",
			LeaseID:     "lease-1",
			LeaseFence:  1,
			ExpiresAt:   portTestTime.Add(time.Minute),
		},
		CredentialMaterialization: ports.ProviderCredentialMaterializationV1{
			Kind: ports.ProviderCredentialDeliveryFileV1, RootDir: "/tmp/sessionless-credential", FilePath: "/tmp/sessionless-credential/auth.json",
		},
		AllowedMCPServers:  []string{"source-control", "docs"},
		ExecutionPlacement: domain.ManagedExecutionPlacementV1(),
		HarnessBinding:     portTestHarnessBinding("tenant-a", "user-1", "run-1", "attempt-1"),
	}
	request.HarnessBinding.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
	request.HarnessBinding.Backend.BackendKind = domain.HarnessBackendCodexExecV1
	request.HarnessBinding.Backend.ArtifactKind = domain.HarnessArtifactExecutableV1
	request.HarnessBinding.Resource = domain.ProviderResourceBindingV1{
		Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-1", OwnerUserID: "user-1",
		Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1,
	}
	expires := portTestTime.Add(time.Hour)
	request.HarnessBinding.EvidenceExpiresAt = &expires
	request.Credential.ProviderResource = request.HarnessBinding.Resource
	if err := request.Validate(); err != nil {
		t.Fatalf("valid execution request rejected: %v", err)
	}
	request.AllowedMCPServers = append(request.AllowedMCPServers, "docs")
	if err := request.Validate(); err == nil {
		t.Fatal("duplicate MCP server accepted")
	}
}

func TestCredentialIssueRequestRequiresActiveMatchingInvocation(t *testing.T) {
	t.Parallel()

	created := portTestTime.Add(-time.Minute)
	request := ports.CredentialIssueRequest{
		OwnerUserID: "user-1",
		Run: domain.Run{
			ID: "run-1", TenantID: "tenant-a", SessionID: "session-1",
			TriggerEventID: "event-1", SubscriptionConnectionID: "subscription-1",
			Status: domain.RunRunning, IdempotencyKey: "run-key-1",
			StartedAt: &created, CreatedAt: created, UpdatedAt: portTestTime,
		},
		Attempt: domain.Attempt{
			ID: "attempt-1", TenantID: "tenant-a", RunID: "run-1", Number: 1,
			Status: domain.AttemptRunning, WorkerID: "worker-1",
			CreatedAt: created, UpdatedAt: portTestTime,
		},
		Lease: domain.Lease{
			ID: "lease-1", TenantID: "tenant-a", RunID: "run-1", AttemptID: "attempt-1",
			WorkerID: "worker-1", FenceToken: 7, AcquiredAt: created,
			ExpiresAt: portTestTime.Add(2 * time.Minute),
		},
		ExpiresAt:        portTestTime.Add(time.Minute),
		ProviderResource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-1", OwnerUserID: "user-1", Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1},
	}
	if err := request.ValidateAt(portTestTime); err != nil {
		t.Fatalf("valid credential issue request rejected: %v", err)
	}
	request.Lease.WorkerID = "worker-2"
	if err := request.ValidateAt(portTestTime); err == nil {
		t.Fatal("mismatched attempt/lease worker accepted")
	}
}

func TestTelegramSendRequestRejectsCrossTenantPayload(t *testing.T) {
	t.Parallel()

	request := ports.TelegramSendRequest{
		TenantID:         "tenant-a",
		RunID:            "run-1",
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
		RunID:            "run-inline",
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
