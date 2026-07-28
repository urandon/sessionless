package ports

import (
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func (request TelegramSendRequest) Validate() error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := request.DeliveryID.Validate(); err != nil {
		return err
	}
	if err := request.Chat.Validate(); err != nil {
		return err
	}
	if err := domain.EnsureSameTenant(request.TenantID, request.Chat.TenantID); err != nil {
		return err
	}
	if request.ReplyToMessageID == 0 {
		return domain.ValidationError{Field: "telegram_send.reply_to_message_id", Reason: "must not be zero"}
	}
	if err := request.Payload.Validate(); err != nil {
		return err
	}
	if err := domain.EnsureSameTenant(request.TenantID, request.Payload.TenantID); err != nil {
		return err
	}
	for _, artifact := range request.Artifacts {
		if err := artifact.Validate(); err != nil {
			return err
		}
		if err := domain.EnsureSameTenant(request.TenantID, artifact.Blob.TenantID); err != nil {
			return err
		}
	}
	return request.IdempotencyKey.Validate()
}

func (request CredentialRequest) Validate() error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := request.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	if err := request.RunID.Validate(); err != nil {
		return err
	}
	if err := request.AttemptID.Validate(); err != nil {
		return err
	}
	return domain.ValidateOpaqueID("credential.worker_id", request.WorkerID)
}

func (handle CredentialHandle) ValidateFor(request CredentialRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := domain.EnsureSameTenant(request.TenantID, handle.TenantID); err != nil {
		return err
	}
	if handle.SubscriptionConnectionID != request.SubscriptionConnectionID {
		return domain.ValidationError{
			Field:  "credential.subscription_connection_id",
			Reason: "must match the request",
		}
	}
	if err := domain.ValidateOpaqueID("credential.handle", handle.Handle); err != nil {
		return err
	}
	if handle.ExpiresAt.IsZero() {
		return domain.ValidationError{Field: "credential.expires_at", Reason: "must not be zero"}
	}
	return nil
}

func (request ExecutionRequest) Validate() error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := request.RunID.Validate(); err != nil {
		return err
	}
	if err := request.AttemptID.Validate(); err != nil {
		return err
	}
	if err := request.ContextSnapshot.Validate(); err != nil {
		return err
	}
	if err := domain.EnsureSameTenant(request.TenantID, request.ContextSnapshot.TenantID); err != nil {
		return err
	}
	if err := domain.EnsureSameTenant(request.TenantID, request.Credential.TenantID); err != nil {
		return err
	}
	if err := domain.ValidateOpaqueID("credential.handle", request.Credential.Handle); err != nil {
		return err
	}
	if request.Credential.ExpiresAt.IsZero() {
		return domain.ValidationError{Field: "credential.expires_at", Reason: "must not be zero"}
	}
	for _, artifact := range request.InputArtifacts {
		if err := artifact.Validate(); err != nil {
			return err
		}
		if err := domain.EnsureSameTenant(request.TenantID, artifact.Blob.TenantID); err != nil {
			return err
		}
	}
	seen := make(map[string]struct{}, len(request.AllowedMCPServers))
	for _, server := range request.AllowedMCPServers {
		if strings.TrimSpace(server) == "" {
			return domain.ValidationError{Field: "allowed_mcp_servers", Reason: "must not contain empty names"}
		}
		if _, exists := seen[server]; exists {
			return domain.ValidationError{Field: "allowed_mcp_servers", Reason: "must not contain duplicates"}
		}
		seen[server] = struct{}{}
	}
	return nil
}

func (identity ExecutionIdentity) Validate() error {
	if err := identity.TenantID.Validate(); err != nil {
		return err
	}
	if err := identity.RunID.Validate(); err != nil {
		return err
	}
	return identity.AttemptID.Validate()
}
