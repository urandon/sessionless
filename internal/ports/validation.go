package ports

import (
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func (request TelegramSendRequest) Validate() error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := request.RunID.Validate(); err != nil {
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
	if request.ReplyToMessageID < 0 {
		return domain.ValidationError{Field: "telegram_send.reply_to_message_id", Reason: "must not be negative"}
	}
	hasText := strings.TrimSpace(request.Text) != ""
	hasPayload := request.Payload.Key != ""
	if hasText == hasPayload {
		return domain.ValidationError{
			Field:  "telegram_send.content",
			Reason: "must contain exactly one of inline text or a blob payload",
		}
	}
	if hasText {
		if utf8.RuneCountInString(request.Text) > 4096 {
			return domain.ValidationError{
				Field:  "telegram_send.text",
				Reason: "must not exceed 4096 Unicode characters",
			}
		}
	} else {
		if err := request.Payload.Validate(); err != nil {
			return err
		}
		if err := domain.EnsureSameTenant(request.TenantID, request.Payload.TenantID); err != nil {
			return err
		}
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

func (request CredentialIssueRequest) ValidateAt(now time.Time) error {
	if err := request.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := request.Run.Validate(); err != nil {
		return err
	}
	if request.Run.Status != domain.RunRunning {
		return domain.ValidationError{Field: "credential_issue.run", Reason: "must be running"}
	}
	if err := request.Attempt.ValidateForRun(request.Run); err != nil {
		return err
	}
	if request.Attempt.Status != domain.AttemptRunning || request.Attempt.WorkerID == "" {
		return domain.ValidationError{Field: "credential_issue.attempt", Reason: "must be assigned and running"}
	}
	if err := request.Lease.ValidateForAttempt(request.Run, request.Attempt); err != nil {
		return err
	}
	if request.Lease.WorkerID != request.Attempt.WorkerID {
		return domain.ValidationError{Field: "credential_issue.worker", Reason: "attempt and lease must match"}
	}
	if !request.Lease.ActiveAt(now) {
		return domain.ValidationError{Field: "credential_issue.lease", Reason: "must be active"}
	}
	if !request.ExpiresAt.After(now) || request.ExpiresAt.After(request.Lease.ExpiresAt) {
		return domain.ValidationError{Field: "credential_issue.expires_at", Reason: "must be after now and no later than lease expiry"}
	}
	return nil
}

func (handle CredentialHandle) Validate() error {
	if err := domain.ValidateOpaqueID("credential.handle_id", handle.HandleID); err != nil {
		return err
	}
	if err := handle.TenantID.Validate(); err != nil {
		return err
	}
	if err := handle.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	if err := handle.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := handle.RunID.Validate(); err != nil {
		return err
	}
	if err := handle.AttemptID.Validate(); err != nil {
		return err
	}
	if err := domain.ValidateOpaqueID("credential.worker_id", handle.WorkerID); err != nil {
		return err
	}
	if err := handle.LeaseID.Validate(); err != nil {
		return err
	}
	if handle.LeaseFence == 0 || handle.BindingGeneration == 0 {
		return domain.ValidationError{Field: "credential.fence", Reason: "lease fence and binding generation must be positive"}
	}
	if handle.ExpiresAt.IsZero() {
		return domain.ValidationError{Field: "credential.expires_at", Reason: "must not be zero"}
	}
	return nil
}

func (request CredentialRevokeRequest) Validate() error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := request.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	return request.OwnerUserID.Validate()
}

func (scope CredentialCandidateScope) Validate() error {
	if err := scope.TenantID.Validate(); err != nil {
		return err
	}
	if err := scope.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	if err := scope.OwnerUserID.Validate(); err != nil {
		return err
	}
	if scope.ExpectedGeneration == 0 {
		return domain.ValidationError{Field: "credential_candidate.expected_generation", Reason: "must be positive"}
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
	if err := request.SessionID.Validate(); err != nil {
		return err
	}
	if err := request.TriggerEventID.Validate(); err != nil {
		return err
	}
	if err := request.AttemptID.Validate(); err != nil {
		return err
	}
	if !filepath.IsAbs(request.WorkDir) || filepath.Clean(request.WorkDir) != request.WorkDir {
		return domain.ValidationError{Field: "execution.work_dir", Reason: "must be a normalized absolute path"}
	}
	if request.ContextWindow != nil {
		if err := request.ContextWindow.Validate(); err != nil {
			return err
		}
	} else {
		if err := request.ContextSnapshot.Validate(); err != nil {
			return err
		}
		if err := domain.EnsureSameTenant(request.TenantID, request.ContextSnapshot.TenantID); err != nil {
			return err
		}
	}
	if request.Credential.HandleID != "" {
		if err := domain.EnsureSameTenant(request.TenantID, request.Credential.TenantID); err != nil {
			return err
		}
		if err := request.Credential.Validate(); err != nil {
			return err
		}
		if request.Credential.RunID != request.RunID || request.Credential.AttemptID != request.AttemptID {
			return domain.ValidationError{Field: "execution.credential", Reason: "must match the run and attempt"}
		}
		if err := request.CredentialMaterialization.Validate(); err != nil {
			return err
		}
	} else if request.CredentialMaterialization.RootDir != "" || request.CredentialMaterialization.AuthFile != "" {
		return domain.ValidationError{Field: "execution.credential_materialization", Reason: "requires a credential handle"}
	}
	for _, artifact := range request.InputArtifacts {
		if err := artifact.Validate(); err != nil {
			return err
		}
		if err := domain.EnsureSameTenant(request.TenantID, artifact.Blob.TenantID); err != nil {
			return err
		}
	}
	if request.ResumeCheckpoint != nil {
		if err := domain.EnsureSameTenant(request.TenantID, request.ResumeCheckpoint.TenantID); err != nil {
			return err
		}
		if request.ResumeCheckpoint.RunID != request.RunID ||
			request.ResumeCheckpoint.AttemptID != request.AttemptID {
			return domain.ValidationError{
				Field:  "execution.resume_checkpoint",
				Reason: "must reference the requested run and attempt",
			}
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

func (request WorkerContextRequest) Validate() error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := request.SessionID.Validate(); err != nil {
		return err
	}
	if err := request.TriggerEventID.Validate(); err != nil {
		return err
	}
	if request.ThroughSequence == 0 {
		return domain.ValidationError{Field: "worker_context.through_sequence", Reason: "must be positive"}
	}
	if request.MaxEvents == 0 {
		return domain.ValidationError{Field: "worker_context.max_events", Reason: "must be positive"}
	}
	if request.AtOrBeforeSnapshotVersion != nil && *request.AtOrBeforeSnapshotVersion == 0 {
		return domain.ValidationError{Field: "worker_context.snapshot_version", Reason: "must be positive when set"}
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
