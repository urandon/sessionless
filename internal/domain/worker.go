package domain

import (
	"strings"
	"time"
)

// WorkerJob is the durable, point-addressable materialization contract for one
// admitted run. Queue messages carry only its tenant/run routing identity.
type WorkerJob struct {
	TenantID          TenantID             `json:"tenant_id"`
	RunID             RunID                `json:"run_id"`
	SessionID         SessionID            `json:"session_id"`
	TriggerEventID    SessionEventID       `json:"trigger_event_id"`
	AttemptID         AttemptID            `json:"attempt_id"`
	ReservationID     QuotaReservationID   `json:"reservation_id"`
	InputManifestID   ArtifactManifestID   `json:"input_manifest_id"`
	ContextSnapshot   BlobRef              `json:"context_snapshot"`
	WorkspaceSnapshot *BlobRef             `json:"workspace_snapshot,omitempty"`
	SkillBundle       *BlobRef             `json:"skill_bundle,omitempty"`
	AllowedMCPServers []string             `json:"allowed_mcp_servers,omitempty"`
	Limits            ProductLimits        `json:"limits"`
	Origin            *FrontendEventOrigin `json:"origin,omitempty"`
	// Compatibility bridge for Telegram until #36 projects results from the
	// canonical session stream.
	DeliveryChat     TelegramChatRef `json:"delivery_chat"`
	ReplyToMessageID int64           `json:"reply_to_message_id"`
	CreatedAt        time.Time       `json:"created_at"`
}

func (job WorkerJob) ValidateForRun(run Run) error {
	if err := run.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(run.TenantID, job.TenantID); err != nil {
		return err
	}
	if job.RunID != run.ID {
		return ValidationError{Field: "worker_job.run_id", Reason: "must reference the owning run"}
	}
	if job.SessionID != run.SessionID || job.TriggerEventID != run.TriggerEventID {
		return ValidationError{Field: "worker_job.session", Reason: "must reference the owning run session and trigger event"}
	}
	if err := job.AttemptID.Validate(); err != nil {
		return err
	}
	if err := job.ReservationID.Validate(); err != nil {
		return err
	}
	if err := job.InputManifestID.Validate(); err != nil {
		return err
	}
	if err := validateWorkerBlob(run.TenantID, "worker_job.context_snapshot", job.ContextSnapshot); err != nil {
		return err
	}
	if job.WorkspaceSnapshot != nil {
		if err := validateWorkerBlob(run.TenantID, "worker_job.workspace_snapshot", *job.WorkspaceSnapshot); err != nil {
			return err
		}
	}
	if job.SkillBundle != nil {
		if err := validateWorkerBlob(run.TenantID, "worker_job.skill_bundle", *job.SkillBundle); err != nil {
			return err
		}
	}
	seen := make(map[string]struct{}, len(job.AllowedMCPServers))
	for _, server := range job.AllowedMCPServers {
		if strings.TrimSpace(server) == "" {
			return ValidationError{Field: "worker_job.allowed_mcp_servers", Reason: "must not contain empty names"}
		}
		if _, exists := seen[server]; exists {
			return ValidationError{Field: "worker_job.allowed_mcp_servers", Reason: "must not contain duplicates"}
		}
		seen[server] = struct{}{}
	}
	if err := job.Limits.Validate(); err != nil {
		return err
	}
	if uint64(job.ContextSnapshot.Size) > job.Limits.MaxContextBytes {
		return ValidationError{Field: "worker_job.context_snapshot", Reason: "exceeds the admitted context limit"}
	}
	if job.Origin == nil && job.DeliveryChat.ChatID == 0 {
		return ValidationError{Field: "worker_job.origin", Reason: "a frontend origin or legacy delivery target is required"}
	}
	if job.Origin != nil {
		if err := job.Origin.Validate(); err != nil {
			return err
		}
	}
	if job.DeliveryChat.ChatID != 0 {
		if err := job.DeliveryChat.Validate(); err != nil {
			return err
		}
		if err := EnsureSameTenant(run.TenantID, job.DeliveryChat.TenantID); err != nil {
			return err
		}
		if job.ReplyToMessageID == 0 {
			return ValidationError{Field: "worker_job.reply_to_message_id", Reason: "must not be zero for a legacy delivery target"}
		}
	}
	if job.CreatedAt.IsZero() || job.CreatedAt.Before(run.CreatedAt) {
		return ValidationError{Field: "worker_job.created_at", Reason: "must not precede the owning run"}
	}
	return nil
}

func validateWorkerBlob(tenantID TenantID, field string, ref BlobRef) error {
	if err := ref.Validate(); err != nil {
		return ValidationError{Field: field, Reason: err.Error()}
	}
	return EnsureSameTenant(tenantID, ref.TenantID)
}
