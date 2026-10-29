package domain

import (
	"slices"
	"strings"
	"time"
)

// WorkerJob is the durable, point-addressable materialization contract for one
// admitted run. Queue messages carry only its tenant/run routing identity.
type WorkerJob struct {
	TenantID                 TenantID                    `json:"tenant_id"`
	RunID                    RunID                       `json:"run_id"`
	SessionID                SessionID                   `json:"session_id"`
	TriggerEventID           SessionEventID              `json:"trigger_event_id"`
	AttemptID                AttemptID                   `json:"attempt_id"`
	ReservationID            QuotaReservationID          `json:"reservation_id"`
	InputManifestID          ArtifactManifestID          `json:"input_manifest_id"`
	ContextSnapshot          BlobRef                     `json:"context_snapshot"`
	ContextWindow            *SessionContextWindow       `json:"context_window,omitempty"`
	WorkspaceSnapshot        *BlobRef                    `json:"workspace_snapshot,omitempty"`
	SkillBundle              *BlobRef                    `json:"skill_bundle,omitempty"`
	AllowedMCPServers        []string                    `json:"allowed_mcp_servers,omitempty"`
	CredentialOwnerUserID    UserID                      `json:"credential_owner_user_id,omitempty"`
	ExecutionPlacementV2     ExecutionPlacementV2        `json:"execution_placement"`
	HarnessBinding           HarnessBindingV2            `json:"harness_binding"`
	SubstrateBinding         *SubstrateBindingV1         `json:"substrate_binding,omitempty"`
	AdmissionCostCeiling     *AdmissionCostCeilingV2     `json:"admission_cost_ceiling,omitempty"`
	LocalCapacityAssociation *LocalCapacityAssociationV1 `json:"local_capacity_association,omitempty"`
	// Original strict ingress bytes; missing final admission is NEVER recovered
	// from this historical job projection.
	OwnedExecutionSourceRecord string               `json:"owned_execution_source_record,omitempty"`
	Limits                     ProductLimits        `json:"limits"`
	Origin                     *FrontendEventOrigin `json:"origin,omitempty"`
	// Compatibility bridge for legacy Telegram jobs until #37 projects results from the
	// canonical session stream.
	DeliveryChat     TelegramChatRef `json:"delivery_chat"`
	ReplyToMessageID int64           `json:"reply_to_message_id"`
	CreatedAt        time.Time       `json:"created_at"`
}

func (job WorkerJob) Clone() WorkerJob {
	if job.LocalCapacityAssociation != nil {
		value := *job.LocalCapacityAssociation
		job.LocalCapacityAssociation = &value
	}
	if job.ContextWindow != nil {
		value := *job.ContextWindow
		if value.SnapshotVersion != nil {
			version := *value.SnapshotVersion
			value.SnapshotVersion = &version
		}
		job.ContextWindow = &value
	}
	for _, ref := range []**BlobRef{&job.WorkspaceSnapshot, &job.SkillBundle} {
		if *ref != nil {
			value := **ref
			*ref = &value
		}
	}
	job.AllowedMCPServers = slices.Clone(job.AllowedMCPServers)
	job.HarnessBinding = job.HarnessBinding.Clone()
	if job.SubstrateBinding != nil {
		value := *job.SubstrateBinding
		job.SubstrateBinding = &value
	}
	if job.AdmissionCostCeiling != nil {
		value := job.AdmissionCostCeiling.Clone()
		job.AdmissionCostCeiling = &value
	}
	if job.Origin != nil {
		value := *job.Origin
		job.Origin = &value
	}
	return job
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
	if job.ContextWindow != nil {
		if err := job.ContextWindow.Validate(); err != nil {
			return err
		}
	} else {
		if err := validateWorkerBlob(run.TenantID, "worker_job.context_snapshot", job.ContextSnapshot); err != nil {
			return err
		}
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
	if job.CredentialOwnerUserID != "" {
		if err := job.CredentialOwnerUserID.Validate(); err != nil {
			return err
		}
	}
	if err := job.ExecutionPlacementV2.Validate(); err != nil {
		return err
	}
	if err := ValidateExecutionAuthorityProjection(job.ExecutionPlacementV2, job.SubstrateBinding, job.AdmissionCostCeiling); err != nil {
		return err
	}
	if err := job.HarnessBinding.ValidateForScope(
		job.TenantID, job.CredentialOwnerUserID, job.RunID, job.AttemptID, job.ExecutionPlacementV2,
	); err != nil {
		return err
	}
	if err := job.HarnessBinding.ValidateAdmissionCostCeiling(job.AdmissionCostCeiling); err != nil {
		return err
	}
	if err := ValidateOwnedExecutionSourceRecord(job.OwnedExecutionSourceRecord,
		AttemptExecutionSourceScopeV1{TenantID: job.TenantID, SessionID: run.SessionID, OwnerUserID: job.CredentialOwnerUserID, RunID: job.RunID, AttemptID: job.AttemptID},
		job.ExecutionPlacementV2, job.HarnessBinding, job.SubstrateBinding, job.AdmissionCostCeiling); err != nil {
		return err
	}
	if err := validateLocalCapacityProjection(run, job.LocalCapacityAssociation, job.CredentialOwnerUserID,
		job.ExecutionPlacementV2, job.HarnessBinding, job.SubstrateBinding, job.AdmissionCostCeiling, job.OwnedExecutionSourceRecord); err != nil {
		return err
	}
	if err := job.Limits.Validate(); err != nil {
		return err
	}
	if job.ContextWindow == nil && uint64(job.ContextSnapshot.Size) > job.Limits.MaxContextBytes {
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

// This is structural correlation after the existing strict source/execution
// validator, not current source authentication, capacity admission or I/O grant.
func validateLocalCapacityProjection(run Run, local *LocalCapacityAssociationV1, owner UserID,
	placement ExecutionPlacementV2, binding HarnessBindingV2, substrate *SubstrateBindingV1, cost *AdmissionCostCeilingV2, record string) error {
	denied := func() error {
		return ValidationError{Field: "local_capacity_association.projection", Reason: "must preserve the owning Run and original configured-local source/resource"}
	}
	if local == nil {
		if run.LocalCapacityAssociation != nil || binding.Resource.Kind == ProviderResourceLocalEndpointV1 {
			return denied()
		}
		return nil // Genuine legacy projections retain their original validation.
	}
	if run.LocalCapacityAssociation == nil || *local != *run.LocalCapacityAssociation ||
		local.ValidateForScope(run.TenantID, owner, binding.Resource.ResourceID) != nil ||
		local.ValidateForOwnedSource(record, placement, binding, substrate, cost) != nil {
		return denied()
	}
	source, err := DecodeAttemptExecutionSourceV1([]byte(record))
	if err != nil || source.Scope.TenantID != run.TenantID || source.Scope.SessionID != run.SessionID ||
		source.Scope.RunID != run.ID || source.Scope.AttemptID != binding.AttemptID || source.Scope.OwnerUserID != owner {
		return denied()
	}
	return nil
}

func validateWorkerBlob(tenantID TenantID, field string, ref BlobRef) error {
	if err := ref.Validate(); err != nil {
		return ValidationError{Field: field, Reason: err.Error()}
	}
	return EnsureSameTenant(tenantID, ref.TenantID)
}
