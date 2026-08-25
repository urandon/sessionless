package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

const (
	ExecutionPlacementVersionV1             uint32 = 1
	AttachedWorkerAttemptVersionV1          uint32 = 1
	AttachedWorkerAttemptMessageVersionV1   uint32 = 1
	AttachedWorkerAttemptDeadlineBuckets           = 16
	maxAttachedWorkerAttemptMessageBytes           = 64 << 10
	AttachedWorkerLeaseFinalizationBudgetV1        = 30 * time.Second
	AttachedWorkerLeaseMaximumTTLV1                = 24 * time.Hour
)

type (
	ExecutionPlacementKind                  string
	ExecutionFallbackPolicy                 string
	AttachedWorkerPolicyDigest              string
	AttachedWorkerContextDigest             string
	AttachedWorkerFenceToken                string
	AttachedWorkerTerminalEvidenceDigest    string
	AttachedWorkerAttemptMessageFingerprint string
	AttachedWorkerAttemptState              string
	AttachedWorkerAttemptDirection          string
	AttachedWorkerAttemptMessageKind        string
	AttachedWorkerAttemptDeadlineKind       string
	AttachedWorkerTerminalStatus            string
)

const (
	ExecutionPlacementManaged        ExecutionPlacementKind  = "managed"
	ExecutionPlacementAttachedWorker ExecutionPlacementKind  = "attached_worker"
	ExecutionFallbackDenied          ExecutionFallbackPolicy = "deny"

	AttachedWorkerAttemptOffered              AttachedWorkerAttemptState = "offered"
	AttachedWorkerAttemptClaimed              AttachedWorkerAttemptState = "claimed"
	AttachedWorkerAttemptCancelRequested      AttachedWorkerAttemptState = "cancel_requested"
	AttachedWorkerAttemptCancelAcknowledged   AttachedWorkerAttemptState = "cancel_acknowledged"
	AttachedWorkerAttemptTerminalPending      AttachedWorkerAttemptState = "terminal_pending"
	AttachedWorkerAttemptTerminalCommitted    AttachedWorkerAttemptState = "terminal_committed"
	AttachedWorkerAttemptCancelledBeforeClaim AttachedWorkerAttemptState = "cancelled_before_claim"
	AttachedWorkerAttemptFencedUnknown        AttachedWorkerAttemptState = "fenced_unknown"
	AttachedWorkerAttemptRetired              AttachedWorkerAttemptState = "retired"

	AttachedWorkerAttemptPlatformToWorker AttachedWorkerAttemptDirection = "platform_to_worker"
	AttachedWorkerAttemptWorkerToPlatform AttachedWorkerAttemptDirection = "worker_to_platform"

	AttachedWorkerAttemptMessageLeaseOffered       AttachedWorkerAttemptMessageKind = "lease_offered"
	AttachedWorkerAttemptMessageLeaseClaim         AttachedWorkerAttemptMessageKind = "lease_claim"
	AttachedWorkerAttemptMessageLeaseAccepted      AttachedWorkerAttemptMessageKind = "lease_accepted"
	AttachedWorkerAttemptMessageProgress           AttachedWorkerAttemptMessageKind = "progress"
	AttachedWorkerAttemptMessageCancelRequested    AttachedWorkerAttemptMessageKind = "cancel_requested"
	AttachedWorkerAttemptMessageCancelAcknowledged AttachedWorkerAttemptMessageKind = "cancel_acknowledged"
	AttachedWorkerAttemptMessageTerminal           AttachedWorkerAttemptMessageKind = "terminal"
	AttachedWorkerAttemptMessageTerminalCommitted  AttachedWorkerAttemptMessageKind = "terminal_committed"

	AttachedWorkerDeadlineLeaseExpiry AttachedWorkerAttemptDeadlineKind = "lease_expiry"
	AttachedWorkerDeadlineCancelAck   AttachedWorkerAttemptDeadlineKind = "cancel_ack"

	AttachedWorkerTerminalSucceeded AttachedWorkerTerminalStatus = "succeeded"
	AttachedWorkerTerminalFailed    AttachedWorkerTerminalStatus = "failed"
	AttachedWorkerTerminalCancelled AttachedWorkerTerminalStatus = "cancelled"
)

// ExecutionPlacementV1 is required on every admitted dispatch and worker job.
// An explicit attached-worker selection is deny-only: it can never fall back
// to managed execution implicitly.
type ExecutionPlacementV1 struct {
	Version          uint32                         `json:"version"`
	Kind             ExecutionPlacementKind         `json:"kind"`
	FallbackPolicy   ExecutionFallbackPolicy        `json:"fallback_policy"`
	OwnerUserID      UserID                         `json:"owner_user_id,omitempty"`
	WorkerID         AttachedWorkerID               `json:"worker_id,omitempty"`
	CapabilityDigest AttachedWorkerCapabilityDigest `json:"capability_digest,omitempty"`
	PolicyDigest     AttachedWorkerPolicyDigest     `json:"policy_digest,omitempty"`
}

func ManagedExecutionPlacementV1() ExecutionPlacementV1 {
	return ExecutionPlacementV1{Version: ExecutionPlacementVersionV1, Kind: ExecutionPlacementManaged, FallbackPolicy: ExecutionFallbackDenied}
}

func (placement ExecutionPlacementV1) Validate() error {
	if placement.Version != ExecutionPlacementVersionV1 || placement.FallbackPolicy != ExecutionFallbackDenied {
		return ValidationError{Field: "execution_placement", Reason: "must be version 1 with deny fallback"}
	}
	switch placement.Kind {
	case ExecutionPlacementManaged:
		if placement.OwnerUserID != "" || placement.WorkerID != "" || placement.CapabilityDigest != "" || placement.PolicyDigest != "" {
			return ValidationError{Field: "execution_placement", Reason: "managed placement must not contain attached-worker targeting"}
		}
	case ExecutionPlacementAttachedWorker:
		if err := placement.OwnerUserID.Validate(); err != nil {
			return err
		}
		if err := placement.WorkerID.Validate(); err != nil {
			return err
		}
		if err := validateAttachedWorkerTransportDigest("execution_placement.capability_digest", string(placement.CapabilityDigest)); err != nil {
			return err
		}
		if err := placement.PolicyDigest.Validate(); err != nil {
			return err
		}
	default:
		return ValidationError{Field: "execution_placement.kind", Reason: "is unknown"}
	}
	return nil
}

func (digest AttachedWorkerPolicyDigest) Validate() error {
	return validateAttachedWorkerTransportDigest("attached_worker_attempt.policy_digest", string(digest))
}
func (digest AttachedWorkerContextDigest) Validate() error {
	return validateAttachedWorkerTransportDigest("attached_worker_attempt.context_digest", string(digest))
}

// AttachedWorkerJobContextDigestV1 binds an offer to the exact immutable
// execution inputs admitted in WorkerJob and the content of its loaded input
// artifact manifest. It deliberately excludes frontend
// origin/delivery routing and CreatedAt: those fields cannot change execution
// authority or worker-visible inputs. AllowedMCPServers is the sole semantic
// set in WorkerJob. Manifest artifacts are a name-keyed semantic set and are
// sorted by their unique name; every other field retains its typed position.
func AttachedWorkerJobContextDigestV1(job WorkerJob, manifest ArtifactManifest) (AttachedWorkerContextDigest, error) {
	if err := validateAttachedWorkerJobContext(job); err != nil {
		return "", err
	}
	if err := validateAttachedWorkerInputManifest(job, manifest); err != nil {
		return "", err
	}
	transcript := []byte("sessionless.attached-worker.job-context.v1\x00")
	appendString := func(name, value string) {
		transcript = appendAttachedWorkerContextField(transcript, name, []byte(value))
	}
	appendUint := func(name string, value uint64) {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], value)
		transcript = appendAttachedWorkerContextField(transcript, name, encoded[:])
	}
	appendBlob := func(name string, blob BlobRef) {
		appendString(name+".tenant_id", string(blob.TenantID))
		appendString(name+".key", blob.Key)
		appendUint(name+".size", uint64(blob.Size))
		appendString(name+".sha256", blob.SHA256)
	}
	appendOptionalBlob := func(name string, blob *BlobRef) {
		if blob == nil {
			appendUint(name+".present", 0)
			return
		}
		appendUint(name+".present", 1)
		appendBlob(name, *blob)
	}

	appendString("tenant_id", string(job.TenantID))
	appendString("run_id", string(job.RunID))
	appendString("session_id", string(job.SessionID))
	appendString("trigger_event_id", string(job.TriggerEventID))
	appendString("attempt_id", string(job.AttemptID))
	appendString("reservation_id", string(job.ReservationID))
	appendString("input_manifest_id", string(job.InputManifestID))
	appendString("input_manifest.tenant_id", string(manifest.TenantID))
	appendString("input_manifest.run_id", string(manifest.RunID))
	artifacts := append([]Artifact(nil), manifest.Artifacts...)
	sort.Slice(artifacts, func(left, right int) bool { return artifacts[left].Name < artifacts[right].Name })
	appendUint("input_manifest.artifacts.count", uint64(len(artifacts)))
	for _, artifact := range artifacts {
		appendString("input_manifest.artifact.name", artifact.Name)
		appendString("input_manifest.artifact.media_type", artifact.MediaType)
		appendBlob("input_manifest.artifact.blob", artifact.Blob)
	}
	if job.ContextWindow == nil {
		appendString("context.kind", "snapshot_blob")
		appendBlob("context.snapshot", job.ContextSnapshot)
	} else {
		appendString("context.kind", "window")
		if job.ContextWindow.SnapshotVersion == nil {
			appendUint("context.window.snapshot_version.present", 0)
		} else {
			appendUint("context.window.snapshot_version.present", 1)
			appendUint("context.window.snapshot_version", *job.ContextWindow.SnapshotVersion)
		}
		appendUint("context.window.after_sequence", job.ContextWindow.AfterSequence)
		appendUint("context.window.through_sequence", job.ContextWindow.ThroughSequence)
	}
	appendOptionalBlob("workspace_snapshot", job.WorkspaceSnapshot)
	appendOptionalBlob("skill_bundle", job.SkillBundle)
	servers := append([]string(nil), job.AllowedMCPServers...)
	sort.Strings(servers)
	appendUint("allowed_mcp_servers.count", uint64(len(servers)))
	for _, server := range servers {
		appendString("allowed_mcp_servers.item", server)
	}
	if job.CredentialOwnerUserID == "" {
		appendUint("credential_owner_user_id.present", 0)
	} else {
		appendUint("credential_owner_user_id.present", 1)
		appendString("credential_owner_user_id", string(job.CredentialOwnerUserID))
	}
	placement := job.ExecutionPlacement
	appendUint("execution_placement.version", uint64(placement.Version))
	appendString("execution_placement.kind", string(placement.Kind))
	appendString("execution_placement.fallback_policy", string(placement.FallbackPolicy))
	appendString("execution_placement.owner_user_id", string(placement.OwnerUserID))
	appendString("execution_placement.worker_id", string(placement.WorkerID))
	appendString("execution_placement.capability_digest", string(placement.CapabilityDigest))
	appendString("execution_placement.policy_digest", string(placement.PolicyDigest))
	limits := job.Limits
	appendUint("limits.max_tenant_queue_depth", uint64(limits.MaxTenantQueueDepth))
	appendUint("limits.max_active_runs", uint64(limits.MaxActiveRuns))
	appendUint("limits.max_runtime_nanoseconds", uint64(limits.MaxRuntime))
	appendUint("limits.max_turns", uint64(limits.MaxTurns))
	appendUint("limits.max_input_bytes", limits.MaxInputBytes)
	appendUint("limits.max_context_bytes", limits.MaxContextBytes)
	appendUint("limits.max_context_events", limits.MaxContextEvents)
	appendUint("limits.max_artifacts", uint64(limits.MaxArtifacts))
	appendUint("limits.max_tool_events", uint64(limits.MaxToolEvents))
	appendUint("limits.max_tool_event_bytes", limits.MaxToolEventBytes)

	sum := sha256.Sum256(transcript)
	return AttachedWorkerContextDigest(hex.EncodeToString(sum[:])), nil
}

func validateAttachedWorkerInputManifest(job WorkerJob, manifest ArtifactManifest) error {
	if manifest.ID != job.InputManifestID || manifest.TenantID != job.TenantID || manifest.RunID != job.RunID {
		return ValidationError{Field: "worker_job.input_manifest", Reason: "must match the admitted job scope"}
	}
	if err := manifest.ID.Validate(); err != nil {
		return err
	}
	if manifest.CreatedAt.IsZero() {
		return ValidationError{Field: "worker_job.input_manifest.created_at", Reason: "must not be zero"}
	}
	seen := make(map[string]struct{}, len(manifest.Artifacts))
	for _, artifact := range manifest.Artifacts {
		if err := artifact.Validate(); err != nil {
			return err
		}
		if artifact.Blob.TenantID != job.TenantID {
			return ValidationError{Field: "worker_job.input_manifest.artifact", Reason: "must belong to the admitted tenant"}
		}
		if _, exists := seen[artifact.Name]; exists {
			return ValidationError{Field: "worker_job.input_manifest.artifacts", Reason: "artifact names must be unique"}
		}
		seen[artifact.Name] = struct{}{}
	}
	return nil
}

func validateAttachedWorkerJobContext(job WorkerJob) error {
	for _, validation := range []func() error{job.TenantID.Validate, job.RunID.Validate, job.SessionID.Validate, job.TriggerEventID.Validate,
		job.AttemptID.Validate, job.ReservationID.Validate, job.InputManifestID.Validate} {
		if err := validation(); err != nil {
			return err
		}
	}
	if job.ExecutionPlacement.Kind != ExecutionPlacementAttachedWorker {
		return ValidationError{Field: "worker_job.execution_placement", Reason: "must select an attached worker"}
	}
	if err := job.ExecutionPlacement.Validate(); err != nil {
		return err
	}
	if err := job.Limits.ValidateForAdmission(); err != nil {
		return err
	}
	if job.ContextWindow == nil {
		if err := validateWorkerBlob(job.TenantID, "worker_job.context_snapshot", job.ContextSnapshot); err != nil {
			return err
		}
	} else if err := job.ContextWindow.Validate(); err != nil {
		return err
	}
	for name, blob := range map[string]*BlobRef{"worker_job.workspace_snapshot": job.WorkspaceSnapshot, "worker_job.skill_bundle": job.SkillBundle} {
		if blob != nil {
			if err := validateWorkerBlob(job.TenantID, name, *blob); err != nil {
				return err
			}
		}
	}
	seen := make(map[string]struct{}, len(job.AllowedMCPServers))
	for _, server := range job.AllowedMCPServers {
		if server == "" || server != strings.TrimSpace(server) {
			return ValidationError{Field: "worker_job.allowed_mcp_servers", Reason: "must contain unique non-empty exact names"}
		}
		if _, exists := seen[server]; exists {
			return ValidationError{Field: "worker_job.allowed_mcp_servers", Reason: "must not contain duplicates"}
		}
		seen[server] = struct{}{}
	}
	if job.CredentialOwnerUserID != "" {
		return job.CredentialOwnerUserID.Validate()
	}
	return nil
}

func appendAttachedWorkerContextField(destination []byte, name string, value []byte) []byte {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(name)))
	destination = append(destination, length[:]...)
	destination = append(destination, name...)
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	destination = append(destination, length[:]...)
	return append(destination, value...)
}
func (digest AttachedWorkerTerminalEvidenceDigest) Validate() error {
	return validateAttachedWorkerTransportDigest("attached_worker_attempt.terminal_evidence_digest", string(digest))
}
func (digest AttachedWorkerAttemptMessageFingerprint) Validate() error {
	return validateAttachedWorkerTransportDigest("attached_worker_attempt_message.fingerprint", string(digest))
}

func (token AttachedWorkerFenceToken) Validate() error {
	return validateAttachedWorkerTransportDigest("attached_worker_attempt.fence_token", string(token))
}

// AttachedWorkerLeaseGeneration is the exact canonical numeric lease fence.
// Attached-worker leases are fixed-expiry and are never renewed.
func AttachedWorkerLeaseGeneration(fence uint64) (uint64, error) {
	if fence == 0 {
		return 0, ValidationError{Field: "lease.fence_token", Reason: "must be positive"}
	}
	return fence, nil
}

// NewAttachedWorkerLeaseIDV1 deterministically maps an admitted attempt scope
// to an opaque retry-stable lease ID. The hash preimage is typed by a domain
// separator and length-prefixes every variable-width field.
func NewAttachedWorkerLeaseIDV1(tenantID TenantID, runID RunID, attemptID AttemptID) (LeaseID, error) {
	if err := tenantID.Validate(); err != nil {
		return "", err
	}
	if err := runID.Validate(); err != nil {
		return "", err
	}
	if err := attemptID.Validate(); err != nil {
		return "", err
	}
	hash := sha256.New()
	hash.Write([]byte("sessionless.attached-worker.lease-id.v1\x00"))
	for _, value := range []string{string(tenantID), string(runID), string(attemptID)} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		hash.Write(length[:])
		hash.Write([]byte(value))
	}
	return LeaseID("lea_" + hex.EncodeToString(hash.Sum(nil))), nil
}

// AttachedWorkerLeaseTTLForLimitsV1 derives the fixed, non-renewable lease
// lifetime from admitted product limits. The finalization budget is part of
// V1 and the resulting lifetime is capped at 24 hours inclusive.
func AttachedWorkerLeaseTTLForLimitsV1(limits ProductLimits) (time.Duration, error) {
	if err := limits.ValidateForAdmission(); err != nil {
		return 0, err
	}
	if limits.MaxRuntime > AttachedWorkerLeaseMaximumTTLV1-AttachedWorkerLeaseFinalizationBudgetV1 {
		return 0, ValidationError{Field: "limits.max_runtime", Reason: "exceeds the attached-worker fixed lease lifetime"}
	}
	return limits.MaxRuntime + AttachedWorkerLeaseFinalizationBudgetV1, nil
}

// NewAttachedWorkerFenceTokenV1 maps the canonical numeric fence to the
// protocol's opaque string fence without exposing sortable authority.
func NewAttachedWorkerFenceTokenV1(tenantID TenantID, ownerUserID UserID, workerID AttachedWorkerID, runID RunID, attemptID AttemptID, leaseID LeaseID, fence uint64) (AttachedWorkerFenceToken, error) {
	if err := tenantID.Validate(); err != nil {
		return "", err
	}
	if err := ownerUserID.Validate(); err != nil {
		return "", err
	}
	if err := workerID.Validate(); err != nil {
		return "", err
	}
	if err := runID.Validate(); err != nil {
		return "", err
	}
	if err := attemptID.Validate(); err != nil {
		return "", err
	}
	if err := leaseID.Validate(); err != nil {
		return "", err
	}
	if _, err := AttachedWorkerLeaseGeneration(fence); err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte("sessionless.attached-worker.fence.v1\x00"))
	for _, value := range []string{string(tenantID), string(ownerUserID), string(workerID), string(runID), string(attemptID), string(leaseID)} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		h.Write(length[:])
		h.Write([]byte(value))
	}
	var numeric [8]byte
	binary.BigEndian.PutUint64(numeric[:], fence)
	h.Write(numeric[:])
	return AttachedWorkerFenceToken(hex.EncodeToString(h.Sum(nil))), nil
}

func AttachedWorkerAttemptDeadlineBucketV1(tenantID TenantID, ownerUserID UserID, workerID AttachedWorkerID, attemptID AttemptID) (uint32, error) {
	if err := tenantID.Validate(); err != nil {
		return 0, err
	}
	if err := ownerUserID.Validate(); err != nil {
		return 0, err
	}
	if err := workerID.Validate(); err != nil {
		return 0, err
	}
	if err := attemptID.Validate(); err != nil {
		return 0, err
	}
	h := sha256.New()
	h.Write([]byte("sessionless.attached-worker.attempt-deadline-bucket.v1\x00"))
	for _, value := range []string{string(tenantID), string(ownerUserID), string(workerID), string(attemptID)} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		h.Write(length[:])
		h.Write([]byte(value))
	}
	return uint32(h.Sum(nil)[0] & (AttachedWorkerAttemptDeadlineBuckets - 1)), nil
}

func (state AttachedWorkerAttemptState) Valid() bool {
	switch state {
	case AttachedWorkerAttemptOffered, AttachedWorkerAttemptClaimed, AttachedWorkerAttemptCancelRequested,
		AttachedWorkerAttemptCancelAcknowledged, AttachedWorkerAttemptTerminalPending,
		AttachedWorkerAttemptTerminalCommitted, AttachedWorkerAttemptCancelledBeforeClaim,
		AttachedWorkerAttemptFencedUnknown, AttachedWorkerAttemptRetired:
		return true
	default:
		return false
	}
}

func (direction AttachedWorkerAttemptDirection) Valid() bool {
	return direction == AttachedWorkerAttemptPlatformToWorker || direction == AttachedWorkerAttemptWorkerToPlatform
}
func (kind AttachedWorkerAttemptMessageKind) Valid() bool {
	switch kind {
	case AttachedWorkerAttemptMessageLeaseOffered, AttachedWorkerAttemptMessageLeaseClaim, AttachedWorkerAttemptMessageLeaseAccepted, AttachedWorkerAttemptMessageProgress,
		AttachedWorkerAttemptMessageCancelRequested, AttachedWorkerAttemptMessageCancelAcknowledged,
		AttachedWorkerAttemptMessageTerminal, AttachedWorkerAttemptMessageTerminalCommitted:
		return true
	default:
		return false
	}
}
func (kind AttachedWorkerAttemptDeadlineKind) Valid() bool {
	return kind == AttachedWorkerDeadlineLeaseExpiry || kind == AttachedWorkerDeadlineCancelAck
}
func (status AttachedWorkerTerminalStatus) Valid() bool {
	return status == AttachedWorkerTerminalSucceeded || status == AttachedWorkerTerminalFailed || status == AttachedWorkerTerminalCancelled
}

// AttachedWorkerAttemptV1 is the durable owner-scoped authority head. A
// fenced_unknown head blocks both retry and canonical finalization until a
// later explicit resolution contract is introduced.
type AttachedWorkerAttemptV1 struct {
	Version                 uint32                               `json:"version"`
	TenantID                TenantID                             `json:"tenant_id"`
	OwnerUserID             UserID                               `json:"owner_user_id"`
	WorkerID                AttachedWorkerID                     `json:"worker_id"`
	ConnectionID            AttachedWorkerConnectionID           `json:"connection_id"`
	RunID                   RunID                                `json:"run_id"`
	AttemptID               AttemptID                            `json:"attempt_id"`
	ReservationID           QuotaReservationID                   `json:"reservation_id"`
	LeaseID                 LeaseID                              `json:"lease_id"`
	LeaseGeneration         uint64                               `json:"lease_generation"`
	FenceToken              AttachedWorkerFenceToken             `json:"fence_token"`
	EnrollmentGeneration    uint64                               `json:"enrollment_generation"`
	ConnectionGeneration    uint64                               `json:"connection_generation"`
	ContextDigest           AttachedWorkerContextDigest          `json:"context_digest"`
	CapabilityDigest        AttachedWorkerCapabilityDigest       `json:"capability_digest"`
	PolicyDigest            AttachedWorkerPolicyDigest           `json:"policy_digest"`
	State                   AttachedWorkerAttemptState           `json:"state"`
	PlatformAttemptSequence uint64                               `json:"platform_attempt_sequence"`
	WorkerAttemptSequence   uint64                               `json:"worker_attempt_sequence"`
	ProgressSequence        uint64                               `json:"progress_sequence"`
	CancelRevision          uint64                               `json:"cancel_revision"`
	TerminalSequence        uint64                               `json:"terminal_sequence"`
	TerminalStatus          AttachedWorkerTerminalStatus         `json:"terminal_status,omitempty"`
	TerminalEvidenceDigest  AttachedWorkerTerminalEvidenceDigest `json:"terminal_evidence_digest,omitempty"`
	LeaseExpiresAt          time.Time                            `json:"lease_expires_at"`
	CancelDeadline          time.Time                            `json:"cancel_deadline,omitempty"`
	CreatedAt               time.Time                            `json:"created_at"`
	UpdatedAt               time.Time                            `json:"updated_at"`
	Revision                uint64                               `json:"revision"`
}

func (attempt AttachedWorkerAttemptV1) Validate() error {
	if attempt.Version != AttachedWorkerAttemptVersionV1 {
		return ValidationError{Field: "attached_worker_attempt.version", Reason: "must be version 1"}
	}
	if err := attempt.TenantID.Validate(); err != nil {
		return err
	}
	if err := attempt.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := attempt.WorkerID.Validate(); err != nil {
		return err
	}
	if err := attempt.ConnectionID.Validate(); err != nil {
		return err
	}
	if err := attempt.RunID.Validate(); err != nil {
		return err
	}
	if err := attempt.AttemptID.Validate(); err != nil {
		return err
	}
	if err := attempt.ReservationID.Validate(); err != nil {
		return err
	}
	if err := attempt.LeaseID.Validate(); err != nil {
		return err
	}
	if attempt.LeaseGeneration == 0 || attempt.EnrollmentGeneration == 0 || attempt.ConnectionGeneration == 0 || attempt.Revision == 0 {
		return ValidationError{Field: "attached_worker_attempt.generations", Reason: "must be positive"}
	}
	if err := attempt.FenceToken.Validate(); err != nil {
		return err
	}
	expectedFence, err := NewAttachedWorkerFenceTokenV1(attempt.TenantID, attempt.OwnerUserID, attempt.WorkerID, attempt.RunID, attempt.AttemptID, attempt.LeaseID, attempt.LeaseGeneration)
	if err != nil {
		return err
	}
	if attempt.FenceToken != expectedFence {
		return ValidationError{Field: "attached_worker_attempt.fence_token", Reason: "must match the canonical lease generation and scope"}
	}
	if err := attempt.ContextDigest.Validate(); err != nil {
		return err
	}
	if err := validateAttachedWorkerTransportDigest("attached_worker_attempt.capability_digest", string(attempt.CapabilityDigest)); err != nil {
		return err
	}
	if err := attempt.PolicyDigest.Validate(); err != nil {
		return err
	}
	if !attempt.State.Valid() {
		return ValidationError{Field: "attached_worker_attempt.state", Reason: "is unknown"}
	}
	if attempt.CreatedAt.IsZero() || !attempt.LeaseExpiresAt.After(attempt.CreatedAt) || attempt.UpdatedAt.Before(attempt.CreatedAt) {
		return ValidationError{Field: "attached_worker_attempt.timestamps", Reason: "must be ordered"}
	}
	if attempt.PlatformAttemptSequence == 0 {
		return ValidationError{Field: "attached_worker_attempt.platform_attempt_sequence", Reason: "must be positive"}
	}
	if attempt.ProgressSequence > attempt.WorkerAttemptSequence {
		return ValidationError{Field: "attached_worker_attempt.progress_sequence", Reason: "must not exceed worker sequence"}
	}
	hasCancel := attempt.CancelRevision > 0 && !attempt.CancelDeadline.IsZero()
	if (attempt.CancelRevision == 0) != attempt.CancelDeadline.IsZero() {
		return ValidationError{Field: "attached_worker_attempt.cancel", Reason: "revision and deadline must be present together"}
	}
	requiresCancel := attempt.State == AttachedWorkerAttemptCancelRequested || attempt.State == AttachedWorkerAttemptCancelAcknowledged || attempt.State == AttachedWorkerAttemptCancelledBeforeClaim
	if requiresCancel && !hasCancel {
		return ValidationError{Field: "attached_worker_attempt.cancel", Reason: "cancel states require revision and deadline only"}
	}
	hasTerminal := attempt.TerminalSequence > 0 && attempt.TerminalStatus.Valid() && attempt.TerminalEvidenceDigest != ""
	if (attempt.TerminalSequence == 0) != (attempt.TerminalStatus == "") || (attempt.TerminalSequence == 0) != (attempt.TerminalEvidenceDigest == "") {
		return ValidationError{Field: "attached_worker_attempt.terminal", Reason: "terminal fields must be present together"}
	}
	if attempt.TerminalSequence > 0 && !attempt.TerminalStatus.Valid() {
		return ValidationError{Field: "attached_worker_attempt.terminal_status", Reason: "is unknown"}
	}
	requiresTerminal := attempt.State == AttachedWorkerAttemptTerminalPending || attempt.State == AttachedWorkerAttemptTerminalCommitted
	if requiresTerminal && !hasTerminal {
		return ValidationError{Field: "attached_worker_attempt.terminal", Reason: "terminal state requires status and sequence"}
	}
	if hasTerminal {
		if attempt.TerminalSequence > attempt.WorkerAttemptSequence {
			return ValidationError{Field: "attached_worker_attempt.terminal", Reason: "terminal state requires status and sequence"}
		}
		if err := attempt.TerminalEvidenceDigest.Validate(); err != nil {
			return err
		}
	}
	if (attempt.State == AttachedWorkerAttemptOffered || attempt.State == AttachedWorkerAttemptClaimed || requiresCancel) && hasTerminal {
		return ValidationError{Field: "attached_worker_attempt.terminal", Reason: "must be empty before terminal state"}
	}
	if attempt.State == AttachedWorkerAttemptRetired && !hasTerminal && !hasCancel {
		return ValidationError{Field: "attached_worker_attempt.state", Reason: "retired state requires durable terminal or cancellation evidence"}
	}
	if attempt.State == AttachedWorkerAttemptOffered && attempt.WorkerAttemptSequence != 0 {
		return ValidationError{Field: "attached_worker_attempt.worker_attempt_sequence", Reason: "offered attempt must be unclaimed"}
	}
	if attempt.State != AttachedWorkerAttemptOffered && attempt.State != AttachedWorkerAttemptCancelledBeforeClaim && attempt.WorkerAttemptSequence == 0 {
		return ValidationError{Field: "attached_worker_attempt.worker_attempt_sequence", Reason: "claimed state requires worker sequence"}
	}
	return nil
}

type AttachedWorkerAttemptMessageV1 struct {
	Version                      uint32                                  `json:"version"`
	TenantID                     TenantID                                `json:"tenant_id"`
	OwnerUserID                  UserID                                  `json:"owner_user_id"`
	WorkerID                     AttachedWorkerID                        `json:"worker_id"`
	AttemptID                    AttemptID                               `json:"attempt_id"`
	Direction                    AttachedWorkerAttemptDirection          `json:"direction"`
	AttemptSequence              uint64                                  `json:"attempt_sequence"`
	ConnectionGeneration         uint64                                  `json:"connection_generation"`
	EnvelopeSequence             uint64                                  `json:"envelope_sequence"`
	Kind                         AttachedWorkerAttemptMessageKind        `json:"kind"`
	Fingerprint                  AttachedWorkerAttemptMessageFingerprint `json:"fingerprint"`
	Payload                      []byte                                  `json:"payload"`
	CreatedAt                    time.Time                               `json:"created_at"`
	OperationDeadline            time.Time                               `json:"operation_deadline,omitempty"`
	MaterializationReservationID QuotaReservationID                      `json:"materialization_reservation_id,omitempty"`
	ExecutionConnectionID        AttachedWorkerConnectionID              `json:"execution_connection_id,omitempty"`
}

func (message AttachedWorkerAttemptMessageV1) Validate() error {
	if message.Version != AttachedWorkerAttemptMessageVersionV1 {
		return ValidationError{Field: "attached_worker_attempt_message.version", Reason: "must be version 1"}
	}
	if err := message.TenantID.Validate(); err != nil {
		return err
	}
	if err := message.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := message.WorkerID.Validate(); err != nil {
		return err
	}
	if err := message.AttemptID.Validate(); err != nil {
		return err
	}
	if !message.Direction.Valid() || !message.Kind.Valid() || message.AttemptSequence == 0 || message.ConnectionGeneration == 0 || message.EnvelopeSequence == 0 {
		return ValidationError{Field: "attached_worker_attempt_message.routing", Reason: "must be complete"}
	}
	if err := message.Fingerprint.Validate(); err != nil {
		return err
	}
	if len(message.Payload) == 0 || len(message.Payload) > maxAttachedWorkerAttemptMessageBytes {
		return ValidationError{Field: "attached_worker_attempt_message.payload", Reason: "must be non-empty and at most 64 KiB"}
	}
	if message.CreatedAt.IsZero() {
		return ValidationError{Field: "attached_worker_attempt_message.created_at", Reason: "must be non-zero"}
	}
	if (message.Kind == AttachedWorkerAttemptMessageCancelRequested) != !message.OperationDeadline.IsZero() {
		return ValidationError{Field: "attached_worker_attempt_message.operation_deadline", Reason: "must be present only for cancellation requests"}
	}
	terminalKind := message.Kind == AttachedWorkerAttemptMessageTerminal || message.Kind == AttachedWorkerAttemptMessageTerminalCommitted
	if terminalKind != (message.MaterializationReservationID != "") {
		return ValidationError{Field: "attached_worker_attempt_message.materialization_reservation_id", Reason: "must be present only for terminal evidence and acknowledgement"}
	}
	if terminalKind {
		if err := message.MaterializationReservationID.Validate(); err != nil {
			return err
		}
		if err := message.ExecutionConnectionID.Validate(); err != nil {
			return err
		}
	} else if message.ExecutionConnectionID != "" {
		return ValidationError{Field: "attached_worker_attempt_message.execution_connection_id", Reason: "must be present only for terminal evidence and acknowledgement"}
	}
	platformKind := message.Kind == AttachedWorkerAttemptMessageLeaseOffered || message.Kind == AttachedWorkerAttemptMessageLeaseAccepted || message.Kind == AttachedWorkerAttemptMessageCancelRequested || message.Kind == AttachedWorkerAttemptMessageTerminalCommitted
	if platformKind != (message.Direction == AttachedWorkerAttemptPlatformToWorker) {
		return ValidationError{Field: "attached_worker_attempt_message.direction", Reason: "must match message kind"}
	}
	return nil
}

type AttachedWorkerAttemptDeadlineV1 struct {
	Bucket          uint32                            `json:"bucket"`
	DeadlineAt      time.Time                         `json:"deadline_at"`
	TenantID        TenantID                          `json:"tenant_id"`
	OwnerUserID     UserID                            `json:"owner_user_id"`
	WorkerID        AttachedWorkerID                  `json:"worker_id"`
	AttemptID       AttemptID                         `json:"attempt_id"`
	Kind            AttachedWorkerAttemptDeadlineKind `json:"kind"`
	LeaseGeneration uint64                            `json:"lease_generation"`
	AttemptRevision uint64                            `json:"attempt_revision"`
}

func (deadline AttachedWorkerAttemptDeadlineV1) Validate() error {
	if deadline.Bucket >= AttachedWorkerAttemptDeadlineBuckets || deadline.DeadlineAt.IsZero() || !deadline.Kind.Valid() || deadline.LeaseGeneration == 0 || deadline.AttemptRevision == 0 {
		return ValidationError{Field: "attached_worker_attempt_deadline", Reason: "must contain a valid bucket, kind, deadline, generation and revision"}
	}
	if err := deadline.TenantID.Validate(); err != nil {
		return err
	}
	if err := deadline.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := deadline.WorkerID.Validate(); err != nil {
		return err
	}
	if err := deadline.AttemptID.Validate(); err != nil {
		return err
	}
	expected, err := AttachedWorkerAttemptDeadlineBucketV1(deadline.TenantID, deadline.OwnerUserID, deadline.WorkerID, deadline.AttemptID)
	if err != nil {
		return err
	}
	if deadline.Bucket != expected {
		return ValidationError{Field: "attached_worker_attempt_deadline.bucket", Reason: "must match the canonical scope bucket"}
	}
	return nil
}
