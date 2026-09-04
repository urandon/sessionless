package attachedworkerux

import "time"

const (
	ReadModelVersionV1 uint32 = 1
	MaxListLimitV1     uint64 = 50
)

type FreshnessV1 string

const (
	FreshnessUnknown FreshnessV1 = "unknown"
	FreshnessFresh   FreshnessV1 = "fresh"
	FreshnessExpired FreshnessV1 = "expired"
)

type ReasonCodeV1 string

const (
	ReasonWorkerNotActive            ReasonCodeV1 = "worker_not_active"
	ReasonWorkerRevoked              ReasonCodeV1 = "worker_revoked"
	ReasonWorkerDraining             ReasonCodeV1 = "worker_draining"
	ReasonWorkerOffline              ReasonCodeV1 = "worker_offline"
	ReasonConnectionAttaching        ReasonCodeV1 = "connection_attaching"
	ReasonConnectionSuperseded       ReasonCodeV1 = "connection_superseded"
	ReasonPresenceExpired            ReasonCodeV1 = "presence_expired"
	ReasonAuthenticationExpired      ReasonCodeV1 = "authentication_expired"
	ReasonProtocolIncompatible       ReasonCodeV1 = "protocol_incompatible"
	ReasonCapabilityMissing          ReasonCodeV1 = "capability_missing"
	ReasonCapabilityStale            ReasonCodeV1 = "capability_stale"
	ReasonCapabilityMismatch         ReasonCodeV1 = "capability_mismatch"
	ReasonPolicyMismatch             ReasonCodeV1 = "policy_mismatch"
	ReasonIsolationUnsupported       ReasonCodeV1 = "isolation_unsupported"
	ReasonIsolationUnverified        ReasonCodeV1 = "isolation_unverified"
	ReasonCredentialUnavailable      ReasonCodeV1 = "credential_unavailable"
	ReasonCredentialReauthRequired   ReasonCodeV1 = "credential_reauth_required"
	ReasonEntitlementUnknown         ReasonCodeV1 = "entitlement_unknown"
	ReasonEntitlementInactive        ReasonCodeV1 = "entitlement_inactive"
	ReasonQuotaUnknown               ReasonCodeV1 = "quota_unknown"
	ReasonQuotaZero                  ReasonCodeV1 = "quota_zero"
	ReasonQuotaExhausted             ReasonCodeV1 = "quota_exhausted"
	ReasonCapacityBusy               ReasonCodeV1 = "capacity_busy"
	ReasonAttemptActive              ReasonCodeV1 = "attempt_active"
	ReasonAttemptAmbiguous           ReasonCodeV1 = "attempt_ambiguous"
	ReasonControlContractUnavailable ReasonCodeV1 = "control_contract_unavailable"
	ReasonBackendUnavailable         ReasonCodeV1 = "backend_unavailable"
)

type ActionCodeV1 string

const (
	ActionCreateEnrollment     ActionCodeV1 = "create_enrollment"
	ActionConsumeEnrollment    ActionCodeV1 = "consume_enrollment"
	ActionRename               ActionCodeV1 = "rename"
	ActionRotateIdentity       ActionCodeV1 = "rotate_identity"
	ActionPauseAdmission       ActionCodeV1 = "pause_admission"
	ActionResumeAdmission      ActionCodeV1 = "resume_admission"
	ActionDrain                ActionCodeV1 = "drain"
	ActionRevoke               ActionCodeV1 = "revoke"
	ActionRequestCancel        ActionCodeV1 = "request_cancel"
	ActionReconnectRemediation ActionCodeV1 = "reconnect_remediation"
	ActionReauthRemediation    ActionCodeV1 = "reauth_remediation"
	ActionCheckUpdate          ActionCodeV1 = "check_update"
	ActionLogout               ActionCodeV1 = "logout"
	ActionUninstallPlan        ActionCodeV1 = "uninstall_plan"
)

type ActionUnavailableCodeV1 string

const (
	ActionUnavailableNotFound                ActionUnavailableCodeV1 = "not_found"
	ActionUnavailableStaleRevision           ActionUnavailableCodeV1 = "stale_revision"
	ActionUnavailableStaleGeneration         ActionUnavailableCodeV1 = "stale_generation"
	ActionUnavailableInvalidState            ActionUnavailableCodeV1 = "invalid_state"
	ActionUnavailableActiveAttempt           ActionUnavailableCodeV1 = "active_attempt"
	ActionUnavailableAmbiguousAttempt        ActionUnavailableCodeV1 = "ambiguous_attempt"
	ActionUnavailableAwaitingAcknowledgement ActionUnavailableCodeV1 = "awaiting_acknowledgement"
	ActionUnavailableAlreadyApplied          ActionUnavailableCodeV1 = "already_applied"
	ActionUnavailableUnsupportedPlatform     ActionUnavailableCodeV1 = "unsupported_platform"
	ActionUnavailableFeatureDisabled         ActionUnavailableCodeV1 = "feature_disabled"
	ActionUnavailableControlContract         ActionUnavailableCodeV1 = "control_contract_unavailable"
	ActionUnavailableConfirmationRequired    ActionUnavailableCodeV1 = "confirmation_required"
	ActionUnavailableOperationInProgress     ActionUnavailableCodeV1 = "operation_in_progress"
)

type WorkerV1 struct {
	WorkerID             string    `json:"worker_id"`
	DisplayName          string    `json:"display_name"`
	Revision             uint64    `json:"revision,string"`
	EnrollmentGeneration uint64    `json:"enrollment_generation,string"`
	ConnectionGeneration uint64    `json:"connection_generation,string"`
	DesiredState         string    `json:"desired_state"`
	ObservedState        string    `json:"observed_state"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
	RevokedAt            time.Time `json:"revoked_at,omitzero"`
}

type IdentityV1 struct {
	Algorithm       string `json:"algorithm"`
	Fingerprint     string `json:"fingerprint"`
	EnrollmentState string `json:"enrollment_state"`
}

type LastFailureV1 struct {
	State      string      `json:"state"`
	Code       string      `json:"code,omitempty"`
	OccurredAt time.Time   `json:"occurred_at,omitzero"`
	Operation  string      `json:"operation,omitempty"`
	RetryClass string      `json:"retry_class,omitempty"`
	Source     string      `json:"source,omitempty"`
	Freshness  FreshnessV1 `json:"freshness,omitempty"`
}

type DaemonObservationV1 struct {
	State      string      `json:"state"`
	Source     string      `json:"source"`
	ObservedAt time.Time   `json:"observed_at,omitzero"`
	Freshness  FreshnessV1 `json:"freshness"`
}

type IsolationV1 struct {
	ConfigurationState string   `json:"configuration_state"`
	AdvertisedEvidence []string `json:"advertised_evidence"`
	VerificationState  string   `json:"verification_state"`
}

type ReadinessV1 struct {
	DaemonObservation DaemonObservationV1 `json:"daemon_observation"`
	LastDaemonFailure LastFailureV1       `json:"last_daemon_failure"`
	CredentialState   string              `json:"credential_state"`
	Isolation         IsolationV1         `json:"isolation"`
}

type ConnectivityV1 struct {
	ConnectionID            string        `json:"connection_id,omitempty"`
	State                   string        `json:"state"`
	ConnectedAt             time.Time     `json:"connected_at,omitzero"`
	LastContactAt           time.Time     `json:"last_contact_at,omitzero"`
	PresenceExpiresAt       time.Time     `json:"presence_expires_at,omitzero"`
	AuthenticationExpiresAt time.Time     `json:"authentication_expires_at,omitzero"`
	Freshness               FreshnessV1   `json:"freshness"`
	LastFailure             LastFailureV1 `json:"last_failure"`
}

type HarnessV1 struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
	Surface string `json:"surface,omitempty"`
}

type CapabilityV1 struct {
	State                 string    `json:"state"`
	ManifestRevision      uint64    `json:"manifest_revision,omitempty,string"`
	DigestFingerprint     string    `json:"digest_fingerprint,omitempty"`
	OperatingSystem       string    `json:"operating_system,omitempty"`
	Architecture          string    `json:"architecture,omitempty"`
	BuildID               string    `json:"build_id,omitempty"`
	Harness               HarnessV1 `json:"harness"`
	IsolationEvidence     []string  `json:"isolation_evidence"`
	Features              []string  `json:"features"`
	MaxConcurrentAttempts uint32    `json:"max_concurrent_attempts,omitempty"`
	ObservedAt            time.Time `json:"observed_at,omitzero"`
}

type AdmissionPreviewV1 struct {
	State                       string    `json:"state"`
	DecisionCode                string    `json:"decision_code,omitempty"`
	EvaluationRef               string    `json:"evaluation_ref,omitempty"`
	CandidateRef                string    `json:"candidate_ref,omitempty"`
	EvaluatedAt                 time.Time `json:"evaluated_at,omitzero"`
	WorkerRevision              uint64    `json:"worker_revision,omitempty,string"`
	EnrollmentGeneration        uint64    `json:"enrollment_generation,omitempty,string"`
	ConnectionGeneration        uint64    `json:"connection_generation,omitempty,string"`
	CapabilityDigestFingerprint string    `json:"capability_digest_fingerprint,omitempty"`
	PolicyDigestFingerprint     string    `json:"policy_digest_fingerprint,omitempty"`
	ContextDigestFingerprint    string    `json:"context_digest_fingerprint,omitempty"`
}

type QuotaObservationV1 struct {
	State      string      `json:"state"`
	Remaining  *uint64     `json:"remaining,omitempty,string"`
	ObservedAt time.Time   `json:"observed_at,omitzero"`
	ResetAt    time.Time   `json:"reset_at,omitzero"`
	Source     string      `json:"source,omitempty"`
	Freshness  FreshnessV1 `json:"freshness,omitempty"`
}

type ResourceV1 struct {
	State            string             `json:"state"`
	ResourceRef      string             `json:"resource_ref,omitempty"`
	CredentialState  string             `json:"credential_state"`
	EntitlementState string             `json:"entitlement_state"`
	Quota            QuotaObservationV1 `json:"quota"`
}

type CancelRequestV1 struct {
	State       string    `json:"state"`
	Revision    uint64    `json:"revision,omitempty,string"`
	RequestedAt time.Time `json:"requested_at,omitzero"`
	AckDeadline time.Time `json:"ack_deadline,omitzero"`
}

type CancelAcknowledgementV1 struct {
	State          string    `json:"state"`
	Revision       uint64    `json:"revision,omitempty,string"`
	AcknowledgedAt time.Time `json:"acknowledged_at,omitzero"`
}

type ProcessObservationV1 struct {
	State            string      `json:"state"`
	AttemptID        string      `json:"attempt_id,omitempty"`
	LeaseGeneration  uint64      `json:"lease_generation,omitempty,string"`
	FenceFingerprint string      `json:"fence_fingerprint,omitempty"`
	Source           string      `json:"source"`
	ObservedAt       time.Time   `json:"observed_at,omitzero"`
	Freshness        FreshnessV1 `json:"freshness"`
}

type WorkerTerminalV1 struct {
	State               string `json:"state"`
	Sequence            uint64 `json:"sequence,omitempty,string"`
	Status              string `json:"status,omitempty"`
	EvidenceFingerprint string `json:"evidence_fingerprint,omitempty"`
}

type CanonicalTerminalV1 struct {
	State       string    `json:"state"`
	CommittedAt time.Time `json:"committed_at,omitzero"`
	Sequence    uint64    `json:"sequence,omitempty,string"`
	Status      string    `json:"status,omitempty"`
}

type ExecutionV1 struct {
	State                 string                  `json:"state"`
	RunID                 string                  `json:"run_id,omitempty"`
	AttemptID             string                  `json:"attempt_id,omitempty"`
	LeaseID               string                  `json:"lease_id,omitempty"`
	LeaseGeneration       uint64                  `json:"lease_generation,omitempty,string"`
	FenceFingerprint      string                  `json:"fence_fingerprint,omitempty"`
	LeaseExpiresAt        time.Time               `json:"lease_expires_at,omitzero"`
	CancelRequest         CancelRequestV1         `json:"cancel_request"`
	CancelAcknowledgement CancelAcknowledgementV1 `json:"cancel_ack"`
	ProcessObservation    ProcessObservationV1    `json:"process_observation"`
	WorkerTerminal        WorkerTerminalV1        `json:"worker_terminal"`
	CanonicalTerminal     CanonicalTerminalV1     `json:"canonical_terminal"`
}

type AvailableActionV1 struct {
	Code         ActionCodeV1            `json:"code"`
	Enabled      bool                    `json:"enabled"`
	ReasonCode   ActionUnavailableCodeV1 `json:"reason_code,omitempty"`
	Confirmation string                  `json:"confirmation,omitempty"`
}

type GovernanceV1 struct {
	AdmissionControl string              `json:"admission_control"`
	RemoteErase      string              `json:"remote_erase"`
	AvailableActions []AvailableActionV1 `json:"available_actions"`
}

type AttachedWorkerUXReadModelV1 struct {
	Version             uint32             `json:"version"`
	EvaluatedAt         time.Time          `json:"evaluated_at"`
	Worker              WorkerV1           `json:"worker"`
	Identity            IdentityV1         `json:"identity"`
	Readiness           ReadinessV1        `json:"readiness"`
	Connectivity        ConnectivityV1     `json:"connectivity"`
	Capability          CapabilityV1       `json:"capability"`
	AdmissionPreview    AdmissionPreviewV1 `json:"admission_preview"`
	ObservationWarnings []ReasonCodeV1     `json:"observation_warnings"`
	Resource            ResourceV1         `json:"resource"`
	Execution           ExecutionV1        `json:"execution"`
	Governance          GovernanceV1       `json:"governance"`
}

type AttachedWorkerSummaryV1 struct {
	EvaluatedAt         time.Time      `json:"evaluated_at"`
	Worker              WorkerV1       `json:"worker"`
	Connectivity        ConnectivityV1 `json:"connectivity"`
	ExecutionState      string         `json:"execution_state"`
	ObservationWarnings []ReasonCodeV1 `json:"observation_warnings"`
}

type AttachedWorkerListV1 struct {
	Version      uint32                    `json:"version"`
	EvaluatedAt  time.Time                 `json:"evaluated_at"`
	Items        []AttachedWorkerSummaryV1 `json:"items"`
	NextWorkerID string                    `json:"next_worker_id,omitempty"`
	HasMore      bool                      `json:"has_more"`
}

type DiagnosticFactV1 struct {
	Cohort     string      `json:"cohort"`
	Code       string      `json:"code"`
	State      string      `json:"state"`
	ObservedAt time.Time   `json:"observed_at,omitzero"`
	Freshness  FreshnessV1 `json:"freshness,omitempty"`
}

type AttachedWorkerDiagnosticsV1 struct {
	Version     uint32             `json:"version"`
	EvaluatedAt time.Time          `json:"evaluated_at"`
	WorkerID    string             `json:"worker_id"`
	Facts       []DiagnosticFactV1 `json:"facts"`
	Warnings    []ReasonCodeV1     `json:"warnings"`
}

// The action envelopes intentionally carry no tenant, owner, revision,
// generation, deadline, fence, desired result, raw command, proof, or secret.
// A future durable plan store seals action-specific authority server-side.
type ActionPlanRequestV1 struct {
	Version uint32       `json:"version"`
	Action  ActionCodeV1 `json:"action"`
}

type ActionPlanV1 struct {
	Version      uint32       `json:"version"`
	PlanID       string       `json:"plan_id"`
	WorkerID     string       `json:"worker_id"`
	Action       ActionCodeV1 `json:"action"`
	ExpiresAt    time.Time    `json:"expires_at"`
	Confirmation string       `json:"confirmation"`
}

type ActionApplyV1 struct {
	Version        uint32 `json:"version"`
	PlanID         string `json:"plan_id"`
	Confirmation   string `json:"confirmation"`
	IdempotencyKey string `json:"idempotency_key"`
}

type ActionOperationV1 struct {
	Version     uint32                  `json:"version"`
	OperationID string                  `json:"operation_id"`
	WorkerID    string                  `json:"worker_id"`
	Action      ActionCodeV1            `json:"action"`
	State       string                  `json:"state"`
	ReasonCode  ActionUnavailableCodeV1 `json:"reason_code,omitempty"`
	CreatedAt   time.Time               `json:"created_at"`
	CompletedAt time.Time               `json:"completed_at,omitzero"`
}
