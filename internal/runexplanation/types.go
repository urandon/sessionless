// Package runexplanation provides non-authoritative, pure read projections.
// Callers must reauthorize the Web session, membership and exact Session read
// participation, and join all inputs in one serializable transaction (#187).
// These reducers never grant execution, scheduling, lease or retry authority.
package runexplanation

import (
	"encoding/json"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	VersionV1              uint32 = 1
	TerminalNoticeSchemaV1        = "sessionless.run-terminal-notice.v1"
	MaxProjectionBytes            = 8 << 10
	MaxResponseBytes              = 16 << 10
	MaxPointStatements            = 20
	ResourceDeadline              = 3 * time.Second
	MinimumRefreshInterval        = 5 * time.Second
)

// Errors and diagnostics are content-free: never wrap private source values.
var (
	ErrInvalidSource     = errors.New("run explanation: invalid source")
	ErrCorruptProjection = errors.New("run explanation: corrupt projection")
	ErrRevisionExhausted = errors.New("run explanation: revision exhausted")
	ErrStaleObservation  = errors.New("run explanation: stale observation")
	ErrInvalidResponse   = errors.New("run explanation: invalid response")
)

type Availability string

const (
	Recorded      Availability = "recorded"
	Unknown       Availability = "unknown"
	NotApplicable Availability = "not_applicable"
)

func (v Availability) Valid() bool                  { return v == Recorded || v == Unknown || v == NotApplicable }
func (v Availability) MarshalJSON() ([]byte, error) { return closedJSON(string(v), v.Valid()) }

type AdmissionOutcome string

const (
	Admitted AdmissionOutcome = "admitted"
	Denied   AdmissionOutcome = "denied"
)

func (v AdmissionOutcome) Valid() bool                  { return v == Admitted || v == Denied }
func (v AdmissionOutcome) MarshalJSON() ([]byte, error) { return closedJSON(string(v), v.Valid()) }

type AdmissionReason string

const (
	ReasonAdmitted                AdmissionReason = "admitted"
	SubscriptionAttentionRequired AdmissionReason = "subscription_attention_required"
	CapacityDraining              AdmissionReason = "capacity_draining"
	QuotaResetPending             AdmissionReason = "quota_reset_pending"
	QuotaExhaustedResetUnknown    AdmissionReason = "quota_exhausted_reset_unknown"
	RuntimeLimitExceeded          AdmissionReason = "runtime_limit_exceeded"
	TurnLimitExceeded             AdmissionReason = "turn_limit_exceeded"
	InputLimitExceeded            AdmissionReason = "input_limit_exceeded"
	ContextLimitExceeded          AdmissionReason = "context_limit_exceeded"
	ArtifactLimitExceeded         AdmissionReason = "artifact_limit_exceeded"
	CapacityBusy                  AdmissionReason = "capacity_busy"
	WorkspaceQueueLimit           AdmissionReason = "workspace_queue_limit"
	WorkspaceActiveRunLimit       AdmissionReason = "workspace_active_run_limit"
)

func (v AdmissionReason) Valid() bool {
	switch v {
	case ReasonAdmitted, SubscriptionAttentionRequired, CapacityDraining, QuotaResetPending, QuotaExhaustedResetUnknown, RuntimeLimitExceeded, TurnLimitExceeded, InputLimitExceeded, ContextLimitExceeded, ArtifactLimitExceeded, CapacityBusy, WorkspaceQueueLimit, WorkspaceActiveRunLimit:
		return true
	}
	return false
}
func (v AdmissionReason) MarshalJSON() ([]byte, error) { return closedJSON(string(v), v.Valid()) }

type TerminalReason string

const (
	ExecutionFailed         TerminalReason = "execution_failed"
	ResultPersistenceFailed TerminalReason = "result_persistence_failed"
	CanonicalCancelled      TerminalReason = "canonical_cancelled"
	UnclassifiedFailure     TerminalReason = "unclassified_failure"
)

func (v TerminalReason) Valid() bool {
	return v == ExecutionFailed || v == ResultPersistenceFailed || v == CanonicalCancelled || v == UnclassifiedFailure
}
func (v TerminalReason) MarshalJSON() ([]byte, error) { return closedJSON(string(v), v.Valid()) }

type AttachedFact string

const (
	OfferRecorded             AttachedFact = "offer_recorded"
	ClaimRecorded             AttachedFact = "claim_recorded"
	TerminalCandidateRecorded AttachedFact = "terminal_candidate_recorded"
	TerminalCommitRecorded    AttachedFact = "terminal_commit_recorded"
	FencedOutcomeUnknown      AttachedFact = "fenced_outcome_unknown"
	ReceiptRetired            AttachedFact = "receipt_retired"
)

func (v AttachedFact) Valid() bool {
	return v == OfferRecorded || v == ClaimRecorded || v == TerminalCandidateRecorded || v == TerminalCommitRecorded || v == FencedOutcomeUnknown || v == ReceiptRetired
}
func (v AttachedFact) MarshalJSON() ([]byte, error) { return closedJSON(string(v), v.Valid()) }

type Freshness string

const (
	WithinLease Freshness = "within_lease"
	Expired     Freshness = "expired"
	Durable     Freshness = "durable"
)

func (v Freshness) Valid() bool                  { return v == WithinLease || v == Expired || v == Durable }
func (v Freshness) MarshalJSON() ([]byte, error) { return closedJSON(string(v), v.Valid()) }

type AdmissionCoverage string

const LastRecordedDecision AdmissionCoverage = "last_recorded_decision"

func (v AdmissionCoverage) MarshalJSON() ([]byte, error) {
	return closedJSON(string(v), v == LastRecordedDecision)
}

func closedJSON(value string, valid bool) ([]byte, error) {
	if !valid {
		return nil, ErrInvalidResponse
	}
	return json.Marshal(value)
}

type Admission struct {
	Availability     Availability      `json:"availability"`
	Outcome          AdmissionOutcome  `json:"outcome,omitempty"`
	ReasonCode       AdmissionReason   `json:"reason_code,omitempty"`
	ObservedAt       *time.Time        `json:"observed_at,omitempty"`
	DecisionRevision uint64            `json:"decision_revision,omitempty"`
	Coverage         AdmissionCoverage `json:"coverage,omitempty"`
}
type Terminal struct {
	Availability  Availability          `json:"availability"`
	ReasonCode    TerminalReason        `json:"reason_code,omitempty"`
	ObservedAt    *time.Time            `json:"observed_at,omitempty"`
	EventID       domain.SessionEventID `json:"event_id,omitempty"`
	EventSequence uint64                `json:"event_sequence,omitempty"`
}
type SelectedAttempt struct {
	Availability Availability         `json:"availability"`
	AttemptID    domain.AttemptID     `json:"attempt_id,omitempty"`
	Number       uint32               `json:"number,omitempty"`
	Status       domain.AttemptStatus `json:"status,omitempty"`
	UpdatedAt    *time.Time           `json:"updated_at,omitempty"`
	FinishedAt   *time.Time           `json:"finished_at,omitempty"`
}
type AttachedObservation struct {
	Availability Availability `json:"availability"`
	Fact         AttachedFact `json:"fact,omitempty"`
	ObservedAt   *time.Time   `json:"observed_at,omitempty"`
	ValidUntil   *time.Time   `json:"valid_until,omitempty"`
	Freshness    Freshness    `json:"freshness,omitempty"`
}
type Coverage struct {
	Admission   Availability `json:"admission"`
	Terminal    Availability `json:"terminal"`
	Attempt     Availability `json:"attempt"`
	Operational Availability `json:"operational"`
}

// PlacementSelector is private join material, never a public DTO. It contains
// only the admitted placement keys, not a WorkerJob, credentials or payloads.
type PlacementSelector struct {
	Kind        domain.ExecutionPlacementKind
	OwnerUserID domain.UserID
	WorkerID    domain.AttachedWorkerID
}

// Basis binds each observation to its owning canonical source snapshot.
type Basis struct {
	Version      uint32
	RunID        domain.RunID
	AttemptID    domain.AttemptID
	RunStatus    domain.RunStatus
	RunUpdatedAt time.Time // exact canonical admission phase basis, not a lease
	ObservedAt   time.Time
}
type AdmissionRecord struct {
	Basis       Basis
	Outcome     AdmissionOutcome `json:",omitempty"`
	Reason      AdmissionReason  `json:",omitempty"` // empty means unsupported, not a raw-code fallback
	Revision    uint64
	Fingerprint string // private fixed-size replay digest, never a public fence
}
type TerminalRecord struct {
	Basis         Basis
	Reason        TerminalReason `json:",omitempty"` // empty means invalid/unsupported evidence
	EventID       domain.SessionEventID
	EventSequence uint64
	Fingerprint   string
}

// HeadV1 is a bounded derived read locator. Canonical writers own its atomic
// persistence in #187; this package implements no persistence or discovery.
type HeadV1 struct {
	Version           uint32
	TenantID          domain.TenantID
	RunID             domain.RunID
	SessionID         domain.SessionID
	SelectedAttemptID domain.AttemptID
	SelectedAt        time.Time
	Placement         PlacementSelector
	Revision          uint64
	Admission         *AdmissionRecord
	Terminal          *TerminalRecord
}

type Diagnostic string

const InvalidTerminalNotice Diagnostic = "invalid_terminal_notice"

// NoticeInput is supplied by the owning finalization, never parsed from a
// loaded transcript's EventContent.Data or guessed from event ID recipes.
type NoticeInput struct {
	Schema    string
	Code      string
	Cancelled bool
}

// Snapshot is already authorized and transaction-consistent by caller contract.
// AttachedWorker and Connection are used only for their bounded identity fields;
// adapters must not load complete secrets/protocol payloads on this read path.
type Snapshot struct {
	Run        domain.Run
	ReadAt     time.Time
	Head       *HeadV1
	Attempt    *domain.Attempt
	Lease      *domain.Lease
	Attached   *domain.AttachedWorkerAttemptV1
	Worker     *domain.AttachedWorker
	Connection *domain.AttachedWorkerConnection
}

type Projection struct {
	Admission Admission
	Terminal  Terminal
	Attempt   SelectedAttempt
	Attached  AttachedObservation
	Coverage  Coverage
}
