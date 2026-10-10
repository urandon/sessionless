package runexplanation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func MapAdmission(code string) (AdmissionOutcome, AdmissionReason) {
	switch code {
	case "admitted", "already_admitted":
		return Admitted, ReasonAdmitted
	case "subscription_reauthentication_required":
		return Denied, SubscriptionAttentionRequired
	case "subscription_draining":
		return Denied, CapacityDraining
	case "provider_quota_blocked":
		return Denied, QuotaResetPending
	case "provider_quota_exhausted_reset_unknown":
		return Denied, QuotaExhaustedResetUnknown
	case "runtime_limit_exceeded":
		return Denied, RuntimeLimitExceeded
	case "turn_limit_exceeded":
		return Denied, TurnLimitExceeded
	case "input_limit_exceeded":
		return Denied, InputLimitExceeded
	case "context_limit_exceeded":
		return Denied, ContextLimitExceeded
	case "artifact_limit_exceeded":
		return Denied, ArtifactLimitExceeded
	case "subscription_slot_busy":
		return Denied, CapacityBusy
	case "tenant_queue_depth_exhausted":
		return Denied, WorkspaceQueueLimit
	case "tenant_active_run_limit":
		return Denied, WorkspaceActiveRunLimit
	default:
		return "", ""
	}
}

// MapTerminal proves only a recorded failure phase, not a diagnosed root cause.
func MapTerminal(status domain.RunStatus, notice NoticeInput) (TerminalReason, Diagnostic) {
	if notice.Schema != TerminalNoticeSchemaV1 || domain.ValidateOpaqueID("code", notice.Code) != nil ||
		(status != domain.RunFailed && status != domain.RunCancelled) || notice.Cancelled != (status == domain.RunCancelled) {
		return "", InvalidTerminalNotice
	}
	if status == domain.RunCancelled {
		if notice.Code == "cancelled" || notice.Code == "cancelled_before_start" {
			return CanonicalCancelled, ""
		}
		return "", InvalidTerminalNotice
	}
	if notice.Code == "cancelled" || notice.Code == "cancelled_before_start" {
		return "", InvalidTerminalNotice
	}
	switch notice.Code {
	case "harness_failed":
		return ExecutionFailed, ""
	case "artifact_upload_failed", "canonical_result_upload_failed":
		return ResultPersistenceFailed, ""
	default:
		return UnclassifiedFailure, ""
	}
}

func selector(placement domain.ExecutionPlacementV2) (PlacementSelector, error) {
	if placement.Validate() != nil {
		return PlacementSelector{}, ErrInvalidSource
	}
	return PlacementSelector{Kind: placement.Kind, OwnerUserID: placement.OwnerUserID, WorkerID: placement.WorkerID}, nil
}

func (s PlacementSelector) valid() bool {
	switch s.Kind {
	case domain.ExecutionPlacementManaged:
		return s.OwnerUserID == "" && s.WorkerID == ""
	case domain.ExecutionPlacementAttachedWorker:
		return s.OwnerUserID.Validate() == nil && s.WorkerID.Validate() == nil
	default:
		return false
	}
}

func validRun(run domain.Run, at time.Time) bool {
	return run.Validate() == nil && !at.IsZero() && !run.UpdatedAt.After(at) &&
		(run.FinishedAt == nil || (!run.FinishedAt.Before(run.CreatedAt) && !run.FinishedAt.After(run.UpdatedAt))) &&
		(run.StartedAt == nil || !run.StartedAt.After(run.UpdatedAt)) &&
		(run.CancellationRequestedAt == nil || !run.CancellationRequestedAt.After(run.UpdatedAt))
}

func validAttempt(run domain.Run, attempt domain.Attempt, at time.Time) bool {
	return attempt.ValidateForRun(run) == nil && !attempt.CreatedAt.Before(run.CreatedAt) && !attempt.UpdatedAt.After(at) &&
		(attempt.FinishedAt == nil || (!attempt.FinishedAt.Before(attempt.CreatedAt) && !attempt.FinishedAt.After(attempt.UpdatedAt)))
}

// Validate checks bounded encoding and structural identity only. Source basis
// mismatches degrade individual groups to unknown in Project, not a target leak.
func (head HeadV1) Validate() error {
	if head.Version != VersionV1 || head.Revision == 0 || head.TenantID.Validate() != nil || head.RunID.Validate() != nil ||
		head.SessionID.Validate() != nil || head.SelectedAttemptID.Validate() != nil || head.SelectedAt.IsZero() || !head.Placement.valid() {
		return ErrCorruptProjection
	}
	// Check variable-sized internal columns before encoding so corrupt records
	// cannot turn the byte ceiling itself into an unbounded allocation.
	boundedBasis := func(b Basis) bool { return len(b.RunID) <= 160 && len(b.AttemptID) <= 160 && len(b.RunStatus) <= 32 }
	if head.Admission != nil && (!boundedBasis(head.Admission.Basis) || len(head.Admission.Fingerprint) > 64) {
		return ErrCorruptProjection
	}
	if head.Terminal != nil && (!boundedBasis(head.Terminal.Basis) || len(head.Terminal.Fingerprint) > 64 || len(head.Terminal.EventID) > 160) {
		return ErrCorruptProjection
	}
	encoded, err := json.Marshal(head)
	if err != nil || len(encoded) > MaxProjectionBytes {
		return ErrCorruptProjection
	}
	return nil
}

func basis(run domain.Run, attempt domain.Attempt, at time.Time) Basis {
	return Basis{Version: VersionV1, RunID: run.ID, AttemptID: attempt.ID, RunStatus: run.Status, RunUpdatedAt: run.UpdatedAt.UTC(), ObservedAt: at.UTC()}
}

func (b Basis) valid(head HeadV1, run domain.Run, at time.Time) bool {
	return b.Version == VersionV1 && b.RunID == run.ID && b.AttemptID == head.SelectedAttemptID && b.RunStatus.Valid() &&
		!b.RunUpdatedAt.Before(run.CreatedAt) && !b.RunUpdatedAt.After(b.ObservedAt) && !b.RunUpdatedAt.After(run.UpdatedAt) && !b.ObservedAt.Before(head.SelectedAt) && !b.ObservedAt.After(at)
}

func digest(value any) string {
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == hex.EncodeToString(decoded)
}

func clone(head HeadV1) HeadV1 {
	if head.Admission != nil {
		value := *head.Admission
		head.Admission = &value
	}
	if head.Terminal != nil {
		value := *head.Terminal
		head.Terminal = &value
	}
	return head
}
func advance(head HeadV1) (HeadV1, error) {
	if head.Revision == math.MaxUint64 {
		return HeadV1{}, ErrRevisionExhausted
	}
	head = clone(head)
	head.Revision++
	return head, nil
}

// Select is called only by canonical ingress/admission of the exact Attempt.
// Replacing the selector resets incompatible evidence; it creates no Attempt.
func Select(current *HeadV1, run domain.Run, attempt domain.Attempt, placement domain.ExecutionPlacementV2, at time.Time) (HeadV1, bool, error) {
	selected, err := selector(placement)
	if err != nil || !validRun(run, at) || !validAttempt(run, attempt, at) || at.Before(run.CreatedAt) || at.Before(attempt.CreatedAt) {
		return HeadV1{}, false, ErrInvalidSource
	}
	if current == nil {
		return HeadV1{Version: VersionV1, TenantID: run.TenantID, RunID: run.ID, SessionID: run.SessionID, SelectedAttemptID: attempt.ID, SelectedAt: at.UTC(), Placement: selected, Revision: 1}, true, nil
	}
	if current.Validate() != nil {
		return HeadV1{}, false, ErrCorruptProjection
	}
	if current.TenantID != run.TenantID || current.RunID != run.ID || current.SessionID != run.SessionID {
		return HeadV1{}, false, ErrInvalidSource
	}
	if at.Before(current.SelectedAt) {
		return HeadV1{}, false, ErrStaleObservation
	}
	if (current.Admission != nil && at.Before(current.Admission.Basis.ObservedAt)) || (current.Terminal != nil && at.Before(current.Terminal.Basis.ObservedAt)) {
		return HeadV1{}, false, ErrStaleObservation
	}
	if current.SelectedAttemptID == attempt.ID {
		if current.Placement != selected {
			return HeadV1{}, false, ErrInvalidSource
		}
		return clone(*current), false, nil
	}
	if run.Status.Terminal() {
		return HeadV1{}, false, ErrInvalidSource
	}
	next, err := advance(*current)
	if err != nil {
		return HeadV1{}, false, err
	}
	next.SelectedAttemptID, next.SelectedAt, next.Placement = attempt.ID, at.UTC(), selected
	next.Admission, next.Terminal = nil, nil
	return next, true, nil
}

func writerBasis(head HeadV1, run domain.Run, attempt domain.Attempt, at time.Time) error {
	if head.Validate() != nil {
		return ErrCorruptProjection
	}
	if !validRun(run, at) || !validAttempt(run, attempt, at) || head.TenantID != run.TenantID || head.RunID != run.ID || head.SessionID != run.SessionID || head.SelectedAttemptID != attempt.ID {
		return ErrInvalidSource
	}
	if at.Before(head.SelectedAt) {
		return ErrStaleObservation
	}
	return nil
}

// RecordAdmission records source observations, not a policy evaluation.
// Equal state/retry time cannot suppress a changed canonical decision code.
// Unsupported codes supersede older reasons without persisting arbitrary text.
func RecordAdmission(head HeadV1, run domain.Run, attempt domain.Attempt, code string, at time.Time) (HeadV1, bool, error) {
	if err := writerBasis(head, run, attempt, at); err != nil {
		return HeadV1{}, false, err
	}
	if code == "dispatch_not_pending" {
		return clone(head), false, nil
	}
	// Unsupported codes still represent admission observations. No admission
	// decision can reopen a canonical terminal phase or erase its finalization.
	if run.Status.Terminal() {
		return HeadV1{}, false, ErrInvalidSource
	}
	if domain.ValidateOpaqueID("decision_code", code) != nil {
		return HeadV1{}, false, ErrInvalidSource
	}
	outcome, reason := MapAdmission(code)
	if (outcome == Admitted && (run.Status == domain.RunCreated || run.Status == domain.RunQuotaBlocked)) ||
		(outcome == Denied && run.Status != domain.RunQuotaBlocked) {
		return HeadV1{}, false, ErrInvalidSource
	}
	observation := AdmissionRecord{Basis: basis(run, attempt, at), Outcome: outcome, Reason: reason, Fingerprint: digest(struct {
		Basis Basis
		Code  string
	}{basis(run, attempt, at), code})}
	if head.Admission != nil {
		if !head.Admission.Basis.valid(head, run, at) || !validDigest(head.Admission.Fingerprint) || head.Admission.Revision == 0 || head.Admission.Revision > head.Revision {
			return HeadV1{}, false, ErrCorruptProjection
		}
		observation.Revision = head.Admission.Revision
		if reflect.DeepEqual(*head.Admission, observation) {
			return clone(head), false, nil
		}
		if at.Before(head.Admission.Basis.ObservedAt) {
			return HeadV1{}, false, ErrStaleObservation
		}
	}
	next, err := advance(head)
	if err != nil {
		return HeadV1{}, false, err
	}
	observation.Revision = next.Revision
	next.Admission = &observation
	// A new canonical admission phase never inherits an earlier finalization.
	next.Terminal = nil
	return next, true, nil
}

// RecordTerminal must receive event metadata allocated by canonical finalization
// in the same transaction. It does not discover notices from transcript pages.
func RecordTerminal(head HeadV1, run domain.Run, attempt domain.Attempt, notice *NoticeInput, eventID domain.SessionEventID, sequence uint64, at time.Time) (HeadV1, bool, Diagnostic, error) {
	if err := writerBasis(head, run, attempt, at); err != nil {
		return HeadV1{}, false, "", err
	}
	if !run.Status.Terminal() || run.FinishedAt == nil || !run.FinishedAt.Equal(at) {
		return HeadV1{}, false, "", ErrInvalidSource
	}
	if run.Status == domain.RunSucceeded {
		if notice != nil || eventID != "" || sequence != 0 {
			return HeadV1{}, false, "", ErrInvalidSource
		}
		if head.Terminal == nil {
			return clone(head), false, "", nil
		}
		next, err := advance(head)
		if err != nil {
			return HeadV1{}, false, "", err
		}
		next.Terminal = nil
		return next, true, "", nil
	}
	if notice == nil || len(notice.Schema) > 160 || len(notice.Code) > 160 || eventID.Validate() != nil || sequence == 0 {
		return HeadV1{}, false, "", ErrInvalidSource
	}
	reason, diagnostic := MapTerminal(run.Status, *notice)
	observation := TerminalRecord{Basis: basis(run, attempt, at), Reason: reason, EventID: eventID, EventSequence: sequence, Fingerprint: digest(struct {
		Notice   NoticeInput
		EventID  domain.SessionEventID
		Sequence uint64
		Basis    Basis
	}{*notice, eventID, sequence, basis(run, attempt, at)})}
	if head.Terminal != nil {
		if reflect.DeepEqual(*head.Terminal, observation) {
			return clone(head), false, diagnostic, nil
		}
		// Finalization is immutable: conflicting replay is not a newer cause.
		return HeadV1{}, false, "", ErrInvalidSource
	}
	next, err := advance(head)
	if err != nil {
		return HeadV1{}, false, "", err
	}
	next.Terminal = &observation
	return next, true, diagnostic, nil
}

// Invalidate clears evidence only; the caller supplies the owning lifecycle
// operation and persists the result atomically. This is not a state transition.
func Invalidate(head HeadV1, admission, terminal bool) (HeadV1, bool, error) {
	if head.Validate() != nil {
		return HeadV1{}, false, ErrCorruptProjection
	}
	if (!admission || head.Admission == nil) && (!terminal || head.Terminal == nil) {
		return clone(head), false, nil
	}
	next, err := advance(head)
	if err != nil {
		return HeadV1{}, false, err
	}
	if admission {
		next.Admission = nil
	}
	if terminal {
		next.Terminal = nil
	}
	return next, true, nil
}
