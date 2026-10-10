package runexplanation

import (
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func utcPointer(at time.Time) *time.Time { value := at.UTC(); return &value }

// Project consumes a single caller-authorized snapshot without I/O or mutation.
// Missing historical fields stay omitted; no event adjacency, fallback Attempt,
// reconstructed IDs, current entitlement or managed process inference is used.
func Project(snapshot Snapshot) (Projection, error) {
	run, at := snapshot.Run, snapshot.ReadAt
	if !validRun(run, at) {
		return Projection{}, ErrInvalidSource
	}
	result := Projection{Admission: Admission{Availability: Unknown}, Terminal: Terminal{Availability: Unknown}, Attempt: SelectedAttempt{Availability: Unknown}, Attached: AttachedObservation{Availability: Unknown}}
	if !run.Status.Terminal() {
		result.Terminal.Availability = NotApplicable
	}
	finish := func() Projection {
		result.Coverage = Coverage{Admission: result.Admission.Availability, Terminal: result.Terminal.Availability, Attempt: result.Attempt.Availability, Operational: result.Attached.Availability}
		return result
	}
	if snapshot.Head == nil {
		return finish(), nil
	}
	head := *snapshot.Head
	if err := head.Validate(); err != nil {
		return Projection{}, err
	}
	if head.TenantID != run.TenantID || head.RunID != run.ID || head.SessionID != run.SessionID || head.SelectedAt.Before(run.CreatedAt) || head.SelectedAt.After(at) {
		return finish(), nil
	}
	if snapshot.Attempt == nil {
		return finish(), nil
	}
	attempt := *snapshot.Attempt
	if attempt.ID != head.SelectedAttemptID || !validAttempt(run, attempt, at) || head.SelectedAt.Before(attempt.CreatedAt) {
		return finish(), nil
	}
	result.Attempt = SelectedAttempt{Availability: Recorded, AttemptID: attempt.ID, Number: attempt.Number, Status: attempt.Status, UpdatedAt: utcPointer(attempt.UpdatedAt)}
	if attempt.FinishedAt != nil {
		result.Attempt.FinishedAt = utcPointer(*attempt.FinishedAt)
	}
	if record := head.Admission; record != nil && record.Basis.valid(head, run, at) && record.Revision > 0 && record.Revision <= head.Revision && validDigest(record.Fingerprint) && record.Outcome.Valid() && record.Reason.Valid() {
		consistent := (record.Outcome == Admitted && record.Reason == ReasonAdmitted && admittedBasis(record.Basis.RunStatus) && run.Status != domain.RunCreated && run.Status != domain.RunQuotaBlocked) ||
			(record.Outcome == Denied && record.Reason != ReasonAdmitted && record.Basis.RunStatus == domain.RunQuotaBlocked && run.Status == domain.RunQuotaBlocked && record.Basis.RunUpdatedAt.Equal(run.UpdatedAt))
		if consistent {
			result.Admission = Admission{Availability: Recorded, Outcome: record.Outcome, ReasonCode: record.Reason, ObservedAt: utcPointer(record.Basis.ObservedAt), DecisionRevision: record.Revision, Coverage: LastRecordedDecision}
		}
	}
	if record := head.Terminal; record != nil && (run.Status == domain.RunFailed || run.Status == domain.RunCancelled) && record.Basis.valid(head, run, at) && record.Basis.RunStatus == run.Status && record.Basis.RunUpdatedAt.Equal(run.UpdatedAt) &&
		run.FinishedAt != nil && run.FinishedAt.Equal(record.Basis.ObservedAt) && record.Reason.Valid() && record.EventID.Validate() == nil && record.EventSequence > 0 && validDigest(record.Fingerprint) &&
		((run.Status == domain.RunCancelled && record.Reason == CanonicalCancelled) || (run.Status == domain.RunFailed && record.Reason != CanonicalCancelled)) {
		result.Terminal = Terminal{Availability: Recorded, ReasonCode: record.Reason, ObservedAt: utcPointer(record.Basis.ObservedAt), EventID: record.EventID, EventSequence: record.EventSequence}
	}
	result.Attached = projectAttached(snapshot, head, attempt)
	return finish(), nil
}

func admittedBasis(status domain.RunStatus) bool {
	return status == domain.RunAdmitted || status == domain.RunQueued || status == domain.RunRunning
}

func projectAttached(snapshot Snapshot, head HeadV1, attempt domain.Attempt) AttachedObservation {
	unknown := AttachedObservation{Availability: Unknown}
	if head.Placement.Kind != domain.ExecutionPlacementAttachedWorker || snapshot.Attached == nil || snapshot.Lease == nil || snapshot.Worker == nil || snapshot.Connection == nil {
		return unknown
	}
	run, at, receipt, lease, worker, connection := snapshot.Run, snapshot.ReadAt, *snapshot.Attached, *snapshot.Lease, *snapshot.Worker, *snapshot.Connection
	if receipt.Validate() != nil || lease.ValidateForAttempt(run, attempt) != nil || receipt.CreatedAt.Before(attempt.CreatedAt) || receipt.UpdatedAt.After(at) || lease.AcquiredAt.After(receipt.UpdatedAt) ||
		lease.WorkerID != string(head.Placement.WorkerID) || (attempt.WorkerID != "" && attempt.WorkerID != lease.WorkerID) ||
		receipt.TenantID != run.TenantID || receipt.OwnerUserID != head.Placement.OwnerUserID || receipt.WorkerID != head.Placement.WorkerID ||
		receipt.RunID != run.ID || receipt.AttemptID != attempt.ID || receipt.LeaseID != lease.ID || receipt.LeaseGeneration != lease.FenceToken || !receipt.LeaseExpiresAt.Equal(lease.ExpiresAt) {
		return unknown
	}
	// The current worker/connection binding is checked independently from the
	// receipt. Never disclose the replacement Run/Attempt/worker/connection.
	if worker.ID.Validate() != nil || worker.OwnerUserID.Validate() != nil || worker.TenantID != run.TenantID || worker.ID != receipt.WorkerID || worker.OwnerUserID != receipt.OwnerUserID ||
		worker.EnrollmentGeneration == 0 || worker.EnrollmentGeneration != receipt.EnrollmentGeneration || worker.ConnectionGeneration == 0 || worker.ConnectionGeneration != receipt.ConnectionGeneration ||
		worker.Revision == 0 || worker.CreatedAt.IsZero() || worker.UpdatedAt.Before(worker.CreatedAt) || worker.UpdatedAt.After(at) || !worker.DesiredState.Valid() || !worker.ObservedState.Valid() ||
		connection.ID.Validate() != nil || connection.TenantID != run.TenantID || connection.OwnerUserID != receipt.OwnerUserID || connection.WorkerID != receipt.WorkerID || connection.ID != receipt.ConnectionID ||
		connection.EnrollmentGeneration != receipt.EnrollmentGeneration || connection.ConnectionGeneration != receipt.ConnectionGeneration || connection.Revision == 0 || !connection.State.Valid() ||
		connection.ConnectedAt.IsZero() || connection.ConnectedAt.After(at) || connection.LastCheckpointAt.After(at) || connection.ManifestObservedAt.After(at) {
		return unknown
	}
	var fact AttachedFact
	var durable bool
	switch receipt.State {
	case domain.AttachedWorkerAttemptOffered:
		fact = OfferRecorded
	case domain.AttachedWorkerAttemptClaimed:
		fact = ClaimRecorded
	case domain.AttachedWorkerAttemptTerminalPending:
		fact = TerminalCandidateRecorded
	case domain.AttachedWorkerAttemptTerminalCommitted:
		if string(receipt.TerminalStatus) != string(run.Status) || !run.Status.Terminal() || string(attempt.Status) != string(run.Status) {
			return unknown
		}
		fact, durable = TerminalCommitRecorded, true
	case domain.AttachedWorkerAttemptFencedUnknown:
		fact, durable = FencedOutcomeUnknown, true
	case domain.AttachedWorkerAttemptRetired:
		fact, durable = ReceiptRetired, true
	default:
		return unknown
	}
	observation := AttachedObservation{Availability: Recorded, Fact: fact, ObservedAt: utcPointer(receipt.UpdatedAt), Freshness: Durable}
	if !durable {
		observation.ValidUntil = utcPointer(lease.ExpiresAt)
		observation.Freshness = WithinLease
		if !at.Before(lease.ExpiresAt) {
			observation.Freshness = Expired
		}
	}
	return observation
}
