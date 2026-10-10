package webcontract

import (
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
)

// RunExplanationV1 is the fixed participant-safe manifest from evidence design
// 0.1.2. No private placement key, generation, owner, provider, raw notice, audit,
// host detail or retry/compute authority belongs in this response.
type RunExplanationV1 struct {
	Version    uint32                             `json:"version"`
	RunID      domain.RunID                       `json:"run_id"`
	SessionID  domain.SessionID                   `json:"session_id"`
	ReadAt     time.Time                          `json:"read_at"`
	Status     domain.RunStatus                   `json:"status"`
	CreatedAt  time.Time                          `json:"created_at"`
	UpdatedAt  time.Time                          `json:"updated_at"`
	FinishedAt *time.Time                         `json:"finished_at,omitempty"`
	Admission  runexplanation.Admission           `json:"admission"`
	Terminal   runexplanation.Terminal            `json:"terminal"`
	Attempt    runexplanation.SelectedAttempt     `json:"attempt"`
	Attached   runexplanation.AttachedObservation `json:"attached"`
	Coverage   runexplanation.Coverage            `json:"coverage"`
}

// NewRunExplanationV1 is a pure projection, not an authorization entry point.
// The adapter must reauthorize and join Snapshot in one resource transaction.
func NewRunExplanationV1(snapshot runexplanation.Snapshot) (RunExplanationV1, error) {
	projection, err := runexplanation.Project(snapshot)
	if err != nil {
		return RunExplanationV1{}, err
	}
	run := snapshot.Run
	result := RunExplanationV1{Version: runexplanation.VersionV1, RunID: run.ID, SessionID: run.SessionID, ReadAt: snapshot.ReadAt.UTC(), Status: run.Status, CreatedAt: run.CreatedAt.UTC(), UpdatedAt: run.UpdatedAt.UTC(), Admission: projection.Admission, Terminal: projection.Terminal, Attempt: projection.Attempt, Attached: projection.Attached, Coverage: projection.Coverage}
	if run.FinishedAt != nil {
		value := run.FinishedAt.UTC()
		result.FinishedAt = &value
	}
	if err := result.Validate(); err != nil {
		return RunExplanationV1{}, err
	}
	return result, nil
}

func utcInstant(at time.Time) bool { _, offset := at.Zone(); return !at.IsZero() && offset == 0 }
func optionalInstant(at *time.Time, readAt time.Time) bool {
	return at == nil || (utcInstant(*at) && !at.After(readAt))
}

func (value RunExplanationV1) Validate() error {
	bad := runexplanation.ErrInvalidResponse
	if value.Version != runexplanation.VersionV1 || value.RunID.Validate() != nil || value.SessionID.Validate() != nil || !value.Status.Valid() ||
		!utcInstant(value.ReadAt) || !utcInstant(value.CreatedAt) || !utcInstant(value.UpdatedAt) || value.UpdatedAt.Before(value.CreatedAt) || value.UpdatedAt.After(value.ReadAt) ||
		value.Status.Terminal() != (value.FinishedAt != nil) || !optionalInstant(value.FinishedAt, value.UpdatedAt) || (value.FinishedAt != nil && value.FinishedAt.Before(value.CreatedAt)) {
		return bad
	}
	a := value.Admission
	switch a.Availability {
	case runexplanation.Unknown:
		if a.Outcome != "" || a.ReasonCode != "" || a.ObservedAt != nil || a.DecisionRevision != 0 || a.Coverage != "" {
			return bad
		}
	case runexplanation.Recorded:
		if !a.Outcome.Valid() || !a.ReasonCode.Valid() || a.ObservedAt == nil || !optionalInstant(a.ObservedAt, value.ReadAt) || a.ObservedAt.Before(value.CreatedAt) || a.DecisionRevision == 0 || a.Coverage != runexplanation.LastRecordedDecision ||
			(a.Outcome == runexplanation.Admitted) != (a.ReasonCode == runexplanation.ReasonAdmitted) ||
			(a.Outcome == runexplanation.Denied && value.Status != domain.RunQuotaBlocked) || (a.Outcome == runexplanation.Admitted && (value.Status == domain.RunCreated || value.Status == domain.RunQuotaBlocked)) {
			return bad
		}
	default:
		return bad
	}
	t := value.Terminal
	switch t.Availability {
	case runexplanation.Unknown, runexplanation.NotApplicable:
		if t.ReasonCode != "" || t.ObservedAt != nil || t.EventID != "" || t.EventSequence != 0 {
			return bad
		}
		if (t.Availability == runexplanation.NotApplicable) != !value.Status.Terminal() {
			return bad
		}
	case runexplanation.Recorded:
		if !t.ReasonCode.Valid() || t.ObservedAt == nil || !optionalInstant(t.ObservedAt, value.ReadAt) || value.FinishedAt == nil || !t.ObservedAt.Equal(*value.FinishedAt) || t.EventID.Validate() != nil || t.EventSequence == 0 ||
			(value.Status != domain.RunFailed && value.Status != domain.RunCancelled) || (value.Status == domain.RunCancelled) != (t.ReasonCode == runexplanation.CanonicalCancelled) {
			return bad
		}
	default:
		return bad
	}
	p := value.Attempt
	switch p.Availability {
	case runexplanation.Unknown:
		if p.AttemptID != "" || p.Number != 0 || p.Status != "" || p.UpdatedAt != nil || p.FinishedAt != nil {
			return bad
		}
	case runexplanation.Recorded:
		if p.AttemptID.Validate() != nil || p.Number == 0 || !p.Status.Valid() || p.UpdatedAt == nil || !optionalInstant(p.UpdatedAt, value.ReadAt) || p.UpdatedAt.Before(value.CreatedAt) || p.Status.Terminal() != (p.FinishedAt != nil) ||
			!optionalInstant(p.FinishedAt, *p.UpdatedAt) || (p.FinishedAt != nil && p.FinishedAt.Before(value.CreatedAt)) {
			return bad
		}
	default:
		return bad
	}
	o := value.Attached
	switch o.Availability {
	case runexplanation.Unknown:
		if o.Fact != "" || o.ObservedAt != nil || o.ValidUntil != nil || o.Freshness != "" {
			return bad
		}
	case runexplanation.Recorded:
		if !o.Fact.Valid() || !o.Freshness.Valid() || o.ObservedAt == nil || !optionalInstant(o.ObservedAt, value.ReadAt) || o.ObservedAt.Before(value.CreatedAt) || p.Availability != runexplanation.Recorded {
			return bad
		}
		durable := o.Fact == runexplanation.TerminalCommitRecorded || o.Fact == runexplanation.FencedOutcomeUnknown || o.Fact == runexplanation.ReceiptRetired
		if durable {
			if o.Freshness != runexplanation.Durable || o.ValidUntil != nil {
				return bad
			}
		} else {
			if o.ValidUntil == nil || !utcInstant(*o.ValidUntil) || !o.ValidUntil.After(value.CreatedAt) || o.Freshness == runexplanation.Durable || (o.Freshness == runexplanation.WithinLease) != value.ReadAt.Before(*o.ValidUntil) {
				return bad
			}
		}
	default:
		return bad
	}
	if value.Coverage != (runexplanation.Coverage{Admission: a.Availability, Terminal: t.Availability, Attempt: p.Availability, Operational: o.Availability}) {
		return bad
	}
	if (a.Availability == runexplanation.Recorded || t.Availability == runexplanation.Recorded) && p.Availability != runexplanation.Recorded {
		return bad
	}
	return nil
}

func (value RunExplanationV1) MarshalJSON() ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	type wire RunExplanationV1
	encoded, err := json.Marshal(wire(value))
	if err != nil {
		return nil, runexplanation.ErrInvalidResponse
	}
	if len(encoded) > runexplanation.MaxResponseBytes {
		return nil, runexplanation.ErrInvalidResponse
	}
	return encoded, nil
}

// RunTerminalNoticeV1 is safe loaded-event copy only, never drawer authority.
// Generic events remain unlinked unless their canonical event has explicit RunID.
type RunTerminalNoticeV1 struct {
	Version    uint32                        `json:"version"`
	ReasonCode runexplanation.TerminalReason `json:"reason_code"`
}

func (value RunTerminalNoticeV1) MarshalJSON() ([]byte, error) {
	if value.Version != runexplanation.VersionV1 || !value.ReasonCode.Valid() {
		return nil, runexplanation.ErrInvalidResponse
	}
	type wire RunTerminalNoticeV1
	return json.Marshal(wire(value))
}
