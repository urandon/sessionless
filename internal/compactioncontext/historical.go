package compactioncontext

import (
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// HistoricalRunV1 is request-free retained DATA. Only the original Store may
// supply it to a serving Source after checking the successful terminal seal,
// capsule, admitted canonical anchor and complete retained closure. This value
// neither attests those checks nor restores the old Run's execution authority.
type HistoricalRunV1 struct {
	Scope      domain.AttemptOperationScopeV1
	Anchor     domain.SessionEventID
	Operations []HistoricalOperationV1
}

type HistoricalOperationV1 struct {
	Reservation domain.AttemptOperationReservationV1
	Observation domain.AttemptOperationObservationV1
	Normalized  []byte
	Projection  string
}

func historicalFrames(profile domain.ConfiguredInferenceProfileV1, run HistoricalRunV1, session domain.SessionID) ([]historicalFrameV1, []retainedSourceV1, error) {
	if run.Scope.Validate() != nil || run.Scope.TenantID != profile.TenantID || run.Scope.OwnerUserID != profile.OwnerUserID || run.Scope.SessionID != session || run.Anchor.Validate() != nil || len(run.Operations) == 0 || len(run.Operations) > 12 {
		return nil, nil, ErrCanonical
	}
	var frames []historicalFrameV1
	var sources []retainedSourceV1
	var previous *HistoricalOperationV1
	for i, item := range run.Operations {
		r, o := item.Reservation, item.Observation
		if r.Operation.Scope != run.Scope || r.Operation.Sequence != uint64(i+1) || o.Usage.ExceedsReservation(r.Operation.Debit) ||
			(i > 0 && r.Operation.ValidatePredecessor(previous.Reservation, previous.Observation) != nil) {
			return nil, nil, ErrCanonical
		}
		response, err := configuredinference.DecodeRetainedNormalizedDataV1(profile, r, o, item.Normalized)
		if err != nil || response.Call != nil || response.Stop != configuredinference.StopFinalV1 {
			return nil, nil, ErrCanonical
		}
		previous = &run.Operations[i]
		switch {
		case i == len(run.Operations)-1:
			if item.Projection != "assistant" || r.Operation.Kind != domain.AttemptOperationModelV1 {
				return nil, nil, ErrCanonical
			}
			// Its exact canonical projection is already present at run.Anchor.
		case r.Operation.Kind == domain.AttemptOperationCompactionV1:
			if item.Projection != "compaction" {
				return nil, nil, ErrCanonical
			}
		case r.Operation.Kind == domain.AttemptOperationModelV1:
			if item.Projection != "retained_only" {
				return nil, nil, ErrCanonical
			}
			key, err := OperationSourceKeyV1(r, o)
			if err != nil {
				return nil, nil, err
			}
			frames = append(frames, historicalFrameV1{Kind: "operation", SourceID: key, Role: "assistant_message", RunID: string(run.Scope.RunID), Sequence: r.Operation.Sequence, Payload: response.Text, Digest: o.ResponseDigest})
			sources = append(sources, retainedSourceV1{SourceID: key, Operation: &retainedOperationV1{Reservation: r.Clone(), Observation: o.Clone()}})
		default:
			return nil, nil, ErrCanonical
		}
	}
	return frames, sources, nil
}
