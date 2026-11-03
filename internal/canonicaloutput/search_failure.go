package canonicaloutput

import (
	"bytes"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

// SearchOperationStatusV4 is a redacted historical projection, NOT a vendor
// result or permission to replay an unresolved effect. No query or receipt is
// exposed. Original capsule/Store verification authenticates these digests.
type SearchOperationStatusV4 struct {
	Schema      string                                     `json:"schema"`
	Outcome     domain.AttemptOperationOutcomeV1           `json:"outcome"`
	Reason      domain.AttemptOperationReasonV1            `json:"reason"`
	Operation   domain.AttemptOperationDigestV1            `json:"operation_digest"`
	Reservation domain.AttemptOperationReservationDigestV1 `json:"reservation_digest"`
	Observation domain.AttemptOperationObservationDigestV1 `json:"observation_digest"`
}

func (v SearchOperationStatusV4) valid() bool {
	return v.Schema == "sessionless.search-operation-status.v4" && v.Operation.Validate() == nil && v.Reservation.Validate() == nil && v.Observation.Validate() == nil && (v.Outcome == domain.AttemptOperationRejectedBeforeSendV1 && ports.FailureSearchRejectedReasonV4(v.Reason) || v.Outcome == domain.AttemptOperationAcceptedOutcomeUnknownV1 && ports.FailureSearchUnknownReasonV4(v.Reason))
}

func EncodeSearchOperationStatusV4(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1) ([]byte, error) {
	if r.Operation.Kind != domain.AttemptOperationWebSearchV1 || o.ValidateForReservation(r) != nil {
		return nil, domain.ErrMembershipDenied
	}
	rd, _ := r.Digest()
	od, _ := o.Digest()
	v := SearchOperationStatusV4{"sessionless.search-operation-status.v4", o.Outcome, o.Reason, r.OperationDigest, rd, od}
	if !v.valid() {
		return nil, domain.ErrMembershipDenied
	}
	return json.Marshal(v)
}

func DecodeSearchOperationStatusV4(body []byte) (SearchOperationStatusV4, error) {
	var v SearchOperationStatusV4
	if _, err := strictjson.Parse(body, 2048, 8, 16); err != nil {
		return v, domain.ErrMembershipDenied
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&v) != nil || !v.valid() {
		return SearchOperationStatusV4{}, domain.ErrMembershipDenied
	}
	raw, _ := json.Marshal(v)
	if !bytes.Equal(raw, body) {
		return SearchOperationStatusV4{}, domain.ErrMembershipDenied
	}
	return v, nil
}

func DecodeSearchFailureNoticeV4(body []byte) (domain.SearchFailureNoticeV4, error) {
	return domain.DecodeSearchFailureNoticeV4(body)
}

// Empty prefix is intentional for a stopped first consent: no SEARCH row or
// execution pair is fabricated. Existing V1–V3 pair encoders are unchanged.
func EncodeSearchFailureV4(run domain.RunID, limits domain.ProductLimits, prefix []ports.ExecutionToolEvent, stop string, tail domain.AttemptOperationReservationV1, observation domain.AttemptOperationObservationV1) (*FailurePlanV1, error) {
	if run.Validate() != nil || len(prefix) > 8 || len(prefix)%2 != 0 || observation.ValidateForReservation(tail) != nil {
		return nil, domain.ErrMembershipDenied
	}
	rd, _ := tail.Digest()
	od, _ := observation.Digest()
	v := domain.SearchFailureNoticeV4{Schema: "sessionless.run-terminal-notice.v4", Code: stop, Kind: tail.Operation.Kind, Outcome: observation.Outcome, Reason: observation.Reason, Operation: tail.OperationDigest, Reservation: rd, Observation: od}
	switch stop {
	case domain.SearchConsentStoppedV4:
		v.Execution = "not_executed"
	case domain.SearchRejectedBeforeSendV4:
		v.Execution = "not_sent"
	case domain.SearchServiceErrorV4:
		v.Execution = "known_service_error"
	case domain.SearchOutcomeUnknownV4:
		v.Execution = "unknown"
	case domain.ModelAfterSearchFailureV4:
		v.Execution = "unknown"
		if observation.Outcome == domain.AttemptOperationRejectedBeforeSendV1 {
			v.Execution = "not_sent"
		}
	}
	if v.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	p := &FailurePlanV1{}
	if len(prefix) != 0 {
		old, err := encodeToolPrefixFailure(run, limits, prefix, stop)
		if err != nil {
			return nil, err
		}
		clear(old.events[len(old.events)-1].Payload)
		p.events = old.events[:len(old.events)-1]
	}
	body, err := json.Marshal(v)
	if err != nil {
		p.Release()
		return nil, err
	}
	p.events = append(p.events, EventV1{ID: domain.SessionEventID(stableID("evt", string(run), "terminal-notice")), Kind: domain.SessionEventSystemNotice, Payload: body})
	return p, nil
}
