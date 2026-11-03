package canonicaloutput

import (
	"bytes"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

// MCPUnknownToolOutcomeV1 is exactly five fields of historical DATA. It is not
// a service response, completed result, dispatch grant or original producer.
type MCPUnknownToolOutcomeV1 struct {
	Schema      string                                     `json:"schema"`
	Outcome     string                                     `json:"outcome"`
	Operation   domain.AttemptOperationDigestV1            `json:"operation_digest"`
	Reservation domain.AttemptOperationReservationDigestV1 `json:"reservation_digest"`
	Observation domain.AttemptOperationObservationDigestV1 `json:"observation_digest"`
}

func (v MCPUnknownToolOutcomeV1) valid() bool {
	return v.Schema == "sessionless.tool-outcome.v1" && v.Outcome == "accepted_outcome_unknown" && v.Operation.Validate() == nil && v.Reservation.Validate() == nil && v.Observation.Validate() == nil
}

func EncodeMCPUnknownToolOutcomeV1(operation domain.AttemptOperationDigestV1, reservation domain.AttemptOperationReservationDigestV1, observation domain.AttemptOperationObservationDigestV1) ([]byte, error) {
	v := MCPUnknownToolOutcomeV1{Schema: "sessionless.tool-outcome.v1", Outcome: "accepted_outcome_unknown", Operation: operation, Reservation: reservation, Observation: observation}
	if !v.valid() {
		return nil, domain.ErrMembershipDenied
	}
	return json.Marshal(v)
}

func DecodeMCPUnknownToolOutcomeV1(body []byte) (MCPUnknownToolOutcomeV1, error) {
	var v MCPUnknownToolOutcomeV1
	if _, err := strictjson.Parse(body, 1024, 4, 8); err != nil {
		return v, domain.ErrMembershipDenied
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&v) != nil || !v.valid() {
		return MCPUnknownToolOutcomeV1{}, domain.ErrMembershipDenied
	}
	raw, err := json.Marshal(v)
	if err != nil || !bytes.Equal(raw, body) {
		return MCPUnknownToolOutcomeV1{}, domain.ErrMembershipDenied
	}
	return v, nil
}
