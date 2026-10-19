package webcontract

import (
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const RouteSearchConsent = "/api/web/v1/sessions/{session_id}/runs/{run_id}/attempts/{attempt_id}/search-consents/{request_id}"

// SearchConsentSummaryV1 is private browser state, never an execution receipt
// or a grant. In particular no blob, query digest, claim or payer is exposed.
type SearchConsentSummaryV1 struct {
	RequestID domain.WebSearchConsentRequestIDV1 `json:"request_id"`
	Revision  uint64                             `json:"revision"`
	Status    domain.WebSearchConsentStatusV1    `json:"status"`
	ExpiresAt time.Time                          `json:"expires_at"`
}

type SearchConsentViewV1 struct {
	SearchConsentSummaryV1
	Query string `json:"query"`
}

// Locator comes from the route; all identity/tenant/CSRF authority is resolved
// server-side. A decision cannot replace arguments or extend execution time.
type DecideSearchConsentV1 struct {
	ExpectedRevision uint64                          `json:"expected_revision"`
	Choice           domain.WebSearchConsentChoiceV1 `json:"choice"`
}

func (v DecideSearchConsentV1) Validate() error {
	if v.ExpectedRevision != 1 {
		return domain.ValidationError{Field: "search_consent.expected_revision", Reason: "must equal 1"}
	}
	return v.Choice.Validate()
}
