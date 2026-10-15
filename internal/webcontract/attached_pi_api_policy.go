package webcontract

import "gitcode.com/urandon/sessionless/internal/domain"

const (
	RouteAttachedPiAPIPolicyRead    = "/api/web/v1/attached-pi-api-policy:read"
	RouteAttachedPiAPIPolicyConfirm = "/api/web/v1/attached-pi-api-policy:authorize"
	RouteAttachedPiAPIPolicyRevoke  = "/api/web/v1/attached-pi-api-policy:revoke"
)

// Only selectors/exact confirmation and CAS/idempotency data cross the browser
// boundary. Tenant, actor, receipt, issuer, decision ID, time, membership and
// native/provider evidence are always selected by authenticated server code.
type AttachedPiAPIPolicySelectorV1 struct {
	Version        uint32                     `json:"version"`
	ResourceID     string                     `json:"resource_id"`
	ModelVendorID  string                     `json:"model_vendor_id"`
	ModelID        string                     `json:"model_id"`
	InputDataClass domain.ProviderDataClassV1 `json:"input_data_class"`
}
type AttachedPiAPIPolicyConfirmationV1 struct {
	AttachedPiAPIPolicySelectorV1
	TermsDigest        domain.ProviderEvidenceDigestV1 `json:"terms_digest"`
	ExpectedGeneration uint64                          `json:"expected_generation"`
	MutationID         domain.IdempotencyKey           `json:"mutation_id"`
}
type AttachedPiAPIPolicyRevocationV1 struct {
	AttachedPiAPIPolicySelectorV1
	ExpectedGeneration uint64                                              `json:"expected_generation"`
	ExpectedMapping    domain.AttachedPiAPIInvocationPolicyMappingDigestV1 `json:"expected_mapping"`
}
