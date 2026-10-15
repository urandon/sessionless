package ports

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// The locator deliberately excludes resource revision/generation and native P:
// replacement within this owner/model lane must replace its ONE current head,
// not leave an older native profile independently current.
type AttachedPiAPIPolicyLocatorV1 struct {
	TenantID       domain.TenantID            `json:"tenant_id"`
	OwnerUserID    domain.UserID              `json:"owner_user_id"`
	ResourceID     string                     `json:"resource_id"`
	ModelVendorID  string                     `json:"model_vendor_id"`
	ModelID        string                     `json:"model_id"`
	InputDataClass domain.ProviderDataClassV1 `json:"input_data_class"`
}

func (l AttachedPiAPIPolicyLocatorV1) Validate() error {
	if l.TenantID.Validate() != nil || l.OwnerUserID.Validate() != nil || domain.ValidateOpaqueID("pi_policy.resource", l.ResourceID) != nil ||
		len(l.ModelVendorID) == 0 || len(l.ModelVendorID) > 128 || len(l.ModelID) == 0 || len(l.ModelID) > 256 ||
		(l.InputDataClass != domain.ProviderDataPublicV1 && l.InputDataClass != domain.ProviderDataPrivateV1 && l.InputDataClass != domain.ProviderDataExternallyShareableV1) {
		return domain.ErrMembershipDenied
	}
	return nil
}
func (l AttachedPiAPIPolicyLocatorV1) Digest() (string, error) {
	if err := l.Validate(); err != nil {
		return "", err
	}
	b, err := json.Marshal(l)
	if err != nil {
		return "", domain.ErrMembershipDenied
	}
	h := sha256.New()
	_, _ = h.Write([]byte("sessionless.attached-pi-api-policy-locator.v1\x00"))
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil)), nil
}
func (l AttachedPiAPIPolicyLocatorV1) Matches(t domain.AttachedPiAPIInvocationPolicyV1) bool {
	return l.TenantID == t.Scope.TenantID && l.OwnerUserID == t.Scope.OwnerUserID && t.Scope.Resource.Kind == domain.ProviderResourceAPIAccountV1 &&
		l.ResourceID == t.Scope.Resource.ResourceID && l.ModelVendorID == t.ModelVendorID && l.ModelID == t.ModelID && l.InputDataClass == t.InputDataClass
}

// Selection is DATA returned by an installed, genuinely source-qualified
// server catalog. Shape/hash equality alone never meets that catalog contract.
type AttachedPiAPIPolicySelectionV1 struct {
	SourceID string                                 `json:"source_id"`
	Profile  domain.AttachedPiSDKNativeProfileV1    `json:"profile"`
	Evidence domain.ProviderEvidenceBundleV1        `json:"evidence"`
	Terms    domain.AttachedPiAPIInvocationPolicyV1 `json:"terms"`
}

// This trusted server dependency must select exact reviewed/installed native
// P, authoritative E and a conservative full-context/vision/cache/reasoning
// tariff proof. It must verify actual source provenance and current revocation,
// not accept client JSON/config booleans/self-generated HMAC as qualification.
// Select is bounded local work with no credential/provider/model/network I/O
// and is repeated against EVERY transaction time. No production implementation
// is fabricated here; nil/absent qualification MUST deny before recording R.
type AttachedPiAPIPolicyCatalogV1 interface {
	SelectAttachedPiAPIPolicy(AttachedPiAPIPolicyLocatorV1, time.Time) (AttachedPiAPIPolicySelectionV1, error)
}

type ReadAttachedPiAPIPolicyV1 struct {
	Locator          AttachedPiAPIPolicyLocatorV1
	WebSessionDigest domain.SecretDigest
}
type ConfirmAttachedPiAPIPolicyV1 struct {
	ReadAttachedPiAPIPolicyV1
	CSRFTokenDigest    domain.SecretDigest
	TermsDigest        domain.ProviderEvidenceDigestV1
	ExpectedGeneration uint64
	MutationID         domain.IdempotencyKey
}
type RevokeAttachedPiAPIPolicyV1 struct {
	ReadAttachedPiAPIPolicyV1
	CSRFTokenDigest    domain.SecretDigest
	ExpectedGeneration uint64
	ExpectedMapping    domain.AttachedPiAPIInvocationPolicyMappingDigestV1
}
type AttachedPiAPIPolicyDecisionV1 struct {
	Terms         domain.AttachedPiAPIInvocationPolicyV1              `json:"terms"`
	Receipt       domain.AttachedPiAPIOwnerAuthorizationReceiptV1     `json:"receipt"`
	Mapping       domain.AttachedPiAPIInvocationPolicyMappingV1       `json:"mapping"`
	MappingDigest domain.AttachedPiAPIInvocationPolicyMappingDigestV1 `json:"mapping_digest"`
	Generation    uint64                                              `json:"generation"`
	Replayed      bool                                                `json:"replayed"`
}
type AttachedPiAPIPolicyOfferV1 struct {
	Terms             domain.AttachedPiAPIInvocationPolicyV1              `json:"terms"`
	TermsDigest       domain.ProviderEvidenceDigestV1                     `json:"terms_digest"`
	CurrentGeneration uint64                                              `json:"current_generation"`
	CurrentMapping    domain.AttachedPiAPIInvocationPolicyMappingDigestV1 `json:"current_mapping,omitempty"`
}
type AttachedPiAPIOwnerPolicyStoreV1 interface {
	ReadAttachedPiAPIPolicy(context.Context, ReadAttachedPiAPIPolicyV1) (AttachedPiAPIPolicyOfferV1, error)
	ConfirmAttachedPiAPIPolicy(context.Context, ConfirmAttachedPiAPIPolicyV1) (AttachedPiAPIPolicyDecisionV1, error)
	RevokeAttachedPiAPIPolicy(context.Context, RevokeAttachedPiAPIPolicyV1) error
}

const AttachedPiAPIOwnerPolicyIssuerV1 = "sessionless.webbff.attached-pi-api.v1"
