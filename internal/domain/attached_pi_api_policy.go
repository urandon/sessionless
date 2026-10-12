package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"time"
	"unicode/utf8"
)

const AttachedPiAPIInvocationPolicyVersionV1 uint32 = 1

// These fixed metadata records have no collections or arbitrary bodies. Eight
// KiB covers their existing bounded opaque IDs, four provider tokens (128/256
// bytes, including worst-case six-byte JSON escaping), fixed digests, UTC times
// and numeric/field framing. This is not a Job, media or transport wire limit.
const attachedPiAPICanonicalMetadataMaxBytesV1 = 8 << 10

// AttachedPiAPIInvocationPolicyV1 is T in the acyclic P/E/T/R/M design. It
// references the original backend P and policy/price E, never R, M or the final
// binding/job. Validate, codecs and Digest check structure only: no method here
// verifies tariffs, current policy, owner permission or executable eligibility.
type AttachedPiAPIInvocationPolicyV1 struct {
	Version                uint32                  `json:"version"`
	Scope                  ProviderEvidenceScopeV1 `json:"scope"`
	ModelVendorID          string                  `json:"model_vendor_id"`
	ModelID                string                  `json:"model_id"`
	InputDataClass         ProviderDataClassV1     `json:"input_data_class"`
	EffectivePolicyDigest  string                  `json:"effective_policy_digest"`
	PriceObservationDigest string                  `json:"price_observation_digest"`
	Currency               string                  `json:"currency"`
	MaxContextTokens       uint64                  `json:"max_context_tokens"`
	MaxOutputTokens        uint64                  `json:"max_output_tokens"`
	MaxChargeMicrounits    uint64                  `json:"max_charge_microunits"`
	NotBefore              time.Time               `json:"not_before"`
	ExpiresAt              time.Time               `json:"expires_at"`
}

func (v AttachedPiAPIInvocationPolicyV1) Validate() error {
	if v.Version != AttachedPiAPIInvocationPolicyVersionV1 {
		return attachedPiAPIPolicyError("version", "must equal 1")
	}
	if err := v.Scope.Validate(); err != nil {
		return err
	}
	if v.Scope.Backend.BackendKind != HarnessBackendPiV1 ||
		v.Scope.Backend.ProviderContractKind != ProviderContractInvocationV1 ||
		v.Scope.Backend.CredentialDeliveryKind != ProviderCredentialDeliveryFileV1 ||
		v.Scope.Resource.Kind != ProviderResourceAPIAccountV1 {
		return attachedPiAPIPolicyError("scope", "requires the Pi API-account file-credential shape")
	}
	if err := validateProviderModel(v.ModelVendorID, v.ModelID); err != nil {
		return err
	}
	if err := validateDataClasses([]ProviderDataClassV1{v.InputDataClass}, false); err != nil {
		return err
	}
	for _, field := range []struct{ name, digest string }{
		{"effective_policy_digest", v.EffectivePolicyDigest},
		{"price_observation_digest", v.PriceObservationDigest},
	} {
		if err := validateSHA256("attached_pi_api_policy."+field.name, field.digest); err != nil {
			return err
		}
	}
	if !validCurrencyV1(v.Currency) || v.MaxContextTokens == 0 || v.MaxOutputTokens == 0 {
		return attachedPiAPIPolicyError("limits", "requires uppercase ASCII currency and positive token bounds")
	}
	return validateAttachedPiAPIWindowV1(v.NotBefore, v.ExpiresAt)
}

// AttachedPiAPIOwnerAuthorizationReceiptV1 is the structural R record, not an
// authenticated owner decision. Its subject is exactly T; its own hash and M
// are absent. A real issuer/source/currentness verifier remains separate.
type AttachedPiAPIOwnerAuthorizationReceiptV1 struct {
	Version                   uint32                   `json:"version"`
	IssuerID                  string                   `json:"issuer_id"`
	DecisionID                string                   `json:"decision_id"`
	TenantID                  TenantID                 `json:"tenant_id"`
	OwnerUserID               UserID                   `json:"owner_user_id"`
	DecisionSourceID          string                   `json:"decision_source_id"`
	SubjectTermsDigest        ProviderEvidenceDigestV1 `json:"subject_terms_digest"`
	Decision                  string                   `json:"decision"`
	IssuedAt                  time.Time                `json:"issued_at"`
	ExpiresAt                 time.Time                `json:"expires_at"`
	MembershipSecurityVersion uint64                   `json:"membership_security_version"`
}

func (v AttachedPiAPIOwnerAuthorizationReceiptV1) Validate() error {
	if v.Version != AttachedPiAPIInvocationPolicyVersionV1 || v.Decision != "authorize" || v.MembershipSecurityVersion == 0 {
		return attachedPiAPIPolicyError("receipt", "requires version 1, explicit authorize and positive security version")
	}
	for _, field := range []struct{ name, value string }{
		{"issuer_id", v.IssuerID}, {"decision_id", v.DecisionID}, {"decision_source_id", v.DecisionSourceID},
	} {
		if err := ValidateOpaqueID("attached_pi_api_policy."+field.name, field.value); err != nil {
			return err
		}
	}
	if err := v.TenantID.Validate(); err != nil {
		return err
	}
	if err := v.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := validateSHA256("attached_pi_api_policy.subject_terms_digest", string(v.SubjectTermsDigest)); err != nil {
		return err
	}
	return validateAttachedPiAPIWindowV1(v.IssuedAt, v.ExpiresAt)
}

// ValidateForTerms checks only canonical subject/scope consistency. A matching
// fabricated receipt remains unauthenticated until the real source verifier.
func (v AttachedPiAPIOwnerAuthorizationReceiptV1) ValidateForTerms(terms AttachedPiAPIInvocationPolicyV1) error {
	if err := v.Validate(); err != nil {
		return err
	}
	digest, err := terms.Digest()
	if err != nil {
		return err
	}
	if v.TenantID != terms.Scope.TenantID || v.OwnerUserID != terms.Scope.OwnerUserID || v.SubjectTermsDigest != digest {
		return attachedPiAPIPolicyError("receipt.subject", "must match exact canonical terms and owning scope")
	}
	return nil
}

// AttachedPiAPIInvocationPolicyMappingV1 is M, containing only independent T/R
// hashes. Structural consistency of M is not source provenance or permission.
type AttachedPiAPIInvocationPolicyMappingV1 struct {
	Version                         uint32                   `json:"version"`
	TermsDigest                     ProviderEvidenceDigestV1 `json:"terms_digest"`
	OwnerAuthorizationReceiptDigest ProviderEvidenceDigestV1 `json:"owner_authorization_receipt_digest"`
}

func (v AttachedPiAPIInvocationPolicyMappingV1) Validate() error {
	if v.Version != AttachedPiAPIInvocationPolicyVersionV1 {
		return attachedPiAPIPolicyError("mapping.version", "must equal 1")
	}
	if err := validateSHA256("attached_pi_api_policy.mapping.terms_digest", string(v.TermsDigest)); err != nil {
		return err
	}
	return validateSHA256("attached_pi_api_policy.mapping.owner_authorization_receipt_digest", string(v.OwnerAuthorizationReceiptDigest))
}

// ValidateFor checks independent T/R hash relationships only. It does not
// resolve a trusted decision source, current mapping or spending eligibility.
func (v AttachedPiAPIInvocationPolicyMappingV1) ValidateFor(terms AttachedPiAPIInvocationPolicyV1, receipt AttachedPiAPIOwnerAuthorizationReceiptV1) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if err := receipt.ValidateForTerms(terms); err != nil {
		return err
	}
	termsDigest, err := terms.Digest()
	if err != nil {
		return err
	}
	receiptDigest, err := receipt.Digest()
	if err != nil {
		return err
	}
	if v.TermsDigest != termsDigest || v.OwnerAuthorizationReceiptDigest != receiptDigest {
		return attachedPiAPIPolicyError("mapping.subject", "must match exact independent terms and receipt digests")
	}
	return nil
}

type AttachedPiAPIInvocationPolicyMappingDigestV1 string

func (v AttachedPiAPIInvocationPolicyMappingDigestV1) Validate() error {
	if err := validateSHA256("attached_pi_api_policy.mapping_digest", string(v)); err != nil {
		return err
	}
	if v == AttachedPiAPIInvocationPolicyMappingDigestV1(string(bytes.Repeat([]byte{'0'}, 64))) {
		return attachedPiAPIPolicyError("mapping_digest", "must not be zero")
	}
	return nil
}

func EncodeAttachedPiAPIInvocationPolicyV1(v AttachedPiAPIInvocationPolicyV1) ([]byte, error) {
	return encodeAttachedPiAPIMetadataV1(v, v.Validate)
}

func DecodeAttachedPiAPIInvocationPolicyV1(raw []byte) (AttachedPiAPIInvocationPolicyV1, error) {
	var v AttachedPiAPIInvocationPolicyV1
	if err := decodeAttachedPiAPIMetadataV1(raw, &v, func() error { return v.Validate() }); err != nil {
		return AttachedPiAPIInvocationPolicyV1{}, err
	}
	return v, nil
}

func EncodeAttachedPiAPIOwnerAuthorizationReceiptV1(v AttachedPiAPIOwnerAuthorizationReceiptV1) ([]byte, error) {
	return encodeAttachedPiAPIMetadataV1(v, v.Validate)
}

func DecodeAttachedPiAPIOwnerAuthorizationReceiptV1(raw []byte) (AttachedPiAPIOwnerAuthorizationReceiptV1, error) {
	var v AttachedPiAPIOwnerAuthorizationReceiptV1
	if err := decodeAttachedPiAPIMetadataV1(raw, &v, func() error { return v.Validate() }); err != nil {
		return AttachedPiAPIOwnerAuthorizationReceiptV1{}, err
	}
	return v, nil
}

func EncodeAttachedPiAPIInvocationPolicyMappingV1(v AttachedPiAPIInvocationPolicyMappingV1) ([]byte, error) {
	return encodeAttachedPiAPIMetadataV1(v, v.Validate)
}

func DecodeAttachedPiAPIInvocationPolicyMappingV1(raw []byte) (AttachedPiAPIInvocationPolicyMappingV1, error) {
	var v AttachedPiAPIInvocationPolicyMappingV1
	if err := decodeAttachedPiAPIMetadataV1(raw, &v, func() error { return v.Validate() }); err != nil {
		return AttachedPiAPIInvocationPolicyMappingV1{}, err
	}
	return v, nil
}

func (v AttachedPiAPIInvocationPolicyV1) Digest() (ProviderEvidenceDigestV1, error) {
	raw, err := EncodeAttachedPiAPIInvocationPolicyV1(v)
	if err != nil {
		return "", err
	}
	return attachedPiAPIMetadataDigestV1("sessionless.attached-pi-api-invocation-policy.v1", raw), nil
}

func (v AttachedPiAPIOwnerAuthorizationReceiptV1) Digest() (ProviderEvidenceDigestV1, error) {
	raw, err := EncodeAttachedPiAPIOwnerAuthorizationReceiptV1(v)
	if err != nil {
		return "", err
	}
	return attachedPiAPIMetadataDigestV1("sessionless.attached-pi-api-owner-authorization-receipt.v1", raw), nil
}

func (v AttachedPiAPIInvocationPolicyMappingV1) Digest() (AttachedPiAPIInvocationPolicyMappingDigestV1, error) {
	raw, err := EncodeAttachedPiAPIInvocationPolicyMappingV1(v)
	if err != nil {
		return "", err
	}
	digest := AttachedPiAPIInvocationPolicyMappingDigestV1(attachedPiAPIMetadataDigestV1("sessionless.attached-pi-api-invocation-policy-mapping.v1", raw))
	return digest, digest.Validate()
}

// CalculateAttachedPiAPIMaximumChargeV1 is arithmetic, not tariff evidence,
// token calibration, owner approval or a cap/eligibility/currentness checker.
// Zero rates and zero arithmetic results do not establish free permission.
func CalculateAttachedPiAPIMaximumChargeV1(inputTokens, outputTokens, inputRate, outputRate uint64) (uint64, error) {
	input, err := ceilAttachedPiAPITokenChargeV1(inputTokens, inputRate)
	if err != nil {
		return 0, err
	}
	output, err := ceilAttachedPiAPITokenChargeV1(outputTokens, outputRate)
	if err != nil {
		return 0, err
	}
	return sumAttachedPiAPIChargeV1(input, output)
}

func ceilAttachedPiAPITokenChargeV1(tokens, rate uint64) (uint64, error) {
	if rate != 0 && tokens > math.MaxUint64/rate {
		return 0, attachedPiAPIPolicyError("charge", "token-rate multiplication overflows")
	}
	product := tokens * rate
	charge := product / 1000000
	if product%1000000 != 0 {
		charge++ // product<=MaxUint64 bounds this quotient well below MaxUint64.
	}
	return charge, nil
}

func sumAttachedPiAPIChargeV1(input, output uint64) (uint64, error) {
	if output > math.MaxUint64-input {
		return 0, attachedPiAPIPolicyError("charge", "component sum overflows")
	}
	return input + output, nil
}

func validateAttachedPiAPIWindowV1(start, end time.Time) error {
	if start.IsZero() || end.IsZero() || start.Location() != time.UTC || end.Location() != time.UTC || !end.After(start) {
		return attachedPiAPIPolicyError("window", "requires explicit UTC start and later expiry")
	}
	return nil
}

func encodeAttachedPiAPIMetadataV1(v any, validate func() error) ([]byte, error) {
	if err := validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(v)
	if err != nil || len(raw) == 0 || len(raw) > attachedPiAPICanonicalMetadataMaxBytesV1 {
		return nil, attachedPiAPIPolicyError("codec", "cannot encode bounded canonical metadata")
	}
	return raw, nil
}

// Exact producer-byte equality rejects duplicate/nested duplicate keys, case
// aliases, omitted/null fields, alternate time/number/order/whitespace forms.
// DisallowUnknownFields also rejects forbidden R/M/profile backreferences.
func decodeAttachedPiAPIMetadataV1(raw []byte, target any, validate func() error) error {
	if len(raw) == 0 || len(raw) > attachedPiAPICanonicalMetadataMaxBytesV1 || !utf8.Valid(raw) {
		return attachedPiAPIPolicyError("codec", "requires bounded UTF-8 canonical metadata")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return attachedPiAPIPolicyError("codec", "invalid canonical metadata")
	}
	var trailing any
	if err := d.Decode(&trailing); !errors.Is(err, io.EOF) {
		return attachedPiAPIPolicyError("codec", "trailing canonical metadata")
	}
	expected, err := encodeAttachedPiAPIMetadataV1(target, validate)
	if err != nil || !bytes.Equal(raw, expected) {
		return attachedPiAPIPolicyError("codec", "requires exact canonical producer bytes")
	}
	return nil
}

func attachedPiAPIMetadataDigestV1(prefix string, raw []byte) ProviderEvidenceDigestV1 {
	h := sha256.New()
	_, _ = h.Write([]byte(prefix))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(raw)
	return ProviderEvidenceDigestV1(hex.EncodeToString(h.Sum(nil)))
}

func attachedPiAPIPolicyError(field, reason string) error {
	return ValidationError{Field: "attached_pi_api_policy." + field, Reason: reason}
}
