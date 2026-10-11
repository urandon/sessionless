package ports

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// These codecs describe only the credential-free owned fixture grammar. They
// are not a general model parser, a tool grant issuer or execution authority.
// A Store must obtain the actual immutable bytes from its private keeper and
// join their digests to the authenticated original admission/current database.
type OwnedFixtureModelResponseV1 struct {
	Version uint32                    `json:"version"`
	Kind    string                    `json:"kind"`
	Text    string                    `json:"text"`
	Calls   []OwnedFixtureModelCallV1 `json:"calls"`
}
type OwnedFixtureModelCallV1 struct {
	CallID    string                        `json:"call_id"`
	Kind      domain.AttemptOperationKindV1 `json:"kind"`
	Arguments json.RawMessage               `json:"arguments"`
}

func OwnedFixtureBytesDigestV1(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// Exact canonical bytes reject duplicate/unknown fields, extra JSON, changed
// argument spelling and null calls. The initial policy permits at most ONE
// call in a response: multiple calls fail before any call can be reserved.
func ParseOwnedFixtureModelResponseV1(body []byte) (OwnedFixtureModelResponseV1, error) {
	var value OwnedFixtureModelResponseV1
	if len(body) == 0 || len(body) > 64<<10 || !utf8.Valid(body) || json.Unmarshal(body, &value) != nil || value.Version != 1 || value.Kind != "model_response" || len(value.Text) > 24<<10 || value.Calls == nil || len(value.Calls) > 1 {
		return value, domain.ErrMembershipDenied
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, body) {
		return OwnedFixtureModelResponseV1{}, domain.ErrMembershipDenied
	}
	for i := range value.Calls {
		call := &value.Calls[i]
		if domain.ValidateOpaqueID("owned_fixture.call_id", call.CallID) != nil || (call.Kind != domain.AttemptOperationMCPToolV1 && call.Kind != domain.AttemptOperationWebSearchV1) || len(call.Arguments) == 0 || len(call.Arguments) > 4<<10 || call.Arguments[0] != '{' {
			return OwnedFixtureModelResponseV1{}, domain.ErrMembershipDenied
		}
		// Canonical object decoding independently rejects duplicated nested keys.
		var object map[string]any
		decoder := json.NewDecoder(bytes.NewReader(call.Arguments))
		decoder.UseNumber()
		if decoder.Decode(&object) != nil || object == nil || len(object) > 16 {
			return OwnedFixtureModelResponseV1{}, domain.ErrMembershipDenied
		}
		normalized, err := json.Marshal(object)
		if err != nil || !bytes.Equal(normalized, call.Arguments) {
			return OwnedFixtureModelResponseV1{}, domain.ErrMembershipDenied
		}
		call.Arguments = bytes.Clone(call.Arguments)
	}
	return value, nil
}

// Grant is frozen BEFORE its digest is admitted into the cost/policy. It does
// not contain the enclosing cost digest (which would introduce a digest cycle).
type OwnedFixtureToolGrantV1 struct {
	Version              uint32                       `json:"version"`
	TenantID             domain.TenantID              `json:"tenant_id"`
	OwnerUserID          domain.UserID                `json:"owner_user_id"`
	BillingOwnerUserID   domain.UserID                `json:"billing_owner_user_id"`
	Kind                 domain.ExternalServiceKindV1 `json:"kind"`
	ResourceID           string                       `json:"resource_id"`
	ResourceRevision     uint64                       `json:"resource_revision"`
	RevocationGeneration uint64                       `json:"revocation_generation"`
	ProfileID            string                       `json:"profile_id"`
	ProfileRevision      uint64                       `json:"profile_revision"`
	ConsentMode          string                       `json:"consent_mode"`
	ConsentRevision      uint64                       `json:"consent_revision"`
	Endpoint             string                       `json:"endpoint"`
	MaxArgumentBytes     uint64                       `json:"max_argument_bytes"`
	MaxResultBytes       uint64                       `json:"max_result_bytes"`
	ExpiresAt            time.Time                    `json:"expires_at"`
}

func (v OwnedFixtureToolGrantV1) Validate() error {
	if v.Version != 1 || v.TenantID.Validate() != nil || v.OwnerUserID.Validate() != nil || v.BillingOwnerUserID.Validate() != nil || domain.ValidateOpaqueID("owned_fixture.resource_id", v.ResourceID) != nil || domain.ValidateOpaqueID("owned_fixture.profile_id", v.ProfileID) != nil || v.ResourceRevision == 0 || v.RevocationGeneration == 0 || v.ProfileRevision == 0 || v.ConsentRevision == 0 || v.MaxArgumentBytes == 0 || v.MaxArgumentBytes > 4<<10 || v.MaxResultBytes == 0 || v.MaxResultBytes > 64<<10 || v.ExpiresAt.IsZero() || v.ExpiresAt.Year() < 1 || v.ExpiresAt.Year() > 9999 || v.ExpiresAt != v.ExpiresAt.UTC().Truncate(time.Microsecond) {
		return domain.ErrMembershipDenied
	}
	switch v.Kind {
	case domain.ExternalServiceMCPToolV1:
		if v.ConsentMode != "none" || v.Endpoint != "fixture://owned/mcp" {
			return domain.ErrMembershipDenied
		}
	case domain.ExternalServiceWebSearchV1:
		if v.ConsentMode != "initiating_user_required" || v.Endpoint != "fixture://owned/web-search" || v.MaxResultBytes > 24<<10 {
			return domain.ErrMembershipDenied
		}
	default:
		return domain.ErrMembershipDenied
	}
	return nil
}
func ParseOwnedFixtureToolGrantV1(body []byte) (OwnedFixtureToolGrantV1, error) {
	var value OwnedFixtureToolGrantV1
	if len(body) == 0 || len(body) > 4<<10 || !utf8.Valid(body) || json.Unmarshal(body, &value) != nil || value.Validate() != nil {
		return value, domain.ErrMembershipDenied
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, body) {
		return OwnedFixtureToolGrantV1{}, domain.ErrMembershipDenied
	}
	return value, nil
}
func OwnedFixtureGrantReferenceV1(digest string) (string, error) {
	if domain.ValidateSHA256Digest("owned_fixture.grant_digest", digest) != nil {
		return "", domain.ErrMembershipDenied
	}
	return "owned-fixture-grant-" + digest, nil
}

// Structural exact join only: authenticity/currentness comes from the Store's
// original source verifier and current transactional price/spending join.
func (v OwnedFixtureToolGrantV1) ValidateForCost(cost domain.ExternalServiceCostCeilingV1, bodyDigest string, at time.Time) error {
	if v.Validate() != nil || cost.Validate() != nil || domain.ValidateSHA256Digest("owned_fixture.grant_digest", bodyDigest) != nil || !at.Before(v.ExpiresAt) || !at.Before(cost.PriceExpiresAt) || at.Before(cost.PriceObservedAt) || v.Kind != cost.Kind || v.TenantID != cost.TenantID || v.OwnerUserID != cost.OwnerUserID || v.BillingOwnerUserID != cost.BillingOwnerUserID || v.ResourceID != cost.ResourceID || v.ResourceRevision != cost.ResourceRevision || v.RevocationGeneration != cost.RevocationGeneration || v.ProfileID != cost.ProfileID || v.ProfileRevision != cost.ProfileRevision || bodyDigest != cost.ScopedGrantDigest || cost.MaxAmountMicrounits == nil || cost.PriceState == domain.ExternalServicePriceUnknownV1 {
		return domain.ErrMembershipDenied
	}
	return nil
}
