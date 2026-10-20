// Package mcpadmission validates installed MCP policy DATA and exact model-call
// bindings. No value in this package is a capability or current authorization.
package mcpadmission

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

const GrantBodyBytesV1 uint64 = 64 << 10

var ErrGrant = errors.New("MCP installed grant or call binding is invalid")

// MCPToolGrantV1 is installed BEFORE an Attempt exists. Its scope is the exact
// owner/resource/policy, not arbitrary future Run/Attempt IDs. Existing original
// execution-source, per-Attempt spending and operation joins supply that scope.
// No enclosing cost/policy/binding/invocation digest enters this earlier body.
// Descriptor is complete canonical DATA, not a locator-only schema promise.
type MCPToolGrantV1 struct {
	Version              uint32           `json:"version"`
	TenantID             domain.TenantID  `json:"tenant_id"`
	OwnerUserID          domain.UserID    `json:"owner_user_id"`
	BillingOwnerUserID   domain.UserID    `json:"billing_owner_user_id"`
	ResourceID           string           `json:"resource_id"`
	ResourceRevision     uint64           `json:"resource_revision"`
	RevocationGeneration uint64           `json:"revocation_generation"`
	ProfileID            string           `json:"profile_id"`
	ProfileRevision      uint64           `json:"profile_revision"`
	DataPolicyDigest     string           `json:"data_policy_digest"`
	DataClass            string           `json:"data_class"`
	ConsentMode          string           `json:"consent_mode"`
	ConsentRevision      uint64           `json:"consent_revision"`
	Endpoint             string           `json:"endpoint"`
	Transport            string           `json:"transport"`
	ProtocolVersion      string           `json:"protocol_version"`
	SchemaValidator      string           `json:"schema_validator"`
	ModelDescription     string           `json:"model_description"`
	Descriptor           json.RawMessage  `json:"descriptor"`
	Limits               mcpwire.LimitsV1 `json:"limits"`
	IssuedAt             time.Time        `json:"issued_at"`
	ExpiresAt            time.Time        `json:"expires_at"`
}

func (MCPToolGrantV1) String() string     { return "MCPToolGrantV1{content:[redacted]}" }
func (g MCPToolGrantV1) GoString() string { return g.String() }

func canonicalTime(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 && t == t.UTC().Truncate(time.Microsecond)
}

func (g MCPToolGrantV1) Validate() error {
	if g.Version != 1 || g.TenantID.Validate() != nil || g.OwnerUserID.Validate() != nil || g.BillingOwnerUserID.Validate() != nil ||
		g.ResourceRevision == 0 || g.RevocationGeneration == 0 || g.ProfileRevision == 0 || g.ConsentRevision == 0 ||
		domain.ValidateOpaqueID("mcp.resource", g.ResourceID) != nil || domain.ValidateOpaqueID("mcp.profile", g.ProfileID) != nil ||
		domain.ValidateSHA256Digest("mcp.data_policy", g.DataPolicyDigest) != nil ||
		g.DataClass != "public" || g.ConsentMode != "owner_public_query" ||
		g.Endpoint != "https://developers.openai.com/mcp" || g.Transport != "streamable_http" || g.ProtocolVersion != mcpwire.ProtocolVersionV1 ||
		g.SchemaValidator != domain.ConfiguredToolSchemaValidatorV1 || g.Limits.Validate() != nil ||
		len(g.ModelDescription) > 4096 || !utf8.ValidString(g.ModelDescription) || bytes.IndexByte([]byte(g.ModelDescription), 0) >= 0 ||
		!canonicalTime(g.IssuedAt) || !canonicalTime(g.ExpiresAt) || !g.ExpiresAt.After(g.IssuedAt) {
		return ErrGrant
	}
	d, err := mcpwire.ParseDescriptorV1(g.Descriptor, g.Limits)
	if err != nil || d.Name() != "search_openai_docs" {
		return ErrGrant
	}
	return nil
}

func ParseMCPToolGrantV1(body []byte) (MCPToolGrantV1, error) {
	var g MCPToolGrantV1
	if _, err := strictjson.Parse(body, GrantBodyBytesV1, 64, 4096); err != nil || json.Unmarshal(body, &g) != nil || g.Validate() != nil {
		return MCPToolGrantV1{}, ErrGrant
	}
	canonical, err := json.Marshal(g)
	if err != nil || !bytes.Equal(canonical, body) {
		return MCPToolGrantV1{}, ErrGrant
	}
	return g, nil
}

// Digest identifies exact canonical installed bytes. It is NOT an approval,
// measured tariff, evidence of a public query or a live service qualification.
func (g MCPToolGrantV1) Digest() (string, error) {
	if g.Validate() != nil {
		return "", ErrGrant
	}
	body, err := json.Marshal(g)
	if err != nil || uint64(len(body)) > GrantBodyBytesV1 {
		return "", ErrGrant
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// ValidateForCost is a structural exact join. Actual source custody, current
// profile/resource/spending/consent and operation authorization remain in Store.
func (g MCPToolGrantV1) ValidateForCost(cost domain.ExternalServiceCostCeilingV1, at time.Time) error {
	digest, err := g.Digest()
	if err != nil || cost.ValidateAt(at) != nil || at.Before(g.IssuedAt) || !at.Before(g.ExpiresAt) ||
		cost.Kind != domain.ExternalServiceMCPToolV1 || cost.TenantID != g.TenantID || cost.OwnerUserID != g.OwnerUserID || cost.BillingOwnerUserID != g.BillingOwnerUserID ||
		cost.ResourceID != g.ResourceID || cost.ResourceRevision != g.ResourceRevision || cost.RevocationGeneration != g.RevocationGeneration ||
		cost.ProfileID != g.ProfileID || cost.ProfileRevision != g.ProfileRevision || cost.ScopedGrantDigest != digest {
		return ErrGrant
	}
	return nil
}
