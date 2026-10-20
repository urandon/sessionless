package mcpadmission

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
)

// Entirely synthetic policy DATA: neither real service catalog nor measured
// tariff/terms, owner consent, custody or operation capability is established.
func grantFixture(t *testing.T) (MCPToolGrantV1, domain.ExternalServiceCostCeilingV1, []byte) {
	t.Helper()
	at := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	descriptor := []byte(`{"inputSchema":{"additionalProperties":false,"properties":{"query":{"maxLength":100,"minLength":1,"type":"string"}},"required":["query"],"type":"object"},"name":"search_openai_docs"}`)
	g := MCPToolGrantV1{Version: 1, TenantID: "tenant-synthetic", OwnerUserID: "owner-synthetic", BillingOwnerUserID: "owner-synthetic",
		ResourceID: "docs-resource", ResourceRevision: 2, RevocationGeneration: 3, ProfileID: "docs-profile", ProfileRevision: 4,
		DataPolicyDigest: strings.Repeat("a", 64), DataClass: "public", ConsentMode: "owner_public_query", ConsentRevision: 5,
		Endpoint: "https://developers.openai.com/mcp", Transport: "streamable_http", ProtocolVersion: mcpwire.ProtocolVersionV1,
		SchemaValidator: domain.ConfiguredToolSchemaValidatorV1, ModelDescription: "Synthetic public documentation lookup", Descriptor: descriptor,
		Limits:   mcpwire.LimitsV1{MessageBytes: 64 << 10, AggregateBytes: 256 << 10, JSONDepth: 32, JSONMembers: 4096, SSELineBytes: 64 << 10, SSEEventBytes: 64 << 10, SSEEvents: 128, Tools: 1, DescriptorBytes: 32 << 10, ArgumentBytes: 4096, ResultTextBytes: 64 << 10, ContentItems: 16},
		IssuedAt: at, ExpiresAt: at.Add(time.Hour)}
	if err := g.Validate(); err != nil {
		t.Fatal("synthetic policy shape", err)
	}
	digest, err := g.Digest()
	if err != nil {
		t.Fatal(err)
	}
	cost := domain.ExternalServiceCostCeilingV1{Version: 1, Kind: domain.ExternalServiceMCPToolV1, TenantID: g.TenantID, OwnerUserID: g.OwnerUserID, BillingOwnerUserID: g.BillingOwnerUserID,
		ResourceID: g.ResourceID, ResourceRevision: g.ResourceRevision, RevocationGeneration: g.RevocationGeneration, ProfileID: g.ProfileID, ProfileRevision: g.ProfileRevision,
		ScopedGrantDigest: digest, Currency: "RUB", PriceRevision: "synthetic-price", PriceObservedAt: at, PriceExpiresAt: at.Add(time.Hour), PriceEvidenceDigest: strings.Repeat("b", 64), TermsEvidenceDigest: strings.Repeat("c", 64), PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: new(uint64)}
	body, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	return g, cost, body
}

func TestMCPInstalledGrantCanonicalAndAcyclic(t *testing.T) {
	g, cost, body := grantFixture(t)
	parsed, err := ParseMCPToolGrantV1(body)
	if err != nil {
		t.Fatal(err)
	}
	if err := parsed.ValidateForCost(cost, g.IssuedAt); err != nil {
		t.Fatal("exact installed join", err)
	}
	digest, err := parsed.Digest()
	sum := sha256.Sum256(body)
	if err != nil || digest != hex.EncodeToString(sum[:]) {
		t.Fatal("grant does not identify complete exact bytes", err)
	}
	// Future Attempt scope belongs to existing spending/operation records. The
	// installed template must not contain the later enclosing authority fields.
	for _, forbidden := range []string{"run_id", "attempt_id", "harness_binding", "invocation_authority", "agent_loop_policy", "scoped_grant_digest"} {
		if bytes.Contains(body, []byte(`"`+forbidden+`"`)) {
			t.Fatalf("construction cycle introduced %s", forbidden)
		}
	}
	if strings.Contains(fmt.Sprintf("%v %#v", parsed, parsed), "Synthetic public") {
		t.Fatal("policy prose leaked in diagnostic")
	}
	copy := append([]byte(nil), body...)
	decoded, err := ParseMCPToolGrantV1(copy)
	if err != nil {
		t.Fatal(err)
	}
	clear(copy)
	if decoded.Validate() != nil {
		t.Fatal("parsed descriptor aliases caller bytes")
	}
}

func TestMCPInstalledGrantRejectsShapeAndDescriptorDrift(t *testing.T) {
	for _, name := range []string{"version", "tenant", "owner", "payer", "resource", "resource-revision", "revoke", "profile", "profile-revision", "policy", "private-data", "consent", "consent-revision", "endpoint", "protocol", "transport", "validator", "catalog-width", "zero-limit", "oversized-prose", "nul-prose", "time-precision", "expiry", "tool-name", "output-schema", "task-required", "noncanonical-descriptor"} {
		t.Run(name, func(t *testing.T) {
			g, _, _ := grantFixture(t)
			switch name {
			case "version":
				g.Version++
			case "tenant":
				g.TenantID = ""
			case "owner":
				g.OwnerUserID = ""
			case "payer":
				g.BillingOwnerUserID = ""
			case "resource":
				g.ResourceID = ""
			case "resource-revision":
				g.ResourceRevision = 0
			case "revoke":
				g.RevocationGeneration = 0
			case "profile":
				g.ProfileID = ""
			case "profile-revision":
				g.ProfileRevision = 0
			case "policy":
				g.DataPolicyDigest = "not-digest"
			case "private-data":
				g.DataClass = "private"
			case "consent":
				g.ConsentMode = "none"
			case "consent-revision":
				g.ConsentRevision = 0
			case "endpoint":
				g.Endpoint = "https://developers.openai.com/mcp?redirect=1"
			case "protocol":
				g.ProtocolVersion = "2025-03-26"
			case "transport":
				g.Transport = "http_sse"
			case "validator":
				g.SchemaValidator = "arbitrary-json-schema"
			case "catalog-width":
				g.Limits.Tools = 65
			case "zero-limit":
				g.Limits.ArgumentBytes = 0
			case "oversized-prose":
				g.ModelDescription = strings.Repeat("x", 4097)
			case "nul-prose":
				g.ModelDescription = "a\x00b"
			case "time-precision":
				g.IssuedAt = g.IssuedAt.Add(time.Nanosecond)
			case "expiry":
				g.ExpiresAt = g.IssuedAt
			case "tool-name":
				g.Descriptor = bytes.Replace(g.Descriptor, []byte("search_openai_docs"), []byte("other"), 1)
			case "output-schema":
				g.Descriptor = append([]byte(`{"outputSchema":{},`), g.Descriptor[1:]...)
			case "task-required":
				g.Descriptor = append([]byte(`{"execution":{"taskSupport":"required"},`), g.Descriptor[1:]...)
			case "noncanonical-descriptor":
				g.Descriptor = append([]byte(" "), g.Descriptor...)
			}
			if g.Validate() == nil {
				t.Fatal("invalid installed policy accepted")
			}
		})
	}
	_, _, body := grantFixture(t)
	for _, raw := range [][]byte{nil, bytes.Repeat([]byte("x"), int(GrantBodyBytesV1+1)), append([]byte(" "), body...), append(append([]byte(nil), body[:len(body)-1]...), []byte(`,"run_id":"future"}`)...), bytes.Replace(body, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)} {
		if _, err := ParseMCPToolGrantV1(raw); err == nil {
			t.Fatal("noncanonical or unsupported grant encoding accepted")
		}
	}
}

func TestMCPInstalledGrantExactCostAndTime(t *testing.T) {
	for _, name := range []string{"tenant", "owner", "payer", "kind", "resource", "resource-revision", "revoke", "profile", "profile-revision", "digest", "unknown-price", "before-grant", "at-grant-expiry", "price-expiry"} {
		t.Run(name, func(t *testing.T) {
			g, cost, _ := grantFixture(t)
			at := g.IssuedAt
			switch name {
			case "tenant":
				cost.TenantID = "foreign"
			case "owner":
				cost.OwnerUserID = "foreign"
			case "payer":
				cost.BillingOwnerUserID = "foreign"
			case "kind":
				cost.Kind = domain.ExternalServiceWebSearchV1
			case "resource":
				cost.ResourceID = "foreign"
			case "resource-revision":
				cost.ResourceRevision++
			case "revoke":
				cost.RevocationGeneration++
			case "profile":
				cost.ProfileID = "foreign"
			case "profile-revision":
				cost.ProfileRevision++
			case "digest":
				cost.ScopedGrantDigest = strings.Repeat("f", 64)
			case "unknown-price":
				cost.PriceState = domain.ExternalServicePriceUnknownV1
				cost.MaxAmountMicrounits = nil
			case "before-grant":
				at = at.Add(-time.Microsecond)
			case "at-grant-expiry":
				at = g.ExpiresAt
			case "price-expiry":
				cost.PriceExpiresAt = g.IssuedAt.Add(time.Minute)
				at = cost.PriceExpiresAt
			}
			// Recompute both exact valid encodings: scope mismatches must fail the
			// cross-record join, not merely stale internal canonical digests.
			if cost.Validate() != nil {
				t.Fatal("negative cost shape invalid")
			}
			if g.ValidateForCost(cost, at) == nil {
				t.Fatal("wrong current cost/window accepted")
			}
		})
	}
}
