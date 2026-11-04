package ports_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func localSearchPolicyData(t *testing.T) (ports.LocalSearchHTTPSProxyPolicyV1, ports.LocalSearchHTTPSProxyAttestationV1) {
	t.Helper()
	d := strings.Repeat("a", 64)
	at := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	g := domain.WebSearchResourceGrantV2{Version: 2, TenantID: "tenant", OwnerUserID: "owner", BillingOwnerUserID: "owner", ResourceID: "search-resource", ResourceRevision: 1, RevocationGeneration: 1, ConsentRevision: 1, ProfileID: domain.WebSearchProfileV1, ProfileRevision: 1, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Endpoint: domain.WebSearchEndpointV1, Method: "POST", CredentialRef: "search-credential", CredentialAudience: domain.WebSearchCredentialAudienceV1, DataPolicyDigest: d, APISchemaDigest: d, AdapterArtifactDigest: d, Currency: "USD", PriceRevision: "price", PriceEvidenceDigest: d, TermsEvidenceDigest: d, PerCallReservationMicrounits: 10000, AggregateReservationMicrounits: 20000, IssuedAt: at, ExpiresAt: at.Add(time.Hour)}
	p := ports.LocalSearchHTTPSProxyPolicyV1{Version: 1, Grant: g, ConfiguredProfileDigest: d, ConnectorArtifactDigest: d, HostIdentityDigest: d, NetworkNamespaceIdentityDigest: d, MaxOperationTimeout: 15 * time.Second, AutomaticRetriesDenied: true, ResumptionDenied: true}
	pd, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	a := ports.LocalSearchHTTPSProxyAttestationV1{Version: 1, PolicyDigest: pd, Observation: domain.RegisteredLocalSearchHTTPSProxyV1{ConfiguredProfileDigest: d, EndpointPolicy: string(domain.RegisteredProxyTransportLocalSearchHTTPSV1), ConnectorArtifactDigest: d, InstallationID: "installation", InstallationRevision: 1, HostIdentityDigest: d, NetworkNamespaceIdentityDigest: d, ResolutionSetDigest: d}, ConnectionAddressPinned: true, DNSRebindingDenied: true, PublicUnicastOnly: true, RedirectsDenied: true, AmbientProxyDenied: true, CertificateValidationRequired: true, TLSHandshakeVerified: true, VerifiedSNI: "api.tavily.com", ResolvedHost: "api.tavily.com", ResolvedPort: 443, ObservedAt: at, ExpiresAt: at.Add(15 * time.Second)}
	if a.ValidateForPolicy(p) != nil {
		t.Fatal("bounded SEARCH policy DATA")
	}
	return p, a
}

func TestLocalSearchPolicyDataStrictAndClosedPublicationShape(t *testing.T) {
	p, a := localSearchPolicyData(t)
	pb, _ := json.Marshal(p)
	ab, _ := json.Marshal(a)
	if _, _, err := ports.DecodeLocalSearchHTTPSProxyDataV1(pb, ab); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"null", "alias", "duplicate", "unknown", "host", "TLS", "expiry", "retry", "grant_endpoint"} {
		badp, bada := bytes.Clone(pb), bytes.Clone(ab)
		switch mutation {
		case "null":
			badp = bytes.Replace(badp, []byte(`"grant":{`), []byte(`"grant":null,"extra":{`), 1)
		case "alias":
			badp = bytes.Replace(badp, []byte(`"version":`), []byte(`"Version":`), 1)
		case "duplicate":
			bada = bytes.Replace(bada, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
		case "unknown":
			bada = bytes.Replace(bada, []byte(`"version":1`), []byte(`"version":1,"approved":true`), 1)
		case "host":
			bada = bytes.ReplaceAll(bada, []byte("api.tavily.com"), []byte("developers.openai.com"))
		case "TLS":
			bada = bytes.Replace(bada, []byte(`"tls_handshake_verified":true`), []byte(`"tls_handshake_verified":false`), 1)
		case "expiry":
			b := a
			b.ExpiresAt = b.ExpiresAt.Add(time.Microsecond)
			bada, _ = json.Marshal(b)
		case "retry":
			badp = bytes.Replace(badp, []byte(`"automatic_retries_denied":true`), []byte(`"automatic_retries_denied":false`), 1)
		case "grant_endpoint":
			badp = bytes.Replace(badp, []byte(domain.WebSearchEndpointV1), []byte("https://api.tavily.com/extract"), 1)
		}
		if _, _, err := ports.DecodeLocalSearchHTTPSProxyDataV1(badp, bada); err == nil {
			t.Fatalf("mutated %s accepted", mutation)
		}
	}
	pub := taggedProxyPublicationFixture(t)
	pub.TransportKind = domain.RegisteredProxyTransportLocalSearchHTTPSV1
	pub.ServiceKind = domain.RegisteredProxyWebSearchV1
	pub.OperationSequence = 2
	pub.ObservedAt = a.ObservedAt
	pub.OperationDeadline = a.ExpiresAt
	pub.PolicyBody, pub.AttestationBody = pb, ab
	if bodies, err := pub.Bodies(); err != nil || bodies.LocalSearchHTTPS == nil || bodies.LocalLoopback != nil || bodies.LocalPublicMCPHTTPS != nil || bodies.PinnedHTTPS != nil {
		t.Fatal("closed explicit SEARCH DATA publication shape", err)
	}
	if pub.Validate() != nil {
		t.Fatal("correlated DATA shape rejected; actual original producer authentication is separate")
	}
}

func TestLocalSearchPolicyBuildUsesOriginalConfiguredProfileAndGrant(t *testing.T) {
	c := localMCPConfiguredProfileFixture(t)
	p, _ := localSearchPolicyData(t)
	g := p.Grant
	g.TenantID, g.OwnerUserID, g.BillingOwnerUserID = c.TenantID, c.OwnerUserID, c.OwnerUserID
	actual, err := ports.BuildLocalSearchHTTPSProxyPolicyV1(c, g)
	digest, _ := c.Digest()
	if err != nil || actual.ConfiguredProfileDigest != digest || actual.Grant != g || actual.MaxOperationTimeout != 15*time.Second || actual.ConnectorArtifactDigest != c.WorkerExecutableDigest {
		t.Fatal("fixed endpoint policy disconnected from original DATA", err)
	}
	g.OwnerUserID = "foreign-owner"
	if _, err := ports.BuildLocalSearchHTTPSProxyPolicyV1(c, g); err == nil {
		t.Fatal("foreign grant owner accepted")
	}
}
