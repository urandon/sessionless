//go:build ydbintegration

package serverlessegress

// Strictly test-only fixed synthetic producer. Release builds do not contain
// this factory, and it exports no resolver/root/dial/endpoint/header injection.
import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
)

func OwnedHTTPSFixturePolicyV1(profile domain.OwnedAgentProfileCurrentV1) (PolicyV1, error) {
	if profile.Validate() != nil || profile.Backend.BackendKind != domain.HarnessBackendDeterministicFixtureV1 || profile.Resource.Kind != domain.ProviderResourceCredentiallessFixtureV1 || profile.Resource.CredentialMode != domain.ProviderCredentialNoneV1 || profile.Resource.CredentialGeneration != 0 || profile.Locator.ModelVendorID != "fixture-vendor" {
		return PolicyV1{}, ErrConfig
	}
	p := PolicyV1{Version: 1, RoutePolicyDigest: profile.ProviderRouteDigest, EffectivePolicyDigest: profile.EffectivePolicyDigest, ProxyArtifactDigest: profile.Substrate.EgressProxyArtifactDigest, ProxyIdentityDigest: profile.Substrate.EgressProxyIdentityDigest,
		Route:    domain.ProviderRouteV1{BackendKind: domain.HarnessBackendDeterministicFixtureV1, ModelVendorID: profile.Locator.ModelVendorID, ModelID: profile.Locator.ModelID, TransportKind: domain.ProviderTransportDirectAPIV1, TransportProvider: "deterministic-fixture", UpstreamProviderID: "synthetic", EndpointID: "owned-operation", BillingKind: domain.ProviderBillingNoneV1, BillingAuthority: profile.Resource.ResourceID},
		Endpoint: EndpointPolicyV1{Scheme: "https", Host: "fixture.sessionless.dev", Port: 443, Path: "/owned-operation", Method: "POST", DNSClass: DNSPublicUnicastOnlyV1, RedirectPolicy: RedirectDenyV1, MaxRequestBytes: min(profile.CostCeiling.MaxIngressBytes, uint64(1<<20)), MaxResponseBytes: min(profile.CostCeiling.MaxEgressBytes, uint64(1<<20))}}
	if _, err := p.Digest(); err != nil {
		return PolicyV1{}, ErrConfig
	}
	return p, nil
}

type OwnedHTTPSFixtureV1 struct {
	proxy    *PinnedHTTPSProxyV1
	server   *httptest.Server
	requests atomic.Uint64
	mu       sync.Mutex
	body     []byte
	response []byte
}

func NewOwnedHTTPSFixtureV1(ctx context.Context, profile domain.OwnedAgentProfileCurrentV1, clock ports.Clock, response []byte) (*OwnedHTTPSFixtureV1, error) {
	return NewOwnedHTTPSFixtureForOperationV1(ctx, profile, clock, response, 1, domain.AttemptOperationModelV1)
}
func NewOwnedHTTPSFixtureForOperationV1(ctx context.Context, profile domain.OwnedAgentProfileCurrentV1, clock ports.Clock, response []byte, sequence uint64, kind domain.AttemptOperationKindV1) (*OwnedHTTPSFixtureV1, error) {
	service := domain.RegisteredProxyProviderV1
	switch kind {
	case domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1:
	case domain.AttemptOperationMCPToolV1:
		service = domain.RegisteredProxyMCPToolV1
	case domain.AttemptOperationWebSearchV1:
		service = domain.RegisteredProxyWebSearchV1
	default:
		return nil, ErrConfig
	}
	if sequence == 0 || sequence > 12 {
		return nil, ErrConfig
	}
	policy, err := OwnedHTTPSFixturePolicyV1(profile)
	if err != nil || ctx == nil || ctx.Err() != nil || clock == nil || len(response) == 0 || uint64(len(response)) > policy.Endpoint.MaxResponseBytes {
		return nil, ErrConfig
	}
	now := clock.Now().UTC()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, ErrConfig
	}
	certTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: policy.Endpoint.Host}, DNSNames: []string{policy.Endpoint.Host}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, certTemplate, certTemplate, pub, key)
	if err != nil {
		return nil, ErrConfig
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, ErrConfig
	}
	f := &OwnedHTTPSFixtureV1{response: bytes.Clone(response)}
	f.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests.Add(1)
		body, err := io.ReadAll(io.LimitReader(r.Body, int64(policy.Endpoint.MaxRequestBytes)+1))
		if err != nil || uint64(len(body)) > policy.Endpoint.MaxRequestBytes || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.TLS.ServerName != policy.Endpoint.Host {
			clear(body)
			w.WriteHeader(400)
			return
		}
		f.mu.Lock()
		f.body = body
		f.mu.Unlock()
		_, _ = w.Write(f.response)
	}))
	f.server.Config.ErrorLog = log.New(io.Discard, "", 0)
	f.server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, NextProtos: []string{"http/1.1"}, MinVersion: tls.VersionTLS12}
	f.server.StartTLS()
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	deps := pinnedHTTPSDependencies{fixture: true, fixtureSequence: sequence, fixtureService: service, roots: roots,
		lookup: func(ctx context.Context, host string) ([]netip.Addr, error) {
			if host != policy.Endpoint.Host {
				return nil, ErrEgress
			}
			return []netip.Addr{netip.MustParseAddr("1.1.1.1")}, nil
		},
		dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			if network != "tcp" || address != "1.1.1.1:443" {
				return nil, ErrEgress
			}
			return (&net.Dialer{}).DialContext(ctx, network, f.server.Listener.Addr().String())
		}}
	f.proxy, err = newPinnedHTTPSProxyV1(policy, clock, 30*time.Second, 60*time.Second, deps)
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
func (f *OwnedHTTPSFixtureV1) Proxy() *PinnedHTTPSProxyV1 { return f.proxy }
func (f *OwnedHTTPSFixtureV1) Requests() uint64           { return f.requests.Load() }
func (f *OwnedHTTPSFixtureV1) RequestBody() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return bytes.Clone(f.body)
}
func (f *OwnedHTTPSFixtureV1) Close() {
	if f == nil {
		return
	}
	if f.proxy != nil {
		_ = f.proxy.Close()
	}
	if f.server != nil {
		f.server.Close()
	}
	f.mu.Lock()
	clear(f.body)
	clear(f.response)
	f.body = nil
	f.response = nil
	f.mu.Unlock()
}
