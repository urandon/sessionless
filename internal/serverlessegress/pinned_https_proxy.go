package serverlessegress

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/boundedhttp"
	"gitcode.com/urandon/sessionless/internal/directopenrouter"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	pinnedMaxBody    = 1 << 20
	pinnedMaxHeaders = 16 << 10
	pinnedMaxSecret  = 4096
)

// PinnedHTTPSProxyV1 owns exactly one preparation and one original TLS
// connection. It is deliberately not registered by startup and supplies no
// durable registration, operation debit, provider parsing or model-body proof.
// Callers must Close it when abandoning a successful preflight.
type PinnedHTTPSProxyV1 struct {
	self                           *PinnedHTTPSProxyV1 // rejects value copies sharing the original socket
	mu                             sync.Mutex
	policy                         PolicyV1
	clock                          ports.Clock
	operationTimeout, totalTimeout time.Duration
	lastTime                       time.Time
	state                          uint8 // new, preparing, ready, consumed, closed
	conn                           net.Conn
	rawConn                        net.Conn
	lifetime                       context.Context
	cancel                         context.CancelFunc
	stopLifetime                   func() bool
	attestation                    ProxyAttestationV1
	observedAt                     time.Time
	publicationAllocation          domain.RegisteredAllocationDigestV2
	publicationSource              string
	publicationOperation           domain.AttemptOperationDigestV1
	publicationDeadline            time.Time
	publicationKey                 [32]byte
	publicationScope               domain.AttemptExecutionSourceScopeV1
	publicationLease               domain.LeaseID
	publicationFence               uint64
	publicationClaim               string
	deps                           pinnedHTTPSDependencies
}

// Dependencies are private: production has no URL, resolver, dialer, root-CA
// or HTTP-client override. Same-package tests use synthetic TLS peers only.
type pinnedHTTPSDependencies struct {
	fixtureSequence uint64
	fixtureService  domain.RegisteredProxyServiceKindV1
	fixture         bool // private test-only fixed credential-free route; never set by public constructor
	lookup          func(context.Context, string) ([]netip.Addr, error)
	dial            func(context.Context, string, string) (net.Conn, error)
	roots           *x509.CertPool
}

var _ Proxy = (*PinnedHTTPSProxyV1)(nil)

func NewPinnedHTTPSProxyV1(policy PolicyV1, clock ports.Clock, operationTimeout, totalTimeout time.Duration) (*PinnedHTTPSProxyV1, error) {
	return newPinnedHTTPSProxyV1(policy, clock, operationTimeout, totalTimeout, pinnedHTTPSDependencies{
		lookup: func(ctx context.Context, host string) ([]netip.Addr, error) {
			return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		},
		dial: (&net.Dialer{}).DialContext,
	})
}

func newPinnedHTTPSProxyV1(policy PolicyV1, clock ports.Clock, operationTimeout, totalTimeout time.Duration, deps pinnedHTTPSDependencies) (*PinnedHTTPSProxyV1, error) {
	if _, err := policy.Digest(); err != nil || clock == nil || deps.lookup == nil || deps.dial == nil ||
		operationTimeout <= 0 || operationTimeout > 30*time.Second || totalTimeout <= 0 || totalTimeout > 60*time.Second || operationTimeout > totalTimeout ||
		policy.Endpoint.MaxRequestBytes > pinnedMaxBody || policy.Endpoint.MaxResponseBytes > pinnedMaxBody || len(policy.Endpoint.Path) > 2048 || (!pinnedReviewedRoute(policy) && !(deps.fixture && pinnedFixtureRoute(policy))) {
		return nil, ErrConfig
	}
	for _, b := range []byte(policy.Endpoint.Path) {
		if b <= ' ' || b >= 127 {
			return nil, ErrConfig
		}
	}
	p := &PinnedHTTPSProxyV1{policy: policy, clock: clock, operationTimeout: operationTimeout, totalTimeout: totalTimeout, deps: deps}
	p.self = p
	if _, err := rand.Read(p.publicationKey[:]); err != nil {
		return nil, ErrConfig
	}
	return p, nil
}

func (p *PinnedHTTPSProxyV1) sampleLocked() (time.Time, bool) {
	now := p.clock.Now().UTC()
	if now.IsZero() || (!p.lastTime.IsZero() && now.Before(p.lastTime)) {
		return now, false
	}
	p.lastTime = now
	return now, true
}

func (p *PinnedHTTPSProxyV1) Preflight(ctx context.Context, policy PolicyV1) (ProxyAttestationV1, error) {
	if p == nil || p.self != p || ctx == nil {
		return ProxyAttestationV1{}, ErrProxyAttestation
	}
	p.mu.Lock()
	now, validTime := p.sampleLocked()
	if p.state != 0 || policy != p.policy || ctx.Err() != nil || !validTime {
		if p.state == 0 {
			p.state = 4
		}
		p.mu.Unlock()
		return ProxyAttestationV1{}, ErrProxyAttestation
	}
	p.state = 1 // burn before DNS or TCP; failure never restores preparation
	p.lifetime, p.cancel = context.WithTimeout(ctx, p.totalTimeout)
	life := p.lifetime
	expires := now.Add(p.totalTimeout)
	if deadline, ok := life.Deadline(); ok && deadline.Before(expires) {
		expires = deadline.UTC()
	}
	p.stopLifetime = context.AfterFunc(life, func() { _ = p.Close() })
	p.mu.Unlock()
	ok := false
	defer func() {
		if !ok {
			_ = p.Close()
		}
	}()
	op, cancel := context.WithTimeout(life, p.operationTimeout)
	defer cancel()
	addresses, err := p.deps.lookup(op, policy.Endpoint.Host)
	if err != nil {
		return ProxyAttestationV1{}, ErrProxyAttestation
	}
	canonical, digest, err := pinnedPublicAddresses(addresses)
	if err != nil {
		return ProxyAttestationV1{}, ErrProxyAttestation
	}
	conn, err := p.deps.dial(op, "tcp", net.JoinHostPort(canonical[0], "443"))
	if err != nil {
		return ProxyAttestationV1{}, ErrProxyAttestation
	}
	p.mu.Lock()
	if p.state != 1 || life.Err() != nil {
		p.mu.Unlock()
		_ = conn.Close()
		return ProxyAttestationV1{}, ErrProxyAttestation
	}
	p.conn, p.rawConn = conn, conn
	p.mu.Unlock()
	stop := context.AfterFunc(op, func() { _ = conn.Close() })
	defer stop()
	deadline, _ := op.Deadline()
	if conn.SetDeadline(deadline) != nil {
		return ProxyAttestationV1{}, ErrProxyAttestation
	}
	secure := tls.Client(conn, &tls.Config{ServerName: policy.Endpoint.Host, RootCAs: p.deps.roots, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}})
	if secure.HandshakeContext(op) != nil || secure.ConnectionState().NegotiatedProtocol != "http/1.1" {
		return ProxyAttestationV1{}, ErrProxyAttestation
	}
	policyDigest, _ := policy.Digest()
	att := ProxyAttestationV1{PolicyDigest: policyDigest, ProxyArtifactDigest: policy.ProxyArtifactDigest, ProxyIdentityDigest: policy.ProxyIdentityDigest,
		ResolvedHost: policy.Endpoint.Host, ResolvedPort: 443, ResolvedDNSClass: DNSPublicUnicastOnlyV1, ResolutionSetDigest: digest,
		ConnectionAddressPinned: true, DNSRebindingDenied: true, RedirectsDenied: true, AmbientProxyDenied: true, CertificateValidationRequired: true, ExpiresAt: expires}
	p.mu.Lock()
	defer p.mu.Unlock()
	now, validTime = p.sampleLocked()
	if p.state != 1 || op.Err() != nil || !validTime || !now.Before(expires) || secure.SetDeadline(expires) != nil {
		return ProxyAttestationV1{}, ErrProxyAttestation
	}
	p.conn, p.attestation, p.observedAt, p.state = secure, att, now, 2
	ok = true
	return att, nil
}

func (p *PinnedHTTPSProxyV1) Invoke(ctx context.Context, invocation ProxyInvocationV1) (result ProxyResultV1, returnedErr error) {
	return p.invoke(ctx, invocation, nil)
}

// beforeSend is never retained and is used only by the owned composition.
// Burn this concrete connection before that bounded current-authority join;
// a denial or lost acknowledgement cannot make this connection sendable again.
func (p *PinnedHTTPSProxyV1) invoke(ctx context.Context, invocation ProxyInvocationV1, beforeSend func(context.Context) error) (result ProxyResultV1, returnedErr error) {
	result.Acceptance = domain.ProviderAcceptanceUnknownV1
	if p == nil || p.self != p || ctx == nil {
		return result, ErrEgress
	}
	p.mu.Lock()
	now, validTime := p.sampleLocked()
	if p.state != 2 || (p.publicationAllocation != "" && beforeSend == nil) || ctx.Err() != nil || p.lifetime.Err() != nil || !validTime || !now.Before(p.attestation.ExpiresAt) ||
		invocation.Policy != p.policy || invocation.Attestation != p.attestation || uint64(len(invocation.Payload)) > p.policy.Endpoint.MaxRequestBytes || !pinnedCredential(invocation) {
		p.mu.Unlock()
		return result, ErrEgress
	}
	p.state = 3 // one-use before any physical write, including partial writes
	conn, life, policy := p.conn, p.lifetime, p.policy
	p.mu.Unlock()
	defer p.Close()
	op, cancel := context.WithTimeout(life, p.operationTimeout)
	defer cancel()
	stopCaller := context.AfterFunc(ctx, cancel)
	defer stopCaller()
	stop := context.AfterFunc(op, func() { _ = conn.Close() })
	defer stop()
	deadline, _ := op.Deadline()
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if conn.SetDeadline(deadline) != nil || op.Err() != nil || ctx.Err() != nil {
		return result, ErrEgress
	}
	if beforeSend != nil && beforeSend(op) != nil {
		return result, ErrEgress
	}
	if op.Err() != nil || ctx.Err() != nil {
		return result, ErrEgress
	}
	payload, secret := bytes.Clone(invocation.Payload), bytes.Clone(invocation.Secret)
	defer clear(payload)
	defer clear(secret)
	prefix := []byte("POST " + policy.Endpoint.Path + " HTTP/1.1\r\nHost: " + policy.Endpoint.Host + "\r\nAccept: application/json\r\nContent-Type: application/json\r\nX-OpenRouter-Metadata: enabled\r\nConnection: close\r\nContent-Length: " + strconv.Itoa(len(payload)) + "\r\n")
	// Allocate once before any secret enters the buffer, so append cannot leave
	// an abandoned secret-bearing backing array outside the clear boundary.
	request := make([]byte, 0, len(prefix)+len(secret)+len(payload)+26)
	request = append(request, prefix...)
	if len(secret) != 0 {
		request = append(request, []byte("Authorization: Bearer ")...)
		request = append(request, secret...)
		request = append(request, '\r', '\n')
	}
	request = append(request, '\r', '\n')
	request = append(request, payload...)
	defer clear(request)
	result.Route, result.RequestBytes, result.ObservedAt = policy.Route, uint64(len(payload)), now
	// tls.Conn.Write is called once; no retry, even on a short write.
	n, err := conn.Write(request)
	if err != nil || n != len(request) {
		return result, ErrEgress
	}
	body, err := boundedhttp.ReadOKResponse(conn, policy.Endpoint.MaxResponseBytes, pinnedMaxHeaders)
	if err != nil || op.Err() != nil || ctx.Err() != nil {
		clear(body)
		return result, ErrEgress
	}
	p.mu.Lock()
	now, validTime = p.sampleLocked()
	p.mu.Unlock()
	if !validTime || !now.Before(p.attestation.ExpiresAt) {
		clear(body)
		return result, ErrEgress
	}
	result.Response, result.ResponseBytes, result.ObservedAt = body, uint64(len(body)), now
	return result, nil // opaque HTTP 200 is not provider acceptance or model proof
}

func pinnedCredential(v ProxyInvocationV1) bool {
	m := v.Materialization
	if m.RootDir != "" || m.FilePath != "" || m.EnvironmentName != "" {
		return false
	}
	if pinnedFixtureRoute(v.Policy) {
		return m.Kind == domain.ProviderCredentialDeliveryNoneV1 && len(v.Secret) == 0
	}
	if m.Kind != domain.ProviderCredentialDeliveryDirectV1 || len(v.Secret) == 0 || len(v.Secret) > pinnedMaxSecret {
		return false
	}
	for _, b := range v.Secret {
		if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || strings.ContainsRune("-._~+/=", rune(b))) {
			return false
		}
	}
	return true
}

// Close releases only this object's connection, timer and cancellation callback.
// TCP Close is bounded and does not wait for TLS close_notify or body draining.
func (p *PinnedHTTPSProxyV1) Close() error {
	if p == nil || p.self != p {
		return nil
	}
	p.mu.Lock()
	p.state = 4
	conn, cancel, stop := p.rawConn, p.cancel, p.stopLifetime
	p.conn, p.rawConn, p.cancel, p.stopLifetime = nil, nil, nil, nil
	p.mu.Unlock()
	if stop != nil {
		stop()
	}
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
	return nil
}

func pinnedReviewedRoute(p PolicyV1) bool {
	r := p.Route
	return p.Endpoint.Scheme+"://"+p.Endpoint.Host+p.Endpoint.Path == directopenrouter.EndpointURLV1 &&
		r.BackendKind == domain.HarnessBackendDirectOpenRouterV1 && r.ModelVendorID == directopenrouter.ModelVendorIDV1 && r.ModelID == directopenrouter.ModelIDV1 &&
		r.TransportKind == domain.ProviderTransportRouterAPIV1 && r.TransportProvider == directopenrouter.TransportProviderV1 && r.UpstreamProviderID == directopenrouter.ProviderIDV1 &&
		r.EndpointID == directopenrouter.EndpointIDV1 && r.BillingKind == domain.ProviderBillingRouterAccountV1 && domain.ValidateOpaqueID("proxy.billing_authority", r.BillingAuthority) == nil
}

// This fixed test-only class is not a no-auth OpenRouter route. It is reachable
// only with the private dependencies of the tagged synthetic TLS fixture.
func pinnedFixtureRoute(p PolicyV1) bool {
	r := p.Route
	return p.Endpoint.Host == "fixture.sessionless.dev" && p.Endpoint.Path == "/owned-operation" && r.BackendKind == domain.HarnessBackendDeterministicFixtureV1 && r.ModelVendorID == "fixture-vendor" && r.TransportKind == domain.ProviderTransportDirectAPIV1 && r.TransportProvider == "deterministic-fixture" && r.UpstreamProviderID == "synthetic" && r.EndpointID == "owned-operation" && r.BillingKind == domain.ProviderBillingNoneV1 && domain.ValidateOpaqueID("fixture.resource", r.BillingAuthority) == nil
}

func pinnedPublicAddresses(addresses []netip.Addr) ([]string, string, error) {
	if len(addresses) == 0 || len(addresses) > 8 {
		return nil, "", ErrProxyAttestation
	}
	canonical := make([]string, 0, len(addresses))
	for _, a := range addresses {
		if !pinnedPublicAddress(a) {
			return nil, "", ErrProxyAttestation
		}
		canonical = append(canonical, a.String())
	}
	sort.Strings(canonical)
	canonical = compactPinnedAddresses(canonical)
	h := sha256.Sum256([]byte("sessionless.pinned-https-resolution.v1\x00" + strings.Join(canonical, "\x00")))
	return canonical, hex.EncodeToString(h[:]), nil
}

func compactPinnedAddresses(values []string) []string {
	n := 0
	for _, v := range values {
		if n == 0 || values[n-1] != v {
			values[n] = v
			n++
		}
	}
	return values[:n]
}

func pinnedPublicAddress(a netip.Addr) bool {
	if !a.IsValid() || a.Zone() != "" || a.Is4In6() || !a.IsGlobalUnicast() || a.IsPrivate() {
		return false
	}
	// Conservative deny of special-purpose/transition ranges. Only the current
	// IPv6 global allocation is admitted; unknown address classes fail closed.
	denied := []string{"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.31.196.0/24", "192.52.193.0/24", "192.88.99.0/24", "192.168.0.0/16", "192.175.48.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4", "2001::/23", "2001:db8::/32", "2002::/16", "2620:4f:8000::/48", "3fff::/20"}
	if a.Is6() && !netip.MustParsePrefix("2000::/3").Contains(a) {
		return false
	}
	for _, cidr := range denied {
		if netip.MustParsePrefix(cidr).Contains(a) {
			return false
		}
	}
	return true
}
