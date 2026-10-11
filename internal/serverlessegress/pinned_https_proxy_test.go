package serverlessegress

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/directopenrouter"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type pinnedTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *pinnedTestClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *pinnedTestClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type pinnedTLSFixture struct {
	p                        *PinnedHTTPSProxyV1
	policy                   PolicyV1
	clock                    *pinnedTestClock
	ctx                      context.Context
	cancel                   context.CancelFunc
	lookups, dials, requests atomic.Int32
	closed                   chan struct{}
}

type pinnedTestConn struct {
	net.Conn
	closed       chan struct{}
	once         sync.Once
	failWrite    atomic.Bool
	failedWrites atomic.Int32
}

func (c *pinnedTestConn) Write(p []byte) (int, error) {
	if c.failWrite.Swap(false) {
		c.failedWrites.Add(1)
		n, _ := c.Conn.Write(p[:min(5, len(p))])
		return n, io.ErrUnexpectedEOF
	}
	return c.Conn.Write(p)
}

func (c *pinnedTestConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closed) })
	return err
}

func pinnedTestPolicy() PolicyV1 {
	return PolicyV1{Version: 1, RoutePolicyDigest: digestFixture("pinned-route"), EffectivePolicyDigest: digestFixture("pinned-effective"),
		ProxyArtifactDigest: digestFixture("pinned-artifact"), ProxyIdentityDigest: digestFixture("pinned-identity"),
		Route: domain.ProviderRouteV1{BackendKind: domain.HarnessBackendDirectOpenRouterV1, ModelVendorID: directopenrouter.ModelVendorIDV1, ModelID: directopenrouter.ModelIDV1,
			TransportKind: domain.ProviderTransportRouterAPIV1, TransportProvider: directopenrouter.TransportProviderV1, UpstreamProviderID: directopenrouter.ProviderIDV1,
			EndpointID: directopenrouter.EndpointIDV1, BillingKind: domain.ProviderBillingRouterAccountV1, BillingAuthority: "synthetic-account"},
		Endpoint: EndpointPolicyV1{Scheme: "https", Host: "openrouter.ai", Port: 443, Path: "/api/v1/chat/completions", Method: "POST", DNSClass: DNSPublicUnicastOnlyV1,
			RedirectPolicy: RedirectDenyV1, MaxRequestBytes: 1024, MaxResponseBytes: 1024}}
}

func newPinnedTLSFixture(t *testing.T, certificateHost string, trusted bool, handler http.HandlerFunc) *pinnedTLSFixture {
	t.Helper()
	f := &pinnedTLSFixture{policy: pinnedTestPolicy(), clock: &pinnedTestClock{now: time.Now().UTC()}, closed: make(chan struct{})}
	f.ctx, f.cancel = context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(f.cancel)
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate synthetic TLS key: %v", err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: certificateHost}, DNSNames: []string{certificateHost},
		NotBefore: f.clock.Now().Add(-time.Hour), NotAfter: f.clock.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatalf("create synthetic TLS certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse synthetic certificate: %v", err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.requests.Add(1); handler(w, r) }))
	t.Cleanup(server.Close)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, NextProtos: []string{"http/1.1"}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	roots := x509.NewCertPool()
	if trusted {
		roots.AddCert(cert)
	}
	deps := pinnedHTTPSDependencies{roots: roots,
		lookup: func(ctx context.Context, host string) ([]netip.Addr, error) {
			f.lookups.Add(1)
			if host != f.policy.Endpoint.Host {
				return nil, errors.New("unexpected synthetic DNS host")
			}
			return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("1.1.1.1")}, nil
		},
		dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			f.dials.Add(1)
			if network != "tcp" || address != "1.1.1.1:443" {
				return nil, errors.New("unapproved synthetic dial")
			}
			conn, err := (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			if err != nil {
				return nil, err
			}
			return &pinnedTestConn{Conn: conn, closed: f.closed}, nil
		}}
	f.p, err = newPinnedHTTPSProxyV1(f.policy, f.clock, 30*time.Second, 60*time.Second, deps)
	if err != nil {
		t.Fatalf("construct pinned proxy: %v", err)
	}
	t.Cleanup(func() { _ = f.p.Close() })
	return f
}

func (f *pinnedTLSFixture) preflight(t *testing.T) ProxyAttestationV1 {
	t.Helper()
	a, err := f.p.Preflight(f.ctx, f.policy)
	if err != nil {
		t.Fatalf("synthetic real TLS preflight: %v", err)
	}
	if err := a.ValidateForPolicy(f.policy, f.clock.Now(), a.ExpiresAt); err != nil {
		t.Fatalf("validate actual attestation: %v", err)
	}
	if f.requests.Load() != 0 {
		t.Fatalf("preflight emitted %d HTTP payloads, want zero", f.requests.Load())
	}
	return a
}

func (f *pinnedTLSFixture) invocation(a ProxyAttestationV1) ProxyInvocationV1 {
	return ProxyInvocationV1{Policy: f.policy, Attestation: a, Materialization: ports.ProviderCredentialMaterializationV1{Kind: domain.ProviderCredentialDeliveryDirectV1},
		Secret: []byte("synthetic-bearer"), Payload: []byte(`{"opaque":"synthetic"}`)}
}

func TestPinnedHTTPSProxyOneOriginalTLSConnection(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("ALL_PROXY", "http://127.0.0.1:1")
	t.Setenv("OPENROUTER_API_KEY", "ambient-forbidden")
	var gotBody, gotAuth, gotHost, gotPath, gotAccept, gotMetadata string
	handlerDone := make(chan struct{})
	f := newPinnedTLSFixture(t, "openrouter.ai", true, func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		body, err := io.ReadAll(io.LimitReader(r.Body, 1025))
		if err != nil {
			t.Errorf("read request: %v", err)
		}
		gotBody, gotAuth, gotHost, gotPath = string(body), r.Header.Get("Authorization"), r.Host, r.URL.Path
		gotAccept, gotMetadata = r.Header.Get("Accept"), r.Header.Get("X-OpenRouter-Metadata")
		if r.Method != "POST" || r.Header.Get("Cookie") != "" || r.Header.Get("Accept-Encoding") != "" || r.TLS.ServerName != "openrouter.ai" {
			t.Errorf("unexpected method/ambient header/SNI: method=%q SNI=%q", r.Method, r.TLS.ServerName)
		}
		_, _ = w.Write([]byte("opaque-provider-body"))
	})
	a := f.preflight(t)
	// A changed DNS answer after preflight cannot redirect the original socket.
	f.p.deps.lookup = func(context.Context, string) ([]netip.Addr, error) {
		f.lookups.Add(1)
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	}
	v := f.invocation(a)
	result, err := f.p.Invoke(f.ctx, v)
	if err != nil {
		t.Fatalf("invoke original TLS connection: %v", err)
	}
	select {
	case <-handlerDone:
	case <-f.ctx.Done():
		t.Fatal("HTTP handler did not complete")
	}
	if string(result.Response) != "opaque-provider-body" || result.Acceptance != domain.ProviderAcceptanceUnknownV1 || result.Route != f.policy.Route {
		t.Errorf("opaque result=%+v, want fixed route, unknown acceptance and opaque body", result)
	}
	if gotBody != string(v.Payload) || gotAuth != "Bearer synthetic-bearer" || gotHost != "openrouter.ai" || gotPath != "/api/v1/chat/completions" || gotAccept != "application/json" || gotMetadata != "enabled" {
		t.Errorf("actual fixed request mismatch: host=%q path=%q accept=%q metadata=%q", gotHost, gotPath, gotAccept, gotMetadata)
	}
	if string(v.Secret) != "synthetic-bearer" || string(v.Payload) != gotBody {
		t.Error("proxy modified caller-owned secret or payload")
	}
	if _, err := f.p.Preflight(f.ctx, f.policy); !errors.Is(err, ErrProxyAttestation) {
		t.Errorf("repeat preflight error=%v, want denial", err)
	}
	if _, err := f.p.Invoke(f.ctx, v); !errors.Is(err, ErrEgress) {
		t.Errorf("repeat invocation error=%v, want denial", err)
	}
	if f.lookups.Load() != 1 || f.dials.Load() != 1 || f.requests.Load() != 1 {
		t.Errorf("effects lookups=%d dials=%d requests=%d, want 1/1/1", f.lookups.Load(), f.dials.Load(), f.requests.Load())
	}
}

func TestPinnedHTTPSProxyTLSValidation(t *testing.T) {
	for _, tc := range []struct {
		name, host string
		trusted    bool
	}{{"untrusted", "openrouter.ai", false}, {"wrong-host", "other.ai", true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPinnedTLSFixture(t, tc.host, tc.trusted, func(http.ResponseWriter, *http.Request) { t.Error("TLS denial reached HTTP") })
			if _, err := f.p.Preflight(f.ctx, f.policy); !errors.Is(err, ErrProxyAttestation) {
				t.Errorf("preflight error=%v, want TLS denial", err)
			}
			if _, err := f.p.Preflight(f.ctx, f.policy); !errors.Is(err, ErrProxyAttestation) {
				t.Errorf("repeated TLS preflight=%v, want burn", err)
			}
			if f.dials.Load() != 1 || f.requests.Load() != 0 {
				t.Errorf("TLS effects dials=%d HTTP=%d, want 1/0", f.dials.Load(), f.requests.Load())
			}
		})
	}
}

func TestPinnedHTTPSProxyDNSFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		addresses []string
	}{
		{"empty", nil}, {"mixed-private", []string{"8.8.8.8", "10.0.0.1"}}, {"loopback", []string{"127.0.0.1"}}, {"linklocal", []string{"169.254.169.254"}},
		{"multicast", []string{"224.0.0.1"}}, {"special", []string{"198.18.0.1"}}, {"unknown-ipv6", []string{"4000::1"}}, {"mapped", []string{"::ffff:8.8.8.8"}},
		{"too-many", []string{"8.8.8.8", "8.8.8.8", "8.8.8.8", "8.8.8.8", "8.8.8.8", "8.8.8.8", "8.8.8.8", "8.8.8.8", "8.8.8.8"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPinnedTLSFixture(t, "openrouter.ai", true, func(http.ResponseWriter, *http.Request) { t.Error("DNS denial reached HTTP") })
			f.p.deps.lookup = func(context.Context, string) ([]netip.Addr, error) {
				var a []netip.Addr
				for _, s := range tc.addresses {
					a = append(a, netip.MustParseAddr(s))
				}
				return a, nil
			}
			if _, err := f.p.Preflight(f.ctx, f.policy); !errors.Is(err, ErrProxyAttestation) {
				t.Errorf("DNS class error=%v, want denial for %v", err, tc.addresses)
			}
			if f.dials.Load() != 0 {
				t.Errorf("DNS denial caused %d dials, want zero", f.dials.Load())
			}
		})
	}
	left, ld, err := pinnedPublicAddresses([]netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("1.1.1.1")})
	_, rd, otherErr := pinnedPublicAddresses([]netip.Addr{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("1.1.1.1")})
	if err != nil || otherErr != nil || ld != rd || left[0] != "1.1.1.1" {
		t.Errorf("canonical resolution differs: digest=%q/%q errors=%v/%v", ld, rd, err, otherErr)
	}
}

func TestPinnedHTTPSProxyTamperingAndDeadlines(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*pinnedTLSFixture, *ProxyInvocationV1)
	}{
		{"attestation", func(_ *pinnedTLSFixture, v *ProxyInvocationV1) {
			v.Attestation.ResolutionSetDigest = digestFixture("counterfeit")
		}},
		{"expiry-copy", func(_ *pinnedTLSFixture, v *ProxyInvocationV1) {
			v.Attestation.ExpiresAt = v.Attestation.ExpiresAt.Add(time.Second)
		}},
		{"policy", func(_ *pinnedTLSFixture, v *ProxyInvocationV1) { v.Policy.Endpoint.Path = "/different" }},
		{"payload-cap", func(_ *pinnedTLSFixture, v *ProxyInvocationV1) { v.Payload = make([]byte, 1025) }},
		{"header-injection", func(_ *pinnedTLSFixture, v *ProxyInvocationV1) { v.Secret = []byte("bearer\r\nHost: attacker.ai") }},
		{"secret-cap", func(_ *pinnedTLSFixture, v *ProxyInvocationV1) { v.Secret = make([]byte, pinnedMaxSecret+1) }},
		{"no-auth", func(_ *pinnedTLSFixture, v *ProxyInvocationV1) {
			v.Materialization.Kind = domain.ProviderCredentialDeliveryNoneV1
			v.Secret = nil
		}},
		{"environment", func(_ *pinnedTLSFixture, v *ProxyInvocationV1) {
			v.Materialization.Kind = domain.ProviderCredentialDeliveryEnvironmentV1
			v.Materialization.EnvironmentName = "API_KEY"
		}},
		{"cancelled", func(f *pinnedTLSFixture, _ *ProxyInvocationV1) { f.cancel() }},
		{"at-expiry", func(f *pinnedTLSFixture, v *ProxyInvocationV1) {
			f.clock.advance(v.Attestation.ExpiresAt.Sub(f.clock.Now()))
		}},
		{"clock-backwards", func(f *pinnedTLSFixture, _ *ProxyInvocationV1) { f.clock.advance(-time.Second) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPinnedTLSFixture(t, "openrouter.ai", true, func(http.ResponseWriter, *http.Request) { t.Error("invalid invocation reached HTTP") })
			v := f.invocation(f.preflight(t))
			tc.mutate(f, &v)
			if _, err := f.p.Invoke(f.ctx, v); !errors.Is(err, ErrEgress) {
				t.Errorf("tampered invoke=%v, want denial", err)
			}
			if f.requests.Load() != 0 || f.dials.Load() != 1 {
				t.Errorf("denied effects requests=%d dials=%d, want 0/1", f.requests.Load(), f.dials.Load())
			}
		})
	}
}

func TestPinnedHTTPSProxyResponseBoundsAndNoRetry(t *testing.T) {
	for _, tc := range []struct{ name, response string }{
		{"redirect", "HTTP/1.1 302 Found\r\nLocation: https://attacker.ai/\r\nContent-Length: 0\r\n\r\n"},
		{"interim", "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"},
		{"bare-lf-status", "HTTP/1.1 200 OK\nContent-Length: 0\r\n\r\n"},
		{"large-header", "HTTP/1.1 200 OK\r\nX-Large: " + strings.Repeat("x", pinnedMaxHeaders) + "\r\n\r\n"},
		{"many-headers", "HTTP/1.1 200 OK\r\n" + strings.Repeat("X-A: x\r\n", 101) + "Content-Length: 0\r\n\r\n"},
		{"chunked-body", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n401\r\n" + strings.Repeat("x", 1025) + "\r\n0\r\n\r\n"},
		{"partial-response", "HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\npartial"},
		{"duplicate-length", "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx"},
		{"conflicting-length", "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx"},
		{"ambiguous-framing", "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n"},
		{"empty-header-name", "HTTP/1.1 200 OK\r\n: value\r\nContent-Length: 0\r\n\r\n"},
		{"spaced-transfer-encoding", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nTransfer-Encoding : chunked\r\n\r\n"},
		{"spaced-cookie", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nSet-Cookie : ambient=value\r\n\r\n"},
		{"spaced-content-encoding", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Encoding : gzip\r\n\r\n"},
		{"tabbed-header-name", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent\tEncoding: gzip\r\n\r\n"},
		{"non-ascii-header-name", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Encod\xffing: gzip\r\n\r\n"},
		{"encoding", "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n"},
		{"cookie", "HTTP/1.1 200 OK\r\nSet-Cookie: ambient=value\r\nContent-Length: 0\r\n\r\n"},
		{"challenge", "HTTP/1.1 200 OK\r\nWWW-Authenticate: Basic\r\nContent-Length: 0\r\n\r\n"},
		{"empty-then-encoding", "HTTP/1.1 200 OK\r\nContent-Encoding:\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n"},
		{"unannounced-trailer", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\nX-Trailer: " + strings.Repeat("x", pinnedMaxHeaders) + "\r\n\r\n"},
		{"announced-trailer", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Trailer\r\n\r\n0\r\nX-Trailer: value\r\n\r\n"},
		{"chunk-size-line", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" + strings.Repeat("0", 257) + "\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPinnedTLSFixture(t, "openrouter.ai", true, func(w http.ResponseWriter, _ *http.Request) {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Errorf("hijack synthetic TLS: %v", err)
					return
				}
				defer conn.Close()
				_, _ = io.WriteString(conn, tc.response)
			})
			v := f.invocation(f.preflight(t))
			result, err := f.p.Invoke(f.ctx, v)
			if !errors.Is(err, ErrEgress) || result.Acceptance != domain.ProviderAcceptanceUnknownV1 || len(result.Response) != 0 {
				t.Errorf("malformed response result=%+v error=%v, want unknown/no-body/denied", result, err)
			}
			_, _ = f.p.Invoke(f.ctx, v)
			if f.requests.Load() != 1 || f.dials.Load() != 1 {
				t.Errorf("failed-response effects requests=%d dials=%d, want 1/1", f.requests.Load(), f.dials.Load())
			}
		})
	}
}

func TestPinnedHTTPSProxyValueCopyAndAbandonedPreflight(t *testing.T) {
	f := newPinnedTLSFixture(t, "openrouter.ai", true, func(http.ResponseWriter, *http.Request) { t.Error("abandoned preflight reached HTTP") })
	v := f.invocation(f.preflight(t))
	// Use reflection so this intentional hostile copy does not violate vet's
	// prohibition on ordinary application copies of sync.Mutex values.
	var copied PinnedHTTPSProxyV1
	reflect.ValueOf(&copied).Elem().Set(reflect.ValueOf(f.p).Elem())
	if _, err := copied.Invoke(f.ctx, v); !errors.Is(err, ErrEgress) {
		t.Errorf("copied proxy invoke=%v, want denial", err)
	}
	_ = copied.Close()
	f.cancel()
	select {
	case <-f.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("abandoned preflight did not close original socket after cancellation")
	}
	if f.requests.Load() != 0 || f.dials.Load() != 1 {
		t.Errorf("abandoned effects requests=%d dials=%d, want 0/1", f.requests.Load(), f.dials.Load())
	}
}

func TestPinnedHTTPSProxyBoundedChunkedSuccess(t *testing.T) {
	f := newPinnedTLSFixture(t, "openrouter.ai", true, func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack chunked peer: %v", err)
			return
		}
		defer conn.Close()
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\none\r\n3\r\ntwo\r\n0\r\n\r\n")
	})
	result, err := f.p.Invoke(f.ctx, f.invocation(f.preflight(t)))
	if err != nil || string(result.Response) != "onetwo" || result.ResponseBytes != 6 || result.Acceptance != domain.ProviderAcceptanceUnknownV1 {
		t.Errorf("chunked positive result=%+v error=%v, want opaque 6 bytes and unknown", result, err)
	}
}

func TestPinnedHTTPSProxyPartialWriteRemainsUnknownAndConsumed(t *testing.T) {
	f := newPinnedTLSFixture(t, "openrouter.ai", true, func(http.ResponseWriter, *http.Request) { t.Error("partial TLS write unexpectedly completed HTTP") })
	v := f.invocation(f.preflight(t))
	f.p.mu.Lock()
	conn := f.p.rawConn.(*pinnedTestConn)
	f.p.mu.Unlock()
	conn.failWrite.Store(true)
	result, err := f.p.Invoke(f.ctx, v)
	if !errors.Is(err, ErrEgress) || result.Acceptance != domain.ProviderAcceptanceUnknownV1 || len(result.Response) != 0 {
		t.Errorf("partial write result=%+v err=%v, want unknown/no-body/error", result, err)
	}
	_, _ = f.p.Invoke(f.ctx, v)
	if conn.failedWrites.Load() != 1 || f.dials.Load() != 1 || f.lookups.Load() != 1 || f.requests.Load() != 0 {
		t.Errorf("partial-write effects writes=%d dials=%d DNS=%d requests=%d, want 1/1/1/0", conn.failedWrites.Load(), f.dials.Load(), f.lookups.Load(), f.requests.Load())
	}
}

func TestPinnedHTTPSProxyPreflightDenialBurnsAndDeadlineContained(t *testing.T) {
	f := newPinnedTLSFixture(t, "openrouter.ai", true, func(http.ResponseWriter, *http.Request) { t.Error("bad preflight reached HTTP") })
	wrong := f.policy
	wrong.ProxyIdentityDigest = digestFixture("changed")
	_, err := f.p.Preflight(f.ctx, wrong)
	if !errors.Is(err, ErrProxyAttestation) {
		t.Errorf("wrong policy preflight=%v, want denial", err)
	}
	_, err = f.p.Preflight(f.ctx, f.policy)
	if !errors.Is(err, ErrProxyAttestation) || f.lookups.Load() != 0 || f.dials.Load() != 0 {
		t.Errorf("denied retry=%v DNS=%d dial=%d, want burn before I/O", err, f.lookups.Load(), f.dials.Load())
	}
	g := newPinnedTLSFixture(t, "openrouter.ai", true, func(http.ResponseWriter, *http.Request) {})
	a := g.preflight(t)
	deadline, _ := g.ctx.Deadline()
	if a.ExpiresAt.After(deadline) {
		t.Errorf("attestation expiry=%v exceeds original context=%v", a.ExpiresAt, deadline)
	}
	if _, err := g.p.Preflight(g.ctx, g.policy); !errors.Is(err, ErrProxyAttestation) {
		t.Errorf("ready preflight repeat=%v, want denial", err)
	}
}

func TestPinnedHTTPSProxyConcurrentInvokeAndCancellation(t *testing.T) {
	started, peerDone := make(chan struct{}), make(chan struct{})
	f := newPinnedTLSFixture(t, "openrouter.ai", true, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack cancellation peer: %v", err)
			return
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		close(started)
		_, err = conn.Read(make([]byte, 1))
		if err == nil {
			t.Error("cancel peer received unexpected second payload")
		}
		close(peerDone)
	})
	v := f.invocation(f.preflight(t))
	ctx, cancel := context.WithCancel(f.ctx)
	t.Cleanup(cancel)
	type outcome struct {
		result ProxyResultV1
		err    error
	}
	done := make(chan outcome, 1)
	go func() { r, err := f.p.Invoke(ctx, v); done <- outcome{r, err} }()
	select {
	case <-started:
	case <-f.ctx.Done():
		t.Fatal("request never reached TLS peer")
	}
	if _, err := f.p.Invoke(f.ctx, v); !errors.Is(err, ErrEgress) {
		t.Errorf("concurrent invoke error=%v, want denial", err)
	}
	cancel()
	select {
	case got := <-done:
		if !errors.Is(got.err, ErrEgress) || got.result.Acceptance != domain.ProviderAcceptanceUnknownV1 {
			t.Errorf("cancel result=%+v err=%v", got.result, got.err)
		}
	case <-f.ctx.Done():
		t.Fatal("cancelled invocation did not return")
	}
	select {
	case <-peerDone:
	case <-f.ctx.Done():
		t.Fatal("cancellation did not close owned TLS connection")
	}
	if f.requests.Load() != 1 || f.dials.Load() != 1 {
		t.Errorf("concurrent effects requests=%d dials=%d, want 1/1", f.requests.Load(), f.dials.Load())
	}
	_ = f.p.Close()
	_ = f.p.Close()
	f.p.mu.Lock()
	defer f.p.mu.Unlock()
	if f.p.conn != nil || f.p.rawConn != nil || f.p.cancel != nil || f.p.stopLifetime != nil {
		t.Error("Close retained owned connection or callback")
	}
}

func TestPinnedHTTPSProxyConstructorClosedContract(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutate    func(*PolicyV1)
		op, total time.Duration
	}{
		{"request-overflow", func(p *PolicyV1) { p.Endpoint.MaxRequestBytes = ^uint64(0) }, time.Second, time.Second},
		{"response-overflow", func(p *PolicyV1) { p.Endpoint.MaxResponseBytes = pinnedMaxBody + 1 }, time.Second, time.Second},
		{"different-endpoint", func(p *PolicyV1) { p.Endpoint.Host = "attacker.ai" }, time.Second, time.Second},
		{"different-model", func(p *PolicyV1) { p.Route.ModelID = "unreviewed" }, time.Second, time.Second},
		{"zero-time", func(*PolicyV1) {}, 0, time.Second},
		{"operation-cap", func(*PolicyV1) {}, 31 * time.Second, 60 * time.Second},
		{"total-cap", func(*PolicyV1) {}, time.Second, 61 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := pinnedTestPolicy()
			tc.mutate(&p)
			if _, err := NewPinnedHTTPSProxyV1(p, &pinnedTestClock{now: time.Now().UTC()}, tc.op, tc.total); !errors.Is(err, ErrConfig) {
				t.Errorf("constructor=%v, want closed contract", err)
			}
		})
	}
}
