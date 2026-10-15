package providercredential

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// All certificates, keys, DNS results and peers here are synthetic test-owned
// fixtures. Trust uses only the synthetic private CA; no real provider,
// credential/config file or serving artifact is selected by these fixtures.
func piTLSFixture(t *testing.T) (piHTTPSRootArtifact, tls.Certificate) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Pi synthetic root"},
		NotBefore: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), NotAfter: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, root, root, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"fixture.invalid"},
		NotBefore: root.NotBefore, NotAfter: root.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, root, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	b := piHTTPSRootArtifact{count: 1}
	b.der[0] = der
	piTLSArtifactDigest(&b)
	return b, tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: key}
}

func piTLSArtifactDigest(b *piHTTPSRootArtifact) {
	h := sha256.New()
	_, _ = h.Write([]byte("Sessionless/Pi/HTTPS/Roots/1\x00"))
	_, _ = h.Write([]byte{b.count})
	for i := range int(b.count) {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(b.der[i])))
		_, _ = h.Write(n[:])
		_, _ = h.Write(b.der[i])
	}
	h.Sum(b.digest[:0])
}

func piTLSSink(t *testing.T, bundle piHTTPSRootArtifact, endpoint string, ledger *piFileLedger) *piHTTPS {
	t.Helper()
	headers, err := piCanonicalRequestHeaders([]byte(piAuthFixtureHeaders), 512)
	if err != nil {
		t.Fatal(err)
	}
	a := &piRequestAuthentication{endpoint: endpoint, headers: headers, requestHeaders: 4096}
	p, err := newPiHTTPS(ledger, a, []byte(`{"fixture":true}`), 64, bundle)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.clear)
	return p
}

func piTLSWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(20 * time.Second) // failure bound, never readiness
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
		select {
		case <-ch:
		default:
			t.Fatal("test-owned TLS operation failed to join")
		}
	}
}

type piTLSPeer struct {
	done         chan struct{}
	requestReady chan struct{}
	request      []byte // only read after done; synthetic key allowed in test memory
	err          error
	dials        atomic.Int32
}

func piTLSPeerStart(t *testing.T, p *piHTTPS, cert tls.Certificate, alpn []string, response string, hold ...<-chan struct{}) *piTLSPeer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &piTLSPeer{done: make(chan struct{}), requestReady: make(chan struct{})}
	var mu sync.Mutex
	var active net.Conn
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		mu.Lock()
		if active != nil {
			_ = active.Close()
		}
		mu.Unlock()
		piTLSWait(t, peer.done)
	})
	expected := p.prefixN + len("synthetic-key") + 4 + len(p.body)
	go func() {
		defer close(peer.done)
		raw, err := listener.Accept()
		if err != nil {
			peer.err = err
			return
		}
		mu.Lock()
		active = raw
		if ctx.Err() != nil {
			_ = raw.Close()
		}
		mu.Unlock()
		defer raw.Close()
		_ = raw.SetDeadline(time.Now().Add(20 * time.Second))
		conn := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: alpn, MinVersion: tls.VersionTLS12})
		if peer.err = conn.HandshakeContext(ctx); peer.err != nil {
			return
		}
		buffer := make([]byte, expected)
		n, err := io.ReadFull(conn, buffer)
		peer.request, peer.err = buffer[:n], err
		if err != nil {
			return
		}
		close(peer.requestReady)
		if len(hold) != 0 {
			select {
			case <-hold[0]:
			case <-ctx.Done():
				return
			}
		}
		_, peer.err = io.WriteString(conn, response)
		_ = conn.Close()
	}()
	p.lookup = func(ctx context.Context, network, host string) ([]netip.Addr, error) {
		if ctx.Err() != nil || network != "ip" || host != p.host {
			return nil, ErrInvalid
		}
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	}
	p.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		peer.dials.Add(1)
		if network != "tcp" || address != "8.8.8.8:443" {
			return nil, ErrInvalid
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}
	return peer
}

func TestPiHTTPSCancellationClosesActualBlockedResponse(t *testing.T) {
	bundle, cert := piTLSFixture(t)
	p := piTLSSink(t, bundle, "https://fixture.invalid/", newPiFileLedger())
	hold := make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(release)
	peer := piTLSPeerStart(t, p, cert, []string{"http/1.1"}, piHTTPFixture("abc", false), hold)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan struct{})
	var result piUnapprovedHTTPView
	var callErr error
	r := piHTTPReceiver(t, 512, 64)
	t.Cleanup(func() { cancel(); release(); piTLSWait(t, done) })
	go func() {
		defer close(done)
		result, callErr = p.receive(ctx, time.Now().Add(time.Hour), []byte("synthetic-key"), r)
	}()
	piTLSWait(t, peer.requestReady) // actual request consumed, response held
	select {
	case <-done:
		t.Fatal("receiver returned before peer response/EOF or cancellation")
	default:
	}
	cancel()
	piTLSWait(t, done) // socket Close, not the server release, must unblock Read
	if callErr == nil || result.entity != nil || !bytes.Equal(p.wire, make([]byte, len(p.wire))) || !bytes.Equal(r.entity, make([]byte, len(r.entity))) {
		t.Fatal("cancel exposed response or retained secret buffer", callErr)
	}
	release()
	piTLSWait(t, peer.done)
}

func TestPiHTTPSCancellationRejectsLateSocketAndHeldTLS(t *testing.T) {
	for _, mode := range []string{"held DNS", "late dial", "held TLS"} {
		t.Run(mode, func(t *testing.T) {
			bundle, _ := piTLSFixture(t)
			p := piTLSSink(t, bundle, "https://fixture.invalid/", newPiFileLedger())
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			entered, release := make(chan struct{}), make(chan struct{})
			unhold := sync.OnceFunc(func() { close(release) })
			client, server := net.Pipe()
			t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
			p.lookup = func(context.Context, string, string) ([]netip.Addr, error) {
				if mode == "held DNS" {
					close(entered)
					<-release
				}
				return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
			}
			dials := 0
			p.dial = func(context.Context, string, string) (net.Conn, error) {
				dials++
				if mode == "late dial" {
					close(entered)
					<-release // ignores cancellation until late publication attempt
				}
				return client, nil
			}
			done := make(chan struct{})
			var callErr error
			r := piHTTPReceiver(t, 512, 64)
			t.Cleanup(func() { cancel(); unhold(); piTLSWait(t, done) })
			go func() {
				defer close(done)
				_, callErr = p.receive(ctx, time.Now().Add(time.Hour), []byte("synthetic-key"), r)
			}()
			if mode == "held TLS" {
				// Actual ClientHello bytes prove handshake reached the socket.
				_ = server.SetReadDeadline(time.Now().Add(20 * time.Second))
				if _, err := io.ReadFull(server, make([]byte, 5)); err != nil {
					t.Fatal("no ClientHello header", err)
				}
			} else {
				piTLSWait(t, entered)
			}
			cancel()
			unhold()
			piTLSWait(t, done)
			if callErr == nil || (dials == 0) != (mode == "held DNS") {
				t.Fatal("cancel did not stop DNS/dial/TLS", callErr, dials)
			}
			if mode != "held DNS" {
				_ = server.SetReadDeadline(time.Now().Add(20 * time.Second))
				if n, err := server.Read(make([]byte, 1)); n != 0 || err != io.EOF {
					t.Fatal("late/held socket remained open", n, err)
				}
			}
		})
	}
}

type piTLSShortRecordConn struct {
	net.Conn
	applicationWrites atomic.Int32
}

func (c *piTLSShortRecordConn) Write(p []byte) (int, error) {
	// Tests pin TLS1.2 so application records are distinguishable from handshake
	// records. The actual socket accepts only a prefix, then reports failure.
	if len(p) > 5 && p[0] == 23 {
		c.applicationWrites.Add(1)
		n, _ := c.Conn.Write(p[:len(p)/2])
		return n, io.ErrUnexpectedEOF
	}
	return c.Conn.Write(p)
}

func TestPiHTTPSActualPartialWriteBurnsWithoutRetry(t *testing.T) {
	bundle, cert := piTLSFixture(t)
	p := piTLSSink(t, bundle, "https://fixture.invalid/", newPiFileLedger())
	p.config.MaxVersion = tls.VersionTLS12 // synthetic partial-record observation
	peer := piTLSPeerStart(t, p, cert, []string{"http/1.1"}, piHTTPFixture("abc", false))
	dial := p.dial
	var raw *piTLSShortRecordConn
	p.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		raw = &piTLSShortRecordConn{Conn: conn}
		return raw, nil
	}
	out, err := p.receive(t.Context(), time.Now().Add(time.Hour), []byte("synthetic-key"), piHTTPReceiver(t, 512, 64))
	piTLSWait(t, peer.done)
	if err == nil || out.entity != nil || raw == nil || raw.applicationWrites.Load() != 1 || peer.dials.Load() != 1 || !bytes.Equal(p.wire, make([]byte, len(p.wire))) {
		t.Fatal("ambiguous partial request did not burn/zero exact single connection", err)
	}
}

func TestPiHTTPSActualTLSUnapprovedResponseAndExactRequest(t *testing.T) {
	bundle, cert := piTLSFixture(t)
	p := piTLSSink(t, bundle, "https://fixture.invalid/v1/responses", newPiFileLedger())
	// Caller-owned artifact bytes must not alias the private parsed CA/DER.
	clear(bundle.der[0])
	const entity = "data: {\"synthetic\":true}\n\n"
	peer := piTLSPeerStart(t, p, cert, []string{"http/1.1"}, piHTTPFixture(entity, true))
	r := piHTTPReceiver(t, 512, 256)
	out, err := p.receive(t.Context(), time.Now().Add(time.Hour), []byte("synthetic-key"), r)
	piTLSWait(t, peer.done)
	if err != nil || peer.err != nil || string(out.entity) != entity || peer.dials.Load() != 1 {
		t.Fatalf("TLS framing DATA: err=%v peer=%v dials=%d", err, peer.err, peer.dials.Load())
	}
	request := string(peer.request)
	for _, expected := range []string{"POST /v1/responses HTTP/1.1\r\n", "Host: fixture.invalid\r\n", "Connection: close\r\n", "Accept-Encoding: identity\r\n", "Content-Length: 16\r\n", "Authorization: Bearer synthetic-key\r\n\r\n{\"fixture\":true}"} {
		if !strings.Contains(request, expected) {
			t.Errorf("request missing fixed field %q", expected)
		}
	}
	if strings.Count(request, "Authorization:") != 1 || !bytes.Equal(p.wire, make([]byte, len(p.wire))) {
		t.Error("duplicate authorization or retained secret request arena")
	}
	if out, err := p.receive(t.Context(), time.Now().Add(time.Hour), []byte("synthetic-key"), r); !errors.Is(err, ErrConsumed) || out.entity != nil || peer.dials.Load() != 1 {
		t.Fatal("second call replenished physical network use", err)
	}
}

func TestPiHTTPSVerifiedPeerAndFramingDenials(t *testing.T) {
	for _, mode := range []string{"wrong host", "wrong root", "absent ALPN", "wrong ALPN", "redirect", "compression", "truncated", "extra"} {
		t.Run(mode, func(t *testing.T) {
			bundle, cert := piTLSFixture(t)
			endpoint, alpn := "https://fixture.invalid/v1/responses", []string{"http/1.1"}
			wire := piHTTPFixture("abc", false)
			switch mode {
			case "wrong host":
				endpoint = "https://other.invalid/v1/responses"
			case "wrong root":
				bundle, _ = piTLSFixture(t)
			case "absent ALPN":
				alpn = nil
			case "wrong ALPN":
				alpn = []string{"h2"}
			case "redirect":
				wire = strings.Replace(wire, "200 OK", "302 Found", 1)
			case "compression":
				wire = strings.Replace(wire, "Connection: close", "Content-Encoding: gzip", 1)
			case "truncated":
				wire = wire[:len(wire)-1]
			case "extra":
				wire += "X"
			}
			p := piTLSSink(t, bundle, endpoint, newPiFileLedger())
			peer := piTLSPeerStart(t, p, cert, alpn, wire)
			out, err := p.receive(t.Context(), time.Now().Add(time.Hour), []byte("synthetic-key"), piHTTPReceiver(t, 512, 256))
			piTLSWait(t, peer.done)
			if err == nil || out.entity != nil || peer.dials.Load() != 1 || !bytes.Equal(p.wire, make([]byte, len(p.wire))) {
				t.Fatal("failed peer/framing exposed DATA, retried or retained request", err)
			}
			if strings.Contains(mode, "ALPN") || mode == "wrong host" || mode == "wrong root" {
				if len(peer.request) != 0 {
					t.Error("secret request was written before TLS peer/ALPN validation")
				}
			}
		})
	}
}

func TestPiHTTPSAddressAndEndpointDenials(t *testing.T) {
	bundle, _ := piTLSFixture(t)
	headers, _ := piCanonicalRequestHeaders([]byte(piAuthFixtureHeaders), 512)
	for _, endpoint := range []string{"http://fixture.invalid/", "https://127.0.0.1/", "https://[::1]/", "https://fixture.invalid:444/", "https://fixture.invalid:/", "https://fixture.invalid/?x=1", "https://fixture.invalid/?", "https://fixture.invalid/#f", "https://u@fixture.invalid/", "https://FIXTURE.invalid/", "https://fixture.invalid./", "https://-bad.invalid/", "https://a..invalid/"} {
		if p, err := newPiHTTPS(newPiFileLedger(), &piRequestAuthentication{endpoint: endpoint, headers: headers, requestHeaders: 4096}, []byte("x"), 64, bundle); err == nil || p != nil {
			t.Errorf("endpoint admitted %q", endpoint)
		}
	}
	for _, raw := range []string{"0.0.0.0", "10.1.2.3", "100.64.0.1", "127.0.0.1", "169.254.1.1", "172.16.0.1", "192.168.1.1", "192.0.2.1", "198.18.0.1", "198.51.100.1", "203.0.113.1", "224.0.0.1", "255.255.255.255", "::1", "::ffff:8.8.8.8", "fc00::1", "fe80::1%en0", "2001:db8::1", "2002::1", "3fff::1"} {
		if piHTTPSPublicAddress(netip.MustParseAddr(raw)) {
			t.Errorf("special address admitted %s", raw)
		}
	}
	for _, raw := range []string{"8.8.8.8", "1.1.1.1", "2001:4860:4860::8888"} {
		if !piHTTPSPublicAddress(netip.MustParseAddr(raw)) {
			t.Errorf("public address denied %s", raw)
		}
	}
	for _, mode := range []string{"empty", "mixed", "nine", "lookup error", "dial error"} {
		t.Run(mode, func(t *testing.T) {
			p := piTLSSink(t, bundle, "https://fixture.invalid/", newPiFileLedger())
			lookup, dial := 0, 0
			p.lookup = func(context.Context, string, string) ([]netip.Addr, error) {
				lookup++
				var addresses []netip.Addr
				switch mode {
				case "mixed":
					addresses = []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("127.0.0.1")}
				case "nine":
					addresses = make([]netip.Addr, 9)
				case "lookup error":
					return nil, io.ErrUnexpectedEOF
				case "dial error":
					addresses = []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("1.1.1.1")}
				}
				return addresses, nil
			}
			p.dial = func(_ context.Context, _, address string) (net.Conn, error) {
				dial++
				if address != "1.1.1.1:443" {
					t.Error("noncanonical selected literal", address)
				}
				return nil, io.ErrUnexpectedEOF
			}
			if out, err := p.receive(t.Context(), time.Now().Add(time.Hour), []byte("synthetic-key"), piHTTPReceiver(t, 512, 64)); err == nil || out.entity != nil || lookup != 1 || (dial == 1) != (mode == "dial error") {
				t.Fatalf("resolution/dial policy: err=%v lookup=%d dial=%d", err, lookup, dial)
			}
		})
	}
}

func TestPiHTTPSFundingAndStrictRootArtifact(t *testing.T) {
	bundle, _ := piTLSFixture(t)
	headers, _ := piCanonicalRequestHeaders([]byte(piAuthFixtureHeaders), 512)
	a := &piRequestAuthentication{endpoint: "https://fixture.invalid/", headers: headers, requestHeaders: 4096}
	baseline := newPiFileLedger()
	p, err := newPiHTTPS(baseline, a, []byte("x"), 64, bundle)
	if err != nil {
		t.Fatal(err)
	}
	p.clear()
	for _, mode := range []string{"exact", "work short", "live short", "nodes short"} {
		t.Run(mode, func(t *testing.T) {
			ledger := &piFileLedger{maxCopy: baseline.copy, maxLive: baseline.live, maxNodes: baseline.nodes}
			switch mode {
			case "work short":
				ledger.maxCopy--
			case "live short":
				ledger.maxLive--
			case "nodes short":
				ledger.maxNodes--
			}
			p, err := newPiHTTPS(ledger, a, []byte("x"), 64, bundle)
			if mode != "exact" {
				if err == nil || p != nil || ledger.copy != 0 || ledger.live != 0 || ledger.nodes != 0 {
					t.Fatal("unfunded root/request allocation", err)
				}
				return
			}
			if err != nil || p.live != ledger.live {
				t.Fatal("exact debit", err)
			}
			live := p.live
			p.clear()
			if ledger.EndLive(live) != nil || ledger.live != 0 || ledger.copy != baseline.copy || ledger.nodes != baseline.nodes {
				t.Fatal("refund replenished cumulative work/nodes")
			}
		})
	}
	for _, mode := range []string{"empty", "count", "oversize", "digest", "malformed", "duplicate", "trailing DER", "extra slot"} {
		t.Run(mode, func(t *testing.T) {
			b := bundle
			switch mode {
			case "empty":
				b.count = 0
			case "count":
				b.count = 5
			case "oversize":
				b.der[0] = make([]byte, 8193)
			case "digest":
				b.digest[0] ^= 1
			case "malformed":
				b.der[0] = []byte("not DER")
			case "duplicate":
				b.count, b.der[1] = 2, b.der[0]
				piTLSArtifactDigest(&b)
			case "trailing DER":
				b.der[0] = append(bytes.Clone(b.der[0]), 0)
				piTLSArtifactDigest(&b)
			case "extra slot":
				b.der[1] = b.der[0]
			}
			ledger := newPiFileLedger()
			if p, err := newPiHTTPS(ledger, a, []byte("x"), 64, b); err == nil || p != nil || ledger.live != 0 {
				t.Fatal("invalid roots accepted or failed allocation retained", err)
			}
		})
	}
}

func TestPiHTTPSInheritedPostAuthHeaderCeiling(t *testing.T) {
	bundle, _ := piTLSFixture(t)
	headers, _ := piCanonicalRequestHeaders([]byte(piAuthFixtureHeaders), 512)
	// Independent literal fixture: eleven HTTP name+value+4 totals307, although
	// canonical SPU3 headers occupy331 bytes. Authorization adds13+7+13+4=37.
	const completeSDKHeaders = 307 + 13 + 7 + 13 + 4
	for _, delta := range []int{0, -1} {
		ledger := newPiFileLedger()
		a := &piRequestAuthentication{endpoint: "https://fixture.invalid/", headers: headers, requestHeaders: uint64(completeSDKHeaders + delta)}
		p, err := newPiHTTPS(ledger, a, []byte("x"), 13, bundle)
		if delta < 0 {
			if err == nil || p != nil || ledger.live != 0 {
				t.Fatal("canonical nonsecret fit bypassed complete post-auth ceiling", err)
			}
			continue
		}
		if err != nil || p.sdkHeaderBytes != 307 || !p.admitsKey([]byte("synthetic-key")) || p.admitsKey([]byte("synthetic-keyX")) {
			t.Fatal("exact post-auth/key bound", err)
		}
		// A forged/stale smaller original ceiling must deny before DNS, not just
		// because a late write happens to run out of the larger wire arena.
		p.requestHeaders--
		lookups := 0
		p.lookup = func(context.Context, string, string) ([]netip.Addr, error) { lookups++; return nil, ErrBackend }
		out, err := p.receive(t.Context(), time.Now().Add(time.Hour), []byte("synthetic-key"), piHTTPReceiver(t, 512, 64))
		if err == nil || out.entity != nil || lookups != 0 {
			t.Fatal("first-over actual post-auth bytes reached DNS", err, lookups)
		}
		p.clear()
	}
}

type piTLSRefundFailure struct{ *piFileLedger }

func (*piTLSRefundFailure) EndLive(uint64) error { return ErrBackend }

func TestPiHTTPSFailedPreparationRetainsFailedRetirementDebit(t *testing.T) {
	bundle, _ := piTLSFixture(t)
	bundle.digest[0] ^= 1
	headers, _ := piCanonicalRequestHeaders([]byte(piAuthFixtureHeaders), 512)
	ledger := &piTLSRefundFailure{newPiFileLedger()}
	p, err := newPiHTTPS(ledger, &piRequestAuthentication{endpoint: "https://fixture.invalid/", headers: headers, requestHeaders: 4096}, []byte("x"), 64, bundle)
	if err == nil || p == nil || p.live == 0 || p.live != ledger.live || p.config != nil || p.wire != nil || p.roots != nil {
		t.Fatal("failed refund lost ownership of the original live debit", err)
	}
}
