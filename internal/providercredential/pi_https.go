package providercredential

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"net"
	"net/netip"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

// Private source-artifact DATA. No exported constructor accepts roots, a dialer
// or a certificate-pool pointer. Installed artifact selection and complete
// TLS/backend allocation qualification are STILL MISSING; tests of these
// mechanics cannot make the original prepared owner eligible.
type piHTTPSRootArtifact struct {
	der    [4][]byte
	count  uint8
	digest [32]byte
}

type piHTTPS struct {
	mu             sync.Mutex // socket publication and withdrawal only; never held over IO
	conn           net.Conn
	stopped        bool
	spent          atomic.Bool
	host           string
	target         string
	wire           []byte // entire request, including private Bearer; always zero after IO
	prefixN        int
	keyLimit       int
	requestHeaders uint64
	sdkHeaderBytes uint64
	body           []byte // original owner retains this alias until all borrowers join
	roots          *x509.CertPool
	der            [4][]byte
	config         *tls.Config
	live           uint64
	lookup         func(context.Context, string, string) ([]netip.Addr, error)
	dial           func(context.Context, string, string) (net.Conn, error)
}

func (*piHTTPS) String() string     { return "piHTTPS{private:[redacted]}" }
func (p *piHTTPS) GoString() string { return p.String() }

// newPiHTTPS is a private, pre-Issue mechanism, NOT serving root selection or
// runtime approval. Bundle DER is copied and strictly parsed before publication;
// AddCert retains the already parsed certificate (no lazy PEM reparse).
func newPiHTTPS(ledger attachedworkerdaemon.PreparedAPILedgerV1, a *piRequestAuthentication, body []byte, keyLimit int64, bundle piHTTPSRootArtifact) (out *piHTTPS, err error) {
	if nilPiLedger(ledger) || a == nil || len(a.endpoint) > 2048 || len(a.headers) > 1<<20 || a.requestHeaders == 0 || a.requestHeaders > 1<<20 || len(body) == 0 || len(body) > 1<<20 || keyLimit < 1 || keyLimit > 64<<10 || bundle.count == 0 || bundle.count > 4 {
		return nil, ErrInvalid
	}
	var rootBytes uint64
	for i := range bundle.der {
		n := len(bundle.der[i])
		if i >= int(bundle.count) {
			if n != 0 {
				return nil, ErrInvalid
			}
		} else if n == 0 || n > 8192 {
			return nil, ErrInvalid
		}
		rootBytes += uint64(n)
	}
	// Bounded application payload/layout and conservative parser workspace.
	// These debits do NOT certify Go TLS/DNS/certificate verification allocation,
	// roots policy identity, backend/toolchain or installed native/model fit.
	wireBytes := uint64(len(body)+len(a.headers)+len(a.endpoint)*2) + uint64(keyLimit) + 1024
	live := wireBytes + 64*rootBytes + 65536 + uint64(reflect.TypeOf(piHTTPS{}).Size())
	work := 8*wireBytes + 128*rootBytes + 4*rootBytes*rootBytes
	if ledger.Reserve(work, live, uint64(bundle.count)*4+8) != nil {
		return nil, ErrInvalid
	}
	p := &piHTTPS{wire: make([]byte, int(wireBytes)), body: body, live: live, keyLimit: int(keyLimit), requestHeaders: a.requestHeaders}
	defer func() {
		if err != nil {
			p.clear()
			if ledger.EndLive(live) != nil {
				// Preserve ownership of a failed retirement debit, never lose it
				// behind a nil result. This cleared state cannot execute IO.
				out, err = p, ErrInvalid
			}
		}
	}()
	u, e := url.Parse(a.endpoint)
	if e != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.String() != a.endpoint || (u.Port() != "" && u.Port() != "443") || !piHTTPSHostname(u.Hostname()) || (u.Host != u.Hostname() && u.Host != u.Hostname()+":443") {
		return nil, ErrInvalid
	}
	p.host, p.target = u.Hostname(), u.EscapedPath()
	if p.target == "" {
		p.target = "/"
	}
	for _, c := range []byte(p.target) {
		if c < 33 || c > 126 || c == '?' || c == '#' {
			return nil, ErrInvalid
		}
	}
	p.roots = x509.NewCertPool() // never SystemCertPool, host PEM, nil or fallback roots
	h := sha256.New()
	_, _ = h.Write([]byte("Sessionless/Pi/HTTPS/Roots/1\x00"))
	_, _ = h.Write([]byte{bundle.count})
	for i := range int(bundle.count) {
		p.der[i] = slices.Clone(bundle.der[i])
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(p.der[i])))
		_, _ = h.Write(n[:])
		_, _ = h.Write(p.der[i])
		for j := 0; j < i; j++ {
			if slices.Equal(p.der[i], p.der[j]) {
				return nil, ErrInvalid
			}
		}
		cert, parseErr := x509.ParseCertificate(p.der[i])
		if parseErr != nil || !cert.IsCA || !cert.BasicConstraintsValid || cert.KeyUsage&x509.KeyUsageCertSign == 0 || cert.CheckSignatureFrom(cert) != nil {
			return nil, ErrInvalid
		}
		p.roots.AddCert(cert)
	}
	var digest [32]byte
	h.Sum(digest[:0])
	if digest != bundle.digest {
		return nil, ErrInvalid
	}
	p.config = &tls.Config{RootCAs: p.roots, ServerName: p.host, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS13,
		NextProtos: []string{"http/1.1"}, SessionTicketsDisabled: true, CurvePreferences: []tls.CurveID{tls.X25519, tls.CurveP256}}
	p.lookup = net.DefaultResolver.LookupNetIP
	dialer := &net.Dialer{}
	p.dial = dialer.DialContext
	// Build only nonsecret fields before Issue. All post-Issue appends use the
	// exact preallocated arena; no fmt/string conversion ever sees the key.
	n := copy(p.wire, "POST ")
	n += copy(p.wire[n:], p.target)
	n += copy(p.wire[n:], " HTTP/1.1\r\nHost: ")
	n += copy(p.wire[n:], u.Host)
	n += copy(p.wire[n:], "\r\nConnection: close\r\nAccept-Encoding: identity\r\nContent-Length: ")
	n += copy(p.wire[n:], strconv.Itoa(len(body)))
	n += copy(p.wire[n:], "\r\n")
	if len(a.headers) < 2 || binary.BigEndian.Uint16(a.headers[:2]) != 11 {
		return nil, ErrInvalid
	}
	at := 2
	for _, name := range piAuthHeaderNames {
		if len(a.headers)-at < 2 || int(binary.BigEndian.Uint16(a.headers[at:at+2])) != len(name) {
			return nil, ErrInvalid
		}
		at += 2
		if len(a.headers)-at < len(name)+4 || string(a.headers[at:at+len(name)]) != name {
			return nil, ErrInvalid
		}
		at += len(name)
		size := uint64(binary.BigEndian.Uint32(a.headers[at : at+4]))
		at += 4
		if size > uint64(len(a.headers)-at) || !piHeaderValue(string(a.headers[at:at+int(size)])) {
			return nil, ErrInvalid
		}
		n += copy(p.wire[n:], name)
		n += copy(p.wire[n:], ": ")
		n += copy(p.wire[n:], a.headers[at:at+int(size)])
		n += copy(p.wire[n:], "\r\n")
		p.sdkHeaderBytes += uint64(len(name)) + size + 4
		at += int(size)
	}
	if at != len(a.headers) {
		return nil, ErrInvalid
	}
	// Same post-auth SDK ceiling as the native guard: name+value+4 for ALL
	// SDK headers including authorization. A larger parent wire arena cannot
	// raise it. Host/framing have their separate prepaid parent-only bound.
	if p.sdkHeaderBytes+uint64(len("authorization")+len("Bearer ")+4)+uint64(keyLimit) > p.requestHeaders {
		return nil, ErrInvalid
	}
	n += copy(p.wire[n:], "Authorization: Bearer ")
	p.prefixN = n
	if n+int(keyLimit)+4+len(body) > len(p.wire) {
		return nil, ErrInvalid
	}
	return p, nil
}

func (p *piHTTPS) admitsKey(key []byte) bool {
	if len(key) == 0 || len(key) > p.keyLimit || p.sdkHeaderBytes+uint64(len("authorization")+len("Bearer ")+4+len(key)) > p.requestHeaders {
		return false
	}
	for _, c := range key {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

func piHTTPSHostname(host string) bool {
	if len(host) == 0 || len(host) > 253 || strings.ToLower(host) != host || !strings.Contains(host, ".") || strings.HasSuffix(host, ".") {
		return false
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return false
	}
	start := 0
	for i := 0; i <= len(host); i++ {
		if i == len(host) || host[i] == '.' {
			if i-start < 1 || i-start > 63 || host[start] == '-' || host[i-1] == '-' {
				return false
			}
			start = i + 1
		} else if c := host[i]; (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

var piHTTPSSpecialPrefixes = [...]netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("192.31.196.0/24"), netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("192.175.48.0/24"), netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("2620:4f:8000::/48"), netip.MustParsePrefix("3fff::/20"),
}

func piHTTPSPublicAddress(a netip.Addr) bool {
	if !a.IsValid() || a.Zone() != "" || a.Is4In6() || !a.IsGlobalUnicast() || a.IsPrivate() || (a.Is6() && !netip.MustParsePrefix("2000::/3").Contains(a)) {
		return false
	}
	for _, prefix := range piHTTPSSpecialPrefixes {
		if prefix.Contains(a) {
			return false
		}
	}
	return true
}

// withdraw closes the actual descriptor before any owner waits for a gate,
// Source or network join. A dial that returns late cannot republish a socket.
func (p *piHTTPS) withdraw() {
	p.mu.Lock()
	p.stopped = true
	c := p.conn
	p.conn = nil
	p.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

func (p *piHTTPS) publish(c net.Conn, ctx context.Context) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || ctx.Err() != nil {
		_ = c.Close()
		return false
	}
	p.conn = c
	return true
}

// receive is private unapproved framing DATA, NOT semantic quarantine, replay
// or custody. The original file consumer must retain its genuine use gate and
// secret through this call AND subsequent guards/borrow joins. No public caller
// can select this sink, supply a key or obtain this response.
func (p *piHTTPS) receive(ctx context.Context, f time.Time, key []byte, receiver *piHTTPReceive) (piUnapprovedHTTPView, error) {
	if p == nil || !p.spent.CompareAndSwap(false, true) {
		return piUnapprovedHTTPView{}, ErrConsumed
	}
	defer clear(p.wire)
	if ctx == nil || ctx.Err() != nil || f == f.Round(0) || !time.Now().Before(f) || receiver == nil || p.config == nil || !p.admitsKey(key) || p.prefixN+len(key)+4+len(p.body) > len(p.wire) {
		return piUnapprovedHTTPView{}, ErrInvalid
	}
	// The original Source owner has ALREADY armed F. Do not construct/renew a
	// transport timer: its exact context cancels DNS/dial/IO at that same F.
	bounded := ctx
	done := make(chan struct{})
	stop := context.AfterFunc(bounded, func() { defer close(done); p.withdraw() })
	defer func() {
		p.withdraw()
		if !stop() {
			<-done // Stop is not a join when the callback has already started.
		}
	}()
	addresses, err := p.lookup(bounded, "ip", p.host)
	if err != nil || bounded.Err() != nil || len(addresses) == 0 || len(addresses) > 8 {
		return piUnapprovedHTTPView{}, ErrBackend
	}
	var selected netip.Addr
	for _, a := range addresses {
		if !piHTTPSPublicAddress(a) {
			return piUnapprovedHTTPView{}, ErrInvalid
		}
		if !selected.IsValid() || a.Compare(selected) < 0 {
			selected = a
		}
	}
	// Exactly one literal destination. No Happy Eyeballs, second address,
	// proxy selection, pooling, redirect, TLS reconnect or application retry.
	raw, err := p.dial(bounded, "tcp", net.JoinHostPort(selected.String(), "443"))
	if err != nil || raw == nil {
		if raw != nil {
			_ = raw.Close()
		}
		return piUnapprovedHTTPView{}, ErrBackend
	}
	if !p.publish(raw, bounded) || raw.SetDeadline(f) != nil {
		return piUnapprovedHTTPView{}, ErrConsumed
	}
	conn := tls.Client(raw, p.config)
	if conn.HandshakeContext(bounded) != nil || bounded.Err() != nil || conn.ConnectionState().NegotiatedProtocol != "http/1.1" {
		return piUnapprovedHTTPView{}, ErrBackend
	}
	n := p.prefixN
	n += copy(p.wire[n:], key)
	n += copy(p.wire[n:], "\r\n\r\n")
	n += copy(p.wire[n:], p.body)
	if bounded.Err() != nil {
		return piUnapprovedHTTPView{}, ErrConsumed
	}
	written, writeErr := conn.Write(p.wire[:n]) // one application Write, never retried
	clear(p.wire)
	if writeErr != nil || written != n || bounded.Err() != nil {
		return piUnapprovedHTTPView{}, ErrBackend
	}
	return receiver.receive(bounded, conn)
}

// Only after every sink/response borrower has joined. Original owner returns
// the live debit separately; no successful receive replenishes work or nodes.
func (p *piHTTPS) clear() {
	if p == nil {
		return
	}
	p.withdraw()
	clear(p.wire)
	for i := range p.der {
		clear(p.der[i])
		p.der[i] = nil
	}
	p.wire, p.body, p.roots, p.config = nil, nil, nil, nil
	p.lookup, p.dial = nil, nil
	p.host, p.target = "", ""
}
