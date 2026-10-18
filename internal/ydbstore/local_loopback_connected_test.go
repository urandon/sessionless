package ydbstore_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localloopback"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type localLoopbackPeerResult struct {
	request     *http.Request
	body, extra []byte
	err         error
}
type localLoopbackPeer struct {
	listener *net.TCPListener
	mu       sync.Mutex
	conn     *net.TCPConn
	closed   bool
	accepted chan struct{}
	result   chan localLoopbackPeerResult
	done     chan struct{}
	response []byte
	success  bool
	partial  bool
}

// One owned numeric peer, no external model/server. Denial mode drains to EOF,
// so zero application bytes is a causal assertion after connector cleanup,
// not a scheduler sample or a sleep claiming absence before a future write.
func newLocalLoopbackPeer(t *testing.T, success bool, partial ...bool) *localLoopbackPeer {
	t.Helper()
	listener, e := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if e != nil {
		t.Fatal(e)
	}
	p := &localLoopbackPeer{listener: listener, accepted: make(chan struct{}), result: make(chan localLoopbackPeerResult, 1), done: make(chan struct{}), success: success, response: []byte(`{"id":"owned-test-response","choices":[]}`)}
	if len(partial) > 1 {
		_ = listener.Close()
		t.Fatal("owned peer permits one partial-response mode")
	}
	if len(partial) == 1 {
		p.partial = partial[0]
	}
	t.Cleanup(func() {
		p.mu.Lock()
		p.closed = true
		c := p.conn
		p.mu.Unlock()
		_ = listener.Close()
		if c != nil {
			_ = c.Close()
		}
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-p.done:
		case <-timer.C:
			select {
			case <-p.done:
			default:
				t.Error("owned loopback peer did not join after listener/connection close")
			}
		}
	})
	go p.serve()
	return p
}
func (p *localLoopbackPeer) endpoint() string { return "http://" + p.listener.Addr().String() + "/v1" }
func (p *localLoopbackPeer) setResponse(body []byte) {
	p.mu.Lock()
	p.response = bytes.Clone(body)
	p.mu.Unlock()
}
func (p *localLoopbackPeer) responseBody() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return bytes.Clone(p.response)
}
func (p *localLoopbackPeer) serve() {
	defer close(p.done)
	c, e := p.listener.AcceptTCP()
	if e != nil {
		p.result <- localLoopbackPeerResult{err: e}
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = c.Close()
		p.result <- localLoopbackPeerResult{err: net.ErrClosed}
		return
	}
	p.conn = c
	p.mu.Unlock()
	defer c.Close()
	if e = c.SetDeadline(time.Now().Add(25 * time.Second)); e != nil {
		p.result <- localLoopbackPeerResult{err: e}
		return
	}
	close(p.accepted)
	if !p.success {
		raw, e := io.ReadAll(io.LimitReader(c, (1<<20)+(16<<10)+1))
		p.result <- localLoopbackPeerResult{body: raw, err: e}
		return
	}
	r := bufio.NewReader(io.LimitReader(c, (1<<20)+(16<<10)+1))
	req, e := http.ReadRequest(r)
	if e != nil {
		p.result <- localLoopbackPeerResult{err: e}
		return
	}
	body, e := io.ReadAll(io.LimitReader(req.Body, (1<<20)+1))
	_ = req.Body.Close()
	if e != nil {
		p.result <- localLoopbackPeerResult{request: req, body: body, err: e}
		return
	}
	responseBody := p.responseBody()
	response := []byte(fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n", len(responseBody)))
	if p.partial {
		responseBody = responseBody[:len(responseBody)/2]
	}
	response = append(response, responseBody...)
	n, e := c.Write(response)
	if e != nil || n != len(response) {
		p.result <- localLoopbackPeerResult{request: req, body: body, err: fmt.Errorf("owned peer response: bytes=%d err=%v", n, e)}
		return
	}
	if p.partial {
		p.result <- localLoopbackPeerResult{request: req, body: body}
		return
	}
	extra, e := io.ReadAll(io.LimitReader(r, 1025))
	p.result <- localLoopbackPeerResult{request: req, body: body, extra: extra, err: e}
}
func (p *localLoopbackPeer) waitAccepted(t *testing.T) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-p.accepted:
	case <-timer.C:
		select {
		case <-p.accepted:
		default:
			t.Fatal("actual numeric preflight never reached owned TCP peer")
		}
	}
}
func (p *localLoopbackPeer) waitResult(t *testing.T) localLoopbackPeerResult {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case r := <-p.result:
		return r
	case <-timer.C:
		select {
		case r := <-p.result:
			return r
		default:
			t.Fatal("owned peer did not reach EOF after connector cleanup")
			return localLoopbackPeerResult{}
		}
	}
}

// Genuine private Source, original same Store reserve/burn/runtime/entry,
// retained numeric connection publication, canonical operation reserve and
// before-send, and one actual HTTP effect. SQL/external funding/effect-source
// verification remain scripted: this is not native YDB, model, or demo proof.
func TestLocalLoopbackConnectedStoreActualOneSend(t *testing.T) {
	p := newLocalLoopbackPeer(t, true)
	f := ydbstore.NewLocalLoopbackConnectedFixture(t, p.endpoint())
	p.setResponse(f.Response(t))
	keeper, e := localloopback.NewKeeperV1(f.Source, f.OperationIssuer, f.Bootstrap)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := keeper.Close(); e != nil {
			t.Error(e)
		}
	})
	f.Complete(t, keeper)
	conn, e := keeper.Preflight(f.Context, f.Allocation, f.ExecutionSource, f.Operation, f.Payload())
	if e != nil {
		t.Fatalf("actual source/allocation literal preflight: %v", e)
	}
	t.Cleanup(func() {
		if e := conn.Close(); e != nil {
			t.Error(e)
		}
	})
	p.waitAccepted(t)
	proof, e := conn.Publication()
	if e != nil {
		t.Fatal(e)
	}
	registered, capability := f.RegisterAndReserve(t, proof)
	result, e := conn.Invoke(f.Context, registered, capability, f.Payload())
	if e != nil || !result.SendAttempted || !bytes.Equal(result.Body, p.responseBody()) || result.ResponseBytes != uint64(len(p.responseBody())) || result.RequestBytes <= uint64(len(f.Payload())) {
		t.Fatalf("actual retained Store-authorized HTTP: err=%v attempted=%t requestBytes=%d responseBytes=%d", e, result.SendAttempted, result.RequestBytes, result.ResponseBytes)
	}
	f.AssertOriginalCapabilityConsumed(t, capability)
	decoded, decodeErr := f.Frozen.DecodeResponse(result.Body)
	if decodeErr != nil || decoded.Stop != configuredinference.StopFinalV1 || decoded.Text != "owned peer result" || decoded.Call != nil || decoded.ModelProvenance != configuredinference.ModelReportedV1 || decoded.ConfiguredModel != decoded.ReportedModel || decoded.Usage.Provenance != configuredinference.UsageProviderReportedV1 || decoded.Usage.InputTokens == nil || *decoded.Usage.InputTokens != 1 || decoded.Usage.OutputTokens == nil || *decoded.Usage.OutputTokens != 1 || decoded.Usage.TotalTokens == nil || *decoded.Usage.TotalTokens != 2 {
		t.Fatalf("actual codec→wire→codec lost bounded reported provenance: err=%v stop=%s model=%s usage=%s", decodeErr, decoded.Stop, decoded.ModelProvenance, decoded.Usage.Provenance)
	}
	peer := p.waitResult(t)
	if peer.err != nil || peer.request == nil {
		t.Fatalf("owned peer request/EOF: %v", peer.err)
	}
	_, authorizationPresent := peer.request.Header["Authorization"]
	if peer.request.Method != "POST" || peer.request.RequestURI != "/v1/chat/completions" || peer.request.Host != p.listener.Addr().String() || authorizationPresent || peer.request.Header.Get("Content-Type") != "application/json" || !peer.request.Close || !bytes.Equal(peer.body, f.Payload()) || len(peer.extra) != 0 {
		t.Fatalf("fixed one-request wire changed: method=%s path=%s host=%s bodyBytes=%d extra=%d", peer.request.Method, peer.request.RequestURI, peer.request.Host, len(peer.body), len(peer.extra))
	}
	if keeper.VerifyOwnedProxyPublication(proof, time.Now().UTC().Truncate(time.Microsecond)) != nil || keeper.VerifyOwnedProxyPublicationCurrent(proof, time.Now().UTC().Truncate(time.Microsecond)) == nil {
		t.Fatal("closed consumed socket lost historical MAC or retained fresh permission")
	}
	if again, e := conn.Invoke(f.Context, registered, capability, f.Payload()); e == nil || again.SendAttempted || len(again.Body) != 0 {
		t.Fatal("consumed retained socket sent twice")
	}
	f.AssertReplayNoNewCapability(t)
}

func TestLocalLoopbackConnectedStoreInvalidCapabilityZeroSend(t *testing.T) {
	p := newLocalLoopbackPeer(t, false)
	f := ydbstore.NewLocalLoopbackConnectedFixture(t, p.endpoint())
	keeper, e := localloopback.NewKeeperV1(f.Source, f.OperationIssuer, f.Bootstrap)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := keeper.Close(); e != nil {
			t.Error(e)
		}
	})
	f.Complete(t, keeper)
	conn, e := keeper.Preflight(f.Context, f.Allocation, f.ExecutionSource, f.Operation, f.Payload())
	if e != nil {
		t.Fatalf("zero-send control needs actual preflight: %v", e)
	}
	t.Cleanup(func() {
		if e := conn.Close(); e != nil {
			t.Error(e)
		}
	})
	p.waitAccepted(t)
	proof, e := conn.Publication()
	if e != nil {
		t.Fatal(e)
	}
	registered, original := f.RegisterAndReserve(t, proof)
	invalid := original.Clone()
	invalid.Authenticator[0] ^= 1
	result, e := conn.Invoke(f.Context, registered, invalid, f.Payload())
	if !errors.Is(e, domain.ErrMembershipDenied) || result.SendAttempted || result.RequestBytes != 0 || len(result.Body) != 0 {
		t.Fatalf("invalid actual issuer MAC escaped Store before-send: err=%v attempted=%t bytes=%d", e, result.SendAttempted, result.RequestBytes)
	}
	peer := p.waitResult(t)
	if peer.err != nil || len(peer.body) != 0 {
		t.Fatalf("preflight/Store denial emitted HTTP before EOF: bytes=%d err=%v", len(peer.body), peer.err)
	}
	if again, e := conn.Invoke(f.Context, registered, original, f.Payload()); e == nil || again.SendAttempted {
		t.Fatal("valid original capability reopened denied socket")
	}
	if again, e := keeper.Preflight(f.Context, f.Allocation, f.ExecutionSource, f.Operation, f.Payload()); e == nil || again != nil {
		if again != nil {
			_ = again.Close()
		}
		t.Fatal("failed invocation redialed original sequence")
	}
	if keeper.VerifyOwnedProxyPublication(proof, time.Now().UTC().Truncate(time.Microsecond)) != nil || keeper.VerifyOwnedProxyPublicationCurrent(proof, time.Now().UTC().Truncate(time.Microsecond)) == nil {
		t.Fatal("denied socket history/current distinction changed")
	}
	f.AssertReplayNoNewCapability(t)
	f.AssertOriginalCapabilityUntouchedThenBurn(t, original)
}

type localLoopbackReady struct {
	fixture    *ydbstore.LocalLoopbackConnectedFixture
	keeper     *localloopback.KeeperV1
	connection *localloopback.ConnectionV1
	proof      ports.OwnedProxyPublicationV2
	registered domain.RegisteredProxyAttestationV2
	capability ports.AttemptOperationCapabilityV1
}

// Same concrete Source/Store/connector path for the remaining causal cases;
// this test helper supplies neither a dialer nor approval/capability callbacks.
func prepareLocalLoopbackConnected(t *testing.T, p *localLoopbackPeer) localLoopbackReady {
	t.Helper()
	f := ydbstore.NewLocalLoopbackConnectedFixture(t, p.endpoint())
	p.setResponse(f.Response(t))
	k, e := localloopback.NewKeeperV1(f.Source, f.OperationIssuer, f.Bootstrap)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := k.Close(); e != nil {
			t.Error(e)
		}
	})
	f.Complete(t, k)
	c, e := k.Preflight(f.Context, f.Allocation, f.ExecutionSource, f.Operation, f.Payload())
	if e != nil {
		t.Fatalf("causal control requires actual retained preflight: %v", e)
	}
	t.Cleanup(func() {
		if e := c.Close(); e != nil {
			t.Error(e)
		}
	})
	p.waitAccepted(t)
	proof, e := c.Publication()
	if e != nil {
		t.Fatal(e)
	}
	registered, capability := f.RegisterAndReserve(t, proof)
	return localLoopbackReady{f, k, c, proof, registered, capability}
}
func (r localLoopbackReady) assertHistoricalOnly(t *testing.T) {
	t.Helper()
	at := time.Now().UTC().Truncate(time.Microsecond)
	if r.keeper.VerifyOwnedProxyPublication(r.proof, at) != nil || r.keeper.VerifyOwnedProxyPublicationCurrent(r.proof, at) == nil {
		t.Fatal("closed actual observation lost history or retained current socket permission")
	}
	if result, e := r.connection.Invoke(r.fixture.Context, r.registered, r.capability, r.fixture.Payload()); e == nil || result.SendAttempted {
		t.Fatal("closed original operation retried application bytes")
	}
}

func TestLocalLoopbackConnectedStoreSourceCloseAfterCurrentZeroSend(t *testing.T) {
	p := newLocalLoopbackPeer(t, false)
	r := prepareLocalLoopbackConnected(t, p)
	r.fixture.ArmSourceCloseAfterCurrentCommit(t)
	result, e := r.connection.Invoke(r.fixture.Context, r.registered, r.capability, r.fixture.Payload())
	if !errors.Is(e, domain.ErrMembershipDenied) || result.SendAttempted || result.RequestBytes != 0 || len(result.Body) != 0 {
		t.Fatalf("post-current original Source closure emitted bytes: err=%v attempted=%t bytes=%d", e, result.SendAttempted, result.RequestBytes)
	}
	r.fixture.AssertSourceClosedAfterCurrent(t)
	if r.fixture.Context.Err() != nil {
		t.Fatal("closing Source cancelled the original caller context")
	}
	peer := p.waitResult(t)
	if peer.err != nil || len(peer.body) != 0 {
		t.Fatalf("custody/Tx succeeded but revoked Source emitted HTTP before EOF: bytes=%d err=%v", len(peer.body), peer.err)
	}
	r.assertHistoricalOnly(t)
	r.fixture.AssertOriginalCapabilityUntouchedThenBurn(t, r.capability)
}

func TestLocalLoopbackConnectedStoreRevokedCurrentProfileZeroSend(t *testing.T) {
	p := newLocalLoopbackPeer(t, false)
	r := prepareLocalLoopbackConnected(t, p)
	r.fixture.RevokeCurrentProfile(t)
	result, e := r.connection.Invoke(r.fixture.Context, r.registered, r.capability, r.fixture.Payload())
	if !errors.Is(e, domain.ErrMembershipDenied) || result.SendAttempted || result.RequestBytes != 0 || len(result.Body) != 0 {
		t.Fatalf("scripted current-profile revocation bypassed real Store join: err=%v attempted=%t bytes=%d", e, result.SendAttempted, result.RequestBytes)
	}
	peer := p.waitResult(t)
	if peer.err != nil || len(peer.body) != 0 {
		t.Fatalf("stale original source/allocation emitted HTTP before EOF: bytes=%d err=%v", len(peer.body), peer.err)
	}
	r.fixture.AssertSingleOriginalWrites(t)
	r.assertHistoricalOnly(t)
	r.fixture.AssertOriginalCapabilityUntouchedThenBurn(t, r.capability)
}

// The same actual Store is called with its independent original caller, not
// the connector's Source-linked lease context. Source closes only AFTER the
// final current transaction commits. A denial here therefore cannot be merely
// ctx.Err; the original genuine issuer must still retain its unconsumed token.
func TestLocalLoopbackConnectedStorePostTransactionSourceGuardBeforeConsume(t *testing.T) {
	p := newLocalLoopbackPeer(t, false)
	r := prepareLocalLoopbackConnected(t, p)
	r.fixture.ArmSourceCloseAfterCurrentCommit(t)
	if err := r.fixture.AuthorizeOriginalStoreBeforeSend(t, r.capability); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("Source custody closed after final current commit but original Store authorized: %v", err)
	}
	r.fixture.AssertSourceClosedAfterCurrent(t)
	if err := r.connection.Close(); err != nil {
		t.Fatalf("join Source-revoked original preflight socket: %v", err)
	}
	peer := p.waitResult(t)
	if peer.err != nil || len(peer.body) != 0 {
		t.Fatalf("direct Store post-transaction guard emitted preflight HTTP: bytes=%d err=%v", len(peer.body), peer.err)
	}
	r.assertHistoricalOnly(t)
	r.fixture.AssertOriginalCapabilityUntouchedThenBurn(t, r.capability)
}

func TestLocalLoopbackConnectedStorePartialReplyIsUnknownNoRetry(t *testing.T) {
	p := newLocalLoopbackPeer(t, true, true)
	r := prepareLocalLoopbackConnected(t, p)
	result, e := r.connection.Invoke(r.fixture.Context, r.registered, r.capability, r.fixture.Payload())
	if !errors.Is(e, domain.ErrMembershipDenied) || !result.SendAttempted || result.RequestBytes <= uint64(len(r.fixture.Payload())) || len(result.Body) != 0 || result.ResponseBytes != 0 {
		t.Fatalf("partial reply was accepted or lost unknown-effect marker: err=%v attempted=%t requestBytes=%d responseBytes=%d", e, result.SendAttempted, result.RequestBytes, result.ResponseBytes)
	}
	peer := p.waitResult(t)
	if peer.err != nil || peer.request == nil || !bytes.Equal(peer.body, r.fixture.Payload()) {
		t.Fatalf("partial-response control did not first receive original single request: bytes=%d err=%v", len(peer.body), peer.err)
	}
	_, authPresent := peer.request.Header["Authorization"]
	if authPresent || peer.request.Method != "POST" || peer.request.RequestURI != "/v1/chat/completions" {
		t.Fatal("partial-response control changed fixed no-auth route")
	}
	r.assertHistoricalOnly(t)
	r.fixture.AssertReplayNoNewCapability(t)
	r.fixture.AssertOriginalCapabilityConsumed(t, r.capability)
}
