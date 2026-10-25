//go:build sessionless_mcp_offline

package ydbstore_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
)

type offlineMCPRemote struct{ net.Conn }

func (offlineMCPRemote) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(8, 8, 8, 8), Port: 443}
}

// DNS/IP observations alone are SYNTHETIC. The socket, TLS handshake, SNI,
// certificate/root validation, ALPN, HTTP frames and closure are actual local IO.
type offlineMCPPeer struct {
	listener       net.Listener
	roots          *x509.CertPool
	certificate    tls.Certificate
	mu             sync.Mutex
	conn           net.Conn
	methods        []string
	bodies         [][]byte
	err            error
	lookups, dials int
	verifiedTLS    bool
	descriptor     []byte
	afterCatalog   func()
	dropReply      bool
	wantFrames     int
	done           chan struct{}
}

func newOfflineMCPPeer(t *testing.T, descriptor []byte, want int, afterCatalog func(), dropReply bool) *offlineMCPPeer {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "offline MCP fixture"}, DNSNames: []string{"developers.openai.com"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: true}
	der, e := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	cert, e := x509.ParseCertificate(der)
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	p := &offlineMCPPeer{listener: listener, roots: roots, certificate: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, descriptor: bytes.Clone(descriptor), afterCatalog: afterCatalog, dropReply: dropReply, wantFrames: want, done: make(chan struct{})}
	t.Cleanup(func() {
		_ = listener.Close()
		p.mu.Lock()
		c := p.conn
		p.mu.Unlock()
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
				t.Error("offline MCP TLS peer did not join")
			}
		}
		for _, b := range p.bodies {
			clear(b)
		}
	})
	go p.serve()
	return p
}
func (p *offlineMCPPeer) lookup(ctx context.Context, host string) ([]netip.Addr, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lookups++
	if ctx == nil || ctx.Err() != nil || host != "developers.openai.com" || p.lookups != 1 {
		return nil, errors.New("offline exact DNS observation")
	}
	return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
}
func (p *offlineMCPPeer) dial(ctx context.Context, network, address string) (net.Conn, error) {
	p.mu.Lock()
	p.dials++
	valid := p.dials == 1 && network == "tcp" && address == "8.8.8.8:443"
	p.mu.Unlock()
	if !valid {
		return nil, errors.New("offline exact pinned dial")
	}
	c, e := (&net.Dialer{}).DialContext(ctx, "tcp4", p.listener.Addr().String())
	if e != nil {
		return nil, e
	}
	return offlineMCPRemote{c}, nil
}
func (p *offlineMCPPeer) serve() {
	defer close(p.done)
	raw, e := p.listener.Accept()
	if e != nil {
		p.setError(e)
		return
	}
	p.mu.Lock()
	p.conn = raw
	p.mu.Unlock()
	defer raw.Close()
	_ = raw.SetDeadline(time.Now().Add(20 * time.Second))
	secure := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{p.certificate}, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}})
	if e = secure.Handshake(); e != nil {
		p.setError(e)
		return
	}
	state := secure.ConnectionState()
	if state.ServerName != "developers.openai.com" || state.NegotiatedProtocol != "http/1.1" {
		p.setError(errors.New("offline TLS SNI/ALPN mismatch"))
		return
	}
	p.mu.Lock()
	p.verifiedTLS = true
	p.mu.Unlock()
	reader := bufio.NewReaderSize(secure, 16<<10)
	for i := 0; i < p.wantFrames; i++ {
		r, e := http.ReadRequest(reader)
		if e != nil {
			p.setError(e)
			return
		}
		body, e := io.ReadAll(io.LimitReader(r.Body, 4097))
		_ = r.Body.Close()
		var message struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Method  string          `json:"method"`
			Params  struct {
				Name      string          `json:"name"`
				Arguments json.RawMessage `json:"arguments"`
			} `json:"params"`
		}
		want := []string{"initialize", "notifications/initialized", "tools/list", "tools/call"}[i]
		if e != nil || len(body) > 4096 || r.Method != "POST" || r.URL.Path != "/mcp" || r.Host != "developers.openai.com" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Mcp-Session-Id") != "" || r.Header.Get("MCP-Protocol-Version") != mcpwire.ProtocolVersionV1 || json.Unmarshal(body, &message) != nil || message.JSONRPC != "2.0" || message.Method != want {
			p.setError(errors.New("offline fixed unauthenticated MCP HTTP frame mismatch"))
			return
		}
		if i == 3 && (message.Params.Name != "search_openai_docs" || string(message.Params.Arguments) != `{"query":"public docs"}`) {
			p.setError(errors.New("offline tools/call payload mismatch"))
			return
		}
		p.mu.Lock()
		p.methods = append(p.methods, message.Method)
		p.bodies = append(p.bodies, bytes.Clone(body))
		p.mu.Unlock()
		if i == 3 && p.dropReply {
			return // The exact tools/call was accepted; its response is lost.
		}
		if i == 1 {
			_, e = io.WriteString(secure, "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
			if e != nil {
				p.setError(e)
				return
			}
			continue
		}
		var result any
		switch i {
		case 0:
			result = map[string]any{"protocolVersion": mcpwire.ProtocolVersionV1, "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]any{"name": "offline-docs", "version": "1"}}
		case 2:
			result = map[string]any{"tools": []json.RawMessage{p.descriptor}}
		case 3:
			result = map[string]any{"content": []any{map[string]any{"type": "text", "text": "offline public docs result"}}, "isError": false}
		}
		response, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": message.ID, "result": result})
		if e != nil {
			p.setError(e)
			return
		}
		if i == 2 && p.afterCatalog != nil {
			p.afterCatalog()
		}
		if _, e = fmt.Fprintf(secure, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", len(response)); e == nil {
			_, e = secure.Write(response)
		}
		if e != nil {
			p.setError(e)
			return
		}
	}
	var extra [1]byte
	n, e := reader.Read(extra[:])
	if n != 0 || !errors.Is(e, io.EOF) {
		p.setError(errors.New("MCP peer saw unexpected extra frame or missing EOF"))
	}
}
func (p *offlineMCPPeer) setError(e error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err == nil {
		p.err = e
	}
}
func (p *offlineMCPPeer) wait(t *testing.T) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-p.done:
	case <-timer.C:
		select {
		case <-p.done:
		default:
			t.Fatal("offline MCP peer missing actual join")
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil || !p.verifiedTLS || p.lookups != 1 || p.dials != 1 || len(p.methods) != p.wantFrames {
		t.Fatalf("offline MCP join failed: error=%v TLS=%t DNS=%d dial=%d frames=%d/%d", p.err, p.verifiedTLS, p.lookups, p.dials, len(p.methods), p.wantFrames)
	}
}

func TestLocalSessionCredentialedMCPOfflineJoined(t *testing.T) {
	t.Run("known_result_unknown_money", testOfflineMCPChainCredentialed)
	for _, mode := range []string{"revoke_after_catalog", "missing_store_call_index", "lost_reply"} {
		t.Run(mode, func(t *testing.T) {
			modelPeer := newLocalLoopbackPeer(t, true)
			seed, _, credentials, _, session := newCredentialedConnectedSessionAt(t, modelPeer.endpoint())
			grant := seed.MCPOfflineInstalledGrant(t)
			want := 4
			var fault func()
			catalogReached, catalogContinue := make(chan struct{}), make(chan struct{})
			faultKind := ""
			if mode == "revoke_after_catalog" || mode == "missing_store_call_index" {
				want = 3
				kind := "revoke_mcp"
				if mode == "missing_store_call_index" {
					kind = "missing_call_index"
				}
				faultKind = kind
				fault = func() {
					close(catalogReached)
					select {
					case <-catalogContinue:
					case <-t.Context().Done():
					}
				}
			}
			mcp := newOfflineMCPPeer(t, grant.Descriptor, want, fault, mode == "lost_reply")
			if e := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); e != nil {
				t.Fatal("inert instance-scoped offline network", e)
			}
			if e := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); e == nil {
				t.Fatal("network instrumentation could be reset")
			}
			arguments := `{"query":"public docs"}`
			response, e := json.Marshal(map[string]any{"model": seed.Profile.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "offline-call-1", "type": "function", "function": map[string]any{"name": "search_openai_docs", "arguments": arguments}}}}, "finish_reason": "tool_calls"}}, "usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
			if e != nil {
				t.Fatal(e)
			}
			modelPeer.setResponse(response)
			before := seed.Stats()
			type runResult struct {
				result   providerloopsession.ModelStepResultV1
				terminal *providerloopsession.PreparedLocalTerminalV1
				err      error
			}
			runCtx, cancel := context.WithCancel(t.Context())
			defer cancel()
			completed := make(chan runResult, 1)
			joined := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				timer := time.NewTimer(10 * time.Second)
				defer timer.Stop()
				select {
				case <-joined:
				case <-timer.C:
					select {
					case <-joined:
					default:
						t.Error("offline original Run worker did not join")
					}
				}
			})
			go func() {
				defer close(joined)
				result, terminal, e := session.Run(runCtx, seed.Request)
				completed <- runResult{result, terminal, e}
			}()
			var run runResult
			if faultKind != "" {
				select {
				case <-catalogReached:
					seed.MCPAfterCatalogFault(t, faultKind)
					close(catalogContinue)
					select {
					case run = <-completed:
					case <-t.Context().Done():
						t.Fatal("original Run did not finish after canonical fault")
					}
				case run = <-completed:
				case <-t.Context().Done():
					t.Fatal("catalog boundary not reached")
				}
			} else {
				select {
				case run = <-completed:
				case <-t.Context().Done():
					t.Fatal("original Run did not finish")
				}
			}
			result, terminal, e := run.result, run.terminal, run.err
			if !errors.Is(e, providerloopsession.ErrIncomplete) || terminal != nil || !result.Model.SendAttempted || result.Tool == nil || result.Compaction != nil {
				model := result.Model.Observation
				t.Fatalf("closed MCP sequence must halt without successful terminal: err=%v terminal=%t tool=%t stats=%+v model=(outcome=%s reason=%s usage=%s)", e, terminal != nil, result.Tool != nil, seed.Stats(), model.Observation.Outcome, model.Observation.Reason, model.Observation.Usage.State)
			}
			mcp.wait(t)
			modelWire := modelPeer.waitResult(t)
			if modelWire.err != nil || modelWire.request == nil || modelWire.request.Header.Get("Authorization") != "Bearer "+localSessionFixtureCredential || len(modelWire.extra) != 0 {
				t.Fatal("one original authenticated MODEL was not followed by socket closure", modelWire.err)
			}
			if credentials.issues != 1 || credentials.materializations != 1 || credentials.callbacks != 1 || credentials.releases != 1 || !credentials.borrowedZeroed || !credentials.materializeReturned || !credentials.releaseSawMaterializeReturned || !credentials.releaseSawBorrowedZeroed || !credentials.releaseHasDeadline {
				t.Fatal("joined MODEL/MCP escaped one materialization/actual disposal/Release")
			}
			if e = session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); e == nil {
				t.Fatal("started or completed original instance could be rewired")
			}
			stats := seed.Stats()
			if stats.Parent != before.Parent+1 || stats.Burn != before.Burn+1 || stats.Allocation != before.Allocation+1 || stats.Operation != before.Operation+2 || stats.Call != before.Call+2 {
				t.Fatalf("same original Store did not reserve exact MODEL+MCP pair: %+v", stats)
			}
			if mode == "lost_reply" {
				tool := result.Tool.Observation
				if !result.Tool.SendAttempted || tool.Observation.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || tool.Observation.Reason != domain.AttemptOperationReasonTransportUnknownV1 || tool.Observation.Usage.State != domain.AttemptOperationUsageUnknownV1 || tool.Observation.ResponseRef != "" || tool.Observation.ResponseDigest != "" || tool.Observation.ResponseBytes != 0 || seed.Operate.VerifyAttemptOperationObservation(tool) != nil {
					t.Fatal("lost actual MCP reply was misclassified as before-send rejection or known result")
				}
				if observed := seed.PersistedCompactionObservations(t); !reflect.DeepEqual(observed, []domain.AttemptOperationObservationV1{result.Model.Observation.Observation, tool.Observation}) {
					t.Fatal("lost MCP reply did not retain exact durable unknown outcome")
				}
			} else if result.Tool.SendAttempted {
				t.Fatal("canonical post-catalog fault disclosed tool arguments")
			}
			beforeReplay := seed.Stats()
			again, next, repeatErr := session.Run(t.Context(), seed.Request)
			if repeatErr == nil || next != nil || again.Model.SendAttempted || seed.Stats() != beforeReplay {
				t.Fatal("unknown or rejected MCP work replayed into another MODEL/effect")
			}
		})
	}
}
