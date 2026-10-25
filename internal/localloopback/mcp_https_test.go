package localloopback

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessegress"
)

type mcpWireTestConn struct {
	reader *bytes.Reader
	writes bytes.Buffer
}

func (c *mcpWireTestConn) Read(p []byte) (int, error)     { return c.reader.Read(p) }
func (c *mcpWireTestConn) Write(p []byte) (int, error)    { return c.writes.Write(p) }
func (*mcpWireTestConn) Close() error                     { return nil }
func (*mcpWireTestConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*mcpWireTestConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*mcpWireTestConn) SetDeadline(time.Time) error      { return nil }
func (*mcpWireTestConn) SetReadDeadline(time.Time) error  { return nil }
func (*mcpWireTestConn) SetWriteDeadline(time.Time) error { return nil }

func TestMCPExactReaderNeverConsumesFollowingHTTPBytes(t *testing.T) {
	conn := &mcpWireTestConn{reader: bytes.NewReader([]byte("first-second"))}
	r := mcpExactReader{Conn: conn}
	var got bytes.Buffer
	for i := 0; i < len("first"); i++ {
		buf := make([]byte, 64)
		n, err := r.Read(buf)
		if err != nil || n != 1 {
			t.Fatalf("read %d returned n=%d err=%v", i, n, err)
		}
		got.Write(buf[:n])
	}
	if got.String() != "first" {
		t.Fatalf("one-byte framing reader returned %q", got.String())
	}
	if conn.reader.Len() != len("-second") {
		t.Fatal("HTTP parser could have swallowed bytes from a later response")
	}
}

func TestMCPExchangeRejectsBadInputBeforeAuthorityOrWire(t *testing.T) {
	conn := &mcpWireTestConn{reader: bytes.NewReader(nil)}
	c := &ConnectionV1{ctx: context.Background(), conn: conn, mcp: &mcpConnectionV1{policy: ports.LocalPublicMCPHTTPSProxyPolicyV1{Limits: mcpwire.LimitsV1{MessageBytes: 8}}}}
	for name, body := range map[string][]byte{"empty": nil, "oversized": []byte("123456789")} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := c.mcpExchange(body, false, false); err == nil {
				t.Fatal("invalid setup message accepted")
			}
			if conn.writes.Len() != 0 {
				t.Fatal("invalid setup message reached transport")
			}
		})
	}
	// A valid-sized message still needs the concrete original Store authority;
	// nil is deliberately a denial, never a test callback or fake approval.
	if _, _, err := c.mcpExchange([]byte("{}"), false, false); err == nil {
		t.Fatal("missing original Store accepted")
	}
	if conn.writes.Len() != 0 {
		t.Fatal("missing Store authority emitted bytes")
	}
}

func TestMCPPreflightInvalidOriginalOwnerDoesNotResolveOrDial(t *testing.T) {
	lookups, dials := 0, 0
	k := &KeeperV1{sourceLifetime: context.Background(), mcpDeps: mcpHTTPSDependencies{
		lookup: func(context.Context, string) ([]netip.Addr, error) {
			lookups++
			return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
		},
		dial: func(context.Context, string, string) (net.Conn, error) { dials++; return nil, io.EOF },
	}}
	k.self = k
	if c, err := k.PreflightMCP(context.Background(), domain.RegisteredAllocationV2{}, domain.AttemptExecutionSourceV1{}, domain.AttemptOperationV1{}, mcpwire.RequestV1{}); err == nil || c != nil {
		t.Fatal("synthetic metadata created a local MCP connection")
	}
	if lookups != 0 || dials != 0 {
		t.Fatalf("invalid original owner reached network dependencies: DNS=%d dial=%d", lookups, dials)
	}
}

func TestMCPPublicResolutionDataRejectsNonPublicAndBoundsSets(t *testing.T) {
	valid := []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("1.1.1.1")}
	literals, digest, err := serverlessegress.PublicResolutionDataV1(valid)
	if err != nil || len(literals) != 2 || digest == "" {
		t.Fatalf("synthetic public resolution DATA: %v", err)
	}
	for _, addresses := range [][]netip.Addr{
		nil,
		{netip.MustParseAddr("127.0.0.1")},
		{netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("8.8.8.8")},
		{netip.MustParseAddr("::ffff:8.8.8.8")},
		{netip.MustParseAddr("fe80::1%en0")},
		{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("9.9.9.9"), netip.MustParseAddr("4.2.2.2"), netip.MustParseAddr("208.67.222.222"), netip.MustParseAddr("64.6.64.6"), netip.MustParseAddr("8.26.56.26"), netip.MustParseAddr("76.76.2.0"), netip.MustParseAddr("94.140.14.14")},
	} {
		if _, _, err := serverlessegress.PublicResolutionDataV1(addresses); err == nil {
			t.Errorf("non-public/oversized resolution accepted: %s", strings.TrimSpace(strings.Join(addrStrings(addresses), ",")))
		}
	}
}

func TestMCPRequestWireContainsNoBorrowedCredentials(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":7}`)
	wire := mcpRequestWire(body)
	for _, want := range []string{
		"POST /mcp HTTP/1.1\r\n", "Host: developers.openai.com\r\n",
		"Content-Type: application/json\r\n", "Accept: application/json, text/event-stream\r\n",
		"MCP-Protocol-Version: " + mcpwire.ProtocolVersionV1 + "\r\n",
		"Content-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n",
	} {
		if !bytes.Contains(wire, []byte(want)) {
			t.Errorf("request lacks fixed header %q", want)
		}
	}
	if !bytes.HasSuffix(wire, body) {
		t.Fatal("request body not appended exactly")
	}
	for _, forbidden := range []string{"Authorization:", "Cookie:", "Bearer", "api_key"} {
		if bytes.Contains(wire, []byte(forbidden)) {
			t.Errorf("request unexpectedly contains credential marker %q", forbidden)
		}
	}
}

func TestMCPExchangeReadLimitsBindOperationAndAggregateUsage(t *testing.T) {
	limits := mcpwire.LimitsV1{MessageBytes: 64, AggregateBytes: 100, JSONDepth: 8, JSONMembers: 64, SSELineBytes: 100, SSEEventBytes: 64, SSEEvents: 8, Tools: 4, DescriptorBytes: 32, ArgumentBytes: 16, ResultTextBytes: 32, ContentItems: 8}
	grant := domain.AttemptOperationDebitV1{MCPOutboundMessages: 5, MCPInboundFrames: 7, MCPInboundBytes: 90, ResponseBytes: 80}
	t.Run("narrower operation than grant/policy", func(t *testing.T) {
		debit := grant
		debit.MCPInboundFrames = 3
		debit.MCPInboundBytes = 40
		debit.ResponseBytes = 30
		gotBytes, gotFrames, err := mcpExchangeReadLimits(limits, debit, mcpExchangeUsageV1{inbound: 5, frames: 1}, mcpExchangeUsageV1{inbound: 10, frames: 2}, false)
		if err != nil || gotBytes != 25 || gotFrames != 2 {
			t.Fatalf("narrow operation budget yielded bytes=%d frames=%d err=%v", gotBytes, gotFrames, err)
		}
	})
	t.Run("operation exhausted", func(t *testing.T) {
		_, _, err := mcpExchangeReadLimits(limits, grant, mcpExchangeUsageV1{messages: 5}, mcpExchangeUsageV1{}, false)
		if err == nil {
			t.Fatal("exhausted operation message reservation was reset")
		}
	})
	t.Run("aggregate exhausted", func(t *testing.T) {
		_, _, err := mcpExchangeReadLimits(limits, grant, mcpExchangeUsageV1{}, mcpExchangeUsageV1{inbound: 256 << 10}, false)
		if err == nil {
			t.Fatal("exhausted aggregate inbound reservation was reset")
		}
	})
	t.Run("source zero does not reset cumulative usage", func(t *testing.T) {
		// A zero-source case is represented by the same retained cumulative
		// counters: the helper has no source/reset input and must deny at debit.
		debit := grant
		debit.MCPInboundBytes = 12
		_, _, err := mcpExchangeReadLimits(limits, debit, mcpExchangeUsageV1{inbound: 12}, mcpExchangeUsageV1{}, false)
		if err == nil {
			t.Fatal("zero/new source semantics reset cumulative operation usage")
		}
	})
}

func addrStrings(values []netip.Addr) []string {
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = value.String()
	}
	return out
}
