package boundedhttp_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/boundedhttp"
)

func TestReadMCPResponseJSONChunkedAndNotificationBoundaries(t *testing.T) {
	body := `{"jsonrpc":"2.0","id":"init-1","result":{}}`
	for name, wire := range map[string]string{
		"content_length": "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n" + body,
		"chunked":        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n" + strconv.FormatInt(int64(len(body)), 16) + "\r\n" + body + "\r\n0\r\n\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			for _, fragment := range []int{1, 7, 4096} {
				r := &fragmentReader{source: strings.NewReader(wire), fragment: fragment}
				response, err := boundedhttp.ReadMCPResponseV1(r, 256, 1024, false)
				if err != nil || string(response.Body) != body || response.ContentType != "application/json" {
					t.Fatalf("fragment %d: %+v %v", fragment, response, err)
				}
				if r.closed {
					t.Fatal("response reader closed caller-owned source")
				}
			}
		})
	}
	ack := &fragmentReader{source: strings.NewReader("HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n"), fragment: 1}
	response, err := boundedhttp.ReadMCPResponseV1(ack, 10, 1024, true)
	if err != nil || len(response.Body) != 0 || response.ContentType != "" {
		t.Fatalf("empty 202 notification ack: %+v %v", response, err)
	}
	for _, tc := range []struct {
		wire         string
		notification bool
	}{
		{"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", false},
		{"HTTP/1.1 202 Accepted\r\nContent-Length: 1\r\n\r\nx", true},
		{"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n", false},
	} {
		if got, err := boundedhttp.ReadMCPResponseV1(strings.NewReader(tc.wire), 16, 1024, tc.notification); err == nil {
			t.Errorf("unexpected status/body accepted: %+v", got)
		}
	}
}

func TestReadMCPResponseRejectsAmbientSessionAuthRedirectEncodingAndAmbiguousHeaders(t *testing.T) {
	for name, header := range map[string]string{
		"session_id":             "Mcp-Session-Id: session-1\r\n",
		"protocol_version":       "Mcp-Protocol-Version: 2025-11-25\r\n",
		"cookie":                 "Set-Cookie: session=secret\r\n",
		"www_auth":               "WWW-Authenticate: Bearer\r\n",
		"proxy_auth":             "Proxy-Authenticate: Basic\r\n",
		"encoding":               "Content-Encoding: gzip\r\n",
		"duplicate_content_type": "Content-Type: application/json\r\n",
		"duplicate_session_id":   "Mcp-Session-Id: a\r\nMcp-Session-Id: b\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			wire := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n" + header + "Content-Length: 2\r\n\r\n{}"
			if got, err := boundedhttp.ReadMCPResponseV1(strings.NewReader(wire), 32, 1024, false); !errors.Is(err, boundedhttp.ErrResponse) || got.Body != nil {
				t.Fatalf("forbidden header accepted: %+v %v", got, err)
			}
		})
	}
	for _, wire := range []string{
		"HTTP/1.1 302 Found\r\nLocation: https://attacker.invalid\r\nContent-Length: 0\r\n\r\n",
		"HTTP/1.1 101 Switching Protocols\r\nContent-Length: 0\r\n\r\n",
		"HTTP/1.0 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
		"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n",
	} {
		if got, err := boundedhttp.ReadMCPResponseV1(strings.NewReader(wire), 32, 1024, false); err == nil || got.Body != nil {
			t.Errorf("redirect/version/ambiguous response accepted: %+v", got)
		}
	}
}

func TestReadMCPResponseEnforcesBodyAndHeaderBoundsWithoutDraining(t *testing.T) {
	wire := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 3\r\n\r\nabc"
	headerBytes := uint64(strings.Index(wire, "\r\n\r\n") + 4)
	for _, tc := range []struct {
		body, headers uint64
		ok            bool
	}{{3, headerBytes, true}, {2, headerBytes, false}, {3, headerBytes - 1, false}, {0, headerBytes, false}, {3, 16<<10 + 1, false}} {
		r := &fragmentReader{source: strings.NewReader(wire), fragment: 4096}
		got, err := boundedhttp.ReadMCPResponseV1(r, tc.body, tc.headers, false)
		if (err == nil) != tc.ok {
			t.Errorf("boundary %+v => %v", tc, err)
		}
		if !tc.ok && got.Body != nil {
			t.Fatal("failed bound returned partial body")
		}
		if r.closed {
			t.Fatal("reader closed caller-owned source")
		}
	}
	large := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 33\r\n\r\n" + strings.Repeat("x", 33)
	r := &fragmentReader{source: strings.NewReader(large), fragment: 4096}
	if got, err := boundedhttp.ReadMCPResponseV1(r, 32, 1024, false); !errors.Is(err, boundedhttp.ErrResponse) || got.Body != nil {
		t.Fatal("oversized declared body accepted")
	}
	for _, limits := range [][2]uint64{{0, 10}, {1, 16<<10 + 1}, {256<<10 + 1, 10}} {
		r := &fragmentReader{source: strings.NewReader(wire), fragment: 4096}
		if got, err := boundedhttp.ReadMCPResponseV1(r, limits[0], limits[1], false); err == nil || got.Body != nil || r.calls != 0 {
			t.Fatal("invalid cap read source or returned content")
		}
	}
}
