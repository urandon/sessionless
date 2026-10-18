package boundedhttp_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/boundedhttp"
)

type fragmentReader struct {
	source       io.Reader
	fragment     int
	bytes, calls int
	closed       bool
}

func (r *fragmentReader) Read(p []byte) (int, error) {
	if len(p) > r.fragment {
		p = p[:r.fragment]
	}
	n, err := r.source.Read(p)
	r.bytes += n
	r.calls++
	return n, err
}
func (r *fragmentReader) Close() error { r.closed = true; return nil }

func TestReadOKResponseCompleteOwnedBodies(t *testing.T) {
	for _, tc := range []struct{ name, wire, want string }{
		{"length", "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nonetwo", "onetwo"},
		{"chunked", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\none\r\n3\r\ntwo\r\n0\r\n\r\n", "onetwo"},
		{"close_delimited", "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nonetwo", "onetwo"},
		{"empty", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", ""},
		{"header_case", "HTTP/1.1 200 OK\r\ncOnTeNt-LeNgTh: 6\r\n\r\nonetwo", "onetwo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, fragment := range []int{1, 7, 4096} {
				r := &fragmentReader{source: strings.NewReader(tc.wire), fragment: fragment}
				body, err := boundedhttp.ReadOKResponse(r, 6, 1024)
				if err != nil || string(body) != tc.want {
					t.Fatalf("fragment=%d body=%q err=%v", fragment, body, err)
				}
				if r.closed {
					t.Fatal("reader closed caller-owned source")
				}
				if len(body) > 0 {
					body[0] = 'x'
					again, err := boundedhttp.ReadOKResponse(strings.NewReader(tc.wire), 6, 1024)
					if err != nil || string(again) != tc.want {
						t.Fatal("returned body not caller-owned")
					}
				}
			}
		})
	}
}

func TestReadOKResponseRejectsOriginalMalformedCorpus(t *testing.T) {
	for _, tc := range []struct{ name, wire string }{
		{"redirect", "HTTP/1.1 302 Found\r\nLocation: https://attacker.invalid/\r\nContent-Length: 0\r\n\r\n"},
		{"interim", "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"},
		{"http10", "HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n"},
		{"bare_lf_status", "HTTP/1.1 200 OK\nContent-Length: 0\r\n\r\n"},
		{"large_header", "HTTP/1.1 200 OK\r\nX-Large: " + strings.Repeat("x", 16<<10) + "\r\n\r\n"},
		{"many_headers", "HTTP/1.1 200 OK\r\n" + strings.Repeat("X-A: x\r\n", 101) + "Content-Length: 0\r\n\r\n"},
		{"chunked_body", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n401\r\n" + strings.Repeat("x", 1025) + "\r\n0\r\n\r\n"},
		{"partial_response", "HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\npartial"},
		{"duplicate_length", "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx"},
		{"conflicting_length", "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx"},
		{"ambiguous_framing", "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n"},
		{"empty_header_name", "HTTP/1.1 200 OK\r\n: value\r\nContent-Length: 0\r\n\r\n"},
		{"spaced_transfer_encoding", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nTransfer-Encoding : chunked\r\n\r\n"},
		{"spaced_cookie", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nSet-Cookie : ambient=value\r\n\r\n"},
		{"spaced_encoding", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Encoding : gzip\r\n\r\n"},
		{"tabbed_name", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent\tEncoding: gzip\r\n\r\n"},
		{"nonascii_name", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Encod\xffing: gzip\r\n\r\n"},
		{"encoding", "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n"},
		{"cookie", "HTTP/1.1 200 OK\r\nSet-Cookie: ambient=value\r\nContent-Length: 0\r\n\r\n"},
		{"challenge", "HTTP/1.1 200 OK\r\nWWW-Authenticate: Basic\r\nContent-Length: 0\r\n\r\n"},
		{"proxy_challenge", "HTTP/1.1 200 OK\r\nProxy-Authenticate: Basic\r\nContent-Length: 0\r\n\r\n"},
		{"empty_encoding", "HTTP/1.1 200 OK\r\nContent-Encoding:\r\nContent-Length: 0\r\n\r\n"},
		{"empty_then_encoding", "HTTP/1.1 200 OK\r\nContent-Encoding:\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n"},
		{"unannounced_trailer", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\nX-Trailer: " + strings.Repeat("x", 16<<10) + "\r\n\r\n"},
		{"announced_trailer", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Trailer\r\n\r\n0\r\nX-Trailer: value\r\n\r\n"},
		{"chunk_line", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" + strings.Repeat("0", 257) + "\r\n\r\n"},
		{"chunk_extension", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1;extension=x\r\nx\r\n0\r\n\r\n"},
		{"chunk_overflow", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nffffffffffffffff\r\n"},
		{"chunk_bad_crlf", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\n0\r\n\r\n"},
		{"multiple_te", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"},
		{"unknown_te", "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n"},
		{"negative_cl", "HTTP/1.1 200 OK\r\nContent-Length: -1\r\n\r\n"},
		{"signed_cl", "HTTP/1.1 200 OK\r\nContent-Length: +1\r\n\r\nx"},
		{"overflow_cl", "HTTP/1.1 200 OK\r\nContent-Length: 18446744073709551616\r\n\r\n"},
		{"obs_fold", "HTTP/1.1 200 OK\r\nX-A: one\r\n two\r\nContent-Length: 0\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, fragment := range []int{1, 17, 4096} {
				r := &fragmentReader{source: strings.NewReader(tc.wire), fragment: fragment}
				body, err := boundedhttp.ReadOKResponse(r, 1024, 16<<10)
				if !errors.Is(err, boundedhttp.ErrResponse) || body != nil {
					t.Fatalf("fragment=%d malformed response returned body/error: len=%d err=%v", fragment, len(body), err)
				}
				if r.closed {
					t.Fatal("failed response closed caller-owned source")
				}
			}
		})
	}
}

func TestReadOKResponseExplicitCapsBeforeReadAndExactBoundaries(t *testing.T) {
	good := "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nbody"
	for _, limits := range [][2]uint64{{0, 100}, {1<<20 + 1, 100}, {4, 0}, {4, 16<<10 + 1}, {^uint64(0), 100}} {
		r := &fragmentReader{source: strings.NewReader(good), fragment: 4096}
		if body, err := boundedhttp.ReadOKResponse(r, limits[0], limits[1]); !errors.Is(err, boundedhttp.ErrResponse) || body != nil || r.calls != 0 {
			t.Fatal("invalid explicit ceilings read source or returned data")
		}
	}
	if _, err := boundedhttp.ReadOKResponse(nil, 1, 1); !errors.Is(err, boundedhttp.ErrResponse) {
		t.Fatal("nil reader accepted")
	}
	header := strings.Index(good, "\r\n\r\n") + 4
	for _, tc := range []struct {
		body, headers uint64
		ok            bool
	}{{4, uint64(header), true}, {3, uint64(header), false}, {4, uint64(header - 1), false}} {
		r := &fragmentReader{source: strings.NewReader(good), fragment: 4096}
		body, err := boundedhttp.ReadOKResponse(r, tc.body, tc.headers)
		if (err == nil) != tc.ok {
			t.Fatalf("exact body/header boundary %+v: err=%v", tc, err)
		}
		if !tc.ok && body != nil {
			t.Fatal("failed boundary returned partial body")
		}
		if tc.headers < uint64(header) && r.bytes > int(tc.headers) {
			t.Fatal("header cap applied after bytes had reached parser")
		}
	}
	for _, wire := range []string{"HTTP/1.1 200 OK\r\n\r\nbodyx", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nbodyx\r\n0\r\n\r\n"} {
		if body, err := boundedhttp.ReadOKResponse(strings.NewReader(wire), 4, 1024); err == nil || body != nil {
			t.Fatal("missing/false Content-Length bypassed actual body ceiling")
		}
	}
}

type failingReader struct{ prefix *strings.Reader }

func (r *failingReader) Read(p []byte) (int, error) {
	if r.prefix.Len() > 0 {
		return r.prefix.Read(p)
	}
	return 0, errors.New("PRIVATE_TRANSPORT_DIAGNOSTIC")
}

func TestReadOKResponseErrorsAreGenericAndNoSourceDrain(t *testing.T) {
	r := &failingReader{prefix: strings.NewReader("HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nshort")}
	body, err := boundedhttp.ReadOKResponse(r, 10, 1024)
	if !errors.Is(err, boundedhttp.ErrResponse) || body != nil || strings.Contains(err.Error(), "PRIVATE_TRANSPORT_DIAGNOSTIC") {
		t.Fatal("partial response/raw transport error exported")
	}
	badHeader := "HTTP/1.1 200 OK\r\nSet-Cookie: private=value\r\nContent-Length: 4\r\n\r\n"
	source := &fragmentReader{source: strings.NewReader(badHeader + strings.Repeat("x", 10000)), fragment: 4096}
	if body, err := boundedhttp.ReadOKResponse(source, 4, 1024); err == nil || body != nil {
		t.Fatal("forbidden cookie response accepted")
	}
	if source.bytes != len(badHeader) || source.closed {
		t.Fatalf("error parsed/drained body or closed source: consumed=%d want=%d closed=%t", source.bytes, len(badHeader), source.closed)
	}
}

func TestReadOKResponseFramingBudgetAndFieldCeiling(t *testing.T) {
	const headers = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
	wire := headers + strings.Repeat("1\r\nx\r\n", 40) + "0\r\n\r\n"
	r := &fragmentReader{source: strings.NewReader(wire), fragment: 4096}
	if body, err := boundedhttp.ReadOKResponse(r, 100, uint64(len(headers))); err == nil || body != nil {
		t.Fatal("small decoded body bypassed finite chunk framing cap")
	}
	if r.bytes > 100+2*len(headers) {
		t.Fatal("reader exceeded total body+2*header wire ceiling")
	}
	for _, n := range []int{99, 100} {
		wire := "HTTP/1.1 200 OK\r\n" + strings.Repeat("X-A: x\r\n", n) + "Content-Length: 0\r\n\r\n"
		body, err := boundedhttp.ReadOKResponse(strings.NewReader(wire), 1, 16<<10)
		if n == 99 && err != nil {
			t.Fatalf("100-header boundary regressed: %v", err)
		}
		if n == 100 && (err == nil || body != nil) {
			t.Fatal("more than100 header fields accepted")
		}
	}
}
