// Package boundedhttp reads a single bounded HTTP/1.1 200 response. It owns no
// connection, context, deadline, authentication, route or effect authority.
package boundedhttp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
)

var ErrResponse = errors.New("bounded HTTP response is invalid")

// ReadOKResponse returns an owned opaque body, not model acceptance. Limits are
// explicit positive ceilings of at most 1 MiB and 16 KiB. The caller owns the
// reader, cancellation/deadlines and connection cleanup. This function never
// closes or drains the source; it has no dial, write, retry or approval seam.
func ReadOKResponse(source io.Reader, bodyLimit, headerLimit uint64) ([]byte, error) {
	if source == nil || bodyLimit == 0 || bodyLimit > 1<<20 || headerLimit == 0 || headerLimit > 16<<10 {
		return nil, ErrResponse
	}
	reader := &responseReader{source: source, bodyLimit: int64(bodyLimit), headerLimit: int(headerLimit), remaining: int64(bodyLimit + 2*headerLimit)}
	response, err := http.ReadResponse(bufio.NewReaderSize(reader, 4096), &http.Request{Method: "POST"})
	if err != nil {
		return nil, ErrResponse
	}
	// Closing the caller-owned connection, not draining Body.Close, remains the
	// cleanup boundary. Header policy is identical to the original HTTPS reader.
	if !reader.validHeaders() || response.Proto != "HTTP/1.1" || response.StatusCode != http.StatusOK ||
		len(response.Header.Values("Content-Encoding")) != 0 || response.ContentLength > int64(bodyLimit) ||
		len(response.Header.Values("Set-Cookie")) != 0 || len(response.Header.Values("WWW-Authenticate")) != 0 ||
		len(response.Header.Values("Proxy-Authenticate")) != 0 {
		return nil, ErrResponse
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(bodyLimit)+1))
	if err != nil || uint64(len(body)) > bodyLimit {
		clear(body)
		return nil, ErrResponse
	}
	return body, nil
}

// Hard-cap headers before http.ReadResponse can allocate a large line/map.
// One-byte header reads prevent buffered body bytes from bypassing that cap.
// A separate finite wire cap bounds chunk framing and trailer overhead too.
type responseReader struct {
	source         io.Reader
	headerLimit    int
	remaining      int64
	header         []byte
	headerDone     bool
	bodyLimit      int64
	chunked        bool
	chunkState     uint8 // size, data, data CRLF, final empty trailer, complete
	chunkRemaining int64
	chunkBytes     int64
	framingBytes   int
	line           []byte
}

func (r *responseReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining <= 0 || (!r.headerDone && len(r.header) >= r.headerLimit) {
		return 0, ErrResponse
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	if !r.headerDone {
		p = p[:1]
	} else if r.chunked {
		if r.chunkState == 4 {
			return 0, io.EOF
		}
		if r.chunkState == 1 {
			if int64(len(p)) > r.chunkRemaining {
				p = p[:r.chunkRemaining]
			}
		} else {
			p = p[:1]
		}
	}
	n, err := r.source.Read(p)
	r.remaining -= int64(n)
	if !r.headerDone && n != 0 {
		r.header = append(r.header, p[:n]...)
		r.headerDone = bytes.HasSuffix(r.header, []byte("\r\n\r\n"))
		if r.headerDone {
			if !r.validHeaders() {
				return 0, ErrResponse
			}
			for _, line := range bytes.Split(r.header, []byte("\r\n")) {
				name, _, _ := bytes.Cut(line, []byte(":"))
				if strings.EqualFold(string(name), "Transfer-Encoding") {
					r.chunked = true
				}
			}
		}
	} else if r.chunked && n > 0 {
		if r.chunkState == 1 {
			r.chunkRemaining -= int64(n)
			if r.chunkRemaining == 0 {
				r.chunkState = 2
			}
		} else {
			r.framingBytes += n
			r.line = append(r.line, p[:n]...)
			if r.framingBytes > r.headerLimit || len(r.line) > 256 {
				return 0, ErrResponse
			}
			if r.chunkState == 2 || r.chunkState == 3 {
				// Trailers are unsupported, even if unannounced; only the final
				// empty CRLF may reach the standard parser. Never parse a map
				// of attacker-selected trailers or a second response.
				if r.line[0] != '\r' || (len(r.line) == 2 && r.line[1] != '\n') {
					return 0, ErrResponse
				}
				if len(r.line) == 2 {
					if r.chunkState == 3 {
						r.chunkState = 4
					} else {
						r.chunkState = 0
					}
					r.line = nil
				}
			} else if bytes.HasSuffix(r.line, []byte("\r\n")) {
				for _, b := range r.line[:len(r.line)-2] {
					if !((b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')) {
						return 0, ErrResponse
					}
				}
				size, parseErr := strconv.ParseUint(string(r.line[:len(r.line)-2]), 16, 63)
				if parseErr != nil || size > uint64(r.bodyLimit-r.chunkBytes) {
					return 0, ErrResponse
				}
				r.chunkBytes += int64(size)
				r.chunkRemaining = int64(size)
				r.line = nil
				if size == 0 {
					r.chunkState = 3
				} else {
					r.chunkState = 1
				}
			}
		}
	}
	return n, err
}

func (r *responseReader) validHeaders() bool {
	if !r.headerDone {
		return false
	}
	lines := bytes.Split(r.header, []byte("\r\n"))
	if len(lines) > 103 {
		return false
	}
	for _, line := range lines {
		if bytes.ContainsAny(line, "\r\n") {
			return false
		}
	}
	cl, te := 0, 0
	for _, line := range lines[1 : len(lines)-2] {
		if len(line) == 0 || line[0] == ' ' || line[0] == '\t' {
			return false
		}
		name, value, found := bytes.Cut(line, []byte(":"))
		if !found || len(name) == 0 {
			return false
		}
		// Validate RFC 9110 tchar before the standard parser can accept a
		// non-token key that hides framing, cookie or encoding semantics.
		for _, b := range name {
			if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(b))) {
				return false
			}
		}
		switch strings.ToLower(string(name)) {
		case "trailer":
			return false
		case "content-length":
			cl++
			v := strings.TrimSpace(string(value))
			if v == "" {
				return false
			}
			for _, b := range []byte(v) {
				if b < '0' || b > '9' {
					return false
				}
			}
		case "transfer-encoding":
			te++
			if strings.TrimSpace(strings.ToLower(string(value))) != "chunked" {
				return false
			}
		}
	}
	return cl <= 1 && te <= 1 && !(cl != 0 && te != 0)
}
