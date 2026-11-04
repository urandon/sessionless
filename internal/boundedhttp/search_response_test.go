package boundedhttp

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestSearchResponseActualStatusAndBodyRemainDistinct(t *testing.T) {
	for _, status := range []int{200, 201, 400, 401, 403, 429, 500, 599} {
		body := []byte(`{"error":"bounded service response"}`)
		wire := fmt.Sprintf("HTTP/1.1 %d status\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", status, len(body), body)
		actual, err := ReadSearchResponseV1(strings.NewReader(wire), uint64(len(body)), 16<<10)
		if err != nil || actual.StatusCode != status || actual.ContentType != "application/json" || actual.ContentEncoding != "" || !bytes.Equal(actual.Body, body) {
			t.Fatal("actual HTTP DATA lost", status, err)
		}
		clear(actual.Body)
		if !bytes.Equal(body, []byte(`{"error":"bounded service response"}`)) {
			t.Fatal("owned response aliased caller bytes")
		}
		if status != 200 {
			if _, err := ReadOKResponse(strings.NewReader(wire), 256<<10, 16<<10); err == nil {
				t.Fatal("SEARCH widened old MODEL status language")
			}
			if _, err := ReadMCPResponseV1(strings.NewReader(wire), 256<<10, 16<<10, false); err == nil {
				t.Fatal("SEARCH widened old MCP status language")
			}
		}
	}
}

func TestSearchResponseRejectsRedirectEncodingAuthorityAndOverflow(t *testing.T) {
	body := `{"results":[]}`
	base := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	for name, wire := range map[string]string{
		"redirect":         strings.Replace(base, "200 OK", "302 Redirect", 1),
		"encoding":         strings.Replace(base, "Content-Type:", "Content-Encoding: gzip\r\nContent-Type:", 1),
		"cookie":           strings.Replace(base, "Content-Type:", "Set-Cookie: x=y\r\nContent-Type:", 1),
		"auth":             strings.Replace(base, "Content-Type:", "WWW-Authenticate: Basic\r\nContent-Type:", 1),
		"proxy-auth":       strings.Replace(base, "Content-Type:", "Proxy-Authenticate: Basic\r\nContent-Type:", 1),
		"trailer":          strings.Replace(base, "Content-Type:", "Trailer: x\r\nContent-Type:", 1),
		"type-duplicate":   strings.Replace(base, "Content-Type:", "Content-Type: application/json\r\nContent-Type:", 1),
		"type-missing":     strings.Replace(base, "Content-Type: application/json\r\n", "", 1),
		"body-overflow":    strings.Replace(base, fmt.Sprintf("Content-Length: %d", len(body)), "Content-Length: 262145", 1),
		"headers-overflow": strings.Replace(base, "Content-Type:", "X-Large: "+strings.Repeat("a", 16<<10)+"\r\nContent-Type:", 1),
		"protocol":         strings.Replace(base, "HTTP/1.1", "HTTP/1.0", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if out, err := ReadSearchResponseV1(strings.NewReader(wire), 256<<10, 16<<10); err == nil || len(out.Body) != 0 {
				t.Fatal("unsafe SEARCH HTTP DATA accepted")
			}
		})
	}
	chunked := "HTTP/1.1 429 Rate limited\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n"
	if out, err := ReadSearchResponseV1(strings.NewReader(chunked), 2, 16<<10); err != nil || out.StatusCode != 429 || string(out.Body) != "{}" {
		t.Fatal("bounded actual chunked service error", err)
	}
	if out, err := ReadSearchResponseV1(strings.NewReader(strings.TrimSuffix(chunked, "\r\n")+"X-Trailer: taint\r\n\r\n"), 2, 16<<10); err == nil || len(out.Body) != 0 {
		t.Fatal("chunk trailer accepted")
	}
}
