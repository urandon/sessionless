package boundedhttp

import (
	"bufio"
	"io"
	"net/http"
)

// MCPResponseV1 is finite transport DATA. This strict initial arm is stateless:
// session IDs, cookies, authentication, redirects and protocol-version changes
// are unavailable, not permission to initialize/resume another connection.
type MCPResponseV1 struct {
	Body        []byte
	ContentType string
}

func ReadMCPResponseV1(source io.Reader, bodyLimit, headerLimit uint64, notification bool) (MCPResponseV1, error) {
	if source == nil || bodyLimit == 0 || bodyLimit > 256<<10 || headerLimit == 0 || headerLimit > 16<<10 {
		return MCPResponseV1{}, ErrResponse
	}
	r := &responseReader{source: source, bodyLimit: int64(bodyLimit), headerLimit: int(headerLimit), remaining: int64(bodyLimit + 2*headerLimit)}
	response, err := http.ReadResponse(bufio.NewReaderSize(r, 4096), &http.Request{Method: "POST"})
	if err != nil || !r.validHeaders() || response.Proto != "HTTP/1.1" || response.ContentLength > int64(bodyLimit) {
		return MCPResponseV1{}, ErrResponse
	}
	for _, name := range []string{"Content-Encoding", "Set-Cookie", "WWW-Authenticate", "Proxy-Authenticate", "Mcp-Session-Id", "Mcp-Protocol-Version"} {
		if len(response.Header.Values(name)) != 0 {
			return MCPResponseV1{}, ErrResponse
		}
	}
	status := http.StatusOK
	if notification {
		status = http.StatusAccepted
	}
	if response.StatusCode != status {
		return MCPResponseV1{}, ErrResponse
	}
	content := response.Header.Values("Content-Type")
	if (!notification && len(content) != 1) || (notification && len(content) > 1) {
		return MCPResponseV1{}, ErrResponse
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(bodyLimit)+1))
	if err != nil || uint64(len(body)) > bodyLimit || (notification && len(body) != 0) {
		clear(body)
		return MCPResponseV1{}, ErrResponse
	}
	value := ""
	if len(content) == 1 {
		value = content[0]
	}
	return MCPResponseV1{Body: body, ContentType: value}, nil
}
