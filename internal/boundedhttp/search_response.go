package boundedhttp

import (
	"bufio"
	"io"
	"net/http"
)

// SearchResponseV1 reports actual finite HTTP DATA only. A non-200 response is
// never no-effect evidence, a replay/refund right or a synthetic vendor result.
// The separate SEARCH codec classifies its exact status/type/body together.
type SearchResponseV1 struct {
	Body            []byte
	StatusCode      int
	ContentType     string
	ContentEncoding string
}

// ReadSearchResponseV1 reuses the original hard header/chunk/wire reader but
// has a separate SEARCH status language. It neither widens MODEL/MCP readers
// nor dials, writes, retries, drains, closes or follows redirects. The caller
// owns original deadlines, cancellation, connection disposal and returned bytes.
func ReadSearchResponseV1(source io.Reader, bodyLimit, headerLimit uint64) (SearchResponseV1, error) {
	if source == nil || bodyLimit == 0 || bodyLimit > 256<<10 || headerLimit == 0 || headerLimit > 16<<10 {
		return SearchResponseV1{}, ErrResponse
	}
	r := &responseReader{source: source, bodyLimit: int64(bodyLimit), headerLimit: int(headerLimit), remaining: int64(bodyLimit + 2*headerLimit)}
	defer func() { clear(r.header); clear(r.line) }()
	response, err := http.ReadResponse(bufio.NewReaderSize(r, 4096), &http.Request{Method: "POST"})
	if err != nil || !r.validHeaders() || response.Proto != "HTTP/1.1" || response.StatusCode < 200 || response.StatusCode > 599 || response.StatusCode >= 300 && response.StatusCode < 400 || response.ContentLength > int64(bodyLimit) {
		return SearchResponseV1{}, ErrResponse
	}
	for _, name := range []string{"Content-Encoding", "Set-Cookie", "WWW-Authenticate", "Proxy-Authenticate"} {
		if len(response.Header.Values(name)) != 0 {
			return SearchResponseV1{}, ErrResponse
		}
	}
	types := response.Header.Values("Content-Type")
	if len(types) != 1 {
		return SearchResponseV1{}, ErrResponse
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(bodyLimit)+1))
	if err != nil || uint64(len(body)) > bodyLimit {
		clear(body)
		return SearchResponseV1{}, ErrResponse
	}
	return SearchResponseV1{Body: body, StatusCode: response.StatusCode, ContentType: types[0]}, nil
}
