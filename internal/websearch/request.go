package websearch

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// FrozenRequestV1 owns copies of original arguments and the exact compiled wire.
// Its private state prevents accidental mutation, NOT fabrication of authority.
type FrozenRequestV1 struct {
	grant                                                 domain.WebSearchGrantV1
	binding                                               BindingV1
	arguments, wire                                       []byte
	query                                                 string
	maximum                                               int
	argumentsDigest, queryDigest, wireDigest, grantDigest string
	compiledAt, validUntil                                time.Time
	// validUntil is the original grant/price upper bound, even after binding.
	// Only BindOperationV1 fills operationDeadline; a caller-supplied digest
	// alone cannot establish the operation's narrower response window.
	operationDeadline time.Time
}

func (*FrozenRequestV1) String() string {
	return "WebSearchRequest{arguments:[redacted] query:[redacted] wire:[redacted]}"
}
func (f *FrozenRequestV1) GoString() string { return f.String() }
func (f *FrozenRequestV1) Arguments() []byte {
	if f == nil {
		return nil
	}
	return bytes.Clone(f.arguments)
}
func (f *FrozenRequestV1) Body() []byte {
	if f == nil {
		return nil
	}
	return bytes.Clone(f.wire)
}
func (f *FrozenRequestV1) Digests() (arguments, query, wire string) {
	if f == nil {
		return "", "", ""
	}
	return f.argumentsDigest, f.queryDigest, f.wireDigest
}

func CompileV1(g domain.WebSearchGrantV1, cost domain.ExternalServiceCostCeilingV1, b BindingV1, args []byte, at time.Time) (*FrozenRequestV1, error) {
	if g.ValidateCostAt(cost, at) != nil || b.validate(g) != nil {
		return nil, ErrContract
	}
	v, e := strictJSON(args, domain.WebSearchArgumentsBytesV1)
	m, ok := v.(map[string]any)
	if e != nil || !ok || !keys(m, "query", "max_results") {
		return nil, ErrContract
	}
	query, ok := m["query"].(string)
	if !ok || strings.TrimSpace(query) == "" || len(query) > domain.WebSearchQueryBytesV1 || !utf8.ValidString(query) {
		return nil, ErrContract
	}
	maximum := 5
	if value, exists := m["max_results"]; exists {
		n, ok := value.(json.Number)
		if !ok {
			return nil, ErrContract
		}
		i, e := n.Int64()
		if e != nil || i < 1 || i > 5 {
			return nil, ErrContract
		}
		maximum = int(i)
	}
	// Explicit supported options, never SDK/vendor defaults or credential bytes.
	request := struct {
		Query                    string `json:"query"`
		MaxResults               int    `json:"max_results"`
		SearchDepth              string `json:"search_depth"`
		Topic                    string `json:"topic"`
		Language                 string `json:"language"`
		FilterByLanguage         bool   `json:"filter_by_language"`
		SafeSearch               bool   `json:"safe_search"`
		AutoParameters           bool   `json:"auto_parameters"`
		IncludeUsage             bool   `json:"include_usage"`
		IncludeAnswer            bool   `json:"include_answer"`
		IncludeRawContent        bool   `json:"include_raw_content"`
		IncludeImages            bool   `json:"include_images"`
		IncludeImageDescriptions bool   `json:"include_image_descriptions"`
		IncludeFavicon           bool   `json:"include_favicon"`
	}{query, maximum, "basic", "general", "en", true, true, false, true, false, false, false, false, false}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if encoder.Encode(request) != nil {
		return nil, ErrContract
	}
	body := bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'})
	if len(body) > domain.WebSearchWireBytesV1 {
		return nil, ErrContract
	}
	// Compilation does not begin a search operation or shorten consent waiting
	// to fifteen seconds. BindOperation later seals its already admitted deadline.
	until := g.ExpiresAt
	if cost.PriceExpiresAt.Before(until) {
		until = cost.PriceExpiresAt
	}
	gd, _ := g.Digest()
	return &FrozenRequestV1{grant: g, binding: b, arguments: bytes.Clone(args), wire: bytes.Clone(body), query: query, maximum: maximum, compiledAt: at, validUntil: until,
		argumentsDigest: digest(args), queryDigest: digest([]byte(query)), wireDigest: digest(body), grantDigest: gd}, nil
}
