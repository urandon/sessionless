package websearch

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"mime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// DecodeResponseV1 performs no transport or retries. The supplied Reader must
// already be the original transport's deadline/cancellation-aware body. This
// function BORROWS the reader; the caller retains Close ownership on all paths.
// Context
// is checked before/after each bounded read; a generic blocking io.Reader cannot
// be interrupted here without an unbounded helper goroutine. Real 15s send/read
// enforcement therefore remains with the required owned transport integration.
// Only identity encoding is supported: encoded and decompressed bytes coincide.
func (f *FrozenRequestV1) DecodeResponseV1(ctx context.Context, meta HTTPMetadataV1, r io.Reader, retrievedAt time.Time) (domain.WebSearchResultV1, error) {
	zero := domain.WebSearchResultV1{}
	if f == nil || ctx == nil || ctx.Err() != nil || r == nil || f.binding.validate(f.grant) != nil || f.binding.OperationDigest.Validate() != nil || f.operationDeadline.IsZero() || retrievedAt.IsZero() || retrievedAt.Before(f.compiledAt) || !retrievedAt.Before(f.operationDeadline) || !retrievedAt.Before(f.validUntil) || meta.StatusCode < 200 || meta.StatusCode > 599 {
		return zero, ErrContract
	}
	contentType, params, e := mime.ParseMediaType(meta.ContentType)
	if e != nil || contentType != "application/json" || (meta.ContentEncoding != "" && meta.ContentEncoding != "identity") {
		return zero, ErrResponse
	}
	for key, value := range params {
		if key != "charset" || !strings.EqualFold(value, "utf-8") {
			return zero, ErrResponse
		}
	}
	raw := make([]byte, domain.WebSearchResponseBytesV1+1)
	n := 0
	defer clear(raw)
	for {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		count, readErr := r.Read(raw[n:])
		if count < 0 || count > len(raw)-n {
			return zero, ErrResponse
		}
		n += count
		if n > domain.WebSearchResponseBytesV1 {
			return zero, ErrOverflow
		}
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return zero, ErrResponse
		}
		if count == 0 {
			return zero, ErrResponse
		}
	}
	v, e := strictJSON(raw[:n], domain.WebSearchResponseBytesV1)
	root, ok := v.(map[string]any)
	if e != nil || !ok || !keys(root, "query", "answer", "follow_up_questions", "images", "results", "response_time", "request_id", "usage", "error", "detail") {
		return zero, ErrResponse
	}
	result := domain.WebSearchResultV1{Version: 1, Scope: f.binding.Scope, OperationDigest: f.binding.OperationDigest, Sequence: f.binding.Sequence, CallID: f.binding.CallID,
		ProfileRevision: f.grant.ProfileRevision, GrantDigest: f.grantDigest, QueryDigest: f.queryDigest, WireRequestDigest: f.wireDigest, RawResponseDigest: digest(raw[:n]), Provider: "tavily",
		RetrievedAt: retrievedAt, Outcome: "known_empty", Usage: domain.WebSearchUsageV1{State: "unknown"}, Sources: []domain.WebSearchSourceV1{}, Warnings: []domain.WebSearchWarningV1{}}
	if query, exists := root["query"]; exists {
		if query != f.query {
			return zero, ErrResponse
		}
	}
	if root["answer"] != nil || root["follow_up_questions"] != nil {
		return zero, ErrResponse
	}
	if images, exists := root["images"]; exists && images != nil {
		list, ok := images.([]any)
		if !ok || len(list) != 0 {
			return zero, ErrResponse
		}
	}
	if usage, exists := root["usage"]; exists && usage != nil {
		m, ok := usage.(map[string]any)
		if !ok || !keys(m, "credits") {
			return zero, ErrResponse
		}
		if credits := m["credits"]; credits != nil {
			number, ok := credits.(json.Number)
			if !ok {
				return zero, ErrResponse
			}
			count, e := strconv.ParseUint(string(number), 10, 64)
			if e != nil || count > 9007199254740991 {
				return zero, ErrResponse
			}
			result.Usage = domain.WebSearchUsageV1{State: "provider_reported", Credits: &count}
		}
	}
	if responseTime, exists := root["response_time"]; exists {
		number, ok := responseTime.(json.Number)
		if !ok {
			return zero, ErrResponse
		}
		n, e := number.Float64()
		if e != nil || n < 0 || math.IsInf(n, 0) || math.IsNaN(n) {
			return zero, ErrResponse
		}
	}
	if id, exists := root["request_id"]; exists {
		if s, ok := id.(string); !ok || len(s) > 1024 {
			return zero, ErrResponse
		}
	}
	serviceError := meta.StatusCode != 200 || root["error"] != nil || root["detail"] != nil
	if serviceError {
		if results, exists := root["results"]; exists && results != nil {
			a, ok := results.([]any)
			if !ok || len(a) != 0 {
				return zero, ErrResponse
			}
		}
		result.Outcome = "known_service_error"
		if e := result.Validate(); e != nil {
			return zero, ErrResponse
		}
		return result, nil
	}
	sources, ok := root["results"].([]any)
	if !ok || len(sources) > f.maximum {
		return zero, ErrResponse
	}
	seen := map[string]bool{}
	for i, rawSource := range sources {
		rank := uint32(i + 1)
		source, ok := rawSource.(map[string]any)
		if !ok {
			result.Warnings = append(result.Warnings, domain.WebSearchWarningV1{Rank: rank, Code: "invalid_source"})
			continue
		}
		if !keys(source, "url", "title", "content", "score", "raw_content", "published_date", "favicon") || source["raw_content"] != nil || source["favicon"] != nil {
			return zero, ErrResponse
		}
		uri, uriOK := source["url"].(string)
		title, titleOK := source["title"].(string)
		excerpt, excerptOK := source["content"].(string)
		if !uriOK || !titleOK || !excerptOK || strings.TrimSpace(title) == "" || domain.ValidateWebSearchCitationURLV1(uri) != nil {
			result.Warnings = append(result.Warnings, domain.WebSearchWarningV1{Rank: rank, Code: "invalid_source"})
			continue
		}
		if score, exists := source["score"]; exists {
			number, ok := score.(json.Number)
			if !ok {
				return zero, ErrResponse
			}
			value, e := number.Float64()
			if e != nil || math.IsInf(value, 0) || math.IsNaN(value) || value < 0 || value > 1 {
				return zero, ErrResponse
			}
		}
		if seen[uri] {
			result.Warnings = append(result.Warnings, domain.WebSearchWarningV1{Rank: rank, Code: "duplicate_source"})
			continue
		}
		seen[uri] = true
		normal := domain.WebSearchSourceV1{SourceID: domain.WebSearchSourceIDV1(result.OperationDigest, rank, uri), Rank: rank, URL: uri}
		normal.Title, normal.TitleTruncated = clipUTF8(title, 512)
		normal.Excerpt, normal.ExcerptTruncated = clipUTF8(excerpt, 2048)
		if date, exists := source["published_date"]; exists && date != nil {
			if value, ok := date.(string); ok {
				if at, err := parsePublication(value); err == nil {
					normal.PublishedAt = &at
				} else {
					result.Warnings = append(result.Warnings, domain.WebSearchWarningV1{Rank: rank, Code: "unknown_publication_time"})
				}
			} else {
				result.Warnings = append(result.Warnings, domain.WebSearchWarningV1{Rank: rank, Code: "unknown_publication_time"})
			}
		}
		result.Sources = append(result.Sources, normal)
	}
	if len(result.Sources) > 0 {
		result.Outcome = "known_results"
	}
	if result.Validate() != nil {
		return zero, ErrOverflow
	}
	return result, nil
}

func clipUTF8(s string, maximum int) (string, bool) {
	if len(s) <= maximum {
		return s, false
	}
	end := maximum
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	// Do not retain an oversized provider string's backing allocation through
	// a short exported excerpt/title prefix.
	return strings.Clone(s[:end]), true
}
func parsePublication(s string) (time.Time, error) {
	at, e := time.Parse(time.RFC3339Nano, s)
	if e != nil {
		return time.Time{}, ErrResponse
	}
	at = at.UTC().Truncate(time.Microsecond)
	if at.IsZero() || at.Year() < 1 || at.Year() > 9999 {
		return time.Time{}, ErrResponse
	}
	return at, nil
}
