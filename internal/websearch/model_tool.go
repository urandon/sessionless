package websearch

import (
	"fmt"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	modelToolNameV1  = "web_search"
	minimumResultsV1 = 1
	maximumResultsV1 = 5
	defaultResultsV1 = maximumResultsV1
)

// ModelToolV1 is the one closed Basic search catalog DATA contract. It neither
// registers a tool nor establishes installation, consent or serving authority.
// JSON Schema maxLength counts characters; CompileV2 independently enforces
// UTF-8 query bytes <=1024, nonblank text and the complete 4KiB argument bound.
func ModelToolV1() configuredinference.ToolV1 {
	return configuredinference.ToolV1{Name: modelToolNameV1, Description: "Search public web sources for a query.", Parameters: []byte(fmt.Sprintf(`{"type":"object","properties":{"query":{"type":"string","minLength":1,"maxLength":%d},"max_results":{"type":"integer","minimum":%d,"maximum":%d}},"required":["query"],"additionalProperties":false}`, domain.WebSearchQueryBytesV1, minimumResultsV1, maximumResultsV1))}
}
