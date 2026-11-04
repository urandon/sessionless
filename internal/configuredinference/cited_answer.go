package configuredinference

import (
	"fmt"
	"io"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	CitedAnswerInstructionVersionV1 = "sessionless.owned-agent.citations.v1"
	CitedAnswerSchemaV1             = "sessionless.cited-answer.v1"
	MaxCitedAnswerBytesV1           = domain.CitedAnswerBytesV1
	MaxCitedAnswerSourcesV1         = 10
)

// CitedAnswerV1 is untrusted candidate DATA. It establishes neither durable
// source provenance nor permission to publish. The original terminal owner must
// independently resolve every ID against its authenticated operation frontier.
type CitedAnswerV1 struct {
	answer    string
	sourceIDs []string
}

func (CitedAnswerV1) String() string               { return "CitedAnswerV1{content:[redacted]}" }
func (v CitedAnswerV1) GoString() string           { return v.String() }
func (v CitedAnswerV1) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, v.String()) }
func (CitedAnswerV1) MarshalJSON() ([]byte, error) { return nil, ErrContract }

func (v *CitedAnswerV1) Answer() string {
	if v == nil {
		return ""
	}
	return v.answer
}

func (v *CitedAnswerV1) SourceIDs() []string {
	if v == nil {
		return nil
	}
	return append([]string{}, v.sourceIDs...)
}

// DecodeCitedAnswerV1 is only a bounded candidate decoder, not an automatic
// final-response format detector. A caller must first select the explicitly
// admitted instruction version. This function performs no source lookup, I/O,
// fallback, summary normalization or generation; its maximum is contained in
// the original admitted final-text allowance, including all JSON overhead.
func DecodeCitedAnswerV1(body []byte, finalTextBytes uint64) (*CitedAnswerV1, error) {
	limit := min(finalTextBytes, uint64(MaxCitedAnswerBytesV1))
	value, err := strictJSON(body, limit, 3, 16)
	if err != nil {
		return nil, ErrContract
	}
	m, ok := object(value)
	if !ok || len(m) != 3 || !exactKeys(m, "schema", "answer", "source_ids") || m["schema"] != CitedAnswerSchemaV1 {
		return nil, ErrContract
	}
	answer, ok := m["answer"].(string)
	// Outer JSON can encode U+0000 without containing a literal NUL. Apply the
	// existing final-text scalar contract AFTER decoding as well as before
	// any future canonical/context encoding. Preserve accepted text exactly.
	if !ok || !validString(answer, limit, false) {
		return nil, ErrContract
	}
	ids, ok := m["source_ids"].([]any)
	if !ok || len(ids) > MaxCitedAnswerSourcesV1 {
		return nil, ErrContract
	}
	out := &CitedAnswerV1{answer: answer, sourceIDs: make([]string, 0, len(ids))}
	seen := make(map[string]bool, len(ids))
	for _, item := range ids {
		id, ok := item.(string)
		if !ok || !strings.HasPrefix(id, "source-") || domain.ValidateSHA256Digest("cited_answer.source_id", strings.TrimPrefix(id, "source-")) != nil || seen[id] {
			return nil, ErrContract
		}
		seen[id] = true
		out.sourceIDs = append(out.sourceIDs, id)
	}
	return out, nil
}
