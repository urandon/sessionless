package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/strictjson"
)

const (
	// The explicit citation instruction's complete final JSON text shares this
	// fixed ceiling with the decoded answer. Caller admission may be shorter.
	CitedAnswerBytesV1             = 16 << 10
	AnswerCitationManifestSchemaV1 = "sessionless.answer-citations.v1"
	AnswerCitationManifestBytesV1  = 64 << 10
	AnswerCitationSourcesV1        = 10
	AnswerCitationSearchExcerptV1  = "search_excerpt"
)

// AnswerCitationManifestV1 is public provenance DATA, not proof of durable
// observations, current membership, original ownership or permission to publish.
// The original terminal owner must resolve it from its authenticated frontier.
// No query, excerpt, consent receipt, credential or claim-support flag belongs
// in this closed representation. Sources retain answer-reference order.
type AnswerCitationManifestV1 struct {
	Schema    string                   `json:"schema"`
	RunID     RunID                    `json:"run_id"`
	AttemptID AttemptID                `json:"attempt_id"`
	Sources   []AnswerCitationSourceV1 `json:"sources"`
}

type AnswerCitationSourceV1 struct {
	SourceID          string                              `json:"source_id"`
	Rank              uint32                              `json:"rank"`
	URL               string                              `json:"url"`
	Title             string                              `json:"title"`
	TitleTruncated    bool                                `json:"title_truncated"`
	OperationDigest   AttemptOperationDigestV1            `json:"operation_digest"`
	ObservationDigest AttemptOperationObservationDigestV1 `json:"observation_digest"`
	ResponseDigest    string                              `json:"response_digest"`
	RetrievedAt       time.Time                           `json:"retrieved_at"`
	PublishedAt       *time.Time                          `json:"published_at,omitempty"`
	ContentKind       string                              `json:"content_kind"`
}

func (AnswerCitationManifestV1) String() string {
	return "AnswerCitationManifestV1{content:[redacted]}"
}
func (v AnswerCitationManifestV1) GoString() string           { return v.String() }
func (v AnswerCitationManifestV1) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, v.String()) }
func (AnswerCitationSourceV1) String() string                 { return "AnswerCitationSourceV1{content:[redacted]}" }
func (v AnswerCitationSourceV1) GoString() string             { return v.String() }
func (v AnswerCitationSourceV1) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, v.String()) }

func answerCitationInvalid() error {
	return ValidationError{Field: "answer_citations", Reason: "invalid bounded citation data"}
}

func (v AnswerCitationManifestV1) Clone() AnswerCitationManifestV1 {
	if v.Sources != nil {
		v.Sources = append([]AnswerCitationSourceV1{}, v.Sources...)
	}
	for i := range v.Sources {
		if p := v.Sources[i].PublishedAt; p != nil {
			copy := *p
			v.Sources[i].PublishedAt = &copy
		}
	}
	return v
}

func (v AnswerCitationManifestV1) validateFields() error {
	if v.Schema != AnswerCitationManifestSchemaV1 || v.RunID.Validate() != nil || v.AttemptID.Validate() != nil || v.Sources == nil || len(v.Sources) > AnswerCitationSourcesV1 {
		return answerCitationInvalid()
	}
	seen := make(map[string]bool, len(v.Sources))
	for _, s := range v.Sources {
		if s.OperationDigest.Validate() != nil || s.ObservationDigest.Validate() != nil || ValidateSHA256Digest("answer_citations.response_digest", s.ResponseDigest) != nil ||
			s.Rank < 1 || s.Rank > 5 || s.SourceID != WebSearchSourceIDV1(s.OperationDigest, s.Rank, s.URL) || seen[s.SourceID] ||
			ValidateWebSearchCitationURLV1(s.URL) != nil || len(s.Title) == 0 || len(s.Title) > 512 || !utf8.ValidString(s.Title) ||
			!ownedCurrentTime(s.RetrievedAt) || s.PublishedAt != nil && !ownedCurrentTime(*s.PublishedAt) || s.ContentKind != AnswerCitationSearchExcerptV1 {
			return answerCitationInvalid()
		}
		seen[s.SourceID] = true
	}
	return nil
}

func (v AnswerCitationManifestV1) Validate() error {
	body, err := EncodeAnswerCitationManifestV1(v)
	clear(body)
	return err
}

// EncodeAnswerCitationManifestV1 is the sole canonical encoding for both real
// payloads and prospective fit. The byte cap is not an added terminal allowance;
// the original caller must also charge its shorter admitted/copy/output limits.
func EncodeAnswerCitationManifestV1(v AnswerCitationManifestV1) ([]byte, error) {
	if v.validateFields() != nil {
		return nil, answerCitationInvalid()
	}
	body, err := json.Marshal(v)
	if err != nil || len(body) > AnswerCitationManifestBytesV1 {
		clear(body)
		return nil, answerCitationInvalid()
	}
	return body, nil
}

// DecodeAnswerCitationManifestV1 accepts only this explicit canonical schema.
// The bounded lexical pass precedes typed decoding. Exact reencoding also
// rejects missing false fields, explicit null, aliases and noncanonical times.
// Structural validation cannot authenticate a durable operation or its scope.
func DecodeAnswerCitationManifestV1(body []byte) (AnswerCitationManifestV1, error) {
	if strictjson.ValidateDocument(body, AnswerCitationManifestBytesV1, 5, 160) != nil {
		return AnswerCitationManifestV1{}, answerCitationInvalid()
	}
	var v AnswerCitationManifestV1
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&v) != nil || d.Decode(new(any)) != io.EOF {
		return AnswerCitationManifestV1{}, answerCitationInvalid()
	}
	canonical, err := EncodeAnswerCitationManifestV1(v)
	defer clear(canonical)
	if err != nil || !bytes.Equal(canonical, body) {
		return AnswerCitationManifestV1{}, answerCitationInvalid()
	}
	return v, nil
}
