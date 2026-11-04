package canonicaloutput

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

const CitedAssistantMessageSchemaV2 = "sessionless.assistant-message.v2"

// This is a parser/allocation ceiling derived from the existing component
// maxima, NOT an additional admitted output budget. Every call also supplies
// its actual shorter payload and final-text allowances, including zero.
const citedAssistantMessageMaxBytesV2 = domain.AnswerCitationManifestBytesV1 + 6*configuredinference.MaxCitedAnswerBytesV1 + 1024

// CitedAssistantMessageV2 is immutable-payload DATA. A valid manifest supplied
// by a caller is not a trusted resolver, completion seal or publication right.
// Nothing here selects the admitted instruction version or activates a reader.
type CitedAssistantMessageV2 struct {
	Schema             string                          `json:"schema"`
	Summary            string                          `json:"summary"`
	ArtifactManifestID domain.ArtifactManifestID       `json:"artifact_manifest_id"`
	Citations          domain.AnswerCitationManifestV1 `json:"citations"`
}

func (CitedAssistantMessageV2) String() string               { return "CitedAssistantMessageV2{content:[redacted]}" }
func (v CitedAssistantMessageV2) GoString() string           { return v.String() }
func (v CitedAssistantMessageV2) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, v.String()) }

func citedAssistantInvalidV2() error {
	return domain.ValidationError{Field: "assistant_message.v2", Reason: "invalid bounded cited answer"}
}

func (v CitedAssistantMessageV2) Clone() CitedAssistantMessageV2 {
	v.Citations = v.Citations.Clone()
	return v
}

// EncodeCitedAssistantMessageV2 preserves the already-decoded answer exactly.
// In particular it never uses v1's NormalizedSummary trim/fallback path. It
// performs no source lookup, tool dispatch, upload or legacy format fallback.
func EncodeCitedAssistantMessageV2(v CitedAssistantMessageV2, finalTextBytes, payloadBytes uint64) ([]byte, error) {
	textLimit := min(finalTextBytes, uint64(configuredinference.MaxCitedAnswerBytesV1))
	limit := min(payloadBytes, uint64(citedAssistantMessageMaxBytesV2))
	if v.Schema != CitedAssistantMessageSchemaV2 || len(v.Summary) == 0 || uint64(len(v.Summary)) > textLimit || !utf8.ValidString(v.Summary) || strings.ContainsRune(v.Summary, 0) || v.ArtifactManifestID.Validate() != nil || limit == 0 {
		return nil, citedAssistantInvalidV2()
	}
	manifest, err := domain.EncodeAnswerCitationManifestV1(v.Citations)
	if err != nil {
		return nil, citedAssistantInvalidV2()
	}
	defer clear(manifest)
	// RawMessage embeds the sole manifest encoding, not a second representation.
	// Its escaping is already json.Marshal's escaping, so enclosing it preserves
	// exactly those canonical bytes. Compare and validate again in the reader.
	wire := struct {
		Schema             string                    `json:"schema"`
		Summary            string                    `json:"summary"`
		ArtifactManifestID domain.ArtifactManifestID `json:"artifact_manifest_id"`
		Citations          json.RawMessage           `json:"citations"`
	}{v.Schema, v.Summary, v.ArtifactManifestID, manifest}
	body, err := json.Marshal(wire)
	if err != nil || uint64(len(body)) > limit {
		clear(body)
		return nil, citedAssistantInvalidV2()
	}
	return body, nil
}

// DecodeCitedAssistantMessageV2 is explicit v2 DATA decoding, not a general
// message detector or permission check. Existing v1 codecs/consumers remain
// untouched until their separately verified version-dispatch integration.
func DecodeCitedAssistantMessageV2(body []byte, finalTextBytes, payloadBytes uint64) (CitedAssistantMessageV2, error) {
	limit := min(payloadBytes, uint64(citedAssistantMessageMaxBytesV2))
	if strictjson.ValidateDocument(body, limit, 6, 160) != nil {
		return CitedAssistantMessageV2{}, citedAssistantInvalidV2()
	}
	var wire struct {
		Schema             string                    `json:"schema"`
		Summary            string                    `json:"summary"`
		ArtifactManifestID domain.ArtifactManifestID `json:"artifact_manifest_id"`
		Citations          json.RawMessage           `json:"citations"`
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&wire) != nil || d.Decode(new(any)) != io.EOF {
		return CitedAssistantMessageV2{}, citedAssistantInvalidV2()
	}
	manifest, err := domain.DecodeAnswerCitationManifestV1(wire.Citations)
	if err != nil {
		return CitedAssistantMessageV2{}, citedAssistantInvalidV2()
	}
	v := CitedAssistantMessageV2{Schema: wire.Schema, Summary: wire.Summary, ArtifactManifestID: wire.ArtifactManifestID, Citations: manifest}
	canonical, err := EncodeCitedAssistantMessageV2(v, finalTextBytes, payloadBytes)
	defer clear(canonical)
	if err != nil || !bytes.Equal(canonical, body) {
		return CitedAssistantMessageV2{}, citedAssistantInvalidV2()
	}
	return v, nil
}
