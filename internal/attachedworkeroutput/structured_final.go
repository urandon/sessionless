package attachedworkeroutput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Fixed strict provider schema. The provider returns [] for answer-only turns;
// the parser also preserves the original absent-list answer-only semantics.
// Schema bytes/data are NOT source, model, process or upload authorization.
const StructuredFinalSchemaV1 = `{"type":"object","properties":{"answer":{"type":"string","maxLength":32000},"artifacts":{"type":"array","maxItems":2,"items":{"type":"object","properties":{"name":{"type":"string"},"media_type":{"type":"string","enum":["text/plain","application/json"]},"content":{"type":"string"}},"required":["name","media_type","content"],"additionalProperties":false}}},"required":["answer","artifacts"],"additionalProperties":false}`

// The caller supplies the SAME original accumulated ledger; this package does
// not construct/reset a budget, issue a fit certificate or mint producer proof.
type StructuredFinalAllocationV1 interface {
	Reserve(copyBytes, liveBytes, nodes uint64) error
}

// These are parsed DATA only. A genuine same-run owner must separately bind
// the supervisor observation, identity, schema/profile, finalization/release
// and original upload authority before creating a private producer carrier.
type StructuredArtifactV1 struct {
	Name, MediaType, Content string
	ContentDigest            [sha256.Size]byte
}
type StructuredFinalV1 struct {
	Answer                    string
	Artifacts                 []StructuredArtifactV1
	FinalDigest, AnswerDigest [sha256.Size]byte
}

func (StructuredFinalV1) String() string     { return "StructuredFinalV1{data:[redacted]}" }
func (v StructuredFinalV1) GoString() string { return v.String() }

func ParseStructuredFinalV1(ctx context.Context, raw []byte, limits domain.AttachedPiSDKNativeLimitsV1, ledger StructuredFinalAllocationV1) (StructuredFinalV1, error) {
	deny := func() (StructuredFinalV1, error) { return StructuredFinalV1{}, ErrCandidateInvalid }
	if ctx == nil || ctx.Err() != nil || ledger == nil || len(raw) == 0 || limits.StructuredFinalBytes == 0 || limits.StructuredFinalBytes > 128<<10 ||
		uint64(len(raw)) > limits.StructuredFinalBytes || limits.AnswerBytes == 0 || limits.AnswerBytes > 32<<10 || limits.ArtifactBytes == 0 || limits.ArtifactBytes > 32<<10 ||
		limits.ArtifactTotalBytes == 0 || limits.ArtifactTotalBytes > 64<<10 || limits.JSONDepth == 0 || limits.JSONDepth > 32 || limits.JSONNodes == 0 {
		return deny()
	}
	// Fund source-bound decoded strings/decoder copies BEFORE constructing the
	// decoder or copying a content buffer. This is logical payload accounting,
	// not a bound on every encoding/json/native heap object.
	if ledger.Reserve(4*uint64(len(raw)), 4*uint64(len(raw)), 0) != nil || !structuredLexicalBound(ctx, raw, limits, ledger) || !json.Valid(raw) {
		return deny()
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return deny()
	}
	var result StructuredFinalV1
	var answerSeen, artifactsSeen bool
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return deny()
		}
		switch key {
		case "answer":
			if answerSeen {
				return deny()
			}
			answerSeen = true
			value, err := d.Token()
			text, ok := value.(string)
			if err != nil || !ok || uint64(len(text)) > limits.AnswerBytes || utf8.RuneCountInString(text) > 32000 {
				return deny()
			}
			result.Answer = text
		case "artifacts":
			if artifactsSeen {
				return deny()
			}
			artifactsSeen = true
			array, err := d.Token()
			if err != nil || array != json.Delim('[') {
				return deny()
			}
			var total uint64
			for d.More() {
				if len(result.Artifacts) == 2 {
					return deny()
				}
				item, err := d.Token()
				if err != nil || item != json.Delim('{') {
					return deny()
				}
				var a StructuredArtifactV1
				var seen uint8
				for d.More() {
					field, err := d.Token()
					if err != nil {
						return deny()
					}
					var bit uint8
					var target *string
					switch field {
					case "name":
						bit, target = 1, &a.Name
					case "media_type":
						bit, target = 2, &a.MediaType
					case "content":
						bit, target = 4, &a.Content
					default:
						return deny()
					}
					value, err := d.Token()
					text, ok := value.(string)
					if seen&bit != 0 || err != nil || !ok {
						return deny()
					}
					*target = text
					seen |= bit
				}
				end, err := d.Token()
				if err != nil || end != json.Delim('}') || seen != 7 || !structuredArtifactName(a.Name) ||
					a.MediaType != "text/plain" && a.MediaType != "application/json" || uint64(len(a.Content)) > limits.ArtifactBytes || uint64(len(a.Content)) > limits.ArtifactTotalBytes-total {
					return deny()
				}
				for _, previous := range result.Artifacts {
					if strings.EqualFold(previous.Name, a.Name) {
						return deny()
					}
				}
				if ledger.Reserve(uint64(len(a.Content)), uint64(len(a.Content)), 1) != nil {
					return deny()
				}
				content := []byte(a.Content)
				if a.MediaType == "application/json" && (!structuredLexicalBound(ctx, content, limits, ledger) || !json.Valid(content)) {
					return deny()
				}
				a.ContentDigest = sha256.Sum256(content)
				total += uint64(len(content))
				if result.Artifacts == nil {
					storage := 2 * uint64(reflect.TypeOf(StructuredArtifactV1{}).Size())
					if ledger.Reserve(storage, storage, 0) != nil {
						return deny()
					}
					result.Artifacts = make([]StructuredArtifactV1, 0, 2)
				}
				result.Artifacts = append(result.Artifacts, a)
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return deny()
			}
		default:
			return deny()
		}
	}
	end, err := d.Token()
	// #200's accepted DATA contract permits absent artifacts for answer-only
	// turns. The strict provider emission schema still requests explicit [].
	if err != nil || end != json.Delim('}') || !answerSeen || ctx.Err() != nil {
		return deny()
	}
	if _, err := d.Token(); err != io.EOF {
		return deny()
	}
	if ledger.Reserve(uint64(len(result.Answer)), uint64(len(result.Answer)), 0) != nil {
		return deny()
	}
	result.FinalDigest, result.AnswerDigest = sha256.Sum256(raw), sha256.Sum256([]byte(result.Answer))
	return result, nil
}

func structuredArtifactName(name string) bool {
	if !validArtifactName(name) {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// BEFORE decode: validate every UTF8/surrogate sequence, cap nesting/nodes and
// charge each key/value/container on the same ledger. Grammar is additionally
// checked by json.Valid. Quoted artifact contents remain literal data; numeric
// tokens in JSON artifacts are never converted through float64 or reserialized.
func structuredLexicalBound(ctx context.Context, raw []byte, limits domain.AttachedPiSDKNativeLimitsV1, ledger StructuredFinalAllocationV1) bool {
	if uint64(len(raw)) > limits.StructuredFinalBytes || !utf8.Valid(raw) {
		return false
	}
	var depth, nodes uint64
	node := func() bool {
		if ctx.Err() != nil || nodes == limits.JSONNodes || ledger.Reserve(0, 0, 1) != nil {
			return false
		}
		nodes++
		return true
	}
	primitive := false
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			if !node() {
				return false
			}
			primitive = false
			closed := false
			for i++; i < len(raw); i++ {
				if raw[i] == '"' {
					closed = true
					break
				}
				if raw[i] < 32 {
					return false
				}
				if raw[i] != '\\' {
					continue
				}
				i++
				if i >= len(raw) {
					return false
				}
				if raw[i] != 'u' {
					if !strings.ContainsRune(`"\/bfnrt`, rune(raw[i])) {
						return false
					}
					continue
				}
				value, ok := structuredHex4(raw, i+1)
				if !ok {
					return false
				}
				i += 4
				if value >= 0xdc00 && value <= 0xdfff {
					return false
				}
				if value >= 0xd800 && value <= 0xdbff {
					if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
						return false
					}
					low, ok := structuredHex4(raw, i+3)
					if !ok || low < 0xdc00 || low > 0xdfff {
						return false
					}
					i += 6
				}
			}
			if !closed {
				return false
			}
		case '{', '[':
			depth++
			primitive = false
			if depth > limits.JSONDepth || !node() {
				return false
			}
		case '}', ']':
			if depth == 0 {
				return false
			}
			depth--
			primitive = false
		case ' ', '\n', '\r', '\t', ':', ',':
			primitive = false
		default:
			if !primitive {
				if !node() {
					return false
				}
				primitive = true
			}
		}
	}
	return depth == 0 && ctx.Err() == nil
}
func structuredHex4(raw []byte, start int) (uint16, bool) {
	if start < 0 || start > len(raw)-4 {
		return 0, false
	}
	var value uint16
	for _, c := range raw[start : start+4] {
		value <<= 4
		switch {
		case c >= '0' && c <= '9':
			value += uint16(c - '0')
		case c >= 'a' && c <= 'f':
			value += uint16(c-'a') + 10
		case c >= 'A' && c <= 'F':
			value += uint16(c-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}
