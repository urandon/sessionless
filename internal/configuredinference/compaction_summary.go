package configuredinference

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const CompactionSummarySchemaV1 = "sessionless.compaction-summary.v1"
const MaxCompactionSummaryBytesV1 = 16 << 10

type CompactionReferenceKindV1 string

const (
	CompactionCanonicalReferenceV1 CompactionReferenceKindV1 = "canonical"
	CompactionOperationReferenceV1 CompactionReferenceKindV1 = "operation"
	CompactionArtifactReferenceV1  CompactionReferenceKindV1 = "artifact"
)

// CompactionReferenceV1 is source-table DATA, not authorization or proof that
// an artifact body was seen. Only the original source producer may select the
// table used by a real generation. OmissionAllowed must come from its admitted
// modality policy, not from model output or an arbitrary caller at dispatch.
type CompactionReferenceV1 struct {
	SourceID        string
	Kind            CompactionReferenceKindV1
	Digest          string
	MediaType       string
	Size            uint64
	OmissionAllowed bool
}

// CompactionSummaryV1 owns exact strictly validated model JSON. It is untrusted
// derived DATA: valid citations/schema do not prove factual fidelity, current
// authority, token fit, retained custody, or permission to publish a head.
type CompactionSummaryV1 struct {
	body   []byte
	digest string
}

func (CompactionSummaryV1) String() string               { return "CompactionSummaryV1{content:[redacted]}" }
func (v CompactionSummaryV1) GoString() string           { return v.String() }
func (v CompactionSummaryV1) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, v.String()) }
func (CompactionSummaryV1) MarshalJSON() ([]byte, error) { return nil, ErrContract }
func (v *CompactionSummaryV1) Body() []byte {
	if v == nil {
		return nil
	}
	return append([]byte(nil), v.body...)
}
func (v *CompactionSummaryV1) Digest() string {
	if v == nil {
		return ""
	}
	return v.digest
}

// DecodeCompactionSummaryV1 accepts exactly one closed, bounded summary value.
// It neither repairs JSON nor performs another generation. The source table
// must be rederived/rejoined by the original Store before publication; this pure
// decoder cannot turn a caller-created table or successful decode into a grant.
func DecodeCompactionSummaryV1(body []byte, references []CompactionReferenceV1) (*CompactionSummaryV1, error) {
	if len(references) == 0 || len(references) > 4096 {
		return nil, ErrContract
	}
	known := make(map[string]CompactionReferenceV1, len(references))
	for _, ref := range references {
		if domain.ValidateSHA256Digest("compaction.source", ref.SourceID) != nil {
			return nil, ErrContract
		}
		if _, exists := known[ref.SourceID]; exists {
			return nil, ErrContract
		}
		switch ref.Kind {
		case CompactionCanonicalReferenceV1, CompactionOperationReferenceV1:
			if ref.Digest != "" || ref.MediaType != "" || ref.Size != 0 || ref.OmissionAllowed {
				return nil, ErrContract
			}
		case CompactionArtifactReferenceV1:
			if domain.ValidateSHA256Digest("compaction.artifact", ref.Digest) != nil || !validString(ref.MediaType, 255, false) || ref.Size == 0 || ref.Size > math.MaxInt64 {
				return nil, ErrContract
			}
		default:
			return nil, ErrContract
		}
		known[ref.SourceID] = ref
	}
	value, err := strictJSON(body, MaxCompactionSummaryBytesV1, 5, 64)
	if err != nil {
		return nil, ErrContract
	}
	m, ok := object(value)
	if !ok || len(m) != 7 || !exactKeys(m, "schema", "task", "constraints", "facts", "unresolved", "artifacts", "omissions") || m["schema"] != CompactionSummarySchemaV1 {
		return nil, ErrContract
	}
	substantive := 0
	for _, key := range []string{"task", "constraints", "facts", "unresolved"} {
		items, ok := m[key].([]any)
		if !ok || len(items) > 64 {
			return nil, ErrContract
		}
		if key != "constraints" {
			substantive += len(items)
		}
		for _, value := range items {
			entry, ok := object(value)
			if !ok || len(entry) != 2 || !exactKeys(entry, "text", "source_ids") {
				return nil, ErrContract
			}
			text, ok := entry["text"].(string)
			ids, idsOK := entry["source_ids"].([]any)
			if !ok || !validString(text, 2048, false) || strings.TrimSpace(text) == "" || !idsOK || len(ids) == 0 || len(ids) > 64 {
				return nil, ErrContract
			}
			seen := make(map[string]bool, len(ids))
			for _, value := range ids {
				id, ok := value.(string)
				if !ok || seen[id] {
					return nil, ErrContract
				}
				if _, exists := known[id]; !exists {
					return nil, ErrContract
				}
				seen[id] = true
			}
		}
	}
	if substantive == 0 {
		return nil, ErrContract
	}
	artifacts, ok := m["artifacts"].([]any)
	if !ok || len(artifacts) > 64 {
		return nil, ErrContract
	}
	seenArtifacts := make(map[string]bool)
	for _, value := range artifacts {
		entry, ok := object(value)
		if !ok || len(entry) != 4 || !exactKeys(entry, "source_id", "digest", "media_type", "size") {
			return nil, ErrContract
		}
		id, idOK := entry["source_id"].(string)
		digest, digestOK := entry["digest"].(string)
		media, mediaOK := entry["media_type"].(string)
		size, sizeOK := uintValue(entry["size"])
		ref, exists := known[id]
		if !idOK || !digestOK || !mediaOK || !sizeOK || !exists || seenArtifacts[id] || ref.Kind != CompactionArtifactReferenceV1 || ref.Digest != digest || ref.MediaType != media || ref.Size != size {
			return nil, ErrContract
		}
		seenArtifacts[id] = true
	}
	omissions, ok := m["omissions"].([]any)
	if !ok || len(omissions) > 64 {
		return nil, ErrContract
	}
	for _, value := range omissions {
		entry, ok := object(value)
		if !ok || len(entry) != 2 || !exactKeys(entry, "source_id", "reason") {
			return nil, ErrContract
		}
		id, idOK := entry["source_id"].(string)
		reason, reasonOK := entry["reason"].(string)
		ref, exists := known[id]
		if !idOK || !reasonOK || !exists || seenArtifacts[id] || ref.Kind != CompactionArtifactReferenceV1 || !ref.OmissionAllowed || reason != "profile_reference_only" {
			return nil, ErrContract
		}
		seenArtifacts[id] = true
	}
	sum := sha256.Sum256(body)
	return &CompactionSummaryV1{body: append([]byte(nil), body...), digest: hex.EncodeToString(sum[:])}, nil
}
