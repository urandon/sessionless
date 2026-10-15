package attachedworkeroutput

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type structuredBudget struct {
	bytes, nodes uint64
	deny         bool
}

func (b *structuredBudget) Reserve(copyBytes, liveBytes, nodes uint64) error {
	if b.deny {
		return errors.New("synthetic exhausted ledger")
	}
	b.bytes += copyBytes
	b.nodes += nodes
	return nil
}
func structuredLimits() domain.AttachedPiSDKNativeLimitsV1 {
	return domain.AttachedPiSDKNativeLimitsV1{StructuredFinalBytes: 128 << 10, AnswerBytes: 32 << 10, ArtifactBytes: 32 << 10, ArtifactTotalBytes: 64 << 10, JSONDepth: 32, JSONNodes: 4096}
}

func TestStructuredFinalPreservesProviderBytesAndDistinctDigests(t *testing.T) {
	artifact := `{ "__proto__": "quoted data", "n": 900719925474099312345, "e": 1e+100 }`
	encoded, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"answer":"original\nanswer 🦉","artifacts":[{"name":"data.json","media_type":"application/json","content":` + string(encoded) + `}]}`)
	budget := &structuredBudget{}
	result, err := ParseStructuredFinalV1(context.Background(), raw, structuredLimits(), budget)
	if err != nil {
		t.Fatal(err)
	}
	if result.Answer != "original\nanswer 🦉" || len(result.Artifacts) != 1 || result.Artifacts[0].Content != artifact || result.Artifacts[0].ContentDigest != sha256.Sum256([]byte(artifact)) || result.FinalDigest != sha256.Sum256(raw) || result.AnswerDigest != sha256.Sum256([]byte(result.Answer)) {
		t.Error("original content/digest association changed")
	}
	if budget.bytes == 0 || budget.nodes == 0 {
		t.Error("shared ledger not debited")
	}
	if strings.Contains(result.String(), "original") || strings.Contains(result.String(), artifact) {
		t.Error("data diagnostics leaked")
	}
}

func TestStructuredFinalAnswerOnlyDoesNotInventArtifact(t *testing.T) {
	for _, raw := range []string{`{"answer":"ready","artifacts":[]}`} {
		result, err := ParseStructuredFinalV1(context.Background(), []byte(raw), structuredLimits(), &structuredBudget{})
		if err != nil || len(result.Artifacts) != 0 {
			t.Errorf("answer-only got artifacts/error: %v", err)
		}
	}
}

func TestStructuredFinalWholeDenials(t *testing.T) {
	for _, raw := range []string{
		`{"answer":"ready"}`,
		`{"answer":null}`, `{"answer":12}`, `{"answer":"ok","answer":"other"}`, `{"answer":"ok","\u0061nswer":"other"}`, `{"answer":"ok","unexpected":false}`,
		`{"answer":"ok","artifacts":null}`, `{"answer":"\ud800"}`, `{"answer":"\udc00"}`, `{"answer":"ok"} {}`,
		`{"answer":"ok","artifacts":[{"name":"a","media_type":"text/plain","content":null}]}`,
		`{"answer":"ok","artifacts":[{"name":"../a","media_type":"text/plain","content":"a"}]}`,
		`{"answer":"ok","artifacts":[{"name":"a","media_type":"image/png","content":"a"}]}`,
		`{"answer":"ok","artifacts":[{"name":"a","media_type":"text/plain","content":"a","content":"b"}]}`,
		`{"answer":"ok","artifacts":[{"name":"a","media_type":"text/plain","content":"a","extra":"b"}]}`,
		`{"answer":"ok","artifacts":[{"name":"a","media_type":"application/json","content":"not JSON"}]}`,
		`{"answer":"ok","artifacts":[{"name":"File","media_type":"text/plain","content":"a"},{"name":"file","media_type":"text/plain","content":"b"}]}`,
	} {
		result, err := ParseStructuredFinalV1(context.Background(), []byte(raw), structuredLimits(), &structuredBudget{})
		if err == nil || result.Answer != "" || result.Artifacts != nil || result.FinalDigest != [32]byte{} {
			t.Errorf("unsafe partial final accepted: %q", raw)
		}
	}
	invalid := []byte{'{', '"', 'a', 'n', 's', 'w', 'e', 'r', '"', ':', '"', 0xff, '"', '}'}
	if _, err := ParseStructuredFinalV1(context.Background(), invalid, structuredLimits(), &structuredBudget{}); err == nil {
		t.Error("invalid UTF8 accepted")
	}
}

func TestStructuredFinalExactAnswerAndEncodedLimits(t *testing.T) {
	for _, delta := range []int{0, 1} {
		// Original answer has BOTH32KiB and32000-rune limits. Use two-byte
		// characters for the byte boundary while remaining below32000runes.
		answer := strings.Repeat("é", 16<<10) + strings.Repeat("a", delta)
		raw, _ := json.Marshal(map[string]any{"answer": answer, "artifacts": []any{}})
		_, err := ParseStructuredFinalV1(context.Background(), raw, structuredLimits(), &structuredBudget{})
		if (err == nil) != (delta == 0) {
			t.Errorf("answer boundary delta%d err=%v", delta, err)
		}
	}
	raw := []byte(`{"answer":"ready","artifacts":[]}`)
	l := structuredLimits()
	l.StructuredFinalBytes = uint64(len(raw))
	if _, err := ParseStructuredFinalV1(context.Background(), raw, l, &structuredBudget{}); err != nil {
		t.Fatal(err)
	}
	l.StructuredFinalBytes--
	if _, err := ParseStructuredFinalV1(context.Background(), raw, l, &structuredBudget{}); err == nil {
		t.Error("first encoded overflow accepted")
	}
	if _, err := ParseStructuredFinalV1(context.Background(), raw, structuredLimits(), &structuredBudget{deny: true}); err == nil {
		t.Error("unfunded decoder accepted")
	}
}
