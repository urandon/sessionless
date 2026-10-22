package strictjson

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestDocumentPreservesArbitraryNumericLexemesWithoutRelaxingProtocol(t *testing.T) {
	for _, body := range []string{
		`{"n":9007199254740993123456789}`,
		`{"n":1e999999999999999999999999999999999999}`,
		`{"n":1e-999999999999999999999999999999999999}`,
		`{"n":` + strings.Repeat("9", 512) + `}`,
	} {
		original := []byte(body)
		preserved := bytes.Clone(original)
		if err := ValidateDocument(original, 1024, 32, 1024); err != nil {
			t.Errorf("valid document numeric lexeme rejected: %v", err)
		}
		if !bytes.Equal(original, preserved) {
			t.Error("document validation rewrote original bytes")
		}
		if _, err := Parse(original, 1024, 32, 1024); !errors.Is(err, ErrInvalid) {
			t.Errorf("protocol numeric bounds relaxed: got %v", err)
		}
	}
}

func TestDocumentRejectsMalformedAndAmbiguousContent(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"lone high surrogate", `"\ud800"`},
		{"lone low surrogate", `"\udc00"`},
		{"wrong pair", `"\ud800\u0041"`},
		{"invalid UTF8", "\"\xff\""},
		{"duplicate escaped key", `{"x":1,"\u0078":2}`},
		{"leading zero", `01`},
		{"missing fraction", `1.`},
		{"missing exponent", `1e+`},
		{"extra value", `{} []`},
		{"excess depth", strings.Repeat("[", 33) + "0" + strings.Repeat("]", 33)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateDocument([]byte(tc.body), 1024, 32, 1024); !errors.Is(err, ErrInvalid) {
				t.Errorf("invalid document accepted: got %v", err)
			}
		})
	}
}

func TestDocumentLimitsAndUnicodePair(t *testing.T) {
	body := []byte(`{"emoji":"\ud83d\ude00","n":1e999}`)
	if err := ValidateDocument(body, uint64(len(body)), 2, 2); err != nil {
		t.Fatalf("exact limits and valid Unicode pair rejected: %v", err)
	}
	for _, bounds := range [][3]uint64{{uint64(len(body) - 1), 2, 2}, {1024, 1, 2}, {1024, 2, 1}, {1024, 0, 2}} {
		if err := ValidateDocument(body, bounds[0], bounds[1], bounds[2]); !errors.Is(err, ErrInvalid) {
			t.Errorf("exceeded bounds %v accepted: %v", bounds, err)
		}
	}
}
