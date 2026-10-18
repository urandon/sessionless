package domain

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// These independent oracles preserve the previous implementations, including
// error field/reason and validation order. Production uses neither oracle.
var priorOpaqueGrammar = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)

func priorOpaqueValidation(field, value string) error {
	if value == "" {
		return ValidationError{Field: field, Reason: "must not be empty"}
	}
	if len(value) > maxOpaqueIDLength {
		return ValidationError{Field: field, Reason: fmt.Sprintf("must not exceed %d bytes", maxOpaqueIDLength)}
	}
	if !priorOpaqueGrammar.MatchString(value) {
		return ValidationError{Field: field, Reason: "must use only ASCII letters, digits, dot, underscore, colon, or dash"}
	}
	return nil
}

func priorSHA256Validation(field, value string) error {
	digest, err := hex.DecodeString(value)
	if err != nil || len(digest) != 32 || value != strings.ToLower(value) {
		return ValidationError{Field: field, Reason: "must be a lowercase 64-character SHA-256 digest"}
	}
	return nil
}

func TestIdentityValidationEquivalentByteGrammar(t *testing.T) {
	check := func(value string) {
		t.Helper()
		got, want := ValidateOpaqueID("test_field", value), priorOpaqueValidation("test_field", value)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("identity %q: got %v; want %v", value, got, want)
		}
	}
	for _, value := range []string{"", "a", "0", "A-z_0.9:abc", ".a", "_a", ":a", "-a", "a\n", "a\r\n", "é", "aé", "a\x00", strings.Repeat("a", 160), strings.Repeat("a", 161), strings.Repeat("é", 81)} {
		check(value)
	}
	// Every possible byte in the first and subsequent position, at both
	// short and maximum admitted lengths. Includes malformed UTF-8.
	for b := 0; b < 256; b++ {
		check(string([]byte{byte(b)}))
		check("a" + string([]byte{byte(b)}) + "Z")
		check(strings.Repeat("a", 159) + string([]byte{byte(b)}))
	}
}

func TestSHA256ValidationEquivalentByteGrammar(t *testing.T) {
	check := func(value string) {
		t.Helper()
		got, want := validateSHA256("test_digest", value), priorSHA256Validation("test_digest", value)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("digest %q: got %v; want %v", value, got, want)
		}
	}
	for _, value := range []string{"", "a", strings.Repeat("0", 63), strings.Repeat("0", 64), strings.Repeat("f", 64), strings.Repeat("F", 64), strings.Repeat("0", 65), strings.Repeat("é", 32), strings.Repeat("0123456789abcdef", 4)} {
		check(value)
	}
	for i := 0; i < 64; i++ {
		for b := 0; b < 256; b++ {
			value := []byte(strings.Repeat("0", 64))
			value[i] = byte(b)
			check(string(value))
		}
	}
}

func FuzzIdentityValidationEquivalent(f *testing.F) {
	for _, value := range []string{"", "valid-id:_01.A", "é", "a\n", "\xff", strings.Repeat("0", 64), strings.Repeat("a", 161)} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 1024 {
			return
		}
		if got, want := ValidateOpaqueID("id", value), priorOpaqueValidation("id", value); !reflect.DeepEqual(got, want) {
			t.Fatalf("identity %q: got %v; want %v", value, got, want)
		}
		if got, want := validateSHA256("digest", value), priorSHA256Validation("digest", value); !reflect.DeepEqual(got, want) {
			t.Fatalf("digest %q: got %v; want %v", value, got, want)
		}
	})
}

func BenchmarkIdentityValidation(b *testing.B) {
	value := "tenant-test.run-01:physical_invocation_claim_0123456789abcdef"
	b.Run("byte_grammar", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := ValidateOpaqueID("id", value); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("prior_regexp", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := priorOpaqueValidation("id", value); err != nil {
				b.Fatal(err)
			}
		}
	})
}
