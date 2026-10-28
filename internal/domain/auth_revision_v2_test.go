package domain_test

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestAuthRevisionV2LosslessDecimalBoundaries(t *testing.T) {
	for _, number := range []uint64{1, 1<<53 - 1, 1 << 53, 1<<53 + 1, math.MaxUint64} {
		t.Run(strconv.FormatUint(number, 10), func(t *testing.T) {
			text := strconv.FormatUint(number, 10)
			parsed, err := domain.ParseAuthRevisionV2(text)
			if err != nil || uint64(parsed) != number {
				t.Fatalf("ParseAuthRevisionV2(%q) = (%v, %v), want %d", text, parsed, err, number)
			}
			raw, err := json.Marshal(parsed)
			if err != nil || string(raw) != `"`+text+`"` {
				t.Fatalf("Marshal(%d) = (%s, %v), want quoted %q", number, raw, err, text)
			}
			var decoded domain.AuthRevisionV2
			if err := json.Unmarshal(raw, &decoded); err != nil || decoded != parsed {
				t.Fatalf("Unmarshal(%s) = (%v, %v), want %v", raw, decoded, err, parsed)
			}
		})
	}
}

func TestAuthRevisionV2RejectsAlternateEncodings(t *testing.T) {
	for _, text := range []string{"", "0", "00", "01", "+1", "-1", " 1", "1 ", "1.0", "1e0", "18446744073709551616", "١", "1\n"} {
		t.Run(strconv.Quote(text), func(t *testing.T) {
			if value, err := domain.ParseAuthRevisionV2(text); err == nil || value != 0 {
				t.Errorf("ParseAuthRevisionV2(%q) = (%v, %v), want zero and error", text, value, err)
			}
			raw, err := json.Marshal(text)
			if err != nil {
				t.Fatal(err)
			}
			value := domain.AuthRevisionV2(7)
			if err := json.Unmarshal(raw, &value); err == nil || value != 0 {
				t.Errorf("Unmarshal(%s) = (%v, %v), want zero and error", raw, value, err)
			}
		})
	}
	for _, raw := range []string{`1`, `9007199254740993`, `null`, `true`, `[]`, `{}`, `"\u0031"`, `"1\u0030"`} {
		t.Run("raw_"+raw, func(t *testing.T) {
			value := domain.AuthRevisionV2(7)
			if err := json.Unmarshal([]byte(raw), &value); err == nil || value != 0 {
				t.Errorf("Unmarshal(%s) = (%v, %v), want zero and error", raw, value, err)
			}
		})
	}
	if raw, err := json.Marshal(domain.AuthRevisionV2(0)); err == nil || raw != nil {
		t.Errorf("Marshal(zero) = (%s, %v), want nil and error", raw, err)
	}
	var missing *domain.AuthRevisionV2
	if err := missing.UnmarshalJSON([]byte(`"1"`)); err == nil {
		t.Error("nil revision receiver accepted input")
	}
}

func TestNextAuthRevisionV2CheckedMaximum(t *testing.T) {
	for _, test := range []struct {
		name   string
		values []domain.AuthRevisionV2
		want   domain.AuthRevisionV2
	}{
		{name: "first advance", values: []domain.AuthRevisionV2{1}, want: 2},
		{name: "tombstone higher", values: []domain.AuthRevisionV2{4, 99}, want: 100},
		{name: "active higher", values: []domain.AuthRevisionV2{99, 4}, want: 100},
		{name: "beyond double precision", values: []domain.AuthRevisionV2{1 << 53}, want: 1<<53 + 1},
		{name: "last representable", values: []domain.AuthRevisionV2{math.MaxUint64 - 1}, want: math.MaxUint64},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := domain.NextAuthRevisionV2(test.values...)
			if err != nil || got != test.want {
				t.Errorf("NextAuthRevisionV2(%v) = (%v, %v), want %v", test.values, got, err, test.want)
			}
		})
	}
	for _, values := range [][]domain.AuthRevisionV2{nil, {0}, {5, 0}, {math.MaxUint64}, {5, math.MaxUint64}} {
		got, err := domain.NextAuthRevisionV2(values...)
		if err == nil || got != 0 {
			t.Errorf("NextAuthRevisionV2(%v) = (%v, %v), want zero and error", values, got, err)
		}
		if len(values) > 0 && values[len(values)-1] == math.MaxUint64 && !errors.Is(err, domain.ErrAuthRevisionOverflowV2) {
			t.Errorf("NextAuthRevisionV2(%v) error = %v, want overflow", values, err)
		}
	}
}

func FuzzAuthRevisionV2StrictRoundTrip(f *testing.F) {
	for _, seed := range []string{"1", "9007199254740993", "18446744073709551615", "0", "01", "1e0", "+1", "18446744073709551616"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		revision, err := domain.ParseAuthRevisionV2(text)
		if err != nil {
			return
		}
		if revision.String() != text || revision == 0 {
			t.Fatalf("accepted noncanonical revision %q as %v", text, revision)
		}
		raw, err := json.Marshal(revision)
		if err != nil {
			t.Fatal(err)
		}
		var decoded domain.AuthRevisionV2
		if err := json.Unmarshal(raw, &decoded); err != nil || decoded != revision {
			t.Fatalf("roundtrip(%q) = (%v, %v), want %v", text, decoded, err, revision)
		}
	})
}
