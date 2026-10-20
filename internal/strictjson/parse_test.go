package strictjson

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
)

func TestParseLexicalContract(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		valid     bool
	}{
		{"object", `{"a":[true,null,"x"],"n":18446744073709551615}`, true},
		{"surrogate_pair", `"\ud83d\ude00"`, true},
		{"unicode", `{"a":"ёж"}`, true},
		{"negative_zero", `-0`, true},
		{"finite_fraction", `1e-300`, true},
		{"empty", ``, false},
		{"trailing", `{} {}`, false},
		{"trailing_comma", `{"a":1,}`, false},
		{"duplicate", `{"a":1,"a":2}`, false},
		{"escaped_duplicate", `{"a":1,"\u0061":2}`, false},
		{"case_distinct", `{"a":1,"A":2}`, true},
		{"lone_high", `"\ud800"`, false},
		{"lone_low", `"\udc00"`, false},
		{"high_without_low", `"\ud800\u0041"`, false},
		{"invalid_utf8", string([]byte{'"', 0xff, '"'}), false},
		{"control", "\"a\nb\"", false},
		{"integer_overflow", `18446744073709551616`, false},
		{"float_overflow", `1e309`, false},
		{"float_underflow", `1e-324`, false},
		{"huge_zero_exponent", `0e9999999`, false},
		{"too_long_number", `0.` + strings.Repeat("0", 127), false},
		{"leading_zero", `01`, false},
		{"nan", `NaN`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, err := Parse([]byte(tc.raw), 4096, 16, 16)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, want %v (error %v)", err == nil, tc.valid, err)
			}
			if !tc.valid && (!errors.Is(err, ErrInvalid) || v != nil) {
				t.Fatalf("invalid input returned value/type: %T, %v", v, err)
			}
		})
	}
	v, err := Parse([]byte(`18446744073709551615`), 20, 1, 0)
	if n, ok := v.(json.Number); err != nil || !ok || n.String() != "18446744073709551615" {
		t.Fatalf("exact number not preserved: %T %v, %v", v, v, err)
	}
}

func TestParseBounds(t *testing.T) {
	for _, tc := range []struct {
		name, raw             string
		bytes, depth, members uint64
		valid                 bool
	}{
		{"exact_bytes", `{"x":1}`, 7, 2, 1, true},
		{"over_bytes", `{"x":1}`, 6, 2, 1, false},
		{"zero_bytes", `0`, 0, 1, 0, false},
		{"root_depth", `0`, 1, 1, 0, true},
		{"zero_depth", `0`, 1, 0, 0, false},
		{"nested_exact", `{"a":[{"x":true}]}`, 128, 4, 1, true},
		{"nested_over", `{"a":[{"x":true}]}`, 128, 3, 1, false},
		{"member_exact", `{"a":1,"b":2}`, 128, 2, 2, true},
		{"member_over", `{"a":1,"b":2}`, 128, 2, 1, false},
		{"array_exact", `[1,2]`, 128, 2, 2, true},
		{"array_over", `[1,2]`, 128, 2, 1, false},
		{"members_are_per_container", `[[1,2],[3,4]]`, 128, 3, 2, true},
		{"empty_object_zero_members", `{}`, 2, 1, 0, true},
		{"empty_array_zero_members", `[]`, 2, 1, 0, true},
		{"integer_limits_do_not_wrap", `{"a":[1]}`, math.MaxUint64, math.MaxUint64, math.MaxUint64, true},
		{"hard_depth_exact", strings.Repeat("[", 63) + "0" + strings.Repeat("]", 63), 1024, math.MaxUint64, 1, true},
		{"hard_depth_over", strings.Repeat("[", 64) + "0" + strings.Repeat("]", 64), 1024, math.MaxUint64, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.raw), tc.bytes, tc.depth, tc.members)
			if (err == nil) != tc.valid {
				t.Fatalf("bytes/depth/members=%d/%d/%d: valid=%v, want %v (%v)", tc.bytes, tc.depth, tc.members, err == nil, tc.valid, err)
			}
		})
	}
}

func FuzzParse(f *testing.F) {
	for _, seed := range []string{`null`, `{"a":1,"\u0061":2}`, `"\ud800"`, `1e-324`, `[]`, `{"a":[true,"x"]}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 4096 {
			return
		}
		v, err := Parse(raw, 4096, 16, 32)
		if err != nil {
			if v != nil || !errors.Is(err, ErrInvalid) {
				t.Fatal("invalid parse did not return the closed failure")
			}
			return
		}
		if !json.Valid(raw) {
			t.Fatal("strict parser accepted invalid JSON")
		}
		canonical, err := json.Marshal(v)
		if err != nil || !json.Valid(canonical) {
			t.Fatalf("accepted tree cannot be encoded: %v", err)
		}
	})
}
