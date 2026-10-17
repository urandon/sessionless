package configuredinference

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// A lexical pass precedes decoding/tree allocation. Besides finite JSON bounds,
// it rejects Go encoding/json's silent replacement of invalid UTF-8 and lone
// UTF-16 surrogates. Decoded object names are exact/case-sensitive; duplicate
// escaped spellings of the same name are duplicates. Arrays are bounded by the
// same member ceiling. Every number, including ignored metadata, must have a
// finite float64 representation; overflow/underflow-to-zero is rejected. Numeric
// lexemes are at most 128 bytes with exponent magnitude at most 324; bare
// integer magnitudes must fit uint64. The exponent bound also prevents a zero
// with an enormous exponent from expanding exact schema rational arithmetic.
// Integral
// wire counters are additionally parsed as exact uint64 decimal integers.
type jsonScanner struct {
	b                 []byte
	i, depth, members int
}

func strictJSON(b []byte, byteLimit, depth, members uint64) (any, error) {
	if len(b) == 0 || uint64(len(b)) > byteLimit || !utf8.Valid(b) {
		return nil, ErrContract
	}
	s := jsonScanner{b: b, depth: int(depth), members: int(members)}
	if !s.value(1) {
		return nil, ErrContract
	}
	s.space()
	if s.i != len(b) {
		return nil, ErrContract
	}
	var v any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if d.Decode(&v) != nil {
		return nil, ErrContract
	}
	return v, nil
}

func (s *jsonScanner) space() {
	for s.i < len(s.b) {
		switch s.b[s.i] {
		case ' ', '\t', '\r', '\n':
			s.i++
		default:
			return
		}
	}
}
func (s *jsonScanner) value(depth int) bool {
	if depth > s.depth {
		return false
	}
	s.space()
	if s.i == len(s.b) {
		return false
	}
	switch s.b[s.i] {
	case '{':
		s.i++
		s.space()
		if s.take('}') {
			return true
		}
		seen := make(map[string]struct{})
		n := 0
		for {
			s.space()
			start := s.i
			if !s.string() {
				return false
			}
			var key string
			if json.Unmarshal(s.b[start:s.i], &key) != nil {
				return false
			}
			if _, ok := seen[key]; ok {
				return false
			}
			seen[key] = struct{}{}
			n++
			if n > s.members {
				return false
			}
			s.space()
			if !s.take(':') || !s.value(depth+1) {
				return false
			}
			s.space()
			if s.take('}') {
				return true
			}
			if !s.take(',') {
				return false
			}
		}
	case '[':
		s.i++
		s.space()
		if s.take(']') {
			return true
		}
		n := 0
		for {
			n++
			if n > s.members || !s.value(depth+1) {
				return false
			}
			s.space()
			if s.take(']') {
				return true
			}
			if !s.take(',') {
				return false
			}
		}
	case '"':
		return s.string()
	case 't':
		return s.literal("true")
	case 'f':
		return s.literal("false")
	case 'n':
		return s.literal("null")
	default:
		return s.number()
	}
}
func (s *jsonScanner) take(c byte) bool {
	if s.i < len(s.b) && s.b[s.i] == c {
		s.i++
		return true
	}
	return false
}
func (s *jsonScanner) literal(v string) bool {
	if len(s.b)-s.i < len(v) || string(s.b[s.i:s.i+len(v)]) != v {
		return false
	}
	s.i += len(v)
	return true
}
func hexDigit(c byte) (uint16, bool) {
	switch {
	case c >= '0' && c <= '9':
		return uint16(c - '0'), true
	case c >= 'a' && c <= 'f':
		return uint16(c - 'a' + 10), true
	case c >= 'A' && c <= 'F':
		return uint16(c - 'A' + 10), true
	}
	return 0, false
}
func (s *jsonScanner) hex4() (uint16, bool) {
	if len(s.b)-s.i < 4 {
		return 0, false
	}
	var n uint16
	for range 4 {
		v, ok := hexDigit(s.b[s.i])
		if !ok {
			return 0, false
		}
		n = n*16 + v
		s.i++
	}
	return n, true
}
func (s *jsonScanner) string() bool {
	if !s.take('"') {
		return false
	}
	for s.i < len(s.b) {
		c := s.b[s.i]
		s.i++
		switch {
		case c == '"':
			return true
		case c < 0x20:
			return false
		case c == '\\':
			if s.i == len(s.b) {
				return false
			}
			c = s.b[s.i]
			s.i++
			switch c {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			case 'u':
				n, ok := s.hex4()
				if !ok {
					return false
				}
				if n >= 0xd800 && n <= 0xdbff {
					if !s.take('\\') || !s.take('u') {
						return false
					}
					low, ok := s.hex4()
					if !ok || low < 0xdc00 || low > 0xdfff {
						return false
					}
				} else if n >= 0xdc00 && n <= 0xdfff {
					return false
				}
			default:
				return false
			}
		}
	}
	return false
}
func (s *jsonScanner) digits() bool {
	start := s.i
	for s.i < len(s.b) && s.b[s.i] >= '0' && s.b[s.i] <= '9' {
		s.i++
	}
	return s.i > start
}
func (s *jsonScanner) number() bool {
	start := s.i
	s.take('-')
	if s.i == len(s.b) {
		return false
	}
	if s.b[s.i] == '0' {
		s.i++
	} else {
		if s.b[s.i] < '1' || s.b[s.i] > '9' || !s.digits() {
			return false
		}
	}
	if s.take('.') && !s.digits() {
		return false
	}
	if s.take('e') || s.take('E') {
		if !s.take('+') {
			s.take('-')
		}
		exponentStart := s.i
		if !s.digits() {
			return false
		}
		exponent := 0
		for _, digit := range s.b[exponentStart:s.i] {
			exponent = exponent*10 + int(digit-'0')
			if exponent > 324 {
				return false
			}
		}
	}
	if s.i-start > 128 {
		return false
	}
	token := string(s.b[start:s.i])
	if !strings.ContainsAny(token, ".eE") {
		if _, err := strconv.ParseUint(strings.TrimPrefix(token, "-"), 10, 64); err != nil {
			return false
		}
	}
	v, err := strconv.ParseFloat(token, 64)
	if err != nil || math.IsInf(v, 0) || math.IsNaN(v) {
		return false
	}
	if v == 0 {
		for _, c := range s.b[start:s.i] {
			if c == 'e' || c == 'E' {
				break
			}
			if c >= '1' && c <= '9' {
				return false
			}
		}
	}
	return true
}
