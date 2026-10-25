package strictjson

import (
	"encoding/json"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

// StringProjection selects an exact decoded-key path. Array steps are decimal
// indices. Projection is DATA only: callers must still validate their typed
// closed schema and original string semantics. Empty/non-string/missing values
// are left unchanged, particularly for fields with omitempty.
type StringProjection struct {
	Path     []string
	MaxBytes uint64
}

type projectedStringSpan struct{ start, end, field int }

type stringProjector struct {
	scanner jsonScanner
	fields  []StringProjection
	spans   []projectedStringSpan
}

// ProjectStrings checks the complete bounded document with the existing lexer,
// then replaces selected nonempty string tokens with "x" in a metadata copy. It
// returns borrowed ORIGINAL string lexemes aligned with fields; those aliases
// remain valid only while the caller retains and does not mutate b. No complete
// value tree or copy of a selected string is allocated. Selected decoded-byte
// limits and the output size are checked before copying. Metadata is no larger
// than the input; nonselected original bytes, including whitespace and escape
// spellings, are preserved. The lexer/path-key unescapes are still bounded by
// the complete document limit, not an independently smaller metadata budget.
func ProjectStrings(b []byte, limit uint64, fields []StringProjection) ([]byte, [][]byte, error) {
	if len(fields) == 0 || len(fields) > 8 || ValidateDocument(b, limit, 64, limit) != nil {
		return nil, nil, ErrInvalid
	}
	for i, field := range fields {
		if len(field.Path) == 0 || len(field.Path) > 64 || field.MaxBytes > limit {
			return nil, nil, ErrInvalid
		}
		for _, prior := range fields[:i] {
			shared := min(len(prior.Path), len(field.Path))
			same := true
			for j := 0; j < shared; j++ {
				if prior.Path[j] != field.Path[j] {
					same = false
					break
				}
			}
			if same { // duplicate/ancestor selections cannot have two owners
				return nil, nil, ErrInvalid
			}
		}
	}
	p := stringProjector{scanner: jsonScanner{b: b, depth: 64, members: len(b), documentNumbers: true}, fields: fields}
	if !p.value(nil, 1) {
		return nil, nil, ErrInvalid
	}
	original := make([][]byte, len(fields))
	size := len(b)
	for _, span := range p.spans {
		original[span.field] = b[span.start:span.end]
		size -= span.end - span.start - 3
	}
	metadata := make([]byte, 0, size)
	position := 0
	for _, span := range p.spans {
		metadata = append(metadata, b[position:span.start]...)
		metadata = append(metadata, `"x"`...)
		position = span.end
	}
	metadata = append(metadata, b[position:]...)
	return metadata, original, nil
}

func (p *stringProjector) value(path []string, depth int) bool {
	s := &p.scanner
	s.space()
	matched := false
	for i, field := range p.fields {
		if len(path) > len(field.Path) {
			continue
		}
		equal := true
		for j, key := range path {
			if field.Path[j] != key {
				equal = false
				break
			}
		}
		if !equal {
			continue
		}
		matched = true
		if len(path) == len(field.Path) {
			if s.i == len(s.b) || s.b[s.i] != '"' {
				return s.value(depth) // retain non-string shape for the typed guard
			}
			start := s.i
			if !s.string() {
				return false
			}
			n := decodedStringBytes(s.b[start:s.i])
			if n > field.MaxBytes {
				return false
			}
			if n > 0 {
				p.spans = append(p.spans, projectedStringSpan{start: start, end: s.i, field: i})
			}
			return true
		}
	}
	if !matched {
		return s.value(depth)
	}
	if s.take('{') {
		s.space()
		if s.take('}') {
			return true
		}
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
			s.space()
			if !s.take(':') || !p.value(append(path, key), depth+1) {
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
	}
	if s.take('[') {
		s.space()
		if s.take(']') {
			return true
		}
		for index := 0; ; index++ {
			if !p.value(append(path, strconv.Itoa(index)), depth+1) {
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
	}
	return s.value(depth)
}

// The original lexer has already proved valid UTF-8/escapes/surrogate pairs.
// Count their decoded UTF-8 bytes without unescaping or allocating content.
func decodedStringBytes(raw []byte) uint64 {
	var size uint64
	for i := 1; i < len(raw)-1; i++ {
		if raw[i] != '\\' {
			size++
			continue
		}
		i++
		if raw[i] != 'u' {
			size++
			continue
		}
		s := jsonScanner{b: raw, i: i + 1}
		n, _ := s.hex4()
		r := rune(n)
		if n >= 0xd800 && n <= 0xdbff {
			s.i += 2 // the already validated second backslash-u
			low, _ := s.hex4()
			r = utf16.DecodeRune(r, rune(low))
		}
		size += uint64(utf8.RuneLen(r))
		i = s.i - 1
	}
	return size
}
