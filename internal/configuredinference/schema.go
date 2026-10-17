package configuredinference

import (
	"encoding/json"
	"math/big"
	"strconv"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// This is a CLOSED codec-only schema subset, not a general JSON Schema engine:
// explicit type; objects require properties, required, additionalProperties:false;
// string enum/minLength/maxLength; number/integer minimum/maximum; arrays require
// items and explicit minItems/maxItems. Primitive boolean/null are supported.
// No dialect, refs, combinators, pattern/format, unions, extensions or ignored
// keywords. String lengths count Unicode code points (wire limits count bytes).
// Finite numeric comparisons use exact rationals, never rounded float ordering.
type schemaV1 struct {
	kind                 string
	properties           map[string]*schemaV1
	required             map[string]bool
	enum                 map[string]bool
	minimum, maximum     *big.Rat
	minLength, maxLength *uint64
	minItems, maxItems   uint64
	items                *schemaV1
}

func object(v any) (map[string]any, bool) { m, ok := v.(map[string]any); return m, ok }
func exactKeys(m map[string]any, allowed ...string) bool {
	for k := range m {
		found := false
		for _, a := range allowed {
			if k == a {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
func uintValue(v any) (uint64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	out, err := strconv.ParseUint(string(n), 10, 64)
	return out, err == nil
}
func rational(v any) (*big.Rat, bool) {
	n, ok := v.(json.Number)
	if !ok || len(n) > 128 {
		return nil, false
	}
	out, ok := new(big.Rat).SetString(string(n))
	return out, ok
}

func compileSchema(v any, depth int, limits domain.ConfiguredInferenceLimitsV1) (*schemaV1, error) {
	if depth > int(limits.JSONDepth) {
		return nil, ErrContract
	}
	m, ok := object(v)
	if !ok {
		return nil, ErrContract
	}
	kind, ok := m["type"].(string)
	if !ok {
		return nil, ErrContract
	}
	s := &schemaV1{kind: kind}
	switch kind {
	case "object":
		if !exactKeys(m, "type", "properties", "required", "additionalProperties") {
			return nil, ErrContract
		}
		additional, ok := m["additionalProperties"].(bool)
		if !ok || additional {
			return nil, ErrContract
		}
		props, ok := object(m["properties"])
		if !ok || uint64(len(props)) > limits.JSONMembers {
			return nil, ErrContract
		}
		s.properties = make(map[string]*schemaV1, len(props))
		for name, p := range props {
			if !validString(name, limits.ToolArgumentsBytes, false) {
				return nil, ErrContract
			}
			child, err := compileSchema(p, depth+1, limits)
			if err != nil {
				return nil, ErrContract
			}
			s.properties[name] = child
		}
		required, ok := m["required"].([]any)
		if !ok || uint64(len(required)) > limits.JSONMembers {
			return nil, ErrContract
		}
		s.required = make(map[string]bool, len(required))
		for _, v := range required {
			name, ok := v.(string)
			if !ok || s.required[name] || s.properties[name] == nil {
				return nil, ErrContract
			}
			s.required[name] = true
		}
	case "string":
		if !exactKeys(m, "type", "enum", "minLength", "maxLength") {
			return nil, ErrContract
		}
		for _, p := range []struct {
			name string
			dst  **uint64
		}{{"minLength", &s.minLength}, {"maxLength", &s.maxLength}} {
			if v, ok := m[p.name]; ok {
				n, ok := uintValue(v)
				if !ok || n > limits.ToolArgumentsBytes {
					return nil, ErrContract
				}
				*p.dst = &n
			}
		}
		if s.minLength != nil && s.maxLength != nil && *s.minLength > *s.maxLength {
			return nil, ErrContract
		}
		if raw, exists := m["enum"]; exists {
			a, ok := raw.([]any)
			if !ok || len(a) == 0 {
				return nil, ErrContract
			}
			s.enum = make(map[string]bool, len(a))
			for _, v := range a {
				text, ok := v.(string)
				if !ok || s.enum[text] || !validString(text, limits.ToolArgumentsBytes, true) {
					return nil, ErrContract
				}
				s.enum[text] = true
			}
		}
	case "number", "integer":
		if !exactKeys(m, "type", "minimum", "maximum") {
			return nil, ErrContract
		}
		for _, p := range []struct {
			name string
			dst  **big.Rat
		}{{"minimum", &s.minimum}, {"maximum", &s.maximum}} {
			if v, ok := m[p.name]; ok {
				n, ok := rational(v)
				if !ok || (kind == "integer" && !n.IsInt()) {
					return nil, ErrContract
				}
				*p.dst = n
			}
		}
		if s.minimum != nil && s.maximum != nil && s.minimum.Cmp(s.maximum) > 0 {
			return nil, ErrContract
		}
	case "array":
		if !exactKeys(m, "type", "items", "minItems", "maxItems") {
			return nil, ErrContract
		}
		var good bool
		s.minItems, good = uintValue(m["minItems"])
		if !good {
			return nil, ErrContract
		}
		s.maxItems, good = uintValue(m["maxItems"])
		if !good || s.maxItems < s.minItems || s.maxItems > limits.JSONMembers {
			return nil, ErrContract
		}
		var err error
		s.items, err = compileSchema(m["items"], depth+1, limits)
		if err != nil {
			return nil, ErrContract
		}
	case "boolean", "null":
		if !exactKeys(m, "type") {
			return nil, ErrContract
		}
	default:
		return nil, ErrContract
	}
	return s, nil
}

func (s *schemaV1) accepts(v any) bool {
	switch s.kind {
	case "object":
		m, ok := object(v)
		if !ok {
			return false
		}
		for k := range s.required {
			if _, ok := m[k]; !ok {
				return false
			}
		}
		for k, v := range m {
			child := s.properties[k]
			if child == nil || !child.accepts(v) {
				return false
			}
		}
		return true
	case "string":
		text, ok := v.(string)
		if !ok {
			return false
		}
		n := uint64(utf8.RuneCountInString(text))
		return (s.enum == nil || s.enum[text]) && (s.minLength == nil || n >= *s.minLength) && (s.maxLength == nil || n <= *s.maxLength)
	case "number", "integer":
		n, ok := rational(v)
		return ok && (s.kind != "integer" || n.IsInt()) && (s.minimum == nil || n.Cmp(s.minimum) >= 0) && (s.maximum == nil || n.Cmp(s.maximum) <= 0)
	case "array":
		a, ok := v.([]any)
		if !ok || uint64(len(a)) < s.minItems || uint64(len(a)) > s.maxItems {
			return false
		}
		for _, item := range a {
			if !s.items.accepts(item) {
				return false
			}
		}
		return true
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "null":
		return v == nil
	}
	return false
}
