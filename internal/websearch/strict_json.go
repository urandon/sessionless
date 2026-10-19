package websearch

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

// A bounded lexical pass is required because encoding/json replaces lone
// surrogates. The token pass then rejects duplicate/case-alias object members,
// trailing JSON and excessive depth/aggregate nodes before building the tree.
func strictJSON(body []byte, limit int) (any, error) {
	if len(body) == 0 || len(body) > limit || !utf8.Valid(body) {
		return nil, ErrResponse
	}
	for i := 0; i < len(body); i++ {
		if body[i] != '"' {
			continue
		}
		i++
		for ; i < len(body) && body[i] != '"'; i++ {
			if body[i] != '\\' {
				continue
			}
			i++
			if i >= len(body) {
				return nil, ErrResponse
			}
			if body[i] != 'u' {
				continue
			}
			if i+4 >= len(body) {
				return nil, ErrResponse
			}
			n, e := strconv.ParseUint(string(body[i+1:i+5]), 16, 16)
			if e != nil {
				return nil, ErrResponse
			}
			i += 4
			if n >= 0xdc00 && n <= 0xdfff {
				return nil, ErrResponse
			}
			if n >= 0xd800 && n <= 0xdbff {
				if i+6 >= len(body) || body[i+1] != '\\' || body[i+2] != 'u' {
					return nil, ErrResponse
				}
				low, e := strconv.ParseUint(string(body[i+3:i+7]), 16, 16)
				if e != nil || low < 0xdc00 || low > 0xdfff {
					return nil, ErrResponse
				}
				i += 6
			}
		}
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	nodes := 0
	var parse func(int) (any, error)
	parse = func(depth int) (any, error) {
		nodes++
		if depth > 32 || nodes > 4096 {
			return nil, ErrResponse
		}
		t, e := d.Token()
		if e != nil {
			return nil, ErrResponse
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return t, nil
		}
		switch delim {
		case '{':
			m := map[string]any{}
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				s, ok := k.(string)
				fold := strings.ToLower(s)
				if e != nil || !ok || seen[fold] {
					return nil, ErrResponse
				}
				seen[fold] = true
				v, e := parse(depth + 1)
				if e != nil {
					return nil, e
				}
				m[s] = v
			}
			end, e := d.Token()
			if e != nil || end != json.Delim('}') {
				return nil, ErrResponse
			}
			return m, nil
		case '[':
			a := []any{}
			for d.More() {
				v, e := parse(depth + 1)
				if e != nil {
					return nil, e
				}
				a = append(a, v)
			}
			end, e := d.Token()
			if e != nil || end != json.Delim(']') {
				return nil, ErrResponse
			}
			return a, nil
		default:
			return nil, ErrResponse
		}
	}
	v, e := parse(0)
	if e != nil {
		return nil, e
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, ErrResponse
	}
	return v, nil
}
func keys(m map[string]any, allowed ...string) bool {
	for k := range m {
		found := false
		for _, a := range allowed {
			found = found || k == a
		}
		if !found {
			return false
		}
	}
	return true
}
