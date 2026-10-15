package attachedworkersealedinput

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// This is closed typed-data preflight, not a native allocator or fit proof.
// It scans the original arena without token strings/maps/slices, counts logical
// typed/string/base64/collection payloads, then reserves those on the SAME
// acquisition ledger before encoding/json may allocate the retained graph.
func decodePiAcquiredInput(ctx context.Context, body []byte, a *attachedworkerdaemontransport.PiInputAcquisitionV1) (input attachedworkerdaemontransport.SealedInputV1, err error) {
	if ctx == nil || ctx.Err() != nil || a == nil {
		return input, ErrUnavailable
	}
	l := a.Profile().Limits
	if len(body) == 0 || uint64(len(body)) > min(uint64(maxInputBytes), l.CanonicalInputBytes) ||
		a.Reserve(2*uint64(len(body)), 32*l.JSONDepth, 0) != nil {
		return input, ErrUnavailable
	}
	p := piInputPreflight{body: body, ctx: ctx, maxDepth: l.JSONDepth, maxNodes: l.JSONNodes}
	if !utf8.Valid(body) || !p.value(reflect.TypeOf(input), 1) {
		return input, ErrUnavailable
	}
	p.space()
	if p.at != len(body) || ctx.Err() != nil || p.work > math.MaxUint64-uint64(len(body)) ||
		a.Reserve(uint64(len(body))+p.work, p.payload, p.nodes) != nil {
		return input, ErrUnavailable
	}
	if json.Unmarshal(body, &input) != nil || ctx.Err() != nil || input.Credential != nil {
		clearResult(&input)
		return input, ErrUnavailable
	}
	return input, nil
}

type piInputPreflight struct {
	body                 []byte
	at                   int
	ctx                  context.Context
	maxDepth, maxNodes   uint64
	nodes, payload, work uint64
}

func (p *piInputPreflight) space() {
	for p.at < len(p.body) {
		switch p.body[p.at] {
		case ' ', '\t', '\r', '\n':
			p.at++
		default:
			return
		}
	}
}
func (p *piInputPreflight) consume(b byte) bool {
	p.space()
	if p.at >= len(p.body) || p.body[p.at] != b {
		return false
	}
	p.at++
	return true
}
func (p *piInputPreflight) literal(s string) bool {
	if !bytes.HasPrefix(p.body[p.at:], []byte(s)) {
		return false
	}
	p.at += len(s)
	return true
}
func (p *piInputPreflight) add(n uint64) bool {
	if n > math.MaxUint64-p.payload || !p.copyWork(n) {
		return false
	}
	p.payload += n
	return true
}
func (p *piInputPreflight) copyWork(n uint64) bool {
	if n > math.MaxUint64-p.work {
		return false
	}
	p.work += n
	return true
}

// Even if each append grew, all copied logical prefixes total n*(n-1)/2
// elements. Add one overlapping old logical prefix before typed decode. This
// bounds payload appearances only, NOT runtime backing-cap/allocator behavior.
func (p *piInputPreflight) arrayGrowth(t reflect.Type, n uint64) bool {
	size := uint64(t.Size())
	if n != 0 && size > math.MaxUint64/n {
		return false
	}
	logical := n * size
	if !p.add(logical) {
		return false
	}
	if n < 2 {
		return true
	}
	prefixes := n * (n - 1) / 2 // n is bounded by MaxSealedContextRecordsV1
	return size == 0 || prefixes <= math.MaxUint64/size && p.copyWork(prefixes*size)
}

func (p *piInputPreflight) value(t reflect.Type, depth uint64) bool {
	p.space()
	if depth > p.maxDepth || p.nodes == p.maxNodes || p.at == len(p.body) || p.ctx.Err() != nil {
		return false
	}
	p.nodes++
	if !p.add(uint64(t.Size())) {
		return false
	}
	if t.Kind() == reflect.Pointer {
		if p.literal("null") {
			return true
		}
		return p.value(t.Elem(), depth)
	}
	if t == reflect.TypeOf(time.Time{}) {
		_, ok := p.quoted(false)
		return ok
	}
	switch t.Kind() {
	case reflect.Struct:
		return p.object(t, depth)
	case reflect.Slice:
		if p.literal("null") {
			return true
		}
		if t.Elem().Kind() == reflect.Uint8 {
			raw, ok := p.quoted(true)
			if !ok || len(raw)%4 != 0 {
				return false
			}
			padding := 0
			for i, b := range raw {
				if b == '=' {
					padding++
					if i < len(raw)-2 || padding > 2 {
						return false
					}
				} else if padding != 0 || !(b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '+' || b == '/') {
					return false
				}
			}
			if padding == 2 && piInputBase64Value(raw[len(raw)-3])&15 != 0 ||
				padding == 1 && piInputBase64Value(raw[len(raw)-2])&3 != 0 {
				return false
			}
			return p.add(uint64(len(raw)/4*3 - padding))
		}
		if !p.consume('[') {
			return false
		}
		if p.consume(']') {
			return true
		}
		limit := 64
		if t.Elem() == reflect.TypeOf(domain.SessionEvent{}) || t.Elem().Kind() == reflect.Slice && t.Elem().Elem().Kind() == reflect.Uint8 {
			limit = attachedworkerdaemontransport.MaxSealedContextRecordsV1
		}
		for count := 0; count < limit; count++ {
			if !p.value(t.Elem(), depth+1) {
				return false
			}
			if p.consume(']') {
				return p.arrayGrowth(t.Elem(), uint64(count+1))
			}
			if !p.consume(',') {
				return false
			}
		}
	case reflect.String:
		_, ok := p.quoted(false)
		return ok
	case reflect.Bool:
		return p.literal("true") || p.literal("false")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return p.integer(t)
	}
	return false // no arbitrary maps/interfaces/custom wire shapes
}

func piInputBase64Value(b byte) byte {
	switch {
	case b >= 'A' && b <= 'Z':
		return b - 'A'
	case b >= 'a' && b <= 'z':
		return b - 'a' + 26
	case b >= '0' && b <= '9':
		return b - '0' + 52
	case b == '+':
		return 62
	default:
		return 63 // alphabet was checked before padding-bit inspection
	}
}

// Exact producer names, including promoted AttachmentRef fields in the
// flattened AttachmentPayload JSON. No case folding or escaped-key aliases.
func piInputField(t reflect.Type, ordinal int) (reflect.StructField, string, bool) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" || f.Tag.Get("json") == "-" {
			continue
		}
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if f.Anonymous && name == "" && f.Type.Kind() == reflect.Struct {
			for n := 0; ; n++ {
				child, childName, ok := piInputField(f.Type, n)
				if !ok {
					break
				}
				if ordinal == 0 {
					return child, childName, true
				}
				ordinal--
			}
			continue
		}
		if ordinal == 0 {
			if name == "" {
				name = f.Name
			}
			return f, name, true
		}
		ordinal--
	}
	return reflect.StructField{}, "", false
}

func (p *piInputPreflight) object(t reflect.Type, depth uint64) bool {
	if !p.consume('{') {
		return false
	}
	var seen [4]uint64
	finish := func() bool {
		for i := 0; i < 256; i++ {
			f, _, ok := piInputField(t, i)
			if !ok {
				return true
			}
			if !strings.Contains(f.Tag.Get("json"), ",omitempty") && seen[i/64]&(1<<uint(i%64)) == 0 {
				return false
			}
		}
		return false
	}
	if p.consume('}') {
		return finish()
	}
	for {
		key, ok := p.quoted(true)
		if !ok || !p.consume(':') || p.nodes == p.maxNodes {
			return false
		}
		p.nodes++ // object key is a retained logical decoder node
		matched := false
		for i := 0; i < 256; i++ {
			f, name, found := piInputField(t, i)
			if !found {
				break
			}
			if !bytes.Equal(key, []byte(name)) {
				continue
			}
			if seen[i/64]&(1<<uint(i%64)) != 0 {
				return false
			}
			seen[i/64] |= 1 << uint(i%64)
			p.space()
			if t == reflect.TypeOf(attachedworkerdaemontransport.SealedInputV1{}) && name == "Credential" &&
				!bytes.HasPrefix(p.body[p.at:], []byte("null")) {
				return false // API acquisition never decodes a lifecycle credential
			}
			if !p.value(f.Type, depth+1) {
				return false
			}
			matched = true
			break
		}
		if !matched {
			return false
		}
		if p.consume('}') {
			return finish()
		}
		if !p.consume(',') {
			return false
		}
	}
}

func (p *piInputPreflight) quoted(plain bool) ([]byte, bool) {
	if !p.consume('"') {
		return nil, false
	}
	start := p.at
	for p.at < len(p.body) {
		b := p.body[p.at]
		p.at++
		if b == '"' {
			raw := p.body[start : p.at-1]
			// A typed string and its logical unquote scratch coexist. The
			// stdlib scratch includes 2*UTFMax extra bytes; valid UTF8 and
			// complete surrogate pairs cannot expand beyond this wire bound.
			n := uint64(len(raw))
			return raw, p.add(2*n+2*utf8.UTFMax) && p.copyWork(n)
		}
		if b < 0x20 {
			return nil, false
		}
		if b != '\\' {
			continue
		}
		if plain || p.at == len(p.body) {
			return nil, false
		}
		escape := p.body[p.at]
		p.at++
		switch escape {
		case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
		case 'u':
			code, ok := p.hex4()
			if !ok || code >= 0xdc00 && code <= 0xdfff {
				return nil, false
			}
			if code >= 0xd800 && code <= 0xdbff {
				if !p.literal(`\u`) {
					return nil, false
				}
				low, ok := p.hex4()
				if !ok || low < 0xdc00 || low > 0xdfff {
					return nil, false
				}
			}
		default:
			return nil, false
		}
	}
	return nil, false
}

func (p *piInputPreflight) hex4() (uint16, bool) {
	if p.at+4 > len(p.body) {
		return 0, false
	}
	var code uint16
	for _, h := range p.body[p.at : p.at+4] {
		code <<= 4
		switch {
		case h >= '0' && h <= '9':
			code |= uint16(h - '0')
		case h >= 'a' && h <= 'f':
			code |= uint16(h-'a') + 10
		case h >= 'A' && h <= 'F':
			code |= uint16(h-'A') + 10
		default:
			return 0, false
		}
	}
	p.at += 4
	return code, true
}

func (p *piInputPreflight) integer(t reflect.Type) bool {
	negative := false
	if p.body[p.at] == '-' {
		negative = true
		p.at++
	}
	if p.at == len(p.body) || p.body[p.at] < '0' || p.body[p.at] > '9' || negative && t.Kind() >= reflect.Uint {
		return false
	}
	start := p.at
	var n uint64
	for p.at < len(p.body) && p.body[p.at] >= '0' && p.body[p.at] <= '9' {
		digit := uint64(p.body[p.at] - '0')
		if n > (math.MaxUint64-digit)/10 {
			return false
		}
		n = n*10 + digit
		p.at++
	}
	if p.at-start > 1 && p.body[start] == '0' || negative && n == 0 {
		return false
	}
	bits := t.Bits()
	if t.Kind() >= reflect.Uint {
		return bits == 64 || n < uint64(1)<<bits
	}
	limit := uint64(1) << (bits - 1)
	return n < limit || negative && n == limit
}
