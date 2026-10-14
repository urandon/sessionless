package pisdkmedia

import (
	"context"
	"encoding/base64"
	"errors"
	"math"
	"sort"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/codexexec"
)

var ErrProjection = errors.New("Pi request projection denied")
var ErrMissingProducer = errors.New("Pi request producer unavailable")

// Ordered JSON DATA only, not executable schema/provider/authority. We reject
// unsupported noninteger Number values rather than approximate native output.
type wireField struct {
	name  string
	value *wireValue
}
type wireValue struct {
	kind    byte
	text    string
	raw     []byte
	fields  []wireField
	items   []*wireValue
	number  int64
	boolean bool
	mime    string
}
type wireParser struct {
	ctx                              context.Context
	b                                []byte
	at                               int
	depth, nodes, maxDepth, maxNodes uint64
	ledger                           *codexexec.CanonicalMediaLedgerV1
}

func checkedAdd(values ...uint64) (uint64, error) {
	var n uint64
	for _, v := range values {
		if v > math.MaxUint64-n {
			return 0, ErrProjection
		}
		n += v
	}
	return n, nil
}
func checkedMul(a, b uint64) (uint64, error) {
	if b != 0 && a > math.MaxUint64/b {
		return 0, ErrProjection
	}
	return a * b, nil
}
func (p *wireParser) ws() {
	for p.at < len(p.b) && (p.b[p.at] == ' ' || p.b[p.at] == '\n' || p.b[p.at] == '\r' || p.b[p.at] == '\t') {
		p.at++
	}
}
func (p *wireParser) node() error {
	if p.ctx.Err() != nil || p.nodes >= p.maxNodes || p.ledger.Reserve(0, 0, 1) != nil {
		return ErrProjection
	}
	p.nodes++
	return nil
}
func parseWireData(ctx context.Context, b []byte, depth, nodes uint64, ledger *codexexec.CanonicalMediaLedgerV1) (*wireValue, error) {
	if ctx == nil || ctx.Err() != nil || len(b) == 0 || !utf8.Valid(b) || depth == 0 || nodes == 0 || ledger == nil {
		return nil, ErrProjection
	}
	// Decoded key/value buffers and their immutable Go strings: four UTF8
	// appearances per source byte is a logical upper bound, not heap overhead.
	cost, e := checkedMul(uint64(len(b)), 4)
	if e != nil || ledger.Reserve(cost, cost, 0) != nil {
		return nil, ErrProjection
	}
	p := wireParser{ctx: ctx, b: b, maxDepth: depth, maxNodes: nodes, ledger: ledger}
	v, e := p.value()
	p.ws()
	if e != nil || p.at != len(b) {
		return nil, ErrProjection
	}
	return v, nil
}
func (p *wireParser) string() (string, error) {
	if p.at >= len(p.b) || p.b[p.at] != '"' {
		return "", ErrProjection
	}
	p.at++
	start := p.at
	// Find/validate grammar before making the source-bounded buffer.
	for p.at < len(p.b) {
		c := p.b[p.at]
		p.at++
		if c == '"' {
			end := p.at - 1
			out := make([]byte, 0, end-start)
			for i := start; i < end; {
				c := p.b[i]
				i++
				if c != '\\' {
					out = append(out, c)
					continue
				}
				if i >= end {
					return "", ErrProjection
				}
				c = p.b[i]
				i++
				switch c {
				case '"', '\\', '/':
					out = append(out, c)
				case 'b':
					out = append(out, '\b')
				case 'f':
					out = append(out, '\f')
				case 'n':
					out = append(out, '\n')
				case 'r':
					out = append(out, '\r')
				case 't':
					out = append(out, '\t')
				case 'u':
					if i+4 > end {
						return "", ErrProjection
					}
					h, e := hex4(p.b[i : i+4])
					if e != nil {
						return "", e
					}
					i += 4
					r := rune(h)
					if h >= 0xd800 && h <= 0xdbff {
						if i+6 > end || p.b[i] != '\\' || p.b[i+1] != 'u' {
							return "", ErrProjection
						}
						lo, e := hex4(p.b[i+2 : i+6])
						if e != nil || lo < 0xdc00 || lo > 0xdfff {
							return "", ErrProjection
						}
						r = utf16.DecodeRune(r, rune(lo))
						i += 6
					} else if h >= 0xdc00 && h <= 0xdfff {
						return "", ErrProjection
					}
					out = utf8.AppendRune(out, r)
				default:
					return "", ErrProjection
				}
			}
			if !utf8.Valid(out) {
				return "", ErrProjection
			}
			return string(out), nil
		}
		if c < 32 {
			return "", ErrProjection
		}
		if c == '\\' {
			if p.at >= len(p.b) {
				return "", ErrProjection
			}
			esc := p.b[p.at]
			p.at++
			if esc == 'u' {
				if p.at+4 > len(p.b) {
					return "", ErrProjection
				}
				if _, e := hex4(p.b[p.at : p.at+4]); e != nil {
					return "", e
				}
				p.at += 4
			} else if esc != '"' && esc != '\\' && esc != '/' && esc != 'b' && esc != 'f' && esc != 'n' && esc != 'r' && esc != 't' {
				return "", ErrProjection
			}
		}
	}
	return "", ErrProjection
}
func hex4(b []byte) (uint16, error) {
	if len(b) != 4 {
		return 0, ErrProjection
	}
	var n uint16
	for _, c := range b {
		n *= 16
		switch {
		case c >= '0' && c <= '9':
			n += uint16(c - '0')
		case c >= 'a' && c <= 'f':
			n += uint16(c - 'a' + 10)
		case c >= 'A' && c <= 'F':
			n += uint16(c - 'A' + 10)
		default:
			return 0, ErrProjection
		}
	}
	return n, nil
}
func (p *wireParser) value() (*wireValue, error) {
	p.ws()
	if p.at >= len(p.b) || p.node() != nil {
		return nil, ErrProjection
	}
	c := p.b[p.at]
	if c == '{' || c == '[' {
		p.depth++
		if p.depth > p.maxDepth {
			return nil, ErrProjection
		}
		defer func() { p.depth-- }()
		p.at++
		v := &wireValue{kind: 'o'}
		close := byte('}')
		if c == '[' {
			v.kind = 'a'
			close = ']'
		}
		p.ws()
		if p.at < len(p.b) && p.b[p.at] == close {
			p.at++
			return v, nil
		}
		for {
			p.ws()
			if c == '{' {
				if p.node() != nil {
					return nil, ErrProjection
				}
				key, e := p.string()
				if e != nil {
					return nil, e
				}
				for _, f := range v.fields {
					if f.name == key {
						return nil, ErrProjection
					}
				}
				p.ws()
				if p.at >= len(p.b) || p.b[p.at] != ':' {
					return nil, ErrProjection
				}
				p.at++
				child, e := p.value()
				if e != nil {
					return nil, e
				}
				v.fields = append(v.fields, wireField{key, child})
			} else {
				child, e := p.value()
				if e != nil {
					return nil, e
				}
				v.items = append(v.items, child)
			}
			p.ws()
			if p.at >= len(p.b) {
				return nil, ErrProjection
			}
			d := p.b[p.at]
			p.at++
			if d == close {
				if c == '{' {
					sort.SliceStable(v.fields, func(i, j int) bool {
						a, ai := indexKey(v.fields[i].name)
						b, bi := indexKey(v.fields[j].name)
						if ai && bi {
							return a < b
						}
						return ai && !bi
					})
				}
				return v, nil
			}
			if d != ',' {
				return nil, ErrProjection
			}
		}
	}
	if c == '"' {
		s, e := p.string()
		return &wireValue{kind: 's', text: s}, e
	}
	for _, lit := range []string{"null", "true", "false"} {
		if len(p.b)-p.at >= len(lit) && string(p.b[p.at:p.at+len(lit)]) == lit {
			p.at += len(lit)
			if lit == "null" {
				return &wireValue{kind: 'z'}, nil
			}
			return &wireValue{kind: 'b', boolean: lit == "true"}, nil
		}
	}
	start := p.at
	if c == '-' {
		p.at++
	}
	if p.at >= len(p.b) {
		return nil, ErrProjection
	}
	if p.b[p.at] == '0' {
		p.at++
	} else {
		if p.b[p.at] < '1' || p.b[p.at] > '9' {
			return nil, ErrProjection
		}
		for p.at < len(p.b) && p.b[p.at] >= '0' && p.b[p.at] <= '9' {
			p.at++
		}
	}
	if p.at < len(p.b) && (p.b[p.at] == '.' || p.b[p.at] == 'e' || p.b[p.at] == 'E') {
		return nil, ErrMissingProducer
	}
	if p.at-start > 17 {
		return nil, ErrProjection
	}
	n, e := strconv.ParseInt(string(p.b[start:p.at]), 10, 64)
	if e != nil || n > 9007199254740991 || n < -9007199254740991 {
		return nil, ErrProjection
	}
	return &wireValue{kind: 'n', number: n}, nil
}
func indexKey(s string) (uint64, bool) {
	if len(s) == 0 || len(s) > 10 || len(s) > 1 && s[0] == '0' {
		return 0, false
	}
	n, e := strconv.ParseUint(s, 10, 32)
	return n, e == nil && n < 4294967295
}

// The same walker measures then emits into ONE exact-capacity byte slice.
// It never calls JSON stringify/Marshal or emits a second expected body.
type wireWriter struct {
	out                                                 []byte
	bytes, units, nodes, depth, maxDepth, maxNodes, cap uint64
	err                                                 error
}

func (w *wireWriter) add(b []byte) {
	if w.err != nil {
		return
	}
	if uint64(len(b)) > w.cap-w.bytes || !utf8.Valid(b) {
		w.err = ErrProjection
		return
	}
	w.bytes += uint64(len(b))
	for len(b) > 0 {
		r, n := utf8.DecodeRune(b)
		b = b[n:]
		w.units++
		if r > 0xffff {
			w.units++
		}
	}
}
func (w *wireWriter) literal(s string) {
	if w.err != nil {
		return
	}
	if uint64(len(s)) > w.cap-w.bytes {
		w.err = ErrProjection
		return
	}
	w.bytes += uint64(len(s))
	w.units += uint64(len(s))
	if w.out != nil {
		w.out = append(w.out, s...)
	}
}
func (w *wireWriter) raw(b []byte) {
	old := w.bytes
	w.add(b)
	if w.err == nil && w.out != nil && w.bytes >= old {
		w.out = append(w.out, b...)
	}
}
func (w *wireWriter) node() {
	if w.err != nil {
		return
	}
	if w.nodes >= w.maxNodes {
		w.err = ErrProjection
	} else {
		w.nodes++
	}
}
func (w *wireWriter) quote(s string, b []byte) {
	w.node()
	w.literal("\"")
	if b == nil {
		w.stringPart(s)
	} else {
		w.bytesPart(b)
	}
	w.literal("\"")
}
func (w *wireWriter) stringPart(s string) {
	if w.err != nil {
		return
	}
	if uint64(len(s)) > w.cap-w.bytes {
		w.err = ErrProjection
		return
	}
	for len(s) > 0 && w.err == nil {
		r, n := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError && n == 1 {
			w.err = ErrProjection
			return
		}
		w.rune(r)
		s = s[n:]
	}
}
func (w *wireWriter) bytesPart(b []byte) {
	if w.err != nil {
		return
	}
	if uint64(len(b)) > w.cap-w.bytes {
		w.err = ErrProjection
		return
	}
	for len(b) > 0 && w.err == nil {
		r, n := utf8.DecodeRune(b)
		if r == utf8.RuneError && n == 1 {
			w.err = ErrProjection
			return
		}
		w.rune(r)
		b = b[n:]
	}
}
func (w *wireWriter) rune(r rune) {
	switch r {
	case '"':
		w.literal("\\\"")
	case '\\':
		w.literal("\\\\")
	case '\b':
		w.literal("\\b")
	case '\f':
		w.literal("\\f")
	case '\n':
		w.literal("\\n")
	case '\r':
		w.literal("\\r")
	case '\t':
		w.literal("\\t")
	default:
		if r < 32 {
			const h = "0123456789abcdef"
			var b = [6]byte{'\\', 'u', '0', '0', h[byte(r)>>4], h[byte(r)&15]}
			w.raw(b[:])
		} else {
			var b [4]byte
			n := utf8.EncodeRune(b[:], r)
			w.raw(b[:n])
		}
	}
}
func (w *wireWriter) open(c byte) {
	if w.err != nil {
		return
	}
	w.node()
	if w.err != nil {
		return
	}
	w.depth++
	if w.depth > w.maxDepth {
		w.err = ErrProjection
	}
	var b = [1]byte{c}
	w.raw(b[:])
}
func (w *wireWriter) close(c byte) {
	if w.err != nil {
		return
	}
	var b = [1]byte{c}
	w.raw(b[:])
	w.depth--
}
func (w *wireWriter) key(s string) { w.quote(s, nil); w.literal(":") }
func (w *wireWriter) integer(n int64) {
	w.node()
	var b [20]byte
	w.raw(strconv.AppendInt(b[:0], n, 10))
}
func (w *wireWriter) value(v *wireValue) {
	if w.err != nil {
		return
	}
	if v == nil {
		w.err = ErrProjection
		return
	}
	switch v.kind {
	case 'o':
		w.open('{')
		for i, f := range v.fields {
			if w.err != nil {
				return
			}
			if i > 0 {
				w.literal(",")
			}
			w.key(f.name)
			w.value(f.value)
		}
		w.close('}')
	case 'a':
		w.open('[')
		for i, x := range v.items {
			if w.err != nil {
				return
			}
			if i > 0 {
				w.literal(",")
			}
			w.value(x)
		}
		w.close(']')
	case 's':
		w.quote(v.text, nil)
	case 't':
		w.quote("", v.raw)
	case 'n':
		w.integer(v.number)
	case 'b':
		w.node()
		if v.boolean {
			w.literal("true")
		} else {
			w.literal("false")
		}
	case 'z':
		w.node()
		w.literal("null")
	default:
		w.err = ErrProjection
	}
}
func (w *wireWriter) dataURL(mime string, body []byte) {
	if w.err != nil {
		return
	}
	w.node()
	w.literal("\"data:")
	w.stringPart(mime)
	w.literal(";base64,")
	if w.err != nil {
		return
	}
	n := uint64(base64.StdEncoding.EncodedLen(len(body)))
	if n > w.cap-w.bytes {
		w.err = ErrProjection
		return
	}
	w.bytes += n
	w.units += n
	if w.out != nil {
		w.out = base64.StdEncoding.AppendEncode(w.out, body)
	}
	w.literal("\"")
}
