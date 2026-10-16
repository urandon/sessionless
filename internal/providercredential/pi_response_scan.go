package providercredential

import (
	"context"
	"math"
	"reflect"
	"strconv"
	"sync/atomic"
	"unicode/utf16"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Private parsed DATA, NOT an allow predicate, replay grant or Pi parser. The
// closed policy and semantic consumers run on these nodes while the same owner
// retains its file/use gate. None is a serving, replay or producer witness.
type piResponseNode struct {
	kind             byte // o/a/s/d/t/f/n; object keys are separate s nodes
	key, child, next int32
	from, to         uint32 // exact decoded UTF16 code units, including lone surrogates
	number           float64
}

type piResponseEvent struct {
	root             int32
	frame            uint32
	nameFrom, nameTo uint32
	named            bool
	slot             piResponseSlot // at most one new Pi output block per event
}

type piResponseScan struct {
	limits               domain.AttachedPiSDKNativeLimitsV1
	nodes                []piResponseNode
	text                 []uint16
	data                 []byte
	events               []piResponseEvent
	failure              []uint32
	identities           []piResponseIdentity
	identityN, retainedN uint32
	responseID           int32
	nodeN, textN, eventN uint32
	frames, work, live   uint64
	maxNodes, maxEvents  uint64
	ctx                  context.Context
	raw, key             []byte // borrowed originals; original use gate must remain held
	spent                atomic.Bool
	parsed               atomic.Bool
	semanticSpent        atomic.Bool
	policySpent          atomic.Bool
	denied               bool
	erased               atomic.Bool
}

func (*piResponseScan) String() string     { return "piResponseScan{unapproved:[redacted]}" }
func (p *piResponseScan) GoString() string { return p.String() }

func newPiResponseScan(ledger attachedworkerdaemon.PreparedAPILedgerV1, l domain.AttachedPiSDKNativeLimitsV1, keyMax uint64) (*piResponseScan, error) {
	r, h, e := l.ResponseBodyBytes, l.ResponseHeaderBytes, l.SSEEventBytes
	if nilPiLedger(ledger) || r == 0 || r > 4<<20 || h == 0 || h > 1<<20 || e == 0 || e > r || l.SSELineBytes == 0 || l.SSELineBytes > e || l.SSEEvents == 0 || l.JSONNodes == 0 || l.JSONDepth == 0 || l.JSONDepth > 32 || keyMax == 0 || keyMax > 64<<10 || l.ResponseCopyWorkBytes == 0 {
		return nil, ErrInvalid
	}
	// Physical wire size bounds the possible counts even if a forged DATA
	// profile declares huge numeric caps. This does not lower any possible cohort.
	n, events := min(l.JSONNodes, 2*r+1), min(l.SSEEvents, r/2+1)
	metadata := uint64(reflect.TypeOf(piResponseScan{}).Size())
	nodeBytes := (n + 1) * uint64(reflect.TypeOf(piResponseNode{}).Size())
	eventBytes := events * uint64(reflect.TypeOf(piResponseEvent{}).Size())
	identityBytes := n * uint64(reflect.TypeOf(piResponseIdentity{}).Size())
	// All decoded strings together use at most R UTF16 units. Number conversion
	// strings/errors reserve TWO R-byte copies (conversion+NumError.Num clone)
	// and96*N metadata bytes in this
	// conservative logical schedule. No general JSON map/tree is materialized.
	// Runtime allocator/stack/strconv backend fit is separate, NOT attested here.
	live := metadata + nodeBytes + 2*r + e + eventBytes + identityBytes + 4*keyMax + 2*r + 96*n + 8192
	// Initialization and ONE complete retirement traversal are prepaid before
	// allocation. Retirement still runs after cancellation without new credit.
	arenaBytes := metadata + nodeBytes + 2*r + e + eventBytes + identityBytes + 4*keyMax
	initialWork := 2 * arenaBytes
	httpLive := 2*h + r + uint64(reflect.TypeOf(piHTTPReceive{}).Size())
	if live > l.ResponseAllocationBytes || httpLive > l.ResponseAllocationBytes-live || initialWork > l.ResponseCopyWorkBytes {
		return nil, ErrInvalid
	}
	if ledger.Reserve(l.ResponseCopyWorkBytes, live, 7+4*n+events) != nil {
		return nil, ErrInvalid
	}
	return &piResponseScan{limits: l, nodes: make([]piResponseNode, int(n+1)), text: make([]uint16, int(r)), data: make([]byte, int(e)),
		events: make([]piResponseEvent, int(events)), identities: make([]piResponseIdentity, int(n)), failure: make([]uint32, int(keyMax)), live: live, work: initialWork, maxNodes: n, maxEvents: events}, nil
}

func (q *piResponseScan) spend(n uint64) bool {
	if q.denied || q.work > q.limits.ResponseCopyWorkBytes || n > q.limits.ResponseCopyWorkBytes-q.work || q.ctx.Err() != nil {
		q.denied = true
		return false
	}
	q.work += n
	return true
}

func (q *piResponseScan) prepareMatcher(key []byte) bool {
	if len(key) == 0 || len(key) > len(q.failure) || !q.spend(uint64(len(key))) {
		return false
	}
	for _, c := range key {
		if c < 33 || c > 126 {
			return false
		}
	}
	q.key = key // no additional plaintext copy
	if !q.spend(6 * uint64(len(key))) {
		return false
	} // ordinary failure-table pass
	for i, j := 1, uint32(0); i < len(key); i++ {
		for j > 0 && key[i] != key[j] {
			if !q.spend(5) {
				return false
			}
			j = q.failure[j-1]
		}
		if key[i] == key[j] {
			j++
		}
		q.failure[i] = j
	}
	return true
}

func (q *piResponseScan) match(j uint32, c uint16, width uint64) (uint32, bool) {
	// Input payload width plus at most two ordinary ASCII-key comparisons.
	if !q.spend(width + 2) {
		return 0, false
	}
	for j > 0 && (c > 127 || byte(c) != q.key[j]) {
		if !q.spend(5) {
			return 0, false
		}
		j = q.failure[j-1]
	}
	if c < 128 && byte(c) == q.key[j] {
		j++
	}
	return j, int(j) != len(q.key)
}

func (q *piResponseScan) scanRaw(b []byte) bool {
	var j uint32
	for _, c := range b {
		var ok bool
		j, ok = q.match(j, uint16(c), 1)
		if !ok {
			return false
		}
	}
	return true
}

func (q *piResponseScan) scanString(from, to uint32) bool {
	var j uint32
	for _, c := range q.text[from:to] {
		var ok bool
		j, ok = q.match(j, c, 2)
		if !ok {
			return false
		}
	}
	return true
}

// scan parses complete framing DATA only. It intentionally does NOT accept
// event/effect shapes, validate terminal state or authorize replay. The future
// exact closed policy consumes the private graph before semantic checking. The retained
// key/context/response aliases require the SAME original file-use gate until
// that policy and semantic scan join, then clear before releasing its live debit.
func (q *piResponseScan) scan(ctx context.Context, view piUnapprovedHTTPView, key []byte) (err error) {
	if q == nil || !q.spent.CompareAndSwap(false, true) {
		return ErrConsumed
	}
	defer func() {
		if err != nil {
			q.clear()
		}
	}()
	if ctx == nil || ctx.Err() != nil || len(view.rawHeaders) == 0 || uint64(len(view.rawHeaders)) > q.limits.ResponseHeaderBytes || len(view.headers) == 0 || uint64(len(view.headers)) > q.limits.ResponseHeaderBytes || len(view.entity) == 0 || uint64(len(view.entity)) > q.limits.ResponseBodyBytes {
		return ErrInvalid
	}
	q.ctx, q.raw = ctx, view.entity
	if !q.prepareMatcher(key) || !q.scanRaw(view.rawHeaders) || !q.scanRaw(view.headers) || !q.scanRaw(view.entity) {
		return ErrDeliveryMismatch
	}
	start, line := 0, uint64(0)
	for i, c := range view.entity {
		if !q.spend(1) || c == 0 {
			return ErrInvalid
		}
		if c == '\r' || c == '\n' {
			line = 0
		} else {
			line++
		}
		if line > q.limits.SSELineBytes || uint64(i-start+1) > q.limits.SSEEventBytes {
			return ErrInvalid
		}
		// The native framer resets lookbehind after EVERY boundary. Never let
		// bytes from the previous frame manufacture an overlapping delimiter.
		boundary := i-start >= 1 && ((c == '\n' && view.entity[i-1] == '\n') || (c == '\r' && view.entity[i-1] == '\r')) ||
			i-start >= 3 && c == '\n' && view.entity[i-1] == '\r' && view.entity[i-2] == '\n' && view.entity[i-3] == '\r'
		if !boundary {
			continue
		}
		if q.frames == q.limits.SSEEvents || !q.spend(uint64(i-start+1)) || !utf8.Valid(view.entity[start:i+1]) {
			return ErrInvalid
		}
		q.frames++
		if q.frame(start, i+1) != nil {
			return ErrInvalid
		}
		start = i + 1
	}
	if start != len(view.entity) || ctx.Err() != nil || q.denied {
		return ErrInvalid
	}
	q.parsed.Store(true)
	return nil
}

func (q *piResponseScan) frame(start, end int) error {
	if !q.spend(2 * uint64(end-start)) {
		return ErrInvalid
	} // line and colon walks
	from, used, lines := start, 0, 0
	var nameFrom, nameTo uint32
	named := false
	dispatch := func() error {
		if lines == 0 && !named {
			return nil
		}
		if lines == 0 || uint64(q.eventN) >= q.maxEvents {
			return ErrInvalid
		}
		p := piResponseJSON{q: q, raw: q.data[:used]}
		root, err := p.parse()
		if err != nil {
			return err
		}
		if named && nameFrom != nameTo && !q.equalsBytes(q.field(root, "type"), q.raw[nameFrom:nameTo]) {
			return ErrInvalid
		}
		if q.denied {
			return ErrInvalid
		}
		q.events[q.eventN] = piResponseEvent{root: root, frame: uint32(q.frames), named: named, nameFrom: nameFrom, nameTo: nameTo}
		q.eventN++
		used, lines, named = 0, 0, false
		return nil
	}
	for at := start; at < end; at++ {
		c := q.raw[at]
		if c != '\r' && c != '\n' {
			continue
		}
		if at-from >= 3 && q.raw[from] == 0xef && q.raw[from+1] == 0xbb && q.raw[from+2] == 0xbf {
			return ErrInvalid
		}
		if at == from {
			if dispatch() != nil {
				return ErrInvalid
			}
		} else if q.raw[from] != ':' {
			colon := from
			for colon < at && q.raw[colon] != ':' {
				colon++
			}
			value := colon
			if value < at {
				value++
			}
			if value < at && q.raw[value] == ' ' {
				value++
			}
			switch string(q.raw[from:colon]) {
			case "data":
				if lines > 0 {
					if used == len(q.data) || !q.spend(1) {
						return ErrInvalid
					}
					q.data[used], used = '\n', used+1
				}
				if at-value > len(q.data)-used || !q.spend(uint64(at-value)) {
					return ErrInvalid
				}
				used += copy(q.data[used:], q.raw[value:at])
				lines++
			case "event":
				if named {
					return ErrInvalid
				}
				nameFrom, nameTo, named = uint32(value), uint32(at), true
			default:
				return ErrInvalid
			}
		}
		if c == '\r' && at+1 < end && q.raw[at+1] == '\n' {
			at++
		}
		from = at + 1
	}
	if from != end || lines != 0 || named {
		return ErrInvalid
	}
	// The scratch is private and only [:used] is parsed. Reuse it without an
	// O(maxEventBytes) clear per comment/event; one final clear was prepaid.
	return nil
}

func (q *piResponseScan) clear() {
	q.spent.Store(true)
	q.semanticSpent.Store(true)
	q.policySpent.Store(true)
	if !q.erased.CompareAndSwap(false, true) {
		return
	}
	clear(q.nodes)
	clear(q.text)
	clear(q.data)
	clear(q.events)
	clear(q.failure)
	clear(q.identities)
	q.responseID = 0
	q.ctx, q.raw, q.key = nil, nil, nil
	// Work/counts are not replenished; spent remains irreversible.
}

func (q *piResponseScan) equalsBytes(i int32, b []byte) bool {
	if i == 0 || q.nodes[i].kind != 's' {
		return false
	}
	n := q.nodes[i]
	if !q.spend(3 * uint64(len(b))) {
		return false
	}
	// Named SSE events and closed policy field names are ASCII. Never silently
	// normalize a non-ASCII member/event name into an admitted one.
	if n.to-n.from != uint32(len(b)) {
		return false
	}
	for j, c := range b {
		if c >= 128 || q.text[n.from+uint32(j)] != uint16(c) {
			return false
		}
	}
	return true
}

func (q *piResponseScan) field(i int32, name string) int32 {
	if i == 0 || q.nodes[i].kind != 'o' {
		return 0
	}
	for child := q.nodes[i].child; child != 0; child = q.nodes[child].next {
		if !q.spend(16) {
			return 0
		}
		if q.equalsBytes(q.nodes[child].key, []byte(name)) {
			return child
		}
	}
	return 0
}

type piResponseJSON struct {
	q     *piResponseScan
	raw   []byte
	at    int
	depth uint64
	// ONLY the private pre-Issue projection parser sets this. Response parsing
	// defaults to credential scanning; no caller or provider selects this mode.
	trustedExpected  bool
	omitInputStrings bool
}

func (p *piResponseJSON) ws() {
	for p.at < len(p.raw) && (p.raw[p.at] == ' ' || p.raw[p.at] == '\t' || p.raw[p.at] == '\r' || p.raw[p.at] == '\n') {
		p.at++
	}
}
func (p *piResponseJSON) node(kind byte) (int32, error) {
	if uint64(p.q.nodeN) >= p.q.maxNodes || !p.q.spend(uint64(reflect.TypeOf(piResponseNode{}).Size())) {
		return 0, ErrInvalid
	}
	p.q.nodeN++
	i := int32(p.q.nodeN)
	p.q.nodes[i].kind = kind
	return i, nil
}
func (p *piResponseJSON) unit(c uint16) bool {
	if int(p.q.textN) >= len(p.q.text) || !p.q.spend(2) {
		return false
	}
	p.q.text[p.q.textN] = c
	p.q.textN++
	return true
}
func piResponseHex(c byte) (uint16, bool) {
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
func (p *piResponseJSON) stringUnit(c uint16, keep bool) bool {
	if !keep {
		return p.q.spend(2)
	}
	return p.unit(c)
}
func (p *piResponseJSON) quoted(keep bool) (int32, error) {
	if p.at >= len(p.raw) || p.raw[p.at] != '"' {
		return 0, ErrInvalid
	}
	i, err := p.node('s')
	if err != nil {
		return 0, err
	}
	p.q.nodes[i].from = p.q.textN
	p.at++
	for p.at < len(p.raw) {
		c := p.raw[p.at]
		p.at++
		if c == '"' {
			p.q.nodes[i].to = p.q.textN
			if !p.trustedExpected && !p.q.scanString(p.q.nodes[i].from, p.q.textN) {
				return 0, ErrDeliveryMismatch
			}
			return i, nil
		}
		if c < 32 {
			return 0, ErrInvalid
		}
		if c == '\\' {
			if p.at >= len(p.raw) {
				return 0, ErrInvalid
			}
			escape := p.raw[p.at]
			p.at++
			var v uint16
			switch escape {
			case '"', '\\', '/':
				v = uint16(escape)
			case 'b':
				v = '\b'
			case 'f':
				v = '\f'
			case 'n':
				v = '\n'
			case 'r':
				v = '\r'
			case 't':
				v = '\t'
			case 'u':
				if len(p.raw)-p.at < 4 {
					return 0, ErrInvalid
				}
				for range 4 {
					digit, ok := piResponseHex(p.raw[p.at])
					if !ok {
						return 0, ErrInvalid
					}
					v = v*16 + digit
					p.at++
				}
			default:
				return 0, ErrInvalid
			}
			if !p.stringUnit(v, keep) {
				return 0, ErrInvalid
			}
		} else if c < utf8.RuneSelf {
			if !p.stringUnit(uint16(c), keep) {
				return 0, ErrInvalid
			}
		} else {
			p.at--
			r, size := utf8.DecodeRune(p.raw[p.at:])
			if r == utf8.RuneError && size == 1 {
				return 0, ErrInvalid
			}
			p.at += size
			if r > 0xffff {
				a, b := utf16.EncodeRune(r)
				if !p.stringUnit(uint16(a), keep) || !p.stringUnit(uint16(b), keep) {
					return 0, ErrInvalid
				}
			} else if !p.stringUnit(uint16(r), keep) {
				return 0, ErrInvalid
			}
		}
	}
	return 0, ErrInvalid
}
func (p *piResponseJSON) sameKey(a, b int32) bool {
	x, y := p.q.nodes[a], p.q.nodes[b]
	if x.to-x.from != y.to-y.from {
		return false
	}
	if !p.q.spend(4 * uint64(x.to-x.from)) {
		return true
	} // budget exhaustion denies as duplicate
	for i := uint32(0); i < x.to-x.from; i++ {
		if p.q.text[x.from+i] != p.q.text[y.from+i] {
			return false
		}
	}
	return true
}
func (p *piResponseJSON) parse() (int32, error) {
	if !p.q.spend(uint64(len(p.raw))) {
		return 0, ErrInvalid
	} // complete lexical walk
	p.ws()
	if p.at == len(p.raw) || p.raw[p.at] != '{' {
		return 0, ErrInvalid
	}
	i, err := p.value()
	p.ws()
	if err != nil || p.at != len(p.raw) {
		return 0, ErrInvalid
	}
	return i, nil
}
func (p *piResponseJSON) value() (int32, error) {
	p.ws()
	if p.at == len(p.raw) || !p.q.spend(1) {
		return 0, ErrInvalid
	}
	c := p.raw[p.at]
	if c == '"' {
		return p.quoted(!p.omitInputStrings)
	}
	if c == '{' || c == '[' {
		p.depth++
		defer func() { p.depth-- }()
		if p.depth > p.q.limits.JSONDepth {
			return 0, ErrInvalid
		}
		kind, end := byte('o'), byte('}')
		if c == '[' {
			kind, end = 'a', ']'
		}
		i, err := p.node(kind)
		if err != nil {
			return 0, err
		}
		p.at++
		p.ws()
		if p.at < len(p.raw) && p.raw[p.at] == end {
			p.at++
			return i, nil
		}
		var last int32
		for {
			p.ws()
			var key int32
			if c == '{' {
				key, err = p.quoted(true)
				if err != nil {
					return 0, err
				}
				for child := p.q.nodes[i].child; child != 0; child = p.q.nodes[child].next {
					if !p.q.spend(1) || p.sameKey(key, p.q.nodes[child].key) {
						return 0, ErrInvalid
					}
				}
				p.ws()
				if p.at == len(p.raw) || p.raw[p.at] != ':' {
					return 0, ErrInvalid
				}
				p.at++
			}
			omit := p.omitInputStrings
			if p.trustedExpected && p.depth == 1 && p.q.equalsBytes(key, []byte("input")) {
				p.omitInputStrings = true
			}
			child, e := p.value()
			p.omitInputStrings = omit
			if e != nil {
				return 0, e
			}
			p.q.nodes[child].key = key
			if last == 0 {
				p.q.nodes[i].child = child
			} else {
				p.q.nodes[last].next = child
			}
			last = child
			p.ws()
			if p.at == len(p.raw) {
				return 0, ErrInvalid
			}
			d := p.raw[p.at]
			p.at++
			if d == end {
				return i, nil
			}
			if d != ',' {
				return 0, ErrInvalid
			}
		}
	}
	for _, literal := range [...]string{"null", "true", "false"} {
		if len(p.raw)-p.at >= len(literal) && string(p.raw[p.at:p.at+len(literal)]) == literal {
			p.at += len(literal)
			return p.node(literal[0])
		}
	}
	start := p.at
	if c == '-' {
		p.at++
	}
	digit := func() bool { return p.at < len(p.raw) && p.raw[p.at] >= '0' && p.raw[p.at] <= '9' }
	if p.at < len(p.raw) && p.raw[p.at] == '0' {
		p.at++
	} else {
		if !digit() || p.raw[p.at] == '0' {
			return 0, ErrInvalid
		}
		for digit() {
			p.at++
		}
	}
	if p.at < len(p.raw) && p.raw[p.at] == '.' {
		p.at++
		if !digit() {
			return 0, ErrInvalid
		}
		for digit() {
			p.at++
		}
	}
	if p.at < len(p.raw) && (p.raw[p.at] == 'e' || p.raw[p.at] == 'E') {
		p.at++
		if p.at < len(p.raw) && (p.raw[p.at] == '+' || p.raw[p.at] == '-') {
			p.at++
		}
		if !digit() {
			return 0, ErrInvalid
		}
		for digit() {
			p.at++
		}
	}
	if !p.q.spend(3*uint64(p.at-start) + 96) {
		return 0, ErrInvalid
	}
	n, err := strconv.ParseFloat(string(p.raw[start:p.at]), 64)
	if err != nil || math.IsInf(n, 0) || math.IsNaN(n) {
		return 0, ErrInvalid
	}
	i, err := p.node('d')
	if err == nil {
		p.q.nodes[i].number = n
	}
	return i, err
}
