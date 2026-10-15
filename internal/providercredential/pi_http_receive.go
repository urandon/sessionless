package providercredential

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"reflect"
	"strings"
	"sync/atomic"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

type piHTTPField struct{ nameStart, nameEnd, valueStart, valueEnd uint32 }

// Private framing DATA only, never response approval. Its single writer must
// hold the original file-use gate; retirement cancels/closes IO before joining
// that gate and clearing these buffers. No public receive/body method exists.
type piHTTPReceive struct {
	head, entity, replay           []byte
	fields                         [64]piHTTPField
	one                            [1]byte
	line                           [18]byte
	spent                          atomic.Bool
	headN, entityN, replayN, count int
	framing, framingLimit, live    uint64
	ctx                            context.Context
	source                         io.Reader
	eof                            bool
}

type piUnapprovedHTTPView struct{ rawHeaders, entity, headers []byte }

func (*piHTTPReceive) String() string           { return "piHTTPReceive{private:[redacted]}" }
func (r *piHTTPReceive) GoString() string       { return r.String() }
func (piUnapprovedHTTPView) String() string     { return "piUnapprovedHTTPView{data:[redacted]}" }
func (v piUnapprovedHTTPView) GoString() string { return v.String() }

// PrepareHTTPReceiveV1 allocates no socket and exposes no response/grant. H/R
// must be the exact original P limits sealed by preparation, not a new budget.
func (p *PreparedPiFileUseV1) PrepareHTTPReceiveV1(h, r uint64) error {
	if p == nil || p.state == nil {
		return ErrInvalid
	}
	s := p.state
	if !s.mu.TryLock() {
		s.abort()
		return ErrConsumed
	}
	defer s.mu.Unlock()
	if s.stopped.Load() || s.httpPrepared || s.claimed.Load() || s.callback != 0 || s.auth == nil || s.file == nil || s.file.ctx.Err() != nil {
		s.abort()
		return ErrConsumed
	}
	s.httpPrepared = true
	f := s.file
	f.service.mu.Lock()
	defer f.service.mu.Unlock()
	if f.service.preparedPiFiles[f.scope] != s || h != s.auth.responseHeaders || r != s.auth.responseBody {
		s.abort()
		return ErrConsumed
	}
	v, err := newPiHTTPReceive(s.ledger, h, r)
	if err != nil {
		s.abort()
		return err
	}
	s.http = v
	s.live += v.live
	if f.ctx.Err() != nil {
		s.abort()
		return ErrConsumed
	}
	return nil
}

func newPiHTTPReceive(ledger attachedworkerdaemon.PreparedAPILedgerV1, h, r uint64) (*piHTTPReceive, error) {
	if nilPiLedger(ledger) || h == 0 || h > 1<<20 || r == 0 || r > 4<<20 {
		return nil, ErrInvalid
	}
	// All terms are bounded above before arithmetic, including on 32-bit Go.
	metadata := uint64(reflect.TypeOf(piHTTPReceive{}).Size())
	framing := 20 * (r + 1)
	live := 2*h + r + metadata
	work := 128*h + 4*r + 4*framing + 1024
	if ledger.Reserve(work, live, 4) != nil {
		return nil, ErrInvalid
	}
	return &piHTTPReceive{head: make([]byte, int(h)), entity: make([]byte, int(r)), replay: make([]byte, int(h)), framingLimit: framing, live: live}, nil
}

func (r *piHTTPReceive) clear() {
	clear(r.head)
	clear(r.entity)
	clear(r.replay)
	clear(r.fields[:])
	clear(r.one[:])
	clear(r.line[:])
	r.headN, r.entityN, r.replayN, r.count = 0, 0, 0, 0
	r.ctx, r.source = nil, nil
}

func (r *piHTTPReceive) read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.eof {
		return 0, io.EOF
	}
	n, err := r.source.Read(p)
	if n < 0 || n > len(p) {
		return 0, ErrInvalid
	}
	if e := r.ctx.Err(); e != nil {
		return n, e
	}
	if err == io.EOF {
		r.eof = true
	}
	if n == 0 && err == nil {
		return 0, ErrInvalid
	}
	return n, err
}

func (r *piHTTPReceive) exact(p []byte) error {
	for len(p) > 0 {
		n, err := r.read(p)
		p = p[n:]
		if err != nil {
			if err == io.EOF && len(p) == 0 {
				return nil
			}
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}
	}
	return nil
}

// Caller supplies the owned fixed TLS connection/context, not a decoder whose
// EOF merely means a declared length or chunk ended. Results remain private
// unapproved DATA, including retained opaque raw metadata for the secret scan.
func (r *piHTTPReceive) receive(ctx context.Context, source io.Reader) (out piUnapprovedHTTPView, err error) {
	if r == nil || !r.spent.CompareAndSwap(false, true) {
		return out, ErrConsumed
	}
	ok := false
	defer func() {
		r.ctx, r.source = nil, nil
		if !ok {
			r.clear()
			out = piUnapprovedHTTPView{}
		}
	}()
	if ctx == nil || source == nil {
		return out, ErrInvalid
	}
	r.ctx, r.source = ctx, source
	for {
		if r.headN == len(r.head) {
			return out, ErrInvalid
		}
		if err = r.exact(r.head[r.headN : r.headN+1]); err != nil {
			return out, err
		}
		r.headN++
		i := r.headN - 1
		if r.head[i] == '\n' && (i == 0 || r.head[i-1] != '\r') || i > 0 && r.head[i-1] == '\r' && r.head[i] != '\n' {
			return out, ErrInvalid
		}
		if r.headN >= 4 && bytes.Equal(r.head[r.headN-4:r.headN], []byte("\r\n\r\n")) {
			break
		}
	}
	length, chunked, e := r.parseHeaders()
	if e != nil {
		return out, e
	}
	if chunked {
		for {
			size, e := r.chunkSize()
			if e != nil {
				return out, e
			}
			if size == 0 {
				if e = r.framingCRLF(); e != nil {
					return out, e
				}
				break
			}
			if size > uint64(len(r.entity)-r.entityN) {
				return out, ErrInvalid
			}
			if e = r.exact(r.entity[r.entityN : r.entityN+int(size)]); e != nil {
				return out, e
			}
			r.entityN += int(size)
			if e = r.framingCRLF(); e != nil {
				return out, e
			}
		}
	} else {
		if e = r.exact(r.entity[:length]); e != nil {
			return out, e
		}
		r.entityN = length
	}
	if r.entityN == 0 {
		return out, ErrInvalid
	}
	// A positive read plus EOF already proves actual underlying EOF. Otherwise
	// probe the ORIGINAL reader; neither expected length nor terminal chunk can
	// manufacture this observation. Extra data is rejected, never drained.
	n, e := r.read(r.one[:])
	if n != 0 || e != io.EOF {
		return out, ErrInvalid
	}
	if e = r.ctx.Err(); e != nil {
		return out, e
	}
	ok = true
	return piUnapprovedHTTPView{r.head[:r.headN:r.headN], r.entity[:r.entityN:r.entityN], r.replay[:r.replayN:r.replayN]}, nil
}

func piASCIIFold(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}
func piASCIIEqual(a []byte, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if piASCIIFold(a[i]) != piASCIIFold(b[i]) {
			return false
		}
	}
	return true
}
func piNamesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if piASCIIFold(a[i]) != piASCIIFold(b[i]) {
			return false
		}
	}
	return true
}
func piHTTPToken(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0
}

func (r *piHTTPReceive) find(name string) int {
	for i := 0; i < r.count; i++ {
		f := r.fields[i]
		if piASCIIEqual(r.head[f.nameStart:f.nameEnd], name) {
			return i
		}
	}
	return -1
}
func (r *piHTTPReceive) value(i int) []byte { f := r.fields[i]; return r.head[f.valueStart:f.valueEnd] }

func piSSEContentType(v []byte) bool {
	const base = "text/event-stream"
	if len(v) < len(base) || !piASCIIEqual(v[:len(base)], base) {
		return false
	}
	v = v[len(base):]
	if len(v) == 0 {
		return true
	}
	for len(v) > 0 && v[0] == ' ' {
		v = v[1:]
	}
	if len(v) == 0 || v[0] != ';' {
		return false
	}
	v = v[1:]
	for len(v) > 0 && v[0] == ' ' {
		v = v[1:]
	}
	return piASCIIEqual(v, "charset=utf-8")
}

func (r *piHTTPReceive) parseHeaders() (int, bool, error) {
	head := r.head[:r.headN]
	end := bytes.Index(head, []byte("\r\n"))
	const status = "HTTP/1.1 200 "
	if end < len(status) || !bytes.Equal(head[:len(status)], []byte(status)) {
		return 0, false, ErrInvalid
	}
	for _, c := range head[len(status):end] {
		if c < 32 || c > 126 {
			return 0, false, ErrInvalid
		}
	}
	for at := end + 2; at < len(head)-2; {
		if r.count == len(r.fields) {
			return 0, false, ErrInvalid
		}
		n := bytes.Index(head[at:], []byte("\r\n"))
		if n <= 0 {
			return 0, false, ErrInvalid
		}
		end = at + n
		colon := bytes.IndexByte(head[at:end], ':')
		if colon <= 0 {
			return 0, false, ErrInvalid
		}
		colon += at
		for _, c := range head[at:colon] {
			if !piHTTPToken(c) {
				return 0, false, ErrInvalid
			}
		}
		start, finish := colon+1, end
		for start < finish && (head[start] == ' ' || head[start] == '\t') {
			start++
		}
		for finish > start && (head[finish-1] == ' ' || head[finish-1] == '\t') {
			finish--
		}
		if start == finish {
			return 0, false, ErrInvalid
		}
		for _, c := range head[start:finish] {
			if c < 32 || c > 126 {
				return 0, false, ErrInvalid
			}
		}
		for j := 0; j < r.count; j++ {
			f := r.fields[j]
			if piNamesEqual(head[at:colon], head[f.nameStart:f.nameEnd]) {
				return 0, false, ErrInvalid
			}
		}
		r.fields[r.count] = piHTTPField{uint32(at), uint32(colon), uint32(start), uint32(finish)}
		r.count++
		at = end + 2
	}
	for _, name := range [...]string{"trailer", "set-cookie", "location", "refresh", "www-authenticate", "proxy-authenticate", "proxy-authorization", "upgrade", "content-disposition"} {
		if r.find(name) >= 0 {
			return 0, false, ErrInvalid
		}
	}
	typeIndex := r.find("content-type")
	if typeIndex < 0 || !piSSEContentType(r.value(typeIndex)) {
		return 0, false, ErrInvalid
	}
	if i := r.find("content-encoding"); i >= 0 && !piASCIIEqual(r.value(i), "identity") {
		return 0, false, ErrInvalid
	}
	if i := r.find("connection"); i >= 0 && !piASCIIEqual(r.value(i), "close") {
		return 0, false, ErrInvalid
	}
	cl, te := r.find("content-length"), r.find("transfer-encoding")
	if (cl >= 0) == (te >= 0) {
		return 0, false, ErrInvalid
	}
	length := 0
	if cl >= 0 {
		v := r.value(cl)
		if v[0] == '0' {
			return 0, false, ErrInvalid
		}
		for _, c := range v {
			if c < '0' || c > '9' {
				return 0, false, ErrInvalid
			}
			digit := int(c - '0')
			if length > len(r.entity)/10 || length == len(r.entity)/10 && digit > len(r.entity)%10 {
				return 0, false, ErrInvalid
			}
			length = 10*length + digit
		}
	} else if !piASCIIEqual(r.value(te), "chunked") {
		return 0, false, ErrInvalid
	}
	n := 2
	count := 0
	for _, name := range [...]string{"content-encoding", "content-length", "content-type"} {
		if i := r.find(name); i >= 0 {
			n += 6 + len(name) + len(r.value(i))
			count++
		}
	}
	if n > len(r.replay) {
		return 0, false, ErrInvalid
	}
	binary.BigEndian.PutUint16(r.replay[:2], uint16(count))
	at := 2
	for _, name := range [...]string{"content-encoding", "content-length", "content-type"} {
		if i := r.find(name); i >= 0 {
			v := r.value(i)
			binary.BigEndian.PutUint16(r.replay[at:at+2], uint16(len(name)))
			at += 2
			at += copy(r.replay[at:], name)
			binary.BigEndian.PutUint32(r.replay[at:at+4], uint32(len(v)))
			at += 4
			at += copy(r.replay[at:], v)
		}
	}
	r.replayN = at
	return length, te >= 0, nil
}

func (r *piHTTPReceive) framingByte() (byte, error) {
	if r.framing == r.framingLimit {
		return 0, ErrInvalid
	}
	r.framing++
	if err := r.exact(r.one[:]); err != nil {
		return 0, err
	}
	return r.one[0], nil
}
func (r *piHTTPReceive) framingCRLF() error {
	for _, want := range [...]byte{'\r', '\n'} {
		c, err := r.framingByte()
		if err != nil {
			return err
		}
		if c != want {
			return ErrInvalid
		}
	}
	return nil
}
func (r *piHTTPReceive) chunkSize() (uint64, error) {
	n := 0
	for {
		c, err := r.framingByte()
		if err != nil {
			return 0, err
		}
		if c == '\r' {
			if n == 0 {
				return 0, ErrInvalid
			}
			c, err = r.framingByte()
			if err != nil {
				return 0, err
			}
			if c != '\n' {
				return 0, ErrInvalid
			}
			break
		}
		if n == 16 {
			return 0, ErrInvalid
		}
		r.line[n] = c
		n++
	}
	var size uint64
	for _, c := range r.line[:n] {
		var d byte
		switch {
		case c >= '0' && c <= '9':
			d = c - '0'
		case c >= 'a' && c <= 'f':
			d = c - 'a' + 10
		case c >= 'A' && c <= 'F':
			d = c - 'A' + 10
		default:
			return 0, ErrInvalid
		}
		size = (size << 4) | uint64(d)
	}
	return size, nil
}
