package attachedworkerdaemon

import (
	"encoding/binary"
	"reflect"
	"sync"
	"sync/atomic"

	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
)

// piHTTPCoordinator is a private DATA state machine. Its completion events are
// NOT witnesses of actual IO, Source permission, HTTP quarantine or process
// success. Only a future binding inside the original Supervisor may supply
// those observations. No producer stamp, driver registration or public lease
// is constructed here. It never performs IO, waits, calls a driver or launches
// goroutines while holding mu (or otherwise).
type piHTTPCoordinator struct {
	mu       sync.Mutex
	failed   atomic.Bool
	writer   atomic.Bool // nonqueueing entry, held across the entire outbound borrow
	cleared  bool
	clearErr error // terminal retirement outcome; a refund failure stays visible
	limits   pisdkhttpwire.Limits
	seq      *pisdkhttpwire.Sequence
	ledger   PreparedAPILedgerV1
	live     uint64

	request, outbound, final []byte // three nonoverlapping, retained arenas
	requestFrame             pisdkhttpwire.Frame
	finalSize                int
	requestReady             bool
	readerStarted, reader    bool
	driverStarted, driver    bool
	eof                      bool
	finalReceived            bool

	epoch, committed uint64
	write            piHTTPWrite
	expect           pisdkhttpwire.Kind
	expectIndex      uint32
	count            uint32
	credit           bool
	input            piHTTPInput
}

type piHTTPWrite struct {
	epoch uint64
	size  int
	frame pisdkhttpwire.Frame // views only into outbound
}

// Tickets include their private owner; an equal epoch from another core cannot
// complete this core. A copied ticket still consumes the same single event.
type piHTTPTicket struct {
	owner *piHTTPCoordinator
	epoch uint64
}

type piHTTPInput struct {
	ticket       piHTTPTicket
	kind         pisdkhttpwire.Kind
	index        uint32
	prerequisite uint64
	stage        byte // 1 header, 2 metadata, 3 body; Pull may wait in stage 3
	header       pisdkhttpwire.Header
	raw          [21]byte // header + sole Pull metadata; never a queue
}

func (*piHTTPCoordinator) String() string     { return "piHTTPCoordinator{data:[redacted]}" }
func (c *piHTTPCoordinator) GoString() string { return c.String() }
func (piHTTPTicket) String() string           { return "piHTTPTicket{data:[redacted]}" }
func (v piHTTPTicket) GoString() string       { return v.String() }

type piHTTPCoordinatorBudget struct{ Work, Live, Nodes uint64 }

func measurePiHTTPCoordinator(l pisdkhttpwire.Limits) (piHTTPCoordinatorBudget, error) {
	a, err := pisdkhttpwire.Measure(l) // bound EVERY input before arithmetic
	if err != nil {
		return piHTTPCoordinatorBudget{}, pisdkhttpwire.ErrInvalid
	}
	s := pisdkhttpwire.MeasureSequencePreparation()
	m := uint64(reflect.TypeOf(piHTTPCoordinator{}).Size()) + s.LiveBytes
	// At most one successful pass through each frame plus one offending prefix.
	// 64 wire-byte visits cover encode alias scans, UTF8/header validation,
	// snapshot copies, decode, preflight, commit and SHA input. Fixed records,
	// zeroing and the final failed header are included separately. This is an
	// application-work upper bound, NOT an extra full-profile pool/heap claim.
	return piHTTPCoordinatorBudget{
		Work:  64*a.TotalBytes + 8*l.FrameBytes + 4*l.FinalBytes + 256*a.Frames + m,
		Live:  2*l.FrameBytes + l.FinalBytes + m,
		Nodes: 4 + s.Nodes, // core, three arenas, sequence's three objects
	}, nil
}

func preparePiHTTPCoordinator(l pisdkhttpwire.Limits, ledger PreparedAPILedgerV1) (*piHTTPCoordinator, error) {
	b, err := measurePiHTTPCoordinator(l)
	if err != nil || nilPreparedAPIDependency(ledger) || !pisdkhttpwire.SequencePreparationSupported() {
		return nil, pisdkhttpwire.ErrInvalid
	}
	if ledger.Reserve(b.Work, b.Live, b.Nodes) != nil {
		return nil, pisdkhttpwire.ErrInvalid
	}
	s, err := pisdkhttpwire.NewSequence(l)
	if err != nil || !s.PreparedLayoutFits() {
		_ = ledger.EndLive(b.Live) // work/nodes remain consumed
		return nil, pisdkhttpwire.ErrInvalid
	}
	return &piHTTPCoordinator{limits: l, seq: s, ledger: ledger, live: b.Live,
		request: make([]byte, int(l.FrameBytes)), outbound: make([]byte, int(l.FrameBytes)),
		final: make([]byte, int(l.FinalBytes))}, nil
}

func (c *piHTTPCoordinator) abort() {
	if c != nil {
		c.failed.Store(true) // nonblocking, including while a writer/reader owns data
	}
}

func (c *piHTTPCoordinator) denyLocked() error {
	c.failed.Store(true)
	c.seq.Abort()
	return pisdkhttpwire.ErrInvalid
}

// beginWrite snapshots before returning the bounded borrowed wire bytes. Caller
// must not mutate its input concurrently with this method or the returned view
// at all. It must completeWrite on every exit, even after abort. No retry.
func (c *piHTTPCoordinator) beginWrite(f pisdkhttpwire.Frame) (piHTTPTicket, []byte, error) {
	if !c.writer.CompareAndSwap(false, true) {
		c.abort()
		return piHTTPTicket{}, nil, pisdkhttpwire.ErrInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	published := false
	defer func() {
		if !published {
			c.writer.Store(false)
		}
	}()
	if c.failed.Load() || c.cleared || c.write.epoch != 0 || c.eof || c.epoch >= c.limits.Frames ||
		(f.Kind != pisdkhttpwire.Initialize && f.Kind != pisdkhttpwire.ResponseStart &&
			f.Kind != pisdkhttpwire.ResponsePart && f.Kind != pisdkhttpwire.ResponseEnd) {
		return piHTTPTicket{}, nil, c.denyLocked()
	}
	if f.Kind == pisdkhttpwire.ResponsePart && !c.credit {
		return piHTTPTicket{}, nil, c.denyLocked()
	}
	n, err := pisdkhttpwire.EncodeInto(c.outbound, f, c.limits)
	if err != nil {
		return piHTTPTicket{}, nil, c.denyLocked()
	}
	owned, err := pisdkhttpwire.DecodeView(c.outbound[:n], pisdkhttpwire.ParentToChild, c.limits)
	if err != nil || c.seq.PreflightWrite(owned) != nil || c.failed.Load() {
		return piHTTPTicket{}, nil, c.denyLocked()
	}
	c.epoch++ // <=4096; no wrap, independent of caller-supplied frame index
	c.write = piHTTPWrite{epoch: c.epoch, size: n, frame: owned}
	published = true
	switch owned.Kind {
	case pisdkhttpwire.ResponseStart:
		c.count = owned.Count
		c.expect, c.expectIndex = pisdkhttpwire.Pull, 0
	case pisdkhttpwire.ResponsePart:
		c.credit = false
		c.expect = 0
		if owned.Index+1 < c.count {
			c.expect, c.expectIndex = pisdkhttpwire.Pull, owned.Index+1
		}
	case pisdkhttpwire.ResponseEnd:
		c.expect = pisdkhttpwire.StructuredFinal
	}
	return piHTTPTicket{owner: c, epoch: c.epoch}, c.outbound[:n:n], nil
}

// completeWrite takes an unqualified DATA completion event. Only exact full
// success advances Sequence; every other result burns the run. Matching failed
// completions still release the borrowed outbound arena, permitting retirement.
func (c *piHTTPCoordinator) completeWrite(t piHTTPTicket, n int, writeErr error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.owner != c || t.epoch == 0 || c.write.epoch != t.epoch || c.cleared {
		return c.denyLocked()
	}
	w := c.write
	c.write = piHTTPWrite{}
	defer c.writer.Store(false)
	if c.failed.Load() || writeErr != nil || n != w.size || c.seq.Accept(pisdkhttpwire.ParentToChild, w.frame) != nil || c.failed.Load() {
		return c.denyLocked()
	}
	c.committed = w.epoch
	if w.frame.Kind == pisdkhttpwire.Initialize {
		c.expect = pisdkhttpwire.FetchRequest
	}
	return c.commitPendingLocked()
}

func (c *piHTTPCoordinator) startReader() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed.Load() || c.cleared || c.readerStarted || c.write.epoch != 0 || c.expect != pisdkhttpwire.FetchRequest {
		return c.denyLocked()
	}
	c.readerStarted, c.reader = true, true
	return nil
}

// observePrefix is called IMMEDIATELY after the sole reader sees its first
// positive byte, before reading the other 16 bytes. This lock acquisition fixes
// kind/index/prerequisite for the entire fragmented header. During quarantine,
// pending/unused credit or after Final, even one byte irreversibly denies.
func (c *piHTTPCoordinator) observePrefix(first byte) (piHTTPTicket, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed.Load() || c.cleared || !c.reader || c.eof || c.finalReceived || c.expect == 0 ||
		c.input.stage != 0 || c.credit || first != 'S' {
		return piHTTPTicket{}, c.denyLocked()
	}
	t := piHTTPTicket{owner: c, epoch: c.epoch}
	c.input = piHTTPInput{ticket: t, kind: c.expect, index: c.expectIndex, prerequisite: c.epoch, stage: 1}
	c.input.raw[0] = first
	return t, nil
}

func (c *piHTTPCoordinator) inputMatches(t piHTTPTicket, stage byte) bool {
	return !c.failed.Load() && !c.cleared && c.reader && c.input.stage == stage &&
		t.owner == c && t.epoch != 0 && t == c.input.ticket
}

func (c *piHTTPCoordinator) acceptHeader(t piHTTPTicket, remaining []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.inputMatches(t, 1) || len(remaining) != 16 {
		return c.denyLocked()
	}
	copy(c.input.raw[1:17], remaining)
	h, err := pisdkhttpwire.InspectHeader(c.input.raw[:17], pisdkhttpwire.ChildToParent, c.limits)
	if err != nil || h.Kind != c.input.kind {
		return c.denyLocked()
	}
	c.input.header, c.input.stage = h, 2
	if h.Kind == pisdkhttpwire.FetchRequest {
		copy(c.request[:17], c.input.raw[:17])
		if _, err := c.seq.BeginHeader(c.request[:17], pisdkhttpwire.ChildToParent); err != nil {
			return c.denyLocked()
		}
	}
	return nil
}

// acceptMetadata completes only the declared metadata; callers must not read
// the body until bodyReady reports true. In particular Final remains header-only
// while End is in flight. Both metadata and write completion use the same
// idempotent pending transition, covering either arrival order.
func (c *piHTTPCoordinator) acceptMetadata(t piHTTPTicket, metadata []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.inputMatches(t, 2) || uint64(len(metadata)) != uint64(c.input.header.MetadataBytes) {
		return c.denyLocked()
	}
	switch c.input.kind {
	case pisdkhttpwire.FetchRequest:
		end := 17 + len(metadata)
		copy(c.request[17:end], metadata)
		if c.seq.PreflightMetadata(c.request[17:end]) != nil {
			return c.denyLocked()
		}
	case pisdkhttpwire.Pull:
		copy(c.input.raw[17:], metadata)
		if binary.BigEndian.Uint32(c.input.raw[17:]) != c.input.index {
			return c.denyLocked()
		}
	case pisdkhttpwire.StructuredFinal:
	default:
		return c.denyLocked()
	}
	c.input.stage = 3
	return c.commitPendingLocked()
}

func (c *piHTTPCoordinator) commitPendingLocked() error {
	if c.input.stage != 3 || c.input.kind != pisdkhttpwire.Pull || c.committed < c.input.prerequisite {
		return nil
	}
	if c.failed.Load() || c.write.epoch != 0 || c.credit {
		return c.denyLocked()
	}
	if _, err := c.seq.BeginHeader(c.input.raw[:17], pisdkhttpwire.ChildToParent); err != nil ||
		c.seq.PreflightMetadata(c.input.raw[17:]) != nil || c.seq.AcceptBody(nil) != nil {
		return c.denyLocked()
	}
	c.credit = true
	c.input = piHTTPInput{}
	return nil
}

func (c *piHTTPCoordinator) bodyReady(t piHTTPTicket) (uint64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.inputMatches(t, 3) || c.input.kind == pisdkhttpwire.Pull {
		return 0, false, c.denyLocked()
	}
	return c.input.header.BodyBytes, c.committed >= c.input.prerequisite && c.write.epoch == 0, nil
}

func (c *piHTTPCoordinator) acceptBody(t piHTTPTicket, body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.inputMatches(t, 3) || c.committed < c.input.prerequisite || c.write.epoch != 0 ||
		uint64(len(body)) != c.input.header.BodyBytes {
		return c.denyLocked()
	}
	switch c.input.kind {
	case pisdkhttpwire.FetchRequest:
		offset := 17 + int(c.input.header.MetadataBytes)
		copy(c.request[offset:], body)
		if c.seq.AcceptBody(c.request[offset:offset+len(body)]) != nil {
			return c.denyLocked()
		}
		f, err := pisdkhttpwire.DecodeView(c.request[:offset+len(body)], pisdkhttpwire.ChildToParent, c.limits)
		if err != nil {
			return c.denyLocked()
		}
		c.requestFrame, c.requestReady = f, true
	case pisdkhttpwire.StructuredFinal:
		if _, err := c.seq.BeginHeader(c.input.raw[:17], pisdkhttpwire.ChildToParent); err != nil || c.seq.PreflightMetadata(nil) != nil {
			return c.denyLocked()
		}
		copy(c.final, body)
		if c.seq.AcceptBody(c.final[:len(body)]) != nil {
			return c.denyLocked()
		}
		c.finalSize, c.finalReceived = len(body), true
	default:
		return c.denyLocked()
	}
	c.expect = 0
	c.input = piHTTPInput{}
	return nil
}

// borrowRequest pins the request until endDriver, including every subordinate
// HTTP borrower. There is no release on driver return in future pipe wiring
// until its actual sub-operations have joined. No buffers are recycled by phase.
func (c *piHTTPCoordinator) borrowRequest() (pisdkhttpwire.Frame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed.Load() || c.cleared || !c.requestReady || c.driverStarted {
		return pisdkhttpwire.Frame{}, c.denyLocked()
	}
	c.driverStarted, c.driver = true, true
	return c.requestFrame, nil
}

func (c *piHTTPCoordinator) endDriver(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.driver || c.cleared {
		return c.denyLocked()
	}
	c.driver = false
	if err != nil || c.failed.Load() {
		return c.denyLocked()
	}
	return nil // nil driver return neither supplies Final/EOF nor stops reader
}

func (c *piHTTPCoordinator) observeEOF() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed.Load() || c.cleared || !c.reader || c.eof || !c.finalReceived || c.input.stage != 0 ||
		c.write.epoch != 0 || c.seq.Finish() != nil {
		return c.denyLocked()
	}
	c.eof = true // wire DATA only, NOT process reap / real provider EOF
	return nil
}

func (c *piHTTPCoordinator) endReader() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.reader || c.cleared {
		return c.denyLocked()
	}
	c.reader = false
	if !c.eof || c.failed.Load() {
		return c.denyLocked()
	}
	return nil
}

func (c *piHTTPCoordinator) clear() error {
	c.abort() // cancellation before owned joins, never wait on a reader/driver
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cleared {
		return c.clearErr
	}
	if c.reader || c.driver || c.writer.Load() || c.write.epoch != 0 {
		return pisdkhttpwire.ErrInvalid // no zero/refund beneath any borrowed arena
	}
	clear(c.request)
	clear(c.outbound)
	clear(c.final)
	c.request, c.outbound, c.final = nil, nil, nil
	c.requestFrame, c.input = pisdkhttpwire.Frame{}, piHTTPInput{}
	c.seq.Abort()
	c.seq = nil // pending Fetch metadata can otherwise retain the request arena
	c.cleared = true
	c.clearErr = c.ledger.EndLive(c.live) // never refund cumulative work or nodes
	if c.clearErr == nil {
		c.ledger, c.live = nil, 0
	}
	return c.clearErr
}
