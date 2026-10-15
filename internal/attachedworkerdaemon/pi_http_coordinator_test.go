package attachedworkerdaemon

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
)

// Every completion below is synthetic DATA. These tests do not start Pi, an
// actual pipe, Source, a credential reader, a network peer or any provider.
func piHTTPTestLimits() pisdkhttpwire.Limits {
	return pisdkhttpwire.Limits{FrameBytes: 4096, InitializeBytes: 128, RequestBytes: 256, FinalBytes: 128,
		RequestHeaderBytes: 512, ResponseHeaderBytes: 128, ResponseBytes: 64, ResponseChunkBytes: 7,
		ParentBytes: 4 << 20, ChildBytes: 16 << 20, Frames: 4096}
}

func piHTTPTestHeaders(names []string) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(names)))
	for _, name := range names {
		b = binary.BigEndian.AppendUint16(b, uint16(len(name)))
		b = append(b, name...)
		b = binary.BigEndian.AppendUint32(b, 1)
		b = append(b, 'x')
	}
	return b
}

func piHTTPTestFrames(l pisdkhttpwire.Limits, response []byte) []pisdkhttpwire.Frame {
	a, err := pisdkhttpwire.Measure(l)
	if err != nil || len(response) == 0 {
		panic("invalid coordinator fixture")
	}
	n := uint32(1 + (uint64(len(response))-1)/a.ChunkBytes)
	d := sha256.Sum256(response)
	frames := []pisdkhttpwire.Frame{
		{Kind: pisdkhttpwire.Initialize, Profile: [32]byte{3}, Body: []byte("{}")},
		{Kind: pisdkhttpwire.FetchRequest, Profile: [32]byte{3}, Body: []byte("{\"input\":\"original\"}"),
			Headers: piHTTPTestHeaders([]string{"accept", "content-type", "user-agent", "x-stainless-arch", "x-stainless-lang", "x-stainless-os",
				"x-stainless-package-version", "x-stainless-retry-count", "x-stainless-runtime", "x-stainless-runtime-version", "x-stainless-timeout"})},
		{Kind: pisdkhttpwire.ResponseStart, Count: n, Bytes: uint64(len(response)), Digest: d, Headers: piHTTPTestHeaders([]string{"content-type"})},
	}
	for i, offset := uint32(0), 0; offset < len(response); i++ {
		end := min(len(response), offset+int(a.ChunkBytes))
		frames = append(frames, pisdkhttpwire.Frame{Kind: pisdkhttpwire.Pull, Index: i},
			pisdkhttpwire.Frame{Kind: pisdkhttpwire.ResponsePart, Index: i, Body: response[offset:end]})
		offset = end
	}
	return append(frames, pisdkhttpwire.Frame{Kind: pisdkhttpwire.ResponseEnd, Count: n, Bytes: uint64(len(response)), Digest: d},
		pisdkhttpwire.Frame{Kind: pisdkhttpwire.StructuredFinal, Body: []byte("{\"answer\":\"ok\"}")})
}

func piHTTPTestCore(t *testing.T, l pisdkhttpwire.Limits) *piHTTPCoordinator {
	t.Helper()
	if !pisdkhttpwire.SequencePreparationSupported() {
		t.Fatal("coordinator fixture requires audited Go1.26.8 standard SHA backend")
	}
	b, err := measurePiHTTPCoordinator(l)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &boundedAPIBorrowLedger{remainingCopy: b.Work, remainingLive: b.Live, remainingNodes: b.Nodes}
	c, err := preparePiHTTPCoordinator(l, ledger)
	if err != nil {
		t.Fatal("prepare", err)
	}
	t.Cleanup(func() {
		c.abort()
		// Test-owned events always join before cleanup, including negative cases.
		if c.write.epoch != 0 {
			_ = c.completeWrite(piHTTPTicket{owner: c, epoch: c.write.epoch}, 0, errors.New("test cleanup"))
		}
		if c.driver {
			_ = c.endDriver(errors.New("test cleanup"))
		}
		if c.reader {
			_ = c.endReader()
		}
		if err := c.clear(); err != nil || ledger.live != 0 {
			t.Errorf("retirement err=%v live=%d", err, ledger.live)
		}
	})
	return c
}

func piHTTPTestEncode(t *testing.T, f pisdkhttpwire.Frame, l pisdkhttpwire.Limits) []byte {
	t.Helper()
	b := make([]byte, int(l.FrameBytes))
	n, err := pisdkhttpwire.EncodeInto(b, f, l)
	if err != nil {
		t.Fatalf("encode kind=%d: %v", f.Kind, err)
	}
	return b[:n]
}

func piHTTPTestWrite(t *testing.T, c *piHTTPCoordinator, f pisdkhttpwire.Frame) {
	t.Helper()
	ticket, raw, err := c.beginWrite(f)
	if err != nil {
		t.Fatalf("begin kind=%d: %v", f.Kind, err)
	}
	if err := c.completeWrite(ticket, len(raw), nil); err != nil {
		t.Fatalf("complete kind=%d: %v", f.Kind, err)
	}
}

func piHTTPTestHeader(t *testing.T, c *piHTTPCoordinator, raw []byte) piHTTPTicket {
	t.Helper()
	ticket, err := c.observePrefix(raw[0])
	if err != nil {
		t.Fatal("prefix", err)
	}
	if err := c.acceptHeader(ticket, raw[1:17]); err != nil {
		t.Fatal("header", err)
	}
	return ticket
}

func piHTTPTestRead(t *testing.T, c *piHTTPCoordinator, f pisdkhttpwire.Frame) {
	t.Helper()
	raw := piHTTPTestEncode(t, f, c.limits)
	ticket := piHTTPTestHeader(t, c, raw)
	m := 17 + int(binary.BigEndian.Uint32(raw[5:9]))
	if err := c.acceptMetadata(ticket, raw[17:m]); err != nil {
		t.Fatal("metadata", err)
	}
	if f.Kind != pisdkhttpwire.Pull {
		if _, ready, err := c.bodyReady(ticket); err != nil || !ready {
			t.Fatalf("body readiness=%t err=%v", ready, err)
		}
		if err := c.acceptBody(ticket, raw[m:]); err != nil {
			t.Fatal("body", err)
		}
	}
}

func piHTTPTestFetch(t *testing.T, c *piHTTPCoordinator, frames []pisdkhttpwire.Frame) {
	t.Helper()
	piHTTPTestWrite(t, c, frames[0])
	if err := c.startReader(); err != nil {
		t.Fatal(err)
	}
	piHTTPTestRead(t, c, frames[1])
}

func TestPiHTTPCoordinatorPendingControlBothOrders(t *testing.T) {
	for _, order := range []string{"metadata_before_write", "write_before_header", "write_between_header_and_metadata", "write_between_prefix_and_header"} {
		t.Run(order, func(t *testing.T) {
			l := piHTTPTestLimits()
			c := piHTTPTestCore(t, l)
			frames := piHTTPTestFrames(l, []byte("0123456789"))
			piHTTPTestFetch(t, c, frames)
			wt, out, err := c.beginWrite(frames[2])
			if err != nil {
				t.Fatal(err)
			}
			raw := piHTTPTestEncode(t, frames[3], l)
			rt, err := c.observePrefix(raw[0])
			if err != nil {
				t.Fatal(err)
			}
			if order == "write_before_header" || order == "write_between_prefix_and_header" {
				if err := c.completeWrite(wt, len(out), nil); err != nil || c.credit {
					t.Fatalf("incomplete header promoted: err=%v credit=%t", err, c.credit)
				}
			}
			if err := c.acceptHeader(rt, raw[1:17]); err != nil {
				t.Fatal(err)
			}
			if order == "write_between_header_and_metadata" {
				if err := c.completeWrite(wt, len(out), nil); err != nil || c.credit {
					t.Fatalf("incomplete metadata promoted: err=%v credit=%t", err, c.credit)
				}
			}
			if err := c.acceptMetadata(rt, raw[17:]); err != nil {
				t.Fatal(err)
			}
			if order == "metadata_before_write" {
				if c.credit {
					t.Fatal("credit before successful write")
				}
				if err := c.completeWrite(wt, len(out), nil); err != nil {
					t.Fatal(err)
				}
			}
			if !c.credit || c.input.stage != 0 {
				t.Fatalf("one committed credit missing credit=%t stage=%d", c.credit, c.input.stage)
			}
			piHTTPTestWrite(t, c, frames[4])
			if c.credit {
				t.Fatal("Part did not consume credit")
			}
			for _, f := range frames[5:] {
				if f.Kind == pisdkhttpwire.Pull || f.Kind == pisdkhttpwire.StructuredFinal {
					piHTTPTestRead(t, c, f)
				} else {
					piHTTPTestWrite(t, c, f)
				}
			}
			if err := c.observeEOF(); err != nil {
				t.Fatal(err)
			}
			if err := c.endReader(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPiHTTPCoordinatorRejectsSpeculativePrefix(t *testing.T) {
	for _, state := range []string{"quarantine", "pending_header", "pending_metadata", "pending_complete", "unused_credit", "last_part"} {
		t.Run(state, func(t *testing.T) {
			l := piHTTPTestLimits()
			c := piHTTPTestCore(t, l)
			frames := piHTTPTestFrames(l, []byte("abc"))
			piHTTPTestFetch(t, c, frames)
			if state != "quarantine" {
				wt, out, err := c.beginWrite(frames[2])
				if err != nil {
					t.Fatal(err)
				}
				raw := piHTTPTestEncode(t, frames[3], l)
				rt, err := c.observePrefix('S')
				if err != nil {
					t.Fatal(err)
				}
				if state != "pending_header" {
					if err := c.acceptHeader(rt, raw[1:17]); err != nil {
						t.Fatal(err)
					}
				}
				if state != "pending_header" && state != "pending_metadata" {
					if err := c.acceptMetadata(rt, raw[17:]); err != nil {
						t.Fatal(err)
					}
				}
				if state == "unused_credit" || state == "last_part" {
					if err := c.completeWrite(wt, len(out), nil); err != nil {
						t.Fatal(err)
					}
				}
				if state == "last_part" {
					piHTTPTestWrite(t, c, frames[4])
				}
			}
			if _, err := c.observePrefix('S'); err == nil || !c.failed.Load() {
				t.Fatal("speculative first byte admitted")
			}
		})
	}
}

func TestPiHTTPCoordinatorFailedWriteCannotRevive(t *testing.T) {
	for _, defect := range []string{"short", "error", "abort_before_metadata", "abort_after_metadata", "wrong_ticket", "duplicate_completion"} {
		t.Run(defect, func(t *testing.T) {
			l := piHTTPTestLimits()
			c := piHTTPTestCore(t, l)
			frames := piHTTPTestFrames(l, []byte("abc"))
			piHTTPTestFetch(t, c, frames)
			wt, out, err := c.beginWrite(frames[2])
			if err != nil {
				t.Fatal(err)
			}
			raw := piHTTPTestEncode(t, frames[3], l)
			rt := piHTTPTestHeader(t, c, raw)
			if defect == "abort_before_metadata" {
				c.abort()
			}
			metaErr := c.acceptMetadata(rt, raw[17:])
			if defect == "abort_before_metadata" && metaErr == nil {
				t.Fatal("late metadata revived aborted run")
			}
			n, writeErr := len(out), error(nil)
			switch defect {
			case "short":
				n--
			case "error":
				writeErr = errors.New("synthetic write failure")
			case "abort_after_metadata":
				c.abort()
			case "wrong_ticket":
				foreign := &piHTTPCoordinator{} // identity substitution, never a runnable core
				bad := wt
				bad.owner = foreign
				if err := c.completeWrite(bad, n, nil); err == nil {
					t.Fatal("foreign ticket accepted")
				}
			case "duplicate_completion":
				if err := c.completeWrite(wt, n, nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.completeWrite(wt, n, writeErr); err == nil || !c.failed.Load() {
				t.Fatal("failed/replayed write promoted")
			}
			if _, _, err := c.beginWrite(frames[4]); err == nil {
				t.Fatal("failed run resumed at Part")
			}
		})
	}
}

func TestPiHTTPCoordinatorEndFinalAndBorrowLifetime(t *testing.T) {
	l := piHTTPTestLimits()
	c := piHTTPTestCore(t, l)
	frames := piHTTPTestFrames(l, []byte("abc"))
	piHTTPTestFetch(t, c, frames)
	request, err := c.borrowRequest()
	if err != nil {
		t.Fatal(err)
	}
	wantRequest := bytes.Clone(request.Body)
	piHTTPTestWrite(t, c, frames[2])
	piHTTPTestRead(t, c, frames[3])
	piHTTPTestWrite(t, c, frames[4])
	wt, out, err := c.beginWrite(frames[5])
	if err != nil {
		t.Fatal(err)
	}
	raw := piHTTPTestEncode(t, frames[6], l)
	rt := piHTTPTestHeader(t, c, raw)
	if err := c.acceptMetadata(rt, nil); err != nil {
		t.Fatal(err)
	}
	if n, ready, err := c.bodyReady(rt); err != nil || ready || n != uint64(len(frames[6].Body)) || c.finalSize != 0 {
		t.Fatalf("End pending body gate n=%d ready=%t err=%v", n, ready, err)
	}
	if err := c.endDriver(nil); err != nil || c.finalReceived || c.eof || !c.reader {
		t.Fatalf("driver return supplied completion/stopped reader: %v", err)
	}
	if err := c.completeWrite(wt, len(out), nil); err != nil {
		t.Fatal(err)
	}
	if _, ready, err := c.bodyReady(rt); err != nil || !ready {
		t.Fatalf("committed End did not admit body: ready=%t err=%v", ready, err)
	}
	if err := c.acceptBody(rt, raw[17:]); err != nil || !bytes.Equal(c.final[:c.finalSize], frames[6].Body) {
		t.Fatalf("stdout Final not retained: %v", err)
	}
	clear(raw)
	if !bytes.Equal(request.Body, wantRequest) || !bytes.Equal(c.final[:c.finalSize], frames[6].Body) {
		t.Fatal("request/Final storage reused or aliases caller input")
	}
	if err := c.observeEOF(); err != nil || !c.eof {
		t.Fatal("wire EOF", err)
	}
	if err := c.endReader(); err != nil {
		t.Fatal(err)
	}
}

func TestPiHTTPCoordinatorRejectsEarlyBodyTruncationAndExtraBytes(t *testing.T) {
	for _, defect := range []string{"early_final_body", "early_eof", "partial_header_eof", "extra_final_prefix", "duplicate_eof", "bad_hq", "wrong_old_index"} {
		t.Run(defect, func(t *testing.T) {
			l := piHTTPTestLimits()
			c := piHTTPTestCore(t, l)
			frames := piHTTPTestFrames(l, []byte("abc"))
			if defect == "bad_hq" {
				piHTTPTestWrite(t, c, frames[0])
				if err := c.startReader(); err != nil {
					t.Fatal(err)
				}
				raw := piHTTPTestEncode(t, frames[1], l)
				rt := piHTTPTestHeader(t, c, raw)
				m := 17 + int(binary.BigEndian.Uint32(raw[5:9]))
				binary.BigEndian.PutUint32(raw[17+64:], math.MaxUint32)
				if err := c.acceptMetadata(rt, raw[17:m]); err == nil || c.requestReady {
					t.Fatal("Hq accepted before body")
				}
				return
			}
			piHTTPTestFetch(t, c, frames)
			if defect == "early_eof" {
				if err := c.observeEOF(); err == nil {
					t.Fatal("EOF before Final accepted")
				}
				return
			}
			piHTTPTestWrite(t, c, frames[2])
			if defect == "partial_header_eof" {
				if _, err := c.observePrefix('S'); err != nil {
					t.Fatal(err)
				}
				if err := c.observeEOF(); err == nil {
					t.Fatal("truncated header accepted")
				}
				return
			}
			if defect == "wrong_old_index" {
				raw := piHTTPTestEncode(t, frames[3], l)
				rt := piHTTPTestHeader(t, c, raw)
				binary.BigEndian.PutUint32(raw[17:], 1)
				if err := c.acceptMetadata(rt, raw[17:]); err == nil {
					t.Fatal("old-epoch wrong index admitted")
				}
				return
			}
			piHTTPTestRead(t, c, frames[3])
			piHTTPTestWrite(t, c, frames[4])
			wt, out, err := c.beginWrite(frames[5])
			if err != nil {
				t.Fatal(err)
			}
			raw := piHTTPTestEncode(t, frames[6], l)
			rt := piHTTPTestHeader(t, c, raw)
			if err := c.acceptMetadata(rt, nil); err != nil {
				t.Fatal(err)
			}
			if defect == "early_final_body" {
				if err := c.acceptBody(rt, raw[17:]); err == nil || c.finalSize != 0 {
					t.Fatal("Final body copied before End commit")
				}
				return
			}
			if err := c.completeWrite(wt, len(out), nil); err != nil {
				t.Fatal(err)
			}
			if err := c.acceptBody(rt, raw[17:]); err != nil {
				t.Fatal(err)
			}
			if defect == "extra_final_prefix" {
				if _, err := c.observePrefix('S'); err == nil {
					t.Fatal("extra byte after Final accepted")
				}
			} else {
				if err := c.observeEOF(); err != nil {
					t.Fatal(err)
				}
				if err := c.observeEOF(); err == nil {
					t.Fatal("EOF replay accepted")
				}
			}
		})
	}
}

func TestPiHTTPCoordinatorSnapshotAndRetirement(t *testing.T) {
	for _, held := range []string{"writer", "reader", "driver"} {
		t.Run(held, func(t *testing.T) {
			l := piHTTPTestLimits()
			c := piHTTPTestCore(t, l)
			frames := piHTTPTestFrames(l, []byte("abc"))
			wt, out, err := c.beginWrite(frames[0])
			if err != nil {
				t.Fatal(err)
			}
			want := bytes.Clone(out)
			clear(frames[0].Body) // no longer aliases the outbound snapshot
			if !bytes.Equal(out, want) {
				t.Fatal("outbound aliases input")
			}
			if held != "writer" {
				if err := c.completeWrite(wt, len(out), nil); err != nil {
					t.Fatal(err)
				}
				if err := c.startReader(); err != nil {
					t.Fatal(err)
				}
				if held == "driver" {
					piHTTPTestRead(t, c, frames[1])
					if _, err := c.borrowRequest(); err != nil {
						t.Fatal(err)
					}
					_ = c.endReader() // premature end burns run but driver still holds data
				}
			}
			live := c.ledger.(*boundedAPIBorrowLedger).live
			if err := c.clear(); err == nil || len(c.request) == 0 || !bytes.Equal(c.outbound[:len(want)], want) || c.ledger.(*boundedAPIBorrowLedger).live != live {
				t.Fatal("cleared/refunded beneath owned borrower")
			}
		})
	}
}

func TestPiHTTPCoordinatorProspectiveBudgetAndBounds(t *testing.T) {
	l := piHTTPTestLimits()
	b, err := measurePiHTTPCoordinator(l)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"exact", "work_short", "live_short", "nodes_short"} {
		t.Run(name, func(t *testing.T) {
			ledger := &boundedAPIBorrowLedger{remainingCopy: b.Work, remainingLive: b.Live, remainingNodes: b.Nodes}
			switch name {
			case "work_short":
				ledger.remainingCopy--
			case "live_short":
				ledger.remainingLive--
			case "nodes_short":
				ledger.remainingNodes--
			}
			c, err := preparePiHTTPCoordinator(l, ledger)
			if name != "exact" {
				if c != nil || err == nil || ledger.copy != 0 || ledger.live != 0 || ledger.nodes != 0 {
					t.Fatalf("unfunded construction c=%v err=%v", c, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := c.clear(); err != nil {
				t.Fatal(err)
			}
			if err := c.clear(); err != nil || ledger.live != 0 || ledger.copy != b.Work || ledger.nodes != b.Nodes {
				t.Fatal("refund/replayed retirement", err)
			}
		})
	}
	bad := l
	bad.FrameBytes = math.MaxUint64
	if _, err := measurePiHTTPCoordinator(bad); err == nil {
		t.Fatal("unchecked oversized arithmetic")
	}
	// Largest admitted chunk count under 4096 total protocol frames.
	max := l
	max.ResponseChunkBytes, max.ResponseBytes = 1, 2045
	a, err := pisdkhttpwire.Measure(max)
	if err != nil || a.Frames != 4096 {
		t.Fatalf("max frames=%d err=%v", a.Frames, err)
	}
	c := piHTTPTestCore(t, max)
	for _, f := range piHTTPTestFrames(max, bytes.Repeat([]byte{'a'}, 2045)) {
		switch f.Kind {
		case pisdkhttpwire.FetchRequest:
			if err := c.startReader(); err != nil {
				t.Fatal(err)
			}
			piHTTPTestRead(t, c, f)
		case pisdkhttpwire.Pull, pisdkhttpwire.StructuredFinal:
			piHTTPTestRead(t, c, f)
		default:
			piHTTPTestWrite(t, c, f)
		}
	}
	if err := c.observeEOF(); err != nil {
		t.Fatal(err)
	}
	if err := c.endReader(); err != nil {
		t.Fatal(err)
	}
}

func TestPiHTTPCoordinatorConcurrentAbortDoesNotWaitForOwners(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := piHTTPTestLimits()
		c := piHTTPTestCore(t, l)
		frames := piHTTPTestFrames(l, []byte("abc"))
		piHTTPTestFetch(t, c, frames)
		wt, out, err := c.beginWrite(frames[2])
		if err != nil {
			t.Fatal(err)
		}
		held, release, joined := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		released, consumed := false, false
		// Registered before launch: even the intended regression's Fatal must
		// release and join the test-owned writer before core retirement.
		t.Cleanup(func() {
			if !released {
				close(release)
			}
			if !consumed {
				<-joined
			}
		})
		go func() { close(held); <-release; joined <- c.completeWrite(wt, len(out), nil) }()
		<-held
		c.abort() // must return without asking the held writer to finish
		if err := c.clear(); err == nil {
			t.Fatal("cleared beneath held writer/reader")
		}
		close(release)
		released = true
		result := <-joined
		consumed = true
		if result == nil {
			t.Fatal("late successful write revived aborted core")
		}
	})
}

func TestPiHTTPCoordinatorRetirementDropsPendingMetadataAliases(t *testing.T) {
	l := piHTTPTestLimits()
	c := piHTTPTestCore(t, l)
	frames := piHTTPTestFrames(l, []byte("abc"))
	piHTTPTestWrite(t, c, frames[0])
	if err := c.startReader(); err != nil {
		t.Fatal(err)
	}
	raw := piHTTPTestEncode(t, frames[1], l)
	rt := piHTTPTestHeader(t, c, raw)
	m := 17 + int(binary.BigEndian.Uint32(raw[5:9]))
	if err := c.acceptMetadata(rt, raw[17:m]); err != nil {
		t.Fatal(err)
	}
	// Leaf pendingFrame.Headers now borrows the retained request backing array.
	request := c.request
	c.abort()
	_ = c.endReader()
	if err := c.clear(); err != nil {
		t.Fatal(err)
	}
	if c.seq != nil || c.request != nil || c.ledger != nil || c.live != 0 || c.requestFrame.Headers != nil || c.input.stage != 0 {
		t.Fatal("retirement retained sequence/ledger/request aliases after refund")
	}
	if !bytes.Equal(request, make([]byte, len(request))) {
		t.Fatal("retired request not zeroed")
	}
}

func TestPiHTTPCoordinatorWriterEntryNeverQueues(t *testing.T) {
	c := piHTTPTestCore(t, piHTTPTestLimits())
	frames := piHTTPTestFrames(c.limits, []byte("abc"))
	c.mu.Lock()
	unlocked, consumed := false, false
	joined := make(chan error, 1)
	secondStarted, secondConsumed := false, false
	second := make(chan error, 1)
	t.Cleanup(func() {
		c.abort()
		if !unlocked {
			c.mu.Unlock()
		}
		if !consumed {
			<-joined
		}
		if secondStarted && !secondConsumed {
			<-second
		}
	})
	go func() {
		_, _, err := c.beginWrite(frames[0])
		joined <- err
	}()
	// Mutex waits are not durably blocked in synctest. Poll the actual
	// CAS claim instead; elapsed time is only a failure bound, not proof.
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for !c.writer.Load() {
		select {
		case <-poll.C:
		case <-deadline.C:
			if !c.writer.Load() {
				t.Fatal("first writer did not claim before coordinator")
			}
		}
	}
	secondStarted = true
	go func() { _, _, err := c.beginWrite(frames[0]); second <- err }()
	secondBound := time.NewTimer(5 * time.Second)
	defer secondBound.Stop()
	var secondErr error
	select {
	case secondErr = <-second:
		secondConsumed = true
	case <-secondBound.C:
		// Completion and timer can both be ready; inspect completion before
		// reporting a queued writer. Cleanup unlocks and joins both callers.
		select {
		case secondErr = <-second:
			secondConsumed = true
		default:
			t.Fatal("second writer queued behind held coordinator")
		}
	}
	if secondErr == nil || !c.failed.Load() {
		t.Fatal("second writer queued behind coordinator instead of burning run")
	}
	c.mu.Unlock()
	unlocked = true
	result := <-joined
	consumed = true
	if result == nil || c.write.epoch != 0 || c.writer.Load() {
		t.Fatal("first writer published after overlapping-entry denial")
	}
}

type piHTTPRejectRetirementLedger struct {
	boundedAPIBorrowLedger
	endCalls int
	err      error
}

func (l *piHTTPRejectRetirementLedger) EndLive(uint64) error { l.endCalls++; return l.err }

func TestPiHTTPCoordinatorRetirementFailureRemainsTerminal(t *testing.T) {
	l := piHTTPTestLimits()
	b, err := measurePiHTTPCoordinator(l)
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("synthetic ledger retirement rejection")
	ledger := &piHTTPRejectRetirementLedger{boundedAPIBorrowLedger: boundedAPIBorrowLedger{
		remainingCopy: b.Work, remainingLive: b.Live, remainingNodes: b.Nodes}, err: want}
	c, err := preparePiHTTPCoordinator(l, ledger)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := c.clear(); !errors.Is(err, want) {
			t.Fatalf("retirement lost failure: %v", err)
		}
	}
	if ledger.endCalls != 1 || ledger.live != b.Live || c.seq != nil || c.ledger != ledger || c.live != b.Live {
		t.Fatalf("ambiguous refund retried/lost: calls=%d ledgerLive=%d retainedLive=%d", ledger.endCalls, ledger.live, c.live)
	}
}
