package providercredential

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"

	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func piHTTPFixture(body string, chunked bool) string {
	if chunked {
		return "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\nX-Untrusted-Metadata: retained-only\r\n\r\n" + fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(body), body)
	}
	return "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: " + fmt.Sprint(len(body)) + "\r\nConnection: close\r\nX-Untrusted-Metadata: retained-only\r\n\r\n" + body
}

func piHTTPReceiver(t *testing.T, h, r uint64) *piHTTPReceive {
	t.Helper()
	p, err := newPiHTTPReceive(newPiFileLedger(), h, r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.clear)
	return p
}

type piFragmentReader struct {
	source              *bytes.Reader
	maximum, cut, readN int
	dataEOF             bool
}

func (r *piFragmentReader) Read(p []byte) (int, error) {
	if r.maximum > 0 && len(p) > r.maximum {
		p = p[:r.maximum]
	}
	if r.cut > r.readN && len(p) > r.cut-r.readN {
		p = p[:r.cut-r.readN]
	}
	n, err := r.source.Read(p)
	r.readN += n
	if r.dataEOF && n > 0 && r.source.Len() == 0 {
		err = io.EOF
	}
	return n, err
}

func TestPiHTTPReceiveFragmentationAndActualEOF(t *testing.T) {
	const body = "data: {\"fixture\":true}\n\n"
	for _, chunked := range []bool{false, true} {
		wire := piHTTPFixture(body, chunked)
		for split := 0; split <= len(wire); split++ {
			t.Run(fmt.Sprintf("chunked=%v/cut=%d", chunked, split), func(t *testing.T) {
				p := piHTTPReceiver(t, 512, uint64(len(body)))
				source := &piFragmentReader{source: bytes.NewReader([]byte(wire)), cut: split, dataEOF: split%2 == 0}
				out, err := p.receive(t.Context(), source)
				if err != nil || string(out.entity) != body || !bytes.Contains(out.rawHeaders, []byte("retained-only")) || bytes.Contains(out.headers, []byte("retained-only")) {
					t.Fatalf("unapproved entity/header preservation err=%v", err)
				}
				if pisdkhttpwire.ValidateHeaders(out.headers, 512, false) != nil || len(out.entity) != cap(out.entity) {
					t.Error("canonical replay DATA / capped view")
				}
				if again, err := p.receive(t.Context(), bytes.NewReader(nil)); !errors.Is(err, ErrConsumed) || again.entity != nil {
					t.Error("receive replenished", err)
				}
			})
		}
		for maximum := 1; maximum <= 7; maximum++ {
			p := piHTTPReceiver(t, 512, uint64(len(body)))
			if out, err := p.receive(t.Context(), &piFragmentReader{source: bytes.NewReader([]byte(wire)), maximum: maximum}); err != nil || string(out.entity) != body {
				t.Fatalf("fragment size%d: %v", maximum, err)
			}
		}
	}
}

func TestPiHTTPReceiveFunding(t *testing.T) {
	const h, r = uint64(512), uint64(64)
	metadata := uint64(reflect.TypeOf(piHTTPReceive{}).Size())
	live := 2*h + r + metadata
	work := 128*h + 4*r + 80*(r+1) + 1024
	for _, mode := range []string{"exact", "copy one short", "live one short", "nodes one short"} {
		t.Run(mode, func(t *testing.T) {
			l := &piFileLedger{maxCopy: work, maxLive: live, maxNodes: 4}
			switch mode {
			case "copy one short":
				l.maxCopy--
			case "live one short":
				l.maxLive--
			case "nodes one short":
				l.maxNodes--
			}
			p, err := newPiHTTPReceive(l, h, r)
			if mode != "exact" {
				if p != nil || err == nil || l.copy != 0 || l.live != 0 || l.nodes != 0 {
					t.Fatal("unfunded receiver", err, l)
				}
				return
			}
			if err != nil || l.copy != work || l.live != live || l.nodes != 4 {
				t.Fatal("exact receiver schedule", err, l)
			}
			p.clear()
			if err := l.EndLive(live); err != nil || l.live != 0 || l.copy != work || l.nodes != 4 {
				t.Fatal("retirement", err, l)
			}
		})
	}
	for _, v := range [][2]uint64{{0, 1}, {1, 0}, {1<<20 + 1, 1}, {1, 4<<20 + 1}, {^uint64(0), ^uint64(0)}} {
		l := newPiFileLedger()
		if p, err := newPiHTTPReceive(l, v[0], v[1]); err == nil || p != nil || l.copy != 0 {
			t.Fatal("unbounded receiver", v)
		}
	}
}

func TestPiHTTPReceiveGrammarDenials(t *testing.T) {
	valid := piHTTPFixture("abc", false)
	chunked := piHTTPFixture("abc", true)
	bad := map[string]string{
		"status prefix": strings.Replace(valid, "200 OK", "2000 OK", 1), "missing reason delimiter": strings.Replace(valid, "200 OK", "200", 1),
		"HTTP1.0": strings.Replace(valid, "HTTP/1.1", "HTTP/1.0", 1), "interim": strings.Replace(valid, "200 OK", "100 Continue", 1),
		"redirect": strings.Replace(valid, "200 OK", "302 Redirect", 1), "status control": strings.Replace(valid, "200 OK", "200 O\tK", 1),
		"loneLF": strings.ReplaceAll(valid, "\r\n", "\n"), "badColon": strings.Replace(valid, "Content-Type:", "Content-Type :", 1),
		"fold": strings.Replace(valid, "X-Untrusted-Metadata:", " X-Untrusted-Metadata:", 1), "duplicate": strings.Replace(valid, "Content-Length: 3", "Content-Length: 3\r\ncontent-length: 3", 1),
		"metadata duplicate": strings.Replace(valid, "X-Untrusted-Metadata: retained-only", "X-Untrusted-Metadata: retained-only\r\nx-untrusted-metadata: other", 1),
		"empty":              strings.Replace(valid, "retained-only", "", 1), "nonASCII": strings.Replace(valid, "retained-only", "é", 1),
		"internalTAB": strings.Replace(valid, "retained-only", "retained\tonly", 1), "contentType": strings.Replace(valid, "text/event-stream", "application/json", 1),
		"charset spaces": strings.Replace(valid, "text/event-stream", "text/event-stream; charset = utf-8", 1),
		"leadingZero":    strings.Replace(valid, "Content-Length: 3", "Content-Length: 03", 1), "negativeCL": strings.Replace(valid, "Content-Length: 3", "Content-Length: -3", 1),
		"zeroCL": strings.Replace(valid, "Content-Length: 3", "Content-Length: 0", 1), "hugeCL": strings.Replace(valid, "Content-Length: 3", "Content-Length: 999999999999999999999999999", 1),
		"closeDelimited": strings.Replace(valid, "Content-Length: 3\r\n", "", 1), "CL+TE": strings.Replace(valid, "Content-Length: 3", "Content-Length: 3\r\nTransfer-Encoding: chunked", 1),
		"extra": valid + "X", "truncated": valid[:len(valid)-1], "chunks truncated": chunked[:len(chunked)-1],
		"chunkExtension": strings.Replace(chunked, "3\r\nabc", "3;foo=bar\r\nabc", 1), "chunk size huge": strings.Replace(chunked, "3\r\nabc", "ffffffffffffffff\r\nabc", 1),
		"chunk17digits": strings.Replace(chunked, "3\r\nabc", "00000000000000003\r\nabc", 1), "chunkCRLF": strings.Replace(chunked, "abc\r\n", "abc\n", 1),
		"trailers": strings.Replace(chunked, "0\r\n\r\n", "0\r\nX: forbidden\r\n\r\n", 1), "chunkExtra": chunked + "X",
	}
	for _, header := range []string{"Trailer: X", "Set-Cookie: x=y", "Location: https://other.invalid/", "Refresh: 0", "WWW-Authenticate: Bearer", "Proxy-Authenticate: Basic", "Proxy-Authorization: x", "Upgrade: websocket", "Content-Disposition: attachment", "Content-Encoding: gzip", "Connection: keep-alive"} {
		wire := strings.Replace(valid, "Connection: close", header, 1)
		bad[header] = wire
	}
	for name, wire := range bad {
		t.Run(name, func(t *testing.T) {
			p := piHTTPReceiver(t, 512, 64)
			out, err := p.receive(t.Context(), bytes.NewBufferString(wire))
			if err == nil || out.rawHeaders != nil || out.entity != nil || out.headers != nil {
				t.Fatal("denial exposed DATA", err)
			}
			if !bytes.Equal(p.head, make([]byte, len(p.head))) || !bytes.Equal(p.entity, make([]byte, len(p.entity))) || !bytes.Equal(p.replay, make([]byte, len(p.replay))) {
				t.Error("denial retained buffer data")
			}
		})
	}
}

func TestPiHTTPReceiveExactBoundsAndFramingWork(t *testing.T) {
	const body = "123456789"
	wire := piHTTPFixture(body, false)
	headBytes := strings.Index(wire, "\r\n\r\n") + 4
	for _, delta := range []int{0, -1} {
		p := piHTTPReceiver(t, uint64(headBytes+delta), uint64(len(body)))
		_, err := p.receive(t.Context(), bytes.NewBufferString(wire))
		if (err == nil) != (delta == 0) {
			t.Fatalf("head boundary delta%d err=%v", delta, err)
		}
	}
	for _, delta := range []int{0, -1} {
		p := piHTTPReceiver(t, 512, uint64(len(body)+delta))
		_, err := p.receive(t.Context(), bytes.NewBufferString(wire))
		if (err == nil) != (delta == 0) {
			t.Fatalf("entity boundary delta%d err=%v", delta, err)
		}
	}
	head := "HTTP/1.1 200 \r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n"
	var b strings.Builder
	b.WriteString(head)
	for _, c := range []byte(body) {
		fmt.Fprintf(&b, "0000000000000001\r\n%c\r\n", c)
	}
	b.WriteString("0000000000000000\r\n\r\n")
	for _, short := range []bool{false, true} {
		p := piHTTPReceiver(t, 512, uint64(len(body)))
		if short {
			p.framingLimit--
		}
		out, err := p.receive(t.Context(), bytes.NewBufferString(b.String()))
		if short {
			if err == nil || out.entity != nil {
				t.Fatal("first-over framing work accepted")
			}
		} else if err != nil || p.framing != 20*(uint64(len(body))+1) {
			t.Fatal("maximum framing", err, p.framing)
		}
	}
	for _, count := range []int{64, 65} {
		var b strings.Builder
		b.WriteString("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 1\r\n")
		for i := 2; i < count; i++ {
			fmt.Fprintf(&b, "X-%d: a\r\n", i)
		}
		b.WriteString("\r\nx")
		p := piHTTPReceiver(t, 4096, 1)
		_, err := p.receive(t.Context(), bytes.NewBufferString(b.String()))
		if (err == nil) != (count == 64) {
			t.Fatal("pair count boundary", count, err)
		}
	}
}

type piEmptyRead struct{}

func (piEmptyRead) Read([]byte) (int, error) { return 0, nil }

type piErrorRead struct{}

func (piErrorRead) Read(p []byte) (int, error) { p[0] = 'H'; return 1, io.ErrUnexpectedEOF }

type piHeldEOF struct {
	source           *bytes.Reader
	entered, release chan struct{}
	once             sync.Once
}

func (r *piHeldEOF) Read(p []byte) (int, error) {
	if r.source.Len() > 0 {
		return r.source.Read(p)
	}
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return 0, io.EOF
}

func TestPiHTTPReceiveWaitsForActualEOFAndCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			p := piHTTPReceiver(t, 512, 3)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source := &piHeldEOF{source: bytes.NewReader([]byte(piHTTPFixture("abc", false))), entered: make(chan struct{}), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(source.release) })
			defer release()
			done := make(chan error, 1)
			joined := false
			go func() {
				out, err := p.receive(ctx, source)
				if err != nil && out.entity != nil {
					t.Error("denial exposed entity")
				}
				done <- err
			}()
			defer func() {
				release()
				if !joined {
					<-done
				}
			}()
			<-source.entered
			select {
			case err := <-done:
				joined = true
				t.Fatalf("framing manufactured EOF: %v", err)
			default:
			}
			if cancelled {
				cancel()
			}
			release()
			err := <-done
			joined = true
			if (err == nil) == cancelled {
				t.Fatalf("EOF/cancelled=%v err=%v", cancelled, err)
			}
		})
	}
	for _, source := range []io.Reader{piEmptyRead{}, piErrorRead{}} {
		p := piHTTPReceiver(t, 512, 3)
		if out, err := p.receive(t.Context(), source); err == nil || out.entity != nil {
			t.Fatal("zero-progress/nonEOF error accepted", err)
		}
	}
}

func TestPiHTTPReceiveNoParseAllocations(t *testing.T) {
	const count = 11 // AllocsPerRun warmup +10 measurements, each genuinely once
	var plans [count]*piHTTPReceive
	var readers [count]bytes.Reader
	wire := []byte(piHTTPFixture("abc", true))
	for i := range plans {
		plans[i] = piHTTPReceiver(t, 512, 3)
		readers[i].Reset(wire)
	}
	i := 0
	allocs := testing.AllocsPerRun(10, func() {
		_, err := plans[i].receive(context.Background(), &readers[i])
		i++
		if err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("post-preparation parser allocations=%v want0", allocs)
	}
}

func TestPiHTTPReceiveOriginalPreparationAndRetirement(t *testing.T) {
	for _, mode := range []string{"valid", "wrong H", "wrong R", "late", "repeat"} {
		t.Run(mode, func(t *testing.T) {
			s, _, issue, p, l, request := piAuthOwnerFixture(t)
			if err := p.PrepareRequestAuthenticationV1(request); err != nil {
				t.Fatal(err)
			}
			h, r := request.Profile.Limits.ResponseHeaderBytes, request.Profile.Limits.ResponseBodyBytes
			switch mode {
			case "wrong H":
				h++
			case "wrong R":
				r++
			case "late":
				_ = issueImmutableAPI(t, s, issue)
			case "repeat":
				if err := p.PrepareHTTPReceiveV1(h, r); err != nil {
					t.Fatal(err)
				}
			}
			err := p.PrepareHTTPReceiveV1(h, r)
			if (err == nil) != (mode == "valid") {
				t.Fatal(mode, err)
			}
			if err := p.Close(); err != nil || l.live != 0 {
				t.Fatal("original cleanup", err, l.live)
			}
		})
	}
}

func TestPiHTTPReceiveOriginalCallbackRetainsUntilJoinedCleanup(t *testing.T) {
	s, _, issue, p, l, request := piAuthOwnerFixture(t)
	if err := p.PrepareRequestAuthenticationV1(request); err != nil {
		t.Fatal(err)
	}
	if err := p.PrepareHTTPReceiveV1(request.Profile.Limits.ResponseHeaderBytes, request.Profile.Limits.ResponseBodyBytes); err != nil {
		t.Fatal(err)
	}
	h := issueImmutableAPI(t, s, issue)
	var view piUnapprovedHTTPView
	before := l.live
	if err := s.MaterializeProviderCredential(t.Context(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error {
		use := p.state.use.Load()
		<-use.gate
		var err error
		view, err = p.state.http.receive(use.ctx, bytes.NewBufferString(piHTTPFixture("private-http-fixture", false)))
		use.gate <- struct{}{}
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(fmt.Sprintf("%+v %#v %+v", view, view, p.state.http), "private-http-fixture") {
			t.Error("formatting leaked retained body")
		}
		if err := p.Close(); !errors.Is(err, ErrConsumed) || l.live != before || p.state.http == nil || len(view.entity) == 0 || view.entity[0] == 0 {
			t.Fatal("early HTTP refund/drop", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil || l.live != 0 || p.state.http != nil {
		t.Fatal("joined HTTP cleanup", err, l.live)
	}
	for _, b := range [][]byte{view.rawHeaders, view.entity, view.headers} {
		for _, c := range b {
			if c != 0 {
				t.Fatal("retired HTTP alias retained data")
			}
		}
	}
}

func TestPiHTTPReceiveMaximumPayloadCeilings(t *testing.T) {
	const maximumH = 1 << 20
	base := piHTTPFixture("x", false)
	pad := "X-Pad: " + strings.Repeat("a", maximumH-(strings.Index(base, "\r\n\r\n")+4)-9) + "\r\n"
	wire := strings.Replace(base, "\r\n\r\n", "\r\n"+pad+"\r\n", 1)
	if got := strings.Index(wire, "\r\n\r\n") + 4; got != maximumH {
		t.Fatalf("maximum head fixture got%d", got)
	}
	p := piHTTPReceiver(t, maximumH, 1)
	if out, err := p.receive(t.Context(), bytes.NewBufferString(wire)); err != nil || len(out.rawHeaders) != maximumH {
		t.Fatal("maximum H", err)
	}
	body := strings.Repeat("a", 4<<20)
	p = piHTTPReceiver(t, 512, 4<<20)
	if out, err := p.receive(t.Context(), bytes.NewBufferString(piHTTPFixture(body, false))); err != nil || len(out.entity) != len(body) {
		t.Fatal("maximum R", err)
	}
}
