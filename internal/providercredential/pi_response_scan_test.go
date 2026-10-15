package providercredential

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func piScanLimits() domain.AttachedPiSDKNativeLimitsV1 {
	return domain.AttachedPiSDKNativeLimitsV1{ResponseHeaderBytes: 512, ResponseBodyBytes: 4096, SSELineBytes: 1024, SSEEventBytes: 2048,
		SSEEvents: 32, JSONNodes: 256, JSONDepth: 8, ResponseCopyWorkBytes: 1 << 20, ResponseAllocationBytes: 1 << 20}
}

func piScanner(t *testing.T, l domain.AttachedPiSDKNativeLimitsV1) *piResponseScan {
	t.Helper()
	q, err := newPiResponseScan(newPiFileLedger(), l, 64)
	if err != nil {
		t.Fatal("pre-Issue scanner", err)
	}
	t.Cleanup(q.clear)
	return q
}

func piScanView(t *testing.T, entity string) piUnapprovedHTTPView {
	t.Helper()
	r := piHTTPReceiver(t, 512, 4096)
	view, err := r.receive(t.Context(), strings.NewReader(piHTTPFixture(entity, false)))
	if err != nil {
		t.Fatal("private framing DATA", err)
	}
	return view
}

func piScanZero(q *piResponseScan) bool {
	if q.ctx != nil || q.raw != nil || q.key != nil {
		return false
	}
	for _, c := range q.text {
		if c != 0 {
			return false
		}
	}
	for _, c := range q.nodes {
		if c != (piResponseNode{}) {
			return false
		}
	}
	for _, c := range q.events {
		if c != (piResponseEvent{}) {
			return false
		}
	}
	for _, c := range q.failure {
		if c != 0 {
			return false
		}
	}
	return bytes.Equal(q.data, make([]byte, len(q.data)))
}

func TestPiResponseScanPrivateGraphAndNativeFraming(t *testing.T) {
	for _, sep := range []string{"\n", "\r", "\r\n"} {
		t.Run(fmt.Sprintf("%q", sep), func(t *testing.T) {
			entity := ": comment" + sep + sep + "event: demo" + sep + "data: {" + sep + "data: \"type\":\"demo\",\"a\":[true,null,-0,1.5e2]}" + sep + sep
			q := piScanner(t, piScanLimits())
			if err := q.scan(t.Context(), piScanView(t, entity), []byte("fixture-secret")); err != nil {
				t.Fatal(err)
			}
			if q.frames != 2 || q.eventN != 1 {
				t.Fatal("framer/dispatch counters conflated", q.frames, q.eventN)
			}
			root := q.events[0].root
			if q.nodes[root].kind != 'o' || !q.equalsBytes(q.field(root, "type"), []byte("demo")) {
				t.Fatal("private graph lost decoded member")
			}
			array := q.field(root, "a")
			if q.nodes[array].kind != 'a' {
				t.Fatal("array shape lost")
			}
			child := q.nodes[array].child
			if q.nodes[child].kind != 't' || q.nodes[q.nodes[child].next].kind != 'n' {
				t.Fatal("literal nodes lost")
			}
			child = q.nodes[q.nodes[child].next].next
			if q.nodes[child].number != 0 || !math.Signbit(q.nodes[child].number) || q.nodes[q.nodes[child].next].number != 150 {
				t.Fatal("JS Number negative zero/exponent lost")
			}
			if q.scan(t.Context(), piScanView(t, entity), []byte("fixture-secret")) != ErrConsumed {
				t.Fatal("scan replenished")
			}
			q.clear()
			if !piScanZero(q) {
				t.Fatal("retirement retained response/key aliases")
			}
		})
	}
	// Mixed line endings can dispatch TWO logical events in ONE byte frame.
	q := piScanner(t, piScanLimits())
	if err := q.scan(t.Context(), piScanView(t, "data: {}\n\rdata: {}\n\n"), []byte("fixture-secret")); err != nil || q.frames != 1 || q.eventN != 2 {
		t.Fatal("mixed-ending independent counters", err, q.frames, q.eventN)
	}
}

func TestPiResponseScanAllLiteralAndDecodedDomains(t *testing.T) {
	for _, mode := range []string{"raw body", "comment", "raw header", "canonical header", "escaped value", "escaped key", "overlap prefix"} {
		t.Run(mode, func(t *testing.T) {
			entity, key := "data: {\"x\":\"safe\"}\n\n", "fixture-secret"
			switch mode {
			case "raw body":
				entity = "data: {\"x\":\"fixture-secret\"}\n\n"
			case "comment":
				entity = ": fixture-secret\n\ndata: {}\n\n"
			case "escaped value":
				entity = "data: {\"x\":\"fixture-\\u0073ecret\"}\n\n"
			case "escaped key":
				entity = "data: {\"fixture-\\u0073ecret\":0}\n\n"
			case "overlap prefix":
				key = "ababac"
				entity = "data: {\"x\":\"ababababac\"}\n\n"
			case "canonical header":
				key = "content-type"
			}
			view := piScanView(t, entity)
			if mode == "raw header" {
				view.rawHeaders = append(bytes.Clone(view.rawHeaders), []byte("fixture-secret")...)
			}
			if mode == "canonical header" && bytes.Contains(view.rawHeaders, []byte(key)) {
				t.Fatal("canonical-case counterexample was already raw match")
			}
			q := piScanner(t, piScanLimits())
			if err := q.scan(t.Context(), view, []byte(key)); err == nil || !piScanZero(q) {
				t.Fatal("secret domain escaped scanner or denial retained aliases", err)
			}
		})
	}
	// Unrelated scalar strings must NOT be joined here. That job belongs to the
	// exact Pi slot/signature layer; this parsed graph is still UNAPPROVED DATA.
	q := piScanner(t, piScanLimits())
	if err := q.scan(t.Context(), piScanView(t, `data: {"a":"fixture-","b":"secret","type":"future_tool"}`+"\n\n"), []byte("fixture-secret")); err != nil {
		t.Fatal("invented unrelated scalar join", err)
	}
}

func TestPiResponseScanStrictJSONAndUTF16(t *testing.T) {
	for name, raw := range map[string]string{
		"decoded duplicate": `{"a":1,"\u0061":2}`, "surrogate duplicate": `{"😀":1,"\ud83d\ude00":2}`,
		"comma": `{"a":1,}`, "array comma": `{"a":[1,]}`, "colon": `{"a" 1}`, "trailing": `{} {}`,
		"nonobject root": `[]`, "leading zero": `{"a":01}`, "missing fraction": `{"a":1.}`, "exponent": `{"a":1e+}`,
		"infinite": `{"a":1e309}`, "NaN": `{"a":NaN}`, "bad escape": `{"a":"\x41"}`, "bad hex": `{"a":"\u00xx"}`,
		"unterminated string": `{"a":"x}`, "control": "{\"a\":\"x\ty\"}", "garbage": `{"a":truefalse}`,
	} {
		t.Run(name, func(t *testing.T) {
			q := piScanner(t, piScanLimits())
			if err := q.scan(t.Context(), piScanView(t, "data: "+raw+"\n\n"), []byte("fixture-secret")); err == nil || !piScanZero(q) {
				t.Fatal("invalid JSON accepted/retained", err)
			}
		})
	}
	for _, raw := range []string{`{"\ud800":"\ud801","\ufffd":"😀","x":"\ud83d\ude00"}`, `{"a":-0,"b":9007199254740993,"c":5e-324,"d":1e-999}`, `{"a":"fixture-\u0100secret"}`} {
		q := piScanner(t, piScanLimits())
		if err := q.scan(t.Context(), piScanView(t, "data: "+raw+"\n\n"), []byte("fixture-secret")); err != nil {
			t.Fatal("valid JS string/number DATA denied", err)
		}
	}
}

func TestPiResponseScanSSEGrammarDenials(t *testing.T) {
	for _, entity := range []string{"data: {}\n", "data: {}\r\n\r", "data: {}", "id: x\ndata: {}\n\n", "retry: 1\ndata: {}\n\n", "event: a\nevent: a\ndata: {}\n\n", "event: a\n\n", "event: a\ndata: {\"type\":\"b\"}\n\n", "data: [DONE]\n\n", "\xef\xbb\xbfdata: {}\n\n", ":\xff\n\n", ":\x00\n\n", "data: {\"x\":\"\xff\"}\n\n"} {
		q := piScanner(t, piScanLimits())
		if err := q.scan(t.Context(), piScanView(t, entity), []byte("fixture-secret")); err == nil || !piScanZero(q) {
			t.Fatalf("invalid SSE accepted/retained: %q err=%v", entity, err)
		}
	}
	for _, entity := range []string{": keepalive\n\n", "event:\ndata: {}\n\n", "data: {\n\"x\":1}\n\n"} {
		// The last form lacks data: on the continued line and must deny.
		q := piScanner(t, piScanLimits())
		err := q.scan(t.Context(), piScanView(t, entity), []byte("fixture-secret"))
		if (err != nil) != strings.Contains(entity, "\n\"x\"") {
			t.Fatalf("SSE field rule %q: %v", entity, err)
		}
	}
}

func TestPiResponseScanExactRuntimeBounds(t *testing.T) {
	const entity = "data: {\"a\":\"b\"}\n\n"
	l := piScanLimits()
	q := piScanner(t, l)
	if err := q.scan(t.Context(), piScanView(t, entity), []byte("fixture-secret")); err != nil {
		t.Fatal(err)
	}
	if q.nodeN != 3 {
		t.Fatal("node oracle", q.nodeN)
	}
	for _, mode := range []string{"body", "line", "frame", "frames including comments", "dispatches within frame", "nodes", "depth", "work"} {
		for _, short := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/short=%v", mode, short), func(t *testing.T) {
				limit, body := l, entity
				switch mode {
				case "body":
					limit.ResponseBodyBytes = uint64(len(body))
					limit.SSEEventBytes = uint64(len(body))
					limit.SSELineBytes = uint64(len(body)) - 2
					if short {
						body += "\n"
					}
				case "line":
					limit.SSELineBytes = uint64(len(body)) - 2
					if short {
						limit.SSELineBytes--
					}
				case "frame":
					limit.SSEEventBytes = uint64(len(body))
					limit.SSELineBytes = uint64(len(body)) - 2
					if short {
						limit.SSEEventBytes--
					}
				case "frames including comments":
					body = ": c\n\n" + entity
					limit.SSEEvents = 2
					if short {
						limit.SSEEvents = 1
					}
				case "dispatches within frame":
					body = "data: {}\n\rdata: {}\n\n"
					limit.SSEEvents = 2
					if short {
						limit.SSEEvents = 1
					}
				case "nodes":
					limit.JSONNodes = 3
					if short {
						limit.JSONNodes = 2
					}
				case "depth":
					body = "data: {\"a\":[1]}\n\n"
					limit.JSONDepth = 2
					if short {
						limit.JSONDepth = 1
					}
				case "work":
					limit.ResponseCopyWorkBytes = q.work
					if short {
						limit.ResponseCopyWorkBytes--
					}
				}
				s := piScanner(t, limit)
				err := s.scan(t.Context(), piScanView(t, body), []byte("fixture-secret"))
				if (err != nil) != short {
					t.Fatal("boundary", err)
				}
			})
		}
	}
}

func TestPiResponseScanProspectiveFunding(t *testing.T) {
	l := piScanLimits()
	n := l.JSONNodes
	events := l.SSEEvents
	live := uint64(reflect.TypeOf(piResponseScan{}).Size()) + (n+1)*uint64(reflect.TypeOf(piResponseNode{}).Size()) + 4*l.ResponseBodyBytes + l.SSEEventBytes + events*uint64(reflect.TypeOf(piResponseEvent{}).Size()) + 4*64 + 96*n + 8192
	for _, mode := range []string{"exact", "work short", "live short", "nodes short", "allocation exact", "allocation short"} {
		t.Run(mode, func(t *testing.T) {
			credit := &piFileLedger{maxCopy: l.ResponseCopyWorkBytes, maxLive: live, maxNodes: 6 + 3*n + events}
			limits := l
			switch mode {
			case "work short":
				credit.maxCopy--
			case "live short":
				credit.maxLive--
			case "nodes short":
				credit.maxNodes--
			case "allocation exact", "allocation short":
				limits.ResponseAllocationBytes = live + 2*l.ResponseHeaderBytes + l.ResponseBodyBytes + uint64(reflect.TypeOf(piHTTPReceive{}).Size())
				if mode == "allocation short" {
					limits.ResponseAllocationBytes--
				}
			}
			q, err := newPiResponseScan(credit, limits, 64)
			if strings.HasSuffix(mode, "short") {
				if err == nil || q != nil || credit.live != 0 || credit.copy != 0 || credit.nodes != 0 {
					t.Fatal("unfunded allocation", err)
				}
				return
			}
			if err != nil || q.live != live || credit.copy != l.ResponseCopyWorkBytes || credit.nodes != 6+3*n+events {
				t.Fatal("exact budget", err)
			}
			q.clear()
			if credit.EndLive(live) != nil || credit.live != 0 || credit.copy != l.ResponseCopyWorkBytes || credit.nodes != 6+3*n+events {
				t.Fatal("retirement replenished cumulative work/nodes")
			}
		})
	}
}

func TestPiResponseScanCancellationAndSingleUse(t *testing.T) {
	q := piScanner(t, piScanLimits())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	view := piScanView(t, "data: {}\n\n")
	if err := q.scan(ctx, view, []byte("fixture-secret")); err == nil || !piScanZero(q) {
		t.Fatal("cancelled scan", err)
	}
	if err := q.scan(t.Context(), view, []byte("fixture-secret")); !errors.Is(err, ErrConsumed) {
		t.Fatal("cancelled invocation replenished", err)
	}
}

func TestPiResponseScanNativeLookbehindResetsAtEveryFrame(t *testing.T) {
	for _, sep := range []string{"\n", "\r", "\r\n"} {
		for _, suffix := range []string{"", sep, sep + sep} {
			t.Run(fmt.Sprintf("sep=%q/suffix=%q", sep, suffix), func(t *testing.T) {
				q := piScanner(t, piScanLimits())
				body := "data: {}" + sep + sep + suffix
				err := q.scan(t.Context(), piScanView(t, body), []byte("fixture-secret"))
				if (err != nil) != (suffix == sep) {
					t.Fatalf("native framer unfinished suffix: %v", err)
				}
				if err == nil && q.frames != 1+uint64(len(suffix)/len(sep)/2) {
					t.Fatal("overlapping native frame count", q.frames)
				}
			})
		}
		q := piScanner(t, piScanLimits())
		if err := q.scan(t.Context(), piScanView(t, sep+sep+sep+sep), []byte("fixture-secret")); err != nil || q.frames != 2 || q.eventN != 0 {
			t.Fatal("adjacent empty frames", err, q.frames, q.eventN)
		}
	}
}

func TestPiResponseScanIndependentCommentWorkOracle(t *testing.T) {
	l := piScanLimits()
	const body = ": xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n\n"
	view := piScanView(t, body)
	// Independent source-pass schedule, not measured by running the candidate:
	// init+ONE retirement of every explicit arena; key validation K+table6K;
	// raw H/Hc/R (input+2 key comparisons); framer R; UTF8 R; line+colon2R.
	// Comments have no decoded UTF16/JSON work and this key has no fallbacks.
	arena := uint64(reflect.TypeOf(piResponseScan{}).Size()) + (l.JSONNodes+1)*uint64(reflect.TypeOf(piResponseNode{}).Size()) + 2*l.ResponseBodyBytes + l.SSEEventBytes + l.SSEEvents*uint64(reflect.TypeOf(piResponseEvent{}).Size()) + 4*64
	const key = "~"
	work := 2*arena + 7*uint64(len(key)) + 3*uint64(len(view.rawHeaders)+len(view.headers)) + 7*uint64(len(body))
	for _, short := range []bool{false, true} {
		limits := l
		limits.ResponseCopyWorkBytes = work
		if short {
			limits.ResponseCopyWorkBytes--
		}
		q := piScanner(t, limits)
		err := q.scan(t.Context(), view, []byte(key))
		if (err != nil) != short {
			t.Fatal("independent work boundary", err, work, q.work)
		}
		if !short && q.work != work {
			t.Fatal("undeclared source traversal", q.work, work)
		}
		consumed := q.work
		q.clear()
		q.clear()
		if q.work != consumed || !piScanZero(q) {
			t.Fatal("retirement repeated or replenished prepaid work")
		}
	}
}

type piScanPauseContext struct {
	context.Context
	q                *piResponseScan
	entered, release chan struct{}
	once             sync.Once
}

func (c *piScanPauseContext) Err() error {
	if c.q.textN >= 4 {
		c.once.Do(func() { close(c.entered); <-c.release })
	}
	return c.Context.Err()
}

func TestPiResponseScanConcurrentReentryCannotClearActiveArena(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := piScanner(t, piScanLimits())
		view := piScanView(t, "data: {\"a\":\"ordinary\"}\n\n")
		ctx := &piScanPauseContext{Context: t.Context(), q: q, entered: make(chan struct{}), release: make(chan struct{})}
		release := sync.OnceFunc(func() { close(ctx.release) })
		done := make(chan error, 1)
		joined := false
		defer func() {
			release()
			if !joined {
				<-done
			}
		}()
		go func() { done <- q.scan(ctx, view, []byte("fixture-secret")) }()
		<-ctx.entered
		synctest.Wait()
		if err := q.scan(t.Context(), view, []byte("fixture-secret")); err != ErrConsumed || q.erased.Load() || q.textN < 4 {
			t.Fatal("reentry cleared active original arena", err)
		}
		release()
		err := <-done
		joined = true
		if err != nil {
			t.Fatal("original scan corrupted by reentry", err)
		}
	})
}

type piScanCancelContext struct {
	context.Context
	q         *piResponseScan
	cancel    context.CancelFunc
	triggered bool
}

func (c *piScanCancelContext) Err() error {
	if c.q.textN >= 4 {
		c.triggered = true
		c.cancel()
	}
	return c.Context.Err()
}

func TestPiResponseScanCancellationDuringDecodeClearsBeforeReturn(t *testing.T) {
	q := piScanner(t, piScanLimits())
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &piScanCancelContext{Context: parent, q: q, cancel: cancel}
	if err := q.scan(ctx, piScanView(t, "data: {\"a\":\"ordinary\"}\n\n"), []byte("fixture-secret")); err == nil || !ctx.triggered || !piScanZero(q) {
		t.Fatal("cancel during decoded string traversal retained data", err)
	}
}

func FuzzPiResponseScanPrivateDATA(f *testing.F) {
	for _, s := range []string{"data: {}\n\n", "data: {}\n\n\n", `data: {"a":1,"\u0061":2}` + "\n\n", `data: {"x":"\ud800"}` + "\n\n", ":\xff\n\n"} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) == 0 || len(body) > 4096 {
			return
		}
		q := piScanner(t, piScanLimits())
		view := piUnapprovedHTTPView{rawHeaders: []byte("fixture-header"), headers: []byte("fixture-canonical"), entity: body}
		err := q.scan(t.Context(), view, []byte("fixture-secret"))
		if q.work > q.limits.ResponseCopyWorkBytes || q.nodeN > uint32(q.maxNodes) || q.eventN > uint32(q.maxEvents) {
			t.Fatal("bounded scanner counters overflowed")
		}
		if err != nil && !piScanZero(q) {
			t.Fatal("denial retained private graph or key")
		}
		if err == nil {
			for _, n := range q.nodes[:q.nodeN+1] {
				if n.child < 0 || uint32(n.child) > q.nodeN || n.next < 0 || uint32(n.next) > q.nodeN || n.key < 0 || uint32(n.key) > q.nodeN || n.from > n.to || n.to > q.textN {
					t.Fatal("private graph offsets escaped original arena")
				}
			}
		}
	})
}
