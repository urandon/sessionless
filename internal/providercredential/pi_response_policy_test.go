package providercredential

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func piPolicyLimits() domain.AttachedPiSDKNativeLimitsV1 {
	l := piScanLimits()
	l.ResponseBodyBytes = 16 << 10
	l.SSEEventBytes = 8 << 10
	l.SSELineBytes = 8 << 10
	l.SSEEvents = 64
	l.JSONNodes = 2048
	l.ResponseCopyWorkBytes = 8 << 20
	l.ResponseAllocationBytes = 8 << 20
	l.PreSendAllocationBytes = 8 << 20
	l.NativeRequestBytes = 1 << 20
	return l
}
func piPolicyJSON(t *testing.T, v any) string {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
}
func piPolicyObject(t *testing.T, raw string) map[string]any {
	t.Helper()
	var v map[string]any
	if e := json.Unmarshal([]byte(raw), &v); e != nil {
		t.Fatal(e)
	}
	return v
}
func piPolicyItem(kind, id string) map[string]any {
	if kind == "reasoning" {
		return map[string]any{"type": kind, "id": id, "summary": []any{}}
	}
	return map[string]any{"type": "message", "id": id, "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": "safe"}}}
}
func piPolicyResponse(status string) map[string]any {
	details := any(nil)
	if status == "incomplete" {
		details = map[string]any{"reason": "max_output_tokens"}
	}
	return map[string]any{"id": "resp_fixture", "created_at": 1, "error": nil, "incomplete_details": details, "instructions": nil, "metadata": map[string]any{}, "model": "stealth-ox-alpha", "object": "response", "output": []any{}, "parallel_tool_calls": true, "temperature": 1, "tool_choice": "none", "tools": []any{}, "top_p": 1, "status": status}
}
func piPolicyLifecycle(kind string, r map[string]any) map[string]any {
	return map[string]any{"type": kind, "sequence_number": 0, "response": r}
}
func piPolicyTerminal() map[string]any {
	return piPolicyLifecycle("response.completed", piPolicyResponse("completed"))
}
func piPolicyAdded(kind, id string, index float64) map[string]any {
	return map[string]any{"type": "response.output_item.added", "sequence_number": 0, "output_index": index, "item": piPolicyItem(kind, id)}
}
func piPolicyBody(t *testing.T, events ...any) string {
	t.Helper()
	var b strings.Builder
	for _, v := range events {
		b.WriteString("data: ")
		b.WriteString(piPolicyJSON(t, v))
		b.WriteString("\n\n")
	}
	return b.String()
}
func piPolicyParsed(t *testing.T, request, body string) (*piResponseScan, *piResponseExpected) {
	t.Helper()
	l := piPolicyLimits()
	ledger := newPiFileLedger()
	raw := []byte(request)
	x, e := newPiResponseExpected(t.Context(), ledger, l, raw, sha256.Sum256(raw))
	if e != nil {
		t.Fatal("pre-Issue expected DATA", e)
	}
	t.Cleanup(x.graph.clear)
	q, e := newPiResponseScan(ledger, l, 64)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(q.clear)
	r := piHTTPReceiver(t, l.ResponseHeaderBytes, l.ResponseBodyBytes)
	v, e := r.receive(t.Context(), strings.NewReader(piHTTPFixture(body, false)))
	if e != nil {
		t.Fatal(e)
	}
	if e = q.scan(t.Context(), v, []byte("fixture-secret")); e != nil {
		t.Fatal("must reach closed policy after raw/scalar DATA checks", e)
	}
	return q, x
}
func piPolicyCheck(t *testing.T, want bool, events ...any) {
	t.Helper()
	q, x := piPolicyParsed(t, piPolicyFixtureRequest, piPolicyBody(t, events...))
	err := q.checkResponsePolicy(x)
	if (err == nil) != want {
		t.Fatal("closed policy verdict", err, want)
	}
	if !want && (!piScanZero(q) || !piScanZero(x.graph)) {
		t.Fatal("denial retained response/expectation aliases")
	}
	if want {
		if e := q.checkResponsePolicy(x); e != ErrConsumed || q.erased.Load() {
			t.Fatal("policy reentry cleared or replenished winner", e)
		}
	}
}

func TestPiResponsePolicyLifecycleAndAllAdmittedEvents(t *testing.T) {
	for _, kind := range []string{"created", "in_progress", "queued", "completed", "incomplete"} {
		t.Run(kind, func(t *testing.T) {
			status := kind
			if kind == "created" {
				status = "in_progress"
			}
			event := piPolicyLifecycle("response."+kind, piPolicyResponse(status))
			if kind == "completed" || kind == "incomplete" {
				piPolicyCheck(t, true, event)
			} else {
				piPolicyCheck(t, true, event, piPolicyTerminal())
			}
		})
	}
	for _, kind := range []string{"message", "reasoning"} {
		for _, op := range []string{"added", "done"} {
			t.Run(kind+op, func(t *testing.T) {
				e := piPolicyAdded(kind, "item_fixture", 0)
				e["type"] = "response.output_item." + op
				piPolicyCheck(t, true, e, piPolicyTerminal())
			})
		}
	}
	for _, part := range []string{"output_text", "refusal", "reasoning_text", "summary_text"} {
		for _, op := range []string{"added", "done"} {
			t.Run(part+op, func(t *testing.T) {
				p := map[string]any{"type": part, "text": "safe"}
				if part == "refusal" {
					delete(p, "text")
					p["refusal"] = "safe"
				}
				e := map[string]any{"type": "response.content_part." + op, "sequence_number": 7, "content_index": 0, "item_id": "part_fixture", "output_index": 0, "part": p}
				if part == "summary_text" {
					e["type"] = "response.reasoning_summary_part." + op
					delete(e, "content_index")
					e["summary_index"] = 0
				}
				piPolicyCheck(t, true, e, piPolicyTerminal())
			})
		}
	}
	for _, family := range []string{"reasoning_summary_text", "reasoning_text", "output_text", "refusal"} {
		for _, op := range []string{"delta", "done"} {
			t.Run(family+op, func(t *testing.T) {
				position := "content_index"
				if family == "reasoning_summary_text" {
					position = "summary_index"
				}
				text := "text"
				if op == "delta" {
					text = "delta"
				} else if family == "refusal" {
					text = "refusal"
				}
				e := map[string]any{"type": "response." + family + "." + op, "sequence_number": 3, "item_id": "delta_fixture", "output_index": 9007199254740991, position: 0, text: "safe"}
				if op == "delta" {
					e["obfuscation"] = "padding"
				}
				if family == "output_text" {
					e["logprobs"] = []any{}
				}
				piPolicyCheck(t, true, e, piPolicyTerminal())
			})
		}
	}
}

func TestPiResponsePolicyUnknownsToolsAndClosedMembers(t *testing.T) {
	for _, kind := range []string{"error", "response.failed", "response.web_search_call.completed", "response.function_call_arguments.delta", "response.future"} {
		t.Run(kind, func(t *testing.T) {
			piPolicyCheck(t, false, map[string]any{"type": kind, "sequence_number": 0}, piPolicyTerminal())
		})
	}
	for _, kind := range []string{"function_call", "custom_tool_call", "web_search_call", "computer_call", "mcp_call", "future"} {
		t.Run(kind, func(t *testing.T) {
			e := piPolicyAdded("message", "tool", 0)
			e["item"] = map[string]any{"type": kind, "id": "tool"}
			piPolicyCheck(t, false, e, piPolicyTerminal())
		})
	}
	for _, path := range []string{"event", "response", "item", "part", "usage", "usage detail"} {
		t.Run(path, func(t *testing.T) {
			r := piPolicyResponse("completed")
			e := piPolicyLifecycle("response.completed", r)
			switch path {
			case "event":
				e["future"] = true
			case "response":
				r["future"] = true
			case "item":
				item := piPolicyItem("message", "item")
				item["future"] = true
				r["output"] = []any{item}
			case "part":
				item := piPolicyItem("message", "item")
				item["content"] = []any{map[string]any{"type": "output_text", "text": "safe", "annotations": []any{map[string]any{"url": "https://fixture.invalid"}}}}
				r["output"] = []any{item}
			case "usage":
				u := piPolicyUsage(10, 5, 15, 0, 0, 0)
				u["future"] = true
				r["usage"] = u
			case "usage detail":
				u := piPolicyUsage(10, 5, 15, 0, 0, 0)
				u["input_tokens_details"].(map[string]any)["future"] = true
				r["usage"] = u
			}
			piPolicyCheck(t, false, e)
		})
	}
}

func TestPiResponsePolicyHistoryBindingsOutliveSemanticSlots(t *testing.T) {
	for _, mode := range []string{"same id index", "new id same index", "same id new index", "same id new kind", "response id changed", "terminal unindexed then indexed"} {
		t.Run(mode, func(t *testing.T) {
			a := piPolicyAdded("message", "item_a", 0)
			d := piPolicyAdded("message", "item_a", 0)
			d["type"] = "response.output_item.done"
			b := piPolicyAdded("message", "item_a", 0)
			terminal := piPolicyTerminal()
			events := []any{a, d, b, terminal}
			good := mode == "same id index" || mode == "terminal unindexed then indexed"
			switch mode {
			case "new id same index":
				b["item"] = piPolicyItem("message", "item_b")
			case "same id new index":
				b["output_index"] = 1
			case "same id new kind":
				b["item"] = piPolicyItem("reasoning", "item_a")
			case "response id changed":
				created := piPolicyResponse("in_progress")
				created["id"] = "old_response"
				events = []any{piPolicyLifecycle("response.created", created), terminal}
			case "terminal unindexed then indexed":
				created := piPolicyResponse("in_progress")
				created["output"] = []any{piPolicyItem("message", "item_a")}
				events = []any{piPolicyLifecycle("response.created", created), b, terminal}
			}
			piPolicyCheck(t, good, events...)
		})
	}
	// Safe nonnegative sequence numbers are NOT required to be monotonic.
	a := piPolicyAdded("message", "same", 0)
	a["sequence_number"] = 9
	piPolicyCheck(t, true, a, piPolicyTerminal())
	for _, bad := range []any{-1, 0.5, float64(9007199254740992), "0", nil} {
		e := piPolicyAdded("message", "bad", 0)
		e["output_index"] = bad
		piPolicyCheck(t, false, e, piPolicyTerminal())
	}
}

func piPolicyUsage(in, out, total, cache, write, reason float64) map[string]any {
	return map[string]any{"input_tokens": in, "output_tokens": out, "total_tokens": total, "input_tokens_details": map[string]any{"cached_tokens": cache, "cache_write_tokens": write}, "output_tokens_details": map[string]any{"reasoning_tokens": reason}}
}
func TestPiResponsePolicyUsageArithmeticAndBounds(t *testing.T) {
	for name, tc := range map[string]struct {
		u  map[string]any
		ok bool
	}{
		"ordinary": {piPolicyUsage(10, 5, 15, 3, 2, 4), true}, "max safe sum": {piPolicyUsage(9007199254740986, 5, 9007199254740991, 0, 0, 5), true},
		"overflow rounded sum": {piPolicyUsage(9007199254740991, 1, 9007199254740991, 0, 0, 0), false}, "wrong sum": {piPolicyUsage(10, 5, 14, 0, 0, 0), false},
		"cache exceeds input": {piPolicyUsage(10, 5, 15, 11, 0, 0), false}, "cache plus write": {piPolicyUsage(10, 5, 15, 8, 3, 0), false}, "reason exceeds output": {piPolicyUsage(10, 5, 15, 0, 0, 6), false}, "fractional": {piPolicyUsage(10.5, 5, 15.5, 0, 0, 0), false},
	} {
		t.Run(name, func(t *testing.T) {
			r := piPolicyResponse("completed")
			r["usage"] = tc.u
			piPolicyCheck(t, tc.ok, piPolicyLifecycle("response.completed", r))
		})
	}
}

func TestPiResponsePolicyEchoesAndNeutralFields(t *testing.T) {
	for _, name := range []string{"model", "store", "background", "conversation", "previous_response_id", "prompt", "max_output_tokens", "top_logprobs", "truncation", "tools", "tool_choice", "temperature", "top_p", "metadata", "instructions", "prompt_cache_key", "safety_identifier", "user", "prompt_cache_retention", "reasoning", "text", "service_tier"} {
		t.Run(name, func(t *testing.T) {
			r := piPolicyResponse("completed")
			switch name {
			case "model":
				r[name] = "other"
			case "store", "background":
				r[name] = true
			case "conversation", "previous_response_id", "prompt", "instructions", "prompt_cache_key", "safety_identifier", "user", "prompt_cache_retention":
				r[name] = "nonneutral"
			case "max_output_tokens":
				r[name] = 1025
			case "top_logprobs":
				r[name] = 1
			case "truncation", "tool_choice":
				r[name] = "auto"
			case "tools":
				r[name] = []any{map[string]any{"type": "web_search"}}
			case "temperature":
				r[name] = 3
			case "top_p":
				r[name] = -1
			case "metadata":
				r[name] = map[string]any{"x": "nonneutral"}
			case "reasoning":
				r[name] = map[string]any{"effort": "high"}
			case "text":
				r[name] = map[string]any{"format": map[string]any{"type": "text"}}
			case "service_tier":
				r[name] = "unknown"
			}
			piPolicyCheck(t, false, piPolicyLifecycle("response.completed", r))
		})
	}
	request := piPolicyObject(t, piPolicyFixtureRequest)
	request["temperature"] = 1
	request["reasoning"] = map[string]any{"effort": "low", "summary": "auto"}
	for _, mode := range []string{"same", "different", "null", "absent"} {
		t.Run(mode, func(t *testing.T) {
			r := piPolicyResponse("completed")
			r["text"] = request["text"]
			r["reasoning"] = map[string]any{"summary": "auto", "effort": "low"}
			good := mode == "same" || mode == "absent"
			if mode == "different" {
				r["temperature"] = 2
			}
			if mode == "null" {
				r["reasoning"] = nil
			}
			if mode == "absent" {
				delete(r, "reasoning")
				delete(r, "text")
			}
			q, x := piPolicyParsed(t, piPolicyJSON(t, request), piPolicyBody(t, piPolicyLifecycle("response.completed", r)))
			if err := q.checkResponsePolicy(x); (err == nil) != good {
				t.Fatal("original echo/null/absent distinction", mode, err)
			}
		})
	}
}

func TestPiResponsePolicyTerminalLastDispatchAndByteFrame(t *testing.T) {
	terminal := piPolicyJSON(t, piPolicyTerminal())
	for _, tc := range []struct {
		name, body string
		ok         bool
	}{
		{"terminal only", "data: " + terminal + "\n\n", true},
		{"comment inside final frame", "data: " + terminal + "\n\r: trailing\n\n", true},
		{"later comment frame", "data: " + terminal + "\n\n: trailing\n\n", false},
		{"later empty frame", "data: " + terminal + "\n\n\n\n", false},
		{"second dispatch same frame", "data: " + terminal + "\n\rdata: " + terminal + "\n\n", false},
		{"only comment", ": none\n\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, x := piPolicyParsed(t, piPolicyFixtureRequest, tc.body)
			if err := q.checkResponsePolicy(x); (err == nil) != tc.ok {
				t.Fatal("terminal/frame bound", err, q.frames, q.eventN)
			}
		})
	}
}

func TestPiResponseExpectedStrictWholeBodyAndNoInputPayloadCopy(t *testing.T) {
	for name, body := range map[string]string{
		"top duplicate":       strings.Replace(piPolicyFixtureRequest, `"model":`, `"model":"other","\u006dodel":`, 1),
		"skipped duplicate":   strings.Replace(piPolicyFixtureRequest, `"role":"user"`, `"role":"user","\u0072ole":"user"`, 1),
		"bad skipped grammar": strings.Replace(piPolicyFixtureRequest, `"fixture"}]`, `"fixture",}]`, 1),
		"wrong store":         strings.Replace(piPolicyFixtureRequest, `"store":false`, `"store":true`, 1),
		"wrong schema strict": strings.Replace(piPolicyFixtureRequest, `"strict":true`, `"strict":false`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			raw := []byte(body)
			l := newPiFileLedger()
			x, e := newPiResponseExpected(t.Context(), l, piPolicyLimits(), raw, sha256.Sum256(raw))
			if e == nil || x == nil || !piScanZero(x.graph) {
				t.Fatal("invalid original expectation accepted/unowned", e)
			}
			if l.EndLive(x.live) != nil || l.live != 0 {
				t.Fatal("failed expected preparation debit lost")
			}
		})
	}
	body := strings.Replace(piPolicyFixtureRequest, `"text":"fixture"`, `"text":"INPUT_PAYLOAD_NOT_COPIED"`, 1)
	raw := []byte(body)
	x, e := newPiResponseExpected(t.Context(), newPiFileLedger(), piPolicyLimits(), raw, sha256.Sum256(raw))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(x.graph.clear)
	var copied strings.Builder
	for _, c := range x.graph.text[:x.graph.textN] {
		copied.WriteRune(rune(c))
	}
	if strings.Contains(copied.String(), "INPUT_PAYLOAD_NOT_COPIED") {
		t.Fatal("expected projection copied input payload")
	}
	// Source/raw bytes stay owned by their original body owner, not graph.clear.
	x.graph.clear()
	if string(raw) != body {
		t.Fatal("expected retirement zeroed borrowed original body")
	}
}

func TestPiResponseExpectedProspectiveFunding(t *testing.T) {
	l := piPolicyLimits()
	raw := []byte(piPolicyFixtureRequest)
	r := uint64(len(raw))
	n := min(l.JSONNodes, 2*r+1)
	arena := uint64(reflect.TypeOf(piResponseScan{}).Size()) + (n+1)*uint64(reflect.TypeOf(piResponseNode{}).Size()) + 2*r
	live := uint64(reflect.TypeOf(piResponseExpected{}).Size()) + 8192 + arena + 2*r + 96*n + 8192
	for _, mode := range []string{"exact", "copy short", "live short", "nodes short", "original cap exact", "original cap short"} {
		t.Run(mode, func(t *testing.T) {
			limits := l
			ledger := &piFileLedger{maxCopy: l.ResponseCopyWorkBytes, maxLive: live, maxNodes: 4 + 3*n}
			switch mode {
			case "copy short":
				ledger.maxCopy--
			case "live short":
				ledger.maxLive--
			case "nodes short":
				ledger.maxNodes--
			case "original cap exact":
				limits.PreSendAllocationBytes = live
			case "original cap short":
				limits.PreSendAllocationBytes = live - 1
			}
			x, err := newPiResponseExpected(t.Context(), ledger, limits, raw, sha256.Sum256(raw))
			short := strings.HasSuffix(mode, "short")
			if (err != nil) != short {
				t.Fatal("expected prospective boundary", mode, err)
			}
			if short {
				if x != nil || ledger.copy != 0 || ledger.live != 0 || ledger.nodes != 0 {
					t.Fatal("unfunded expectation allocation or partial debit")
				}
				return
			}
			if x.live != live || ledger.live != live || ledger.copy != l.ResponseCopyWorkBytes || ledger.nodes != 4+3*n {
				t.Fatal("independent expected reservation", x.live, ledger)
			}
			x.graph.clear()
			if ledger.EndLive(live) != nil || ledger.live != 0 || ledger.copy != l.ResponseCopyWorkBytes || ledger.nodes != 4+3*n {
				t.Fatal("expected retirement replenished cumulative work/nodes")
			}
		})
	}
}

func TestPiResponsePolicyOriginalOwnerCombinedLedgerDenialBeforeIssue(t *testing.T) {
	for _, mode := range []string{"copy", "nodes"} {
		t.Run(mode, func(t *testing.T) {
			s, secrets, _, p, ledger, request := piAuthOwnerFixture(t, 64)
			if err := p.PrepareRequestAuthenticationV1(request); err != nil {
				t.Fatal(err)
			}
			l := request.Profile.Limits
			if err := p.PrepareHTTPReceiveV1(l.ResponseHeaderBytes, l.ResponseBodyBytes); err != nil {
				t.Fatal(err)
			}
			copyBefore, liveBefore, nodesBefore := ledger.copy, ledger.live, ledger.nodes
			qNodes := 7 + 4*min(l.JSONNodes, 2*l.ResponseBodyBytes+1) + min(l.SSEEvents, l.ResponseBodyBytes/2+1)
			eNodes := 4 + 3*min(l.JSONNodes, 2*uint64(len(request.Body))+1)
			if mode == "copy" {
				ledger.maxCopy = copyBefore + 2*l.ResponseCopyWorkBytes - 1
			} else {
				ledger.maxNodes = nodesBefore + qNodes + eNodes - 1
			}
			if err := p.prepareResponseScan(); err == nil || p.state.responseScan == nil || p.state.expected != nil || !p.state.stopped.Load() {
				t.Fatal("second stage substituted fresh ledger or lost first funded arena", err)
			}
			q := p.state.responseScan
			if ledger.copy != copyBefore+l.ResponseCopyWorkBytes || ledger.live != liveBefore+q.live || ledger.nodes != nodesBefore+qNodes || len(s.handles) != 0 || secrets.readCalls != 0 {
				t.Fatal("partial preparation lost debit or reached credential Issue")
			}
			if err := p.Close(); err != nil || ledger.live != 0 || !piScanZero(q) || ledger.copy != copyBefore+l.ResponseCopyWorkBytes {
				t.Fatal("failed combined preparation retirement", err)
			}
		})
	}
}

func TestPiResponsePolicyIndependentHistoryWorkAndNodeBoundaries(t *testing.T) {
	for _, mode := range []string{"first indexed", "repeat same canonical"} {
		for _, short := range []bool{false, true} {
			r := piPolicyResponse("completed")
			r["output"] = []any{piPolicyItem("message", "id_a")}
			q, _ := piPolicyParsed(t, piPolicyFixtureRequest, piPolicyBody(t, piPolicyLifecycle("response.completed", r)))
			response := q.field(q.events[0].root, "response")
			output := q.field(response, "output")
			id := q.field(q.nodes[output].child, "id")
			work := uint64(96) // first ID retain32 + one record read32 + index retain32
			if mode == "repeat same canonical" {
				if !q.observe(id, 1, 0, true) {
					t.Fatal("initial synthetic history")
				}
				work = 64 + 4*uint64(len("id_a"))
			}
			before := q.work
			q.limits.ResponseCopyWorkBytes = before + work
			if short {
				q.limits.ResponseCopyWorkBytes--
			}
			ok := q.observe(id, 1, 0, true)
			if ok == short || !short && q.work != before+work {
				t.Fatal("independent history work boundary", mode, short, q.work-before, work)
			}
			q.clear()
			if !piScanZero(q) {
				t.Fatal("history retirement retained identity")
			}
		}
	}
	for _, short := range []bool{false, true} {
		q, x := piPolicyParsed(t, piPolicyFixtureRequest, piPolicyBody(t, piPolicyAdded("message", "id_a", 0), piPolicyTerminal()))
		q.limits.JSONNodes = uint64(q.nodeN) + 3 // canonical ID, index binding, response ID
		if short {
			q.limits.JSONNodes--
		}
		err := q.checkResponsePolicy(x)
		if (err != nil) != short {
			t.Fatal("cumulative grammar+history JSON node boundary", short, err)
		}
	}
}

func TestPiResponsePolicyReentryCannotEraseActiveOriginal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, x := piPolicyParsed(t, piPolicyFixtureRequest, piPolicyBody(t, piPolicyTerminal()))
		ctx := &piSemanticPauseContext{Context: q.ctx, entered: make(chan struct{}), release: make(chan struct{})}
		q.ctx = ctx
		release := sync.OnceFunc(func() { close(ctx.release) })
		done := make(chan error, 1)
		joined := false
		defer func() {
			release()
			if !joined {
				<-done
			}
		}()
		go func() { done <- q.checkResponsePolicy(x) }()
		<-ctx.entered
		synctest.Wait()
		if err := q.checkResponsePolicy(x); err != ErrConsumed || q.erased.Load() || x.graph.erased.Load() {
			t.Fatal("losing policy reentry erased winner", err)
		}
		release()
		err := <-done
		joined = true
		if err != nil {
			t.Fatal("active original policy corrupted", err)
		}
	})
}

type piPolicyCancelContext struct {
	context.Context
	q      *piResponseScan
	cancel context.CancelFunc
}

func (c *piPolicyCancelContext) Err() error {
	if c.q.identityN != 0 {
		c.cancel()
	}
	return c.Context.Err()
}
func TestPiResponsePolicyCancellationDuringHistoricalBinding(t *testing.T) {
	q, x := piPolicyParsed(t, piPolicyFixtureRequest, piPolicyBody(t, piPolicyAdded("message", "id_a", 0), piPolicyTerminal()))
	ctx, cancel := context.WithCancel(q.ctx)
	defer cancel()
	q.ctx = &piPolicyCancelContext{ctx, q, cancel}
	if err := q.checkResponsePolicy(x); err == nil || ctx.Err() == nil || !piScanZero(q) || !piScanZero(x.graph) {
		t.Fatal("historical binding ignored original cancellation or retained aliases", err)
	}
}

func TestPiResponsePolicyThenSemanticGuardStillRequired(t *testing.T) {
	added := piPolicyAdded("message", "same", 0)
	a := map[string]any{"type": "response.output_text.delta", "sequence_number": 0, "item_id": "same", "output_index": 0, "content_index": 0, "delta": "fixture-"}
	b := map[string]any{"type": "response.refusal.delta", "sequence_number": 0, "item_id": "same", "output_index": 0, "content_index": 0, "delta": "secret"}
	q, x := piPolicyParsed(t, piPolicyFixtureRequest, piPolicyBody(t, added, a, b, piPolicyTerminal()))
	if err := q.checkResponsePolicy(x); err != nil {
		t.Fatal("closed-shape fixture", err)
	}
	if err := q.scanPiSemantics(); err != ErrDeliveryMismatch || !piScanZero(q) {
		t.Fatal("closed policy bypassed mandatory cross-event secret guard", err)
	}
}

func TestPiResponseExpectedOriginalMaximumBodyWithoutRetainedInput(t *testing.T) {
	l := piPolicyLimits()
	l.ResponseCopyWorkBytes = 16 << 20 // test-owned profile funds this MAX cohort
	const maximum = 1 << 20            // same original PreparePiFileUse transport ceiling
	body := strings.Replace(piPolicyFixtureRequest, `"text":"fixture"`, `"text":"`+strings.Repeat("x", maximum-len(piPolicyFixtureRequest)+len("fixture"))+`"`, 1)
	if len(body) != maximum {
		t.Fatal("fixture did not reach original MAX", len(body))
	}
	for _, extra := range []bool{false, true} {
		raw := []byte(body)
		if extra {
			raw = append(raw, ' ')
		}
		ledger := newPiFileLedger()
		x, err := newPiResponseExpected(t.Context(), ledger, l, raw, sha256.Sum256(raw))
		if extra {
			if err == nil || x != nil || ledger.copy != 0 || ledger.live != 0 {
				t.Fatal("first-over source ceiling allocated or accepted", err)
			}
			continue
		}
		if err != nil {
			t.Fatal("original MAX expected body was narrowed", err)
		}
		if x.graph.textN > 512 {
			t.Fatal("MAX input payload was retained", x.graph.textN)
		}
		x.graph.clear()
		if ledger.EndLive(x.live) != nil || ledger.live != 0 {
			t.Fatal("MAX original expectation retirement")
		}
	}
}
