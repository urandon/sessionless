package providercredential

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
)

// Source-pinned vectors: Pi6c4f3602 openai-responses-shared.ts createSlot
// (463-482), getOrCreateSlot(524-526), append branches(602-645), done(680-707).
// All strings/credentials are synthetic. These tests are not real SDK proof.
func piSemanticAdd(kind string, index int) string {
	return fmt.Sprintf(`{"type":"response.output_item.added","output_index":%d,"item":{"type":%q}}`, index, kind)
}
func piSemanticDelta(kind, text string, index int) string {
	return fmt.Sprintf(`{"type":"response.%s.delta","output_index":%d,"delta":%q}`, kind, index, text)
}
func piSemanticDone(kind string, index int, contents string) string {
	return fmt.Sprintf(`{"type":"response.output_item.done","output_index":%d,"item":{"type":%q,%s}}`, index, kind, contents)
}
func piSemanticParsed(t *testing.T, key string, events ...string) *piResponseScan {
	t.Helper()
	var body strings.Builder
	for _, event := range events {
		body.WriteString("data: ")
		body.WriteString(event)
		body.WriteString("\n\n")
	}
	q := piScanner(t, piScanLimits())
	if err := q.scan(t.Context(), piScanView(t, body.String()), []byte(key)); err != nil {
		t.Fatal("raw/scalar fixture must pass BEFORE assembly guard", err)
	}
	return q
}

func TestPiResponseSemanticSplitCredentialDenials(t *testing.T) {
	for name, events := range map[string][]string{
		"text refusal share slot":                   {piSemanticAdd("message", 0), piSemanticDelta("output_text", "fixture-", 0), piSemanticDelta("refusal", "secret", 0)},
		"summary reasoning share slot":              {piSemanticAdd("reasoning", 0), piSemanticDelta("reasoning_summary_text", "fixture-", 0), piSemanticDelta("reasoning_text", "secret", 0)},
		"message done join":                         {piSemanticDone("message", 0, `"content":[{"type":"output_text","text":"fixture-"},{"type":"refusal","refusal":"secret"}]`)},
		"intermediate leak before safe replacement": {piSemanticAdd("message", 0), piSemanticDelta("output_text", "fixture-", 0), piSemanticDelta("output_text", "secret", 0), piSemanticDone("message", 0, `"content":[{"type":"output_text","text":"safe"}]`)},
		"fallback old thinking leaks first":         {piSemanticAdd("reasoning", 0), piSemanticDelta("reasoning_summary_text", "fixture-", 0), piSemanticDelta("reasoning_text", "secret", 0), piSemanticDone("reasoning", 0, `"summary":[],"content":[]`)},
	} {
		t.Run(name, func(t *testing.T) {
			q := piSemanticParsed(t, "fixture-secret", events...)
			if err := q.scanPiSemantics(); !errors.Is(err, ErrDeliveryMismatch) || !piScanZero(q) {
				t.Fatal("split credential accepted or failure retained graph/key", err)
			}
			if err := q.scanPiSemantics(); !errors.Is(err, ErrConsumed) {
				t.Fatal("failed assembly guard replenished", err)
			}
		})
	}
}

func TestPiResponseSemanticActualSlotBoundaries(t *testing.T) {
	for name, events := range map[string][]string{
		"no slot delta ignored":                       {piSemanticDelta("output_text", "fixture-", 0), piSemanticAdd("message", 0), piSemanticDelta("refusal", "secret", 0)},
		"different output slots":                      {piSemanticAdd("message", 0), piSemanticAdd("message", 1), piSemanticDelta("output_text", "fixture-", 0), piSemanticDelta("output_text", "secret", 1)},
		"added replaces mapping not block":            {piSemanticAdd("message", 0), piSemanticDelta("output_text", "fixture-", 0), piSemanticAdd("message", 0), piSemanticDelta("refusal", "secret", 0)},
		"done then reused index new block":            {piSemanticDone("message", 0, `"content":[{"type":"output_text","text":"fixture-"}]`), piSemanticAdd("message", 0), piSemanticDelta("refusal", "secret", 0)},
		"standalone done does not append":             {piSemanticAdd("message", 0), piSemanticDelta("output_text", "fixture-", 0), `{"type":"response.output_text.done","output_index":0,"text":"ignored"}`, piSemanticDelta("refusal", "ordinary", 0)},
		"summary part inserts two LF":                 {piSemanticAdd("reasoning", 0), piSemanticDelta("reasoning_text", "fixture-", 0), `{"type":"response.reasoning_summary_part.done","output_index":0}`, piSemanticDelta("reasoning_text", "secret", 0)},
		"reasoning done separator breaks ASCII token": {piSemanticDone("reasoning", 0, `"summary":[{"text":"fixture-"},{"text":"secret"}]`)},
		"empty summary fallback content":              {piSemanticAdd("reasoning", 0), piSemanticDelta("reasoning_text", "fixture-", 0), piSemanticDone("reasoning", 0, `"summary":[],"content":[{"text":"safe"}]`), piSemanticDelta("reasoning_text", "secret", 0)},
		"wrong type delta ignored":                    {piSemanticAdd("reasoning", 0), piSemanticDelta("output_text", "fixture-", 0), piSemanticDelta("reasoning_text", "secret", 0)},
		"replacement resets matcher":                  {piSemanticAdd("message", 0), piSemanticDelta("output_text", "fixture-", 0), piSemanticDone("message", 0, `"content":[{"type":"refusal","refusal":"secret"}]`)},
		"max safe integer slot":                       {`{"type":"response.output_item.added","output_index":9007199254740991,"item":{"type":"message"}}`, `{"type":"response.output_text.delta","output_index":9007199254740991,"delta":"safe"}`},
	} {
		t.Run(name, func(t *testing.T) {
			q := piSemanticParsed(t, "fixture-secret", events...)
			if err := q.scanPiSemantics(); err != nil {
				t.Fatal("invented cross-block join/delta or lost pinned replacement", err)
			}
			if q.erased.Load() || len(q.key) == 0 || q.ctx == nil {
				t.Fatal("guard released original file/key/context before owner retirement")
			}
			if err := q.scanPiSemantics(); err != ErrConsumed || q.erased.Load() {
				t.Fatal("late guard cleared or replenished successful original", err)
			}
			q.clear()
			if !piScanZero(q) {
				t.Fatal("slot metadata retained at retirement")
			}
		})
	}
}

func TestPiResponseSemanticIndexNotItemOrContentIdentity(t *testing.T) {
	q := piSemanticParsed(t, "fixture-secret", piSemanticAdd("message", 0),
		`{"type":"response.output_text.delta","output_index":0,"content_index":1,"item_id":"old","delta":"fixture-"}`,
		`{"type":"response.refusal.delta","output_index":0,"content_index":9,"item_id":"new","delta":"secret"}`)
	// Closed policy will separately reject the changed ID. Semantic lookup must
	// STILL mirror Pi: content_index and item_id do not separate its text slot.
	if err := q.scanPiSemantics(); err != ErrDeliveryMismatch {
		t.Fatal("grouped by scalar item/content identity rather than actual Pi slot", err)
	}
}

func TestPiResponseSemanticWrongTypeDoneKeepsExistingSlot(t *testing.T) {
	q := piSemanticParsed(t, "fixture-secret", piSemanticAdd("message", 0), piSemanticDelta("output_text", "fixture-", 0),
		piSemanticDone("reasoning", 0, `"summary":[{"text":"safe"}]`), piSemanticDelta("refusal", "secret", 0))
	if err := q.scanPiSemantics(); err != ErrDeliveryMismatch {
		t.Fatal("wrong-type done replaced/deleted existing Pi slot", err)
	}
}

func TestPiResponseSemanticRetainedStatesAndReasoningFallback(t *testing.T) {
	q := piSemanticParsed(t, "fixture-secret", piSemanticAdd("reasoning", 0), piSemanticDelta("reasoning_text", "fixture-", 0),
		piSemanticDone("reasoning", 0, `"summary":[],"content":[]`), piSemanticAdd("message", 0), piSemanticDelta("output_text", "secret", 0))
	if err := q.scanPiSemantics(); err != nil {
		t.Fatal(err)
	}
	if first, last := q.events[0].slot, q.events[3].slot; first.kind != 'r' || first.active || first.match != uint32(len("fixture-")) || last.kind != 't' || !last.active || last.match != 0 {
		t.Fatal("old empty-fallback block lost or joined reused index", first, last)
	}
}

func TestPiResponseSemanticUTF16AndEscapedSplit(t *testing.T) {
	q := piSemanticParsed(t, "fixture-secret", piSemanticAdd("message", 0),
		`{"type":"response.output_text.delta","output_index":0,"delta":"fixture-"}`,
		`{"type":"response.refusal.delta","output_index":0,"delta":"\u0073ecret"}`)
	if err := q.scanPiSemantics(); err != ErrDeliveryMismatch {
		t.Fatal("escaped split bypass", err)
	}
	q = piSemanticParsed(t, "fixture-secret", piSemanticAdd("message", 0),
		`{"type":"response.output_text.delta","output_index":0,"delta":"fixture-\ud800"}`,
		piSemanticDelta("refusal", "secret", 0))
	if err := q.scanPiSemantics(); err != nil {
		t.Fatal("lone surrogate incorrectly elided", err)
	}
}

func TestPiResponseSemanticInvalidDataAndWorkCancellation(t *testing.T) {
	for _, index := range []string{"-1", "0.5", "9007199254740992", `"0"`, "null"} {
		q := piSemanticParsed(t, "fixture-secret", fmt.Sprintf(`{"type":"response.output_item.added","output_index":%s,"item":{"type":"message"}}`, index))
		if err := q.scanPiSemantics(); err == nil || !piScanZero(q) {
			t.Fatal("invalid active index accepted/retained", index, err)
		}
	}
	q := piSemanticParsed(t, "fixture-secret", piSemanticAdd("message", 0), piSemanticDelta("output_text", "ordinary", 0))
	q.limits.ResponseCopyWorkBytes = q.work
	if err := q.scanPiSemantics(); err == nil || !piScanZero(q) {
		t.Fatal("fresh work budget granted after DATA parser", err)
	}
	q = piSemanticParsed(t, "fixture-secret", piSemanticAdd("message", 0))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	q.ctx = ctx
	if err := q.scanPiSemantics(); err == nil || !piScanZero(q) {
		t.Fatal("original cancelled context bypassed/retained", err)
	}
	q = piScanner(t, piScanLimits())
	if err := q.scanPiSemantics(); err != ErrConsumed || q.erased.Load() {
		t.Fatal("guard accepted unparsed graph or destroyed unused preparation", err)
	}
}

func TestPiResponseSemanticIndependentAddedWorkOracle(t *testing.T) {
	// One added message, no prior slots and no payload walk. Count from source
	// passes, not by measuring candidate success: start1+fixed256, member walk
	// 16/child plus3*member-name bytes, event/item type comparisons3*ASCII bytes.
	const member = 16
	const work = 1 + 256 + member + 3*len("type") + 3*len("response.output_item.added") +
		2*(member+3*len("output_index")) + 3*(member+3*len("item")) + member + 3*len("type") +
		3*len("reasoning") + 3*len("message")
	for _, short := range []bool{false, true} {
		q := piSemanticParsed(t, "fixture-secret", piSemanticAdd("message", 0))
		before := q.work
		q.limits.ResponseCopyWorkBytes = before + uint64(work)
		if short {
			q.limits.ResponseCopyWorkBytes--
		}
		err := q.scanPiSemantics()
		if (err != nil) != short || !short && q.work != before+uint64(work) {
			t.Fatal("independent semantic work boundary", short, err, q.work-before, work)
		}
		if short && !piScanZero(q) {
			t.Fatal("one-short work retained slot/key/graph")
		}
	}
}

func TestPiResponseSemanticIndependentLookupAndJoinWorkOracle(t *testing.T) {
	// Independent fixed-shape oracle. Each source member search visits the
	// stated number of children and compares the ASCII member name at3B/byte.
	// The synthetic '~' key never needs a KMP fallback for these payloads.
	field := func(name string, visits int) uint64 { return uint64(visits * (16 + 3*len(name))) }
	const addedType = "response.output_item.added"
	const doneType = "response.output_item.done"
	const deltaType = "response.output_text.delta"
	added := uint64(256+3*len(addedType)+3*len("reasoning")+3*len("message")) +
		2*field("type", 1) + field("output_index", 2) + field("item", 3)
	delta := uint64(256+3*(len(addedType)+len(doneType)+len(deltaType))+32+4*len("xyz")) +
		field("type", 1) + field("output_index", 2) + field("delta", 3)
	// Done reads the existing active record(32), then joins TWO parts, each
	// charging32 before part/type/payload lookup. It replaces, not appends.
	done := uint64(256+3*(len(addedType)+len(doneType))+32+3*len("reasoning")+3*len("message")) +
		2*field("type", 1) + field("output_index", 2) + field("item", 3) + field("content", 2) +
		uint64(2*(32+3*len("output_text"))+4*len("xyzxyz")) + 2*field("type", 1) + field("text", 2) + field("refusal", 2)
	for _, mode := range []string{"prior lookup", "prior lookup and multipart replacement"} {
		for _, short := range []bool{false, true} {
			event, expected := piSemanticDelta("output_text", "xyz", 0), 1+added+delta
			if mode == "prior lookup and multipart replacement" {
				event = piSemanticDone("message", 0, `"content":[{"type":"output_text","text":"xyz"},{"type":"refusal","refusal":"xyz"}]`)
				expected = 1 + added + done
			}
			q := piSemanticParsed(t, "~", piSemanticAdd("message", 0), event)
			before := q.work
			q.limits.ResponseCopyWorkBytes = before + expected
			if short {
				q.limits.ResponseCopyWorkBytes--
			}
			err := q.scanPiSemantics()
			if (err != nil) != short || !short && q.work-before != expected {
				t.Fatal("independent lookup/join work boundary", mode, short, err, q.work-before, expected)
			}
			if short && !piScanZero(q) {
				t.Fatal("one-short lookup/join retained original aliases")
			}
		}
	}
}

type piSemanticPauseContext struct {
	context.Context
	entered, release chan struct{}
	once             sync.Once
}

func (c *piSemanticPauseContext) Err() error {
	c.once.Do(func() { close(c.entered); <-c.release })
	return c.Context.Err()
}

func TestPiResponseSemanticReentryCannotClearWinner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := piSemanticParsed(t, "fixture-secret", piSemanticAdd("message", 0), piSemanticDelta("output_text", "safe", 0))
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
		go func() { done <- q.scanPiSemantics() }()
		<-ctx.entered
		synctest.Wait()
		if err := q.scanPiSemantics(); err != ErrConsumed || q.erased.Load() {
			t.Fatal("reentry destroyed active semantic scan", err)
		}
		release()
		err := <-done
		joined = true
		if err != nil {
			t.Fatal("winner failed after losing reentry", err)
		}
	})
}

type piSemanticCancelContext struct {
	context.Context
	q      *piResponseScan
	cancel context.CancelFunc
}

func (c *piSemanticCancelContext) Err() error {
	if c.q.events[0].slot.match != 0 {
		c.cancel() // causal: first append reached the retained slot
	}
	return c.Context.Err()
}

func TestPiResponseSemanticCancellationBetweenActualAppends(t *testing.T) {
	q := piSemanticParsed(t, "fixture-secret", piSemanticAdd("message", 0), piSemanticDelta("output_text", "fixture-", 0), piSemanticDelta("refusal", "ordinary", 0))
	ctx, cancel := context.WithCancel(q.ctx)
	defer cancel()
	q.ctx = &piSemanticCancelContext{Context: ctx, q: q, cancel: cancel}
	if err := q.scanPiSemantics(); err == nil || !piScanZero(q) || ctx.Err() == nil {
		t.Fatal("semantic scan ignored original cancellation between appends", err)
	}
}
