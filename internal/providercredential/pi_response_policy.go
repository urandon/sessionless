package providercredential

import "math"

// Private history, separate from Pi's active semantic slots. Detached blocks
// never erase these identity/index associations. String payload is referenced
// by its first canonical parsed node, not cloned or allocated by numeric index.
type piResponseIdentity struct {
	id      int32
	kind    byte
	index   float64
	indexed bool
}

var piResponseRequired = [...]string{"id", "created_at", "error", "incomplete_details", "instructions", "metadata", "model", "object", "output", "parallel_tool_calls", "temperature", "tool_choice", "tools", "top_p", "status"}
var piResponseFields = [...]string{"id", "created_at", "error", "incomplete_details", "instructions", "metadata", "model", "object", "output", "parallel_tool_calls", "temperature", "tool_choice", "tools", "top_p", "status", "output_text", "store", "background", "completed_at", "conversation", "max_output_tokens", "previous_response_id", "prompt", "prompt_cache_key", "prompt_cache_retention", "reasoning", "safety_identifier", "service_tier", "text", "top_logprobs", "truncation", "usage", "user"}

func (q *piResponseScan) kind(i int32, k byte) bool { return i != 0 && q.nodes[i].kind == k }
func (q *piResponseScan) str(i int32) bool          { return q.kind(i, 's') }
func (q *piResponseScan) id(i int32) bool           { return q.str(i) && q.nodes[i].to > q.nodes[i].from }
func (q *piResponseScan) boolean(i int32) bool      { return q.kind(i, 't') || q.kind(i, 'f') }
func (q *piResponseScan) empty(i int32) bool        { return q.kind(i, 'a') && q.nodes[i].child == 0 }
func (q *piResponseScan) emptyObject(i int32) bool  { return q.kind(i, 'o') && q.nodes[i].child == 0 }
func (q *piResponseScan) count(i int32) bool {
	return q.kind(i, 'd') && q.nodes[i].number >= 0 && q.nodes[i].number <= 9007199254740991 && math.Trunc(q.nodes[i].number) == q.nodes[i].number
}
func (q *piResponseScan) positive(i int32) bool { return q.count(i) && q.nodes[i].number > 0 }
func (q *piResponseScan) oneOf(i int32, values ...string) bool {
	for _, s := range values {
		if q.equalsBytes(i, []byte(s)) {
			return true
		}
	}
	return false
}
func (q *piResponseScan) optional(root int32, name string, check func(int32) bool) bool {
	i := q.field(root, name)
	return i == 0 || check(i)
}
func (q *piResponseScan) closed(root int32, required, allowed []string) bool {
	if !q.kind(root, 'o') || len(allowed) > 63 {
		return false
	}
	var seen uint64
	for i := q.nodes[root].child; i != 0; i = q.nodes[i].next {
		if !q.spend(32) {
			return false
		}
		found := false
		for at, name := range allowed {
			if q.equalsBytes(q.nodes[i].key, []byte(name)) {
				seen |= 1 << at
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for _, name := range required {
		present := false
		for at, s := range allowed {
			if name == s && seen&(1<<at) != 0 {
				present = true
				break
			}
		}
		if !present {
			return false
		}
	}
	return !q.denied
}
func (q *piResponseScan) status(i int32) bool {
	return q.oneOf(i, "in_progress", "completed", "incomplete")
}
func (q *piResponseScan) metadata(i int32) bool {
	if q.kind(i, 'n') {
		return true
	}
	if !q.kind(i, 'o') {
		return false
	}
	count := 0
	for v := q.nodes[i].child; v != 0; v = q.nodes[v].next {
		if !q.spend(32) {
			return false
		}
		count++
		key := q.nodes[q.nodes[v].key]
		if count > 16 || key.to-key.from > 64 || !q.str(v) || q.nodes[v].to-q.nodes[v].from > 512 {
			return false
		}
	}
	return true
}
func (q *piResponseScan) reasoning(i int32) bool {
	return q.kind(i, 'n') || q.closed(i, nil, []string{"effort", "summary", "generate_summary"}) &&
		q.optional(i, "effort", func(v int32) bool {
			return q.kind(v, 'n') || q.oneOf(v, "none", "minimal", "low", "medium", "high", "xhigh")
		}) &&
		q.optional(i, "summary", func(v int32) bool { return q.kind(v, 'n') || q.oneOf(v, "auto", "concise", "detailed") }) &&
		q.optional(i, "generate_summary", func(v int32) bool { return q.kind(v, 'n') || q.oneOf(v, "auto", "concise", "detailed") })
}
func (q *piResponseScan) textConfig(i int32) bool {
	if !q.closed(i, nil, []string{"format", "verbosity"}) || !q.optional(i, "verbosity", func(v int32) bool { return q.kind(v, 'n') || q.oneOf(v, "low", "medium", "high") }) {
		return false
	}
	f := q.field(i, "format")
	if f == 0 {
		return true
	}
	if q.closed(f, []string{"type"}, []string{"type"}) && q.oneOf(q.field(f, "type"), "text", "json_object") {
		return true
	}
	if !q.closed(f, []string{"type", "name", "schema"}, []string{"type", "name", "schema", "description", "strict"}) || !q.oneOf(q.field(f, "type"), "json_schema") || !q.kind(q.field(f, "schema"), 'o') ||
		!q.optional(f, "description", q.str) || !q.optional(f, "strict", func(v int32) bool { return q.kind(v, 'n') || q.boolean(v) }) {
		return false
	}
	name := q.field(f, "name")
	if !q.str(name) {
		return false
	}
	n := q.nodes[name]
	if n.to == n.from || n.to-n.from > 64 || !q.spend(3*uint64(n.to-n.from)) {
		return false
	}
	for _, c := range q.text[n.from:n.to] {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// The fixed native/source request is narrower than the generic transport's
// optional echo fields. Never invent missing fields or accept a caller echo.
func (q *piResponseScan) expectedContract(root int32) bool {
	if !q.closed(root, []string{"model", "input", "stream", "store", "max_output_tokens", "tool_choice", "truncation", "text"}, piOriginalRequestFields[:]) ||
		!q.id(q.field(root, "model")) || !q.kind(q.field(root, "input"), 'a') || !q.kind(q.field(root, "stream"), 't') || !q.kind(q.field(root, "store"), 'f') || !q.positive(q.field(root, "max_output_tokens")) ||
		!q.oneOf(q.field(root, "truncation"), "disabled") || !q.oneOf(q.field(root, "tool_choice"), "none") ||
		!q.optional(root, "temperature", func(v int32) bool {
			return q.kind(v, 'n') || q.kind(v, 'd') && q.nodes[v].number >= 0 && q.nodes[v].number <= 2
		}) || !q.optional(root, "reasoning", q.reasoning) {
		return false
	}
	t := q.field(root, "text")
	f := q.field(t, "format")
	if !q.textConfig(t) || !q.oneOf(q.field(f, "type"), "json_schema") || !q.kind(q.field(f, "strict"), 't') {
		return false
	}
	if !q.optional(root, "include", func(v int32) bool {
		if !q.kind(v, 'a') {
			return false
		}
		c := q.nodes[v].child
		return c != 0 && q.nodes[c].next == 0 && q.oneOf(c, "reasoning.encrypted_content")
	}) || !q.optional(root, "prompt_cache_options", func(v int32) bool {
		return q.closed(v, []string{"mode"}, []string{"mode"}) && q.oneOf(q.field(v, "mode"), "explicit")
	}) {
		return false
	}
	return !q.denied
}

func (q *piResponseScan) sameText(a *piResponseScan, x int32, b *piResponseScan, y int32) bool {
	if x == 0 || y == 0 || a.nodes[x].kind != 's' || b.nodes[y].kind != 's' {
		return false
	}
	l, r := a.nodes[x], b.nodes[y]
	if l.to-l.from != r.to-r.from {
		return false
	}
	if !q.spend(4 * uint64(l.to-l.from)) {
		return false
	}
	for i, c := range a.text[l.from:l.to] {
		if b.text[r.from+uint32(i)] != c {
			return false
		}
	}
	return true
}
func (q *piResponseScan) equalTree(a *piResponseScan, x int32, b *piResponseScan, y int32) bool {
	if !q.spend(32) || x == 0 || y == 0 || a.nodes[x].kind != b.nodes[y].kind {
		return false
	}
	switch a.nodes[x].kind {
	case 's':
		return q.sameText(a, x, b, y)
	case 'd':
		return a.nodes[x].number == b.nodes[y].number
	case 'n', 't', 'f':
		return true
	case 'a':
		i, j := a.nodes[x].child, b.nodes[y].child
		for i != 0 && j != 0 {
			if !q.equalTree(a, i, b, j) {
				return false
			}
			i, j = a.nodes[i].next, b.nodes[j].next
		}
		return i == 0 && j == 0
	case 'o':
		left, right := 0, 0
		for i := a.nodes[x].child; i != 0; i = a.nodes[i].next {
			left++
			match := int32(0)
			for j := b.nodes[y].child; j != 0; j = b.nodes[j].next {
				if !q.spend(32) {
					return false
				}
				if q.sameText(a, a.nodes[i].key, b, b.nodes[j].key) {
					match = j
					break
				}
			}
			if match == 0 || !q.equalTree(a, i, b, match) {
				return false
			}
		}
		for j := b.nodes[y].child; j != 0; j = b.nodes[j].next {
			if !q.spend(16) {
				return false
			}
			right++
		}
		return left == right
	}
	return false
}
func (q *piResponseScan) expectedField(x *piResponseExpected, name string) int32 {
	g := x.graph
	for i := g.nodes[x.root].child; i != 0; i = g.nodes[i].next {
		if !q.spend(32) {
			return 0
		}
		k := g.nodes[g.nodes[i].key]
		if !q.spend(3 * uint64(len(name))) {
			return 0
		}
		if k.to-k.from != uint32(len(name)) {
			continue
		}
		same := true
		for at, c := range []byte(name) {
			if g.text[k.from+uint32(at)] != uint16(c) {
				same = false
				break
			}
		}
		if same {
			return i
		}
	}
	return 0
}
func (q *piResponseScan) echo(x *piResponseExpected, i int32, name string, neutral func(int32) bool) bool {
	e := q.expectedField(x, name)
	if e == 0 {
		return neutral(i)
	}
	return q.equalTree(q, i, x.graph, e)
}
func (q *piResponseScan) retainNode() bool {
	if uint64(q.nodeN)+uint64(q.retainedN) >= q.limits.JSONNodes || !q.spend(32) {
		return false
	}
	q.retainedN++
	return true
}
func (q *piResponseScan) observe(id int32, kind byte, index float64, indexed bool) bool {
	if !q.id(id) || kind != 1 && kind != 2 {
		return false
	}
	old := -1
	for at := uint32(0); at < q.identityN; at++ {
		if !q.spend(32) {
			return false
		}
		r := q.identities[at]
		if q.sameText(q, r.id, q, id) {
			old = int(at)
			if r.kind != kind || indexed && r.indexed && r.index != index {
				return false
			}
			break
		}
	}
	if old < 0 {
		if int(q.identityN) == len(q.identities) || !q.retainNode() {
			return false
		}
		old = int(q.identityN)
		q.identityN++
		q.identities[old] = piResponseIdentity{id: id, kind: kind}
	}
	if indexed {
		for at := uint32(0); at < q.identityN; at++ {
			if !q.spend(32) {
				return false
			}
			r := q.identities[at]
			if r.indexed && r.index == index && int(at) != old {
				return false
			}
		}
		if !q.identities[old].indexed {
			if !q.retainNode() {
				return false
			}
			q.identities[old].indexed = true
			q.identities[old].index = index
		}
	}
	return !q.denied
}

func (q *piResponseScan) part(i int32, kinds ...string) bool {
	t := q.field(i, "type")
	if !q.oneOf(t, kinds...) {
		return false
	}
	if q.oneOf(t, "output_text") {
		return q.closed(i, []string{"type", "text"}, []string{"type", "text", "annotations", "logprobs"}) && q.str(q.field(i, "text")) && q.optional(i, "annotations", q.empty) && q.optional(i, "logprobs", q.empty)
	}
	if q.oneOf(t, "refusal") {
		return q.closed(i, []string{"type", "refusal"}, []string{"type", "refusal"}) && q.str(q.field(i, "refusal"))
	}
	return q.closed(i, []string{"type", "text"}, []string{"type", "text"}) && q.str(q.field(i, "text"))
}
func (q *piResponseScan) parts(i int32, kinds ...string) bool {
	if !q.kind(i, 'a') {
		return false
	}
	for c := q.nodes[i].child; c != 0; c = q.nodes[c].next {
		if !q.spend(32) || !q.part(c, kinds...) {
			return false
		}
	}
	return true
}
func (q *piResponseScan) item(i int32, index float64, indexed bool) bool {
	t := q.field(i, "type")
	kind := byte(0)
	if q.oneOf(t, "message") {
		kind = 1
		if !q.closed(i, []string{"type", "id", "content", "role", "status"}, []string{"type", "id", "content", "role", "status", "phase"}) || !q.oneOf(q.field(i, "role"), "assistant") || !q.status(q.field(i, "status")) || !q.parts(q.field(i, "content"), "output_text", "refusal") || !q.optional(i, "phase", func(v int32) bool { return q.kind(v, 'n') || q.oneOf(v, "commentary", "final_answer") }) {
			return false
		}
	} else if q.oneOf(t, "reasoning") {
		kind = 2
		if !q.closed(i, []string{"type", "id", "summary"}, []string{"type", "id", "summary", "content", "encrypted_content", "status"}) || !q.parts(q.field(i, "summary"), "summary_text") || !q.optional(i, "content", func(v int32) bool { return q.parts(v, "reasoning_text") }) || !q.optional(i, "encrypted_content", func(v int32) bool { return q.kind(v, 'n') || q.str(v) }) || !q.optional(i, "status", q.status) {
			return false
		}
	} else {
		return false
	}
	return q.observe(q.field(i, "id"), kind, index, indexed)
}
func (q *piResponseScan) usage(i int32) bool {
	if q.kind(i, 'n') {
		return true
	}
	if !q.closed(i, []string{"input_tokens", "output_tokens", "total_tokens", "input_tokens_details", "output_tokens_details"}, []string{"input_tokens", "output_tokens", "total_tokens", "input_tokens_details", "output_tokens_details"}) {
		return false
	}
	in, out, total := q.field(i, "input_tokens"), q.field(i, "output_tokens"), q.field(i, "total_tokens")
	if !q.count(in) || !q.count(out) || !q.count(total) {
		return false
	}
	a, b, c := q.nodes[in].number, q.nodes[out].number, q.nodes[total].number
	if b > 9007199254740991-a || a+b != c {
		return false
	}
	id, od := q.field(i, "input_tokens_details"), q.field(i, "output_tokens_details")
	if !q.closed(id, []string{"cached_tokens"}, []string{"cached_tokens", "cache_write_tokens"}) || !q.closed(od, []string{"reasoning_tokens"}, []string{"reasoning_tokens"}) {
		return false
	}
	cache, write, reason := q.field(id, "cached_tokens"), q.field(id, "cache_write_tokens"), q.field(od, "reasoning_tokens")
	if !q.count(cache) || write != 0 && !q.count(write) || !q.count(reason) {
		return false
	}
	w := float64(0)
	if write != 0 {
		w = q.nodes[write].number
	}
	return q.nodes[cache].number <= a && w <= a-q.nodes[cache].number && q.nodes[reason].number <= b
}

func (q *piResponseScan) response(i int32, event string, x *piResponseExpected) bool {
	if !q.closed(i, piResponseRequired[:], piResponseFields[:]) || !q.id(q.field(i, "id")) || !q.oneOf(q.field(i, "object"), "response") ||
		!q.equalTree(q, q.field(i, "model"), x.graph, q.expectedField(x, "model")) || !q.kind(q.field(i, "created_at"), 'd') || q.nodes[q.field(i, "created_at")].number < 0 || !q.kind(q.field(i, "error"), 'n') ||
		!q.kind(q.field(i, "output"), 'a') || !q.boolean(q.field(i, "parallel_tool_calls")) || !q.empty(q.field(i, "tools")) || !q.oneOf(q.field(i, "tool_choice"), "none") {
		return false
	}
	wanted := event[len("response."):]
	if event == "response.created" {
		wanted = "in_progress"
	}
	if !q.oneOf(q.field(i, "status"), wanted) {
		return false
	}
	details := q.field(i, "incomplete_details")
	if event == "response.incomplete" {
		if !q.closed(details, []string{"reason"}, []string{"reason"}) || !q.oneOf(q.field(details, "reason"), "max_output_tokens", "content_filter") {
			return false
		}
	} else if !q.kind(details, 'n') {
		return false
	}
	id := q.field(i, "id")
	if q.responseID == 0 {
		if !q.retainNode() {
			return false
		}
		q.responseID = id
	} else if !q.sameText(q, q.responseID, q, id) {
		return false
	}
	for c := q.nodes[q.field(i, "output")].child; c != 0; c = q.nodes[c].next {
		if !q.spend(32) || !q.item(c, 0, false) {
			return false
		}
	}
	instructions := q.field(i, "instructions")
	if !(q.kind(instructions, 'n') || q.str(instructions)) || !q.echo(x, instructions, "instructions", func(v int32) bool { return q.kind(v, 'n') || q.oneOf(v, "") }) ||
		!q.metadata(q.field(i, "metadata")) || !q.echo(x, q.field(i, "metadata"), "metadata", func(v int32) bool { return q.kind(v, 'n') || q.emptyObject(v) }) ||
		!q.echo(x, q.field(i, "parallel_tool_calls"), "parallel_tool_calls", q.boolean) {
		return false
	}
	for _, entry := range [...]struct {
		name string
		max  float64
	}{{"temperature", 2}, {"top_p", 1}} {
		v := q.field(i, entry.name)
		if !q.kind(v, 'n') && (!q.kind(v, 'd') || q.nodes[v].number < 0 || q.nodes[v].number > entry.max) || !q.echo(x, v, entry.name, func(int32) bool { return true }) {
			return false
		}
	}
	if !q.optional(i, "output_text", q.str) || !q.optional(i, "store", func(v int32) bool { return q.kind(v, 'f') }) ||
		!q.optional(i, "background", func(v int32) bool { return q.kind(v, 'n') || q.kind(v, 'f') }) ||
		!q.optional(i, "completed_at", func(v int32) bool { return q.kind(v, 'n') || q.kind(v, 'd') && q.nodes[v].number >= 0 }) ||
		!q.optional(i, "conversation", func(v int32) bool { return q.kind(v, 'n') }) || !q.optional(i, "previous_response_id", func(v int32) bool { return q.kind(v, 'n') }) || !q.optional(i, "prompt", func(v int32) bool { return q.kind(v, 'n') }) ||
		!q.optional(i, "max_output_tokens", func(v int32) bool {
			return q.kind(v, 'n') || q.positive(v) && q.equalTree(q, v, x.graph, q.expectedField(x, "max_output_tokens"))
		}) ||
		!q.optional(i, "top_logprobs", func(v int32) bool { return q.kind(v, 'n') || q.kind(v, 'd') && q.nodes[v].number == 0 }) || !q.optional(i, "truncation", func(v int32) bool { return q.kind(v, 'n') || q.oneOf(v, "disabled") }) {
		return false
	}
	for _, name := range [...]string{"prompt_cache_key", "safety_identifier"} {
		if !q.optional(i, name, func(v int32) bool {
			return q.str(v) && q.echo(x, v, name, func(n int32) bool { return q.oneOf(n, "") })
		}) {
			return false
		}
	}
	if !q.optional(i, "user", func(v int32) bool {
		return (q.kind(v, 'n') || q.str(v)) && q.echo(x, v, "user", func(n int32) bool { return q.kind(n, 'n') || q.oneOf(n, "") })
	}) ||
		!q.optional(i, "prompt_cache_retention", func(v int32) bool {
			return q.kind(v, 'n') || q.oneOf(v, "in_memory", "24h") && q.echo(x, v, "prompt_cache_retention", func(int32) bool { return false })
		}) ||
		!q.optional(i, "reasoning", func(v int32) bool {
			return q.reasoning(v) && q.echo(x, v, "reasoning", func(n int32) bool { return q.kind(n, 'n') || q.emptyObject(n) })
		}) ||
		!q.optional(i, "text", func(v int32) bool { return q.textConfig(v) && q.echo(x, v, "text", q.emptyObject) }) ||
		!q.optional(i, "service_tier", func(v int32) bool {
			return (q.kind(v, 'n') || q.oneOf(v, "auto", "default", "flex", "scale", "priority")) && q.echo(x, v, "service_tier", func(int32) bool { return true })
		}) ||
		!q.optional(i, "usage", q.usage) {
		return false
	}
	return !q.denied
}

func (q *piResponseScan) event(root int32, x *piResponseExpected) (terminal, valid bool) {
	if !q.count(q.field(root, "sequence_number")) {
		return false, false
	}
	t := q.field(root, "type")
	for _, name := range [...]string{"response.created", "response.in_progress", "response.queued", "response.completed", "response.incomplete"} {
		if q.oneOf(t, name) {
			return name == "response.completed" || name == "response.incomplete", q.closed(root, []string{"type", "sequence_number", "response"}, []string{"type", "sequence_number", "response"}) && q.response(q.field(root, "response"), name, x)
		}
	}
	indexNode := q.field(root, "output_index")
	if !q.count(indexNode) {
		return false, false
	}
	index := q.nodes[indexNode].number
	if q.oneOf(t, "response.output_item.added", "response.output_item.done") {
		return false, q.closed(root, []string{"type", "sequence_number", "output_index", "item"}, []string{"type", "sequence_number", "output_index", "item"}) && q.item(q.field(root, "item"), index, true)
	}
	if q.oneOf(t, "response.content_part.added", "response.content_part.done") {
		p := q.field(root, "part")
		kind := byte(1)
		if q.oneOf(q.field(p, "type"), "reasoning_text") {
			kind = 2
		}
		return false, q.closed(root, []string{"type", "sequence_number", "content_index", "item_id", "output_index", "part"}, []string{"type", "sequence_number", "content_index", "item_id", "output_index", "part"}) && q.count(q.field(root, "content_index")) && q.part(p, "output_text", "refusal", "reasoning_text") && q.observe(q.field(root, "item_id"), kind, index, true)
	}
	if q.oneOf(t, "response.reasoning_summary_part.added", "response.reasoning_summary_part.done") {
		return false, q.closed(root, []string{"type", "sequence_number", "summary_index", "item_id", "output_index", "part"}, []string{"type", "sequence_number", "summary_index", "item_id", "output_index", "part"}) && q.count(q.field(root, "summary_index")) && q.part(q.field(root, "part"), "summary_text") && q.observe(q.field(root, "item_id"), 2, index, true)
	}
	for at, name := range [...]string{"response.reasoning_summary_text.delta", "response.reasoning_summary_text.done", "response.reasoning_text.delta", "response.reasoning_text.done", "response.output_text.delta", "response.output_text.done", "response.refusal.delta", "response.refusal.done"} {
		if !q.oneOf(t, name) {
			continue
		}
		delta := at%2 == 0
		position := "content_index"
		if at < 2 {
			position = "summary_index"
		}
		text := "text"
		if delta {
			text = "delta"
		} else if at == 7 {
			text = "refusal"
		}
		required := []string{"type", "sequence_number", "item_id", "output_index", position, text}
		allowed := [8]string{"type", "sequence_number", "item_id", "output_index", position, text}
		count := 6
		if delta {
			allowed[count] = "obfuscation"
			count++
		}
		if at == 4 || at == 5 {
			allowed[count] = "logprobs"
			count++
		}
		kind := byte(1)
		if at < 4 {
			kind = 2
		}
		return false, q.closed(root, required, allowed[:count]) && q.count(q.field(root, position)) && q.str(q.field(root, text)) && q.optional(root, "obfuscation", q.str) && q.optional(root, "logprobs", q.empty) && q.observe(q.field(root, "item_id"), kind, index, true)
	}
	return false, false
}

// Reject-only, no public approval witness. Caller retains SAME file/use gate,
// original key/context and HTTP EOF borrow; real native policy/parser remain.
func (q *piResponseScan) checkResponsePolicy(x *piResponseExpected) (err error) {
	if q == nil || x == nil || x.graph == nil || x.root == 0 || !q.parsed.Load() || q.erased.Load() || !q.policySpent.CompareAndSwap(false, true) {
		return ErrConsumed
	}
	defer func() {
		if err != nil {
			q.clear()
			x.graph.clear()
		}
	}()
	if x.graph.erased.Load() || q.ctx == nil || !q.spend(1) {
		return ErrInvalid
	}
	terminal := false
	for at := uint32(0); at < q.eventN; at++ {
		if !q.spend(256) || terminal {
			return ErrInvalid
		}
		done, ok := q.event(q.events[at].root, x)
		if !ok || q.denied {
			return ErrDeliveryMismatch
		}
		if done {
			if at+1 != q.eventN || uint64(q.events[at].frame) != q.frames {
				return ErrDeliveryMismatch
			}
			terminal = true
		}
	}
	if !terminal || q.denied || q.ctx.Err() != nil {
		return ErrDeliveryMismatch
	}
	return nil
}
