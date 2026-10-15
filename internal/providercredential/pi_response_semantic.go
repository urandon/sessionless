package providercredential

import "math"

// Private shadow of Pi6c4f3602 processResponsesStream's text/thinking assembly.
// It never constructs output text or approves effects, echoes, identity history,
// terminal state or replay. The closed policy and real Pi parser remain required.
// Slots live INSIDE the already prepaid event arena, one creation per event.
type piResponseSlot struct {
	index  float64 // exact JS safe integer; never allocate by attacker index
	match  uint32
	kind   byte // t=text, r=thinking; zero means no retained block
	active bool
}

// Every search charges the whole fixed slot record before reading it. Replaced
// and detached blocks retain their checked matcher state until final retirement;
// they must never be joined to a later block that reuses the same index.
func (q *piResponseScan) semanticSlot(before uint32, index float64) *piResponseSlot {
	for at := before; at > 0; at-- {
		if !q.spend(32) {
			return nil
		}
		slot := &q.events[at-1].slot
		if slot.active && slot.index == index {
			return slot
		}
	}
	return nil
}

func (q *piResponseScan) semanticIndex(root int32) (float64, bool) {
	i := q.field(root, "output_index")
	if i == 0 || q.nodes[i].kind != 'd' {
		return 0, false
	}
	v := q.nodes[i].number
	return v, v >= 0 && v <= 9007199254740991 && math.Trunc(v) == v
}

func (q *piResponseScan) semanticString(state uint32, i int32) (uint32, uint64, bool) {
	if i == 0 || q.nodes[i].kind != 's' {
		return 0, 0, false
	}
	n := q.nodes[i]
	for _, c := range q.text[n.from:n.to] {
		var ok bool
		state, ok = q.match(state, c, 2)
		if !ok {
			return 0, 0, false
		}
	}
	return state, uint64(n.to - n.from), true
}

// Scan exactly one map(...).join(separator) result without copying it. Pi
// constructs BOTH reasoning summary and content joins before choosing a nonempty
// result. Even a subsequently unselected or overwritten assembly is checked.
func (q *piResponseScan) semanticJoin(array int32, reasoning bool) (uint32, uint64, bool) {
	if array == 0 { // Pi's optional summary/content maps produce the empty string
		return 0, 0, true
	}
	if q.nodes[array].kind != 'a' {
		return 0, 0, false
	}
	var state uint32
	var units uint64
	first := true
	for i := q.nodes[array].child; i != 0; i = q.nodes[i].next {
		if !q.spend(32) {
			return 0, 0, false
		}
		if reasoning && !first {
			for range 2 {
				var ok bool
				state, ok = q.match(state, '\n', 2)
				if !ok {
					return 0, 0, false
				}
			}
			units += 2
		}
		first = false
		field := "text"
		if !reasoning && !q.equalsBytes(q.field(i, "type"), []byte("output_text")) {
			field = "refusal"
		}
		var n uint64
		var ok bool
		state, n, ok = q.semanticString(state, q.field(i, field))
		if !ok {
			return 0, 0, false
		}
		units += n // bounded by R decoded units plus 2*N separators
	}
	return state, units, !q.denied
}

// scanPiSemantics is single-use and uses ONLY the successful private parser's
// original context, key alias, graph and remaining cumulative work credit. The
// owner holds the same file/use gate throughout. A losing reentry cannot clear
// a winner's graph. Nil is NOT full quarantine or a transferable allow witness.
func (q *piResponseScan) scanPiSemantics() (err error) {
	if q == nil || !q.parsed.Load() || q.erased.Load() || !q.semanticSpent.CompareAndSwap(false, true) {
		return ErrConsumed
	}
	defer func() {
		if err != nil {
			q.clear()
		}
	}()
	if q.ctx == nil || len(q.key) == 0 || !q.spend(1) {
		return ErrInvalid
	}
	for at := uint32(0); at < q.eventN; at++ {
		// Fixed per-event state reads/mutations, including slot replacement and
		// retirement. Variable lookups and payload walks are charged separately.
		if !q.spend(256) {
			return ErrInvalid
		}
		root := q.events[at].root
		typeNode := q.field(root, "type")
		kind := -1
		for index, name := range [...]string{
			"response.output_item.added", "response.output_item.done",
			"response.output_text.delta", "response.refusal.delta",
			"response.reasoning_summary_text.delta", "response.reasoning_text.delta",
			"response.reasoning_summary_part.done",
		} {
			if q.equalsBytes(typeNode, []byte(name)) {
				kind = index
				break
			}
		}
		if kind < 0 {
			// Pi does not append content_part.* or standalone *.done. Lifecycle,
			// effect/unknown-event and terminal admission belongs to closed policy.
			continue
		}
		index, ok := q.semanticIndex(root)
		if !ok {
			return ErrInvalid
		}
		slot := q.semanticSlot(at, index)
		if kind <= 1 {
			item := q.field(root, "item")
			itemType := q.field(item, "type")
			wanted := byte('t')
			if q.equalsBytes(itemType, []byte("reasoning")) {
				wanted = 'r'
			} else if !q.equalsBytes(itemType, []byte("message")) {
				return ErrInvalid // tool variants have no supported semantic shadow
			}
			if kind == 0 || slot == nil {
				if kind == 0 && slot != nil {
					slot.active = false
				}
				slot = &q.events[at].slot
				*slot = piResponseSlot{index: index, kind: wanted, active: true}
			}
			if kind == 0 || slot.kind != wanted {
				continue // getOrCreateSlot never replaces an existing wrong-type slot
			}
			if wanted == 't' {
				state, _, valid := q.semanticJoin(q.field(item, "content"), false)
				if !valid {
					return ErrDeliveryMismatch
				}
				slot.match = state // message done can replace old text with empty
			} else {
				summary, summaryN, summaryOK := q.semanticJoin(q.field(item, "summary"), true)
				content, contentN, contentOK := q.semanticJoin(q.field(item, "content"), true)
				if !summaryOK || !contentOK {
					return ErrDeliveryMismatch
				}
				if summaryN != 0 {
					slot.match = summary
				} else if contentN != 0 {
					slot.match = content
				} // both empty: Pi retains the checked old thinking
			}
			slot.active = false
			continue
		}
		wanted := byte('t')
		if kind >= 4 {
			wanted = 'r'
		}
		if slot == nil || slot.kind != wanted {
			continue // no fabricated slot for a delta ignored by Pi
		}
		if kind == 6 {
			for range 2 {
				var valid bool
				slot.match, valid = q.match(slot.match, '\n', 2)
				if !valid {
					return ErrDeliveryMismatch
				}
			}
		} else {
			var valid bool
			slot.match, _, valid = q.semanticString(slot.match, q.field(root, "delta"))
			if !valid {
				return ErrDeliveryMismatch
			}
		}
	}
	if q.denied || q.ctx.Err() != nil {
		return ErrInvalid
	}
	return nil
}
