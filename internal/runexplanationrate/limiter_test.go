package runexplanationrate_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanationrate"
)

var fixtureTime = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

type fixture struct {
	identity string
	keys     [4]uint32
	selector string
	rows     [4]*runexplanationrate.Slot
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	identity, keys, err := runexplanationrate.IdentityKeys("tenant-a", "user-a")
	if err != nil {
		t.Fatalf("IdentityKeys fixture: %v", err)
	}
	selector, err := runexplanationrate.SelectorDigest("run-a")
	if err != nil {
		t.Fatalf("SelectorDigest fixture: %v", err)
	}
	return fixture{identity: identity, keys: keys, selector: selector}
}

func (f *fixture) evaluate(t *testing.T, at time.Time, requestID string) runexplanationrate.Decision {
	t.Helper()
	decision, err := runexplanationrate.Evaluate(at, f.identity, f.keys, f.rows, requestID, f.selector)
	if err != nil {
		t.Fatalf("Evaluate at=%s request=%q: %v", at, requestID, err)
	}
	return decision
}

func (f *fixture) commit(t *testing.T, decision runexplanationrate.Decision) {
	t.Helper()
	if !decision.Allowed || !decision.Debited || decision.Slot == nil {
		t.Fatalf("expected debit replacement, got %+v", decision)
	}
	for i, key := range f.keys {
		if key == decision.SlotID {
			f.rows[i] = decision.Slot
			return
		}
	}
	t.Fatalf("decision key=%d not in candidates=%v", decision.SlotID, f.keys)
}

func clone(slot runexplanationrate.Slot) runexplanationrate.Slot {
	slot.Receipts = append([]runexplanationrate.Receipt(nil), slot.Receipts...)
	return slot
}

func collidingIdentity(t *testing.T, user domain.UserID, wantKeys [4]uint32) string {
	t.Helper()
	identity, keys, err := runexplanationrate.IdentityKeys("tenant-collision", user)
	if err != nil || keys != wantKeys {
		t.Fatalf("real collision fixture user=%s keys=%v want=%v err=%v", user, keys, wantKeys, err)
	}
	return identity
}

func TestHashGoldenAndFraming(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	// Independently generated with Python hashlib/struct, not package helpers:
	// SHA256(prefix || 00 || uint64BE(len(value)) || value ...).
	if want := "abe3625183cb51a626959cebf3ff6ba221dbf4f80e0e1e5cf7144c0448107e55"; f.identity != want {
		t.Errorf("identity got=%q want=%q", f.identity, want)
	}
	if want := [4]uint32{422, 423, 424, 425}; f.keys != want {
		t.Errorf("keys got=%v want=%v", f.keys, want)
	}
	if want := "c775bc10a157841a0aa2978036c612729d9ff5222d685617f79ffb3a292efbe4"; f.selector != want {
		t.Errorf("selector got=%q want=%q", f.selector, want)
	}
	a, _, err := runexplanationrate.IdentityKeys("ab", "c")
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := runexplanationrate.IdentityKeys("a", "bc")
	if err != nil {
		t.Fatal(err)
	}
	if a == b || f.identity == f.selector {
		t.Errorf("framing/domain separation failed: a=%s b=%s", a, b)
	}
}

func TestRateBurstThirdRefillAndNoMutation(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	first := f.evaluate(t, fixtureTime, "first")
	f.commit(t, first)
	if !first.Slot.TheoreticalArrivalAt.Equal(fixtureTime.Add(5*time.Second)) || !first.Slot.ExpiresAt.Equal(fixtureTime.Add(10*time.Minute)) {
		t.Errorf("first debit times=%+v", first.Slot)
	}
	before := clone(*first.Slot)
	second := f.evaluate(t, fixtureTime, "second")
	if !reflect.DeepEqual(*first.Slot, before) {
		t.Error("second debit mutated snapshot row")
	}
	f.commit(t, second)
	if !second.Slot.TheoreticalArrivalAt.Equal(fixtureTime.Add(10 * time.Second)) {
		t.Errorf("second TAT=%s want=%s", second.Slot.TheoreticalArrivalAt, fixtureTime.Add(10*time.Second))
	}
	before = clone(*second.Slot)
	for _, tc := range []struct {
		name    string
		elapsed time.Duration
		retry   time.Duration
	}{
		{name: "third", elapsed: 0, retry: 5 * time.Second},
		{name: "fractional", elapsed: time.Nanosecond, retry: 5 * time.Second},
		{name: "almost_eligible", elapsed: 5*time.Second - time.Nanosecond, retry: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decision := f.evaluate(t, fixtureTime.Add(tc.elapsed), "third")
			if decision.Allowed || decision.Debited || decision.Slot != nil || decision.RetryAfter != tc.retry {
				t.Errorf("denial got=%+v want retry=%s", decision, tc.retry)
			}
			if !reflect.DeepEqual(*f.rows[0], before) {
				t.Error("denial mutated snapshot/TTL")
			}
		})
	}
	f.commit(t, f.evaluate(t, fixtureTime.Add(5*time.Second), "third"))
	if got := f.rows[0].TheoreticalArrivalAt; !got.Equal(fixtureTime.Add(15 * time.Second)) {
		t.Errorf("refill TAT=%s want=%s", got, fixtureTime.Add(15*time.Second))
	}
	f.commit(t, f.evaluate(t, fixtureTime.Add(time.Minute), "later"))
	if got := f.rows[0].TheoreticalArrivalAt; !got.Equal(fixtureTime.Add(time.Minute + 5*time.Second)) {
		t.Errorf("idle refill TAT=%s", got)
	}
	if len(f.rows[0].Receipts) != 1 {
		t.Errorf("expired receipts retained: %+v", f.rows[0].Receipts)
	}
}

func TestReplayRequiresExactSelectorAndNeverTouchesTTL(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.commit(t, f.evaluate(t, fixtureTime, "first"))
	f.commit(t, f.evaluate(t, fixtureTime, "second"))
	before := clone(*f.rows[0])
	for _, elapsed := range []time.Duration{0, 29*time.Second + 999*time.Millisecond} {
		decision := f.evaluate(t, fixtureTime.Add(elapsed), "first")
		if !decision.Allowed || decision.Debited || decision.Slot != nil || decision.RetryAfter != 0 {
			t.Errorf("replay at=%s got=%+v", elapsed, decision)
		}
		if !reflect.DeepEqual(*f.rows[0], before) {
			t.Error("replay mutated snapshot/TTL")
		}
	}
	other, err := runexplanationrate.SelectorDigest("run-b")
	if err != nil {
		t.Fatal(err)
	}
	decision, err := runexplanationrate.Evaluate(fixtureTime, f.identity, f.keys, f.rows, "first", other)
	if !errors.Is(err, runexplanationrate.ErrInvalidState) || decision != (runexplanationrate.Decision{}) {
		t.Errorf("selector mismatch got=%+v err=%v", decision, err)
	}
	f.commit(t, f.evaluate(t, fixtureTime.Add(30*time.Second), "first"))
	if len(f.rows[0].Receipts) != 1 {
		t.Errorf("30s boundary receipts=%+v", f.rows[0].Receipts)
	}
}

func TestSelectionExpiryCollisionsAndOwnPreference(t *testing.T) {
	t.Parallel()
	t.Run("own_later_than_free", func(t *testing.T) {
		f := newFixture(t)
		first := f.evaluate(t, fixtureTime, "first")
		f.rows[3] = first.Slot
		decision := f.evaluate(t, fixtureTime, "second")
		if decision.SlotID != f.keys[3] || decision.Slot.TheoreticalArrivalAt != fixtureTime.Add(10*time.Second) {
			t.Errorf("later own row ignored: %+v", decision)
		}
	})
	t.Run("all_live_then_exact_expiry", func(t *testing.T) {
		f := newFixture(t)
		first := f.evaluate(t, fixtureTime, "first")
		users := [4]domain.UserID{"user-400", "user-997", "user-13396", "user-14043"}
		for i := range f.rows {
			slot := clone(*first.Slot)
			slot.IdentityDigest = collidingIdentity(t, users[i], f.keys)
			f.rows[i] = &slot
		}
		before := [4]runexplanationrate.Slot{}
		for i, slot := range f.rows {
			before[i] = clone(*slot)
		}
		decision := f.evaluate(t, fixtureTime.Add(10*time.Minute-time.Nanosecond), "blocked")
		if decision.Allowed || decision.Debited || decision.Slot != nil || decision.RetryAfter != 5*time.Second {
			t.Errorf("collision denial=%+v", decision)
		}
		for i, slot := range f.rows {
			if !reflect.DeepEqual(*slot, before[i]) {
				t.Errorf("live collision slot %d mutated", i)
			}
		}
		decision = f.evaluate(t, fixtureTime.Add(10*time.Minute), "reuse")
		if !decision.Debited || decision.SlotID != f.keys[0] || decision.Slot.IdentityDigest != f.identity {
			t.Errorf("exact expiry reuse=%+v", decision)
		}
	})
	t.Run("expired_own_restarts_burst", func(t *testing.T) {
		f := newFixture(t)
		f.commit(t, f.evaluate(t, fixtureTime, "first"))
		f.commit(t, f.evaluate(t, fixtureTime, "second"))
		decision := f.evaluate(t, fixtureTime.Add(10*time.Minute), "first")
		if !decision.Debited || len(decision.Slot.Receipts) != 1 || !decision.Slot.TheoreticalArrivalAt.Equal(fixtureTime.Add(10*time.Minute+5*time.Second)) {
			t.Errorf("expired own reset=%+v", decision)
		}
	})
	t.Run("duplicate_own_even_expired", func(t *testing.T) {
		f := newFixture(t)
		f.commit(t, f.evaluate(t, fixtureTime, "first"))
		duplicate := clone(*f.rows[0])
		f.rows[3] = &duplicate
		decision, err := runexplanationrate.Evaluate(fixtureTime.Add(10*time.Minute), f.identity, f.keys, f.rows, "next", f.selector)
		if !errors.Is(err, runexplanationrate.ErrInvalidState) || decision != (runexplanationrate.Decision{}) {
			t.Errorf("duplicate own got=%+v err=%v", decision, err)
		}
	})
}

func TestFiniteKeysAndCollision(t *testing.T) {
	t.Parallel()
	var occupied [runexplanationrate.MaxSlots]bool
	var identities [runexplanationrate.MaxSlots]string
	foundCollision, foundWrap := false, false
	for i := 0; i < 10000; i++ {
		identity, keys, err := runexplanationrate.IdentityKeys("tenant-finite", domain.UserID(fmt.Sprintf("user-%d", i)))
		if err != nil {
			t.Fatalf("keys user=%d: %v", i, err)
		}
		for j, key := range keys {
			if key >= runexplanationrate.MaxSlots {
				t.Fatalf("key out of bounds user=%d key=%d", i, key)
			}
			for k := 0; k < j; k++ {
				if keys[k] == key {
					t.Fatalf("duplicate keys user=%d keys=%v", i, keys)
				}
			}
		}
		if occupied[keys[0]] && identities[keys[0]] != identity {
			foundCollision = true
		}
		occupied[keys[0]], identities[keys[0]] = true, identity
		foundWrap = foundWrap || keys[0] > keys[3]
	}
	if !foundCollision || !foundWrap {
		t.Errorf("bounded corpus collision=%v wrap=%v", foundCollision, foundWrap)
	}
}

func TestInvalidInputsFailContentFree(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"", " bad", "x/y", strings.Repeat("x", 161), "é"} {
		t.Run(fmt.Sprintf("id_%x", id), func(t *testing.T) {
			if _, _, err := runexplanationrate.IdentityKeys(domain.TenantID(id), "user"); err != runexplanationrate.ErrInvalidState {
				t.Errorf("invalid tenant err=%v", err)
			}
			if _, _, err := runexplanationrate.IdentityKeys("tenant", domain.UserID(id)); err != runexplanationrate.ErrInvalidState {
				t.Errorf("invalid user err=%v", err)
			}
			if _, err := runexplanationrate.SelectorDigest(domain.RunID(id)); err != runexplanationrate.ErrInvalidState {
				t.Errorf("invalid run err=%v", err)
			}
			f := newFixture(t)
			if _, err := runexplanationrate.Evaluate(fixtureTime, f.identity, f.keys, f.rows, id, f.selector); err != runexplanationrate.ErrInvalidState {
				t.Errorf("invalid request err=%v", err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func(*fixture, *time.Time)
	}{
		{name: "zero_time", mutate: func(_ *fixture, at *time.Time) { *at = time.Time{} }},
		{name: "non_utc", mutate: func(_ *fixture, at *time.Time) { *at = at.In(time.FixedZone("other", 3600)) }},
		{name: "year_zero", mutate: func(_ *fixture, at *time.Time) { *at = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{name: "year_10000", mutate: func(_ *fixture, at *time.Time) { *at = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{name: "identity", mutate: func(f *fixture, _ *time.Time) { f.identity = strings.Repeat("g", 64) }},
		{name: "uppercase_digest", mutate: func(f *fixture, _ *time.Time) { f.identity = strings.ToUpper(f.identity) }},
		{name: "selector", mutate: func(f *fixture, _ *time.Time) { f.selector = "invalid" }},
		{name: "wrong_key", mutate: func(f *fixture, _ *time.Time) { f.keys[0]++ }},
		{name: "out_of_bounds_key", mutate: func(f *fixture, _ *time.Time) { f.keys[0] = runexplanationrate.MaxSlots }},
		{name: "duplicate_key", mutate: func(f *fixture, _ *time.Time) { f.keys[1] = f.keys[0] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, at := newFixture(t), fixtureTime
			tc.mutate(&f, &at)
			decision, err := runexplanationrate.Evaluate(at, f.identity, f.keys, f.rows, "request", f.selector)
			if err != runexplanationrate.ErrInvalidState || decision != (runexplanationrate.Decision{}) {
				t.Errorf("invalid input got=%+v err=%v", decision, err)
			}
		})
	}
}

func TestCorruptRowsRejectedBeforeExpiryAllocationOrReplay(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*runexplanationrate.Slot)
	}{
		{name: "version", mutate: func(s *runexplanationrate.Slot) { s.Version++ }},
		{name: "identity", mutate: func(s *runexplanationrate.Slot) { s.IdentityDigest = "bad" }},
		{name: "tat_too_early", mutate: func(s *runexplanationrate.Slot) {
			s.TheoreticalArrivalAt = s.LastDebitAt.Add(5*time.Second - time.Nanosecond)
		}},
		{name: "tat_too_late", mutate: func(s *runexplanationrate.Slot) {
			s.TheoreticalArrivalAt = s.LastDebitAt.Add(10*time.Second + time.Nanosecond)
		}},
		{name: "expiry", mutate: func(s *runexplanationrate.Slot) { s.ExpiresAt = s.ExpiresAt.Add(time.Nanosecond) }},
		{name: "zero_timestamp", mutate: func(s *runexplanationrate.Slot) { s.LastDebitAt = time.Time{} }},
		{name: "non_utc_timestamp", mutate: func(s *runexplanationrate.Slot) { s.ExpiresAt = s.ExpiresAt.In(time.FixedZone("other", 3600)) }},
		{name: "no_receipts", mutate: func(s *runexplanationrate.Slot) { s.Receipts = nil }},
		{name: "duplicate_request", mutate: func(s *runexplanationrate.Slot) { s.Receipts = append(s.Receipts, s.Receipts[0]) }},
		{name: "receipt_selector", mutate: func(s *runexplanationrate.Slot) { s.Receipts[0].SelectorDigest = "bad" }},
		{name: "receipt_request", mutate: func(s *runexplanationrate.Slot) { s.Receipts[0].RequestID = "bad/id" }},
		{name: "future_receipt", mutate: func(s *runexplanationrate.Slot) { s.Receipts[0].DebitedAt = s.LastDebitAt.Add(time.Nanosecond) }},
		{name: "receipt_retention", mutate: func(s *runexplanationrate.Slot) { s.Receipts[0].DebitedAt = s.LastDebitAt.Add(-30 * time.Second) }},
		{name: "missing_last_receipt", mutate: func(s *runexplanationrate.Slot) { s.Receipts[0].DebitedAt = s.LastDebitAt.Add(-time.Second) }},
		{name: "unordered_receipts", mutate: func(s *runexplanationrate.Slot) {
			s.Receipts = append(s.Receipts, runexplanationrate.Receipt{RequestID: "older", SelectorDigest: s.Receipts[0].SelectorDigest, DebitedAt: s.LastDebitAt.Add(-time.Second)})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.commit(t, f.evaluate(t, fixtureTime, "first"))
			bad := clone(*f.rows[0])
			tc.mutate(&bad)
			if _, err := runexplanationrate.EncodeSlot(bad); err != runexplanationrate.ErrInvalidState {
				t.Errorf("corrupt encode err=%v", err)
			}
			encoded, err := json.Marshal(bad)
			if err != nil {
				t.Fatalf("fixture marshal: %v", err)
			}
			if _, err := runexplanationrate.DecodeSlot(encoded); err != runexplanationrate.ErrInvalidState {
				t.Errorf("corrupt decode err=%v", err)
			}
			f.rows[3] = &bad
			for _, at := range []time.Time{fixtureTime, fixtureTime.Add(20 * time.Minute)} {
				decision, err := runexplanationrate.Evaluate(at, f.identity, f.keys, f.rows, "first", f.selector)
				if err != runexplanationrate.ErrInvalidState || decision != (runexplanationrate.Decision{}) {
					t.Errorf("corrupt row at=%s got=%+v err=%v", at, decision, err)
				}
			}
		})
	}
}

func TestBackwardsClockAndReceiptCapacityFailClosed(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.commit(t, f.evaluate(t, fixtureTime, "first"))
	if decision, err := runexplanationrate.Evaluate(fixtureTime.Add(-time.Nanosecond), f.identity, f.keys, f.rows, "first", f.selector); err != runexplanationrate.ErrInvalidState || decision != (runexplanationrate.Decision{}) {
		t.Errorf("backwards clock got=%+v err=%v", decision, err)
	}
	slot := clone(*f.rows[0])
	slot.Receipts = nil
	for i := 0; i < runexplanationrate.MaxReceipts; i++ {
		slot.Receipts = append(slot.Receipts, runexplanationrate.Receipt{RequestID: fmt.Sprintf("receipt-%d", i), SelectorDigest: f.selector, DebitedAt: fixtureTime})
	}
	f.rows[0] = &slot
	before := clone(slot)
	if _, err := runexplanationrate.EncodeSlot(slot); err != nil {
		t.Fatalf("valid capacity fixture: %v", err)
	}
	if decision, err := runexplanationrate.Evaluate(fixtureTime.Add(time.Second), f.identity, f.keys, f.rows, "next", f.selector); err != runexplanationrate.ErrInvalidState || decision != (runexplanationrate.Decision{}) {
		t.Errorf("capacity got=%+v err=%v", decision, err)
	}
	if !reflect.DeepEqual(slot, before) {
		t.Error("capacity failure evicted a required receipt")
	}
	replay := f.evaluate(t, fixtureTime.Add(time.Second), "receipt-0")
	if !replay.Allowed || replay.Debited || replay.Slot != nil {
		t.Errorf("full-capacity replay=%+v", replay)
	}
	f.commit(t, f.evaluate(t, fixtureTime.Add(30*time.Second), "next"))
	if len(f.rows[0].Receipts) != 1 {
		t.Errorf("capacity expiry receipts=%d want=1", len(f.rows[0].Receipts))
	}
	slot.Receipts = append(slot.Receipts, runexplanationrate.Receipt{RequestID: "excess", SelectorDigest: f.selector, DebitedAt: fixtureTime})
	if _, err := runexplanationrate.EncodeSlot(slot); err != runexplanationrate.ErrInvalidState {
		t.Errorf("excess receipt encode err=%v", err)
	}
}

func TestFutureForeignRowFailsBeforeFreeSlotAllocation(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	future := f.evaluate(t, fixtureTime.Add(time.Nanosecond), "future")
	future.Slot.IdentityDigest = collidingIdentity(t, "user-400", f.keys)
	f.rows[3] = future.Slot
	decision, err := runexplanationrate.Evaluate(fixtureTime, f.identity, f.keys, f.rows, "now", f.selector)
	if err != runexplanationrate.ErrInvalidState || decision != (runexplanationrate.Decision{}) {
		t.Errorf("future occupied row with free first slot: got=%+v err=%v", decision, err)
	}
}

func TestInvalidForeignPlacementFailsBeforeExpiryAllocationOrReplay(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		at   time.Time
		own  bool
	}{
		{name: "live_with_free_slot", at: fixtureTime},
		{name: "expired_with_free_slot", at: fixtureTime.Add(10 * time.Minute)},
		{name: "live_before_own_replay", at: fixtureTime, own: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			debit := f.evaluate(t, fixtureTime, "first")
			misplaced := clone(*debit.Slot)
			// A real different identity whose four keys exclude fixture key 425.
			foreign, keys, err := runexplanationrate.IdentityKeys("tenant-foreign", "user-foreign")
			if err != nil {
				t.Fatalf("foreign fixture: %v", err)
			}
			for _, key := range keys {
				if key == f.keys[3] {
					t.Fatalf("foreign fixture unexpectedly allows key=%d keys=%v", key, keys)
				}
			}
			misplaced.IdentityDigest = foreign
			if _, err := runexplanationrate.EncodeSlot(misplaced); err != nil {
				t.Fatalf("well-formed but misplaced fixture: %v", err)
			}
			if tc.own {
				f.rows[0] = debit.Slot
			}
			f.rows[3] = &misplaced
			before := clone(misplaced)
			decision, err := runexplanationrate.Evaluate(tc.at, f.identity, f.keys, f.rows, "first", f.selector)
			if err != runexplanationrate.ErrInvalidState || decision != (runexplanationrate.Decision{}) {
				t.Errorf("misplaced foreign row at=%s own=%v got=%+v err=%v", tc.at, tc.own, decision, err)
			}
			if !reflect.DeepEqual(misplaced, before) {
				t.Error("placement failure mutated snapshot")
			}
		})
	}
}

func TestMaximumBoundedReceiptEncoding(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	decision := f.evaluate(t, fixtureTime, "first")
	slot := clone(*decision.Slot)
	slot.Receipts = nil
	for i := 0; i < runexplanationrate.MaxReceipts; i++ {
		request := fmt.Sprintf("%03d%s", i, strings.Repeat("x", 157))
		slot.Receipts = append(slot.Receipts, runexplanationrate.Receipt{RequestID: request, SelectorDigest: f.selector, DebitedAt: fixtureTime})
	}
	encoded, err := runexplanationrate.EncodeSlot(slot)
	if err != nil || len(encoded) > runexplanationrate.MaxRowBytes {
		t.Fatalf("maximum bounded receipt encoding: bytes=%d ceiling=%d err=%v", len(encoded), runexplanationrate.MaxRowBytes, err)
	}
	decoded, err := runexplanationrate.DecodeSlot(encoded)
	if err != nil || !reflect.DeepEqual(slot, decoded) {
		t.Errorf("maximum receipt roundtrip: got=%+v err=%v", decoded, err)
	}
}

func TestWireClosedSchemaRoundTripAndByteBounds(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	decision := f.evaluate(t, fixtureTime, "first")
	encoded, err := runexplanationrate.EncodeSlot(*decision.Slot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := runexplanationrate.DecodeSlot(encoded)
	if err != nil || !reflect.DeepEqual(decoded, *decision.Slot) {
		t.Fatalf("roundtrip got=%+v err=%v want=%+v", decoded, err, decision.Slot)
	}
	canonical, err := runexplanationrate.EncodeSlot(decoded)
	if err != nil || !bytes.Equal(encoded, canonical) {
		t.Errorf("canonical roundtrip got=%q err=%v want=%q", canonical, err, encoded)
	}
	exact := append(bytes.Clone(encoded), bytes.Repeat([]byte(" "), runexplanationrate.MaxRowBytes-len(encoded))...)
	if _, err := runexplanationrate.DecodeSlot(exact); err != nil {
		t.Errorf("exact byte bound err=%v", err)
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "oversized", data: append(bytes.Clone(exact), ' ')},
		{name: "empty", data: nil},
		{name: "malformed", data: []byte("{")},
		{name: "null", data: []byte("null")},
		{name: "array", data: []byte("[]")},
		{name: "trailing_json", data: append(bytes.Clone(encoded), []byte(" {}")...)},
		{name: "trailing_junk", data: append(bytes.Clone(encoded), 'x')},
		{name: "unknown", data: bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"extra":1`), 1)},
		{name: "duplicate", data: bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)},
		{name: "case_alias", data: bytes.Replace(encoded, []byte(`"version":1`), []byte(`"Version":1`), 1)},
		{name: "missing", data: bytes.Replace(encoded, []byte(`"version":1,`), nil, 1)},
		{name: "null_field", data: bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":null`), 1)},
		{name: "receipt_unknown", data: bytes.Replace(encoded, []byte(`"request_id":"first"`), []byte(`"request_id":"first","extra":1`), 1)},
		{name: "receipt_duplicate", data: bytes.Replace(encoded, []byte(`"request_id":"first"`), []byte(`"request_id":"first","request_id":"first"`), 1)},
		{name: "receipt_case_alias", data: bytes.Replace(encoded, []byte(`"request_id":"first"`), []byte(`"Request_ID":"first"`), 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := runexplanationrate.DecodeSlot(tc.data); err != runexplanationrate.ErrInvalidState || !reflect.DeepEqual(got, runexplanationrate.Slot{}) {
				t.Errorf("invalid wire got=%+v err=%v bytes=%d", got, err, len(tc.data))
			}
		})
	}
}

func TestYearEndArithmetic(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		at    time.Time
		valid bool
	}{
		{name: "ordinary_year_boundary", at: time.Date(2026, 12, 31, 23, 59, 58, 0, time.UTC), valid: true},
		{name: "maximum_safe_expiry", at: time.Date(9999, 12, 31, 23, 49, 59, 999999999, time.UTC), valid: true},
		{name: "expiry_year_overflow", at: time.Date(9999, 12, 31, 23, 50, 0, 0, time.UTC)},
		{name: "tat_year_overflow", at: time.Date(9999, 12, 31, 23, 59, 58, 0, time.UTC)},
		{name: "zero_instant", at: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
		{name: "earliest_nonzero", at: time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC), valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			decision, err := runexplanationrate.Evaluate(tc.at, f.identity, f.keys, f.rows, "request", f.selector)
			if !tc.valid {
				if err != runexplanationrate.ErrInvalidState || decision != (runexplanationrate.Decision{}) {
					t.Errorf("overflow got=%+v err=%v", decision, err)
				}
				return
			}
			if err != nil || !decision.Debited {
				t.Fatalf("safe arithmetic at=%s got=%+v err=%v", tc.at, decision, err)
			}
			if !decision.Slot.ExpiresAt.Equal(tc.at.Add(10 * time.Minute)) {
				t.Errorf("expiry=%s want=%s", decision.Slot.ExpiresAt, tc.at.Add(10*time.Minute))
			}
			encoded, err := runexplanationrate.EncodeSlot(*decision.Slot)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runexplanationrate.DecodeSlot(encoded); err != nil {
				t.Errorf("year boundary roundtrip: %v", err)
			}
		})
	}
}

func FuzzDecodeSlot(f *testing.F) {
	f.Add([]byte("null"))
	f.Add([]byte(`{"version":1,"version":1}`))
	f.Add([]byte(`{"version":1,"receipts":[null]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		slot, err := runexplanationrate.DecodeSlot(data)
		if err != nil {
			if err != runexplanationrate.ErrInvalidState {
				t.Fatalf("non-content-free error: %v", err)
			}
			return
		}
		encoded, err := runexplanationrate.EncodeSlot(slot)
		if err != nil || len(encoded) > runexplanationrate.MaxRowBytes {
			t.Fatalf("accepted row cannot encode: bytes=%d err=%v", len(encoded), err)
		}
		decoded, err := runexplanationrate.DecodeSlot(encoded)
		if err != nil || !reflect.DeepEqual(slot, decoded) {
			t.Fatalf("accepted row cannot roundtrip: err=%v", err)
		}
	})
}
