package computechoice

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanationrate"
)

func listRateTime() time.Time {
	return time.Date(2026, 10, 10, 18, 0, 0, 123456789, time.UTC)
}

func listRateQuery() ListQuery { return ListQuery{Limit: 4, InputKind: InputText} }

func mustListRateIdentity(t *testing.T, scope Scope) (string, [runexplanationrate.ProbeCount]uint32) {
	t.Helper()
	identity, keys, err := ListRateIdentityKeys(scope.TenantID, scope.UserID)
	if err != nil {
		t.Fatalf("ListRateIdentityKeys: %v", err)
	}
	return identity, keys
}

func commitListRateDecision(t *testing.T, slots *[runexplanationrate.ProbeCount]*runexplanationrate.Slot, keys [runexplanationrate.ProbeCount]uint32, decision runexplanationrate.Decision) {
	t.Helper()
	if !decision.Allowed || !decision.Debited || decision.Slot == nil {
		t.Fatalf("not a detached debit: %+v", decision)
	}
	for i, key := range keys {
		if key == decision.SlotID {
			encoded, err := runexplanationrate.EncodeSlot(*decision.Slot)
			if err != nil {
				t.Fatal(err)
			}
			persisted, err := runexplanationrate.DecodeSlot(encoded)
			if err != nil {
				t.Fatal(err)
			}
			slots[i] = &persisted
			return
		}
	}
	t.Fatalf("replacement key%d outside exact candidates%v", decision.SlotID, keys)
}

func assertInvalidListRate(t *testing.T, decision runexplanationrate.Decision, err error) {
	t.Helper()
	if decision != (runexplanationrate.Decision{}) || err != ErrInvalidListRateState || err.Error() != "compute choice list rate state invalid" {
		t.Fatalf("got %+v,%v; want zero/content-free ErrInvalidListRateState", decision, err)
	}
}

func TestListRateIdentityDomainScopeAndFiniteKeys(t *testing.T) {
	scope := testScope()
	identity, keys := mustListRateIdentity(t, scope)
	oldIdentity, _, err := runexplanationrate.IdentityKeys(scope.TenantID, scope.UserID)
	if err != nil || identity == oldIdentity || len(identity) != 64 {
		t.Fatalf("identity domains not separated: compute%q explanation%q err%v", identity, oldIdentity, err)
	}
	// Independently verify canonical length framing and finite addressing.
	digest := sha256.New()
	for _, field := range []string{"sessionless.compute-choice-rate.identity.v1", string(scope.TenantID), string(scope.UserID)} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(field)))
		digest.Write(size[:])
		digest.Write([]byte(field))
	}
	if want := hex.EncodeToString(digest.Sum(nil)); identity != want {
		t.Fatalf("identity framing: got%q want%q", identity, want)
	}
	decoded, _ := hex.DecodeString(identity)
	base := uint32(binary.BigEndian.Uint64(decoded[:8]) % 4096)
	for i, key := range keys {
		if key != (base+uint32(i))%4096 || key >= 4096 {
			t.Errorf("probe%d got%d base%d", i, key, base)
		}
	}
	for _, pair := range [][2]string{{"other-tenant", string(scope.UserID)}, {string(scope.TenantID), "other-owner"}, {"ab", "c"}, {"a", "bc"}} {
		got, _, err := ListRateIdentityKeys(domain.TenantID(pair[0]), domain.UserID(pair[1]))
		if err != nil || got == identity {
			t.Errorf("tenant/user isolation: %v hash%q err%v", pair, got, err)
		}
	}
	one, _, _ := ListRateIdentityKeys("ab", "c")
	two, _, _ := ListRateIdentityKeys("a", "bc")
	if one == two {
		t.Fatal("unframed tenant/user collision")
	}
	for _, pair := range [][2]string{{"", "owner"}, {"tenant", ""}, {"private/tenant", "owner"}, {"tenant", strings.Repeat("x", 161)}} {
		got, gotKeys, err := ListRateIdentityKeys(domain.TenantID(pair[0]), domain.UserID(pair[1]))
		if got != "" || gotKeys != ([4]uint32{}) || err != ErrInvalidListRateState {
			t.Errorf("invalid scope minted identity: hash%q keys%v err%v", got, gotKeys, err)
		}
	}
}

func TestListRateSelectorSealsScopeAndNormalizedQuery(t *testing.T) {
	scope, query := testScope(), listRateQuery()
	base, err := ListRateSelectorDigest(scope, query)
	if err != nil || len(base) != 64 {
		t.Fatalf("selector: %q %v", base, err)
	}
	old, err := runexplanationrate.SelectorDigest("session-a")
	if err != nil || old == base {
		t.Fatalf("selector domain not separated: %q %v", old, err)
	}
	for _, raw := range []string{"", "input_kind=text&limit=4", "limit=%34&input_kind=%74ext"} {
		normalized, err := ParseListQuery(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := ListRateSelectorDigest(scope, normalized); err != nil || got != base {
			t.Errorf("equivalent query altered receipt selector: %q %v", got, err)
		}
	}
	codec, registry, now := cursorFixture(t)
	withCursor := query
	withCursor.Cursor = mustCursor(t, codec, registry, now)
	for _, test := range []struct {
		name   string
		mutate func(*Scope, *ListQuery)
	}{
		{"tenant", func(s *Scope, _ *ListQuery) { s.TenantID = "other" }},
		{"user", func(s *Scope, _ *ListQuery) { s.UserID = "other" }},
		{"session", func(s *Scope, _ *ListQuery) { s.SessionID = "other" }},
		{"membership", func(s *Scope, _ *ListQuery) { s.MembershipSecurityVersion++ }},
		{"limit", func(_ *Scope, q *ListQuery) { q.Limit = 1 }},
		{"input", func(_ *Scope, q *ListQuery) { q.InputKind = InputImage }},
		{"cursor", func(_ *Scope, q *ListQuery) { q.Cursor = withCursor.Cursor }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changedScope, changedQuery := scope, query
			test.mutate(&changedScope, &changedQuery)
			if got, err := ListRateSelectorDigest(changedScope, changedQuery); err != nil || got == base {
				t.Fatalf("changed%s not sealed: %q %v", test.name, got, err)
			}
		})
	}
	for _, invalid := range []Scope{{}, {TenantID: scope.TenantID, UserID: scope.UserID, SessionID: scope.SessionID}} {
		if got, err := ListRateSelectorDigest(invalid, query); got != "" || err != ErrInvalidListRateState {
			t.Errorf("invalid scope minted selector: %q %v", got, err)
		}
	}
	if got, err := ListRateSelectorDigest(scope, ListQuery{}); got != "" || err != ErrInvalidListRateState {
		t.Errorf("invalid query minted selector: %q %v", got, err)
	}
}

func TestListRateSharedEngineBurstRefillAndReplay(t *testing.T) {
	scope, query, now := testScope(), listRateQuery(), listRateTime()
	identity, keys := mustListRateIdentity(t, scope)
	selector, err := ListRateSelectorDigest(scope, query)
	if err != nil {
		t.Fatal(err)
	}
	var slots [runexplanationrate.ProbeCount]*runexplanationrate.Slot
	for index, request := range []string{"request-one", "request-two"} {
		decision, err := EvaluateListRate(now, scope, query, slots, request)
		if err != nil {
			t.Fatal(err)
		}
		shared, sharedErr := runexplanationrate.Evaluate(now, identity, keys, slots, request, selector)
		if sharedErr != nil || !reflect.DeepEqual(decision, shared) {
			t.Fatalf("binding forked engine semantics: %+v shared%+v err%v", decision, shared, sharedErr)
		}
		commitListRateDecision(t, &slots, keys, decision)
		if len(slots[0].Receipts) != index+1 {
			t.Fatalf("wrong receipt count: %d", len(slots[0].Receipts))
		}
	}
	before, err := runexplanationrate.EncodeSlot(*slots[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{now, now.Add(time.Second)} {
		replay, err := EvaluateListRate(at, scope, query, slots, "request-one")
		if err != nil || !replay.Allowed || replay.Debited || replay.Slot != nil || replay.RetryAfter != 0 {
			t.Fatalf("same request/query replay debited again: %+v %v", replay, err)
		}
	}
	denied, err := EvaluateListRate(now, scope, query, slots, "request-three")
	if err != nil || denied.Allowed || denied.Debited || denied.Slot != nil || denied.RetryAfter != 5*time.Second {
		t.Fatalf("burst exceeded: %+v %v", denied, err)
	}
	denied, err = EvaluateListRate(now.Add(5*time.Second-time.Nanosecond), scope, query, slots, "request-three")
	if err != nil || denied.Allowed || denied.RetryAfter != time.Second {
		t.Fatalf("rounded refill boundary: %+v %v", denied, err)
	}
	after, _ := runexplanationrate.EncodeSlot(*slots[0])
	if !bytes.Equal(before, after) {
		t.Fatal("pure replay/denial mutated supplied row")
	}
	refilled, err := EvaluateListRate(now.Add(5*time.Second), scope, query, slots, "request-three")
	if err != nil || !refilled.Allowed || !refilled.Debited || refilled.Slot == nil {
		t.Fatalf("exact refill denied: %+v %v", refilled, err)
	}
	refilled.Slot.Receipts[0].RequestID = "mutated-return"
	if after, _ := runexplanationrate.EncodeSlot(*slots[0]); !bytes.Equal(before, after) {
		t.Fatal("returned replacement aliases input receipts")
	}
}

func TestListRateReplayRejectsChangedExactContext(t *testing.T) {
	scope, query, now := testScope(), listRateQuery(), listRateTime()
	_, keys := mustListRateIdentity(t, scope)
	var slots [runexplanationrate.ProbeCount]*runexplanationrate.Slot
	decision, err := EvaluateListRate(now, scope, query, slots, "same-request")
	if err != nil {
		t.Fatal(err)
	}
	commitListRateDecision(t, &slots, keys, decision)
	codec, registry, cursorNow := cursorFixture(t)
	cursor := mustCursor(t, codec, registry, cursorNow)
	for _, test := range []struct {
		name   string
		mutate func(*Scope, *ListQuery)
	}{
		{"membership", func(s *Scope, _ *ListQuery) { s.MembershipSecurityVersion++ }},
		{"session", func(s *Scope, _ *ListQuery) { s.SessionID = "other-session" }},
		{"limit", func(_ *Scope, q *ListQuery) { q.Limit = 3 }},
		{"input", func(_ *Scope, q *ListQuery) { q.InputKind = InputMixed }},
		{"cursor", func(_ *Scope, q *ListQuery) { q.Cursor = cursor }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changedScope, changedQuery := scope, query
			test.mutate(&changedScope, &changedQuery)
			decision, err := EvaluateListRate(now, changedScope, changedQuery, slots, "same-request")
			assertInvalidListRate(t, decision, err)
		})
	}
}

func TestListRateReceiptAndIdleExpiryBoundaries(t *testing.T) {
	scope, query, now := testScope(), listRateQuery(), listRateTime()
	_, keys := mustListRateIdentity(t, scope)
	var slots [runexplanationrate.ProbeCount]*runexplanationrate.Slot
	initial, err := EvaluateListRate(now, scope, query, slots, "request-one")
	if err != nil {
		t.Fatal(err)
	}
	commitListRateDecision(t, &slots, keys, initial)
	replay, err := EvaluateListRate(now.Add(30*time.Second-time.Nanosecond), scope, query, slots, "request-one")
	if err != nil || !replay.Allowed || replay.Debited {
		t.Fatalf("receipt expired early: %+v %v", replay, err)
	}
	for _, at := range []time.Time{now.Add(30 * time.Second), now.Add(10*time.Minute - time.Nanosecond), now.Add(10 * time.Minute)} {
		next, err := EvaluateListRate(at, scope, query, slots, "request-one")
		if err != nil || !next.Allowed || !next.Debited || len(next.Slot.Receipts) != 1 || !next.Slot.Receipts[0].DebitedAt.Equal(at) {
			t.Fatalf("expiry boundary%s did not require a fresh debit: %+v %v", at, next, err)
		}
	}
	encoded, _ := runexplanationrate.EncodeSlot(*slots[0])
	for _, mutate := range []func(*runexplanationrate.Slot){
		func(s *runexplanationrate.Slot) { s.Version = 2 },
		func(s *runexplanationrate.Slot) { s.IdentityDigest = "private-corruption" },
		func(s *runexplanationrate.Slot) { s.Receipts[0].SelectorDigest = "private-corruption" },
		func(s *runexplanationrate.Slot) { s.ExpiresAt = s.ExpiresAt.Add(time.Nanosecond) },
	} {
		corrupt, err := runexplanationrate.DecodeSlot(encoded)
		if err != nil {
			t.Fatal(err)
		}
		mutate(&corrupt)
		badSlots := slots
		badSlots[0] = &corrupt
		decision, err := EvaluateListRate(now.Add(11*time.Minute), scope, query, badSlots, "request-new")
		assertInvalidListRate(t, decision, err)
	}
}

func TestListRateCollisionSaturationAndExpiredPlacementCorruption(t *testing.T) {
	scope, query, now := testScope(), listRateQuery(), listRateTime()
	identity, keys := mustListRateIdentity(t, scope)
	selector, err := ListRateSelectorDigest(scope, query)
	if err != nil {
		t.Fatal(err)
	}
	// Synthetic valid occupant digests all derive the same finite base. The
	// shared engine validates their placement independently of our caller.
	var slots [runexplanationrate.ProbeCount]*runexplanationrate.Slot
	for index := range slots {
		var digest [sha256.Size]byte
		binary.BigEndian.PutUint64(digest[:8], uint64(keys[0]))
		digest[31] = byte(index + 1)
		foreign := hex.EncodeToString(digest[:])
		if foreign == identity {
			t.Fatal("fixture unexpectedly equals caller identity")
		}
		slots[index] = &runexplanationrate.Slot{Version: 1, IdentityDigest: foreign, TheoreticalArrivalAt: now.Add(5 * time.Second), LastDebitAt: now, ExpiresAt: now.Add(10 * time.Minute), Receipts: []runexplanationrate.Receipt{{RequestID: fmt.Sprintf("foreign-%d", index), SelectorDigest: selector, DebitedAt: now}}}
		if _, err := runexplanationrate.EncodeSlot(*slots[index]); err != nil {
			t.Fatal(err)
		}
	}
	denied, err := EvaluateListRate(now, scope, query, slots, "request-one")
	if err != nil || denied.Allowed || denied.Debited || denied.Slot != nil || denied.RetryAfter != 5*time.Second {
		t.Fatalf("saturated four probes allocated outside bound: %+v %v", denied, err)
	}
	partial := slots
	partial[2] = nil
	debit, err := EvaluateListRate(now, scope, query, partial, "request-one")
	if err != nil || !debit.Allowed || !debit.Debited || debit.SlotID != keys[2] {
		t.Fatalf("bounded collision did not select available key: %+v %v", debit, err)
	}
	expired, err := EvaluateListRate(now.Add(10*time.Minute), scope, query, slots, "request-one")
	if err != nil || !expired.Allowed || expired.SlotID != keys[0] {
		t.Fatalf("valid expired occupant not reused: %+v %v", expired, err)
	}
	bad := *slots[3]
	var misplaced [sha256.Size]byte
	binary.BigEndian.PutUint64(misplaced[:8], uint64((keys[0]+100)%4096))
	bad.IdentityDigest = hex.EncodeToString(misplaced[:])
	corrupt := slots
	corrupt[3] = &bad
	decision, err := EvaluateListRate(now.Add(11*time.Minute), scope, query, corrupt, "request-one")
	assertInvalidListRate(t, decision, err)
	// All supplied rows are checked before an own replay or empty-slot debit.
	partial[3] = &bad
	decision, err = EvaluateListRate(now, scope, query, partial, "request-one")
	assertInvalidListRate(t, decision, err)
	// Corruption in another supplied row also refuses an exact own replay;
	// no early receipt return may skip validation of the coherent snapshot.
	own := debit.Slot
	replaySlots := partial
	replaySlots[2] = own
	decision, err = EvaluateListRate(now, scope, query, replaySlots, "request-one")
	assertInvalidListRate(t, decision, err)
}

func TestListRateReceiptBoundsAndDuplicateOwnOccupant(t *testing.T) {
	scope, query, now := testScope(), listRateQuery(), listRateTime()
	_, keys := mustListRateIdentity(t, scope)
	var slots [runexplanationrate.ProbeCount]*runexplanationrate.Slot
	debit, err := EvaluateListRate(now, scope, query, slots, "request-one")
	if err != nil {
		t.Fatal(err)
	}
	commitListRateDecision(t, &slots, keys, debit)
	t.Run("duplicate_own", func(t *testing.T) {
		duplicate := slots
		duplicate[1] = slots[0]
		decision, err := EvaluateListRate(now, scope, query, duplicate, "request-one")
		assertInvalidListRate(t, decision, err)
	})
	t.Run("receipt_saturation", func(t *testing.T) {
		full := *slots[0]
		full.Receipts = make([]runexplanationrate.Receipt, runexplanationrate.MaxReceipts)
		for i := range full.Receipts {
			full.Receipts[i] = runexplanationrate.Receipt{RequestID: fmt.Sprintf("request-%d", i), SelectorDigest: slots[0].Receipts[0].SelectorDigest, DebitedAt: now}
		}
		if _, err := runexplanationrate.EncodeSlot(full); err != nil {
			t.Fatalf("synthetic bounded receipt fixture: %v", err)
		}
		fullSlots := slots
		fullSlots[0] = &full
		decision, err := EvaluateListRate(now, scope, query, fullSlots, "new-request")
		assertInvalidListRate(t, decision, err)
		// A legitimate existing receipt still replays without allocating a
		// seventeenth receipt or changing the finite row.
		replay, err := EvaluateListRate(now, scope, query, fullSlots, "request-0")
		if err != nil || !replay.Allowed || replay.Debited || replay.Slot != nil {
			t.Fatalf("saturated receipt replay: %+v %v", replay, err)
		}
		full.Receipts = append(full.Receipts, full.Receipts[0])
		decision, err = EvaluateListRate(now, scope, query, fullSlots, "request-0")
		assertInvalidListRate(t, decision, err)
	})
}

func TestListRateInvalidInputsUniformAndNoRawIdentityInRows(t *testing.T) {
	scope, query, now := testScope(), listRateQuery(), listRateTime()
	var slots [runexplanationrate.ProbeCount]*runexplanationrate.Slot
	for _, test := range []struct {
		name    string
		scope   Scope
		query   ListQuery
		now     time.Time
		request string
	}{
		{"scope", Scope{}, query, now, "request"},
		{"membership", Scope{TenantID: scope.TenantID, UserID: scope.UserID, SessionID: scope.SessionID}, query, now, "request"},
		{"query", scope, ListQuery{}, now, "request"},
		{"clock_zero", scope, query, time.Time{}, "request"},
		{"clock_zone", scope, query, now.In(time.FixedZone("non-UTC", 0)), "request"},
		{"clock_overflow", scope, query, time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC), "request"},
		{"request_empty", scope, query, now, ""},
		{"request_bad", scope, query, now, "private/request"},
	} {
		t.Run(test.name, func(t *testing.T) {
			decision, err := EvaluateListRate(test.now, test.scope, test.query, slots, test.request)
			assertInvalidListRate(t, decision, err)
		})
	}
	decision, err := EvaluateListRate(now, scope, query, slots, "opaque-server-request")
	if err != nil || decision.Slot == nil {
		t.Fatalf("valid debit: %+v %v", decision, err)
	}
	encoded, err := runexplanationrate.EncodeSlot(*decision.Slot)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{string(scope.TenantID), string(scope.UserID), string(scope.SessionID)} {
		if bytes.Contains(encoded, []byte(raw)) {
			t.Errorf("raw private identity %q serialized in rate row", raw)
		}
	}
}
