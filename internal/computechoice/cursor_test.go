package computechoice

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func cursorFixture(t testing.TB) (*CursorCodec, *ScopedRegistry, time.Time) {
	t.Helper()
	codec, err := NewCursorCodec(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return codec, mustScoped(t, testScope(), testRegistrations(9)), time.Date(2026, 10, 10, 18, 0, 0, 123456789, time.UTC)
}

func mustCursor(t *testing.T, codec *CursorCodec, registry *ScopedRegistry, now time.Time) string {
	t.Helper()
	token, err := codec.Encode(testScope(), registry, InputText, 4, now, now.Add(60*time.Second), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return token
}

func assertStale(t *testing.T, position uint32, err error) {
	t.Helper()
	if position != 0 || err != ErrChoiceStale || err.Error() != "compute choice stale" {
		t.Fatalf("got position=%d err=%v; want 0/content-free exact ErrChoiceStale", position, err)
	}
}

func TestCursorRoundTripAndCopiedKey(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	codec, err := NewCursorCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	key[0]++
	_, registry, now := cursorFixture(t)
	for _, kind := range []InputKind{InputText, InputImage, InputFile, InputMixed} {
		t.Run(string(kind), func(t *testing.T) {
			token, err := codec.Encode(testScope(), registry, kind, 4, now, now.Add(time.Minute), now.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if len(token) != 164 || len(token) > MaxCursorBytes {
				t.Errorf("token length %d", len(token))
			}
			decoded, err := base64.RawURLEncoding.DecodeString(token)
			if err != nil {
				t.Fatal(err)
			}
			for _, raw := range []string{"tenant-a", "owner-a", "session-a", "choice-00", strings.Repeat("a", 64)} {
				if bytes.Contains(decoded, []byte(raw)) {
					t.Errorf("raw private field present %q", raw)
				}
			}
			for _, at := range []time.Time{now, now.Add(time.Minute - time.Nanosecond)} {
				position, err := codec.Decode(token, testScope(), registry, kind, at, now.Add(time.Minute))
				if err != nil || position != 4 {
					t.Errorf("at=%s position=%d err=%v", at, position, err)
				}
			}
			original, _, _ := cursorFixture(t)
			if _, err := original.Decode(token, testScope(), registry, kind, now, now.Add(time.Minute)); err != nil {
				t.Errorf("constructor retained mutable key: %v", err)
			}
		})
	}
	for _, size := range []int{0, 31, 33} {
		if _, err := NewCursorCodec(make([]byte, size)); !errors.Is(err, ErrInvalidCursorKey) {
			t.Errorf("key size%d: %v", size, err)
		}
	}
	zone := time.FixedZone("equivalent", 3*3600)
	first := mustCursor(t, codec, registry, now)
	second := mustCursor(t, codec, registry, now.In(zone))
	if first != second {
		t.Error("equivalent mixed-zone times changed token")
	}
}

func TestCursorForeignChangesAcceptedOwnChangesStale(t *testing.T) {
	codec, base, now := cursorFixture(t)
	own := testRegistrations(9)
	foreign := Registration{TenantID: "foreign", OwnerUserID: "foreign-owner", ChoiceID: "000-foreign", RegistrationRevision: 1, DisclosureRevision: strings.Repeat("a", 64)}
	base = mustScoped(t, testScope(), append(append([]Registration(nil), own...), foreign))
	token := mustCursor(t, codec, base, now)
	for _, entries := range [][]Registration{
		append(append(append([]Registration(nil), own...), foreign), Registration{TenantID: "second-foreign", OwnerUserID: "foreign-owner", ChoiceID: "000-addition", RegistrationRevision: 1, DisclosureRevision: foreign.DisclosureRevision}), own,
		append(append([]Registration(nil), own...), Registration{TenantID: foreign.TenantID, OwnerUserID: foreign.OwnerUserID, ChoiceID: "changed", RegistrationRevision: 3, DisclosureRevision: strings.Repeat("b", 64)}),
	} {
		scoped := mustScoped(t, testScope(), entries)
		if position, err := codec.Decode(token, testScope(), scoped, InputText, now, now.Add(time.Minute)); err != nil || position != 4 {
			t.Errorf("foreign-only change invalidated cursor: %d %v", position, err)
		}
		if page, err := scoped.Page(4, 4); err != nil || page.LastPosition != 8 || page.Registrations[0].ChoiceID != "choice-04" {
			t.Errorf("foreign-only page changed: %+v %v", page, err)
		}
	}
	for _, mutate := range []func([]Registration) []Registration{
		func(e []Registration) []Registration { e[0].ChoiceID = "changed"; return e },
		func(e []Registration) []Registration { e[0].RegistrationRevision++; return e },
		func(e []Registration) []Registration { e[0].DisclosureRevision = strings.Repeat("b", 64); return e },
		func(e []Registration) []Registration { return e[1:] },
		func(e []Registration) []Registration { more := e[0]; more.ChoiceID = "extra"; return append(e, more) },
	} {
		scoped := mustScoped(t, testScope(), mutate(append([]Registration(nil), own...)))
		position, err := codec.Decode(token, testScope(), scoped, InputText, now, now.Add(time.Minute))
		assertStale(t, position, err)
	}
}

func TestCursorScopeAndInputIsolation(t *testing.T) {
	codec, registry, now := cursorFixture(t)
	token := mustCursor(t, codec, registry, now)
	for _, test := range []struct {
		name   string
		mutate func(*Scope)
	}{
		{"tenant", func(s *Scope) { s.TenantID = "other" }}, {"owner", func(s *Scope) { s.UserID = "other" }}, {"session", func(s *Scope) { s.SessionID = "other" }}, {"membership_version", func(s *Scope) { s.MembershipSecurityVersion++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			scope := testScope()
			test.mutate(&scope)
			scoped := mustScoped(t, scope, testRegistrations(9))
			position, err := codec.Decode(token, scope, scoped, InputText, now, now.Add(time.Minute))
			assertStale(t, position, err)
			position, err = codec.Decode(token, scope, registry, InputText, now, now.Add(time.Minute))
			assertStale(t, position, err)
		})
	}
	for _, kind := range []InputKind{InputImage, InputFile, InputMixed, "", "unknown"} {
		position, err := codec.Decode(token, testScope(), registry, kind, now, now.Add(time.Minute))
		assertStale(t, position, err)
	}
}

func TestCursorExpiryAndAuthorityWindows(t *testing.T) {
	codec, registry, now := cursorFixture(t)
	token := mustCursor(t, codec, registry, now)
	for _, test := range []struct {
		name          string
		at, authority time.Time
	}{
		{"before_issue", now.Add(-time.Nanosecond), now.Add(time.Minute)},
		{"exact_expiry", now.Add(time.Minute), now.Add(2 * time.Minute)},
		{"after_expiry", now.Add(time.Minute + time.Nanosecond), now.Add(2 * time.Minute)},
		{"shortened_current_authority", now, now.Add(time.Minute - time.Nanosecond)},
		{"authority_exact_now", now, now}, {"authority_expired", now, now.Add(-time.Nanosecond)},
		{"missing_authority", now, time.Time{}}, {"missing_now", time.Time{}, now.Add(time.Minute)},
	} {
		t.Run(test.name, func(t *testing.T) {
			position, err := codec.Decode(token, testScope(), registry, InputText, test.at, test.authority)
			assertStale(t, position, err)
		})
	}
	for _, test := range []struct {
		name               string
		expires, authority time.Time
	}{
		{"TTL_overflow", now.Add(time.Minute + time.Nanosecond), now.Add(2 * time.Minute)},
		{"expiry_exact_issue", now, now.Add(time.Minute)}, {"expiry_before_issue", now.Add(-time.Nanosecond), now.Add(time.Minute)},
		{"beyond_authority", now.Add(30 * time.Second), now.Add(29 * time.Second)},
		{"missing_expiry", time.Time{}, now.Add(time.Minute)}, {"missing_authority", now.Add(time.Second), time.Time{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			token, err := codec.Encode(testScope(), registry, InputText, 4, now, test.expires, test.authority)
			if token != "" || err != ErrChoiceStale {
				t.Fatalf("token=%q err=%v", token, err)
			}
		})
	}
	short := now.Add(10 * time.Second)
	if token, err := codec.Encode(testScope(), registry, InputText, 4, now, short, short); err != nil {
		t.Fatal(err)
	} else if position, err := codec.Decode(token, testScope(), registry, InputText, short.Add(-time.Nanosecond), short); err != nil || position != 4 {
		t.Fatalf("short valid window %d %v", position, err)
	}
	for _, position := range []uint32{0, 9, 10, ^uint32(0)} {
		if token, err := codec.Encode(testScope(), registry, InputText, position, now, now.Add(time.Second), now.Add(time.Minute)); token != "" || err != ErrChoiceStale {
			t.Errorf("position%d: %q %v", position, token, err)
		}
	}
}

func TestCursorStrictEncodingAndAuthenticatedMalformedPayload(t *testing.T) {
	codec, registry, now := cursorFixture(t)
	token := mustCursor(t, codec, registry, now)
	mutants := []string{"", token + "=", token + "\n", token[:len(token)-1], strings.Repeat("x", 513), token[:3] + "+" + token[4:]}
	for index := range token {
		mutant := []byte(token)
		if mutant[index] == 'A' {
			mutant[index] = 'B'
		} else {
			mutant[index] = 'A'
		}
		mutants = append(mutants, string(mutant))
	}
	other, _ := NewCursorCodec(bytes.Repeat([]byte{8}, 32))
	position, err := other.Decode(token, testScope(), registry, InputText, now, now.Add(time.Minute))
	assertStale(t, position, err)
	for _, mutant := range mutants {
		position, err := codec.Decode(mutant, testScope(), registry, InputText, now, now.Add(time.Minute))
		assertStale(t, position, err)
	}
	// Even validly MACed malformed fields may not bypass shape/time bounds.
	for _, mutate := range []func([]byte){
		func(b []byte) { b[0] = 2 }, func(b []byte) { b[65] = 0 }, func(b []byte) { b[66] = 0 }, func(b []byte) { b[66] = 9 }, func(b []byte) { b[66] = 255 },
		func(b []byte) { binary.BigEndian.PutUint32(b[75:79], 1_000_000_000) },
		func(b []byte) { putCursorTime(b[67:79], now.Add(time.Second)) },
		func(b []byte) { putCursorTime(b[79:91], now.Add(time.Minute+time.Nanosecond)) },
		func(b []byte) { putCursorTime(b[79:91], now) },
	} {
		encoded, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil {
			t.Fatal(err)
		}
		mutate(encoded)
		copy(encoded[cursorPayloadBytes:], codec.mac(encoded[:cursorPayloadBytes]))
		position, err := codec.Decode(base64.RawURLEncoding.EncodeToString(encoded), testScope(), registry, InputText, now, now.Add(2*time.Minute))
		assertStale(t, position, err)
	}
	position, err = (*CursorCodec)(nil).Decode(token, testScope(), registry, InputText, now, now.Add(time.Minute))
	assertStale(t, position, err)
	position, err = new(CursorCodec).Decode(token, testScope(), registry, InputText, now, now.Add(time.Minute))
	assertStale(t, position, err)
	for _, unconfigured := range []*CursorCodec{nil, new(CursorCodec)} {
		if token, err := unconfigured.Encode(testScope(), registry, InputText, 4, now, now.Add(time.Minute), now.Add(time.Minute)); token != "" || err != ErrChoiceStale {
			t.Errorf("unconfigured codec token=%q err=%v", token, err)
		}
	}
}

func TestCursorMaximumScopeHasFixedSize(t *testing.T) {
	codec, _, now := cursorFixture(t)
	scope := Scope{TenantID: domain.TenantID(strings.Repeat("t", 160)), UserID: domain.UserID(strings.Repeat("u", 160)), SessionID: domain.SessionID(strings.Repeat("s", 160)), MembershipSecurityVersion: ^uint64(0)}
	entries := []Registration{
		{TenantID: scope.TenantID, OwnerUserID: scope.UserID, ChoiceID: strings.Repeat("c", 128), RegistrationRevision: ^uint64(0), DisclosureRevision: strings.Repeat("f", 64)},
		{TenantID: scope.TenantID, OwnerUserID: scope.UserID, ChoiceID: strings.Repeat("d", 128), RegistrationRevision: ^uint64(0), DisclosureRevision: strings.Repeat("f", 64)},
	}
	registry := mustScoped(t, scope, entries)
	token, err := codec.Encode(scope, registry, InputMixed, 1, now, now.Add(time.Minute), now.Add(time.Minute))
	if err != nil || len(token) != 164 || len(token) > MaxCursorBytes {
		t.Fatalf("maximum scope token length=%d err=%v", len(token), err)
	}
	position, err := codec.Decode(token, scope, registry, InputMixed, now, now.Add(time.Minute))
	if position != 1 || err != nil {
		t.Fatalf("maximum scope position=%d err=%v", position, err)
	}
}

func FuzzCursorDecode(f *testing.F) {
	f.Add("")
	f.Add(strings.Repeat("A", 164))
	f.Add(strings.Repeat("A", 513))
	codec, registry, now := cursorFixture(f)
	token, err := codec.Encode(testScope(), registry, InputText, 4, now, now.Add(time.Minute), now.Add(time.Minute))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(token)
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > MaxCursorBytes+1 {
			return
		}
		position, err := codec.Decode(value, testScope(), registry, InputText, now, now.Add(time.Minute))
		if err != nil {
			assertStale(t, position, err)
		} else if value != token || position != 4 {
			t.Fatalf("unexpected accepted cursor position=%d", position)
		}
	})
}
