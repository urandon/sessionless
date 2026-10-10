package computechoice

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"
	"testing"
	"time"
)

func TestParseListQueryClosedProtocol(t *testing.T) {
	for _, test := range []struct {
		name, raw string
		want      ListQuery
	}{
		{"defaults", "", ListQuery{Limit: 4, InputKind: InputText}},
		{"limit_one", "limit=1", ListQuery{Limit: 1, InputKind: InputText}},
		{"text", "input_kind=text&limit=2", ListQuery{Limit: 2, InputKind: InputText}},
		{"image", "limit=3&input_kind=image", ListQuery{Limit: 3, InputKind: InputImage}},
		{"file", "input_kind=file", ListQuery{Limit: 4, InputKind: InputFile}},
		{"mixed", "input_kind=mixed&limit=4", ListQuery{Limit: 4, InputKind: InputMixed}},
		{"escaping", "%6cimit=%31&input_kind=%74ext", ListQuery{Limit: 1, InputKind: InputText}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseListQuery(test.raw)
			if err != nil || got != test.want {
				t.Fatalf("ParseListQuery(%q)=%+v,%v; want %+v,nil", test.raw, got, err, test.want)
			}
		})
	}
}

func assertInvalidListQuery(t testing.TB, raw string) {
	t.Helper()
	got, err := ParseListQuery(raw)
	if got != (ListQuery{}) || err != ErrInvalidQuery || err.Error() != "compute choice query invalid" {
		t.Fatalf("ParseListQuery raw length=%d: got %+v,%v; want zero/content-free ErrInvalidQuery", len(raw), got, err)
	}
}

func TestParseListQueryRejectsMalformedAndAuthorityFields(t *testing.T) {
	for _, raw := range []string{
		"limit", "limit=", "cursor=", "input_kind=", "=1", "&", "&limit=1", "limit=1&", "limit=1&&input_kind=text",
		"limit=1&limit=1", "limit=1&%6cimit=2", "input_kind=text&input_kind=text", "cursor=abc&cursor=abc",
		"limit=0", "limit=5", "limit=04", "limit=+4", "limit=%2B4", "limit=-1", "limit=1.0", "limit= 1", "limit=1=1",
		"limit=4294967296", "input_kind=Text", "input_kind=unknown", "input_kind=text%00", "input_kind=text+",
		"limit=%", "limit=%GG", "%GG=1", "limit=1;input_kind=text", "limit=1%3B", "cursor=abc",
		"tenant_id=foreign", "user_id=foreign", "owner_id=foreign", "session_id=foreign", "resource_id=guessed",
		"model_id=guessed", "policy=allow", "consent=true", "sort=resource", "limit=1&approved=true",
		strings.Repeat("x", MaxListQueryBytes), strings.Repeat("x", MaxListQueryBytes+1),
	} {
		t.Run(raw[:min(len(raw), 48)], func(t *testing.T) { assertInvalidListQuery(t, raw) })
	}
}

func TestListQueryCursorSyntaxVersusAuthenticatedStaleness(t *testing.T) {
	codec, registry, now := cursorFixture(t)
	token := mustCursor(t, codec, registry, now)
	encoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"MAC_tamper", func(b []byte) { b[len(b)-1] ^= 1 }},
		{"foreign_scope", func(b []byte) { b[1] ^= 1 }},
		{"foreign_revision", func(b []byte) { b[33] ^= 1 }},
		{"hint_changed", func(b []byte) { b[65] = 2 }},
		{"position_global_max", func(b []byte) { b[66] = MaxRegistrations - 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutant := append([]byte(nil), encoded...)
			test.mutate(mutant)
			shaped := base64.RawURLEncoding.EncodeToString(mutant)
			query, err := ParseListQuery("cursor=" + shaped)
			if err != nil || query.Cursor != shaped {
				t.Fatalf("well-formed stale cursor parsed as syntax failure: %+v %v", query, err)
			}
			position, err := codec.Decode(shaped, testScope(), registry, InputText, now, now.Add(time.Minute))
			assertStale(t, position, err)
		})
	}
	// A valid token's current expiry is not query syntax, and neither is a
	// mismatch between its authenticated input hint and the new query hint.
	query, err := ParseListQuery("cursor=" + token + "&input_kind=file")
	if err != nil {
		t.Fatal(err)
	}
	position, err := codec.Decode(query.Cursor, testScope(), registry, query.InputKind, now, now.Add(time.Minute))
	assertStale(t, position, err)
	if _, err := ParseListQuery("cursor=" + token); err != nil {
		t.Fatal(err)
	}
	position, err = codec.Decode(token, testScope(), registry, InputText, now.Add(time.Minute), now.Add(2*time.Minute))
	assertStale(t, position, err)
}

func TestListQueryCursorStructuralRejection(t *testing.T) {
	codec, registry, now := cursorFixture(t)
	token := mustCursor(t, codec, registry, now)
	encoded, _ := base64.RawURLEncoding.DecodeString(token)
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"version_zero", func(b []byte) { b[0] = 0 }}, {"version_two", func(b []byte) { b[0] = 2 }},
		{"kind_zero", func(b []byte) { b[65] = 0 }}, {"kind_unknown", func(b []byte) { b[65] = 5 }},
		{"position_zero", func(b []byte) { b[66] = 0 }}, {"position_terminal", func(b []byte) { b[66] = 64 }}, {"position_overflow", func(b []byte) { b[66] = 255 }},
		{"issued_nanos", func(b []byte) { binary.BigEndian.PutUint32(b[75:79], 1_000_000_000) }},
		{"expiry_nanos", func(b []byte) { binary.BigEndian.PutUint32(b[87:91], 1_000_000_000) }},
		{"issued_year_zero", func(b []byte) { putCursorTime(b[67:79], time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)) }},
		{"expiry_year_10000", func(b []byte) { putCursorTime(b[79:91], time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)) }},
		{"seconds_overflow", func(b []byte) { binary.BigEndian.PutUint64(b[67:75], math.MaxInt64) }},
		{"zero_window", func(b []byte) { putCursorTime(b[79:91], now) }},
		{"negative_window", func(b []byte) { putCursorTime(b[79:91], now.Add(-time.Nanosecond)) }},
		{"TTL_overflow", func(b []byte) { putCursorTime(b[79:91], now.Add(MaxCursorLifetime+time.Nanosecond)) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutant := append([]byte(nil), encoded...)
			test.mutate(mutant)
			assertInvalidListQuery(t, "cursor="+base64.RawURLEncoding.EncodeToString(mutant))
		})
	}
	for _, malformed := range []string{token + "=", token[:163], strings.Repeat("A", 165), token[:3] + "+" + token[4:], token[:3] + "/" + token[4:], token[:3] + "\n" + token[4:]} {
		assertInvalidListQuery(t, "cursor="+malformed)
	}
	// RawURL does not allow padding, newline tolerance or standard-base64
	// aliases, even when some decoding routines would accept them.
	assertInvalidListQuery(t, "cursor="+token+"%3D")
}

func TestListQueryDigestCanonicalAndExact(t *testing.T) {
	base, err := ParseListQuery("")
	if err != nil {
		t.Fatal(err)
	}
	digest, err := base.Digest("session-a")
	if err != nil || len(digest) != 64 {
		t.Fatalf("Digest: %q %v", digest, err)
	}
	// Independent canonical framing verifies version/domain and all fields,
	// rather than merely comparing the implementation with itself.
	canonical := sha256.New()
	for _, field := range [][]byte{[]byte("sessionless.compute-choice.list-query.v1"), []byte("session-a"), {0, 0, 0, 0, 0, 0, 0, 4}, []byte("text"), nil} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(field)))
		canonical.Write(size[:])
		canonical.Write(field)
	}
	if want := hex.EncodeToString(canonical.Sum(nil)); digest != want {
		t.Fatalf("canonical digest=%q want %q", digest, want)
	}
	for _, raw := range []string{"limit=4&input_kind=text", "input_kind=text&limit=4", "%6cimit=%34&input_kind=%74ext"} {
		query, err := ParseListQuery(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := query.Digest("session-a"); err != nil || got != digest {
			t.Errorf("normalized raw%q digest=%q err=%v", raw, got, err)
		}
	}
	codec, registry, now := cursorFixture(t)
	withCursor := base
	withCursor.Cursor = mustCursor(t, codec, registry, now)
	seen := map[string]bool{digest: true}
	for _, query := range []ListQuery{{Limit: 1, InputKind: InputText}, {Limit: 4, InputKind: InputImage}, {Limit: 4, InputKind: InputFile}, {Limit: 4, InputKind: InputMixed}, withCursor} {
		got, err := query.Digest("session-a")
		if err != nil || seen[got] {
			t.Fatalf("field change not sealed: %+v digest%q err%v", query, got, err)
		}
		seen[got] = true
	}
	if got, err := base.Digest("session-b"); err != nil || seen[got] {
		t.Errorf("Session change not sealed: %q %v", got, err)
	}
	mutantBytes, _ := base64.RawURLEncoding.DecodeString(withCursor.Cursor)
	mutantBytes[122] ^= 1
	withCursor.Cursor = base64.RawURLEncoding.EncodeToString(mutantBytes)
	if got, err := withCursor.Digest("session-a"); err != nil || seen[got] {
		t.Errorf("exact well-formed cursor change not sealed: %q %v", got, err)
	}
	for _, query := range []ListQuery{{}, {Limit: 5, InputKind: InputText}, {Limit: 1, InputKind: "unknown"}, {Limit: 1, InputKind: InputText, Cursor: "private"}} {
		if got, err := query.Digest("session-a"); got != "" || err != ErrInvalidQuery {
			t.Errorf("invalid query minted digest: %q %v", got, err)
		}
	}
	if got, err := base.Digest(""); got != "" || err != ErrInvalidQuery {
		t.Errorf("invalid Session minted digest: %q %v", got, err)
	}
}

func FuzzParseListQuery(f *testing.F) {
	for _, seed := range []string{"", "limit=4", "input_kind=mixed", "limit=1&limit=2", "tenant_id=private", "cursor=abc", "limit=%GG", strings.Repeat("x", MaxListQueryBytes+1)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		query, err := ParseListQuery(raw)
		if err != nil {
			if err != ErrInvalidQuery || query != (ListQuery{}) {
				t.Fatalf("nonuniform parse failure: %+v %v", query, err)
			}
			return
		}
		if len(raw) > MaxListQueryBytes || query.Validate() != nil {
			t.Fatalf("accepted unbounded/invalid query length%d %+v", len(raw), query)
		}
		if digest, err := query.Digest("fuzz-session"); err != nil || len(digest) != 64 {
			t.Fatalf("accepted query cannot bind digest: %q %v", digest, err)
		}
	})
}
