package domain_test

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestIdentityAuthV2CodecsLosslessReorderedRoundTrip(t *testing.T) {
	for _, revision := range []domain.AuthRevisionV2{1, 1<<53 - 1, 1 << 53, 1<<53 + 1, math.MaxUint64} {
		t.Run(revision.String(), func(t *testing.T) {
			identity := identityV2Fixture(domain.IdentityProviderYandex, revision)
			raw, err := domain.EncodeExternalIdentityV2(identity)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(raw, []byte(`"security_revision":"`+revision.String()+`"`)) {
				t.Fatalf("identity codec emitted non-string revision: %s", raw)
			}
			for _, reordered := range [][]byte{raw, reorderIdentityAuthJSONV2(t, raw)} {
				decoded, err := domain.DecodeExternalIdentityV2(reordered)
				if err != nil || !reflect.DeepEqual(decoded, identity) {
					t.Errorf("identity roundtrip = (%+v, %v), want %+v", decoded, err, identity)
				}
			}
			session, _, _, _ := webSessionV2Fixture()
			session.IdentityRevision = revision
			raw, err = domain.EncodeWebSessionV2(session)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(raw, []byte(`"identity_revision":"`+revision.String()+`"`)) {
				t.Fatalf("session codec emitted non-string revision: %s", raw)
			}
			for _, reordered := range [][]byte{raw, reorderIdentityAuthJSONV2(t, raw)} {
				decoded, err := domain.DecodeWebSessionV2(reordered)
				if err != nil || !reflect.DeepEqual(decoded, session) {
					t.Errorf("session roundtrip = (%+v, %v), want %+v", decoded, err, session)
				}
			}
		})
	}
}

func reorderIdentityAuthJSONV2(t *testing.T, raw []byte) []byte {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	reordered, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return reordered
}

func TestExternalIdentityV2CodecRejectsOpenOrLegacyRecords(t *testing.T) {
	identity := identityV2Fixture(domain.IdentityProviderYandex, 7)
	raw, err := domain.EncodeExternalIdentityV2(identity)
	if err != nil {
		t.Fatal(err)
	}
	valid := string(raw)
	for _, test := range []struct {
		name string
		raw  string
	}{
		{name: "unknown", raw: strings.TrimSuffix(valid, "}") + `,"extra":true}`},
		{name: "case alias", raw: strings.Replace(valid, `"version"`, `"Version"`, 1)},
		{name: "duplicate", raw: strings.Replace(valid, `"version":2`, `"version":2,"version":2`, 1)},
		{name: "escaped duplicate", raw: strings.Replace(valid, `"version":2`, `"version":2,"\u0076ersion":2`, 1)},
		{name: "nested duplicate", raw: strings.Replace(valid, `"provider":"yandex"`, `"provider":"yandex","provider":"telegram"`, 1)},
		{name: "nested unknown", raw: strings.Replace(valid, `"provider":"yandex"`, `"provider":"yandex","issuer":"https://wrong.invalid"`, 1)},
		{name: "nested case alias", raw: strings.Replace(valid, `"provider"`, `"Provider"`, 1)},
		{name: "wrong version", raw: strings.Replace(valid, `"version":2`, `"version":1`, 1)},
		{name: "missing version", raw: strings.Replace(valid, `"version":2,`, ``, 1)},
		{name: "missing state", raw: strings.Replace(valid, `"state":"active",`, ``, 1)},
		{name: "null subject", raw: strings.Replace(valid, `{"provider":"yandex","subject":"123"}`, `null`, 1)},
		{name: "null revision", raw: strings.Replace(valid, `"security_revision":"7"`, `"security_revision":null`, 1)},
		{name: "number revision", raw: strings.Replace(valid, `"security_revision":"7"`, `"security_revision":7`, 1)},
		{name: "alternate revision key", raw: strings.Replace(valid, `"security_revision"`, `"revision"`, 1)},
		{name: "escaped revision value", raw: strings.Replace(valid, `"security_revision":"7"`, `"security_revision":"\u0037"`, 1)},
		{name: "trailing data", raw: valid + `{}`},
		{name: "invalid surrogate", raw: strings.Replace(valid, `"subject":"123"`, `"subject":"\ud800"`, 1)},
		{name: "invalid UTF8", raw: strings.Replace(valid, `"subject":"123"`, "\"subject\":\"\xff\"", 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			decoded, err := domain.DecodeExternalIdentityV2([]byte(test.raw))
			if err == nil || !reflect.DeepEqual(decoded, domain.ExternalIdentityV2{}) {
				t.Errorf("DecodeExternalIdentityV2(%q) = (%+v, %v), want zero/error", test.raw, decoded, err)
			}
			decoded = identity
			if err := json.Unmarshal([]byte(test.raw), &decoded); err == nil {
				t.Errorf("ordinary json.Unmarshal bypassed V2 rejection for %q", test.raw)
			}
		})
	}
	legacy := domain.ExternalIdentity{Subject: identity.Subject, UserID: identity.UserID, CreatedAt: identity.CreatedAt, UpdatedAt: identity.UpdatedAt}
	legacyRaw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := domain.DecodeExternalIdentityV2(legacyRaw); err == nil || !reflect.DeepEqual(decoded, domain.ExternalIdentityV2{}) {
		t.Errorf("legacy identity accepted as V2: (%+v, %v)", decoded, err)
	}
}

func TestWebSessionV2CodecRejectsNestedAliasesAndLegacy(t *testing.T) {
	session, _, _, _ := webSessionV2Fixture()
	raw, err := domain.EncodeWebSessionV2(session)
	if err != nil {
		t.Fatal(err)
	}
	valid := string(raw)
	for _, test := range []struct {
		name string
		raw  string
	}{
		{name: "wrong version", raw: strings.Replace(valid, `"version":2`, `"version":1`, 1)},
		{name: "unknown", raw: strings.TrimSuffix(valid, "}") + `,"extra":true}`},
		{name: "duplicate", raw: strings.Replace(valid, `"identity_revision":"7"`, `"identity_revision":"7","identity_revision":"7"`, 1)},
		{name: "number revision", raw: strings.Replace(valid, `"identity_revision":"7"`, `"identity_revision":7`, 1)},
		{name: "nested unknown", raw: strings.Replace(valid, `"session":{`, `"session":{"extra":true,`, 1)},
		{name: "nested case alias", raw: strings.Replace(valid, `"user_id"`, `"User_ID"`, 1)},
		{name: "nested duplicate", raw: strings.Replace(valid, `"session":{`, `"session":{"user_id":"foreign",`, 1)},
		{name: "deep duplicate", raw: strings.Replace(valid, `"provider":"yandex"`, `"provider":"yandex","provider":"telegram"`, 1)},
		{name: "null session", raw: `{"version":2,"session":null,"identity_revision":"7"}`},
		{name: "null timestamp", raw: strings.Replace(valid, `"last_seen_at":"2026-10-26T10:00:00Z"`, `"last_seen_at":null`, 1)},
		{name: "null optional revoked", raw: strings.Replace(valid, `"session":{`, `"session":{"revoked_at":null,`, 1)},
		{name: "missing session field", raw: strings.Replace(valid, `"user_id":"user-v2",`, ``, 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			decoded, err := domain.DecodeWebSessionV2([]byte(test.raw))
			if err == nil || !reflect.DeepEqual(decoded, domain.WebSessionV2{}) {
				t.Errorf("DecodeWebSessionV2(%q) = (%+v, %v), want zero/error", test.raw, decoded, err)
			}
			if err := json.Unmarshal([]byte(test.raw), &decoded); err == nil {
				t.Errorf("ordinary json.Unmarshal bypassed V2 rejection for %q", test.raw)
			}
		})
	}
	legacy, err := json.Marshal(session.Session)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := domain.DecodeWebSessionV2(legacy); err == nil || !reflect.DeepEqual(decoded, domain.WebSessionV2{}) {
		t.Errorf("legacy WebSession accepted as V2: (%+v, %v)", decoded, err)
	}
}

func TestIdentityAuthV2CodecBoundAndFailClosedReceivers(t *testing.T) {
	identity := identityV2Fixture(domain.IdentityProviderYandex, 7)
	raw, err := domain.EncodeExternalIdentityV2(identity)
	if err != nil {
		t.Fatal(err)
	}
	boundary := append(append([]byte(nil), raw...), bytes.Repeat([]byte{' '}, domain.IdentityAuthMaxRecordBytesV2-len(raw))...)
	if decoded, err := domain.DecodeExternalIdentityV2(boundary); err != nil || !reflect.DeepEqual(decoded, identity) {
		t.Errorf("exact8KiB record = (%+v, %v), want original", decoded, err)
	}
	if decoded, err := domain.DecodeExternalIdentityV2(append(boundary, ' ')); err == nil || !reflect.DeepEqual(decoded, domain.ExternalIdentityV2{}) {
		t.Errorf("oversize record = (%+v, %v), want zero/error", decoded, err)
	}
	if err := identity.UnmarshalJSON([]byte(`{}`)); err == nil || !reflect.DeepEqual(identity, domain.ExternalIdentityV2{}) {
		t.Errorf("failed receiver decode retained identity: (%+v, %v)", identity, err)
	}
	session, _, _, _ := webSessionV2Fixture()
	if err := session.UnmarshalJSON([]byte(`{}`)); err == nil || !reflect.DeepEqual(session, domain.WebSessionV2{}) {
		t.Errorf("failed receiver decode retained session: (%+v, %v)", session, err)
	}
	if _, err := domain.EncodeExternalIdentityV2(domain.ExternalIdentityV2{}); err == nil {
		t.Error("encoded invalid identity")
	}
	if _, err := domain.EncodeWebSessionV2(domain.WebSessionV2{}); err == nil {
		t.Error("encoded invalid session")
	}
	var nilIdentity *domain.ExternalIdentityV2
	var nilSession *domain.WebSessionV2
	if nilIdentity.UnmarshalJSON(raw) == nil || nilSession.UnmarshalJSON(raw) == nil {
		t.Error("nil record receiver accepted input")
	}
}

func FuzzIdentityAuthV2ClosedDecoders(f *testing.F) {
	identity := identityV2Fixture(domain.IdentityProviderYandex, 7)
	raw, err := domain.EncodeExternalIdentityV2(identity)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add([]byte(`{"version":2,"version":2}`))
	f.Add([]byte(`{"version":1}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		value, err := domain.DecodeExternalIdentityV2(raw)
		if err != nil {
			if !reflect.DeepEqual(value, domain.ExternalIdentityV2{}) {
				t.Fatalf("decoder leaked usable partial value on error: %+v", value)
			}
			return
		}
		if len(raw) > domain.IdentityAuthMaxRecordBytesV2 || value.Validate() != nil {
			t.Fatalf("accepted unbounded/invalid value %+v", value)
		}
		canonical, err := domain.EncodeExternalIdentityV2(value)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := domain.DecodeExternalIdentityV2(canonical)
		if err != nil || !reflect.DeepEqual(decoded, value) {
			t.Fatalf("accepted value failed roundtrip: (%+v, %v), want %+v", decoded, err, value)
		}
	})
}
