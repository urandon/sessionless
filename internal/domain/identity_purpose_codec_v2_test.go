package domain_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestIdentityPurposeCodecV2RejectsAmbiguousOperation(t *testing.T) {
	operation := operationV2Fixture(domain.IdentityOperationLinkV2)
	operation.Pending.Scope.ExpectedRevision = domain.AuthRevisionV2(1<<53 + 1)
	raw, err := domain.EncodeIdentityOperationV2(operation)
	if err != nil {
		t.Fatal(err)
	}
	for name, encoded := range map[string]string{
		"duplicate_root":     strings.Replace(string(raw), `"version":2`, `"version":2,"version":2`, 1),
		"wrong_case_root":    strings.Replace(string(raw), `"purpose"`, `"Purpose"`, 1),
		"duplicate_scope":    strings.Replace(string(raw), `"expected_revision":"9007199254740993"`, `"expected_revision":"9007199254740993","expected_revision":"9007199254740993"`, 1),
		"numeric_revision":   strings.Replace(string(raw), `"expected_revision":"9007199254740993"`, `"expected_revision":9007199254740993`, 1),
		"alternate_revision": strings.Replace(string(raw), `"9007199254740993"`, `"09007199254740993"`, 1),
		"unknown_secret":     strings.TrimSuffix(string(raw), "}") + `,"access_token":"forbidden"}`,
		"trailing_value":     string(raw) + `{}`,
		"oversized":          string(raw) + strings.Repeat(" ", domain.IdentityAuthMaxRecordBytesV2),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := domain.DecodeIdentityOperationV2([]byte(encoded))
			if err == nil || !reflect.DeepEqual(got, domain.IdentityOperationV2{}) {
				t.Errorf("ambiguous operation returned partial authority: got=%+v err=%v", got, err)
			}
		})
	}
}

func TestIdentityPurposeCodecV2RejectsAmbiguousChallenge(t *testing.T) {
	operation := operationV2Fixture(domain.IdentityOperationLinkV2)
	challenge := operationV2ProofChallenge(operation)
	raw, err := domain.EncodeIdentityChallengeV2(challenge)
	if err != nil {
		t.Fatal(err)
	}
	for name, encoded := range map[string]string{
		"duplicate_nested_binding": strings.Replace(string(raw), `"client_id":`, `"client_id":"extra","client_id":`, 1),
		"wrong_case_nested":        strings.Replace(string(raw), `"callback_uri"`, `"Callback_URI"`, 1),
		"numeric_scope_revision":   strings.Replace(string(raw), `"expected_revision":"7"`, `"expected_revision":7`, 1),
		"mixed_purpose":            strings.TrimSuffix(string(raw), "}") + `,"login":{"return_path":"/"}}`,
		"unknown_code":             strings.TrimSuffix(string(raw), "}") + `,"code":"forbidden"}`,
		"null_proof":               strings.Replace(string(raw), `"proof":{`, `"proof":null,"extra":{`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := domain.DecodeIdentityChallengeV2([]byte(encoded))
			if err == nil || !reflect.DeepEqual(got, domain.IdentityChallengeV2{}) {
				t.Errorf("ambiguous challenge returned partial exchange: got=%+v err=%v", got, err)
			}
		})
	}
}

func FuzzIdentityPurposeCodecV2(f *testing.F) {
	operation := operationV2Fixture(domain.IdentityOperationLinkV2)
	for _, value := range []any{operation, operationV2ProofChallenge(operation)} {
		raw, err := json.Marshal(value)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > domain.IdentityAuthMaxRecordBytesV2+1 {
			return
		}
		if operation, err := domain.DecodeIdentityOperationV2(raw); err == nil {
			encoded, err := domain.EncodeIdentityOperationV2(operation)
			if err != nil {
				t.Fatal("accepted operation cannot be encoded", err)
			}
			got, err := domain.DecodeIdentityOperationV2(encoded)
			if err != nil || !reflect.DeepEqual(got, operation) {
				t.Fatal("accepted operation cannot roundtrip", err)
			}
		}
		if challenge, err := domain.DecodeIdentityChallengeV2(raw); err == nil {
			encoded, err := domain.EncodeIdentityChallengeV2(challenge)
			if err != nil {
				t.Fatal("accepted challenge cannot be encoded", err)
			}
			got, err := domain.DecodeIdentityChallengeV2(encoded)
			if err != nil || !reflect.DeepEqual(got, challenge) {
				t.Fatal("accepted challenge cannot roundtrip", err)
			}
		}
	})
}
