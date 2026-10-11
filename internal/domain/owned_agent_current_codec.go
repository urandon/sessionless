package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

const OwnedAgentCurrentMaxRecordBytesV1 = 32 << 10

func EncodeOwnedAgentProfileCurrentV1(v OwnedAgentProfileCurrentV1) ([]byte, error) {
	v = v.Clone()
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return encodeOwnedCurrent(v)
}

func DecodeOwnedAgentProfileCurrentV1(raw []byte) (OwnedAgentProfileCurrentV1, error) {
	var v OwnedAgentProfileCurrentV1
	if err := decodeOwnedCurrent(raw, &v); err != nil {
		return OwnedAgentProfileCurrentV1{}, err
	}
	expected, err := EncodeOwnedAgentProfileCurrentV1(v)
	if err != nil || !bytes.Equal(raw, expected) {
		return OwnedAgentProfileCurrentV1{}, ownedCurrentError("codec", "requires exact canonical profile producer bytes")
	}
	return v.Clone(), nil
}

func EncodeAttemptSpendingAuthorizationCurrentV1(v AttemptSpendingAuthorizationCurrentV1) ([]byte, error) {
	v = v.Clone()
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return encodeOwnedCurrent(v)
}

func DecodeAttemptSpendingAuthorizationCurrentV1(raw []byte) (AttemptSpendingAuthorizationCurrentV1, error) {
	var v AttemptSpendingAuthorizationCurrentV1
	if err := decodeOwnedCurrent(raw, &v); err != nil {
		return AttemptSpendingAuthorizationCurrentV1{}, err
	}
	expected, err := EncodeAttemptSpendingAuthorizationCurrentV1(v)
	if err != nil || !bytes.Equal(raw, expected) {
		return AttemptSpendingAuthorizationCurrentV1{}, ownedCurrentError("codec", "requires exact canonical spending producer bytes")
	}
	return v.Clone(), nil
}

func encodeOwnedCurrent(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil || len(raw) == 0 || len(raw) > OwnedAgentCurrentMaxRecordBytesV1 {
		return nil, ownedCurrentError("codec", "cannot encode bounded authority record")
	}
	return raw, nil
}

// Persisted String records have exactly one producer format. Comparing decoded
// canonical bytes rejects omitted fields, null substitution, duplicate nested
// keys, case aliases, whitespace, alternate numeric/time forms and field-order
// variants; DisallowUnknownFields independently rejects unknown members. No
// decoder failure returns a partial usable authority, even after a valid prefix.
func decodeOwnedCurrent(raw []byte, target any) error {
	if len(raw) == 0 || len(raw) > OwnedAgentCurrentMaxRecordBytesV1 || !utf8.Valid(raw) {
		return ownedCurrentError("codec", "requires bounded UTF-8 authority bytes")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return ownedCurrentError("codec", "invalid authority record")
	}
	var trailing any
	if err := d.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ownedCurrentError("codec", "trailing authority data")
	}
	return nil
}
