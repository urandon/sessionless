package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

const IdentityAuthMaxRecordBytesV2 = 8 << 10

func EncodeExternalIdentityV2(identity ExternalIdentityV2) ([]byte, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	return encodeIdentityAuthV2(identity)
}

func DecodeExternalIdentityV2(raw []byte) (ExternalIdentityV2, error) {
	var identity ExternalIdentityV2
	if err := identity.UnmarshalJSON(raw); err != nil {
		return ExternalIdentityV2{}, err
	}
	return identity, nil
}

func (identity *ExternalIdentityV2) UnmarshalJSON(raw []byte) error {
	if identity == nil {
		return invalidIdentityAuthJSONV2()
	}
	*identity = ExternalIdentityV2{}
	type wire ExternalIdentityV2
	var decoded wire
	if err := decodeIdentityAuthV2(raw, "identity", &decoded); err != nil {
		return err
	}
	value := ExternalIdentityV2(decoded)
	if err := value.Validate(); err != nil {
		return err
	}
	*identity = value
	return nil
}

func EncodeWebSessionV2(session WebSessionV2) ([]byte, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return encodeIdentityAuthV2(session)
}

func DecodeWebSessionV2(raw []byte) (WebSessionV2, error) {
	var session WebSessionV2
	if err := session.UnmarshalJSON(raw); err != nil {
		return WebSessionV2{}, err
	}
	return session, nil
}

func (session *WebSessionV2) UnmarshalJSON(raw []byte) error {
	if session == nil {
		return invalidIdentityAuthJSONV2()
	}
	*session = WebSessionV2{}
	type wire WebSessionV2
	var decoded wire
	if err := decodeIdentityAuthV2(raw, "session_v2", &decoded); err != nil {
		return err
	}
	value := WebSessionV2(decoded)
	if err := value.Validate(); err != nil {
		return err
	}
	*session = value
	return nil
}

func encodeIdentityAuthV2(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || len(raw) > IdentityAuthMaxRecordBytesV2 {
		return nil, invalidIdentityAuthJSONV2()
	}
	return raw, nil
}

// JsonDocument may reorder members. Validate decoded EXACT contextual key names
// and duplicates instead of assuming storage preserves producer byte order.
func decodeIdentityAuthV2(raw []byte, shape string, target any) error {
	if len(raw) == 0 || len(raw) > IdentityAuthMaxRecordBytesV2 || !identityAuthUnicodeValidV2(raw) {
		return invalidIdentityAuthJSONV2()
	}
	preflight := json.NewDecoder(bytes.NewReader(raw))
	preflight.UseNumber()
	if err := identityAuthObjectV2(preflight, shape); err != nil {
		return err
	}
	if _, err := preflight.Token(); !errors.Is(err, io.EOF) {
		return invalidIdentityAuthJSONV2()
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return invalidIdentityAuthJSONV2()
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return invalidIdentityAuthJSONV2()
	}
	return nil
}

type identityAuthJSONFieldV2 struct {
	shape    string
	optional bool
}

func identityAuthFieldsV2(shape string) map[string]identityAuthJSONFieldV2 {
	scalar := identityAuthJSONFieldV2{shape: "scalar"}
	switch shape {
	case "identity":
		return map[string]identityAuthJSONFieldV2{"version": scalar, "subject": {shape: "subject"}, "user_id": scalar, "created_at": scalar, "state": scalar, "security_revision": scalar, "updated_at": scalar}
	case "subject":
		return map[string]identityAuthJSONFieldV2{"provider": scalar, "subject": scalar}
	case "session_v2":
		return map[string]identityAuthJSONFieldV2{"version": scalar, "session": {shape: "session"}, "identity_revision": scalar}
	case "session":
		return map[string]identityAuthJSONFieldV2{"session_digest": scalar, "csrf_token_digest": scalar, "user_id": scalar, "active_tenant_id": scalar, "authenticated_subject": {shape: "subject"}, "membership_security_version": scalar, "issued_at": scalar, "last_seen_at": scalar, "idle_expires_at": scalar, "absolute_expires_at": scalar, "revoked_at": {shape: "scalar", optional: true}}
	default:
		return nil
	}
}

func identityAuthObjectV2(decoder *json.Decoder, shape string) error {
	fields := identityAuthFieldsV2(shape)
	if fields == nil {
		return invalidIdentityAuthJSONV2()
	}
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return invalidIdentityAuthJSONV2()
	}
	seen := make(map[string]bool, len(fields))
	for decoder.More() {
		token, err := decoder.Token()
		name, ok := token.(string)
		field, known := fields[name]
		if err != nil || !ok || !known || seen[name] {
			return invalidIdentityAuthJSONV2()
		}
		seen[name] = true
		if field.shape != "scalar" {
			if err := identityAuthObjectV2(decoder, field.shape); err != nil {
				return err
			}
		} else {
			token, err := decoder.Token()
			_, container := token.(json.Delim)
			if err != nil || token == nil || container {
				return invalidIdentityAuthJSONV2()
			}
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return invalidIdentityAuthJSONV2()
	}
	for name, field := range fields {
		if !field.optional && !seen[name] {
			return invalidIdentityAuthJSONV2()
		}
	}
	return nil
}

// Reject the invalid UTF-8/unpaired-surrogate replacement performed by the
// standard decoder before it can normalize different ownership strings.
func identityAuthUnicodeValidV2(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	inString := false
	for index := 0; index < len(raw); index++ {
		if !inString {
			inString = raw[index] == '"'
			continue
		}
		if raw[index] == '"' {
			inString = false
		} else if raw[index] == '\\' {
			index++
			if index >= len(raw) {
				return false
			}
			if raw[index] != 'u' {
				continue
			}
			if len(raw)-index < 5 {
				return false
			}
			unit, ok := identityAuthHex4V2(raw[index+1 : index+5])
			if !ok {
				return false
			}
			index += 4
			if unit >= 0xd800 && unit <= 0xdbff {
				if len(raw)-index < 7 || raw[index+1] != '\\' || raw[index+2] != 'u' {
					return false
				}
				low, ok := identityAuthHex4V2(raw[index+3 : index+7])
				if !ok || low < 0xdc00 || low > 0xdfff {
					return false
				}
				index += 6
			} else if unit >= 0xdc00 && unit <= 0xdfff {
				return false
			}
		}
	}
	return !inString
}

func identityAuthHex4V2(raw []byte) (uint16, bool) {
	var value uint16
	for _, character := range raw {
		var digit byte
		switch {
		case character >= '0' && character <= '9':
			digit = character - '0'
		case character >= 'a' && character <= 'f':
			digit = character - 'a' + 10
		case character >= 'A' && character <= 'F':
			digit = character - 'A' + 10
		default:
			return 0, false
		}
		value = value*16 + uint16(digit)
	}
	return value, true
}

func invalidIdentityAuthJSONV2() error {
	return ValidationError{Field: "identity_auth_v2.record", Reason: "requires a closed versioned record of at most 8 KiB"}
}
