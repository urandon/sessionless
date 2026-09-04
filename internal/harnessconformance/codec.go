package harnessconformance

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	MaxFixtureBytes    = 64 << 10
	maxJSONDepth       = 16
	maxJSONMembers     = 32
	maxJSONArrayItems  = 512
	maxJSONStringBytes = 4096
)

var ErrInvalidFixture = errors.New("invalid provider conformance fixture")

func DecodeFixtureV1(encoded []byte) (FixtureV1, error) {
	var fixture FixtureV1
	if err := decodeStrictJSON(encoded, &fixture); err != nil {
		return FixtureV1{}, err
	}
	if err := fixture.Validate(); err != nil {
		return FixtureV1{}, ErrInvalidFixture
	}
	return fixture.Clone(), nil
}

func EncodeFixtureV1(fixture FixtureV1) ([]byte, error) {
	if err := fixture.Validate(); err != nil {
		return nil, ErrInvalidFixture
	}
	encoded, err := json.Marshal(fixture.Clone())
	if err != nil || len(encoded) == 0 || len(encoded) > MaxFixtureBytes || strictJSONPreflight(encoded) != nil {
		return nil, ErrInvalidFixture
	}
	return encoded, nil
}

func EncodeResultV1(result ResultV1) ([]byte, error) {
	if err := result.Validate(); err != nil {
		return nil, ErrInvalidFixture
	}
	encoded, err := json.Marshal(result.Clone())
	if err != nil || len(encoded) == 0 || len(encoded) > MaxFixtureBytes || strictJSONPreflight(encoded) != nil {
		return nil, ErrInvalidFixture
	}
	return encoded, nil
}

func decodeStrictJSON(encoded []byte, target any) error {
	if len(encoded) == 0 || len(encoded) > MaxFixtureBytes || target == nil ||
		!utf8.Valid(encoded) || bytes.HasPrefix(encoded, []byte{0xef, 0xbb, 0xbf}) || strictJSONPreflight(encoded) != nil {
		return ErrInvalidFixture
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return ErrInvalidFixture
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrInvalidFixture
	}
	return nil
}

func strictJSONPreflight(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanJSONValue(decoder, 1); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrInvalidFixture
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return ErrInvalidFixture
	}
	token, err := decoder.Token()
	if err != nil {
		return ErrInvalidFixture
	}
	switch token := token.(type) {
	case nil:
		return ErrInvalidFixture
	case string:
		if len(token) > maxJSONStringBytes {
			return ErrInvalidFixture
		}
	case json.Number:
		if !canonicalUnsignedJSONNumber(token.String()) {
			return ErrInvalidFixture
		}
	case bool:
	case json.Delim:
		switch token {
		case '{':
			return scanJSONObject(decoder, depth)
		case '[':
			return scanJSONArray(decoder, depth)
		default:
			return ErrInvalidFixture
		}
	default:
		return ErrInvalidFixture
	}
	return nil
}

func scanJSONObject(decoder *json.Decoder, depth int) error {
	seen := make(map[string]struct{}, maxJSONMembers)
	for members := 0; decoder.More(); members++ {
		if members >= maxJSONMembers {
			return ErrInvalidFixture
		}
		token, err := decoder.Token()
		if err != nil {
			return ErrInvalidFixture
		}
		key, ok := token.(string)
		if !ok || !canonicalJSONKey(key) {
			return ErrInvalidFixture
		}
		folded := strings.ToLower(key)
		if _, duplicate := seen[folded]; duplicate {
			return ErrInvalidFixture
		}
		seen[folded] = struct{}{}
		if err := scanJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return ErrInvalidFixture
	}
	return nil
}

func scanJSONArray(decoder *json.Decoder, depth int) error {
	for items := 0; decoder.More(); items++ {
		if items >= maxJSONArrayItems {
			return ErrInvalidFixture
		}
		if err := scanJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim(']') {
		return ErrInvalidFixture
	}
	return nil
}

func canonicalUnsignedJSONNumber(value string) bool {
	if value == "0" {
		return true
	}
	if len(value) == 0 || len(value) > 20 || value[0] < '1' || value[0] > '9' {
		return false
	}
	for index := 1; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func canonicalJSONKey(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}
