package attachedworkerhttp

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
)

const (
	strictJSONMaxDepth       = 16
	strictJSONMaxMembers     = 32
	strictJSONMaxArrayItems  = attachedworkerprotocol.MaxBatchFrames
	strictJSONMaxStringBytes = 1024
)

func decodeStrictJSON(encoded []byte, target any) error {
	if len(encoded) == 0 || len(encoded) > attachedworkerprotocol.MaxBatchBytes || target == nil ||
		!utf8.Valid(encoded) || bytes.HasPrefix(encoded, []byte{0xef, 0xbb, 0xbf}) || strictJSONPreflight(encoded) != nil {
		return ErrInvalidRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return ErrInvalidRequest
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrInvalidRequest
	}
	return nil
}

func encodeStrictJSON(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) == 0 || len(encoded) > attachedworkerprotocol.MaxBatchBytes || strictJSONPreflight(encoded) != nil {
		return nil, errCoreExchange
	}
	return encoded, nil
}

func strictJSONPreflight(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanStrictJSONValue(decoder, 1); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrInvalidRequest
	}
	return nil
}

func scanStrictJSONValue(decoder *json.Decoder, depth int) error {
	if depth > strictJSONMaxDepth {
		return ErrInvalidRequest
	}
	token, err := decoder.Token()
	if err != nil {
		return ErrInvalidRequest
	}
	switch token := token.(type) {
	case nil:
		return ErrInvalidRequest
	case string:
		if len(token) > strictJSONMaxStringBytes {
			return ErrInvalidRequest
		}
		return nil
	case json.Number:
		if !canonicalUnsignedJSONNumber(token.String()) {
			return ErrInvalidRequest
		}
		return nil
	case bool:
		return nil
	case json.Delim:
		switch token {
		case '{':
			return scanStrictJSONObject(decoder, depth)
		case '[':
			return scanStrictJSONArray(decoder, depth)
		default:
			return ErrInvalidRequest
		}
	default:
		return ErrInvalidRequest
	}
}

func scanStrictJSONObject(decoder *json.Decoder, depth int) error {
	seen := make(map[string]struct{}, strictJSONMaxMembers)
	for members := 0; decoder.More(); members++ {
		if members >= strictJSONMaxMembers {
			return ErrInvalidRequest
		}
		token, err := decoder.Token()
		if err != nil {
			return ErrInvalidRequest
		}
		key, ok := token.(string)
		if !ok || !canonicalJSONKey(key) {
			return ErrInvalidRequest
		}
		folded := strings.ToLower(key)
		if _, duplicate := seen[folded]; duplicate {
			return ErrInvalidRequest
		}
		seen[folded] = struct{}{}
		if err := scanStrictJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return ErrInvalidRequest
	}
	return nil
}

func scanStrictJSONArray(decoder *json.Decoder, depth int) error {
	for items := 0; decoder.More(); items++ {
		if items >= strictJSONMaxArrayItems {
			return ErrInvalidRequest
		}
		if err := scanStrictJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim(']') {
		return ErrInvalidRequest
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
