package attachedworkerprotocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	maxJSONDepth         = 16
	maxJSONObjectMembers = 32
	maxJSONArrayItems    = MaxBatchFrames
	maxJSONStringBytes   = 1024
)

var canonicalUnsignedJSONNumber = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})$`)

func DecodeBatchV1(encoded []byte) (BatchV1, error) {
	if len(encoded) == 0 || len(encoded) > MaxBatchBytes {
		if len(encoded) > MaxBatchBytes {
			return BatchV1{}, protocolError(ErrorFrameTooLarge)
		}
		return BatchV1{}, protocolError(ErrorInvalidFrame)
	}
	if !utf8.Valid(encoded) || bytes.HasPrefix(encoded, []byte{0xef, 0xbb, 0xbf}) || preflightJSON(encoded) != nil {
		return BatchV1{}, protocolError(ErrorInvalidFrame)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var batch BatchV1
	if err := decoder.Decode(&batch); err != nil {
		return BatchV1{}, protocolError(ErrorInvalidFrame)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return BatchV1{}, protocolError(ErrorInvalidFrame)
	}
	if err := batch.Validate(); err != nil {
		return BatchV1{}, err
	}
	return batch, nil
}

func EncodeBatchV1(batch BatchV1) ([]byte, error) {
	if err := batch.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(batch)
	if err != nil {
		return nil, protocolError(ErrorInvalidFrame)
	}
	if len(encoded) > MaxBatchBytes {
		return nil, protocolError(ErrorFrameTooLarge)
	}
	if preflightJSON(encoded) != nil {
		return nil, protocolError(ErrorInvalidFrame)
	}
	return encoded, nil
}

func preflightJSON(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanJSONValue(decoder, 1); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return protocolError(ErrorInvalidFrame)
	}
	token, err := decoder.Token()
	if err != nil {
		return protocolError(ErrorInvalidFrame)
	}
	switch token := token.(type) {
	case nil:
		return protocolError(ErrorInvalidFrame)
	case string:
		if len(token) > maxJSONStringBytes {
			return protocolError(ErrorInvalidFrame)
		}
		return nil
	case json.Number:
		if !canonicalUnsignedJSONNumber.MatchString(token.String()) {
			return protocolError(ErrorInvalidFrame)
		}
		return nil
	case bool:
		return nil
	case json.Delim:
		switch token {
		case '{':
			return scanJSONObject(decoder, depth)
		case '[':
			return scanJSONArray(decoder, depth)
		default:
			return protocolError(ErrorInvalidFrame)
		}
	default:
		return protocolError(ErrorInvalidFrame)
	}
}

func scanJSONObject(decoder *json.Decoder, depth int) error {
	seen := make(map[string]struct{}, maxJSONObjectMembers)
	members := 0
	for decoder.More() {
		members++
		if members > maxJSONObjectMembers {
			return protocolError(ErrorInvalidFrame)
		}
		token, err := decoder.Token()
		if err != nil {
			return protocolError(ErrorInvalidFrame)
		}
		key, ok := token.(string)
		if !ok || !canonicalJSONKey(key) {
			return protocolError(ErrorInvalidFrame)
		}
		folded := strings.ToLower(key)
		if _, duplicate := seen[folded]; duplicate {
			return protocolError(ErrorInvalidFrame)
		}
		seen[folded] = struct{}{}
		if err := scanJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func scanJSONArray(decoder *json.Decoder, depth int) error {
	items := 0
	for decoder.More() {
		items++
		if items > maxJSONArrayItems {
			return protocolError(ErrorInvalidFrame)
		}
		if err := scanJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim(']') {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func canonicalJSONKey(key string) bool {
	if key == "" || len(key) > maxOpaqueBytes {
		return false
	}
	for _, character := range key {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}
