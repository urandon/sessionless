package attachedworkerhttp

import (
	"errors"
)

const maxBearerTokenBytes = 4096

var ErrInvalidBearerToken = errors.New("attached worker bearer token is invalid")

// BearerToken keeps worker credentials out of ordinary formatting and JSON.
// Bytes is the explicit boundary for an authenticator that must digest or
// compare the credential.
type BearerToken struct {
	value string
}

func ParseBearerToken(value string) (BearerToken, error) {
	if !validToken68(value) {
		return BearerToken{}, ErrInvalidBearerToken
	}
	return BearerToken{value: value}, nil
}

func (token BearerToken) Bytes() []byte { return append([]byte(nil), token.value...) }

func (BearerToken) String() string   { return "[REDACTED]" }
func (BearerToken) GoString() string { return "[REDACTED]" }
func (BearerToken) MarshalJSON() ([]byte, error) {
	return []byte(`"[REDACTED]"`), nil
}

func (token BearerToken) headerValue() string { return "Bearer " + token.value }

func (token BearerToken) valid() bool { return validToken68(token.value) }

func validToken68(value string) bool {
	if value == "" || len(value) > maxBearerTokenBytes {
		return false
	}
	padding := false
	payload := false
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character == '=' {
			padding = true
			continue
		}
		if padding || !token68Byte(character) {
			return false
		}
		payload = true
	}
	return payload
}

// validToken68Bytes lets the connection-local HTTP client validate a bearer
// without first retaining it in an immutable Go string.
func validToken68Bytes(value []byte) bool {
	if len(value) == 0 || len(value) > maxBearerTokenBytes {
		return false
	}
	padding := false
	payload := false
	for _, character := range value {
		if character == '=' {
			padding = true
			continue
		}
		if padding || !token68Byte(character) {
			return false
		}
		payload = true
	}
	return payload
}

func token68Byte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' ||
		value >= '0' && value <= '9' || value == '-' || value == '.' ||
		value == '_' || value == '~' || value == '+' || value == '/'
}
