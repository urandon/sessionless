package attachedworkerhttp

import (
	"errors"
	"strings"
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
	for _, character := range value {
		if character == '=' {
			padding = true
			continue
		}
		if padding || !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~+/", character) {
			return false
		}
		payload = true
	}
	return payload
}
