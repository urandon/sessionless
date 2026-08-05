package webcontract

import (
	"crypto/subtle"
	"net/url"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// ValidateMutationSecurity requires an exact normalized HTTPS Origin and a
// session-bound double-submit token whose digest is stored with the session.
func ValidateMutationSecurity(expectedOrigin, actualOrigin, presentedToken string, expectedDigest domain.SecretDigest) error {
	expected, err := normalizeOrigin(expectedOrigin)
	if err != nil {
		return domain.ValidationError{Field: "csrf.expected_origin", Reason: "is invalid"}
	}
	actual, err := normalizeOrigin(actualOrigin)
	if err != nil || actual != expected {
		return domain.ValidationError{Field: "csrf.origin", Reason: "does not match the configured origin"}
	}
	if err := expectedDigest.Validate("csrf.token_digest"); err != nil {
		return err
	}
	actualDigest := domain.DigestSecret(presentedToken)
	if presentedToken == "" || subtle.ConstantTimeCompare([]byte(actualDigest), []byte(expectedDigest)) != 1 {
		return domain.ValidationError{Field: "csrf.token", Reason: "does not match the web session"}
	}
	return nil
}

func normalizeOrigin(raw string) (string, error) {
	origin, err := url.Parse(raw)
	if err != nil || origin.Scheme != "https" || origin.Host == "" || origin.User != nil ||
		origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" {
		return "", domain.ValidationError{Field: "origin", Reason: "must be an HTTPS origin without path, query, or credentials"}
	}
	return strings.ToLower(origin.Scheme + "://" + origin.Host), nil
}
