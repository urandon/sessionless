package webcontract_test

import (
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

func TestMutationSecurityRequiresExactOriginAndSessionToken(t *testing.T) {
	t.Parallel()
	digest := domain.DigestSecret("csrf-secret")
	if err := webcontract.ValidateMutationSecurity(
		"https://web.dev.sessionless.triborg.dev", "https://web.dev.sessionless.triborg.dev",
		"csrf-secret", digest,
	); err != nil {
		t.Fatalf("valid mutation security rejected: %v", err)
	}
	for _, test := range []struct{ origin, token string }{
		{"https://attacker.invalid", "csrf-secret"},
		{"https://web.dev.sessionless.triborg.dev/path", "csrf-secret"},
		{"https://web.dev.sessionless.triborg.dev", "wrong"},
		{"http://web.dev.sessionless.triborg.dev", "csrf-secret"},
	} {
		if err := webcontract.ValidateMutationSecurity(
			"https://web.dev.sessionless.triborg.dev", test.origin, test.token, digest,
		); err == nil {
			t.Fatalf("invalid origin/token accepted: %#v", test)
		}
	}
}
