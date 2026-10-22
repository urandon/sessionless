package telegramoidc_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
)

type opsRoundTripper func(*http.Request) (*http.Response, error)

func (fn opsRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type opsCountingReader struct {
	reader io.Reader
	read   int
}

func (reader *opsCountingReader) Read(p []byte) (int, error) {
	n, err := reader.reader.Read(p)
	reader.read += n
	return n, err
}

func TestOpsOIDCProviderServiceFailurePreservesAdmissionError(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		body         string
		transportErr error
		unavailable  bool
	}{
		{"denied", http.StatusBadRequest, `{"error":"invalid_grant","error_description":"secret-payload"}`, nil, false},
		{"access-denied", http.StatusBadRequest, `{"error":"access_denied"}`, nil, false},
		{"client-auth", http.StatusUnauthorized, `{"error":"invalid_client"}`, nil, true},
		{"forbidden", http.StatusForbidden, `{"error":"access_denied"}`, nil, true},
		{"invalid-client", http.StatusBadRequest, `{"error":"invalid_client"}`, nil, true},
		{"unauthorized-client", http.StatusBadRequest, `{"error":"unauthorized_client"}`, nil, true},
		{"unknown", http.StatusBadRequest, `{"error":"private-unknown"}`, nil, true},
		{"malformed-rejection", http.StatusBadRequest, "secret-payload", nil, true},
		{"duplicate-rejection", http.StatusBadRequest, `{"error":"invalid_client","error":"invalid_grant"}`, nil, true},
		{"oversized-rejection", http.StatusBadRequest, `{"error":"invalid_grant","error_description":"` + strings.Repeat("s", 1<<20) + `"}`, nil, true},
		{"rate-limited", http.StatusTooManyRequests, "secret-payload", nil, true},
		{"service", http.StatusServiceUnavailable, "secret-payload", nil, true},
		{"network", 0, "", errors.New("private-url secret-payload"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			bodyReader := &opsCountingReader{reader: strings.NewReader(tc.body)}
			provider, err := telegramoidc.New(telegramoidc.Config{ClientID: "100000", ClientSecret: "private-secret", RedirectURI: "https://web.example.invalid/auth/telegram/callback", HTTPClient: &http.Client{Transport: opsRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				if tc.transportErr != nil {
					return nil, tc.transportErr
				}
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(bodyReader), Header: make(http.Header)}, tc.transportErr
			})}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = provider.ExchangeAndVerify(context.Background(), ports.OIDCTokenRequest{Provider: domain.IdentityProviderTelegram, Code: "private-code", RedirectURI: "https://web.example.invalid/auth/telegram/callback", PKCEVerifier: strings.Repeat("a", 43), ExpectedNonce: "nonce", Now: time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC), Policy: domain.OIDCVerificationPolicy{Issuer: telegramoidc.DefaultIssuer, Audience: "100000", AllowedAlgorithms: []string{"RS256"}}})
			if !errors.Is(err, telegramoidc.ErrProviderResponse) || errors.Is(err, telegramoidc.ErrProviderUnavailable) != tc.unavailable || calls != 1 {
				t.Fatalf("err=%v unavailable=%t want=%t calls=%d", err, errors.Is(err, telegramoidc.ErrProviderUnavailable), tc.unavailable, calls)
			}
			if bodyReader.read > (1<<20)+1 {
				t.Fatalf("provider body read=%d exceeds bounded probe", bodyReader.read)
			}
			if strings.Contains(err.Error(), "secret-payload") || strings.Contains(err.Error(), "private-code") {
				t.Fatalf("provider error retained secrets: %v", err)
			}
		})
	}
}

func TestOpsOIDCEmptyJWKSIsServiceFailureUnknownKeyIsDenial(t *testing.T) {
	modulus := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{127}, 256))
	for _, tc := range []struct {
		name, body  string
		unavailable bool
	}{
		{"empty", `{"keys":[]}`, true},
		{"unusable", `{"keys":[{"kid":"private-kid","kty":"RSA","alg":"RS256","n":"!","e":"AwAB"}]}`, true},
		{"unknown-key", `{"keys":[{"kid":"different-kid","kty":"RSA","alg":"RS256","n":"` + modulus + `","e":"AwAB"}]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			// A syntactically valid token reaches the key lookup. No usable
			// signature or identity is admitted by any of these fixtures.
			token := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","kid":"private-kid"}`)) + "." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"424242","iat":1792627200,"exp":1792630800}`)) + ".c2ln"
			provider, err := telegramoidc.New(telegramoidc.Config{ClientID: "100000", ClientSecret: "private-secret", RedirectURI: "https://web.example.invalid/auth/telegram/callback", HTTPClient: &http.Client{Transport: opsRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				body := `{"id_token":"` + token + `"}`
				if r.URL.String() == telegramoidc.DefaultJWKSURL {
					body = tc.body
				} else if r.URL.String() != telegramoidc.DefaultTokenEndpoint {
					t.Errorf("unexpected provider operation")
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})}})
			if err != nil {
				t.Fatal(err)
			}
			claims, err := provider.ExchangeAndVerify(context.Background(), ports.OIDCTokenRequest{Provider: domain.IdentityProviderTelegram, Code: "private-code", RedirectURI: "https://web.example.invalid/auth/telegram/callback", PKCEVerifier: strings.Repeat("a", 43), ExpectedNonce: "nonce", Now: time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC), Policy: domain.OIDCVerificationPolicy{Issuer: telegramoidc.DefaultIssuer, Audience: "100000", AllowedAlgorithms: []string{"RS256"}}})
			if !errors.Is(err, telegramoidc.ErrProviderResponse) || errors.Is(err, telegramoidc.ErrProviderUnavailable) != tc.unavailable || claims.Subject != "" || calls != 2 {
				t.Fatalf("err=%v unavailable=%t want=%t admitted=%t calls=%d", err, errors.Is(err, telegramoidc.ErrProviderUnavailable), tc.unavailable, claims.Subject != "", calls)
			}
			if strings.Contains(err.Error(), "private-kid") || strings.Contains(err.Error(), "private-code") || strings.Contains(err.Error(), modulus) {
				t.Fatalf("provider classification retained private material: %v", err)
			}
		})
	}
}
