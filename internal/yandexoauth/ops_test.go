package yandexoauth

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/ports"
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

func TestOpsProviderServiceFailureIsTypedAndPrivate(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		body         string
		transportErr error
		unavailable  bool
	}{
		{"denied", http.StatusBadRequest, `{"error":"invalid_grant","error_description":"secret-payload"}`, nil, false},
		{"access-denied", http.StatusBadRequest, `{"error":"access_denied","error_description":"secret-payload"}`, nil, false},
		{"client-auth", http.StatusUnauthorized, `{"error":"invalid_client","error_description":"secret-payload"}`, nil, true},
		{"forbidden", http.StatusForbidden, `{"error":"access_denied"}`, nil, true},
		{"invalid-client", http.StatusBadRequest, `{"error":"invalid_client"}`, nil, true},
		{"unauthorized-client", http.StatusBadRequest, `{"error":"unauthorized_client"}`, nil, true},
		{"unknown", http.StatusBadRequest, `{"error":"private-unknown"}`, nil, true},
		{"malformed-rejection", http.StatusBadRequest, "secret-payload", nil, true},
		{"duplicate-rejection", http.StatusBadRequest, `{"error":"invalid_client","error":"invalid_grant"}`, nil, true},
		{"oversized-rejection", http.StatusBadRequest, `{"error":"invalid_grant","error_description":"` + strings.Repeat("s", maxResponseBytes) + `"}`, nil, true},
		{"rate-limited", http.StatusTooManyRequests, "secret-payload", nil, true},
		{"service", http.StatusServiceUnavailable, "secret-payload", nil, true},
		{"malformed-success", http.StatusOK, "secret-payload", nil, true},
		{"network", 0, "", errors.New("private-url secret-payload"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			bodyReader := &opsCountingReader{reader: strings.NewReader(tc.body)}
			provider, err := New(Config{ClientID: "client-test", ClientSecret: "private-secret", RedirectURI: testCallback, HTTPClient: &http.Client{Transport: opsRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				if tc.transportErr != nil {
					return nil, tc.transportErr
				}
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(bodyReader), Header: make(http.Header)}, tc.transportErr
			})}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = provider.ExchangeAndVerify(context.Background(), ports.OAuthTokenRequest{Code: "private-code", RedirectURI: testCallback, PKCEVerifier: testVerifier})
			if !errors.Is(err, ErrProviderResponse) || errors.Is(err, ErrProviderUnavailable) != tc.unavailable || calls != 1 {
				t.Fatalf("classification err=%v unavailable=%t want=%t calls=%d", err, errors.Is(err, ErrProviderUnavailable), tc.unavailable, calls)
			}
			if bodyReader.read > maxResponseBytes+1 {
				t.Fatalf("provider body read=%d exceeds bounded probe=%d", bodyReader.read, maxResponseBytes+1)
			}
			for _, secret := range []string{"secret-payload", "private-secret", "private-code", "private-url"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("classification retained %q: %v", secret, err)
				}
			}
		})
	}
}
