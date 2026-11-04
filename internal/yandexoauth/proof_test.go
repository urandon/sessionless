package yandexoauth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type proofRoundTrip func(*http.Request) (*http.Response, error)

func (f proofRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type proofBody struct {
	io.Reader
	closed *atomic.Int32
}

func (b proofBody) Close() error { b.closed.Add(1); return nil }
func proofResponse(r *http.Request, status int, body string, closed *atomic.Int32) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: proofBody{strings.NewReader(body), closed}, Request: r}
}
func proofRequest(now time.Time) ports.IdentityProviderProofTokenRequestV1 {
	return ports.IdentityProviderProofTokenRequestV1{Binding: domain.IdentityProviderBindingV2{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, ClientID: "client-test", CallbackURI: testCallback}, Code: "one-use-code", PKCEVerifier: testVerifier, ChallengeCreatedAt: now, ChallengeExpiresAt: now.Add(10 * time.Minute), Now: now}
}
func newProofProvider(t *testing.T, transport http.RoundTripper) *Provider {
	t.Helper()
	p, err := New(Config{ClientID: "client-test", ClientSecret: "synthetic-client-secret", RedirectURI: testCallback, HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func proofPeer(t *testing.T, calls, closed *atomic.Int32) http.RoundTripper {
	t.Helper()
	return proofRoundTrip(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		switch r.URL.String() {
		case DefaultTokenEndpoint:
			id, secret, ok := r.BasicAuth()
			if !ok || id != "client-test" || secret != "synthetic-client-secret" {
				t.Error("private client binding")
			}
			if r.Method != http.MethodPost || r.ParseForm() != nil || r.PostForm.Get("code") != "one-use-code" || r.PostForm.Get("code_verifier") != testVerifier || r.PostForm.Get("redirect_uri") != testCallback {
				t.Error("original code/PKCE/callback binding")
			}
			return proofResponse(r, 200, `{"access_token":"synthetic-access","token_type":"bearer","expires_in":3600}`, closed), nil
		case DefaultInfoEndpoint + "?format=json":
			if r.Method != http.MethodGet || r.Header.Get("Authorization") != "OAuth synthetic-access" {
				t.Error("account authorization must be header only")
			}
			return proofResponse(r, 200, `{"id":"424242","client_id":"client-test"}`, closed), nil
		default:
			t.Error("unconfigured provider endpoint")
			return nil, errors.New("unconfigured endpoint")
		}
	})
}

func TestYandexProofAddsConsentWithoutInventingPasswordAge(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	var calls, closed atomic.Int32
	p := newProofProvider(t, proofPeer(t, &calls, &closed))
	r := proofRequest(now)
	a := ports.IdentityProviderProofAuthorizationRequestV1{Binding: r.Binding, State: "proof-state", CodeChallenge: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", ChallengeCreatedAt: r.ChallengeCreatedAt, ChallengeExpiresAt: r.ChallengeExpiresAt, Now: now}
	raw, err := p.ProofAuthorizationURL(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(raw)
	q := u.Query()
	if u.Scheme+"://"+u.Host+u.Path != DefaultAuthorizationEndpoint || q.Get("force_confirm") != "yes" || q.Get("state") != "proof-state" || q.Get("code_challenge_method") != "S256" || q.Get("scope") != "login:info" || q.Get("client_id") != "client-test" || q.Get("redirect_uri") != testCallback || q.Has("nonce") || q.Has("max_age") {
		t.Error("Yandex proof authorization mismatch")
	}
	ordinary, err := p.AuthorizationURL(context.Background(), ports.OAuthAuthorizationRequest{RedirectURI: testCallback, State: "ordinary-state", CodeChallenge: a.CodeChallenge})
	if err != nil {
		t.Fatal(err)
	}
	u, _ = url.Parse(ordinary)
	if u.Query().Has("force_confirm") {
		t.Error("ordinary login acquired proof consent policy")
	}
	got, err := p.ExchangeAndVerifyProof(context.Background(), r)
	if err != nil || got.Subject != (domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "424242"}) || got.Kind != domain.IdentityProofYandexAuthorizationV2 || got.SignedAuthTime != nil {
		t.Fatalf("proof result=%+v err=%v", got, err)
	}
	if calls.Load() != 2 || closed.Load() != 2 {
		t.Errorf("single token/account and closed bodies=%d/%d", calls.Load(), closed.Load())
	}
	encoded, _ := json.Marshal(got)
	if strings.Contains(string(encoded), "synthetic-access") || strings.Contains(string(encoded), "secret") {
		t.Error("result exposed credentials")
	}
}

func TestYandexProofChecksBindingBeforeIO(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	p := newProofProvider(t, proofRoundTrip(func(*http.Request) (*http.Response, error) { calls.Add(1); return nil, errors.New("unexpected HTTP") }))
	r := proofRequest(now)
	for _, tc := range []struct {
		name   string
		mutate func(*ports.IdentityProviderProofTokenRequestV1)
	}{
		{"client", func(r *ports.IdentityProviderProofTokenRequestV1) { r.Binding.ClientID = "other-client" }},
		{"callback", func(r *ports.IdentityProviderProofTokenRequestV1) {
			r.Binding.CallbackURI = "https://other.example.com/auth/login/callback"
		}},
		{"issuer", func(r *ports.IdentityProviderProofTokenRequestV1) { r.Binding.Issuer = "https://other.example.com" }},
		{"nonce", func(r *ports.IdentityProviderProofTokenRequestV1) { r.ExpectedNonce = "invented-nonce" }},
		{"expired", func(r *ports.IdentityProviderProofTokenRequestV1) { r.Now = r.ChallengeExpiresAt }},
		{"code", func(r *ports.IdentityProviderProofTokenRequestV1) { r.Code = "bad\ncode" }},
		{"verifier", func(r *ports.IdentityProviderProofTokenRequestV1) { r.PKCEVerifier = "short" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := r
			tc.mutate(&bad)
			got, err := p.ExchangeAndVerifyProof(context.Background(), bad)
			if err == nil || got.Subject != (domain.ExternalSubject{}) || got.SignedAuthTime != nil {
				t.Error("invalid bound request accepted")
			}
		})
	}
	if calls.Load() != 0 {
		t.Errorf("invalid requests made %d HTTP calls", calls.Load())
	}
}

func TestYandexProofKeepsWholeExchangeDeadline(t *testing.T) {
	for _, mode := range []string{"total", "earlier-parent", "original-remaining", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := time.Now().UTC()
				var calls, closed, attempts atomic.Int32
				peer := proofPeer(t, &calls, &closed)
				var first time.Time
				transport := proofRoundTrip(func(r *http.Request) (*http.Response, error) {
					attempts.Add(1)
					deadline, ok := r.Context().Deadline()
					if !ok {
						t.Error("unbounded request")
					}
					if first.IsZero() {
						first = deadline
					} else if !deadline.Equal(first) {
						t.Error("account stage renewed deadline")
					}
					delay := 6 * time.Second
					if r.Method == http.MethodGet {
						delay = 5 * time.Second
					}
					timer := time.NewTimer(delay)
					defer timer.Stop()
					select {
					case <-r.Context().Done():
						return nil, r.Context().Err()
					case <-timer.C:
						if r.Context().Err() != nil {
							return nil, r.Context().Err()
						}
						return peer.RoundTrip(r)
					}
				})
				p := newProofProvider(t, transport)
				request := proofRequest(now)
				ctx := t.Context()
				if mode == "earlier-parent" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 3*time.Second)
					defer cancel()
				}
				if mode == "original-remaining" {
					request.ChallengeExpiresAt = now.Add(2 * time.Second)
				}
				if mode == "cancelled" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				got, err := p.ExchangeAndVerifyProof(ctx, request)
				if !errors.Is(err, ErrProviderResponse) || got.Subject != (domain.ExternalSubject{}) {
					t.Errorf("deadline result=%+v err=%v", got, err)
				}
				want := 10 * time.Second
				wantAttempts := int32(2)
				switch mode {
				case "earlier-parent":
					want = 3 * time.Second
					wantAttempts = 1
				case "original-remaining":
					want = 2 * time.Second
					wantAttempts = 1
				case "cancelled":
					want = 0
					wantAttempts = 0
				}
				if elapsed := time.Since(now); elapsed != want {
					t.Errorf("elapsed=%s want=%s", elapsed, want)
				}
				if attempts.Load() != wantAttempts || closed.Load() != calls.Load() {
					t.Errorf("single attempts/closed responses=%d/%d/%d", attempts.Load(), closed.Load(), calls.Load())
				}
			})
		})
	}
}

func TestYandexProofResponseDenialsRemainBoundedAndContentFree(t *testing.T) {
	for _, tc := range []struct {
		name, stage, body string
		status            int
		unavailable       bool
	}{
		{"wrong-account-client", "info", `{"id":"424242","client_id":"other"}`, 200, false},
		{"wrong-account-id", "info", `{"id":424242,"client_id":"client-test"}`, 200, false},
		{"duplicate-account", "info", `{"id":"1","id":"2","client_id":"client-test"}`, 200, true},
		{"trailing-account", "info", `{"id":"1","client_id":"client-test"} {}`, 200, true},
		{"overflow-token", "token", strings.Repeat(" ", maxResponseBytes+1), 200, true},
		{"redirect", "token", "", 302, true},
		{"known-denial", "token", `{"error":"invalid_grant"}`, 400, false},
		{"unknown-denial", "token", `{"error":"private-upstream-secret"}`, 500, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls, closed, attempts atomic.Int32
			peer := proofPeer(t, &calls, &closed)
			transport := proofRoundTrip(func(r *http.Request) (*http.Response, error) {
				attempts.Add(1)
				if (r.Method == http.MethodPost) == (tc.stage == "token") {
					calls.Add(1)
					response := proofResponse(r, tc.status, tc.body, &closed)
					response.Header.Set("Location", "https://other.example.com/private-upstream-secret")
					return response, nil
				}
				return peer.RoundTrip(r)
			})
			client := &http.Client{Transport: transport, Timeout: time.Hour}
			p, err := New(Config{ClientID: "client-test", ClientSecret: "synthetic-client-secret", RedirectURI: testCallback, HTTPClient: client})
			if err != nil {
				t.Fatal(err)
			}
			got, err := p.ExchangeAndVerifyProof(t.Context(), proofRequest(time.Now().UTC()))
			if !errors.Is(err, ErrProviderResponse) || errors.Is(err, ErrProviderUnavailable) != tc.unavailable || got.Subject != (domain.ExternalSubject{}) || got.SignedAuthTime != nil {
				t.Errorf("response result=%+v err=%v unavailable want=%t", got, err, tc.unavailable)
			}
			if err != nil && strings.Contains(err.Error(), "private-upstream-secret") {
				t.Error("upstream detail leaked")
			}
			wantAttempts := int32(1)
			if tc.stage == "info" {
				wantAttempts = 2
			}
			if attempts.Load() != wantAttempts || calls.Load() != closed.Load() {
				t.Errorf("retry/closed bodies=%d/%d/%d", attempts.Load(), calls.Load(), closed.Load())
			}
			if client.Timeout != time.Hour || client.CheckRedirect != nil {
				t.Error("caller client mutated")
			}
		})
	}
}
