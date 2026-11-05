package telegramoidc_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
	"github.com/golang-jwt/jwt/v4"
)

const proofCallback = "https://web.example.com/auth/telegram/callback"

type proofRoundTrip func(*http.Request) (*http.Response, error)

func (f proofRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type proofReadCloser struct {
	io.Reader
	closed *atomic.Int32
}

func (b proofReadCloser) Close() error { b.closed.Add(1); return nil }
func proofResponse(r *http.Request, status int, body string, closed *atomic.Int32) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: proofReadCloser{strings.NewReader(body), closed}, Request: r}
}
func proofKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return key
}
func proofJWKS(t *testing.T, key *rsa.PrivateKey) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"keys": []any{map[string]any{"kid": "proof-key", "kty": "RSA", "alg": "RS256", "use": "sig", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
func signedProofToken(t *testing.T, key *rsa.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "proof-key"
	raw, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func proofClaims(now time.Time) jwt.MapClaims {
	return jwt.MapClaims{"iss": telegramoidc.DefaultIssuer, "aud": "100000", "sub": "424242", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix(), "nonce": "proof-nonce", "auth_time": json.RawMessage(fmt.Sprintf("%d.125", now.Unix()))}
}
func proofTokenRequest(now time.Time) ports.IdentityProviderProofTokenRequestV1 {
	return ports.IdentityProviderProofTokenRequestV1{Binding: domain.IdentityProviderBindingV2{Provider: domain.IdentityProviderTelegram, Issuer: telegramoidc.DefaultIssuer, ClientID: "100000", CallbackURI: proofCallback}, Code: "proof-code", PKCEVerifier: strings.Repeat("v", 43), ExpectedNonce: "proof-nonce", ChallengeCreatedAt: now, ChallengeExpiresAt: now.Add(10 * time.Minute), Now: now}
}
func ordinaryProofRequest(now time.Time) ports.OIDCTokenRequest {
	return ports.OIDCTokenRequest{Provider: domain.IdentityProviderTelegram, Code: "proof-code", RedirectURI: proofCallback, PKCEVerifier: strings.Repeat("v", 43), ExpectedNonce: "proof-nonce", Policy: domain.OIDCVerificationPolicy{Issuer: telegramoidc.DefaultIssuer, Audience: "100000", AllowedAlgorithms: []string{"RS256"}, MaxClockSkew: domain.IdentityProofClockSkewV2}, Now: now}
}
func proofProvider(t *testing.T, transport http.RoundTripper) *telegramoidc.Provider {
	t.Helper()
	p, err := telegramoidc.New(telegramoidc.Config{ClientID: "100000", ClientSecret: "synthetic-client-secret", RedirectURI: proofCallback, HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func proofPeer(t *testing.T, token, jwks string, calls, closed *atomic.Int32) http.RoundTripper {
	t.Helper()
	return proofRoundTrip(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		switch r.URL.String() {
		case telegramoidc.DefaultTokenEndpoint:
			if r.Method != http.MethodPost {
				t.Error("token exchange method")
			}
			id, secret, ok := r.BasicAuth()
			if !ok || id != "100000" || secret != "synthetic-client-secret" {
				t.Error("token client binding")
			}
			if r.ParseForm() != nil || r.PostForm.Get("code") != "proof-code" || r.PostForm.Get("code_verifier") != strings.Repeat("v", 43) || r.PostForm.Get("redirect_uri") != proofCallback {
				t.Error("token original code/PKCE/callback")
			}
			raw, _ := json.Marshal(map[string]string{"id_token": token})
			return proofResponse(r, 200, string(raw), closed), nil
		case telegramoidc.DefaultJWKSURL:
			return proofResponse(r, 200, jwks, closed), nil
		default:
			t.Error("unconfigured endpoint reached")
			return nil, errors.New("unconfigured endpoint")
		}
	})
}

func TestTelegramProofAuthorizationDistinctFromOrdinary(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	p := proofProvider(t, proofRoundTrip(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("authorization must be local")
	}))
	r := proofTokenRequest(now)
	a := ports.IdentityProviderProofAuthorizationRequestV1{Binding: r.Binding, State: "proof-state", Nonce: r.ExpectedNonce, CodeChallenge: pkceChallenge(r.PKCEVerifier), ChallengeCreatedAt: r.ChallengeCreatedAt, ChallengeExpiresAt: r.ChallengeExpiresAt, Now: now}
	raw, err := p.ProofAuthorizationURL(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(raw)
	q := u.Query()
	if u.Scheme+"://"+u.Host+u.Path != telegramoidc.DefaultAuthorizationEndpoint || q.Get("max_age") != "0" || q.Get("scope") != "openid profile" || q.Get("nonce") != r.ExpectedNonce || q.Get("code_challenge_method") != "S256" || q.Get("redirect_uri") != proofCallback || q.Get("client_id") != "100000" || q.Get("state") != "proof-state" {
		t.Error("proof authorization policy mismatch")
	}
	ordinary, err := p.AuthorizationURL(context.Background(), ports.OIDCAuthorizationRequest{Provider: domain.IdentityProviderTelegram, RedirectURI: proofCallback, State: "ordinary-state", Nonce: "ordinary-nonce", CodeChallenge: a.CodeChallenge, Scopes: []string{"openid", "profile"}})
	if err != nil {
		t.Fatal(err)
	}
	u, _ = url.Parse(ordinary)
	if u.Query().Has("max_age") {
		t.Error("ordinary login acquired proof freshness")
	}
	for _, mutate := range []func(*ports.IdentityProviderProofTokenRequestV1){func(r *ports.IdentityProviderProofTokenRequestV1) { r.Binding.ClientID = "200000" }, func(r *ports.IdentityProviderProofTokenRequestV1) {
		r.Binding.CallbackURI = "https://other.example.com/auth/telegram/callback"
	}, func(r *ports.IdentityProviderProofTokenRequestV1) { r.Binding.Issuer = "https://other.example.com" }, func(r *ports.IdentityProviderProofTokenRequestV1) { r.Now = r.ChallengeExpiresAt }} {
		bad := r
		mutate(&bad)
		if got, err := p.ExchangeAndVerifyProof(context.Background(), bad); err == nil || got.Subject != (domain.ExternalSubject{}) || got.SignedAuthTime != nil {
			t.Error("wrong proof tuple accepted")
		}
	}
	if calls.Load() != 0 {
		t.Errorf("invalid request performed %d HTTP calls", calls.Load())
	}
}

func TestTelegramProofRequiresSignedFreshAuthTime(t *testing.T) {
	key := proofKey(t)
	other := proofKey(t)
	jwks := proofJWKS(t, key)
	for _, tc := range []struct {
		name     string
		mutate   func(jwt.MapClaims, time.Time)
		wrongKey bool
		valid    bool
	}{
		{name: "fractional-valid", valid: true},
		{name: "integer-valid", valid: true, mutate: func(c jwt.MapClaims, n time.Time) { c["auth_time"] = n.Unix() }},
		{name: "exponent-valid", valid: true, mutate: func(c jwt.MapClaims, n time.Time) {
			c["auth_time"] = json.RawMessage(fmt.Sprintf("%de-1", n.Unix()*10))
		}},
		{name: "old-boundary-valid", valid: true, mutate: func(c jwt.MapClaims, n time.Time) { c["auth_time"] = n.Add(-30 * time.Second).Unix() }},
		{name: "future-boundary-valid", valid: true, mutate: func(c jwt.MapClaims, n time.Time) { c["auth_time"] = n.Add(30 * time.Second).Unix() }},
		{name: "missing", mutate: func(c jwt.MapClaims, _ time.Time) { delete(c, "auth_time") }},
		{name: "null", mutate: func(c jwt.MapClaims, _ time.Time) { c["auth_time"] = nil }},
		{name: "string", mutate: func(c jwt.MapClaims, n time.Time) { c["auth_time"] = fmt.Sprint(n.Unix()) }},
		{name: "object", mutate: func(c jwt.MapClaims, _ time.Time) { c["auth_time"] = map[string]int{"value": 1} }},
		{name: "stale-one-nanosecond", mutate: func(c jwt.MapClaims, n time.Time) {
			c["auth_time"] = json.RawMessage(fmt.Sprintf("%d.999999999", n.Add(-31*time.Second).Unix()))
		}},
		{name: "future-one-nanosecond", mutate: func(c jwt.MapClaims, n time.Time) {
			c["auth_time"] = json.RawMessage(fmt.Sprintf("%d.000000001", n.Add(30*time.Second).Unix()))
		}},
		{name: "subnanosecond", mutate: func(c jwt.MapClaims, n time.Time) {
			c["auth_time"] = json.RawMessage(fmt.Sprintf("%d.0000000001", n.Unix()))
		}},
		{name: "huge-exponent", mutate: func(c jwt.MapClaims, _ time.Time) { c["auth_time"] = json.RawMessage("1e999999999") }},
		{name: "tiny-exponent", mutate: func(c jwt.MapClaims, _ time.Time) { c["auth_time"] = json.RawMessage("1e-999999999") }},
		{name: "case-alias", mutate: func(c jwt.MapClaims, _ time.Time) { c["AUTH_TIME"] = c["auth_time"]; delete(c, "auth_time") }},
		{name: "wrong-nonce", mutate: func(c jwt.MapClaims, _ time.Time) { c["nonce"] = "other" }},
		{name: "wrong-issuer", mutate: func(c jwt.MapClaims, _ time.Time) { c["iss"] = "https://other.example.com" }},
		{name: "wrong-audience", mutate: func(c jwt.MapClaims, _ time.Time) { c["aud"] = "200000" }},
		{name: "expired-token", mutate: func(c jwt.MapClaims, n time.Time) {
			c["iat"] = n.Add(-time.Hour).Unix()
			c["exp"] = n.Add(-time.Minute).Unix()
		}},
		{name: "future-iat", mutate: func(c jwt.MapClaims, n time.Time) { c["iat"] = n.Add(time.Minute).Unix() }},
		{name: "wrong-signature", wrongKey: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := time.Now().UTC()
				claims := proofClaims(now)
				if tc.mutate != nil {
					tc.mutate(claims, now)
				}
				signer := key
				if tc.wrongKey {
					signer = other
				}
				token := signedProofToken(t, signer, claims)
				var calls, closed atomic.Int32
				p := proofProvider(t, proofPeer(t, token, jwks, &calls, &closed))
				got, err := p.ExchangeAndVerifyProof(t.Context(), proofTokenRequest(now))
				if tc.valid {
					if err != nil || got.Subject != (domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}) || got.Kind != domain.IdentityProofTelegramAuthTimeV2 || got.SignedAuthTime == nil {
						t.Fatalf("valid proof = %+v, %v", got, err)
					}
					if tc.name == "fractional-valid" && !got.SignedAuthTime.Equal(now.Add(125*time.Millisecond)) {
						t.Errorf("fraction lost: %s", got.SignedAuthTime)
					}
				} else if !errors.Is(err, telegramoidc.ErrProviderResponse) || got.Subject != (domain.ExternalSubject{}) || got.SignedAuthTime != nil {
					t.Errorf("invalid signed proof returned %+v, %v", got, err)
				}
				if calls.Load() != 2 || closed.Load() != 2 {
					t.Errorf("single exchange/JWKS and closed bodies = %d/%d, want 2/2", calls.Load(), closed.Load())
				}
			})
		})
	}
	t.Run("ordinary-does-not-require-auth-time", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			now := time.Now().UTC()
			claims := proofClaims(now)
			delete(claims, "auth_time")
			var calls, closed atomic.Int32
			p := proofProvider(t, proofPeer(t, signedProofToken(t, key, claims), jwks, &calls, &closed))
			got, err := p.ExchangeAndVerify(t.Context(), ordinaryProofRequest(now))
			if err != nil || got.Subject != "424242" {
				t.Fatalf("ordinary login missing auth_time = %+v, %v", got, err)
			}
		})
	})
}

func TestTelegramExchangeHasOneStageBudget(t *testing.T) {
	key := proofKey(t)
	jwks := proofJWKS(t, key)
	for _, mode := range []string{"ordinary-total", "proof-total", "earlier-context", "challenge-remaining", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := time.Now().UTC()
				token := signedProofToken(t, key, proofClaims(now))
				var calls, closed, attempts atomic.Int32
				peer := proofPeer(t, token, jwks, &calls, &closed)
				var firstDeadline time.Time
				transport := proofRoundTrip(func(r *http.Request) (*http.Response, error) {
					attempts.Add(1)
					deadline, ok := r.Context().Deadline()
					if !ok {
						t.Error("unbounded exchange")
					}
					if firstDeadline.IsZero() {
						firstDeadline = deadline
					} else if !deadline.Equal(firstDeadline) {
						t.Error("stage reset exchange deadline")
					}
					delay := 6 * time.Second
					if r.URL.String() == telegramoidc.DefaultJWKSURL {
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
				p := proofProvider(t, transport)
				ctx := t.Context()
				if mode == "earlier-context" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 3*time.Second)
					defer cancel()
				}
				request := proofTokenRequest(now)
				if mode == "challenge-remaining" {
					request.ChallengeExpiresAt = now.Add(2 * time.Second)
				}
				if mode == "cancelled" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				var err error
				if mode == "ordinary-total" {
					_, err = p.ExchangeAndVerify(ctx, ordinaryProofRequest(now))
				} else {
					_, err = p.ExchangeAndVerifyProof(ctx, request)
				}
				if !errors.Is(err, telegramoidc.ErrProviderResponse) {
					t.Errorf("bounded exchange error = %v", err)
				}
				want := 10 * time.Second
				wantAttempts := int32(2)
				if mode == "earlier-context" {
					want = 3 * time.Second
					wantAttempts = 1
				}
				if mode == "challenge-remaining" {
					want = 2 * time.Second
					wantAttempts = 1
				}
				if mode == "cancelled" {
					want = 0
					wantAttempts = 0
				}
				if elapsed := time.Since(now); elapsed != want {
					t.Errorf("exchange elapsed = %s, want %s", elapsed, want)
				}
				if attempts.Load() != wantAttempts || closed.Load() != calls.Load() {
					t.Errorf("single attempts/body closure = %d/%d/%d", attempts.Load(), calls.Load(), closed.Load())
				}
			})
		})
	}
}

func TestTelegramJWKSContentionDoesNotHoldNetworkLock(t *testing.T) {
	key := proofKey(t)
	jwks := proofJWKS(t, key)
	synctest.Test(t, func(t *testing.T) {
		now := time.Now().UTC()
		token := signedProofToken(t, key, proofClaims(now))
		var calls, closed, fetches atomic.Int32
		peer := proofPeer(t, token, jwks, &calls, &closed)
		entered := make(chan struct{})
		release := make(chan struct{})
		transport := proofRoundTrip(func(r *http.Request) (*http.Response, error) {
			if r.URL.String() == telegramoidc.DefaultJWKSURL {
				if fetches.Add(1) == 1 {
					close(entered)
					select {
					case <-release:
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
				}
			}
			return peer.RoundTrip(r)
		})
		p := proofProvider(t, transport)
		first := make(chan error, 1)
		go func() { _, err := p.ExchangeAndVerifyProof(t.Context(), proofTokenRequest(now)); first <- err }()
		<-entered
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_, err := p.ExchangeAndVerifyProof(ctx, proofTokenRequest(now))
		if err != nil {
			t.Errorf("independent exchange blocked behind JWKS fetch: %v", err)
		}
		if time.Since(now) != 0 {
			t.Error("cache network contention consumed independent exchange deadline")
		}
		close(release)
		if err := <-first; err != nil {
			t.Error("original fetch:", err)
		}
		if fetches.Load() != 2 || calls.Load() != 4 || closed.Load() != 4 {
			t.Errorf("bounded per-exchange fetches/calls/closes = %d/%d/%d", fetches.Load(), calls.Load(), closed.Load())
		}
	})
}

func TestTelegramProofResponseAndClientBounds(t *testing.T) {
	key := proofKey(t)
	jwks := proofJWKS(t, key)
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	token := signedProofToken(t, key, proofClaims(now))
	valid, _ := json.Marshal(map[string]string{"id_token": token})
	for _, tc := range []struct {
		name, stage, body string
		status            int
	}{
		{"token-trailing", "token", string(valid) + " {}", 200}, {"token-duplicate", "token", `{"id_token":"one","id_token":"two"}`, 200}, {"token-overflow", "token", string(valid) + strings.Repeat(" ", 1<<20), 200},
		{"jwks-trailing", "jwks", jwks + " {}", 200}, {"jwks-overflow", "jwks", jwks + strings.Repeat(" ", 1<<20), 200}, {"redirect", "token", "", 302}, {"known-denial", "token", `{"error":"invalid_grant"}`, 400}, {"unknown-service", "token", `{"error":"private-secret"}`, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls, closed atomic.Int32
			peer := proofPeer(t, token, jwks, &calls, &closed)
			attempts := 0
			transport := proofRoundTrip(func(r *http.Request) (*http.Response, error) {
				attempts++
				if (r.URL.String() == telegramoidc.DefaultTokenEndpoint) == (tc.stage == "token") {
					calls.Add(1)
					response := proofResponse(r, tc.status, tc.body, &closed)
					response.Header.Set("Location", "https://other.example.com/private-secret")
					return response, nil
				}
				return peer.RoundTrip(r)
			})
			client := &http.Client{Transport: transport, Timeout: time.Hour}
			p, err := telegramoidc.New(telegramoidc.Config{ClientID: "100000", ClientSecret: "synthetic-client-secret", RedirectURI: proofCallback, HTTPClient: client})
			if err != nil {
				t.Fatal(err)
			}
			got, err := p.ExchangeAndVerifyProof(context.Background(), proofTokenRequest(now))
			if !errors.Is(err, telegramoidc.ErrProviderResponse) || got.Subject != (domain.ExternalSubject{}) || got.SignedAuthTime != nil {
				t.Errorf("unsafe response accepted %+v, %v", got, err)
			}
			if errors.Is(err, telegramoidc.ErrProviderUnavailable) != (tc.name != "known-denial") {
				t.Error("provider denial/unavailable classification mismatch")
			}
			if err != nil && strings.Contains(err.Error(), "private-secret") {
				t.Error("error leaked provider data")
			}
			wantAttempts := 1
			if tc.stage == "jwks" {
				wantAttempts = 2
			}
			if closed.Load() != calls.Load() || attempts != wantAttempts {
				t.Errorf("response close/retry = %d/%d/%d", closed.Load(), calls.Load(), attempts)
			}
			if client.Timeout != time.Hour || client.CheckRedirect != nil {
				t.Error("caller HTTP client mutated")
			}
		})
	}
}

func TestTelegramProofOwnsConfiguredAlgorithmSlice(t *testing.T) {
	key := proofKey(t)
	jwks := proofJWKS(t, key)
	synctest.Test(t, func(t *testing.T) {
		now := time.Now().UTC()
		var calls, closed atomic.Int32
		algorithms := []string{"RS256"}
		p, err := telegramoidc.New(telegramoidc.Config{ClientID: "100000", ClientSecret: "synthetic-client-secret", RedirectURI: proofCallback, AllowedAlgorithms: algorithms, HTTPClient: &http.Client{Transport: proofPeer(t, signedProofToken(t, key, proofClaims(now)), jwks, &calls, &closed)}})
		if err != nil {
			t.Fatal(err)
		}
		algorithms[0] = "HS256"
		got, err := p.ExchangeAndVerifyProof(t.Context(), proofTokenRequest(now))
		if err != nil || got.Kind != domain.IdentityProofTelegramAuthTimeV2 || got.Subject.Subject != "424242" {
			t.Errorf("caller slice changed admitted proof policy: %+v, %v", got, err)
		}
		if calls.Load() != 2 || closed.Load() != 2 {
			t.Errorf("proof calls/closed bodies = %d/%d", calls.Load(), closed.Load())
		}
	})
}
