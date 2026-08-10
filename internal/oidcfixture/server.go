// Package oidcfixture provides a local-only Telegram-shaped OIDC server for
// deterministic integration tests. It must never be enabled in cloud modes.
package oidcfixture

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

const keyID = "sessionless-local-rs256"

type Config struct {
	Environment  string
	Issuer       string
	ClientID     string
	ClientSecret string
	RedirectURI  string
	Subject      string
	Now          func() time.Time
}

type Server struct {
	config Config
	key    *rsa.PrivateKey
	mux    *http.ServeMux
	mu     sync.Mutex
	codes  map[string]authorization
}

type authorization struct {
	challenge   string
	nonce       string
	redirectURI string
	expiresAt   time.Time
	consumed    bool
}

func New(config Config) (*Server, error) {
	if config.Environment != "local" {
		return nil, errors.New("OIDC fixture refuses every environment except local")
	}
	if config.Issuer == "" || config.ClientID == "" || config.ClientSecret == "" || config.RedirectURI == "" || config.Subject == "" {
		return nil, errors.New("OIDC fixture issuer, client, secret, redirect URI, and subject are required")
	}
	issuer, err := url.Parse(config.Issuer)
	if err != nil {
		return nil, errors.New("OIDC fixture issuer must be a loopback origin")
	}
	hostname := issuer.Hostname()
	if (issuer.Scheme != "http" && issuer.Scheme != "https") ||
		(hostname != "127.0.0.1" && hostname != "localhost" && hostname != "::1" && !strings.HasSuffix(hostname, ".localhost")) {
		return nil, errors.New("OIDC fixture issuer must be a loopback origin")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generate fixture signing key: %w", err)
	}
	server := &Server{config: config, key: key, mux: http.NewServeMux(), codes: make(map[string]authorization)}
	server.routes()
	return server, nil
}

func (server *Server) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	server.mux.ServeHTTP(w, request)
}

func (server *Server) routes() {
	server.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	server.mux.HandleFunc("GET /.well-known/openid-configuration", server.discovery)
	server.mux.HandleFunc("GET /.well-known/jwks.json", server.jwks)
	server.mux.HandleFunc("GET /auth", server.authorize)
	server.mux.HandleFunc("POST /token", server.token)
}

func (server *Server) discovery(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"issuer":                                server.config.Issuer,
		"authorization_endpoint":                server.config.Issuer + "/auth",
		"token_endpoint":                        server.config.Issuer + "/token",
		"jwks_uri":                              server.config.Issuer + "/.well-known/jwks.json",
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"code_challenge_methods_supported":      []string{"S256"},
	})
}

func (server *Server) jwks(w http.ResponseWriter, _ *http.Request) {
	public := server.key.PublicKey
	exponent := big.NewInt(int64(public.E)).Bytes()
	writeJSON(w, http.StatusOK, map[string]any{"keys": []map[string]string{{
		"kty": "RSA", "use": "sig", "alg": "RS256", "kid": keyID,
		"n": base64.RawURLEncoding.EncodeToString(public.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(exponent),
	}}})
}

func (server *Server) authorize(w http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	if query.Get("client_id") != server.config.ClientID || query.Get("redirect_uri") != server.config.RedirectURI ||
		query.Get("response_type") != "code" || query.Get("code_challenge_method") != "S256" ||
		query.Get("state") == "" || query.Get("nonce") == "" || query.Get("code_challenge") == "" ||
		query.Get("scope") != "openid profile" {
		http.Error(w, "invalid authorization request", http.StatusBadRequest)
		return
	}
	code, err := randomValue(32)
	if err != nil {
		http.Error(w, "fixture unavailable", http.StatusServiceUnavailable)
		return
	}
	server.mu.Lock()
	server.codes[code] = authorization{
		challenge: query.Get("code_challenge"), nonce: query.Get("nonce"),
		redirectURI: query.Get("redirect_uri"), expiresAt: server.config.Now().UTC().Add(time.Minute),
	}
	server.mu.Unlock()
	redirect, _ := url.Parse(server.config.RedirectURI)
	values := redirect.Query()
	values.Set("code", code)
	values.Set("state", query.Get("state"))
	redirect.RawQuery = values.Encode()
	w.Header().Set("Location", redirect.String())
	w.WriteHeader(http.StatusSeeOther)
}

func (server *Server) token(w http.ResponseWriter, request *http.Request) {
	clientID, secret, ok := request.BasicAuth()
	if !ok || subtle.ConstantTimeCompare([]byte(clientID), []byte(server.config.ClientID)) != 1 ||
		subtle.ConstantTimeCompare([]byte(secret), []byte(server.config.ClientSecret)) != 1 {
		http.Error(w, "invalid client", http.StatusUnauthorized)
		return
	}
	request.Body = http.MaxBytesReader(w, request.Body, 64<<10)
	if err := request.ParseForm(); err != nil || request.Form.Get("grant_type") != "authorization_code" ||
		request.Form.Get("client_id") != server.config.ClientID || request.Form.Get("redirect_uri") != server.config.RedirectURI {
		http.Error(w, "invalid token request", http.StatusBadRequest)
		return
	}
	code := request.Form.Get("code")
	server.mu.Lock()
	authorization, found := server.codes[code]
	if !found || authorization.consumed ||
		!server.config.Now().UTC().Before(authorization.expiresAt) || authorization.redirectURI != request.Form.Get("redirect_uri") {
		server.mu.Unlock()
		http.Error(w, "invalid authorization code", http.StatusBadRequest)
		return
	}
	digest := sha256.Sum256([]byte(request.Form.Get("code_verifier")))
	actualChallenge := base64.RawURLEncoding.EncodeToString(digest[:])
	if subtle.ConstantTimeCompare([]byte(actualChallenge), []byte(authorization.challenge)) != 1 {
		server.mu.Unlock()
		http.Error(w, "invalid PKCE verifier", http.StatusBadRequest)
		return
	}
	authorization.consumed = true
	server.codes[code] = authorization
	server.mu.Unlock()
	now := server.config.Now().UTC()
	claims := jwt.MapClaims{
		"iss": server.config.Issuer, "aud": server.config.ClientID, "sub": server.config.Subject,
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(), "nonce": authorization.nonce,
		"name": "Sessionless Local User", "preferred_username": "sessionless-local",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = keyID
	signed, err := token.SignedString(server.key)
	if err != nil {
		http.Error(w, "fixture signing failed", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token": "fixture-access-token", "token_type": "Bearer",
		"expires_in": 3600, "id_token": signed,
	})
}

func randomValue(size int) (string, error) {
	value := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
