// Package telegramoidc implements the server-side Telegram OpenID Connect
// adapter. Provider tokens remain inside this package and are never returned
// through the ports.OIDCProvider boundary.
package telegramoidc

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	DefaultIssuer                = "https://oauth.telegram.org"
	DefaultAuthorizationEndpoint = "https://oauth.telegram.org/auth"
	DefaultTokenEndpoint         = "https://oauth.telegram.org/token"
	DefaultJWKSURL               = "https://oauth.telegram.org/.well-known/jwks.json"
	defaultJWKSCacheTTL          = 10 * time.Minute
	maxProviderResponseBytes     = 1 << 20
)

var ErrProviderResponse = errors.New("Telegram OIDC provider response is invalid")

type Config struct {
	Issuer                string
	AuthorizationEndpoint string
	TokenEndpoint         string
	JWKSURL               string
	ClientID              string
	ClientSecret          string
	RedirectURI           string
	AllowedAlgorithms     []string
	JWKSCacheTTL          time.Duration
	HTTPClient            *http.Client
	AllowLoopbackProvider bool
}

type Provider struct {
	config Config
	client *http.Client
	keys   jwksCache
}

type jwksCache struct {
	mu        sync.Mutex
	keys      map[string]any
	expiresAt time.Time
}

func New(config Config) (*Provider, error) {
	if config.Issuer == "" {
		config.Issuer = DefaultIssuer
	}
	if config.AuthorizationEndpoint == "" {
		config.AuthorizationEndpoint = DefaultAuthorizationEndpoint
	}
	if config.TokenEndpoint == "" {
		config.TokenEndpoint = DefaultTokenEndpoint
	}
	if config.JWKSURL == "" {
		config.JWKSURL = DefaultJWKSURL
	}
	if len(config.AllowedAlgorithms) == 0 {
		config.AllowedAlgorithms = []string{"RS256"}
	}
	if config.JWKSCacheTTL <= 0 || config.JWKSCacheTTL > defaultJWKSCacheTTL {
		config.JWKSCacheTTL = defaultJWKSCacheTTL
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	return &Provider{config: config, client: config.HTTPClient}, nil
}

func (provider *Provider) AuthorizationURL(
	_ context.Context,
	request ports.OIDCAuthorizationRequest,
) (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	if request.Provider != domain.IdentityProviderTelegram {
		return "", domain.ValidationError{Field: "oidc.provider", Reason: "must be telegram"}
	}
	if request.RedirectURI != provider.config.RedirectURI {
		return "", domain.ValidationError{Field: "oidc.redirect_uri", Reason: "does not match the configured callback"}
	}
	endpoint, err := url.Parse(provider.config.AuthorizationEndpoint)
	if err != nil {
		return "", err
	}
	query := endpoint.Query()
	query.Set("client_id", provider.config.ClientID)
	query.Set("redirect_uri", request.RedirectURI)
	query.Set("response_type", "code")
	query.Set("scope", strings.Join(request.Scopes, " "))
	query.Set("state", request.State)
	query.Set("nonce", request.Nonce)
	query.Set("code_challenge", request.CodeChallenge)
	query.Set("code_challenge_method", "S256")
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

func (provider *Provider) ExchangeAndVerify(
	ctx context.Context,
	request ports.OIDCTokenRequest,
) (domain.OIDCIdentityClaims, error) {
	if err := request.Validate(); err != nil {
		return domain.OIDCIdentityClaims{}, err
	}
	if request.Provider != domain.IdentityProviderTelegram || request.RedirectURI != provider.config.RedirectURI {
		return domain.OIDCIdentityClaims{}, ErrProviderResponse
	}
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {request.Code},
		"redirect_uri":  {request.RedirectURI},
		"client_id":     {provider.config.ClientID},
		"code_verifier": {request.PKCEVerifier},
	}
	httpRequest, err := http.NewRequestWithContext(
		ctx, http.MethodPost, provider.config.TokenEndpoint, strings.NewReader(form.Encode()),
	)
	if err != nil {
		return domain.OIDCIdentityClaims{}, ErrProviderResponse
	}
	httpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	httpRequest.SetBasicAuth(provider.config.ClientID, provider.config.ClientSecret)
	response, err := provider.client.Do(httpRequest)
	if err != nil {
		return domain.OIDCIdentityClaims{}, fmt.Errorf("%w: token exchange failed", ErrProviderResponse)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxProviderResponseBytes))
		return domain.OIDCIdentityClaims{}, fmt.Errorf("%w: token endpoint status %d", ErrProviderResponse, response.StatusCode)
	}
	var tokens struct {
		IDToken string `json:"id_token"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxProviderResponseBytes))
	if err := decoder.Decode(&tokens); err != nil || tokens.IDToken == "" {
		return domain.OIDCIdentityClaims{}, ErrProviderResponse
	}
	claims, err := provider.verifyIDToken(ctx, tokens.IDToken, request.Now)
	if err != nil {
		return domain.OIDCIdentityClaims{}, err
	}
	if err := claims.Verify(request.Policy, request.ExpectedNonce, request.Now); err != nil {
		return domain.OIDCIdentityClaims{}, err
	}
	return claims, nil
}

type telegramClaims struct {
	jwt.RegisteredClaims
	Nonce string `json:"nonce"`
}

func (provider *Provider) verifyIDToken(
	ctx context.Context,
	rawToken string,
	now time.Time,
) (domain.OIDCIdentityClaims, error) {
	parser := &jwt.Parser{
		ValidMethods:         append([]string(nil), provider.config.AllowedAlgorithms...),
		SkipClaimsValidation: true,
	}
	claims := &telegramClaims{}
	token, err := parser.ParseWithClaims(rawToken, claims, func(token *jwt.Token) (any, error) {
		kid, ok := token.Header["kid"].(string)
		if !ok || kid == "" {
			return nil, ErrProviderResponse
		}
		return provider.signingKey(ctx, kid, now)
	})
	if err != nil || token == nil || !token.Valid {
		return domain.OIDCIdentityClaims{}, fmt.Errorf("%w: ID token verification failed", ErrProviderResponse)
	}
	if claims.IssuedAt == nil || claims.ExpiresAt == nil {
		return domain.OIDCIdentityClaims{}, ErrProviderResponse
	}
	return domain.OIDCIdentityClaims{
		Issuer: claims.Issuer, Audience: []string(claims.Audience), Subject: claims.Subject,
		Nonce: claims.Nonce, IssuedAt: claims.IssuedAt.Time.UTC(), ExpiresAt: claims.ExpiresAt.Time.UTC(),
	}, nil
}

func (provider *Provider) signingKey(ctx context.Context, kid string, now time.Time) (any, error) {
	provider.keys.mu.Lock()
	defer provider.keys.mu.Unlock()
	if now.Before(provider.keys.expiresAt) {
		if key, found := provider.keys.keys[kid]; found {
			return key, nil
		}
	}
	if err := provider.refreshKeys(ctx, now); err != nil {
		return nil, err
	}
	key, found := provider.keys.keys[kid]
	if !found {
		return nil, ErrProviderResponse
	}
	return key, nil
}

func (provider *Provider) refreshKeys(ctx context.Context, now time.Time) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, provider.config.JWKSURL, nil)
	if err != nil {
		return ErrProviderResponse
	}
	response, err := provider.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: JWKS fetch failed", ErrProviderResponse)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: JWKS endpoint status %d", ErrProviderResponse, response.StatusCode)
	}
	var document struct {
		Keys []struct {
			KID string `json:"kid"`
			KTY string `json:"kty"`
			Use string `json:"use"`
			Alg string `json:"alg"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, maxProviderResponseBytes)).Decode(&document); err != nil {
		return ErrProviderResponse
	}
	keys := make(map[string]any)
	for _, encoded := range document.Keys {
		if encoded.KID == "" || encoded.KTY != "RSA" || encoded.Alg != "RS256" || (encoded.Use != "" && encoded.Use != "sig") {
			continue
		}
		key, err := decodeRSAKey(encoded.N, encoded.E)
		if err != nil {
			continue
		}
		keys[encoded.KID] = key
	}
	if len(keys) == 0 {
		return ErrProviderResponse
	}
	provider.keys.keys = keys
	provider.keys.expiresAt = now.Add(provider.config.JWKSCacheTTL)
	return nil
}

func decodeRSAKey(modulus, exponent string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(modulus)
	if err != nil || len(nBytes) == 0 {
		return nil, ErrProviderResponse
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(exponent)
	if err != nil || len(eBytes) == 0 || len(eBytes) > 4 {
		return nil, ErrProviderResponse
	}
	exponentValue := 0
	for _, value := range eBytes {
		exponentValue = exponentValue<<8 + int(value)
	}
	if exponentValue < 3 {
		return nil, ErrProviderResponse
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: exponentValue}, nil
}

func validateConfig(config Config) error {
	if strings.TrimSpace(config.ClientID) == "" || strings.TrimSpace(config.ClientSecret) == "" {
		return errors.New("Telegram OIDC client ID and secret are required")
	}
	if config.RedirectURI == "" {
		return errors.New("Telegram OIDC redirect URI is required")
	}
	if len(config.AllowedAlgorithms) != 1 || config.AllowedAlgorithms[0] != "RS256" {
		return errors.New("Telegram OIDC must be pinned to RS256 for the MVP")
	}
	for name, raw := range map[string]string{
		"issuer": config.Issuer, "authorization endpoint": config.AuthorizationEndpoint,
		"token endpoint": config.TokenEndpoint, "JWKS URL": config.JWKSURL,
	} {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
			return fmt.Errorf("Telegram OIDC %s is invalid", name)
		}
		loopback := parsed.Scheme == "http" && (parsed.Hostname() == "localhost" || parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "::1")
		if parsed.Scheme != "https" && !(config.AllowLoopbackProvider && loopback) {
			return fmt.Errorf("Telegram OIDC %s must use HTTPS", name)
		}
	}
	if !config.AllowLoopbackProvider && (config.Issuer != DefaultIssuer ||
		config.AuthorizationEndpoint != DefaultAuthorizationEndpoint ||
		config.TokenEndpoint != DefaultTokenEndpoint || config.JWKSURL != DefaultJWKSURL) {
		return errors.New("non-local Telegram OIDC must use the documented Telegram issuer and endpoints")
	}
	if _, err := strconv.ParseUint(config.ClientID, 10, 64); err != nil && !config.AllowLoopbackProvider {
		return errors.New("Telegram OIDC client ID must be the numeric BotFather client ID")
	}
	return nil
}

var _ ports.OIDCProvider = (*Provider)(nil)
