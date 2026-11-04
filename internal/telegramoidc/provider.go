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
	callbackTimeout              = 10 * time.Second
)

var ErrProviderResponse = errors.New("Telegram OIDC provider response is invalid")

// ErrProviderUnavailable preserves fail-closed response compatibility while
// exposing only a finite transport/service category to operational observers.
var ErrProviderUnavailable = fmt.Errorf("%w: provider unavailable", ErrProviderResponse)

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
	client := &http.Client{}
	if config.HTTPClient != nil {
		*client = *config.HTTPClient
	}
	if client.Timeout <= 0 || client.Timeout > callbackTimeout {
		client.Timeout = callbackTimeout
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	config.AllowedAlgorithms = append([]string(nil), config.AllowedAlgorithms...)
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	return &Provider{config: config, client: client}, nil
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
	started := time.Now()
	ctx, cancel := context.WithTimeout(ctx, callbackTimeout)
	defer cancel()
	claims, _, err := provider.exchangeAndVerify(ctx, request, started)
	return claims, err
}

func (provider *Provider) ProofAuthorizationURL(ctx context.Context, request ports.IdentityProviderProofAuthorizationRequestV1) (string, error) {
	if request.Validate() != nil || !provider.matchesProofBinding(request.Binding) || ctx.Err() != nil {
		return "", ErrProviderResponse
	}
	raw, err := provider.AuthorizationURL(ctx, ports.OIDCAuthorizationRequest{Provider: domain.IdentityProviderTelegram, RedirectURI: request.Binding.CallbackURI, State: request.State, Nonce: request.Nonce, CodeChallenge: request.CodeChallenge, Scopes: []string{"openid", "profile"}})
	if err != nil {
		return "", err
	}
	u, _ := url.Parse(raw)
	q := u.Query()
	q.Set("max_age", "0")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (provider *Provider) ExchangeAndVerifyProof(ctx context.Context, request ports.IdentityProviderProofTokenRequestV1) (ports.IdentityProviderProofResultV1, error) {
	started := time.Now()
	if request.Validate() != nil || !provider.matchesProofBinding(request.Binding) || ctx.Err() != nil {
		return ports.IdentityProviderProofResultV1{}, ErrProviderResponse
	}
	ctx, cancel := context.WithDeadline(ctx, started.Add(min(callbackTimeout, request.ChallengeExpiresAt.Sub(request.Now))))
	defer cancel()
	ordinary := ports.OIDCTokenRequest{Provider: domain.IdentityProviderTelegram, Code: request.Code, RedirectURI: request.Binding.CallbackURI, PKCEVerifier: request.PKCEVerifier, ExpectedNonce: request.ExpectedNonce, Now: request.Now, Policy: domain.OIDCVerificationPolicy{Issuer: provider.config.Issuer, Audience: provider.config.ClientID, AllowedAlgorithms: []string{"RS256"}, MaxClockSkew: domain.IdentityProofClockSkewV2}}
	claims, signed, err := provider.exchangeAndVerify(ctx, ordinary, started)
	if err != nil {
		return ports.IdentityProviderProofResultV1{}, err
	}
	authTime, err := proofAuthTime(signed.AuthTime)
	completed := request.Now.Add(time.Since(started))
	if ctx.Err() != nil {
		return ports.IdentityProviderProofResultV1{}, ErrProviderUnavailable
	}
	if err != nil || !completed.Before(request.ChallengeExpiresAt) || authTime.Before(request.ChallengeCreatedAt.Add(-domain.IdentityProofClockSkewV2)) || authTime.After(completed.Add(domain.IdentityProofClockSkewV2)) {
		return ports.IdentityProviderProofResultV1{}, ErrProviderResponse
	}
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: claims.Subject}
	if subject.Validate() != nil {
		return ports.IdentityProviderProofResultV1{}, ErrProviderResponse
	}
	return ports.IdentityProviderProofResultV1{Subject: subject, Kind: domain.IdentityProofTelegramAuthTimeV2, SignedAuthTime: &authTime}, nil
}

func (provider *Provider) matchesProofBinding(binding domain.IdentityProviderBindingV2) bool {
	return binding == (domain.IdentityProviderBindingV2{Provider: domain.IdentityProviderTelegram, Issuer: provider.config.Issuer, ClientID: provider.config.ClientID, CallbackURI: provider.config.RedirectURI})
}

func (provider *Provider) exchangeAndVerify(ctx context.Context, request ports.OIDCTokenRequest, started time.Time) (domain.OIDCIdentityClaims, *telegramClaims, error) {
	deny := func(err error) (domain.OIDCIdentityClaims, *telegramClaims, error) {
		return domain.OIDCIdentityClaims{}, nil, err
	}
	if ctx.Err() != nil {
		return deny(ErrProviderUnavailable)
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
		return deny(ErrProviderResponse)
	}
	httpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	httpRequest.SetBasicAuth(provider.config.ClientID, provider.config.ClientSecret)
	response, err := provider.client.Do(httpRequest)
	if err != nil {
		return deny(ErrProviderUnavailable)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if expectedOAuthRejection(response.StatusCode, response.Body) && ctx.Err() == nil {
			return deny(ErrProviderResponse)
		}
		return deny(ErrProviderUnavailable)
	}
	tokens, err := readProviderObject(response.Body)
	var idToken string
	if err != nil || json.Unmarshal(tokens["id_token"], &idToken) != nil || idToken == "" || ctx.Err() != nil {
		return deny(ErrProviderUnavailable)
	}
	claims, signed, err := provider.verifyIDToken(ctx, idToken, request.Now.Add(time.Since(started)))
	if err != nil {
		return deny(err)
	}
	if ctx.Err() != nil {
		return deny(ErrProviderUnavailable)
	}
	if err := claims.Verify(request.Policy, request.ExpectedNonce, request.Now.Add(time.Since(started))); err != nil {
		return deny(ErrProviderResponse)
	}
	if ctx.Err() != nil {
		return deny(ErrProviderUnavailable)
	}
	return claims, signed, nil
}

type telegramClaims struct {
	jwt.RegisteredClaims
	Nonce    string          `json:"nonce"`
	AuthTime json.RawMessage `json:"auth_time"`
}

func (provider *Provider) verifyIDToken(
	ctx context.Context,
	rawToken string,
	now time.Time,
) (domain.OIDCIdentityClaims, *telegramClaims, error) {
	// Reject ambiguous duplicate claim fields before parsing. Values remain
	// untrusted until the SAME token's signature has been verified below.
	parts := strings.SplitN(rawToken, ".", 4)
	if len(parts) != 3 {
		return domain.OIDCIdentityClaims{}, nil, ErrProviderResponse
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return domain.OIDCIdentityClaims{}, nil, ErrProviderResponse
	}
	values, err := readProviderObject(strings.NewReader(string(payload)))
	if err != nil {
		return domain.OIDCIdentityClaims{}, nil, ErrProviderResponse
	}
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
		if errors.Is(err, ErrProviderUnavailable) {
			return domain.OIDCIdentityClaims{}, nil, ErrProviderUnavailable
		}
		return domain.OIDCIdentityClaims{}, nil, fmt.Errorf("%w: ID token verification failed", ErrProviderResponse)
	}
	if claims.IssuedAt == nil || claims.ExpiresAt == nil {
		return domain.OIDCIdentityClaims{}, nil, ErrProviderResponse
	}
	// encoding/json's case-insensitive struct matching is not proof-field
	// authority. Only the exact signed auth_time member can provide freshness.
	claims.AuthTime = values["auth_time"]
	return domain.OIDCIdentityClaims{
		Issuer: claims.Issuer, Audience: []string(claims.Audience), Subject: claims.Subject,
		Nonce: claims.Nonce, IssuedAt: claims.IssuedAt.Time.UTC(), ExpiresAt: claims.ExpiresAt.Time.UTC(),
	}, claims, nil
}

func (provider *Provider) signingKey(ctx context.Context, kid string, now time.Time) (any, error) {
	provider.keys.mu.Lock()
	if now.Before(provider.keys.expiresAt) {
		if key, found := provider.keys.keys[kid]; found {
			provider.keys.mu.Unlock()
			return key, nil
		}
	}
	provider.keys.mu.Unlock()
	// No network I/O while holding the cache mutex: another bounded exchange
	// must not inherit this exchange's stalled fetch. At most one fetch here.
	keys, err := provider.fetchKeys(ctx)
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, ErrProviderUnavailable
	}
	provider.keys.mu.Lock()
	provider.keys.keys, provider.keys.expiresAt = keys, now.Add(provider.config.JWKSCacheTTL)
	provider.keys.mu.Unlock()
	key, found := keys[kid]
	if !found {
		return nil, ErrProviderResponse
	}
	return key, nil
}

func (provider *Provider) fetchKeys(ctx context.Context) (map[string]any, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, provider.config.JWKSURL, nil)
	if err != nil {
		return nil, ErrProviderResponse
	}
	response, err := provider.client.Do(request)
	if err != nil {
		return nil, ErrProviderUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, ErrProviderUnavailable
	}
	var encodedKeys []json.RawMessage
	values, err := readProviderObject(response.Body)
	if err != nil || json.Unmarshal(values["keys"], &encodedKeys) != nil || ctx.Err() != nil {
		return nil, ErrProviderUnavailable
	}
	keys := make(map[string]any)
	for _, encoded := range encodedKeys {
		fields, err := readProviderObject(strings.NewReader(string(encoded)))
		if err != nil {
			return nil, ErrProviderUnavailable
		}
		var kid, kind, algorithm, use, modulus, exponent string
		if json.Unmarshal(fields["kid"], &kid) != nil || json.Unmarshal(fields["kty"], &kind) != nil || json.Unmarshal(fields["alg"], &algorithm) != nil || json.Unmarshal(fields["n"], &modulus) != nil || json.Unmarshal(fields["e"], &exponent) != nil {
			continue
		}
		if raw, present := fields["use"]; present && json.Unmarshal(raw, &use) != nil {
			continue
		}
		if kid == "" || kind != "RSA" || algorithm != "RS256" || (use != "" && use != "sig") {
			continue
		}
		key, err := decodeRSAKey(modulus, exponent)
		if err != nil {
			continue
		}
		if _, duplicate := keys[kid]; duplicate {
			return nil, ErrProviderUnavailable
		}
		keys[kid] = key
	}
	if len(keys) == 0 {
		return nil, ErrProviderUnavailable
	}
	return keys, nil
}

func readProviderObject(reader io.Reader) (map[string]json.RawMessage, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxProviderResponseBytes+1))
	if err != nil || len(body) > maxProviderResponseBytes {
		return nil, ErrProviderUnavailable
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return nil, ErrProviderUnavailable
	}
	values := make(map[string]json.RawMessage)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, ErrProviderUnavailable
		}
		name, ok := key.(string)
		if !ok {
			return nil, ErrProviderUnavailable
		}
		if _, duplicate := values[name]; duplicate {
			return nil, ErrProviderUnavailable
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return nil, ErrProviderUnavailable
		}
		values[name] = value
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return nil, ErrProviderUnavailable
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return nil, ErrProviderUnavailable
	}
	return values, nil
}

// Preserve signed fractional NumericDate values exactly through nanoseconds.
// jwt.NumericDate uses process-global second precision and must not be used
// for this proof boundary (nor may that global setting be mutated).
func proofAuthTime(raw json.RawMessage) (time.Time, error) {
	value := strings.TrimSpace(string(raw))
	if len(value) == 0 || len(value) > 64 || value[0] < '0' || value[0] > '9' {
		return time.Time{}, ErrProviderResponse
	}
	var number json.Number
	if json.Unmarshal([]byte(value), &number) != nil {
		return time.Time{}, ErrProviderResponse
	}
	// This coarse representation bound is NOT a freshness decision. Check it
	// before Rat.SetString so enormous exponents cannot allocate huge integers.
	approximate, err := strconv.ParseFloat(number.String(), 64)
	if err != nil || approximate < 1e-9 || approximate > 1e10 {
		return time.Time{}, ErrProviderResponse
	}
	seconds, ok := new(big.Rat).SetString(number.String())
	if !ok || seconds.Sign() <= 0 {
		return time.Time{}, ErrProviderResponse
	}
	nanos := new(big.Rat).Mul(seconds, big.NewRat(int64(time.Second), 1))
	if !nanos.IsInt() || !nanos.Num().IsInt64() {
		return time.Time{}, ErrProviderResponse
	}
	return time.Unix(0, nanos.Num().Int64()).UTC(), nil
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

// expectedOAuthRejection admits only explicit, bounded authorization-code
// rejections. Client authentication/configuration failures and malformed or
// unknown responses are operational failures, not user admission denials.
func expectedOAuthRejection(status int, reader io.Reader) bool {
	body, err := io.ReadAll(io.LimitReader(reader, maxProviderResponseBytes+1))
	if err != nil || len(body) > maxProviderResponseBytes || status != http.StatusBadRequest {
		return false
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	seen := make(map[string]struct{})
	var code string
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return false
		}
		name, ok := key.(string)
		if !ok {
			return false
		}
		if _, duplicate := seen[name]; duplicate {
			return false
		}
		seen[name] = struct{}{}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return false
		}
		if name == "error" && json.Unmarshal(value, &code) != nil {
			return false
		}
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return false
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return false
	}
	return code == "invalid_grant" || code == "access_denied"
}

var _ ports.OIDCProvider = (*Provider)(nil)
var _ ports.IdentityProviderProverV1 = (*Provider)(nil)
