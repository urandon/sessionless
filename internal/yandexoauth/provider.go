// Package yandexoauth implements Yandex ID Authorization Code + S256 PKCE.
// This is OAuth with a trusted account API, not an OIDC JWT/JWKS protocol.
// Tokens never leave this adapter or survive the callback exchange.
package yandexoauth

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	DefaultAuthorizationEndpoint = "https://oauth.yandex.ru/authorize"
	DefaultTokenEndpoint         = "https://oauth.yandex.ru/token"
	DefaultInfoEndpoint          = "https://login.yandex.ru/info"
	maxResponseBytes             = 32 << 10
	callbackTimeout              = 10 * time.Second
)

var ErrProviderResponse = errors.New("Yandex authentication could not be verified")

type Config struct {
	ClientID              string
	ClientSecret          string
	RedirectURI           string
	AuthorizationEndpoint string
	TokenEndpoint         string
	InfoEndpoint          string
	HTTPClient            *http.Client
	AllowLoopbackProvider bool
}

type Provider struct {
	config Config
	client *http.Client
}

func New(config Config) (*Provider, error) {
	if config.ClientID == "" || strings.TrimSpace(config.ClientID) != config.ClientID || len(config.ClientID) > 160 || config.ClientSecret == "" || len(config.ClientSecret) > 8192 {
		return nil, errors.New("Yandex client ID and server-side client secret are required")
	}
	redirect, err := url.Parse(config.RedirectURI)
	if err != nil || redirect.Scheme != "https" || redirect.Host == "" || redirect.User != nil || redirect.RawQuery != "" || redirect.Fragment != "" || redirect.Path != "/auth/login/callback" {
		return nil, errors.New("Yandex callback must be the exact HTTPS login callback")
	}
	if config.AuthorizationEndpoint == "" {
		config.AuthorizationEndpoint = DefaultAuthorizationEndpoint
	}
	if config.TokenEndpoint == "" {
		config.TokenEndpoint = DefaultTokenEndpoint
	}
	if config.InfoEndpoint == "" {
		config.InfoEndpoint = DefaultInfoEndpoint
	}
	endpoints := []string{config.AuthorizationEndpoint, config.TokenEndpoint, config.InfoEndpoint}
	defaults := []string{DefaultAuthorizationEndpoint, DefaultTokenEndpoint, DefaultInfoEndpoint}
	for i, endpoint := range endpoints {
		if endpoint == defaults[i] {
			continue
		}
		u, err := url.Parse(endpoint)
		if !config.AllowLoopbackProvider || err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
			return nil, errors.New("Yandex endpoints must be pinned; fixtures must be explicit loopback endpoints")
		}
	}
	// Never follow token/profile redirects, including same-origin redirects.
	client := &http.Client{}
	if config.HTTPClient != nil {
		*client = *config.HTTPClient
	}
	if client.Timeout <= 0 || client.Timeout > callbackTimeout {
		client.Timeout = callbackTimeout
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Provider{config: config, client: client}, nil
}

func (p *Provider) AuthorizationURL(_ context.Context, request ports.OAuthAuthorizationRequest) (string, error) {
	if request.RedirectURI != p.config.RedirectURI || domain.ValidateOpaqueID("oauth.state", request.State) != nil {
		return "", ErrProviderResponse
	}
	challenge, err := base64.RawURLEncoding.DecodeString(request.CodeChallenge)
	if err != nil || len(challenge) != 32 || base64.RawURLEncoding.EncodeToString(challenge) != request.CodeChallenge {
		return "", ErrProviderResponse
	}
	u, _ := url.Parse(p.config.AuthorizationEndpoint)
	q := u.Query()
	q.Set("response_type", "code")
	q.Set("client_id", p.config.ClientID)
	q.Set("redirect_uri", p.config.RedirectURI)
	q.Set("scope", "login:info")
	q.Set("state", request.State)
	q.Set("code_challenge", request.CodeChallenge)
	q.Set("code_challenge_method", "S256")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (p *Provider) ExchangeAndVerify(ctx context.Context, request ports.OAuthTokenRequest) (domain.ExternalSubject, error) {
	if request.RedirectURI != p.config.RedirectURI || request.Code == "" || len(request.Code) > 4096 || domain.ValidatePKCEVerifier(request.PKCEVerifier) != nil {
		return domain.ExternalSubject{}, ErrProviderResponse
	}
	ctx, cancel := context.WithTimeout(ctx, callbackTimeout)
	defer cancel()
	form := url.Values{"grant_type": {"authorization_code"}, "code": {request.Code}, "code_verifier": {request.PKCEVerifier}, "redirect_uri": {p.config.RedirectURI}}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, p.config.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return domain.ExternalSubject{}, ErrProviderResponse
	}
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.SetBasicAuth(p.config.ClientID, p.config.ClientSecret)
	values, err := p.jsonResponse(r)
	if err != nil {
		return domain.ExternalSubject{}, ErrProviderResponse
	}
	var access, kind string
	var expires int64
	if json.Unmarshal(values["access_token"], &access) != nil || access == "" || len(access) > 8192 || strings.ContainsAny(access, "\r\n\t ") || json.Unmarshal(values["token_type"], &kind) != nil || !strings.EqualFold(kind, "bearer") || json.Unmarshal(values["expires_in"], &expires) != nil || expires <= 0 {
		return domain.ExternalSubject{}, ErrProviderResponse
	}
	u, _ := url.Parse(p.config.InfoEndpoint)
	u.RawQuery = "format=json"
	r, err = http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return domain.ExternalSubject{}, ErrProviderResponse
	}
	r.Header.Set("Authorization", "OAuth "+access)
	values, err = p.jsonResponse(r)
	if err != nil {
		return domain.ExternalSubject{}, ErrProviderResponse
	}
	var id, clientID string
	if json.Unmarshal(values["id"], &id) != nil || json.Unmarshal(values["client_id"], &clientID) != nil || subtle.ConstantTimeCompare([]byte(clientID), []byte(p.config.ClientID)) != 1 || id == "" || len(id) > 160 || id[0] == '0' {
		return domain.ExternalSubject{}, ErrProviderResponse
	}
	for _, c := range id {
		if c < '0' || c > '9' {
			return domain.ExternalSubject{}, ErrProviderResponse
		}
	}
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: id}
	if subject.Validate() != nil {
		return domain.ExternalSubject{}, ErrProviderResponse
	}
	return subject, nil
}

func (p *Provider) jsonResponse(r *http.Request) (map[string]json.RawMessage, error) {
	r.Header.Set("Accept", "application/json")
	response, err := p.client.Do(r)
	if err != nil {
		return nil, ErrProviderResponse
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxResponseBytes))
		return nil, ErrProviderResponse
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(body) > maxResponseBytes {
		return nil, ErrProviderResponse
	}
	// Reject duplicate security-bearing fields and trailing documents. Optional
	// profile fields are not projected and never cross the identity boundary.
	d := json.NewDecoder(strings.NewReader(string(body)))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, ErrProviderResponse
	}
	values := make(map[string]json.RawMessage)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil, ErrProviderResponse
		}
		name, ok := key.(string)
		if !ok {
			return nil, ErrProviderResponse
		}
		if _, exists := values[name]; exists {
			return nil, ErrProviderResponse
		}
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return nil, ErrProviderResponse
		}
		values[name] = value
	}
	if end, err := d.Token(); err != nil || end != json.Delim('}') {
		return nil, ErrProviderResponse
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, ErrProviderResponse
	}
	return values, nil
}

var _ ports.OAuthIdentityProvider = (*Provider)(nil)
