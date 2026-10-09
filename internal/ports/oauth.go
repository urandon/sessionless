package ports

import (
	"context"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// OAuthAuthorizationRequest carries only browser-facing authorization values.
// Credentials, access tokens and raw user-info responses remain in the adapter.
type OAuthAuthorizationRequest struct{ RedirectURI, State, CodeChallenge string }
type OAuthTokenRequest struct{ Code, RedirectURI, PKCEVerifier string }

// OAuthIdentityProvider exchanges a code and authenticates an issuer-scoped
// subject using the pinned provider's authenticated user-info endpoint. It does
// not fabricate OIDC claims from an OAuth access token.
type OAuthIdentityProvider interface {
	AuthorizationURL(context.Context, OAuthAuthorizationRequest) (string, error)
	ExchangeAndVerify(context.Context, OAuthTokenRequest) (domain.ExternalSubject, error)
}
