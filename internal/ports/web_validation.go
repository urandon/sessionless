package ports

import (
	"encoding/base64"
	"net"
	"net/url"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func (request OIDCAuthorizationRequest) Validate() error {
	if err := request.Provider.Validate(); err != nil {
		return err
	}
	if err := validateRedirectURI(request.RedirectURI); err != nil {
		return err
	}
	if err := domain.ValidateOpaqueID("oidc.state", request.State); err != nil {
		return err
	}
	if err := domain.ValidateOpaqueID("oidc.nonce", request.Nonce); err != nil {
		return err
	}
	challenge, err := base64.RawURLEncoding.DecodeString(request.CodeChallenge)
	if err != nil || len(challenge) != 32 {
		return domain.ValidationError{Field: "oidc.code_challenge", Reason: "must be an S256 base64url digest"}
	}
	seen, hasOpenID := make(map[string]struct{}, len(request.Scopes)), false
	for _, scope := range request.Scopes {
		if strings.TrimSpace(scope) == "" {
			return domain.ValidationError{Field: "oidc.scopes", Reason: "must not contain empty values"}
		}
		if _, exists := seen[scope]; exists {
			return domain.ValidationError{Field: "oidc.scopes", Reason: "must not contain duplicates"}
		}
		if scope != "openid" && scope != "profile" {
			return domain.ValidationError{Field: "oidc.scopes", Reason: "only openid and profile are allowed for the MVP"}
		}
		seen[scope], hasOpenID = struct{}{}, hasOpenID || scope == "openid"
	}
	if !hasOpenID {
		return domain.ValidationError{Field: "oidc.scopes", Reason: "must include openid"}
	}
	return nil
}

func (request OIDCTokenRequest) Validate() error {
	if err := request.Provider.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(request.Code) == "" {
		return domain.ValidationError{Field: "oidc.code", Reason: "must not be empty"}
	}
	if err := validateRedirectURI(request.RedirectURI); err != nil {
		return err
	}
	if err := domain.ValidatePKCEVerifier(request.PKCEVerifier); err != nil {
		return err
	}
	if err := domain.ValidateOpaqueID("oidc.expected_nonce", request.ExpectedNonce); err != nil {
		return err
	}
	if request.Now.IsZero() {
		return domain.ValidationError{Field: "oidc.now", Reason: "must not be zero"}
	}
	if err := request.Policy.Validate(); err != nil {
		return err
	}
	return nil
}

func (request EnrollmentRequest) Validate() error {
	if err := request.Identity.Validate(); err != nil {
		return err
	}
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if request.At.IsZero() {
		return domain.ValidationError{Field: "enrollment.at", Reason: "must not be zero"}
	}
	switch request.Source {
	case domain.EnrollmentExistingFrontend:
		if request.FrontendBindingID == nil || request.InvitationID != nil || request.InvitationDigest != nil || request.Bootstrap != nil {
			return domain.ValidationError{Field: "enrollment.grant", Reason: "an existing frontend binding is required exclusively"}
		}
		if err := request.FrontendBindingID.Validate(); err != nil {
			return err
		}
	case domain.EnrollmentTenantInvitation:
		if request.InvitationID == nil || request.InvitationDigest == nil || request.FrontendBindingID != nil || request.Bootstrap != nil {
			return domain.ValidationError{Field: "enrollment.grant", Reason: "invitation ID and digest are required exclusively"}
		}
		if err := request.InvitationID.Validate(); err != nil {
			return err
		}
		if err := request.InvitationDigest.Validate("enrollment.invitation_digest"); err != nil {
			return err
		}
	case domain.EnrollmentDevelopmentBootstrap:
		if request.Bootstrap == nil || request.FrontendBindingID != nil || request.InvitationID != nil || request.InvitationDigest != nil {
			return domain.ValidationError{Field: "enrollment.grant", Reason: "development bootstrap is required exclusively"}
		}
		if err := request.Bootstrap.Validate(); err != nil {
			return err
		}
		if request.Bootstrap.TenantID != request.TenantID || request.Bootstrap.UserID != request.Identity.UserID {
			return domain.ErrMembershipDenied
		}
	default:
		return domain.ErrEnrollmentGrantRequired
	}
	return nil
}

func validateRedirectURI(raw string) error {
	redirect, err := url.Parse(raw)
	if err != nil || redirect.Host == "" || redirect.User != nil || redirect.Fragment != "" {
		return domain.ValidationError{Field: "oidc.redirect_uri", Reason: "must be an absolute URI without credentials or fragment"}
	}
	if redirect.Scheme == "https" {
		return nil
	}
	host := redirect.Hostname()
	parsedIP := net.ParseIP(host)
	if redirect.Scheme == "http" && (host == "localhost" || (parsedIP != nil && parsedIP.IsLoopback())) {
		return nil
	}
	return domain.ValidationError{Field: "oidc.redirect_uri", Reason: "must use HTTPS except for a loopback local-development callback"}
}
