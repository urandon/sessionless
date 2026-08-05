package ports_test

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestOIDCRequestContracts(t *testing.T) {
	t.Parallel()
	authorization := ports.OIDCAuthorizationRequest{
		Provider:    domain.IdentityProviderTelegram,
		RedirectURI: "https://web.dev.sessionless.triborg.dev/auth/telegram/callback",
		State:       "state-1", Nonce: "nonce-1", CodeChallenge: strings.Repeat("a", 43),
		Scopes: []string{"openid", "profile"},
	}
	if err := authorization.Validate(); err != nil {
		t.Fatalf("valid authorization request rejected: %v", err)
	}
	authorization.RedirectURI = "http://attacker.invalid/callback"
	if err := authorization.Validate(); err == nil {
		t.Fatal("non-loopback HTTP redirect accepted")
	}
	authorization.RedirectURI = "http://127.0.0.1:8080/auth/telegram/callback"
	if err := authorization.Validate(); err != nil {
		t.Fatalf("local loopback redirect rejected: %v", err)
	}

	token := ports.OIDCTokenRequest{
		Provider: domain.IdentityProviderTelegram, Code: "authorization-code",
		RedirectURI:  authorization.RedirectURI,
		PKCEVerifier: strings.Repeat("b", 64), ExpectedNonce: "nonce-1",
		Policy: domain.OIDCVerificationPolicy{
			Issuer: "https://oauth.telegram.org", Audience: "123", AllowedAlgorithms: []string{"RS256"},
		},
		Now: portTestTime,
	}
	if err := token.Validate(); err != nil {
		t.Fatalf("valid token request rejected: %v", err)
	}
}

func TestEnrollmentRequestRequiresOneExplicitGrant(t *testing.T) {
	t.Parallel()
	identity := domain.ExternalIdentity{
		Subject: domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "1001"},
		UserID:  "user-a", CreatedAt: portTestTime, UpdatedAt: portTestTime,
	}
	base := ports.EnrollmentRequest{Identity: identity, TenantID: "tenant-a", At: portTestTime}
	if err := base.Validate(); err == nil {
		t.Fatal("enrollment without a grant accepted")
	}
	inviteID := domain.TenantInvitationID("invite-1")
	digest := domain.DigestSecret("invitation")
	base.Source, base.InvitationID, base.InvitationDigest = domain.EnrollmentTenantInvitation, &inviteID, &digest
	if err := base.Validate(); err != nil {
		t.Fatalf("valid invitation enrollment rejected: %v", err)
	}
	bootstrap := domain.DevelopmentBootstrapGrant{
		TenantID: "tenant-a", UserID: "user-a", Role: domain.TenantMembershipOwner,
		Environment: domain.DevelopmentEnvironment, Operator: "operator@example.test",
		Reason: "initial operator", GrantedAt: portTestTime,
	}
	base.Source, base.InvitationID, base.InvitationDigest, base.Bootstrap = domain.EnrollmentDevelopmentBootstrap, nil, nil, &bootstrap
	if err := base.Validate(); err != nil {
		t.Fatalf("valid bootstrap enrollment rejected: %v", err)
	}
	bootstrap.GrantedAt = time.Time{}
	if err := base.Validate(); err == nil {
		t.Fatal("unauditable bootstrap enrollment accepted")
	}
}
