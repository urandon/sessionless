package domain_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

var webTestTime = time.Date(2026, 8, 5, 9, 0, 0, 0, time.UTC)

func externalSubject(id string) domain.ExternalSubject {
	return domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: id}
}

func membership(tenant domain.TenantID, user domain.UserID, role domain.TenantMembershipRole) domain.TenantMembership {
	return domain.TenantMembership{
		TenantID: tenant, UserID: user, Role: role, Status: domain.TenantMembershipActive,
		SecurityVersion: 2, CreatedAt: webTestTime, UpdatedAt: webTestTime,
	}
}

func invitation(target *domain.ExternalSubject) domain.TenantInvitation {
	return domain.TenantInvitation{
		ID: "invite-1", TenantID: "tenant-a", SecretDigest: domain.DigestSecret("invite-secret"),
		Role: domain.TenantMembershipMember, TargetSubject: target,
		CreatedAt: webTestTime, ExpiresAt: webTestTime.Add(time.Hour),
	}
}

func webSession(tenant domain.TenantID, user domain.UserID, token string, issued time.Time) domain.WebSession {
	return domain.WebSession{
		SessionDigest: domain.DigestSecret(token), CSRFTokenDigest: domain.DigestSecret(token + "-csrf"),
		UserID: user, ActiveTenantID: tenant, AuthenticatedSubject: externalSubject("1001"),
		MembershipSecurityVersion: 2, IssuedAt: issued, LastSeenAt: issued,
		IdleExpiresAt: issued.Add(12 * time.Hour), AbsoluteExpiresAt: issued.Add(7 * 24 * time.Hour),
	}
}

func TestOIDCClaimsVerificationMatrix(t *testing.T) {
	t.Parallel()
	policy := domain.OIDCVerificationPolicy{
		Issuer: "https://oauth.telegram.org", Audience: "123456",
		AllowedAlgorithms: []string{"RS256"}, MaxClockSkew: time.Minute,
	}
	valid := domain.OIDCIdentityClaims{
		Issuer: policy.Issuer, Audience: []string{"123456"}, Subject: "1001", Nonce: "nonce-1",
		IssuedAt: webTestTime.Add(-time.Minute), ExpiresAt: webTestTime.Add(time.Hour),
	}
	if err := valid.Verify(policy, "nonce-1", webTestTime); err != nil {
		t.Fatalf("valid claims rejected: %v", err)
	}
	tests := map[string]func(*domain.OIDCIdentityClaims){
		"issuer":   func(claims *domain.OIDCIdentityClaims) { claims.Issuer = "https://attacker.invalid" },
		"audience": func(claims *domain.OIDCIdentityClaims) { claims.Audience = []string{"other-client"} },
		"nonce":    func(claims *domain.OIDCIdentityClaims) { claims.Nonce = "replayed-nonce" },
		"expiry":   func(claims *domain.OIDCIdentityClaims) { claims.ExpiresAt = webTestTime.Add(-2 * time.Minute) },
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			claims := valid
			mutate(&claims)
			if err := claims.Verify(policy, "nonce-1", webTestTime); err == nil {
				t.Fatal("invalid OIDC claims accepted")
			}
		})
	}
}

func TestLoginChallengeIsBrowserBoundExpiringAndOneTime(t *testing.T) {
	t.Parallel()
	challenge := domain.OIDCLoginChallenge{
		StateDigest: domain.DigestSecret("state"), BrowserBindingDigest: domain.DigestSecret("browser"),
		PKCEVerifier: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~",
		Nonce:        "nonce-1", RedirectPath: "/sessions", CreatedAt: webTestTime,
		ExpiresAt: webTestTime.Add(10 * time.Minute),
	}
	wrongBrowser := challenge
	if err := wrongBrowser.Consume("attacker", webTestTime.Add(time.Minute)); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("wrong browser error = %v", err)
	}
	expired := challenge
	if err := expired.Consume("browser", expired.ExpiresAt); !errors.Is(err, domain.ErrLoginChallengeExpired) {
		t.Fatalf("expired challenge error = %v", err)
	}
	if err := challenge.Consume("browser", webTestTime.Add(time.Minute)); err != nil {
		t.Fatalf("challenge consume failed: %v", err)
	}
	if err := challenge.Consume("browser", webTestTime.Add(2*time.Minute)); !errors.Is(err, domain.ErrLoginChallengeConsumed) {
		t.Fatalf("replayed challenge error = %v", err)
	}
}

func TestExternalIdentityCannotBeRemapped(t *testing.T) {
	t.Parallel()
	existing := domain.ExternalIdentity{
		Subject: externalSubject("1001"), UserID: "user-a",
		CreatedAt: webTestTime, UpdatedAt: webTestTime,
	}
	retry := existing
	retry.UpdatedAt = webTestTime.Add(time.Minute)
	if err := domain.ValidateExternalIdentityMapping(existing, retry); err != nil {
		t.Fatalf("same identity refresh rejected: %v", err)
	}
	retry.UserID = "user-b"
	if err := domain.ValidateExternalIdentityMapping(existing, retry); !errors.Is(err, domain.ErrExternalIdentityConflict) {
		t.Fatalf("identity remap error = %v", err)
	}
}

func TestEnrollmentPrecedenceAndGrantFailures(t *testing.T) {
	t.Parallel()
	subject := externalSubject("1001")
	bindingID := domain.FrontendBindingID("binding-1")
	invite := invitation(&subject)
	bootstrap := domain.DevelopmentBootstrapGrant{
		TenantID: "tenant-a", UserID: "user-a", Role: domain.TenantMembershipOwner,
		Environment: domain.DevelopmentEnvironment, Operator: "operator@example.test",
		Reason: "initial cloud-dev operator", GrantedAt: webTestTime,
	}
	source, err := domain.SelectEnrollmentSource(domain.EnrollmentCandidates{
		ExistingFrontendBindingID: &bindingID, Invitation: &invite, Bootstrap: &bootstrap,
	}, subject, "user-a", webTestTime)
	if err != nil || source != domain.EnrollmentExistingFrontend {
		t.Fatalf("source = %q, err = %v", source, err)
	}
	source, err = domain.SelectEnrollmentSource(domain.EnrollmentCandidates{Invitation: &invite, Bootstrap: &bootstrap}, subject, "user-a", webTestTime)
	if err != nil || source != domain.EnrollmentTenantInvitation {
		t.Fatalf("invitation source = %q, err = %v", source, err)
	}
	source, err = domain.SelectEnrollmentSource(domain.EnrollmentCandidates{Bootstrap: &bootstrap}, subject, "user-a", webTestTime)
	if err != nil || source != domain.EnrollmentDevelopmentBootstrap {
		t.Fatalf("bootstrap source = %q, err = %v", source, err)
	}
	if _, err := domain.SelectEnrollmentSource(domain.EnrollmentCandidates{}, subject, "user-a", webTestTime); !errors.Is(err, domain.ErrEnrollmentGrantRequired) {
		t.Fatalf("missing grant error = %v", err)
	}

	wrongSubject := invite
	if _, err := domain.SelectEnrollmentSource(domain.EnrollmentCandidates{Invitation: &wrongSubject}, externalSubject("2002"), "user-a", webTestTime); !errors.Is(err, domain.ErrInvitationSubjectMismatch) {
		t.Fatalf("wrong-subject invitation error = %v", err)
	}
	expired := invite
	if _, err := domain.SelectEnrollmentSource(domain.EnrollmentCandidates{Invitation: &expired}, subject, "user-a", expired.ExpiresAt); !errors.Is(err, domain.ErrInvitationExpired) {
		t.Fatalf("expired invitation error = %v", err)
	}
	bootstrap.Environment = "production"
	if _, err := domain.SelectEnrollmentSource(domain.EnrollmentCandidates{Bootstrap: &bootstrap}, subject, "user-a", webTestTime); err == nil {
		t.Fatal("production development-bootstrap grant accepted")
	}
}

func TestInvitationConcurrentConsumptionHasOneWinner(t *testing.T) {
	var lock sync.Mutex
	invite := invitation(nil)
	var winners atomic.Uint32
	var wait sync.WaitGroup
	for index := 0; index < 16; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			lock.Lock()
			defer lock.Unlock()
			if invite.Consume(externalSubject("1001"), "user-a", webTestTime.Add(time.Minute)) == nil {
				winners.Add(1)
			}
		}()
	}
	wait.Wait()
	if winners.Load() != 1 {
		t.Fatalf("invitation winners = %d, want 1", winners.Load())
	}
}

func TestWebAuthorizationMatrixAndSessionRotation(t *testing.T) {
	t.Parallel()
	ownerA := membership("tenant-a", "user-a", domain.TenantMembershipOwner)
	viewerB := membership("tenant-b", "user-a", domain.TenantMembershipViewer)
	otherUser := membership("tenant-a", "user-b", domain.TenantMembershipMember)
	suspended := ownerA
	suspended.Status = domain.TenantMembershipSuspended
	sessionA := webSession("tenant-a", "user-a", "session-a", webTestTime)
	if err := sessionA.Authorize(ownerA, domain.TenantPermissionAdmin, webTestTime.Add(time.Minute)); err != nil {
		t.Fatalf("owner authorization failed: %v", err)
	}
	for name, candidate := range map[string]domain.TenantMembership{
		"wrong user": otherUser, "wrong tenant": viewerB, "suspended": suspended,
	} {
		if err := sessionA.Authorize(candidate, domain.TenantPermissionRead, webTestTime.Add(time.Minute)); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatalf("%s error = %v", name, err)
		}
	}
	sessionB := webSession("tenant-b", "user-a", "session-b", webTestTime.Add(time.Minute))
	if err := domain.ValidateWebSessionRotation(sessionA, sessionB, viewerB, webTestTime.Add(time.Minute)); err != nil {
		t.Fatalf("valid tenant switch rejected: %v", err)
	}
	if err := sessionB.Authorize(viewerB, domain.TenantPermissionWrite, webTestTime.Add(2*time.Minute)); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("viewer write error = %v", err)
	}
	changed := viewerB
	changed.SecurityVersion++
	if err := sessionB.Authorize(changed, domain.TenantPermissionRead, webTestTime.Add(2*time.Minute)); !errors.Is(err, domain.ErrMembershipVersionChanged) {
		t.Fatalf("security-version error = %v", err)
	}
	revoked := sessionA
	if err := revoked.Revoke(webTestTime.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := revoked.Authorize(ownerA, domain.TenantPermissionRead, webTestTime.Add(2*time.Minute)); !errors.Is(err, domain.ErrWebSessionRevoked) {
		t.Fatalf("revoked session error = %v", err)
	}
	if err := sessionA.Authorize(ownerA, domain.TenantPermissionRead, sessionA.IdleExpiresAt); !errors.Is(err, domain.ErrWebSessionExpired) {
		t.Fatalf("expired session error = %v", err)
	}
}
