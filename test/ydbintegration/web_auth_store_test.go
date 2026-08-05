//go:build ydbintegration

package ydbintegration

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

func TestLoginChallengeConsumptionHasExactlyOneWinner(t *testing.T) {
	store, _ := openStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	bindingSecret := "browser-binding-secret"
	challenge := domain.OIDCLoginChallenge{
		StateDigest:          domain.DigestSecret("one-time-state"),
		BrowserBindingDigest: domain.DigestSecret(bindingSecret),
		PKCEVerifier:         "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ",
		Nonce:                "one-time-nonce", RedirectPath: "/sessions",
		CreatedAt: now, ExpiresAt: now.Add(10 * time.Minute),
	}
	if err := store.CreateLoginChallenge(context.Background(), challenge); err != nil {
		t.Fatal(err)
	}
	const contenders = 8
	results := make(chan error, contenders)
	var wait sync.WaitGroup
	for range contenders {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := store.ConsumeLoginChallenge(
				context.Background(), challenge.StateDigest, bindingSecret, now.Add(time.Second),
			)
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
			continue
		}
		if !errors.Is(err, domain.ErrLoginChallengeConsumed) {
			t.Fatalf("loser error = %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("challenge winners = %d, want 1", winners)
	}
}

func TestTelegramIdentityMaterializesWebIdentityAndMembership(t *testing.T) {
	store, _ := openStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-web-identity"))
	request := webTelegramIdentityFixture(tenantID, "424242", now)
	state, err := store.EnsureTelegramIdentity(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	identity, created, err := store.ResolveOrCreateExternalIdentity(
		context.Background(),
		domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"},
		domain.UserID(uniqueID("untrusted-candidate")), now.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if created || identity.UserID != state.UserID {
		t.Fatalf("identity = %+v created=%t state=%+v", identity, created, state)
	}
	memberships, err := store.ListTenantMemberships(context.Background(), state.UserID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(memberships) != 1 || memberships[0].TenantID != tenantID ||
		memberships[0].Role != domain.TenantMembershipOwner || memberships[0].Status != domain.TenantMembershipActive {
		t.Fatalf("memberships = %+v", memberships)
	}
	bootstrapTenant := domain.TenantID(uniqueID("tenant-web-bootstrap"))
	grant := domain.DevelopmentBootstrapGrant{
		TenantID: bootstrapTenant, UserID: state.UserID, Role: domain.TenantMembershipOwner,
		Environment: domain.DevelopmentEnvironment, Operator: "ci@example.invalid",
		Reason: "verify audited cloud-dev bootstrap", GrantedAt: now.Add(2 * time.Second),
	}
	bootstrapped, err := store.BootstrapDevelopmentMembership(context.Background(), grant)
	if err != nil || bootstrapped.TenantID != bootstrapTenant {
		t.Fatalf("bootstrapped membership = %+v err=%v", bootstrapped, err)
	}
	grant.GrantedAt = grant.GrantedAt.Add(time.Second)
	if repeated, err := store.BootstrapDevelopmentMembership(context.Background(), grant); err != nil || repeated != bootstrapped {
		t.Fatalf("idempotent bootstrap = %+v err=%v", repeated, err)
	}
	grant.Reason = "changed authority"
	if _, err := store.BootstrapDevelopmentMembership(context.Background(), grant); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("changed bootstrap grant error = %v", err)
	}
}

func TestWebSessionRotationTenantIsolationAndMembershipVersion(t *testing.T) {
	store, client := openStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	actorID := domain.ActorID(uniqueID("shared-web-actor"))
	userSubject := "515151"
	tenantA := domain.TenantID(uniqueID("tenant-web-a"))
	tenantB := domain.TenantID(uniqueID("tenant-web-b"))
	stateA, err := store.EnsureTelegramIdentity(context.Background(), webTelegramIdentityFixtureWithActor(tenantA, actorID, userSubject, now))
	if err != nil {
		t.Fatal(err)
	}
	stateB, err := store.EnsureTelegramIdentity(context.Background(), webTelegramIdentityFixtureWithActor(tenantB, actorID, userSubject, now))
	if err != nil {
		t.Fatal(err)
	}
	if stateA.UserID != stateB.UserID {
		t.Fatalf("same external identity resolved to %q and %q", stateA.UserID, stateB.UserID)
	}
	memberships, err := store.ListTenantMemberships(context.Background(), stateA.UserID, 10)
	if err != nil || len(memberships) != 2 {
		t.Fatalf("memberships = %+v err=%v", memberships, err)
	}
	membershipByTenant := map[domain.TenantID]domain.TenantMembership{}
	for _, membership := range memberships {
		membershipByTenant[membership.TenantID] = membership
	}
	previous := webSessionFixture("previous", stateA.UserID, tenantA, userSubject, membershipByTenant[tenantA].SecurityVersion, now)
	if err := store.CreateWebSession(context.Background(), previous); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthorizeWebSession(context.Background(), previous.SessionDigest, domain.TenantPermissionAdmin, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	next := webSessionFixture("next", stateA.UserID, tenantB, userSubject, membershipByTenant[tenantB].SecurityVersion, now.Add(2*time.Minute))
	if _, err := store.SwitchTenant(context.Background(), previous.SessionDigest, next, tenantB, next.IssuedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthorizeWebSession(context.Background(), previous.SessionDigest, domain.TenantPermissionRead, now.Add(3*time.Minute)); !errors.Is(err, domain.ErrWebSessionRevoked) {
		t.Fatalf("old session error = %v", err)
	}
	if authorization, err := store.AuthorizeWebSession(context.Background(), next.SessionDigest, domain.TenantPermissionRead, now.Add(3*time.Minute)); err != nil || authorization.Session.ActiveTenantID != tenantB {
		t.Fatalf("next authorization = %+v err=%v", authorization, err)
	}

	membership := membershipByTenant[tenantB]
	membership.SecurityVersion++
	membership.UpdatedAt = now.Add(4 * time.Minute)
	payload, err := json.Marshal(membership)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := ydbpartition.BucketV1(string(stateA.UserID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB.ExecContext(context.Background(),
		`UPDATE tenant_memberships
		 SET security_version = $1, updated_at = $2, record = CAST($3 AS JsonDocument)
		 WHERE user_bucket = $4 AND user_id = $5 AND tenant_id = $6`,
		membership.SecurityVersion, membership.UpdatedAt, string(payload), bucket, stateA.UserID, tenantB,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthorizeWebSession(context.Background(), next.SessionDigest, domain.TenantPermissionRead, now.Add(5*time.Minute)); !errors.Is(err, domain.ErrMembershipVersionChanged) {
		t.Fatalf("membership-version authorization error = %v", err)
	}
}

func webTelegramIdentityFixture(tenantID domain.TenantID, subject string, now time.Time) ports.TelegramIdentityRequest {
	return webTelegramIdentityFixtureWithActor(tenantID, domain.ActorID(uniqueID("web-actor")), subject, now)
}

func webTelegramIdentityFixtureWithActor(tenantID domain.TenantID, actorID domain.ActorID, subject string, now time.Time) ports.TelegramIdentityRequest {
	return ports.TelegramIdentityRequest{
		TenantID: tenantID,
		Actor:    domain.ActorRef{TenantID: tenantID, Frontend: domain.FrontendTelegram, ExternalID: subject, ID: actorID},
		Conversation: domain.ConversationRef{
			TenantID: tenantID, Frontend: domain.FrontendTelegram,
			ExternalID: "chat-" + string(tenantID), ID: domain.ConversationID(uniqueID("web-conversation")),
		},
		SubscriptionConnectionID: domain.SubscriptionConnectionID(uniqueID("web-subscription")),
		Provider:                 "codex", ObservedAt: now,
	}
}

func webSessionFixture(
	name string,
	userID domain.UserID,
	tenantID domain.TenantID,
	subject string,
	securityVersion uint64,
	issuedAt time.Time,
) domain.WebSession {
	return domain.WebSession{
		SessionDigest:   domain.DigestSecret("session-" + name),
		CSRFTokenDigest: domain.DigestSecret("csrf-" + name),
		UserID:          userID, ActiveTenantID: tenantID,
		AuthenticatedSubject:      domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: subject},
		MembershipSecurityVersion: securityVersion,
		IssuedAt:                  issuedAt, LastSeenAt: issuedAt,
		IdleExpiresAt: issuedAt.Add(12 * time.Hour), AbsoluteExpiresAt: issuedAt.Add(7 * 24 * time.Hour),
	}
}
