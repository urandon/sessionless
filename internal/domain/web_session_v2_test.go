package domain_test

import (
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func webSessionV2Fixture() (domain.WebSessionV2, domain.ExternalIdentityV2, domain.TenantMembership, time.Time) {
	at := identityV2TestTime()
	identity := identityV2Fixture(domain.IdentityProviderYandex, 7)
	member := domain.TenantMembership{TenantID: "tenant-v2", UserID: identity.UserID, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 3, CreatedAt: at, UpdatedAt: at}
	session := domain.WebSessionV2{Version: 2, IdentityRevision: identity.Revision, Session: domain.WebSession{
		SessionDigest: domain.DigestSecret("v2-browser-session"), CSRFTokenDigest: domain.DigestSecret("v2-browser-csrf"), UserID: identity.UserID, ActiveTenantID: member.TenantID, AuthenticatedSubject: identity.Subject,
		MembershipSecurityVersion: member.SecurityVersion, IssuedAt: at, LastSeenAt: at, IdleExpiresAt: at.Add(time.Hour), AbsoluteExpiresAt: at.Add(24 * time.Hour),
	}}
	return session, identity, member, at.Add(time.Minute)
}

func TestWebSessionV2AuthorizeCurrentMatrix(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*domain.WebSessionV2, *domain.ExternalIdentityV2, *domain.TenantMembership, *time.Time)
	}{
		{name: "unlinked identity", mutate: func(_ *domain.WebSessionV2, i *domain.ExternalIdentityV2, _ *domain.TenantMembership, _ *time.Time) {
			i.State = domain.ExternalIdentityUnlinkedV2
		}},
		{name: "different owner", mutate: func(_ *domain.WebSessionV2, i *domain.ExternalIdentityV2, _ *domain.TenantMembership, _ *time.Time) {
			i.UserID = "foreign-user"
		}},
		{name: "different provider equal subject", mutate: func(_ *domain.WebSessionV2, i *domain.ExternalIdentityV2, _ *domain.TenantMembership, _ *time.Time) {
			i.Subject.Provider = domain.IdentityProviderTelegram
		}},
		{name: "different subject", mutate: func(_ *domain.WebSessionV2, i *domain.ExternalIdentityV2, _ *domain.TenantMembership, _ *time.Time) {
			i.Subject.Subject = "456"
		}},
		{name: "stale identity revision", mutate: func(_ *domain.WebSessionV2, i *domain.ExternalIdentityV2, _ *domain.TenantMembership, _ *time.Time) {
			i.Revision++
		}},
		{name: "missing identity", mutate: func(_ *domain.WebSessionV2, i *domain.ExternalIdentityV2, _ *domain.TenantMembership, _ *time.Time) {
			*i = domain.ExternalIdentityV2{}
		}},
		{name: "wrong session version", mutate: func(s *domain.WebSessionV2, _ *domain.ExternalIdentityV2, _ *domain.TenantMembership, _ *time.Time) {
			s.Version = 1
		}},
		{name: "zero identity revision", mutate: func(s *domain.WebSessionV2, _ *domain.ExternalIdentityV2, _ *domain.TenantMembership, _ *time.Time) {
			s.IdentityRevision = 0
		}},
		{name: "revoked session", mutate: func(s *domain.WebSessionV2, _ *domain.ExternalIdentityV2, _ *domain.TenantMembership, now *time.Time) {
			s.Session.RevokedAt = now
		}},
		{name: "idle expiry equality", mutate: func(s *domain.WebSessionV2, _ *domain.ExternalIdentityV2, _ *domain.TenantMembership, now *time.Time) {
			*now = s.Session.IdleExpiresAt
		}},
		{name: "absolute expiry equality", mutate: func(s *domain.WebSessionV2, _ *domain.ExternalIdentityV2, _ *domain.TenantMembership, now *time.Time) {
			*now = s.Session.AbsoluteExpiresAt
		}},
		{name: "suspended membership", mutate: func(_ *domain.WebSessionV2, _ *domain.ExternalIdentityV2, m *domain.TenantMembership, _ *time.Time) {
			m.Status = domain.TenantMembershipSuspended
		}},
		{name: "revoked membership", mutate: func(_ *domain.WebSessionV2, _ *domain.ExternalIdentityV2, m *domain.TenantMembership, _ *time.Time) {
			m.Status = domain.TenantMembershipRevoked
		}},
		{name: "membership changed", mutate: func(_ *domain.WebSessionV2, _ *domain.ExternalIdentityV2, m *domain.TenantMembership, _ *time.Time) {
			m.SecurityVersion++
		}},
		{name: "foreign tenant", mutate: func(_ *domain.WebSessionV2, _ *domain.ExternalIdentityV2, m *domain.TenantMembership, _ *time.Time) {
			m.TenantID = "foreign-tenant"
		}},
		{name: "zero clock", mutate: func(_ *domain.WebSessionV2, _ *domain.ExternalIdentityV2, _ *domain.TenantMembership, now *time.Time) {
			*now = time.Time{}
		}},
		{name: "future issued", mutate: func(s *domain.WebSessionV2, _ *domain.ExternalIdentityV2, _ *domain.TenantMembership, now *time.Time) {
			s.Session.IssuedAt = now.Add(time.Second)
			s.Session.LastSeenAt = s.Session.IssuedAt
		}},
		{name: "future last seen", mutate: func(s *domain.WebSessionV2, _ *domain.ExternalIdentityV2, _ *domain.TenantMembership, now *time.Time) {
			s.Session.LastSeenAt = now.Add(time.Second)
		}},
		{name: "future identity", mutate: func(_ *domain.WebSessionV2, i *domain.ExternalIdentityV2, _ *domain.TenantMembership, now *time.Time) {
			i.UpdatedAt = now.Add(time.Second)
		}},
		{name: "future membership", mutate: func(_ *domain.WebSessionV2, _ *domain.ExternalIdentityV2, m *domain.TenantMembership, now *time.Time) {
			m.UpdatedAt = now.Add(time.Second)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			session, identity, member, now := webSessionV2Fixture()
			if err := session.AuthorizeCurrent(identity, member, domain.TenantPermissionWrite, now); err != nil {
				t.Fatalf("valid fixture authorization = %v", err)
			}
			test.mutate(&session, &identity, &member, &now)
			if err := session.AuthorizeCurrent(identity, member, domain.TenantPermissionWrite, now); err == nil {
				t.Errorf("invalid current authority accepted: session=%+v identity=%+v membership=%+v now=%v", session, identity, member, now)
			}
		})
	}
}

func TestWebSessionV2PermissionsRemainMembershipScoped(t *testing.T) {
	session, identity, member, now := webSessionV2Fixture()
	member.Role = domain.TenantMembershipViewer
	if err := session.AuthorizeCurrent(identity, member, domain.TenantPermissionRead, now); err != nil {
		t.Errorf("viewer read = %v", err)
	}
	for _, permission := range []domain.TenantPermission{domain.TenantPermissionWrite, domain.TenantPermissionAdmin} {
		if err := session.AuthorizeCurrent(identity, member, permission, now); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Errorf("viewer %s = %v, want membership denied", permission, err)
		}
	}
}

func TestWebSessionV2SharedRevisionInvalidatesAcrossTenants(t *testing.T) {
	session, identity, member, now := webSessionV2Fixture()
	otherSession, otherMember := session, member
	otherSession.Session.ActiveTenantID, otherMember.TenantID = "other-tenant", "other-tenant"
	targetSubject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "456"}
	plan, err := domain.PlanExternalIdentityChangeV2(identity.UserID, []domain.ExternalIdentityV2{identity}, targetSubject, nil, domain.IdentityOperationLinkV2, []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		session domain.WebSessionV2
		member  domain.TenantMembership
	}{
		{name: "original tenant", session: session, member: member},
		{name: "other tenant", session: otherSession, member: otherMember},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.session.AuthorizeCurrent(plan.Active[0], test.member, domain.TenantPermissionRead, now); !errors.Is(err, domain.ErrWebSessionRevoked) {
				t.Errorf("old cookie after shared revision advance = %v, want revoked", err)
			}
			test.session.IdentityRevision = plan.AfterRevision
			if err := test.session.AuthorizeCurrent(plan.Active[0], test.member, domain.TenantPermissionRead, now); err != nil {
				t.Errorf("fresh revision with unchanged membership = %v", err)
			}
		})
	}
}
