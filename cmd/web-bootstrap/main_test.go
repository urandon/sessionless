package main

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestGrantFromEnvironmentRequiresAuditedCloudDevelopmentInput(t *testing.T) {
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	setGrantEnvironment(t)
	grant, err := grantFromEnvironment(now)
	if err != nil {
		t.Fatal(err)
	}
	if grant.TenantID != "ten_alpha" || grant.UserID != "usr_known_user" ||
		grant.Role != domain.TenantMembershipOwner || grant.GrantedAt != now {
		t.Fatalf("grant = %+v", grant)
	}

	t.Setenv("SESSIONLESS_ENVIRONMENT", "production")
	if _, err := grantFromEnvironment(now); err == nil {
		t.Fatal("production bootstrap unexpectedly passed validation")
	}
}

func TestGrantFromEnvironmentRequiresConnectionCoordinates(t *testing.T) {
	setGrantEnvironment(t)
	t.Setenv("YDB_CONNECTION_STRING", "")
	if _, err := grantFromEnvironment(time.Now().UTC()); err == nil {
		t.Fatal("bootstrap without YDB coordinates unexpectedly passed validation")
	}
}

func setGrantEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("WEB_BOOTSTRAP_TENANT_ID", "ten_alpha")
	t.Setenv("WEB_BOOTSTRAP_USER_ID", "usr_known_user")
	t.Setenv("WEB_BOOTSTRAP_ROLE", string(domain.TenantMembershipOwner))
	t.Setenv("SESSIONLESS_ENVIRONMENT", domain.DevelopmentEnvironment)
	t.Setenv("WEB_BOOTSTRAP_OPERATOR", "operator@example.com")
	t.Setenv("WEB_BOOTSTRAP_REASON", "initial Web access")
	t.Setenv("YDB_CONNECTION_STRING", "grpcs://example.invalid/local")
}
