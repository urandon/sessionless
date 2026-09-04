package domain_test

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func deterministicManagedAuthority(t testing.TB, tenant domain.TenantID, owner domain.UserID, run domain.RunID, attempt domain.AttemptID, at time.Time) ports.ManagedExecutionAuthorityV2 {
	t.Helper()
	authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(tenant, owner, run, attempt, "subscription-1", at)
	if err != nil {
		t.Fatal(err)
	}
	return authority
}

func deterministicHarnessBindingForPlacement(t testing.TB, tenant domain.TenantID, owner domain.UserID, run domain.RunID, attempt domain.AttemptID, placement domain.ExecutionPlacementV2, at time.Time) domain.HarnessBindingV1 {
	t.Helper()
	authority := deterministicManagedAuthority(t, tenant, owner, run, attempt, at)
	digest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	binding := authority.HarnessBinding.Clone()
	binding.ExecutionPlacementDigest = string(digest)
	if err := binding.ValidateForScope(tenant, owner, run, attempt, placement); err != nil {
		t.Fatal(err)
	}
	return binding
}
