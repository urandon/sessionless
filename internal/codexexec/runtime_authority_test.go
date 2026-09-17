package codexexec

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestAcceptedAuthorityResolverProjectsOnlyExactLiveClaim(t *testing.T) {
	request, want := validDriverRequest(t)
	attempt := attemptForAuthority(want, domain.AttachedWorkerAttemptClaimed)
	resolver, err := NewAcceptedAuthorityResolverV1(attempt, want.ProviderResource, func() time.Time { return driverNow })
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolver.ResolveExecution(context.Background(), executionIdentity(request))
	if err != nil || got != want {
		t.Fatalf("ResolveExecution() = (%+v, %v)", got, err)
	}

	crossOwner := executionIdentity(request)
	crossOwner.OwnerUserID = "owner-other"
	if _, err := resolver.ResolveExecution(context.Background(), crossOwner); !errors.Is(err, ErrAuthorityUnavailable) {
		t.Fatalf("cross-owner ResolveExecution() error = %v", err)
	}

	unclaimed := attemptForAuthority(want, domain.AttachedWorkerAttemptOffered)
	unclaimed.WorkerAttemptSequence = 0
	if _, err := NewAcceptedAuthorityResolverV1(unclaimed, want.ProviderResource, nil); !errors.Is(err, ErrContract) {
		t.Fatalf("unclaimed constructor error = %v", err)
	}
}

func TestAcceptedAuthorityResolverSeparatesExecutionFromCancellationState(t *testing.T) {
	request, want := validDriverRequest(t)
	attempt := attemptForAuthority(want, domain.AttachedWorkerAttemptCancelRequested)
	attempt.LeaseExpiresAt = driverNow.Add(-time.Minute)
	attempt.CancelRevision = 1
	attempt.CancelDeadline = driverNow.Add(time.Minute)
	if err := attempt.Validate(); err != nil {
		t.Fatalf("cancel attempt invalid: %v", err)
	}
	resolver, err := NewAcceptedAuthorityResolverV1(attempt, want.ProviderResource, func() time.Time { return driverNow })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveExecution(context.Background(), executionIdentity(request)); !errors.Is(err, ErrAuthorityUnavailable) {
		t.Fatalf("expired cancel-state execution error = %v", err)
	}
	got, err := resolver.ResolveCancellation(context.Background(), executionIdentity(request))
	if err != nil || got.LeaseID != want.LeaseID || got.LeaseGeneration != want.LeaseGeneration {
		t.Fatalf("ResolveCancellation() = (%+v, %v)", got, err)
	}
}

func TestAcceptedAuthorityResolverPinsResourceBeforeRequest(t *testing.T) {
	request, want := validDriverRequest(t)
	attempt := attemptForAuthority(want, domain.AttachedWorkerAttemptClaimed)
	other := want.ProviderResource
	other.CredentialGeneration++
	resolver, err := NewAcceptedAuthorityResolverV1(attempt, other, func() time.Time { return driverNow })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveExecution(context.Background(), executionIdentity(request)); !errors.Is(err, ErrAuthorityUnavailable) {
		t.Fatalf("resource-drift ResolveExecution() error = %v", err)
	}
}

func attemptForAuthority(authority AuthorityV1, state domain.AttachedWorkerAttemptState) domain.AttachedWorkerAttemptV1 {
	return domain.AttachedWorkerAttemptV1{
		Version:  domain.AttachedWorkerAttemptVersionV1,
		TenantID: authority.TenantID, OwnerUserID: authority.OwnerUserID,
		WorkerID: authority.WorkerID, ConnectionID: authority.ConnectionID,
		RunID: authority.RunID, AttemptID: authority.AttemptID,
		ReservationID: authority.ReservationID, LeaseID: authority.LeaseID,
		LeaseGeneration: authority.LeaseGeneration, FenceToken: authority.FenceToken,
		EnrollmentGeneration: authority.EnrollmentGeneration,
		ConnectionGeneration: authority.ConnectionGeneration,
		ContextDigest:        authority.ContextDigest, CapabilityDigest: authority.CapabilityDigest,
		PolicyDigest: authority.PolicyDigest, State: state,
		PlatformAttemptSequence: 2, WorkerAttemptSequence: 1,
		LeaseExpiresAt: authority.LeaseExpiresAt,
		CreatedAt:      driverNow.Add(-10 * time.Minute), UpdatedAt: driverNow.Add(-5 * time.Minute),
		Revision: 1,
	}
}
