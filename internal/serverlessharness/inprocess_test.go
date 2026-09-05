package serverlessharness

import (
	"context"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestExactPreparerRunsAuthenticatedInProcessSession(t *testing.T) {
	t.Parallel()
	authority, reservation, _, at := capabilityFixture(t)
	issuer, err := NewCapabilityIssuer(func() time.Time { return at }, nil)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := issuer.MintAttemptEffectOwnershipGrant(authority, reservation, at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	driver, err := NewInProcessExecutionSubstrateV1(func() time.Time { return at }, issuer, authority.HarnessBinding.Backend)
	if err != nil {
		t.Fatal(err)
	}
	resolveCalls := 0
	preparer, err := NewExactExecutionPreparerV1(
		func() time.Time { return at },
		issuer,
		func(resolvedAuthority domain.ServerlessInvocationAuthorityV1) (SubstrateRegistrationV1, error) {
			resolveCalls++
			return SubstrateRegistrationV1{Binding: resolvedAuthority.SubstrateBinding, Enabled: true, Driver: driver}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	owned := ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectOwnedV1, Reservation: reservation, Grant: &grant}
	execution, err := preparer.PrepareExecution(context.Background(), owned)
	if err != nil {
		t.Fatal(err)
	}
	result, evidence, err := execution.Execute(
		context.Background(), executionRequestForAuthority(authority), substrateSinkNoop{}, substrateHarnessNoop{},
	)
	if err != nil || result.ProviderEvidence == nil || evidence.ProviderEvidence == nil {
		t.Fatalf("result/evidence/error = %+v/%+v/%v", result, evidence, err)
	}
	if _, err := execution.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel prepared execution: %v", err)
	}
	if _, err := execution.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile prepared execution: %v", err)
	}

	tampered := owned
	tamperedGrant := grant.Clone()
	tamperedGrant.Authenticator[0] ^= 0xff
	tampered.Grant = &tamperedGrant
	if _, err := preparer.PrepareExecution(context.Background(), tampered); err == nil {
		t.Fatal("tampered grant reached exact registration resolution")
	}
	if resolveCalls != 1 {
		t.Fatalf("registration resolve calls = %d, want 1", resolveCalls)
	}
}
