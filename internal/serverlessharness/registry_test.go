package serverlessharness

import (
	"context"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type substrateRegistryRecordingDriver struct {
	executeCalls   int
	cancelCalls    int
	reconcileCalls int
}

type substrateHarnessNoop struct{}

func (substrateHarnessNoop) Preflight(context.Context, ports.ExecutionIdentity) error { return nil }
func (substrateHarnessNoop) Execute(context.Context, ports.ExecutionRequest, ports.ExecutionEventSink) (ports.ExecutionResult, error) {
	return ports.ExecutionResult{}, nil
}
func (substrateHarnessNoop) Cancel(context.Context, ports.ExecutionIdentity) error { return nil }

func (driver *substrateRegistryRecordingDriver) Preflight(_ context.Context, _ domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV1, error) {
	return domain.PreparedAllocationV1{}, nil
}

func (driver *substrateRegistryRecordingDriver) Execute(_ context.Context, _ PreparedInvocation, _ ports.HarnessDriver) (domain.SubstrateExecutionEvidenceV1, error) {
	driver.executeCalls++
	return domain.SubstrateExecutionEvidenceV1{}, nil
}

func (driver *substrateRegistryRecordingDriver) Cancel(_ context.Context, authority domain.ServerlessInvocationAuthorityV1) (SubstrateOperationObservationV1, error) {
	driver.cancelCalls++
	return substrateRegistryObservation(authority), nil
}

func (driver *substrateRegistryRecordingDriver) Reconcile(_ context.Context, authority domain.ServerlessInvocationAuthorityV1) (SubstrateOperationObservationV1, error) {
	driver.reconcileCalls++
	return substrateRegistryObservation(authority), nil
}

func substrateRegistryObservation(authority domain.ServerlessInvocationAuthorityV1) SubstrateOperationObservationV1 {
	authorityDigest, _ := authority.Digest()
	substrateDigest, _ := authority.SubstrateBinding.Digest()
	return SubstrateOperationObservationV1{
		State:                SubstrateOperationObservedV1,
		InvocationAuthority:  authorityDigest,
		SubstrateBinding:     substrateDigest,
		PhysicalInvocationID: string(authority.Lease.ID) + "-physical",
		ObservedAt:           time.Now().UTC(),
	}
}

func TestSubstrateRegistryExecuteRequiresFreshProfile(t *testing.T) {
	t.Parallel()
	authority, reservation, allocation, at := capabilityFixture(t)
	issuer, err := NewCapabilityIssuer(func() time.Time { return at }, nil)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := issuer.MintAttemptEffectOwnershipGrant(authority, reservation, at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := issuer.Issue(grant, allocation)
	if err != nil {
		t.Fatal(err)
	}
	driver := &substrateRegistryRecordingDriver{}
	registryNow := authority.SubstrateBinding.ProfileEvidenceExpiresAt.Add(time.Second)
	registry, err := NewSubstrateRegistryV1(func() time.Time { return registryNow }, issuer, SubstrateRegistrationV1{
		Binding: authority.SubstrateBinding,
		Enabled: true,
		Driver:  driver,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Execute(context.Background(), prepared, substrateHarnessNoop{}); err == nil {
		t.Fatal("expired substrate profile reached Execute")
	}
	if driver.executeCalls != 0 {
		t.Fatalf("execute calls = %d, want 0", driver.executeCalls)
	}
}

func TestSubstrateRegistryCancelAndReconcileRemainRoutableAfterProfileExpiry(t *testing.T) {
	t.Parallel()
	authority, _, _, _ := capabilityFixture(t)
	driver := &substrateRegistryRecordingDriver{}
	registryNow := authority.SubstrateBinding.ProfileEvidenceExpiresAt.Add(time.Second)
	issuer, err := NewCapabilityIssuer(func() time.Time { return registryNow }, nil)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewSubstrateRegistryV1(func() time.Time { return registryNow }, issuer, SubstrateRegistrationV1{
		Binding: authority.SubstrateBinding,
		Enabled: false,
		Driver:  driver,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Cancel(context.Background(), authority); err != nil {
		t.Fatalf("expired profile cancellation was not routable: %v", err)
	}
	if _, err := registry.Reconcile(context.Background(), authority); err != nil {
		t.Fatalf("expired profile reconciliation was not routable: %v", err)
	}
	if driver.executeCalls != 0 {
		t.Fatalf("execute calls = %d, want 0", driver.executeCalls)
	}
	if driver.cancelCalls != 1 || driver.reconcileCalls != 1 {
		t.Fatalf("cancel/reconcile calls = %d/%d, want 1/1", driver.cancelCalls, driver.reconcileCalls)
	}
}
