package serverlessharness

import (
	"context"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type substrateRegistryRecordingDriver struct {
	allocation     domain.PreparedAllocationV1
	preflightCalls int
	executeCalls   int
	cancelCalls    int
	reconcileCalls int
}

type substrateRegistryPreparedDriver struct {
	issuer     *CapabilityIssuer
	allocation domain.PreparedAllocationV1
	effects    int
}

type substrateHarnessNoop struct{}

func (substrateHarnessNoop) Preflight(context.Context, ports.ExecutionIdentity) error { return nil }
func (substrateHarnessNoop) Execute(_ context.Context, request ports.ExecutionRequest, _ ports.ExecutionEventSink) (ports.ExecutionResult, error) {
	evidence, err := (domain.ProviderExecutionEvidenceV1{
		AcceptanceClass: domain.ProviderAcceptanceAcceptedV1, FinishClass: domain.ProviderFinishCompletedV1,
		RouteState: domain.ProviderEvidenceSupportedV1, ActualModelVendorID: request.HarnessBinding.ModelVendorID,
		ActualModelID: request.HarnessBinding.ModelID, TransportKind: domain.ProviderTransportLocalCLIV1,
		TransportProvider: "sessionless", UpstreamProviderID: "local", EndpointID: "deterministic-fixture",
		PolicyVerdict: domain.ProviderPolicyGoV1, UsageProvenance: domain.ProviderUsageUnknownV1,
	}).SealForBinding(request.HarnessBinding)
	if err != nil {
		return ports.ExecutionResult{}, err
	}
	return ports.ExecutionResult{Summary: "fixture", ProviderEvidence: &evidence}, nil
}
func (substrateHarnessNoop) Cancel(context.Context, ports.ExecutionIdentity) error { return nil }

func (driver *substrateRegistryRecordingDriver) Preflight(_ context.Context, _ domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV1, error) {
	driver.preflightCalls++
	return driver.allocation.Clone(), nil
}

func (driver *substrateRegistryRecordingDriver) Execute(_ context.Context, _ PreparedInvocation, _ ports.ExecutionRequest, _ ports.ExecutionEventSink, _ ports.HarnessDriver) (ports.ExecutionResult, domain.SubstrateExecutionEvidenceV1, error) {
	driver.executeCalls++
	return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, nil
}

func (driver *substrateRegistryRecordingDriver) Cancel(_ context.Context, authority domain.ServerlessInvocationAuthorityV1) (SubstrateOperationObservationV1, error) {
	driver.cancelCalls++
	return substrateRegistryObservation(authority), nil
}

func (driver *substrateRegistryRecordingDriver) Reconcile(_ context.Context, authority domain.ServerlessInvocationAuthorityV1) (SubstrateOperationObservationV1, error) {
	driver.reconcileCalls++
	return substrateRegistryObservation(authority), nil
}

func (driver *substrateRegistryPreparedDriver) Preflight(_ context.Context, _ domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV1, error) {
	return driver.allocation.Clone(), nil
}

func (driver *substrateRegistryPreparedDriver) Execute(
	ctx context.Context,
	prepared PreparedInvocation,
	request ports.ExecutionRequest,
	sink ports.ExecutionEventSink,
	harness ports.HarnessDriver,
) (ports.ExecutionResult, domain.SubstrateExecutionEvidenceV1, error) {
	if err := driver.issuer.Consume(prepared); err != nil {
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, err
	}
	driver.effects++
	result, err := harness.Execute(ctx, request, sink)
	if err != nil {
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, err
	}
	evidence, err := sealSuccessfulInProcessEvidence(prepared, result.ProviderEvidence)
	return result, evidence, err
}

func (driver *substrateRegistryPreparedDriver) Cancel(_ context.Context, authority domain.ServerlessInvocationAuthorityV1) (SubstrateOperationObservationV1, error) {
	return substrateRegistryObservation(authority), nil
}

func (driver *substrateRegistryPreparedDriver) Reconcile(_ context.Context, authority domain.ServerlessInvocationAuthorityV1) (SubstrateOperationObservationV1, error) {
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
	if _, _, err := registry.Execute(context.Background(), prepared, ports.ExecutionRequest{}, substrateSinkNoop{}, substrateHarnessNoop{}); err == nil {
		t.Fatal("expired substrate profile reached Execute")
	}
	if driver.executeCalls != 0 {
		t.Fatalf("execute calls = %d, want 0", driver.executeCalls)
	}
}

type substrateSinkNoop struct{}

func (substrateSinkNoop) Emit(context.Context, ports.ExecutionEvent) error { return nil }

func TestSubstrateRegistryPreparesOnlyAuthenticatedEffectOwner(t *testing.T) {
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
	driver := &substrateRegistryRecordingDriver{allocation: allocation}
	registry, err := NewSubstrateRegistryV1(func() time.Time { return at }, issuer, SubstrateRegistrationV1{
		Binding: authority.SubstrateBinding, Enabled: true, Driver: driver,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := ports.ReserveAttemptEffectResultV1{
		Status: ports.AttemptEffectOwnedV1, Reservation: reservation, Grant: &grant,
	}
	prepared, err := registry.Prepare(context.Background(), result)
	if err != nil || issuer.Validate(prepared) != nil || driver.preflightCalls != 1 {
		t.Fatalf("prepared = %v, validate = %v, preflights = %d", err, issuer.Validate(prepared), driver.preflightCalls)
	}

	tampered := result
	tamperedGrant := grant.Clone()
	tamperedGrant.Authenticator[0] ^= 0xff
	tampered.Grant = &tamperedGrant
	if _, err := registry.Prepare(context.Background(), tampered); err == nil {
		t.Fatal("tampered effect grant prepared")
	}
	if driver.preflightCalls != 1 {
		t.Fatalf("tampered grant reached preflight: calls = %d", driver.preflightCalls)
	}

	reconcile := ports.ReserveAttemptEffectResultV1{
		Status: ports.AttemptEffectReconcileOnlyV1, Reservation: reservation,
	}
	if _, err := registry.Prepare(context.Background(), reconcile); err == nil {
		t.Fatal("reconcile-only reservation prepared")
	}
	if driver.preflightCalls != 1 {
		t.Fatalf("reconcile-only reservation reached preflight: calls = %d", driver.preflightCalls)
	}
}

func TestSubstrateRegistryExecutesOnlyExactPreparedRequestOnce(t *testing.T) {
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
	driver := &substrateRegistryPreparedDriver{issuer: issuer, allocation: allocation}
	registry, err := NewSubstrateRegistryV1(func() time.Time { return at }, issuer, SubstrateRegistrationV1{
		Binding: authority.SubstrateBinding, Enabled: true, Driver: driver,
	})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := registry.PrepareExecution(context.Background(), ports.ReserveAttemptEffectResultV1{
		Status: ports.AttemptEffectOwnedV1, Reservation: reservation, Grant: &grant,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := executionRequestForAuthority(authority)
	result, evidence, err := execution.Execute(context.Background(), request, substrateSinkNoop{}, substrateHarnessNoop{})
	if err != nil || result.ProviderEvidence == nil || evidence.ProviderEvidence == nil || driver.effects != 1 {
		t.Fatalf("result/evidence/effects/error = %+v/%+v/%d/%v", result, evidence, driver.effects, err)
	}

	mutated := request
	mutated.HarnessBinding.Backend.BackendProfileDigest = strings.Repeat("c", 64)
	if _, _, err := execution.Execute(context.Background(), mutated, substrateSinkNoop{}, substrateHarnessNoop{}); err == nil {
		t.Fatal("mutated execution request reached prepared driver")
	}
	if driver.effects != 1 {
		t.Fatalf("mutated execution changed effects = %d", driver.effects)
	}
	if _, _, err := execution.Execute(context.Background(), request, substrateSinkNoop{}, substrateHarnessNoop{}); err == nil {
		t.Fatal("replayed prepared invocation executed")
	}
	if driver.effects != 1 {
		t.Fatalf("replay changed effects = %d", driver.effects)
	}
}

func TestPreparedExecutionKeepsCancelAndReconcileBoundToPreparedAuthority(t *testing.T) {
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
	driver := &substrateRegistryRecordingDriver{allocation: allocation}
	registry, err := NewSubstrateRegistryV1(func() time.Time { return at }, issuer, SubstrateRegistrationV1{
		Binding: authority.SubstrateBinding, Enabled: true, Driver: driver,
	})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := registry.PrepareExecution(context.Background(), ports.ReserveAttemptEffectResultV1{
		Status: ports.AttemptEffectOwnedV1, Reservation: reservation, Grant: &grant,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel prepared execution: %v", err)
	}
	if _, err := execution.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile prepared execution: %v", err)
	}
	if driver.cancelCalls != 1 || driver.reconcileCalls != 1 {
		t.Fatalf("cancel/reconcile calls = %d/%d, want 1/1", driver.cancelCalls, driver.reconcileCalls)
	}
}

func executionRequestForAuthority(authority domain.ServerlessInvocationAuthorityV1) ports.ExecutionRequest {
	substrate := authority.SubstrateBinding
	cost := authority.AdmissionCostCeiling.Clone()
	return ports.ExecutionRequest{
		TenantID: authority.HarnessBinding.TenantID, OwnerUserID: authority.HarnessBinding.OwnerUserID,
		RunID: authority.HarnessBinding.RunID, SessionID: "session-1", TriggerEventID: "event-1",
		AttemptID: authority.HarnessBinding.AttemptID, WorkDir: "/tmp/sessionless-prepared-registry-test",
		ContextWindow:        &domain.SessionContextWindow{ThroughSequence: 1},
		ExecutionPlacementV2: authority.ExecutionPlacementV2, HarnessBinding: authority.HarnessBinding.Clone(),
		SubstrateBinding: &substrate, AdmissionCostCeiling: &cost,
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
