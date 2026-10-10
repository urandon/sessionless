package serverlessegress

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// This adapter exists only in tests. It joins the real preparer/registry and
// Boundary to synchronous synthetic ports; it is not a production registration,
// Manager credential-custody adapter, canonical finalizer, or platform probe.
type joinedEgressSubstrate struct {
	mu                                      sync.Mutex
	fixture                                 *boundaryFixture
	preflights, executions, reconciliations int
	boundaryResult                          ResultV1
	boundaryErr                             error
	sealErr                                 error
	preparedDigests                         []domain.PreparedInvocationDigestV1
}

func (driver *joinedEgressSubstrate) Preflight(ctx context.Context, authority domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV1, error) {
	driver.preflights++
	if err := ctx.Err(); err != nil {
		return domain.PreparedAllocationV1{}, err
	}
	if err := authority.ValidateAt(driver.fixture.clock.Now()); err != nil {
		return domain.PreparedAllocationV1{}, err
	}
	return driver.fixture.request.Prepared.Allocation(), nil
}

func (driver *joinedEgressSubstrate) Execute(ctx context.Context, prepared serverlessharness.PreparedInvocation, _ ports.ExecutionRequest, _ ports.ExecutionEventSink, _ ports.HarnessDriver) (ports.ExecutionResult, domain.SubstrateExecutionEvidenceV1, error) {
	driver.mu.Lock()
	driver.executions++
	driver.preparedDigests = append(driver.preparedDigests, prepared.Digest())
	driver.mu.Unlock()
	request := driver.fixture.request
	request.Prepared = prepared // Use the registry-issued capability, not a fixture shortcut.
	boundaryResult, boundaryErr := driver.fixture.boundary.Execute(ctx, request)
	driver.mu.Lock()
	driver.boundaryResult, driver.boundaryErr = boundaryResult, boundaryErr
	driver.mu.Unlock()
	if boundaryErr != nil {
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, boundaryErr
	}
	observed := boundaryResult.Evidence
	provider, err := (domain.ProviderExecutionEvidenceV1{
		AcceptanceClass: observed.Acceptance, FinishClass: domain.ProviderFinishCompletedV1,
		RouteState: observed.RouteState, ActualModelVendorID: observed.ActualModelVendorID,
		ActualModelID: observed.ActualModelID, TransportKind: observed.TransportKind,
		TransportProvider: observed.TransportProvider, UpstreamProviderID: observed.UpstreamProviderID,
		EndpointID: observed.EndpointID, PolicyVerdict: request.EffectivePolicy.Verdict,
		UsageProvenance: domain.ProviderUsageUnknownV1,
	}).SealForBinding(prepared.Authority().HarnessBinding)
	if err != nil {
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, err
	}
	evidence, err := joinedSuccessEvidence(boundaryResult.Evidence, provider).SealForAuthority(prepared.Authority(), prepared.Reservation(), prepared.Allocation(), prepared.Digest())
	driver.mu.Lock()
	driver.sealErr = err
	driver.mu.Unlock()
	return ports.ExecutionResult{Summary: "synthetic result", ProviderEvidence: &provider}, evidence, err
}

func joinedSuccessEvidence(observed EvidenceV1, provider domain.ProviderExecutionEvidenceV1) domain.SubstrateExecutionEvidenceV1 {
	resources := make([]domain.SubstrateResourceObservationV1, 0, 7)
	for _, kind := range []domain.SubstrateResourceKindV1{
		domain.SubstrateResourceCPUTimeV1, domain.SubstrateResourceEgressBytesV1,
		domain.SubstrateResourceEvidenceBytesV1, domain.SubstrateResourceIngressBytesV1,
		domain.SubstrateResourceLogBytesV1, domain.SubstrateResourceMemoryPeakV1, domain.SubstrateResourceScratchPeakV1,
	} {
		resources = append(resources, domain.SubstrateResourceObservationV1{Kind: kind, State: domain.SubstrateResourceUnknownV1, Provenance: domain.SubstrateResourceProvenanceUnknownV1})
	}
	return domain.SubstrateExecutionEvidenceV1{
		Allocation: domain.SubstrateAllocationStartedV1, Process: domain.SubstrateProcessNotApplicableV1,
		CredentialFinalization: observed.CredentialFinalization,
		// Only this in-memory fixture has no workspace, process or socket to remove.
		// This value is not evidence of OS isolation or warm-container cleanup.
		Cleanup: domain.SubstrateCleanupNotRequiredV1,
		Egress:  observed.Egress, ProxyAttestation: observed.ProxyAttestation,
		ImageAttestation: domain.SubstrateAttestationVerifiedV1, BackendAttestation: domain.SubstrateAttestationVerifiedV1,
		Cancellation:     domain.SubstrateCancellationEvidenceV1{Request: domain.SubstrateCancellationRequestNoneV1, BackendSignal: domain.SubstrateCancellationSignalNotRequiredV1},
		ProviderEvidence: &provider, ResourceObservations: resources, FailureCode: domain.SubstrateExecutionFailureNoneV1,
	}
}

func (driver *joinedEgressSubstrate) observation(authority domain.ServerlessInvocationAuthorityV1, state domain.SubstrateOperationStateV1) (domain.SubstrateOperationObservationV1, error) {
	authorityDigest, err := authority.Digest()
	if err != nil {
		return domain.SubstrateOperationObservationV1{}, err
	}
	substrateDigest, _ := authority.SubstrateBinding.Digest()
	return domain.SubstrateOperationObservationV1{State: state, InvocationAuthority: authorityDigest, SubstrateBinding: substrateDigest,
		PhysicalInvocationID: driver.fixture.request.Prepared.Reservation().PhysicalInvocationClaimID, ObservedAt: driver.fixture.clock.Now()}, nil
}

func (driver *joinedEgressSubstrate) Cancel(_ context.Context, authority domain.ServerlessInvocationAuthorityV1) (domain.SubstrateOperationObservationV1, error) {
	return driver.observation(authority, domain.SubstrateOperationAcknowledgedV1)
}

func (driver *joinedEgressSubstrate) Reconcile(_ context.Context, authority domain.ServerlessInvocationAuthorityV1) (domain.SubstrateOperationObservationV1, error) {
	driver.reconciliations++
	return driver.observation(authority, domain.SubstrateOperationObservedV1)
}

type joinedFixture struct {
	*boundaryFixture
	driver     *joinedEgressSubstrate
	preparer   *serverlessharness.ExactExecutionPreparerV1
	owned      ports.ReserveAttemptEffectResultV1
	projection ports.ExecutionRequest
}

func newJoinedFixture(t *testing.T) *joinedFixture {
	t.Helper()
	boundary := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPrivateV1)
	// The old recording gate has a single-threaded trace. Use the real issuer
	// directly here so concurrent replay exercises its atomic Consume safely.
	var err error
	boundary.boundary, err = NewBoundaryV1(ConfigV1{Clock: boundary.clock, Gate: boundary.gate.issuer, Credentials: boundary.credentials, Proxy: boundary.proxy})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &joinedFixture{boundaryFixture: boundary}
	fixture.driver = &joinedEgressSubstrate{fixture: boundary}
	authority := boundary.request.Prepared.Authority()
	grant, err := boundary.gate.issuer.MintAttemptEffectOwnershipGrant(authority, boundary.request.Prepared.Reservation(), boundary.clock.Now().Add(25*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	fixture.owned = ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectOwnedV1, Reservation: grant.Reservation, Grant: &grant}
	// Fixed process-owned registration: never derive a replacement profile from
	// incoming authority or bypass registry equality/expiry validation.
	registration := serverlessharness.SubstrateRegistrationV1{Binding: authority.SubstrateBinding, Enabled: true, Driver: fixture.driver}
	fixture.preparer, err = serverlessharness.NewExactExecutionPreparerV1(boundary.clock.Now, boundary.gate.issuer,
		func(domain.ServerlessInvocationAuthorityV1) (serverlessharness.SubstrateRegistrationV1, error) {
			return registration, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	substrate, ceiling := authority.SubstrateBinding, authority.AdmissionCostCeiling
	fixture.projection = ports.ExecutionRequest{
		TenantID: authority.HarnessBinding.TenantID, OwnerUserID: authority.HarnessBinding.OwnerUserID,
		RunID: authority.HarnessBinding.RunID, AttemptID: authority.HarnessBinding.AttemptID,
		SessionID: "session-1", TriggerEventID: "event-1", WorkDir: "/synthetic-workspace",
		ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1}, HarnessBinding: authority.HarnessBinding,
		ExecutionPlacementV2: authority.ExecutionPlacementV2, SubstrateBinding: &substrate, AdmissionCostCeiling: &ceiling,
		// ExecutionRequest's current codec requires this projection for an
		// invocation resource. It is fixture-preseeded, never issued/materialized,
		// contains no secret, and is NOT passed as Boundary's credential authority.
		// Manager/API-router custody integration remains #175, not proven here.
		Credential: ports.ProviderInvocationCredentialV1{HandleID: "projection-fixture-only", TenantID: authority.HarnessBinding.TenantID,
			OwnerUserID: authority.HarnessBinding.OwnerUserID, RunID: authority.HarnessBinding.RunID, AttemptID: authority.HarnessBinding.AttemptID,
			WorkerID: authority.Lease.WorkerID, LeaseID: authority.Lease.ID, LeaseFence: authority.Lease.FenceToken,
			ProviderResource: authority.HarnessBinding.Resource, ExpiresAt: authority.InvocationDeadline},
		CredentialMaterialization: materializationFixture(domain.ProviderCredentialDeliveryDirectV1),
	}
	if err := fixture.projection.Validate(); err != nil {
		t.Fatalf("synthetic execution projection: %v", err)
	}
	return fixture
}

func (fixture *joinedFixture) prepare(t *testing.T) serverlessharness.PreparedExecutionV1 {
	t.Helper()
	execution, err := fixture.preparer.PrepareExecution(context.Background(), fixture.owned)
	if err != nil {
		t.Fatalf("prepare owned effect: %v", err)
	}
	return execution
}

// These sentinels ensure no legacy HarnessDriver/streaming side path is used.
type joinedUnusedHarness struct{}

func (joinedUnusedHarness) Preflight(context.Context, ports.ExecutionIdentity) error {
	return errors.New("unexpected legacy preflight")
}
func (joinedUnusedHarness) Execute(context.Context, ports.ExecutionRequest, ports.ExecutionEventSink) (ports.ExecutionResult, error) {
	return ports.ExecutionResult{}, errors.New("unexpected legacy execution")
}
func (joinedUnusedHarness) Cancel(context.Context, ports.ExecutionIdentity) error {
	return errors.New("unexpected legacy cancel")
}

type joinedUnusedSink struct{}

func (joinedUnusedSink) Emit(context.Context, ports.ExecutionEvent) error {
	return errors.New("unexpected streaming event")
}

func assertJoinedEffects(t *testing.T, fixture *joinedFixture, issue, materialize, release, proxyPreflight, send int) {
	t.Helper()
	got := [5]int{fixture.credentials.issues, fixture.credentials.materializations, fixture.credentials.releases, fixture.proxy.preflights, fixture.proxy.invocations}
	want := [5]int{issue, materialize, release, proxyPreflight, send}
	if got != want {
		t.Errorf("effects [issue materialize release proxy-preflight send] = %v, want %v; boundary error=%v seal error=%v", got, want, fixture.driver.boundaryErr, fixture.driver.sealErr)
	}
}

func TestJoinedEgressSealsExactOwnedExecution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixture := newJoinedFixture(t)
		result, evidence, err := fixture.prepare(t).Execute(context.Background(), fixture.projection, joinedUnusedSink{}, joinedUnusedHarness{})
		if err != nil {
			t.Fatalf("joined execute: %v; boundary=%v seal=%v", err, fixture.driver.boundaryErr, fixture.driver.sealErr)
		}
		authority := fixture.request.Prepared.Authority()
		if err := evidence.ValidateForPersistedAuthority(authority, fixture.owned.Reservation); err != nil {
			t.Errorf("sealed evidence against persisted authority: %v", err)
		}
		if result.ProviderEvidence == nil || evidence.ProviderEvidence == nil || result.ProviderEvidence.EvidenceDigest != evidence.ProviderEvidence.EvidenceDigest {
			t.Error("result and sealed provider evidence diverged")
		}
		if evidence.CredentialFinalization != domain.CredentialFinalizationVerifiedV1 {
			t.Errorf("credential finalization=%s, want verified", evidence.CredentialFinalization)
		}
		assertJoinedEffects(t, fixture, 1, 1, 1, 1, 1)
	})
}

func TestJoinedEgressRejectsSubstitutionBeforeEffects(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*joinedFixture)
	}{
		{"request_owner", func(f *joinedFixture) { f.projection.OwnerUserID = "foreign-owner" }},
		{"request_model", func(f *joinedFixture) { f.projection.HarnessBinding.ModelID = "different-model" }},
		{"request_substrate", func(f *joinedFixture) { f.projection.SubstrateBinding.ProfileRevision++ }},
		{"projection_resource", func(f *joinedFixture) { f.projection.Credential.ProviderResource.Revision++ }},
		{"boundary_policy_owner", func(f *joinedFixture) { f.request.RoutePolicy.Scope.OwnerUserID = "foreign-owner" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fixture := newJoinedFixture(t)
				execution := fixture.prepare(t)
				test.mutate(fixture)
				_, evidence, err := execution.Execute(context.Background(), fixture.projection, joinedUnusedSink{}, joinedUnusedHarness{})
				if err == nil || evidence.EvidenceDigest != "" {
					t.Fatalf("substitution accepted or clean evidence returned: err=%v digest=%s", err, evidence.EvidenceDigest)
				}
				assertJoinedEffects(t, fixture, 0, 0, 0, 0, 0)
			})
		})
	}
	t.Run("authenticated_grant", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fixture := newJoinedFixture(t)
			fixture.owned.Grant.Authenticator[0] ^= 0xff
			if _, err := fixture.preparer.PrepareExecution(context.Background(), fixture.owned); err == nil {
				t.Fatal("tampered grant prepared")
			}
			if fixture.driver.preflights != 0 {
				t.Errorf("tampered grant reached allocation: calls=%d", fixture.driver.preflights)
			}
			assertJoinedEffects(t, fixture, 0, 0, 0, 0, 0)
		})
	})
}

func TestJoinedEgressRejectsIssuedHandleSubstitution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixture := newJoinedFixture(t)
		fixture.credentials.handleMutation = func(handle *ports.ProviderInvocationCredentialV1) { handle.LeaseFence++ }
		_, evidence, err := fixture.prepare(t).Execute(context.Background(), fixture.projection, joinedUnusedSink{}, joinedUnusedHarness{})
		if err == nil || !errors.Is(fixture.driver.boundaryErr, ErrCredential) || evidence.EvidenceDigest != "" {
			t.Fatalf("substituted handle: err=%v boundary=%v evidence=%s", err, fixture.driver.boundaryErr, evidence.EvidenceDigest)
		}
		assertJoinedEffects(t, fixture, 1, 0, 1, 1, 0)
	})
}

func TestJoinedEgressReservationReplaySendsOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixture := newJoinedFixture(t)
		execution := fixture.prepare(t)
		if _, _, err := execution.Execute(context.Background(), fixture.projection, joinedUnusedSink{}, joinedUnusedHarness{}); err != nil {
			t.Fatalf("first execute: %v; boundary=%v seal=%v", err, fixture.driver.boundaryErr, fixture.driver.sealErr)
		}
		// Later preparation changes the capability, not its durable reservation.
		fixture.clock.now = fixture.clock.now.Add(time.Second)
		for _, replay := range []serverlessharness.PreparedExecutionV1{execution, fixture.prepare(t)} {
			_, evidence, err := replay.Execute(context.Background(), fixture.projection, joinedUnusedSink{}, joinedUnusedHarness{})
			if err == nil || evidence.EvidenceDigest != "" {
				t.Fatalf("replay accepted: err=%v evidence=%s", err, evidence.EvidenceDigest)
			}
		}
		digests := fixture.driver.preparedDigests
		if len(digests) != 3 || digests[0] != digests[1] || digests[0] == digests[2] {
			t.Errorf("same/fresh capability digests=%v, want first=second and first!=third", digests)
		}
		assertJoinedEffects(t, fixture, 1, 1, 1, 1, 1)
	})
}

func TestJoinedEgressObservationGrantIsReconcileOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixture := newJoinedFixture(t)
		authority := fixture.request.Prepared.Authority()
		grant, err := fixture.gate.issuer.MintAttemptEffectObservationGrant(authority, fixture.owned.Reservation, fixture.clock.Now(), fixture.clock.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		observed := ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectReconcileOnlyV1, Reservation: fixture.owned.Reservation, ObservationGrant: &grant}
		if _, err := fixture.preparer.PrepareExecution(context.Background(), observed); err == nil {
			t.Fatal("observation grant acquired execution")
		}
		reconciliation, err := fixture.preparer.PrepareReconciliation(context.Background(), observed)
		if err != nil {
			t.Fatalf("prepare reconciliation: %v", err)
		}
		if _, exposesExecute := reconciliation.(interface {
			Execute(context.Context, ports.ExecutionRequest, ports.ExecutionEventSink, ports.HarnessDriver) (ports.ExecutionResult, domain.SubstrateExecutionEvidenceV1, error)
		}); exposesExecute {
			t.Fatal("reconciliation exposes execution")
		}
		evidence, err := reconciliation.Reconcile(context.Background())
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if err := evidence.ValidateForPersistedAuthority(authority, fixture.owned.Reservation); err != nil {
			t.Errorf("reconciliation evidence: %v", err)
		}
		if fixture.driver.reconciliations != 1 || fixture.driver.preflights != 0 || fixture.driver.executions != 0 {
			t.Errorf("reconcile/preflight/execute=%d/%d/%d, want 1/0/0", fixture.driver.reconciliations, fixture.driver.preflights, fixture.driver.executions)
		}
		assertJoinedEffects(t, fixture, 0, 0, 0, 0, 0)
	})
}

func TestJoinedEgressConcurrentReplaySendsOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixture := newJoinedFixture(t)
		execution := fixture.prepare(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		t.Cleanup(cancel)
		start, done := make(chan struct{}), make(chan error, 2)
		for range 2 {
			go func() {
				<-start
				_, _, err := execution.Execute(ctx, fixture.projection, joinedUnusedSink{}, joinedUnusedHarness{})
				done <- err
			}()
		}
		close(start)
		accepted, rejected := 0, 0
		for range 2 {
			if err := <-done; err == nil {
				accepted++
			} else {
				rejected++
			}
		}
		if accepted != 1 || rejected != 1 {
			t.Errorf("concurrent accepted/rejected=%d/%d, want 1/1", accepted, rejected)
		}
		assertJoinedEffects(t, fixture, 1, 1, 1, 1, 1)
	})
}

func TestJoinedEgressActiveRedeliveryThenCancellationStopsProxyAndReleases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixture := newJoinedFixture(t)
		execution := fixture.prepare(t)
		fixture.proxy.blockUntilContext, fixture.proxy.invokeStarted = true, make(chan struct{})
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		t.Cleanup(cancel)
		type outcome struct {
			evidence domain.SubstrateExecutionEvidenceV1
			err      error
		}
		done := make(chan outcome, 1)
		go func() {
			_, evidence, err := execution.Execute(ctx, fixture.projection, joinedUnusedSink{}, joinedUnusedHarness{})
			done <- outcome{evidence, err}
		}()
		// Register cancel-and-join before waiting, including every failure path.
		joined := false
		t.Cleanup(func() {
			cancel()
			if !joined {
				<-done
			}
		})
		select {
		case <-fixture.proxy.invokeStarted:
		case <-ctx.Done():
			t.Fatalf("proxy did not start: %v", ctx.Err())
		}
		// Retry the same durable owner while the first accepted send is silent.
		// A newly prepared opaque session must share the issuer's reservation CAS.
		_, replayEvidence, replayErr := fixture.prepare(t).Execute(ctx, fixture.projection, joinedUnusedSink{}, joinedUnusedHarness{})
		if replayErr == nil || replayEvidence.EvidenceDigest != "" {
			t.Errorf("active redelivery accepted: err=%v evidence=%s", replayErr, replayEvidence.EvidenceDigest)
		}
		cancel()
		result := <-done
		joined = true
		if result.err == nil || result.evidence.EvidenceDigest != "" {
			t.Errorf("cancelled execute returned clean evidence: err=%v digest=%s", result.err, result.evidence.EvidenceDigest)
		}
		if !fixture.credentials.releaseContextLive {
			t.Error("credential release inherited cancelled execution context")
		}
		assertJoinedEffects(t, fixture, 1, 1, 1, 1, 1)
	})
}

func TestJoinedEgressReleaseFailureCannotSealCleanEvidence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixture := newJoinedFixture(t)
		fixture.credentials.releaseErr = errors.New("synthetic release failure")
		result, evidence, err := fixture.prepare(t).Execute(context.Background(), fixture.projection, joinedUnusedSink{}, joinedUnusedHarness{})
		if err == nil || !errors.Is(fixture.driver.boundaryErr, ErrCredentialFinalize) || evidence.EvidenceDigest != "" {
			t.Fatalf("release failure: err=%v boundary=%v digest=%s", err, fixture.driver.boundaryErr, evidence.EvidenceDigest)
		}
		if result.Summary != "" || result.ProviderEvidence != nil {
			t.Errorf("release failure projected success: summary=%q provider=%v", result.Summary, result.ProviderEvidence)
		}
		if fixture.driver.boundaryResult.Evidence.CredentialFinalization != domain.CredentialFinalizationFailedV1 {
			t.Errorf("release failure state=%s", fixture.driver.boundaryResult.Evidence.CredentialFinalization)
		}
		// A caller cannot re-label a successful provider send as clean substrate
		// completion while preserving Boundary's observed failed finalization.
		observed := fixture.driver.boundaryResult.Evidence
		provider, sealErr := (domain.ProviderExecutionEvidenceV1{AcceptanceClass: observed.Acceptance, FinishClass: domain.ProviderFinishCompletedV1,
			RouteState: observed.RouteState, ActualModelVendorID: observed.ActualModelVendorID, ActualModelID: observed.ActualModelID,
			TransportKind: observed.TransportKind, TransportProvider: observed.TransportProvider, UpstreamProviderID: observed.UpstreamProviderID,
			EndpointID: observed.EndpointID, PolicyVerdict: domain.ProviderPolicyGoV1, UsageProvenance: domain.ProviderUsageUnknownV1}).SealForBinding(fixture.request.Prepared.Authority().HarnessBinding)
		if sealErr != nil {
			t.Fatal(sealErr)
		}
		control := joinedSuccessEvidence(fixture.driver.boundaryResult.Evidence, provider)
		control.CredentialFinalization = domain.CredentialFinalizationVerifiedV1
		if _, err := control.SealForAuthority(fixture.request.Prepared.Authority(), fixture.owned.Reservation, fixture.request.Prepared.Allocation(), fixture.request.Prepared.Digest()); err != nil {
			t.Fatalf("single-field control evidence must seal: %v", err)
		}
		_, sealErr = joinedSuccessEvidence(fixture.driver.boundaryResult.Evidence, provider).SealForAuthority(fixture.request.Prepared.Authority(), fixture.owned.Reservation, fixture.request.Prepared.Allocation(), fixture.request.Prepared.Digest())
		if sealErr == nil {
			t.Error("failed credential finalization sealed as clean completion")
		}
		assertJoinedEffects(t, fixture, 1, 1, 1, 1, 1)
	})
}

var _ serverlessharness.ExecutionSubstrateV1 = (*joinedEgressSubstrate)(nil)
