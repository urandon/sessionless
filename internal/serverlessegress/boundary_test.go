package serverlessegress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

type fixtureClock struct{ now time.Time }

func (clock *fixtureClock) Now() time.Time { return clock.now }

type recordingGate struct {
	issuer *serverlessharness.CapabilityIssuer
	trace  *[]string
}

func (gate *recordingGate) Validate(prepared serverlessharness.PreparedInvocation) error {
	*gate.trace = append(*gate.trace, "validate")
	return gate.issuer.Validate(prepared)
}

func (gate *recordingGate) Consume(prepared serverlessharness.PreparedInvocation) error {
	*gate.trace = append(*gate.trace, "consume")
	return gate.issuer.Consume(prepared)
}

type fixtureCredentials struct {
	trace              *[]string
	materialization    ports.ProviderCredentialMaterializationV1
	secret             []byte
	issueRequest       ports.ProviderCredentialIssueRequestV1
	handleMutation     func(*ports.ProviderInvocationCredentialV1)
	issueErr           error
	materializeErr     error
	releaseErr         error
	cancelBeforeRead   context.CancelFunc
	advanceBeforeRead  func()
	consumeTwice       bool
	captureCallback    bool
	lateCallback       ports.ProviderCredentialConsumerV1
	asyncCallback      bool
	asyncReady         <-chan struct{}
	issues             int
	materializations   int
	releases           int
	releaseContextLive bool
}

func (credentials *fixtureCredentials) IssueProviderCredential(_ context.Context, request ports.ProviderCredentialIssueRequestV1) (ports.ProviderInvocationCredentialV1, error) {
	*credentials.trace = append(*credentials.trace, "issue")
	credentials.issues++
	credentials.issueRequest = request
	if credentials.issueErr != nil {
		return ports.ProviderInvocationCredentialV1{}, credentials.issueErr
	}
	handle := ports.ProviderInvocationCredentialV1{
		HandleID: "provider-handle-1", TenantID: request.HarnessBinding.TenantID,
		OwnerUserID: request.HarnessBinding.OwnerUserID, RunID: request.HarnessBinding.RunID,
		AttemptID: request.HarnessBinding.AttemptID, WorkerID: request.WorkerID,
		LeaseID: request.LeaseID, LeaseFence: request.LeaseFence,
		ProviderResource: request.HarnessBinding.Resource, ExpiresAt: request.ExpiresAt,
	}
	if credentials.handleMutation != nil {
		credentials.handleMutation(&handle)
	}
	return handle, nil
}

func (credentials *fixtureCredentials) MaterializeProviderCredential(ctx context.Context, _ ports.ProviderInvocationCredentialV1, consume ports.ProviderCredentialConsumerV1) error {
	*credentials.trace = append(*credentials.trace, "materialize")
	credentials.materializations++
	if credentials.cancelBeforeRead != nil {
		credentials.cancelBeforeRead()
	}
	if credentials.advanceBeforeRead != nil {
		credentials.advanceBeforeRead()
	}
	if credentials.materializeErr != nil {
		return credentials.materializeErr
	}
	if credentials.captureCallback {
		credentials.lateCallback = consume
		return nil
	}
	if credentials.asyncCallback {
		secret := append([]byte(nil), credentials.secret...)
		go func() {
			defer clear(secret)
			_ = consume(credentials.materialization, secret)
		}()
		if credentials.asyncReady != nil {
			<-credentials.asyncReady
		}
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	secret := append([]byte(nil), credentials.secret...)
	defer clear(secret)
	err := consume(credentials.materialization, secret)
	if err == nil && credentials.consumeTwice {
		err = consume(credentials.materialization, secret)
	}
	return err
}

func (credentials *fixtureCredentials) ReleaseProviderCredential(ctx context.Context, _ ports.ProviderInvocationCredentialV1) error {
	*credentials.trace = append(*credentials.trace, "release")
	credentials.releases++
	credentials.releaseContextLive = ctx.Err() == nil
	return credentials.releaseErr
}

type fixtureProxy struct {
	clock             *fixtureClock
	trace             *[]string
	attestationMutate func(*ProxyAttestationV1)
	resultMutate      func(*ProxyResultV1)
	preflightErr      error
	advancePreflight  func()
	invokeErr         error
	preflights        int
	invocations       int
	lastInvocation    ProxyInvocationV1
	preflightDeadline time.Time
	invokeDeadline    time.Time
	invokeStarted     chan struct{}
	blockUntilContext bool
}

func (proxy *fixtureProxy) Preflight(ctx context.Context, policy PolicyV1) (ProxyAttestationV1, error) {
	*proxy.trace = append(*proxy.trace, "preflight")
	proxy.preflights++
	proxy.preflightDeadline, _ = ctx.Deadline()
	if err := ctx.Err(); err != nil {
		return ProxyAttestationV1{}, err
	}
	digest, _ := policy.Digest()
	attestation := ProxyAttestationV1{
		PolicyDigest: digest, ProxyArtifactDigest: policy.ProxyArtifactDigest,
		ProxyIdentityDigest: policy.ProxyIdentityDigest, ResolvedHost: policy.Endpoint.Host,
		ResolvedPort: policy.Endpoint.Port, ResolvedDNSClass: DNSPublicUnicastOnlyV1,
		ResolutionSetDigest: digestFixture("public-resolution-set"), ConnectionAddressPinned: true, DNSRebindingDenied: true,
		RedirectsDenied: true, AmbientProxyDenied: true, CertificateValidationRequired: true,
		ExpiresAt: proxy.clock.Now().Add(10 * time.Minute),
	}
	if proxy.attestationMutate != nil {
		proxy.attestationMutate(&attestation)
	}
	if proxy.advancePreflight != nil {
		proxy.advancePreflight()
	}
	return attestation, proxy.preflightErr
}

func (proxy *fixtureProxy) Invoke(ctx context.Context, invocation ProxyInvocationV1) (ProxyResultV1, error) {
	*proxy.trace = append(*proxy.trace, "invoke")
	proxy.invocations++
	proxy.invokeDeadline, _ = ctx.Deadline()
	if proxy.invokeStarted != nil {
		close(proxy.invokeStarted)
	}
	proxy.lastInvocation = ProxyInvocationV1{
		Policy: invocation.Policy, Attestation: invocation.Attestation, Materialization: invocation.Materialization,
		Secret: append([]byte(nil), invocation.Secret...), Payload: append([]byte(nil), invocation.Payload...),
	}
	result := ProxyResultV1{
		Route: invocation.Policy.Route, Acceptance: domain.ProviderAcceptanceAcceptedV1,
		RequestBytes: uint64(len(invocation.Payload)), ResponseBytes: 2, Response: []byte("ok"), ObservedAt: proxy.clock.Now(),
	}
	if proxy.resultMutate != nil {
		proxy.resultMutate(&result)
	}
	if proxy.blockUntilContext {
		<-ctx.Done()
		return result, ctx.Err()
	}
	if err := ctx.Err(); err != nil && proxy.invokeErr == nil {
		return result, err
	}
	return result, proxy.invokeErr
}

type boundaryFixture struct {
	clock       *fixtureClock
	gate        *recordingGate
	credentials *fixtureCredentials
	proxy       *fixtureProxy
	boundary    *BoundaryV1
	request     RequestV1
	trace       []string
}

func newBoundaryFixture(t *testing.T, delivery domain.ProviderCredentialDeliveryKindV1, inputClass domain.ProviderDataClassV1) *boundaryFixture {
	t.Helper()
	fixture := &boundaryFixture{clock: &fixtureClock{now: time.Now().UTC().Truncate(time.Millisecond)}}
	now := fixture.clock.Now()
	backend := domain.HarnessBackendDescriptorV1{
		HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "serverless-v1",
		BackendKind: domain.HarnessBackendDirectOpenRouterV1, ArtifactKind: domain.HarnessArtifactEmbeddedProfileV1,
		ArtifactDigest: digestFixture("backend-artifact"), NativeProtocolVersion: "openrouter-http-v1",
		BackendProfileDigest: digestFixture("backend-profile"), ProviderContractKind: domain.ProviderContractInvocationV1,
		CredentialDeliveryKind: delivery,
	}
	resource := domain.ProviderResourceBindingV1{
		Kind: domain.ProviderResourceRouterAccountV1, ResourceID: "openrouter-account-1", OwnerUserID: "user-1",
		Revision: 7, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 3,
	}
	scope := domain.ProviderEvidenceScopeV1{TenantID: "tenant-1", OwnerUserID: "user-1", Resource: resource, Backend: backend}
	route := domain.ProviderRouteV1{
		BackendKind: backend.BackendKind, ModelVendorID: "openai", TransportKind: domain.ProviderTransportRouterAPIV1,
		TransportProvider: "openrouter", UpstreamProviderID: "openai", EndpointID: "chat-completions-v1",
		BillingKind: domain.ProviderBillingRouterAccountV1, BillingAuthority: resource.ResourceID, ModelID: "gpt-5-mini",
	}
	routePolicy := domain.ProviderRoutePolicyV1{
		Version: domain.ProviderEvidenceVersionV1, Scope: scope, State: domain.ProviderEvidenceSupportedV1,
		PolicyID: "openrouter-route-1", Revision: 4, FallbackPolicy: domain.ProviderFallbackDenyV1,
		Routes: []domain.ProviderRouteV1{route}, ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(45 * time.Minute),
	}
	routeDigest, err := routePolicy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	effectivePolicy := domain.ProviderPolicyEvidenceV1{
		Version: domain.ProviderEvidenceVersionV1, Scope: scope, PolicyID: "provider-policy-1", Revision: 5,
		DecisionOwner: "sessionless-policy", EvidenceSource: "reviewed-provider-evidence", Verdict: domain.ProviderPolicyGoV1,
		AllowedDataClasses: []domain.ProviderDataClassV1{inputClass}, CapabilityEvidenceDigest: digestFixture("capability"),
		PrivacyEvidenceDigest: digestFixture("privacy"), PriceObservationDigest: digestFixture("price"),
		RoutePolicyDigest: string(routeDigest), ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(45 * time.Minute),
	}
	effectiveDigest, err := effectivePolicy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	egressPolicy := PolicyV1{
		Version: PolicyVersionV1, RoutePolicyDigest: string(routeDigest), EffectivePolicyDigest: string(effectiveDigest),
		ProxyArtifactDigest: digestFixture("proxy-artifact"), ProxyIdentityDigest: digestFixture("proxy-identity"), Route: route,
		Endpoint: EndpointPolicyV1{Scheme: "https", Host: "openrouter.ai", Port: 443, Path: "/api/v1/chat/completions", Method: "POST",
			DNSClass: DNSPublicUnicastOnlyV1, RedirectPolicy: RedirectDenyV1, MaxRequestBytes: 1024, MaxResponseBytes: 1024},
	}
	egressDigest, err := egressPolicy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	zero := uint64(0)
	limits := domain.SubstrateLimitsV1{
		InvocationTimeout: time.Hour, ExecutionTimeout: 20 * time.Minute, CleanupTimeout: 5 * time.Minute,
		CPUMillis: 1000, MemoryBytes: 256 << 20, ScratchBytes: 64 << 20, StdoutBytes: 128, StderrBytes: 128,
		NativeEventCount: 128, ArtifactBytes: 512,
	}
	cost := domain.AdmissionCostCeilingV1{
		Version: domain.AdmissionCostCeilingVersionV1, Currency: "USD", PriceRevision: "fixture-v1",
		PriceObservedAt: now.Add(-time.Minute), PriceExpiresAt: now.Add(50 * time.Minute), MaxDeliveries: 1,
		MaxPreEffectDurationPerDelivery: 5 * time.Minute, MaxActiveDuration: limits.ExecutionTimeout,
		MaxCleanupAndReconcileDuration: limits.CleanupTimeout, ConfiguredMemoryBytes: limits.MemoryBytes,
		ConfiguredVCPUMillis: limits.CPUMillis, MaxIngressBytes: 1024, MaxEgressBytes: 1024, MaxLogBytes: 256,
		MaxEvidenceBytes: 128, SubstratePriceState: domain.CostEvidenceKnownV1, ProviderPriceState: domain.ProviderPriceKnownFreeV1,
		MaxSubstrateAmountMicrounits: &zero, MaxProviderAmountMicrounits: &zero, MaxTotalAmountMicrounits: &zero,
	}
	costDigest, err := cost.Digest()
	if err != nil {
		t.Fatal(err)
	}
	substrate := domain.SubstrateBindingV1{
		Version: domain.SubstrateBindingVersionV1, Kind: domain.SubstrateDeterministicFixtureV1,
		ProfileID: "serverless-egress-fixture", ProfileRevision: 2, ProfileDigest: digestFixture("profile"),
		ProfileEvidenceExpiresAt: now.Add(50 * time.Minute), Region: "local-fixture", ImageDigest: digestFixture("image"),
		OuterHarnessArtifactDigest: digestFixture("outer"), WorkloadMode: domain.SubstrateWorkloadInProcessDirectV1,
		IsolationProfileDigest: digestFixture("isolation"), EgressPolicyDigest: egressDigest,
		CleanupPolicyDigest: digestFixture("cleanup"), EgressProxyArtifactDigest: egressPolicy.ProxyArtifactDigest,
		EgressProxyIdentityDigest: egressPolicy.ProxyIdentityDigest, AdmissionCostCeilingDigest: costDigest, Limits: limits,
	}
	substrateDigest, err := substrate.Digest()
	if err != nil {
		t.Fatal(err)
	}
	placement, err := domain.ManagedExecutionPlacementV2(string(substrateDigest))
	if err != nil {
		t.Fatal(err)
	}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	evidenceExpiry := now.Add(40 * time.Minute)
	binding := domain.HarnessBindingV1{
		Version: domain.HarnessBindingVersionV1, TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID,
		RunID: "run-1", AttemptID: "attempt-1", Backend: backend, Resource: resource,
		ModelVendorID: route.ModelVendorID, ModelID: route.ModelID, InputDataClass: inputClass,
		ProviderCatalogDigest: digestFixture("catalog"), ProviderRouteDigest: string(routeDigest),
		PrivacyPolicyDigest: effectivePolicy.PrivacyEvidenceDigest, CapabilityEvidenceDigest: effectivePolicy.CapabilityEvidenceDigest,
		EffectivePolicyDigest: string(effectiveDigest), ExecutionPlacementDigest: string(placementDigest), EvidenceExpiresAt: &evidenceExpiry,
	}
	authority := domain.ServerlessInvocationAuthorityV1{
		Version: domain.ServerlessInvocationAuthorityVersionV1, HarnessBinding: binding, ExecutionPlacementV2: placement,
		SubstrateBinding: substrate, AdmissionCostCeiling: cost,
		Lease: domain.Lease{ID: "lease-1", TenantID: binding.TenantID, RunID: binding.RunID, AttemptID: binding.AttemptID,
			WorkerID: "managed-worker-1", FenceToken: 11, AcquiredAt: now.Add(-time.Second), ExpiresAt: now.Add(time.Hour)},
		ContextManifestDigest: digestFixture("context"), InputManifestDigest: digestFixture("input"), InvocationDeadline: now.Add(30 * time.Minute),
	}
	reservation, err := domain.BuildAttemptEffectReservationV1(authority, "physical-claim-1", nil, now)
	if err != nil {
		t.Fatal(err)
	}
	allocation := domain.PreparedAllocationV1{
		Version: domain.PreparedAllocationVersionV1, SubstrateBindingDigest: substrateDigest,
		ObservedImageDigest: substrate.ImageDigest, ObservedOuterHarnessDigest: substrate.OuterHarnessArtifactDigest,
		ObservedProxyArtifactDigest: substrate.EgressProxyArtifactDigest, ObservedProxyIdentityDigest: substrate.EgressProxyIdentityDigest,
		WorkloadMode: domain.SubstrateWorkloadInProcessDirectV1,
		InProcess:    &domain.InProcessAttestationV1{LinkedBackendProfileDigest: backend.BackendProfileDigest},
	}
	issuer, err := serverlessharness.NewCapabilityIssuer(func() time.Time { return fixture.clock.Now() }, bytes.NewReader(bytes.Repeat([]byte{9}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := issuer.MintAttemptEffectOwnershipGrant(authority, reservation, now.Add(25*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := issuer.Issue(grant, allocation)
	if err != nil {
		t.Fatal(err)
	}
	fixture.gate = &recordingGate{issuer: issuer, trace: &fixture.trace}
	fixture.credentials = &fixtureCredentials{trace: &fixture.trace, materialization: materializationFixture(delivery), secret: []byte("credential-marker")}
	fixture.proxy = &fixtureProxy{clock: fixture.clock, trace: &fixture.trace}
	fixture.boundary, err = NewBoundaryV1(ConfigV1{Clock: fixture.clock, Gate: fixture.gate, Credentials: fixture.credentials, Proxy: fixture.proxy})
	if err != nil {
		t.Fatal(err)
	}
	fixture.request = RequestV1{Prepared: prepared, RoutePolicy: routePolicy, EffectivePolicy: effectivePolicy, Policy: egressPolicy, Payload: []byte("payload-marker")}
	return fixture
}

func materializationFixture(delivery domain.ProviderCredentialDeliveryKindV1) ports.ProviderCredentialMaterializationV1 {
	switch delivery {
	case domain.ProviderCredentialDeliveryFileV1:
		return ports.ProviderCredentialMaterializationV1{Kind: delivery, RootDir: "/tmp/sessionless-provider-1", FilePath: "/tmp/sessionless-provider-1/provider.json"}
	case domain.ProviderCredentialDeliveryEnvironmentV1:
		return ports.ProviderCredentialMaterializationV1{Kind: delivery, EnvironmentName: "OPENROUTER_API_KEY"}
	default:
		return ports.ProviderCredentialMaterializationV1{Kind: delivery}
	}
}

func digestFixture(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func TestBoundaryExecutesExactAttestedRouteOnce(t *testing.T) {
	fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPrivateV1)
	result, err := fixture.boundary.Execute(context.Background(), fixture.request)
	if err != nil || string(result.Response) != "ok" || result.Evidence.Egress != domain.SubstrateEgressPolicyEnforcedV1 ||
		result.Evidence.ProxyAttestation != domain.SubstrateProxyAttestationVerifiedV1 ||
		result.Evidence.CredentialFinalization != domain.CredentialFinalizationVerifiedV1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	expectedTrace := []string{"validate", "consume", "preflight", "validate", "issue", "materialize", "validate", "invoke", "release"}
	if !reflect.DeepEqual(fixture.trace, expectedTrace) {
		t.Fatalf("trace=%v", fixture.trace)
	}
	if fixture.credentials.issueRequest.ExpiresAt != fixture.request.Prepared.Authority().InvocationDeadline.Add(-20*time.Minute) {
		// The ten-minute proxy attestation is the shortest authority window.
		t.Fatalf("credential expiry=%s", fixture.credentials.issueRequest.ExpiresAt)
	}
	if !fixture.proxy.invokeDeadline.Equal(fixture.credentials.issueRequest.ExpiresAt) {
		t.Fatalf("invoke context deadline=%s credential expiry=%s", fixture.proxy.invokeDeadline, fixture.credentials.issueRequest.ExpiresAt)
	}
	if string(fixture.proxy.lastInvocation.Secret) != "credential-marker" || string(fixture.proxy.lastInvocation.Payload) != "payload-marker" {
		t.Fatal("proxy did not receive the exact process-local material")
	}
	beforePreflight, beforeIssue := fixture.proxy.preflights, fixture.credentials.issues
	if _, err := fixture.boundary.Execute(context.Background(), fixture.request); !errors.Is(err, ErrAuthority) {
		t.Fatalf("replay error=%v", err)
	}
	if fixture.proxy.preflights != beforePreflight || fixture.credentials.issues != beforeIssue {
		t.Fatal("replay reached proxy preflight or credential issue")
	}
}

func TestBoundaryDeniesPolicyDriftBeforeCapabilityOrSecretEffects(t *testing.T) {
	tests := map[string]func(*boundaryFixture){
		"private data not admitted": func(f *boundaryFixture) {
			f.request.EffectivePolicy.AllowedDataClasses = []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}
		},
		"no-go verdict": func(f *boundaryFixture) {
			f.request.EffectivePolicy.Verdict = domain.ProviderPolicyNoGoV1
			f.request.EffectivePolicy.AllowedDataClasses = nil
		},
		"cross-owner evidence": func(f *boundaryFixture) { f.request.RoutePolicy.Scope.OwnerUserID = "user-2" },
		"proxy identity drift": func(f *boundaryFixture) { f.request.Policy.ProxyIdentityDigest = digestFixture("other-proxy") },
		"private endpoint":     func(f *boundaryFixture) { f.request.Policy.Endpoint.Host = "127.0.0.1" },
		"metadata endpoint":    func(f *boundaryFixture) { f.request.Policy.Endpoint.Host = "metadata.google.internal" },
		"alternate port":       func(f *boundaryFixture) { f.request.Policy.Endpoint.Port = 8443 },
		"redirect allowed":     func(f *boundaryFixture) { f.request.Policy.Endpoint.RedirectPolicy = "follow" },
		"payload overflow":     func(f *boundaryFixture) { f.request.Payload = bytes.Repeat([]byte("x"), 1025) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPrivateV1)
			mutate(fixture)
			if _, err := fixture.boundary.Execute(context.Background(), fixture.request); !errors.Is(err, ErrPolicy) {
				t.Fatalf("error=%v", err)
			}
			if fixture.proxy.preflights != 0 || fixture.credentials.issues != 0 || fixture.credentials.materializations != 0 || fixture.proxy.invocations != 0 {
				t.Fatalf("effects preflight=%d issue=%d materialize=%d invoke=%d", fixture.proxy.preflights, fixture.credentials.issues, fixture.credentials.materializations, fixture.proxy.invocations)
			}
			if !reflect.DeepEqual(fixture.trace, []string{"validate"}) {
				t.Fatalf("trace=%v", fixture.trace)
			}
		})
	}
}

func TestBoundaryDeniesProxyAttestationBeforeCredentialIssue(t *testing.T) {
	tests := map[string]func(*ProxyAttestationV1){
		"wrong identity":     func(value *ProxyAttestationV1) { value.ProxyIdentityDigest = digestFixture("other") },
		"private dns class":  func(value *ProxyAttestationV1) { value.ResolvedDNSClass = "private" },
		"unpinned address":   func(value *ProxyAttestationV1) { value.ConnectionAddressPinned = false },
		"dns rebinding":      func(value *ProxyAttestationV1) { value.DNSRebindingDenied = false },
		"redirects enabled":  func(value *ProxyAttestationV1) { value.RedirectsDenied = false },
		"ambient proxy":      func(value *ProxyAttestationV1) { value.AmbientProxyDenied = false },
		"outlives authority": func(value *ProxyAttestationV1) { value.ExpiresAt = value.ExpiresAt.Add(time.Hour) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPublicV1)
			fixture.proxy.attestationMutate = mutate
			result, err := fixture.boundary.Execute(context.Background(), fixture.request)
			if !errors.Is(err, ErrProxyAttestation) || result.Evidence.ProxyAttestation != domain.SubstrateProxyAttestationMismatchV1 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if fixture.credentials.issues != 0 || fixture.proxy.invocations != 0 {
				t.Fatal("invalid proxy attestation reached credentials or network")
			}
		})
	}
}

func TestBoundaryRechecksAuthorityAfterProxyPreflight(t *testing.T) {
	fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPublicV1)
	fixture.proxy.advancePreflight = func() { fixture.clock.now = fixture.clock.now.Add(31 * time.Minute) }
	result, err := fixture.boundary.Execute(context.Background(), fixture.request)
	if !errors.Is(err, ErrAuthority) || result.Evidence.Egress != domain.SubstrateEgressDeniedV1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if fixture.credentials.issues != 0 || fixture.credentials.materializations != 0 || fixture.proxy.invocations != 0 {
		t.Fatal("expired post-preflight authority reached credentials or network")
	}
}

func TestBoundaryRequiresExactCredentialHandleAndDelivery(t *testing.T) {
	handleTests := map[string]func(*ports.ProviderInvocationCredentialV1){
		"owner":       func(value *ports.ProviderInvocationCredentialV1) { value.OwnerUserID = "user-2" },
		"generation":  func(value *ports.ProviderInvocationCredentialV1) { value.ProviderResource.CredentialGeneration++ },
		"lease fence": func(value *ports.ProviderInvocationCredentialV1) { value.LeaseFence++ },
		"expiry":      func(value *ports.ProviderInvocationCredentialV1) { value.ExpiresAt = value.ExpiresAt.Add(time.Second) },
	}
	for name, mutate := range handleTests {
		t.Run("handle "+name, func(t *testing.T) {
			fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPublicV1)
			fixture.credentials.handleMutation = mutate
			if _, err := fixture.boundary.Execute(context.Background(), fixture.request); !errors.Is(err, ErrCredential) {
				t.Fatalf("error=%v", err)
			}
			if fixture.credentials.releases != 1 || fixture.credentials.materializations != 0 || fixture.proxy.invocations != 0 {
				t.Fatalf("release=%d materialize=%d invoke=%d", fixture.credentials.releases, fixture.credentials.materializations, fixture.proxy.invocations)
			}
		})
	}
	for _, delivery := range []domain.ProviderCredentialDeliveryKindV1{domain.ProviderCredentialDeliveryFileV1, domain.ProviderCredentialDeliveryEnvironmentV1, domain.ProviderCredentialDeliveryDirectV1} {
		t.Run("exact "+string(delivery), func(t *testing.T) {
			fixture := newBoundaryFixture(t, delivery, domain.ProviderDataPublicV1)
			if delivery == domain.ProviderCredentialDeliveryFileV1 {
				fixture.credentials.secret = nil
			}
			if _, err := fixture.boundary.Execute(context.Background(), fixture.request); err != nil {
				t.Fatal(err)
			}
			if fixture.proxy.lastInvocation.Materialization.Kind != delivery {
				t.Fatalf("delivery=%s", fixture.proxy.lastInvocation.Materialization.Kind)
			}
		})
	}
	t.Run("delivery conversion", func(t *testing.T) {
		fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPublicV1)
		fixture.credentials.materialization = materializationFixture(domain.ProviderCredentialDeliveryEnvironmentV1)
		if _, err := fixture.boundary.Execute(context.Background(), fixture.request); !errors.Is(err, ErrDelivery) {
			t.Fatalf("error=%v", err)
		}
		if fixture.proxy.invocations != 0 || fixture.credentials.releases != 1 {
			t.Fatal("delivery mismatch was not contained and released")
		}
	})
	t.Run("materializer callback replay", func(t *testing.T) {
		fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPublicV1)
		fixture.credentials.consumeTwice = true
		result, err := fixture.boundary.Execute(context.Background(), fixture.request)
		if !errors.Is(err, ErrCredential) || fixture.proxy.invocations != 1 || len(result.Response) != 0 || fixture.credentials.releases != 1 {
			t.Fatalf("result=%+v err=%v invokes=%d releases=%d", result, err, fixture.proxy.invocations, fixture.credentials.releases)
		}
	})
	t.Run("late callback", func(t *testing.T) {
		fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPublicV1)
		fixture.credentials.captureCallback = true
		result, err := fixture.boundary.Execute(context.Background(), fixture.request)
		if !errors.Is(err, ErrCredential) || len(result.Response) != 0 || fixture.credentials.releases != 1 {
			t.Fatalf("result=%+v err=%v releases=%d", result, err, fixture.credentials.releases)
		}
		if fixture.credentials.lateCallback == nil {
			t.Fatal("fixture did not capture callback")
		}
		if err := fixture.credentials.lateCallback(fixture.credentials.materialization, fixture.credentials.secret); !errors.Is(err, ErrCredential) {
			t.Fatalf("late callback error=%v", err)
		}
		if fixture.proxy.invocations != 0 {
			t.Fatal("late callback reached provider effect")
		}
	})
}

func TestBoundaryRejectsProxyEvidenceTamperingAndBoundsResponse(t *testing.T) {
	tests := map[string]func(*ProxyResultV1){
		"route":          func(value *ProxyResultV1) { value.Route.EndpointID = "other" },
		"request bytes":  func(value *ProxyResultV1) { value.RequestBytes++ },
		"response bytes": func(value *ProxyResultV1) { value.ResponseBytes++ },
		"response overflow": func(value *ProxyResultV1) {
			value.Response = bytes.Repeat([]byte("x"), 1025)
			value.ResponseBytes = 1025
		},
		"acceptance":  func(value *ProxyResultV1) { value.Acceptance = "invented" },
		"observation": func(value *ProxyResultV1) { value.ObservedAt = time.Time{} },
		"stale observation": func(value *ProxyResultV1) {
			value.ObservedAt = value.ObservedAt.Add(-time.Second)
		},
		"future observation": func(value *ProxyResultV1) {
			value.ObservedAt = value.ObservedAt.Add(time.Second)
		},
		"non-UTC observation": func(value *ProxyResultV1) {
			value.ObservedAt = value.ObservedAt.In(time.FixedZone("fixture", 3600))
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPublicV1)
			fixture.proxy.resultMutate = mutate
			result, err := fixture.boundary.Execute(context.Background(), fixture.request)
			if !errors.Is(err, ErrEvidence) || len(result.Response) != 0 || fixture.credentials.releases != 1 {
				t.Fatalf("result=%+v err=%v releases=%d", result, err, fixture.credentials.releases)
			}
		})
	}
}

func TestBoundaryCleanupIsIndependentFromCallerCancellationAndOutcome(t *testing.T) {
	t.Run("cancelled materialization", func(t *testing.T) {
		fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPublicV1)
		ctx, cancel := context.WithCancel(context.Background())
		fixture.credentials.cancelBeforeRead = cancel
		if _, err := fixture.boundary.Execute(ctx, fixture.request); !errors.Is(err, ErrCredential) {
			t.Fatalf("error=%v", err)
		}
		if fixture.credentials.releases != 1 || !fixture.credentials.releaseContextLive || fixture.proxy.invocations != 0 {
			t.Fatalf("release=%d live=%t invoke=%d", fixture.credentials.releases, fixture.credentials.releaseContextLive, fixture.proxy.invocations)
		}
	})
	t.Run("attestation expires before secret read", func(t *testing.T) {
		fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPublicV1)
		fixture.credentials.advanceBeforeRead = func() { fixture.clock.now = fixture.clock.now.Add(11 * time.Minute) }
		if _, err := fixture.boundary.Execute(context.Background(), fixture.request); !errors.Is(err, ErrAuthority) {
			t.Fatalf("error=%v", err)
		}
		if fixture.proxy.invocations != 0 || fixture.credentials.releases != 1 {
			t.Fatal("expired attestation reached network or skipped release")
		}
	})
	t.Run("release failure remains orthogonal", func(t *testing.T) {
		fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPublicV1)
		fixture.credentials.releaseErr = errors.New("private cleanup failure")
		result, err := fixture.boundary.Execute(context.Background(), fixture.request)
		if !errors.Is(err, ErrCredentialFinalize) || string(result.Response) != "ok" ||
			result.Evidence.CredentialFinalization != domain.CredentialFinalizationFailedV1 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	})
	t.Run("provider error clears partial response", func(t *testing.T) {
		fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPublicV1)
		fixture.proxy.invokeErr = errors.New("private provider failure")
		result, err := fixture.boundary.Execute(context.Background(), fixture.request)
		if !errors.Is(err, ErrEgress) || len(result.Response) != 0 || result.Evidence.Egress != domain.SubstrateEgressPolicyEnforcedV1 ||
			fixture.credentials.releases != 1 {
			t.Fatalf("result=%+v err=%v releases=%d", result, err, fixture.credentials.releases)
		}
	})
	t.Run("caller cancellation bounds in-flight proxy", func(t *testing.T) {
		fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryDirectV1, domain.ProviderDataPublicV1)
		fixture.proxy.invokeStarted = make(chan struct{})
		fixture.proxy.blockUntilContext = true
		fixture.credentials.asyncCallback = true
		fixture.credentials.asyncReady = fixture.proxy.invokeStarted
		ctx, cancel := context.WithCancel(context.Background())
		type outcome struct {
			result ResultV1
			err    error
		}
		done := make(chan outcome, 1)
		go func() {
			result, err := fixture.boundary.Execute(ctx, fixture.request)
			done <- outcome{result: result, err: err}
		}()
		<-fixture.proxy.invokeStarted
		cancel()
		observed := <-done
		if !errors.Is(observed.err, ErrEgress) || len(observed.result.Response) != 0 ||
			fixture.credentials.releases != 1 || !fixture.credentials.releaseContextLive {
			t.Fatalf("result=%+v err=%v release=%d live=%t", observed.result, observed.err, fixture.credentials.releases, fixture.credentials.releaseContextLive)
		}
	})
}

func TestBoundaryPublicRepresentationsAreContentFree(t *testing.T) {
	fixture := newBoundaryFixture(t, domain.ProviderCredentialDeliveryFileV1, domain.ProviderDataPublicV1)
	fixture.credentials.secret = nil
	result, err := fixture.boundary.Execute(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]any{"request": fixture.request, "invocation": fixture.proxy.lastInvocation, "result": result} {
		encoded, encodeErr := json.Marshal(value)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		visible := string(encoded) + " " + strings.TrimSpace(toString(value))
		for _, marker := range []string{"credential-marker", "payload-marker", "\"ok\"", "/tmp/sessionless-provider-1", "provider.json"} {
			if strings.Contains(visible, marker) {
				t.Fatalf("%s exposed marker %q in %s", name, marker, visible)
			}
		}
	}
}

func toString(value any) string {
	if stringer, ok := value.(interface{ String() string }); ok {
		return stringer.String()
	}
	return ""
}
