package providercomposition

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/codexopenrouter"
	"gitcode.com/urandon/sessionless/internal/directopenrouter"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/opencodeopenrouter"
	"gitcode.com/urandon/sessionless/internal/piopenrouter"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

var compositionNow = time.Date(2026, time.September, 6, 12, 0, 0, 0, time.UTC)

func TestNewDisabledRegistryV1RejectsIncompleteOrEnabledDependencies(t *testing.T) {
	complete := newDriverSet(t, false)
	tests := []struct {
		name         string
		now          func() time.Time
		dependencies DependenciesV1
	}{
		{name: "nil clock", dependencies: complete.dependencies()},
		{name: "missing codex", now: fixedClock, dependencies: DependenciesV1{OpenCode: complete.openCode, Pi: complete.pi, Direct: complete.direct}},
		{name: "missing opencode", now: fixedClock, dependencies: DependenciesV1{Codex: complete.codex, Pi: complete.pi, Direct: complete.direct}},
		{name: "missing pi", now: fixedClock, dependencies: DependenciesV1{Codex: complete.codex, OpenCode: complete.openCode, Direct: complete.direct}},
		{name: "missing direct", now: fixedClock, dependencies: DependenciesV1{Codex: complete.codex, OpenCode: complete.openCode, Pi: complete.pi}},
		{name: "enabled profiles", now: fixedClock, dependencies: newDriverSet(t, true).dependencies()},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			registry, err := NewDisabledRegistryV1(testCase.now, testCase.dependencies)
			if registry != nil || !errors.Is(err, ErrContract) {
				t.Fatalf("NewDisabledRegistryV1() = (%v, %v), want (nil, ErrContract)", registry, err)
			}
		})
	}
}

func TestDisabledRegistrationsAreExactUniqueAndOrderIndependent(t *testing.T) {
	drivers := newDriverSet(t, false)
	registrations, err := registrationsV1(drivers.dependencies())
	if err != nil {
		t.Fatalf("registrationsV1() error = %v", err)
	}
	if len(registrations) != 4 {
		t.Fatalf("registration count = %d, want 4", len(registrations))
	}
	wantKinds := []domain.HarnessBackendKindV1{
		domain.HarnessBackendCodexOpenRouterV1,
		domain.HarnessBackendOpenCodeV1,
		domain.HarnessBackendPiV1,
		domain.HarnessBackendDirectOpenRouterV1,
	}
	seen := make(map[domain.HarnessBackendDescriptorV1]struct{}, len(registrations))
	for index, registration := range registrations {
		if registration.Enabled {
			t.Errorf("registration %d is enabled", index)
		}
		if registration.Descriptor.BackendKind != wantKinds[index] {
			t.Errorf("registration %d backend = %q, want %q", index, registration.Descriptor.BackendKind, wantKinds[index])
		}
		if _, duplicate := seen[registration.Descriptor]; duplicate {
			t.Errorf("registration %d duplicates descriptor %+v", index, registration.Descriptor)
		}
		seen[registration.Descriptor] = struct{}{}
	}

	reversed := append([]sessionlessharness.Registration(nil), registrations...)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	forwardRegistry, err := sessionlessharness.NewRegistry(fixedClock, registrations...)
	if err != nil {
		t.Fatalf("NewRegistry(forward) error = %v", err)
	}
	reverseRegistry, err := sessionlessharness.NewRegistry(fixedClock, reversed...)
	if err != nil {
		t.Fatalf("NewRegistry(reverse) error = %v", err)
	}
	for _, descriptor := range drivers.descriptors() {
		identity := validIdentity(t, descriptor)
		requireFailureCode(t, forwardRegistry.Preflight(context.Background(), identity), sessionlessharness.FailureHarnessBackendDisabled)
		requireFailureCode(t, reverseRegistry.Preflight(context.Background(), identity), sessionlessharness.FailureHarnessBackendDisabled)
	}
	if effects := drivers.effects(); effects.runs != 0 || effects.invokes != 0 || effects.cancels != 0 {
		t.Fatalf("disabled preflight effects = %+v, want zero", effects)
	}
}

func TestCancelRoutesOnlyToExactMatchingDisabledBackend(t *testing.T) {
	drivers := newDriverSet(t, false)
	registry, err := NewDisabledRegistryV1(fixedClock, drivers.dependencies())
	if err != nil {
		t.Fatalf("NewDisabledRegistryV1() error = %v", err)
	}
	for _, descriptor := range drivers.descriptors() {
		before := drivers.effects()
		beforeCancels := drivers.cancelCounts()
		if err := registry.Cancel(context.Background(), validIdentity(t, descriptor)); err != nil {
			t.Fatalf("Cancel(%q) error = %v", descriptor.BackendKind, err)
		}
		after := drivers.effects()
		if after.cancels != before.cancels+1 || after.runs != before.runs || after.invokes != before.invokes {
			t.Fatalf("Cancel(%q) effects before=%+v after=%+v, want one cancellation only", descriptor.BackendKind, before, after)
		}
		requireOnlyCancelIncrement(t, descriptor.BackendKind, beforeCancels, drivers.cancelCounts())
	}

	before := drivers.effects()
	for _, descriptor := range drivers.descriptors() {
		mutations := []struct {
			name   string
			code   sessionlessharness.FailureCode
			mutate func(*ports.ExecutionIdentity)
		}{
			{name: "unknown descriptor", code: sessionlessharness.FailureHarnessBackendUnsupported, mutate: func(identity *ports.ExecutionIdentity) {
				identity.HarnessBinding.Backend.BackendProfileDigest = differentDigest(identity.HarnessBinding.Backend.BackendProfileDigest)
			}},
			{name: "wrong model", code: sessionlessharness.FailureProviderCatalogExpired, mutate: func(identity *ports.ExecutionIdentity) {
				identity.HarnessBinding.ModelID = "near-match"
			}},
			{name: "wrong resource", code: sessionlessharness.FailureProviderResourceMismatch, mutate: func(identity *ports.ExecutionIdentity) {
				identity.HarnessBinding.Resource.Kind = domain.ProviderResourceSubscriptionV1
			}},
			{name: "private data", code: sessionlessharness.FailureEffectivePolicyMismatch, mutate: func(identity *ports.ExecutionIdentity) {
				identity.HarnessBinding.InputDataClass = domain.ProviderDataPrivateV1
			}},
		}
		for _, mutation := range mutations {
			t.Run(string(descriptor.BackendKind)+"/"+mutation.name, func(t *testing.T) {
				identity := validIdentity(t, descriptor)
				mutation.mutate(&identity)
				requireFailureCode(t, registry.Cancel(context.Background(), identity), mutation.code)
				if after := drivers.effects(); after != before {
					t.Fatalf("mutation effects before=%+v after=%+v", before, after)
				}
			})
		}
	}

	scopeMutations := []struct {
		name   string
		mutate func(*ports.ExecutionIdentity)
	}{
		{name: "tenant", mutate: func(identity *ports.ExecutionIdentity) { identity.TenantID = "tenant-other" }},
		{name: "owner", mutate: func(identity *ports.ExecutionIdentity) { identity.OwnerUserID = "owner-other" }},
		{name: "run", mutate: func(identity *ports.ExecutionIdentity) { identity.RunID = "run-other" }},
		{name: "attempt", mutate: func(identity *ports.ExecutionIdentity) { identity.AttemptID = "attempt-other" }},
	}
	for _, mutation := range scopeMutations {
		t.Run("scope/"+mutation.name, func(t *testing.T) {
			identity := validIdentity(t, drivers.codex.DescriptorV1())
			mutation.mutate(&identity)
			requireFailureCode(t, registry.Cancel(context.Background(), identity), sessionlessharness.FailureHarnessBindingInvalid)
			if after := drivers.effects(); after != before {
				t.Fatalf("scope mismatch effects before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestDisabledCompositionCannotClaimNativeProtocolSupport(t *testing.T) {
	drivers := newDriverSet(t, false)
	observers := []harnessconformance.BackendProtocolObserver{drivers.codex, drivers.openCode, drivers.pi, drivers.direct}
	for index, observer := range observers {
		if state := observer.BackendProtocolState(); state != harnessconformance.BackendProtocolUnsupportedV1 {
			t.Errorf("observer %d state = %q, want %q", index, state, harnessconformance.BackendProtocolUnsupportedV1)
		}
	}
}

type driverSet struct {
	codex          *codexopenrouter.Driver
	openCode       *opencodeopenrouter.Driver
	pi             *piopenrouter.Driver
	direct         *directopenrouter.Driver
	codexBoundary  *codexBoundary
	openBoundary   *openCodeBoundary
	piBoundary     *piBoundary
	directBoundary *directBoundary
}

func newDriverSet(t *testing.T, enabled bool) driverSet {
	t.Helper()
	set := driverSet{
		codexBoundary:  &codexBoundary{},
		openBoundary:   &openCodeBoundary{},
		piBoundary:     &piBoundary{},
		directBoundary: &directBoundary{},
	}
	var err error
	set.codex, err = codexopenrouter.NewDriver(codexopenrouter.ProfileV1{
		Enabled: enabled, Executable: "/sessionless/bin/codex", ExecutableVersion: "1.0.0",
		ExecutableDigest: executableDigest(1), ProviderTimeoutMS: 60_000, ContextWindow: 65_536, MaxOutputTokens: 8_192,
	}, set.codexBoundary)
	if err != nil {
		t.Fatalf("NewDriver(codex) error = %v", err)
	}
	set.openCode, err = opencodeopenrouter.NewDriver(opencodeopenrouter.ProfileV1{
		Enabled: enabled, Executable: "/sessionless/bin/opencode", ExecutableVersion: "1.0.0",
		ExecutableDigest: executableDigest(2), SourceRevision: opencodeopenrouter.OpenCodeSourceRevisionV1, ProviderTimeoutMS: 60_000,
	}, set.openBoundary)
	if err != nil {
		t.Fatalf("NewDriver(opencode) error = %v", err)
	}
	set.pi, err = piopenrouter.NewDriver(piopenrouter.ProfileV1{
		Enabled: enabled, Executable: "/sessionless/bin/pi", ExecutableVersion: "1.0.0",
		ExecutableDigest: executableDigest(3), SourceRevision: piopenrouter.PiSourceRevisionV1, ProviderTimeoutMS: 60_000,
	}, set.piBoundary)
	if err != nil {
		t.Fatalf("NewDriver(pi) error = %v", err)
	}
	set.direct, err = directopenrouter.NewDriver(directopenrouter.ProfileV1{
		Enabled: enabled, RequestTimeoutMS: 60_000, MaxCompletionTokens: 8_192,
	}, set.directBoundary)
	if err != nil {
		t.Fatalf("NewDriver(direct) error = %v", err)
	}
	return set
}

func (set driverSet) dependencies() DependenciesV1 {
	return DependenciesV1{Codex: set.codex, OpenCode: set.openCode, Pi: set.pi, Direct: set.direct}
}

func (set driverSet) descriptors() []domain.HarnessBackendDescriptorV1 {
	return []domain.HarnessBackendDescriptorV1{
		set.codex.DescriptorV1(), set.openCode.DescriptorV1(), set.pi.DescriptorV1(), set.direct.DescriptorV1(),
	}
}

type effectCounts struct{ runs, invokes, cancels int }

type backendCancelCounts struct{ codex, openCode, pi, direct int }

func (set driverSet) effects() effectCounts {
	return effectCounts{
		runs:    set.codexBoundary.runs + set.openBoundary.runs + set.piBoundary.runs,
		invokes: set.directBoundary.invokes,
		cancels: set.codexBoundary.cancels + set.openBoundary.cancels + set.piBoundary.cancels + set.directBoundary.cancels,
	}
}

func (set driverSet) cancelCounts() backendCancelCounts {
	return backendCancelCounts{
		codex: set.codexBoundary.cancels, openCode: set.openBoundary.cancels,
		pi: set.piBoundary.cancels, direct: set.directBoundary.cancels,
	}
}

func requireOnlyCancelIncrement(t *testing.T, kind domain.HarnessBackendKindV1, before, after backendCancelCounts) {
	t.Helper()
	want := before
	switch kind {
	case domain.HarnessBackendCodexOpenRouterV1:
		want.codex++
	case domain.HarnessBackendOpenCodeV1:
		want.openCode++
	case domain.HarnessBackendPiV1:
		want.pi++
	case domain.HarnessBackendDirectOpenRouterV1:
		want.direct++
	default:
		t.Fatalf("unsupported backend kind %q", kind)
	}
	if after != want {
		t.Fatalf("Cancel(%q) routed calls = %+v, want %+v", kind, after, want)
	}
}

func differentDigest(value string) string {
	replacement := byte('f')
	if len(value) > 0 && value[0] == replacement {
		replacement = 'e'
	}
	return string(replacement) + value[1:]
}

func validIdentity(t *testing.T, descriptor domain.HarnessBackendDescriptorV1) ports.ExecutionIdentity {
	t.Helper()
	authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(
		"tenant-composition", "owner-composition", "run-composition", "attempt-composition", "connection-composition", compositionNow,
	)
	if err != nil {
		t.Fatalf("NewDeterministicFixtureManagedAuthorityV2() error = %v", err)
	}
	binding := authority.HarnessBinding
	binding.Backend = descriptor
	binding.Resource = domain.ProviderResourceBindingV1{
		Kind: domain.ProviderResourceRouterAccountV1, ResourceID: "router-composition", OwnerUserID: binding.OwnerUserID,
		Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1,
	}
	binding.ModelVendorID = "stealth"
	binding.ModelID = "stealth/ox-alpha"
	if descriptor.BackendKind == domain.HarnessBackendDirectOpenRouterV1 {
		binding.ModelID = "ox-alpha"
	}
	binding.InputDataClass = domain.ProviderDataExternallyShareableV1
	binding.ProviderCatalogDigest = strings.Repeat("1", 64)
	binding.ProviderRouteDigest = strings.Repeat("2", 64)
	binding.PrivacyPolicyDigest = strings.Repeat("3", 64)
	binding.CapabilityEvidenceDigest = strings.Repeat("4", 64)
	binding.EffectivePolicyDigest = strings.Repeat("5", 64)
	expiresAt := compositionNow.Add(time.Hour)
	binding.EvidenceExpiresAt = &expiresAt
	substrate := authority.SubstrateBinding
	cost := authority.AdmissionCostCeiling.Clone()
	identity := ports.ExecutionIdentity{
		TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, RunID: binding.RunID, AttemptID: binding.AttemptID,
		ExecutionPlacementV2: authority.ExecutionPlacementV2, HarnessBinding: binding,
		SubstrateBinding: &substrate, AdmissionCostCeiling: &cost,
	}
	if err := identity.Validate(); err != nil {
		t.Fatalf("validIdentity(%q) validation error = %v", descriptor.BackendKind, err)
	}
	return identity
}

func requireFailureCode(t *testing.T, err error, want sessionlessharness.FailureCode) {
	t.Helper()
	var classified *domain.ClassifiedError
	if !errors.As(err, &classified) || classified.Code != string(want) {
		t.Fatalf("error = %v, want classified code %q", err, want)
	}
}

func executableDigest(value byte) attachedworkerdaemon.ExecutableDigest {
	var digest attachedworkerdaemon.ExecutableDigest
	for index := range digest {
		digest[index] = value
	}
	return digest
}

func fixedClock() time.Time { return compositionNow }

type codexBoundary struct{ runs, cancels int }

func (boundary *codexBoundary) Run(context.Context, codexopenrouter.ProcessInvocationV1) (codexopenrouter.ProcessResultV1, error) {
	boundary.runs++
	return codexopenrouter.ProcessResultV1{}, errors.New("unexpected process run")
}
func (boundary *codexBoundary) Cancel(context.Context, ports.ExecutionIdentity) error {
	boundary.cancels++
	return nil
}

type openCodeBoundary struct{ runs, cancels int }

func (boundary *openCodeBoundary) Run(context.Context, opencodeopenrouter.ProcessInvocationV1) (opencodeopenrouter.ProcessResultV1, error) {
	boundary.runs++
	return opencodeopenrouter.ProcessResultV1{}, errors.New("unexpected process run")
}
func (boundary *openCodeBoundary) Cancel(context.Context, ports.ExecutionIdentity) error {
	boundary.cancels++
	return nil
}

type piBoundary struct{ runs, cancels int }

func (boundary *piBoundary) Run(context.Context, piopenrouter.ProcessInvocationV1) (piopenrouter.ProcessResultV1, error) {
	boundary.runs++
	return piopenrouter.ProcessResultV1{}, errors.New("unexpected process run")
}
func (boundary *piBoundary) Cancel(context.Context, ports.ExecutionIdentity) error {
	boundary.cancels++
	return nil
}

type directBoundary struct{ invokes, cancels int }

func (boundary *directBoundary) Invoke(context.Context, directopenrouter.InvocationV1) (directopenrouter.ResultV1, error) {
	boundary.invokes++
	return directopenrouter.ResultV1{}, errors.New("unexpected network invocation")
}
func (boundary *directBoundary) Cancel(context.Context, ports.ExecutionIdentity) error {
	boundary.cancels++
	return nil
}
