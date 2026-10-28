package attachedworkerdaemontransport

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegenerationsession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ownerlocalcredential"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// These structural preflight inputs deliberately have unopened concrete owner
// pointers. They cannot bind a Session or publish an ACK. Genuine Session and
// Stack integration lives in attachedworkerstack's external transport tests.
func generationRuntimeFixtureV3(t *testing.T) (*adapterFixture, *pinnedInitialConnectPort, attachedworkersession.ConnectInputV1, attachedworkerstack.Config, *BoundMaterializer) {
	t.Helper()
	f, port, input, config := nativeRuntimeFixtureV3(t)
	old := config.PreparedCodexNative
	p := old.Profile
	profile := codexexec.NativeProfileV3{Backend: p.Backend, Launch: p.Launch, Platform: p.Platform,
		OperatingSystem: p.OperatingSystem, Architecture: p.Architecture, DeclaredContextWindow: p.DeclaredContextWindow,
		OutputReserveTokens: p.OutputReserveTokens, StreamIdleTimeoutMS: p.StreamIdleTimeoutMS}
	arguments, err := profile.ArgumentsV3()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := profile.DescriptorV3()
	if err != nil {
		t.Fatal(err)
	}
	input.CapabilityManifest.Features = []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureOutputReceipt}
	f.config.Profile.Arguments, port.manifest.Harness.Arguments = slices.Clone(arguments), slices.Clone(arguments)
	nativeRuntimeResealV3(t, f, &input)
	f.config.CapabilityManifest = &input.CapabilityManifest
	f.config.ReceiptPublisher = &receiptPublisherFixture{}
	bundle := old.Evidence
	for _, scope := range []*domain.ProviderEvidenceScopeV1{&bundle.Catalog.Scope, &bundle.Route.Scope, &bundle.Capability.Scope,
		&bundle.Privacy.Scope, &bundle.Price.Scope, &bundle.Policy.Scope} {
		scope.Backend = descriptor
	}
	bundle.Policy.PolicyID, bundle.Policy.Revision, bundle.Policy.EvidenceSource = codexexec.NativeEnvelopePolicyIDV3, codexexec.NativeEnvelopePolicyRevisionV3, "reviewed-native-envelope-v3"
	expires := bundle.Policy.ExpiresAt
	binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: bundle.Policy.Scope.TenantID,
		OwnerUserID: bundle.Policy.Scope.OwnerUserID, Resource: bundle.Policy.Scope.Resource, Backend: descriptor,
		ModelVendorID: "openai", ModelID: profile.Backend.Model, InputDataClass: domain.ProviderDataPrivateV1, EvidenceExpiresAt: &expires}
	for _, item := range []struct {
		digest func() (domain.ProviderEvidenceDigestV1, error)
		fields []*string
	}{
		{bundle.Catalog.Digest, []*string{&binding.ProviderCatalogDigest}},
		{bundle.Route.Digest, []*string{&binding.ProviderRouteDigest, &bundle.Policy.RoutePolicyDigest}},
		{bundle.Capability.Digest, []*string{&binding.CapabilityEvidenceDigest, &bundle.Policy.CapabilityEvidenceDigest}},
		{bundle.Privacy.Digest, []*string{&binding.PrivacyPolicyDigest, &bundle.Policy.PrivacyEvidenceDigest}},
		{bundle.Price.Digest, []*string{&bundle.Policy.PriceObservationDigest}},
		{func() (domain.ProviderEvidenceDigestV1, error) { return bundle.Policy.Digest() }, []*string{&binding.EffectivePolicyDigest}},
	} {
		digest, err := item.digest()
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range item.fields {
			*field = string(digest)
		}
	}
	baseline := domain.AttachedResourceGenerationBaselineV1{Version: 1, TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID,
		SubscriptionConnectionID: domain.SubscriptionConnectionID(binding.Resource.ResourceID), WorkerID: domain.AttachedWorkerID(input.CapabilityManifest.WorkerID),
		EnrollmentGeneration: input.CapabilityManifest.EnrollmentGeneration, BaselineRevision: binding.Resource.Revision,
		CapabilityDigest: f.config.Profile.CapabilityDigest, GenerationOnlyDelegation: true, Binding: binding, Evidence: bundle}
	if err := baseline.Validate(); err != nil {
		t.Fatal("complete structural baseline", err)
	}
	config.PreparedCodexNative = nil
	config.PreparedCodexNativeGeneration = &codexexec.NativePreparedConfigV3{Profile: profile, Baseline: baseline,
		GenerationOwner: &attachedresourcegenerationsession.OwnerV1{}, Lifecycle: &ownerlocalcredential.Lifecycle{}}
	materializer, err := NewNativeSubscriptionBoundMaterializerV1(&sealedSourceFixture{}, 8192, f.config.Now)
	if err != nil {
		t.Fatal(err)
	}
	return f, port, input, config, materializer
}

func TestNativeGenerationRuntimeV3FreezesFourthSelection(t *testing.T) {
	f, port, input, config, _ := generationRuntimeFixtureV3(t)
	original := config.PreparedCodexNativeGeneration
	original.InlineOutputV2 = true
	input.CapabilityManifest.Features = append(input.CapabilityManifest.Features, attachedworkerprotocol.FeatureInlineOutputV2)
	slices.Sort(input.CapabilityManifest.Features)
	nativeRuntimeResealV3(t, f, &input)
	original.Baseline.CapabilityDigest = f.config.Profile.CapabilityDigest
	before := original.Baseline.Clone()
	var selected *codexexec.NativePreparedConfigV3
	preflight, runner, err := pinnedRuntimePreflight(input.CapabilityManifest, f.config.Profile, config, original.Lifecycle,
		func(_ context.Context, _ attachedworkerlocal.ManifestV1, c attachedworkerstack.Config, credentials ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
			selected = c.PreparedCodexNativeGeneration
			if c.PreparedCodexNative != nil || credentials != original.Lifecycle {
				t.Fatal("wrong closed selection/concrete lifecycle")
			}
			return &runtimeRunner{}, nil
		})
	if err != nil || preflight == nil || runner == nil || !runner.nativeGenerationV3 || !runner.inlineOutputV2 || runner.ready() {
		t.Fatalf("generation preflight err=%v runner=%v", err, runner)
	}
	original.Baseline.Evidence.Catalog.Models[0].ModelID = "caller-mutated"
	original.Profile.Launch.ProviderHosts[0] = "caller.example"
	original.InlineOutputV2 = false
	if err := preflight(t.Context(), port.manifest); err != nil {
		t.Fatal(err)
	}
	if selected == nil || selected == original || !selected.InlineOutputV2 || !reflect.DeepEqual(selected.Baseline, before) || selected.Profile.Launch.ProviderHosts[0] == "caller.example" || selected.GenerationOwner != original.GenerationOwner || selected.Lifecycle != original.Lifecycle {
		t.Fatal("mutable declarations changed frozen original selection")
	}
}

func TestNativeGenerationRuntimeV3InitialReconnectUseLeaseHeldSelection(t *testing.T) {
	for _, reconnect := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial", true: "reconnect"}[reconnect], func(t *testing.T) {
			f, initial, input, config, materializer := generationRuntimeFixtureV3(t)
			native := config.PreparedCodexNativeGeneration
			stop := errors.New("controlled stop before connection network effects")
			connectors, stacks := 0, 0
			factory := func(_ context.Context, _ attachedworkerlocal.ManifestV1, actual attachedworkerstack.Config, credentials ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
				stacks++
				if actual.PreparedCodexNativeGeneration == nil || actual.PreparedCodexNative != nil || credentials != native.Lifecycle || actual.PreparedCodexNativeGeneration.GenerationOwner != native.GenerationOwner {
					t.Fatal("runtime lost original concrete generation owners")
				}
				return nil, stop
			}
			var runtime *ForegroundRuntime
			var err error
			var calls, network int
			if reconnect {
				port := &pinnedReconnectPort{fakeReconnectPort: &fakeReconnectPort{}, manifest: initial.manifest}
				runtime, err = reconnectPinnedForegroundRuntime(t.Context(), nil, nil, native.GenerationOwner, attachedworkersession.Config{}, attachedworkersession.ReconnectInputV1(input), materializer, f.config, cadenceConfig(true), config, native.Lifecycle, initialRuntimeConfig(),
					func(_ *attachedworkerlocal.Store, _ attachedworkersession.BootstrapPort, _ attachedworkersession.ExchangeFactory, c attachedworkersession.Config) (reconnectPort, error) {
						connectors++
						port.preflight = c.RuntimePreflight
						return port, nil
					}, factory)
				calls, network = port.calls, port.networkCalls
			} else {
				runtime, err = connectPinnedForegroundRuntime(t.Context(), nil, nil, native.GenerationOwner, attachedworkersession.Config{}, input, materializer, f.config, cadenceConfig(true), config, native.Lifecycle, initialRuntimeConfig(),
					func(_ *attachedworkerlocal.Store, _ attachedworkersession.BootstrapPort, _ attachedworkersession.ExchangeFactory, c attachedworkersession.Config) (initialConnectPort, error) {
						connectors++
						initial.preflight = c.RuntimePreflight
						return initial, nil
					}, factory)
				calls, network = initial.calls, initial.networkCalls
			}
			if !errors.Is(err, stop) || runtime != nil || connectors != 1 || stacks != 1 || calls != 1 || network != 0 {
				t.Fatalf("generation lease-held path: runtime=%t err=%v connector=%d stack=%d call=%d network=%d", runtime != nil, err, connectors, stacks, calls, network)
			}
		})
	}
}

func TestNativeGenerationRuntimeV3RejectsOwnerModeBeforeConnectorEffects(t *testing.T) {
	for _, reconnect := range []bool{false, true} {
		for _, mode := range []string{"foreign-owner", "wrapped-owner", "foreign-lifecycle", "interface-lifecycle", "default-materializer", "fixture-provider", "missing-receipt", "mixed-fixed"} {
			t.Run(mode+map[bool]string{false: "/initial", true: "/reconnect"}[reconnect], func(t *testing.T) {
				f, _, input, config, normal := generationRuntimeFixtureV3(t)
				native := config.PreparedCodexNativeGeneration
				var exchange attachedworkersession.ExchangeFactory = native.GenerationOwner
				var credentials ports.CredentialLifecycle = native.Lifecycle
				var materializer Materializer = normal
				switch mode {
				case "foreign-owner":
					exchange = &attachedresourcegenerationsession.OwnerV1{}
				case "wrapped-owner":
					exchange = generationOwnerWrapperV3{native.GenerationOwner}
				case "foreign-lifecycle":
					credentials = &ownerlocalcredential.Lifecycle{}
				case "interface-lifecycle":
					credentials = unusedInitialCredentials{}
				case "default-materializer":
					var err error
					materializer, err = NewBoundMaterializer(&sealedSourceFixture{}, 8192, f.config.Now)
					if err != nil {
						t.Fatal(err)
					}
				case "fixture-provider":
					normal.allowTestProvider = true
				case "missing-receipt":
					f.config.ReceiptPublisher = nil
				case "mixed-fixed":
					config.PreparedCodexNative = &codexexec.NativePreparedConfigV2{}
				}
				connectors, stacks := 0, 0
				factory := func(context.Context, attachedworkerlocal.ManifestV1, attachedworkerstack.Config, ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
					stacks++
					return nil, errors.New("must not construct stack")
				}
				var runtime *ForegroundRuntime
				var err error
				if reconnect {
					runtime, err = reconnectPinnedForegroundRuntime(t.Context(), nil, nil, exchange, attachedworkersession.Config{}, attachedworkersession.ReconnectInputV1(input), materializer, f.config, cadenceConfig(true), config, credentials, initialRuntimeConfig(),
						func(*attachedworkerlocal.Store, attachedworkersession.BootstrapPort, attachedworkersession.ExchangeFactory, attachedworkersession.Config) (reconnectPort, error) {
							connectors++
							return nil, errors.New("must not construct connector")
						}, factory)
				} else {
					runtime, err = connectPinnedForegroundRuntime(t.Context(), nil, nil, exchange, attachedworkersession.Config{}, input, materializer, f.config, cadenceConfig(true), config, credentials, initialRuntimeConfig(),
						func(*attachedworkerlocal.Store, attachedworkersession.BootstrapPort, attachedworkersession.ExchangeFactory, attachedworkersession.Config) (initialConnectPort, error) {
							connectors++
							return nil, errors.New("must not construct connector")
						}, factory)
				}
				if err == nil || runtime != nil || connectors != 0 || stacks != 0 {
					t.Fatalf("%s reached effects: runtime=%t err=%v connector=%d stack=%d", mode, runtime != nil, err, connectors, stacks)
				}
			})
		}
	}
}

type generationOwnerWrapperV3 struct {
	*attachedresourcegenerationsession.OwnerV1
}

func TestNativeGenerationRuntimeV3PreflightRejectsScopeProfileAndReceiptDrift(t *testing.T) {
	for _, mode := range []string{"worker", "enrollment", "capability", "argv", "environment", "os", "architecture", "version", "disabled", "mixed-v1", "mixed-media", "missing-inline-feature", "missing-receipt-feature"} {
		t.Run(mode, func(t *testing.T) {
			f, _, input, config, _ := generationRuntimeFixtureV3(t)
			native := config.PreparedCodexNativeGeneration
			switch mode {
			case "worker":
				native.Baseline.WorkerID = "foreign-worker"
			case "enrollment":
				native.Baseline.EnrollmentGeneration++
			case "capability":
				digest := []byte(native.Baseline.CapabilityDigest)
				if digest[0] == 'a' {
					digest[0] = 'b'
				} else {
					digest[0] = 'a'
				}
				native.Baseline.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(digest)
			case "argv":
				f.config.Profile.Arguments[0] = "foreign"
			case "environment":
				f.config.Profile.Environment = []attachedworkerdaemon.EnvironmentVariable{{Name: "CODEX_HOME", Value: "/foreign"}}
			case "os":
				input.CapabilityManifest.OperatingSystem = "darwin"
			case "architecture":
				input.CapabilityManifest.Architecture = "arm64"
			case "version":
				input.CapabilityManifest.HarnessVersion = "foreign-version"
			case "disabled":
				native.Profile.Backend.Enabled = false
			case "mixed-v1":
				config.PreparedCodex = &codexexec.Config{}
			case "mixed-media":
				config.PreparedCodexMedia = &codexexec.MediaConfigV1{}
			case "missing-inline-feature":
				native.InlineOutputV2 = true
			case "missing-receipt-feature":
				native.InlineOutputV2 = true
				input.CapabilityManifest.Features = []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureInlineOutputV2}
			}
			calls := 0
			preflight, runner, err := pinnedRuntimePreflight(input.CapabilityManifest, f.config.Profile, config, native.Lifecycle,
				func(context.Context, attachedworkerlocal.ManifestV1, attachedworkerstack.Config, ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
					calls++
					return &runtimeRunner{}, nil
				})
			if err == nil || preflight != nil || runner != nil || calls != 0 {
				t.Fatalf("%s selected generation mode: err=%v calls=%d", mode, err, calls)
			}
		})
	}
}

func TestNativeGenerationRuntimeV3NoContextlessOrSnapshotAssembly(t *testing.T) {
	f := newAdapterFixture(t)
	var runner attachedworkerdaemon.Runner = &runtimeRunner{}
	f.config.preparedReceipt = &preflightRunner{current: &runner, nativeGenerationV3: true}
	if adapter, err := New(f.session, f.materializer, f.config); err == nil || adapter != nil {
		t.Fatal("contextless New selected generation mode")
	}
	if adapter, err := newRuntimeAdapterV3(t.Context(), f.session, f.materializer, f.config, attachedworkersession.ResourceGenerationInitialV1); err == nil || adapter != nil {
		t.Fatal("snapshot-equivalent fake Session selected generation mode")
	}
	if adapter, err := NewNativeGenerationPreparedAdapterV3(t.Context(), &attachedworkersession.Session{}, f.materializer, Config{}, &attachedworkerstack.Stack{}, attachedworkersession.ResourceGenerationInitialV1); err == nil || adapter != nil {
		t.Fatal("fixture materializer promoted into normal generation mode")
	}
}

func TestNativeGenerationRuntimeV3ActiveGuardPrecedesFreshSynchronization(t *testing.T) {
	f := newAdapterFixture(t)
	f.adapter.generationStack = &attachedworkerstack.Stack{}
	f.adapter.active = &activeAttempt{}
	if _, found, err := f.adapter.Next(t.Context()); !errors.Is(err, ErrAttemptActive) || found {
		t.Fatalf("active owner reached new-work metadata fence: found=%t err=%v", found, err)
	}
}
