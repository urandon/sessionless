package attachedworkerdaemontransport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// The full metadata shape below is a local structural fixture, not a genuine
// admitted job or provider entitlement. No test here executes a process.
func nativeRuntimeFixtureV3(t *testing.T) (*adapterFixture, *pinnedInitialConnectPort, attachedworkersession.ConnectInputV1, attachedworkerstack.Config) {
	t.Helper()
	f, _, port, input := pinnedInitialFixture(t)
	artifact := func(name string) attachedworkerdaemon.NativeArtifactV2 {
		t.Helper()
		body := []byte("inert transport native artifact " + name)
		path := filepath.Join(f.config.MaterializationRoot, name)
		if err := os.WriteFile(path, body, 0o500); err != nil {
			t.Fatal(err)
		}
		return attachedworkerdaemon.NativeArtifactV2{Executable: path, Digest: attachedworkerdaemon.ExecutableDigest(sha256.Sum256(body))}
	}
	workload, bootstrap, relay, companion := artifact("native-workload"), artifact("native-bootstrap"), artifact("native-relay"), artifact("native-companion")
	backend := codexexec.Config{Enabled: true, Executable: workload.Executable, ExecutableDigest: workload.Digest, ExecutableVersion: "0.162.1", Model: "native-transport-model"}
	p := codexexec.NativeProfileV2{Backend: backend, OperatingSystem: "linux", Architecture: "amd64", DeclaredContextWindow: 128000, OutputReserveTokens: 8192, StreamIdleTimeoutMS: 60000,
		Platform: attachedworkerlocal.OCIConfigV1{DockerPath: "/owned/docker", DockerSHA256: strings.Repeat("d", 64), CLIConfigDir: "/owned/docker-config", Host: "unix:///run/user/1000/docker.sock", EngineID: "native-transport-engine", InstallationID: "native-transport-installation", Boundary: attachedworkerlocal.BoundaryLinuxRootlessNativeV3, Image: "registry.example/native@sha256:" + strings.Repeat("b", 64), UserID: 0, GroupID: 0, DiskBytes: 32 << 20, CredentialFileBytes: 1 << 20, MemoryBytes: 128 << 20, PIDsLimit: 32, StopSeconds: 5},
		Launch:   attachedworkerdaemon.NativeLaunchV3{Version: 3, Workload: workload, Bootstrap: bootstrap, Relay: relay, Companion: companion, SocketDestination: "/conduit/native.sock", ProviderHosts: []string{"auth.openai.com", "chatgpt.com"}, Limits: attachedworkerdaemon.NativeConduitLimitsV2{Connections: 2, InboundBytes: 1 << 20, OutboundBytes: 1 << 20, Active: time.Minute, Cleanup: 10 * time.Second, CompanionMemoryBytes: 64 << 20, CompanionGoroutines: 16}}}
	descriptor, err := p.DescriptorV2()
	if err != nil {
		t.Fatal(err)
	}
	arguments, err := p.ArgumentsV2()
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.ProviderEvidenceScopeV1{TenantID: "native-transport-tenant", OwnerUserID: "native-transport-owner", Backend: descriptor, Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "native-transport-subscription", OwnerUserID: "native-transport-owner", Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}}
	observed := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	expires := observed.Add(time.Hour)
	b := domain.ProviderEvidenceBundleV1{
		Catalog:    domain.ProviderCatalogObservationV1{Version: 1, Scope: scope, CatalogRevision: "fixture-catalog", Models: []domain.ProviderCatalogModelV1{{ModelVendorID: "openai", ModelID: backend.Model, CatalogDigest: strings.Repeat("a", 64)}}, ObservedAt: observed, ExpiresAt: expires},
		Route:      domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, FallbackPolicy: domain.ProviderFallbackDenyV1, ObservedAt: observed, ExpiresAt: expires},
		Capability: domain.ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "openai", ModelID: backend.Model, MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, ToolCalling: domain.ProviderEvidenceUnsupportedV1, StructuredOutput: domain.ProviderEvidenceUnknownV1, ObservedAt: observed, ExpiresAt: expires},
		Privacy:    domain.ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "openai", ModelID: backend.Model, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1}, TrainingUse: domain.ProviderEvidenceUnknownV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, PolicyRevision: "fixture-privacy", ObservedAt: observed, ExpiresAt: expires},
		Price:      domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, ModelVendorID: "openai", ModelID: backend.Model, ObservedAt: observed, ExpiresAt: expires},
		Policy:     domain.ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: codexexec.NativeEnvelopePolicyIDV2, Revision: 1, DecisionOwner: "sessionless-policy", EvidenceSource: "reviewed-native-envelope-v2", Verdict: domain.ProviderPolicyConditionalV1, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1}, CapabilityEvidenceDigest: strings.Repeat("a", 64), PrivacyEvidenceDigest: strings.Repeat("b", 64), PriceObservationDigest: strings.Repeat("c", 64), RoutePolicyDigest: strings.Repeat("d", 64), ObservedAt: observed, ExpiresAt: expires}}
	native := &codexexec.NativePreparedConfigV2{Profile: p, Evidence: b, CredentialBindings: nativeRuntimeMetadataV3{}}
	if _, err := native.Clone(); err != nil {
		t.Fatalf("native transport structural metadata: %v", err)
	}
	f.config.Profile.Arguments, f.config.Profile.Environment = slices.Clone(arguments), nil
	f.config.Profile.Executable, f.config.Profile.ExecutableDigest = backend.Executable, backend.ExecutableDigest
	port.manifest.Harness.Arguments = slices.Clone(arguments)
	port.manifest.Harness.Executable, port.manifest.Harness.SHA256 = backend.Executable, hex.EncodeToString(backend.ExecutableDigest[:])
	input.CapabilityManifest.HarnessVersion = backend.ExecutableVersion
	input.CapabilityManifest.HarnessExecutableDigest = slices.Clone(backend.ExecutableDigest[:])
	nativeRuntimeResealV3(t, f, &input)
	return f, port, input, attachedworkerstack.Config{PreparedCodexNative: native}
}

func nativeRuntimeResealV3(t *testing.T, f *adapterFixture, input *attachedworkersession.ConnectInputV1) {
	t.Helper()
	digest, err := attachedworkerprotocol.ManifestDigestV1(input.CapabilityManifest)
	if err != nil {
		t.Fatal(err)
	}
	f.config.Profile.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(digest))
}

type nativeRuntimeMetadataV3 struct{}

func (nativeRuntimeMetadataV3) LoadCredentialBinding(context.Context, domain.TenantID, domain.SubscriptionConnectionID) (domain.CredentialBinding, bool, error) {
	return domain.CredentialBinding{}, false, ErrInvalidConfiguration
}

func TestNativeRuntimeV3FreezesCompleteSelectionBeforeConnection(t *testing.T) {
	f, port, input, config := nativeRuntimeFixtureV3(t)
	original := config.PreparedCodexNative
	var selected *codexexec.NativePreparedConfigV2
	preflight, runner, err := pinnedRuntimePreflight(input.CapabilityManifest, f.config.Profile, config, unusedInitialCredentials{},
		func(_ context.Context, _ attachedworkerlocal.ManifestV1, c attachedworkerstack.Config, _ ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
			selected = c.PreparedCodexNative
			return &runtimeRunner{}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	original.Profile.Backend.Model = "changed-model"
	original.Profile.Launch.ProviderHosts[0] = "changed.example"
	original.Profile.Platform.EngineID = "changed-engine"
	original.Evidence.Catalog.Models[0].ModelID = "changed-catalog"
	original.Evidence.Privacy.AllowedDataClasses[0] = domain.ProviderDataPublicV1
	original.Evidence.Policy.AllowedDataClasses[0] = domain.ProviderDataPublicV1
	if runner.ready() {
		t.Fatal("runner acquired authority before lease-held preflight")
	}
	if err := preflight(t.Context(), port.manifest); err != nil {
		t.Fatal(err)
	}
	if selected == nil || selected == original || selected.Profile.Backend.Model != "native-transport-model" || selected.Profile.Launch.ProviderHosts[0] != "auth.openai.com" || selected.Profile.Platform.EngineID != "native-transport-engine" || selected.Evidence.Catalog.Models[0].ModelID != "native-transport-model" || selected.Evidence.Privacy.AllowedDataClasses[0] != domain.ProviderDataPrivateV1 || selected.Evidence.Policy.AllowedDataClasses[0] != domain.ProviderDataPrivateV1 {
		t.Fatal("caller mutation changed frozen native selection or evidence")
	}
}

func TestNativeRuntimeV3RejectsChangedCapabilityAndInvocationProfile(t *testing.T) {
	for _, mode := range []string{"os", "architecture", "version", "argv", "environment", "mixed-v1", "mixed-media", "disabled", "metadata"} {
		t.Run(mode, func(t *testing.T) {
			f, _, input, config := nativeRuntimeFixtureV3(t)
			switch mode {
			case "os":
				input.CapabilityManifest.OperatingSystem = "darwin"
			case "architecture":
				input.CapabilityManifest.Architecture = "arm64"
			case "version":
				input.CapabilityManifest.HarnessVersion = "0.162.2"
			case "argv":
				f.config.Profile.Arguments[0] = "changed"
			case "environment":
				f.config.Profile.Environment = []attachedworkerdaemon.EnvironmentVariable{{Name: "CODEX_HOME", Value: "/caller/auth"}}
				config.AllowedEnvironmentNames = []string{"CODEX_HOME"}
			case "mixed-v1":
				config.PreparedCodex = &codexexec.Config{}
			case "mixed-media":
				config.PreparedCodexMedia = &codexexec.MediaConfigV1{}
			case "disabled":
				config.PreparedCodexNative.Profile.Backend.Enabled = false
			case "metadata":
				config.PreparedCodexNative.CredentialBindings = nil
			}
			nativeRuntimeResealV3(t, f, &input)
			calls := 0
			preflight, runner, err := pinnedRuntimePreflight(input.CapabilityManifest, f.config.Profile, config, unusedInitialCredentials{},
				func(context.Context, attachedworkerlocal.ManifestV1, attachedworkerstack.Config, ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
					calls++
					return &runtimeRunner{}, nil
				})
			if err == nil || preflight != nil || runner != nil || calls != 0 {
				t.Fatalf("native %s acquired preflight: err=%v stackCalls=%d", mode, err, calls)
			}
		})
	}
}

func TestNativeRuntimeV3RequiresOriginalReceiptBridgeBeforeConnectOrReconnect(t *testing.T) {
	for _, reconnect := range []bool{false, true} {
		for _, mode := range []string{"feature", "publisher", "caller-builder"} {
			t.Run(mode+map[bool]string{false: "/initial", true: "/reconnect"}[reconnect], func(t *testing.T) {
				f, _, input, config := nativeRuntimeFixtureV3(t)
				input.CapabilityManifest.Features = []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureOutputReceipt}
				f.config.ReceiptPublisher = &receiptPublisherFixture{}
				switch mode {
				case "feature":
					input.CapabilityManifest.Features = []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation}
				case "publisher":
					f.config.ReceiptPublisher = nil
				case "caller-builder":
					f.config.ReceiptCandidate = func(attachedworkerdaemon.InvocationIdentity, attachedworkerdaemon.InvocationResult, error, domain.AttachedWorkerTerminalStatus) (attachedworkeroutput.Candidate, error) {
						return attachedworkeroutput.Candidate{}, nil
					}
				}
				nativeRuntimeResealV3(t, f, &input)
				connections, stacks := 0, 0
				factory := func(context.Context, attachedworkerlocal.ManifestV1, attachedworkerstack.Config, ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
					stacks++
					return &runtimeRunner{}, nil
				}
				var runtime *ForegroundRuntime
				var err error
				if reconnect {
					runtime, err = reconnectPinnedForegroundRuntime(t.Context(), nil, nil, nil, attachedworkersession.Config{}, attachedworkersession.ReconnectInputV1{CapabilityManifest: input.CapabilityManifest}, nil, f.config, cadenceConfig(true), config, unusedInitialCredentials{}, initialRuntimeConfig(),
						func(*attachedworkerlocal.Store, attachedworkersession.BootstrapPort, attachedworkersession.ExchangeFactory, attachedworkersession.Config) (reconnectPort, error) {
							connections++
							return nil, ErrInvalidConfiguration
						}, factory)
				} else {
					runtime, err = connectPinnedForegroundRuntime(t.Context(), nil, nil, nil, attachedworkersession.Config{}, input, nil, f.config, cadenceConfig(true), config, unusedInitialCredentials{}, initialRuntimeConfig(),
						func(*attachedworkerlocal.Store, attachedworkersession.BootstrapPort, attachedworkersession.ExchangeFactory, attachedworkersession.Config) (initialConnectPort, error) {
							connections++
							return nil, ErrInvalidConfiguration
						}, factory)
				}
				if err == nil || runtime != nil || connections != 0 || stacks != 0 {
					t.Fatalf("native receipt %s reconnected=%t bypassed before connection: err=%v connector=%d stack=%d", mode, reconnect, err, connections, stacks)
				}
			})
		}
	}
}
