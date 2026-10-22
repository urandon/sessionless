//go:build darwin || linux

package attachedworkerstack

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// This fixture has inert files, a fake launcher and protocol-only local peer.
// It never executes an engine/native/helper or acquires real credentials.
func nativeStackFixtureV3(t *testing.T) (*stackFixture, nativeDependenciesV3, *nativeStackLauncherV3) {
	t.Helper()
	f := newFixture(t)
	f.manifest.ConnectionGeneration = 0
	f.manifest.OCI.Boundary = attachedworkerlocal.BoundaryLinuxRootlessNativeV3
	f.manifest.OCI.UserID, f.manifest.OCI.GroupID = 0, 0
	f.manifest.OCI.DiskBytes, f.manifest.OCI.PIDsLimit, f.manifest.OCI.StopSeconds = 32<<20, 32, 5
	now := f.manifest.CreatedAt
	artifact := func(name string) attachedworkerdaemon.NativeArtifactV2 {
		t.Helper()
		body := []byte("inert native stack fixture " + name)
		path := filepath.Join(filepath.Dir(f.scratch), name)
		if err := os.WriteFile(path, body, 0o500); err != nil {
			t.Fatal(err)
		}
		return attachedworkerdaemon.NativeArtifactV2{Executable: path, Digest: attachedworkerdaemon.ExecutableDigest(sha256.Sum256(body))}
	}
	p := codexexec.NativeProfileV2{Backend: codexexec.Config{Enabled: true, Executable: f.manifest.Harness.Executable, ExecutableDigest: f.harnessDigest, ExecutableVersion: "0.162.1", Model: "native-stack-model", Now: func() time.Time { return now }},
		Platform: f.manifest.OCI, OperatingSystem: "linux", Architecture: "amd64", DeclaredContextWindow: 128000, OutputReserveTokens: 8192, StreamIdleTimeoutMS: 60000,
		Launch: attachedworkerdaemon.NativeLaunchV3{Version: 3, Bootstrap: artifact("bootstrap-native-stack"), Relay: artifact("relay-native-stack"), Companion: artifact("companion-native-stack"),
			Workload: attachedworkerdaemon.NativeArtifactV2{Executable: f.manifest.Harness.Executable, Digest: f.harnessDigest}, SocketDestination: "/conduit/native.sock",
			ProviderHosts: []string{"auth.openai.com", "chatgpt.com"}, Limits: attachedworkerdaemon.NativeConduitLimitsV2{Connections: 2, InboundBytes: 1 << 20, OutboundBytes: 1 << 20, Active: time.Minute, Cleanup: 10 * time.Second, CompanionMemoryBytes: 64 << 20, CompanionGoroutines: 16}}}
	args, err := p.ArgumentsV2()
	if err != nil {
		t.Fatalf("derive native fixture argv: %v", err)
	}
	f.manifest.Harness.Arguments = args
	descriptor, err := p.DescriptorV2()
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.ProviderEvidenceScopeV1{TenantID: f.manifest.TenantID, OwnerUserID: f.manifest.OwnerUserID, Backend: descriptor,
		Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "native-stack-subscription", OwnerUserID: f.manifest.OwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}}
	observed, expires := now.Add(-time.Minute), now.Add(time.Hour)
	bundle := domain.ProviderEvidenceBundleV1{
		Catalog:    domain.ProviderCatalogObservationV1{Version: 1, Scope: scope, CatalogRevision: "native-stack-catalog", Models: []domain.ProviderCatalogModelV1{{ModelVendorID: "openai", ModelID: p.Backend.Model, CatalogDigest: strings.Repeat("a", 64)}}, ObservedAt: observed, ExpiresAt: expires},
		Route:      domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, FallbackPolicy: domain.ProviderFallbackDenyV1, ObservedAt: observed, ExpiresAt: expires},
		Capability: domain.ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "openai", ModelID: p.Backend.Model, MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, ToolCalling: domain.ProviderEvidenceUnsupportedV1, StructuredOutput: domain.ProviderEvidenceUnknownV1, ObservedAt: observed, ExpiresAt: expires},
		Privacy:    domain.ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "openai", ModelID: p.Backend.Model, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1}, TrainingUse: domain.ProviderEvidenceUnknownV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, PolicyRevision: "native-stack-privacy", ObservedAt: observed, ExpiresAt: expires},
		Price:      domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, ModelVendorID: "openai", ModelID: p.Backend.Model, ObservedAt: observed, ExpiresAt: expires},
		Policy:     domain.ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: codexexec.NativeEnvelopePolicyIDV2, Revision: codexexec.NativeEnvelopePolicyRevisionV2, DecisionOwner: "sessionless-policy", EvidenceSource: "reviewed-native-envelope-v2", Verdict: domain.ProviderPolicyConditionalV1, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1}, ObservedAt: observed, ExpiresAt: expires}}
	for _, item := range []struct {
		digest func() (domain.ProviderEvidenceDigestV1, error)
		field  *string
	}{{bundle.Route.Digest, &bundle.Policy.RoutePolicyDigest}, {bundle.Capability.Digest, &bundle.Policy.CapabilityEvidenceDigest}, {bundle.Privacy.Digest, &bundle.Policy.PrivacyEvidenceDigest}, {bundle.Price.Digest, &bundle.Policy.PriceObservationDigest}} {
		digest, err := item.digest()
		if err != nil {
			t.Fatal(err)
		}
		*item.field = string(digest)
	}
	f.config.PreparedCodexNative = &codexexec.NativePreparedConfigV2{Profile: p, Evidence: bundle, CredentialBindings: nativeStackMetadataV3{}}
	f.config.Timeout, f.config.TerminationGrace = 30*time.Second, time.Second
	if err := f.manifest.Validate(); err != nil {
		t.Fatalf("native original manifest: %v", err)
	}
	if _, err := f.config.PreparedCodexNative.Clone(); err != nil {
		t.Fatalf("native metadata fixture: %v", err)
	}
	launcher := &nativeStackLauncherV3{}
	deps := nativeDependenciesV3{hostOS: "linux", hostArchitecture: "amd64", digest: attachedworkerdaemon.DigestExecutable,
		newLauncher: func(context.Context, attachedworkeroci.Config) (nativeReconcilingLauncherV3, error) {
			launcher.constructors++
			return launcher, nil
		}, newSupervisor: attachedworkerdaemon.NewSupervisorV3}
	return f, deps, launcher
}

type nativeStackMetadataV3 struct{}

func (nativeStackMetadataV3) LoadCredentialBinding(context.Context, domain.TenantID, domain.SubscriptionConnectionID) (domain.CredentialBinding, bool, error) {
	return domain.CredentialBinding{}, false, errors.New("metadata not used during construction")
}

type nativeStackLauncherV3 struct{ constructors, reconciles, prepares int }

func (*nativeStackLauncherV3) ProfileV3() attachedworkerdaemon.IsolationProfileV3 {
	return attachedworkerdaemon.IsolationProfileV3{Version: 3, Name: "native-stack-inert-fixture-v3", FilesystemReadBoundary: true, FilesystemWriteBoundary: true, ProcessBoundary: true, DiskBytesBounded: true, Network: attachedworkerdaemon.NativeConnectUnixV2}
}
func (l *nativeStackLauncherV3) Reconcile(context.Context) (int, error) {
	l.reconciles++
	return 2, nil
}
func (l *nativeStackLauncherV3) PrepareV3(context.Context, attachedworkerdaemon.LaunchEnvelopeV3) (attachedworkerdaemon.IsolationBoundaryV3, error) {
	l.prepares++
	return nil, errors.New("fixture must never execute")
}

func TestNativeStackV3ExplicitCompositionPinsOriginalPlatform(t *testing.T) {
	f, deps, launcher := nativeStackFixtureV3(t)
	want := f.manifest.OCI
	deps.newLauncher = func(_ context.Context, config attachedworkeroci.Config) (nativeReconcilingLauncherV3, error) {
		launcher.constructors++
		if config.Boundary != attachedworkeroci.BoundaryLinuxRootless || config.UserID != 0 || config.GroupID != 0 || config.DockerPath != want.DockerPath || config.Image != want.Image {
			t.Fatalf("wrong explicit lowered native platform: %+v", config)
		}
		return launcher, nil
	}
	stack, err := newNativeStackV3(t.Context(), f.manifest, f.config, f.credentials, deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stack.Close(t.Context()); err != nil {
			t.Error(err)
		}
	})
	if f.manifest.OCI != want || stack.evidence.Boundary != attachedworkerlocal.BoundaryLinuxRootlessNativeV3 || !stack.nativePrepared || stack.prepared == nil || launcher.constructors != 1 || launcher.reconciles != 1 || launcher.prepares != 0 {
		t.Fatalf("native composition drifted: evidence=%+v constructors=%d reconcile=%d prepare=%d", stack.evidence, launcher.constructors, launcher.reconciles, launcher.prepares)
	}
	if _, err := stack.Run(t.Context(), attachedworkerdaemon.Invocation{}); !errors.Is(err, ErrReconciliationRequired) || launcher.prepares != 0 {
		t.Fatalf("unbound native stack acquired process authority: %v", err)
	}
	pinned := stack.runner.(*pinnedRunner)
	if !slices.Equal(pinned.arguments, f.manifest.Harness.Arguments) || pinned.delegate != stack.prepared {
		t.Fatal("native path bypassed original pinned runner")
	}
}

func TestNativeStackV3RejectsDriftBeforeEngine(t *testing.T) {
	cases := []struct {
		name   string
		change func(*stackFixture, *nativeDependenciesV3)
	}{
		{"missing-native", func(f *stackFixture, _ *nativeDependenciesV3) { f.config.PreparedCodexNative = nil }},
		{"mixed-v1", func(f *stackFixture, _ *nativeDependenciesV3) { f.config.PreparedCodex = &codexexec.Config{} }},
		{"mixed-media", func(f *stackFixture, _ *nativeDependenciesV3) {
			f.config.PreparedCodexMedia = &codexexec.MediaConfigV1{}
		}},
		{"disabled", func(f *stackFixture, _ *nativeDependenciesV3) {
			f.config.PreparedCodexNative.Profile.Backend.Enabled = false
		}},
		{"platform-image", func(f *stackFixture, _ *nativeDependenciesV3) {
			f.manifest.OCI.Image = "registry.example/other@sha256:" + strings.Repeat("c", 64)
		}},
		{"platform-memory", func(f *stackFixture, _ *nativeDependenciesV3) { f.manifest.OCI.MemoryBytes++ }},
		{"platform-engine", func(f *stackFixture, _ *nativeDependenciesV3) { f.manifest.OCI.EngineID = "other-engine-001" }},
		{"old-platform", func(f *stackFixture, _ *nativeDependenciesV3) {
			f.manifest.OCI.Boundary = attachedworkerlocal.BoundaryLinuxRootless
			f.manifest.OCI.UserID, f.manifest.OCI.GroupID = 1000, 1000
		}},
		{"argv", func(f *stackFixture, _ *nativeDependenciesV3) { f.manifest.Harness.Arguments[0] = "changed" }},
		{"model", func(f *stackFixture, _ *nativeDependenciesV3) {
			f.config.PreparedCodexNative.Profile.Backend.Model = "other-model"
		}},
		{"foreign-host-os", func(_ *stackFixture, d *nativeDependenciesV3) { d.hostOS = "darwin" }},
		{"foreign-host-architecture", func(_ *stackFixture, d *nativeDependenciesV3) { d.hostArchitecture = "arm64" }},
		{"timeout-unbounded", func(f *stackFixture, _ *nativeDependenciesV3) { f.config.Timeout = 0 }},
		{"timeout-widened", func(f *stackFixture, _ *nativeDependenciesV3) { f.config.Timeout = 2 * time.Minute }},
		{"cleanup-widened", func(f *stackFixture, _ *nativeDependenciesV3) { f.config.TerminationGrace = 11 * time.Second }},
		{"stdout-widened", func(f *stackFixture, _ *nativeDependenciesV3) { f.config.MaxStdoutBytes = (16 << 20) + 1 }},
		{"stderr-widened", func(f *stackFixture, _ *nativeDependenciesV3) { f.config.MaxStderrBytes = (1 << 20) + 1 }},
		{"stdout-negative", func(f *stackFixture, _ *nativeDependenciesV3) { f.config.MaxStdoutBytes = -1 }},
		{"stderr-negative", func(f *stackFixture, _ *nativeDependenciesV3) { f.config.MaxStderrBytes = -1 }},
		{"ambient-environment", func(f *stackFixture, _ *nativeDependenciesV3) {
			f.config.AllowedEnvironmentNames = []string{"CODEX_HOME", "OPENAI_API_KEY"}
		}},
		{"engine-digest", func(f *stackFixture, _ *nativeDependenciesV3) {
			f.manifest.OCI.DockerSHA256 = strings.Repeat("0", 64)
			f.config.PreparedCodexNative.Profile.Platform = f.manifest.OCI
		}},
		{"harness-digest", func(f *stackFixture, _ *nativeDependenciesV3) { f.manifest.Harness.SHA256 = strings.Repeat("0", 64) }},
		{"bootstrap-digest", func(f *stackFixture, _ *nativeDependenciesV3) {
			f.config.PreparedCodexNative.Profile.Launch.Bootstrap.Digest[0]++
		}},
		{"relay-digest", func(f *stackFixture, _ *nativeDependenciesV3) {
			f.config.PreparedCodexNative.Profile.Launch.Relay.Digest[0]++
		}},
		{"companion-digest", func(f *stackFixture, _ *nativeDependenciesV3) {
			f.config.PreparedCodexNative.Profile.Launch.Companion.Digest[0]++
		}},
		{"metadata-reader", func(f *stackFixture, _ *nativeDependenciesV3) { f.config.PreparedCodexNative.CredentialBindings = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, deps, launcher := nativeStackFixtureV3(t)
			tc.change(f, &deps)
			if stack, err := newNativeStackV3(t.Context(), f.manifest, f.config, f.credentials, deps); err == nil || stack != nil || launcher.constructors != 0 || launcher.reconciles != 0 || launcher.prepares != 0 {
				t.Fatalf("%s accepted/caused engine access: err=%v stack=%v constructors=%d reconcile=%d prepare=%d", tc.name, err, stack, launcher.constructors, launcher.reconciles, launcher.prepares)
			}
		})
	}
}

func TestNativeStackV3OrdinaryConstructorRejectsSelectionAndBoundary(t *testing.T) {
	for _, mode := range []string{"selection", "boundary"} {
		t.Run(mode, func(t *testing.T) {
			f, _, _ := nativeStackFixtureV3(t)
			if mode == "selection" {
				f.manifest.OCI.Boundary = attachedworkerlocal.BoundaryLinuxRootless
				f.manifest.OCI.UserID, f.manifest.OCI.GroupID = 1000, 1000
			} else {
				f.config.PreparedCodexNative = nil
			}
			called := false
			if stack, err := newStack(t.Context(), f.manifest, f.config, f.credentials, f.dependencies(&called)); err == nil || stack != nil || called {
				t.Fatalf("ordinary constructor accepted native %s: stack=%v err=%v engine=%t", mode, stack, err, called)
			}
		})
	}
}

func TestNativeStackV3BindsActualSessionOnce(t *testing.T) {
	f, deps, launcher := nativeStackFixtureV3(t)
	stack, err := newNativeStackV3(t.Context(), f.manifest, f.config, f.credentials, deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stack.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	connector, _, peer, capability := newPreparedConnectorFixture(t, f.manifest, f.manifest.CreatedAt, nil)
	capability.OperatingSystem, capability.Architecture, capability.HarnessVersion = "linux", "amd64", "0.162.1"
	session, err := connector.Connect(t.Context(), attachedworkersession.ConnectInputV1{ExpectedWorkerRevision: 7, CapabilityManifest: capability})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	exchangesBeforeBind := peer.exchanges
	if err := stack.BindPreparedConnection(&attachedworkersession.Session{}); err == nil {
		t.Fatal("empty Session bound")
	}
	if err := stack.BindPreparedConnection(session); err != nil {
		t.Fatal(err)
	}
	if stack.connection != session || stack.boundID != session.Snapshot().ConnectionID {
		t.Fatal("Stack did not retain original connected owner")
	}
	// Rebinding the concrete prepared owner would succeed if Stack merely kept
	// a matching snapshot instead of forwarding this original Session.
	if err := stack.prepared.BindNativeSessionV2(session); err == nil {
		t.Fatal("native prepared owner never received actual Session")
	}
	if err := stack.BindPreparedConnection(session); err == nil {
		t.Fatal("second Stack bind accepted")
	}
	if launcher.prepares != 0 || peer.exchanges != exchangesBeforeBind {
		t.Fatalf("binding dispatched process/exchange: prepare=%d exchange=%d", launcher.prepares, peer.exchanges)
	}
}
