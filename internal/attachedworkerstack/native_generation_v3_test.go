//go:build darwin || linux

package attachedworkerstack

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegenerationsession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ownerlocalcredential"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

// All executables, credential bytes, keys and HTTP responses here are synthetic.
// The original Connector and concrete local lifecycle run, but no engine,
// helper, model, provider, socket or HTTP server is started.
type generationStackFixtureV3 struct {
	base       *stackFixture
	deps       nativeDependenciesV3
	launcher   *nativeStackLauncherV3
	clock      *testkit.FakeClock
	lifecycle  *ownerlocalcredential.Lifecycle
	owner      *attachedresourcegenerationsession.OwnerV1
	worker     *attachedworkerlocal.Store
	peer       *preparedSessionPeer
	capability attachedworkerprotocol.CapabilityManifestV1
	metadata   *generationStackHTTPV3
	scratch    []string
}

func newGenerationStackFixtureV3(t *testing.T) *generationStackFixtureV3 {
	t.Helper()
	f, deps, launcher := nativeStackFixtureV3(t)
	clock := testkit.NewFakeClock(f.manifest.CreatedAt)
	old := f.config.PreparedCodexNative
	p := old.Profile
	profile := codexexec.NativeProfileV3{Backend: p.Backend, Launch: p.Launch, Platform: p.Platform,
		OperatingSystem: p.OperatingSystem, Architecture: p.Architecture, DeclaredContextWindow: p.DeclaredContextWindow,
		OutputReserveTokens: p.OutputReserveTokens, StreamIdleTimeoutMS: p.StreamIdleTimeoutMS}
	profile.Backend.Now = clock.Now
	arguments, err := profile.ArgumentsV3()
	if err != nil {
		t.Fatal("V3 arguments", err)
	}
	f.manifest.Harness.Arguments = arguments
	_, worker, peer, capability := newPreparedConnectorFixture(t, f.manifest, clock.Now(), nil)
	capability.OperatingSystem, capability.Architecture, capability.HarnessVersion = "linux", "amd64", profile.Backend.ExecutableVersion
	capDigest, err := attachedworkerprotocol.ManifestDigestV1(capability)
	if err != nil {
		t.Fatal("original capability", err)
	}
	descriptor, err := profile.DescriptorV3()
	if err != nil {
		t.Fatal("V3 descriptor", err)
	}
	bundle := old.Evidence
	for _, scope := range []*domain.ProviderEvidenceScopeV1{&bundle.Catalog.Scope, &bundle.Route.Scope, &bundle.Capability.Scope,
		&bundle.Privacy.Scope, &bundle.Price.Scope, &bundle.Policy.Scope} {
		scope.Backend = descriptor
	}
	bundle.Policy.PolicyID, bundle.Policy.Revision, bundle.Policy.EvidenceSource = codexexec.NativeEnvelopePolicyIDV3, codexexec.NativeEnvelopePolicyRevisionV3, "reviewed-native-envelope-v3"
	expires := bundle.Policy.ExpiresAt
	binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: f.manifest.TenantID,
		OwnerUserID: f.manifest.OwnerUserID, Backend: descriptor, Resource: bundle.Policy.Scope.Resource,
		ModelVendorID: "openai", ModelID: profile.Backend.Model, InputDataClass: domain.ProviderDataPrivateV1, EvidenceExpiresAt: &expires}
	baseline := domain.AttachedResourceGenerationBaselineV1{Version: 1, TenantID: f.manifest.TenantID, OwnerUserID: f.manifest.OwnerUserID,
		SubscriptionConnectionID: domain.SubscriptionConnectionID(binding.Resource.ResourceID), WorkerID: f.manifest.WorkerID,
		EnrollmentGeneration: f.manifest.EnrollmentGeneration, BaselineRevision: binding.Resource.Revision,
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capDigest)), GenerationOnlyDelegation: true, Binding: binding, Evidence: bundle}
	rebindGenerationStackBaselineV3(t, &baseline)
	parent := filepath.Dir(f.scratch)
	credentialRoot := filepath.Join(parent, "generation-credentials")
	if err := os.Mkdir(credentialRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := ownerlocalcredential.New(t.Context(), ownerlocalcredential.Config{Root: credentialRoot,
		Scope:    ownerlocalcredential.Scope{TenantID: baseline.TenantID, OwnerUserID: baseline.OwnerUserID, SubscriptionConnectionID: baseline.SubscriptionConnectionID},
		KeyEpoch: "synthetic-stack-v3"}, bytes.Repeat([]byte{0x37}, 32))
	if err != nil {
		t.Fatal("synthetic owner store", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := store.Initialize(t.Context(), ownerlocalcredential.Registration{Provider: "openai", AuthMode: "chatgpt", Entitlement: domain.EntitlementActive, At: clock.Now()}, []byte("synthetic-stack-original-credential")); err != nil {
		t.Fatal("initialize synthetic owner", err)
	}
	lifecycle, err := ownerlocalcredential.NewLifecycle(store, credentiallifecycle.Config{ScratchRoot: f.scratch, Clock: clock,
		IDs: testkit.NewSequenceIDGenerator("generation-stack-"), MaxAuthBytes: 1024})
	if err != nil {
		t.Fatal("original lifecycle", err)
	}
	t.Cleanup(func() {
		if err := lifecycle.Close(); err != nil {
			t.Error(err)
		}
	})
	metadata := &generationStackHTTPV3{}
	source := &generationStackSourceV3{preparedSessionPeer: peer}
	owner, err := attachedresourcegenerationsession.NewV1(f.manifest.ControlPlaneOrigin, &http.Client{Transport: metadata}, source, lifecycle, baseline)
	if err != nil {
		t.Fatal("original generation owner", err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	f.config.PreparedCodexNative = nil
	f.config.PreparedCodexNativeGeneration = &codexexec.NativePreparedConfigV3{Profile: profile, Baseline: baseline, GenerationOwner: owner, Lifecycle: lifecycle}
	return &generationStackFixtureV3{base: f, deps: deps, launcher: launcher, clock: clock, lifecycle: lifecycle,
		owner: owner, worker: worker, peer: peer, capability: capability, metadata: metadata, scratch: generationStackScratchV3(t, f.scratch)}
}

func generationStackScratchV3(t *testing.T, root string) []string {
	t.Helper()
	var entries []string
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		entries = append(entries, name)
		return nil
	}); err != nil {
		t.Fatal("scratch inventory", err)
	}
	return entries
}

func rebindGenerationStackBaselineV3(t *testing.T, baseline *domain.AttachedResourceGenerationBaselineV1) {
	t.Helper()
	bundle, binding := &baseline.Evidence, &baseline.Binding
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
			t.Fatal("baseline digest", err)
		}
		for _, field := range item.fields {
			*field = string(digest)
		}
	}
	if err := baseline.Validate(); err != nil {
		t.Fatal("complete V3 baseline", err)
	}
}

func (f *generationStackFixtureV3) stack(t *testing.T) *Stack {
	t.Helper()
	stack, err := newNativeGenerationStackV3(t.Context(), f.base.manifest, f.base.config, f.lifecycle, f.deps)
	if err != nil {
		t.Fatal("generation stack", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := stack.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return stack
}

func (f *generationStackFixtureV3) connect(t *testing.T) *attachedworkersession.Session {
	t.Helper()
	connector, err := attachedworkersession.New(f.worker, f.peer, f.owner, attachedworkersession.Config{
		Audience: "sessionless:attached-worker:v1", WorkerOffer: f.capability.ProtocolOffer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1},
		OperationTimeout: time.Second, Now: f.clock.Now, Random: bytes.NewReader(bytes.Repeat([]byte{0x41}, 64)),
	})
	if err != nil {
		t.Fatal("original Connector", err)
	}
	session, err := connector.Connect(t.Context(), attachedworkersession.ConnectInputV1{ExpectedWorkerRevision: 7, CapabilityManifest: f.capability})
	if err != nil {
		t.Fatal("genuine initial Ready", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return session
}

type generationStackSourceV3 struct{ *preparedSessionPeer }

func (*generationStackSourceV3) RetireConnectionSourcesV1() error { return nil }

type generationStackHTTPV3 struct {
	mu               sync.Mutex
	publishes, reads int
	head             *domain.AttachedResourceGenerationHeadV1
	mismatch         bool
	hook             func(*http.Request) error
}

func (rt *generationStackHTTPV3) counts() (int, int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.publishes, rt.reads
}
func (rt *generationStackHTTPV3) RoundTrip(req *http.Request) (*http.Response, error) {
	defer req.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(req.Body, domain.AttachedResourceGenerationMetadataMaxBytesV1+1))
	if err != nil || len(raw) > domain.AttachedResourceGenerationMetadataMaxBytesV1 || req.Method != http.MethodPost || req.URL.RawQuery != "" || req.Header.Get("Authorization") == "" {
		return nil, errors.New("synthetic metadata request is not bounded/exact")
	}
	rt.mu.Lock()
	var head domain.AttachedResourceGenerationHeadV1
	switch req.URL.Path {
	case attachedworkerhttp.ResourceGenerationPublishPathV1:
		var input attachedworkerhttp.ResourceGenerationPublishRequestV1
		if json.Unmarshal(raw, &input) != nil || input.Version != 1 || input.Target.Validate() != nil ||
			domain.ValidateAttachedResourceGenerationTransitionV1(rt.head, input.ExpectedCredentialGeneration, input.Target) != nil {
			rt.mu.Unlock()
			return nil, errors.New("synthetic metadata publication is not canonical")
		}
		rt.publishes++
		head = input.Target
		copy := head
		rt.head = &copy
	case attachedworkerhttp.ResourceGenerationReadPathV1:
		var input attachedworkerhttp.ResourceGenerationReadRequestV1
		if json.Unmarshal(raw, &input) != nil || input.Version != 1 || rt.head == nil {
			rt.mu.Unlock()
			return nil, errors.New("synthetic metadata read has no exact head")
		}
		rt.reads++
		head = *rt.head
	default:
		rt.mu.Unlock()
		return nil, errors.New("unexpected metadata path")
	}
	hook, mismatch := rt.hook, rt.mismatch
	rt.mu.Unlock()
	if hook != nil {
		if err := hook(req); err != nil {
			return nil, err
		}
	}
	if mismatch {
		head.CredentialGeneration++
	}
	body, err := json.Marshal(attachedworkerhttp.ResourceGenerationResponseV1{Version: 1, Status: ports.AttachedWorkerExecutionApplied, Head: head})
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(body)), Request: req}, nil
}

func TestNativeGenerationStackV3PinsClosedCompositionAndImmutableInput(t *testing.T) {
	f := newGenerationStackFixtureV3(t)
	before := f.base.config.PreparedCodexNativeGeneration.Baseline.Clone()
	stack := f.stack(t)
	if stack.nativeGeneration == nil || !stack.nativePrepared || stack.prepared == nil || stack.nativeGeneration.GenerationOwner != f.owner || stack.nativeGeneration.Lifecycle != f.lifecycle ||
		f.launcher.constructors != 1 || f.launcher.reconciles != 1 || f.launcher.prepares != 0 {
		t.Fatalf("closed composition drift: owner=%t lifecycle=%t constructors=%d reconciles=%d prepares=%d", stack.nativeGeneration != nil && stack.nativeGeneration.GenerationOwner == f.owner,
			stack.nativeGeneration != nil && stack.nativeGeneration.Lifecycle == f.lifecycle, f.launcher.constructors, f.launcher.reconciles, f.launcher.prepares)
	}
	pinned := stack.runner.(*pinnedRunner)
	if pinned.delegate != stack.prepared || !slices.Equal(pinned.arguments, f.base.manifest.Harness.Arguments) {
		t.Fatal("replaced original pinned runner")
	}
	f.base.config.PreparedCodexNativeGeneration.Baseline.Evidence.Catalog.Models[0].ModelID = "caller-mutated"
	f.base.config.PreparedCodexNativeGeneration.Profile.Launch.ProviderHosts[0] = "caller.example"
	f.base.config.PreparedCodexNativeGeneration.InlineOutputV2 = true
	if !reflect.DeepEqual(stack.nativeGeneration.Baseline, before) || stack.nativeGeneration.Profile.Launch.ProviderHosts[0] == "caller.example" || stack.inlineOutputV2 {
		t.Fatal("installed input was not cloned")
	}
	if _, err := stack.Run(t.Context(), attachedworkerdaemon.Invocation{}); err == nil {
		t.Fatal("unbound stack acquired invocation authority")
	}
	if p, r := f.metadata.counts(); p != 0 || r != 0 {
		t.Fatalf("constructor performed metadata I/O: publish=%d read=%d", p, r)
	}
	if entries := generationStackScratchV3(t, f.base.scratch); !slices.Equal(entries, f.scratch) {
		t.Fatalf("constructor changed original scratch inventory: got=%v want=%v", entries, f.scratch)
	}
}

func TestNativeGenerationStackV3RejectsSelectionsScopeAndLifecycleBeforeEffects(t *testing.T) {
	for _, mode := range []string{"missing", "mixed-fixed", "mixed-v1", "mixed-media", "tenant", "owner", "worker", "enrollment", "foreign-lifecycle", "nonconcrete-lifecycle", "platform", "argv", "timeout", "environment", "host"} {
		t.Run(mode, func(t *testing.T) {
			f := newGenerationStackFixtureV3(t)
			credentials := ports.CredentialLifecycle(f.lifecycle)
			switch mode {
			case "missing":
				f.base.config.PreparedCodexNativeGeneration = nil
			case "mixed-fixed":
				f.base.config.PreparedCodexNative = &codexexec.NativePreparedConfigV2{}
			case "mixed-v1":
				f.base.config.PreparedCodex = &codexexec.Config{}
			case "mixed-media":
				f.base.config.PreparedCodexMedia = &codexexec.MediaConfigV1{}
			case "tenant":
				f.base.manifest.TenantID = "foreign-tenant"
			case "owner":
				f.base.manifest.OwnerUserID = "foreign-owner"
			case "worker":
				f.base.manifest.WorkerID = "foreign-worker"
			case "enrollment":
				f.base.manifest.EnrollmentGeneration++
			case "foreign-lifecycle":
				credentials = &ownerlocalcredential.Lifecycle{}
			case "nonconcrete-lifecycle":
				credentials = f.base.credentials
			case "platform":
				f.base.manifest.OCI.MemoryBytes++
			case "argv":
				f.base.manifest.Harness.Arguments[0] = "foreign-argv"
			case "timeout":
				f.base.config.Timeout = 2 * time.Minute
			case "environment":
				f.base.config.AllowedEnvironmentNames = []string{"CODEX_HOME", "OPENAI_API_KEY"}
			case "host":
				f.deps.hostArchitecture = "arm64"
			}
			stack, err := newNativeGenerationStackV3(t.Context(), f.base.manifest, f.base.config, credentials, f.deps)
			p, r := f.metadata.counts()
			if err == nil || stack != nil || f.launcher.constructors != 0 || f.launcher.reconciles != 0 || f.launcher.prepares != 0 || p != 0 || r != 0 {
				t.Fatalf("%s caused effects: err=%v stack=%v constructor=%d reconcile=%d prepare=%d publish=%d read=%d", mode, err, stack, f.launcher.constructors, f.launcher.reconciles, f.launcher.prepares, p, r)
			}
		})
	}
	for _, constructor := range []string{"ordinary", "fixed-native"} {
		t.Run(constructor, func(t *testing.T) {
			f := newGenerationStackFixtureV3(t)
			var stack *Stack
			var err error
			if constructor == "ordinary" {
				stack, err = newStack(t.Context(), f.base.manifest, f.base.config, f.lifecycle, f.base.dependencies(nil))
			} else {
				stack, err = newNativeStackV3(t.Context(), f.base.manifest, f.base.config, f.lifecycle, f.deps)
			}
			if err == nil || stack != nil || f.launcher.constructors != 0 {
				t.Fatalf("%s silently selected generation semantics: err=%v", constructor, err)
			}
		})
	}
}

func TestNativeGenerationStackV3BindsAndSynchronizesOnlyOriginalSession(t *testing.T) {
	f := newGenerationStackFixtureV3(t)
	stack, session := f.stack(t), f.connect(t)
	if err := stack.BindPreparedConnection(session); err == nil {
		t.Fatal("contextless legacy bind accepted generation stack")
	}
	if err := stack.BindPreparedGenerationConnectionV3(t.Context(), session, attachedworkersession.ResourceGenerationInitialV1); err != nil {
		t.Fatal("original generation bind", err)
	}
	if stack.connection != session || stack.boundID != session.Snapshot().ConnectionID || !stack.generationBindAttempted {
		t.Fatal("original Session not retained")
	}
	if err := stack.prepared.BindNativeSessionV3(session); err == nil {
		t.Fatal("prepared runner did not receive original Session")
	}
	if err := stack.SynchronizePreparedGenerationV3(t.Context(), session); err != nil {
		t.Fatal("unchanged exact ACK", err)
	}
	if err := stack.BindPreparedGenerationConnectionV3(t.Context(), session, attachedworkersession.ResourceGenerationInitialV1); err == nil {
		t.Fatal("second bind accepted")
	}
	foreign := newGenerationStackFixtureV3(t).connect(t)
	if err := stack.SynchronizePreparedGenerationV3(t.Context(), foreign); err == nil {
		t.Fatal("foreign genuine Session accepted")
	}
	if err := f.owner.CheckNewResourceForSessionV1(t.Context(), session, f.lifecycle, stack.nativeGeneration.Baseline, stack.nativeGeneration.Baseline.Binding.Resource); err != nil {
		t.Fatal("concrete original owner closure", err)
	}
	if p, r := f.metadata.counts(); p != 1 || r != 0 || f.launcher.prepares != 0 {
		t.Fatalf("unexpected publication/read/process effects: %d/%d/%d", p, r, f.launcher.prepares)
	}
	if entries := generationStackScratchV3(t, f.base.scratch); !slices.Equal(entries, f.scratch) {
		t.Fatalf("bind/sync changed original scratch inventory: got=%v want=%v", entries, f.scratch)
	}
}

func TestNativeGenerationStackV3BindAttemptIsOneShot(t *testing.T) {
	f := newGenerationStackFixtureV3(t)
	stack, session := f.stack(t), f.connect(t)
	var group sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		group.Go(func() {
			results <- stack.BindPreparedGenerationConnectionV3(t.Context(), session, attachedworkersession.ResourceGenerationInitialV1)
		})
	}
	group.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		}
	}
	if p, r := f.metadata.counts(); accepted != 1 || p != 1 || r != 0 {
		t.Fatalf("binding owners=%d publish=%d read=%d", accepted, p, r)
	}
}

func TestNativeGenerationStackV3FailedBindCannotRetryOrBindRunner(t *testing.T) {
	for _, mode := range []string{"empty-session", "expired", "wrong-connection-mode", "mismatched-ack"} {
		t.Run(mode, func(t *testing.T) {
			f := newGenerationStackFixtureV3(t)
			stack, actual := f.stack(t), f.connect(t)
			session, connectionMode := actual, attachedworkersession.ResourceGenerationInitialV1
			switch mode {
			case "empty-session":
				session = &attachedworkersession.Session{}
			case "expired":
				f.clock.Advance(2 * time.Hour)
			case "wrong-connection-mode":
				connectionMode = attachedworkersession.ResourceGenerationReconnectV1
			case "mismatched-ack":
				f.metadata.mismatch = true
			}
			if err := stack.BindPreparedGenerationConnectionV3(t.Context(), session, connectionMode); err == nil {
				t.Fatal("failed authority bound", mode)
			}
			p, r := f.metadata.counts()
			if err := stack.BindPreparedGenerationConnectionV3(t.Context(), actual, attachedworkersession.ResourceGenerationInitialV1); err == nil {
				t.Fatal("failed bind retried", mode)
			}
			p2, r2 := f.metadata.counts()
			if stack.connection != nil || !stack.generationBindAttempted || p != p2 || r != r2 || p+r > 2 || f.launcher.prepares != 0 {
				t.Fatalf("failure reset/retried/acquired effects: connection=%t attempted=%t requests=%d/%d→%d/%d prepare=%d", stack.connection != nil, stack.generationBindAttempted, p, r, p2, r2, f.launcher.prepares)
			}
			if _, err := stack.Run(t.Context(), attachedworkerdaemon.Invocation{}); err == nil {
				t.Fatal("failed bind acquired invocation authority")
			}
		})
	}
}

func TestNativeGenerationStackV3SyncFailureFencesOnlyNewInput(t *testing.T) {
	f := newGenerationStackFixtureV3(t)
	stack, session := f.stack(t), f.connect(t)
	if err := stack.BindPreparedGenerationConnectionV3(t.Context(), session, attachedworkersession.ResourceGenerationInitialV1); err != nil {
		t.Fatal(err)
	}
	prepared, pinned := stack.prepared, stack.runner
	if err := f.lifecycle.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stack.SynchronizePreparedGenerationV3(t.Context(), session); err == nil {
		t.Fatal("closed original lifecycle synchronized")
	}
	if stack.connection != nil || stack.prepared != prepared || stack.runner != pinned || stack.closed {
		t.Fatal("sync failure replaced/closed old prepared output owner")
	}
	// The negative fence is only Stack's new-input connection. Each output
	// method still reaches the SAME prepared owner: its original contract
	// denial (no completed slot in this no-process fixture) must not become a
	// stack reconciliation/ACK denial. Actual completed-N output and its private
	// nativeSession pointer are exercised by the codex V3 joined runner tests.
	identity, result := attachedworkerdaemon.InvocationIdentity{}, attachedworkerdaemon.InvocationResult{}
	if _, err := stack.ReceiptCandidate(identity, result, nil, domain.AttachedWorkerTerminalSucceeded); !errors.Is(err, codexexec.ErrContract) {
		t.Fatalf("new-input fence intercepted original receipt owner: %v", err)
	}
	if _, err := stack.InlineReceiptCandidate(t.Context(), identity, result, nil); !errors.Is(err, codexexec.ErrContract) {
		t.Fatalf("new-input fence intercepted original inline owner: %v", err)
	}
	if err := stack.RetireReceipt(identity); !errors.Is(err, codexexec.ErrContract) {
		t.Fatalf("new-input fence intercepted original retirement owner: %v", err)
	}
	if err := prepared.BindNativeSessionV3(session); err == nil {
		t.Fatal("new-input fence removed original prepared Session binding")
	}
	if err := stack.SynchronizePreparedGenerationV3(t.Context(), session); err == nil {
		t.Fatal("fenced stack retried")
	}
	if p, r := f.metadata.counts(); p != 1 || r != 0 {
		t.Fatalf("failed metadata-only fence performed HTTP: publish=%d read=%d", p, r)
	}
}

func TestNativeGenerationStackV3CloseFencesBlockedMetadataWithoutStackLock(t *testing.T) {
	f := newGenerationStackFixtureV3(t)
	stack, session := f.stack(t), f.connect(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	f.metadata.hook = func(req *http.Request) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-req.Context().Done():
			return req.Context().Err()
		}
	}
	bound := make(chan error, 1)
	closed := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(func() { unblock.Do(func() { close(release) }); cancel() })
	go func() {
		bound <- stack.BindPreparedGenerationConnectionV3(ctx, session, attachedworkersession.ResourceGenerationInitialV1)
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("metadata request did not enter", ctx.Err())
	}
	go func() { closed <- stack.Close(ctx) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close held behind metadata/Stack.mu", ctx.Err())
	}
	unblock.Do(func() { close(release) })
	select {
	case err := <-bound:
		if err == nil {
			t.Fatal("closed stack accepted late metadata reply")
		}
	case <-ctx.Done():
		t.Fatal("binding did not terminate", ctx.Err())
	}
	if stack.connection != nil || !stack.closed {
		t.Fatal("late metadata reply restored connection authority")
	}
	if err := stack.BindPreparedGenerationConnectionV3(t.Context(), session, attachedworkersession.ResourceGenerationInitialV1); err == nil {
		t.Fatal("closed stack retried bind")
	}
}

func TestNativeGenerationStackV3ReconnectUsesFreshOwnerAndReadOnlyACK(t *testing.T) {
	f := newGenerationStackFixtureV3(t)
	initial, session := f.stack(t), f.connect(t)
	if err := initial.BindPreparedGenerationConnectionV3(t.Context(), session, attachedworkersession.ResourceGenerationInitialV1); err != nil {
		t.Fatal(err)
	}
	if err := initial.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Close(); err != nil {
		t.Fatal(err)
	}
	lease, err := f.worker.AcquireRuntime(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := lease.LoadReconnectCheckpoint(t.Context())
	if err != nil {
		_ = lease.Close()
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, err := f.worker.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	peer := &preparedSessionPeer{now: f.clock.Now(), public: private.Public().(ed25519.PublicKey), previous: &checkpoint}
	owner, err := attachedresourcegenerationsession.NewV1(manifest.ControlPlaneOrigin, &http.Client{Transport: f.metadata}, &generationStackSourceV3{preparedSessionPeer: peer}, f.lifecycle, f.base.config.PreparedCodexNativeGeneration.Baseline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	config := f.base.config
	owned, err := config.PreparedCodexNativeGeneration.Clone()
	if err != nil {
		t.Fatal(err)
	}
	owned.GenerationOwner = owner
	config.PreparedCodexNativeGeneration = &owned
	var stack *Stack
	connector, err := attachedworkersession.New(f.worker, peer, owner, attachedworkersession.Config{Audience: "sessionless:attached-worker:v1", WorkerOffer: f.capability.ProtocolOffer,
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, OperationTimeout: time.Second, Now: f.clock.Now, Random: bytes.NewReader(bytes.Repeat([]byte{0x62}, 64)),
		RuntimePreflight: func(ctx context.Context, m attachedworkerlocal.ManifestV1) error {
			var err error
			stack, err = newNativeGenerationStackV3(ctx, m, config, f.lifecycle, f.deps)
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	reconnected, err := connector.Reconnect(t.Context(), attachedworkersession.ReconnectInputV1{ExpectedWorkerRevision: 7, CapabilityManifest: f.capability})
	if err != nil {
		t.Fatal("genuine idle reconnect", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := reconnected.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := stack.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	recovery, err := reconnected.ReconnectRecovery()
	if err != nil || recovery.ConnectionState != attachedworkerprotocol.ConnectionReady || recovery.AttemptState != attachedworkerprotocol.AttemptIdle || recovery.TerminalDecision != attachedworkerprotocol.ReconnectTerminalNone || recovery.Terminal != nil {
		t.Fatalf("not genuine idle recovery: %+v err=%v", recovery, err)
	}
	if err := stack.BindPreparedGenerationConnectionV3(t.Context(), reconnected, attachedworkersession.ResourceGenerationReconnectV1); err != nil {
		t.Fatal("reconnect generation bind", err)
	}
	if err := stack.SynchronizePreparedGenerationV3(t.Context(), reconnected); err != nil {
		t.Fatal("unchanged reconnect ACK", err)
	}
	if stack.nativeGeneration.GenerationOwner == f.owner || stack.nativeGeneration.Lifecycle != f.lifecycle || stack.scope.connection != 1 || reconnected.Snapshot().ConnectionGeneration != 2 {
		t.Fatal("reused old owner or changed lifecycle/generation")
	}
	if p, r := f.metadata.counts(); p != 1 || r != 1 || f.launcher.prepares != 0 {
		t.Fatalf("reconnect published/re-executed: publish=%d read=%d prepare=%d", p, r, f.launcher.prepares)
	}
	if err := f.owner.BindReconnectedSessionV1(t.Context(), reconnected); err == nil {
		t.Fatal("retired initial owner reused")
	}
}
