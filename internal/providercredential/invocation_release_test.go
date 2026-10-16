package providercredential

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Synthetic canonical API-account files exercise ordinary materialization,
// release and fencing; no provider or model is contacted.
func apiFileInvocationFixture(t *testing.T, delivery domain.ProviderCredentialDeliveryKindV1) (*InvocationService, *fakeBindingStore, *fakeSecretStore, ports.ProviderCredentialIssueRequestV1, *mutableClock) {
	t.Helper()
	now := time.Unix(100, 0).UTC()
	expires := now.Add(time.Hour)
	clock := &mutableClock{now: now}
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceAPIAccountV1, ResourceID: "api-account-a", OwnerUserID: "user-a", Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	binding := domain.HarnessBindingV2{
		Version: domain.HarnessBindingVersionV2, Profile: domain.HarnessExecutionSingleTurnV2,
		TenantID: "tenant-a", OwnerUserID: "user-a", RunID: "run-a", AttemptID: "attempt-a",
		Backend: domain.HarnessBackendDescriptorV1{
			HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendDirectOpenRouterV1,
			ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("1", 64), NativeProtocolVersion: "openrouter-http.v1",
			BackendProfileDigest: strings.Repeat("2", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: delivery,
		},
		Resource: resource, ModelVendorID: "openrouter", ModelID: "stealth-ox-alpha", InputDataClass: domain.ProviderDataPublicV1,
		ProviderCatalogDigest: strings.Repeat("3", 64), ProviderRouteDigest: strings.Repeat("4", 64), PrivacyPolicyDigest: strings.Repeat("5", 64),
		CapabilityEvidenceDigest: strings.Repeat("6", 64), EffectivePolicyDigest: strings.Repeat("7", 64), ExecutionPlacementDigest: strings.Repeat("8", 64), EvidenceExpiresAt: &expires,
	}
	ref, err := domain.NewCredentialSecretRef("test/provider/api-account-a/generation-1")
	if err != nil {
		t.Fatalf("construct synthetic reference: %v", err)
	}
	marker := []byte("synthetic-immutable-api-marker")
	credential := domain.ProviderCredentialBindingV1{
		Version: domain.ProviderCredentialBindingVersionV1, TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID,
		ResourceKind: resource.Kind, ResourceID: resource.ResourceID, ResourceRevision: resource.Revision, CredentialGeneration: resource.CredentialGeneration,
		CandidateMutationID: "synthetic-api-mutation", State: domain.ProviderCredentialActiveV1, SecretRef: ref, SecretFingerprint: domain.FingerprintCredential(marker), UpdatedAt: now,
	}
	if err := credential.Validate(); err != nil {
		t.Fatalf("validate canonical API credential fixture: %v", err)
	}
	bindings, secrets := newFakeBindingStore(), newFakeSecretStore()
	bindings.bindings[locatorFor(credential)] = credential
	secrets.values[ref], secrets.committed[ref] = marker, struct{}{}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize test scratch: %v", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("protect test scratch: %v", err)
	}
	template := ports.ProviderCredentialDeliveryTemplateV1{Kind: delivery}
	if delivery == domain.ProviderCredentialDeliveryFileV1 {
		template.FileName = "api-key"
	} else if delivery == domain.ProviderCredentialDeliveryEnvironmentV1 {
		template.EnvironmentName = "SYNTHETIC_API_KEY"
	}
	service, err := NewInvocationService(InvocationConfig{Clock: clock, IDs: &fixedIDs{}, ScratchRoot: root}, &fixedDeliveryPlanner{template: template}, bindings, secrets)
	if err != nil {
		t.Fatalf("construct API invocation service: %v", err)
	}
	t.Cleanup(func() {
		if service.files != nil {
			if err := service.files.Close(); err != nil {
				t.Errorf("close owned API descriptors: %v", err)
			}
		}
	})
	request := ports.ProviderCredentialIssueRequestV1{HarnessBinding: binding, WorkerID: "worker-a", LeaseID: "lease-a", LeaseFence: 1, ExpiresAt: now.Add(30 * time.Minute)}
	if err := request.Validate(); err != nil {
		t.Fatalf("validate canonical API issue fixture: %v", err)
	}
	return service, bindings, secrets, request, clock
}

func issueAPIFile(t *testing.T, service *InvocationService, request ports.ProviderCredentialIssueRequestV1) ports.ProviderInvocationCredentialV1 {
	t.Helper()
	handle, err := service.IssueProviderCredential(context.Background(), request)
	if err != nil {
		t.Fatalf("issue genuine API handle: %v", err)
	}
	t.Cleanup(func() {
		if err := service.ReleaseProviderCredential(context.Background(), handle); err != nil {
			t.Errorf("release owned API invocation: %v", err)
		}
	})
	return handle
}

func materializeAPIFile(t *testing.T, service *InvocationService, handle ports.ProviderInvocationCredentialV1) ports.ProviderCredentialMaterializationV1 {
	t.Helper()
	var plan ports.ProviderCredentialMaterializationV1
	calls := 0
	err := service.MaterializeProviderCredential(context.Background(), handle, func(value ports.ProviderCredentialMaterializationV1, secret []byte) error {
		calls++
		plan = value
		if value.Kind == domain.ProviderCredentialDeliveryFileV1 && len(secret) != 0 {
			t.Error("API file callback received plaintext")
		}
		return nil
	})
	if err != nil || calls != 1 {
		t.Fatalf("materialize API invocation: callbacks=%d want=1 err=%v", calls, err)
	}
	return plan
}

func expectAPIFileRelease(t *testing.T, service *InvocationService, handle ports.ProviderInvocationCredentialV1, plan ports.ProviderCredentialMaterializationV1) {
	t.Helper()
	if err := service.ReleaseProviderCredential(context.Background(), handle); err != nil {
		t.Errorf("release owned credential file: %v", err)
	}
	if plan.FilePath != "" {
		if _, err := os.Lstat(plan.FilePath); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("owned credential path survived Release: err=%v", err)
		}
	}
}

func waitImmutableEvent[T any](t *testing.T, channel <-chan T, operation string) T {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case value := <-channel:
		return value
	case <-timer.C:
		select {
		case value := <-channel:
			return value
		default:
			t.Fatalf("timed out waiting for %s acknowledgement", operation)
			var zero T
			return zero
		}
	}
}

func TestInvocationReleaseCancelledAndNilRetainCustody(t *testing.T) {
	for _, name := range []string{"nil", "cancelled", "deadline"} {
		t.Run(name, func(t *testing.T) {
			service, _, _, request, _ := apiFileInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			handle := issueAPIFile(t, service, request)
			plan := materializeAPIFile(t, service, handle)
			state := service.handles[handle.HandleID]
			file := state.file
			var ctx context.Context
			var want error = ErrInvalid
			if name == "cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
				want = context.Canceled
			}
			if name == "deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(context.Background(), time.Unix(1, 0))
				defer cancel()
				want = context.DeadlineExceeded
			}
			if err := service.ReleaseProviderCredential(ctx, handle); !errors.Is(err, want) {
				t.Errorf("Release error=%v want=%v", err, want)
			}
			if service.handles[handle.HandleID] != state || state.file != file || state.released {
				t.Error("rejected Release lost original custody or claimed release")
			}
			if _, err := os.Lstat(plan.FilePath); err != nil {
				t.Error("rejected Release removed original owned file")
			}
			expectAPIFileRelease(t, service, handle, plan)
			if err := service.ReleaseProviderCredential(context.Background(), handle); err != nil {
				t.Errorf("completed Release was not idempotent: %v", err)
			}
		})
	}
}

// The timer is only a test failure bound. Completion wins even if the timer
// and actual result became ready together; no sleep decides readiness.
func waitReleaseResult[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case value := <-result:
		return value
	case <-timer.C:
		select {
		case value := <-result:
			return value
		default:
			t.Fatal("owned Release fixture did not complete")
			var zero T
			return zero
		}
	}
}

func assertReleaseForeignTuples(t *testing.T, service *InvocationService, ctx context.Context, handle ports.ProviderInvocationCredentialV1) {
	t.Helper()
	mutations := []struct {
		name   string
		change func(*ports.ProviderInvocationCredentialV1)
	}{
		{"tenant", func(h *ports.ProviderInvocationCredentialV1) { h.TenantID = "other-tenant" }},
		{"owner", func(h *ports.ProviderInvocationCredentialV1) {
			h.OwnerUserID = "other-owner"
			h.ProviderResource.OwnerUserID = h.OwnerUserID
		}},
		{"run", func(h *ports.ProviderInvocationCredentialV1) { h.RunID = "other-run" }},
		{"attempt", func(h *ports.ProviderInvocationCredentialV1) { h.AttemptID = "other-attempt" }},
		{"worker", func(h *ports.ProviderInvocationCredentialV1) { h.WorkerID = "other-worker" }},
		{"lease", func(h *ports.ProviderInvocationCredentialV1) { h.LeaseID = "other-lease" }},
		{"fence", func(h *ports.ProviderInvocationCredentialV1) { h.LeaseFence++ }},
		{"resource", func(h *ports.ProviderInvocationCredentialV1) { h.ProviderResource.ResourceID = "other-resource" }},
		{"revision", func(h *ports.ProviderInvocationCredentialV1) { h.ProviderResource.Revision++ }},
		{"generation", func(h *ports.ProviderInvocationCredentialV1) { h.ProviderResource.CredentialGeneration++ }},
		{"expiry", func(h *ports.ProviderInvocationCredentialV1) { h.ExpiresAt = h.ExpiresAt.Add(time.Second) }},
	}
	for _, mutation := range mutations {
		foreign := handle
		mutation.change(&foreign)
		if foreign.Validate() != nil {
			t.Fatalf("invalid %s test tuple", mutation.name)
		}
		if err := service.ReleaseProviderCredential(ctx, foreign); !errors.Is(err, ErrNotFound) {
			t.Errorf("foreign %s tuple error=%v want=%v", mutation.name, err, ErrNotFound)
		}
	}
}

func TestInvocationReleaseCancellableBusyConsumer(t *testing.T) {
	service, _, _, request, _ := apiFileInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	handle := issueAPIFile(t, service, request)
	entered := make(chan ports.ProviderCredentialMaterializationV1, 1)
	resume, done := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	var result error
	// Registered before launch and after handle cleanup: cancel the held
	// callback and independently join before any credential/TempDir cleanup.
	t.Cleanup(func() { unblock(); waitReleaseResult(t, done) })
	go func() {
		defer close(done)
		result = service.MaterializeProviderCredential(context.Background(), handle, func(plan ports.ProviderCredentialMaterializationV1, secret []byte) error {
			if len(secret) != 0 {
				return ErrBackend
			}
			entered <- plan
			<-resume
			return nil
		})
	}()
	plan := waitReleaseResult(t, entered)
	state := service.handles[handle.HandleID]
	file := state.file
	before, err := os.Lstat(plan.FilePath)
	if err != nil {
		t.Fatal("missing held consumer credential file")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assertReleaseForeignTuples(t, service, ctx, handle)
	if err := service.ReleaseProviderCredential(ctx, handle); !errors.Is(err, ErrConsumed) {
		t.Errorf("busy Release error=%v want=%v", err, ErrConsumed)
	}
	if state.released || state.file != file || service.handles[handle.HandleID] != state {
		t.Error("busy Release changed retained state")
	}
	after, err := os.Lstat(plan.FilePath)
	if err != nil || !os.SameFile(before, after) {
		t.Error("busy Release changed original inode")
	}
	cancel()
	if err := service.ReleaseProviderCredential(ctx, handle); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled busy Release: %v", err)
	}
	unblock()
	waitReleaseResult(t, done)
	if result != nil {
		t.Fatalf("genuine consumer completion: %v", result)
	}
	live, cancelLive := context.WithCancel(context.Background())
	defer cancelLive()
	if err := service.ReleaseProviderCredential(live, handle); err != nil {
		t.Fatalf("after-unlock finite Release: %v", err)
	}
	if _, err := os.Lstat(plan.FilePath); !errors.Is(err, os.ErrNotExist) {
		t.Error("successful Release left original file")
	}
	if service.handles[handle.HandleID] != nil {
		t.Error("successful Release retained registry entry")
	}
}

func TestInvocationReleaseCancellableRegistryBusy(t *testing.T) {
	service, _, _, request, _ := apiFileInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	handle := issueAPIFile(t, service, request)
	plan := materializeAPIFile(t, service, handle)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	func() {
		service.mu.Lock()
		defer service.mu.Unlock()
		if err := service.ReleaseProviderCredential(ctx, handle); !errors.Is(err, ErrBackend) {
			t.Errorf("busy registry Release: %v", err)
		}
		cancel()
		if err := service.ReleaseProviderCredential(ctx, handle); !errors.Is(err, context.Canceled) {
			t.Errorf("cancelled busy registry Release: %v", err)
		}
	}()
	state := service.handles[handle.HandleID]
	if state.released || state.file == nil {
		t.Error("registry contention lost custody")
	}
	expectAPIFileRelease(t, service, handle, plan)
}

func TestInvocationReleaseCleanupFailureKeepsRevocationTombstone(t *testing.T) {
	service, _, _, request, _ := apiFileInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	handle, err := service.IssueProviderCredential(context.Background(), request)
	if err != nil {
		t.Fatal("issue cleanup failure fixture")
	}
	plan := materializeAPIFile(t, service, handle)
	state := service.handles[handle.HandleID]
	file := state.file
	if err := service.files.Close(); err != nil {
		t.Fatal("close exact owned wrapper")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := service.ReleaseProviderCredential(ctx, handle); !errors.Is(err, ErrDeliveryMismatch) {
		t.Errorf("cleanup failure: %v", err)
	}
	if service.handles[handle.HandleID] != state || state.file != file || !state.released {
		t.Error("cleanup error lost custody or revocation tombstone")
	}
	if _, err := os.Lstat(plan.FilePath); err != nil {
		t.Error("closed-wrapper residue was misreported as cleanup")
	}
	called := false
	if err := service.MaterializeProviderCredential(context.Background(), handle, func(ports.ProviderCredentialMaterializationV1, []byte) error { called = true; return nil }); !errors.Is(err, ErrNotFound) || called {
		t.Error("cleanup failure reopened Materialize")
	}
	if err := service.ReleaseProviderCredential(context.Background(), handle); !errors.Is(err, ErrDeliveryMismatch) {
		t.Error("repeated failed cleanup manufactured success")
	}
}

func TestInvocationReleaseAfterOriginalFence(t *testing.T) {
	service, _, _, request, _ := apiFileInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	handle := issueAPIFile(t, service, request)
	plan := materializeAPIFile(t, service, handle)
	state := service.handles[handle.HandleID]
	locator := ports.ProviderCredentialLocatorV1{TenantID: handle.TenantID, OwnerUserID: handle.OwnerUserID, ResourceKind: handle.ProviderResource.Kind, ResourceID: handle.ProviderResource.ResourceID}
	if err := service.FenceProviderCredentialInvocations(context.Background(), locator, 2); err != nil {
		t.Fatalf("original Fence: %v", err)
	}
	if !state.revoked || state.file != nil {
		t.Fatal("Fence did not complete original owned teardown")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := service.ReleaseProviderCredential(ctx, handle); err != nil {
		t.Errorf("revoked handle Release: %v", err)
	}
	expectAPIFileRelease(t, service, handle, plan)
}
