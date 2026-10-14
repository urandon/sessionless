package providercredential

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestInvocationReleaseCancelledAndNilRetainCustody(t *testing.T) {
	for _, name := range []string{"nil", "cancelled", "deadline"} {
		t.Run(name, func(t *testing.T) {
			service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			handle := issueImmutableAPI(t, service, request)
			plan := materializeImmutableAPI(t, service, handle)
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
			expectImmutableRelease(t, service, handle, plan)
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
	service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	handle := issueImmutableAPI(t, service, request)
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

func TestInvocationReleaseCancellableBusyFinalize(t *testing.T) {
	service, bindings, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	handle := issueImmutableAPI(t, service, request)
	plan := materializeImmutableAPI(t, service, handle)
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	first := true
	service.bindings = &immutableBindingProbe{ProviderCredentialBindingStore: bindings, load: func(ctx context.Context, locator ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
		if first {
			first = false
			close(entered)
			<-resume
		}
		return bindings.LoadProviderCredential(ctx, locator)
	}}
	var result error
	t.Cleanup(func() { unblock(); waitReleaseResult(t, done) })
	go func() {
		defer close(done)
		result = service.FinalizeProviderCredentialFile(context.Background(), handle, plan)
	}()
	waitReleaseResult(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assertReleaseForeignTuples(t, service, ctx, handle)
	if err := service.ReleaseProviderCredential(ctx, handle); !errors.Is(err, ErrConsumed) {
		t.Errorf("busy Finalize Release: %v", err)
	}
	if _, err := os.Lstat(plan.FilePath); err != nil {
		t.Error("busy Finalize Release removed file")
	}
	unblock()
	waitReleaseResult(t, done)
	if result != nil {
		t.Fatalf("original Finalize after busy Release: %v", result)
	}
	state := service.handles[handle.HandleID]
	if !state.finalized || state.released || state.file == nil {
		t.Fatal("busy denial changed Finalize state")
	}
	if err := service.ReleaseProviderCredential(ctx, handle); err != nil {
		t.Fatalf("finalized finite Release: %v", err)
	}
	expectImmutableRelease(t, service, handle, plan)
}

func TestInvocationReleaseCancellableRegistryBusy(t *testing.T) {
	service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	handle := issueImmutableAPI(t, service, request)
	plan := materializeImmutableAPI(t, service, handle)
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
	expectImmutableRelease(t, service, handle, plan)
}

func TestInvocationReleaseCleanupFailureKeepsRevocationTombstone(t *testing.T) {
	service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	handle, err := service.IssueProviderCredential(context.Background(), request)
	if err != nil {
		t.Fatal("issue cleanup failure fixture")
	}
	plan := materializeImmutableAPI(t, service, handle)
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
	if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); !errors.Is(err, ErrNotFound) {
		t.Errorf("cleanup failure reopened Finalize: %v", err)
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
	service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	handle := issueImmutableAPI(t, service, request)
	plan := materializeImmutableAPI(t, service, handle)
	state := service.handles[handle.HandleID]
	if err := service.FenceProviderCredentialInvocations(context.Background(), locatorFor(state.binding), 2); err != nil {
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
	expectImmutableRelease(t, service, handle, plan)
}
