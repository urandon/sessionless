package providercredential

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type fixedIDs struct{ next uint64 }

type mutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *mutableClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *mutableClock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

func (ids *fixedIDs) NewID(_ context.Context, kind ports.IDKind) (string, error) {
	if kind != ports.IDCredentialHandle && kind != ports.IDProviderCredentialMutation {
		return "", errors.New("unexpected id kind")
	}
	ids.next++
	return fmt.Sprintf("%s-%d", kind, ids.next), nil
}

type fixedDeliveryPlanner struct {
	template ports.ProviderCredentialDeliveryTemplateV1
	calls    int
}

func (planner *fixedDeliveryPlanner) PlanProviderCredentialDelivery(_ context.Context, binding domain.HarnessBindingV1) (ports.ProviderCredentialDeliveryTemplateV1, error) {
	planner.calls++
	if binding.Validate() != nil {
		return ports.ProviderCredentialDeliveryTemplateV1{}, errors.New("private invalid binding")
	}
	return planner.template, nil
}

func invocationFixture(t *testing.T, delivery domain.ProviderCredentialDeliveryKindV1) (*InvocationService, *fakeBindingStore, *fakeSecretStore, ports.ProviderCredentialIssueRequestV1, string) {
	t.Helper()
	now := time.Unix(100, 0).UTC()
	expires := now.Add(time.Hour)
	owner := domain.UserID("user-a")
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceRouterAccountV1, ResourceID: "openrouter-a", OwnerUserID: owner, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	descriptor := domain.HarnessBackendDescriptorV1{
		HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendDirectOpenRouterV1,
		ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("1", 64), NativeProtocolVersion: "openrouter-http.v1",
		BackendProfileDigest: strings.Repeat("2", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: delivery,
	}
	binding := domain.HarnessBindingV1{
		Version: domain.HarnessBindingVersionV1, TenantID: "tenant-a", OwnerUserID: owner, RunID: "run-a", AttemptID: "attempt-a",
		Backend: descriptor, Resource: resource, ModelVendorID: "openrouter", ModelID: "stealth-ox-alpha", InputDataClass: domain.ProviderDataPublicV1,
		ProviderCatalogDigest: strings.Repeat("3", 64), ProviderRouteDigest: strings.Repeat("4", 64), PrivacyPolicyDigest: strings.Repeat("5", 64),
		CapabilityEvidenceDigest: strings.Repeat("6", 64), EffectivePolicyDigest: strings.Repeat("7", 64), ExecutionPlacementDigest: strings.Repeat("8", 64), EvidenceExpiresAt: &expires,
	}
	if err := binding.Validate(); err != nil {
		t.Fatal(err)
	}
	secret := "openrouter-private-invocation-marker"
	ref, err := domain.NewCredentialSecretRef("lockbox/provider/openrouter-a/generation-1")
	if err != nil {
		t.Fatal(err)
	}
	authority := domain.ProviderCredentialBindingV1{
		Version: domain.ProviderCredentialBindingVersionV1, TenantID: binding.TenantID, OwnerUserID: owner, ResourceKind: resource.Kind, ResourceID: resource.ResourceID,
		ResourceRevision: resource.Revision, CredentialGeneration: resource.CredentialGeneration, CandidateMutationID: "mutation-invocation", State: domain.ProviderCredentialActiveV1,
		SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte(secret)), UpdatedAt: now,
	}
	if err := authority.Validate(); err != nil {
		t.Fatal(err)
	}
	bindings := newFakeBindingStore()
	bindings.bindings[locatorFor(authority)] = authority
	secrets := newFakeSecretStore()
	secrets.committed[ref] = struct{}{}
	secrets.values[ref] = []byte(secret)
	root, err := os.MkdirTemp("/private/tmp", "sessionless-provider-credential-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	template := ports.ProviderCredentialDeliveryTemplateV1{Kind: delivery}
	switch delivery {
	case domain.ProviderCredentialDeliveryFileV1:
		template.FileName = "provider.json"
	case domain.ProviderCredentialDeliveryEnvironmentV1:
		template.EnvironmentName = "OPENROUTER_API_KEY"
	}
	service, err := NewInvocationService(InvocationConfig{Clock: fixedClock{now: now}, IDs: &fixedIDs{}, ScratchRoot: root}, &fixedDeliveryPlanner{template: template}, bindings, secrets)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if service.files != nil {
			_ = service.files.Close()
		}
	})
	request := ports.ProviderCredentialIssueRequestV1{HarnessBinding: binding, WorkerID: "worker-a", LeaseID: "lease-a", LeaseFence: 1, ExpiresAt: now.Add(30 * time.Minute)}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	return service, bindings, secrets, request, secret
}

func TestInvocationMaterializationDeliveryMatrixIsOneShotAndReleases(t *testing.T) {
	for _, delivery := range []domain.ProviderCredentialDeliveryKindV1{domain.ProviderCredentialDeliveryFileV1, domain.ProviderCredentialDeliveryEnvironmentV1, domain.ProviderCredentialDeliveryDirectV1} {
		t.Run(string(delivery), func(t *testing.T) {
			service, _, secrets, request, marker := invocationFixture(t, delivery)
			handle, err := service.IssueProviderCredential(context.Background(), request)
			if err != nil || secrets.readCalls != 0 {
				t.Fatalf("issue handle=%+v err=%v reads=%d", handle, err, secrets.readCalls)
			}
			var materialization ports.ProviderCredentialMaterializationV1
			var callbackValue []byte
			err = service.MaterializeProviderCredential(context.Background(), handle, func(plan ports.ProviderCredentialMaterializationV1, value []byte) error {
				materialization = plan
				callbackValue = append([]byte(nil), value...)
				if delivery == domain.ProviderCredentialDeliveryFileV1 {
					content, readErr := os.ReadFile(plan.FilePath)
					if readErr != nil || string(content) != marker {
						t.Fatalf("file content=%q err=%v", content, readErr)
					}
					info, statErr := os.Stat(plan.FilePath)
					if statErr != nil || info.Mode().Perm() != 0o600 || filepath.Dir(plan.FilePath) != plan.RootDir {
						t.Fatalf("file mode/path info=%v err=%v", info, statErr)
					}
				}
				return nil
			})
			if err != nil || materialization.Kind != delivery || secrets.readCalls != 1 {
				t.Fatalf("materialize=%+v err=%v reads=%d", materialization, err, secrets.readCalls)
			}
			if delivery == domain.ProviderCredentialDeliveryFileV1 && len(callbackValue) != 0 {
				t.Fatal("file delivery exposed secret to callback")
			}
			if delivery != domain.ProviderCredentialDeliveryFileV1 && string(callbackValue) != marker {
				t.Fatalf("callback value=%q", callbackValue)
			}
			if err := service.MaterializeProviderCredential(context.Background(), handle, func(ports.ProviderCredentialMaterializationV1, []byte) error { return nil }); !errors.Is(err, ErrConsumed) {
				t.Fatalf("second materialization error=%v", err)
			}
			if err := service.ReleaseProviderCredential(context.Background(), handle); err != nil {
				t.Fatal(err)
			}
			if materialization.FilePath != "" {
				if _, err := os.Stat(materialization.FilePath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("credential file survived release: %v", err)
				}
			}
		})
	}
}

func TestEnvironmentAndDirectDeliveryDoNotRequireAFileScratchRoot(t *testing.T) {
	for _, delivery := range []domain.ProviderCredentialDeliveryKindV1{domain.ProviderCredentialDeliveryEnvironmentV1, domain.ProviderCredentialDeliveryDirectV1} {
		t.Run(string(delivery), func(t *testing.T) {
			fixture, bindings, secrets, request, _ := invocationFixture(t, delivery)
			service, err := NewInvocationService(
				InvocationConfig{Clock: fixture.clock, IDs: &fixedIDs{}},
				fixture.planner, bindings, secrets,
			)
			if err != nil {
				t.Fatalf("portable constructor error=%v", err)
			}
			handle, err := service.IssueProviderCredential(context.Background(), request)
			if err != nil {
				t.Fatalf("portable issue error=%v", err)
			}
			if err := service.MaterializeProviderCredential(context.Background(), handle, func(plan ports.ProviderCredentialMaterializationV1, secret []byte) error {
				if plan.Kind != delivery || len(secret) == 0 {
					t.Fatalf("portable materialization=%+v secret_bytes=%d", plan, len(secret))
				}
				return nil
			}); err != nil {
				t.Fatalf("portable materialize error=%v", err)
			}
			if service.files != nil {
				t.Fatal("non-file delivery acquired a file scratch namespace")
			}
		})
	}
}

func TestFileDeliveryRequiresPinnedScratchBeforeCredentialBackend(t *testing.T) {
	fixture, bindings, secrets, request, _ := invocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	service, err := NewInvocationService(
		InvocationConfig{Clock: fixture.clock, IDs: &fixedIDs{}},
		fixture.planner, bindings, secrets,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.IssueProviderCredential(context.Background(), request); !errors.Is(err, ErrDeliveryMismatch) {
		t.Fatalf("file issue without scratch error=%v", err)
	}
	if secrets.recoverCalls != 0 || secrets.readCalls != 0 {
		t.Fatalf("file scratch failure crossed credential backend: recover=%d read=%d", secrets.recoverCalls, secrets.readCalls)
	}
}

func TestInvocationRejectsAuthorityAndDeliveryMismatchBeforeSecretRead(t *testing.T) {
	service, _, secrets, request, _ := invocationFixture(t, domain.ProviderCredentialDeliveryEnvironmentV1)
	mutations := map[string]func(*ports.ProviderCredentialIssueRequestV1){
		"owner": func(value *ports.ProviderCredentialIssueRequestV1) { value.HarnessBinding.OwnerUserID = "user-b" },
		"resource id": func(value *ports.ProviderCredentialIssueRequestV1) {
			value.HarnessBinding.Resource.ResourceID = "openrouter-b"
		},
		"revision": func(value *ports.ProviderCredentialIssueRequestV1) { value.HarnessBinding.Resource.Revision++ },
		"generation": func(value *ports.ProviderCredentialIssueRequestV1) {
			value.HarnessBinding.Resource.CredentialGeneration++
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := request
			candidate.HarnessBinding = request.HarnessBinding.Clone()
			mutate(&candidate)
			if _, err := service.IssueProviderCredential(context.Background(), candidate); err == nil {
				t.Fatal("mutated authority issued a credential")
			}
		})
	}
	if secrets.readCalls != 0 {
		t.Fatalf("authority mismatch read secret %d times", secrets.readCalls)
	}
	other, _, otherSecrets, otherRequest, _ := invocationFixture(t, domain.ProviderCredentialDeliveryEnvironmentV1)
	other.planner = &fixedDeliveryPlanner{template: ports.ProviderCredentialDeliveryTemplateV1{Kind: domain.ProviderCredentialDeliveryDirectV1}}
	if _, err := other.IssueProviderCredential(context.Background(), otherRequest); !errors.Is(err, ErrDeliveryMismatch) {
		t.Fatalf("delivery mismatch error=%v", err)
	}
	if otherSecrets.readCalls != 0 || otherSecrets.recoverCalls != 0 {
		t.Fatalf("delivery mismatch crossed secret boundary: recover=%d read=%d", otherSecrets.recoverCalls, otherSecrets.readCalls)
	}
}

func TestInvocationExpiryRevocationAndErrorsAreContentFree(t *testing.T) {
	service, _, secrets, request, marker := invocationFixture(t, domain.ProviderCredentialDeliveryDirectV1)
	handle, err := service.IssueProviderCredential(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.FenceProviderCredentialInvocations(context.Background(), testLocator("user-a"), 2); err != nil {
		t.Fatal(err)
	}
	if err := service.MaterializeProviderCredential(context.Background(), handle, func(ports.ProviderCredentialMaterializationV1, []byte) error { return nil }); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoked handle error=%v", err)
	}
	if secrets.readCalls != 0 {
		t.Fatalf("revoked handle read secret %d times", secrets.readCalls)
	}

	missing, _, missingSecrets, missingRequest, _ := invocationFixture(t, domain.ProviderCredentialDeliveryDirectV1)
	for ref := range missingSecrets.committed {
		delete(missingSecrets.committed, ref)
	}
	if _, err := missing.IssueProviderCredential(context.Background(), missingRequest); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing authoritative secret error=%v", err)
	}

	service, _, secrets, request, marker = invocationFixture(t, domain.ProviderCredentialDeliveryDirectV1)
	handle, err = service.IssueProviderCredential(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = service.MaterializeProviderCredential(context.Background(), handle, func(ports.ProviderCredentialMaterializationV1, []byte) error { return errors.New(marker) })
	if !errors.Is(err, ErrBackend) || strings.Contains(err.Error(), marker) {
		t.Fatalf("callback error leaked: %v", err)
	}
	encoded, marshalErr := json.Marshal(handle)
	if marshalErr != nil || strings.Contains(string(encoded), marker) || strings.Contains(fmt.Sprintf("%+v", handle), marker) {
		t.Fatalf("handle leaked secret: json=%s value=%+v err=%v", encoded, handle, marshalErr)
	}

	expired := request
	expired.ExpiresAt = time.Unix(99, 0).UTC()
	if _, err := service.IssueProviderCredential(context.Background(), expired); !errors.Is(err, ErrExpired) && !errors.Is(err, ErrInvalid) {
		t.Fatalf("expired issue error=%v", err)
	}
}

func TestInvocationMaterializeAfterExpiryFailsBeforeSecretRead(t *testing.T) {
	service, _, secrets, request, _ := invocationFixture(t, domain.ProviderCredentialDeliveryDirectV1)
	clock := &mutableClock{now: time.Unix(100, 0).UTC()}
	service.clock = clock
	handle, err := service.IssueProviderCredential(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	clock.Set(handle.ExpiresAt)
	if err := service.MaterializeProviderCredential(context.Background(), handle, func(ports.ProviderCredentialMaterializationV1, []byte) error { return nil }); !errors.Is(err, ErrExpired) {
		t.Fatalf("materialize at exact expiry error=%v", err)
	}
	if secrets.readCalls != 0 {
		t.Fatalf("expired materialization read plaintext %d times", secrets.readCalls)
	}
}

func TestInvocationFenceWaitsForActiveConsumerAndBlocksLaterUse(t *testing.T) {
	service, _, _, request, _ := invocationFixture(t, domain.ProviderCredentialDeliveryDirectV1)
	handle, err := service.IssueProviderCredential(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	consumerStarted := make(chan struct{})
	releaseConsumer := make(chan struct{})
	materialized := make(chan error, 1)
	go func() {
		materialized <- service.MaterializeProviderCredential(context.Background(), handle, func(ports.ProviderCredentialMaterializationV1, []byte) error {
			close(consumerStarted)
			<-releaseConsumer
			return nil
		})
	}()
	<-consumerStarted
	fenced := make(chan error, 1)
	var started sync.WaitGroup
	started.Add(1)
	go func() {
		started.Done()
		fenced <- service.FenceProviderCredentialInvocations(context.Background(), testLocator("user-a"), 2)
	}()
	started.Wait()
	select {
	case err := <-fenced:
		t.Fatalf("fence returned before active consumer stopped: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseConsumer)
	if err := <-materialized; err != nil {
		t.Fatal(err)
	}
	if err := <-fenced; err != nil {
		t.Fatal(err)
	}
	if err := service.MaterializeProviderCredential(context.Background(), handle, func(ports.ProviderCredentialMaterializationV1, []byte) error { return nil }); !errors.Is(err, ErrNotFound) {
		t.Fatalf("post-fence materialization error=%v", err)
	}

	current, _, _, currentRequest, _ := invocationFixture(t, domain.ProviderCredentialDeliveryDirectV1)
	currentHandle, err := current.IssueProviderCredential(context.Background(), currentRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err := current.FenceProviderCredentialInvocations(context.Background(), testLocator("user-a"), currentHandle.ProviderResource.CredentialGeneration); err != nil {
		t.Fatal(err)
	}
	if err := current.MaterializeProviderCredential(context.Background(), currentHandle, func(ports.ProviderCredentialMaterializationV1, []byte) error { return nil }); err != nil {
		t.Fatalf("same-generation handle was fenced by an older replay: %v", err)
	}
}

func TestRotationWaitsForActiveMaterializationBeforeDeletingOldGeneration(t *testing.T) {
	invocations, bindings, secrets, request, _ := invocationFixture(t, domain.ProviderCredentialDeliveryDirectV1)
	bindings.casApplied = make(chan struct{}, 1)
	mutations, err := New(
		Config{Clock: fixedClock{now: time.Unix(101, 0).UTC()}, IDs: &fixedIDs{}},
		&fakeAuthorizer{}, invocations, bindings, secrets,
	)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := invocations.IssueProviderCredential(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	consumerStarted := make(chan struct{})
	releaseConsumer := make(chan struct{})
	materialized := make(chan error, 1)
	go func() {
		materialized <- invocations.MaterializeProviderCredential(context.Background(), handle, func(ports.ProviderCredentialMaterializationV1, []byte) error {
			close(consumerStarted)
			<-releaseConsumer
			return nil
		})
	}()
	<-consumerStarted
	rotated := make(chan error, 1)
	go func() {
		_, rotateErr := mutations.Ingest(context.Background(), ingestRequest(testLocator("user-a")), []byte("rotated-provider-key"))
		rotated <- rotateErr
	}()
	<-bindings.casApplied
	select {
	case err := <-rotated:
		t.Fatalf("rotation returned before active plaintext consumer stopped: %v", err)
	default:
	}
	secrets.mu.Lock()
	oldSecretStillPresent := len(secrets.deleted) == 0
	secrets.mu.Unlock()
	if !oldSecretStillPresent {
		t.Fatal("rotation deleted the old generation during an active consumer")
	}
	close(releaseConsumer)
	if err := <-materialized; err != nil {
		t.Fatal(err)
	}
	if err := <-rotated; err != nil {
		t.Fatal(err)
	}
	if err := invocations.MaterializeProviderCredential(context.Background(), handle, func(ports.ProviderCredentialMaterializationV1, []byte) error { return nil }); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old handle survived rotation: %v", err)
	}
}

func TestRevokeAndIssueRaceNeverExposesRevokedPlaintext(t *testing.T) {
	for iteration := 0; iteration < 50; iteration++ {
		invocations, bindings, secrets, request, _ := invocationFixture(t, domain.ProviderCredentialDeliveryDirectV1)
		mutations, err := New(
			Config{Clock: fixedClock{now: time.Unix(101, 0).UTC()}, IDs: &fixedIDs{}},
			&fakeAuthorizer{}, invocations, bindings, secrets,
		)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		handles := make(chan ports.ProviderInvocationCredentialV1, 1)
		issueErrors := make(chan error, 1)
		revokeErrors := make(chan error, 1)
		go func() {
			<-start
			handle, issueErr := invocations.IssueProviderCredential(context.Background(), request)
			handles <- handle
			issueErrors <- issueErr
		}()
		go func() {
			<-start
			_, revokeErr := mutations.Revoke(context.Background(), revokeRequest(testLocator("user-a")))
			revokeErrors <- revokeErr
		}()
		close(start)
		handle, issueErr := <-handles, <-issueErrors
		if err := <-revokeErrors; err != nil {
			t.Fatalf("iteration %d: revoke error=%v", iteration, err)
		}
		if issueErr != nil && !errors.Is(issueErr, ErrNotFound) {
			t.Fatalf("iteration %d: issue error=%v", iteration, issueErr)
		}
		if issueErr == nil {
			if err := invocations.MaterializeProviderCredential(context.Background(), handle, func(ports.ProviderCredentialMaterializationV1, []byte) error { return nil }); !errors.Is(err, ErrNotFound) {
				t.Fatalf("iteration %d: revoked handle materialized: %v", iteration, err)
			}
		}
		if secrets.readCalls != 0 {
			t.Fatalf("iteration %d: revoke/issue race read plaintext %d times", iteration, secrets.readCalls)
		}
	}
}
