package providercredential

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

type fakeAuthorizer struct {
	deny  bool
	calls int
}

type fakeInvocationRevoker struct {
	calls             int
	beforeGenerations []uint64
}

func (revoker *fakeInvocationRevoker) FenceProviderCredentialInvocations(_ context.Context, locator ports.ProviderCredentialLocatorV1, beforeGeneration uint64) error {
	revoker.calls++
	revoker.beforeGenerations = append(revoker.beforeGenerations, beforeGeneration)
	if beforeGeneration == 0 {
		return errors.New("invalid generation fence")
	}
	return locator.Validate()
}

func (authorizer *fakeAuthorizer) AuthorizeProviderCredentialMutation(_ context.Context, principal ports.ProviderCredentialPrincipalV1, operation ports.ProviderCredentialMutationOperationV1) (bool, error) {
	authorizer.calls++
	if principal.Validate() != nil || (operation != ports.ProviderCredentialOperationIngestV1 && operation != ports.ProviderCredentialOperationRevokeV1) {
		return false, errors.New("private authorization detail")
	}
	return !authorizer.deny, nil
}

type fakeBindingStore struct {
	mu         sync.Mutex
	bindings   map[ports.ProviderCredentialLocatorV1]domain.ProviderCredentialBindingV1
	fenced     map[string]bool
	cleanups   []ports.ProviderCredentialCleanupV1
	forceCAS   bool
	casError   bool
	casApplied chan struct{}
	loadCalls  int
}

func newFakeBindingStore() *fakeBindingStore {
	return &fakeBindingStore{bindings: map[ports.ProviderCredentialLocatorV1]domain.ProviderCredentialBindingV1{}, fenced: map[string]bool{}}
}

func (store *fakeBindingStore) LoadProviderCredential(_ context.Context, locator ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.loadCalls++
	value, ok := store.bindings[locator]
	return value, ok, nil
}

func (store *fakeBindingStore) CompareAndSwapProviderCredential(_ context.Context, expected uint64, next domain.ProviderCredentialBindingV1) (ports.ProviderCredentialSwapV1, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.casError {
		store.casError = false
		return ports.ProviderCredentialSwapV1{}, errors.New("ambiguous binding CAS")
	}
	locator := locatorFor(next)
	current, found := store.bindings[locator]
	if store.fenced[next.CandidateMutationID] {
		return ports.ProviderCredentialSwapV1{}, nil
	}
	if found && current == next {
		action := domain.ProviderCredentialAuditRotatedV1
		if current.ResourceRevision == 1 {
			action = domain.ProviderCredentialAuditIngestedV1
		}
		audit, err := domain.NewProviderCredentialAuditEventV1(current, action)
		if err != nil {
			return ports.ProviderCredentialSwapV1{}, err
		}
		return ports.ProviderCredentialSwapV1{Applied: true, Found: true, Binding: current, AuditReceiptID: audit.ReceiptID}, nil
	}
	if store.forceCAS || (found && current.ResourceRevision != expected) || (!found && expected != 0) {
		return ports.ProviderCredentialSwapV1{}, nil
	}
	if next.Validate() != nil || next.ResourceRevision != expected+1 || (found && next.CredentialGeneration != current.CredentialGeneration+1) || (!found && next.CredentialGeneration != 1) {
		return ports.ProviderCredentialSwapV1{}, errors.New("invalid provider credential transition")
	}
	if found && current.State == domain.ProviderCredentialActiveV1 {
		store.cleanups = append(store.cleanups, ports.ProviderCredentialCleanupV1{Locator: locator, CredentialGeneration: current.CredentialGeneration, Reference: current.SecretRef})
	}
	store.bindings[locator] = next
	if store.casApplied != nil {
		select {
		case store.casApplied <- struct{}{}:
		default:
		}
	}
	action := domain.ProviderCredentialAuditRotatedV1
	if next.ResourceRevision == 1 {
		action = domain.ProviderCredentialAuditIngestedV1
	}
	audit, err := domain.NewProviderCredentialAuditEventV1(next, action)
	if err != nil {
		return ports.ProviderCredentialSwapV1{}, err
	}
	return ports.ProviderCredentialSwapV1{Applied: true, Found: true, Binding: next, AuditReceiptID: audit.ReceiptID}, nil
}

func (store *fakeBindingStore) FenceProviderCredentialCandidate(_ context.Context, candidate ports.ProviderCredentialSecretCandidateV1, _ time.Time) (ports.ProviderCredentialCandidateFenceV1, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	current, found := store.bindings[candidate.Scope.Locator]
	if found && current.State == domain.ProviderCredentialActiveV1 && current.ResourceRevision == candidate.Scope.ResourceRevision && current.CredentialGeneration == candidate.Scope.CredentialGeneration && current.CandidateMutationID == candidate.Scope.MutationID && current.SecretRef == candidate.Reference && current.SecretFingerprint == candidate.Fingerprint {
		return ports.ProviderCredentialCandidateFenceV1{Authoritative: true, Binding: current}, nil
	}
	store.fenced[candidate.Scope.MutationID] = true
	return ports.ProviderCredentialCandidateFenceV1{}, nil
}

func (store *fakeBindingStore) RevokeProviderCredential(_ context.Context, locator ports.ProviderCredentialLocatorV1, at time.Time) (ports.ProviderCredentialSwapV1, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	current, found := store.bindings[locator]
	if !found {
		return ports.ProviderCredentialSwapV1{Found: false}, nil
	}
	if current.State == domain.ProviderCredentialRevokedV1 {
		audit, err := domain.NewProviderCredentialAuditEventV1(current, domain.ProviderCredentialAuditRevokedV1)
		if err != nil {
			return ports.ProviderCredentialSwapV1{}, err
		}
		return ports.ProviderCredentialSwapV1{Found: true, Binding: current, AuditReceiptID: audit.ReceiptID}, nil
	}
	store.cleanups = append(store.cleanups, ports.ProviderCredentialCleanupV1{Locator: locator, CredentialGeneration: current.CredentialGeneration, Reference: current.SecretRef})
	current.ResourceRevision++
	current.CredentialGeneration++
	current.State, current.SecretRef, current.SecretFingerprint = domain.ProviderCredentialRevokedV1, domain.CredentialSecretRef{}, ""
	current.UpdatedAt = at
	if err := current.Validate(); err != nil {
		return ports.ProviderCredentialSwapV1{}, err
	}
	store.bindings[locator] = current
	audit, err := domain.NewProviderCredentialAuditEventV1(current, domain.ProviderCredentialAuditRevokedV1)
	if err != nil {
		return ports.ProviderCredentialSwapV1{}, err
	}
	return ports.ProviderCredentialSwapV1{Applied: true, Found: true, Binding: current, AuditReceiptID: audit.ReceiptID}, nil
}

func (store *fakeBindingStore) ListProviderCredentialCleanups(_ context.Context, locator ports.ProviderCredentialLocatorV1, limit uint64) ([]ports.ProviderCredentialCleanupV1, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]ports.ProviderCredentialCleanupV1, 0, limit)
	for _, cleanup := range store.cleanups {
		if cleanup.Locator == locator && uint64(len(result)) < limit {
			result = append(result, cleanup)
		}
	}
	return result, nil
}

func (store *fakeBindingStore) ListDueProviderCredentialCleanups(_ context.Context, _ uint32, before time.Time, _ ports.ProviderCredentialCleanupCursorV1, limit uint64) (ports.ProviderCredentialCleanupPageV1, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	page := ports.ProviderCredentialCleanupPageV1{Items: make([]ports.ProviderCredentialCleanupItemV1, 0, limit)}
	for _, cleanup := range store.cleanups {
		if uint64(len(page.Items)) >= limit {
			page.HasMore = true
			break
		}
		page.Items = append(page.Items, ports.ProviderCredentialCleanupItemV1{Cleanup: cleanup, CreatedAt: before.Add(-time.Second)})
	}
	return page, nil
}

func (store *fakeBindingStore) AcknowledgeProviderCredentialCleanup(_ context.Context, cleanup ports.ProviderCredentialCleanupV1) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	for index, candidate := range store.cleanups {
		if candidate == cleanup {
			store.cleanups = append(store.cleanups[:index], store.cleanups[index+1:]...)
			return nil
		}
	}
	return errors.New("cleanup unavailable")
}

type fakeSecretStore struct {
	mu           sync.Mutex
	next         uint64
	candidates   map[domain.CredentialSecretRef]ports.ProviderCredentialSecretCandidateV1
	committed    map[domain.CredentialSecretRef]struct{}
	values       map[domain.CredentialSecretRef][]byte
	deleted      []ports.ProviderCredentialCleanupV1
	failCommit   bool
	failDelete   bool
	putCalls     int
	readCalls    int
	recoverCalls int
}

func newFakeSecretStore() *fakeSecretStore {
	return &fakeSecretStore{candidates: map[domain.CredentialSecretRef]ports.ProviderCredentialSecretCandidateV1{}, committed: map[domain.CredentialSecretRef]struct{}{}, values: map[domain.CredentialSecretRef][]byte{}}
}

func (store *fakeSecretStore) ReadProviderCredentialSecret(_ context.Context, binding domain.ProviderCredentialBindingV1, limit int64) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.readCalls++
	if _, ok := store.committed[binding.SecretRef]; !ok {
		return nil, errors.New("secret is not committed")
	}
	value := store.values[binding.SecretRef]
	if len(value) == 0 || int64(len(value)) > limit {
		return nil, errors.New("secret size is invalid")
	}
	return append([]byte(nil), value...), nil
}

func (store *fakeSecretStore) PutProviderCredentialCandidate(_ context.Context, scope ports.ProviderCredentialCandidateScopeV1, value []byte) (ports.ProviderCredentialSecretCandidateV1, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.next++
	ref, err := domain.NewCredentialSecretRef(fmt.Sprintf("candidate-%d", store.next))
	if err != nil {
		return ports.ProviderCredentialSecretCandidateV1{}, err
	}
	candidate := ports.ProviderCredentialSecretCandidateV1{Scope: scope, Reference: ref, Fingerprint: domain.FingerprintCredential(value), CreatedAt: time.Unix(90+int64(store.next), 0).UTC()}
	store.candidates[ref] = candidate
	store.values[ref] = append([]byte(nil), value...)
	store.putCalls++
	return candidate, nil
}

func (store *fakeSecretStore) CommitProviderCredentialCandidate(_ context.Context, candidate ports.ProviderCredentialSecretCandidateV1) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.failCommit {
		store.failCommit = false
		return errors.New("ambiguous candidate commit")
	}
	if store.candidates[candidate.Reference] != candidate {
		return errors.New("candidate mismatch")
	}
	delete(store.candidates, candidate.Reference)
	store.committed[candidate.Reference] = struct{}{}
	return nil
}

func (store *fakeSecretStore) DeleteProviderCredentialCandidate(_ context.Context, candidate ports.ProviderCredentialSecretCandidateV1) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.candidates, candidate.Reference)
	zero(store.values[candidate.Reference])
	delete(store.values, candidate.Reference)
	return nil
}

func (store *fakeSecretStore) DeleteProviderCredentialSecret(_ context.Context, cleanup ports.ProviderCredentialCleanupV1) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.failDelete {
		store.failDelete = false
		return errors.New("secret cleanup unavailable")
	}
	delete(store.committed, cleanup.Reference)
	delete(store.candidates, cleanup.Reference)
	zero(store.values[cleanup.Reference])
	delete(store.values, cleanup.Reference)
	store.deleted = append(store.deleted, cleanup)
	return nil
}

func (store *fakeSecretStore) ListUncommittedProviderCredentialCandidates(_ context.Context, locator ports.ProviderCredentialLocatorV1, limit uint64) ([]ports.ProviderCredentialSecretCandidateV1, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]ports.ProviderCredentialSecretCandidateV1, 0, limit)
	for _, candidate := range store.candidates {
		if candidate.Scope.Locator == locator && uint64(len(result)) < limit {
			result = append(result, candidate)
		}
	}
	return result, nil
}

func (store *fakeSecretStore) ListAbandonedProviderCredentialCandidates(_ context.Context, before time.Time, cursor ports.ProviderCredentialCandidateCursorV1, limit uint64) (ports.ProviderCredentialCandidatePageV1, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	candidates := make([]ports.ProviderCredentialSecretCandidateV1, 0, len(store.candidates))
	for _, candidate := range store.candidates {
		if candidate.CreatedAt.Before(before) {
			candidates = append(candidates, candidate)
		}
	}
	sort.Slice(candidates, func(left, right int) bool {
		return providerCandidateSortKey(candidates[left]) < providerCandidateSortKey(candidates[right])
	})
	page := ports.ProviderCredentialCandidatePageV1{Items: make([]ports.ProviderCredentialSecretCandidateV1, 0, limit)}
	for _, candidate := range candidates {
		if cursor.Present && providerCandidateSortKey(candidate) <= providerCandidateCursorSortKey(cursor) {
			continue
		}
		if uint64(len(page.Items)) >= limit {
			page.HasMore = true
			break
		}
		page.Items = append(page.Items, candidate)
		page.NextCursor = providerCandidateCursor(candidate)
	}
	return page, nil
}

func providerCandidateSortKey(candidate ports.ProviderCredentialSecretCandidateV1) string {
	return fmt.Sprintf("%020d\x00%s\x00%s\x00%s\x00%s\x00%020d\x00%020d\x00%s\x00%s", candidate.CreatedAt.UnixMicro(), candidate.Scope.Locator.TenantID, candidate.Scope.Locator.OwnerUserID, candidate.Scope.Locator.ResourceKind, candidate.Scope.Locator.ResourceID, candidate.Scope.ResourceRevision, candidate.Scope.CredentialGeneration, candidate.Scope.MutationID, candidate.Reference.StorageValue())
}

func providerCandidateCursorSortKey(cursor ports.ProviderCredentialCandidateCursorV1) string {
	return fmt.Sprintf("%020d\x00%s\x00%s\x00%s\x00%s\x00%020d\x00%020d\x00%s\x00%s", cursor.CreatedAt.UnixMicro(), cursor.TenantID, cursor.OwnerUserID, cursor.ResourceKind, cursor.ResourceID, cursor.ResourceRevision, cursor.CredentialGeneration, cursor.MutationID, cursor.Reference.StorageValue())
}

func providerCandidateCursor(candidate ports.ProviderCredentialSecretCandidateV1) ports.ProviderCredentialCandidateCursorV1 {
	return ports.ProviderCredentialCandidateCursorV1{Present: true, CreatedAt: candidate.CreatedAt, TenantID: candidate.Scope.Locator.TenantID, OwnerUserID: candidate.Scope.Locator.OwnerUserID, ResourceKind: candidate.Scope.Locator.ResourceKind, ResourceID: candidate.Scope.Locator.ResourceID, ResourceRevision: candidate.Scope.ResourceRevision, CredentialGeneration: candidate.Scope.CredentialGeneration, MutationID: candidate.Scope.MutationID, Reference: candidate.Reference}
}

func (store *fakeSecretStore) RecoverProviderCredentialCandidate(_ context.Context, binding domain.ProviderCredentialBindingV1) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.recoverCalls++
	if _, ok := store.committed[binding.SecretRef]; ok {
		return true, nil
	}
	candidate, ok := store.candidates[binding.SecretRef]
	if !ok {
		return false, nil
	}
	if candidate.Fingerprint != binding.SecretFingerprint || candidate.Scope.CredentialGeneration != binding.CredentialGeneration || candidate.Scope.ResourceRevision != binding.ResourceRevision {
		return false, errors.New("candidate authority mismatch")
	}
	delete(store.candidates, binding.SecretRef)
	store.committed[binding.SecretRef] = struct{}{}
	return true, nil
}

func testLocator(owner string) ports.ProviderCredentialLocatorV1 {
	return ports.ProviderCredentialLocatorV1{TenantID: "tenant-a", OwnerUserID: domain.UserID(owner), ResourceKind: domain.ProviderResourceRouterAccountV1, ResourceID: "openrouter-a"}
}

func testService(t *testing.T) (*Service, *fakeAuthorizer, *fakeBindingStore, *fakeSecretStore) {
	t.Helper()
	authorizer := &fakeAuthorizer{}
	bindings, secrets := newFakeBindingStore(), newFakeSecretStore()
	service, err := New(Config{Clock: fixedClock{now: time.Unix(100, 0).UTC()}, IDs: &fixedIDs{}}, authorizer, &fakeInvocationRevoker{}, bindings, secrets)
	if err != nil {
		t.Fatal(err)
	}
	return service, authorizer, bindings, secrets
}

func TestIngestReplayRotationAndRevocationAreGenerationFenced(t *testing.T) {
	service, _, bindings, secrets := testService(t)
	revoker := service.revoker.(*fakeInvocationRevoker)
	locator := testLocator("user-a")
	request := ingestRequest(locator)
	firstSecret := []byte("openrouter-key-secret-marker-a")
	first, err := service.Ingest(context.Background(), request, firstSecret)
	if err != nil || first.Status != ports.ProviderCredentialAppliedV1 || first.Resource.Revision != 1 || first.Resource.CredentialGeneration != 1 || first.Revoked {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	replay, err := service.Ingest(context.Background(), request, firstSecret)
	if err != nil || replay.Status != ports.ProviderCredentialReplayedV1 || secrets.putCalls != 1 || secrets.recoverCalls != 0 {
		t.Fatalf("replay=%+v err=%v puts=%d recovers=%d", replay, err, secrets.putCalls, secrets.recoverCalls)
	}
	rotated, err := service.Ingest(context.Background(), request, []byte("openrouter-key-secret-marker-b"))
	if err != nil || rotated.Resource.Revision != 2 || rotated.Resource.CredentialGeneration != 2 || len(secrets.deleted) != 1 || len(bindings.cleanups) != 0 {
		t.Fatalf("rotated=%+v err=%v deleted=%d cleanup=%d", rotated, err, len(secrets.deleted), len(bindings.cleanups))
	}
	revoked, err := service.Revoke(context.Background(), revokeRequest(locator))
	if err != nil || !revoked.Revoked || revoked.Resource.Revision != 3 || revoked.Resource.CredentialGeneration != 3 || revoked.Fingerprint != "" || len(secrets.deleted) != 2 {
		t.Fatalf("revoked=%+v err=%v deleted=%d", revoked, err, len(secrets.deleted))
	}
	revokedReplay, err := service.Revoke(context.Background(), revokeRequest(locator))
	if err != nil || revokedReplay.Status != ports.ProviderCredentialReplayedV1 || revokedReplay.Resource != revoked.Resource {
		t.Fatalf("revoked replay=%+v err=%v", revokedReplay, err)
	}
	wantFences := []uint64{1, 1, 2, 3, 3}
	if fmt.Sprint(revoker.beforeGenerations) != fmt.Sprint(wantFences) {
		t.Fatalf("generation fences=%v want=%v", revoker.beforeGenerations, wantFences)
	}
}

func TestIngestRecoversAmbiguousCommitAndCleanup(t *testing.T) {
	service, _, bindings, secrets := testService(t)
	locator := testLocator("user-a")
	request := ingestRequest(locator)
	secrets.failCommit = true
	if _, err := service.Ingest(context.Background(), request, []byte("openrouter-key-a")); !errors.Is(err, ErrBackend) {
		t.Fatalf("commit loss error=%v", err)
	}
	replayed, err := service.Ingest(context.Background(), request, []byte("openrouter-key-a"))
	if err != nil || replayed.Status != ports.ProviderCredentialReplayedV1 || secrets.recoverCalls != 1 {
		t.Fatalf("recovered=%+v err=%v", replayed, err)
	}
	secrets.failDelete = true
	if _, err := service.Ingest(context.Background(), request, []byte("openrouter-key-b")); !errors.Is(err, ErrBackend) || len(bindings.cleanups) != 1 {
		t.Fatalf("cleanup loss err=%v cleanups=%d", err, len(bindings.cleanups))
	}
	replayed, err = service.Ingest(context.Background(), request, []byte("openrouter-key-b"))
	if err != nil || replayed.Status != ports.ProviderCredentialReplayedV1 || len(bindings.cleanups) != 0 {
		t.Fatalf("cleanup recovery=%+v err=%v", replayed, err)
	}
}

func TestIngestFailsClosedOnConflictOwnerAndSecretBounds(t *testing.T) {
	service, authorizer, bindings, secrets := testService(t)
	locator := testLocator("user-a")
	bindings.forceCAS = true
	if _, err := service.Ingest(context.Background(), ingestRequest(locator), []byte("openrouter-key-a")); !errors.Is(err, ErrConflict) || len(secrets.candidates) != 0 {
		t.Fatalf("CAS conflict err=%v candidates=%d", err, len(secrets.candidates))
	}
	for name, secret := range map[string][]byte{"empty": nil, "newline": []byte("key\n"), "space": []byte(" key"), "nul": []byte{'k', 0, 'y'}} {
		t.Run(name, func(t *testing.T) {
			if _, err := service.Ingest(context.Background(), ingestRequest(locator), secret); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid secret error=%v", err)
			}
		})
	}
	foreign := testLocator("user-b")
	if _, err := service.Revoke(context.Background(), revokeRequest(foreign)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign revoke error=%v", err)
	}
	authorizer.deny = true
	authorizerCalls, bindingLoads := authorizer.calls, bindings.loadCalls
	if _, err := service.Ingest(context.Background(), ingestRequest(locator), []byte("denied-key")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("denied ingress error=%v authorization calls=%d", err, authorizer.calls)
	}
	if authorizer.calls != authorizerCalls+1 || bindings.loadCalls != bindingLoads {
		t.Fatalf("denied request crossed authorization boundary: auth=%d loads=%d", authorizer.calls, bindings.loadCalls)
	}
}

func TestIngestRejectsRevisionAndGenerationOverflowBeforeCandidateWrite(t *testing.T) {
	service, _, bindings, secrets := testService(t)
	locator := testLocator("overflow-owner")
	ref, err := domain.NewCredentialSecretRef("lockbox/provider/overflow")
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.ProviderCredentialBindingV1{
		Version: domain.ProviderCredentialBindingVersionV1, TenantID: locator.TenantID, OwnerUserID: locator.OwnerUserID,
		ResourceKind: locator.ResourceKind, ResourceID: locator.ResourceID, ResourceRevision: math.MaxUint64, CredentialGeneration: math.MaxUint64,
		CandidateMutationID: "mutation-overflow", State: domain.ProviderCredentialActiveV1, SecretRef: ref,
		SecretFingerprint: domain.FingerprintCredential([]byte("current-key")), UpdatedAt: time.Unix(99, 0).UTC(),
	}
	if err := binding.Validate(); err != nil {
		t.Fatal(err)
	}
	bindings.bindings[locator] = binding
	if _, err := service.Ingest(context.Background(), ingestRequest(locator), []byte("replacement-key")); !errors.Is(err, ErrConflict) {
		t.Fatalf("overflow ingest error=%v", err)
	}
	if secrets.putCalls != 0 {
		t.Fatalf("overflow attempted candidate write %d times", secrets.putCalls)
	}
}

func TestProviderCredentialPublicSurfacesNeverContainSecret(t *testing.T) {
	service, _, bindings, _ := testService(t)
	locator := testLocator("user-a")
	marker := "openrouter-private-secret-marker"
	receipt, err := service.Ingest(context.Background(), ingestRequest(locator), []byte(marker))
	if err != nil {
		t.Fatal(err)
	}
	binding := bindings.bindings[locator]
	for name, value := range map[string]any{"receipt": receipt, "binding": binding, "error": ErrBackend} {
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil || strings.Contains(string(encoded), marker) || strings.Contains(fmt.Sprintf("%+v", value), marker) {
			t.Fatalf("%s leaked secret: json=%s format=%+v err=%v", name, encoded, value, marshalErr)
		}
	}
}

func TestIngestReconcilesAbandonedCandidateBeforeCreatingAnother(t *testing.T) {
	service, _, bindings, secrets := testService(t)
	locator := testLocator("user-a")
	scope := ports.ProviderCredentialCandidateScopeV1{Locator: locator, ResourceRevision: 1, CredentialGeneration: 1, MutationID: "mutation-abandoned"}
	abandoned, err := secrets.PutProviderCredentialCandidate(context.Background(), scope, []byte("abandoned-key"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Ingest(context.Background(), ingestRequest(locator), []byte("replacement-key"))
	if err != nil || result.Status != ports.ProviderCredentialAppliedV1 || len(bindings.bindings) != 1 {
		t.Fatalf("result=%+v err=%v bindings=%d", result, err, len(bindings.bindings))
	}
	if _, remains := secrets.candidates[abandoned.Reference]; remains {
		t.Fatal("abandoned pre-CAS candidate was not removed")
	}
}

func TestIngestRestartReconcilesCandidateAfterAmbiguousCASRollback(t *testing.T) {
	service, authorizer, bindings, secrets := testService(t)
	locator := testLocator("user-a")
	request := ingestRequest(locator)
	bindings.casError = true
	if _, err := service.Ingest(context.Background(), request, []byte("first-attempt-key")); !errors.Is(err, ErrBackend) {
		t.Fatalf("ambiguous CAS error=%v", err)
	}
	if len(secrets.candidates) != 1 || len(bindings.bindings) != 0 {
		t.Fatalf("candidate/binding after rollback candidates=%d bindings=%d", len(secrets.candidates), len(bindings.bindings))
	}
	// The production ID generator is process-independent. Reuse the same fake
	// generator to preserve that contract across this simulated restart.
	restarted, err := New(Config{Clock: fixedClock{now: time.Unix(101, 0).UTC()}, IDs: service.ids}, authorizer, &fakeInvocationRevoker{}, bindings, secrets)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := restarted.Ingest(context.Background(), request, []byte("replacement-key"))
	if err != nil || receipt.Status != ports.ProviderCredentialAppliedV1 || len(secrets.candidates) != 0 || len(bindings.bindings) != 1 {
		t.Fatalf("restart receipt=%+v err=%v candidates=%d bindings=%d", receipt, err, len(secrets.candidates), len(bindings.bindings))
	}
}

func TestDrainDueCleanupsRecoversWithoutAnotherOwnerMutation(t *testing.T) {
	service, _, bindings, secrets := testService(t)
	locator := testLocator("user-a")
	ref, err := domain.NewCredentialSecretRef("lockbox/provider/superseded")
	if err != nil {
		t.Fatal(err)
	}
	cleanup := ports.ProviderCredentialCleanupV1{Locator: locator, CredentialGeneration: 3, Reference: ref}
	bindings.cleanups = append(bindings.cleanups, cleanup)
	secrets.committed[ref] = struct{}{}
	page, err := service.DrainDueCleanups(context.Background(), 0, time.Unix(200, 0).UTC(), ports.ProviderCredentialCleanupCursorV1{})
	if err != nil || len(page.Items) != 1 || len(bindings.cleanups) != 0 {
		t.Fatalf("page=%+v err=%v cleanups=%d", page, err, len(bindings.cleanups))
	}
	if _, exists := secrets.committed[ref]; exists {
		t.Fatal("due cleanup did not delete the superseded secret")
	}
}

func TestDrainAbandonedCandidatesIsGlobalAndPreservesExactAuthority(t *testing.T) {
	service, _, bindings, secrets := testService(t)
	abandonedLocator := testLocator("user-a")
	abandoned, err := secrets.PutProviderCredentialCandidate(context.Background(), ports.ProviderCredentialCandidateScopeV1{Locator: abandonedLocator, ResourceRevision: 1, CredentialGeneration: 1, MutationID: "mutation-abandoned"}, []byte("abandoned-key"))
	if err != nil {
		t.Fatal(err)
	}
	authoritativeLocator := testLocator("user-b")
	authoritative, err := secrets.PutProviderCredentialCandidate(context.Background(), ports.ProviderCredentialCandidateScopeV1{Locator: authoritativeLocator, ResourceRevision: 1, CredentialGeneration: 1, MutationID: "mutation-authoritative"}, []byte("authoritative-key"))
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.ProviderCredentialBindingV1{Version: domain.ProviderCredentialBindingVersionV1, TenantID: authoritativeLocator.TenantID, OwnerUserID: authoritativeLocator.OwnerUserID, ResourceKind: authoritativeLocator.ResourceKind, ResourceID: authoritativeLocator.ResourceID, ResourceRevision: 1, CredentialGeneration: 1, CandidateMutationID: authoritative.Scope.MutationID, State: domain.ProviderCredentialActiveV1, SecretRef: authoritative.Reference, SecretFingerprint: authoritative.Fingerprint, UpdatedAt: time.Unix(95, 0).UTC()}
	if err := binding.Validate(); err != nil {
		t.Fatal(err)
	}
	bindings.bindings[authoritativeLocator] = binding
	page, err := service.DrainAbandonedCandidates(context.Background(), time.Unix(100, 0).UTC(), ports.ProviderCredentialCandidateCursorV1{})
	if err != nil || len(page.Items) != 2 || !page.NextCursor.Present {
		t.Fatalf("candidate page=%+v err=%v", page, err)
	}
	if _, exists := secrets.candidates[abandoned.Reference]; exists {
		t.Fatal("unbound candidate survived autonomous cleanup")
	}
	if _, exists := secrets.committed[authoritative.Reference]; !exists {
		t.Fatal("exact binding candidate was deleted instead of recovered")
	}
}

func TestCandidateCleanupAndBindingCASHaveOneSerializableWinner(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		service, _, bindings, secrets := testService(t)
		locator := testLocator(fmt.Sprintf("race-owner-%d", iteration))
		scope := ports.ProviderCredentialCandidateScopeV1{
			Locator: locator, ResourceRevision: 1, CredentialGeneration: 1,
			MutationID: fmt.Sprintf("candidate-race-%d", iteration),
		}
		candidate, err := secrets.PutProviderCredentialCandidate(context.Background(), scope, []byte("race-candidate-key"))
		if err != nil {
			t.Fatal(err)
		}
		next := domain.ProviderCredentialBindingV1{
			Version: domain.ProviderCredentialBindingVersionV1, TenantID: locator.TenantID, OwnerUserID: locator.OwnerUserID,
			ResourceKind: locator.ResourceKind, ResourceID: locator.ResourceID, ResourceRevision: 1, CredentialGeneration: 1,
			CandidateMutationID: scope.MutationID, State: domain.ProviderCredentialActiveV1, SecretRef: candidate.Reference,
			SecretFingerprint: candidate.Fingerprint, UpdatedAt: time.Unix(100, 0).UTC(),
		}

		start := make(chan struct{})
		drainResult := make(chan error, 1)
		swapResult := make(chan ports.ProviderCredentialSwapV1, 1)
		go func() {
			<-start
			_, drainErr := service.DrainAbandonedCandidates(context.Background(), time.Unix(100, 0).UTC(), ports.ProviderCredentialCandidateCursorV1{})
			drainResult <- drainErr
		}()
		go func() {
			<-start
			swap, swapErr := bindings.CompareAndSwapProviderCredential(context.Background(), 0, next)
			if swapErr != nil {
				t.Errorf("iteration %d: swap error: %v", iteration, swapErr)
			}
			swapResult <- swap
		}()
		close(start)
		if err := <-drainResult; err != nil {
			t.Fatalf("iteration %d: drain error: %v", iteration, err)
		}
		swap := <-swapResult

		bindings.mu.Lock()
		stored, bound := bindings.bindings[locator]
		fenced := bindings.fenced[scope.MutationID]
		bindings.mu.Unlock()
		secrets.mu.Lock()
		_, pending := secrets.candidates[candidate.Reference]
		_, committed := secrets.committed[candidate.Reference]
		secrets.mu.Unlock()
		if bound {
			if !swap.Applied || stored != next || !committed || pending || fenced {
				t.Fatalf("iteration %d: CAS winner invariant failed: swap=%+v bound=%t committed=%t pending=%t fenced=%t", iteration, swap, bound, committed, pending, fenced)
			}
			continue
		}
		if swap.Applied || !fenced || pending || committed {
			t.Fatalf("iteration %d: cleanup winner invariant failed: swap=%+v bound=%t committed=%t pending=%t fenced=%t", iteration, swap, bound, committed, pending, fenced)
		}
	}
}

func locatorFor(binding domain.ProviderCredentialBindingV1) ports.ProviderCredentialLocatorV1 {
	return ports.ProviderCredentialLocatorV1{TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, ResourceKind: binding.ResourceKind, ResourceID: binding.ResourceID}
}

func ingestRequest(locator ports.ProviderCredentialLocatorV1) ports.ProviderCredentialIngestRequestV1 {
	return ports.ProviderCredentialIngestRequestV1{
		Principal:    ports.ProviderCredentialPrincipalV1{TenantID: locator.TenantID, OwnerUserID: locator.OwnerUserID, Channel: ports.ProviderCredentialChannelLocalOperatorV1},
		ResourceKind: locator.ResourceKind, ResourceID: locator.ResourceID,
	}
}

func revokeRequest(locator ports.ProviderCredentialLocatorV1) ports.ProviderCredentialRevokeRequestV1 {
	return ports.ProviderCredentialRevokeRequestV1{
		Principal:    ports.ProviderCredentialPrincipalV1{TenantID: locator.TenantID, OwnerUserID: locator.OwnerUserID, Channel: ports.ProviderCredentialChannelLocalOperatorV1},
		ResourceKind: locator.ResourceKind, ResourceID: locator.ResourceID,
	}
}

var _ ports.ProviderCredentialSecretStore = (*fakeSecretStore)(nil)
