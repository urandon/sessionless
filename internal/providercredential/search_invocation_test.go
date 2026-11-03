package providercredential

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func searchInvocationFixture(t *testing.T) (*InvocationService, *fakeBindingStore, *fakeSecretStore, ports.SearchCredentialIssueRequestV1, ports.ProviderCredentialIssueRequestV1) {
	t.Helper()
	s, bindings, secrets, model, _ := invocationFixture(t, domain.ProviderCredentialDeliveryDirectV1)
	r := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceWebSearchAccountV1, ResourceID: "search-account", OwnerUserID: model.HarnessBinding.OwnerUserID, Revision: 3, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 7}
	ref, err := domain.NewCredentialSecretRef("synthetic-search-key-reference")
	if err != nil {
		t.Fatal(err)
	}
	marker := []byte("synthetic-search-key-not-the-model-key")
	b := domain.ProviderCredentialBindingV1{Version: 1, TenantID: model.HarnessBinding.TenantID, OwnerUserID: r.OwnerUserID, ResourceKind: r.Kind, ResourceID: r.ResourceID, ResourceRevision: r.Revision, CredentialGeneration: r.CredentialGeneration, CandidateMutationID: "search-mutation", State: domain.ProviderCredentialActiveV1, SecretRef: ref, SecretFingerprint: domain.FingerprintCredential(marker), UpdatedAt: s.clock.Now()}
	if b.Validate() != nil {
		t.Fatalf("invalid search binding: %+v", b)
	}
	bindings.bindings[locatorFor(b)] = b
	secrets.committed[ref], secrets.values[ref] = struct{}{}, marker
	h := strings.Repeat("a", 64)
	scope := domain.AttemptOperationScopeV1{TenantID: b.TenantID, SessionID: "session-a", OwnerUserID: b.OwnerUserID, RunID: model.HarnessBinding.RunID, AttemptID: model.HarnessBinding.AttemptID, LeaseID: model.LeaseID, FenceToken: model.LeaseFence, PhysicalInvocationClaimID: "physical-claim", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(h), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(h), HarnessBindingDigest: domain.HarnessBindingDigestV2(h), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(h)}
	req := ports.SearchCredentialIssueRequestV1{Declaration: ports.SearchCredentialDeclarationV1{Version: 1, TenantID: b.TenantID, Resource: r, Audience: domain.WebSearchCredentialAudienceV1, DeliveryKind: domain.ProviderCredentialDeliveryDirectV1}, Correlation: ports.SearchCredentialCorrelationV1{Scope: scope, OperationDigest: domain.AttemptOperationDigestV1(h), ConsentRequestDigest: domain.WebSearchConsentRequestDigestV1(h), ConsentDecisionDigest: domain.WebSearchConsentDecisionDigestV1(h)}, WorkerID: model.WorkerID, ExpiresAt: model.ExpiresAt}
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
	return s, bindings, secrets, req, model
}

func TestSearchInvocationDistinctOneShotSharedRegistry(t *testing.T) {
	s, _, secrets, req, model := searchInvocationFixture(t)
	m, err := s.IssueProviderCredential(t.Context(), model)
	if err != nil {
		t.Fatal(err)
	}
	h, err := s.IssueSearchCredential(t.Context(), req)
	if err != nil || h.Validate() != nil || h.Credential.HandleID == m.HandleID || len(s.handles) != 2 || secrets.readCalls != 0 {
		t.Fatalf("issue search=%+v err=%v handles=%d reads=%d", h, err, len(s.handles), secrets.readCalls)
	}
	if err := s.MaterializeProviderCredential(t.Context(), h.Credential, func(ports.ProviderCredentialMaterializationV1, []byte) error {
		t.Error("SEARCH reached MODEL consumer")
		return nil
	}); err == nil {
		t.Fatal("MODEL materialized SEARCH")
	}
	if err := s.ReleaseProviderCredential(t.Context(), h.Credential); err == nil {
		t.Fatal("MODEL released SEARCH")
	}
	foreign := h
	foreign.Credential.HandleID = m.HandleID
	if err := s.MaterializeSearchCredential(t.Context(), foreign, func(ports.ProviderCredentialMaterializationV1, []byte) error {
		t.Error("MODEL key reached SEARCH")
		return nil
	}); err == nil {
		t.Fatal("SEARCH accepted MODEL handle ID")
	}
	var borrowed []byte
	if err := s.MaterializeSearchCredential(t.Context(), h, func(plan ports.ProviderCredentialMaterializationV1, b []byte) error {
		if plan.Validate() != nil || plan.Kind != domain.ProviderCredentialDeliveryDirectV1 || string(b) != "synthetic-search-key-not-the-model-key" {
			t.Errorf("wrong SEARCH delivery plan=%+v", plan)
		}
		borrowed = b
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, b := range borrowed {
		if b != 0 {
			t.Fatal("borrowed SEARCH bytes not zeroed after return")
		}
	}
	if err := s.MaterializeSearchCredential(t.Context(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error {
		t.Error("replayed SEARCH consumer")
		return nil
	}); !errors.Is(err, ErrConsumed) {
		t.Fatalf("replay err=%v want=%v", err, ErrConsumed)
	}
	if err := s.MaterializeProviderCredential(t.Context(), m, func(_ ports.ProviderCredentialMaterializationV1, b []byte) error {
		if string(b) != "openrouter-private-invocation-marker" {
			t.Error("SEARCH key crossed to MODEL")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseSearchCredential(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseSearchCredential(t.Context(), h); err != nil {
		t.Fatal("exact release not idempotent", err)
	}
	if err := s.ReleaseProviderCredential(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	if len(s.handles) != 0 || secrets.readCalls != 2 {
		t.Fatalf("remaining handles=%d reads=%d", len(s.handles), secrets.readCalls)
	}
}

func TestSearchInvocationCurrentBindingAndCorrelationDeny(t *testing.T) {
	for _, name := range []string{"audience", "delivery", "owner", "tenant", "revision", "generation", "revoked", "receipt", "claim", "operation", "expiry"} {
		t.Run(name, func(t *testing.T) {
			s, bindings, secrets, req, _ := searchInvocationFixture(t)
			h, err := s.IssueSearchCredential(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.ReleaseSearchCredential(context.Background(), h); err != nil {
					t.Error(err)
				}
			}()
			bad := h
			switch name {
			case "audience":
				bad.Declaration.Audience = "model"
			case "delivery":
				bad.Declaration.DeliveryKind = domain.ProviderCredentialDeliveryFileV1
			case "owner":
				bad.Credential.OwnerUserID = "foreign-owner"
			case "tenant":
				bad.Credential.TenantID = "foreign-tenant"
			case "receipt":
				bad.Correlation.ConsentDecisionDigest = domain.WebSearchConsentDecisionDigestV1(strings.Repeat("b", 64))
			case "claim":
				bad.Correlation.Scope.PhysicalInvocationClaimID = "foreign-claim"
			case "operation":
				bad.Correlation.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("b", 64))
			case "expiry":
				bad.Credential.ExpiresAt = s.clock.Now()
			default:
				locator := ports.ProviderCredentialLocatorV1{TenantID: req.Declaration.TenantID, OwnerUserID: req.Declaration.Resource.OwnerUserID, ResourceKind: req.Declaration.Resource.Kind, ResourceID: req.Declaration.Resource.ResourceID}
				b := bindings.bindings[locator]
				if name == "revision" {
					b.ResourceRevision++
				}
				if name == "generation" {
					b.CredentialGeneration++
				}
				if name == "revoked" {
					b.State, b.SecretRef, b.SecretFingerprint = domain.ProviderCredentialRevokedV1, domain.CredentialSecretRef{}, ""
				}
				bindings.bindings[locator] = b
			}
			if err := s.MaterializeSearchCredential(t.Context(), bad, func(ports.ProviderCredentialMaterializationV1, []byte) error {
				t.Error("unauthorized SEARCH consumer")
				return nil
			}); err == nil || secrets.readCalls != 0 {
				t.Fatalf("case=%s err=%v secret reads=%d want denial before read", name, err, secrets.readCalls)
			}
		})
	}
}

func TestSearchInvocationFenceJoinsExistingConsumer(t *testing.T) {
	s, _, _, req, _ := searchInvocationFixture(t)
	h, err := s.IssueSearchCredential(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	stopConsumer := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(stopConsumer)
	materialized, fenced := make(chan struct{}), make(chan struct{})
	var materializeErr, fenceErr error
	go func() {
		defer close(materialized)
		materializeErr = s.MaterializeSearchCredential(t.Context(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error { close(entered); <-release; return nil })
	}()
	t.Cleanup(func() { stopConsumer(); waitImmutableEvent(t, materialized, "SEARCH consumer cleanup") })
	waitImmutableEvent(t, entered, "SEARCH consumer entered")
	state := s.handles[h.Credential.HandleID]
	if state.mu.TryLock() {
		state.mu.Unlock()
		t.Fatal("SEARCH consumer did not retain the shared fence mutex")
	}
	locator := ports.ProviderCredentialLocatorV1{TenantID: req.Declaration.TenantID, OwnerUserID: req.Declaration.Resource.OwnerUserID, ResourceKind: req.Declaration.Resource.Kind, ResourceID: req.Declaration.Resource.ResourceID}
	fenceStarted := make(chan struct{})
	go func() {
		defer close(fenced)
		close(fenceStarted)
		fenceErr = s.FenceProviderCredentialInvocations(t.Context(), locator, req.Declaration.Resource.CredentialGeneration+1)
	}()
	t.Cleanup(func() { stopConsumer(); waitImmutableEvent(t, fenced, "SEARCH fence cleanup") })
	waitImmutableEvent(t, fenceStarted, "SEARCH fence initiated")
	stopConsumer()
	waitImmutableEvent(t, materialized, "SEARCH consumer joined")
	if materializeErr != nil {
		t.Error(materializeErr)
	}
	waitImmutableEvent(t, fenced, "SEARCH fence joined")
	if fenceErr != nil {
		t.Error(fenceErr)
	}
	if err := s.MaterializeSearchCredential(t.Context(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error {
		t.Error("revoked consumer ran")
		return nil
	}); err == nil {
		t.Error("revoked search materialized")
	}
	if err := s.ReleaseSearchCredential(t.Context(), h); err != nil {
		t.Error(err)
	}
}

type searchReadBarrierSecretStore struct {
	*fakeSecretStore
	afterRead func()
	lastRead  []byte
	readError error
}

func (s *searchReadBarrierSecretStore) ReadProviderCredentialSecret(ctx context.Context, b domain.ProviderCredentialBindingV1, limit int64) ([]byte, error) {
	value, err := s.fakeSecretStore.ReadProviderCredentialSecret(ctx, b, limit)
	s.lastRead = value
	if s.afterRead != nil {
		s.afterRead()
	}
	if s.readError != nil {
		return value, s.readError
	}
	return value, err
}

func TestSearchInvocationCancellationAndExpiryBeforeBorrow(t *testing.T) {
	for _, name := range []string{"cancelled-before-read", "cancelled-during-read", "expired-before-read", "expired-during-read"} {
		t.Run(name, func(t *testing.T) {
			s, _, secrets, req, _ := searchInvocationFixture(t)
			clock := &mutableClock{now: s.clock.Now()}
			s.clock = clock
			h, err := s.IssueSearchCredential(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.ReleaseSearchCredential(context.Background(), h); err != nil {
					t.Error(err)
				}
			}()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if name == "cancelled-before-read" {
				cancel()
			}
			if name == "expired-before-read" {
				clock.Set(req.ExpiresAt)
			}
			var barrier *searchReadBarrierSecretStore
			if name == "cancelled-during-read" || name == "expired-during-read" {
				barrier = &searchReadBarrierSecretStore{fakeSecretStore: secrets, afterRead: func() {
					if name == "cancelled-during-read" {
						cancel()
					} else {
						clock.Set(req.ExpiresAt)
					}
				}}
				s.secrets = barrier
			}
			if err := s.MaterializeSearchCredential(ctx, h, func(ports.ProviderCredentialMaterializationV1, []byte) error {
				t.Error("cancelled/expired SEARCH reached borrower")
				return nil
			}); err == nil {
				t.Fatalf("case=%s expected cancellation/expiry denial", name)
			}
			if (name == "cancelled-before-read" || name == "expired-before-read") && secrets.readCalls != 0 {
				t.Fatalf("case=%s secret reads=%d want0", name, secrets.readCalls)
			}
			if barrier != nil {
				for _, b := range barrier.lastRead {
					if b != 0 {
						t.Fatal("canceled/expired custody read retained owned secret bytes")
					}
				}
			}
			if err := s.MaterializeSearchCredential(context.Background(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error {
				t.Error("failed SEARCH reopened materialization")
				return nil
			}); err == nil {
				t.Fatal("failed SEARCH materialization replayed")
			}
		})
	}
}

func TestSearchInvocationClearsSecretOnCallbackAndCustodyErrors(t *testing.T) {
	for _, name := range []string{"callback-error", "read-error"} {
		t.Run(name, func(t *testing.T) {
			s, _, secrets, req, _ := searchInvocationFixture(t)
			h, err := s.IssueSearchCredential(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.ReleaseSearchCredential(context.Background(), h); err != nil {
					t.Error(err)
				}
			}()
			barrier := &searchReadBarrierSecretStore{fakeSecretStore: secrets}
			if name == "read-error" {
				barrier.readError = errors.New("synthetic read failure with returned owned buffer")
			}
			s.secrets = barrier
			calls := 0
			if err := s.MaterializeSearchCredential(t.Context(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error {
				calls++
				return errors.New("synthetic consumer failure")
			}); !errors.Is(err, ErrBackend) {
				t.Fatalf("error=%v want=%v", err, ErrBackend)
			}
			wantCalls := 1
			if name == "read-error" {
				wantCalls = 0
			}
			if calls != wantCalls || len(barrier.lastRead) == 0 {
				t.Fatalf("callbacks=%d want=%d owned bytes=%d", calls, wantCalls, len(barrier.lastRead))
			}
			for _, b := range barrier.lastRead {
				if b != 0 {
					t.Fatal("error path retained owned credential bytes")
				}
			}
			if err := s.MaterializeSearchCredential(t.Context(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error {
				t.Error("error reopened one-shot materialization")
				return nil
			}); !errors.Is(err, ErrConsumed) {
				t.Fatalf("replay error=%v want=%v", err, ErrConsumed)
			}
		})
	}
}

func TestSearchInvocationIssueAuthenticatesExactCurrentCredential(t *testing.T) {
	for _, name := range []string{"foreign-tenant-row", "foreign-owner-row", "revision", "generation", "revoked", "expired", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			s, bindings, secrets, req, _ := searchInvocationFixture(t)
			locator := ports.ProviderCredentialLocatorV1{TenantID: req.Declaration.TenantID, OwnerUserID: req.Declaration.Resource.OwnerUserID, ResourceKind: req.Declaration.Resource.Kind, ResourceID: req.Declaration.Resource.ResourceID}
			b := bindings.bindings[locator]
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch name {
			case "foreign-tenant-row":
				b.TenantID = "foreign-tenant"
			case "foreign-owner-row":
				b.OwnerUserID = "foreign-owner"
			case "revision":
				b.ResourceRevision++
			case "generation":
				b.CredentialGeneration++
			case "revoked":
				b.State, b.SecretRef, b.SecretFingerprint = domain.ProviderCredentialRevokedV1, domain.CredentialSecretRef{}, ""
			case "expired":
				req.ExpiresAt = s.clock.Now().Add(-time.Microsecond)
			case "cancelled":
				cancel()
			}
			bindings.bindings[locator] = b
			if h, err := s.IssueSearchCredential(ctx, req); err == nil || h != (ports.SearchInvocationCredentialV1{}) || len(s.handles) != 0 || secrets.readCalls != 0 {
				t.Fatalf("case=%s handle=%+v err=%v handles=%d reads=%d", name, h, err, len(s.handles), secrets.readCalls)
			}
		})
	}
}
