package ydbstore_test

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"net/url"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegeneration"
	"gitcode.com/urandon/sessionless/internal/attachedworkeronboarding"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
	"gitcode.com/urandon/sessionless/test/support/aw07native"
)

// Native SQL/JsonDocument/CAS qualification only. Ready uses signed synthetic
// protocol input; no native harness, real credential or provider is invoked.
type nativeGenerationFixture struct {
	store    *ydbstore.Store
	db       *sql.DB
	ctx      context.Context
	auth     ports.AttachedResourceGenerationAuthorizationV1
	original nativeGenerationMetadata
}

type nativeGenerationMetadata struct {
	actor, provider, credential, entitlement, quota string
	observed                                        time.Time
}

func newNativeGenerationFixture(t *testing.T) *nativeGenerationFixture {
	t.Helper()
	// Disabled means SKIP before reading the DSN or opening any dependency.
	if os.Getenv("SESSIONLESS_OWNED_CURRENT_YDB_GATE") != "1" {
		t.Skip("explicit disposable attached resource generation YDB gate not requested")
	}
	u, err := url.Parse(os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil || u == nil || os.Getenv("SESSIONLESS_OWNED_CURRENT_YDB_DISPOSABLE") != "1" ||
		(u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		t.Fatal("generation native gate requires an explicit disposable loopback database")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(query["go_query_mode"]) != 1 || query.Get("go_query_mode") != "data" || query.Has("go_fake_tx") {
		t.Fatal("generation native gate requires true data transactions and forbids fake transactions")
	}
	store, db, ctx, tenant := ydbstore.OpenTerminalHistoryNativeV1(t)
	ready := aw07native.Ready(t, store, db, aw07native.ReadyOptions{Suffix: "generation-native", TenantID: tenant})
	worker := ready.Worker
	resource := attachedworkeronboarding.ResourceIDForWorker(tenant, worker.OwnerUserID, worker.ID)
	actor := attachedworkeronboarding.ActorIDForOwner(tenant, worker.OwnerUserID)
	if string(actor) == string(worker.OwnerUserID) {
		t.Fatal("native fixture must distinguish actor ID from owner user ID")
	}
	// The shared bridge already cleans its fresh tenant's worker/resource rows.
	// These two omitted projections are armed BEFORE registration can write,
	// and only our exact independently generated keys may be deleted.
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := db.ExecContext(cleanup, `DELETE FROM subscription_connections_by_user WHERE tenant_id=$1 AND user_id=$2 AND subscription_connection_id=$3`, tenant, worker.OwnerUserID, resource); err != nil {
			t.Errorf("cleanup exact generation owner projection: %v", err)
		}
		if _, err := db.ExecContext(cleanup, `DELETE FROM actors WHERE tenant_id=$1 AND actor_id=$2`, tenant, actor); err != nil {
			t.Errorf("cleanup exact generation actor: %v", err)
		}
	})
	registered, err := store.OnboardingStore().RegisterAttachedComputeResource(ctx, ports.AttachedComputeRegistration{
		Worker: worker, ResourceID: resource, ActorID: actor,
	})
	if err != nil || registered.ID != resource || registered.Provider != "codex" || registered.Entitlement != domain.EntitlementUnknown || registered.Quota != domain.ProviderQuotaUnknown {
		t.Fatalf("actual normal resource registration: err=%v resource=%s provider=%s entitlement=%s quota=%s", err, registered.ID, registered.Provider, registered.Entitlement, registered.Quota)
	}
	baseline := nativeGenerationBaseline(t, ready, resource)
	registry, err := attachedresourcegeneration.NewRegistry([]domain.AttachedResourceGenerationBaselineV1{baseline})
	if err != nil {
		t.Fatal(err)
	}
	// Same real database and signed current worker; no second metadata store,
	// source setter or alternate authority is introduced for native tests.
	configured, err := ydbstore.New(db, ydbstore.Options{AttachedResourceGenerations: registry})
	if err != nil {
		t.Fatal(err)
	}
	f := &nativeGenerationFixture{store: configured, db: db, ctx: ctx,
		auth: ports.AttachedResourceGenerationAuthorizationV1{TenantID: tenant, OwnerUserID: worker.OwnerUserID,
			SubscriptionConnectionID: resource, WorkerID: worker.ID, EnrollmentGeneration: worker.EnrollmentGeneration,
			BaselineRevision: baseline.BaselineRevision, ConnectionID: ready.Connection.ID,
			ConnectionGeneration: ready.Connection.ConnectionGeneration, PresentedSecretDigest: ready.SecretDigest}}
	var encoded sql.NullString
	f.original, encoded = f.stored(t, ctx)
	if f.original.actor != string(actor) || f.original.credential != "" || encoded.Valid {
		t.Fatal("normal onboarding produced unexpected identity, credential custody or generation metadata")
	}
	return f
}

func nativeGenerationBaseline(t *testing.T, ready aw07native.ReadyV1, resource domain.SubscriptionConnectionID) domain.AttachedResourceGenerationBaselineV1 {
	t.Helper()
	raw, err := os.ReadFile("../../test/fixtures/provider-conformance/v1/openrouter-ox-alpha-public.json")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := harnessconformance.DecodeFixtureV1(raw)
	if err != nil || fixture.EvidenceBundle == nil {
		t.Fatalf("committed synthetic generation evidence: %v", err)
	}
	binding, bundle := fixture.Binding.Clone(), fixture.EvidenceBundle.Clone()
	binding.TenantID, binding.OwnerUserID = ready.Worker.TenantID, ready.Worker.OwnerUserID
	binding.RunID, binding.AttemptID, binding.ExecutionPlacementDigest = "", "", ""
	binding.Backend.BackendKind, binding.Backend.ArtifactKind = domain.HarnessBackendCodexExecV1, domain.HarnessArtifactExecutableV1
	binding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
	binding.Resource.Kind, binding.Resource.ResourceID, binding.Resource.OwnerUserID = domain.ProviderResourceSubscriptionV1, string(resource), ready.Worker.OwnerUserID
	binding.Resource.CredentialGeneration = 1
	for _, scope := range []*domain.ProviderEvidenceScopeV1{&bundle.Catalog.Scope, &bundle.Route.Scope, &bundle.Capability.Scope, &bundle.Privacy.Scope, &bundle.Price.Scope, &bundle.Policy.Scope} {
		*scope = domain.ProviderEvidenceScopeV1{TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, Resource: binding.Resource, Backend: binding.Backend}
	}
	bundle.Route.State, bundle.Route.PolicyID, bundle.Route.Revision, bundle.Route.Routes = domain.ProviderEvidenceUnknownV1, "", 0, nil
	// Construct synthetic evidence once from the SAME DB clock as Ready.
	// Publication/projection must never renew these original observations.
	expires := ready.At.Add(time.Hour)
	for _, window := range []struct{ observed, expires *time.Time }{
		{&bundle.Catalog.ObservedAt, &bundle.Catalog.ExpiresAt}, {&bundle.Route.ObservedAt, &bundle.Route.ExpiresAt},
		{&bundle.Capability.ObservedAt, &bundle.Capability.ExpiresAt}, {&bundle.Privacy.ObservedAt, &bundle.Privacy.ExpiresAt},
		{&bundle.Price.ObservedAt, &bundle.Price.ExpiresAt}, {&bundle.Policy.ObservedAt, &bundle.Policy.ExpiresAt},
	} {
		*window.observed, *window.expires = ready.At.Add(-time.Minute), expires
	}
	binding.EvidenceExpiresAt = &expires
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
	return domain.AttachedResourceGenerationBaselineV1{Version: 1, TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID,
		SubscriptionConnectionID: resource, WorkerID: ready.Worker.ID, EnrollmentGeneration: ready.Worker.EnrollmentGeneration,
		BaselineRevision: binding.Resource.Revision, CapabilityDigest: ready.Connection.CapabilityDigest,
		GenerationOnlyDelegation: true, Binding: binding, Evidence: bundle}
}

func (f *nativeGenerationFixture) head(generation uint64, revoked bool) domain.AttachedResourceGenerationHeadV1 {
	return domain.AttachedResourceGenerationHeadV1{Version: 1, OwnerUserID: f.auth.OwnerUserID, WorkerID: f.auth.WorkerID,
		EnrollmentGeneration: f.auth.EnrollmentGeneration, BaselineRevision: f.auth.BaselineRevision,
		CredentialGeneration: generation, Revoked: revoked}
}

func (f *nativeGenerationFixture) stored(t *testing.T, ctx context.Context) (nativeGenerationMetadata, sql.NullString) {
	t.Helper()
	var metadata nativeGenerationMetadata
	var encoded sql.NullString
	err := f.db.QueryRowContext(ctx, `SELECT actor_id,provider,credential_ref,entitlement_state,quota_state,observed_at,
 SUBSTRING(CAST(attached_resource_generation_v1 AS String),0,4097)
 FROM subscription_connections WHERE tenant_id=$1 AND subscription_connection_id=$2`,
		f.auth.TenantID, f.auth.SubscriptionConnectionID).Scan(&metadata.actor, &metadata.provider, &metadata.credential,
		&metadata.entitlement, &metadata.quota, &metadata.observed, &encoded)
	if err != nil {
		t.Fatalf("actual generation JsonDocument/metadata SQL read: %v", err)
	}
	metadata.observed = metadata.observed.UTC()
	if encoded.Valid && len(encoded.String) > domain.AttachedResourceGenerationMetadataMaxBytesV1 {
		t.Fatalf("stored generation metadata bytes=%d exceeds bound", len(encoded.String))
	}
	return metadata, encoded
}

func (f *nativeGenerationFixture) requireStored(t *testing.T, ctx context.Context, expected domain.AttachedResourceGenerationHeadV1) {
	t.Helper()
	metadata, raw := f.stored(t, ctx)
	if !reflect.DeepEqual(metadata, f.original) {
		t.Fatal("publication changed actor/provider/entitlement/quota/observed_at/credential_ref")
	}
	if !raw.Valid {
		t.Fatal("acknowledged generation has no persisted JsonDocument")
	}
	// MUST use the strict numeric codec without float/string coercion. In
	// particular MaxUint64 cannot be accepted as a rounded or exponent value.
	head, err := domain.DecodeAttachedResourceGenerationHeadV1([]byte(raw.String))
	if err != nil || head != expected {
		t.Fatalf("actual stored JsonDocument failed lossless exact-head roundtrip: err=%v generation=%d want=%d revoked=%t want=%t bytes=%d", err, head.CredentialGeneration, expected.CredentialGeneration, head.Revoked, expected.Revoked, len(raw.String))
	}
}

func (f *nativeGenerationFixture) publishAndRead(t *testing.T, expected uint64, target, stored domain.AttachedResourceGenerationHeadV1, status ports.AttachedWorkerExecutionStatus) {
	t.Helper()
	// One mutation and its one explicit point read retain this SAME window.
	call, cancel := context.WithTimeout(f.ctx, ports.AttachedResourceGenerationOperationTimeoutV1)
	defer cancel()
	result, err := f.store.PublishAttachedResourceGenerationV1(call, ports.AttachedResourceGenerationPublicationV1{Authorization: f.auth, ExpectedCredentialGeneration: expected, Target: target})
	if err != nil || result.Status != status {
		t.Fatalf("native publication expected=%d target=%d revoked=%t status=%s want=%s err=%v", expected, target.CredentialGeneration, target.Revoked, result.Status, status, err)
	}
	if status == ports.AttachedWorkerExecutionConflict {
		if result.Head != (domain.AttachedResourceGenerationHeadV1{}) {
			t.Fatal("conflicting publication exposed an acknowledged head")
		}
	} else if result.Head != target {
		t.Fatal("native publication acknowledged a different head")
	}
	f.requireStored(t, call, stored)
	read, err := f.store.ReadAttachedResourceGenerationV1(call, f.auth)
	if err != nil || read.Status != ports.AttachedWorkerExecutionApplied || read.Head != stored {
		t.Fatalf("actual authenticated exact point read: status=%s generation=%d want=%d err=%v", read.Status, read.Head.CredentialGeneration, stored.CredentialGeneration, err)
	}
}

func TestAttachedResourceGenerationYDBPublicationAndLosslessMaximum(t *testing.T) {
	f := newNativeGenerationFixture(t)
	one, two := f.head(1, false), f.head(2, false)
	f.publishAndRead(t, 0, one, one, ports.AttachedWorkerExecutionApplied)
	f.publishAndRead(t, 0, one, one, ports.AttachedWorkerExecutionReplayed)
	f.publishAndRead(t, 1, two, two, ports.AttachedWorkerExecutionApplied)
	f.publishAndRead(t, 0, f.head(3, false), two, ports.AttachedWorkerExecutionConflict)
	final := f.head(math.MaxUint64, true)
	f.publishAndRead(t, 2, final, final, ports.AttachedWorkerExecutionApplied)
	f.publishAndRead(t, 2, final, final, ports.AttachedWorkerExecutionReplayed)
	f.publishAndRead(t, math.MaxUint64, f.head(3, false), final, ports.AttachedWorkerExecutionConflict)
}

func TestAttachedResourceGenerationYDBConcurrentExactCAS(t *testing.T) {
	f := newNativeGenerationFixture(t)
	one, two := f.head(1, false), f.head(2, false)
	f.publishAndRead(t, 0, one, one, ports.AttachedWorkerExecutionApplied)
	call, cancel := context.WithTimeout(f.ctx, ports.AttachedResourceGenerationOperationTimeoutV1)
	defer cancel()
	type outcome struct {
		result ports.AttachedResourceGenerationResultV1
		err    error
	}
	ready, start, finished := make(chan struct{}, 2), make(chan struct{}), make(chan outcome, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	for range 2 {
		go func() {
			defer workers.Done()
			ready <- struct{}{}
			select {
			case <-start:
				result, err := f.store.PublishAttachedResourceGenerationV1(call, ports.AttachedResourceGenerationPublicationV1{Authorization: f.auth, ExpectedCredentialGeneration: 1, Target: two})
				finished <- outcome{result, err}
			case <-call.Done():
				finished <- outcome{err: call.Err()}
			}
		}()
	}
	joined := make(chan struct{})
	go func() { workers.Wait(); close(joined) }()
	t.Cleanup(func() {
		cancel()
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		select {
		case <-joined:
		case <-cleanup.Done():
			t.Error("native generation CAS workers did not join before owned-row cleanup")
		}
	})
	released := false
	defer func() {
		if !released {
			close(start)
		}
	}()
	for range 2 {
		select {
		case <-ready:
		case <-call.Done():
			t.Fatal("native generation CAS readiness deadline", call.Err())
		}
	}
	close(start)
	released = true
	var outcomes [2]outcome
	for i := range outcomes {
		select {
		case outcomes[i] = <-finished:
		case <-call.Done():
			t.Fatal("native generation CAS completion deadline", call.Err())
		}
	}
	<-joined // Both worker calls have already returned their outcomes.
	applied, replayed, unresolved := 0, 0, 0
	for i, value := range outcomes {
		if value.err != nil {
			if !errors.Is(value.err, ydbstore.ErrAttachedResourceGenerationUnavailable) || value.result != (ports.AttachedResourceGenerationResultV1{}) {
				t.Errorf("native CAS caller=%d escaped unknown/conflict boundary: err=%v status=%s", i, value.err, value.result.Status)
			}
			unresolved++
			continue // Never retry the mutation or claim this caller saw an ACK.
		}
		switch value.result.Status {
		case ports.AttachedWorkerExecutionApplied:
			applied++
		case ports.AttachedWorkerExecutionReplayed:
			replayed++
		default:
			t.Errorf("native CAS caller=%d unexpected successful status=%s", i, value.result.Status)
		}
		if value.result.Head != two {
			t.Errorf("native CAS caller=%d acknowledged a different generation/scope", i)
		}
	}
	if applied > 1 || applied == 0 && unresolved == 0 || applied+replayed+unresolved != 2 {
		t.Fatalf("native CAS acknowledged multiple winners or lost outcomes: applied=%d replayed=%d unresolved=%d", applied, replayed, unresolved)
	}
	// Exactly ONE explicit read reconciles persisted truth, still under the
	// original concurrent-call window. It does not turn unknown replies into ACKs.
	read, err := f.store.ReadAttachedResourceGenerationV1(call, f.auth)
	if err != nil || read.Status != ports.AttachedWorkerExecutionApplied || read.Head != two {
		t.Fatalf("native CAS exact persisted successor unavailable: err=%v status=%s generation=%d", err, read.Status, read.Head.CredentialGeneration)
	}
	f.requireStored(t, call, two)
	t.Logf("native CAS caller acknowledgements applied=%d replayed=%d unresolved=%d; exact successor persisted", applied, replayed, unresolved)
}
