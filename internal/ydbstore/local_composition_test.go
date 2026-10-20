package ydbstore

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// This keeper cannot issue/verify any transport proof. Its panic proves that
// constructor identity checks invoke no supposed approval/verification callback.
type localBootstrapDenyKeeper struct{ identity byte }

func (*localBootstrapDenyKeeper) VerifyOwnedProxyPublication(ports.OwnedProxyPublicationV2, time.Time) error {
	panic("bootstrap called verifier")
}
func (*localBootstrapDenyKeeper) VerifyOwnedProxyPublicationCurrent(ports.OwnedProxyPublicationV2, time.Time) error {
	panic("bootstrap called current verifier")
}

type localBootstrapFixture struct {
	actual    localRuntimeActualFixture
	parent    *ownedAttemptEffectParentFixture
	bootstrap *LocalOwnedStoreBootstrapV1
	store     *Store
	bridge    *serverlessharness.BoundedOwnedAllocationPreparationV1
	keeper    *localBootstrapDenyKeeper
}

// Genuine private-file Source and real preparation/operation issuers, canonical
// Store reserve/burn and actual runtime. All four original funding publications
// and effect-source authentication are real; SQL mechanics and independent
// service documents are synthetic. The denying keeper is NOT network evidence.
func newLocalBootstrapFixture(t *testing.T) localBootstrapFixture {
	t.Helper()
	return newLocalBootstrapFixtureWithPrebuild(t, localRuntimeActualPrebuild{})
}

func newLocalBootstrapFixtureWithPrebuild(t *testing.T, prebuild localRuntimeActualPrebuild) localBootstrapFixture {
	t.Helper()
	state := currentWorkerAuthorityFixture(t)
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: state.Run.TenantID, OwnerUserID: state.Job.CredentialOwnerUserID, SessionID: state.Run.SessionID, RunID: state.Run.ID, AttemptID: state.Attempt.ID}
	prebuild.scope, prebuild.setupDuration = scope, 10*time.Second
	prebuild.authenticFunding = true
	actual := newLocalRuntimeActualFixture(t, true, true, prebuild)
	parent := newOwnedAttemptEffectParentFixture(t, ownedFinalPrebuild{at: actual.at, sources: &ownedFinalSources{profile: actual.profile, spending: actual.execution.Spending}})
	issuer, err := serverlessharness.NewOperationCapabilityIssuer(func() time.Time {
		if prebuild.operationClockBarrier != nil {
			prebuild.operationClockBarrier()
		}
		return parent.graph.at
	}, strings.NewReader(strings.Repeat("B", 32)), 16)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewLocalOwnedStoreBootstrapV1(actual.source, parent.issuer.issuer, issuer)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &allocationSQLFixture{parent: parent.sql}
	db := sql.OpenDB(allocationConnector{fixture})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	keeper := &localBootstrapDenyKeeper{identity: 1}
	store, err := b.CreateStore(db, Options{OwnedAttemptEffectSourceVerifier: actual.source, OwnedProfilePublicationVerifier: actual.source}, keeper)
	if err != nil {
		t.Fatal(err)
	}
	parent.store = store
	bridge, err := serverlessharness.NewBoundedOwnedLocalAllocationPreparationV1(parent.issuer.issuer, parent.parent.Authority.HarnessBinding.Backend, parent.parent.Authority.SubstrateBinding, actual.source, b)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bridge.Close() })
	return localBootstrapFixture{actual, parent, b, store, bridge, keeper}
}

func TestLocalOwnedStoreBootstrapInertThenSameStoreBurnAndPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixture(t)
		if f.store.localCompositionReady() || f.store.validOwnedAllocationContext(t.Context()) {
			t.Fatal("incomplete local Store ready")
		}
		if store, bridge, err := f.bootstrap.RuntimeFor(f.keeper); err == nil || store != nil || bridge != nil {
			t.Fatal("incomplete tuple exposed serving handles")
		}
		f.parent.deny(t, f.parent.request) // valid original reserve must deny pre-SQL
		called := false
		if err := f.store.ownedAuthorityTx(t.Context(), func(_ context.Context, _ *ownedAuthorityQuery) error { called = true; return nil }); err == nil || called {
			t.Fatal("inert Store entered owned transaction")
		}
		if f.parent.sql.inserts != 0 || f.parent.sql.burnUpdates != 0 {
			t.Fatal("inert Store changed canonical state")
		}
		if err := f.bootstrap.Complete(f.bridge); err != nil {
			t.Fatal(err)
		}
		store, bridge, err := f.bootstrap.RuntimeFor(f.keeper)
		if err != nil || store != f.store || bridge != f.bridge || !store.localCompositionReady() {
			t.Fatal("completed tuple lost exact owners")
		}
		grant := f.parent.reserve(t, f.parent.request)
		if grant.Grant == nil || f.parent.sql.inserts != 1 {
			t.Fatal("actual original Store did not reserve parent")
		}
		proof, err := bridge.PrepareAllocationPublication(f.actual.ctx, grant.Grant.Clone())
		if err != nil || f.parent.sql.burnUpdates != 1 || f.parent.sql.accountingRow == nil || proof.Allocation.LocalRuntime == nil {
			t.Fatalf("concrete same-Store burn/runtime: err=%v burns=%d", err, f.parent.sql.burnUpdates)
		}
		registered, err := store.publishBoundedOwnedAllocation(t.Context(), proof)
		if err != nil || registered.Original.LocalRuntime == nil {
			t.Fatalf("genuine allocation publication through same initialized Store: %v", err)
		}
		if _, err := bridge.PrepareAllocationPublication(f.actual.ctx, grant.Grant.Clone()); err == nil || f.parent.sql.burnUpdates != 1 {
			t.Fatal("replayed grant caused another durable burn/preparation")
		}
	})
}

func TestLocalOwnedStoreBootstrapCopiesForeignAndRepeatedInitializationDeny(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixture(t)
		other := newLocalBootstrapFixture(t)
		if err := f.bootstrap.Complete(other.bridge); err == nil {
			t.Fatal("foreign concrete bridge accepted")
		}
		copyBootstrap := new(LocalOwnedStoreBootstrapV1)
		reflect.ValueOf(copyBootstrap).Elem().Set(reflect.ValueOf(f.bootstrap).Elem())
		if err := copyBootstrap.Complete(f.bridge); err == nil {
			t.Fatal("copied bootstrap initialized original")
		}
		if _, _, err := copyBootstrap.RuntimeFor(f.keeper); err == nil {
			t.Fatal("copied bootstrap returned tuple")
		}
		copyBridge := new(serverlessharness.BoundedOwnedAllocationPreparationV1)
		reflect.ValueOf(copyBridge).Elem().Set(reflect.ValueOf(f.bridge).Elem())
		if err := f.bootstrap.Complete(copyBridge); err == nil {
			t.Fatal("copied bridge accepted for initial binding")
		}
		if err := f.bootstrap.Complete(f.bridge); err != nil {
			t.Fatal(err)
		}
		if err := f.bootstrap.Complete(f.bridge); err == nil {
			t.Fatal("repeated Complete accepted")
		}
		if _, _, err := f.bootstrap.RuntimeFor(other.keeper); err == nil {
			t.Fatal("foreign keeper acquired original runtime")
		}
		copiedStore := *f.store
		if copiedStore.localCompositionReady() || copiedStore.validOwnedAllocationContext(t.Context()) {
			t.Fatal("copied Store reused shared ready flag")
		}
		if _, err := f.bootstrap.CreateStore(f.store.db, Options{OwnedAttemptEffectSourceVerifier: f.parent.verifier}, f.keeper); err == nil {
			t.Fatal("second Store created")
		}
		fresh, err := NewLocalOwnedStoreBootstrapV1(f.actual.source, f.bootstrap.prepare, f.bootstrap.operate)
		if err != nil {
			t.Fatal(err)
		}
		foreignOpts := Options{OwnedAttemptEffectSourceVerifier: f.parent.verifier, AttemptOperationIssuer: other.bootstrap.operate}
		if _, err := fresh.CreateStore(f.store.db, foreignOpts, f.keeper); err == nil {
			t.Fatal("foreign options accepted by unused constructor")
		}
		if !f.bootstrap.HasLocalConnectorIdentity(f.actual.source, f.bootstrap.operate) || f.bootstrap.HasLocalConnectorIdentity(other.actual.source, f.bootstrap.operate) {
			t.Fatal("constructor identity correlation incorrect")
		}
	})
}

func TestLocalOwnedStoreBootstrapAtomicTuplePublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixture(t)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				<-start
				for range 100 {
					store, bridge, err := f.bootstrap.RuntimeFor(f.keeper)
					if err == nil && (store != f.store || bridge != f.bridge || !store.localCompositionReady() || !sameLocalOwnerPointer(store.ownedAllocationPublicationVerifier, bridge)) {
						t.Error("observed partial published tuple")
					}
				}
			})
		}
		wg.Go(func() {
			<-start
			if err := f.bootstrap.Complete(f.bridge); err != nil {
				t.Error(err)
			}
		})
		close(start)
		wg.Wait()
		if _, _, err := f.bootstrap.RuntimeFor(f.keeper); err != nil {
			t.Fatal("atomic completion not retained")
		}
	})
}
