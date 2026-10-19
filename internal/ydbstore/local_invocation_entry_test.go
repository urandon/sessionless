package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// Storage only. The original Source/private files, issuer, Store reserve/burn,
// runtime observation, allocation publication and once-only entry are real.
// SQL isolation, complete effect-source verifier and external funding remain
// SCRIPTED; no model/transport/token-fit/native YDB qualification is claimed.
type localEntryBlobs struct {
	objects map[string][]byte
	calls   int
}

// Embedding supplies the unused custody methods; any read would panic. The
// constructor must retain this original owner without invoking or replacing it.
type localEntryResidualReader struct {
	ports.OwnedOperationResidualCustodyV1
}

func (*localEntryResidualReader) ReadAttemptOperationBody(context.Context, domain.AttemptOperationScopeV1, string, uint64) ([]byte, error) {
	panic("constructor or denied Start invoked residual body owner")
}

func (b *localEntryBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	b.calls++
	if ctx.Err() != nil {
		return domain.BlobRef{}, ctx.Err()
	}
	raw, err := io.ReadAll(io.LimitReader(body, (1<<20)+1))
	if err != nil || len(raw) == 0 || len(raw) > 1<<20 {
		return domain.BlobRef{}, errors.New("test storage body out of bounds")
	}
	sum := sha256.Sum256(raw)
	if b.objects == nil {
		b.objects = make(map[string][]byte)
	}
	b.objects[key] = bytes.Clone(raw)
	return domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(raw)), SHA256: hex.EncodeToString(sum[:])}, nil
}
func (b *localEntryBlobs) Open(ctx context.Context, _ domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	b.calls++
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	body, ok := b.objects[ref.Key]
	if !ok {
		return nil, errors.New("test storage object missing")
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(body))), nil
}
func (*localEntryBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	panic("entry must not delete storage")
}

func completeLocalEntryFixture(t *testing.T, f localBootstrapFixture) {
	t.Helper()
	if err := f.bootstrap.Complete(f.bridge); err != nil {
		t.Fatal(err)
	}
}

func assertLocalEntryDenied(t *testing.T, entry *LocalInvocationEntryV1, err error) {
	t.Helper()
	if entry != nil || !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("entry=%t err=%v, want nil denied entry", entry != nil, err)
	}
}

func TestLocalInvocationEntryConstructorPrearmAndOriginalOwnership(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixture(t)
		blobs := &localEntryBlobs{}
		reader := f.store.attemptOperationBodies
		if f.bootstrap.bodySlot == nil || reader != f.bootstrap.bodySlot || f.bootstrap.bodySlot.bodies.Load() != nil {
			t.Fatal("constructor did not install one closed fixed body reader")
		}
		if body, err := reader.ReadAttemptOperationBody(t.Context(), domain.AttemptOperationScopeV1{}, "unarmed", 32); err == nil || len(body) != 0 {
			t.Fatal("unarmed fixed body slot performed a read")
		}
		entry, err := f.bootstrap.StartLocalInvocation(f.actual.ctx, f.parent.request, blobs)
		assertLocalEntryDenied(t, entry, err)
		if blobs.calls != 0 || f.parent.sql.inserts != 0 || f.parent.sql.burnUpdates != 0 || f.bootstrap.startClaimed.Load() {
			t.Fatal("incomplete construction consumed entry or performed I/O")
		}
		completeLocalEntryFixture(t, f)
		entry, err = f.bootstrap.StartLocalInvocation(f.actual.ctx, f.parent.request, blobs)
		if err != nil || entry == nil {
			t.Fatalf("genuine original entry: %v", err)
		}
		parent, pe := entry.Parent()
		source, se := entry.Source()
		allocation, ae := entry.Allocation()
		bodies, be := entry.Bodies()
		if pe != nil || se != nil || ae != nil || be != nil || bodies == nil || f.store.attemptOperationBodies != reader || f.bootstrap.bodySlot.bodies.Load() != bodies || blobs.calls != 0 {
			t.Fatalf("entry changed body reader/owners or caused blob I/O: errors=%v/%v/%v/%v blobCalls=%d", pe, se, ae, be, blobs.calls)
		}
		if !reflect.DeepEqual(parent, f.parent.parent) || !reflect.DeepEqual(source, f.parent.source) || allocation.LocalRuntime == nil || allocation.ObservedImageDigest != "" || f.bridge.VerifyEnteredLocalAllocation(allocation, time.Now()) != nil || f.parent.sql.inserts != 1 || f.parent.sql.burnUpdates != 1 || f.parent.sql.accountingRow == nil {
			t.Fatal("entry did not join genuine persisted parent/Final/source and sole reserve/burn/entered local runtime")
		}
		payload := []byte("scoped immutable DATA")
		ref, err := bodies.Write(t.Context(), payload)
		if err != nil {
			t.Fatal(err)
		}
		read, err := reader.ReadAttemptOperationBody(t.Context(), ownedOperationScope(parent), ref.Ref, ref.Bytes)
		if err != nil || !bytes.Equal(read, payload) {
			t.Fatalf("fixed reader does not use SAME actual adapter/scope: %v", err)
		}
		arguments := []byte(`{"query":"public compiler documentation"}`)
		argumentRef, err := bodies.WriteConsentArguments(t.Context(), arguments)
		if err != nil {
			t.Fatal("original entry consent argument custody", err)
		}
		argumentRead, err := reader.ReadAttemptOperationBody(t.Context(), ownedOperationScope(parent), argumentRef.Key, 4096)
		if err != nil || !bytes.Equal(argumentRead, arguments) || f.store.attemptOperationBodies != reader || f.bootstrap.bodySlot.bodies.Load() != bodies {
			t.Fatalf("consent key did not traverse the unchanged original installed slot: %v", err)
		}
		foreign := ownedOperationScope(parent)
		foreign.OwnerUserID = "foreign-owner"
		before := blobs.calls
		if _, err := reader.ReadAttemptOperationBody(t.Context(), foreign, ref.Ref, ref.Bytes); err == nil || blobs.calls != before {
			t.Fatal("fixed reader crossed original body scope before storage I/O")
		}
		parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxOperations = 1
		source.Spending[0].Window.ExpiresAt = source.Spending[0].Window.ObservedAt
		allocation.LocalRuntime.Limits.MaxInputTokens = 1
		freshParent, _ := entry.Parent()
		freshSource, _ := entry.Source()
		freshAllocation, _ := entry.Allocation()
		if !reflect.DeepEqual(freshParent, f.parent.parent) || !reflect.DeepEqual(freshSource, f.parent.source) || freshAllocation.LocalRuntime.Limits != f.actual.config.Limits {
			t.Fatal("historical entry accessors alias caller-mutated snapshots")
		}
		copyEntry := *entry
		if _, err := copyEntry.Bodies(); err == nil {
			t.Fatal("copied entry reconstructed original body owner")
		}
		if err := f.actual.source.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Parent(); err != nil {
			t.Fatal("Source close erased historical entry DATA")
		}
	})
}

func TestLocalInvocationEntryDuplicateCannotCloseWinner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixture(t)
		completeLocalEntryFixture(t, f)
		blobs := &localEntryBlobs{}
		entry, err := f.bootstrap.StartLocalInvocation(f.actual.ctx, f.parent.request, blobs)
		if err != nil {
			t.Fatal(err)
		}
		allocation, _ := entry.Allocation()
		for _, request := range []ports.ReserveAttemptEffectRequestV1{f.parent.request, {TenantID: "foreign", RunID: f.parent.request.RunID, AttemptID: f.parent.request.AttemptID, LeaseID: f.parent.request.LeaseID, FenceToken: f.parent.request.FenceToken, PhysicalInvocationClaimID: f.parent.request.PhysicalInvocationClaimID}} {
			other, err := f.bootstrap.StartLocalInvocation(f.actual.ctx, request, &localEntryBlobs{})
			assertLocalEntryDenied(t, other, err)
		}
		if f.bridge.VerifyEnteredLocalAllocation(allocation, time.Now()) != nil || f.parent.sql.inserts != 1 || f.parent.sql.burnUpdates != 1 {
			t.Fatal("denied duplicate closed or re-reserved the active winner")
		}
		if _, err := f.actual.source.Configuration(); err != nil {
			t.Fatal("entry/duplicate closed caller-owned Source")
		}
	})
}

func TestLocalInvocationEntryConcurrentStartFailFast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixture(t)
		completeLocalEntryFixture(t, f)
		started, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		var group sync.WaitGroup
		t.Cleanup(func() { unblock(); group.Wait() })
		f.parent.graph.afterCommit = func() { f.parent.graph.afterCommit = nil; close(started); <-release }
		type result struct {
			entry *LocalInvocationEntryV1
			err   error
		}
		results := make(chan result, 1)
		group.Go(func() {
			entry, err := f.bootstrap.StartLocalInvocation(f.actual.ctx, f.parent.request, &localEntryBlobs{})
			results <- result{entry, err}
		})
		<-started
		entry, err := f.bootstrap.StartLocalInvocation(f.actual.ctx, f.parent.request, &localEntryBlobs{})
		assertLocalEntryDenied(t, entry, err)
		unblock()
		group.Wait()
		winner := <-results
		if winner.err != nil || winner.entry == nil {
			t.Fatalf("duplicate damaged pending original start: %v", winner.err)
		}
		allocation, _ := winner.entry.Allocation()
		if f.bridge.VerifyEnteredLocalAllocation(allocation, time.Now()) != nil || f.parent.sql.inserts != 1 || f.parent.sql.burnUpdates != 1 {
			t.Fatal("concurrent calls did not retain one genuine entered winner")
		}
	})
}

func TestLocalInvocationEntryFailureReplayAndAmbiguityNeverRearm(t *testing.T) {
	for _, name := range []string{"foreign-request", "existing-parent-replay", "source-verifier-denied", "terminal-ambiguous", "cancel-after-reserve"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalBootstrapFixture(t)
				completeLocalEntryFixture(t, f)
				request := f.parent.request
				ctx, cancel := context.WithCancel(f.actual.ctx)
				defer cancel()
				switch name {
				case "foreign-request":
					request.TenantID = "foreign-tenant"
				case "existing-parent-replay":
					f.parent.reserve(t, f.parent.request)
				case "source-verifier-denied":
					f.parent.verifier.reject = true
				case "terminal-ambiguous":
					f.parent.graph.commitModes = []string{"ambiguous", "ambiguous", "ambiguous"}
				case "cancel-after-reserve":
					f.parent.graph.afterCommit = func() { f.parent.graph.afterCommit = nil; cancel() }
				}
				blobs := &localEntryBlobs{}
				entry, err := f.bootstrap.StartLocalInvocation(ctx, request, blobs)
				assertLocalEntryDenied(t, entry, err)
				if f.bootstrap.entry.Load() != nil || f.bootstrap.bodySlot.bodies.Load() != nil || f.parent.sql.burnUpdates != 0 || blobs.calls != 0 {
					t.Fatal("failed/replayed/ambiguous start armed body slot or entered runtime")
				}
				entry, err = f.bootstrap.StartLocalInvocation(f.actual.ctx, f.parent.request, blobs)
				assertLocalEntryDenied(t, entry, err)
				if _, err := f.actual.source.Configuration(); err != nil {
					t.Fatal("failed original start closed caller-owned Source")
				}
			})
		})
	}
}

func TestLocalInvocationEntryMissingCopiedAndPreinstalledBodiesDeny(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixture(t)
		completeLocalEntryFixture(t, f)
		for _, tc := range []struct {
			b       *LocalOwnedStoreBootstrapV1
			ctx     context.Context
			request ports.ReserveAttemptEffectRequestV1
			blobs   ports.BlobStore
		}{
			{nil, f.actual.ctx, f.parent.request, &localEntryBlobs{}},
			{f.bootstrap, nil, f.parent.request, &localEntryBlobs{}},
			{f.bootstrap, f.actual.ctx, ports.ReserveAttemptEffectRequestV1{}, &localEntryBlobs{}},
			{f.bootstrap, f.actual.ctx, f.parent.request, nil},
			{f.bootstrap, f.actual.ctx, f.parent.request, (*localEntryBlobs)(nil)},
		} {
			entry, err := tc.b.StartLocalInvocation(tc.ctx, tc.request, tc.blobs)
			assertLocalEntryDenied(t, entry, err)
		}
		copyBootstrap := new(LocalOwnedStoreBootstrapV1)
		reflect.ValueOf(copyBootstrap).Elem().Set(reflect.ValueOf(f.bootstrap).Elem())
		entry, err := copyBootstrap.StartLocalInvocation(f.actual.ctx, f.parent.request, &localEntryBlobs{})
		assertLocalEntryDenied(t, entry, err)
		// A constructor explicitly supplied an existing reader retains it; the
		// new entry may not overwrite/rebind that composition through a setter.
		other, err := NewLocalOwnedStoreBootstrapV1(f.actual.source, f.bootstrap.prepare, f.bootstrap.operate)
		if err != nil {
			t.Fatal(err)
		}
		preinstalled := &loopbackOperationBody{}
		store, err := other.CreateStore(f.store.db, Options{OwnedAttemptEffectSourceVerifier: f.parent.verifier, OwnedProfilePublicationVerifier: f.actual.source, AttemptOperationBodies: preinstalled}, f.keeper)
		if err != nil || store.attemptOperationBodies != preinstalled || other.bodySlot != nil {
			t.Fatalf("preinstalled reader path changed: %v", err)
		}
		bridge, err := serverlessharness.NewBoundedOwnedLocalAllocationPreparationV1(other.prepare, f.parent.parent.Authority.HarnessBinding.Backend, f.parent.parent.Authority.SubstrateBinding, f.actual.source, other)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = bridge.Close() })
		if err := other.Complete(bridge); err != nil {
			t.Fatal(err)
		}
		entry, err = other.StartLocalInvocation(f.actual.ctx, f.parent.request, &localEntryBlobs{})
		assertLocalEntryDenied(t, entry, err)
		if f.parent.sql.inserts != 0 || f.parent.sql.burnUpdates != 0 || store.attemptOperationBodies != preinstalled {
			t.Fatal("invalid/copied/preinstalled entry changed canonical state or reader identity")
		}
	})
}

func TestLocalInvocationEntryPreservesResidualOnlyConstructor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixture(t)
		other, err := NewLocalOwnedStoreBootstrapV1(f.actual.source, f.bootstrap.prepare, f.bootstrap.operate)
		if err != nil {
			t.Fatal(err)
		}
		original := &localEntryResidualReader{}
		store, err := other.CreateStore(f.store.db, Options{OwnedAttemptEffectSourceVerifier: f.parent.verifier,
			OwnedProfilePublicationVerifier: f.actual.source, OwnedOperationResidualCustody: original}, f.keeper)
		if err != nil || store.attemptOperationBodies != original || store.ownedOperationResidualCustody != original || other.bodySlot != nil {
			t.Fatalf("existing residual-only constructor changed: %v", err)
		}
		bridge, err := serverlessharness.NewBoundedOwnedLocalAllocationPreparationV1(other.prepare,
			f.parent.parent.Authority.HarnessBinding.Backend, f.parent.parent.Authority.SubstrateBinding, f.actual.source, other)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = bridge.Close() })
		if err := other.Complete(bridge); err != nil {
			t.Fatal(err)
		}
		entry, err := other.StartLocalInvocation(f.actual.ctx, f.parent.request, &localEntryBlobs{})
		assertLocalEntryDenied(t, entry, err)
		if other.startClaimed.Load() || f.parent.sql.inserts != 0 || f.parent.sql.burnUpdates != 0 || store.attemptOperationBodies != original {
			t.Fatal("new Start changed preinstalled residual owner or canonical state")
		}
	})
}
