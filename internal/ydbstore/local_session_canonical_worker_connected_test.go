package ydbstore_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Actual canonical commit/admission/facade/lease/entry. SQL and the declared
// numeric endpoint are test peers; this is not Manager.RunOnce or a model loop.
func TestLocalSessionCanonicalQueuedFacadeEntryConnected(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected pre-entry HTTP", http.StatusInternalServerError)
	}))
	server.Config.ReadHeaderTimeout = 5 * time.Second
	server.Config.ReadTimeout = 10 * time.Second
	server.Config.WriteTimeout = 10 * time.Second
	server.Config.MaxHeaderBytes = 16 << 10
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
	server.Start()
	seed := ydbstore.NewLocalCanonicalSessionSeed(t, server.URL+"/v1")
	configuration, err := seed.Source.Configuration()
	if err != nil || configuration.BaseURL != server.URL+"/v1" {
		t.Fatal("endpoint was not sealed before original Source intake", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: seed.Loaded.Run.TenantID, SessionID: seed.Loaded.Run.SessionID, OwnerUserID: seed.Loaded.Job.CredentialOwnerUserID, RunID: seed.Loaded.Run.ID, AttemptID: seed.Loaded.Attempt.ID}
	blobs := newConnectedOperationBlobs(scope)
	before := seed.Stats()
	session, err := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal("same original Source factory", err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error("original session close", err)
		}
	})
	state, err := session.WorkerState()
	if err != nil {
		t.Fatal(err)
	}
	if seed.Stats() != before {
		t.Fatal("inert factory/facade performed SQL effects")
	}
	if err := session.PreflightWorkerJob(ctx, seed.Loaded); err != nil {
		t.Fatal("actual queued factory preflight", err)
	}
	if seed.Stats() != before {
		t.Fatal("read-only preflight minted lease/effect")
	}
	claim := seed.Claim
	claim.Now = seed.Now()
	claim.ExpiresAt = claim.Now.Add(30 * time.Second)
	lease, err := state.ClaimWorkerLease(ctx, claim)
	if err != nil {
		t.Fatal("actual factory-facade claim", err)
	}
	if lease.FenceToken != 1 {
		t.Fatal("claim did not create original fence")
	}
	if err := state.StartWorkerJob(ctx, seed.Loaded, lease, seed.Now()); err != nil {
		t.Fatal("actual factory-facade start", err)
	}
	effects, ok := state.(ports.AttemptEffectStoreV1)
	if !ok {
		t.Fatal("original facade effect port absent")
	}
	request := ports.ReserveAttemptEffectRequestV1{TenantID: claim.TenantID, RunID: claim.RunID, AttemptID: claim.AttemptID, LeaseID: lease.ID, FenceToken: lease.FenceToken, PhysicalInvocationClaimID: "physical-local-canonical-connected"}
	fresh, err := effects.ReserveAttemptEffect(ctx, request)
	if err != nil || fresh.Status != ports.AttemptEffectOwnedV1 {
		t.Fatalf("actual fresh canonical effect: status=%s err=%v", fresh.Status, err)
	}
	changed := fresh
	changed.Status = ports.AttemptEffectReplayedV1
	if _, err := session.EnterWorkerEffect(ctx, changed); err == nil || seed.Stats().Burns != 0 || seed.Stats().Allocations != 0 {
		t.Fatal("mutable result DATA minted fresh original entry", err)
	}
	parent, err := session.EnterWorkerEffect(ctx, fresh)
	if err != nil {
		t.Fatal("same bootstrap actual fresh entry", err)
	}
	stats := seed.Stats()
	if parent.Scope != scope || parent.Authority.Lease != lease || stats.Parents != 1 || stats.Burns != 1 || stats.Allocations != 1 || requests.Load() != 0 {
		t.Fatalf("original claimed entry mismatch: stats=%+v requests=%d", stats, requests.Load())
	}
	if _, err := session.EnterWorkerEffect(ctx, fresh); err == nil {
		t.Fatal("same fresh result entered twice")
	}
}

func TestLocalSessionCanonicalQueuedPreflightDeniesWithoutLeaseConnected(t *testing.T) {
	for _, fault := range []string{"foreign owner DATA", "original Source closed"} {
		t.Run(fault, func(t *testing.T) {
			var requests atomic.Int64
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				http.Error(w, "unexpected HTTP", http.StatusInternalServerError)
			}))
			server.Config.ReadHeaderTimeout = 5 * time.Second
			server.Config.ReadTimeout = 10 * time.Second
			server.Config.WriteTimeout = 10 * time.Second
			server.Config.MaxHeaderBytes = 16 << 10
			t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
			server.Start()
			seed := ydbstore.NewLocalCanonicalSessionSeed(t, server.URL+"/v1")
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			t.Cleanup(cancel)
			scope := domain.AttemptExecutionSourceScopeV1{TenantID: seed.Loaded.Run.TenantID, SessionID: seed.Loaded.Run.SessionID, OwnerUserID: seed.Loaded.Job.CredentialOwnerUserID, RunID: seed.Loaded.Run.ID, AttemptID: seed.Loaded.Attempt.ID}
			session, err := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: newConnectedOperationBlobs(scope)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := session.Close(); err != nil {
					t.Error(err)
				}
			})
			expected := seed.Loaded
			expected.Job = expected.Job.Clone()
			before := seed.Stats()
			if fault == "foreign owner DATA" {
				expected.Job.CredentialOwnerUserID = "foreign-owner"
			} else {
				if err := seed.Source.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := session.PreflightWorkerJob(ctx, expected); err == nil || seed.Stats() != before || before.LeasePresent || requests.Load() != 0 {
				t.Fatalf("denied preflight acquired effects: fault=%s err=%v before=%+v after=%+v requests=%d", fault, err, before, seed.Stats(), requests.Load())
			}
		})
	}
}
