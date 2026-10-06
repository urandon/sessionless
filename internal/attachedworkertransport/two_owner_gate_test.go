package attachedworkertransport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// The workers deliberately reuse a worker locator and the test ID generator's
// challenge/connection locators. Scope must come from the authoritative
// tenant-owner-worker tuple, never from a globally unique-looking ID.
func TestAW07TwoOwnersCannotExchangeOrStealConnectionAuthority(t *testing.T) {
	_, aStore, aWorker, aKey := newTransportFixture(t)
	// A copied identity key must not turn a worker ID into cross-owner
	// authority. Keep the two owners in one tenant to exercise that boundary.
	bKey := aKey
	bWorker := aWorker
	bWorker.OwnerUserID = "owner-b"
	bWorker.IdentityPublicKey = append([]byte(nil), bKey.Public().(ed25519.PublicKey)...)
	bStore := &transportMemoryStore{worker: bWorker, now: aStore.now}
	router := &twoOwnerTransportStore{owners: map[twoOwnerScope]*transportMemoryStore{
		{aWorker.TenantID, aWorker.OwnerUserID, aWorker.ID}: aStore,
		{bWorker.TenantID, bWorker.OwnerUserID, bWorker.ID}: bStore,
	}}
	service, err := NewService(ServiceConfig{
		IDs: transportIDs{}, Audience: "sessionless:attached-worker:v1", PlatformOffer: testOffer(),
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, ChallengeLifetime: 5 * time.Minute,
		ChallengeRetention: time.Hour, PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: MinimumHeartbeatInterval, Random: bytes.NewReader(bytes.Repeat([]byte{0x35}, 256)),
	}, router, nil)
	if err != nil {
		t.Fatal(err)
	}
	a := readyTransportFixtureOnService(t, service, aStore, aWorker, aKey, 0x62)
	b := readyTransportFixtureOnService(t, service, bStore, bWorker, bKey, 0x63)
	if a.connection.ID != b.connection.ID || a.worker.ID != b.worker.ID ||
		a.worker.TenantID != b.worker.TenantID || !bytes.Equal(a.worker.IdentityPublicKey, b.worker.IdentityPublicKey) {
		t.Fatal("fixture must share tenant, worker locator, connection locator, and cloned identity key")
	}

	// A valid proof for one owner must not create a challenge for the other.
	for _, tc := range []struct {
		name string
		from readyTransportFixture
		to   readyTransportFixture
	}{
		{"a_to_b", a, b}, {"b_to_a", b, a},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeSource, beforeTarget := tc.from.store.createCalls, tc.to.store.createCalls
			_, err := service.IssueChallenge(context.Background(), tc.to.worker.TenantID, tc.to.worker.OwnerUserID,
				signedChallengeRequest(t, tc.from.worker, tc.from.privateKey))
			if !errors.Is(err, ErrTransportUnauthorized) {
				t.Fatalf("foreign proof error=%v, want unauthorized", err)
			}
			if tc.from.store.createCalls != beforeSource || tc.to.store.createCalls != beforeTarget {
				t.Fatal("foreign proof changed either owner's challenge state")
			}
			beforeSourceConnection, beforeTargetConnection := tc.from.store.connection, tc.to.store.connection
			beforeSourceCalls, beforeTargetCalls := tc.from.store.authorizeCalls, tc.to.store.authorizeCalls
			stolen, err := NewConnectionBearer(tc.to.worker.TenantID, tc.to.worker.OwnerUserID, tc.to.worker.ID,
				tc.to.connection.ID, tc.from.secret)
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.Exchange(context.Background(), stolen, tc.to.heartbeat(0))
			if !errors.Is(err, ErrTransportUnauthorized) {
				t.Fatalf("stolen secret under foreign scope error=%v, want unauthorized", err)
			}
			if tc.from.store.authorizeCalls != beforeSourceCalls || tc.to.store.authorizeCalls != beforeTargetCalls+1 ||
				!reflect.DeepEqual(tc.from.store.connection, beforeSourceConnection) ||
				!reflect.DeepEqual(tc.to.store.connection, beforeTargetConnection) {
				t.Fatal("stolen secret advanced either owner's connection")
			}
		})
	}
}

type twoOwnerScope struct {
	tenant domain.TenantID
	owner  domain.UserID
	worker domain.AttachedWorkerID
}

// This in-memory router models an owner-scoped persistence boundary while
// retaining the real transport service and protocol validation. Unknown scope
// is never redirected to either owner's store.
type twoOwnerTransportStore struct {
	ports.AttachedWorkerTransportStore
	owners map[twoOwnerScope]*transportMemoryStore
}

func (s *twoOwnerTransportStore) owner(tenant domain.TenantID, owner domain.UserID, worker domain.AttachedWorkerID) *transportMemoryStore {
	return s.owners[twoOwnerScope{tenant, owner, worker}]
}

func (s *twoOwnerTransportStore) LoadAttachedWorker(ctx context.Context, tenant domain.TenantID, owner domain.UserID, worker domain.AttachedWorkerID) (domain.AttachedWorker, bool, error) {
	if store := s.owner(tenant, owner, worker); store != nil {
		return store.LoadAttachedWorker(ctx, tenant, owner, worker)
	}
	return domain.AttachedWorker{}, false, nil
}

func (s *twoOwnerTransportStore) CreateAttachedWorkerAttachChallenge(ctx context.Context, request ports.AttachedWorkerChallengeCreate) (domain.AttachedWorkerAttachChallenge, error) {
	return s.owner(request.TenantID, request.OwnerUserID, request.WorkerID).CreateAttachedWorkerAttachChallenge(ctx, request)
}

func (s *twoOwnerTransportStore) LoadAttachedWorkerAttachChallenge(ctx context.Context, tenant domain.TenantID, owner domain.UserID, worker domain.AttachedWorkerID, challenge domain.AttachedWorkerChallengeID) (domain.AttachedWorkerAttachChallenge, bool, error) {
	if store := s.owner(tenant, owner, worker); store != nil {
		return store.LoadAttachedWorkerAttachChallenge(ctx, tenant, owner, worker, challenge)
	}
	return domain.AttachedWorkerAttachChallenge{}, false, nil
}

func (s *twoOwnerTransportStore) ActivateAttachedWorkerConnection(ctx context.Context, request ports.AttachedWorkerConnectionActivation) (ports.AttachedWorkerConnectionResult, error) {
	return s.owner(request.TenantID, request.OwnerUserID, request.WorkerID).ActivateAttachedWorkerConnection(ctx, request)
}

func (s *twoOwnerTransportStore) AcceptAttachedWorkerManifest(ctx context.Context, request ports.AttachedWorkerManifestAcceptance) (ports.AttachedWorkerAuthorizationResult, error) {
	return s.owner(request.TenantID, request.OwnerUserID, request.WorkerID).AcceptAttachedWorkerManifest(ctx, request)
}

func (s *twoOwnerTransportStore) LoadAttachedWorkerConnection(ctx context.Context, tenant domain.TenantID, owner domain.UserID, worker domain.AttachedWorkerID) (domain.AttachedWorkerConnection, bool, error) {
	if store := s.owner(tenant, owner, worker); store != nil {
		return store.LoadAttachedWorkerConnection(ctx, tenant, owner, worker)
	}
	return domain.AttachedWorkerConnection{}, false, nil
}

func (s *twoOwnerTransportStore) LoadAttachedWorkerAttempt(ctx context.Context, tenant domain.TenantID, owner domain.UserID, worker domain.AttachedWorkerID) (domain.AttachedWorkerAttemptV1, bool, error) {
	if store := s.owner(tenant, owner, worker); store != nil {
		return store.LoadAttachedWorkerAttempt(ctx, tenant, owner, worker)
	}
	return domain.AttachedWorkerAttemptV1{}, false, nil
}

func (s *twoOwnerTransportStore) AuthorizeAttachedWorkerExchange(ctx context.Context, request ports.AttachedWorkerExchangeAuthorization) (ports.AttachedWorkerAuthorizationResult, error) {
	return s.owner(request.TenantID, request.OwnerUserID, request.WorkerID).AuthorizeAttachedWorkerExchange(ctx, request)
}
