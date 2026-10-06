//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type aw07JoinedCycle func(context.Context) error

func (cycle aw07JoinedCycle) Exchange(ctx context.Context) error { return cycle(ctx) }

// Two owners share a real YDB-backed control plane and a TLS endpoint. The
// server commits A's heartbeat before its response is lost. B must still make
// progress, while A must reconcile instead of replaying an ambiguous write.
func TestAW07TwoOwnerYDBTLSLostResponseKeepsPeerAndFencesRevokedOwner(t *testing.T) {
	aStore, aDB, aWorker, aConnection, aDigest, _, _, aNow := readyAttachedWorkerForDrain(t, "aw07-joined-a")
	bStore, bDB, bWorker, bConnection, bDigest, _, _, bNow := readyAttachedWorkerForDrainWithIdentity(t,
		"aw07-joined-b", aWorker.TenantID, aWorker.ID)
	a := aw07ClaimedInput(t, aStore, aDB, aWorker, aConnection, aDigest, aNow,
		attachedWorkerDrainTestSuffix(t, "aw07-joined-claim-a"))
	b := aw07ClaimedInput(t, bStore, bDB, bWorker, bConnection, bDigest, bNow,
		attachedWorkerDrainTestSuffix(t, "aw07-joined-claim-b"))
	if a.request.OwnerUserID == b.request.OwnerUserID || a.request.WorkerID != b.request.WorkerID ||
		a.request.TenantID != b.request.TenantID {
		t.Fatal("joined fixture must contain distinct owners under one tenant and colliding worker locator")
	}

	service, err := attachedworkertransport.NewService(attachedworkertransport.ServiceConfig{
		IDs: testkit.NewSequenceIDGenerator("aw07-joined-"), Audience: "sessionless:attached-worker:v1",
		PlatformOffer: attachedworkerprotocol.VersionOfferV1{
			Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
			Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		},
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		ChallengeLifetime:   5 * time.Minute, ChallengeRetention: time.Hour,
		PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: attachedworkertransport.MinimumHeartbeatInterval,
	}, aStore, aStore)
	if err != nil {
		t.Fatalf("construct YDB-backed transport: %v", err)
	}
	aBearer := aw07JoinedBearer(t, aWorker, a.request.ConnectionID, aDigest)
	bBearer := aw07JoinedBearer(t, bWorker, b.request.ConnectionID, bDigest)
	for _, pair := range []struct {
		name   string
		bearer []byte
		req    ports.AttachedWorkerSealedInputAuthorization
	}{
		{name: "a_cannot_borrow_b", bearer: aBearer, req: b.request},
		{name: "b_cannot_borrow_a", bearer: bBearer, req: a.request},
	} {
		request := pair.req
		request.PresentedSecretDigest = ""
		if _, authErr := service.AuthorizeSealedInputBearer(context.Background(), pair.bearer, request); !errors.Is(authErr, attachedworkertransport.ErrTransportUnauthorized) {
			t.Fatalf("%s: cross-owner sealed-input authority error=%v, want unauthorized", pair.name, authErr)
		}
	}

	adapter, err := attachedworkerhttp.NewCoreExchangeAdapter(service)
	if err != nil {
		t.Fatalf("construct core HTTP adapter: %v", err)
	}
	handler, err := attachedworkerhttp.NewHandler(adapter)
	if err != nil {
		t.Fatalf("construct exchange handler: %v", err)
	}
	var aHits, bHits atomic.Int32
	aApplied := make(chan int, 1)
	releaseA := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseA) }) }
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") == "Bearer "+string(aBearer) {
			if aHits.Add(1) == 1 {
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)
				aApplied <- recorder.Code
				<-releaseA
				hijacker, ok := writer.(http.Hijacker)
				if !ok {
					writer.WriteHeader(http.StatusInternalServerError)
					return
				}
				connection, _, hijackErr := hijacker.Hijack()
				if hijackErr == nil {
					_ = connection.Close()
				}
				return
			}
		} else if request.Header.Get("Authorization") == "Bearer "+string(bBearer) {
			bHits.Add(1)
		}
		handler.ServeHTTP(writer, request)
	}))
	defer func() {
		release()
		server.Close()
	}()

	newPoller := func(bearer []byte, frame attachedworkerprotocol.BatchV1) *attachedworkertransport.Poller {
		t.Helper()
		client, clientErr := attachedworkerhttp.NewClientFromBearerBytes(attachedworkerhttp.ClientBytesConfig{
			BaseURL: server.URL, Bearer: bearer, HTTPClient: server.Client(), RequestTimeout: time.Minute,
		})
		if clientErr != nil {
			t.Fatalf("construct TLS exchange client: %v", clientErr)
		}
		t.Cleanup(func() { _ = client.Close() })
		poller, pollerErr := attachedworkertransport.NewPoller(attachedworkertransport.Config{
			Enabled: true, PollInterval: attachedworkertransport.MinimumHeartbeatInterval,
			InitialBackoff: time.Second, MaxBackoff: time.Minute,
			Random: bytes.NewReader(make([]byte, 8)),
		}, aw07JoinedCycle(func(ctx context.Context) error {
			_, exchangeErr := client.Exchange(ctx, frame)
			return exchangeErr
		}))
		if pollerErr != nil {
			t.Fatalf("construct exchange poller: %v", pollerErr)
		}
		return poller
	}
	aBefore := aw07JoinedConnection(t, aStore, aWorker)
	bBefore := aw07JoinedConnection(t, bStore, bWorker)
	aPoller := newPoller(aBearer, aw07JoinedHeartbeat(a, aBefore, aNow))
	bPoller := newPoller(bBearer, aw07JoinedHeartbeat(b, bBefore, bNow))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	aResult := make(chan error, 1)
	go func() { aResult <- aPoller.Step(ctx) }()
	select {
	case status := <-aApplied:
		if status != http.StatusNoContent && status != http.StatusOK {
			t.Fatalf("YDB did not commit A's heartbeat before response loss: HTTP status=%d", status)
		}
	case <-ctx.Done():
		t.Fatalf("A exchange did not reach durable YDB before deadline: %v", ctx.Err())
	}
	bCtx, bCancel := context.WithTimeout(ctx, 30*time.Second)
	defer bCancel()
	if err := bPoller.Step(bCtx); err != nil {
		t.Fatalf("B could not progress while A response was withheld: %v", err)
	}
	select {
	case err := <-aResult:
		t.Fatalf("A completed before its response was dropped: %v", err)
	default:
	}
	release()
	select {
	case err := <-aResult:
		var exchangeErr *attachedworkerhttp.ExchangeError
		if !errors.As(err, &exchangeErr) || exchangeErr.Kind != attachedworkerhttp.ErrorUnavailable || !exchangeErr.Retryable() {
			t.Fatalf("A lost committed response: error=%v, want retryable unavailable", err)
		}
	case <-ctx.Done():
		t.Fatalf("A did not observe bounded response loss: %v", ctx.Err())
	}
	if err := aPoller.Step(ctx); !errors.Is(err, attachedworkertransport.ErrReconciliationRequired) {
		t.Fatalf("A replayed ambiguous YDB write: error=%v, want reconciliation required", err)
	}
	if aHits.Load() != 1 || bHits.Load() != 1 {
		t.Fatalf("TLS exchanges A=%d B=%d, want one each", aHits.Load(), bHits.Load())
	}
	for _, scope := range []struct {
		name   string
		before uint64
		after  uint64
	}{
		{name: "A", before: aBefore.WorkerSequence, after: aw07JoinedConnection(t, aStore, aWorker).WorkerSequence},
		{name: "B", before: bBefore.WorkerSequence, after: aw07JoinedConnection(t, bStore, bWorker).WorkerSequence},
	} {
		if scope.after != scope.before+1 {
			t.Errorf("%s durable worker sequence=%d, want %d after one exchange", scope.name, scope.after, scope.before+1)
		}
	}
	aw07Revoke(t, aStore, ctx, aWorker)
	aRequest := a.request
	aRequest.PresentedSecretDigest = ""
	if _, err := service.AuthorizeSealedInputBearer(ctx, aBearer, aRequest); !errors.Is(err, attachedworkertransport.ErrTransportUnauthorized) {
		t.Errorf("revoked A regained sealed-input authority: %v", err)
	}
	bRequest := b.request
	bRequest.PresentedSecretDigest = ""
	if revision, err := service.AuthorizeSealedInputBearer(ctx, bBearer, bRequest); err != nil || revision != b.revision {
		t.Errorf("revoking A changed B authority: revision=%d want=%d err=%v", revision, b.revision, err)
	}
}

func aw07JoinedBearer(t *testing.T, worker domain.AttachedWorker, connectionID domain.AttachedWorkerConnectionID,
	wantDigest domain.AttachedWorkerConnectionSecretDigest,
) []byte {
	t.Helper()
	raw := sha256.Sum256([]byte("drain-race-bearer-" + string(worker.OwnerUserID)))
	secret, err := attachedworkertransport.ParseConnectionSecret(raw[:])
	if err != nil || secret.Digest() != wantDigest {
		t.Fatalf("fixture bearer digest does not match YDB connection: err=%v", err)
	}
	bearer, err := attachedworkertransport.NewConnectionBearer(worker.TenantID, worker.OwnerUserID, worker.ID, connectionID, secret)
	if err != nil {
		t.Fatalf("construct scoped connection bearer: %v", err)
	}
	return bearer.Bytes()
}

func aw07JoinedConnection(t *testing.T, store *ydbstore.Store, worker domain.AttachedWorker) domain.AttachedWorkerConnection {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	connection, found, err := store.LoadAttachedWorkerConnection(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("load owner %s connection: found=%t err=%v", worker.OwnerUserID, found, err)
	}
	return connection
}

func aw07JoinedHeartbeat(owner aw07ClaimedAuthorization, connection domain.AttachedWorkerConnection,
	now time.Time,
) attachedworkerprotocol.BatchV1 {
	sequence := connection.WorkerSequence + 1
	return attachedworkerprotocol.BatchV1{Version: attachedworkerprotocol.ProtocolVersionV1,
		Frames: []attachedworkerprotocol.FrameV1{{
			Version:   attachedworkerprotocol.ProtocolVersionV1,
			MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, sequence),
			WorkerID:  string(owner.request.WorkerID), EnrollmentGeneration: connection.EnrollmentGeneration,
			ConnectionGeneration: connection.ConnectionGeneration, Sequence: sequence,
			Ack: connection.PlatformSequence, Kind: attachedworkerprotocol.MessageHeartbeat,
			Heartbeat: &attachedworkerprotocol.HeartbeatV1{
				ObservedAtUnixMicro: now.UnixMicro(), Available: false, ActiveAttempts: 1,
			},
		}},
	}
}
