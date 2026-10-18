package ydbstore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func sessionRequest() configuredinference.RequestV1 {
	return configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "owned test peer, not a model"}}, MaxOutputTokens: 1}
}
func newFactorySession(t *testing.T, peer *localLoopbackPeer) (*providerloopsession.LocalV1, *ydbstore.LocalSessionConnectedSeed, *connectedOperationBlobs) {
	t.Helper()
	seed := ydbstore.NewLocalSessionConnectedSeed(t, peer.endpoint())
	blobs := newConnectedOperationBlobs(seed.Scope)
	before := seed.Stats()
	session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options,
		Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil || seed.Stats() != before {
		t.Fatalf("actual factory must be inert: %v", err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	})
	peer.setResponse([]byte(`{"model":"` + seed.Profile.ModelID + `","choices":[{"index":0,"message":{"role":"assistant","content":"owned peer result"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	return session, seed, blobs
}
func firstSessionOperation(t *testing.T, seed *ydbstore.LocalSessionConnectedSeed, parent domain.OwnedAttemptEffectParentV1, ref operationbody.ReferenceV1) domain.AttemptOperationV1 {
	t.Helper()
	r := parent.Reservation
	rd, err := r.DigestForAuthority(parent.Authority)
	if err != nil {
		t.Fatal(err)
	}
	pd, err := parent.Authority.HarnessBinding.AgentLoopPolicy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	cost := parent.Authority.AdmissionCostCeiling
	cd, err := cost.Digest()
	if err != nil {
		t.Fatal(err)
	}
	duration := seed.Profile.Limits.OperationTimeout
	op := domain.AttemptOperationV1{Version: 1, Scope: domain.AttemptOperationScopeV1{TenantID: r.TenantID, SessionID: parent.Scope.SessionID, OwnerUserID: parent.Scope.OwnerUserID,
		RunID: r.RunID, AttemptID: r.AttemptID, LeaseID: r.LeaseID, FenceToken: r.FenceToken, PhysicalInvocationClaimID: r.PhysicalInvocationClaimID,
		EffectReservationDigest: rd, InvocationAuthorityDigest: r.InvocationAuthorityDigest, HarnessBindingDigest: r.HarnessBindingDigest, AgentLoopPolicyDigest: pd},
		Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "factory-first-model", RequestRef: ref.Ref, RequestDigest: ref.Digest, RequestBytes: ref.Bytes,
		Deadline: time.Now().UTC().Truncate(time.Microsecond).Add(duration), CleanupDuration: seed.Profile.Limits.CleanupTimeout,
		ResultBytes: 4096, ContextBytes: 1, Currency: cost.Currency, PriceRevision: cost.PriceRevision, AdmissionCostCeilingDigest: cd,
		Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 1, OutputTokens: 1, BodyBytes: ref.Bytes + 4096, ResponseBytes: 4096, ActiveDuration: duration}}
	if err := op.ValidateForParent(parent.Authority, parent.Reservation); err != nil {
		t.Fatalf("original parent operation: %v", err)
	}
	return op
}

// Genuine entire factory/entry/codec/body/issuer/Store/HTTP path, with SCRIPTED
// SQL, complete source verifier, external funding and blob STORAGE. No actual
// model/native YDB/token-fit, tool continuation or full MVP is proved here.
func TestLocalSessionFactoryConnectedOneDurableModel(t *testing.T) {
	peer := newLocalLoopbackPeer(t, true)
	session, seed, blobs := newFactorySession(t, peer)
	parent, err := session.Start(t.Context(), seed.Request)
	if err != nil {
		t.Fatalf("original factory entry: %v", err)
	}
	if stats := seed.Stats(); stats.Parent != 1 || stats.Burn != 1 || stats.Allocation != 1 || stats.Operation != 0 {
		t.Fatalf("factory entry did not use one real reserve/burn/allocation: %+v", stats)
	}
	ref, err := session.WriteModelRequest(t.Context(), seed.Profile.Limits, sessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	op := firstSessionOperation(t, seed, parent, ref)
	result, err := session.InvokeFirstModel(t.Context(), op, seed.Profile.Limits, sessionRequest())
	if err != nil || !result.SendAttempted || result.Observation.Observation.Outcome != domain.AttemptOperationCompletedV1 {
		t.Fatalf("factory one model durable result: err=%v attempted=%t", err, result.SendAttempted)
	}
	if err := seed.Operate.VerifyAttemptOperationObservation(result.Observation); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seed.PersistedObservation(t), result.Observation.Observation) {
		t.Fatal("returned observation differs from actual persisted row")
	}
	observed := peer.waitResult(t)
	frozen, err := configuredinference.FreezeRequestV1(seed.Profile, seed.Profile.Limits, sessionRequest())
	if err != nil || observed.err != nil || observed.request == nil || len(observed.extra) != 0 || !bytes.Equal(observed.body, frozen.Body()) {
		t.Fatalf("one original bounded HTTP: codec=%v peer=%v", err, observed.err)
	}
	if _, exists := observed.request.Header["Authorization"]; exists {
		t.Fatal("noauth factory sent a credential")
	}
	before := seed.Stats()
	copySession := *session
	if _, err := copySession.Start(t.Context(), seed.Request); err == nil {
		t.Fatal("copy re-entered original invocation")
	}
	op.Sequence = 2
	if result, err := copySession.InvokeFirstModel(t.Context(), op, seed.Profile.Limits, sessionRequest()); err == nil || result.SendAttempted {
		t.Fatal("fixture-free successor not yet implemented but a new effect was admitted")
	}
	if seed.Stats() != before {
		t.Fatal("reentry/successor changed canonical rows")
	}
	if puts, opens, closes, deletes := blobs.counts(); puts != 2 || opens != closes || deletes != 0 {
		t.Fatalf("factory original body custody: puts=%d opens=%d closes=%d deletes=%d", puts, opens, closes, deletes)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Source.Configuration(); err != nil {
		t.Fatal("session closed borrowed Source")
	}
	if err := seed.DB.PingContext(t.Context()); err != nil {
		t.Fatal("session closed borrowed DB")
	}
}

func TestLocalSessionFactoryConnectedPartialReplyUnknown(t *testing.T) {
	peer := newLocalLoopbackPeer(t, true, true)
	session, seed, _ := newFactorySession(t, peer)
	parent, err := session.Start(t.Context(), seed.Request)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := session.WriteModelRequest(t.Context(), seed.Profile.Limits, sessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	op := firstSessionOperation(t, seed, parent, ref)
	result, err := session.InvokeFirstModel(t.Context(), op, seed.Profile.Limits, sessionRequest())
	if !errors.Is(err, providerloopsession.ErrIncomplete) || !result.SendAttempted {
		t.Fatalf("partial response: err=%v attempted=%t", err, result.SendAttempted)
	}
	observed := seed.PersistedObservation(t)
	if observed.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || observed.ResponseRef != "" || observed.Usage.State != domain.AttemptOperationUsageUnknownV1 {
		t.Fatal("partial reply fabricated complete body/usage")
	}
	before := seed.Stats()
	if _, err := session.InvokeFirstModel(t.Context(), op, seed.Profile.Limits, sessionRequest()); err == nil || seed.Stats() != before {
		t.Fatal("partial effect retried original operation")
	}
	_ = peer.waitResult(t)
}

func TestLocalSessionFactorySourceCloseDeniesFreshBodyIO(t *testing.T) {
	peer := newLocalLoopbackPeer(t, true)
	session, seed, blobs := newFactorySession(t, peer)
	if _, err := session.Start(t.Context(), seed.Request); err != nil {
		t.Fatal(err)
	}
	before := seed.Stats()
	puts, opens, _, _ := blobs.counts()
	if err := seed.Source.Close(); err != nil {
		t.Fatal(err)
	}
	// No scheduler wait: immediately after the concrete Source.Close returns.
	if _, err := session.WriteModelRequest(t.Context(), seed.Profile.Limits, sessionRequest()); err == nil {
		t.Fatal("revoked Source allowed fresh body I/O")
	}
	p2, o2, _, _ := blobs.counts()
	if p2 != puts || o2 != opens || seed.Stats() != before {
		t.Fatal("revoked Source touched storage or canonical rows")
	}
}

func TestLocalSessionFactoryCorruptRequestDeniedBeforeConnection(t *testing.T) {
	peer := newLocalLoopbackPeer(t, true)
	session, seed, blobs := newFactorySession(t, peer)
	parent, err := session.Start(t.Context(), seed.Request)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := session.WriteModelRequest(t.Context(), seed.Profile.Limits, sessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	op := firstSessionOperation(t, seed, parent, ref)
	blobs.corruptOriginal(t)
	before := seed.Stats()
	if result, err := session.InvokeFirstModel(t.Context(), op, seed.Profile.Limits, sessionRequest()); err == nil || result.SendAttempted {
		t.Fatal("corrupt original request admitted a fresh effect")
	}
	if seed.Stats() != before {
		t.Fatal("corrupt request changed proxy/operation rows")
	}
	// Successful Close joins the keeper: no later connection can be opened by
	// this session. This assertion is not a scheduler-based absence sample.
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-peer.accepted:
		t.Fatal("corrupt request reached TCP peer")
	default:
	}
}

type failSessionResponseBlobs struct{ *connectedOperationBlobs }

func (b failSessionResponseBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	puts, _, _, _ := b.counts()
	if puts == 1 {
		return domain.BlobRef{}, errors.New("test storage rejects normalized response")
	}
	return b.connectedOperationBlobs.Put(ctx, tenant, key, body)
}

func TestLocalSessionFactoryResponseStorageFailureIsUnknown(t *testing.T) {
	peer := newLocalLoopbackPeer(t, true)
	seed := ydbstore.NewLocalSessionConnectedSeed(t, peer.endpoint())
	blobs := failSessionResponseBlobs{newConnectedOperationBlobs(seed.Scope)}
	session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options,
		Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	})
	peer.setResponse([]byte(`{"model":"` + seed.Profile.ModelID + `","choices":[{"index":0,"message":{"role":"assistant","content":"owned peer result"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	parent, err := session.Start(t.Context(), seed.Request)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := session.WriteModelRequest(t.Context(), seed.Profile.Limits, sessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	op := firstSessionOperation(t, seed, parent, ref)
	result, err := session.InvokeFirstModel(t.Context(), op, seed.Profile.Limits, sessionRequest())
	if !errors.Is(err, providerloopsession.ErrIncomplete) || !result.SendAttempted {
		t.Fatalf("storage failure: %v, attempted=%t", err, result.SendAttempted)
	}
	observed := seed.PersistedObservation(t)
	if observed.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || observed.Reason != domain.AttemptOperationReasonWorkerLostV1 || observed.ResponseRef != "" {
		t.Fatal("failed response storage fabricated durable success")
	}
	before := seed.Stats()
	if _, err := session.InvokeFirstModel(t.Context(), op, seed.Profile.Limits, sessionRequest()); err == nil || seed.Stats() != before {
		t.Fatal("response storage failure retried provider")
	}
	_ = peer.waitResult(t)
}

func TestLocalSessionFactoryCloseCancelsActiveHTTP(t *testing.T) {
	received, handlerDone, stop := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(stop) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 1<<20))
		close(received)
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(release)
	seed := ydbstore.NewLocalSessionConnectedSeed(t, server.URL+"/v1")
	blobs := newConnectedOperationBlobs(seed.Scope)
	session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options,
		Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	})
	parent, err := session.Start(t.Context(), seed.Request)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := session.WriteModelRequest(t.Context(), seed.Profile.Limits, sessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	op := firstSessionOperation(t, seed, parent, ref)
	type outcome struct {
		result providerloopsession.ModelResultV1
		err    error
	}
	done := make(chan struct{})
	var got outcome // close(done) publishes it to every caller/cleanup join
	await := func(ch <-chan struct{}, stage string) bool {
		t.Helper()
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-ch:
		case <-timer.C:
			select {
			case <-ch:
			default:
				t.Errorf("%s did not complete", stage)
				return false
			}
		}
		return true
	}
	// Registered BEFORE launching: all Fatal/return paths cancel and join the
	// caller before the borrowed SQL/Source fixtures are cleaned up. The server
	// cleanup joins handlers; release also unblocks one after a broken client.
	t.Cleanup(func() {
		release()
		if err := session.Close(); err != nil {
			t.Error(err)
		}
		await(done, "operation caller cleanup")
	})
	go func() {
		got.result, got.err = session.InvokeFirstModel(t.Context(), op, seed.Profile.Limits, sessionRequest())
		close(done)
	}()
	if !await(received, "owned HTTP request") {
		return
	}
	copySession := *session
	if _, err := copySession.Start(t.Context(), seed.Request); !errors.Is(err, providerloopsession.ErrBusy) {
		t.Fatalf("concurrent copied wrapper: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("cancel-before-join: %v", err)
	}
	// Close joins Invoke's gate; synchronize the caller's final instruction too.
	if !await(done, "operation caller") {
		return
	}
	if !errors.Is(got.err, providerloopsession.ErrIncomplete) || !got.result.SendAttempted {
		t.Fatalf("cancelled HTTP result: %v attempted=%t", got.err, got.result.SendAttempted)
	}
	if !await(handlerDone, "actual HTTP connection cancellation") {
		return
	}
	observed := seed.PersistedObservation(t)
	if observed.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || observed.ResponseRef != "" {
		t.Fatal("cancelled HTTP fabricated success")
	}
}
