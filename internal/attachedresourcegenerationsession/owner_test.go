//go:build darwin || linux

package attachedresourcegenerationsession

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ownerlocalcredential"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func (f roundTripper) client() *http.Client                              { return &http.Client{Transport: f} }

type ownerCloseSource struct {
	SourceFactoryV1
	calls    atomic.Int32
	terminal error
	entered  chan struct{}
	proceed  chan struct{}
}

func (s *ownerCloseSource) Close() error {
	s.calls.Add(1)
	if s.entered != nil {
		close(s.entered)
		<-s.proceed
	}
	if err := s.SourceFactoryV1.Close(); err != nil {
		return err
	}
	return s.terminal
}

func TestOwnerCloseKeepsOriginalCleanupFailure(t *testing.T) {
	f := newOwnerFixture(t, true)
	delegate := &ownerCloseSource{SourceFactoryV1: f.peer, terminal: errors.New("private-close-detail")}
	owner, err := NewV1("https://generation.example", (&metadataPeer{}).transport(), delegate, f.lifecycle, f.baseline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	for range 2 {
		if err := owner.Close(); err != ErrUnavailable || strings.Contains(err.Error(), "private-close-detail") {
			t.Errorf("original failed cleanup became success or leaked: %v", err)
		}
	}
	if delegate.calls.Load() != 1 {
		t.Fatal("failed cleanup was automatically retried")
	}
	if _, found, err := f.lifecycle.LoadCredentialBinding(t.Context(), f.baseline.TenantID, f.baseline.SubscriptionConnectionID); err != nil || !found {
		t.Fatal("metadata Close closed borrowed lifecycle", err)
	}
}

func TestOwnerCloseCannotClaimSuccessWhileOriginalCleanupPending(t *testing.T) {
	f := newOwnerFixture(t, true)
	delegate := &ownerCloseSource{SourceFactoryV1: f.peer, entered: make(chan struct{}), proceed: make(chan struct{})}
	owner, err := NewV1("https://generation.example", (&metadataPeer{}).transport(), delegate, f.lifecycle, f.baseline)
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(delegate.proceed) }) }
	done := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		release()
		if !joined {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("original cleanup worker not joined")
			}
		}
		_ = owner.Close()
	})
	go func() { done <- owner.Close() }()
	waitOwnerSignal(t, delegate.entered, "original cleanup did not enter")
	if err := owner.Close(); err != ErrUnavailable {
		t.Errorf("pending original cleanup claimed completed success: %v", err)
	}
	owner.mu.Lock()
	retired := owner.retired && owner.ack == nil && owner.client == nil
	owner.mu.Unlock()
	if !retired || delegate.calls.Load() != 1 {
		t.Error("pending cleanup retained metadata or invoked delegate twice")
	}
	if _, found, err := f.lifecycle.LoadCredentialBinding(t.Context(), f.baseline.TenantID, f.baseline.SubscriptionConnectionID); err != nil || !found {
		t.Error("pending foreign cleanup affected original lifecycle", err)
	}
	release()
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal("controlled original cleanup failed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("controlled original cleanup did not finish")
	}
	if err := owner.Close(); err != nil || delegate.calls.Load() != 1 {
		t.Fatal("terminal successful cleanup was not stable", err)
	}
}

type metadataRequest struct {
	path        string
	deadline    time.Time
	publication ports.AttachedResourceGenerationPublicationV1
	auth        ports.AttachedResourceGenerationAuthorizationV1
}
type metadataPeer struct {
	mu           sync.Mutex
	head         *domain.AttachedResourceGenerationHeadV1
	requests     []metadataRequest
	losePublish  bool
	readMissing  bool
	readForeign  bool
	afterPublish func()
}

func (p *metadataPeer) transport() *http.Client { return roundTripper(p.roundTrip).client() }
func (p *metadataPeer) roundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodPost || r.URL.Scheme != "https" || r.URL.Host != "generation.example" || r.GetBody != nil {
		return nil, errors.New("unexpected metadata wire")
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4097))
	if err != nil || len(raw) > 4096 {
		return nil, errors.New("metadata body exceeds bound")
	}
	defer clear(raw)
	if bytes.Contains(raw, []byte("secret")) || bytes.Contains(raw, []byte("fingerprint")) {
		return nil, errors.New("private credential metadata serialized")
	}
	deadline, ok := r.Context().Deadline()
	if !ok {
		return nil, errors.New("metadata operation lacks deadline")
	}
	entry := metadataRequest{path: r.URL.Path, deadline: deadline}
	p.mu.Lock()
	var result ports.AttachedResourceGenerationResultV1
	switch r.URL.Path {
	case attachedworkerhttp.ResourceGenerationPublishPathV1:
		var request attachedworkerhttp.ResourceGenerationPublishRequestV1
		if json.Unmarshal(raw, &request) != nil || request.Version != 1 {
			p.mu.Unlock()
			return nil, errors.New("invalid canonical publication")
		}
		entry.publication = ports.AttachedResourceGenerationPublicationV1{Authorization: request.Authorization, ExpectedCredentialGeneration: request.ExpectedCredentialGeneration, Target: request.Target}
		entry.auth = request.Authorization
		if err := domain.ValidateAttachedResourceGenerationTransitionV1(p.head, request.ExpectedCredentialGeneration, request.Target); err != nil {
			p.requests = append(p.requests, entry)
			p.mu.Unlock()
			return metadataResponse(http.StatusConflict, nil), nil
		}
		copy := request.Target
		p.head = &copy
		result = ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionApplied, Head: copy}
	case attachedworkerhttp.ResourceGenerationReadPathV1:
		var request attachedworkerhttp.ResourceGenerationReadRequestV1
		if json.Unmarshal(raw, &request) != nil || request.Version != 1 {
			p.mu.Unlock()
			return nil, errors.New("invalid canonical read")
		}
		entry.auth = request.Authorization
		if p.head == nil || p.readMissing {
			p.requests = append(p.requests, entry)
			p.mu.Unlock()
			return metadataResponse(http.StatusNotFound, nil), nil
		}
		copy := *p.head
		if p.readForeign {
			copy.CredentialGeneration++
		}
		result = ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionApplied, Head: copy}
	default:
		p.mu.Unlock()
		return nil, errors.New("unknown metadata route")
	}
	p.requests = append(p.requests, entry)
	lose := p.losePublish && r.URL.Path == attachedworkerhttp.ResourceGenerationPublishPathV1
	hook := p.afterPublish
	p.mu.Unlock()
	if r.URL.Path == attachedworkerhttp.ResourceGenerationPublishPathV1 && hook != nil {
		hook()
	}
	if lose {
		return nil, errors.New("synthetic-private-unknown-reply")
	}
	body, _ := json.Marshal(attachedworkerhttp.ResourceGenerationResponseV1{Version: 1, Status: result.Status, Head: result.Head})
	return metadataResponse(http.StatusOK, body), nil
}
func metadataResponse(status int, body []byte) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body))}
}
func (p *metadataPeer) calls() []metadataRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]metadataRequest(nil), p.requests...)
}

func (f *ownerFixture) resource(generation uint64) domain.ProviderResourceBindingV1 {
	r := f.baseline.Binding.Resource
	r.CredentialGeneration = generation
	return r
}
func (f *ownerFixture) revoke(t *testing.T) {
	t.Helper()
	if err := f.lifecycle.RevokeConnection(t.Context(), ports.CredentialRevokeRequest{TenantID: f.baseline.TenantID, OwnerUserID: f.baseline.OwnerUserID, SubscriptionConnectionID: f.baseline.SubscriptionConnectionID}); err != nil {
		t.Fatal("revoke original owner lifecycle", err)
	}
}
func assertNoNewWork(t *testing.T, f *ownerFixture, generation uint64) {
	t.Helper()
	if err := f.owner.CheckNewResourceV1(t.Context(), f.resource(generation)); err == nil {
		t.Fatal("NEW input accepted without exact current active ACK")
	}
}

func TestOwnerOriginalReadyInitialUnchangedAndOwnRefresh(t *testing.T) {
	f := newOwnerFixture(t, true)
	peer := &metadataPeer{}
	f.peer.openHook = func() {
		if len(peer.calls()) != 0 {
			t.Error("Open performed metadata I/O before Ready")
		}
	}
	f.connect(t, peer.roundTrip)
	if len(peer.calls()) != 0 {
		t.Fatal("binding genuine Ready eagerly published")
	}
	assertNoNewWork(t, f, 1)
	original := f.baseline.Clone()
	if err := f.owner.SynchronizeV1(t.Context()); err != nil {
		t.Fatal("initial generation ACK", err)
	}
	if err := f.owner.CheckNewResourceV1(t.Context(), f.resource(1)); err != nil {
		t.Fatal("exact initial resource", err)
	}
	f.writeback(t, 1, false)
	if err := f.owner.SynchronizeV1(t.Context()); err != nil || len(peer.calls()) != 1 {
		t.Fatal("unchanged WB republished or lost ACK", err, len(peer.calls()))
	}
	f.writeback(t, 1, true)
	assertNoNewWork(t, f, 1)
	assertNoNewWork(t, f, 2)
	if err := f.owner.SynchronizeV1(t.Context()); err != nil {
		t.Fatal("own successor ACK", err)
	}
	if err := f.owner.CheckNewResourceV1(t.Context(), f.resource(2)); err != nil {
		t.Fatal("exact successor resource", err)
	}
	assertNoNewWork(t, f, 1)
	calls := peer.calls()
	if len(calls) != 2 || calls[0].publication.ExpectedCredentialGeneration != 0 || calls[0].publication.Target.CredentialGeneration != 1 || calls[1].publication.ExpectedCredentialGeneration != 1 || calls[1].publication.Target.CredentialGeneration != 2 {
		t.Fatalf("publication CAS sequence=%+v", calls)
	}
	for _, call := range calls {
		if call.auth.ConnectionID != f.owner.binding.ConnectionID || call.auth.ConnectionGeneration != 1 || call.auth.PresentedSecretDigest != "" {
			t.Fatal("publication not derived from original Open binding")
		}
	}
	if !reflect.DeepEqual(original, f.baseline) || !reflect.DeepEqual(original, f.owner.baseline) {
		t.Fatal("publication or refresh mutated installed evidence")
	}
	// Generic resources/foreign baseline revisions cannot become ACK matches.
	for _, change := range []func(*domain.ProviderResourceBindingV1){func(r *domain.ProviderResourceBindingV1) { r.ResourceID = "foreign" }, func(r *domain.ProviderResourceBindingV1) { r.OwnerUserID = "foreign" }, func(r *domain.ProviderResourceBindingV1) { r.Revision++ }, func(r *domain.ProviderResourceBindingV1) { r.CredentialMode = domain.ProviderCredentialNoneV1 }} {
		r := f.resource(2)
		change(&r)
		if f.owner.CheckNewResourceV1(t.Context(), r) == nil {
			t.Fatal("foreign resource passed exact current ACK")
		}
	}
}

func TestOwnerRejectsAbsentInitialAdvanceAndWrongLocalTuple(t *testing.T) {
	for _, name := range []string{"absent", "initial generation two", "wrong provider", "wrong auth mode", "jump beyond acknowledged successor"} {
		t.Run(name, func(t *testing.T) {
			initialize := name != "absent" && name != "wrong provider" && name != "wrong auth mode"
			f := newOwnerFixture(t, initialize)
			peer := &metadataPeer{}
			if name == "wrong provider" || name == "wrong auth mode" {
				registration := ownerlocalcredential.Registration{Provider: "openai", AuthMode: "chatgpt", Entitlement: domain.EntitlementActive, At: f.clock.Now()}
				if name == "wrong provider" {
					registration.Provider = "foreign"
				} else {
					registration.AuthMode = "api-key"
				}
				if err := f.store.Initialize(t.Context(), registration, []byte("synthetic-wrong-tuple")); err == nil {
					t.Fatal("original local store admitted unsupported provider/auth tuple")
				}
			}
			f.connect(t, peer.roundTrip)
			if name == "initial generation two" {
				f.writeback(t, 1, true)
			}
			if name == "jump beyond acknowledged successor" {
				if err := f.owner.SynchronizeV1(t.Context()); err != nil {
					t.Fatal(err)
				}
				f.writeback(t, 1, true)
				f.writeback(t, 2, true)
			}
			before := len(peer.calls())
			if err := f.owner.SynchronizeV1(t.Context()); err == nil {
				t.Fatal("invalid initial/local metadata published")
			}
			if len(peer.calls()) != before {
				t.Fatal("invalid local metadata made HTTP call")
			}
			assertNoNewWork(t, f, 1)
		})
	}
}

func TestOwnerLostReplyUsesOneExactReadWithOriginalBudget(t *testing.T) {
	for _, name := range []string{"exact", "missing", "different generation"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnerFixture(t, true)
			peer := &metadataPeer{losePublish: true, readMissing: name == "missing", readForeign: name == "different generation"}
			f.connect(t, peer.roundTrip)
			ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
			defer cancel()
			deadline, _ := ctx.Deadline()
			err := f.owner.SynchronizeV1(ctx)
			calls := peer.calls()
			if len(calls) != 2 || calls[0].path != attachedworkerhttp.ResourceGenerationPublishPathV1 || calls[1].path != attachedworkerhttp.ResourceGenerationReadPathV1 || calls[0].deadline != deadline || calls[1].deadline != deadline || calls[0].auth != calls[1].auth {
				t.Fatalf("publish/read not one original window: %+v", calls)
			}
			if name == "exact" {
				if err != nil {
					t.Fatal("exact point read lost ACK", err)
				}
				if err := f.owner.CheckNewResourceV1(t.Context(), f.resource(1)); err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil {
					t.Fatal("nonmatching point read accepted")
				}
				assertNoNewWork(t, f, 1)
			}
			_ = f.owner.SynchronizeV1(ctx)
			if len(peer.calls()) != 2 {
				t.Fatal("unknown or unchanged target automatically republished/reread")
			}
			if err != nil && strings.Contains(err.Error(), "synthetic-private") {
				t.Fatal("private transport error leaked")
			}
		})
	}
}

func TestOwnerRevocationPublishesDenialDespiteExpiredEvidence(t *testing.T) {
	for _, name := range []string{"after initial ACK", "before first publication", "after lost refresh ACK"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnerFixture(t, true)
			peer := &metadataPeer{}
			f.connect(t, peer.roundTrip)
			if name != "before first publication" {
				if err := f.owner.SynchronizeV1(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if name == "after lost refresh ACK" {
				f.writeback(t, 1, true)
				peer.mu.Lock()
				peer.losePublish, peer.readMissing = true, true
				peer.mu.Unlock()
				if f.owner.SynchronizeV1(t.Context()) == nil {
					t.Fatal("missing refresh ACK accepted")
				}
				peer.mu.Lock()
				peer.losePublish, peer.readMissing = false, false
				peer.mu.Unlock()
			}
			f.clock.Advance(31 * time.Minute) // baseline expired; original auth remains current
			assertNoNewWork(t, f, 1)
			f.revoke(t)
			if err := f.owner.SynchronizeV1(t.Context()); err != nil {
				t.Fatal("denial improperly depended on positive evidence", err)
			}
			calls := peer.calls()
			last := calls[len(calls)-1]
			want := uint64(2)
			if name == "after lost refresh ACK" {
				want = 3
			}
			if last.path != attachedworkerhttp.ResourceGenerationPublishPathV1 || !last.publication.Target.Revoked || last.publication.Target.CredentialGeneration != want {
				t.Fatal("revocation not exact local denial")
			}
			assertNoNewWork(t, f, want)
		})
	}
}

func TestOwnerRereadsOriginalMetadataBeforeCachingACK(t *testing.T) {
	f := newOwnerFixture(t, true)
	peer := &metadataPeer{}
	peer.afterPublish = func() { f.revoke(t) }
	f.connect(t, peer.roundTrip)
	if err := f.owner.SynchronizeV1(t.Context()); err == nil {
		t.Fatal("ACK cached across concurrent local revoke")
	}
	f.owner.mu.Lock()
	ack := f.owner.ack
	f.owner.mu.Unlock()
	if ack != nil {
		t.Fatal("stale target cached despite final local reread")
	}
	assertNoNewWork(t, f, 1)
}

func TestOwnerActualReconnectReadsOnlyThenPublishesOwnSuccessor(t *testing.T) {
	f := newOwnerFixture(t, true)
	peer := &metadataPeer{}
	f.connect(t, peer.roundTrip)
	if err := f.owner.SynchronizeV1(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.writeback(t, 1, true)
	if err := f.owner.SynchronizeV1(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.reconnect(t, peer.roundTrip)
	before := len(peer.calls())
	assertNoNewWork(t, f, 2)
	if err := f.owner.SynchronizeV1(t.Context()); err != nil {
		t.Fatal("read exact reconnect head", err)
	}
	calls := peer.calls()
	if len(calls) != before+1 || calls[before].path != attachedworkerhttp.ResourceGenerationReadPathV1 || calls[before].auth.ConnectionGeneration != 2 || calls[before].auth.ConnectionID != "connection-generation-2" {
		t.Fatal("reconnect was not one read on its authenticated new connection")
	}
	if err := f.owner.CheckNewResourceV1(t.Context(), f.resource(2)); err != nil {
		t.Fatal("metadata-only reconnect fence", err)
	}
	f.writeback(t, 2, true)
	if err := f.owner.SynchronizeV1(t.Context()); err != nil {
		t.Fatal("own successor after acknowledged reconnect was not published", err)
	}
	calls = peer.calls()
	if len(calls) != before+2 || calls[before+1].path != attachedworkerhttp.ResourceGenerationPublishPathV1 ||
		calls[before+1].publication.ExpectedCredentialGeneration != 2 || calls[before+1].publication.Target.CredentialGeneration != 3 ||
		calls[before+1].auth != calls[before].auth {
		t.Fatal("own successor did not use one exact CAS on the recovered current connection")
	}
	if err := f.owner.CheckNewResourceV1(t.Context(), f.resource(3)); err != nil {
		t.Fatal("acknowledged own successor not available to new input", err)
	}
	assertNoNewWork(t, f, 2)
	if err := f.owner.SynchronizeV1(t.Context()); err != nil || len(peer.calls()) != before+2 {
		t.Fatal("unchanged recovered successor republished/reread", err)
	}
}

func TestOwnerReconnectSuccessorPreservesExpirySequenceAndUnknownFences(t *testing.T) {
	for _, name := range []string{"expired baseline", "jump beyond successor", "lost successor ACK"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnerFixture(t, true)
			peer := &metadataPeer{}
			f.connect(t, peer.roundTrip)
			if err := f.owner.SynchronizeV1(t.Context()); err != nil {
				t.Fatal("initial ACK", err)
			}
			f.reconnect(t, peer.roundTrip)
			if err := f.owner.SynchronizeV1(t.Context()); err != nil {
				t.Fatal("exact recovery read positive control", err)
			}
			if err := f.owner.CheckNewResourceV1(t.Context(), f.resource(1)); err != nil {
				t.Fatal("recovered positive control", err)
			}
			f.writeback(t, 1, true)
			wantCalls := 0
			switch name {
			case "expired baseline":
				f.clock.Advance(31 * time.Minute)
			case "jump beyond successor":
				f.writeback(t, 2, true)
			case "lost successor ACK":
				peer.mu.Lock()
				peer.losePublish, peer.readMissing = true, true
				peer.mu.Unlock()
				wantCalls = 2 // One CAS and one exact read within the SAME budget.
			}
			before := len(peer.calls())
			if err := f.owner.SynchronizeV1(t.Context()); err == nil {
				t.Fatal("denied/unknown recovered successor accepted")
			}
			calls := peer.calls()[before:]
			if len(calls) != wantCalls {
				t.Fatalf("metadata calls=%d want=%d", len(calls), wantCalls)
			}
			if wantCalls == 2 && (calls[0].path != attachedworkerhttp.ResourceGenerationPublishPathV1 ||
				calls[1].path != attachedworkerhttp.ResourceGenerationReadPathV1 || calls[0].deadline != calls[1].deadline || calls[0].auth != calls[1].auth) {
				t.Fatal("unknown successor escaped original publication/read budget or connection")
			}
			assertNoNewWork(t, f, 2)
			assertNoNewWork(t, f, 3)
			if err := f.owner.SynchronizeV1(t.Context()); err == nil || len(peer.calls()) != before+wantCalls {
				t.Fatal("denied/unknown successor automatically retried", err)
			}
		})
	}
}

func TestOwnerReconnectReadDoesNotRenewPositiveEligibility(t *testing.T) {
	for _, name := range []string{"expired baseline", "missing head", "different generation", "unexpected revoked head"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnerFixture(t, true)
			peer := &metadataPeer{}
			f.connect(t, peer.roundTrip)
			if err := f.owner.SynchronizeV1(t.Context()); err != nil {
				t.Fatal(err)
			}
			if name == "expired baseline" {
				f.clock.Advance(31 * time.Minute)
			}
			peer.mu.Lock()
			peer.readMissing = name == "missing head"
			peer.readForeign = name == "different generation"
			if name == "unexpected revoked head" {
				peer.head.Revoked = true
			}
			peer.mu.Unlock()
			f.reconnect(t, peer.roundTrip)
			before := len(peer.calls())
			err := f.owner.SynchronizeV1(t.Context())
			if (name == "expired baseline") != (err == nil) {
				t.Fatalf("read-only reconciliation %s: %v", name, err)
			}
			calls := peer.calls()
			if len(calls) != before+1 || calls[before].path != attachedworkerhttp.ResourceGenerationReadPathV1 {
				t.Fatal("reconnect read depended on positive eligibility or mutated head")
			}
			assertNoNewWork(t, f, 1)
			_ = f.owner.SynchronizeV1(t.Context())
			if len(peer.calls()) != before+1 {
				t.Fatal("reconnect automatically repeated missing/nonmatching read")
			}
		})
	}
}

func TestOwnerGenuineSessionExpiryAndCloseFenceNewInput(t *testing.T) {
	for _, name := range []string{"baseline expiry", "authentication expiry", "Session close"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnerFixture(t, true)
			peer := &metadataPeer{}
			f.connect(t, peer.roundTrip)
			if err := f.owner.SynchronizeV1(t.Context()); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "baseline expiry":
				f.clock.Advance(30 * time.Minute)
			case "authentication expiry":
				f.clock.Advance(time.Hour)
			case "Session close":
				if err := f.session.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			assertNoNewWork(t, f, 1)
			if f.owner.SynchronizeV1(t.Context()) == nil || len(peer.calls()) != 1 {
				t.Fatal("expired/closed original Session used cached ACK or made new I/O")
			}
		})
	}
}

func TestOwnerSessionCloseCancelsMetadataBeforeForeignCleanup(t *testing.T) {
	f := newOwnerFixture(t, true)
	entered, finished := make(chan struct{}), make(chan struct{})
	transport := roundTripper(func(r *http.Request) (*http.Response, error) {
		close(entered)
		<-r.Context().Done()
		close(finished)
		return nil, r.Context().Err()
	})
	f.connect(t, transport)
	outerEntered, outerRelease := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(outerRelease) }) }
	t.Cleanup(release)
	f.peer.closeHook = func() { close(outerEntered); <-outerRelease }
	done := make(chan error, 1)
	go func() { done <- f.owner.SynchronizeV1(t.Context()) }()
	joined := false
	t.Cleanup(func() {
		_ = f.owner.retire()
		if !joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("cleanup did not join original metadata operation")
			}
		}
	})
	waitOwnerSignal(t, entered, "metadata request did not start")
	if err := f.session.Close(t.Context()); err != nil {
		t.Fatal("Session-facing retirement waited for foreign cleanup", err)
	}
	waitOwnerSignal(t, finished, "metadata request not cancelled by original Session close")
	select {
	case err := <-done:
		joined = true
		if err == nil {
			t.Fatal("cancelled metadata became ACK")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("metadata owner failed to join cancelled operation")
	}
	f.peer.mu.Lock()
	retired, closed := f.peer.retired, f.peer.closed
	f.peer.mu.Unlock()
	if !retired || closed {
		t.Fatal("Session-facing retirement did not preserve two-phase source cleanup")
	}
	outerDone := make(chan error, 1)
	go func() { outerDone <- f.owner.Close() }()
	outerJoined := false
	t.Cleanup(func() {
		release()
		if !outerJoined {
			select {
			case <-outerDone:
			case <-time.After(5 * time.Second):
				t.Error("cleanup did not join outer delegate Close")
			}
		}
	})
	waitOwnerSignal(t, outerEntered, "outer delegate cleanup not called")
	if _, found, err := f.lifecycle.LoadCredentialBinding(t.Context(), f.baseline.TenantID, f.baseline.SubscriptionConnectionID); err != nil || !found {
		t.Fatal("metadata owner closed borrowed lifecycle", err)
	}
	release()
	select {
	case err := <-outerDone:
		outerJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("outer cleanup failed to join")
	}
}

func TestOwnerCloseDuringOriginalFactoryOpenFencesLateConstruction(t *testing.T) {
	f := newOwnerFixture(t, true)
	peer := &metadataPeer{}
	owner, err := NewV1(f.manifest.ControlPlaneOrigin, peer.transport(), f.peer, f.lifecycle, f.baseline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(proceed) }) }
	t.Cleanup(release)
	f.peer.openHook = func() { close(entered); <-proceed }
	connector, err := attachedworkersession.New(f.workerStore, f.peer, owner, f.config)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		session, err := connector.Connect(t.Context(), f.input)
		if session != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = session.Close(ctx)
			cancel()
		}
		done <- err
	}()
	joined := false
	t.Cleanup(func() {
		release()
		_ = owner.Close()
		if !joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("cleanup did not join original Open")
			}
		}
	})
	waitOwnerSignal(t, entered, "original source Open did not start")
	owner.mu.Lock()
	client := owner.client
	owner.mu.Unlock()
	if client == nil {
		t.Fatal("metadata client not owned before entering delegate Open")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Read(t.Context(), ports.AttachedResourceGenerationAuthorizationV1{}); err == nil {
		t.Fatal("Open-retained metadata bearer survived Close")
	}
	release()
	select {
	case err := <-done:
		joined = true
		if err == nil {
			t.Fatal("late Open revived original Session")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("late original Open failed to join")
	}
	if len(peer.calls()) != 0 {
		t.Fatal("late Open performed metadata I/O")
	}
}

func TestOwnerFailedGenuineManifestNeverPublishesAndRejectsForgedBinding(t *testing.T) {
	f := newOwnerFixture(t, true)
	peer := &metadataPeer{}
	owner, err := NewV1(f.manifest.ControlPlaneOrigin, peer.transport(), f.peer, f.lifecycle, f.baseline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if owner.BindInitialSessionV1(t.Context(), &attachedworkersession.Session{}) == nil || owner.SynchronizeV1(t.Context()) == nil {
		t.Fatal("fabricated Session became publication owner")
	}
	f.peer.manifestHook = func() error { return errors.New("synthetic manifest rejected") }
	connector, err := attachedworkersession.New(f.workerStore, f.peer, owner, f.config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connector.Connect(t.Context(), f.input); err == nil {
		t.Fatal("failed manifest produced Ready Session")
	}
	if owner.SynchronizeV1(t.Context()) == nil || len(peer.calls()) != 0 {
		t.Fatal("failed genuine manifest crossed publication boundary")
	}
}

func TestOwnerConstructorClonesBaselineAndCannotBeCopied(t *testing.T) {
	f := newOwnerFixture(t, true)
	peer := &metadataPeer{}
	owner, err := NewV1(f.manifest.ControlPlaneOrigin, peer.transport(), f.peer, f.lifecycle, f.baseline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	want := f.baseline.Clone()
	f.baseline.Evidence.Catalog.Models[0].ModelID = "mutated-caller-model"
	*f.baseline.Binding.EvidenceExpiresAt = time.Time{}
	if !reflect.DeepEqual(owner.baseline, want) {
		t.Fatal("owner retained mutable baseline alias")
	}
	copy := reflect.New(reflect.TypeOf(owner).Elem())
	copy.Elem().Set(reflect.ValueOf(owner).Elem())
	foreign := copy.Interface().(*OwnerV1)
	if foreign.SynchronizeV1(t.Context()) == nil || foreign.CheckNewResourceV1(t.Context(), want.Binding.Resource) == nil {
		t.Fatal("copied owner authenticated")
	}
	if strings.Contains(owner.String(), "generation.example") || strings.Contains(owner.String(), string(want.OwnerUserID)) {
		t.Fatal("owner formatting leaks private configuration")
	}
}

func waitOwnerSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(message)
	}
}

type concreteCheckEffectsV1 struct {
	binding  domain.CredentialBinding
	metadata [32]byte
	scratch  []string
	http     int
}

// Observe only the fixture's metadata file and scratch names, never its secret
// body/key. Issue/Materialize/Release would change this original durable state
// or scratch inventory; the check must only read existing in-memory metadata.
func concreteCheckEffects(t *testing.T, f *ownerFixture, peer *metadataPeer) concreteCheckEffectsV1 {
	t.Helper()
	binding, found, err := f.lifecycle.LoadCredentialBinding(t.Context(), f.baseline.TenantID, f.baseline.SubscriptionConnectionID)
	if err != nil || !found {
		t.Fatal("cannot inspect original fixture metadata", err)
	}
	parent := filepath.Dir(f.manifest.Harness.Executable)
	metadata := filepath.Join(parent, "credentials", "state.v1")
	info, err := os.Stat(metadata)
	if err != nil || info.Size() > 64<<10 {
		t.Fatal("fixture metadata is absent or unbounded", err)
	}
	raw, err := os.ReadFile(metadata)
	if err != nil {
		t.Fatal("read synthetic fixture metadata", err)
	}
	result := concreteCheckEffectsV1{binding: binding, metadata: sha256.Sum256(raw), http: len(peer.calls())}
	entries, err := os.ReadDir(filepath.Join(parent, "scratch"))
	if err != nil || len(entries) > 64 {
		t.Fatal("fixture scratch inventory is absent or unbounded", err)
	}
	for _, entry := range entries {
		result.scratch = append(result.scratch, entry.Name())
	}
	return result
}

func requireConcreteCheckNoEffects(t *testing.T, f *ownerFixture, peer *metadataPeer, before concreteCheckEffectsV1) {
	t.Helper()
	if after := concreteCheckEffects(t, f, peer); !reflect.DeepEqual(before, after) {
		t.Error("nongrant check changed HTTP count, credential metadata/state or scratch inventory")
	}
}

func TestOwnerConcreteCheckSameSessionLifecycleBaselineAndSuccessor(t *testing.T) {
	f := newOwnerFixture(t, true)
	peer := &metadataPeer{}
	f.connect(t, peer.roundTrip)
	before := concreteCheckEffects(t, f, peer)
	if err := f.owner.CheckNewResourceForSessionV1(t.Context(), f.session, f.lifecycle, f.baseline.Clone(), f.resource(1)); err != ErrDenied {
		t.Errorf("original owners without ACK accepted: %v", err)
	}
	requireConcreteCheckNoEffects(t, f, peer, before)
	if err := f.owner.SynchronizeV1(t.Context()); err != nil {
		t.Fatal(err)
	}
	for generation := uint64(1); generation <= 2; generation++ {
		before := concreteCheckEffects(t, f, peer)
		if err := f.owner.CheckNewResourceForSessionV1(t.Context(), f.session, f.lifecycle, f.baseline.Clone(), f.resource(generation)); err != nil {
			t.Fatalf("same original owners/exact ACK generation=%d denied: %v", generation, err)
		}
		requireConcreteCheckNoEffects(t, f, peer, before)
		if generation == 1 {
			f.writeback(t, 1, true) // original lifecycle alone creates N+1
			before = concreteCheckEffects(t, f, peer)
			for _, stale := range []uint64{1, 2} {
				if err := f.owner.CheckNewResourceForSessionV1(t.Context(), f.session, f.lifecycle, f.baseline, f.resource(stale)); err != ErrDenied {
					t.Errorf("stale/unacknowledged generation=%d accepted: %v", stale, err)
				}
			}
			requireConcreteCheckNoEffects(t, f, peer, before)
			if err := f.owner.SynchronizeV1(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestOwnerConcreteCheckRejectsForeignGenuineOwnersAndInvalidCalls(t *testing.T) {
	f := newOwnerFixture(t, true)
	peer := &metadataPeer{}
	f.connect(t, peer.roundTrip)
	if err := f.owner.SynchronizeV1(t.Context()); err != nil {
		t.Fatal(err)
	}
	foreign := newOwnerFixture(t, true)
	otherPeer := &metadataPeer{}
	foreign.connect(t, otherPeer.roundTrip)
	if !reflect.DeepEqual(f.session.Snapshot(), foreign.session.Snapshot()) || f.session == foreign.session || f.lifecycle == foreign.lifecycle {
		t.Fatal("causal fixture requires distinct genuine owners with identical Ready snapshots")
	}
	copiedLifecycle := *f.lifecycle // shares original internals, not its concrete owner pointer
	for _, test := range []struct {
		name      string
		session   *attachedworkersession.Session
		lifecycle *ownerlocalcredential.Lifecycle
	}{
		{name: "genuine matching-snapshot Session", session: foreign.session, lifecycle: f.lifecycle},
		{name: "foreign concrete lifecycle", session: f.session, lifecycle: foreign.lifecycle},
		{name: "copied lifecycle wrapper", session: f.session, lifecycle: &copiedLifecycle},
		{name: "both foreign", session: foreign.session, lifecycle: foreign.lifecycle},
		{name: "nil Session", lifecycle: f.lifecycle},
		{name: "nil lifecycle", session: f.session},
		{name: "unconstructed Session", session: &attachedworkersession.Session{}, lifecycle: f.lifecycle},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := concreteCheckEffects(t, f, peer)
			if err := f.owner.CheckNewResourceForSessionV1(t.Context(), test.session, test.lifecycle, f.baseline, f.resource(1)); err != ErrDenied {
				t.Errorf("%s replaced original concrete authority: %v", test.name, err)
			}
			requireConcreteCheckNoEffects(t, f, peer, before)
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := concreteCheckEffects(t, f, peer)
	for _, invalid := range []context.Context{nil, ctx} {
		if err := f.owner.CheckNewResourceForSessionV1(invalid, f.session, f.lifecycle, f.baseline, f.resource(1)); err != ErrDenied {
			t.Errorf("invalid context produced a nongrant success: %v", err)
		}
	}
	var absent *OwnerV1
	if err := absent.CheckNewResourceForSessionV1(t.Context(), f.session, f.lifecycle, f.baseline, f.resource(1)); err != ErrDenied {
		t.Errorf("nil owner accepted: %v", err)
	}
	requireConcreteCheckNoEffects(t, f, peer, before)
}

func rebindConcreteCheckBaseline(t *testing.T, b *domain.AttachedResourceGenerationBaselineV1) {
	t.Helper()
	for _, scope := range []*domain.ProviderEvidenceScopeV1{&b.Evidence.Catalog.Scope, &b.Evidence.Route.Scope, &b.Evidence.Capability.Scope, &b.Evidence.Privacy.Scope, &b.Evidence.Price.Scope, &b.Evidence.Policy.Scope} {
		*scope = domain.ProviderEvidenceScopeV1{TenantID: b.Binding.TenantID, OwnerUserID: b.Binding.OwnerUserID, Resource: b.Binding.Resource, Backend: b.Binding.Backend}
	}
	for _, item := range []struct {
		digest func() (domain.ProviderEvidenceDigestV1, error)
		fields []*string
	}{
		{b.Evidence.Catalog.Digest, []*string{&b.Binding.ProviderCatalogDigest}},
		{b.Evidence.Route.Digest, []*string{&b.Binding.ProviderRouteDigest, &b.Evidence.Policy.RoutePolicyDigest}},
		{b.Evidence.Capability.Digest, []*string{&b.Binding.CapabilityEvidenceDigest, &b.Evidence.Policy.CapabilityEvidenceDigest}},
		{b.Evidence.Privacy.Digest, []*string{&b.Binding.PrivacyPolicyDigest, &b.Evidence.Policy.PrivacyEvidenceDigest}},
		{b.Evidence.Price.Digest, []*string{&b.Evidence.Policy.PriceObservationDigest}},
		{func() (domain.ProviderEvidenceDigestV1, error) { return b.Evidence.Policy.Digest() }, []*string{&b.Binding.EffectivePolicyDigest}},
	} {
		digest, err := item.digest()
		if err != nil {
			t.Fatal("test metadata rehash failed", err)
		}
		for _, field := range item.fields {
			*field = string(digest)
		}
	}
}

func TestOwnerConcreteCheckRequiresExactCompleteImmutableBaseline(t *testing.T) {
	f := newOwnerFixture(t, true)
	peer := &metadataPeer{}
	f.connect(t, peer.roundTrip)
	if err := f.owner.SynchronizeV1(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*domain.AttachedResourceGenerationBaselineV1)
	}{
		{name: "body", change: func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.Evidence.Catalog.CatalogRevision = "different-catalog"
		}},
		{name: "tenant", change: func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.TenantID, b.Binding.TenantID = "foreign-tenant", "foreign-tenant"
		}},
		{name: "owner", change: func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.OwnerUserID, b.Binding.OwnerUserID, b.Binding.Resource.OwnerUserID = "foreign-owner", "foreign-owner", "foreign-owner"
		}},
		{name: "resource", change: func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.SubscriptionConnectionID, b.Binding.Resource.ResourceID = "foreign-resource", "foreign-resource"
		}},
		{name: "worker", change: func(b *domain.AttachedResourceGenerationBaselineV1) { b.WorkerID = "foreign-worker" }},
		{name: "enrollment", change: func(b *domain.AttachedResourceGenerationBaselineV1) { b.EnrollmentGeneration++ }},
		{name: "revision", change: func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.BaselineRevision++
			b.Binding.Resource.Revision++
		}},
		{name: "nil versus empty", change: func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.Evidence.Route.Routes = []domain.ProviderRouteV1{}
		}},
		{name: "observation timestamp", change: func(b *domain.AttachedResourceGenerationBaselineV1) {
			b.Evidence.Catalog.ObservedAt = b.Evidence.Catalog.ObservedAt.Add(time.Nanosecond)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			installed := f.baseline.Clone()
			test.change(&installed)
			rebindConcreteCheckBaseline(t, &installed)
			if err := installed.Validate(); err != nil || reflect.DeepEqual(installed, f.baseline) {
				t.Fatalf("negative must be a different but VALID complete baseline: %v", err)
			}
			before := concreteCheckEffects(t, f, peer)
			if err := f.owner.CheckNewResourceForSessionV1(t.Context(), f.session, f.lifecycle, installed, f.resource(1)); err != ErrDenied {
				t.Errorf("rehash/structural validity replaced original complete %s baseline: %v", test.name, err)
			}
			requireConcreteCheckNoEffects(t, f, peer, before)
		})
	}
	bad := f.baseline.Clone()
	bad.Evidence.Catalog.CatalogRevision = strings.Repeat("x", domain.AttachedResourceGenerationBaselineMaxBytesV1+1)
	before := concreteCheckEffects(t, f, peer)
	if err := f.owner.CheckNewResourceForSessionV1(t.Context(), f.session, f.lifecycle, bad, f.resource(1)); err != ErrDenied {
		t.Errorf("unbounded/invalid baseline accepted: %v", err)
	}
	bad = f.baseline.Clone()
	bad.Evidence.Catalog.ObservedAt = bad.Evidence.Catalog.ObservedAt.In(time.FixedZone("same-instant-non-UTC", 0))
	if err := bad.Validate(); err == nil {
		t.Fatal("fixture must preserve existing UTC-only baseline time validation")
	}
	if err := f.owner.CheckNewResourceForSessionV1(t.Context(), f.session, f.lifecycle, bad, f.resource(1)); err != ErrDenied {
		t.Errorf("equal-instant noncanonical time bypassed installed baseline validation: %v", err)
	}
	requireConcreteCheckNoEffects(t, f, peer, before)
}

func TestOwnerConcreteCheckStillRequiresCurrentActiveACKAndConnection(t *testing.T) {
	for _, name := range []string{"stale generation", "unacknowledged successor", "revoked", "baseline expiry", "Session close", "Owner close", "currentness changes after metadata"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnerFixture(t, true)
			peer := &metadataPeer{}
			checking, checks := false, 0
			if name == "currentness changes after metadata" {
				f.config.Now = func() time.Time {
					if checking {
						checks++
						if checks == 2 {
							return *f.baseline.Binding.EvidenceExpiresAt
						}
					}
					return f.clock.Now()
				}
			}
			f.connect(t, peer.roundTrip)
			if err := f.owner.SynchronizeV1(t.Context()); err != nil {
				t.Fatal(err)
			}
			generation := uint64(1)
			switch name {
			case "stale generation", "unacknowledged successor":
				f.writeback(t, 1, true)
				if name == "unacknowledged successor" {
					generation = 2
				}
			case "revoked":
				f.revoke(t)
			case "baseline expiry":
				f.clock.Advance(30 * time.Minute)
			case "Session close":
				if err := f.session.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "Owner close":
				if err := f.owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
			before := concreteCheckEffects(t, f, peer)
			checking = true
			err := f.owner.CheckNewResourceForSessionV1(t.Context(), f.session, f.lifecycle, f.baseline.Clone(), f.resource(generation))
			checking = false
			if err != ErrDenied {
				t.Errorf("%s passed concrete check: %v", name, err)
			}
			if name == "currentness changes after metadata" && checks != 2 {
				t.Errorf("expected original connection checks before AND after metadata, got %d", checks)
			}
			requireConcreteCheckNoEffects(t, f, peer, before)
		})
	}
}
