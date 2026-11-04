//go:build sessionless_search_offline

package ydbstore_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localcontrolplane"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/webapi"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/worker"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const searchLoopFixtureKey = "synthetic-controlled-search-key"
const searchLoopQuery = "public compiler documentation"

type searchLoopPeer struct {
	server   *httptest.Server
	model    bool
	modelID  string
	mu       sync.Mutex
	bodies   [][]byte
	failures []string
	calls    atomic.Uint32
	roots    *x509.CertPool
}

func newSearchLoopPeer(t *testing.T, model bool, attempts int, failureMode string) *searchLoopPeer {
	t.Helper()
	if attempts < 1 || attempts > 2 || failureMode != "" && !domain.SearchFailureStopV4(failureMode) {
		t.Fatal("closed one/two-attempt controlled peer")
	}
	p := &searchLoopPeer{model: model}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := p.calls.Add(1)
		cap := int64(1 << 20)
		if !model {
			cap = 8 << 10
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, cap+1))
		_ = r.Body.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		if err != nil || int64(len(body)) > cap || r.Method != http.MethodPost || model && (n > uint32(2*attempts) || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "") || !model && (n > uint32(attempts) || r.URL.Path != "/search" || r.Host != "api.tavily.com" || r.Header.Get("Authorization") != "Bearer "+searchLoopFixtureKey) {
			p.failures = append(p.failures, "controlled peer rejected fixed bounded POST/header/path")
			clear(body)
			http.Error(w, "controlled rejection", http.StatusBadRequest)
			return
		}
		p.bodies = append(p.bodies, body)
		w.Header().Set("Content-Type", "application/json")
		if !model {
			var request struct {
				Query      string `json:"query"`
				MaxResults uint8  `json:"max_results"`
			}
			if json.Unmarshal(body, &request) != nil || request.Query != searchLoopQuery || request.MaxResults != 3 {
				p.failures = append(p.failures, "controlled SEARCH did not receive exact consent arguments")
			}
			switch failureMode {
			case domain.SearchServiceErrorV4:
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"detail":"controlled known service error"}`)
				return
			case domain.SearchOutcomeUnknownV4:
				_, _ = io.WriteString(w, `{"results":"controlled malformed result shape"}`)
				return
			}
			_, _ = io.WriteString(w, `{"query":"public compiler documentation","results":[{"url":"https://example.org/compiler","title":"Compiler <reference> & examples","content":"public compiler excerpt <>&"}]}`)
			return
		}
		if n%2 == 1 {
			_, _ = fmt.Fprintf(w, `{"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"owned-search-call","type":"function","function":{"name":"web_search","arguments":"{\"query\":\"public compiler documentation\",\"max_results\":3}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, p.modelID)
		} else {
			if failureMode == domain.ModelAfterSearchFailureV4 {
				_, _ = fmt.Fprintf(w, `{"model":%q,"choices":[]}`, p.modelID)
				return
			}
			_, _ = fmt.Fprintf(w, `{"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"controlled searched final answer %d"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, p.modelID, n/2)
		}
	})
	p.server = httptest.NewUnstartedServer(h)
	p.server.Config.ReadHeaderTimeout, p.server.Config.ReadTimeout, p.server.Config.WriteTimeout = 5*time.Second, 5*time.Second, 5*time.Second
	p.server.Config.MaxHeaderBytes = 16 << 10
	if model {
		p.server.Start()
	} else {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		now := localManagerClock{}.Now()
		cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "controlled fixture CA"}, DNSNames: []string{"api.tavily.com"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), BasicConstraintsValid: true, IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		p.roots = x509.NewCertPool()
		p.roots.AddCert(parsed)
		p.server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, NextProtos: []string{"http/1.1"}, MinVersion: tls.VersionTLS12}
		p.server.StartTLS()
	}
	t.Cleanup(func() {
		p.server.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, failure := range p.failures {
			t.Error(failure)
		}
		for _, body := range p.bodies {
			clear(body)
		}
	})
	return p
}

type searchLoopRemoteConn struct{ net.Conn }

func (searchLoopRemoteConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("8.8.8.8"), Port: 443}
}
func (p *searchLoopPeer) lookup(ctx context.Context, host string) ([]netip.Addr, error) {
	if ctx.Err() != nil || host != "api.tavily.com" {
		return nil, errors.New("closed controlled SEARCH DNS input")
	}
	return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
}
func (p *searchLoopPeer) dial(ctx context.Context, network, address string) (net.Conn, error) {
	if network != "tcp" || address != "8.8.8.8:443" {
		return nil, errors.New("closed controlled SEARCH socket input")
	}
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.server.Listener.Addr().String())
	if err != nil {
		return nil, err
	}
	return searchLoopRemoteConn{c}, nil
}

type searchLoopSecrets struct {
	*connectedCredentialSecretStore
	returned     [][]byte
	beforeBorrow func() error
}

func (s *searchLoopSecrets) ReadProviderCredentialSecret(ctx context.Context, b domain.ProviderCredentialBindingV1, n int64) ([]byte, error) {
	raw, err := s.connectedCredentialSecretStore.ReadProviderCredentialSecret(ctx, b, n)
	if err == nil {
		s.returned = append(s.returned, raw)
		if s.beforeBorrow != nil {
			if err := s.beforeBorrow(); err != nil {
				clear(raw)
				return nil, err
			}
		}
	}
	return raw, err
}

type searchLoopBlobs struct {
	*sessionCompactionBlobs
	event             domain.SessionEvent
	attempt           domain.AttemptID
	deletes, absences int
	putCalls          atomic.Uint64
}

func (b *searchLoopBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, reader io.Reader) (domain.BlobRef, error) {
	b.putCalls.Add(1)
	return b.sessionCompactionBlobs.Put(ctx, tenant, key, reader)
}

func (b *searchLoopBlobs) Delete(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || ref.Validate() != nil || ref.TenantID != tenant || b.event.RunID == nil {
		return errors.New("controlled SEARCH delete scope")
	}
	base := domain.SessionRunObjectPrefix(b.event.TenantID, b.event.SessionID, *b.event.RunID)
	if !strings.HasPrefix(ref.Key, base+"attempt-operations-v1/") && !strings.HasPrefix(ref.Key, base+"attempts/"+string(b.attempt)+"/consent-arguments-v1/") {
		return errors.New("controlled SEARCH delete outside original inventory namespace")
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("controlled SEARCH unbounded delete")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := string(tenant) + "\x00" + ref.Key
	if _, exists := b.objects[key]; !exists {
		return errors.New("controlled SEARCH delete missing object")
	}
	delete(b.objects, key)
	b.deletes++
	return nil
}
func (b *searchLoopBlobs) RequireBlobAbsent(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil {
		return errors.New("controlled SEARCH absence scope")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.objects[string(tenant)+"\x00"+ref.Key]; exists {
		return errors.New("controlled SEARCH object remains")
	}
	b.absences++
	return nil
}

func newSearchLoopWebHandler(t *testing.T, seed *ydbstore.LocalSearchWebSeed, blobs ports.BlobStore, auth *localTextWebAuth) http.Handler {
	t.Helper()
	path := filepath.Join(seed.Installation.Root, "search-web-receipt.json")
	slot, err := localoperator.ReserveProfileReceipt(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = slot.Close() })
	if err := slot.WriteReceipt(t.Context(), seed.OriginalReceipt()); err != nil {
		t.Fatal(err)
	}
	installation, err := localcontrolplane.Open(t.Context(), localcontrolplane.Config{Enabled: true, Installation: seed.Installation.Root, Executable: seed.Installation.Executable, Receipt: path})
	if err != nil || installation == nil {
		t.Fatal("independent BFF Source reopen", err)
	}
	t.Cleanup(func() { _ = installation.Close() })
	options, err := installation.WebStoreOptions(&searchBFFOpenOnlyBlobs{inner: blobs})
	if err != nil {
		t.Fatal(err)
	}
	consentStore, err := ydbstore.New(seed.NewBFFDatabase(t), options)
	if err != nil || consentStore == seed.Store {
		t.Fatal("independent BFF DATA reader/Store", err)
	}
	sessions, err := sessionapi.New(sessionapi.Config{CursorKey: bytes.Repeat([]byte{51}, 32), IDKey: bytes.Repeat([]byte{52}, 32)}, seed.Store, blobs, localManagerClock{})
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := sessioningress.New(sessioningress.Config{IDKey: bytes.Repeat([]byte{53}, 32), HarnessBinder: seed.Binder(t)}, seed.IngressStore(), blobs)
	if err != nil {
		t.Fatal(err)
	}
	api, err := webapi.New(webapi.Config{IDKey: bytes.Repeat([]byte{54}, 32)}, sessions, ingress, seed.Store, seed.Resources(t), localTextWebObjects{}, localManagerClock{})
	if err != nil {
		t.Fatal(err)
	}
	h, err := webbff.New(webbff.Config{BaseURL: localTextWebOrigin, RedirectURI: localTextWebOrigin + webcontract.RouteLoginCallback, ObjectStorageOrigin: "https://objects.fixture.invalid", OAuthProvider: &localTextWebIdentity{subject: "owner-subject", auth: auth}, OAuthClientID: localTextWebOAuthClient, Store: auth, Sessions: sessions, API: api, IDs: localTextWebIDs{}, Clock: localManagerClock{}, SearchConsents: consentStore, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// Actual normal Web ingress, queued admission, original Manager/entry, durable
// MODEL→consent→SEARCH→MODEL2 and original terminal/physical cleanup. Only SQL,
// credential bytes, identity and HTTP/TLS are controlled peers. Network overlay
// changes only absent test-support files, never production authority. Runtime
// uses the same private constructor/run/close path as public RunOnce; this is
// not literal public RunOnce, native YDB, real DNS or endpoint qualification.
func TestLocalSessionWebSearchOfflineNormalQueuedApproved(t *testing.T) {
	runLocalSessionWebSearchOffline(t, 1, "", "", false, time.Time{})
}

func TestLocalSessionWebSearchOfflineTwoProductiveAttemptsSameSource(t *testing.T) {
	runLocalSessionWebSearchOffline(t, 2, "", "", false, time.Time{})
}

func TestLocalSessionWebSearchOfflineActiveBorrowKeepsOriginalGeneration(t *testing.T) {
	runLocalSessionWebSearchOffline(t, 1, "", "", true, time.Time{})
}

func TestLocalSessionWebSearchOfflineCurrentAuthorityDeniesBeforeSend(t *testing.T) {
	for _, mode := range []string{"membership", "session", "claim_fence", "search_authority_revoked"} {
		t.Run(mode, func(t *testing.T) { runLocalSessionWebSearchOffline(t, 1, mode, "", false, time.Time{}) })
	}
}

func TestLocalSessionWebSearchOfflineSourceAndCallerClosureBeforeSend(t *testing.T) {
	for _, mode := range []string{"source_closed", "caller_cancelled"} {
		t.Run(mode, func(t *testing.T) { runLocalSessionWebSearchOffline(t, 1, mode, "", false, time.Time{}) })
	}
}

func TestLocalSessionWebSearchOfflineFailureTails(t *testing.T) {
	for _, mode := range []string{domain.SearchConsentStoppedV4, domain.SearchRejectedBeforeSendV4, domain.SearchServiceErrorV4, domain.SearchOutcomeUnknownV4, domain.ModelAfterSearchFailureV4} {
		t.Run(mode, func(t *testing.T) { runLocalSessionWebSearchOffline(t, 1, "", mode, false, time.Time{}) })
	}
}

func TestLocalSessionWebSearchOfflinePendingExpiryIsStoredStop(t *testing.T) {
	runLocalSessionWebSearchOffline(t, 1, "", domain.SearchConsentStoppedV4, false, time.Now().UTC().Truncate(time.Microsecond).Add(20*time.Second))
}

func TestLocalSessionWebSearchOfflineDeniedAtExpiryBoundaryRemainsDenied(t *testing.T) {
	runLocalSessionWebSearchOffline(t, 1, "boundary_deny", domain.SearchConsentStoppedV4, false, time.Now().UTC().Truncate(time.Microsecond).Add(20*time.Second))
}

func runLocalSessionWebSearchOffline(t *testing.T, attempts int, authorityMutation, failureMode string, rotateCurrentCredential bool, grantExpiry time.Time) {
	t.Helper()
	model, search := newSearchLoopPeer(t, true, attempts, failureMode), newSearchLoopPeer(t, false, attempts, failureMode)
	seed := ydbstore.NewLocalSearchWebSeed(t, model.server.URL+"/v1", grantExpiry)
	var pendingBoundary <-chan time.Time
	if authorityMutation == "boundary_deny" {
		pendingBoundary = seed.ObserveFinalPendingPoll(t)
	}
	model.modelID = seed.Installation.Configuration.ModelID
	if !seed.Empty() {
		t.Fatal("joined setup seeded invocation/funding/consent")
	}
	blobs := &searchLoopBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}
	auth := newTextWebAuth(seed.Tenant, seed.Owner)
	h := newSearchLoopWebHandler(t, seed, blobs, auth)
	cookie, csrf := localTextWebLogin(t, h)
	seed.SetWebSessionData(t, auth.sessions[domain.DigestSecret(cookie.Value)])
	path := "/api/web/v1/sessions/" + string(seed.Session) + "/messages"
	ref, err := domain.NewCredentialSecretRef("synthetic-controlled-search-reference")
	if err != nil {
		t.Fatal(err)
	}
	r := seed.Installation.Declaration.Resource
	seed.SetCredentialBindingData(t, domain.ProviderCredentialBindingV1{Version: 1, TenantID: seed.Tenant, OwnerUserID: seed.Owner, ResourceKind: r.Kind, ResourceID: r.ResourceID, ResourceRevision: r.Revision, CredentialGeneration: r.CredentialGeneration, CandidateMutationID: "synthetic-credential-installation", State: domain.ProviderCredentialActiveV1, SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte(searchLoopFixtureKey)), UpdatedAt: localManagerClock{}.Now()})
	secrets := &searchLoopSecrets{connectedCredentialSecretStore: &connectedCredentialSecretStore{ref: ref, secret: []byte(searchLoopFixtureKey)}}
	var mutationErr error
	var cancelBorrow context.CancelFunc
	mutated := false
	if authorityMutation != "" && authorityMutation != "boundary_deny" {
		secrets.beforeBorrow = func() error {
			if mutated {
				mutationErr = errors.New("current authority mutation repeated")
				return mutationErr
			}
			mutated = true
			switch authorityMutation {
			case "source_closed", "caller_cancelled":
				if mutationErr = seed.RequireApprovedSearchBorrowData(); mutationErr != nil {
					return mutationErr
				}
				if _, mutationErr = seed.Source.SearchCredentialConfiguration(); mutationErr != nil {
					return mutationErr
				}
				if authorityMutation == "source_closed" {
					mutationErr = seed.Source.Close()
				} else if cancelBorrow == nil {
					mutationErr = errors.New("late caller cancellation missing original active context")
				} else {
					cancelBorrow()
				}
			default:
				// Exercise a real sub-microsecond wall-clock sample through the
				// shared fixture path on every host, not just Linux clock hardware.
				sample := time.Now().UTC().Truncate(time.Microsecond).Add(789 * time.Nanosecond)
				mutationErr = seed.InvalidateCurrentSearchAuthorityData(authorityMutation, sample)
			}
			return mutationErr
		}
	}
	credentials, err := providercredential.NewInvocationService(providercredential.InvocationConfig{Clock: localManagerClock{}, IDs: &connectedCredentialIDs{}}, connectedCredentialPlanner{}, seed.Store, secrets)
	if err != nil {
		t.Fatal(err)
	}
	var runtime *worker.LocalRuntime
	var retryError error
	wakes := &localManagerQueue{}
	var previousRun domain.RunID
	for attempt := 1; attempt <= attempts; attempt++ {
		w := localTextWebRequest(h, http.MethodPost, path, fmt.Sprintf(`{"text":"use public compiler sources %d","idempotency_key":"original-owned-search-%d"}`, attempt, attempt), cookie, csrf)
		if w.Code != http.StatusCreated {
			t.Fatalf("normal SEARCH Web ingress status=%d body=%s SQL=%s", w.Code, w.Body.String(), seed.LastError())
		}
		if heads, audits := seed.FundingCounts(); heads != attempt*4 || audits != attempt*4 {
			t.Fatal("original binder did not publish exactly four real heads", heads, audits)
		}
		m := seed.AdmitManager(t)
		if m.Loaded.Run.ID == previousRun {
			t.Fatal("second productive message reused its predecessor Run")
		}
		previousRun = m.Loaded.Run.ID
		original, err := domain.DecodeAttemptExecutionSourceV1([]byte(m.Loaded.Job.OwnedExecutionSourceRecord))
		if err != nil || original.Profile.Digest != seed.OriginalReceipt().Digest || original.Profile.Locator != seed.OriginalReceipt().Profile.Locator || len(original.Spending) != 4 {
			t.Fatal("productive attempt changed the actual installed full profile", err)
		}
		blobs.event, blobs.attempt = m.Event, m.Loaded.Attempt.ID
		q := &localManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "normal-search-delivery", Kind: queuecontract.KindDispatchRun, TenantID: seed.Tenant, SubjectID: string(m.Loaded.Run.ID), EnqueuedAt: m.Loaded.Job.CreatedAt}}}
		retryError = nil
		if runtime == nil {
			runtime, err = worker.NewLocalRuntime(worker.LocalRuntimeConfig{Source: seed.Source, Receipt: seed.OriginalReceipt(), DB: seed.DB, Blobs: blobs, SearchCredentials: credentials, Worker: worker.Config{ScratchRoot: t.TempDir(), WorkerID: m.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, ProjectionWakePublisher: wakes, DeliveryWakePublisher: wakes, RetryObserver: func(err error) { retryError = err }}}, localManagerClock{})
			if err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		cancelBorrow = cancel
		defer cancel()
		// Intentional caller cancellation must not make a premature assertion
		// that the original cleanup has already joined. Observe completion until
		// the SAME absolute test deadline; this grants the runtime no new time.
		deadline, _ := ctx.Deadline()
		completionWait, stopCompletionWait := context.WithDeadline(t.Context(), deadline)
		defer stopCompletionWait()
		type completion struct {
			outcome worker.Outcome
			err     error
		}
		done := make(chan completion, 1)
		var shutdown chan func() error
		if failureMode == domain.SearchRejectedBeforeSendV4 {
			shutdown = make(chan func() error, 1)
		}
		waitPending, stopWaiting := context.WithCancel(ctx)
		defer stopWaiting()
		go func() {
			out, err := runtime.RunOnceSearchOffline(ctx, q, search.lookup, search.dial, search.roots, shutdown)
			done <- completion{out, err}
			stopWaiting()
		}()
		joined := false
		t.Cleanup(func() {
			cancel()
			if !joined {
				select {
				case <-done:
				case <-time.After(30 * time.Second):
					select {
					case <-done:
					default:
						t.Error("original SEARCH runtime did not join")
					}
				}
			}
		})
		var stopActualSearch func() error
		if shutdown != nil {
			select {
			case stopActualSearch = <-shutdown:
			case <-waitPending.Done():
				select {
				case stopActualSearch = <-shutdown:
				default:
					t.Fatal("original session did not publish its bounded shutdown-only test control")
				}
			}
		}
		pending, err := seed.WaitPending(waitPending)
		if err != nil {
			t.Fatalf("original MODEL never produced pending consent: %v SQL=%s trace=%s", err, seed.LastError(), m.Trace())
		}
		consentPath := "/api/web/v1/sessions/" + string(seed.Session) + "/runs/" + string(pending.Scope.RunID) + "/attempts/" + string(pending.Scope.AttemptID) + "/search-consents/" + string(pending.ID)
		get := httptest.NewRequest(http.MethodGet, localTextWebOrigin+consentPath, nil).WithContext(ctx)
		get.AddCookie(cookie)
		view := httptest.NewRecorder()
		h.ServeHTTP(view, get)
		var private webcontract.SearchConsentViewV1
		if view.Code != http.StatusOK || json.Unmarshal(view.Body.Bytes(), &private) != nil || private.Query != searchLoopQuery || private.MaxResults == nil || *private.MaxResults != 3 || private.Status != domain.WebSearchConsentPendingV1 {
			t.Fatalf("separate BFF exact original pending view status=%d body=%s SQL=%s", view.Code, view.Body.String(), seed.LastError())
		}
		choice := "approve"
		if failureMode == domain.SearchConsentStoppedV4 {
			choice = "deny"
		}
		if grantExpiry.IsZero() {
			decision := localTextWebRequest(h, http.MethodPost, consentPath, fmt.Sprintf(`{"expected_revision":1,"choice":%q}`, choice), cookie, csrf)
			if decision.Code != http.StatusOK {
				t.Fatalf("authenticated original consent decision status=%d body=%s SQL=%s", decision.Code, decision.Body.String(), seed.LastError())
			}
		} else {
			if !pending.ExpiresAt.Equal(grantExpiry) || !seed.Installation.GrantV2.ExpiresAt.Equal(grantExpiry) || !pending.CreatedAt.Before(grantExpiry) || !grantExpiry.Before(seed.Installation.Configuration.ExpiresAt) {
				t.Fatal("genuine request did not seal the pre-intake shorter grant cutoff")
			}
			if pendingBoundary == nil {
				t.Log("actual Source intake/publication, normal Web/Manager, durable MODEL and pending consent validated; no browser decision submitted")
			} else {
				var observed time.Time
				select {
				case observed = <-pendingBoundary:
				case <-waitPending.Done():
					select {
					case observed = <-pendingBoundary:
					default:
						t.Fatal("actual final healthy Pending interval not observed")
					}
				}
				if !observed.Before(pending.ExpiresAt) || observed.Add(5*time.Second).Before(pending.ExpiresAt) {
					t.Fatal("boundary observer did not see the actual final Pending interval")
				}
				decision := localTextWebRequest(h, http.MethodPost, consentPath, `{"expected_revision":1,"choice":"deny"}`, cookie, csrf)
				actual := seed.ConsentStateData(t)
				if decision.Code != http.StatusOK || actual.Status != domain.WebSearchConsentDeniedV1 || actual.Revision != 2 || actual.Decision == nil || actual.Decision.Choice != domain.WebSearchConsentDenyV1 || !actual.UpdatedAt.Before(pending.ExpiresAt) {
					t.Fatalf("actual authenticated boundary DENY failed status=%d consent=%s revision=%d remaining=%s", decision.Code, actual.Status, actual.Revision, pending.ExpiresAt.Sub(observed))
				}
				t.Log("real authenticated BFF DENY committed after final observed healthy Pending and before original sealed expiry")
			}
		}
		if failureMode == domain.SearchRejectedBeforeSendV4 {
			if err := seed.StopTransportAfterSearchReservation(stopActualSearch); err != nil {
				t.Fatal("post-reservation original transport shutdown prerequisite", err)
			}
		}
		if rotateCurrentCredential {
			if err := seed.RotateCredentialAfterSearchReservation(); err != nil {
				t.Fatal("post-reservation active-borrow rotation prerequisite", err)
			}
		}
		var result completion
		select {
		case result = <-done:
			joined = true
		case <-completionWait.Done():
			select {
			case result = <-done:
				joined = true
			default:
				t.Fatal("original searched runtime did not finish within its bound")
			}
		}
		if failureMode != "" {
			if !grantExpiry.IsZero() {
				actual := seed.ConsentStateData(t)
				validStop := actual.Status == domain.WebSearchConsentExpiredV1 && actual.Decision == nil && !actual.UpdatedAt.Before(pending.ExpiresAt)
				if pendingBoundary != nil {
					validStop = actual.Status == domain.WebSearchConsentDeniedV1 && actual.Decision != nil && actual.Decision.Choice == domain.WebSearchConsentDenyV1 && actual.UpdatedAt.Before(pending.ExpiresAt)
				}
				if !validStop || actual.Revision != 2 || !reflect.DeepEqual(actual.Request, pending) {
					t.Fatalf("original sealed expiry was not stored: status=%s revision=%d outcome=%s err=%v retry=%v MODEL=%d SEARCH=%d SQL=%s trace=%s", actual.Status, actual.Revision, result.outcome, result.err, retryError, model.calls.Load(), search.calls.Load(), seed.LastError(), m.Trace())
				}
			}
			if result.err != nil || retryError != nil || result.outcome != worker.OutcomeFailed || q.ack != 1 || q.retry != 0 {
				stopped, stopErr := seed.TransportStopReadback()
				t.Fatalf("actual supported failure not finalized mode=%s outcome=%s err=%v retry=%v MODEL=%d SEARCH=%d transportStopped=%t/%v SQL=%s trace=%s", failureMode, result.outcome, result.err, retryError, model.calls.Load(), search.calls.Load(), stopped, stopErr, seed.LastError(), m.Trace())
			}
			reservations, observations := m.OperationReadback(t)
			final := m.TerminalSnapshot(t)
			opCount, eventCount, modelCount, searchCount := 2, 3, uint32(1), uint32(1)
			outcome, reason := domain.AttemptOperationCompletedV1, domain.AttemptOperationReasonServiceErrorV1
			switch failureMode {
			case domain.SearchConsentStoppedV4:
				opCount, eventCount, searchCount, outcome, reason = 1, 1, 0, domain.AttemptOperationCompletedV1, domain.AttemptOperationReasonNoneV1
			case domain.SearchRejectedBeforeSendV4:
				searchCount, outcome, reason = 0, domain.AttemptOperationRejectedBeforeSendV1, domain.AttemptOperationReasonTransportUnknownV1
				stopped, stopErr := seed.TransportStopReadback()
				if !stopped || stopErr != nil {
					t.Fatal("original transport shutdown was vacuous", stopped, stopErr)
				}
			case domain.SearchOutcomeUnknownV4:
				outcome, reason = domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonMalformedV1
			case domain.ModelAfterSearchFailureV4:
				opCount, modelCount, outcome, reason = 3, 2, domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonMalformedV1
			}
			if len(reservations) != opCount || len(observations) != opCount || observations[opCount-1].Outcome != outcome || observations[opCount-1].Reason != reason || final.Run.Status != domain.RunFailed || final.Run.TerminalFailure == nil || final.Run.TerminalFailure.Stop != failureMode || final.Run.TerminalFailure.CapsuleSchema != "sessionless.terminal-failure-custody.v4" || final.Finalizations != 1 || len(final.Events) != eventCount || !m.StorageReleased() || model.calls.Load() != modelCount || search.calls.Load() != searchCount {
				t.Fatalf("actual failure frontier/custody mismatch mode=%s ops=%d observations=%d status=%s events=%d finals=%d MODEL=%d SEARCH=%d", failureMode, len(reservations), len(observations), final.Run.Status, len(final.Events), final.Finalizations, model.calls.Load(), search.calls.Load())
			}
			// joined_retained is NOT erasure. The original owner has fenced its
			// own adapter and retired the actual allocation before release. This
			// separate DATA-only reader proves the exact objects still exist; it
			// neither reconstructs that owner nor grants serving/deletion rights.
			if final.Run.TerminalFailure.Cleanup != "joined_retained" || domain.ValidateSHA256Digest("retired", final.Run.TerminalFailure.RetiredAllocationDigest) != nil || blobs.deletes != 0 || blobs.absences != 0 || final.Run.TerminalCustody == nil || final.Run.TerminalCustody.LocalEvidence != nil || final.Run.TerminalCustody.Correspondence != nil {
				t.Fatal("retained failure invented erasure/success custody")
			}
			dataReader, err := operationbody.NewV1(reservations[0].Operation.Scope, original.Profile.Locator.Profile, m.Loaded.Job.HarnessBinding.AgentLoopPolicy.Limits, blobs)
			if err != nil {
				t.Fatal("retained DATA reader", err)
			}
			knownBodies := 1 // exact actually appended consent arguments
			for i, r := range reservations {
				knownBodies++
				raw, err := dataReader.ReadAttemptOperationBody(ctx, r.Operation.Scope, r.Operation.RequestRef, r.Operation.RequestBytes)
				if err != nil || uint64(len(raw)) != r.Operation.RequestBytes {
					t.Fatal("actual retained request absent", err)
				}
				clear(raw)
				if observations[i].ResponseRef != "" {
					knownBodies++
					raw, err := dataReader.ReadAttemptOperationBody(ctx, r.Operation.Scope, observations[i].ResponseRef, observations[i].ResponseBytes)
					if err != nil || uint64(len(raw)) != observations[i].ResponseBytes {
						t.Fatal("actual retained observation body absent", err)
					}
					clear(raw)
				}
				if r.Operation.Kind == domain.AttemptOperationWebSearchV1 && (r.Operation.Debit.AmountMicrounits != 10000 || observations[i].Usage.State != domain.AttemptOperationUsageUnknownV1) {
					t.Fatal("failure refunded SEARCH or invented measured vendor USD")
				}
			}
			args, err := blobs.Open(ctx, seed.Tenant, pending.Arguments)
			if err != nil {
				t.Fatal("retained original consent arguments", err)
			}
			argsBody, argsErr := io.ReadAll(io.LimitReader(args, pending.Arguments.Size+1))
			argsClose := args.Close()
			argsValid := pending.ValidateArguments(argsBody)
			clear(argsBody)
			if argsErr != nil || argsClose != nil || argsValid != nil {
				t.Fatal("retained exact consent arguments invalid", argsErr, argsClose, argsValid)
			}
			if knownBodies < 3 || knownBodies > 6 {
				t.Fatal("unbounded retained failure inventory", knownBodies)
			}
			noticeEvent := final.Events[len(final.Events)-1]
			reader, err := blobs.Open(ctx, seed.Tenant, noticeEvent.Payload)
			if err != nil {
				t.Fatal("actual canonical failure notice body", err)
			}
			noticeBody, readErr := io.ReadAll(io.LimitReader(reader, (16<<10)+1))
			closeErr := reader.Close()
			defer clear(noticeBody)
			notice, noticeErr := domain.DecodeSearchFailureNoticeV4(noticeBody)
			if readErr != nil || closeErr != nil || noticeErr != nil || noticeEvent.Kind != domain.SessionEventSystemNotice || notice.Code != failureMode || notice.Outcome != outcome || notice.Reason != reason || notice.Operation != reservations[opCount-1].OperationDigest {
				t.Fatal("actual V4 notice misrepresented durable tail", readErr, closeErr, noticeErr)
			}
			for _, raw := range secrets.returned {
				for _, b := range raw {
					if b != 0 {
						t.Fatal("failure retained borrowed credential bytes")
					}
				}
			}
			// A fresh delivery of the SAME terminal Run/Attempt must take the
			// ordinary Manager terminal-readback branch, not an empty Receive.
			// The same Runtime/service builds its normal fresh private session.
			stats, writes := m.Stats(), blobs.putCalls.Load()
			credentialReads, keyReads := seed.CredentialReadCount(), len(secrets.returned)
			blobs.mu.Lock()
			objects := make(map[string][]byte, len(blobs.objects))
			for key, raw := range blobs.objects {
				objects[key] = bytes.Clone(raw)
			}
			blobs.mu.Unlock()
			redelivery := &localManagerQueue{message: q.message}
			redelivery.message.ReceiptHandle = "same-terminal-search-redelivery"
			redelivery.message.DeliveryCount++
			duplicateCtx, duplicateCancel := context.WithTimeout(t.Context(), 30*time.Second)
			duplicateOutcome, duplicateErr := runtime.RunOnceSearchOffline(duplicateCtx, redelivery, search.lookup, search.dial, search.roots, nil)
			duplicateCancel()
			if duplicateErr != nil || duplicateOutcome != worker.OutcomeDuplicate || redelivery.ack != 1 || redelivery.retry != 0 || redelivery.dead != 0 || retryError != nil || wakes.wake != 2 {
				t.Fatalf("terminal failure redelivery mode=%s outcome=%s err=%v retry=%v queue=%+v", failureMode, duplicateOutcome, duplicateErr, retryError, redelivery)
			}
			afterReservations, afterObservations := m.OperationReadback(t)
			if m.Stats() != stats || blobs.putCalls.Load() != writes || seed.CredentialReadCount() != credentialReads || len(secrets.returned) != keyReads || model.calls.Load() != modelCount || search.calls.Load() != searchCount || !reflect.DeepEqual(reservations, afterReservations) || !reflect.DeepEqual(observations, afterObservations) || !reflect.DeepEqual(final, m.TerminalSnapshot(t)) || !m.StorageReleased() || blobs.deletes != 0 || blobs.absences != 0 {
				t.Fatal("terminal redelivery repeated effects/debits/finalization or changed retained failure")
			}
			blobs.mu.Lock()
			objectsUnchanged := reflect.DeepEqual(objects, blobs.objects)
			blobs.mu.Unlock()
			for _, raw := range objects {
				clear(raw)
			}
			if !objectsUnchanged {
				t.Fatal("terminal redelivery replaced or erased original retained body bytes")
			}
			cancel()
			return
		}
		if authorityMutation != "" {
			if authorityMutation == "caller_cancelled" && !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatal("late caller cancellation was vacuous", ctx.Err())
			}
			if authorityMutation == "source_closed" {
				if _, err := seed.Source.SearchCredentialConfiguration(); err == nil {
					t.Fatal("late original Source closure was vacuous")
				}
			}
			if !mutated || mutationErr != nil || result.err != nil || retryError == nil || result.outcome != worker.OutcomeRetried || q.ack != 0 || q.retry != 1 || model.calls.Load() != 1 || search.calls.Load() != 0 || len(secrets.returned) != 1 {
				t.Fatalf("current authority denial missing/reached send: mutation=%t/%v outcome=%s err=%v retry=%v MODEL=%d SEARCH=%d SQL=%s", mutated, mutationErr, result.outcome, result.err, retryError, model.calls.Load(), search.calls.Load(), seed.LastError())
			}
			reservations, observations := m.OperationReadback(t)
			final := m.TerminalSnapshot(t)
			if len(reservations) != 1 || len(observations) != 1 || observations[0].Outcome != domain.AttemptOperationCompletedV1 || final.Finalizations != 0 || len(final.Events) != 0 || final.Run.Status.Terminal() || blobs.deletes != 0 || blobs.absences != 0 || m.StorageReleased() {
				t.Fatal("unreserved current-auth denial invented SEARCH/terminal/erasure/release")
			}
			for _, raw := range secrets.returned {
				for _, b := range raw {
					if b != 0 {
						t.Fatal("denied borrow retained credential bytes")
					}
				}
			}
			cancel()
			return
		}
		if result.err != nil || retryError != nil || result.outcome != worker.OutcomeCompleted || q.ack != 1 || q.retry != 0 {
			t.Fatalf("normal searched runtime outcome=%s err=%v retry=%v MODEL=%d SEARCH=%d credentialReads=%d materializations=%d SQL=%s trace=%s", result.outcome, result.err, retryError, model.calls.Load(), search.calls.Load(), seed.CredentialReadCount(), len(secrets.returned), seed.LastError(), m.Trace())
		}
		if rotateCurrentCredential {
			rotated, rotationErr := seed.CredentialRotationReadback()
			if !rotated || rotationErr != nil || len(secrets.returned) != 1 {
				t.Fatal("active original-generation borrow did not drain after actual current-row rotation", rotated, rotationErr)
			}
		}
		reservations, observations := m.OperationReadback(t)
		if len(reservations) != 3 || len(observations) != 3 || reservations[0].Operation.Kind != domain.AttemptOperationModelV1 || reservations[1].Operation.Kind != domain.AttemptOperationWebSearchV1 || reservations[2].Operation.Kind != domain.AttemptOperationModelV1 || reservations[1].Operation.Debit.AmountMicrounits != 10000 || observations[1].Usage.State != domain.AttemptOperationUsageUnknownV1 {
			t.Fatal("original exact MODEL/paid SEARCH/MODEL durable frontier")
		}
		final := m.TerminalSnapshot(t)
		if final.Run.Status != domain.RunSucceeded || final.Finalizations != 1 || len(final.Events) != 3 || !m.StorageReleased() || blobs.deletes != attempt*7 || blobs.absences != attempt*7 || model.calls.Load() != uint32(attempt*2) || search.calls.Load() != uint32(attempt) || seed.CredentialReadCount() < attempt*2 || len(secrets.returned) != attempt {
			t.Fatalf("original terminal/cleanup/one-shot counts status=%s events=%d finals=%d deletes=%d absence=%d model=%d search=%d credentials=%d", final.Run.Status, len(final.Events), final.Finalizations, blobs.deletes, blobs.absences, model.calls.Load(), search.calls.Load(), seed.CredentialReadCount())
		}
		for _, raw := range secrets.returned {
			for _, b := range raw {
				if b != 0 {
					t.Fatal("returned borrowed credential bytes were not zeroed")
				}
			}
		}
		func() {
			model.mu.Lock()
			defer model.mu.Unlock()
			if len(model.bodies) != attempt*2 || !bytes.Contains(model.bodies[attempt*2-1], []byte("example.org/compiler")) || !bytes.Contains(model.bodies[attempt*2-1], []byte("public compiler excerpt")) {
				t.Fatal("MODEL2 did not receive the actual durable typed SEARCH result")
			}
			if attempt == 2 && (!bytes.Contains(model.bodies[2], []byte("controlled searched final answer 1")) || !bytes.Contains(model.bodies[2], []byte("use public compiler sources 1")) || seed.PriorHistoryReads() == 0) {
				t.Fatal("second actual MODEL omitted the original successful SEARCH history")
			}
		}()
		cancel()
		if attempt < attempts {
			seed.BeginNextAfterSuccess(t, m)
		}
	}
}
