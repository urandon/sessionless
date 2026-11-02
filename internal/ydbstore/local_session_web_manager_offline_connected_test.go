//go:build sessionless_mcp_offline

package ydbstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/webapi"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/worker"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const webManagerOrigin = "https://web.fixture.invalid"
const webManagerOAuthClient = "owned-yandex-client"

// Yandex identity and auth persistence are controlled DATA boundaries, not a
// real Yandex adapter/user-info qualification. The actual OAuth BFF owns the
// bound one-use challenge, PKCE, cookies/CSRF and authorization, without an OIDC
// nonce or a Telegram dependency. Canonical authorization uses the real Store.
type webManagerIdentity struct {
	subject string
	auth    *webManagerAuth
	state   domain.SecretDigest
}

func (p *webManagerIdentity) AuthorizationURL(_ context.Context, r ports.OAuthAuthorizationRequest) (string, error) {
	c, ok := p.auth.challenges[domain.DigestSecret(r.State)]
	if !ok || c.Validate() != nil || c.Provider != domain.IdentityProviderYandex || c.Issuer != domain.YandexOAuthIssuer || c.Audience != webManagerOAuthClient || c.Nonce != "" || c.ConsumedAt != nil || r.RedirectURI != webManagerOrigin+webcontract.RouteLoginCallback {
		return "", errors.New("controlled Yandex authorization binding")
	}
	digest := sha256.Sum256([]byte(c.PKCEVerifier))
	if r.CodeChallenge != base64.RawURLEncoding.EncodeToString(digest[:]) {
		return "", errors.New("controlled Yandex PKCE challenge")
	}
	p.state = c.StateDigest
	query := url.Values{"state": {r.State}, "code_challenge": {r.CodeChallenge}, "client_id": {webManagerOAuthClient}, "redirect_uri": {r.RedirectURI}}
	return domain.YandexOAuthIssuer + "/authorize?" + query.Encode(), nil
}
func (p *webManagerIdentity) ExchangeAndVerify(_ context.Context, r ports.OAuthTokenRequest) (domain.ExternalSubject, error) {
	c, ok := p.auth.challenges[p.state]
	if !ok || c.Validate() != nil || c.Provider != domain.IdentityProviderYandex || c.Issuer != domain.YandexOAuthIssuer || c.Audience != webManagerOAuthClient || c.Nonce != "" || c.ConsumedAt == nil || r.Code != "owned-offline-code" || r.PKCEVerifier != c.PKCEVerifier || r.RedirectURI != webManagerOrigin+webcontract.RouteLoginCallback {
		return domain.ExternalSubject{}, errors.New("controlled Yandex token/source binding")
	}
	return domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: p.subject}, nil
}

type webManagerAuth struct {
	challenges map[domain.SecretDigest]domain.OIDCLoginChallenge
	sessions   map[domain.SecretDigest]domain.WebSession
	member     domain.TenantMembership
	identities map[string]domain.UserID
}

func (a *webManagerAuth) RecordWebSecurityEvent(_ context.Context, e domain.WebSecurityAuditEvent) error {
	return e.Validate()
}
func (a *webManagerAuth) CreateLoginChallenge(_ context.Context, c domain.OIDCLoginChallenge) error {
	if e := c.Validate(); e != nil {
		return e
	}
	if _, ok := a.challenges[c.StateDigest]; ok {
		return errors.New("duplicate challenge")
	}
	a.challenges[c.StateDigest] = c
	return nil
}
func (a *webManagerAuth) ConsumeLoginChallenge(_ context.Context, d domain.SecretDigest, secret string, at time.Time) (domain.OIDCLoginChallenge, error) {
	c, ok := a.challenges[d]
	if !ok {
		return c, domain.ErrMembershipDenied
	}
	e := c.Consume(secret, at)
	if e == nil {
		a.challenges[d] = c
	}
	return c, e
}
func (a *webManagerAuth) ResolveOrCreateExternalIdentity(_ context.Context, s domain.ExternalSubject, _ domain.UserID, at time.Time) (domain.ExternalIdentity, bool, error) {
	user, ok := a.identities[s.Subject]
	if !ok || s.Provider != domain.IdentityProviderYandex {
		return domain.ExternalIdentity{}, false, domain.ErrMembershipDenied
	}
	return domain.ExternalIdentity{Subject: s, UserID: user, CreatedAt: at, UpdatedAt: at}, false, nil
}
func (a *webManagerAuth) ListTenantMemberships(_ context.Context, u domain.UserID, limit uint64) ([]domain.TenantMembership, error) {
	if limit < 1 {
		return nil, errors.New("auth bounded membership limit")
	}
	if u != a.member.UserID {
		return nil, nil
	}
	return []domain.TenantMembership{a.member}, nil
}
func (*webManagerAuth) Enroll(context.Context, ports.EnrollmentRequest) (domain.TenantMembership, error) {
	return domain.TenantMembership{}, errors.New("no fixture enrollment")
}
func (*webManagerAuth) BootstrapDevelopmentMembership(context.Context, domain.DevelopmentBootstrapGrant) (domain.TenantMembership, error) {
	return domain.TenantMembership{}, errors.New("no fixture membership mint")
}
func (a *webManagerAuth) CreateWebSession(_ context.Context, s domain.WebSession) error {
	if e := s.Validate(); e != nil {
		return e
	}
	if _, ok := a.sessions[s.SessionDigest]; ok {
		return errors.New("duplicate Web session")
	}
	a.sessions[s.SessionDigest] = s
	return nil
}
func (a *webManagerAuth) AuthorizeWebSession(_ context.Context, d domain.SecretDigest, p domain.TenantPermission, at time.Time) (ports.WebAuthorization, error) {
	s, ok := a.sessions[d]
	if !ok {
		return ports.WebAuthorization{}, domain.ErrMembershipDenied
	}
	if e := s.Authorize(a.member, p, at); e != nil {
		return ports.WebAuthorization{}, e
	}
	return ports.WebAuthorization{Session: s, Membership: a.member}, nil
}
func (*webManagerAuth) SwitchTenant(context.Context, domain.SecretDigest, domain.WebSession, domain.TenantID, time.Time) (ports.WebAuthorization, error) {
	return ports.WebAuthorization{}, domain.ErrMembershipDenied
}
func (a *webManagerAuth) RevokeWebSession(_ context.Context, d domain.SecretDigest, at time.Time) error {
	s, ok := a.sessions[d]
	if !ok {
		return domain.ErrMembershipDenied
	}
	if e := s.Revoke(at); e != nil {
		return e
	}
	a.sessions[d] = s
	return nil
}

type webManagerIDs struct{}

func (webManagerIDs) NewID(_ context.Context, k ports.IDKind) (string, error) {
	if k != ports.IDUser {
		return "", errors.New("unexpected identity kind")
	}
	return "candidate-never-enrolled", nil
}

// Text-only normal ingress must never ask for an object capability or upload.
type webManagerObjects struct{}

func (webManagerObjects) PresignUpload(context.Context, ports.UploadCapabilityRequest) (ports.ObjectCapability, error) {
	return ports.ObjectCapability{}, errors.New("unexpected upload")
}
func (webManagerObjects) StatObject(context.Context, domain.TenantID, string) (ports.ObjectMetadata, error) {
	return ports.ObjectMetadata{}, errors.New("unexpected object stat")
}
func (webManagerObjects) PromoteObject(context.Context, ports.PromoteObjectRequest) (domain.BlobRef, error) {
	return domain.BlobRef{}, errors.New("unexpected object promotion")
}
func (webManagerObjects) PresignDownload(context.Context, domain.TenantID, domain.BlobRef, time.Duration) (ports.ObjectCapability, error) {
	return ports.ObjectCapability{}, errors.New("unexpected object capability")
}

func newWebManagerHandler(t *testing.T, seed *ydbstore.LocalWebManagerSeed, blobs ports.BlobStore, auth *webManagerAuth) http.Handler {
	t.Helper()
	sessions, e := sessionapi.New(sessionapi.Config{CursorKey: bytes.Repeat([]byte{51}, 32), IDKey: bytes.Repeat([]byte{52}, 32)}, seed.Store, blobs, localManagerClock{})
	if e != nil {
		t.Fatal(e)
	}
	ingress, e := sessioningress.New(sessioningress.Config{IDKey: bytes.Repeat([]byte{53}, 32), HarnessBinder: seed.Binder(t)}, seed.IngressStore(), blobs)
	if e != nil {
		t.Fatal(e)
	}
	api, e := webapi.New(webapi.Config{IDKey: bytes.Repeat([]byte{54}, 32), AllowedMCPServers: []string{"openai_docs"}}, sessions, ingress, seed.Store, seed.Resources(t), webManagerObjects{}, localManagerClock{})
	if e != nil {
		t.Fatal(e)
	}
	h, e := webbff.New(webbff.Config{BaseURL: webManagerOrigin, RedirectURI: webManagerOrigin + webcontract.RouteLoginCallback, ObjectStorageOrigin: "https://objects.fixture.invalid", OAuthProvider: &webManagerIdentity{subject: "owner-subject", auth: auth}, OAuthClientID: webManagerOAuthClient, Store: auth, Sessions: sessions, API: api, IDs: webManagerIDs{}, Clock: localManagerClock{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if e != nil {
		t.Fatal(e)
	}
	return h
}
func webManagerCookie(t *testing.T, r *http.Response, name string) *http.Cookie {
	t.Helper()
	defer r.Body.Close()
	for _, c := range r.Cookies() {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("missing %s", name)
	return nil
}
func webManagerLogin(t *testing.T, h http.Handler) (*http.Cookie, *http.Cookie) {
	t.Helper()
	start := httptest.NewRecorder()
	h.ServeHTTP(start, httptest.NewRequest(http.MethodGet, webManagerOrigin+webcontract.RouteLoginStart+"?return_to=/sessions", nil))
	if start.Code != http.StatusSeeOther {
		t.Fatal("login start", start.Code, start.Body.String())
	}
	u, e := url.Parse(start.Header().Get("Location"))
	if e != nil || u.Scheme != "https" || u.Host != "oauth.yandex.ru" || u.Path != "/authorize" || u.Query().Get("state") == "" || u.Query().Get("code_challenge") == "" || u.Query().Get("client_id") != webManagerOAuthClient || u.Query().Get("redirect_uri") != webManagerOrigin+webcontract.RouteLoginCallback {
		t.Fatal("controlled Yandex authorization URL", e)
	}
	r := httptest.NewRequest(http.MethodGet, webManagerOrigin+webcontract.RouteLoginCallback+"?code=owned-offline-code&state="+url.QueryEscape(u.Query().Get("state")), nil)
	r.AddCookie(webManagerCookie(t, start.Result(), webbff.LoginBindingCookieName))
	callback := httptest.NewRecorder()
	h.ServeHTTP(callback, r)
	if callback.Code != http.StatusSeeOther || callback.Header().Get("Location") != "/sessions" {
		t.Fatal("login callback", callback.Code, callback.Header().Get("Location"), callback.Body.String())
	}
	return webManagerCookie(t, callback.Result(), webcontract.SessionCookieName), webManagerCookie(t, callback.Result(), webcontract.CSRFCookieName)
}
func webManagerRequest(h http.Handler, method, path, body string, session, csrf *http.Cookie) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, webManagerOrigin+path, strings.NewReader(body))
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	r = r.WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	if session != nil {
		r.AddCookie(session)
	}
	if csrf != nil {
		r.AddCookie(csrf)
		r.Header.Set(webcontract.CSRFHeaderName, csrf.Value)
		r.Header.Set("Origin", webManagerOrigin)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func newWebManagerAuth(tenant domain.TenantID, user domain.UserID) *webManagerAuth {
	now := localManagerClock{}.Now()
	return &webManagerAuth{challenges: make(map[domain.SecretDigest]domain.OIDCLoginChallenge), sessions: make(map[domain.SecretDigest]domain.WebSession), identities: map[string]domain.UserID{"owner-subject": user}, member: domain.TenantMembership{TenantID: tenant, UserID: user, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: now, UpdatedAt: now}}
}

func TestLocalSessionCredentialedMCPOfflineWebCanonicalManager(t *testing.T) {
	model := newOfflineModelChainPeer(t)
	seed := ydbstore.NewLocalWebManagerSeed(t, model.endpoint())
	profile, e := seed.Source.Configuration()
	if e != nil {
		t.Fatal(e)
	}
	model.configure(t, profile.ModelID)
	if !seed.Empty() {
		t.Fatal("Web setup seeded request or spending authority")
	}
	auth := newWebManagerAuth(seed.Tenant, seed.Owner)
	blobs := &canonicalManagerBlobs{canonicalTerminalBlobs: &canonicalTerminalBlobs{sessionCompactionBlobs: newSessionCompactionBlobs(), owner: t}}
	h := newWebManagerHandler(t, seed, blobs, auth)
	for _, legacyRoute := range []string{webcontract.RouteOIDCStart, webcontract.RouteOIDCCallback} {
		if response := webManagerRequest(h, http.MethodGet, legacyRoute, "", nil, nil); response.Code != http.StatusNotFound {
			t.Fatal("Yandex Web handler enabled a Telegram login route", legacyRoute, response.Code)
		}
	}
	session, csrf := webManagerLogin(t, h)
	identity := webManagerRequest(h, http.MethodGet, webcontract.RouteMe, "", session, nil)
	if identity.Code != http.StatusOK || !bytes.Contains(identity.Body.Bytes(), []byte(`"provider":"yandex"`)) {
		t.Fatal("authenticated Yandex Web identity", identity.Code, identity.Body.String())
	}
	path := "/api/web/v1/sessions/" + string(seed.Session) + "/messages"
	body := `{"text":"public authenticated Web text","idempotency_key":"web-original-message"}`
	if w := webManagerRequest(h, http.MethodPost, path, body, nil, nil); w.Code != http.StatusUnauthorized || !seed.Empty() {
		t.Fatal("unauthenticated ingress", w.Code, w.Body.String())
	}
	if w := webManagerRequest(h, http.MethodPost, path, body, session, nil); w.Code != http.StatusForbidden || !seed.Empty() {
		t.Fatal("CSRF ingress", w.Code, w.Body.String())
	}
	// A valid authenticated Web identity is not canonical participant/source
	// authority. Both other-owner and other-tenant actors lack original DATA.
	for _, actor := range []struct {
		tenant domain.TenantID
		user   domain.UserID
	}{{seed.Tenant, "another-owner"}, {"another-tenant", seed.Owner}} {
		foreign := newWebManagerHandler(t, seed, blobs, newWebManagerAuth(actor.tenant, actor.user))
		s, c := webManagerLogin(t, foreign)
		w := webManagerRequest(foreign, http.MethodPost, path, body, s, c)
		if w.Code != http.StatusNotFound || !seed.Empty() {
			t.Fatal("foreign Web actor reached canonical authority", w.Code, w.Body.String())
		}
	}
	w := webManagerRequest(h, http.MethodPost, path, body, session, csrf)
	if w.Code != http.StatusCreated {
		t.Fatalf("normal authenticated Web ingress status=%d body=%s SQL=%s", w.Code, w.Body.String(), seed.LastError())
	}
	var response webcontract.CreateMessageResponse
	if e := json.Unmarshal(w.Body.Bytes(), &response); e != nil {
		t.Fatal(e)
	}
	heads, audits := seed.FundingCounts()
	if heads != 4 || audits != 4 {
		t.Fatal("actual binder did not publish exactly four original components", heads, audits)
	}
	originalStorage := seed.OriginalStorageDigest(t)
	managerSeed := seed.AdmitManager(t)
	blobs.seed = managerSeed.LocalCanonicalTerminalSeed
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	invocation, e := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: managerSeed.Prepare, Operate: managerSeed.Operate, Backend: managerSeed.Backend, Substrate: managerSeed.Substrate, Blobs: blobs})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := invocation.Close(); e != nil {
			t.Error(e)
		}
	})
	mcp := newOfflineMCPPeer(t, managerSeed.Grant.Descriptor, 4, nil, false)
	if e := invocation.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); e != nil {
		t.Fatal(e)
	}
	state, e := invocation.WorkerState()
	if e != nil {
		t.Fatal(e)
	}
	initial := managerSeed.Stats()
	if initial.LeasePresent || initial.Parents != 0 || initial.Burns != 0 || initial.Allocations != 0 || managerSeed.Loaded.Run.Status != domain.RunQueued || managerSeed.Loaded.Attempt.Status != domain.AttemptCreated {
		t.Fatal("Web ingress fabricated a running invocation")
	}
	q := &canonicalManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "web-manager-delivery", Kind: queuecontract.KindDispatchRun, TenantID: seed.Tenant, SubjectID: string(managerSeed.Loaded.Run.ID), EnqueuedAt: managerSeed.Loaded.Job.CreatedAt}}}
	var retryError error
	manager, e := worker.New(worker.Config{ScratchRoot: t.TempDir(), WorkerID: managerSeed.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, LocalOwnedSession: invocation, ProjectionWakePublisher: q, DeliveryWakePublisher: q, RetryObserver: func(e error) { retryError = e }}, localManagerClock{}, q, state, blobs, nil)
	if e != nil {
		t.Fatal(e)
	}
	q.beforeAck = func() error {
		if managerSeed.TerminalSnapshot(t).Finalizations != 1 || !managerSeed.TerminalStorageReleased() || q.wake != 1 || blobs.deletes != 6 || blobs.absences != 6 {
			return errors.New("Web ACK before terminal cleanup/wake")
		}
		if _, e := invocation.WorkerState(); e == nil {
			return errors.New("productive Web ACK before close")
		}
		return nil
	}
	outcome, e := manager.RunOnce(ctx)
	if e != nil || retryError != nil || outcome != worker.OutcomeCompleted || q.ack != 1 || q.retry != 0 || q.dead != 0 {
		t.Fatalf("actual Web queued Manager outcome=%s err=%v retry=%v trace=%s SQL=%s", outcome, e, retryError, managerSeed.TerminalTrace(), seed.LastError())
	}
	mcp.wait(t)
	wires := model.wait(t, false)
	if len(wires) != 2 {
		t.Fatal("Web model continuation count")
	}
	got := managerSeed.TerminalSnapshot(t)
	if got.Run.Status != domain.RunSucceeded || got.Attempt.Status != domain.AttemptSucceeded || got.Reservation.Status != domain.ReservationCommitted || got.Finalizations != 1 || got.Commits != 1 || len(got.Events) != 3 {
		t.Fatal("Web lineage did not finalize once")
	}
	principals, leases, runs, attempts := managerSeed.ManagerLifecycle()
	if principals < 1 || leases != 1 || runs != 1 || attempts != 1 {
		t.Fatal("Web Manager skipped actual claim/start", principals, leases, runs, attempts)
	}
	reservations, observed := managerSeed.OperationReadback(t)
	if len(reservations) != 3 || len(observed) != 3 || observed[2].Outcome != domain.AttemptOperationCompletedV1 {
		t.Fatal("Web durable model/tool/continuation frontier")
	}
	parent := managerSeed.ManagerParent(t)
	for i, r := range reservations {
		if r.Operation.Scope.PhysicalInvocationClaimID != parent.Reservation.PhysicalInvocationClaimID || r.Operation.Scope.LeaseID != parent.Authority.Lease.ID {
			t.Fatal("Web operation escaped original claimed invocation")
		}
		if i > 0 {
			digest, e := observed[i-1].Digest()
			if e != nil || r.Operation.PredecessorObservationDigest != digest {
				t.Fatal("Web successor before durable predecessor")
			}
		}
	}
	assertOfflinePairOnce(t, wires[0].body, 0, string(managerSeed.Loaded.Run.ID))
	assertOfflinePairOnce(t, wires[1].body, 1, string(managerSeed.Loaded.Run.ID))
	if managerSeed.Stats().Parents != 1 || managerSeed.Stats().Burns != 1 || managerSeed.Stats().Allocations != 2 || blobs.puts != 12 || blobs.capsulePuts != 1 || blobs.eventWrites != 3 || blobs.deletes != 6 || blobs.absences != 6 {
		t.Fatal("Web repeated/missed original custody/cleanup")
	}
	historyPath := "/api/web/v1/sessions/" + string(seed.Session) + "/events?limit=10"
	history := webManagerRequest(h, http.MethodGet, historyPath, "", session, nil)
	if history.Code != http.StatusOK {
		t.Fatalf("public history status=%d body=%s SQL=%s", history.Code, history.Body.String(), seed.LastError())
	}
	var originalHistory map[string]json.RawMessage
	if e := json.Unmarshal(history.Body.Bytes(), &originalHistory); e != nil {
		t.Fatal(e)
	}
	var publicEvents webcontract.Page[webcontract.SessionEvent]
	if json.Unmarshal(history.Body.Bytes(), &publicEvents) != nil || len(publicEvents.Items) != 4 || publicEvents.NextCursor != "" {
		t.Fatal("public canonical history frontier")
	}
	answers := 0
	for i, event := range publicEvents.Items {
		if event.Sequence != uint64(i+1) || event.RunID == nil || *event.RunID != managerSeed.Loaded.Run.ID {
			t.Fatal("public history changed canonical order/Run correlation")
		}
		if i == 0 {
			if event.EventID != response.EventID || event.Content.Text != "public authenticated Web text" {
				t.Fatal("public original Web event")
			}
		} else if event.EventID != got.Events[i-1].ID || event.Kind != got.Events[i-1].Kind {
			t.Fatal("public terminal event identity")
		}
		if event.Kind == domain.SessionEventAssistantMessage {
			answers++
			if event.Content.Text != "offline joined final answer" {
				t.Fatal("public durable final answer projection")
			}
		}
		if i > 0 && event.Kind != domain.SessionEventAssistantMessage {
			reader, err := blobs.Open(ctx, seed.Tenant, got.Events[i-1].Payload)
			if err != nil {
				t.Fatal("original public tool event body", err)
			}
			raw, readErr := io.ReadAll(io.LimitReader(reader, (1<<20)+1))
			closeErr := reader.Close()
			if readErr != nil || closeErr != nil || len(raw) > 1<<20 || !bytes.Equal(raw, event.Content.Data) {
				t.Fatal("public tool event changed original canonical data", readErr, closeErr)
			}
		}
	}
	if answers != 1 {
		t.Fatal("public history duplicated/missed final answer")
	}
	beforeStats, puts, deletes := managerSeed.Stats(), blobs.puts, blobs.deletes
	beforeHeads, beforeAudits := seed.FundingCounts()
	duplicate := webManagerRequest(h, http.MethodPost, path, body, session, csrf)
	if duplicate.Code != http.StatusOK {
		t.Fatalf("Web duplicate status=%d body=%s SQL=%s", duplicate.Code, duplicate.Body.String(), seed.LastError())
	}
	var replay webcontract.CreateMessageResponse
	if json.Unmarshal(duplicate.Body.Bytes(), &replay) != nil || replay.Created || replay.RunID != response.RunID || replay.EventID != response.EventID {
		t.Fatal("Web idempotency changed original result")
	}
	if managerSeed.Stats() != beforeStats || blobs.puts != puts || blobs.deletes != deletes || !reflect.DeepEqual(got, managerSeed.TerminalSnapshot(t)) {
		t.Fatal("Web duplicate repeated runtime/canonical effects")
	}
	if heads, audits := seed.FundingCounts(); heads != beforeHeads || audits != beforeAudits {
		t.Fatal("Web duplicate renewed Attempt funding")
	}
	conflict := webManagerRequest(h, http.MethodPost, path, `{"text":"different mutation","idempotency_key":"web-original-message"}`, session, csrf)
	if conflict.Code != http.StatusConflict {
		t.Fatal("Web conflicting mutation replaced original", conflict.Code, conflict.Body.String())
	}
	logout := webManagerRequest(h, http.MethodPost, webcontract.RouteLogout, "", session, csrf)
	if logout.Code != http.StatusNoContent {
		t.Fatal("Web logout", logout.Code)
	}
	if old := webManagerRequest(h, http.MethodGet, historyPath, "", session, nil); old.Code != http.StatusUnauthorized {
		t.Fatal("revoked Web cookie read history", old.Code)
	}
	relogin, recsrf := webManagerLogin(t, h)
	if relogin.Value == session.Value || recsrf.Value == csrf.Value {
		t.Fatal("login did not rotate browser secrets")
	}
	refreshed := webManagerRequest(h, http.MethodGet, historyPath, "", relogin, nil)
	if refreshed.Code != http.StatusOK {
		t.Fatal("relogin history", refreshed.Code, refreshed.Body.String())
	}
	var refreshHistory map[string]json.RawMessage
	if json.Unmarshal(refreshed.Body.Bytes(), &refreshHistory) != nil || !reflect.DeepEqual(originalHistory, refreshHistory) {
		t.Fatal("refresh/relogin changed original canonical answer")
	}
	for _, actor := range []struct {
		tenant domain.TenantID
		user   domain.UserID
	}{{seed.Tenant, "another-owner"}, {"another-tenant", seed.Owner}} {
		foreign := newWebManagerHandler(t, seed, blobs, newWebManagerAuth(actor.tenant, actor.user))
		s, _ := webManagerLogin(t, foreign)
		w := webManagerRequest(foreign, http.MethodGet, historyPath, "", s, nil)
		if w.Code != http.StatusNotFound {
			t.Fatal("foreign public history exposed answer", w.Code, w.Body.String())
		}
	}
	if managerSeed.Stats() != beforeStats || blobs.puts != puts || blobs.deletes != deletes || !reflect.DeepEqual(got, managerSeed.TerminalSnapshot(t)) {
		t.Fatal("public refresh/auth reads mutated original execution")
	}
	afterProfile, e := seed.Source.Configuration()
	life, lifeErr := seed.Source.ExecutionLifetime()
	if e != nil || lifeErr != nil || life.Err() != nil || !reflect.DeepEqual(profile, afterProfile) || seed.Source != managerSeed.Source || originalStorage != seed.OriginalStorageDigest(t) {
		t.Fatal("Web/Manager/readback changed borrowed original Source/profile/four publications")
	}
	t.Logf("Web actual queued Manager prelease=%d lease=%d start=%d/%d authority maxSQL=%d canonical raw=%d", principals, leases, runs, attempts, managerSeed.MaxCallbackSQL(), managerSeed.TerminalRawStatements())
}
