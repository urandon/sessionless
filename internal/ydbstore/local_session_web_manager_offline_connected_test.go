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

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
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

// Genuine original Web admission/Source/Manager path. Only the SQL and owned
// MODEL/TLS peers are synthetic. A known application error is neither MODEL2
// permission nor financially measured/zero usage, erasure or success evidence.
func TestLocalSessionCredentialedMCPOfflineWebKnownServiceError(t *testing.T) {
	runKnownServiceErrorWeb(t, false)
}

// The ordinary original engine sends MODEL2 only after an authenticated durable
// successful MCP result. A malformed HTTP200 is a persisted UNKNOWN outcome,
// never a fabricated service_error or assistant response. The known tool pair
// must survive Registry sanitization and the original failure transaction.
func TestLocalSessionCredentialedMCPOfflineWebModelAfterMCPFailure(t *testing.T) {
	runModelAfterMCPFailureWeb(t, false, "")
}
func TestLocalSessionCredentialedMCPOfflineWebModelAfterMCPFailureFreshHistory(t *testing.T) {
	runModelAfterMCPFailureWeb(t, true, "")
}
func TestLocalSessionCredentialedMCPOfflineWebModelAfterMCPFailureChangedHistory(t *testing.T) {
	runModelAfterMCPFailureWeb(t, true, "capsule_missing_tail")
}
func TestLocalSessionCredentialedMCPOfflineWebModelAfterMCPFailureCommitLoss(t *testing.T) {
	for _, fault := range []string{"rollback", "ambiguous"} {
		t.Run(fault, func(t *testing.T) { runModelAfterMCPFailureWeb(t, false, fault) })
	}
}
func TestLocalSessionCredentialedMCPOfflineWebModelAfterMCPFailureIncompleteTail(t *testing.T) {
	for _, fault := range []string{"missing_readback", "missing_tail", "changed_tail", "response_taint"} {
		t.Run(fault, func(t *testing.T) { runModelAfterMCPFailureWeb(t, false, fault) })
	}
}

type modelTailLostPutBlobs struct {
	ports.BlobStore
	ordinary, uncertain int
}

func (b *modelTailLostPutBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	ref, err := b.BlobStore.Put(ctx, tenant, key, r)
	if strings.Contains(key, "/attempt-operations-v1/") {
		b.ordinary++
		if b.ordinary == 6 && err == nil {
			b.uncertain++
			return domain.BlobRef{}, errors.New("controlled MODEL2 response PUT lost acknowledgement")
		}
	}
	return ref, err
}

func runModelAfterMCPFailureWeb(t *testing.T, next bool, fault string) {
	if fault != "" && fault != "rollback" && fault != "ambiguous" && fault != "missing_readback" && fault != "missing_tail" && fault != "changed_tail" && fault != "response_taint" && !(next && fault == "capsule_missing_tail") {
		t.Fatal("closed MODEL2 failure fault")
	}
	wantCalls := 2
	if next {
		wantCalls = 3
	}
	model := newOfflineModelChainPeer(t, wantCalls)
	seed := ydbstore.NewLocalWebManagerSeed(t, model.endpoint())
	profile, err := seed.Source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	model.configure(t, profile.ModelID)
	if fault != "response_taint" {
		model.malformedFinal(t, profile.ModelID)
	}
	blobs := &canonicalManagerBlobs{canonicalTerminalBlobs: &canonicalTerminalBlobs{sessionCompactionBlobs: newSessionCompactionBlobs(), owner: t}}
	h := newWebManagerHandler(t, seed, blobs, newWebManagerAuth(seed.Tenant, seed.Owner))
	cookie, csrf := webManagerLogin(t, h)
	path := "/api/web/v1/sessions/" + string(seed.Session) + "/messages"
	response := webManagerRequest(h, http.MethodPost, path, `{"text":"known tool then failed model","idempotency_key":"web-model-after-tool-failure"}`, cookie, csrf)
	if response.Code != http.StatusCreated {
		t.Fatalf("genuine Web admission status=%d SQL=%s", response.Code, seed.LastError())
	}
	managerSeed := seed.AdmitManager(t)
	if fault == "rollback" || fault == "ambiguous" {
		managerSeed.LoseNextTerminalCommit(t, fault)
	}
	if fault == "missing_readback" || fault == "missing_tail" || fault == "changed_tail" {
		seed.SetModelTailObservationFault(t, fault)
	}
	blobs.seed = managerSeed.LocalCanonicalTerminalSeed
	var storage ports.BlobStore = blobs
	tainted := &modelTailLostPutBlobs{BlobStore: blobs}
	if fault == "response_taint" {
		storage = tainted
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	invocation, err := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: managerSeed.Prepare, Operate: managerSeed.Operate, Backend: managerSeed.Backend, Substrate: managerSeed.Substrate, Blobs: storage})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := invocation.Close(); err != nil {
			t.Error("original model-failure session join", err)
		}
	})
	mcp := newOfflineMCPPeer(t, managerSeed.Grant.Descriptor, 4, nil, false)
	if err := invocation.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err != nil {
		t.Fatal(err)
	}
	state, err := invocation.WorkerState()
	if err != nil {
		t.Fatal(err)
	}
	q := &canonicalManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "web-model-failure-delivery", Kind: queuecontract.KindDispatchRun, TenantID: seed.Tenant, SubjectID: string(managerSeed.Loaded.Run.ID), EnqueuedAt: managerSeed.Loaded.Job.CreatedAt}}}
	var retryError error
	manager, err := worker.New(worker.Config{ScratchRoot: t.TempDir(), WorkerID: managerSeed.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, LocalOwnedSession: invocation, ProjectionWakePublisher: q, DeliveryWakePublisher: q, RetryObserver: func(err error) { retryError = err }}, localManagerClock{}, q, state, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	q.beforeAck = func() error {
		got := managerSeed.TerminalSnapshot(t)
		if got.Run.Status != domain.RunFailed || got.Attempt.Status != domain.AttemptFailed || got.Reservation.Status != domain.ReservationReleased || got.Finalizations != 1 || got.Run.TerminalFailure == nil || got.Run.TerminalFailure.Stop != domain.ModelOutcomeUnknownV2 || got.Run.TerminalCustody == nil || got.Run.TerminalCustody.LocalEvidence != nil || got.Run.TerminalCustody.Correspondence != nil || got.LeasePresent || got.ActiveRuns != 0 || q.wake != q.ack+1 || blobs.deletes != 0 || blobs.absences != 0 {
			return errors.New("MODEL failure ACK before truthful canonical failure/joined-retained release/wake")
		}
		if _, err := invocation.WorkerState(); err == nil {
			return errors.New("MODEL failure ACK before original session close")
		}
		return nil
	}
	outcome, runErr := manager.RunOnce(ctx)
	// Restore only the controlled read peer after the actual original owner
	// has stopped; persisted reservation/observation bytes are never repaired.
	seed.EndModelTailObservationFault()
	mcp.wait(t)
	model.waitCompleted(t, 2)
	if !next {
		model.wait(t, false)
	}
	reservations, observations := managerSeed.OperationReadback(t)
	wantReason := domain.AttemptOperationReasonMalformedV1
	if fault == "response_taint" {
		wantReason = domain.AttemptOperationReasonWorkerLostV1
	}
	if model.completedCalls() != 2 || len(reservations) != 3 || len(observations) != 3 || reservations[2].Operation.Kind != domain.AttemptOperationModelV1 || observations[1].Outcome != domain.AttemptOperationCompletedV1 || observations[1].Reason != domain.AttemptOperationReasonNoneV1 || observations[2].Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || observations[2].Reason != wantReason || observations[2].ResponseRef != "" || observations[2].ResponseDigest != "" || observations[2].ResponseBytes != 0 {
		t.Fatalf("actual sent MODEL2/authenticated UNKNOWN tail missing: completed=%d reservations=%d observations=%+v", model.completedCalls(), len(reservations), observations)
	}
	// The original decoder extracts the independent valid provider report
	// before rejecting finish_reason or writing its normalized response body.
	// UNKNOWN outcome/absent validated response must not erase that report.
	usage := observations[2].Usage
	if usage.State != domain.AttemptOperationUsageProviderReportedV1 || usage.InputTokens == nil || *usage.InputTokens != 1 || usage.OutputTokens == nil || *usage.OutputTokens != 1 || usage.AmountMicrounits != nil {
		t.Fatal("actual UNKNOWN MODEL tail lost independent provider-reported usage")
	}
	got := managerSeed.TerminalSnapshot(t)
	if fault == "missing_readback" || fault == "missing_tail" || fault == "changed_tail" || fault == "response_taint" || fault == "rollback" {
		if runErr != nil || retryError == nil || outcome != worker.OutcomeRetried || q.ack != 0 || q.wake != 0 || q.retry != 1 || got.Run.Status != domain.RunRunning || got.Run.TerminalFailure != nil || got.Finalizations != 0 || len(got.Events) != 0 {
			t.Fatalf("uncertain MODEL2 tail was salvaged into partial canonical failure: fault=%s outcome=%s err=%v retry=%v", fault, outcome, runErr, retryError)
		}
		if fault == "response_taint" && tainted.uncertain != 1 {
			t.Fatal("response PUT uncertainty was not exercised")
		}
		if (fault == "missing_readback" || fault == "missing_tail" || fault == "changed_tail") && seed.ModelTailObservationFaults() == 0 {
			t.Fatal("original persisted tail fault was not exercised")
		}
		puts := blobs.puts
		if again, err := manager.RunOnce(ctx); err != nil || again != worker.OutcomeRetried || blobs.puts != puts || model.completedCalls() != 2 {
			t.Fatal("uncertain tail redelivery performed another effect", again, err)
		}
		return
	}
	if runErr != nil || retryError != nil || outcome != worker.OutcomeFailed || q.ack != 1 || q.retry != 0 || q.wake != 1 || got.Run.Status != domain.RunFailed || got.Attempt.Status != domain.AttemptFailed || got.Run.TerminalFailure == nil || len(got.Events) != 3 || got.Finalizations != 1 {
		t.Fatalf("known durable MCP prefix lost after actually sent MODEL2 UNKNOWN: outcome=%s err=%v retry=%v events=%d finalizations=%d metadata=%t", outcome, runErr, retryError, len(got.Events), got.Finalizations, got.Run.TerminalFailure != nil)
	}
	if got.Events[0].Kind != domain.SessionEventToolCall || got.Events[1].Kind != domain.SessionEventToolResult || got.Events[2].Kind != domain.SessionEventSystemNotice || got.Manifest.ID != "" || got.EvidenceRecord != "" || got.Run.TerminalFailure.Version != 1 || got.Run.TerminalFailure.CapsuleSchema != "sessionless.terminal-failure-custody.v2" || got.Run.TerminalFailure.Stop != domain.ModelOutcomeUnknownV2 || !strings.HasPrefix(got.FinalDigest, "local_f1:") || blobs.eventWrites != 3 || blobs.capsulePuts != 1 || blobs.deletes != 0 {
		t.Fatal("MODEL2 failure changed original slots/marker or invented success/erasure")
	}
	// The known-success pair must retain the ordinary success wire, not acquire
	// an error wrapper just because the later actual MODEL response was invalid.
	pair := []ports.ExecutionToolEvent{
		{Kind: domain.SessionEventToolCall, CallID: "offline-call-1", ToolName: "search_openai_docs", Payload: []byte(`{"call_id":"offline-call-1","name":"search_openai_docs","arguments":{"query":"public docs"}}`)},
		{Kind: domain.SessionEventToolResult, CallID: "offline-call-1", ToolName: "search_openai_docs", Payload: []byte(`{"call_id":"offline-call-1","is_error":false,"content":["offline public docs result"]}`)},
	}
	expected, err := canonicaloutput.EncodeCompletionV1(got.Run.ID, managerSeed.Loaded.Job.Limits, ports.ExecutionResult{ToolEvents: pair}, "unused-success-manifest")
	if err != nil {
		t.Fatal(err)
	}
	defer expected.Release()
	notice, err := canonicaloutput.EncodeFailureV1(got.Run.ID, false, domain.ModelOutcomeUnknownV2)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(notice.Payload)
	wantEvents := append(expected.Events()[:2], notice)
	for i, event := range got.Events {
		blobs.mu.Lock()
		payload := bytes.Clone(blobs.objects[string(event.TenantID)+"\x00"+event.Payload.Key])
		blobs.mu.Unlock()
		same := event.ID == wantEvents[i].ID && bytes.Equal(payload, wantEvents[i].Payload)
		clear(payload)
		if !same {
			t.Fatal("actual MODEL failure changed successful canonical pair or fixed notice", i)
		}
	}
	if fault == "ambiguous" && got.LostACKs != 1 {
		t.Fatal("exact ambiguous F2 commit was not observed")
	}
	var capsule struct {
		Schema     string                                  `json:"schema"`
		Version    uint32                                  `json:"version"`
		Accounting ports.OwnedOperationFailureAccountingV2 `json:"accounting"`
		Bodies     []domain.BlobRef                        `json:"retained_bodies"`
		Operations []struct {
			Reservation domain.AttemptOperationReservationV1 `json:"reservation"`
			Observation domain.AttemptOperationObservationV1 `json:"observation"`
			Normalized  json.RawMessage                      `json:"normalized"`
		} `json:"operations"`
	}
	blobs.mu.Lock()
	retained := bytes.Clone(blobs.objects[string(seed.Tenant)+"\x00"+got.Run.TerminalCustody.Plan.Capsule.Key])
	blobs.mu.Unlock()
	defer clear(retained)
	if json.Unmarshal(retained, &capsule) != nil || capsule.Schema != "sessionless.terminal-failure-custody.v2" || capsule.Version != 2 || len(capsule.Operations) != 3 || !bytes.Equal(capsule.Operations[2].Normalized, []byte("null")) || capsule.Accounting.Version != 2 || capsule.Accounting.AggregateDebit != reservations[2].AggregateDebit || capsule.Accounting.ComponentAmounts != reservations[2].ComponentAmounts || len(capsule.Bodies) != 5 || bytes.Contains(retained, []byte("unvalidated model bytes")) {
		t.Fatal("F2 lost complete original UNKNOWN tail/debits or invented response")
	}
	for i, op := range capsule.Operations {
		if !reflect.DeepEqual(op.Reservation, reservations[i]) || !reflect.DeepEqual(op.Observation, observations[i]) || !reflect.DeepEqual(capsule.Accounting.Operations[i].Usage, observations[i].Usage) {
			t.Fatal("F2 capsule/accounting changed authentic frontier", i)
		}
	}
	for _, mode := range []string{"positive", "missing_observation", "missing_parent", "changed_marker", "changed_accounting", "missing_body"} {
		if err := seed.CheckKnownFailureDeletion(t, managerSeed, blobs.sessionCompactionBlobs, mode); err != nil {
			t.Fatal("F2 public retained deletion", mode, err)
		}
	}
	puts, stats := blobs.puts, managerSeed.Stats()
	if again, err := manager.RunOnce(ctx); err != nil || again != worker.OutcomeDuplicate || blobs.puts != puts || managerSeed.Stats() != stats || model.completedCalls() != 2 {
		t.Fatal("committed F2 duplicate replayed an old effect", again, err)
	}
	if next {
		if fault == "capsule_missing_tail" {
			runNextAfterKnownFailure(t, seed, managerSeed, blobs, h, cookie, csrf, model, fault)
		} else {
			runNextAfterKnownFailure(t, seed, managerSeed, blobs, h, cookie, csrf, model)
		}
	}
}
func TestLocalSessionCredentialedMCPOfflineWebKnownServiceErrorFreshHistory(t *testing.T) {
	runKnownServiceErrorWeb(t, true)
}
func TestLocalSessionCredentialedMCPOfflineWebKnownServiceErrorChangedNoticeDenied(t *testing.T) {
	for _, mode := range []string{"changed_notice", "missing_notice", "metadata_stripped", "marker_stripped"} {
		t.Run(mode, func(t *testing.T) { runKnownServiceErrorWeb(t, true, mode) })
	}
}
func TestLocalSessionCredentialedMCPOfflineWebKnownServiceErrorCommitLoss(t *testing.T) {
	for _, mode := range []string{"rollback", "ambiguous"} {
		t.Run(mode, func(t *testing.T) { runKnownServiceErrorWeb(t, false, mode) })
	}
}
func runKnownServiceErrorWeb(t *testing.T, next bool, commitFault ...string) {
	if len(commitFault) > 1 || len(commitFault) == 1 && ((!next && (commitFault[0] != "rollback" && commitFault[0] != "ambiguous")) || (next && commitFault[0] != "changed_notice" && commitFault[0] != "missing_notice" && commitFault[0] != "metadata_stripped" && commitFault[0] != "marker_stripped")) {
		t.Fatal("closed first F1 terminal commit fault")
	}
	want := 1
	if next {
		want = 2
	}
	model := newOfflineModelChainPeer(t, want)
	seed := ydbstore.NewLocalWebManagerSeed(t, model.endpoint())
	profile, err := seed.Source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	model.configure(t, profile.ModelID)
	blobs := &canonicalManagerBlobs{canonicalTerminalBlobs: &canonicalTerminalBlobs{sessionCompactionBlobs: newSessionCompactionBlobs(), owner: t}}
	h := newWebManagerHandler(t, seed, blobs, newWebManagerAuth(seed.Tenant, seed.Owner))
	cookie, csrf := webManagerLogin(t, h)
	path := "/api/web/v1/sessions/" + string(seed.Session) + "/messages"
	body := `{"text":"known application error","idempotency_key":"web-service-error"}`
	ingress := webManagerRequest(h, http.MethodPost, path, body, cookie, csrf)
	if ingress.Code != http.StatusCreated {
		t.Fatalf("genuine Web admission status=%d SQL=%s", ingress.Code, seed.LastError())
	}
	originalStorage := seed.OriginalStorageDigest(t)
	managerSeed := seed.AdmitManager(t)
	if len(commitFault) == 1 && !next {
		managerSeed.LoseNextTerminalCommit(t, commitFault[0])
	}
	blobs.seed = managerSeed.LocalCanonicalTerminalSeed
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	invocation, err := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: managerSeed.Prepare, Operate: managerSeed.Operate, Backend: managerSeed.Backend, Substrate: managerSeed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := invocation.Close(); err != nil {
			t.Error("original session join", err)
		}
	})
	mcp := newOfflineMCPPeer(t, managerSeed.Grant.Descriptor, 4, nil, false, true)
	if err := invocation.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err != nil {
		t.Fatal(err)
	}
	state, err := invocation.WorkerState()
	if err != nil {
		t.Fatal(err)
	}
	q := &canonicalManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "web-failure-delivery", Kind: queuecontract.KindDispatchRun, TenantID: seed.Tenant, SubjectID: string(managerSeed.Loaded.Run.ID), EnqueuedAt: managerSeed.Loaded.Job.CreatedAt}}}
	var retryError error
	manager, err := worker.New(worker.Config{ScratchRoot: t.TempDir(), WorkerID: managerSeed.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, LocalOwnedSession: invocation, ProjectionWakePublisher: q, DeliveryWakePublisher: q, RetryObserver: func(err error) { retryError = err }}, localManagerClock{}, q, state, blobs, nil)
	if err != nil {
		t.Fatal(err)
	}
	q.beforeAck = func() error {
		got := managerSeed.TerminalSnapshot(t)
		if got.Run.Status != domain.RunFailed || got.Attempt.Status != domain.AttemptFailed || got.Reservation.Status != domain.ReservationReleased || got.Finalizations != 1 || got.Run.TerminalFailure == nil || got.Run.TerminalCustody == nil || got.Run.TerminalCustody.LocalEvidence != nil || got.Run.TerminalCustody.Correspondence != nil || got.LeasePresent || got.ActiveRuns != 0 || q.wake != q.ack+1 || blobs.deletes != 0 || blobs.absences != 0 {
			return errors.New("F1 ACK before truthful canonical failure/joined-retained release/wake")
		}
		if _, err := invocation.WorkerState(); err == nil {
			return errors.New("F1 ACK before original session close")
		}
		return nil
	}
	outcome, err := manager.RunOnce(ctx)
	if len(commitFault) == 1 && !next {
		if commitFault[0] == "rollback" {
			if err != nil || retryError == nil || outcome != worker.OutcomeRetried || q.retry != 1 || q.ack != 0 || q.wake != 0 || q.dead != 0 {
				t.Fatalf("uncommitted F1 was acknowledged outcome=%s err=%v retry=%v", outcome, err, retryError)
			}
		} else if err != nil || retryError != nil || outcome != worker.OutcomeFailed || q.retry != 0 || q.ack != 1 || q.wake != 1 || q.dead != 0 {
			t.Fatalf("same-budget exact committed observation unavailable outcome=%s err=%v retry=%v", outcome, err, retryError)
		}
		mcp.wait(t)
		model.waitCompleted(t, 1)
		model.wait(t, false)
		got := managerSeed.TerminalSnapshot(t)
		if got.LostACKs != 1 || blobs.eventWrites != 3 || blobs.capsulePuts != 1 || blobs.deletes != 0 || blobs.absences != 0 {
			t.Fatal("commit uncertainty changed one-use physical output/retained disposition")
		}
		want := worker.OutcomeDuplicate
		if commitFault[0] == "rollback" {
			want = worker.OutcomeRetried
			if got.Run.Status != domain.RunRunning || got.Finalizations != 0 || got.Run.TerminalFailure != nil || len(got.Events) != 0 {
				t.Fatal("rolled-back F1 became known canonical failure")
			}
		} else if got.Run.Status != domain.RunFailed || got.Finalizations != 1 || got.Run.TerminalFailure == nil || len(got.Events) != 3 {
			t.Fatal("persisted ambiguous F1 not exactly retained")
		}
		before, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		stats, puts := managerSeed.Stats(), blobs.puts
		if outcome, err := manager.RunOnce(ctx); err != nil || outcome != want {
			t.Fatal("repeat did not observe known commit or reconcile only", outcome, err)
		}
		after, err := json.Marshal(managerSeed.TerminalSnapshot(t))
		if err != nil || !bytes.Equal(before, after) || managerSeed.Stats() != stats || blobs.puts != puts || blobs.deletes != 0 || model.completedCalls() != 1 || seed.OriginalStorageDigest(t) != originalStorage {
			t.Fatal("repeat uncertainty replayed/refunded original operation or rewrote canonical state")
		}
		reservations, observed := managerSeed.OperationReadback(t)
		if len(reservations) != 2 || len(observed) != 2 || observed[1].Reason != domain.AttemptOperationReasonServiceErrorV1 || observed[1].Usage.State != domain.AttemptOperationUsageUnknownV1 {
			t.Fatal("commit uncertainty changed authenticated complete frontier/UNKNOWN money")
		}
		return
	}
	if err != nil || retryError != nil || outcome != worker.OutcomeFailed || q.ack != 1 || q.retry != 0 || q.dead != 0 {
		t.Fatalf("known service error outcome=%s err=%v retry=%v trace=%s SQL=%s puts=%d events=%d canonical=%d", outcome, err, retryError, managerSeed.TerminalTrace(), seed.LastError(), blobs.puts, blobs.eventWrites, managerSeed.TerminalSnapshot(t).Finalizations)
	}
	mcp.wait(t)
	model.waitCompleted(t, 1)
	if model.completedCalls() != 1 {
		t.Fatal("service error continued MODEL")
	}
	if !next {
		model.wait(t, false)
	}
	got := managerSeed.TerminalSnapshot(t)
	if len(got.Events) != 3 || got.Events[0].Kind != domain.SessionEventToolCall || got.Events[1].Kind != domain.SessionEventToolResult || got.Events[2].Kind != domain.SessionEventSystemNotice || got.Manifest.ID != "" || got.EvidenceRecord != "" || !strings.HasPrefix(got.FinalDigest, "local_f1:") {
		t.Fatal("F1 did not publish exactly pair plus fixed notice without success metadata")
	}
	reservations, observed := managerSeed.OperationReadback(t)
	if len(reservations) != 2 || len(observed) != 2 || observed[1].Outcome != domain.AttemptOperationCompletedV1 || observed[1].Reason != domain.AttemptOperationReasonServiceErrorV1 || observed[1].Usage.State != domain.AttemptOperationUsageUnknownV1 || reservations[1].Operation.Debit.AmountMicrounits != *managerSeed.ManagerParent(t).Authority.AdmissionCostCeiling.MCPToolCost.MaxAmountMicrounits {
		t.Fatal("durable known failure/full reserved UNKNOWN money frontier")
	}
	stats, puts := managerSeed.Stats(), blobs.puts
	if outcome, err := manager.RunOnce(ctx); err != nil || outcome != worker.OutcomeDuplicate || q.ack != 2 || q.wake != 2 || q.retry != 0 || q.dead != 0 || managerSeed.Stats() != stats || blobs.puts != puts || !reflect.DeepEqual(got, managerSeed.TerminalSnapshot(t)) {
		t.Fatal("duplicate failed delivery repeated effects or canonical failure", outcome, err)
	}
	duplicate := webManagerRequest(h, http.MethodPost, path, body, cookie, csrf)
	if duplicate.Code != http.StatusOK {
		t.Fatal("duplicate Web admission", duplicate.Code)
	}
	historyPath := "/api/web/v1/sessions/" + string(seed.Session) + "/events?limit=10"
	history := webManagerRequest(h, http.MethodGet, historyPath, "", cookie, nil)
	if history.Code != http.StatusOK || !bytes.Contains(history.Body.Bytes(), []byte(`service_error`)) {
		t.Fatal("canonical known error history", history.Code, history.Body.String())
	}
	if managerSeed.Stats() != stats || blobs.puts != puts || blobs.deletes != 0 || !reflect.DeepEqual(got, managerSeed.TerminalSnapshot(t)) || seed.OriginalStorageDigest(t) != originalStorage {
		t.Fatal("failure duplicate/history mutated original effects/funding")
	}
	for _, mode := range []string{"positive", "missing_observation", "missing_parent", "changed_marker", "changed_accounting", "missing_body"} {
		t.Run("retained_deletion_"+mode, func(t *testing.T) {
			before, err := json.Marshal(managerSeed.TerminalSnapshot(t))
			if err != nil {
				t.Fatal(err)
			}
			blobs.mu.Lock()
			objects := make(map[string][]byte, len(blobs.objects))
			for key, body := range blobs.objects {
				objects[key] = bytes.Clone(body)
			}
			blobs.mu.Unlock()
			defer func() {
				for _, body := range objects {
					clear(body)
				}
			}()
			if err := seed.CheckKnownFailureDeletion(t, managerSeed, blobs, mode); err != nil {
				t.Fatal(err)
			}
			after, err := json.Marshal(managerSeed.TerminalSnapshot(t))
			if err != nil || !bytes.Equal(before, after) || managerSeed.Stats() != stats || blobs.puts != puts || blobs.deletes != 0 || seed.OriginalStorageDigest(t) != originalStorage {
				t.Fatal("isolated deletion changed original serving rows/effects")
			}
			blobs.mu.Lock()
			unchanged := reflect.DeepEqual(objects, blobs.objects)
			blobs.mu.Unlock()
			if !unchanged {
				t.Fatal("isolated deletion changed original retained serving bytes")
			}
		})
	}
	if next {
		runNextAfterKnownFailure(t, seed, managerSeed, blobs, h, cookie, csrf, model, commitFault...)
	}
}

func runNextAfterKnownFailure(t *testing.T, seed *ydbstore.LocalWebManagerSeed, prior *ydbstore.LocalCanonicalManagerSeed, oldBlobs *canonicalManagerBlobs, h http.Handler, cookie, csrf *http.Cookie, model *offlineModelChainPeer, historyFault ...string) {
	t.Helper()
	priorCalls := model.completedCalls()
	if priorCalls != 1 && priorCalls != 2 {
		t.Fatal("closed prior MODEL count")
	}
	priorSnapshot := prior.TerminalSnapshot(t)
	seed.BeginNextAfterKnownFailure(t, prior)
	if len(historyFault) == 1 && historyFault[0] == "capsule_missing_tail" {
		key := string(priorSnapshot.Run.TenantID) + "\x00" + priorSnapshot.Run.TerminalCustody.Plan.Capsule.Key
		oldBlobs.mu.Lock()
		original := bytes.Clone(oldBlobs.objects[key])
		var fields map[string]json.RawMessage
		var operations []json.RawMessage
		if json.Unmarshal(original, &fields) != nil || json.Unmarshal(fields["operations"], &operations) != nil || len(operations) != 3 {
			oldBlobs.mu.Unlock()
			t.Fatal("actual V2 complete capsule before corruption")
		}
		changed, err := json.Marshal(operations[:2])
		if err != nil {
			oldBlobs.mu.Unlock()
			t.Fatal(err)
		}
		oldBlobs.objects[key] = bytes.Replace(original, fields["operations"], changed, 1)
		oldBlobs.mu.Unlock()
		t.Cleanup(func() { oldBlobs.mu.Lock(); oldBlobs.objects[key] = original; oldBlobs.mu.Unlock() })
	} else if len(historyFault) == 1 && historyFault[0] == "changed_notice" {
		notice, err := canonicaloutput.EncodeFailureV1(priorSnapshot.Run.ID, false, "harness_failed")
		if err != nil {
			t.Fatal(err)
		}
		defer clear(notice.Payload)
		var event domain.SessionEvent
		for _, e := range priorSnapshot.Events {
			if e.Kind == domain.SessionEventSystemNotice {
				event = e
			}
		}
		if event.ID == "" {
			t.Fatal("actual first F1 notice absent")
		}
		ref, err := oldBlobs.sessionCompactionBlobs.Put(t.Context(), event.TenantID, domain.SessionEventObjectPrefix(event.TenantID, event.SessionID, event.ID)+"changed-notice.json", bytes.NewReader(notice.Payload))
		if err != nil {
			t.Fatal(err)
		}
		seed.ReplacePriorKnownFailureNotice(t, ref)
	} else if len(historyFault) == 1 {
		seed.CorruptPriorKnownFailureCorrespondence(t, historyFault[0])
	}
	path := "/api/web/v1/sessions/" + string(seed.Session) + "/messages"
	response := webManagerRequest(h, http.MethodPost, path, `{"text":"new explicit user request after known error","idempotency_key":"after-known-error"}`, cookie, csrf)
	if response.Code != http.StatusCreated {
		for i, statement := range seed.LastCommitStatements() {
			t.Logf("intake_statement[%d]=%s", i+1, statement)
		}
		t.Fatalf("actual next Web request status=%d body=%s SQL=%s", response.Code, response.Body.String(), seed.LastError())
	}
	next := seed.AdmitManager(t)
	blobs := &canonicalManagerBlobs{canonicalTerminalBlobs: &canonicalTerminalBlobs{sessionCompactionBlobs: oldBlobs.sessionCompactionBlobs, owner: t, seed: next.LocalCanonicalTerminalSeed}}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	invocation, err := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: next.Prepare, Operate: next.Operate, Backend: next.Backend, Substrate: next.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := invocation.Close(); err != nil {
			t.Error(err)
		}
	})
	state, err := invocation.WorkerState()
	if err != nil {
		t.Fatal(err)
	}
	q := &canonicalManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "after-error-delivery", Kind: queuecontract.KindDispatchRun, TenantID: seed.Tenant, SubjectID: string(next.Loaded.Run.ID), EnqueuedAt: next.Loaded.Job.CreatedAt}}}
	var retried error
	manager, err := worker.New(worker.Config{ScratchRoot: t.TempDir(), WorkerID: next.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, LocalOwnedSession: invocation, ProjectionWakePublisher: q, DeliveryWakePublisher: q, RetryObserver: func(err error) { retried = err }}, localManagerClock{}, q, state, blobs, nil)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := manager.RunOnce(ctx)
	if len(historyFault) == 1 {
		reservations, observed := next.OperationReadback(t)
		if err != nil || retried == nil || outcome != worker.OutcomeRetried || q.ack != 0 || q.retry != 1 || q.dead != 0 || model.completedCalls() != priorCalls || len(reservations) != 0 || len(observed) != 0 || blobs.puts != 0 {
			t.Fatalf("changed valid notice bypassed retained F1 before effect: outcome=%s err=%v retry=%v MODEL=%d reservations=%d puts=%d", outcome, err, retried, model.completedCalls(), len(reservations), blobs.puts)
		}
		return
	}
	if err != nil || retried != nil || outcome != worker.OutcomeCompleted || q.ack != 1 {
		t.Fatalf("fresh original serving history outcome=%s err=%v retry=%v trace=%s SQL=%s", outcome, err, retried, next.TerminalTrace(), seed.LastError())
	}
	wires := model.wait(t, false)
	if len(wires) != priorCalls+1 {
		t.Fatal("fresh MODEL request count")
	}
	var wire globalClosureWire
	var history globalClosureHistory
	if json.Unmarshal(wires[priorCalls].body, &wire) != nil || len(wire.Messages) != 2 || json.Unmarshal([]byte(wire.Messages[1].Content), &history) != nil || history.Schema != "sessionless.history.v1" {
		t.Fatal("actual fresh MODEL frozen history grammar")
	}
	for _, event := range prior.TerminalSnapshot(t).Events {
		oldBlobs.mu.Lock()
		payload := bytes.Clone(oldBlobs.objects[string(event.TenantID)+"\x00"+event.Payload.Key])
		oldBlobs.mu.Unlock()
		if len(payload) == 0 {
			t.Fatal("original failed canonical body absent")
		}
		found := 0
		for _, frame := range append(history.Protected, history.Units...) {
			if frame.EventID == string(event.ID) {
				found++
				if frame.Kind != "canonical" || frame.RunID != string(prior.Loaded.Run.ID) || frame.Role != string(event.Kind) || frame.Sequence != event.Sequence || frame.Digest != event.Payload.SHA256 || frame.Payload != string(payload) {
					t.Fatal("fresh input changed exact authenticated failed canonical frame")
				}
			}
		}
		clear(payload)
		if found != 1 {
			t.Fatal("fresh input dropped/duplicated authenticated known failure frame")
		}
	}
	reservations, observed := next.OperationReadback(t)
	if len(reservations) != 1 || len(observed) != 1 || reservations[0].Operation.Kind != domain.AttemptOperationModelV1 {
		t.Fatal("new explicit Run replayed prior MCP rather than one new MODEL")
	}
}
