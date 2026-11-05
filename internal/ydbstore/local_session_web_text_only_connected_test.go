package ydbstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/webapi"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/worker"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const localTextWebOrigin = "https://web.fixture.invalid"
const localTextWebOAuthClient = "owned-yandex-client"

// Yandex identity and auth persistence are controlled DATA boundaries, not a
// real Yandex adapter/user-info qualification. The actual OAuth BFF owns the
// bound one-use challenge, PKCE, cookies/CSRF and authorization, without an OIDC
// nonce or a Telegram dependency. Canonical authorization uses the real Store.
type localTextWebIdentity struct {
	subject string
	auth    *localTextWebAuth
	state   domain.SecretDigest
}

func (p *localTextWebIdentity) AuthorizationURL(_ context.Context, r ports.OAuthAuthorizationRequest) (string, error) {
	c, ok := p.auth.challenges[domain.DigestSecret(r.State)]
	if !ok || c.Validate() != nil || c.Provider != domain.IdentityProviderYandex || c.Issuer != domain.YandexOAuthIssuer || c.Audience != localTextWebOAuthClient || c.Nonce != "" || c.ConsumedAt != nil || r.RedirectURI != localTextWebOrigin+webcontract.RouteLoginCallback {
		return "", errors.New("controlled Yandex authorization binding")
	}
	digest := sha256.Sum256([]byte(c.PKCEVerifier))
	if r.CodeChallenge != base64.RawURLEncoding.EncodeToString(digest[:]) {
		return "", errors.New("controlled Yandex PKCE challenge")
	}
	p.state = c.StateDigest
	query := url.Values{"state": {r.State}, "code_challenge": {r.CodeChallenge}, "client_id": {localTextWebOAuthClient}, "redirect_uri": {r.RedirectURI}}
	return domain.YandexOAuthIssuer + "/authorize?" + query.Encode(), nil
}
func (p *localTextWebIdentity) ExchangeAndVerify(_ context.Context, r ports.OAuthTokenRequest) (domain.ExternalSubject, error) {
	c, ok := p.auth.challenges[p.state]
	if !ok || c.Validate() != nil || c.Provider != domain.IdentityProviderYandex || c.Issuer != domain.YandexOAuthIssuer || c.Audience != localTextWebOAuthClient || c.Nonce != "" || c.ConsumedAt == nil || r.Code != "owned-offline-code" || r.PKCEVerifier != c.PKCEVerifier || r.RedirectURI != localTextWebOrigin+webcontract.RouteLoginCallback {
		return domain.ExternalSubject{}, errors.New("controlled Yandex token/source binding")
	}
	return domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: p.subject}, nil
}

type localTextWebAuth struct {
	challenges map[domain.SecretDigest]domain.OIDCLoginChallenge
	sessions   map[domain.SecretDigest]domain.WebSession
	member     domain.TenantMembership
	identities map[string]domain.UserID
}

func (a *localTextWebAuth) RecordWebSecurityEvent(_ context.Context, e domain.WebSecurityAuditEvent) error {
	return e.Validate()
}
func (a *localTextWebAuth) CreateLoginChallenge(_ context.Context, c domain.OIDCLoginChallenge) error {
	if e := c.Validate(); e != nil {
		return e
	}
	if _, ok := a.challenges[c.StateDigest]; ok {
		return errors.New("duplicate challenge")
	}
	a.challenges[c.StateDigest] = c
	return nil
}
func (a *localTextWebAuth) ConsumeLoginChallenge(_ context.Context, d domain.SecretDigest, secret string, at time.Time) (domain.OIDCLoginChallenge, error) {
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
func (a *localTextWebAuth) ResolveOrCreateExternalIdentity(_ context.Context, s domain.ExternalSubject, _ domain.UserID, at time.Time) (domain.ExternalIdentity, bool, error) {
	user, ok := a.identities[s.Subject]
	if !ok || s.Provider != domain.IdentityProviderYandex {
		return domain.ExternalIdentity{}, false, domain.ErrMembershipDenied
	}
	return domain.ExternalIdentity{Subject: s, UserID: user, CreatedAt: at, UpdatedAt: at}, false, nil
}
func (a *localTextWebAuth) ListTenantMemberships(_ context.Context, u domain.UserID, limit uint64) ([]domain.TenantMembership, error) {
	if limit < 1 {
		return nil, errors.New("auth bounded membership limit")
	}
	if u != a.member.UserID {
		return nil, nil
	}
	return []domain.TenantMembership{a.member}, nil
}
func (*localTextWebAuth) Enroll(context.Context, ports.EnrollmentRequest) (domain.TenantMembership, error) {
	return domain.TenantMembership{}, errors.New("no fixture enrollment")
}
func (*localTextWebAuth) BootstrapDevelopmentMembership(context.Context, domain.DevelopmentBootstrapGrant) (domain.TenantMembership, error) {
	return domain.TenantMembership{}, errors.New("no fixture membership mint")
}
func (a *localTextWebAuth) CreateWebSession(_ context.Context, s domain.WebSession) error {
	if e := s.Validate(); e != nil {
		return e
	}
	if _, ok := a.sessions[s.SessionDigest]; ok {
		return errors.New("duplicate Web session")
	}
	a.sessions[s.SessionDigest] = s
	return nil
}
func (a *localTextWebAuth) AuthorizeWebSession(_ context.Context, d domain.SecretDigest, p domain.TenantPermission, at time.Time) (ports.WebAuthorization, error) {
	s, ok := a.sessions[d]
	if !ok {
		return ports.WebAuthorization{}, domain.ErrMembershipDenied
	}
	if e := s.Authorize(a.member, p, at); e != nil {
		return ports.WebAuthorization{}, e
	}
	return ports.WebAuthorization{Session: s, Membership: a.member}, nil
}
func (*localTextWebAuth) SwitchTenant(context.Context, domain.SecretDigest, domain.WebSession, domain.TenantID, time.Time) (ports.WebAuthorization, error) {
	return ports.WebAuthorization{}, domain.ErrMembershipDenied
}
func (a *localTextWebAuth) RevokeWebSession(_ context.Context, d domain.SecretDigest, at time.Time) error {
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

type localTextWebIDs struct{}

func (localTextWebIDs) NewID(_ context.Context, k ports.IDKind) (string, error) {
	if k != ports.IDUser {
		return "", errors.New("unexpected identity kind")
	}
	return "candidate-never-enrolled", nil
}

// Text-only normal ingress must never ask for an object capability or upload.
type localTextWebObjects struct{}

func (localTextWebObjects) PresignUpload(context.Context, ports.UploadCapabilityRequest) (ports.ObjectCapability, error) {
	return ports.ObjectCapability{}, errors.New("unexpected upload")
}
func (localTextWebObjects) StatObject(context.Context, domain.TenantID, string) (ports.ObjectMetadata, error) {
	return ports.ObjectMetadata{}, errors.New("unexpected object stat")
}
func (localTextWebObjects) PromoteObject(context.Context, ports.PromoteObjectRequest) (domain.BlobRef, error) {
	return domain.BlobRef{}, errors.New("unexpected object promotion")
}
func (localTextWebObjects) PresignDownload(context.Context, domain.TenantID, domain.BlobRef, time.Duration) (ports.ObjectCapability, error) {
	return ports.ObjectCapability{}, errors.New("unexpected object capability")
}

func newTextWebHandler(t *testing.T, seed *ydbstore.LocalTextWebSeed, blobs ports.BlobStore, auth *localTextWebAuth) http.Handler {
	t.Helper()
	sessions, e := sessionapi.New(sessionapi.Config{CursorKey: bytes.Repeat([]byte{51}, 32), IDKey: bytes.Repeat([]byte{52}, 32)}, seed.Store, blobs, localManagerClock{})
	if e != nil {
		t.Fatal(e)
	}
	ingress, e := sessioningress.New(sessioningress.Config{IDKey: bytes.Repeat([]byte{53}, 32), HarnessBinder: seed.Binder(t)}, seed.IngressStore(), blobs)
	if e != nil {
		t.Fatal(e)
	}
	api, e := webapi.New(webapi.Config{IDKey: bytes.Repeat([]byte{54}, 32), AllowedMCPServers: nil}, sessions, ingress, seed.Store, seed.Resources(t), localTextWebObjects{}, localManagerClock{})
	if e != nil {
		t.Fatal(e)
	}
	h, e := webbff.New(webbff.Config{LoginProviders: []webbff.LoginProviderConfig{{Provider: domain.IdentityProviderYandex, OAuthProvider: &localTextWebIdentity{subject: "owner-subject", auth: auth}, OAuthClientID: localTextWebOAuthClient}},
		BaseURL:             localTextWebOrigin,
		ObjectStorageOrigin: "https://objects.fixture.invalid",
		Store:               auth,
		Sessions:            sessions,
		API:                 api,
		IDs:                 localTextWebIDs{},
		Clock:               localManagerClock{},
		Logger:              slog.New(slog.NewTextHandler(io.Discard, nil))})
	if e != nil {
		t.Fatal(e)
	}
	return h
}
func localTextWebCookie(t *testing.T, r *http.Response, name string) *http.Cookie {
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
func localTextWebLogin(t *testing.T, h http.Handler) (*http.Cookie, *http.Cookie) {
	t.Helper()
	start := httptest.NewRecorder()
	h.ServeHTTP(start, func() *http.Request {
		u, _ := url.Parse(localTextWebOrigin + webcontract.RouteYandexLoginStart + "?return_to=/sessions")
		body := u.Query().Encode()
		u.RawQuery = ""
		r := httptest.NewRequest(http.MethodPost, u.String(), strings.NewReader(body))
		r.Header.Set("Origin", u.Scheme+"://"+u.Host)
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return r
	}())
	if start.Code != http.StatusSeeOther {
		t.Fatal("login start", start.Code, start.Body.String())
	}
	u, e := url.Parse(start.Header().Get("Location"))
	if e != nil || u.Scheme != "https" || u.Host != "oauth.yandex.ru" || u.Path != "/authorize" || u.Query().Get("state") == "" || u.Query().Get("code_challenge") == "" || u.Query().Get("client_id") != localTextWebOAuthClient || u.Query().Get("redirect_uri") != localTextWebOrigin+webcontract.RouteLoginCallback {
		t.Fatal("controlled Yandex authorization URL", e)
	}
	r := httptest.NewRequest(http.MethodGet, localTextWebOrigin+webcontract.RouteLoginCallback+"?code=owned-offline-code&state="+url.QueryEscape(u.Query().Get("state")), nil)
	r.AddCookie(localTextWebCookie(t, start.Result(), webbff.LoginBindingCookieName))
	callback := httptest.NewRecorder()
	h.ServeHTTP(callback, r)
	if callback.Code != http.StatusSeeOther || callback.Header().Get("Location") != "/sessions" {
		t.Fatal("login callback", callback.Code, callback.Header().Get("Location"), callback.Body.String())
	}
	return localTextWebCookie(t, callback.Result(), webcontract.SessionCookieName), localTextWebCookie(t, callback.Result(), webcontract.CSRFCookieName)
}
func localTextWebRequest(h http.Handler, method, path, body string, session, csrf *http.Cookie) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, localTextWebOrigin+path, strings.NewReader(body))
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
		r.Header.Set("Origin", localTextWebOrigin)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func newTextWebAuth(tenant domain.TenantID, user domain.UserID) *localTextWebAuth {
	now := localManagerClock{}.Now()
	return &localTextWebAuth{challenges: make(map[domain.SecretDigest]domain.OIDCLoginChallenge), sessions: make(map[domain.SecretDigest]domain.WebSession), identities: map[string]domain.UserID{"owner-subject": user}, member: domain.TenantMembership{TenantID: tenant, UserID: user, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: now, UpdatedAt: now}}
}

// Immutable in-memory STORAGE only. No cleanup receipt or owner is injected;
// actual physical deletion and absence are independently observed here.
type localTextWebBlobs struct {
	*sessionCompactionBlobs
	event                          domain.SessionEvent
	deletes, absences, capsulePuts int
	opPuts                         int
	failNormalized                 bool
	afterRequestPut                func()
}

func (b *localTextWebBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	if strings.Contains(key, "/attempt-operations-v1/") {
		b.opPuts++
		if b.failNormalized && b.opPuts == 2 {
			return domain.BlobRef{}, errors.New("controlled normalized body PUT failure")
		}
	}
	if strings.Contains(key, "/terminal-custody-v1/") {
		b.capsulePuts++
	}
	ref, err := b.sessionCompactionBlobs.Put(ctx, tenant, key, r)
	if err == nil && strings.Contains(key, "/attempt-operations-v1/") && b.opPuts == 1 && b.afterRequestPut != nil {
		b.afterRequestPut()
	}
	return ref, err
}

func TestLocalSessionLocalTextUnknownModelNeverReplays(t *testing.T) {
	for _, mode := range []string{"malformed", "unsolicited_tool", "nonfinal", "partial", "normalized_put_failure"} {
		t.Run(mode, func(t *testing.T) {
			model := newLocalLoopbackPeer(t, true, mode == "partial")
			seed := ydbstore.NewLocalTextWebSeed(t, model.endpoint())
			configuration, err := seed.Source.Configuration()
			if err != nil {
				t.Fatal(err)
			}
			response := `{"model":"` + configuration.ModelID + `","choices":[{"index":0,"message":{"role":"assistant","content":"not a durable answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`
			switch mode {
			case "malformed":
				response = `{"model":"` + configuration.ModelID + `","choices":[]}`
			case "unsolicited_tool":
				response = `{"model":"` + configuration.ModelID + `","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"unsolicited","type":"function","function":{"name":"search_openai_docs","arguments":"{\"query\":\"x\"}"}}]},"finish_reason":"tool_calls"}]}`
			case "nonfinal":
				response = strings.Replace(response, `"finish_reason":"stop"`, `"finish_reason":"length"`, 1)
			}
			model.setResponse([]byte(response))
			blobs := &localTextWebBlobs{sessionCompactionBlobs: newSessionCompactionBlobs(), failNormalized: mode == "normalized_put_failure"}
			h := newTextWebHandler(t, seed, blobs, newTextWebAuth(seed.Tenant, seed.Owner))
			cookie, csrf := localTextWebLogin(t, h)
			path := "/api/web/v1/sessions/" + string(seed.Session) + "/messages"
			created := localTextWebRequest(h, http.MethodPost, path, `{"text":"bounded public negative text","idempotency_key":"local-text-negative"}`, cookie, csrf)
			if created.Code != http.StatusCreated {
				t.Fatalf("actual negative Web baseline status=%d body=%s SQL=%s", created.Code, created.Body.String(), seed.LastError())
			}
			managerSeed := seed.AdmitManager(t)
			blobs.event = managerSeed.Event
			q := &localManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "local-text-negative-delivery", Kind: queuecontract.KindDispatchRun, TenantID: seed.Tenant, SubjectID: string(managerSeed.Loaded.Run.ID), EnqueuedAt: managerSeed.Loaded.Job.CreatedAt}}}
			var retryError error
			runtime, err := worker.NewLocalRuntime(worker.LocalRuntimeConfig{Source: seed.Source, Receipt: seed.OriginalReceipt(), DB: seed.DB, Blobs: blobs, Worker: worker.Config{ScratchRoot: t.TempDir(), WorkerID: managerSeed.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, ProjectionWakePublisher: q, DeliveryWakePublisher: q, RetryObserver: func(err error) { retryError = err }}}, localManagerClock{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			t.Cleanup(cancel)
			outcome, err := runtime.RunOnce(ctx, q)
			if err != nil || outcome != worker.OutcomeRetried || retryError == nil || q.ack != 0 || q.retry != 1 || q.wake != 0 {
				t.Fatalf("unknown text outcome=%s err=%v retry=%v SQL=%s", outcome, err, retryError, seed.LastError())
			}
			wire := model.waitResult(t)
			if wire.err != nil || wire.request == nil || len(wire.body) == 0 || len(wire.extra) != 0 {
				t.Fatalf("one possibly sent request err=%v bytes=%d extra=%d", wire.err, len(wire.body), len(wire.extra))
			}
			reservation, observation, observed := managerSeed.DurableModel()
			if reservation.Validate() != nil || reservation.Operation.Kind != domain.AttemptOperationModelV1 || reservation.Operation.Sequence != 1 {
				t.Fatal("negative text lacks actual original MODEL reservation")
			}
			if observed && (observation.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || observation.ResponseRef != "" || observation.ResponseDigest != "" || observation.ResponseBytes != 0) {
				t.Fatal("unsupported/possibly-sent MODEL became successful response fiction")
			}
			if mode != "normalized_put_failure" && !observed {
				t.Fatal("actual unknown MODEL observation was not durable")
			}
			snapshot := managerSeed.TerminalSnapshot(t)
			if snapshot.Finalizations != 0 || len(snapshot.Events) != 0 || snapshot.Run.Status.Terminal() || blobs.capsulePuts != 0 || blobs.deletes != 0 || blobs.absences != 0 {
				t.Fatal("unknown text invented terminal success/erasure")
			}
			stats := managerSeed.Stats()
			puts := blobs.opPuts
			retryError = nil
			if repeated, err := runtime.RunOnce(ctx, q); err != nil || repeated != worker.OutcomeRetried || retryError == nil || q.retry != 2 || q.ack != 0 {
				t.Fatalf("original unknown redelivery outcome=%s err=%v retry=%v SQL=%s", repeated, err, retryError, seed.LastError())
			}
			if managerSeed.Stats() != stats || blobs.opPuts != puts || blobs.capsulePuts != 0 || blobs.deletes != 0 {
				t.Fatal("unknown original MODEL reentered/prepared/sent on redelivery")
			}
			if heads, audits := seed.FundingCounts(); heads != 2 || audits != 2 {
				t.Fatal("unknown redelivery renewed actual funding", heads, audits)
			}
		})
	}
}

func TestLocalSessionLocalTextNoFitAndSourceClosedDoNotSend(t *testing.T) {
	for _, mode := range []string{"no_fit", "source_closed", "cancelled_caller", "stale_fence", "cancel_requested", "revoked_current", "notice_history", "unowned_tool_history", "file_history", "image_history", "unsupported_retained_head_data"} {
		t.Run(mode, func(t *testing.T) {
			model := newLocalLoopbackPeer(t, false)
			seed := ydbstore.NewLocalTextWebSeed(t, model.endpoint(), func(m *localoperator.ManifestV1) {
				if mode == "no_fit" {
					m.Configuration.Limits.MaxInputTokens = 1
					m.Profile.Limits.MaxInputTokensPerCall = 1
					m.Profile.Limits.MaxAggregateInputTokens = 1
				}
			})
			blobs := &localTextWebBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}
			if strings.HasSuffix(mode, "_history") {
				event := domain.SessionEvent{ID: "prior-text-context", TenantID: seed.Tenant, SessionID: seed.Session, IdempotencyKey: "prior-text-context", Kind: domain.SessionEventSystemNotice}
				body := []byte(`{"schema":"sessionless.run-terminal-notice.v1","code":"failed","cancelled":false}`)
				var uploads []domain.Artifact
				if mode == "unowned_tool_history" {
					event.Kind = domain.SessionEventToolCall
					run := domain.RunID("unowned-historical-tool-run")
					event.RunID = &run
					body = []byte(`{"schema":"sessionless.tool-event.v1","call_id":"legacy-tool-call","tool_name":"legacy-tool","content":{}}`)
				}
				if mode == "file_history" || mode == "image_history" {
					event.Kind = domain.SessionEventUserMessage
					owner := seed.Owner
					event.AuthorUserID = &owner
					media := "text/plain"
					if mode == "image_history" {
						media = "image/png"
					}
					attachment, err := blobs.Put(t.Context(), seed.Tenant, domain.SessionEventObjectPrefix(seed.Tenant, seed.Session, event.ID)+"attachment-01-private.bin", strings.NewReader("bounded referenced historical DATA"))
					if err != nil {
						t.Fatal(err)
					}
					descriptor := domain.Artifact{Name: "private.bin", MediaType: media, Blob: attachment}
					body, err = json.Marshal(map[string]any{"version": 1, "text": "historical media context", "attachments": []domain.Artifact{descriptor}})
					if err != nil {
						t.Fatal(err)
					}
					upload := descriptor
					upload.Name = "attachment-01-private.bin"
					uploads = []domain.Artifact{upload}
				}
				ref, err := blobs.Put(t.Context(), seed.Tenant, domain.SessionEventObjectPrefix(seed.Tenant, seed.Session, event.ID)+"message.json", bytes.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				event.Payload = ref
				seed.SeedPriorCanonicalContext(t, event, body, uploads)
			}
			h := newTextWebHandler(t, seed, blobs, newTextWebAuth(seed.Tenant, seed.Owner))
			cookie, csrf := localTextWebLogin(t, h)
			created := localTextWebRequest(h, http.MethodPost, "/api/web/v1/sessions/"+string(seed.Session)+"/messages", `{"text":"this complete canonical public text cannot fit one input token","idempotency_key":"text-no-send"}`, cookie, csrf)
			if created.Code != http.StatusCreated {
				t.Fatalf("actual no-send ingress baseline=%d %s SQL=%s", created.Code, created.Body.String(), seed.LastError())
			}
			managerSeed := seed.AdmitManager(t)
			if mode == "unsupported_retained_head_data" {
				seed.SetUnsupportedRetainedHeadData(t)
			}
			blobs.event = managerSeed.Event
			q := &localManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "text-no-send-delivery", Kind: queuecontract.KindDispatchRun, TenantID: seed.Tenant, SubjectID: string(managerSeed.Loaded.Run.ID), EnqueuedAt: managerSeed.Loaded.Job.CreatedAt}}}
			var retryError error
			runtime, err := worker.NewLocalRuntime(worker.LocalRuntimeConfig{Source: seed.Source, Receipt: seed.OriginalReceipt(), DB: seed.DB, Blobs: blobs, Worker: worker.Config{ScratchRoot: t.TempDir(), WorkerID: managerSeed.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, ProjectionWakePublisher: q, DeliveryWakePublisher: q, RetryObserver: func(err error) { retryError = err }}}, localManagerClock{})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "source_closed" {
				if err := seed.Source.Close(); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			t.Cleanup(cancel)
			mutated := false
			currentGuard := mode == "cancelled_caller" || mode == "stale_fence" || mode == "cancel_requested" || mode == "revoked_current"
			if currentGuard {
				blobs.afterRequestPut = func() {
					mutated = true
					if mode == "cancelled_caller" {
						cancel()
					} else {
						seed.InvalidatePreparedModelCurrent(t, mode)
					}
				}
			}
			outcome, err := runtime.RunOnce(ctx, q)
			if mode != "source_closed" {
				if err != nil || outcome != worker.OutcomeRetried || retryError == nil || q.retry != 1 {
					t.Fatalf("no-fit actual outcome=%s err=%v retry=%v SQL=%s", outcome, err, retryError, seed.LastError())
				}
			} else if !errors.Is(err, worker.ErrLocalRuntime) || q.retry != 0 {
				t.Fatalf("closed original Source outcome=%s err=%v", outcome, err)
			}
			expectedPuts := 0
			if currentGuard {
				expectedPuts = 1
				if !mutated {
					t.Fatal("current denial did not reach actual prepared request PUT seam")
				}
			}
			if q.ack != 0 || q.wake != 0 || !managerSeed.ModelEffectsAbsent() || blobs.opPuts != expectedPuts || blobs.capsulePuts != 0 {
				t.Fatal("disabled/no-fit text mode created MODEL/tool/compaction or terminal effects")
			}
			if currentGuard {
				stats := managerSeed.Stats()
				retryError = nil
				repeatedContext, done := context.WithTimeout(t.Context(), 30*time.Second)
				repeated, repeatErr := runtime.RunOnce(repeatedContext, q)
				done()
				if repeatErr != nil || repeated != worker.OutcomeRetried || retryError == nil || q.retry != 2 || q.ack != 0 || managerSeed.Stats() != stats || blobs.opPuts != expectedPuts || blobs.capsulePuts != 0 {
					t.Fatalf("denied original text reentered on redelivery: outcome=%s err=%v retry=%v SQL=%s", repeated, repeatErr, retryError, seed.LastError())
				}
			}
			// Close/join the owned listening peer before asserting absence; no select
			// default or scheduler timing is used as proof of zero future I/O.
			model.mu.Lock()
			model.closed = true
			conn := model.conn
			model.mu.Unlock()
			_ = model.listener.Close()
			if conn != nil {
				_ = conn.Close()
			}
			timer := time.NewTimer(10 * time.Second)
			defer timer.Stop()
			select {
			case <-model.done:
			case <-timer.C:
				select {
				case <-model.done:
				default:
					t.Fatal("owned no-send peer did not join")
				}
			}
			wire := model.waitResult(t)
			if !errors.Is(wire.err, net.ErrClosed) || wire.request != nil || len(wire.body) != 0 {
				t.Fatalf("no-send mode reached HTTP peer err=%v bytes=%d", wire.err, len(wire.body))
			}
			if managerSeed.TerminalSnapshot(t).Finalizations != 0 {
				t.Fatal("no-fit/closed Source invented terminal answer")
			}
		})
	}
}
func (b *localTextWebBlobs) Delete(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil || b.event.RunID == nil || !strings.HasPrefix(ref.Key, domain.SessionRunObjectPrefix(b.event.TenantID, b.event.SessionID, *b.event.RunID)+"attempt-operations-v1/") {
		return errors.New("text delete outside original namespace")
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("text delete unbounded")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := string(tenant) + "\x00" + ref.Key
	if _, ok := b.objects[key]; !ok {
		return errors.New("text delete missing original object")
	}
	delete(b.objects, key)
	b.deletes++
	return nil
}
func (b *localTextWebBlobs) RequireBlobAbsent(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil {
		return errors.New("text absence target")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.objects[string(tenant)+"\x00"+ref.Key]; ok {
		return errors.New("text object still present")
	}
	b.absences++
	return nil
}

func TestLocalSessionLocalTextNormalWebQueuedRuntimeFinal(t *testing.T) {
	model := newLocalLoopbackPeer(t, true)
	seed := ydbstore.NewLocalTextWebSeed(t, model.endpoint())
	configuration, err := seed.Source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	model.setResponse([]byte(`{"model":"` + configuration.ModelID + `","choices":[{"index":0,"message":{"role":"assistant","content":"owned text-only result"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	if !seed.Empty() {
		t.Fatal("normal text Web setup seeded request/funding/invocation")
	}
	blobs := &localTextWebBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}
	h := newTextWebHandler(t, seed, blobs, newTextWebAuth(seed.Tenant, seed.Owner))
	cookie, csrf := localTextWebLogin(t, h)
	path := "/api/web/v1/sessions/" + string(seed.Session) + "/messages"
	body := `{"text":"public authenticated text-only Web input","idempotency_key":"original-local-text-message"}`
	if w := localTextWebRequest(h, http.MethodPost, path, body, nil, nil); w.Code != http.StatusUnauthorized || !seed.Empty() {
		t.Fatal("unauthenticated text ingress changed state", w.Code, w.Body.String())
	}
	if w := localTextWebRequest(h, http.MethodPost, path, body, cookie, nil); w.Code != http.StatusForbidden || !seed.Empty() {
		t.Fatal("text ingress without CSRF changed state", w.Code, w.Body.String())
	}
	for _, actor := range []struct {
		tenant domain.TenantID
		user   domain.UserID
	}{{seed.Tenant, "another-owner"}, {"another-tenant", seed.Owner}} {
		foreign := newTextWebHandler(t, seed, blobs, newTextWebAuth(actor.tenant, actor.user))
		c, s := localTextWebLogin(t, foreign)
		w := localTextWebRequest(foreign, http.MethodPost, path, body, c, s)
		if w.Code != http.StatusNotFound || !seed.Empty() {
			t.Fatal("foreign text actor reached original source", w.Code, w.Body.String())
		}
	}
	created := localTextWebRequest(h, http.MethodPost, path, body, cookie, csrf)
	if created.Code != http.StatusCreated {
		t.Fatalf("actual text Web ingress status=%d body=%s SQL=%s", created.Code, created.Body.String(), seed.LastError())
	}
	var response webcontract.CreateMessageResponse
	if json.Unmarshal(created.Body.Bytes(), &response) != nil {
		t.Fatal("text Web response")
	}
	if heads, audits := seed.FundingCounts(); heads != 2 || audits != 2 {
		t.Fatal("normal binder did not create exactly two actual heads", heads, audits)
	}
	managerSeed := seed.AdmitManager(t)
	blobs.event = managerSeed.Event
	if stats := managerSeed.Stats(); stats.Parents != 0 || stats.Burns != 0 || stats.Allocations != 0 || stats.LeasePresent {
		t.Fatal("queued setup fabricated actual invocation", stats)
	}
	q := &localManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "normal-local-text-delivery", Kind: queuecontract.KindDispatchRun, TenantID: seed.Tenant, SubjectID: string(managerSeed.Loaded.Run.ID), EnqueuedAt: managerSeed.Loaded.Job.CreatedAt}}}
	var retryError error
	runtime, err := worker.NewLocalRuntime(worker.LocalRuntimeConfig{Source: seed.Source, Receipt: seed.OriginalReceipt(), DB: seed.DB, Blobs: blobs, Worker: worker.Config{ScratchRoot: t.TempDir(), WorkerID: managerSeed.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, ProjectionWakePublisher: q, DeliveryWakePublisher: q, RetryObserver: func(err error) { retryError = err }}}, localManagerClock{})
	if err != nil {
		t.Fatalf("normal no-auth runtime: %v", err)
	}
	q.onAck = func() error {
		if managerSeed.TerminalSnapshot(t).Finalizations != 1 || !managerSeed.StorageReleased() || q.wake != q.ack+1 || blobs.deletes != 2 || blobs.absences != 2 {
			return errors.New("text ACK before original finalization/cleanup/release/wake")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	outcome, err := runtime.RunOnce(ctx, q)
	if err != nil || retryError != nil || outcome != worker.OutcomeCompleted || q.ack != 1 || q.retry != 0 || q.dead != 0 {
		t.Fatalf("actual normal Web/runtime outcome=%s err=%v retry=%v trace=%s SQL=%s", outcome, err, retryError, managerSeed.Trace(), seed.LastError())
	}
	wire := model.waitResult(t)
	if wire.err != nil || len(wire.extra) != 0 {
		t.Fatalf("one actual model request err=%v extra=%d", wire.err, len(wire.extra))
	}
	if wire.request == nil || wire.request.Method != http.MethodPost || wire.request.RequestURI != "/v1/chat/completions" || wire.request.Header.Get("Authorization") != "" {
		t.Fatal("normal text runtime changed fixed no-auth MODEL wire")
	}
	var request struct {
		Tools          []json.RawMessage `json:"tools"`
		ResponseFormat json.RawMessage   `json:"response_format"`
	}
	if json.Unmarshal(wire.body, &request) != nil || len(request.Tools) != 0 || len(request.ResponseFormat) != 0 {
		t.Fatal("text wire advertised tools/schema response")
	}
	reservation, observation, ok := managerSeed.DurableModel()
	if !ok || reservation.Operation.Kind != domain.AttemptOperationModelV1 || observation.Outcome != domain.AttemptOperationCompletedV1 {
		t.Fatal("text result was not durably observed")
	}
	got := managerSeed.TerminalSnapshot(t)
	if got.Run.Status != domain.RunSucceeded || got.Attempt.Status != domain.AttemptSucceeded || got.Reservation.Status != domain.ReservationCommitted || got.Finalizations != 1 || len(got.Events) != 1 || blobs.capsulePuts != 1 {
		t.Fatal("text original canonical finalizer did not complete exactly once")
	}
	principals, leases, runs, attempts := managerSeed.ManagerLifecycle()
	if principals < 1 || leases != 1 || runs != 1 || attempts != 1 {
		t.Fatal("normal text Manager skipped actual claim/start", principals, leases, runs, attempts)
	}
	historyPath := "/api/web/v1/sessions/" + string(seed.Session) + "/events?limit=10"
	history := localTextWebRequest(h, http.MethodGet, historyPath, "", cookie, nil)
	if history.Code != http.StatusOK || !bytes.Contains(history.Body.Bytes(), []byte("owned text-only result")) {
		t.Fatalf("canonical text Web readback status=%d body=%s SQL=%s", history.Code, history.Body.String(), seed.LastError())
	}
	stats := managerSeed.Stats()
	duplicate := localTextWebRequest(h, http.MethodPost, path, body, cookie, csrf)
	var replay webcontract.CreateMessageResponse
	if duplicate.Code != http.StatusOK || json.Unmarshal(duplicate.Body.Bytes(), &replay) != nil || replay.Created || replay.RunID != response.RunID || replay.EventID != response.EventID {
		t.Fatalf("normal text idempotent ingress status=%d body=%s SQL=%s", duplicate.Code, duplicate.Body.String(), seed.LastError())
	}
	if managerSeed.Stats() != stats || !reflect.DeepEqual(got, managerSeed.TerminalSnapshot(t)) {
		t.Fatal("text duplicate POST mutated original execution")
	}
	if conflict := localTextWebRequest(h, http.MethodPost, path, `{"text":"changed text","idempotency_key":"original-local-text-message"}`, cookie, csrf); conflict.Code != http.StatusConflict {
		t.Fatal("text same-key mutation replaced original", conflict.Code)
	}
	// The real runtime creates another fresh session/Manager for redelivery;
	// canonical terminal readback must ACK without another physical claimant.
	if duplicateOutcome, err := runtime.RunOnce(ctx, q); err != nil || duplicateOutcome != worker.OutcomeDuplicate || q.ack != 2 || q.retry != 0 || q.wake != 2 {
		t.Fatalf("normal text terminal redelivery outcome=%s err=%v retry=%v SQL=%s", duplicateOutcome, err, retryError, seed.LastError())
	}
	if managerSeed.Stats() != stats || blobs.deletes != 2 || blobs.absences != 2 || blobs.capsulePuts != 1 {
		t.Fatal("terminal redelivery repeated text effects/cleanup")
	}
	if logout := localTextWebRequest(h, http.MethodPost, webcontract.RouteLogout, "", cookie, csrf); logout.Code != http.StatusNoContent {
		t.Fatal("text logout", logout.Code)
	}
	if stale := localTextWebRequest(h, http.MethodGet, historyPath, "", cookie, nil); stale.Code != http.StatusUnauthorized {
		t.Fatal("revoked browser session read text answer", stale.Code)
	}
	relogin, recsrf := localTextWebLogin(t, h)
	if relogin.Value == cookie.Value || recsrf.Value == csrf.Value {
		t.Fatal("text re-login did not rotate browser secrets")
	}
	refreshed := localTextWebRequest(h, http.MethodGet, historyPath, "", relogin, nil)
	var before, after map[string]json.RawMessage
	if refreshed.Code != http.StatusOK || json.Unmarshal(history.Body.Bytes(), &before) != nil || json.Unmarshal(refreshed.Body.Bytes(), &after) != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("text refresh/re-login changed durable canonical answer", refreshed.Code)
	}
	if managerSeed.Stats() != stats || !reflect.DeepEqual(got, managerSeed.TerminalSnapshot(t)) {
		t.Fatal("browser readback repeated text runtime effects")
	}
	if heads, audits := seed.FundingCounts(); heads != 2 || audits != 2 {
		t.Fatal("duplicate/re-login renewed original text funding", heads, audits)
	}
	if current, err := seed.Source.Configuration(); err != nil || !reflect.DeepEqual(current, configuration) {
		t.Fatal("per-delivery runtime closed/changed borrowed installation", err)
	}
}

func TestLocalSessionLocalTextTwoProductiveMessagesPreserveCanonicalHistory(t *testing.T) {
	var mu sync.Mutex
	var wires [][]byte
	var peerError error
	modelID := ""
	peer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if len(wires) >= 2 || r.Method != http.MethodPost || r.URL.RequestURI() != "/v1/chat/completions" || r.Header.Get("Authorization") != "" {
			peerError = errors.New("unexpected/extra text model request")
			http.Error(w, "denied", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
		if err != nil || len(body) > 1<<20 {
			peerError = errors.New("bounded text request")
			http.Error(w, "denied", http.StatusBadRequest)
			return
		}
		var request struct {
			Model          string            `json:"model"`
			Tools          []json.RawMessage `json:"tools"`
			ResponseFormat json.RawMessage   `json:"response_format"`
		}
		if json.Unmarshal(body, &request) != nil || request.Model != modelID || len(request.Tools) != 0 || len(request.ResponseFormat) != 0 {
			peerError = errors.New("text request catalog/model mismatch")
			http.Error(w, "denied", http.StatusBadRequest)
			return
		}
		wires = append(wires, body)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"model": modelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": fmt.Sprintf("canonical text answer %d", len(wires))}, "finish_reason": "stop"}}, "usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}}); err != nil {
			peerError = err
		}
	}))
	peer.Config.ReadHeaderTimeout, peer.Config.ReadTimeout, peer.Config.WriteTimeout = 5*time.Second, 10*time.Second, 10*time.Second
	peer.Config.MaxHeaderBytes = 16 << 10
	t.Cleanup(peer.Close)
	peer.Start()
	seed := ydbstore.NewLocalTextWebSeed(t, peer.URL+"/v1")
	config, err := seed.Source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	modelID = config.ModelID
	mu.Unlock()
	blobs := &localTextWebBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}
	h := newTextWebHandler(t, seed, blobs, newTextWebAuth(seed.Tenant, seed.Owner))
	cookie, csrf := localTextWebLogin(t, h)
	path := "/api/web/v1/sessions/" + string(seed.Session) + "/messages"
	var previousRun domain.RunID
	for i := 1; i <= 2; i++ {
		body := fmt.Sprintf(`{"text":"canonical text question %d","idempotency_key":"productive-text-%d"}`, i, i)
		created := localTextWebRequest(h, http.MethodPost, path, body, cookie, csrf)
		if created.Code != http.StatusCreated {
			t.Fatalf("message%d normal ingress=%d %s SQL=%s", i, created.Code, created.Body.String(), seed.LastError())
		}
		m := seed.AdmitManager(t)
		if m.Loaded.Run.ID == previousRun {
			t.Fatal("productive message reused prior Run")
		}
		previousRun = m.Loaded.Run.ID
		original, err := domain.DecodeAttemptExecutionSourceV1([]byte(m.Loaded.Job.OwnedExecutionSourceRecord))
		if err != nil || len(original.Spending) != 2 {
			t.Fatal("productive original two-head package", err)
		}
		blobs.event = m.Event
		q := &localManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: domain.MessageID(fmt.Sprintf("productive-text-%d", i)), Kind: queuecontract.KindDispatchRun, TenantID: seed.Tenant, SubjectID: string(m.Loaded.Run.ID), EnqueuedAt: m.Loaded.Job.CreatedAt}}}
		var retryError error
		runtime, err := worker.NewLocalRuntime(worker.LocalRuntimeConfig{Source: seed.Source, Receipt: seed.OriginalReceipt(), DB: seed.DB, Blobs: blobs, Worker: worker.Config{ScratchRoot: t.TempDir(), WorkerID: m.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, ProjectionWakePublisher: q, DeliveryWakePublisher: q, RetryObserver: func(e error) { retryError = e }}}, localManagerClock{})
		if err != nil {
			t.Fatal(err)
		}
		q.onAck = func() error {
			if !m.StorageReleased() || m.TerminalSnapshot(t).Finalizations != 1 || blobs.deletes != i*2 || blobs.absences != i*2 {
				return errors.New("productive text ACK before exact cleanup/release")
			}
			return nil
		}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		outcome, err := runtime.RunOnce(ctx, q)
		cancel()
		if err != nil || retryError != nil || outcome != worker.OutcomeCompleted || q.ack != 1 || q.retry != 0 {
			t.Fatalf("message%d runtime=%s err=%v retry=%v SQL=%s trace=%s", i, outcome, err, retryError, seed.LastError(), m.Trace())
		}
		if heads, audits := seed.FundingCounts(); heads != i*2 || audits != i*2 {
			t.Fatal("productive message invented/renewed funding", heads, audits)
		}
		if i == 1 {
			seed.BeginNextAfterSuccess(t, m)
		}
	}
	peer.Close()
	mu.Lock()
	defer mu.Unlock()
	if peerError != nil || len(wires) != 2 {
		t.Fatal("two bounded MODEL sends", len(wires), peerError)
	}
	for _, text := range []string{"canonical text question 1", "canonical text answer 1", "canonical text question 2"} {
		if !bytes.Contains(wires[1], []byte(text)) {
			t.Fatalf("second actual MODEL omitted canonical history %q", text)
		}
	}
	if seed.PriorHistoryReads() == 0 {
		t.Fatal("second MODEL bypassed immutable prior terminal-history reader")
	}
	history := localTextWebRequest(h, http.MethodGet, "/api/web/v1/sessions/"+string(seed.Session)+"/events?limit=10", "", cookie, nil)
	if history.Code != http.StatusOK || !bytes.Contains(history.Body.Bytes(), []byte("canonical text answer 1")) || !bytes.Contains(history.Body.Bytes(), []byte("canonical text answer 2")) {
		t.Fatalf("canonical two-message readback=%d %s", history.Code, history.Body.String())
	}
}
