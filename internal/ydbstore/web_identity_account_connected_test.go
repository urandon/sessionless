package ydbstore_test

import (
	"encoding/json"
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
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const accountConnectedBaseV2 = "https://account.example.com"
const accountConnectedOpCookieV2 = "__Host-sessionless-identity-operation"

type accountConnectedV2 struct {
	raw     *ydbstore.IdentityAccountConnectedFixtureV2
	handler *webbff.Handler
	cookies map[string]*http.Cookie
}

func newAccountConnectedV2(t *testing.T, provider domain.IdentityProvider, prelinked bool) *accountConnectedV2 {
	t.Helper()
	raw := ydbstore.NewIdentityAccountConnectedFixtureV2(t, provider, prelinked)
	handler, err := webbff.New(webbff.Config{Store: raw.Store, IDs: idgen.New(), Clock: raw.Clock, BaseURL: accountConnectedBaseV2, IdentityAccounts: raw.Service, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), LoginProviders: []webbff.LoginProviderConfig{{Provider: domain.IdentityProviderYandex, OAuthProvider: raw.Yandex, OAuthClientID: "account-yandex"}, {Provider: domain.IdentityProviderTelegram, OIDCProvider: raw.Telegram, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: telegramoidc.DefaultIssuer, Audience: "100000", AllowedAlgorithms: []string{"RS256"}, MaxClockSkew: domain.IdentityProofClockSkewV2}}}})
	if err != nil {
		t.Fatal(err)
	}
	return &accountConnectedV2{raw: raw, handler: handler, cookies: map[string]*http.Cookie{webcontract.SessionCookieName: {Name: webcontract.SessionCookieName, Value: "rows-session"}, webcontract.CSRFCookieName: {Name: webcontract.CSRFCookieName, Value: "rows-csrf"}}}
}

func (f *accountConnectedV2) request(t *testing.T, method, path, body string, strict bool) *httptest.ResponseRecorder {
	t.Helper()
	f.raw.Advance(time.Second)
	var input io.Reader
	if body != "" {
		input = strings.NewReader(body)
	}
	r := httptest.NewRequest(method, accountConnectedBaseV2+path, input)
	if method == http.MethodPost {
		r.Header.Set("Origin", accountConnectedBaseV2)
		r.Header.Set("Content-Type", "application/json")
		if cookie := f.cookies[webcontract.CSRFCookieName]; cookie != nil {
			r.Header.Set(webcontract.CSRFHeaderName, cookie.Value)
		}
	}
	for name, cookie := range f.cookies {
		if strict || name != webcontract.CSRFCookieName {
			r.AddCookie(cookie)
		}
	}
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, r)
	for _, cookie := range w.Result().Cookies() {
		if cookie.MaxAge < 0 {
			delete(f.cookies, cookie.Name)
		} else {
			f.cookies[cookie.Name] = cookie
		}
	}
	return w
}
func accountDecodeV2(t *testing.T, w *httptest.ResponseRecorder, status int, out any) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("account HTTP status=%d want=%d body=%s", w.Code, status, w.Body.String())
	}
	if out != nil && json.Unmarshal(w.Body.Bytes(), out) != nil {
		t.Fatal("invalid safe account response")
	}
}
func (f *accountConnectedV2) continueProof(t *testing.T, id string, revision domain.AuthRevisionV2, code string) {
	t.Helper()
	w := f.request(t, http.MethodPost, "/api/web/v1/account/identity-operations/"+id+"/continue", `{"expected_revision":"`+revision.String()+`"}`, true)
	var continuation webcontract.IdentityOperationContinuation
	accountDecodeV2(t, w, 200, &continuation)
	writes, children := f.raw.Snapshot().Writes, f.raw.Snapshot().Children
	var repeated webcontract.IdentityOperationContinuation
	accountDecodeV2(t, f.request(t, http.MethodPost, "/api/web/v1/account/identity-operations/"+id+"/continue", `{"expected_revision":"`+revision.String()+`"}`, true), 200, &repeated)
	if repeated.AuthorizationURL != continuation.AuthorizationURL || f.raw.Snapshot().Writes != writes || !reflect.DeepEqual(children, f.raw.Snapshot().Children) {
		t.Fatal("continuation retry replaced its original child/URL/deadline")
	}
	u, err := url.Parse(continuation.AuthorizationURL)
	if err != nil {
		t.Fatal(err)
	}
	provider := domain.IdentityProviderYandex
	if u.Host == "oauth.telegram.org" {
		provider = domain.IdentityProviderTelegram
	}
	f.raw.Authorization(provider, u.Query())
	callback, _ := url.Parse(u.Query().Get("redirect_uri"))
	// No Strict CSRF cookie/header survives this simulated top-level navigation.
	w = f.request(t, http.MethodGet, callback.Path+"?"+url.Values{"state": {u.Query().Get("state")}, "code": {code}}.Encode(), "", false)
	accountDecodeV2(t, w, 303, nil)
	if !strings.HasPrefix(w.Header().Get("Location"), "/account/login-methods?operation_id="+id) {
		t.Error("callback did not clear code/state to fixed own settings")
	}
}

func (f *accountConnectedV2) ordinaryLogin(t *testing.T, provider domain.IdentityProvider, code string) {
	t.Helper()
	f.raw.Advance(time.Second)
	r := httptest.NewRequest(http.MethodPost, accountConnectedBaseV2+"/auth/login/"+string(provider)+"/start", strings.NewReader("return_to=%2F"))
	r.Header.Set("Origin", accountConnectedBaseV2)
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, r)
	accountDecodeV2(t, w, 303, nil)
	u, err := url.Parse(w.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	if u.Query().Has("max_age") || u.Query().Has("force_confirm") {
		t.Error("ordinary LOGIN acquired sensitive proof policy")
	}
	f.raw.Authorization(provider, u.Query())
	for _, cookie := range w.Result().Cookies() {
		if cookie.Name == webbff.LoginBindingCookieName {
			f.cookies[cookie.Name] = cookie
		}
	}
	callback, _ := url.Parse(u.Query().Get("redirect_uri"))
	w = f.request(t, http.MethodGet, callback.Path+"?"+url.Values{"state": {u.Query().Get("state")}, "code": {code}}.Encode(), "", false)
	accountDecodeV2(t, w, 303, nil)
	if w.Header().Get("Location") != "/" {
		t.Error("ordinary login did not preserve local return")
	}
	current := f.cookies[webcontract.SessionCookieName]
	if current == nil {
		t.Fatal("ordinary login did not create cookie")
	}
	session, err := domain.DecodeWebSessionV2([]byte(f.raw.Snapshot().Sessions[string(domain.DigestSecret(current.Value))]))
	if err != nil || session.Session.UserID != "rows-user" || session.Session.AuthenticatedSubject.Provider != provider || session.IdentityRevision != 8 {
		t.Fatal("linked ordinary login changed canonical owner/revision", err)
	}
}

func TestIdentityAccountV2ConnectedBFFLinkBothDirections(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(provider), func(t *testing.T) {
			f := newAccountConnectedV2(t, provider, false)
			var methods webcontract.AccountLoginMethods
			accountDecodeV2(t, f.request(t, "GET", "/api/web/v1/account/login-methods", "", true), 200, &methods)
			target := domain.IdentityProviderTelegram
			if provider == target {
				target = domain.IdentityProviderYandex
			}
			var start webcontract.IdentityOperationView
			accountDecodeV2(t, f.request(t, "POST", "/api/web/v1/account/identity-operations", `{"kind":"link","target_provider":"`+string(target)+`","expected_revision":"7"}`, true), 200, &start)
			if f.cookies[accountConnectedOpCookieV2] == nil || f.raw.Snapshot().Tokens != 0 {
				t.Fatal("start omitted binding or exchanged without explicit continue")
			}
			f.continueProof(t, start.OperationID, 7, "source-code")
			if len(f.raw.Snapshot().Forward) != 1 {
				t.Fatal("SOURCE callback silently linked target")
			}
			f.continueProof(t, start.OperationID, 7, "target-code")
			if len(f.raw.Snapshot().Forward) != 1 || f.raw.Snapshot().Tokens != 2 {
				t.Fatal("TARGET callback linked before local confirm or retried exchange")
			}
			var confirmed webcontract.IdentityOperationView
			accountDecodeV2(t, f.request(t, "POST", "/api/web/v1/account/identity-operations/"+start.OperationID+"/confirm", `{"expected_revision":"7"}`, true), 200, &confirmed)
			if confirmed.Outcome != domain.IdentityOutcomeLinkedV2 || confirmed.AfterRevision == nil || *confirmed.AfterRevision != 8 || f.cookies[webcontract.SessionCookieName].Value == "rows-session" || f.cookies[accountConnectedOpCookieV2] != nil {
				t.Fatal("actual confirmation did not rotate/revoke/prune exactly")
			}
			if len(f.raw.Snapshot().Forward) != 2 || !reflect.DeepEqual(f.raw.OriginalMembers, f.raw.Snapshot().Members) {
				t.Fatal("link changed membership or omitted canonical method")
			}
			for _, raw := range f.raw.Snapshot().Forward {
				identity, err := domain.DecodeExternalIdentityV2([]byte(raw))
				if err != nil || identity.UserID != "rows-user" || identity.Revision != 8 {
					t.Error("linked method changed canonical owner/revision", err)
				}
			}
			var outcome webcontract.IdentityOperationView
			accountDecodeV2(t, f.request(t, "GET", "/api/web/v1/account/identity-operations/"+start.OperationID, "", true), 200, &outcome)
			if outcome.Outcome != confirmed.Outcome {
				t.Error("refresh lost committed receipt")
			}
			if f.raw.Snapshot().Tokens != 2 || f.raw.Snapshot().Infos != 1 || f.raw.Snapshot().JWKS != 1 || f.raw.Snapshot().Closed != 4 {
				t.Errorf("provider effects/closed bodies=%d/%d/%d/%d", f.raw.Snapshot().Tokens, f.raw.Snapshot().Infos, f.raw.Snapshot().JWKS, f.raw.Snapshot().Closed)
			}
			linked, receipt := f.raw.Snapshot(), f.raw.Snapshot().Operation
			f.ordinaryLogin(t, domain.IdentityProviderYandex, "ordinary-yandex-code")
			f.ordinaryLogin(t, domain.IdentityProviderTelegram, "ordinary-telegram-code")
			if !reflect.DeepEqual(linked.Forward, f.raw.Snapshot().Forward) || !reflect.DeepEqual(linked.Reverse, f.raw.Snapshot().Reverse) || !reflect.DeepEqual(linked.Members, f.raw.Snapshot().Members) || f.raw.Snapshot().Operation != receipt {
				t.Error("subsequent login changed identity owner/projection/membership/receipt")
			}
		})
	}
}

func TestIdentityAccountV2ConnectedBFFUnlinkUsesRemainingProof(t *testing.T) {
	f := newAccountConnectedV2(t, domain.IdentityProviderYandex, true)
	var methods webcontract.AccountLoginMethods
	accountDecodeV2(t, f.request(t, "GET", "/api/web/v1/account/login-methods", "", true), 200, &methods)
	var handle string
	for _, method := range methods.Methods {
		if method.Provider == domain.IdentityProviderYandex {
			handle = method.MethodHandle
			if method.RemainingProvider != domain.IdentityProviderTelegram {
				t.Fatal("server did not select actual remaining method")
			}
		}
	}
	var start webcontract.IdentityOperationView
	accountDecodeV2(t, f.request(t, "POST", "/api/web/v1/account/identity-operations", `{"kind":"unlink","method_handle":"`+handle+`","expected_revision":"7"}`, true), 200, &start)
	f.continueProof(t, start.OperationID, 7, "remaining-telegram-code")
	if f.raw.Snapshot().Tokens != 1 || f.raw.Snapshot().Infos != 0 || f.raw.Snapshot().JWKS != 1 {
		t.Fatal("unlink did not prove only the remaining configured method")
	}
	for _, raw := range f.raw.Snapshot().Forward {
		identity, _ := domain.DecodeExternalIdentityV2([]byte(raw))
		if identity.State != domain.ExternalIdentityActiveV2 {
			t.Error("proof callback unlinked before confirmation")
		}
	}
	var confirmed webcontract.IdentityOperationView
	accountDecodeV2(t, f.request(t, "POST", "/api/web/v1/account/identity-operations/"+start.OperationID+"/confirm", `{"expected_revision":"7"}`, true), 200, &confirmed)
	if confirmed.Outcome != domain.IdentityOutcomeUnlinkedV2 || confirmed.AfterRevision == nil || *confirmed.AfterRevision != 8 {
		t.Fatal("actual unlink outcome", confirmed)
	}
	for _, raw := range f.raw.Snapshot().Forward {
		identity, err := domain.DecodeExternalIdentityV2([]byte(raw))
		if err != nil || identity.UserID != "rows-user" || identity.Revision != 8 {
			t.Error("unlink changed immutable owner or lost revision", err)
		}
		if (identity.State == domain.ExternalIdentityUnlinkedV2) != (identity.Subject.Provider == domain.IdentityProviderYandex) {
			t.Error("wrong method tombstoned")
		}
	}
	if !reflect.DeepEqual(f.raw.OriginalMembers, f.raw.Snapshot().Members) {
		t.Error("unlink changed membership")
	}
	current, err := domain.DecodeWebSessionV2([]byte(f.raw.Snapshot().Sessions[string(domain.DigestSecret(f.cookies[webcontract.SessionCookieName].Value))]))
	if err != nil || current.Session.AuthenticatedSubject.Provider != domain.IdentityProviderTelegram {
		t.Fatal("unlink did not rotate onto proved remaining method", err)
	}
	old := f.cookies[webcontract.SessionCookieName]
	f.cookies[webcontract.SessionCookieName] = &http.Cookie{Name: webcontract.SessionCookieName, Value: "rows-session"}
	accountDecodeV2(t, f.request(t, "GET", "/api/web/v1/account/login-methods", "", true), 401, nil)
	f.cookies[webcontract.SessionCookieName] = old
}

func TestIdentityAccountV2ConnectedBFFLostConfirmReloginCanReplaceTerminalCookie(t *testing.T) {
	f := newAccountConnectedV2(t, domain.IdentityProviderYandex, false)
	var start webcontract.IdentityOperationView
	accountDecodeV2(t, f.request(t, "POST", "/api/web/v1/account/identity-operations", `{"kind":"link","target_provider":"telegram","expected_revision":"7"}`, true), 200, &start)
	oldOperationCookie := *f.cookies[accountConnectedOpCookieV2]
	f.continueProof(t, start.OperationID, 7, "recover-source")
	f.continueProof(t, start.OperationID, 7, "recover-target")
	var confirmed webcontract.IdentityOperationView
	accountDecodeV2(t, f.request(t, "POST", "/api/web/v1/account/identity-operations/"+start.OperationID+"/confirm", `{"expected_revision":"7"}`, true), 200, &confirmed)
	if confirmed.Outcome != domain.IdentityOutcomeLinkedV2 {
		t.Fatal("genuine terminal success prerequisite")
	}
	// Simulate losing the confirmation cookie response. A genuine ordinary
	// login obtains CURRENT revision 8; the old operation cookie remains in the
	// browser and must not lock the same owner out of a subsequent explicit Start.
	f.cookies[accountConnectedOpCookieV2] = &oldOperationCookie
	f.ordinaryLogin(t, domain.IdentityProviderYandex, "recover-real-relogin")
	before := f.raw.Snapshot()
	var receipt webcontract.IdentityOperationView
	accountDecodeV2(t, f.request(t, "GET", "/api/web/v1/account/identity-operations/"+start.OperationID, "", true), 200, &receipt)
	if receipt.Outcome != confirmed.Outcome || receipt.BeforeRevision == nil || confirmed.BeforeRevision == nil || *receipt.BeforeRevision != *confirmed.BeforeRevision || receipt.AfterRevision == nil || *receipt.AfterRevision != 8 {
		t.Fatal("same-user relogin did not recover exact receipt")
	}
	var methods webcontract.AccountLoginMethods
	accountDecodeV2(t, f.request(t, "GET", "/api/web/v1/account/login-methods", "", true), 200, &methods)
	var handle string
	for _, method := range methods.Methods {
		if method.Provider == domain.IdentityProviderTelegram {
			handle = method.MethodHandle
		}
	}
	if handle == "" {
		t.Fatal("actual revision-8 unlink method missing")
	}
	var next webcontract.IdentityOperationView
	accountDecodeV2(t, f.request(t, "POST", "/api/web/v1/account/identity-operations", `{"kind":"unlink","method_handle":"`+handle+`","expected_revision":"8"}`, true), 200, &next)
	after := f.raw.Snapshot()
	if next.OperationID == start.OperationID || next.BeforeRevision == nil || *next.BeforeRevision != 8 || next.Phase != domain.IdentityPhaseSourceProofV2 || f.cookies[accountConnectedOpCookieV2] == nil || f.cookies[accountConnectedOpCookieV2].Value == oldOperationCookie.Value || after.Operation != before.Operation || len(after.AdditionalOperations) != 1 {
		t.Fatal("terminal predecessor blocked/replaced receipt or failed to publish only new pending operation")
	}
	for _, raw := range after.AdditionalOperations {
		operation, err := domain.DecodeIdentityOperationV2([]byte(raw))
		if err != nil || operation.Pending == nil || operation.Pending.Scope.ExpectedRevision != 8 || operation.OperationDigest != domain.DigestSecret(next.OperationID) {
			t.Fatal("new pending was not written by actual current Start", err)
		}
	}
	if !reflect.DeepEqual(before.Forward, after.Forward) || !reflect.DeepEqual(before.Members, after.Members) || !reflect.DeepEqual(before.Sessions, after.Sessions) || after.Tokens != before.Tokens {
		t.Fatal("predecessor observation changed identity/session or exchanged a code")
	}
}
