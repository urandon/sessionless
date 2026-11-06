package ydbstore_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const nativeAccountOpCookieV2 = "__Host-sessionless-identity-operation"

// Direct normal HTTP handlers + genuine YDB + real concrete adapters over a
// controlled RoundTripper. This models explicit cookie inputs; it is NOT an
// actual browser, SameSite engine, Playwright run or live provider experiment.
type nativeAccountHTTPV2 struct {
	raw     *ydbstore.IdentityAccountNativeFixtureV2
	handler *webbff.Handler
	cookies map[string]*http.Cookie
	code    int
}

func newNativeAccountHTTPV2(t *testing.T, initial domain.IdentityProvider, conflict bool) *nativeAccountHTTPV2 {
	t.Helper()
	raw := ydbstore.NewIdentityAccountNativeFixtureV2(t, initial, conflict)
	handler, err := webbff.New(webbff.Config{Store: raw.Store, IdentityAccounts: raw.Service, IDs: idgen.New(), Clock: raw.Clock, BaseURL: ydbstore.IdentityAccountNativeOriginV2, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), LoginProviders: []webbff.LoginProviderConfig{{Provider: domain.IdentityProviderYandex, OAuthProvider: raw.Yandex, OAuthClientID: "native-account-yandex"}, {Provider: domain.IdentityProviderTelegram, OIDCProvider: raw.Telegram, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: telegramoidc.DefaultIssuer, Audience: "100000", AllowedAlgorithms: []string{"RS256"}, MaxClockSkew: domain.IdentityProofClockSkewV2}}}})
	if err != nil {
		t.Fatal("normal native BFF construction", err)
	}
	return &nativeAccountHTTPV2{raw: raw, handler: handler, cookies: make(map[string]*http.Cookie)}
}
func nativeAccountJSONV2(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
func nativeAccountDecodeV2(t *testing.T, response *httptest.ResponseRecorder, status int, out any) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("native account HTTP status=%d want=%d safeBody=%s", response.Code, status, response.Body.String())
	}
	if out != nil && json.Unmarshal(response.Body.Bytes(), out) != nil {
		t.Fatal("invalid native BFF response")
	}
}
func (f *nativeAccountHTTPV2) call(t *testing.T, method, path, body string, form, callback bool) *httptest.ResponseRecorder {
	t.Helper()
	var input io.Reader
	if body != "" {
		input = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, ydbstore.IdentityAccountNativeOriginV2+path, input).WithContext(f.raw.Context())
	if method == http.MethodPost {
		request.Header.Set("Origin", ydbstore.IdentityAccountNativeOriginV2)
		if form {
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		} else {
			request.Header.Set("Content-Type", "application/json")
			if csrf := f.cookies[webcontract.CSRFCookieName]; csrf != nil {
				request.Header.Set(webcontract.CSRFHeaderName, csrf.Value)
			}
		}
	}
	for name, cookie := range f.cookies {
		if !callback || name != webcontract.CSRFCookieName {
			request.AddCookie(cookie)
		}
	}
	response := httptest.NewRecorder()
	f.handler.ServeHTTP(response, request)
	for _, cookie := range response.Result().Cookies() {
		if cookie.MaxAge < 0 {
			delete(f.cookies, cookie.Name)
		} else {
			f.cookies[cookie.Name] = cookie
		}
	}
	return response
}
func (f *nativeAccountHTTPV2) ordinary(t *testing.T, provider domain.IdentityProvider, success bool) webcontract.Identity {
	t.Helper()
	start := f.call(t, http.MethodPost, "/auth/login/"+string(provider)+"/start", "return_to=%2F", true, false)
	nativeAccountDecodeV2(t, start, 303, nil)
	u, err := url.Parse(start.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	if u.Query().Has("force_confirm") || u.Query().Has("max_age") {
		t.Fatal("ordinary native login acquired sensitive-proof policy")
	}
	f.code++
	code := "native-code-" + strconv.Itoa(f.code)
	f.raw.ObserveAuthorization(code, u.String())
	callback, err := url.Parse(u.Query().Get("redirect_uri"))
	if err != nil {
		t.Fatal(err)
	}
	result := f.call(t, http.MethodGet, callback.Path+"?"+url.Values{"state": {u.Query().Get("state")}, "code": {code}}.Encode(), "", false, true)
	nativeAccountDecodeV2(t, result, 303, nil)
	if !success {
		if !strings.HasPrefix(result.Header().Get("Location"), "/login?") {
			t.Fatal("tombstone ordinary login did not fail safely")
		}
		return webcontract.Identity{}
	}
	if result.Header().Get("Location") != "/" {
		t.Fatalf("ordinary native login failed: %s", result.Header().Get("Location"))
	}
	var me webcontract.Identity
	nativeAccountDecodeV2(t, f.call(t, http.MethodGet, webcontract.RouteMe, "", false, false), 200, &me)
	if me.Provider != string(provider) || me.UserID == "" || len(me.Tenants) == 0 {
		t.Fatal("native ordinary login lacks current canonical membership")
	}
	return me
}
func (f *nativeAccountHTTPV2) logout(t *testing.T) {
	t.Helper()
	response := f.call(t, http.MethodPost, webcontract.RouteLogout, "", false, false)
	nativeAccountDecodeV2(t, response, 204, nil)
	if f.cookies[webcontract.SessionCookieName] != nil || f.cookies[webcontract.CSRFCookieName] != nil || f.cookies[nativeAccountOpCookieV2] != nil {
		t.Fatal("actual native logout did not clear all three cookies")
	}
}
func (f *nativeAccountHTTPV2) methods(t *testing.T) webcontract.AccountLoginMethods {
	t.Helper()
	var result webcontract.AccountLoginMethods
	nativeAccountDecodeV2(t, f.call(t, http.MethodGet, webcontract.RouteAccountLoginMethods, "", false, false), 200, &result)
	if result.Revision.Validate() != nil || len(result.Methods) != 2 {
		t.Fatal("native owned methods invalid")
	}
	return result
}
func (f *nativeAccountHTTPV2) start(t *testing.T, request webcontract.IdentityOperationStartRequest) webcontract.IdentityOperationView {
	t.Helper()
	var view webcontract.IdentityOperationView
	nativeAccountDecodeV2(t, f.call(t, http.MethodPost, webcontract.RouteIdentityOperations, nativeAccountJSONV2(t, request), false, false), 200, &view)
	wantPhase := domain.IdentityPhaseSourceProofV2
	if request.Kind == domain.IdentityOperationLinkV2 {
		wantPhase = domain.IdentityPhaseTargetProofV2
	}
	if view.Phase != wantPhase || view.BeforeRevision == nil || *view.BeforeRevision != request.ExpectedRevision || f.cookies[nativeAccountOpCookieV2] == nil {
		t.Fatal("native Start did not create exact original pending operation")
	}
	return view
}
func nativeAccountRevisionV2(t *testing.T, revision domain.AuthRevisionV2) string {
	return nativeAccountJSONV2(t, webcontract.IdentityOperationRevisionRequest{ExpectedRevision: revision})
}
func (f *nativeAccountHTTPV2) proof(t *testing.T, id string, revision domain.AuthRevisionV2, repeat bool) {
	t.Helper()
	result := f.proofCallback(t, id, revision, repeat)
	nativeAccountDecodeV2(t, result, 303, nil)
	if result.Header().Get("Location") != "/account/login-methods?"+(url.Values{"operation_id": {id}}).Encode() {
		t.Fatal("native proof callback did not clear code/state to exact settings")
	}
}
func (f *nativeAccountHTTPV2) proofCallback(t *testing.T, id string, revision domain.AuthRevisionV2, repeat bool) *httptest.ResponseRecorder {
	t.Helper()
	path := webcontract.RouteIdentityOperations + "/" + url.PathEscape(id) + "/continue"
	var continuation webcontract.IdentityOperationContinuation
	nativeAccountDecodeV2(t, f.call(t, http.MethodPost, path, nativeAccountRevisionV2(t, revision), false, false), 200, &continuation)
	if repeat {
		var same webcontract.IdentityOperationContinuation
		nativeAccountDecodeV2(t, f.call(t, http.MethodPost, path, nativeAccountRevisionV2(t, revision), false, false), 200, &same)
		if continuation != same {
			t.Fatal("native Continue replaced original child/state/PKCE")
		}
	}
	u, err := url.Parse(continuation.AuthorizationURL)
	if err != nil {
		t.Fatal(err)
	}
	if u.Host == "oauth.telegram.org" {
		if u.Query().Get("max_age") != "0" || u.Query().Get("nonce") == "" {
			t.Fatal("native Telegram proof lost freshness policy")
		}
	} else if u.Host != "oauth.yandex.ru" || u.Query().Get("force_confirm") != "yes" {
		t.Fatal("native Yandex proof lost fresh-authorization policy")
	}
	f.code++
	code := "native-code-" + strconv.Itoa(f.code)
	f.raw.ObserveAuthorization(code, u.String())
	callback, err := url.Parse(u.Query().Get("redirect_uri"))
	if err != nil {
		t.Fatal(err)
	}
	return f.call(t, http.MethodGet, callback.Path+"?"+url.Values{"state": {u.Query().Get("state")}, "code": {code}}.Encode(), "", false, true)
}
func (f *nativeAccountHTTPV2) confirm(t *testing.T, id string, revision domain.AuthRevisionV2, status int) webcontract.IdentityOperationView {
	t.Helper()
	response := f.call(t, http.MethodPost, webcontract.RouteIdentityOperations+"/"+url.PathEscape(id)+"/confirm", nativeAccountRevisionV2(t, revision), false, false)
	var view webcontract.IdentityOperationView
	if status == 200 {
		nativeAccountDecodeV2(t, response, status, &view)
	} else {
		nativeAccountDecodeV2(t, response, status, nil)
		if len(response.Result().Cookies()) != 0 {
			t.Fatal("failed/unknown native confirm published cookie")
		}
	}
	return view
}
func cloneNativeAccountCookiesV2(values map[string]*http.Cookie) map[string]*http.Cookie {
	out := make(map[string]*http.Cookie, len(values))
	for key, value := range values {
		copy := *value
		out[key] = &copy
	}
	return out
}
func (f *nativeAccountHTTPV2) denyOld(t *testing.T, old map[string]*http.Cookie) {
	t.Helper()
	current := f.cookies
	f.cookies = cloneNativeAccountCookiesV2(old)
	defer func() { f.cookies = current }()
	nativeAccountDecodeV2(t, f.call(t, http.MethodGet, webcontract.RouteMe, "", false, false), 401, nil)
}
func sameNativeAccountResourcesV2(t *testing.T, before, after ydbstore.IdentityAccountNativeSnapshotV2) {
	t.Helper()
	if !reflect.DeepEqual(before.Members, after.Members) || !reflect.DeepEqual(before.Session, after.Session) || !reflect.DeepEqual(before.Participant, after.Participant) || before.Material != after.Material {
		t.Fatal("identity operation changed membership/canonical session/owner/genesis")
	}
}

func TestWebIdentityV2YDBAccountLinkReloginAndUnlink(t *testing.T) {
	for _, initial := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(initial), func(t *testing.T) {
			f := newNativeAccountHTTPV2(t, initial, false)
			target := domain.IdentityProviderTelegram
			if initial == target {
				target = domain.IdentityProviderYandex
			}
			if me := f.ordinary(t, initial, true); me.UserID != f.raw.Owner {
				t.Fatal("original native login changed pre-provisioned owner")
			}
			oldFirst := cloneNativeAccountCookiesV2(f.cookies)
			if me := f.ordinary(t, initial, true); me.UserID != f.raw.Owner {
				t.Fatal("second genuine native login owner")
			}
			nativeAccountDecodeV2(t, f.call(t, http.MethodPost, webcontract.RouteActiveTenant, nativeAccountJSONV2(t, map[string]domain.TenantID{"tenant_id": f.raw.Tenants[1]}), false, false), 200, nil)
			oldSecond := cloneNativeAccountCookiesV2(f.cookies)
			f.cookies = cloneNativeAccountCookiesV2(oldFirst)
			baseline := f.raw.Snapshot()
			methods := f.methods(t)
			if methods.Revision != 1 {
				t.Fatal("initial native link revision")
			}
			if baseline.Sessions[domain.DigestSecret(oldFirst[webcontract.SessionCookieName].Value)].Session.ActiveTenantID != f.raw.Tenants[0] || baseline.Sessions[domain.DigestSecret(oldSecond[webcontract.SessionCookieName].Value)].Session.ActiveTenantID != f.raw.Tenants[1] {
				t.Fatal("native old-cookie baseline must actually cover BOTH tenant scopes")
			}
			start := f.start(t, webcontract.IdentityOperationStartRequest{Kind: domain.IdentityOperationLinkV2, TargetProvider: target, ExpectedRevision: 1})
			f.proof(t, start.OperationID, 1, true)
			if len(f.raw.Snapshot().Active[f.raw.Owner]) != 1 {
				t.Fatal("TARGET callback linked before local confirm")
			}
			confirmed := f.confirm(t, start.OperationID, 1, 200)
			if confirmed.Outcome != domain.IdentityOutcomeLinkedV2 || confirmed.AfterRevision == nil || *confirmed.AfterRevision != 2 || confirmed.TargetProvider != "" || f.cookies[nativeAccountOpCookieV2] != nil || f.cookies[webcontract.SessionCookieName].Value == oldFirst[webcontract.SessionCookieName].Value {
				t.Fatal("native link did not atomically rotate/prune")
			}
			linked := f.raw.Snapshot()
			sameNativeAccountResourcesV2(t, baseline, linked)
			if len(linked.Active[f.raw.Owner]) != 2 || linked.Audits["web.identity.linked"] != 1 || !linked.Scrubbed[domain.DigestSecret(start.OperationID)] {
				t.Fatal("native link canonical/audit/physical receipt correspondence")
			}
			for _, identity := range linked.Active[f.raw.Owner] {
				if identity.UserID != f.raw.Owner || identity.Revision != 2 {
					t.Fatal("native shared link revision/owner mismatch")
				}
			}
			for digest, child := range linked.Children {
				if child.Purpose == domain.IdentityPurposeProviderProofV2 && !linked.Scrubbed[digest] {
					t.Fatal("native proof child retains scalar/JSON secrets")
				}
			}
			f.denyOld(t, oldFirst)
			f.denyOld(t, oldSecond)
			for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
				f.logout(t)
				if me := f.ordinary(t, provider, true); me.UserID != f.raw.Owner {
					t.Fatal("EACH linked provider must resolve SAME canonical owner")
				}
				after := f.raw.Snapshot()
				sameNativeAccountResourcesV2(t, baseline, after)
				if !reflect.DeepEqual(linked.Identities, after.Identities) || !reflect.DeepEqual(linked.Active, after.Active) {
					t.Fatal("ordinary linked login mutated identity ownership/revision")
				}
			}
			beforeUnlink := f.raw.Snapshot()
			oldUnlink := cloneNativeAccountCookiesV2(f.cookies)
			methods = f.methods(t)
			var handle string
			for _, method := range methods.Methods {
				if method.Provider == target {
					handle = method.MethodHandle
					if method.RemainingProvider != initial {
						t.Fatal("native remaining method not server-selected")
					}
				}
			}
			if handle == "" {
				t.Fatal("native unlink handle missing")
			}
			remove := f.start(t, webcontract.IdentityOperationStartRequest{Kind: domain.IdentityOperationUnlinkV2, MethodHandle: handle, ExpectedRevision: 2})
			f.proof(t, remove.OperationID, 2, false)
			if f.raw.Snapshot().Identities[target].State != domain.ExternalIdentityActiveV2 {
				t.Fatal("native source proof removed method before confirmation")
			}
			removed := f.confirm(t, remove.OperationID, 2, 200)
			if removed.Outcome != domain.IdentityOutcomeUnlinkedV2 || removed.AfterRevision == nil || *removed.AfterRevision != 3 {
				t.Fatal("native unlink outcome/revision")
			}
			afterUnlink := f.raw.Snapshot()
			sameNativeAccountResourcesV2(t, beforeUnlink, afterUnlink)
			if len(afterUnlink.Active[f.raw.Owner]) != 1 || afterUnlink.Identities[target].State != domain.ExternalIdentityUnlinkedV2 || afterUnlink.Identities[target].UserID != f.raw.Owner || afterUnlink.Audits["web.identity.unlinked"] != 1 {
				t.Fatal("native unlink tombstone/owner/active set/audit")
			}
			current := afterUnlink.Sessions[domain.DigestSecret(f.cookies[webcontract.SessionCookieName].Value)]
			if current.Session.AuthenticatedSubject.Provider != initial || current.IdentityRevision != 3 {
				t.Fatal("native unlink did not rotate onto proved survivor")
			}
			f.denyOld(t, oldUnlink)
			methods = f.methods(t)
			var survivor webcontract.AccountLoginMethod
			for _, method := range methods.Methods {
				if method.Provider == initial {
					survivor = method
				}
			}
			if len(survivor.AllowedActions) != 0 {
				t.Fatal("native last method advertised removal")
			}
			response := f.call(t, http.MethodPost, webcontract.RouteIdentityOperations, nativeAccountJSONV2(t, webcontract.IdentityOperationStartRequest{Kind: domain.IdentityOperationUnlinkV2, MethodHandle: survivor.MethodHandle, ExpectedRevision: 3}), false, false)
			nativeAccountDecodeV2(t, response, 409, nil)
			beforeDenied := f.raw.Snapshot()
			f.ordinary(t, target, false)
			afterDenied := f.raw.Snapshot()
			if !reflect.DeepEqual(beforeDenied.Identities, afterDenied.Identities) || !reflect.DeepEqual(beforeDenied.Active, afterDenied.Active) {
				t.Fatal("ordinary native login reactivated/remapped tombstone")
			}
			if afterDenied.Closed != afterDenied.Tokens+afterDenied.Infos+afterDenied.JWKS {
				t.Fatal("native provider response bodies not all joined/closed")
			}
		})
	}
}

func TestWebIdentityV2YDBAccountConflictIsolatesOwners(t *testing.T) {
	f := newNativeAccountHTTPV2(t, domain.IdentityProviderYandex, true)
	if me := f.ordinary(t, domain.IdentityProviderTelegram, true); me.UserID != f.raw.OtherOwner {
		t.Fatal("coherent target-owner positive baseline")
	}
	f.logout(t)
	if me := f.ordinary(t, domain.IdentityProviderYandex, true); me.UserID != f.raw.Owner {
		t.Fatal("coherent original-owner positive baseline")
	}
	baseline := f.raw.Snapshot()
	start := f.start(t, webcontract.IdentityOperationStartRequest{Kind: domain.IdentityOperationLinkV2, TargetProvider: domain.IdentityProviderTelegram, ExpectedRevision: 1})
	cookie := f.cookies[webcontract.SessionCookieName].Value
	// The original proof recorder checks target ownership before local Confirm.
	// This is that exact native conflict boundary, not an arbitrary later error.
	nativeAccountDecodeV2(t, f.proofCallback(t, start.OperationID, 1, false), 409, nil)
	after := f.raw.Snapshot()
	sameNativeAccountResourcesV2(t, baseline, after)
	if !reflect.DeepEqual(baseline.Identities, after.Identities) || !reflect.DeepEqual(baseline.Active, after.Active) || after.Audits["web.identity.linked"] != 0 || f.cookies[webcontract.SessionCookieName].Value != cookie {
		t.Fatal("native conflict merged/remapped/rotated another owner")
	}
}

func TestWebIdentityV2YDBAccountUnknownConfirmationRecoversExactReceipt(t *testing.T) {
	f := newNativeAccountHTTPV2(t, domain.IdentityProviderYandex, false)
	f.ordinary(t, domain.IdentityProviderYandex, true)
	start := f.start(t, webcontract.IdentityOperationStartRequest{Kind: domain.IdentityOperationLinkV2, TargetProvider: domain.IdentityProviderTelegram, ExpectedRevision: 1})
	f.proof(t, start.OperationID, 1, false)
	before := f.raw.Snapshot()
	old := cloneNativeAccountCookiesV2(f.cookies)
	f.raw.LoseNextConfirmationReply(start.OperationID)
	f.confirm(t, start.OperationID, 1, 503)
	committed := f.raw.Snapshot()
	operation := committed.Operations[domain.DigestSecret(start.OperationID)]
	if committed.LostReplies != 1 || committed.WriteCommits != before.WriteCommits+1 || operation.Receipt == nil || operation.Receipt.Outcome != domain.IdentityOutcomeLinkedV2 || operation.Receipt.AfterRevision == nil || *operation.Receipt.AfterRevision != 2 || committed.Audits["web.identity.linked"] != 1 {
		t.Fatal("unknown response lacks exactly ONE actual committed native confirmation")
	}
	f.denyOld(t, old)
	if me := f.ordinary(t, domain.IdentityProviderTelegram, true); me.UserID != f.raw.Owner {
		t.Fatal("actual current relogin after lost replacement")
	}
	var receipt webcontract.IdentityOperationView
	nativeAccountDecodeV2(t, f.call(t, http.MethodGet, webcontract.RouteIdentityOperations+"/"+url.PathEscape(start.OperationID), "", false, false), 200, &receipt)
	if receipt.Outcome != domain.IdentityOutcomeLinkedV2 || receipt.AfterRevision == nil || *receipt.AfterRevision != 2 || receipt.BeforeRevision == nil || *receipt.BeforeRevision != 1 {
		t.Fatal("native receipt GET did not recover exact result")
	}
	// Same result replay has current same-user authorization, but cannot recreate
	// lost cookies or exchange either consumed code again.
	tokens := f.raw.Snapshot().Tokens
	response := f.call(t, http.MethodPost, webcontract.RouteIdentityOperations+"/"+url.PathEscape(start.OperationID)+"/confirm", nativeAccountRevisionV2(t, 1), false, false)
	nativeAccountDecodeV2(t, response, 200, &receipt)
	for _, cookie := range response.Result().Cookies() {
		if cookie.Name == webcontract.SessionCookieName || cookie.Name == webcontract.CSRFCookieName {
			t.Fatal("receipt-only native replay minted replacement cookies")
		}
	}
	after := f.raw.Snapshot()
	if after.Tokens != tokens || after.LostReplies != 1 || after.Audits["web.identity.linked"] != 1 || !reflect.DeepEqual(committed.Operations[domain.DigestSecret(start.OperationID)], after.Operations[domain.DigestSecret(start.OperationID)]) {
		t.Fatal("native receipt replay repeated exchange/commit/result")
	}
	sameNativeAccountResourcesV2(t, before, after)
}

func TestWebIdentityV2YDBAccountCurrentnessDenialsBeforeConfirmation(t *testing.T) {
	for _, mode := range []string{"expected_revision", "membership"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeAccountHTTPV2(t, domain.IdentityProviderYandex, false)
			f.ordinary(t, domain.IdentityProviderYandex, true)
			start := f.start(t, webcontract.IdentityOperationStartRequest{Kind: domain.IdentityOperationLinkV2, TargetProvider: domain.IdentityProviderTelegram, ExpectedRevision: 1})
			f.proof(t, start.OperationID, 1, false)
			revision, status := domain.AuthRevisionV2(2), 409
			if mode == "membership" {
				f.raw.RevokeMembership(f.raw.Tenants[0])
				revision, status = 1, 401
			}
			before := f.raw.Snapshot()
			f.confirm(t, start.OperationID, revision, status)
			after := f.raw.Snapshot()
			if !reflect.DeepEqual(before.Identities, after.Identities) || !reflect.DeepEqual(before.Active, after.Active) || !reflect.DeepEqual(before.Members, after.Members) || !reflect.DeepEqual(before.Sessions, after.Sessions) || !reflect.DeepEqual(before.Operations, after.Operations) || after.Audits["web.identity.linked"] != 0 {
				t.Fatal("native denied currentness partially mutated identity/session/operation")
			}
		})
	}
}
