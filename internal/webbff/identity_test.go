package webbff_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

const accountTestOrigin = "https://web.dev.sessionless.triborg.dev"
const accountTestOperationCookie = "__Host-sessionless-identity-operation"

type identityAccountStub struct {
	validateBindings func([]domain.IdentityProviderBindingV2) error
	methods          func(context.Context, ports.IdentityAccountMethodsRequestV2) (ports.IdentityAccountMethodsViewV2, error)
	start            func(context.Context, ports.IdentityAccountStartRequestV2) (ports.IdentityAccountStartResultV2, error)
	continuation     func(context.Context, ports.IdentityAccountContinueRequestV2) (ports.IdentityAccountContinueResultV2, error)
	purpose          func(context.Context, ports.IdentityAccountCallbackPurposeRequestV2) (ports.IdentityAccountCallbackPurposeV2, error)
	callback         func(context.Context, ports.IdentityAccountCallbackRequestV2) (ports.IdentityAccountOperationViewV2, error)
	confirm          func(context.Context, ports.IdentityAccountConfirmRequestV2) (ports.IdentityAccountConfirmResultV2, error)
	outcome          func(context.Context, ports.IdentityAccountOutcomeRequestV2) (ports.IdentityAccountOperationViewV2, error)
	cancel           func(context.Context, ports.IdentityAccountCancelRequestV2) (ports.IdentityAccountOperationViewV2, error)
}

func (s *identityAccountStub) ValidateLoginBindings(bindings []domain.IdentityProviderBindingV2) error {
	if s.validateBindings != nil {
		return s.validateBindings(bindings)
	}
	return nil
}

func (s *identityAccountStub) Methods(ctx context.Context, r ports.IdentityAccountMethodsRequestV2) (ports.IdentityAccountMethodsViewV2, error) {
	if s.methods == nil {
		return ports.IdentityAccountMethodsViewV2{}, ports.IdentityAccountUnavailableV2
	}
	return s.methods(ctx, r)
}
func (s *identityAccountStub) Start(ctx context.Context, r ports.IdentityAccountStartRequestV2) (ports.IdentityAccountStartResultV2, error) {
	if s.start == nil {
		return ports.IdentityAccountStartResultV2{}, ports.IdentityAccountUnavailableV2
	}
	return s.start(ctx, r)
}
func (s *identityAccountStub) Continue(ctx context.Context, r ports.IdentityAccountContinueRequestV2) (ports.IdentityAccountContinueResultV2, error) {
	if s.continuation == nil {
		return ports.IdentityAccountContinueResultV2{}, ports.IdentityAccountUnavailableV2
	}
	return s.continuation(ctx, r)
}
func (s *identityAccountStub) CallbackPurpose(ctx context.Context, r ports.IdentityAccountCallbackPurposeRequestV2) (ports.IdentityAccountCallbackPurposeV2, error) {
	if s.purpose == nil {
		return 0, ports.IdentityAccountUnavailableV2
	}
	return s.purpose(ctx, r)
}
func (s *identityAccountStub) Callback(ctx context.Context, r ports.IdentityAccountCallbackRequestV2) (ports.IdentityAccountOperationViewV2, error) {
	if s.callback == nil {
		return ports.IdentityAccountOperationViewV2{}, ports.IdentityAccountUnavailableV2
	}
	return s.callback(ctx, r)
}
func (s *identityAccountStub) Confirm(ctx context.Context, r ports.IdentityAccountConfirmRequestV2) (ports.IdentityAccountConfirmResultV2, error) {
	if s.confirm == nil {
		return ports.IdentityAccountConfirmResultV2{}, ports.IdentityAccountUnavailableV2
	}
	return s.confirm(ctx, r)
}
func (s *identityAccountStub) Outcome(ctx context.Context, r ports.IdentityAccountOutcomeRequestV2) (ports.IdentityAccountOperationViewV2, error) {
	if s.outcome == nil {
		return ports.IdentityAccountOperationViewV2{}, ports.IdentityAccountUnavailableV2
	}
	return s.outcome(ctx, r)
}
func (s *identityAccountStub) Cancel(ctx context.Context, r ports.IdentityAccountCancelRequestV2) (ports.IdentityAccountOperationViewV2, error) {
	if s.cancel == nil {
		return ports.IdentityAccountOperationViewV2{}, ports.IdentityAccountUnavailableV2
	}
	return s.cancel(ctx, r)
}

func identityAccountHandler(t *testing.T, service ports.IdentityAccountServiceV2) (http.Handler, *memoryAuthStore) {
	t.Helper()
	store := newMemoryAuthStore()
	h, err := webbff.New(webbff.Config{BaseURL: accountTestOrigin, LoginProviders: []webbff.LoginProviderConfig{{Provider: domain.IdentityProviderTelegram, OIDCProvider: fakeProvider{subject: "424242"}, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}}}}, Store: store, IDs: fixedIDs{}, Clock: fixedClock{now: bffTestTime}, IdentityAccounts: service, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return h, store
}

func accountRequest(method, path, body string) *http.Request {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, accountTestOrigin+path, nil)
	} else {
		r = httptest.NewRequest(method, accountTestOrigin+path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	}
	r.Header.Set("Origin", accountTestOrigin)
	r.Header.Set(webcontract.CSRFHeaderName, "private-csrf")
	r.AddCookie(&http.Cookie{Name: webcontract.SessionCookieName, Value: "private-session"})
	r.AddCookie(&http.Cookie{Name: webcontract.CSRFCookieName, Value: "private-csrf"})
	return r
}

func pendingAccountView() ports.IdentityAccountOperationViewV2 {
	return ports.IdentityAccountOperationViewV2{OperationID: "opaque-operation", Kind: domain.IdentityOperationLinkV2, Phase: domain.IdentityPhaseSourceProofV2, TargetProvider: domain.IdentityProviderTelegram, ExpiresAt: bffTestTime.Add(10 * time.Minute), BeforeRevision: 1, AllowedActions: []ports.IdentityAccountActionV2{ports.IdentityAccountActionContinueV2, ports.IdentityAccountActionCancelV2}}
}

func TestIdentityAccountRoutesDefaultOffAndReadOnly(t *testing.T) {
	h, store := identityAccountHandler(t, nil)
	for _, path := range []string{webcontract.RouteAccountLoginMethods, webcontract.RouteIdentityOperations + "/opaque-operation"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, accountRequest(http.MethodGet, path, ""))
		if w.Code != 404 {
			t.Fatalf("nil service exposed route %s status=%d", path, w.Code)
		}
	}
	called := 0
	service := &identityAccountStub{methods: func(ctx context.Context, r ports.IdentityAccountMethodsRequestV2) (ports.IdentityAccountMethodsViewV2, error) {
		called++
		if r.Auth.SessionCookie != "private-session" {
			t.Fatal("session not passed to current-store owner")
		}
		return ports.IdentityAccountMethodsViewV2{Revision: 1, Methods: []ports.IdentityAccountMethodViewV2{{Provider: domain.IdentityProviderYandex, Linked: true, Current: true, Enabled: true, MethodHandle: "opaque-own-handle", AllowedActions: []ports.IdentityAccountActionV2{}}, {Provider: domain.IdentityProviderTelegram, Enabled: true, AllowedActions: []ports.IdentityAccountActionV2{ports.IdentityAccountActionLinkV2}}}}, nil
	}}
	h, store = identityAccountHandler(t, service)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, accountRequest(http.MethodGet, webcontract.RouteAccountLoginMethods, ""))
	if w.Code != 200 || called != 1 || len(store.recordedSecurityEvents()) != 0 || len(store.sessions) != 0 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("read performed generic auth/effect: status=%d calls=%d body=%s", w.Code, called, w.Body.String())
	}
	for _, private := range []string{"private-session", "private-csrf", "subject", "pkce", "nonce", "user_id"} {
		if strings.Contains(w.Body.String(), private) {
			t.Fatalf("public method view leaked %s", private)
		}
	}
	for _, request := range []*http.Request{accountRequest(http.MethodGet, webcontract.RouteAccountLoginMethods+"?unexpected=1", ""), accountRequest(http.MethodGet, webcontract.RouteAccountLoginMethods, `{}`)} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, request)
		if w.Code != 400 || called != 1 {
			t.Fatal("read accepted body/query or called service")
		}
	}
}

func TestIdentityAccountBFFConstructionChecksActualLoginBindings(t *testing.T) {
	calls := 0
	service := &identityAccountStub{validateBindings: func(bindings []domain.IdentityProviderBindingV2) error {
		calls++
		if len(bindings) != 1 || bindings[0] != (domain.IdentityProviderBindingV2{Provider: domain.IdentityProviderTelegram, Issuer: "https://oauth.telegram.org", ClientID: "100000", CallbackURI: accountTestOrigin + webcontract.RouteOIDCCallback}) {
			t.Fatal("BFF did not pair actual ordinary provider/client/issuer/callback")
		}
		return ports.IdentityAccountUnavailableV2
	}}
	_, err := webbff.New(webbff.Config{BaseURL: accountTestOrigin, LoginProviders: []webbff.LoginProviderConfig{{Provider: domain.IdentityProviderTelegram, OIDCProvider: fakeProvider{subject: "424242"}, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}}}}, Store: newMemoryAuthStore(), IDs: fixedIDs{}, Clock: fixedClock{now: bffTestTime}, IdentityAccounts: service})
	if err == nil || calls != 1 {
		t.Fatal("mismatching joined/ordinary login configuration was admitted")
	}
}

func TestIdentityAccountStartRejectsAmbiguousMutationBeforeService(t *testing.T) {
	calls := 0
	service := &identityAccountStub{start: func(ctx context.Context, r ports.IdentityAccountStartRequestV2) (ports.IdentityAccountStartResultV2, error) {
		calls++
		if r.Auth.CSRFCookie != "private-csrf" || r.Auth.PresentedCSRF != "private-csrf" || r.ExpectedRevision != 1 || r.TargetProvider != domain.IdentityProviderTelegram {
			t.Fatal("mutation authority/selector changed")
		}
		return ports.IdentityAccountStartResultV2{View: pendingAccountView(), OperationCookie: "opaque-operation.private-browser"}, nil
	}}
	h, _ := identityAccountHandler(t, service)
	valid := `{"kind":"link","target_provider":"telegram","expected_revision":"1"}`
	for _, test := range []struct {
		name, body string
		mutate     func(*http.Request)
		status     int
	}{
		{"duplicate", `{"kind":"link","kind":"unlink","target_provider":"telegram","expected_revision":"1"}`, nil, 400},
		{"unknown", `{"kind":"link","target_provider":"telegram","expected_revision":"1","user_id":"other"}`, nil, 400},
		{"case_alias", `{"Kind":"link","target_provider":"telegram","expected_revision":"1"}`, nil, 400},
		{"numeric_revision", `{"kind":"link","target_provider":"telegram","expected_revision":1}`, nil, 400},
		{"null", `{"kind":"link","target_provider":"telegram","expected_revision":null}`, nil, 400},
		{"trailing", valid + ` {}`, nil, 400},
		{"mixed", `{"kind":"link","target_provider":"telegram","method_handle":"own","expected_revision":"1"}`, nil, 400},
		{"empty_handle_is_still_present", `{"kind":"link","target_provider":"telegram","method_handle":"","expected_revision":"1"}`, nil, 400},
		{"empty_provider_is_still_present", `{"kind":"unlink","target_provider":"","method_handle":"own","expected_revision":"1"}`, nil, 400},
		{"too_large", `{"kind":"` + strings.Repeat("x", 2048) + `"}`, nil, 413},
		{"origin", valid, func(r *http.Request) { r.Header.Set("Origin", "https://other.example") }, 403},
		{"duplicate_origin", valid, func(r *http.Request) { r.Header.Add("Origin", accountTestOrigin) }, 403},
		{"csrf", valid, func(r *http.Request) { r.Header.Set(webcontract.CSRFHeaderName, "wrong") }, 403},
		{"duplicate_csrf", valid, func(r *http.Request) { r.Header.Add(webcontract.CSRFHeaderName, "private-csrf") }, 403},
		{"duplicate_session", valid, func(r *http.Request) { r.AddCookie(&http.Cookie{Name: webcontract.SessionCookieName, Value: "other"}) }, 403},
		{"query", valid, func(r *http.Request) { r.URL.RawQuery = "other=1" }, 400},
		{"wrong_media", valid, func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := accountRequest(http.MethodPost, webcontract.RouteIdentityOperations, test.body)
			if test.mutate != nil {
				test.mutate(r)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != test.status || calls != 0 || len(w.Result().Cookies()) != 0 {
				t.Fatalf("invalid request reached owner/cookie: status=%d want=%d calls=%d", w.Code, test.status, calls)
			}
		})
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, accountRequest(http.MethodPost, webcontract.RouteIdentityOperations, valid))
	if w.Code != 200 || calls != 1 {
		t.Fatalf("valid start status=%d calls=%d body=%s", w.Code, calls, w.Body.String())
	}
	cookie := responseCookie(t, w.Result(), accountTestOperationCookie)
	if !cookie.HttpOnly || !cookie.Secure || cookie.SameSite != http.SameSiteLaxMode || cookie.Path != "/" || cookie.Domain != "" || strings.Contains(w.Body.String(), "private-browser") {
		t.Fatal("browser secret escaped intended Lax HttpOnly cookie")
	}
}

func TestIdentityAccountCallbackStoredPurposeNoStrictCSRFOrLoginFallback(t *testing.T) {
	for _, purpose := range []ports.IdentityAccountCallbackPurposeV2{ports.IdentityAccountCallbackProofV2, 0, 99} {
		t.Run(string(rune('a'+purpose)), func(t *testing.T) {
			calls := 0
			service := &identityAccountStub{purpose: func(ctx context.Context, r ports.IdentityAccountCallbackPurposeRequestV2) (ports.IdentityAccountCallbackPurposeV2, error) {
				if r.CallbackPath != webcontract.RouteOIDCCallback || r.State != "private-state" {
					t.Fatal("callback binding not retained")
				}
				return purpose, nil
			}, callback: func(ctx context.Context, r ports.IdentityAccountCallbackRequestV2) (ports.IdentityAccountOperationViewV2, error) {
				calls++
				if r.Auth.SessionCookie != "private-session" || r.Auth.OperationCookie != "opaque-operation.private-browser" || r.Code != "private-code" || r.ProviderDenied {
					t.Fatal("proof callback protocol changed")
				}
				return pendingAccountView(), nil
			}}
			h, store := identityAccountHandler(t, service)
			r := httptest.NewRequest(http.MethodGet, accountTestOrigin+webcontract.RouteOIDCCallback+"?state=private-state&code=private-code", nil)
			r.AddCookie(&http.Cookie{Name: webcontract.SessionCookieName, Value: "private-session"})
			r.AddCookie(&http.Cookie{Name: accountTestOperationCookie, Value: "opaque-operation.private-browser"})
			// No Strict CSRF, Origin or ordinary login-binding cookie: top-level
			// cross-provider navigation legitimately omits these.
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			want := 503
			if purpose == ports.IdentityAccountCallbackProofV2 {
				want = 303
			}
			if w.Code != want || calls != map[bool]int{true: 1, false: 0}[purpose == ports.IdentityAccountCallbackProofV2] || len(store.challenges) != 0 || len(store.identities) != 0 || len(store.sessions) != 0 || len(w.Result().Cookies()) != 0 {
				t.Fatalf("callback fell through to login or cleared cookies status=%d calls=%d", w.Code, calls)
			}
			if want == 303 && w.Header().Get("Location") != "/account/login-methods?operation_id=opaque-operation" {
				t.Fatal("callback redirect escaped fixed settings route")
			}
		})
	}
	service := &identityAccountStub{purpose: func(context.Context, ports.IdentityAccountCallbackPurposeRequestV2) (ports.IdentityAccountCallbackPurposeV2, error) {
		t.Fatal("duplicate callback key reached lookup")
		return 0, nil
	}}
	h, _ := identityAccountHandler(t, service)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, accountTestOrigin+webcontract.RouteOIDCCallback+"?state=a&state=b&code=c", nil))
	if w.Code != 403 {
		t.Fatal("ambiguous callback accepted")
	}
}

func TestIdentityAccountCallbackOriginalDeadlineIncludesPurposeLookup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var original time.Time
		service := &identityAccountStub{purpose: func(ctx context.Context, _ ports.IdentityAccountCallbackPurposeRequestV2) (ports.IdentityAccountCallbackPurposeV2, error) {
			var ok bool
			original, ok = ctx.Deadline()
			if !ok || time.Until(original) != 15*time.Second {
				t.Fatal("missing original handler deadline")
			}
			time.Sleep(6 * time.Second)
			return ports.IdentityAccountCallbackProofV2, nil
		}, callback: func(ctx context.Context, _ ports.IdentityAccountCallbackRequestV2) (ports.IdentityAccountOperationViewV2, error) {
			deadline, _ := ctx.Deadline()
			if !deadline.Equal(original) || time.Until(deadline) != 9*time.Second {
				t.Fatal("purpose lookup renewed sensitive deadline")
			}
			<-ctx.Done()
			return pendingAccountView(), nil
		}}
		h, _ := identityAccountHandler(t, service)
		r := httptest.NewRequest(http.MethodGet, accountTestOrigin+webcontract.RouteOIDCCallback+"?state=private-state&code=private-code", nil)
		r.AddCookie(&http.Cookie{Name: webcontract.SessionCookieName, Value: "private-session"})
		r.AddCookie(&http.Cookie{Name: accountTestOperationCookie, Value: "opaque-operation.private-browser"})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 503 || w.Header().Get("Location") != "" {
			t.Fatal("late callback published success")
		}
	})
}

func TestIdentityAccountOutcomeAfterReloginAndConfirmCookieReplay(t *testing.T) {
	for _, optionalCookie := range []string{"", "malformed"} {
		t.Run("read_"+optionalCookie, func(t *testing.T) {
			service := &identityAccountStub{outcome: func(_ context.Context, r ports.IdentityAccountOutcomeRequestV2) (ports.IdentityAccountOperationViewV2, error) {
				if r.Auth.SessionCookie != "private-session" || r.OperationID != "opaque-operation" {
					t.Fatal("same-user read delegated wrong values")
				}
				return ports.IdentityAccountOperationViewV2{OperationID: r.OperationID, Outcome: domain.IdentityOutcomeUnavailableV2}, nil
			}}
			h, store := identityAccountHandler(t, service)
			r := accountRequest(http.MethodGet, webcontract.RouteIdentityOperations+"/opaque-operation", "")
			r.Header.Del("Origin")
			r.Header.Del(webcontract.CSRFHeaderName)
			if optionalCookie != "" {
				r.AddCookie(&http.Cookie{Name: accountTestOperationCookie, Value: optionalCookie})
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 410 || len(store.sessions) != 0 || strings.Contains(w.Body.String(), "revision") || strings.Contains(w.Body.String(), "completed_at") || len(w.Result().Cookies()) != 0 {
				t.Fatalf("missing outcome fabricated receipt or touched session: %d %s", w.Code, w.Body.String())
			}
		})
	}
	for _, fresh := range []bool{false, true} {
		t.Run(map[bool]string{false: "receipt_replay", true: "new_commit"}[fresh], func(t *testing.T) {
			service := &identityAccountStub{confirm: func(_ context.Context, r ports.IdentityAccountConfirmRequestV2) (ports.IdentityAccountConfirmResultV2, error) {
				if r.ExpectedRevision != 1 {
					t.Fatal("confirm lost original revision")
				}
				after := domain.AuthRevisionV2(2)
				completed := bffTestTime.Add(time.Minute)
				view := pendingAccountView()
				view.Phase = domain.IdentityPhaseTerminalV2
				view.TargetProvider = ""
				view.Outcome = domain.IdentityOutcomeLinkedV2
				view.AfterRevision = &after
				view.CompletedAt = &completed
				view.AllowedActions = nil
				result := ports.IdentityAccountConfirmResultV2{View: view}
				if fresh {
					result.Cookies = &ports.IdentityAccountSessionCookiesV2{SessionCookie: "new-private-session", CSRFCookie: "new-private-csrf"}
				}
				return result, nil
			}}
			h, _ := identityAccountHandler(t, service)
			r := accountRequest(http.MethodPost, webcontract.RouteIdentityOperations+"/opaque-operation/confirm", `{"expected_revision":"1"}`)
			r.AddCookie(&http.Cookie{Name: accountTestOperationCookie, Value: "opaque-operation.private-browser"})
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 200 {
				t.Fatalf("confirm status=%d body=%s", w.Code, w.Body.String())
			}
			sessions := 0
			for _, cookie := range w.Result().Cookies() {
				if cookie.Name == webcontract.SessionCookieName || cookie.Name == webcontract.CSRFCookieName {
					sessions++
				}
			}
			if sessions != map[bool]int{false: 0, true: 2}[fresh] || strings.Contains(w.Body.String(), "new-private") {
				t.Fatal("receipt replay reminted cookies or body leaked secrets")
			}
		})
	}
}

func TestIdentityAccountErrorsAreClosedAndContentFree(t *testing.T) {
	for _, test := range []struct {
		failure error
		status  int
		code    string
	}{{ports.IdentityAccountUnavailableV2, 503, "temporarily_unavailable"}, {ports.IdentityAccountProviderUnavailableV2, 503, "provider_unavailable"}, {ports.IdentityAccountStaleRevisionV2, 409, "stale_revision"}, {ports.IdentityAccountLastMethodV2, 409, "last_method"}, {ports.IdentityAccountProofRequiredV2, 409, "proof_required"}, {ports.IdentityAccountProofUnavailableV2, 403, "proof_unavailable"}, {ports.IdentityAccountExpiredV2, 410, "expired"}, {errors.New("private upstream token/code/subject"), 503, "temporarily_unavailable"}} {
		t.Run(test.code+"_"+test.failure.Error(), func(t *testing.T) {
			service := &identityAccountStub{methods: func(context.Context, ports.IdentityAccountMethodsRequestV2) (ports.IdentityAccountMethodsViewV2, error) {
				return ports.IdentityAccountMethodsViewV2{}, test.failure
			}}
			h, _ := identityAccountHandler(t, service)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, accountRequest(http.MethodGet, webcontract.RouteAccountLoginMethods, ""))
			if w.Code != test.status || !strings.Contains(w.Body.String(), `"code":"`+test.code+`"`) || strings.Contains(w.Body.String(), "private") {
				t.Fatalf("unsafe error response %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestIdentityAccountStoredLoginPurposeRetainsOrdinaryLogin(t *testing.T) {
	lookups := 0
	service := &identityAccountStub{purpose: func(_ context.Context, request ports.IdentityAccountCallbackPurposeRequestV2) (ports.IdentityAccountCallbackPurposeV2, error) {
		lookups++
		if request.CallbackPath != webcontract.RouteOIDCCallback || request.State == "" {
			t.Fatal("ordinary callback lost fixed stored binding")
		}
		return ports.IdentityAccountCallbackLoginV2, nil
	}, callback: func(context.Context, ports.IdentityAccountCallbackRequestV2) (ports.IdentityAccountOperationViewV2, error) {
		t.Fatal("stored LOGIN entered provider proof")
		return ports.IdentityAccountOperationViewV2{}, nil
	}}
	h, store := identityAccountHandler(t, service)
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}
	store.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: "canonical-owner", CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	store.memberships["canonical-owner"] = []domain.TenantMembership{membership("canonical-tenant", "canonical-owner", domain.TenantMembershipOwner)}
	session, csrf := performLogin(t, h, "/sessions")
	if lookups != 1 || session.Value == "" || csrf.Value == "" || len(store.identities) != 1 || len(store.sessions) != 1 {
		t.Fatal("positive stored LOGIN changed canonical account or failed ordinary login")
	}
}

func TestIdentityAccountContinueAndCancelRequireExactMutationInput(t *testing.T) {
	continued, cancelled := 0, 0
	service := &identityAccountStub{continuation: func(_ context.Context, r ports.IdentityAccountContinueRequestV2) (ports.IdentityAccountContinueResultV2, error) {
		continued++
		if r.OperationID != "opaque-operation" || r.OperationCookie != "opaque-operation.private-browser" || r.ExpectedRevision != 9007199254740993 {
			t.Fatal("continuation lost exact original binding or uint64 revision")
		}
		return ports.IdentityAccountContinueResultV2{AuthorizationURL: "https://oauth.telegram.org/auth?state=private-state&code_challenge=private-challenge"}, nil
	}, cancel: func(_ context.Context, r ports.IdentityAccountCancelRequestV2) (ports.IdentityAccountOperationViewV2, error) {
		cancelled++
		if r.OperationID != "opaque-operation" || r.ExpectedRevision != 9007199254740993 {
			t.Fatal("cancel lost exact operation/revision")
		}
		view := pendingAccountView()
		view.Phase = domain.IdentityPhaseTerminalV2
		view.TargetProvider = ""
		view.Outcome = domain.IdentityOutcomeCancelledV2
		view.AllowedActions = nil
		completed := bffTestTime.Add(time.Minute)
		view.CompletedAt = &completed
		return view, nil
	}}
	h, _ := identityAccountHandler(t, service)
	for _, suffix := range []string{"continue", "cancel"} {
		r := accountRequest(http.MethodPost, webcontract.RouteIdentityOperations+"/opaque-operation/"+suffix, `{"expected_revision":"9007199254740993"}`)
		r.AddCookie(&http.Cookie{Name: accountTestOperationCookie, Value: "opaque-operation.private-browser"})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("%s status=%d body=%s", suffix, w.Code, w.Body.String())
		}
		if suffix == "continue" && (len(w.Result().Cookies()) != 0 || strings.Contains(w.Body.String(), "operation_id") || !strings.Contains(w.Body.String(), "authorization_url")) {
			t.Fatal("continuation returned cookie/extra authority instead of only URL")
		}
	}
	if continued != 1 || cancelled != 1 {
		t.Fatal("explicit mutation count changed")
	}
}
