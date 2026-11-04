package webbff_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

type identityLifecycleStore struct {
	*memoryAuthStore
	revokeErr, switchErr, afterSwitchReadErr error
	rotated                                  bool
}

func (s *identityLifecycleStore) RevokeWebSession(ctx context.Context, digest domain.SecretDigest, at time.Time) error {
	if s.revokeErr != nil {
		return s.revokeErr
	}
	return s.memoryAuthStore.RevokeWebSession(ctx, digest, at)
}
func (s *identityLifecycleStore) SwitchTenant(ctx context.Context, digest domain.SecretDigest, next domain.WebSession, selected domain.TenantID, at time.Time) (ports.WebAuthorization, error) {
	if s.switchErr != nil {
		return ports.WebAuthorization{}, s.switchErr
	}
	result, err := s.memoryAuthStore.SwitchTenant(ctx, digest, next, selected, at)
	if err == nil {
		s.rotated = true
	}
	return result, err
}
func (s *identityLifecycleStore) ListTenantMemberships(ctx context.Context, user domain.UserID, limit uint64) ([]domain.TenantMembership, error) {
	if s.rotated && s.afterSwitchReadErr != nil {
		return nil, s.afterSwitchReadErr
	}
	return s.memoryAuthStore.ListTenantMemberships(ctx, user, limit)
}

func identityLifecycleHandler(t *testing.T, enabled bool) (http.Handler, *identityLifecycleStore, *http.Cookie, *http.Cookie) {
	t.Helper()
	s := &identityLifecycleStore{memoryAuthStore: newMemoryAuthStore()}
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}
	s.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: "lifecycle-owner", CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	s.memberships["lifecycle-owner"] = []domain.TenantMembership{membership("tenant-one", "lifecycle-owner", domain.TenantMembershipOwner), membership("tenant-two", "lifecycle-owner", domain.TenantMembershipMember)}
	config := webbff.Config{BaseURL: accountTestOrigin, LoginProviders: []webbff.LoginProviderConfig{{Provider: domain.IdentityProviderTelegram, OIDCProvider: fakeProvider{subject: "424242"}, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}}}}, Store: s, IDs: fixedIDs{}, Clock: fixedClock{now: bffTestTime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	initial, err := webbff.New(config)
	if err != nil {
		t.Fatal(err)
	}
	session, csrf := performLogin(t, initial, "/")
	if enabled {
		config.IdentityAccounts = &identityAccountStub{purpose: func(context.Context, ports.IdentityAccountCallbackPurposeRequestV2) (ports.IdentityAccountCallbackPurposeV2, error) {
			return ports.IdentityAccountCallbackLoginV2, nil
		}, cancel: func(context.Context, ports.IdentityAccountCancelRequestV2) (ports.IdentityAccountOperationViewV2, error) {
			t.Fatal("cookie cleanup attempted an operation mutation")
			return ports.IdentityAccountOperationViewV2{}, nil
		}}
	}
	h, err := webbff.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return h, s, session, csrf
}

func TestIdentityAccountBindingClearsOnlyAfterKnownSessionInvalidation(t *testing.T) {
	for _, operation := range []string{"logout", "switch"} {
		for _, test := range []struct {
			name                                   string
			enabled, unknown, badCSRF, readFailure bool
			status                                 int
			clear                                  bool
		}{
			{name: "success", enabled: true, status: 200, clear: true},
			{name: "legacy_nil_service", status: 200},
			{name: "unknown_store_reply", enabled: true, unknown: true, status: 503},
			{name: "csrf_denied", enabled: true, badCSRF: true, status: 403},
			{name: "post_rotation_read_failed", enabled: true, readFailure: true, status: 503, clear: true},
		} {
			if operation == "logout" && test.readFailure {
				continue
			}
			t.Run(operation+"/"+test.name, func(t *testing.T) {
				h, s, session, csrf := identityLifecycleHandler(t, test.enabled)
				if test.unknown {
					if operation == "logout" {
						s.revokeErr = errors.New("synthetic unknown revoke reply")
					} else {
						s.switchErr = errors.New("synthetic unknown rotate reply")
					}
				}
				if test.readFailure {
					s.afterSwitchReadErr = errors.New("synthetic post-commit read failure")
				}
				var r *http.Request
				if operation == "logout" {
					r = httptest.NewRequest(http.MethodPost, accountTestOrigin+webcontract.RouteLogout, nil)
					r.AddCookie(session)
					r.AddCookie(csrf)
					r.Header.Set("Origin", accountTestOrigin)
					r.Header.Set(webcontract.CSRFHeaderName, csrf.Value)
				} else {
					r = tenantSwitchRequest(session, csrf, "tenant-two", csrf.Value)
				}
				r.AddCookie(&http.Cookie{Name: accountTestOperationCookie, Value: "opaque-operation.private-browser"})
				if test.badCSRF {
					r.Header.Set(webcontract.CSRFHeaderName, "wrong")
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				want := test.status
				if operation == "logout" && want == 200 {
					want = 204
				}
				if w.Code != want {
					t.Fatalf("%s status=%d want=%d body=%s", operation, w.Code, want, w.Body.String())
				}
				clears := 0
				for _, cookie := range w.Result().Cookies() {
					if cookie.Name == accountTestOperationCookie {
						clears++
						if cookie.Value != "" || cookie.MaxAge != -1 || !cookie.Secure || !cookie.HttpOnly || cookie.SameSite != http.SameSiteLaxMode || cookie.Path != "/" || cookie.Domain != "" {
							t.Fatal("operation clear changed binding attributes")
						}
					}
				}
				if (clears == 1) != test.clear || clears > 1 {
					t.Fatalf("operation binding clear=%d want=%t", clears, test.clear)
				}
				if test.clear {
					old := s.sessions[domain.DigestSecret(session.Value)]
					if old.RevokedAt == nil {
						t.Fatal("binding cleared without known original-session invalidation")
					}
				}
			})
		}
	}
}

func TestIdentityAccountNewOrdinaryLoginDoesNotDiscardLiveOperationBinding(t *testing.T) {
	h, s, old, _ := identityLifecycleHandler(t, true)
	start := loginStartRequest(accountTestOrigin + webcontract.RouteTelegramLoginStart + "?return_to=" + url.QueryEscape("/"))
	start.AddCookie(&http.Cookie{Name: accountTestOperationCookie, Value: "opaque-operation.private-browser"})
	startResponse := httptest.NewRecorder()
	h.ServeHTTP(startResponse, start)
	if startResponse.Code != http.StatusSeeOther {
		t.Fatalf("start status=%d", startResponse.Code)
	}
	location, err := url.Parse(startResponse.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	callback := httptest.NewRequest(http.MethodGet, accountTestOrigin+webcontract.RouteOIDCCallback+"?code=fixture&state="+url.QueryEscape(location.Query().Get("state")), nil)
	callback.AddCookie(responseCookie(t, startResponse.Result(), webbff.LoginBindingCookieName))
	callback.AddCookie(&http.Cookie{Name: accountTestOperationCookie, Value: "opaque-operation.private-browser"})
	callbackResponse := httptest.NewRecorder()
	h.ServeHTTP(callbackResponse, callback)
	if callbackResponse.Code != http.StatusSeeOther || callbackResponse.Header().Get("Location") != "/" {
		t.Fatalf("callback status=%d", callbackResponse.Code)
	}
	for _, response := range []*httptest.ResponseRecorder{startResponse, callbackResponse} {
		for _, cookie := range response.Result().Cookies() {
			if cookie.Name == accountTestOperationCookie {
				t.Fatal("ordinary LOGIN changed an existing operation binding")
			}
		}
	}
	responseCookie(t, callbackResponse.Result(), webcontract.SessionCookieName)
	responseCookie(t, callbackResponse.Result(), webcontract.CSRFCookieName)
	if original := s.sessions[domain.DigestSecret(old.Value)]; original.RevokedAt != nil {
		t.Fatal("ordinary login unexpectedly revoked another original session")
	}
}
