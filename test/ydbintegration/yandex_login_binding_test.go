//go:build ydbintegration

package ydbintegration

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/yandexoauth"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// These cases use the actual adapter, BFF and serializable YDB auth store.
// Loopback OAuth responses are fixtures, not proof of live Yandex connectivity.
func TestYandexLoginYDBClientAndProviderBindingDenials(t *testing.T) {
	for _, tc := range []struct {
		name           string
		profileClient  string
		mutate         func(*domain.OIDCLoginChallenge)
		wantExchanges  int32
		wantAudit      string
		wantAuthorized bool
	}{
		{name: "matching-control", profileClient: "test-yandex-client", wantExchanges: 1, wantAuthorized: true},
		{name: "wrong-profile-client", profileClient: "foreign-client", wantExchanges: 1, wantAudit: "provider_verification_failed"},
		{name: "wrong-persisted-client", profileClient: "test-yandex-client", mutate: func(c *domain.OIDCLoginChallenge) { c.Audience = "foreign-client" }, wantAudit: "login_provider_binding_rejected"},
		{name: "wrong-persisted-provider", profileClient: "test-yandex-client", mutate: func(c *domain.OIDCLoginChallenge) {
			c.Provider = domain.IdentityProviderTelegram
			c.Issuer = "https://oauth.telegram.org"
			c.Nonce = "fixture-telegram-nonce"
		}, wantAudit: "login_provider_binding_rejected"},
		{name: "legacy-unbound-challenge", profileClient: "test-yandex-client", mutate: func(c *domain.OIDCLoginChallenge) {
			c.Provider, c.Issuer, c.Audience = "", "", ""
			c.Nonce = "fixture-legacy-nonce"
		}, wantAudit: "login_provider_binding_rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, client := openStore(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			t.Cleanup(cancel)
			clock := onboardingDatabaseClock{t: t, ctx: ctx, db: client.DB}
			const origin = "https://web.example"
			// A unique enrolled subject makes successful authentication possible;
			// membership_missing must not accidentally satisfy a denial assertion.
			subjectDigest := sha256.Sum256([]byte(uniqueID("yandex-binding")))
			subject := domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: strconv.FormatUint(binary.BigEndian.Uint64(subjectDigest[:8])|1, 10)}
			identity, _, err := store.ResolveOrCreateExternalIdentity(ctx, subject, domain.UserID(uniqueID("binding-user")), clock.Now())
			if err != nil {
				t.Fatalf("seed enrolled identity: %v", err)
			}
			tenantID := domain.TenantID(uniqueID("binding-tenant"))
			if _, err := store.BootstrapDevelopmentMembership(ctx, domain.DevelopmentBootstrapGrant{TenantID: tenantID, UserID: identity.UserID, Role: domain.TenantMembershipOwner, Environment: domain.DevelopmentEnvironment, Operator: "yandex-binding-ci", Reason: "explicit native fixture", GrantedAt: clock.Now()}); err != nil {
				t.Fatalf("seed explicit membership: %v", err)
			}
			originalMemberships, err := store.ListTenantMemberships(ctx, identity.UserID, 10)
			if err != nil || len(originalMemberships) != 1 {
				t.Fatalf("seed membership snapshot: %+v err=%v", originalMemberships, err)
			}
			var exchanges, profiles atomic.Int32
			var expectedChallenge atomic.Value
			providerHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/token":
					exchanges.Add(1)
					clientID, secret, ok := r.BasicAuth()
					if !ok || clientID != "test-yandex-client" || secret != "fixture-secret" {
						t.Error("token request lost configured client authentication")
					}
					if err := r.ParseForm(); err != nil {
						t.Errorf("token form: %v", err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					pkce := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
					if r.Method != http.MethodPost || r.PostForm.Get("code") != "fixture-code" || r.PostForm.Get("redirect_uri") != origin+webcontract.RouteLoginCallback || base64.RawURLEncoding.EncodeToString(pkce[:]) != expectedChallenge.Load().(string) {
						t.Error("token request lost code/callback/PKCE binding")
					}
					fmt.Fprint(w, `{"access_token":"fixture-token","token_type":"bearer","expires_in":3600}`)
				case "/info":
					profiles.Add(1)
					if r.Header.Get("Authorization") != "OAuth fixture-token" || r.URL.RawQuery != "format=json" {
						t.Error("profile token must be header-only")
					}
					fmt.Fprintf(w, `{"id":%q,"client_id":%q}`, subject.Subject, tc.profileClient)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(providerHTTP.Close)
			provider, err := yandexoauth.New(yandexoauth.Config{ClientID: "test-yandex-client", ClientSecret: "fixture-secret", RedirectURI: origin + webcontract.RouteLoginCallback, AuthorizationEndpoint: providerHTTP.URL + "/authorize", TokenEndpoint: providerHTTP.URL + "/token", InfoEndpoint: providerHTTP.URL + "/info", AllowLoopbackProvider: true, HTTPClient: providerHTTP.Client()})
			if err != nil {
				t.Fatalf("actual adapter: %v", err)
			}
			h, err := webbff.New(webbff.Config{BaseURL: origin, RedirectURI: origin + webcontract.RouteLoginCallback, OAuthProvider: provider, OAuthClientID: "test-yandex-client", Store: store, IDs: idgen.New(), Clock: clock, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err != nil {
				t.Fatalf("normal BFF: %v", err)
			}
			start := httptest.NewRecorder()
			h.ServeHTTP(start, httptest.NewRequest(http.MethodGet, origin+webcontract.RouteLoginStart+"?return_to=%2Fsessions", nil).WithContext(ctx))
			if start.Code != http.StatusSeeOther {
				t.Fatalf("start status=%d want303", start.Code)
			}
			authorize, err := url.Parse(start.Header().Get("Location"))
			if err != nil {
				t.Fatalf("authorization URL: %v", err)
			}
			expectedChallenge.Store(authorize.Query().Get("code_challenge"))
			state := authorize.Query().Get("state")
			binding := yandexYDBCookie(t, start, webbff.LoginBindingCookieName)
			stateDigest := domain.DigestSecret(state)
			bucket, err := ydbpartition.BucketV1(string(stateDigest))
			if err != nil {
				t.Fatal(err)
			}
			var record string
			if err := client.DB.QueryRowContext(ctx, `SELECT record FROM oidc_login_challenges WHERE shard_bucket = $1 AND state_digest = $2`, bucket, stateDigest).Scan(&record); err != nil {
				t.Fatalf("read genuine persisted challenge: %v", err)
			}
			var challenge domain.OIDCLoginChallenge
			if err := json.Unmarshal([]byte(record), &challenge); err != nil {
				t.Fatal(err)
			}
			if challenge.Provider != domain.IdentityProviderYandex || challenge.Audience != "test-yandex-client" || challenge.Issuer != domain.YandexOAuthIssuer || challenge.ConsumedAt != nil {
				t.Fatal("start did not persist an unused selected-provider challenge")
			}
			if tc.mutate != nil {
				tc.mutate(&challenge)
				if err := challenge.Validate(); err != nil {
					t.Fatalf("negative must remain a valid other binding, not malformed data: %v", err)
				}
				payload, err := json.Marshal(challenge)
				if err != nil {
					t.Fatal(err)
				}
				// Mutate only this test-owned challenge, including its indexed nonce.
				if _, err := client.DB.ExecContext(ctx, `UPDATE oidc_login_challenges SET nonce = $1, record = CAST($2 AS JsonDocument) WHERE shard_bucket = $3 AND state_digest = $4`, challenge.Nonce, string(payload), bucket, stateDigest); err != nil {
					t.Fatalf("persist mismatched challenge: %v", err)
				}
			}
			finish := func() *httptest.ResponseRecorder {
				response := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, origin+webcontract.RouteLoginCallback+"?code=fixture-code&state="+url.QueryEscape(state), nil).WithContext(ctx)
				req.AddCookie(binding)
				h.ServeHTTP(response, req)
				return response
			}
			response := finish()
			wantLocation := "/login?auth_error=access_denied"
			var wantSessions uint64
			if tc.wantAuthorized {
				wantLocation, wantSessions = "/sessions", 1
				cookie := yandexYDBCookie(t, response, webcontract.SessionCookieName)
				if !cookie.Secure || !cookie.HttpOnly || cookie.Value == "" {
					t.Fatal("matching control missing secure authenticated session")
				}
			} else {
				for _, cookie := range response.Result().Cookies() {
					if cookie.Name == webcontract.SessionCookieName && cookie.Value != "" {
						t.Fatal("mismatch minted authenticated browser cookie")
					}
				}
				auditBucket, err := ydbpartition.BucketV1(response.Header().Get("X-Request-ID"))
				if err != nil {
					t.Fatal(err)
				}
				var auditProvider, reason string
				if err := client.DB.QueryRowContext(ctx, `SELECT provider, reason_code FROM web_security_audit_events WHERE shard_bucket = $1 AND request_id = $2`, auditBucket, response.Header().Get("X-Request-ID")).Scan(&auditProvider, &reason); err != nil {
					t.Fatalf("read rejection audit: %v", err)
				}
				if auditProvider != "yandex" || reason != tc.wantAudit {
					t.Fatalf("audit provider=%q reason=%q wantyandex/%s", auditProvider, reason, tc.wantAudit)
				}
			}
			if response.Code != http.StatusSeeOther || response.Header().Get("Location") != wantLocation || exchanges.Load() != tc.wantExchanges || profiles.Load() != tc.wantExchanges {
				t.Fatalf("callback status=%d location=%q exchanges=%d profiles=%d want303/%s/%d", response.Code, response.Header().Get("Location"), exchanges.Load(), profiles.Load(), wantLocation, tc.wantExchanges)
			}
			assertDurable := func() {
				t.Helper()
				var sessions uint64
				if err := client.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM web_sessions WHERE user_id = $1`, identity.UserID).Scan(&sessions); err != nil || sessions != wantSessions {
					t.Fatalf("durable sessions=%d want%d err=%v", sessions, wantSessions, err)
				}
				identityBucket, err := ydbpartition.BucketV1(subject.String())
				if err != nil {
					t.Fatal(err)
				}
				var userID string
				if err := client.DB.QueryRowContext(ctx, `SELECT user_id FROM external_identities WHERE shard_bucket = $1 AND provider = $2 AND subject = $3`, identityBucket, subject.Provider, subject.Subject).Scan(&userID); err != nil || userID != string(identity.UserID) {
					t.Fatalf("identity mapping=%q want%q err=%v", userID, identity.UserID, err)
				}
				memberships, err := store.ListTenantMemberships(ctx, identity.UserID, 10)
				if err != nil || !reflect.DeepEqual(memberships, originalMemberships) {
					t.Fatalf("membership changed after callback: %+v err=%v", memberships, err)
				}
				userBucket, err := ydbpartition.BucketV1(string(identity.UserID))
				if err != nil {
					t.Fatal(err)
				}
				var reverseIdentities, grants, grantAudits uint64
				if err := client.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM external_identities_by_user WHERE user_bucket = $1 AND user_id = $2 AND provider = $3 AND subject = $4`, userBucket, identity.UserID, subject.Provider, subject.Subject).Scan(&reverseIdentities); err != nil || reverseIdentities != 1 {
					t.Fatalf("reverse identity count=%d want1 err=%v", reverseIdentities, err)
				}
				if err := client.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM development_bootstrap_grants WHERE tenant_id = $1 AND user_id = $2`, tenantID, identity.UserID).Scan(&grants); err != nil || grants != 1 {
					t.Fatalf("explicit grant count=%d want1 err=%v", grants, err)
				}
				if err := client.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_events WHERE tenant_id = $1 AND actor_id = $2 AND action = $3`, tenantID, identity.UserID, "web.membership.cloud_dev_bootstrap").Scan(&grantAudits); err != nil || grantAudits != 1 {
					t.Fatalf("explicit grant audit count=%d want1 err=%v", grantAudits, err)
				}
			}
			assertDurable()
			replay := finish()
			if replay.Code != http.StatusSeeOther || replay.Header().Get("Location") != "/login?auth_error=access_denied" || exchanges.Load() != tc.wantExchanges || profiles.Load() != tc.wantExchanges {
				t.Fatalf("consumed challenge replay status=%d exchanges=%d profiles=%d", replay.Code, exchanges.Load(), profiles.Load())
			}
			assertDurable()
		})
	}
}
