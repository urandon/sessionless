package webbff_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/yandexoauth"
)

type opsAuthSink struct {
	records  []opssignal.Record
	contexts []context.Context
	err      error
	observe  func(opssignal.Record)
}

func (sink *opsAuthSink) Observe(ctx context.Context, record opssignal.Record) error {
	if sink.observe != nil {
		sink.observe(record)
	}
	sink.records = append(sink.records, record)
	sink.contexts = append(sink.contexts, ctx)
	return sink.err
}

type opsAuthStore struct {
	ports.WebAuthStore
	consumeErr, createErr, membershipErr, authorizeErr error
}

func (store *opsAuthStore) ConsumeLoginChallenge(ctx context.Context, digest domain.SecretDigest, binding string, at time.Time) (domain.OIDCLoginChallenge, error) {
	if store.consumeErr != nil {
		return domain.OIDCLoginChallenge{}, store.consumeErr
	}
	return store.WebAuthStore.ConsumeLoginChallenge(ctx, digest, binding, at)
}
func (store *opsAuthStore) CreateWebSession(ctx context.Context, session domain.WebSession) error {
	if store.createErr != nil {
		return store.createErr
	}
	return store.WebAuthStore.CreateWebSession(ctx, session)
}
func (store *opsAuthStore) ListTenantMemberships(ctx context.Context, user domain.UserID, limit uint64) ([]domain.TenantMembership, error) {
	if store.membershipErr != nil {
		return nil, store.membershipErr
	}
	return store.WebAuthStore.ListTenantMemberships(ctx, user, limit)
}
func (store *opsAuthStore) AuthorizeWebSession(ctx context.Context, digest domain.SecretDigest, permission domain.TenantPermission, at time.Time) (ports.WebAuthorization, error) {
	if store.authorizeErr != nil {
		return ports.WebAuthorization{}, store.authorizeErr
	}
	return store.WebAuthStore.AuthorizeWebSession(ctx, digest, permission, at)
}

func TestOpsLoginFailureClassificationPreservesFailClosedRedirect(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stage opssignal.Stage
		class opssignal.ErrorClass
		kind  string
		err   error
		want  int
	}{
		{"expired-challenge", opssignal.AuthChallenge, opssignal.Store, "challenge", domain.ErrLoginChallengeExpired, 0},
		{"challenge-store", opssignal.AuthChallenge, opssignal.Store, "challenge", errors.New("private-state secret-payload"), 1},
		{"provider-denial", opssignal.AuthProvider, opssignal.Provider, "provider", yandexoauth.ErrProviderResponse, 0},
		{"provider-service", opssignal.AuthProvider, opssignal.Provider, "provider", yandexoauth.ErrProviderUnavailable, 1},
		{"provider-unknown", opssignal.AuthProvider, opssignal.Provider, "provider", errors.New("secret-payload https://private.invalid"), 1},
		{"membership-store", opssignal.AuthMembership, opssignal.Store, "membership", errors.New("usr_yandex secret-payload"), 1},
		{"session-store", opssignal.AuthSession, opssignal.Store, "session", errors.New("session-cookie secret-payload"), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			memory, subject := yandexKnownStore()
			store := &opsAuthStore{WebAuthStore: memory}
			provider := &yandexTestProvider{subject: subject}
			switch tc.kind {
			case "challenge":
				store.consumeErr = tc.err
			case "provider":
				provider.err = tc.err
			case "membership":
				store.membershipErr = tc.err
			case "session":
				store.createErr = tc.err
			}
			sink := &opsAuthSink{err: errors.New("log writer failed")}
			config := yandexTestConfig(store, provider)
			config.OpsSignals = sink
			handler, err := webbff.New(config)
			if err != nil {
				t.Fatal(err)
			}
			binding, state := startYandexLogin(t, handler)
			ctx := context.WithValue(context.Background(), struct{}{}, "private-context")
			request := httptest.NewRequest(http.MethodGet, config.BaseURL+webcontract.RouteLoginCallback+"?code=secret-payload&state="+url.QueryEscape(state), nil).WithContext(ctx)
			request.AddCookie(binding)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Header().Get("Location") != "/login?auth_error=access_denied" || len(memory.sessions) != 0 {
				t.Fatalf("failure changed redirect=%q sessions=%d", response.Header().Get("Location"), len(memory.sessions))
			}
			if len(sink.records) != tc.want {
				t.Fatalf("records=%+v want=%d", sink.records, tc.want)
			}
			if tc.want == 1 {
				record := sink.records[0]
				if record.SignalID != opssignal.WebAuthFailure || record.Stage != tc.stage || record.ErrorClass != tc.class || sink.contexts[0].Value(struct{}{}) != "private-context" {
					t.Fatalf("record=%+v context value=%v", record, sink.contexts[0].Value(struct{}{}))
				}
				encoded, _ := json.Marshal(record)
				for _, secret := range []string{"secret-payload", "usr_yandex", "424242", "session-cookie", "https://", "private-state"} {
					if strings.Contains(string(encoded), secret) {
						t.Fatalf("record leaked %q: %s", secret, encoded)
					}
				}
			}
		})
	}
}

func TestOpsAuthSuccessAfterPersistenceIgnoresSinkFailure(t *testing.T) {
	memory, subject := yandexKnownStore()
	sink := &opsAuthSink{err: errors.New("short write")}
	sink.observe = func(record opssignal.Record) {
		if record.SignalID == opssignal.WebAuthSuccess && len(memory.sessions) != 1 {
			t.Errorf("auth success before durable session: sessions=%d", len(memory.sessions))
		}
	}
	config := yandexTestConfig(memory, &yandexTestProvider{subject: subject})
	config.OpsSignals = sink
	handler, err := webbff.New(config)
	if err != nil {
		t.Fatal(err)
	}
	binding, state := startYandexLogin(t, handler)
	response := finishYandexLogin(handler, binding, state)
	if response.Header().Get("Location") != "/sessions" || len(memory.sessions) != 1 || len(sink.records) != 1 || sink.records[0].SignalID != opssignal.WebAuthSuccess {
		t.Fatalf("redirect=%q durableSessions=%d records=%+v", response.Header().Get("Location"), len(memory.sessions), sink.records)
	}
	// Replaying the consumed challenge never emits a second success or creates
	// another durable session, even though the first observation was dropped.
	finishYandexLogin(handler, binding, state)
	if len(memory.sessions) != 1 || len(sink.records) != 1 {
		t.Fatalf("replay sessions=%d records=%+v", len(memory.sessions), sink.records)
	}
}

func TestOpsWebAPIAuthenticationFailureIsObservedWithoutRawRoute(t *testing.T) {
	memory, subject := yandexKnownStore()
	store := &opsAuthStore{WebAuthStore: memory, authorizeErr: errors.New("private-cookie raw-path secret-payload")}
	sink := &opsAuthSink{err: errors.New("dropped")}
	config := yandexTestConfig(store, &yandexTestProvider{subject: subject})
	config.OpsSignals = sink
	handler, err := webbff.New(config)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, config.BaseURL+webcontract.RouteMe, nil)
	request.AddCookie(&http.Cookie{Name: webcontract.SessionCookieName, Value: "private-cookie"})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || len(sink.records) != 1 || sink.records[0].SignalID != opssignal.WebAuthFailure || sink.records[0].Stage != opssignal.AuthSession {
		t.Fatalf("response=%d records=%+v", response.Code, sink.records)
	}
	store.authorizeErr = domain.ErrMembershipDenied
	sink.records = nil
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || len(sink.records) != 0 {
		t.Fatalf("denial response=%d records=%+v", response.Code, sink.records)
	}
}

type opsFailIDs struct{}

func (opsFailIDs) NewID(context.Context, ports.IDKind) (string, error) {
	return "", errors.New("secret-payload ID service")
}

type opsLimitedRandom struct{ remaining int }

func (reader *opsLimitedRandom) Read(p []byte) (int, error) {
	if reader.remaining == 0 {
		return 0, errors.New("secret-payload entropy")
	}
	n := min(len(p), reader.remaining)
	for i := 0; i < n; i++ {
		p[i] = 'a'
	}
	reader.remaining -= n
	return n, nil
}

func TestOpsLoginRandomAndIDFailuresHaveFiniteStages(t *testing.T) {
	for _, remaining := range []int{0, 16} {
		t.Run(map[int]string{0: "request-id", 16: "challenge-secret"}[remaining], func(t *testing.T) {
			memory, subject := yandexKnownStore()
			sink := &opsAuthSink{err: errors.New("drop")}
			config := yandexTestConfig(memory, &yandexTestProvider{subject: subject})
			config.OpsSignals = sink
			config.Random = &opsLimitedRandom{remaining: remaining}
			handler, err := webbff.New(config)
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, config.BaseURL+webcontract.RouteLoginStart, nil))
			if response.Code != http.StatusServiceUnavailable || len(memory.challenges) != 0 || len(sink.records) != 1 || sink.records[0].Stage != opssignal.AuthStart || sink.records[0].ErrorClass != opssignal.Unexpected {
				t.Fatalf("status=%d challenges=%d records=%+v", response.Code, len(memory.challenges), sink.records)
			}
		})
	}
	memory, subject := yandexKnownStore()
	sink := &opsAuthSink{err: errors.New("drop")}
	config := yandexTestConfig(memory, &yandexTestProvider{subject: subject})
	config.OpsSignals = sink
	config.IDs = opsFailIDs{}
	handler, err := webbff.New(config)
	if err != nil {
		t.Fatal(err)
	}
	binding, state := startYandexLogin(t, handler)
	response := finishYandexLogin(handler, binding, state)
	if response.Header().Get("Location") != "/login?auth_error=access_denied" || len(memory.sessions) != 0 || len(sink.records) != 1 || sink.records[0].Stage != opssignal.AuthIdentity || sink.records[0].ErrorClass != opssignal.Unexpected {
		t.Fatalf("redirect=%q sessions=%d records=%+v", response.Header().Get("Location"), len(memory.sessions), sink.records)
	}
}
