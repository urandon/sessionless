package webbff_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

type opsSessionStore struct {
	ports.SessionAPIStore
	failure error
}

func (store opsSessionStore) ListSessionsForUser(context.Context, ports.SessionListRequest) ([]ports.SessionRecord, error) {
	return nil, store.failure
}
func (store opsSessionStore) CreateSessionForUser(context.Context, ports.SessionCreateRequest) (domain.Session, bool, error) {
	return domain.Session{}, false, store.failure
}
func (store opsSessionStore) GetSessionForUser(context.Context, domain.TenantID, domain.UserID, domain.SessionID, bool) (ports.SessionRecord, bool, error) {
	return ports.SessionRecord{}, false, store.failure
}

func TestOpsCanonicalSessionReadWriteFailuresHaveOneOwner(t *testing.T) {
	for _, tc := range []struct {
		name, method, path string
		stage              opssignal.Stage
	}{
		{"list", http.MethodGet, webcontract.RouteSessions, opssignal.SessionRead},
		{"create", http.MethodPost, webcontract.RouteSessions, opssignal.SessionWrite},
		{"masked-get", http.MethodGet, webcontract.RouteSessions + "/session-private", opssignal.SessionRead},
	} {
		for _, denied := range []bool{false, true} {
			name := tc.name + "/unexpected"
			failure := errors.New("private-session secret-payload")
			if denied {
				name = tc.name + "/denied"
				failure = domain.ErrMembershipDenied
			}
			t.Run(name, func(t *testing.T) {
				memory, subject := yandexKnownStore()
				sink := &opsAuthSink{err: errors.New("dropped observation")}
				sessions, err := sessionapi.New(sessionapi.Config{OpsSignals: sink, CursorKey: bytes.Repeat([]byte("c"), 32), IDKey: bytes.Repeat([]byte("i"), 32)}, opsSessionStore{failure: failure}, memorySessionBlobs{}, fixedClock{now: bffTestTime})
				if err != nil {
					t.Fatal(err)
				}
				config := yandexTestConfig(memory, &yandexTestProvider{subject: subject})
				config.Sessions = sessions
				config.OpsSignals = sink
				handler, err := webbff.New(config)
				if err != nil {
					t.Fatal(err)
				}
				binding, state := startYandexLogin(t, handler)
				login := finishYandexLogin(handler, binding, state)
				sessionCookie := responseCookie(t, login.Result(), webcontract.SessionCookieName)
				csrf := responseCookie(t, login.Result(), webcontract.CSRFCookieName)
				sink.records = nil
				sink.contexts = nil
				request := httptest.NewRequest(tc.method, config.BaseURL+tc.path, strings.NewReader(`{"idempotency_key":"create-private"}`))
				request.AddCookie(sessionCookie)
				if tc.method == http.MethodPost {
					request.AddCookie(csrf)
					request.Header.Set(webcontract.CSRFHeaderName, csrf.Value)
					request.Header.Set("Origin", config.BaseURL)
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				wantStatus, wantRecords := http.StatusServiceUnavailable, 1
				if denied {
					wantStatus, wantRecords = http.StatusForbidden, 0
				}
				if tc.name == "masked-get" {
					wantStatus = http.StatusNotFound
				}
				if response.Code != wantStatus || len(sink.records) != wantRecords {
					t.Fatalf("status=%d want=%d records=%+v want count=%d", response.Code, wantStatus, sink.records, wantRecords)
				}
				if !denied && (sink.records[0].SignalID != opssignal.WebSessionFailure || sink.records[0].Stage != tc.stage || sink.records[0].ErrorClass != opssignal.Store) {
					t.Fatalf("wrong actual boundary observation=%+v", sink.records[0])
				}
			})
		}
	}
}
