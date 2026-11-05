package webbff_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

type citationHistoryStore struct {
	memorySessionAPIStore
	events []domain.SessionEvent
	reads  int
}

func (s *citationHistoryStore) ListSessionHistoryForUser(_ context.Context, tenant domain.TenantID, user domain.UserID, session domain.SessionID, _ uint64, _ uint64) ([]domain.SessionEvent, error) {
	s.reads++
	if tenant != "ten_alpha" || user != "usr_cited" || session != "session-cited" {
		return nil, domain.ErrMembershipDenied
	}
	return append([]domain.SessionEvent{}, s.events...), nil
}

type citationHistoryBlobs struct {
	memorySessionBlobs
	values map[string][]byte
	opens  int
}

func (b *citationHistoryBlobs) Open(_ context.Context, _ domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	b.opens++
	return io.NopCloser(bytes.NewReader(b.values[ref.Key])), nil
}

func TestCitedHistoryHTTPRefreshAndSafeWholePageFailure(t *testing.T) {
	auth := newMemoryAuthStore()
	user := domain.UserID("usr_cited")
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "cited-owner"}
	auth.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: user, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	auth.memberships[user] = []domain.TenantMembership{membership("ten_alpha", user, domain.TenantMembershipOwner)}
	v := domain.CitedAssistantMessageV2{Schema: domain.CitedAssistantMessageSchemaV2, Summary: "private exact answer", ArtifactManifestID: "manifest-cited", Citations: domain.AnswerCitationManifestV1{Schema: domain.AnswerCitationManifestSchemaV1, RunID: "run-cited", AttemptID: "attempt-cited", Sources: []domain.AnswerCitationSourceV1{}}}
	payload, err := domain.EncodeCitedAssistantMessageV2(v, domain.CitedAnswerBytesV1, 256<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(payload)
	store := &citationHistoryStore{}
	blobs := &citationHistoryBlobs{values: map[string][]byte{}}
	event := func(id domain.SessionEventID, sequence uint64, body []byte) domain.SessionEvent {
		h := sha256.Sum256(body)
		run := v.Citations.RunID
		e := domain.SessionEvent{ID: id, TenantID: "ten_alpha", SessionID: "session-cited", Sequence: sequence, Kind: domain.SessionEventAssistantMessage, RunID: &run, IdempotencyKey: domain.IdempotencyKey(id), Payload: domain.BlobRef{TenantID: "ten_alpha", Key: domain.SessionEventObjectPrefix("ten_alpha", "session-cited", id) + "payload.json", Size: int64(len(body)), SHA256: hex.EncodeToString(h[:])}, CreatedAt: bffTestTime}
		if e.Validate() != nil {
			t.Fatal("invalid canonical test event")
		}
		blobs.values[e.Payload.Key] = bytes.Clone(body)
		return e
	}
	store.events = []domain.SessionEvent{event("evt-cited", 1, payload)}
	sessions, err := sessionapi.New(sessionapi.Config{CursorKey: bytes.Repeat([]byte("c"), 32), IDKey: bytes.Repeat([]byte("i"), 32)}, store, blobs, fixedClock{now: bffTestTime})
	if err != nil {
		t.Fatal(err)
	}
	h, err := webbff.New(webbff.Config{BaseURL: "https://web.dev.sessionless.triborg.dev", LoginProviders: []webbff.LoginProviderConfig{{Provider: domain.IdentityProviderTelegram, OIDCProvider: fakeProvider{subject: subject.Subject}, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}}}}, Store: auth, Sessions: sessions, IDs: fixedIDs{}, Clock: fixedClock{now: bffTestTime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	cookie, _ := performLogin(t, h, "/sessions")
	read := func(etag string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev/api/web/v1/sessions/session-cited/events", nil)
		r.AddCookie(cookie)
		if etag != "" {
			r.Header.Set("If-None-Match", etag)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	first := read("")
	if first.Code != http.StatusOK {
		t.Fatalf("valid cited history status=%d", first.Code)
	}
	var page webcontract.Page[webcontract.SessionEvent]
	if json.Unmarshal(first.Body.Bytes(), &page) != nil || len(page.Items) != 1 || page.Items[0].Content.Citations == nil || page.Items[0].Content.Citations.Sources == nil || page.Items[0].Content.Text != v.Summary {
		t.Fatal("canonical cited HTTP projection missing")
	}
	refresh := read("")
	if refresh.Code != http.StatusOK || !bytes.Equal(first.Body.Bytes(), refresh.Body.Bytes()) {
		t.Fatal("refresh changed canonical answer/manifest")
	}
	// Coherent immutable bytes, not a hash corruption: fail at the selected
	// strict v2 projection after an otherwise valid first page item.
	original := store.events[0]
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"extra", bytes.Replace(payload, []byte(`"summary":`), []byte(`"receipt":"private-receipt","summary":`), 1)},
		{"partial-before", bytes.Replace(payload, []byte(`"schema":`), []byte(`"schema":123,"schema":`), 1)},
		{"partial-after", bytes.Replace(payload, []byte(`"summary":`), []byte(`"schema":123,"summary":`), 1)},
	} {
		store.events = []domain.SessionEvent{original, event(domain.SessionEventID("evt-invalid-"+tc.name), 2, tc.body)}
		clear(tc.body)
		failed := read(first.Header().Get("ETag"))
		if failed.Code < 500 || failed.Header().Get("ETag") != "" || strings.Contains(failed.Body.String(), "private") || strings.Contains(failed.Body.String(), "items") {
			t.Fatalf("invalid %s page leaked partial/cached cited success status=%d", tc.name, failed.Code)
		}
	}
	before := blobs.opens
	auth.memberships[user] = nil
	denied := read("")
	if denied.Code == http.StatusOK || blobs.opens != before || strings.Contains(denied.Body.String(), "private") {
		t.Fatal("permission loss disclosed or read citation bodies")
	}
}
