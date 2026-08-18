//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/syntheticfrontend"
)

func TestSessionAPIStoreAuthorizationPaginationAndFrontendBinding(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID(fmt.Sprintf("tenant-session-api-%d", now.UnixNano())))
	userID := domain.UserID(uniqueID("user-session-api"))
	seedCanonicalMembership(t, client.DB, tenantID, userID, now)

	var sessions []domain.Session
	for index := range 4 {
		at := now.Add(time.Duration(index) * time.Second)
		session, owner := canonicalSessionFixture(
			tenantID, userID, domain.SessionID(uniqueID("session-api")), at,
		)
		created, fresh, err := store.CreateSessionForUser(ctx, ports.SessionCreateRequest{
			Session: session, Owner: owner,
			IdempotencyKey: domain.IdempotencyKey(uniqueID("create-api")),
		})
		if err != nil || !fresh || created.ID != session.ID {
			t.Fatalf("create session = %+v fresh=%t err=%v", created, fresh, err)
		}
		sessions = append(sessions, created)
	}
	retry, fresh, err := store.CreateSessionForUser(ctx, ports.SessionCreateRequest{
		Session: sessions[0], Owner: ownerForSession(sessions[0], userID),
		IdempotencyKey: "retry-key",
	})
	if err != nil || !fresh || retry.ID != sessions[0].ID {
		t.Fatalf("new key for existing session = %+v fresh=%t err=%v", retry, fresh, err)
	}
	retry, fresh, err = store.CreateSessionForUser(ctx, ports.SessionCreateRequest{
		Session: sessions[0], Owner: ownerForSession(sessions[0], userID),
		IdempotencyKey: "retry-key",
	})
	if err != nil || fresh || retry.ID != sessions[0].ID {
		t.Fatalf("idempotent create = %+v fresh=%t err=%v", retry, fresh, err)
	}

	archivedSession, err := store.SetSessionArchivedForUser(ctx, tenantID, userID, sessions[0].ID, true, "archive-1", now.Add(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	retriedArchive, err := store.SetSessionArchivedForUser(ctx, tenantID, userID, sessions[0].ID, true, "archive-1", now.Add(20*time.Second))
	if err != nil || retriedArchive.UpdatedAt != archivedSession.UpdatedAt {
		t.Fatalf("archive retry = %+v err=%v", retriedArchive, err)
	}
	if _, err := store.SetSessionArchivedForUser(ctx, tenantID, userID, sessions[0].ID, false, "archive-1", now.Add(20*time.Second)); !errors.Is(err, domain.ErrSessionMutationConflict) {
		t.Fatalf("conflicting archive retry error = %v", err)
	}
	active, err := store.ListSessionsForUser(ctx, ports.SessionListRequest{
		TenantID: tenantID, UserID: userID, Status: domain.SessionActive, Limit: 2,
	})
	if err != nil || len(active) != 2 || active[0].Session.ID != sessions[3].ID || active[1].Session.ID != sessions[2].ID {
		t.Fatalf("active first page = %+v err=%v", active, err)
	}
	next, err := store.ListSessionsForUser(ctx, ports.SessionListRequest{
		TenantID: tenantID, UserID: userID, Status: domain.SessionActive, Limit: 2,
		Before: &ports.SessionListPosition{UpdatedAt: active[1].Session.UpdatedAt, SessionID: active[1].Session.ID},
	})
	if err != nil || len(next) != 1 || next[0].Session.ID != sessions[1].ID {
		t.Fatalf("active second page = %+v err=%v", next, err)
	}
	archived, err := store.ListSessionsForUser(ctx, ports.SessionListRequest{
		TenantID: tenantID, UserID: userID, Status: domain.SessionArchived, Limit: 10,
	})
	if err != nil || len(archived) != 1 || archived[0].Session.ID != sessions[0].ID {
		t.Fatalf("archived sessions = %+v err=%v", archived, err)
	}
	if _, err := store.SetSessionArchivedForUser(ctx, tenantID, userID, sessions[0].ID, false, "unarchive-1", now.Add(21*time.Second)); err != nil {
		t.Fatal(err)
	}
	unarchived, found, err := store.GetSessionForUser(ctx, tenantID, userID, sessions[0].ID, false)
	if err != nil || !found || unarchived.Session.Status != domain.SessionActive || unarchived.Session.ArchivedAt != nil {
		t.Fatalf("unarchived session = %+v found=%t err=%v", unarchived, found, err)
	}
	if _, found, err := store.GetSessionForUser(ctx, tenantID, "another-user", sessions[1].ID, false); !errors.Is(err, domain.ErrMembershipDenied) || found {
		t.Fatalf("unauthorized get found=%t err=%v", found, err)
	}

	bindingRequest := ports.FrontendBindingRequest{
		TenantID: tenantID, UserID: userID, Frontend: "synthetic",
		ExternalConversationID: uniqueID("browser-conversation"),
		BindingID:              domain.FrontendBindingID(uniqueID("binding-api")),
		SessionID:              sessions[1].ID, At: now.Add(20 * time.Second),
	}
	binding, err := store.BindOrSwitchFrontendForUser(ctx, bindingRequest)
	if err != nil || binding.Revision != 1 || binding.SessionID != sessions[1].ID {
		t.Fatalf("new binding = %+v err=%v", binding, err)
	}
	bindingRequest.ExpectedRevision, bindingRequest.SessionID = binding.Revision, sessions[2].ID
	bindingRequest.At = bindingRequest.At.Add(time.Second)
	binding, err = store.BindOrSwitchFrontendForUser(ctx, bindingRequest)
	if err != nil || binding.Revision != 2 || binding.SessionID != sessions[2].ID {
		t.Fatalf("switched binding = %+v err=%v", binding, err)
	}
	bindingRequest.UserID = "another-user"
	if _, err := store.BindOrSwitchFrontendForUser(ctx, bindingRequest); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("unauthorized binding error = %v", err)
	}

	metadata, found, err := store.GetSessionAdminMetadata(ctx, tenantID, sessions[2].ID)
	if err != nil || !found || metadata.Session.ID != sessions[2].ID || metadata.Display.SessionID != sessions[2].ID {
		t.Fatalf("admin-safe metadata = %+v found=%t err=%v", metadata, found, err)
	}
}

func TestSessionAPIStorePagesAuthorizedEventsAndRuns(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID(fmt.Sprintf("tenant-session-page-%d", now.UnixNano())))
	userID := domain.UserID(uniqueID("user-session-page"))
	seedCanonicalMembership(t, client.DB, tenantID, userID, now)
	session, owner := canonicalSessionFixture(tenantID, userID, domain.SessionID(uniqueID("session-page")), now)
	if _, _, err := store.CreateSessionForUser(ctx, ports.SessionCreateRequest{
		Session: session, Owner: owner, IdempotencyKey: "create-page",
	}); err != nil {
		t.Fatal(err)
	}

	var events []domain.SessionEvent
	for index := range 3 {
		event := canonicalEventFixture(
			tenantID, session.ID, userID, domain.SessionEventID(uniqueID("event-page")),
			now.Add(time.Duration(index+1)*time.Second),
		)
		if _, err := store.AppendSessionEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	firstEvents, err := store.ListSessionHistoryForUser(ctx, tenantID, userID, session.ID, 0, 2)
	if err != nil || len(firstEvents) != 2 || firstEvents[0].Sequence != 1 || firstEvents[1].Sequence != 2 {
		t.Fatalf("first events = %+v err=%v", firstEvents, err)
	}
	nextEvents, err := store.ListSessionHistoryForUser(ctx, tenantID, userID, session.ID, 2, 2)
	if err != nil || len(nextEvents) != 1 || nextEvents[0].Sequence != 3 {
		t.Fatalf("next events = %+v err=%v", nextEvents, err)
	}
	if _, err := store.ListSessionHistoryForUser(ctx, tenantID, "another-user", session.ID, 0, 2); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("unauthorized history error = %v", err)
	}

	for index, event := range events {
		at := now.Add(time.Duration(index+10) * time.Second)
		run := domain.Run{
			ID: domain.RunID(uniqueID("run-page")), TenantID: tenantID, SessionID: session.ID,
			TriggerEventID: event.ID, SubscriptionConnectionID: domain.SubscriptionConnectionID(uniqueID("subscription-page")),
			Status: domain.RunCreated, IdempotencyKey: domain.IdempotencyKey(uniqueID("run-key-page")),
			CreatedAt: at, UpdatedAt: at,
		}
		if err := store.Transact(ctx, tenantID, func(tx ports.StateTx) error { return tx.PutRun(ctx, run) }); err != nil {
			t.Fatal(err)
		}
	}
	firstRuns, err := store.ListRunsForUser(ctx, ports.RunListRequest{
		TenantID: tenantID, UserID: userID, SessionID: session.ID, Limit: 2,
	})
	if err != nil || len(firstRuns) != 2 || !firstRuns[0].Run.CreatedAt.After(firstRuns[1].Run.CreatedAt) {
		t.Fatalf("first runs = %+v err=%v", firstRuns, err)
	}
	nextRuns, err := store.ListRunsForUser(ctx, ports.RunListRequest{
		TenantID: tenantID, UserID: userID, SessionID: session.ID, Limit: 2,
		Before: &ports.RunListPosition{CreatedAt: firstRuns[1].Run.CreatedAt, RunID: firstRuns[1].Run.ID},
	})
	if err != nil || len(nextRuns) != 1 {
		t.Fatalf("next runs = %+v err=%v", nextRuns, err)
	}
	if _, err := store.ListRunsForUser(ctx, ports.RunListRequest{
		TenantID: tenantID, UserID: "another-user", SessionID: session.ID, Limit: 2,
	}); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("unauthorized runs error = %v", err)
	}
}

func TestTelegramAndSyntheticFrontendOpenTheSameAuthorizedSession(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID(fmt.Sprintf("tenant-two-frontends-%d", now.UnixNano())))
	userID := domain.UserID(uniqueID("user-two-frontends"))
	seedCanonicalMembership(t, client.DB, tenantID, userID, now)
	blobs := newSessionAPITestBlobs()
	ingress, err := sessioningress.New(sessioningress.Config{
		IDKey: []byte(strings.Repeat("i", 32)),
	}, store, blobs)
	if err != nil {
		t.Fatal(err)
	}
	telegram, err := ingress.EnsureSession(ctx, sessioningress.Actor{
		TenantID: tenantID, UserID: userID, Frontend: domain.FrontendTelegram,
		ExternalConversationID: "telegram-chat-42",
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	synthetic := syntheticfrontend.New(ingress, tenantID, userID, "synthetic-browser-42")
	syntheticState, err := synthetic.EnsureSession(ctx, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if syntheticState.Session.ID == telegram.Session.ID {
		t.Fatal("independent frontend conversations unexpectedly shared their initial session")
	}
	api, err := sessionapi.New(sessionapi.Config{
		CursorKey: []byte(strings.Repeat("c", 32)), IDKey: []byte(strings.Repeat("a", 32)),
	}, store, blobs, sessionAPITestClock{at: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := api.BindFrontend(
		ctx, tenantID, userID, syntheticfrontend.Frontend, "synthetic-browser-42",
		telegram.Session.ID, syntheticState.Binding.Revision,
	)
	if err != nil || binding.SessionID != telegram.Session.ID || binding.ID != syntheticState.Binding.ID {
		t.Fatalf("synthetic attach = %+v err=%v", binding, err)
	}
	result, err := synthetic.Send(ctx, "synthetic-delivery-1", "same canonical session", "subscription-two-frontends", now.Add(3*time.Second))
	if err != nil || result.SessionID != telegram.Session.ID {
		t.Fatalf("synthetic event = %+v err=%v", result, err)
	}
	record, err := api.Get(ctx, tenantID, userID, telegram.Session.ID)
	if err != nil || record.Session.LastEventSequence != 1 || record.Display.Origin == nil || *record.Display.Origin != syntheticfrontend.Frontend {
		t.Fatalf("shared session metadata = %+v err=%v", record, err)
	}
}

func ownerForSession(session domain.Session, userID domain.UserID) domain.SessionParticipant {
	return domain.SessionParticipant{
		TenantID: session.TenantID, SessionID: session.ID, UserID: userID,
		Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive,
		CreatedAt: session.CreatedAt, UpdatedAt: session.CreatedAt,
	}
}

type sessionAPITestClock struct{ at time.Time }

func (clock sessionAPITestClock) Now() time.Time { return clock.at }

type sessionAPITestBlobs struct{ values map[string][]byte }

func newSessionAPITestBlobs() *sessionAPITestBlobs {
	return &sessionAPITestBlobs{values: make(map[string][]byte)}
}

func (blobs *sessionAPITestBlobs) Put(_ context.Context, tenantID domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	payload, err := io.ReadAll(body)
	if err != nil {
		return domain.BlobRef{}, err
	}
	digest := sha256.Sum256(payload)
	blobs.values[key] = append([]byte(nil), payload...)
	return domain.BlobRef{
		TenantID: tenantID, Key: key, Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func (blobs *sessionAPITestBlobs) Open(_ context.Context, _ domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	payload, found := blobs.values[ref.Key]
	if !found {
		return nil, errors.New("blob not found")
	}
	return io.NopCloser(bytes.NewReader(payload)), nil
}

func (*sessionAPITestBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	return nil
}
