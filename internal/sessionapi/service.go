// Package sessionapi implements frontend-neutral, participant-authorized
// session listing and history operations shared by WebUI and future adapters.
package sessionapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	defaultMaxPageSize  = uint32(100)
	defaultMaxEventSize = int64(1 << 20)
	cursorVersion       = uint32(1)
	cursorKindSessions  = "sessions"
	cursorKindEvents    = "events"
	cursorKindRuns      = "runs"
)

var ErrSessionUnavailable = errors.New("session is unavailable")

type Config struct {
	CursorKey    []byte
	IDKey        []byte
	MaxPageSize  uint32
	MaxEventSize int64
}

type Service struct {
	store        ports.SessionAPIStore
	blobs        ports.BlobStore
	clock        ports.Clock
	cursorKey    []byte
	idKey        []byte
	maxPageSize  uint32
	maxEventSize int64
}

type Page[T any] struct {
	Items      []T
	NextCursor string
}

type Event struct {
	Event   domain.SessionEvent
	Payload json.RawMessage
}

func New(config Config, store ports.SessionAPIStore, blobs ports.BlobStore, clock ports.Clock) (*Service, error) {
	if store == nil || blobs == nil || clock == nil {
		return nil, errors.New("session API store, blob store, and clock are required")
	}
	if len(config.CursorKey) < 32 || len(config.IDKey) < 32 {
		return nil, errors.New("session API cursor and ID HMAC keys must each contain at least 32 bytes")
	}
	if config.MaxPageSize == 0 {
		config.MaxPageSize = defaultMaxPageSize
	}
	if config.MaxPageSize > defaultMaxPageSize {
		return nil, errors.New("session API page size may not exceed 100")
	}
	if config.MaxEventSize <= 0 {
		config.MaxEventSize = defaultMaxEventSize
	}
	return &Service{
		store: store, blobs: blobs, clock: clock,
		cursorKey: append([]byte(nil), config.CursorKey...), idKey: append([]byte(nil), config.IDKey...),
		maxPageSize: config.MaxPageSize, maxEventSize: config.MaxEventSize,
	}, nil
}

func (service *Service) Create(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	idempotencyKey domain.IdempotencyKey,
) (domain.Session, bool, error) {
	if err := tenantID.Validate(); err != nil {
		return domain.Session{}, false, err
	}
	if err := userID.Validate(); err != nil {
		return domain.Session{}, false, err
	}
	if err := idempotencyKey.Validate(); err != nil {
		return domain.Session{}, false, err
	}
	at := service.clock.Now().UTC()
	sessionID := domain.SessionID(service.stableID("ses_", "session", tenantID, userID, idempotencyKey))
	session := domain.Session{
		ID: sessionID, TenantID: tenantID, CreatedBy: userID, Status: domain.SessionActive,
		CreatedAt: at, UpdatedAt: at,
	}
	owner := domain.SessionParticipant{
		TenantID: tenantID, SessionID: sessionID, UserID: userID,
		Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive,
		CreatedAt: at, UpdatedAt: at,
	}
	return service.store.CreateSessionForUser(ctx, ports.SessionCreateRequest{
		Session: session, Owner: owner, IdempotencyKey: idempotencyKey,
	})
}

func (service *Service) Get(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
) (ports.SessionRecord, error) {
	record, found, err := service.store.GetSessionForUser(ctx, tenantID, userID, sessionID, false)
	if errors.Is(err, domain.ErrMembershipDenied) || !found {
		return ports.SessionRecord{}, ErrSessionUnavailable
	}
	if err != nil {
		return ports.SessionRecord{}, err
	}
	if err := validateSessionRecord(record, tenantID, sessionID); err != nil {
		return ports.SessionRecord{}, err
	}
	return record, nil
}

func (service *Service) List(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	status domain.SessionStatus,
	cursor string,
	limit uint32,
) (Page[ports.SessionRecord], error) {
	var page Page[ports.SessionRecord]
	if limit == 0 || limit > service.maxPageSize {
		return page, domain.ValidationError{Field: "sessions.limit", Reason: "is outside the configured bound"}
	}
	if !status.Valid() {
		return page, domain.ValidationError{Field: "sessions.status", Reason: "is unknown"}
	}
	position, err := service.decodeSessionCursor(cursor, tenantID, userID, status)
	if err != nil {
		return page, err
	}
	items, err := service.store.ListSessionsForUser(ctx, ports.SessionListRequest{
		TenantID: tenantID, UserID: userID, Status: status,
		Before: position, Limit: uint64(limit) + 1,
	})
	if err != nil {
		return page, err
	}
	for _, item := range items {
		if err := validateSessionRecord(item, tenantID, item.Session.ID); err != nil || item.Session.CreatedBy != userID || item.Session.Status != status {
			return Page[ports.SessionRecord]{}, errors.New("session store returned a record outside the authorized listing scope")
		}
	}
	hasMore := len(items) > int(limit)
	if hasMore {
		items = items[:limit]
	}
	page.Items = items
	if hasMore && len(items) != 0 {
		last := items[len(items)-1].Session
		page.NextCursor, err = service.encodeCursor(cursorPayload{
			Version: cursorVersion, Kind: cursorKindSessions,
			TenantID: tenantID, UserID: userID, Status: status,
			UpdatedAt: last.UpdatedAt, SessionID: last.ID,
		})
	}
	return page, err
}

func (service *Service) History(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	cursor string,
	limit uint32,
) (Page[Event], error) {
	var page Page[Event]
	if limit == 0 || limit > service.maxPageSize {
		return page, domain.ValidationError{Field: "events.limit", Reason: "is outside the configured bound"}
	}
	afterSequence, err := service.decodeEventCursor(cursor, tenantID, userID, sessionID)
	if err != nil {
		return page, err
	}
	return service.historyAfter(ctx, tenantID, userID, sessionID, afterSequence, limit)
}

// HistoryAfter provides the explicit canonical sequence boundary used by
// polling clients. The returned continuation remains an authenticated cursor.
func (service *Service) HistoryAfter(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	afterSequence uint64,
	limit uint32,
) (Page[Event], error) {
	if limit == 0 || limit > service.maxPageSize {
		return Page[Event]{}, domain.ValidationError{Field: "events.limit", Reason: "is outside the configured bound"}
	}
	return service.historyAfter(ctx, tenantID, userID, sessionID, afterSequence, limit)
}

func (service *Service) historyAfter(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	afterSequence uint64,
	limit uint32,
) (Page[Event], error) {
	var page Page[Event]
	events, err := service.store.ListSessionHistoryForUser(
		ctx, tenantID, userID, sessionID, afterSequence, uint64(limit)+1,
	)
	if errors.Is(err, domain.ErrMembershipDenied) {
		return page, ErrSessionUnavailable
	}
	if err != nil {
		return page, err
	}
	hasMore := len(events) > int(limit)
	if hasMore {
		events = events[:limit]
	}
	page.Items = make([]Event, 0, len(events))
	for _, event := range events {
		if err := event.Validate(); err != nil || event.TenantID != tenantID || event.SessionID != sessionID {
			return Page[Event]{}, errors.New("session store returned an event outside the authorized history scope")
		}
		payload, err := service.openEventPayload(ctx, tenantID, event)
		if err != nil {
			return Page[Event]{}, err
		}
		page.Items = append(page.Items, Event{Event: event, Payload: payload})
	}
	if hasMore && len(events) != 0 {
		page.NextCursor, err = service.encodeCursor(cursorPayload{
			Version: cursorVersion, Kind: cursorKindEvents,
			TenantID: tenantID, UserID: userID, SessionID: sessionID,
			Sequence: events[len(events)-1].Sequence,
		})
	}
	return page, err
}

func (service *Service) Runs(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	cursor string,
	limit uint32,
) (Page[ports.RunRecord], error) {
	var page Page[ports.RunRecord]
	if limit == 0 || limit > service.maxPageSize {
		return page, domain.ValidationError{Field: "runs.limit", Reason: "is outside the configured bound"}
	}
	position, err := service.decodeRunCursor(cursor, tenantID, userID, sessionID)
	if err != nil {
		return page, err
	}
	runs, err := service.store.ListRunsForUser(ctx, ports.RunListRequest{
		TenantID: tenantID, UserID: userID, SessionID: sessionID,
		Before: position, Limit: uint64(limit) + 1,
	})
	if errors.Is(err, domain.ErrMembershipDenied) {
		return page, ErrSessionUnavailable
	}
	if err != nil {
		return page, err
	}
	for _, record := range runs {
		if err := record.Run.Validate(); err != nil || record.Run.TenantID != tenantID || record.Run.SessionID != sessionID {
			return Page[ports.RunRecord]{}, errors.New("session store returned a run outside the authorized run scope")
		}
	}
	hasMore := len(runs) > int(limit)
	if hasMore {
		runs = runs[:limit]
	}
	page.Items = runs
	if hasMore && len(runs) != 0 {
		last := runs[len(runs)-1].Run
		page.NextCursor, err = service.encodeCursor(cursorPayload{
			Version: cursorVersion, Kind: cursorKindRuns,
			TenantID: tenantID, UserID: userID, SessionID: sessionID,
			CreatedAt: last.CreatedAt, RunID: last.ID,
		})
	}
	return page, err
}

func (service *Service) BindFrontend(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	frontend domain.Frontend,
	externalConversationID string,
	sessionID domain.SessionID,
	expectedRevision uint64,
) (domain.FrontendBinding, error) {
	if err := tenantID.Validate(); err != nil {
		return domain.FrontendBinding{}, err
	}
	if err := userID.Validate(); err != nil {
		return domain.FrontendBinding{}, err
	}
	if err := frontend.Validate(); err != nil {
		return domain.FrontendBinding{}, err
	}
	if strings.TrimSpace(externalConversationID) == "" || len(externalConversationID) > 512 {
		return domain.FrontendBinding{}, domain.ValidationError{Field: "external_conversation_id", Reason: "must contain between 1 and 512 bytes"}
	}
	if err := sessionID.Validate(); err != nil {
		return domain.FrontendBinding{}, err
	}
	bindingID := domain.FrontendBindingID(service.stableID(
		"binding_", "frontend-binding", tenantID, frontend, externalConversationID,
	))
	binding, err := service.store.BindOrSwitchFrontendForUser(ctx, ports.FrontendBindingRequest{
		TenantID: tenantID, UserID: userID, Frontend: frontend,
		ExternalConversationID: externalConversationID, BindingID: bindingID,
		SessionID: sessionID, ExpectedRevision: expectedRevision, At: service.clock.Now().UTC(),
	})
	if errors.Is(err, domain.ErrMembershipDenied) {
		return domain.FrontendBinding{}, ErrSessionUnavailable
	}
	return binding, err
}

func (service *Service) SetArchived(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	archived bool,
	idempotencyKey domain.IdempotencyKey,
) (domain.Session, error) {
	if err := idempotencyKey.Validate(); err != nil {
		return domain.Session{}, err
	}
	session, err := service.store.SetSessionArchivedForUser(
		ctx, tenantID, userID, sessionID, archived, idempotencyKey, service.clock.Now().UTC(),
	)
	if errors.Is(err, domain.ErrMembershipDenied) {
		return domain.Session{}, ErrSessionUnavailable
	}
	return session, err
}

func (service *Service) openEventPayload(
	ctx context.Context,
	tenantID domain.TenantID,
	event domain.SessionEvent,
) (json.RawMessage, error) {
	if event.Payload.Size < 0 || event.Payload.Size > service.maxEventSize {
		return nil, domain.ValidationError{Field: "session_event.payload", Reason: "exceeds the API materialization bound"}
	}
	reader, err := service.blobs.Open(ctx, tenantID, event.Payload)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	payload, err := io.ReadAll(io.LimitReader(reader, service.maxEventSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) != event.Payload.Size || int64(len(payload)) > service.maxEventSize {
		return nil, errors.New("canonical event payload size mismatch")
	}
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != event.Payload.SHA256 {
		return nil, errors.New("canonical event payload digest mismatch")
	}
	if !json.Valid(payload) {
		return nil, errors.New("canonical event payload is not valid JSON")
	}
	return json.RawMessage(payload), nil
}

type cursorPayload struct {
	Version   uint32               `json:"v"`
	Kind      string               `json:"kind"`
	TenantID  domain.TenantID      `json:"tenant_id"`
	UserID    domain.UserID        `json:"user_id"`
	Status    domain.SessionStatus `json:"status"`
	UpdatedAt time.Time            `json:"updated_at"`
	SessionID domain.SessionID     `json:"session_id"`
	Sequence  uint64               `json:"sequence,omitempty"`
	CreatedAt time.Time            `json:"created_at,omitempty"`
	RunID     domain.RunID         `json:"run_id,omitempty"`
}

func (service *Service) encodeCursor(payload cursorPayload) (string, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	signature := hmac.New(sha256.New, service.cursorKey)
	_, _ = signature.Write(encoded)
	return base64.RawURLEncoding.EncodeToString(encoded) + "." +
		base64.RawURLEncoding.EncodeToString(signature.Sum(nil)), nil
}

func (service *Service) decodeSessionCursor(
	token string,
	tenantID domain.TenantID,
	userID domain.UserID,
	status domain.SessionStatus,
) (*ports.SessionListPosition, error) {
	if token == "" {
		return nil, nil
	}
	if len(token) > 512 {
		return nil, domain.ValidationError{Field: "sessions.cursor", Reason: "is invalid"}
	}
	parts := splitCursor(token)
	if len(parts) != 2 {
		return nil, domain.ValidationError{Field: "sessions.cursor", Reason: "is invalid"}
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, domain.ValidationError{Field: "sessions.cursor", Reason: "is invalid"}
	}
	presented, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, domain.ValidationError{Field: "sessions.cursor", Reason: "is invalid"}
	}
	signature := hmac.New(sha256.New, service.cursorKey)
	_, _ = signature.Write(payloadBytes)
	if !hmac.Equal(presented, signature.Sum(nil)) {
		return nil, domain.ValidationError{Field: "sessions.cursor", Reason: "is invalid"}
	}
	var payload cursorPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil || payload.Version != cursorVersion ||
		payload.Kind != cursorKindSessions ||
		payload.TenantID != tenantID || payload.UserID != userID || payload.Status != status ||
		payload.UpdatedAt.IsZero() || payload.SessionID.Validate() != nil {
		return nil, domain.ValidationError{Field: "sessions.cursor", Reason: "does not match the authorized listing scope"}
	}
	return &ports.SessionListPosition{UpdatedAt: payload.UpdatedAt, SessionID: payload.SessionID}, nil
}

func (service *Service) decodeEventCursor(
	token string,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
) (uint64, error) {
	payload, err := service.decodeScopedCursor(token, "events.cursor")
	if err != nil || token == "" {
		return 0, err
	}
	if payload.Kind != cursorKindEvents || payload.TenantID != tenantID || payload.UserID != userID ||
		payload.SessionID != sessionID || payload.Sequence == 0 {
		return 0, domain.ValidationError{Field: "events.cursor", Reason: "does not match the authorized history scope"}
	}
	return payload.Sequence, nil
}

func (service *Service) decodeRunCursor(
	token string,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
) (*ports.RunListPosition, error) {
	payload, err := service.decodeScopedCursor(token, "runs.cursor")
	if err != nil || token == "" {
		return nil, err
	}
	if payload.Kind != cursorKindRuns || payload.TenantID != tenantID || payload.UserID != userID ||
		payload.SessionID != sessionID || payload.CreatedAt.IsZero() || payload.RunID.Validate() != nil {
		return nil, domain.ValidationError{Field: "runs.cursor", Reason: "does not match the authorized run scope"}
	}
	return &ports.RunListPosition{CreatedAt: payload.CreatedAt, RunID: payload.RunID}, nil
}

func (service *Service) decodeScopedCursor(token string, field string) (cursorPayload, error) {
	var payload cursorPayload
	if token == "" {
		return payload, nil
	}
	if len(token) > 512 {
		return payload, domain.ValidationError{Field: field, Reason: "is invalid"}
	}
	parts := splitCursor(token)
	if len(parts) != 2 {
		return payload, domain.ValidationError{Field: field, Reason: "is invalid"}
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return payload, domain.ValidationError{Field: field, Reason: "is invalid"}
	}
	presented, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return payload, domain.ValidationError{Field: field, Reason: "is invalid"}
	}
	signature := hmac.New(sha256.New, service.cursorKey)
	_, _ = signature.Write(payloadBytes)
	if !hmac.Equal(presented, signature.Sum(nil)) {
		return payload, domain.ValidationError{Field: field, Reason: "is invalid"}
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil || payload.Version != cursorVersion {
		return cursorPayload{}, domain.ValidationError{Field: field, Reason: "is invalid"}
	}
	return payload, nil
}

func splitCursor(value string) []string {
	for index := range value {
		if value[index] == '.' {
			return []string{value[:index], value[index+1:]}
		}
	}
	return nil
}

func (service *Service) stableID(prefix string, parts ...any) string {
	digest := hmac.New(sha256.New, service.idKey)
	for _, part := range parts {
		value := fmt.Sprint(part)
		_, _ = digest.Write([]byte{byte(len(value) >> 8), byte(len(value))})
		_, _ = digest.Write([]byte(value))
	}
	return prefix + hex.EncodeToString(digest.Sum(nil)[:20])
}

func validateSessionRecord(record ports.SessionRecord, tenantID domain.TenantID, sessionID domain.SessionID) error {
	if err := record.Session.Validate(); err != nil || record.Session.TenantID != tenantID || record.Session.ID != sessionID {
		return errors.New("session store returned invalid session metadata")
	}
	if err := record.Display.Validate(); err != nil || record.Display.TenantID != tenantID || record.Display.SessionID != sessionID {
		return errors.New("session store returned invalid display metadata")
	}
	if record.Run != nil {
		if err := record.Run.Validate(); err != nil || record.Run.TenantID != tenantID || record.Run.SessionID != sessionID {
			return errors.New("session store returned invalid current-run metadata")
		}
	}
	return nil
}
