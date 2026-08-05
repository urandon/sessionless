// Package webcontract defines the transport DTOs for the same-origin WebUI.
// Tenant and user authority always comes from a resolved server-side session;
// resource IDs in requests are selectors only.
package webcontract

import (
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	RouteOIDCStart       = "/auth/telegram/start"
	RouteOIDCCallback    = "/auth/telegram/callback"
	RouteLogout          = "/auth/logout"
	RouteMe              = "/api/web/v1/me"
	RouteTenants         = "/api/web/v1/tenants"
	RouteActiveTenant    = "/api/web/v1/active-tenant"
	RouteSessions        = "/api/web/v1/sessions"
	RouteSessionEvents   = "/api/web/v1/sessions/{session_id}/events"
	RouteArchiveSession  = "/api/web/v1/sessions/{session_id}/archive"
	RouteSessionMessages = "/api/web/v1/sessions/{session_id}/messages"
	RouteUploads         = "/api/web/v1/uploads"
	RouteUploadCommit    = "/api/web/v1/uploads/{upload_id}/commit"
	RouteRun             = "/api/web/v1/runs/{run_id}"
)

const MaxMessageUploadCount = 8
const MaxPageSize = uint32(100)

const (
	SessionCookieName = "__Host-sessionless"
	CSRFCookieName    = "__Host-sessionless-csrf"
	CSRFHeaderName    = "X-Sessionless-CSRF"
)

type ErrorCode string

const (
	ErrorInvalidRequest         ErrorCode = "invalid_request"
	ErrorUnauthenticated        ErrorCode = "unauthenticated"
	ErrorAccessDenied           ErrorCode = "access_denied"
	ErrorCSRFFailed             ErrorCode = "csrf_failed"
	ErrorNotFound               ErrorCode = "not_found"
	ErrorConflict               ErrorCode = "conflict"
	ErrorPayloadTooLarge        ErrorCode = "payload_too_large"
	ErrorRateLimited            ErrorCode = "rate_limited"
	ErrorTemporarilyUnavailable ErrorCode = "temporarily_unavailable"
)

type Error struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	RequestID string    `json:"request_id"`
}

func (failure Error) Validate() error {
	if failure.Code.HTTPStatus() == 0 {
		return domain.ValidationError{Field: "error.code", Reason: "is unknown"}
	}
	if strings.TrimSpace(failure.Message) == "" {
		return domain.ValidationError{Field: "error.message", Reason: "must not be empty"}
	}
	return domain.ValidateOpaqueID("error.request_id", failure.RequestID)
}

func (code ErrorCode) HTTPStatus() int {
	switch code {
	case ErrorInvalidRequest:
		return http.StatusBadRequest
	case ErrorUnauthenticated:
		return http.StatusUnauthorized
	case ErrorAccessDenied, ErrorCSRFFailed:
		return http.StatusForbidden
	case ErrorNotFound:
		return http.StatusNotFound
	case ErrorConflict:
		return http.StatusConflict
	case ErrorPayloadTooLarge:
		return http.StatusRequestEntityTooLarge
	case ErrorRateLimited:
		return http.StatusTooManyRequests
	case ErrorTemporarilyUnavailable:
		return http.StatusServiceUnavailable
	default:
		return 0
	}
}

type ErrorEnvelope struct {
	Error Error `json:"error"`
}

type OIDCStartRequest struct {
	ReturnTo string `json:"return_to,omitempty"`
}

func (request OIDCStartRequest) Validate() error {
	if request.ReturnTo == "" {
		return nil
	}
	parsed, err := url.Parse(request.ReturnTo)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || !strings.HasPrefix(parsed.Path, "/") ||
		strings.HasPrefix(parsed.Path, "//") || strings.Contains(request.ReturnTo, "\\") ||
		strings.Contains(parsed.Path, "\\") || hasControl(parsed.Path) {
		return domain.ValidationError{Field: "return_to", Reason: "must be a local absolute path"}
	}
	return nil
}

type OIDCCallback struct {
	Code             string `json:"code,omitempty"`
	State            string `json:"state"`
	Error            string `json:"error,omitempty"`
	ErrorDescription string `json:"error_description,omitempty"`
}

func (callback OIDCCallback) Validate() error {
	if strings.TrimSpace(callback.State) == "" {
		return domain.ValidationError{Field: "oidc_callback.state", Reason: "is required"}
	}
	if err := domain.ValidateOpaqueID("oidc_callback.state", callback.State); err != nil {
		return err
	}
	hasCode, hasError := strings.TrimSpace(callback.Code) != "", strings.TrimSpace(callback.Error) != ""
	if hasCode == hasError {
		return domain.ValidationError{Field: "oidc_callback", Reason: "must contain exactly one of code or error"}
	}
	if len(callback.Code) > 2048 || len(callback.State) > 512 || len(callback.ErrorDescription) > 512 {
		return domain.ValidationError{Field: "oidc_callback", Reason: "contains an oversized value"}
	}
	if hasError {
		if err := domain.ValidateOpaqueID("oidc_callback.error", callback.Error); err != nil {
			return err
		}
	}
	return nil
}

type Identity struct {
	UserID   domain.UserID   `json:"user_id"`
	Provider string          `json:"provider"`
	Tenants  []TenantSummary `json:"tenants"`
}

type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type TenantSummary struct {
	TenantID domain.TenantID             `json:"tenant_id"`
	Role     domain.TenantMembershipRole `json:"role"`
	Active   bool                        `json:"active"`
}

// SelectTenantRequest carries a selector, not authority. The BFF must resolve
// an active membership and rotate the opaque web session before accepting it.
type SelectTenantRequest struct {
	TenantID domain.TenantID `json:"tenant_id"`
}

func (request SelectTenantRequest) Validate() error { return request.TenantID.Validate() }

type CreateSessionRequest struct {
	IdempotencyKey domain.IdempotencyKey `json:"idempotency_key"`
}

type SessionListQuery struct {
	Cursor string
	Limit  uint32
}

func (query SessionListQuery) Validate() error {
	if query.Limit == 0 || query.Limit > MaxPageSize {
		return domain.ValidationError{Field: "sessions.limit", Reason: "must be between 1 and 100"}
	}
	if len(query.Cursor) > 512 {
		return domain.ValidationError{Field: "sessions.cursor", Reason: "must not exceed 512 bytes"}
	}
	return nil
}

type EventListQuery struct {
	AfterSequence uint64
	Limit         uint32
}

func (query EventListQuery) Validate() error {
	if query.Limit == 0 || query.Limit > MaxPageSize {
		return domain.ValidationError{Field: "events.limit", Reason: "must be between 1 and 100"}
	}
	return nil
}

type ArchiveSessionRequest struct {
	Archived       bool                  `json:"archived"`
	IdempotencyKey domain.IdempotencyKey `json:"idempotency_key"`
}

func (request ArchiveSessionRequest) Validate() error { return request.IdempotencyKey.Validate() }

func (request CreateSessionRequest) Validate() error { return request.IdempotencyKey.Validate() }

type CreateMessageRequest struct {
	IdempotencyKey domain.IdempotencyKey   `json:"idempotency_key"`
	Text           string                  `json:"text,omitempty"`
	UploadIDs      []domain.UploadIntentID `json:"upload_ids,omitempty"`
}

func (request CreateMessageRequest) Validate() error {
	if err := request.IdempotencyKey.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(request.Text) == "" && len(request.UploadIDs) == 0 {
		return domain.ValidationError{Field: "message.content", Reason: "text or an upload is required"}
	}
	if len(request.UploadIDs) > MaxMessageUploadCount {
		return domain.ValidationError{Field: "message.upload_ids", Reason: "exceeds the bounded attachment count"}
	}
	if utf8.RuneCountInString(request.Text) > 32_000 {
		return domain.ValidationError{Field: "message.text", Reason: "must not exceed 32000 Unicode characters"}
	}
	seen := make(map[domain.UploadIntentID]struct{}, len(request.UploadIDs))
	for _, uploadID := range request.UploadIDs {
		if err := uploadID.Validate(); err != nil {
			return err
		}
		if _, exists := seen[uploadID]; exists {
			return domain.ValidationError{Field: "message.upload_ids", Reason: "must not contain duplicates"}
		}
		seen[uploadID] = struct{}{}
	}
	return nil
}

type CreateUploadIntentRequest struct {
	SessionID domain.SessionID `json:"session_id"`
	Name      string           `json:"name"`
	MediaType string           `json:"media_type"`
	Size      int64            `json:"size"`
	SHA256    string           `json:"sha256"`
}

func (request CreateUploadIntentRequest) Validate(maxBytes int64) error {
	if err := request.SessionID.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(request.Name) == "" || strings.TrimSpace(request.MediaType) == "" {
		return domain.ValidationError{Field: "upload.metadata", Reason: "name and media_type are required"}
	}
	if len(request.Name) > 255 || len(request.MediaType) > 127 {
		return domain.ValidationError{Field: "upload.metadata", Reason: "name or media_type exceeds its bounded length"}
	}
	if request.Size <= 0 || maxBytes <= 0 || request.Size > maxBytes {
		return domain.ValidationError{Field: "upload.size", Reason: "must be positive and within the configured limit"}
	}
	return validateDigest(request.SHA256)
}

// UploadIntentResponse contains a short-lived capability URL. It must be
// redacted from logs and never stored in browser-persistent storage.
type UploadIntentResponse struct {
	UploadID  domain.UploadIntentID `json:"upload_id"`
	Method    string                `json:"method"`
	URL       string                `json:"url"`
	Headers   map[string]string     `json:"headers"`
	ExpiresAt time.Time             `json:"expires_at"`
}

type CommitUploadRequest struct {
	UploadID domain.UploadIntentID `json:"upload_id"`
}

type UploadCommitResponse struct {
	UploadID  domain.UploadIntentID `json:"upload_id"`
	Name      string                `json:"name"`
	MediaType string                `json:"media_type"`
	Size      int64                 `json:"size"`
}

func (request CommitUploadRequest) Validate(pathUploadID domain.UploadIntentID) error {
	if err := request.UploadID.Validate(); err != nil {
		return err
	}
	if request.UploadID != pathUploadID {
		return domain.ValidationError{Field: "upload_id", Reason: "body and path selectors must match"}
	}
	return nil
}

type SessionSummary struct {
	SessionID domain.SessionID     `json:"session_id"`
	Status    domain.SessionStatus `json:"status"`
	LastSeq   uint64               `json:"last_sequence"`
	UpdatedAt time.Time            `json:"updated_at"`
}

type SessionEvent struct {
	EventID   domain.SessionEventID   `json:"event_id"`
	Sequence  uint64                  `json:"sequence"`
	Kind      domain.SessionEventKind `json:"kind"`
	Content   EventContent            `json:"content"`
	CreatedAt time.Time               `json:"created_at"`
}

type EventContent struct {
	Text        string       `json:"text,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
}

type Attachment struct {
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
	Size      int64  `json:"size"`
	Download  string `json:"download_url"`
}

type Run struct {
	RunID     domain.RunID          `json:"run_id"`
	SessionID domain.SessionID      `json:"session_id"`
	TriggerID domain.SessionEventID `json:"trigger_event_id"`
	Status    domain.RunStatus      `json:"status"`
	UpdatedAt time.Time             `json:"updated_at"`
}

func SessionCookie(value string, maxAge time.Duration) *http.Cookie {
	return &http.Cookie{
		Name: SessionCookieName, Value: value, Path: "/", MaxAge: int(maxAge.Seconds()),
		Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode,
	}
}

func CSRFCookie(value string, maxAge time.Duration) *http.Cookie {
	return &http.Cookie{
		Name: CSRFCookieName, Value: value, Path: "/", MaxAge: int(maxAge.Seconds()),
		Secure: true, HttpOnly: false, SameSite: http.SameSiteStrictMode,
	}
}

func validateDigest(value string) error {
	if value != strings.ToLower(value) {
		return domain.ValidationError{Field: "upload.sha256", Reason: "must be a lowercase SHA-256 digest"}
	}
	if err := (domain.BlobRef{TenantID: "validation", Key: "tenants/validation/digest", SHA256: value}).Validate(); err != nil {
		return domain.ValidationError{Field: "upload.sha256", Reason: "must be a lowercase SHA-256 digest"}
	}
	return nil
}

func hasControl(value string) bool {
	return strings.IndexFunc(value, func(character rune) bool { return character < 0x20 || character == 0x7f }) >= 0
}
