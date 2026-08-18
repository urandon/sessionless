// Package webbff implements the same-origin authentication BFF. It owns
// browser cookies and protocol orchestration while delegating Telegram tokens
// and durable authorization state to narrow ports.
package webbff

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"gitcode.com/urandon/sessionless/internal/buildinfo"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
	"gitcode.com/urandon/sessionless/internal/webapi"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

const (
	LoginBindingCookieName = "__Host-sessionless-login"
	defaultChallengeTTL    = 10 * time.Minute
	defaultIdleTTL         = 12 * time.Hour
	defaultAbsoluteTTL     = 7 * 24 * time.Hour
	maxRequestBytes        = 64 << 10
	maxMemberships         = uint64(200)
)

type Config struct {
	BaseURL      string
	RedirectURI  string
	OIDCPolicy   domain.OIDCVerificationPolicy
	Provider     ports.OIDCProvider
	Store        ports.WebAuthStore
	Sessions     *sessionapi.Service
	API          *webapi.Service
	IDs          ports.IDGenerator
	Clock        ports.Clock
	Random       io.Reader
	Logger       *slog.Logger
	Build        buildinfo.Info
	ChallengeTTL time.Duration
	IdleTTL      time.Duration
	AbsoluteTTL  time.Duration
}

type Handler struct {
	config Config
	mux    *http.ServeMux
}

func New(config Config) (*Handler, error) {
	if config.Provider == nil || config.Store == nil || config.IDs == nil || config.Clock == nil {
		return nil, errors.New("web BFF provider, store, ID generator, and clock are required")
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.ChallengeTTL <= 0 {
		config.ChallengeTTL = defaultChallengeTTL
	}
	if config.IdleTTL <= 0 {
		config.IdleTTL = defaultIdleTTL
	}
	if config.AbsoluteTTL <= 0 {
		config.AbsoluteTTL = defaultAbsoluteTTL
	}
	if config.ChallengeTTL > defaultChallengeTTL || config.IdleTTL > defaultIdleTTL || config.AbsoluteTTL > defaultAbsoluteTTL {
		return nil, errors.New("web BFF security lifetimes may only be shortened from the documented defaults")
	}
	base, err := url.Parse(config.BaseURL)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.Path != "" || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("WEB_BASE_URL must be an HTTPS origin")
	}
	redirect, err := url.Parse(config.RedirectURI)
	if err != nil || redirect.Scheme != "https" || redirect.Host != base.Host || redirect.Path != webcontract.RouteOIDCCallback || redirect.RawQuery != "" || redirect.Fragment != "" {
		return nil, errors.New("Telegram redirect URI must be the exact same-origin callback")
	}
	if err := config.OIDCPolicy.Validate(); err != nil {
		return nil, err
	}
	handler := &Handler{config: config, mux: http.NewServeMux()}
	handler.routes()
	return handler, nil
}

func (handler *Handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	requestID, err := handler.secret("req_", 16)
	if err != nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("X-Request-ID", requestID)
	w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
	w.Header().Set("Cache-Control", "no-store")
	request = request.WithContext(context.WithValue(request.Context(), requestIDKey{}, requestID))
	response := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	handler.mux.ServeHTTP(response, request)
	handler.config.Logger.Info("web request completed",
		"request_id", requestID,
		"method", request.Method,
		"path", request.URL.Path,
		"status", response.status,
	)
}

func (handler *Handler) routes() {
	handler.mux.HandleFunc("GET /healthz", handler.health)
	handler.mux.HandleFunc("GET /readyz", handler.ready)
	handler.mux.HandleFunc("GET /version", handler.version)
	handler.mux.HandleFunc("GET "+webcontract.RouteOIDCStart, handler.startLogin)
	handler.mux.HandleFunc("GET "+webcontract.RouteOIDCCallback, handler.loginCallback)
	handler.mux.HandleFunc("POST "+webcontract.RouteLogout, handler.logout)
	handler.mux.HandleFunc("GET "+webcontract.RouteMe, handler.me)
	handler.mux.HandleFunc("GET "+webcontract.RouteTenants, handler.tenants)
	handler.mux.HandleFunc("POST "+webcontract.RouteActiveTenant, handler.switchTenant)
	if handler.config.Sessions != nil {
		handler.mux.HandleFunc("GET "+webcontract.RouteSessions, handler.listSessions)
		handler.mux.HandleFunc("POST "+webcontract.RouteSessions, handler.createSession)
		handler.mux.HandleFunc("GET "+webcontract.RouteSession, handler.getSession)
		handler.mux.HandleFunc("GET "+webcontract.RouteSessionEvents, handler.listSessionEvents)
		handler.mux.HandleFunc("GET "+webcontract.RouteSessionRuns, handler.listSessionRuns)
		handler.mux.HandleFunc("POST "+webcontract.RouteArchiveSession, handler.setSessionArchived)
	}
	if handler.config.API != nil {
		handler.mux.HandleFunc("POST "+webcontract.RouteSessionMessages, handler.createMessage)
		handler.mux.HandleFunc("POST "+webcontract.RouteUploads, handler.createUpload)
		handler.mux.HandleFunc("POST "+webcontract.RouteUploadCommit, handler.commitUpload)
		handler.mux.HandleFunc("GET "+webcontract.RouteRun, handler.getRun)
		handler.mux.HandleFunc("GET "+webcontract.RouteEventAttachment, handler.downloadAttachment)
	}
}

func (handler *Handler) listSessions(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorize(request, domain.TenantPermissionRead)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	status := domain.SessionStatus(request.URL.Query().Get("status"))
	if status == "" {
		status = domain.SessionActive
	}
	query := webcontract.SessionListQuery{
		Cursor: request.URL.Query().Get("cursor"), Limit: queryLimit(request, 50), Status: status,
	}
	if err := query.Validate(); err != nil {
		handler.writeError(w, request, err)
		return
	}
	page, err := handler.config.Sessions.List(
		request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID,
		query.Status, query.Cursor, query.Limit,
	)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	items := make([]webcontract.SessionSummary, 0, len(page.Items))
	for _, item := range page.Items {
		items = append(items, sessionSummary(item))
	}
	handler.writeJSONConditional(w, request, webcontract.Page[webcontract.SessionSummary]{
		Items: items, NextCursor: page.NextCursor,
	}, pollAfterSessions(items))
}

func (handler *Handler) createSession(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorizeMutation(request, domain.TenantPermissionWrite)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	var input webcontract.CreateSessionRequest
	if err := decodeJSON(request, &input); err != nil || input.Validate() != nil {
		if err == nil {
			err = input.Validate()
		}
		handler.writeError(w, request, err)
		return
	}
	session, created, err := handler.config.Sessions.Create(
		request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID, input.IdempotencyKey,
	)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	handler.writeJSON(w, status, webcontract.SessionSummary{
		SessionID: session.ID, Status: session.Status, LastSeq: session.LastEventSequence,
		CreatedAt: session.CreatedAt, UpdatedAt: session.UpdatedAt, ArchivedAt: session.ArchivedAt,
	})
}

func (handler *Handler) getSession(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorize(request, domain.TenantPermissionRead)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	record, err := handler.config.Sessions.Get(
		request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID,
		domain.SessionID(request.PathValue("session_id")),
	)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	summary := sessionSummary(record)
	handler.writeJSONConditional(w, request, summary, pollAfterRun(summary.CurrentRun))
}

func (handler *Handler) listSessionEvents(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorize(request, domain.TenantPermissionRead)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	afterSequence, err := optionalUint64(request, "after_sequence")
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	query := webcontract.EventListQuery{
		Cursor: request.URL.Query().Get("cursor"), AfterSequence: afterSequence,
		Limit: queryLimit(request, 50),
	}
	if err := query.Validate(); err != nil {
		handler.writeError(w, request, err)
		return
	}
	var page sessionapi.Page[sessionapi.Event]
	if query.AfterSequence != nil {
		page, err = handler.config.Sessions.HistoryAfter(
			request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID,
			domain.SessionID(request.PathValue("session_id")), *query.AfterSequence, query.Limit,
		)
	} else {
		page, err = handler.config.Sessions.History(
			request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID,
			domain.SessionID(request.PathValue("session_id")), query.Cursor, query.Limit,
		)
	}
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	items := make([]webcontract.SessionEvent, 0, len(page.Items))
	for _, item := range page.Items {
		items = append(items, projectEvent(item))
	}
	handler.writeJSONConditional(w, request, webcontract.Page[webcontract.SessionEvent]{
		Items: items, NextCursor: page.NextCursor,
	}, 0)
}

func (handler *Handler) listSessionRuns(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorize(request, domain.TenantPermissionRead)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	query := webcontract.RunListQuery{
		Cursor: request.URL.Query().Get("cursor"), Limit: queryLimit(request, 50),
	}
	if err := query.Validate(); err != nil {
		handler.writeError(w, request, err)
		return
	}
	page, err := handler.config.Sessions.Runs(
		request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID,
		domain.SessionID(request.PathValue("session_id")), query.Cursor, query.Limit,
	)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	items := make([]webcontract.Run, 0, len(page.Items))
	for _, record := range page.Items {
		items = append(items, projectRun(record.Run, record.Provider))
	}
	handler.writeJSONConditional(w, request, webcontract.Page[webcontract.Run]{
		Items: items, NextCursor: page.NextCursor,
	}, pollAfterRuns(items))
}

func (handler *Handler) bindFrontend(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorizeMutation(request, domain.TenantPermissionWrite)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	var input webcontract.BindFrontendRequest
	if err := decodeJSON(request, &input); err != nil {
		handler.writeError(w, request, err)
		return
	}
	if err := input.Validate(); err != nil {
		handler.writeError(w, request, err)
		return
	}
	binding, err := handler.config.Sessions.BindFrontend(
		request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID,
		input.Frontend, input.ExternalConversationID, input.SessionID, input.ExpectedRevision,
	)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	status := http.StatusOK
	if binding.Revision == 1 && input.ExpectedRevision == 0 {
		status = http.StatusCreated
	}
	handler.writeJSON(w, status, webcontract.FrontendBinding{
		BindingID: binding.ID, Frontend: binding.Frontend,
		ExternalConversationID: binding.ExternalConversationID, SessionID: binding.SessionID,
		Revision: binding.Revision, CreatedAt: binding.CreatedAt, UpdatedAt: binding.UpdatedAt,
	})
}

func (handler *Handler) setSessionArchived(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorizeMutation(request, domain.TenantPermissionWrite)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	var input webcontract.ArchiveSessionRequest
	if err := decodeJSON(request, &input); err != nil || input.Validate() != nil {
		if err == nil {
			err = input.Validate()
		}
		handler.writeError(w, request, err)
		return
	}
	session, err := handler.config.Sessions.SetArchived(
		request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID,
		domain.SessionID(request.PathValue("session_id")), input.Archived, input.IdempotencyKey,
	)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	handler.writeJSON(w, http.StatusOK, webcontract.SessionSummary{
		SessionID: session.ID, Status: session.Status, LastSeq: session.LastEventSequence,
		CreatedAt: session.CreatedAt, UpdatedAt: session.UpdatedAt, ArchivedAt: session.ArchivedAt,
	})
}

func (handler *Handler) createUpload(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorizeMutation(request, domain.TenantPermissionWrite)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	var input webcontract.CreateUploadIntentRequest
	if err := decodeJSON(request, &input); err != nil {
		handler.writeError(w, request, err)
		return
	}
	response, created, err := handler.config.API.CreateUpload(
		request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID, input,
	)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	handler.writeJSON(w, status, response)
}

func (handler *Handler) commitUpload(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorizeMutation(request, domain.TenantPermissionWrite)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	pathID := domain.UploadIntentID(request.PathValue("upload_id"))
	var input webcontract.CommitUploadRequest
	if err := decodeJSON(request, &input); err != nil {
		handler.writeError(w, request, err)
		return
	}
	if err := input.Validate(pathID); err != nil {
		handler.writeError(w, request, err)
		return
	}
	intent, err := handler.config.API.CommitUpload(
		request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID, pathID,
	)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	handler.writeJSON(w, http.StatusOK, webcontract.UploadCommitResponse{
		UploadID: intent.ID, Name: intent.Name, MediaType: intent.MediaType, Size: intent.ExpectedSize,
	})
}

func (handler *Handler) createMessage(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorizeMutation(request, domain.TenantPermissionWrite)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	var input webcontract.CreateMessageRequest
	if err := decodeJSON(request, &input); err != nil {
		handler.writeError(w, request, err)
		return
	}
	response, err := handler.config.API.SubmitMessage(
		request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID,
		domain.SessionID(request.PathValue("session_id")), input,
	)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	status := http.StatusOK
	if response.Created {
		status = http.StatusCreated
	}
	if !response.Compute.Quota.Valid() || response.Compute.Quota == domain.ProviderQuotaUnknown {
		w.Header().Set("Retry-After", "5")
	} else {
		w.Header().Set("Retry-After", "2")
	}
	handler.writeJSON(w, status, response)
}

func (handler *Handler) getRun(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorize(request, domain.TenantPermissionRead)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	record, err := handler.config.API.GetRun(
		request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID,
		domain.RunID(request.PathValue("run_id")),
	)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	run := projectRun(record.Run, record.Provider)
	handler.writeJSONConditional(w, request, run, pollAfterRun(&run))
}

func (handler *Handler) downloadAttachment(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorize(request, domain.TenantPermissionRead)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	sequence, err := strconv.ParseUint(request.PathValue("sequence"), 10, 64)
	if err != nil {
		handler.writeError(w, request, domain.ValidationError{Field: "sequence", Reason: "must be an unsigned integer"})
		return
	}
	index, err := strconv.ParseUint(request.PathValue("index"), 10, 32)
	if err != nil {
		handler.writeError(w, request, domain.ValidationError{Field: "index", Reason: "must be an unsigned integer"})
		return
	}
	capability, err := handler.config.API.DownloadAttachment(
		request.Context(), authorization.Session.ActiveTenantID, authorization.Session.UserID,
		domain.SessionID(request.PathValue("session_id")), sequence, uint32(index),
	)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	handler.writeJSON(w, http.StatusOK, webcontract.DownloadCapabilityResponse{
		Method: capability.Method, URL: capability.URL, Headers: capability.Headers, ExpiresAt: capability.ExpiresAt,
	})
}

func (handler *Handler) health(w http.ResponseWriter, _ *http.Request) {
	handler.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (handler *Handler) ready(w http.ResponseWriter, _ *http.Request) {
	handler.writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (handler *Handler) version(w http.ResponseWriter, _ *http.Request) {
	handler.writeJSON(w, http.StatusOK, handler.config.Build)
}

func (handler *Handler) startLogin(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Referrer-Policy", "no-referrer")
	start := webcontract.OIDCStartRequest{ReturnTo: request.URL.Query().Get("return_to")}
	if err := start.Validate(); err != nil {
		handler.writeLoginFailure(w, request, "invalid_login_request", webcontract.ErrorInvalidRequest, "The login request is invalid.", nil, "")
		return
	}
	if start.ReturnTo == "" {
		start.ReturnTo = "/"
	}
	state, err := handler.secret("state_", 32)
	if err != nil {
		handler.writeLoginError(w, request, "challenge_generation_failed", err, nil, "")
		return
	}
	browserBinding, err := handler.secret("binding_", 32)
	if err != nil {
		handler.writeLoginError(w, request, "challenge_generation_failed", err, nil, "")
		return
	}
	verifier, err := handler.secret("pkce_", 32)
	if err != nil {
		handler.writeLoginError(w, request, "challenge_generation_failed", err, nil, "")
		return
	}
	nonce, err := handler.secret("nonce_", 32)
	if err != nil {
		handler.writeLoginError(w, request, "challenge_generation_failed", err, nil, "")
		return
	}
	now := handler.config.Clock.Now().UTC()
	challenge := domain.OIDCLoginChallenge{
		StateDigest: domain.DigestSecret(state), BrowserBindingDigest: domain.DigestSecret(browserBinding),
		PKCEVerifier: verifier, Nonce: nonce, RedirectPath: start.ReturnTo,
		CreatedAt: now, ExpiresAt: now.Add(handler.config.ChallengeTTL),
	}
	if err := handler.config.Store.CreateLoginChallenge(request.Context(), challenge); err != nil {
		handler.writeLoginError(w, request, "challenge_persistence_failed", err, nil, "")
		return
	}
	digest := sha256.Sum256([]byte(verifier))
	authorizationURL, err := handler.config.Provider.AuthorizationURL(request.Context(), ports.OIDCAuthorizationRequest{
		Provider: domain.IdentityProviderTelegram, RedirectURI: handler.config.RedirectURI,
		State: state, Nonce: nonce, CodeChallenge: base64.RawURLEncoding.EncodeToString(digest[:]),
		Scopes: []string{"openid", "profile"},
	})
	if err != nil {
		handler.writeLoginError(w, request, "provider_authorization_failed", err, nil, "")
		return
	}
	http.SetCookie(w, loginBindingCookie(browserBinding, handler.config.ChallengeTTL))
	w.Header().Set("Location", authorizationURL)
	w.WriteHeader(http.StatusSeeOther)
}

func (handler *Handler) loginCallback(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Referrer-Policy", "no-referrer")
	callback := webcontract.OIDCCallback{
		Code: request.URL.Query().Get("code"), State: request.URL.Query().Get("state"),
		Error: request.URL.Query().Get("error"), ErrorDescription: request.URL.Query().Get("error_description"),
	}
	if err := callback.Validate(); err != nil {
		handler.writeLoginFailure(w, request, "invalid_callback", webcontract.ErrorInvalidRequest, "The login callback is invalid.", nil, "")
		return
	}
	bindingCookie, err := request.Cookie(LoginBindingCookieName)
	if err != nil || bindingCookie.Value == "" {
		handler.writeLoginFailure(w, request, "browser_binding_missing", webcontract.ErrorAccessDenied, "The login transaction could not be verified.", nil, "")
		return
	}
	http.SetCookie(w, clearCookie(LoginBindingCookieName, true))
	now := handler.config.Clock.Now().UTC()
	challenge, err := handler.config.Store.ConsumeLoginChallenge(
		request.Context(), domain.DigestSecret(callback.State), bindingCookie.Value, now,
	)
	if err != nil {
		handler.writeLoginFailure(w, request, "login_challenge_rejected", webcontract.ErrorAccessDenied, "The login transaction could not be verified.", nil, "")
		return
	}
	if callback.Error != "" {
		handler.writeLoginFailure(w, request, "provider_denied", webcontract.ErrorAccessDenied, "Telegram login was not completed.", nil, "")
		return
	}
	claims, err := handler.config.Provider.ExchangeAndVerify(request.Context(), ports.OIDCTokenRequest{
		Provider: domain.IdentityProviderTelegram, Code: callback.Code,
		RedirectURI: handler.config.RedirectURI, PKCEVerifier: challenge.PKCEVerifier,
		ExpectedNonce: challenge.Nonce, Policy: handler.config.OIDCPolicy, Now: now,
	})
	if err != nil {
		handler.writeLoginFailure(w, request, "provider_verification_failed", webcontract.ErrorAccessDenied, "Telegram login could not be verified.", nil, "")
		return
	}
	verifiedSubject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: claims.Subject}
	candidate, err := handler.config.IDs.NewID(request.Context(), ports.IDUser)
	if err != nil {
		handler.writeLoginError(w, request, "user_id_generation_failed", err, &verifiedSubject, "")
		return
	}
	identity, _, err := handler.config.Store.ResolveOrCreateExternalIdentity(
		request.Context(),
		verifiedSubject,
		domain.UserID(candidate), now,
	)
	if err != nil {
		handler.writeLoginError(w, request, "identity_resolution_failed", err, &verifiedSubject, "")
		return
	}
	memberships, err := handler.activeMemberships(request.Context(), identity.UserID)
	if err != nil {
		handler.writeLoginError(w, request, "membership_lookup_failed", err, &identity.Subject, identity.UserID)
		return
	}
	if len(memberships) == 0 {
		handler.writeLoginFailure(w, request, "membership_missing", webcontract.ErrorAccessDenied, "No Sessionless tenant is linked to this Telegram account.", &identity.Subject, identity.UserID)
		return
	}
	rawSession, rawCSRF, session, err := handler.newSession(identity, memberships[0], now)
	if err != nil {
		handler.writeLoginError(w, request, "session_generation_failed", err, &identity.Subject, identity.UserID)
		return
	}
	if err := handler.config.Store.CreateWebSession(request.Context(), session); err != nil {
		handler.writeLoginError(w, request, "session_persistence_failed", err, &identity.Subject, identity.UserID)
		return
	}
	handler.setSessionCookies(w, rawSession, rawCSRF)
	w.Header().Set("Location", challenge.RedirectPath)
	w.WriteHeader(http.StatusSeeOther)
}

func (handler *Handler) me(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorize(request, domain.TenantPermissionRead)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	identity, err := handler.identityResponse(request.Context(), authorization.Session)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	handler.writeJSON(w, http.StatusOK, identity)
}

func (handler *Handler) tenants(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorize(request, domain.TenantPermissionRead)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	identity, err := handler.identityResponse(request.Context(), authorization.Session)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	handler.writeJSON(w, http.StatusOK, webcontract.Page[webcontract.TenantSummary]{Items: identity.Tenants})
}

func (handler *Handler) switchTenant(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorizeMutation(request, domain.TenantPermissionRead)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	var selection webcontract.SelectTenantRequest
	if err := decodeJSON(request, &selection); err != nil || selection.Validate() != nil {
		handler.writeFailure(w, request, webcontract.ErrorInvalidRequest, "The tenant selection is invalid.")
		return
	}
	memberships, err := handler.activeMemberships(request.Context(), authorization.Session.UserID)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	var selected *domain.TenantMembership
	for index := range memberships {
		if memberships[index].TenantID == selection.TenantID {
			selected = &memberships[index]
			break
		}
	}
	if selected == nil {
		handler.writeFailure(w, request, webcontract.ErrorAccessDenied, "The selected tenant is not available.")
		return
	}
	now := handler.config.Clock.Now().UTC()
	identity := domain.ExternalIdentity{
		Subject: authorization.Session.AuthenticatedSubject, UserID: authorization.Session.UserID,
		CreatedAt: authorization.Session.IssuedAt, UpdatedAt: now,
	}
	rawSession, rawCSRF, next, err := handler.newSession(identity, *selected, now)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	rotated, err := handler.config.Store.SwitchTenant(
		request.Context(), authorization.Session.SessionDigest, next, selection.TenantID, now,
	)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	handler.setSessionCookies(w, rawSession, rawCSRF)
	response, err := handler.identityResponse(request.Context(), rotated.Session)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	handler.writeJSON(w, http.StatusOK, response)
}

func (handler *Handler) logout(w http.ResponseWriter, request *http.Request) {
	authorization, err := handler.authorizeMutation(request, domain.TenantPermissionRead)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	if err := handler.config.Store.RevokeWebSession(
		request.Context(), authorization.Session.SessionDigest, handler.config.Clock.Now().UTC(),
	); err != nil {
		handler.writeError(w, request, err)
		return
	}
	http.SetCookie(w, clearCookie(webcontract.SessionCookieName, true))
	http.SetCookie(w, clearCookie(webcontract.CSRFCookieName, false))
	w.WriteHeader(http.StatusNoContent)
}

func (handler *Handler) authorize(request *http.Request, permission domain.TenantPermission) (ports.WebAuthorization, error) {
	cookie, err := request.Cookie(webcontract.SessionCookieName)
	if err != nil || cookie.Value == "" {
		return ports.WebAuthorization{}, domain.ErrWebSessionRevoked
	}
	return handler.config.Store.AuthorizeWebSession(
		request.Context(), domain.DigestSecret(cookie.Value), permission, handler.config.Clock.Now().UTC(),
	)
}

func (handler *Handler) authorizeMutation(request *http.Request, permission domain.TenantPermission) (ports.WebAuthorization, error) {
	authorization, err := handler.authorize(request, permission)
	if err != nil {
		return authorization, err
	}
	csrfCookie, err := request.Cookie(webcontract.CSRFCookieName)
	presented := request.Header.Get(webcontract.CSRFHeaderName)
	if err != nil || presented == "" || subtle.ConstantTimeCompare([]byte(csrfCookie.Value), []byte(presented)) != 1 {
		return ports.WebAuthorization{}, handler.rejectCSRF(request, authorization, "cookie_header_mismatch")
	}
	if err := webcontract.ValidateMutationSecurity(
		handler.config.BaseURL, request.Header.Get("Origin"), presented,
		authorization.Session.CSRFTokenDigest,
	); err != nil {
		return ports.WebAuthorization{}, handler.rejectCSRF(request, authorization, "origin_or_session_digest_mismatch")
	}
	return authorization, nil
}

func (handler *Handler) rejectCSRF(request *http.Request, authorization ports.WebAuthorization, reason string) error {
	event := handler.securityEvent(request, domain.WebSecurityCSRFRejected, reason,
		&authorization.Session.AuthenticatedSubject, authorization.Session.ActiveTenantID,
		authorization.Session.UserID, authorization.Session.MembershipSecurityVersion)
	if err := handler.config.Store.RecordWebSecurityEvent(request.Context(), event); err != nil {
		return err
	}
	return errCSRF
}

func (handler *Handler) activeMemberships(ctx context.Context, userID domain.UserID) ([]domain.TenantMembership, error) {
	memberships, err := handler.config.Store.ListTenantMemberships(ctx, userID, maxMemberships)
	if err != nil {
		return nil, err
	}
	active := memberships[:0]
	for _, membership := range memberships {
		if membership.Status == domain.TenantMembershipActive {
			active = append(active, membership)
		}
	}
	return active, nil
}

func (handler *Handler) identityResponse(ctx context.Context, session domain.WebSession) (webcontract.Identity, error) {
	memberships, err := handler.activeMemberships(ctx, session.UserID)
	if err != nil {
		return webcontract.Identity{}, err
	}
	tenants := make([]webcontract.TenantSummary, 0, len(memberships))
	for _, membership := range memberships {
		tenants = append(tenants, webcontract.TenantSummary{
			TenantID: membership.TenantID, Role: membership.Role,
			Active: membership.TenantID == session.ActiveTenantID,
		})
	}
	return webcontract.Identity{UserID: session.UserID, Provider: string(session.AuthenticatedSubject.Provider), Tenants: tenants}, nil
}

func (handler *Handler) newSession(
	identity domain.ExternalIdentity,
	membership domain.TenantMembership,
	now time.Time,
) (rawSession string, rawCSRF string, session domain.WebSession, err error) {
	rawSession, err = handler.secret("session_", 32)
	if err != nil {
		return "", "", session, err
	}
	rawCSRF, err = handler.secret("csrf_", 32)
	if err != nil {
		return "", "", session, err
	}
	session = domain.WebSession{
		SessionDigest: domain.DigestSecret(rawSession), CSRFTokenDigest: domain.DigestSecret(rawCSRF),
		UserID: identity.UserID, ActiveTenantID: membership.TenantID,
		AuthenticatedSubject: identity.Subject, MembershipSecurityVersion: membership.SecurityVersion,
		IssuedAt: now, LastSeenAt: now, IdleExpiresAt: now.Add(handler.config.IdleTTL),
		AbsoluteExpiresAt: now.Add(handler.config.AbsoluteTTL),
	}
	return rawSession, rawCSRF, session, session.Validate()
}

func (handler *Handler) setSessionCookies(w http.ResponseWriter, rawSession, rawCSRF string) {
	http.SetCookie(w, webcontract.SessionCookie(rawSession, handler.config.AbsoluteTTL))
	http.SetCookie(w, webcontract.CSRFCookie(rawCSRF, handler.config.AbsoluteTTL))
}

func (handler *Handler) secret(prefix string, bytes int) (string, error) {
	value := make([]byte, bytes)
	if _, err := io.ReadFull(handler.config.Random, value); err != nil {
		return "", errors.New("secure random source failed")
	}
	return prefix + base64.RawURLEncoding.EncodeToString(value), nil
}

func (handler *Handler) writeError(w http.ResponseWriter, request *http.Request, err error) {
	switch {
	case errors.Is(err, errCSRF):
		handler.writeFailure(w, request, webcontract.ErrorCSRFFailed, "The mutation security check failed.")
	case errors.Is(err, domain.ErrWebSessionExpired), errors.Is(err, domain.ErrWebSessionRevoked):
		handler.writeFailure(w, request, webcontract.ErrorUnauthenticated, "Authentication is required.")
	case errors.Is(err, sessionapi.ErrSessionUnavailable):
		handler.writeFailure(w, request, webcontract.ErrorNotFound, "The requested session is not available.")
	case errors.Is(err, webapi.ErrResourceUnavailable):
		handler.writeFailure(w, request, webcontract.ErrorNotFound, "The requested resource is not available.")
	case errors.Is(err, domain.ErrSessionMutationConflict):
		handler.writeFailure(w, request, webcontract.ErrorConflict, "The idempotency key conflicts with an earlier session mutation.")
	case errors.Is(err, webapi.ErrComputeUnavailable):
		handler.writeFailure(w, request, webcontract.ErrorConflict, "Exactly one compute connection must be configured before sending a message.")
	case errors.Is(err, domain.ErrUploadIntentConflict), errors.Is(err, domain.ErrUploadIntentExpired),
		errors.Is(err, domain.ErrUploadIntentCommitted), errors.Is(err, domain.ErrUploadIntentNotCommitted),
		errors.Is(err, domain.ErrUploadIntentClaimed), errors.Is(err, domain.ErrUploadMismatch):
		handler.writeFailure(w, request, webcontract.ErrorConflict, "The upload state conflicts with this request.")
	case errors.Is(err, domain.ErrMembershipDenied), errors.Is(err, domain.ErrMembershipVersionChanged),
		errors.Is(err, domain.ErrExternalIdentityConflict), errors.Is(err, domain.ErrWebSessionRotation):
		handler.writeFailure(w, request, webcontract.ErrorAccessDenied, "The requested operation is not authorized.")
	case errors.Is(err, domain.ErrLoginChallengeConsumed), errors.Is(err, domain.ErrLoginChallengeExpired),
		errors.Is(err, telegramoidc.ErrProviderResponse):
		handler.writeFailure(w, request, webcontract.ErrorAccessDenied, "The login transaction could not be verified.")
	default:
		var staleBinding domain.StaleBindingError
		if errors.As(err, &staleBinding) {
			handler.writeFailure(w, request, webcontract.ErrorConflict, "The frontend binding has changed.")
			return
		}
		var validation domain.ValidationError
		if errors.As(err, &validation) {
			handler.writeFailure(w, request, webcontract.ErrorInvalidRequest, "The request is invalid.")
			return
		}
		handler.writeFailure(w, request, webcontract.ErrorTemporarilyUnavailable, "The service is temporarily unavailable.")
	}
}

func (handler *Handler) writeLoginError(
	w http.ResponseWriter,
	request *http.Request,
	reason string,
	err error,
	subject *domain.ExternalSubject,
	userID domain.UserID,
) {
	if auditErr := handler.recordLoginFailure(request, reason, subject, userID); auditErr != nil {
		handler.writeFailure(w, request, webcontract.ErrorTemporarilyUnavailable, "The service is temporarily unavailable.")
		return
	}
	handler.writeError(w, request, err)
}

func (handler *Handler) writeLoginFailure(
	w http.ResponseWriter,
	request *http.Request,
	reason string,
	code webcontract.ErrorCode,
	message string,
	subject *domain.ExternalSubject,
	userID domain.UserID,
) {
	if err := handler.recordLoginFailure(request, reason, subject, userID); err != nil {
		handler.writeFailure(w, request, webcontract.ErrorTemporarilyUnavailable, "The service is temporarily unavailable.")
		return
	}
	handler.writeFailure(w, request, code, message)
}

func (handler *Handler) recordLoginFailure(
	request *http.Request,
	reason string,
	subject *domain.ExternalSubject,
	userID domain.UserID,
) error {
	event := handler.securityEvent(request, domain.WebSecurityLoginFailed, reason, subject, "", userID, 0)
	return handler.config.Store.RecordWebSecurityEvent(request.Context(), event)
}

func (handler *Handler) securityEvent(
	request *http.Request,
	action domain.WebSecurityAuditAction,
	reason string,
	subject *domain.ExternalSubject,
	tenantID domain.TenantID,
	userID domain.UserID,
	membershipVersion uint64,
) domain.WebSecurityAuditEvent {
	event := domain.WebSecurityAuditEvent{
		RequestID: requestIDFrom(request), Action: action,
		Provider: domain.IdentityProviderTelegram, TenantID: tenantID, UserID: userID,
		MembershipSecurityVersion: membershipVersion, ReasonCode: reason,
		OccurredAt: handler.config.Clock.Now().UTC(),
	}
	if subject != nil {
		event.SubjectFingerprint = domain.DigestSecret(subject.String())
	}
	return event
}

func (handler *Handler) writeFailure(w http.ResponseWriter, request *http.Request, code webcontract.ErrorCode, message string) {
	requestID := w.Header().Get("X-Request-ID")
	handler.writeJSON(w, code.HTTPStatus(), webcontract.ErrorEnvelope{Error: webcontract.Error{
		Code: code, Message: message, RequestID: requestID,
	}})
	_ = request
}

func (handler *Handler) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (handler *Handler) writeJSONConditional(
	w http.ResponseWriter,
	request *http.Request,
	value any,
	pollAfter time.Duration,
) {
	payload, err := json.Marshal(value)
	if err != nil {
		handler.writeFailure(w, request, webcontract.ErrorTemporarilyUnavailable, "The response could not be encoded.")
		return
	}
	digest := sha256.Sum256(payload)
	etag := `"sha256-` + base64.RawURLEncoding.EncodeToString(digest[:]) + `"`
	w.Header().Set("ETag", etag)
	if pollAfter > 0 {
		seconds := int64((pollAfter + time.Second - 1) / time.Second)
		w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
		w.Header().Set("X-Sessionless-Poll-After-Ms", strconv.FormatInt(pollAfter.Milliseconds(), 10))
	}
	if request.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(payload, '\n'))
}

func decodeJSON(request *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxRequestBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON value")
	}
	return nil
}

func sessionSummary(record ports.SessionRecord) webcontract.SessionSummary {
	summary := webcontract.SessionSummary{
		SessionID: record.Session.ID, Status: record.Session.Status,
		Title: record.Display.Title, Preview: record.Display.Preview,
		FrontendOrigin: record.Display.Origin, LastSeq: record.Session.LastEventSequence,
		CreatedAt: record.Session.CreatedAt, UpdatedAt: record.Session.UpdatedAt,
		ArchivedAt: record.Session.ArchivedAt,
	}
	if record.Run != nil {
		run := projectRun(*record.Run, record.Provider)
		summary.CurrentRun = &run
	}
	return summary
}

func projectRun(run domain.Run, provider string) webcontract.Run {
	return webcontract.Run{
		RunID: run.ID, SessionID: run.SessionID, TriggerID: run.TriggerEventID,
		SubscriptionConnectionID: run.SubscriptionConnectionID, Provider: provider,
		Status: run.Status, CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, FinishedAt: run.FinishedAt,
	}
}

func projectEvent(item sessionapi.Event) webcontract.SessionEvent {
	content := webcontract.EventContent{Data: append(json.RawMessage(nil), item.Payload...)}
	switch item.Event.Kind {
	case domain.SessionEventUserMessage:
		var envelope struct {
			Text        string `json:"text"`
			Attachments []struct {
				Name      string         `json:"name"`
				MediaType string         `json:"media_type"`
				Blob      domain.BlobRef `json:"blob"`
			} `json:"attachments"`
		}
		if json.Unmarshal(item.Payload, &envelope) == nil {
			content.Text, content.Data = envelope.Text, nil
			for _, attachment := range envelope.Attachments {
				content.Attachments = append(content.Attachments, webcontract.Attachment{
					Name: attachment.Name, MediaType: attachment.MediaType, Size: attachment.Blob.Size,
				})
			}
		}
	case domain.SessionEventAssistantMessage:
		var envelope struct {
			Summary string `json:"summary"`
		}
		if json.Unmarshal(item.Payload, &envelope) == nil && envelope.Summary != "" {
			content.Text, content.Data = envelope.Summary, nil
		}
	}
	return webcontract.SessionEvent{
		EventID: item.Event.ID, Sequence: item.Event.Sequence, Kind: item.Event.Kind,
		Content: content, CreatedAt: item.Event.CreatedAt,
	}
}

func queryLimit(request *http.Request, fallback uint32) uint32 {
	raw := request.URL.Query().Get("limit")
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(value)
}

func optionalUint64(request *http.Request, name string) (*uint64, error) {
	raw, exists := request.URL.Query()[name]
	if !exists {
		return nil, nil
	}
	if len(raw) != 1 || raw[0] == "" {
		return nil, domain.ValidationError{Field: name, Reason: "must be one unsigned integer"}
	}
	value, err := strconv.ParseUint(raw[0], 10, 64)
	if err != nil {
		return nil, domain.ValidationError{Field: name, Reason: "must be one unsigned integer"}
	}
	return &value, nil
}

func pollAfterRun(run *webcontract.Run) time.Duration {
	if run != nil && !run.Status.Terminal() {
		return 2 * time.Second
	}
	return 0
}

func pollAfterRuns(runs []webcontract.Run) time.Duration {
	for index := range runs {
		if delay := pollAfterRun(&runs[index]); delay > 0 {
			return delay
		}
	}
	return 0
}

func pollAfterSessions(sessions []webcontract.SessionSummary) time.Duration {
	for index := range sessions {
		if delay := pollAfterRun(sessions[index].CurrentRun); delay > 0 {
			return delay
		}
	}
	return 0
}

func loginBindingCookie(value string, ttl time.Duration) *http.Cookie {
	return &http.Cookie{
		Name: LoginBindingCookieName, Value: value, Path: "/", MaxAge: int(ttl.Seconds()),
		Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode,
	}
}

func clearCookie(name string, httpOnly bool) *http.Cookie {
	return &http.Cookie{
		Name: name, Value: "", Path: "/", MaxAge: -1,
		Secure: true, HttpOnly: httpOnly, SameSite: http.SameSiteLaxMode,
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (recorder *statusRecorder) WriteHeader(status int) {
	recorder.status = status
	recorder.ResponseWriter.WriteHeader(status)
}

var errCSRF = errors.New("web mutation CSRF validation failed")

type requestIDKey struct{}

func requestIDFrom(request *http.Request) string {
	requestID, _ := request.Context().Value(requestIDKey{}).(string)
	return requestID
}

var _ http.Handler = (*Handler)(nil)
