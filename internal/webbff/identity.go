package webbff

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

const identityAccountTimeout = 15 * time.Second
const identityAccountBodyBytes = 2 << 10
const identityOperationCookieName = "__Host-sessionless-identity-operation"

// These routes deliberately do not call authorize/authorizeMutation: those
// methods refresh session activity and cannot grant authority to a later SQL
// transaction. The joined service owns current-session and mutation checks.
func identityAccountCookie(r *http.Request, name string, required bool) (string, error) {
	value, count := "", 0
	for _, cookie := range r.Cookies() {
		if cookie.Name == name {
			value, count = cookie.Value, count+1
		}
	}
	if count > 1 || count == 1 && (value == "" || len(value) > 512) {
		return "", ports.IdentityAccountAccessDeniedV2
	}
	if count == 0 && required {
		return "", ports.IdentityAccountUnauthenticatedV2
	}
	return value, nil
}

func identityAccountRead(r *http.Request) (ports.IdentityAccountReadAuthV2, error) {
	value, err := identityAccountCookie(r, webcontract.SessionCookieName, true)
	return ports.IdentityAccountReadAuthV2{SessionCookie: value}, err
}

func (h *Handler) identityAccountMutation(r *http.Request) (ports.IdentityAccountMutationAuthV2, error) {
	var empty ports.IdentityAccountMutationAuthV2
	auth, err := identityAccountRead(r)
	if err != nil {
		return empty, err
	}
	csrf, err := identityAccountCookie(r, webcontract.CSRFCookieName, true)
	origins, headers := r.Header.Values("Origin"), r.Header.Values(webcontract.CSRFHeaderName)
	if err != nil || len(origins) != 1 || origins[0] != h.config.BaseURL || len(headers) != 1 || headers[0] == "" || len(headers[0]) > 512 || subtle.ConstantTimeCompare([]byte(csrf), []byte(headers[0])) != 1 {
		return empty, errCSRF
	}
	return ports.IdentityAccountMutationAuthV2{SessionCookie: auth.SessionCookie, CSRFCookie: csrf, PresentedCSRF: headers[0]}, nil
}

func identityAccountNoInput(r *http.Request) bool {
	return r.URL.RawQuery == "" && !r.URL.ForceQuery && r.ContentLength == 0 && (r.Body == nil || r.Body == http.NoBody)
}

func (h *Handler) accountLoginMethods(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), identityAccountTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	if !identityAccountNoInput(r) {
		h.writeFailure(w, r, webcontract.ErrorInvalidRequest, "The request is invalid.")
		return
	}
	auth, err := identityAccountRead(r)
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return
	}
	result, err := h.config.IdentityAccounts.Methods(ctx, ports.IdentityAccountMethodsRequestV2{Auth: auth})
	if err != nil || ctx.Err() != nil {
		h.writeIdentityAccountError(w, r, identityAccountCallError(err, ctx))
		return
	}
	if result.Revision.Validate() != nil || len(result.Methods) != 2 {
		h.writeIdentityAccountError(w, r, ports.IdentityAccountUnavailableV2)
		return
	}
	view := webcontract.AccountLoginMethods{Revision: result.Revision, Methods: make([]webcontract.AccountLoginMethod, 0, 2)}
	seen := map[domain.IdentityProvider]bool{}
	for _, method := range result.Methods {
		if !identityAccountKnownProvider(method.Provider) || seen[method.Provider] || method.Current && !method.Linked || method.Linked && domain.ValidateOpaqueID("identity.method_handle", method.MethodHandle) != nil || !method.Linked && method.MethodHandle != "" {
			h.writeIdentityAccountError(w, r, ports.IdentityAccountUnavailableV2)
			return
		}
		seen[method.Provider] = true
		actions := make([]string, 0, len(method.AllowedActions))
		for _, action := range method.AllowedActions {
			if action != ports.IdentityAccountActionLinkV2 && action != ports.IdentityAccountActionUnlinkV2 || len(actions) != 0 || action == ports.IdentityAccountActionLinkV2 && (!method.Enabled || method.Linked) || action == ports.IdentityAccountActionUnlinkV2 && (!method.Linked || !identityAccountKnownProvider(method.RemainingProvider) || method.RemainingProvider == method.Provider) {
				h.writeIdentityAccountError(w, r, ports.IdentityAccountUnavailableV2)
				return
			}
			actions = append(actions, string(action))
		}
		if method.RemainingProvider != "" && (len(actions) != 1 || actions[0] != string(ports.IdentityAccountActionUnlinkV2)) {
			h.writeIdentityAccountError(w, r, ports.IdentityAccountUnavailableV2)
			return
		}
		view.Methods = append(view.Methods, webcontract.AccountLoginMethod{Provider: method.Provider, MethodHandle: method.MethodHandle, Linked: method.Linked, Current: method.Current, Enabled: method.Enabled, AllowedActions: actions, RemainingProvider: method.RemainingProvider})
	}
	h.writeBoundedJSON(w, r, view, 8<<10)
}

func (h *Handler) startIdentityOperation(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), identityAccountTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	auth, err := h.identityAccountMutation(r)
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return
	}
	var input webcontract.IdentityOperationStartRequest
	if err := decodeIdentityAccountBody(w, r, &input); err != nil {
		h.writeIdentityAccountError(w, r, err)
		return
	}
	if err := input.Validate(); err != nil {
		h.writeIdentityAccountError(w, r, err)
		return
	}
	previous, err := identityAccountCookie(r, identityOperationCookieName, false)
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return
	}
	result, err := h.config.IdentityAccounts.Start(ctx, ports.IdentityAccountStartRequestV2{Auth: auth, Kind: input.Kind, TargetProvider: input.TargetProvider, MethodHandle: input.MethodHandle, ExpectedRevision: input.ExpectedRevision, PreviousOperationCookie: previous})
	if err != nil || ctx.Err() != nil {
		h.writeIdentityAccountError(w, r, identityAccountCallError(err, ctx))
		return
	}
	view, err := projectIdentityOperation(result.View, "")
	if err != nil || !strings.HasPrefix(result.OperationCookie, view.OperationID+".") || len(result.OperationCookie) > 512 || !validIdentityCookieValue(result.OperationCookie) {
		h.writeIdentityAccountError(w, r, ports.IdentityAccountUnavailableV2)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: identityOperationCookieName, Value: result.OperationCookie, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: int(defaultChallengeTTL.Seconds())})
	h.writeBoundedJSON(w, r, view, 4<<10)
}

func (h *Handler) identityOperationOutcome(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), identityAccountTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	if !identityAccountNoInput(r) || domain.ValidateOpaqueID("identity.operation_id", r.PathValue("operation_id")) != nil {
		h.writeFailure(w, r, webcontract.ErrorInvalidRequest, "The request is invalid.")
		return
	}
	auth, err := identityAccountRead(r)
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return
	}
	// An invalid optional browser cookie removes hints, never denies same-user
	// receipt access after a legitimate relogin.
	cookie, cookieErr := identityAccountCookie(r, identityOperationCookieName, false)
	if cookieErr != nil {
		cookie = ""
	}
	result, err := h.config.IdentityAccounts.Outcome(ctx, ports.IdentityAccountOutcomeRequestV2{Auth: auth, OperationID: r.PathValue("operation_id"), OperationCookie: cookie})
	if err != nil || ctx.Err() != nil {
		h.writeIdentityAccountError(w, r, identityAccountCallError(err, ctx))
		return
	}
	view, err := projectIdentityOperation(result, r.PathValue("operation_id"))
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return
	}
	status := http.StatusOK
	if view.Outcome == domain.IdentityOutcomeUnavailableV2 {
		status = http.StatusGone
	}
	h.writeJSON(w, status, view)
}

func (h *Handler) identityOperationMutationInput(w http.ResponseWriter, r *http.Request) (ports.IdentityAccountMutationAuthV2, string, domain.AuthRevisionV2, error) {
	auth, err := h.identityAccountMutation(r)
	if err != nil {
		return auth, "", 0, err
	}
	if domain.ValidateOpaqueID("identity.operation_id", r.PathValue("operation_id")) != nil {
		return auth, "", 0, domain.ValidationError{Field: "identity.operation_id", Reason: "is invalid"}
	}
	var input webcontract.IdentityOperationRevisionRequest
	if err := decodeIdentityAccountBody(w, r, &input); err != nil {
		return auth, "", 0, err
	}
	if err := input.Validate(); err != nil {
		return auth, "", 0, err
	}
	cookie, err := identityAccountCookie(r, identityOperationCookieName, true)
	return auth, cookie, input.ExpectedRevision, err
}

func (h *Handler) continueIdentityOperation(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), identityAccountTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	auth, cookie, revision, err := h.identityOperationMutationInput(w, r)
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return
	}
	result, err := h.config.IdentityAccounts.Continue(ctx, ports.IdentityAccountContinueRequestV2{Auth: auth, OperationID: r.PathValue("operation_id"), OperationCookie: cookie, ExpectedRevision: revision})
	if err != nil || ctx.Err() != nil {
		h.writeIdentityAccountError(w, r, identityAccountCallError(err, ctx))
		return
	}
	u, err := url.Parse(result.AuthorizationURL)
	if err != nil || len(result.AuthorizationURL) > 8192 || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		h.writeIdentityAccountError(w, r, ports.IdentityAccountUnavailableV2)
		return
	}
	h.writeBoundedJSON(w, r, webcontract.IdentityOperationContinuation{AuthorizationURL: result.AuthorizationURL}, 12<<10)
}

func (h *Handler) confirmIdentityOperation(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), identityAccountTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	auth, cookie, revision, err := h.identityOperationMutationInput(w, r)
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return
	}
	result, err := h.config.IdentityAccounts.Confirm(ctx, ports.IdentityAccountConfirmRequestV2{Auth: auth, OperationID: r.PathValue("operation_id"), OperationCookie: cookie, ExpectedRevision: revision})
	if err != nil || ctx.Err() != nil {
		h.writeIdentityAccountError(w, r, identityAccountCallError(err, ctx))
		return
	}
	view, err := projectIdentityOperation(result.View, r.PathValue("operation_id"))
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return
	}
	if result.Cookies != nil {
		if !validIdentityCookieValue(result.Cookies.SessionCookie) || !validIdentityCookieValue(result.Cookies.CSRFCookie) || view.Outcome != domain.IdentityOutcomeLinkedV2 && view.Outcome != domain.IdentityOutcomeUnlinkedV2 {
			h.writeIdentityAccountError(w, r, ports.IdentityAccountUnavailableV2)
			return
		}
		h.setSessionCookies(w, result.Cookies.SessionCookie, result.Cookies.CSRFCookie)
	}
	http.SetCookie(w, clearCookie(identityOperationCookieName, true))
	h.writeBoundedJSON(w, r, view, 4<<10)
}

func (h *Handler) cancelIdentityOperation(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), identityAccountTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	auth, cookie, revision, err := h.identityOperationMutationInput(w, r)
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return
	}
	result, err := h.config.IdentityAccounts.Cancel(ctx, ports.IdentityAccountCancelRequestV2{Auth: auth, OperationID: r.PathValue("operation_id"), OperationCookie: cookie, ExpectedRevision: revision})
	if err != nil || ctx.Err() != nil {
		h.writeIdentityAccountError(w, r, identityAccountCallError(err, ctx))
		return
	}
	view, err := projectIdentityOperation(result, r.PathValue("operation_id"))
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return
	}
	http.SetCookie(w, clearCookie(identityOperationCookieName, true))
	h.writeBoundedJSON(w, r, view, 4<<10)
}

// Return false ONLY for the positive stored LOGIN purpose. No parse/lookup/
// proof failure can enter the ordinary resolver or grant enrollment.
func (h *Handler) dispatchIdentityCallback(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.ForceQuery || len(r.URL.RawQuery) > maxRequestBytes {
		h.writeIdentityAccountError(w, r, ports.IdentityAccountAccessDeniedV2)
		return true
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		h.writeIdentityAccountError(w, r, ports.IdentityAccountAccessDeniedV2)
		return true
	}
	for key, values := range query {
		if len(values) != 1 || key != "state" && key != "code" && key != "error" && key != "error_description" {
			h.writeIdentityAccountError(w, r, ports.IdentityAccountAccessDeniedV2)
			return true
		}
	}
	callback := webcontract.OIDCCallback{State: query.Get("state"), Code: query.Get("code"), Error: query.Get("error"), ErrorDescription: query.Get("error_description")}
	if callback.Validate() != nil {
		h.writeIdentityAccountError(w, r, ports.IdentityAccountAccessDeniedV2)
		return true
	}
	purpose, err := h.config.IdentityAccounts.CallbackPurpose(r.Context(), ports.IdentityAccountCallbackPurposeRequestV2{CallbackPath: r.URL.Path, State: callback.State})
	if err != nil || r.Context().Err() != nil {
		h.writeIdentityAccountError(w, r, identityAccountCallError(err, r.Context()))
		return true
	}
	if purpose == ports.IdentityAccountCallbackLoginV2 {
		return false
	}
	if purpose != ports.IdentityAccountCallbackProofV2 {
		h.writeIdentityAccountError(w, r, ports.IdentityAccountUnavailableV2)
		return true
	}
	auth, err := identityAccountRead(r)
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return true
	}
	cookie, err := identityAccountCookie(r, identityOperationCookieName, true)
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return true
	}
	result, err := h.config.IdentityAccounts.Callback(r.Context(), ports.IdentityAccountCallbackRequestV2{Auth: ports.IdentityAccountCallbackAuthV2{SessionCookie: auth.SessionCookie, OperationCookie: cookie}, CallbackPath: r.URL.Path, State: callback.State, Code: callback.Code, ProviderDenied: callback.Error != ""})
	if err != nil || r.Context().Err() != nil {
		h.writeIdentityAccountError(w, r, identityAccountCallError(err, r.Context()))
		return true
	}
	view, err := projectIdentityOperation(result, "")
	if err != nil {
		h.writeIdentityAccountError(w, r, err)
		return true
	}
	// No Strict CSRF cookie is required on this cross-provider top-level GET.
	// The private consume owner checks its digest from the CURRENT original
	// session; no login binding cookie is read, cleared or repurposed here.
	w.Header().Set("Location", webcontract.RouteAccountLoginMethodsPage+"?"+url.Values{"operation_id": {view.OperationID}}.Encode())
	w.WriteHeader(http.StatusSeeOther)
	return true
}

func decodeIdentityAccountBody(w http.ResponseWriter, r *http.Request, target any) error {
	invalid := domain.ValidationError{Field: "identity.body", Reason: "requires one closed JSON object"}
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(params) > 1 || len(params) == 1 && !strings.EqualFold(params["charset"], "utf-8") || r.URL.RawQuery != "" || r.URL.ForceQuery || r.Body == nil {
		return invalid
	}
	deadline, _ := r.Context().Deadline()
	if err := http.NewResponseController(w).SetReadDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return ports.IdentityAccountUnavailableV2
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, identityAccountBodyBytes+1))
	defer clear(body)
	if err != nil {
		return invalid
	}
	if len(body) > identityAccountBodyBytes {
		return errRequestTooLarge
	}
	if !utf8.Valid(body) {
		return invalid
	}
	// This request schema is flat and string-only. Reject duplicate keys before
	// decoding: last-value-wins is not an identity mutation contract.
	d := json.NewDecoder(bytes.NewReader(body))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return invalid
	}
	seen := map[string]bool{}
	fields := map[string]string{}
	allowed := map[string]bool{"expected_revision": true}
	_, start := target.(*webcontract.IdentityOperationStartRequest)
	if start {
		allowed["kind"], allowed["target_provider"], allowed["method_handle"] = true, true, true
	}
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] || !allowed[key] {
			return invalid
		}
		seen[key] = true
		value, err := d.Token()
		text, ok := value.(string)
		if err != nil || !ok {
			return invalid
		}
		fields[key] = text
	}
	if start {
		switch fields["kind"] {
		case "link":
			if !seen["target_provider"] || seen["method_handle"] {
				return invalid
			}
		case "unlink":
			if !seen["method_handle"] || seen["target_provider"] {
				return invalid
			}
		default:
			return invalid
		}
	}
	if token, err := d.Token(); err != nil || token != json.Delim('}') {
		return invalid
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return invalid
	}
	d = json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil || r.Context().Err() != nil {
		return invalid
	}
	return nil
}

func identityAccountKnownProvider(provider domain.IdentityProvider) bool {
	return provider == domain.IdentityProviderYandex || provider == domain.IdentityProviderTelegram
}

func validIdentityCookieValue(value string) bool {
	if value == "" || len(value) > 512 {
		return false
	}
	for _, char := range []byte(value) {
		if char < 0x21 || char > 0x7e || char == '"' || char == ',' || char == ';' || char == '\\' {
			return false
		}
	}
	return true
}

func projectIdentityOperation(source ports.IdentityAccountOperationViewV2, expectedID string) (webcontract.IdentityOperationView, error) {
	view := webcontract.IdentityOperationView{OperationID: source.OperationID, Kind: source.Kind, Phase: source.Phase, TargetProvider: source.TargetProvider, Outcome: source.Outcome, AllowedActions: make([]string, 0, len(source.AllowedActions))}
	if domain.ValidateOpaqueID("identity.operation_id", source.OperationID) != nil || expectedID != "" && expectedID != source.OperationID {
		return view, ports.IdentityAccountUnavailableV2
	}
	if source.Outcome == domain.IdentityOutcomeUnavailableV2 {
		if source.Kind != "" || source.Phase != "" || source.TargetProvider != "" || !source.ExpiresAt.IsZero() || source.BeforeRevision != 0 || source.AfterRevision != nil || source.CompletedAt != nil || len(source.AllowedActions) != 0 {
			return view, ports.IdentityAccountUnavailableV2
		}
		return view, nil
	}
	if source.Kind != domain.IdentityOperationLinkV2 && source.Kind != domain.IdentityOperationUnlinkV2 || source.ExpiresAt.IsZero() || source.BeforeRevision.Validate() != nil {
		return view, ports.IdentityAccountUnavailableV2
	}
	switch source.Phase {
	case domain.IdentityPhaseSourceProofV2, domain.IdentityPhaseTargetProofV2, domain.IdentityPhaseConfirmV2:
		if !identityAccountKnownProvider(source.TargetProvider) || source.CompletedAt != nil || source.AfterRevision != nil || source.Outcome != "" && source.Outcome != domain.IdentityOutcomeExpiredV2 {
			return view, ports.IdentityAccountUnavailableV2
		}
	case domain.IdentityPhaseTerminalV2:
		if source.CompletedAt == nil || source.CompletedAt.IsZero() || len(source.AllowedActions) != 0 || source.TargetProvider != "" {
			return view, ports.IdentityAccountUnavailableV2
		}
		switch source.Outcome {
		case domain.IdentityOutcomeLinkedV2, domain.IdentityOutcomeUnlinkedV2, domain.IdentityOutcomeCancelledV2, domain.IdentityOutcomeConflictV2, domain.IdentityOutcomeStaleRevisionV2, domain.IdentityOutcomeProofRequiredV2, domain.IdentityOutcomeProofUnavailableV2, domain.IdentityOutcomeLastMethodV2, domain.IdentityOutcomeProviderUnavailableV2, domain.IdentityOutcomeExpiredV2, domain.IdentityOutcomeAccessDeniedV2:
		default:
			return view, ports.IdentityAccountUnavailableV2
		}
	default:
		return view, ports.IdentityAccountUnavailableV2
	}
	revision := source.BeforeRevision
	view.BeforeRevision = &revision
	view.ExpiresAt = source.ExpiresAt.UTC().Format(time.RFC3339Nano)
	if source.AfterRevision != nil {
		value := *source.AfterRevision
		if value.Validate() != nil {
			return view, ports.IdentityAccountUnavailableV2
		}
		view.AfterRevision = &value
	}
	if source.CompletedAt != nil {
		view.CompletedAt = source.CompletedAt.UTC().Format(time.RFC3339Nano)
	}
	seen := map[ports.IdentityAccountActionV2]bool{}
	for _, action := range source.AllowedActions {
		if seen[action] || action != ports.IdentityAccountActionContinueV2 && action != ports.IdentityAccountActionConfirmV2 && action != ports.IdentityAccountActionCancelV2 {
			return view, ports.IdentityAccountUnavailableV2
		}
		seen[action] = true
		view.AllowedActions = append(view.AllowedActions, string(action))
	}
	return view, nil
}

func identityAccountCallError(err error, ctx context.Context) error {
	if ctx.Err() != nil {
		return ports.IdentityAccountUnavailableV2
	}
	return err
}

func (h *Handler) writeIdentityAccountError(w http.ResponseWriter, r *http.Request, err error) {
	code := webcontract.ErrorTemporarilyUnavailable
	var failure ports.IdentityAccountErrorV2
	var invalid domain.ValidationError
	switch {
	case errors.Is(err, errCSRF):
		code = webcontract.ErrorCSRFFailed
	case errors.Is(err, errRequestTooLarge):
		code = webcontract.ErrorPayloadTooLarge
	case errors.As(err, &invalid):
		code = webcontract.ErrorInvalidRequest
	case errors.As(err, &failure):
		switch failure {
		case ports.IdentityAccountUnauthenticatedV2:
			code = webcontract.ErrorUnauthenticated
		case ports.IdentityAccountAccessDeniedV2:
			code = webcontract.ErrorAccessDenied
		case ports.IdentityAccountConflictV2:
			code = webcontract.ErrorConflict
		case ports.IdentityAccountStaleRevisionV2:
			code = webcontract.ErrorStaleRevision
		case ports.IdentityAccountProofRequiredV2:
			code = webcontract.ErrorProofRequired
		case ports.IdentityAccountProofUnavailableV2:
			code = webcontract.ErrorProofUnavailable
		case ports.IdentityAccountLastMethodV2:
			code = webcontract.ErrorLastMethod
		case ports.IdentityAccountProviderUnavailableV2:
			code = webcontract.ErrorProviderUnavailable
		case ports.IdentityAccountExpiredV2:
			code = webcontract.ErrorExpired
		}
	}
	h.writeFailure(w, r, code, "The account operation could not be completed.")
}
