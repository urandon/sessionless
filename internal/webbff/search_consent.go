package webbff

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

const (
	searchConsentTimeout       = 10 * time.Second
	searchConsentResponseBytes = 8 << 10
	searchConsentDecisionBytes = 512
)

func privateConsentInput(r *http.Request, digest domain.SecretDigest) ports.ReadPrivateWebSearchConsentRequestV1 {
	return ports.ReadPrivateWebSearchConsentRequestV1{
		SessionID: domain.SessionID(r.PathValue("session_id")), RunID: domain.RunID(r.PathValue("run_id")),
		AttemptID: domain.AttemptID(r.PathValue("attempt_id")), RequestID: domain.WebSearchConsentRequestIDV1(r.PathValue("request_id")), WebSessionDigest: digest,
	}
}

func consentMatchesInput(state domain.WebSearchConsentStateV1, input ports.ReadPrivateWebSearchConsentRequestV1) bool {
	r := state.Request
	return state.Validate() == nil && r.ID == input.RequestID && r.Scope.SessionID == input.SessionID && r.Scope.RunID == input.RunID && r.Scope.AttemptID == input.AttemptID
}

func consentSummary(state domain.WebSearchConsentStateV1) webcontract.SearchConsentSummaryV1 {
	return webcontract.SearchConsentSummaryV1{RequestID: state.Request.ID, Revision: state.Revision, Status: state.Status, ExpiresAt: state.Request.ExpiresAt}
}

func (h *Handler) getSearchConsent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), searchConsentTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	if ctx.Err() != nil {
		h.writeConsentError(w, r, ctx.Err())
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || r.Body != nil && r.Body != http.NoBody {
		h.writeFailure(w, r, webcontract.ErrorInvalidRequest, "The request is invalid.")
		return
	}
	cookie, err := r.Cookie(webcontract.SessionCookieName)
	if err != nil || cookie.Value == "" {
		h.writeError(w, r, domain.ErrWebSessionRevoked)
		return
	}
	input := privateConsentInput(r, domain.DigestSecret(cookie.Value))
	if input.Validate() != nil {
		h.writeFailure(w, r, webcontract.ErrorInvalidRequest, "The request is invalid.")
		return
	}
	// Do not use authorize here: its production implementation refreshes idle
	// activity. The original Store resolves actor/tenant and rechecks them after
	// immutable body I/O within one unchanged authority budget.
	view, err := h.config.SearchConsents.ReadPrivateWebSearchConsent(ctx, input)
	if ctx.Err() != nil {
		h.writeConsentError(w, r, ctx.Err())
		return
	}
	if err != nil {
		h.writeConsentError(w, r, err)
		return
	}
	digest := sha256.Sum256([]byte(view.Query))
	if !consentMatchesInput(view.State, input) || !utf8.ValidString(view.Query) || len(view.Query) == 0 || len(view.Query) > 1024 || hex.EncodeToString(digest[:]) != view.State.Request.QueryDigest {
		h.writeConsentError(w, r, errors.New("invalid private consent view"))
		return
	}
	h.writeBoundedJSON(w, r, webcontract.SearchConsentViewV1{SearchConsentSummaryV1: consentSummary(view.State), Query: view.Query}, searchConsentResponseBytes)
}

func (h *Handler) decideSearchConsent(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), searchConsentTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	if ctx.Err() != nil {
		h.writeConsentError(w, r, ctx.Err())
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery {
		h.writeFailure(w, r, webcontract.ErrorInvalidRequest, "The request is invalid.")
		return
	}
	authorization, err := h.authorizeMutation(r, domain.TenantPermissionWrite)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	input := privateConsentInput(r, authorization.Session.SessionDigest)
	if input.Validate() != nil {
		h.writeFailure(w, r, webcontract.ErrorInvalidRequest, "The request is invalid.")
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		h.writeFailure(w, r, webcontract.ErrorInvalidRequest, "The request is invalid.")
		return
	}
	// Real net/http ingress supports read deadlines through the recorder's
	// Unwrap method. No goroutine is started to race an arbitrary borrowed
	// reader. Unsupported wrappers retain the Web server's 15-second read bound;
	// the context below still prevents a late body from causing a decision.
	deadline, _ := ctx.Deadline()
	if err := http.NewResponseController(w).SetReadDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		h.writeConsentError(w, r, err)
		return
	}
	decision, err := decodeSearchConsentDecision(r)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	if ctx.Err() != nil {
		h.writeConsentError(w, r, ctx.Err())
		return
	}
	locator := ports.WebSearchConsentLocatorV1{TenantID: authorization.Session.ActiveTenantID, SessionID: input.SessionID, RunID: input.RunID, AttemptID: input.AttemptID, RequestID: input.RequestID}
	state, err := h.config.SearchConsents.DecideWebSearchConsent(ctx, ports.DecideWebSearchConsentRequestV1{
		Locator: locator, WebSessionDigest: input.WebSessionDigest, CSRFTokenDigest: domain.DigestSecret(r.Header.Get(webcontract.CSRFHeaderName)), ExpectedRevision: decision.ExpectedRevision, Choice: decision.Choice,
	})
	if ctx.Err() != nil {
		h.writeConsentError(w, r, ctx.Err())
		return
	}
	if err != nil {
		h.writeConsentError(w, r, err)
		return
	}
	if !consentMatchesInput(state, input) || state.Request.Scope.TenantID != locator.TenantID || state.Request.InitiatingUserID != authorization.Session.UserID || state.Decision == nil || state.Decision.Choice != decision.Choice || state.Decision.WebSessionDigest != input.WebSessionDigest || state.Revision != 2 {
		h.writeConsentError(w, r, errors.New("invalid immutable consent decision"))
		return
	}
	// Never reflect a plaintext query or the stored receipt back from POST.
	h.writeBoundedJSON(w, r, consentSummary(state), searchConsentResponseBytes)
}

func decodeSearchConsentDecision(r *http.Request) (webcontract.DecideSearchConsentV1, error) {
	zero := webcontract.DecideSearchConsentV1{}
	invalid := domain.ValidationError{Field: "search_consent.body", Reason: "requires only expected_revision and choice"}
	if r.Body == nil {
		return zero, invalid
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, searchConsentDecisionBytes+1))
	if err != nil {
		return zero, invalid
	}
	defer clear(body)
	if len(body) > searchConsentDecisionBytes {
		return zero, errRequestTooLarge
	}
	if !utf8.Valid(body) {
		return zero, invalid
	}
	d := json.NewDecoder(bytes.NewReader(body))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return zero, invalid
	}
	seen := map[string]bool{}
	var value webcontract.DecideSearchConsentV1
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return zero, invalid
		}
		seen[key] = true
		switch key {
		case "expected_revision":
			err = d.Decode(&value.ExpectedRevision)
		case "choice":
			err = d.Decode(&value.Choice)
		default:
			return zero, invalid
		}
		if err != nil {
			return zero, invalid
		}
	}
	token, err = d.Token()
	if err != nil || token != json.Delim('}') || len(seen) != 2 || value.Validate() != nil {
		return zero, invalid
	}
	var extra json.RawMessage
	if !errors.Is(d.Decode(&extra), io.EOF) {
		return zero, invalid
	}
	return value, nil
}

func (h *Handler) writeConsentError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrWebSearchConsentConflict), errors.Is(err, domain.ErrWebSearchConsentStopped):
		h.writeFailure(w, r, webcontract.ErrorConflict, "The consent decision conflicts with current state.")
	case errors.Is(err, domain.ErrWebSearchConsentAuthority):
		h.writeFailure(w, r, webcontract.ErrorAccessDenied, "The requested operation is not authorized.")
	default:
		h.writeError(w, r, err)
	}
}
