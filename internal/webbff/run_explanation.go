package webbff

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

func (handler *Handler) getRunExplanation(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// One deadline, no handler retry and no activity-refreshing preauthorization.
	ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
	defer cancel()
	cookie, err := request.Cookie(webcontract.SessionCookieName)
	if err != nil || cookie.Value == "" {
		handler.writeError(w, request, domain.ErrWebSessionRevoked)
		return
	}
	runID := domain.RunID(request.PathValue("run_id"))
	if request.URL.RawQuery != "" || request.URL.ForceQuery || runID.Validate() != nil {
		handler.writeFailure(w, request, webcontract.ErrorInvalidRequest, "The request is invalid.")
		return
	}
	// No body is accepted, including unknown-length/chunked bodies. Do not
	// read even one byte: a slow body must not outlive the three-second path.
	if request.ContentLength != 0 || request.Body != nil && request.Body != http.NoBody {
		handler.writeFailure(w, request, webcontract.ErrorInvalidRequest, "The request is invalid.")
		return
	}
	outcome, err := handler.config.RunExplanations.ReadRatedRunExplanationV1(ctx,
		domain.DigestSecret(cookie.Value), requestIDFrom(request), runID)
	if err != nil {
		handler.writeError(w, request, err)
		return
	}
	if outcome.Validate() != nil {
		handler.writeError(w, request, ports.ErrRunExplanationUnavailable)
		return
	}
	switch outcome.Kind {
	case ports.RunExplanationReadSuccessV1:
		if outcome.Explanation.RunID != runID {
			handler.writeError(w, request, ports.ErrRunExplanationUnavailable)
			return
		}
		handler.writeBoundedJSON(w, request, outcome.Explanation, runexplanation.MaxResponseBytes)
	case ports.RunExplanationReadNotFoundV1:
		handler.writeFailure(w, request, webcontract.ErrorNotFound, "The requested resource is not available.")
	case ports.RunExplanationReadRateLimitedV1:
		w.Header().Set("Retry-After", strconv.FormatInt(int64(outcome.RetryAfter/time.Second), 10))
		handler.writeFailure(w, request, webcontract.ErrorRateLimited, "The request rate is limited.")
	}
}
