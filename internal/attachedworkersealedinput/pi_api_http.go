package attachedworkersealedinput

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
)

// PiAPIUseHandler is the fixed, read-only HTTP arm of the original attached
// connection. It delegates to the real transport bearer gate, not a caller
// receipt/Boolean or a second Session. The store still MUST implement the
// genuine same-transaction Source/owner decision predicate; matching wire
// metadata never supplies that authority. No ordinary route registers this
// default-off handler or enables a Pi profile.
//
// The owning HTTP server retains its ingress header/read/deadline bounds. This
// adapter bounds body allocation and preserves the original request context;
// it does not manufacture another use timeout or renew a phase permission.
func PiAPIUseHandler(service *attachedworkertransport.Service) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		if request.Body != nil {
			defer request.Body.Close()
		}
		if service == nil || request.URL == nil || request.Body == nil ||
			request.Method != http.MethodPost || request.URL.Path != PiAPIUsePathV1 ||
			request.URL.RawPath != "" || request.URL.RawQuery != "" || request.URL.ForceQuery ||
			request.URL.Fragment != "" || request.URL.Opaque != "" || request.URL.User != nil ||
			len(request.Header.Values("Content-Type")) != 1 || request.Header.Get("Content-Type") != "application/json" ||
			len(request.Header.Values("Authorization")) != 1 || len(request.TransferEncoding) != 0 ||
			len(request.Header.Values("Content-Encoding")) != 0 ||
			request.ContentLength == 0 || request.ContentLength > pisdkphasewire.RequestMaxBytes || request.ContentLength < -1 {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		const prefix = "Bearer "
		authorization := request.Header.Get("Authorization")
		if !strings.HasPrefix(authorization, prefix) || len(authorization) <= len(prefix) ||
			len(authorization)-len(prefix) > attachedworkertransport.ConnectionBearerMaxBytes {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		if request.Context().Err() != nil {
			http.Error(writer, "unavailable", http.StatusServiceUnavailable)
			return
		}
		// One fixed arena and one overflow byte, no growing ReadAll/decoder.
		var arena [pisdkphasewire.RequestMaxBytes + 1]byte
		defer clear(arena[:])
		n, err := io.ReadFull(request.Body, arena[:])
		if n == 0 || n > pisdkphasewire.RequestMaxBytes || err != io.ErrUnexpectedEOF ||
			request.ContentLength >= 0 && request.ContentLength != int64(n) || request.Context().Err() != nil {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		phase, err := pisdkphasewire.DecodeRequest(arena[:n])
		if err != nil {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		bearer := []byte(authorization[len(prefix):])
		defer clear(bearer)
		reply, err := service.AuthorizePiAPIUseBearer(request.Context(), bearer, phase)
		if err != nil || request.Context().Err() != nil {
			status := http.StatusServiceUnavailable
			if errors.Is(err, attachedworkertransport.ErrTransportUnauthorized) {
				status = http.StatusUnauthorized
			}
			http.Error(writer, http.StatusText(status), status)
			return
		}
		payload, err := pisdkphasewire.EncodeReply(reply)
		if err != nil {
			http.Error(writer, "unavailable", http.StatusServiceUnavailable)
			return
		}
		defer clear(payload)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(payload)
	})
}
