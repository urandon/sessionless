package attachedworkerupload

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeruploadwire"
)

const PathV1 = "/attached-worker/v1/output-upload"

// Handler is unregistered by ordinary serving/profile composition. Ingress
// header/read deadlines remain the owning HTTP server's responsibility. This
// adapter adds no timeout or model/storage authority to the original context.
func Handler(service *Service) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		if request.Body != nil {
			defer request.Body.Close()
		}
		if service == nil || request.URL == nil || request.Body == nil || request.Method != http.MethodPost ||
			request.URL.Path != PathV1 || request.URL.RawPath != "" || request.URL.RawQuery != "" || request.URL.ForceQuery ||
			request.URL.Fragment != "" || request.URL.User != nil || request.URL.Opaque != "" ||
			len(request.Header.Values("Content-Type")) != 1 || request.Header.Get("Content-Type") != "application/json" ||
			len(request.Header.Values("Authorization")) != 1 || len(request.TransferEncoding) != 0 ||
			len(request.Header.Values("Content-Encoding")) != 0 || request.ContentLength == 0 ||
			request.ContentLength < -1 || request.ContentLength > attachedworkeruploadwire.AdmitMaxBytes {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		const prefix = "Bearer "
		authorization := request.Header.Get("Authorization")
		// Reject before byte conversion, trim/clone, body read or binder. The
		// existing connection parser owns the actual bearer grammar/secret.
		if !strings.HasPrefix(authorization, prefix) || len(authorization) <= len(prefix) ||
			len(authorization)-len(prefix) > attachedworkertransport.ConnectionBearerMaxBytes {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		if request.Context().Err() != nil {
			http.Error(writer, "unavailable", http.StatusServiceUnavailable)
			return
		}
		var arena [attachedworkeruploadwire.AdmitMaxBytes + 1]byte
		defer clear(arena[:])
		n, err := io.ReadFull(request.Body, arena[:])
		if n == 0 || n > attachedworkeruploadwire.AdmitMaxBytes || err != io.ErrUnexpectedEOF ||
			request.ContentLength >= 0 && request.ContentLength != int64(n) || request.Context().Err() != nil {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		input, err := attachedworkeruploadwire.DecodeRequest(arena[:n])
		if err != nil {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		bearer := []byte(authorization[len(prefix):])
		defer clear(bearer)
		result, err := service.Execute(request.Context(), bearer, input)
		if err != nil || request.Context().Err() != nil {
			status := http.StatusServiceUnavailable
			if errors.Is(err, ErrUnauthorized) {
				status = http.StatusUnauthorized
			} else if errors.Is(err, ErrInvalid) {
				status = http.StatusBadRequest
			}
			http.Error(writer, http.StatusText(status), status)
			return
		}
		payload, err := attachedworkeruploadwire.EncodeReply(result)
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
