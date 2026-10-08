package attachedworkerreceipt

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const PathV1 = "/attached-worker/v1/output-receipt"

const maxRequestBytes = 128 << 10
const maxResponseBytes = 512 << 10

func Handler(service *Service) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		if service == nil || request.Method != http.MethodPost || request.URL.Path != PathV1 ||
			request.URL.RawQuery != "" || request.Header.Get("Content-Type") != "application/json" ||
			len(request.Header.Values("Authorization")) != 1 {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		bearer := request.Header.Get("Authorization")
		if !strings.HasPrefix(bearer, "Bearer ") || len(bearer) <= len("Bearer ") {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		var input ydbstore.AttachedWorkerOutputReceiptRequest
		if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		result, err := service.Create(request.Context(), []byte(strings.TrimPrefix(bearer, "Bearer ")), input)
		if err != nil {
			status := http.StatusServiceUnavailable
			if errors.Is(err, ErrUnauthorized) {
				status = http.StatusUnauthorized
			} else if errors.Is(err, ErrConflict) {
				status = http.StatusConflict
			}
			http.Error(writer, http.StatusText(status), status)
			return
		}
		payload, err := json.Marshal(result)
		if err != nil || len(payload) > maxResponseBytes {
			http.Error(writer, "unavailable", http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(payload)
	})
}
