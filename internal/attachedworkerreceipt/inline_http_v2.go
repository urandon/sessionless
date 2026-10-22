package attachedworkerreceipt

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/strictjson"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const (
	PathInlinePrepareV2     = "/attached-worker/v2/output/prepare"
	PathInlineBodyV2        = "/attached-worker/v2/output/body"
	PathInlineReconcileV2   = "/attached-worker/v2/output/reconcile"
	InlineBodyMediaTypeV2   = "application/vnd.sessionless.attached-output.v2"
	InlineRequestHeaderV2   = "X-Sessionless-Output-Request"
	maxInlinePrepareBytesV2 = 256 << 10
	maxInlineControlBytesV2 = 32 << 10
	maxInlineHeaderBytesV2  = 8 << 10
	maxInlineBodyBytesV2    = 64 << 10
)

// HandlerInlineV2 is an explicit V2-only boundary, with no redirect or V1
// fallback. Its header check occurs after net/http parsing; activation also
// requires the outer server/ingress header bound, not an 8 KiB allocation claim.
func HandlerInlineV2(service *InlineServiceV2) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// One transport failure window starts at handler entry. It is not a
		// replacement for the store's immutable expiry and permission clock.
		deadline := time.Now().Add(15 * time.Second)
		if parent, exists := r.Context().Deadline(); exists && parent.Before(deadline) {
			deadline = parent
		}
		controller := http.NewResponseController(w)
		if controller.SetReadDeadline(deadline) != nil || controller.SetWriteDeadline(deadline) != nil {
			// There is no safe error response without both physical controls:
			// net/http may drain unread request bytes while flushing headers.
			// net/http aborts this stream/connection without a fallback write.
			panic(http.ErrAbortHandler)
		}
		defer func() {
			if failure := recover(); failure != nil {
				panic(failure)
			}
			// Flush bounded content, but retain both physical deadlines after
			// return: net/http still owns the final response tail (including
			// HTTP/2 END_STREAM). The pinned server owns connection deadline
			// resets and stream-timer retirement, not this handler. Middleware
			// must preserve deadline and flush support through that tail.
			if controller.Flush() != nil {
				panic(http.ErrAbortHandler)
			}
		}()
		ctx, cancel := context.WithDeadline(r.Context(), deadline)
		defer cancel()
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if service == nil || r.URL == nil || r.Method != http.MethodPost || r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" ||
			(r.URL.Path != PathInlinePrepareV2 && r.URL.Path != PathInlineBodyV2 && r.URL.Path != PathInlineReconcileV2) || !inlineHeadersV2(r) || len(r.TransferEncoding) != 0 || len(r.Trailer) != 0 {
			inlineHTTPErrorV2(w, ErrInvalid)
			return
		}
		auth, ok := inlineOneHeaderV2(r.Header, "Authorization")
		if !ok || !strings.HasPrefix(auth, "Bearer ") || len(auth) <= len("Bearer ") || strings.ContainsAny(strings.TrimPrefix(auth, "Bearer "), " \t\r\n,") {
			inlineHTTPErrorV2(w, ErrUnauthorized)
			return
		}
		bearer := []byte(strings.TrimPrefix(auth, "Bearer "))
		defer clear(bearer)
		media, ok := inlineOneHeaderV2(r.Header, "Content-Type")
		if !ok || r.Body == nil {
			inlineHTTPErrorV2(w, ErrInvalid)
			return
		}
		var result ydbstore.AttachedWorkerInlineResultV2
		var err error
		if r.URL.Path == PathInlineBodyV2 {
			metadata, single := inlineOneHeaderV2(r.Header, InlineRequestHeaderV2)
			if media != InlineBodyMediaTypeV2 || !single || r.ContentLength <= 0 || r.ContentLength > maxInlineBodyBytesV2 || !inlineContentLengthV2(r) {
				inlineHTTPErrorV2(w, ErrInvalid)
				return
			}
			raw, decodeErr := base64.RawStdEncoding.Strict().DecodeString(metadata)
			defer clear(raw)
			var input ydbstore.AttachedWorkerInlineBodyV2
			if decodeErr != nil || base64.RawStdEncoding.EncodeToString(raw) != metadata || !inlineDecodeV2(raw, maxInlineControlBytesV2, &input) {
				inlineHTTPErrorV2(w, ErrInvalid)
				return
			}
			// This is a transport failure bound, not permission time. The store
			// must compare ExpiresAt with its immutable plan and database clock.
			if input.ExpiresAt.Before(deadline) {
				deadline = input.ExpiresAt
			}
			if input.ExpiresAt.IsZero() {
				inlineHTTPErrorV2(w, ErrInvalid)
				return
			}
			if controller.SetWriteDeadline(deadline) != nil {
				panic(http.ErrAbortHandler)
			}
			ctx, cleanup, deadlineErr := inlineReadContextV2(w, r, deadline)
			if deadlineErr != nil {
				inlineHTTPErrorV2(w, deadlineErr)
				return
			}
			defer cleanup()
			// The original reader reaches the store untouched. The store must claim
			// before reading, then verify exact length including the trailing-byte
			// check before any promotion. MaxBytesReader is an additional hard cap.
			result, err = service.Submit(ctx, bearer, input, r.ContentLength, http.MaxBytesReader(w, r.Body, r.ContentLength))
		} else {
			if media != "application/json" || inlineHeaderCountV2(r.Header, InlineRequestHeaderV2) != 0 || r.ContentLength < 0 || !inlineContentLengthV2(r) {
				inlineHTTPErrorV2(w, ErrInvalid)
				return
			}
			limit := int64(maxInlineControlBytesV2)
			if r.URL.Path == PathInlinePrepareV2 {
				limit = maxInlinePrepareBytesV2
			}
			if r.ContentLength > limit {
				inlineHTTPErrorV2(w, ErrInvalid)
				return
			}
			// Allocate one bounded read buffer at its declared size, instead of
			// io.ReadAll's growth capacity above the admitted control ceiling.
			raw := make([]byte, int(r.ContentLength))
			defer clear(raw)
			_, readErr := io.ReadFull(r.Body, raw)
			if readErr != nil {
				inlineHTTPErrorV2(w, ErrInvalid)
				return
			}
			var trailing [1]byte
			defer clear(trailing[:])
			n, trailingErr := io.ReadFull(r.Body, trailing[:])
			if n != 0 || trailingErr != io.EOF {
				inlineHTTPErrorV2(w, ErrInvalid)
				return
			}
			if r.URL.Path == PathInlinePrepareV2 {
				var input ydbstore.AttachedWorkerInlinePrepareV2
				if !inlineDecodeV2(raw, int(limit), &input) {
					inlineHTTPErrorV2(w, ErrInvalid)
					return
				}
				if input.ExpiresAt.IsZero() {
					inlineHTTPErrorV2(w, ErrInvalid)
					return
				}
				if input.ExpiresAt.Before(deadline) {
					deadline = input.ExpiresAt
					if controller.SetWriteDeadline(deadline) != nil {
						panic(http.ErrAbortHandler)
					}
					tightened, tightenedCleanup, tightenErr := inlineReadContextV2(w, r, deadline)
					if tightenErr != nil {
						inlineHTTPErrorV2(w, tightenErr)
						return
					}
					defer tightenedCleanup()
					ctx = tightened
				}
				result, err = service.Prepare(ctx, bearer, input)
			} else {
				var input ydbstore.AttachedWorkerInlineReconcileV2
				if !inlineDecodeV2(raw, int(limit), &input) {
					inlineHTTPErrorV2(w, ErrInvalid)
					return
				}
				result, err = service.Reconcile(ctx, bearer, input)
			}
		}
		if err != nil {
			inlineHTTPErrorV2(w, err)
			return
		}
		if !inlineResponseV2(result) {
			inlineHTTPErrorV2(w, ErrUnavailable)
			return
		}
		payload, marshalErr := json.Marshal(result)
		defer clear(payload)
		if marshalErr != nil || len(payload) > maxInlineControlBytesV2 {
			inlineHTTPErrorV2(w, ErrUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	})
}

// The physical deadline must precede every request-body Read: cancellation
// alone does not interrupt net/http's blocking body reader. Middleware must
// preserve ResponseController support; unsupported wrappers fail closed.
func inlineReadContextV2(w http.ResponseWriter, r *http.Request, deadline time.Time) (context.Context, func(), error) {
	controller := http.NewResponseController(w)
	if controller.SetReadDeadline(deadline) != nil {
		panic(http.ErrAbortHandler)
	}
	if !deadline.After(time.Now()) {
		return nil, nil, ErrInvalid
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	return ctx, cancel, nil
}

// Typed Marshal equality is the closed wire shape, not general JSON canonical
// reordering. It also rejects case aliases accepted by encoding/json. Content
// in the separate raw body is never decoded or reserialized here.
func inlineDecodeV2(raw []byte, limit int, target any) bool {
	if strictjson.ValidateDocument(raw, uint64(limit), 16, 64) != nil {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil || d.Decode(new(any)) != io.EOF {
		return false
	}
	canonical, err := json.Marshal(target)
	defer clear(canonical)
	return err == nil && bytes.Equal(raw, canonical)
}

func inlineHeaderCountV2(header http.Header, name string) int {
	n := 0
	for key, values := range header {
		if strings.EqualFold(key, name) {
			n += len(values)
		}
	}
	return n
}

func inlineOneHeaderV2(header http.Header, name string) (string, bool) {
	var value string
	n := 0
	for key, values := range header {
		if strings.EqualFold(key, name) {
			n += len(values)
			if len(values) == 1 {
				value = values[0]
			}
		}
	}
	return value, n == 1
}

func inlineHeadersV2(r *http.Request) bool {
	// Count serialized header lines, Host (stored separately by net/http), and
	// the terminating CRLF. No allocation proportional to header content.
	n := len("Host: ") + len(r.Host) + 4
	for key, values := range r.Header {
		if len(key) > maxInlineHeaderBytesV2-n {
			return false
		}
		for _, value := range values {
			if len(key)+len(value)+4 > maxInlineHeaderBytesV2-n {
				return false
			}
			n += len(key) + len(value) + 4
		}
	}
	return n <= maxInlineHeaderBytesV2
}

func inlineContentLengthV2(r *http.Request) bool {
	if inlineHeaderCountV2(r.Header, "Content-Length") == 0 {
		return true
	} // net/http stores the parsed value separately.
	value, ok := inlineOneHeaderV2(r.Header, "Content-Length")
	return ok && value == strconv.FormatInt(r.ContentLength, 10)
}

func inlineResponseV2(r ydbstore.AttachedWorkerInlineResultV2) bool {
	if r.Version != 2 || r.Nonce.Validate() != nil || !inlineHexV2(r.Fingerprint) || r.ExpiresAt.IsZero() {
		return false
	}
	if r.Capability != "" && !inlineHexV2(r.Capability) {
		return false
	}
	switch r.Stage {
	case ydbstore.InlinePreparedV2:
		return r.CanonicalDigest == ""
	case ydbstore.InlineBodyClaimedV2, ydbstore.InlineRetiredWithoutWritesV2:
		return r.Capability == "" && r.CanonicalDigest == ""
	case ydbstore.InlineCanonicalCopyingV2, ydbstore.InlineReadyV2:
		return r.Capability == "" && inlineHexV2(string(r.CanonicalDigest))
	default:
		return false
	}
}

func inlineHexV2(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func inlineHTTPErrorV2(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	switch {
	case errors.Is(err, ErrInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, ErrUnauthorized):
		status = http.StatusUnauthorized
	case errors.Is(err, ErrConflict):
		status = http.StatusConflict
	}
	// Exact framing avoids a trailing HTTP/1 chunk; the original physical
	// deadlines also remain active through the server-owned final response tail.
	payload := []byte(http.StatusText(status) + "\n")
	defer clear(payload)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}
