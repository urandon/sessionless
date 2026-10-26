package attachedworkerhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

const (
	ResourceGenerationPublishPathV1    = "/attached-worker/v1/resource-generation/publish"
	ResourceGenerationReadPathV1       = "/attached-worker/v1/resource-generation/read"
	resourceGenerationHeaderMaxBytesV1 = 8 << 10
)

type ResourceGenerationCoreV1 interface {
	PublishResourceGenerationBearer(context.Context, []byte, ports.AttachedResourceGenerationPublicationV1) (ports.AttachedResourceGenerationResultV1, error)
	ReadResourceGenerationBearer(context.Context, []byte, ports.AttachedResourceGenerationAuthorizationV1) (ports.AttachedResourceGenerationResultV1, error)
}

type ResourceGenerationPublishRequestV1 struct {
	Version                      uint32                                          `json:"version"`
	Authorization                ports.AttachedResourceGenerationAuthorizationV1 `json:"authorization"`
	ExpectedCredentialGeneration uint64                                          `json:"expected_credential_generation"`
	Target                       domain.AttachedResourceGenerationHeadV1         `json:"target"`
}

type ResourceGenerationReadRequestV1 struct {
	Version       uint32                                          `json:"version"`
	Authorization ports.AttachedResourceGenerationAuthorizationV1 `json:"authorization"`
}

type ResourceGenerationResponseV1 struct {
	Version uint32                                  `json:"version"`
	Status  ports.AttachedWorkerExecutionStatus     `json:"status"`
	Head    domain.AttachedResourceGenerationHeadV1 `json:"head"`
}

type ResourceGenerationHandlerV1 struct{ core ResourceGenerationCoreV1 }

func NewResourceGenerationHandlerV1(core ResourceGenerationCoreV1) (*ResourceGenerationHandlerV1, error) {
	if core == nil {
		return nil, ErrInvalidRequest
	}
	return &ResourceGenerationHandlerV1{core: core}, nil
}

func (handler *ResourceGenerationHandlerV1) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	setSafeResponseHeaders(writer.Header())
	if request.Method != http.MethodPost || request.URL.Path != ResourceGenerationPublishPathV1 && request.URL.Path != ResourceGenerationReadPathV1 {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if !resourceGenerationHeadersBounded(request.Header) {
		writer.WriteHeader(http.StatusRequestHeaderFieldsTooLarge)
		return
	}
	if request.URL.RawQuery != "" {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if !exactJSONMediaType(request.Header.Values("Content-Type")) {
		writer.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	if !exactJSONMediaType(request.Header.Values("Accept")) {
		writer.WriteHeader(http.StatusNotAcceptable)
		return
	}
	values := request.Header.Values("Authorization")
	if len(values) != 1 || len(values[0]) > len("Bearer ")+attachedworkertransport.ConnectionBearerMaxBytes {
		writer.WriteHeader(http.StatusUnauthorized)
		return
	}
	token, err := authorizationToken(values)
	if err != nil {
		writer.Header().Set("WWW-Authenticate", `Bearer realm="attached-worker"`)
		writer.WriteHeader(http.StatusUnauthorized)
		return
	}
	// The same caller budget covers body parsing and the store operation.
	// net/http's hosting server owns the actual socket read deadline; context
	// alone cannot interrupt an arbitrary non-cooperative request Body reader.
	call, cancel := context.WithTimeout(request.Context(), ports.AttachedResourceGenerationOperationTimeoutV1)
	defer cancel()
	if call.Err() != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if request.ContentLength > domain.AttachedResourceGenerationMetadataMaxBytesV1 {
		writer.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, domain.AttachedResourceGenerationMetadataMaxBytesV1)
	raw, err := io.ReadAll(request.Body)
	if call.Err() != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writer.WriteHeader(http.StatusRequestEntityTooLarge)
		} else {
			writer.WriteHeader(http.StatusBadRequest)
		}
		return
	}
	bearer := token.Bytes()
	defer clear(bearer)
	var result ports.AttachedResourceGenerationResultV1
	var authorization ports.AttachedResourceGenerationAuthorizationV1
	var publicationTarget *domain.AttachedResourceGenerationHeadV1
	if request.URL.Path == ResourceGenerationPublishPathV1 {
		var payload ResourceGenerationPublishRequestV1
		if decodeResourceGenerationV1(raw, &payload) != nil || payload.Version != 1 {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		authorization, publicationTarget = payload.Authorization, &payload.Target
		result, err = handler.core.PublishResourceGenerationBearer(call, bearer, ports.AttachedResourceGenerationPublicationV1{Authorization: payload.Authorization, ExpectedCredentialGeneration: payload.ExpectedCredentialGeneration, Target: payload.Target})
	} else {
		var payload ResourceGenerationReadRequestV1
		if decodeResourceGenerationV1(raw, &payload) != nil || payload.Version != 1 {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		authorization = payload.Authorization
		result, err = handler.core.ReadResourceGenerationBearer(call, bearer, payload.Authorization)
	}
	if err != nil || call.Err() != nil {
		switch {
		case errors.Is(err, attachedworkertransport.ErrTransportUnauthorized):
			writer.WriteHeader(http.StatusUnauthorized)
		case errors.Is(err, attachedworkertransport.ErrTransportConflict):
			writer.WriteHeader(http.StatusConflict)
		case errors.Is(err, attachedworkertransport.ErrTransportBackend) || call.Err() != nil:
			writer.WriteHeader(http.StatusServiceUnavailable)
		default:
			writer.WriteHeader(http.StatusInternalServerError)
		}
		return
	}
	if result.Status == ports.AttachedWorkerExecutionNotFound && request.URL.Path == ResourceGenerationReadPathV1 && result.Head == (domain.AttachedResourceGenerationHeadV1{}) {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if result.Status != ports.AttachedWorkerExecutionApplied && (result.Status != ports.AttachedWorkerExecutionReplayed || request.URL.Path != ResourceGenerationPublishPathV1) || result.Head.Validate() != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	if result.Head.OwnerUserID != authorization.OwnerUserID || result.Head.WorkerID != authorization.WorkerID || result.Head.EnrollmentGeneration != authorization.EnrollmentGeneration || result.Head.BaselineRevision != authorization.BaselineRevision || publicationTarget != nil && result.Head != *publicationTarget {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	encoded, err := json.Marshal(ResourceGenerationResponseV1{Version: 1, Status: result.Status, Head: result.Head})
	if err != nil || len(encoded) > domain.AttachedResourceGenerationMetadataMaxBytesV1 {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func resourceGenerationHeadersBounded(header http.Header) bool {
	bytes, values := 0, 0
	for name, entries := range header {
		bytes += len(name)
		if bytes > resourceGenerationHeaderMaxBytesV1 {
			return false
		}
		for _, value := range entries {
			values++
			bytes += len(value)
			if values > 64 || bytes > resourceGenerationHeaderMaxBytesV1 {
				return false
			}
		}
	}
	return true
}

func decodeResourceGenerationV1(raw []byte, target any) error {
	if strictjson.ValidateDocument(raw, domain.AttachedResourceGenerationMetadataMaxBytesV1, 4, 16) != nil {
		return ErrInvalidRequest
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil || d.Decode(new(any)) != io.EOF {
		return ErrInvalidRequest
	}
	canonical, err := json.Marshal(target)
	if err != nil || len(canonical) > domain.AttachedResourceGenerationMetadataMaxBytesV1 {
		return ErrInvalidRequest
	}
	var originalShape, canonicalShape any
	for _, item := range []struct {
		raw    []byte
		target *any
	}{{raw, &originalShape}, {canonical, &canonicalShape}} {
		decoder := json.NewDecoder(bytes.NewReader(item.raw))
		decoder.UseNumber()
		if decoder.Decode(item.target) != nil {
			return ErrInvalidRequest
		}
	}
	if !reflect.DeepEqual(originalShape, canonicalShape) {
		return ErrInvalidRequest
	}
	return nil
}
