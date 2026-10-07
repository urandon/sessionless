package attachedworkerreceipt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type receiptBinderFixture struct {
	calls int
}

func (binder *receiptBinderFixture) BindOutputReceiptBearer(bearer []byte, auth ports.AttachedWorkerSealedInputAuthorization) (ports.AttachedWorkerSealedInputAuthorization, error) {
	binder.calls++
	if string(bearer) != "bound-bearer" || auth.OwnerUserID != "owner-a" || auth.PresentedSecretDigest != "" {
		return ports.AttachedWorkerSealedInputAuthorization{}, errors.New("private bearer failure")
	}
	auth.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("server-derived"))
	return auth, nil
}

type receiptStoreFixture struct {
	calls   int
	request ydbstore.AttachedWorkerOutputReceiptRequest
	status  ports.AttachedWorkerExecutionStatus
	err     error
}

func (store *receiptStoreFixture) CreateAttachedWorkerOutputReceipt(_ context.Context, _ ports.BlobStore, request ydbstore.AttachedWorkerOutputReceiptRequest) (ydbstore.AttachedWorkerOutputReceiptResult, error) {
	store.calls++
	store.request = request
	return ydbstore.AttachedWorkerOutputReceiptResult{Status: store.status, Receipt: ydbstore.AttachedWorkerOutputReceiptV1{CanonicalDigest: "canonical-digest"}}, store.err
}

type receiptBlobFixture struct{ ports.BlobStore }

func TestReceiptServiceBindsBearerBeforeMutatingStoreAndFailsClosed(t *testing.T) {
	binder := &receiptBinderFixture{}
	store := &receiptStoreFixture{status: ports.AttachedWorkerExecutionApplied}
	service, err := NewService(binder, store, receiptBlobFixture{})
	if err != nil {
		t.Fatal(err)
	}
	request := ydbstore.AttachedWorkerOutputReceiptRequest{
		Authorization: ports.AttachedWorkerSealedInputAuthorization{OwnerUserID: "owner-a"},
	}
	result, err := service.Create(context.Background(), []byte("bound-bearer"), request)
	if err != nil || result.Receipt.CanonicalDigest != "canonical-digest" || store.calls != 1 ||
		store.request.Authorization.PresentedSecretDigest == "" {
		t.Fatalf("result=%+v error=%v calls=%d request=%+v", result, err, store.calls, store.request)
	}
	request.Authorization.OwnerUserID = "owner-b"
	if _, err := service.Create(context.Background(), []byte("bound-bearer"), request); !errors.Is(err, ErrUnauthorized) || store.calls != 1 {
		t.Fatalf("foreign owner error=%v calls=%d", err, store.calls)
	}
	request.Authorization.OwnerUserID = "owner-a"
	store.status = ports.AttachedWorkerExecutionFenced
	if _, err := service.Create(context.Background(), []byte("bound-bearer"), request); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("fenced error=%v", err)
	}
	store.status = ports.AttachedWorkerExecutionConflict
	if _, err := service.Create(context.Background(), []byte("bound-bearer"), request); !errors.Is(err, ErrConflict) {
		t.Fatalf("divergent error=%v", err)
	}
	store.err = errors.New("provider credential must not leak")
	if _, err := service.Create(context.Background(), []byte("bound-bearer"), request); !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "credential") {
		t.Fatalf("backend error=%v", err)
	}
}

func TestReceiptHTTPBoundsInputAndReturnsOnlyServerResult(t *testing.T) {
	binder := &receiptBinderFixture{}
	store := &receiptStoreFixture{status: ports.AttachedWorkerExecutionApplied}
	service, err := NewService(binder, store, receiptBlobFixture{})
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(ydbstore.AttachedWorkerOutputReceiptRequest{Authorization: ports.AttachedWorkerSealedInputAuthorization{OwnerUserID: "owner-a"}})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, PathV1, bytes.NewReader(input))
	request.Header.Set("Authorization", "Bearer bound-bearer")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	Handler(service).ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "canonical-digest") ||
		strings.Contains(response.Body.String(), "bound-bearer") || store.calls != 1 {
		t.Fatalf("status=%d body=%q calls=%d", response.Code, response.Body.String(), store.calls)
	}
	request = httptest.NewRequest(http.MethodPost, PathV1, strings.NewReader(strings.Repeat("x", maxRequestBytes+1)))
	request.Header.Set("Authorization", "Bearer bound-bearer")
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	Handler(service).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || store.calls != 1 {
		t.Fatalf("oversized status=%d calls=%d", response.Code, store.calls)
	}
}
