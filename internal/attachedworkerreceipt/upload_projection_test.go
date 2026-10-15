package attachedworkerreceipt

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type uploadProjectionStore struct {
	receiptStoreFixture
}

func (store *uploadProjectionStore) CreateAttachedWorkerOutputReceipt(ctx context.Context, blobs ports.BlobStore, request ydbstore.AttachedWorkerOutputReceiptRequest) (ydbstore.AttachedWorkerOutputReceiptResult, error) {
	result, err := store.receiptStoreFixture.CreateAttachedWorkerOutputReceipt(ctx, blobs, request)
	// Deliberately malformed fake DATA: HTTP must project away even a store's
	// private-only metadata, rather than rely on a fixture having none.
	result.Receipt.UploadPlan = &attachedworkeroutput.AttemptOutputUploadPlanV1{PlanDigest: "private-plan-marker"}
	return result, err
}

func TestReceiptHTTPNeverSerializesPrivateUploadPlan(t *testing.T) {
	store := &uploadProjectionStore{receiptStoreFixture: receiptStoreFixture{status: ports.AttachedWorkerExecutionApplied}}
	service, err := NewService(&receiptBinderFixture{}, store, receiptBlobFixture{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, PathV1, strings.NewReader(`{"Authorization":{"OwnerUserID":"owner-a"}}`))
	request.Header.Set("Authorization", "Bearer bound-bearer")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	Handler(service).ServeHTTP(response, request)
	if response.Code != http.StatusOK || store.calls != 1 || !strings.Contains(response.Body.String(), "canonical-digest") {
		t.Fatalf("HTTP projection not exercised: status=%d store calls=%d body=%q", response.Code, store.calls, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "upload_plan") || strings.Contains(response.Body.String(), "private-plan-marker") {
		t.Fatal("public receipt serialized private pre-Put custody")
	}
}
