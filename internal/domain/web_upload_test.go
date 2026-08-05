package domain_test

import (
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func uploadIntent() domain.UploadIntent {
	return domain.UploadIntent{
		ID: "upload-1", TenantID: "tenant-a", UserID: "user-a", SessionID: "session-a",
		ObjectKey: "tenants/tenant-a/uploads/upload-1/photo.jpg",
		Name:      "photo.jpg", MediaType: "image/jpeg", ExpectedSize: 42,
		ExpectedSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Status:         domain.UploadIntentPending, CreatedAt: webTestTime, ExpiresAt: webTestTime.Add(10 * time.Minute),
	}
}

func TestUploadIntentCommitIsTenantBoundAndOneTime(t *testing.T) {
	t.Parallel()
	intent := uploadIntent()
	blob := domain.BlobRef{
		TenantID: intent.TenantID, Key: intent.ObjectKey, Size: intent.ExpectedSize, SHA256: intent.ExpectedSHA256,
	}
	wrong := blob
	wrong.TenantID, wrong.Key = "tenant-b", "tenants/tenant-b/uploads/upload-1/photo.jpg"
	if err := intent.Commit(wrong, webTestTime.Add(time.Minute)); !errors.Is(err, domain.ErrUploadMismatch) {
		t.Fatalf("cross-tenant commit error = %v", err)
	}
	if err := intent.Commit(blob, webTestTime.Add(time.Minute)); err != nil {
		t.Fatalf("valid commit failed: %v", err)
	}
	if err := intent.Commit(blob, webTestTime.Add(2*time.Minute)); !errors.Is(err, domain.ErrUploadIntentCommitted) {
		t.Fatalf("repeated commit error = %v", err)
	}
}

func TestUploadIntentRejectsExpiryAndTraversal(t *testing.T) {
	t.Parallel()
	intent := uploadIntent()
	blob := domain.BlobRef{TenantID: intent.TenantID, Key: intent.ObjectKey, Size: intent.ExpectedSize, SHA256: intent.ExpectedSHA256}
	if err := intent.Commit(blob, intent.ExpiresAt); !errors.Is(err, domain.ErrUploadIntentExpired) {
		t.Fatalf("expired commit error = %v", err)
	}
	intent = uploadIntent()
	intent.ObjectKey = "tenants/tenant-a/uploads/upload-1/../other/photo.jpg"
	if err := intent.Validate(); err == nil {
		t.Fatal("traversing object key accepted")
	}
}
