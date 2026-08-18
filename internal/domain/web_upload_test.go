package domain_test

import (
	"encoding/json"
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
		ExpectedMD5:    "AAAAAAAAAAAAAAAAAAAAAA==",
		Status:         domain.UploadIntentPending, CreatedAt: webTestTime, ExpiresAt: webTestTime.Add(10 * time.Minute),
	}
}

func TestUploadIntentRequiresCanonicalStandardBase64MD5AndPersistsIt(t *testing.T) {
	t.Parallel()
	intent := uploadIntent()
	encoded, err := json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	var decoded domain.UploadIntent
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ExpectedMD5 != intent.ExpectedMD5 {
		t.Fatalf("persisted expected_md5 = %q, want %q", decoded.ExpectedMD5, intent.ExpectedMD5)
	}

	for _, invalid := range []string{
		"",
		"AAAAAAAAAAAAAAAAAAAAAA",
		"AAAAAAAAAAAAAAAAAAAAAA=",
		"AAAAAAAAAAAAAAAAAAAAAA===",
		"_____________________w==",
		"AAAAAAAAAAAAAAAAAAAAAAAA",
	} {
		candidate := uploadIntent()
		candidate.ExpectedMD5 = invalid
		if err := candidate.Validate(); err == nil {
			t.Fatalf("non-canonical MD5 %q accepted", invalid)
		}
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
