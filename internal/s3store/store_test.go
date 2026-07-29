package s3store

import (
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestTenantObjectKeyAddsTenantPrefix(t *testing.T) {
	key, err := tenantObjectKey("tenant-a", "inputs/message.txt")
	if err != nil {
		t.Fatalf("tenantObjectKey: %v", err)
	}
	if key != "tenants/tenant-a/inputs/message.txt" {
		t.Fatalf("key = %q", key)
	}
}

func TestTenantObjectKeyRejectsOtherTenantPrefix(t *testing.T) {
	_, err := tenantObjectKey("tenant-a", "tenants/tenant-b/private.txt")
	var mismatch domain.TenantMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %v, want TenantMismatchError", err)
	}
	if mismatch.Expected != "tenant-a" || mismatch.Actual != "tenant-b" {
		t.Fatalf("mismatch = %#v", mismatch)
	}
}

func TestTenantObjectKeyRejectsTraversal(t *testing.T) {
	for _, key := range []string{"../private.txt", "inputs/../private.txt", "/absolute"} {
		if _, err := tenantObjectKey("tenant-a", key); err == nil {
			t.Fatalf("key %q was accepted", key)
		}
	}
}

func TestAuthorizeRefRequiresCallerTenant(t *testing.T) {
	ref := domain.BlobRef{
		TenantID: "tenant-a",
		Key:      "tenants/tenant-a/result.txt",
		Size:     1,
		SHA256:   "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
	}
	err := authorizeRef("tenant-b", ref)
	var mismatch domain.TenantMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %v, want TenantMismatchError", err)
	}
}
