package domain_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestProviderCredentialBindingSeparatesActiveAndRevokedAuthority(t *testing.T) {
	ref, err := domain.NewCredentialSecretRef("lockbox/provider/secret/version-1")
	if err != nil {
		t.Fatal(err)
	}
	active := domain.ProviderCredentialBindingV1{
		Version: domain.ProviderCredentialBindingVersionV1, TenantID: "tenant-a", OwnerUserID: "user-a",
		ResourceKind: domain.ProviderResourceRouterAccountV1, ResourceID: "openrouter-a",
		ResourceRevision: 3, CredentialGeneration: 7, CandidateMutationID: "mutation-active", State: domain.ProviderCredentialActiveV1,
		SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte("secret-marker")), UpdatedAt: time.Unix(10, 0).UTC(),
	}
	if err := active.Validate(); err != nil {
		t.Fatal(err)
	}
	resource, err := active.ResourceBinding()
	if err != nil || resource.Revision != 3 || resource.CredentialGeneration != 7 || resource.Kind != domain.ProviderResourceRouterAccountV1 {
		t.Fatalf("resource=%+v err=%v", resource, err)
	}
	encoded, err := json.Marshal(active)
	if err != nil || strings.Contains(string(encoded), "lockbox/provider") || strings.Contains(active.String(), "lockbox/provider") {
		t.Fatalf("secret reference leaked: json=%s string=%s err=%v", encoded, active.String(), err)
	}
	revoked := active
	revoked.State, revoked.SecretRef, revoked.SecretFingerprint = domain.ProviderCredentialRevokedV1, domain.CredentialSecretRef{}, ""
	revoked.ResourceRevision, revoked.CredentialGeneration = 4, 8
	if err := revoked.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := revoked.ResourceBinding(); err == nil {
		t.Fatal("revoked credential produced an active invocation resource")
	}
	if _, err := revoked.AuthorityResourceBinding(); err != nil {
		t.Fatal(err)
	}
}

func TestProviderCredentialAuditReceiptIsDeterministicAndContentFree(t *testing.T) {
	ref, err := domain.NewCredentialSecretRef("lockbox/provider/audit-secret")
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.ProviderCredentialBindingV1{
		Version: domain.ProviderCredentialBindingVersionV1, TenantID: "tenant-a", OwnerUserID: "user-a",
		ResourceKind: domain.ProviderResourceRouterAccountV1, ResourceID: "openrouter-a",
		ResourceRevision: 1, CredentialGeneration: 1, CandidateMutationID: "mutation-audit", State: domain.ProviderCredentialActiveV1,
		SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte("secret")), UpdatedAt: time.Unix(10, 0).UTC(),
	}
	first, err := domain.NewProviderCredentialAuditEventV1(binding, domain.ProviderCredentialAuditIngestedV1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := domain.NewProviderCredentialAuditEventV1(binding, domain.ProviderCredentialAuditIngestedV1)
	if err != nil || first != second || first.ReceiptID == "" {
		t.Fatalf("first=%+v second=%+v err=%v", first, second, err)
	}
	rotated, err := domain.NewProviderCredentialAuditEventV1(binding, domain.ProviderCredentialAuditRotatedV1)
	if err != nil || rotated.ReceiptID == first.ReceiptID {
		t.Fatalf("rotated=%+v err=%v", rotated, err)
	}
	encoded, err := json.Marshal(first)
	if err != nil || strings.Contains(string(encoded), ref.StorageValue()) {
		t.Fatalf("audit leaked secret locator: %s err=%v", encoded, err)
	}
}
