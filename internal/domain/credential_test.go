package domain_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestCredentialSecretReferenceIsRedactedOnEveryGenericSurface(t *testing.T) {
	t.Parallel()

	const rawReference = "vault://tenant-a/private-token"
	ref, err := domain.NewCredentialSecretRef(rawReference)
	if err != nil {
		t.Fatalf("NewCredentialSecretRef() error = %v", err)
	}
	binding := testCredentialBinding(t, ref)

	encodedRef, err := json.Marshal(ref)
	if err != nil {
		t.Fatalf("json.Marshal(ref) error = %v", err)
	}
	encodedBinding, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("json.Marshal(binding) error = %v", err)
	}
	for name, value := range map[string]string{
		"ref string": fmt.Sprint(ref), "ref go string": fmt.Sprintf("%#v", ref),
		"binding string": fmt.Sprint(binding), "binding go string": fmt.Sprintf("%#v", binding),
		"ref json": string(encodedRef), "binding json": string(encodedBinding),
	} {
		if strings.Contains(value, rawReference) {
			t.Fatalf("%s leaked the secret reference: %q", name, value)
		}
	}
	if strings.Contains(string(encodedBinding), "secret_ref") {
		t.Fatalf("binding JSON includes secret_ref: %s", encodedBinding)
	}
}

func TestCredentialBindingRevocationShapeClearsActiveSecret(t *testing.T) {
	t.Parallel()

	ref, err := domain.NewCredentialSecretRef("vault://tenant-a/current")
	if err != nil {
		t.Fatal(err)
	}
	binding := testCredentialBinding(t, ref)
	if err := binding.Validate(); err != nil {
		t.Fatalf("active binding rejected: %v", err)
	}

	binding.Revoked = true
	binding.Entitlement = domain.EntitlementDisconnected
	if err := binding.Validate(); err == nil {
		t.Fatal("revoked binding retaining active secret accepted")
	}
	binding.SecretRef = domain.CredentialSecretRef{}
	binding.SecretFingerprint = ""
	if err := binding.Validate(); err != nil {
		t.Fatalf("valid revoked binding rejected: %v", err)
	}
}

func testCredentialBinding(t *testing.T, ref domain.CredentialSecretRef) domain.CredentialBinding {
	t.Helper()
	return domain.CredentialBinding{
		Version: domain.CredentialBindingVersionV1, TenantID: "tenant-a",
		SubscriptionConnectionID: "connection-a", OwnerUserID: "user-a",
		Provider: "provider-a", AuthMode: "subscription",
		SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte(`{"token":"value"}`)),
		Entitlement: domain.EntitlementActive, Generation: 1,
		UpdatedAt: time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC),
	}
}
