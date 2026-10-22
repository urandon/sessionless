package providerloopsession

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestLocalSessionCredentialedZeroOwnerCannotRun(t *testing.T) {
	for _, v := range []*CredentialedV1{nil, {}} {
		result, terminal, err := v.Run(context.Background(), ports.ReserveAttemptEffectRequestV1{})
		if !errors.Is(err, ErrSession) || terminal != nil || result.Model.SendAttempted {
			t.Fatal("absent original owner created an effect or terminal")
		}
		if err := v.Close(); err != nil {
			t.Fatal("closing an absent original owner failed")
		}
	}
}

func TestLocalSessionCredentialHandleOwnershipPrecedesExpiryValidation(t *testing.T) {
	r := ports.ProviderCredentialIssueRequestV1{HarnessBinding: domain.HarnessBindingV2{TenantID: "tenant", OwnerUserID: "owner", RunID: "run", AttemptID: "attempt",
		Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceLocalEndpointV1, ResourceID: "resource", OwnerUserID: "owner", Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}},
		WorkerID: "worker", LeaseID: "lease", LeaseFence: 1, ExpiresAt: time.Unix(2000000000, 0)}
	h := ports.ProviderInvocationCredentialV1{HandleID: "handle", TenantID: r.HarnessBinding.TenantID, OwnerUserID: r.HarnessBinding.OwnerUserID, RunID: r.HarnessBinding.RunID,
		AttemptID: r.HarnessBinding.AttemptID, WorkerID: r.WorkerID, LeaseID: r.LeaseID, LeaseFence: r.LeaseFence, ProviderResource: r.HarnessBinding.Resource}
	h.ExpiresAt = r.ExpiresAt
	if err := h.Validate(); err != nil {
		t.Fatalf("positive owned handle is invalid before expiry mutation: %v", err)
	}
	h.ExpiresAt = time.Time{}
	if h.Validate() == nil {
		t.Fatal("zero expiry unexpectedly passes full handle validation")
	}
	if !ownedCredentialHandle(h, r) {
		t.Fatal("owned opaque handle with invalid expiry would skip release guard")
	}
	for _, mutate := range []func(*ports.ProviderInvocationCredentialV1){
		func(v *ports.ProviderInvocationCredentialV1) { v.TenantID = "foreign" },
		func(v *ports.ProviderInvocationCredentialV1) { v.OwnerUserID = "foreign" },
		func(v *ports.ProviderInvocationCredentialV1) { v.RunID = "foreign" },
		func(v *ports.ProviderInvocationCredentialV1) { v.LeaseFence++ },
		func(v *ports.ProviderInvocationCredentialV1) { v.ProviderResource.CredentialGeneration++ },
		func(v *ports.ProviderInvocationCredentialV1) { v.HandleID = "" },
	} {
		foreign := h
		mutate(&foreign)
		if ownedCredentialHandle(foreign, r) {
			t.Fatal("foreign/unowned handle would be released by this invocation")
		}
	}
}
