package ports_test

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func searchCredentialHandleData() ports.SearchInvocationCredentialV1 {
	d := ports.SearchCredentialDeclarationV1{Version: 1, TenantID: "tenant-a", Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceWebSearchAccountV1, ResourceID: "search-account", OwnerUserID: "owner-a", Revision: 3, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 7}, Audience: domain.WebSearchCredentialAudienceV1, DeliveryKind: domain.ProviderCredentialDeliveryDirectV1}
	h := strings.Repeat("a", 64)
	s := domain.AttemptOperationScopeV1{TenantID: d.TenantID, SessionID: "session-a", OwnerUserID: d.Resource.OwnerUserID, RunID: "run-a", AttemptID: "attempt-a", LeaseID: "lease-a", FenceToken: 2, PhysicalInvocationClaimID: "physical-a", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(h), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(h), HarnessBindingDigest: domain.HarnessBindingDigestV2(h), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(h)}
	return ports.SearchInvocationCredentialV1{Credential: ports.ProviderInvocationCredentialV1{HandleID: "handle-a", TenantID: s.TenantID, OwnerUserID: s.OwnerUserID, RunID: s.RunID, AttemptID: s.AttemptID, WorkerID: "worker-a", LeaseID: s.LeaseID, LeaseFence: s.FenceToken, ProviderResource: d.Resource, ExpiresAt: time.Unix(200, 0).UTC()}, Declaration: d, Correlation: ports.SearchCredentialCorrelationV1{Scope: s, OperationDigest: domain.AttemptOperationDigestV1(h), ConsentRequestDigest: domain.WebSearchConsentRequestDigestV1(h), ConsentDecisionDigest: domain.WebSearchConsentDecisionDigestV1(h)}}
}

func TestSearchCredentialComparableExactCorrelationData(t *testing.T) {
	h := searchCredentialHandleData()
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
	keys := map[ports.SearchInvocationCredentialV1]bool{h: true}
	copy := h
	copy.Correlation.Scope.PhysicalInvocationClaimID = "other-claim"
	if keys[copy] {
		t.Fatal("changed claim remained identical comparable handle")
	}
	for _, name := range []string{"tenant", "owner", "run", "attempt", "lease", "fence", "resource", "audience", "mode", "delivery", "receipt", "operation", "expiry"} {
		t.Run(name, func(t *testing.T) {
			bad := h
			switch name {
			case "tenant":
				bad.Credential.TenantID = "foreign-tenant"
			case "owner":
				bad.Credential.OwnerUserID = "foreign-owner"
			case "run":
				bad.Credential.RunID = "foreign-run"
			case "attempt":
				bad.Credential.AttemptID = "foreign-attempt"
			case "lease":
				bad.Credential.LeaseID = "foreign-lease"
			case "fence":
				bad.Credential.LeaseFence++
			case "resource":
				bad.Credential.ProviderResource.Revision++
			case "audience":
				bad.Declaration.Audience = "model"
			case "mode":
				bad.Declaration.Resource.CredentialMode = domain.ProviderCredentialNoneV1
			case "delivery":
				bad.Declaration.DeliveryKind = domain.ProviderCredentialDeliveryFileV1
			case "receipt":
				bad.Correlation.ConsentDecisionDigest = ""
			case "operation":
				bad.Correlation.OperationDigest = ""
			case "expiry":
				bad.Credential.ExpiresAt = bad.Credential.ExpiresAt.Add(time.Nanosecond)
			}
			if err := bad.Validate(); err == nil {
				t.Fatalf("case=%s accepted inconsistent SEARCH handle", name)
			}
		})
	}
}
