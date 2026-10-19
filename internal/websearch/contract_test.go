package websearch

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func searchConsent(f *FrozenRequestV1) domain.WebSearchConsentRequestV1 {
	s := f.binding.Scope
	expiresAt := f.compiledAt.Add(60 * time.Second)
	if f.validUntil.Before(expiresAt) {
		expiresAt = f.validUntil
	}
	return domain.WebSearchConsentRequestV1{Version: 1, ID: "consent-a", Scope: domain.WebSearchConsentScopeV1{TenantID: s.TenantID, SessionID: s.SessionID, OwnerUserID: s.OwnerUserID, RunID: s.RunID, AttemptID: s.AttemptID, LeaseID: s.LeaseID, FenceToken: s.FenceToken,
		PhysicalInvocationClaimID: s.PhysicalInvocationClaimID, EffectReservationDigest: string(s.EffectReservationDigest), InvocationAuthorityDigest: string(s.InvocationAuthorityDigest), HarnessBindingDigest: string(s.HarnessBindingDigest), PolicyDigest: string(s.AgentLoopPolicyDigest)},
		InitiatingUserID: f.binding.InitiatingUserID, ModelResponseDigest: strings.Repeat("a", 64), CallID: f.binding.CallID, ArgumentsDigest: f.argumentsDigest, QueryDigest: f.queryDigest, GrantDigest: f.grantDigest,
		ConsentRevision: f.grant.ConsentRevision, RevocationGeneration: f.grant.RevocationGeneration, Arguments: domain.BlobRef{TenantID: s.TenantID,
			Key: domain.SessionRunObjectPrefix(s.TenantID, s.SessionID, s.RunID) + "attempts/" + string(s.AttemptID) + "/search-args.json", Size: int64(len(f.arguments)), SHA256: f.argumentsDigest}, CreatedAt: f.compiledAt, ExpiresAt: expiresAt}
}
func TestWebSearchContractConsentByteAssociationOnly(t *testing.T) {
	f := boundSearch(t)
	r := searchConsent(f)
	if e := f.ValidateConsentRequest(r); e != nil {
		t.Fatal(e)
	}
	for _, test := range []struct {
		name   string
		change func(*domain.WebSearchConsentRequestV1)
	}{
		{"foreign-owner", func(v *domain.WebSearchConsentRequestV1) { v.Scope.OwnerUserID = "other-owner" }},
		{"claim", func(v *domain.WebSearchConsentRequestV1) { v.Scope.PhysicalInvocationClaimID = "other-claim" }},
		{"revision", func(v *domain.WebSearchConsentRequestV1) { v.ConsentRevision++ }},
		{"initiator", func(v *domain.WebSearchConsentRequestV1) { v.InitiatingUserID = "other-user" }},
		{"model-response", func(v *domain.WebSearchConsentRequestV1) { v.ModelResponseDigest = strings.Repeat("b", 64) }},
		{"query", func(v *domain.WebSearchConsentRequestV1) { v.QueryDigest = strings.Repeat("b", 64) }},
		{"deadline", func(v *domain.WebSearchConsentRequestV1) { v.ExpiresAt = f.grant.ExpiresAt.Add(time.Second) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := r
			test.change(&bad)
			if f.ValidateConsentRequest(bad) == nil {
				t.Fatal("divergent pending request associated")
			}
		})
	}
}

func TestWebSearchContractCanonicalInitiatorNotResourceOwner(t *testing.T) {
	for _, kind := range []string{"genuine-different-initiator", "resource-owner-is-not-initiator"} {
		t.Run(kind, func(t *testing.T) {
			g, c, b, at := searchFixture()
			b.InitiatingUserID = "task-initiator-a"
			f, e := CompileV1(g, c, b, []byte(`{"query":"public query"}`), at)
			if e != nil {
				t.Fatal(e)
			}
			f, e = f.BindOperationV1(searchOperation(f))
			if e != nil {
				t.Fatal(e)
			}
			r := searchConsent(f)
			if kind == "resource-owner-is-not-initiator" {
				r.InitiatingUserID = g.OwnerUserID
			}
			err := f.ValidateConsentRequest(r)
			if (err == nil) != (kind == "genuine-different-initiator") {
				t.Fatalf("canonical actor/resource owner conflated: kind=%s initiating=%s resource_owner=%s err=%v", kind, r.InitiatingUserID, g.OwnerUserID, err)
			}
		})
	}
}

func TestWebSearchContractRequiresCanonicalParentAndInitiatorData(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*BindingV1)
	}{
		{"missing-initiator", func(b *BindingV1) { b.InitiatingUserID = "" }},
		{"invalid-initiator", func(b *BindingV1) { b.InitiatingUserID = "not a user" }},
		{"missing-parent-cost", func(b *BindingV1) { b.ParentCostCeilingDigestV2 = "" }},
		{"invalid-parent-cost", func(b *BindingV1) { b.ParentCostCeilingDigestV2 = "not-a-digest" }},
		{"missing-parent-price", func(b *BindingV1) { b.ParentPriceRevision = "" }},
		{"invalid-parent-price", func(b *BindingV1) { b.ParentPriceRevision = "not a revision" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			g, c, b, at := searchFixture()
			test.change(&b)
			if _, err := CompileV1(g, c, b, []byte(`{"query":"public query"}`), at); err == nil {
				t.Fatal("missing or malformed canonical association accepted")
			}
		})
	}
}

func TestWebSearchContractDistinctParentCostAndSearchPrice(t *testing.T) {
	for _, kind := range []string{"valid-distinct-outer-price", "changed-outer-price", "changed-parent-cost", "wrong-search-evidence"} {
		t.Run(kind, func(t *testing.T) {
			g, c, b, at := searchFixture()
			if kind == "valid-distinct-outer-price" || kind == "changed-outer-price" {
				b.ParentPriceRevision = "parent-price-2"
			}
			f, e := CompileV1(g, c, b, []byte(`{"query":"public query"}`), at)
			if e != nil {
				t.Fatal(e)
			}
			op := searchOperation(f)
			switch kind {
			case "changed-outer-price":
				op.PriceRevision = g.PriceRevision
			case "changed-parent-cost":
				op.AdmissionCostCeilingDigest = domain.AdmissionCostCeilingDigestV2(strings.Repeat("b", 64))
			case "wrong-search-evidence":
				op.ExternalChargeEvidenceDigest = strings.Repeat("c", 64)
			}
			_, err := f.BindOperationV1(op)
			if (err == nil) != (kind == "valid-distinct-outer-price") {
				t.Fatalf("parent admission/search price join: kind=%s outer=%s search=%s err=%v", kind, op.PriceRevision, g.PriceRevision, err)
			}
		})
	}
}

func TestWebSearchContractOriginalConsentCapAndOperationDeadline(t *testing.T) {
	for _, seconds := range []int{60, 10} {
		t.Run((time.Duration(seconds) * time.Second).String(), func(t *testing.T) {
			g, c, b, at := searchFixture()
			c.PriceExpiresAt = at.Add(90 * time.Second)
			f, e := CompileV1(g, c, b, []byte(`{"query":"public query"}`), at)
			if e != nil {
				t.Fatal(e)
			}
			r := searchConsent(f)
			r.ExpiresAt = at.Add(time.Duration(seconds) * time.Second)
			if e := f.ValidateConsentRequest(r); e != nil {
				t.Fatalf("unbound pending consent within original grant/price cap: seconds=%d err=%v", seconds, e)
			}
			bound, e := f.BindOperationV1(searchOperation(f))
			if e != nil {
				t.Fatal(e)
			}
			err := bound.ValidateConsentRequest(r)
			if (err == nil) != (seconds == 60) {
				t.Fatalf("bound15s operation must fit consent expiry without replacing original cap: consent=%ds err=%v", seconds, err)
			}
		})
	}
}

func TestWebSearchContractBoundDeadlineDoesNotReplaceAuthorityCap(t *testing.T) {
	g, c, b, at := searchFixture()
	c.PriceExpiresAt = at.Add(90 * time.Second)
	original, err := CompileV1(g, c, b, []byte(`{"query":"public query"}`), at)
	if err != nil {
		t.Fatal(err)
	}
	op := searchOperation(original)
	bound, err := original.BindOperationV1(op)
	if err != nil {
		t.Fatal(err)
	}
	if !original.operationDeadline.IsZero() || !bound.operationDeadline.Equal(op.Deadline) || !bound.validUntil.Equal(original.validUntil) || !bound.validUntil.Equal(c.PriceExpiresAt) {
		t.Fatal("binding mutated original request or renewed/replaced original authority window")
	}
	request := searchConsent(bound)
	for _, expiry := range []time.Time{op.Deadline, c.PriceExpiresAt} {
		request.ExpiresAt = expiry
		if err := bound.ValidateConsentRequest(request); err != nil {
			t.Fatalf("inclusive consent bound %s denied: %v", expiry, err)
		}
	}
	for _, expiry := range []time.Time{op.Deadline.Add(-time.Microsecond), c.PriceExpiresAt.Add(time.Microsecond)} {
		request.ExpiresAt = expiry
		if bound.ValidateConsentRequest(request) == nil {
			t.Fatalf("outside consent bound %s accepted", expiry)
		}
	}
}
func TestWebSearchContractExactOperationNoCapability(t *testing.T) {
	f := boundSearch(t)
	op := searchOperation(f)
	if e := f.ValidateOperation(op); e != nil {
		t.Fatal(e)
	}
	for _, test := range []struct {
		name   string
		change func(*domain.AttemptOperationV1)
	}{
		{"args-as-wire", func(v *domain.AttemptOperationV1) { v.RequestDigest = v.ArgumentsDigest }},
		{"foreign-claim", func(v *domain.AttemptOperationV1) { v.Scope.PhysicalInvocationClaimID = "other-claim" }},
		{"price", func(v *domain.AttemptOperationV1) { v.PriceRevision = "different" }},
		{"amount", func(v *domain.AttemptOperationV1) { v.Debit.AmountMicrounits = 0 }},
		{"call", func(v *domain.AttemptOperationV1) { v.CallID = "other-call" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := op
			test.change(&bad)
			if f.ValidateOperation(bad) == nil {
				t.Fatal("divergent operation associated")
			}
		})
	}
	copyBody := f.Body()
	copyBody[0] = '!'
	if f.ValidateOperation(op) != nil {
		t.Fatal("exported copy changed original request commitment")
	}
}
