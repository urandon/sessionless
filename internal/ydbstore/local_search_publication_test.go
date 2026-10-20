package ydbstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// These accepted snapshots are synthetic nonsecret DATA, not vendor prices,
// model execution, account credit or legal/HTTP evidence. The real original
// private-file Source and Store publication code are connected below; only
// serializable SQL/commit mechanics use the existing local installation model.
func localSearchDeclaration(t *testing.T, m *localoperator.ManifestV1) {
	t.Helper()
	body := func(id string, value any) localoperator.EvidenceBodyV1 {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		return localoperator.EvidenceBodyV1{Reference: domain.OwnedAgentAuthorityBodyRefV1{Ref: id, Digest: hex.EncodeToString(sum[:]), Bytes: uint64(len(raw))}, Body: raw}
	}
	at, end := m.InvoiceTerms.IssuedAt, m.InvoiceTerms.ExpiresAt
	pricePayload := body("synthetic-search-tariff-payload", "owner-accepted synthetic quote, not current vendor HTTP")
	termsPayload := body("synthetic-search-terms-payload", "owner-accepted synthetic terms, not legal interpretation")
	price := body("synthetic-search-price", localoperator.OwnerAcceptedSearchPriceV2{Version: 2, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1,
		Origin: "https://www.tavily.com/pricing", ObservedAt: at, ExpiresAt: end, Payload: pricePayload.Reference,
		PriceRevision: "owner-accepted-price-v2", Currency: "USD", Unit: "request", TariffMicrounits: 8000})
	terms := body("synthetic-search-terms", localoperator.OwnerAcceptedSearchTermsV2{Version: 2, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1,
		Origin: "https://www.tavily.com/terms", ObservedAt: at, ExpiresAt: end, Payload: termsPayload.Reference,
		DataPolicyDigest: strings.Repeat("a", 64), CredentialAudience: domain.WebSearchCredentialAudienceV1})
	g := domain.WebSearchResourceGrantV2{Version: 2, TenantID: m.Configuration.TenantID, OwnerUserID: m.Configuration.OwnerUserID, BillingOwnerUserID: m.Configuration.OwnerUserID,
		ResourceID: "search-resource-distinct-from-minted-local-model", ResourceRevision: 7, RevocationGeneration: 9, ConsentRevision: 11,
		ProfileID: domain.WebSearchProfileV1, ProfileRevision: 3, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Endpoint: domain.WebSearchEndpointV1, Method: "POST",
		CredentialRef: "synthetic-search-credential-locator-no-key", CredentialAudience: domain.WebSearchCredentialAudienceV1,
		DataPolicyDigest: strings.Repeat("a", 64), APISchemaDigest: strings.Repeat("b", 64), AdapterArtifactDigest: strings.Repeat("c", 64),
		Currency: "USD", PriceRevision: "owner-accepted-price-v2", PriceEvidenceDigest: price.Reference.Digest, TermsEvidenceDigest: terms.Reference.Digest,
		PerCallReservationMicrounits: 10000, AggregateReservationMicrounits: 20000, IssuedAt: at, ExpiresAt: end}
	grantBody := body("original-search-resource-body", g)
	semantic, err := g.Digest()
	if err != nil {
		t.Fatalf("search resource DATA prerequisite: %v", err)
	}
	if semantic == grantBody.Reference.Digest {
		t.Fatal("resource semantic identity confused with raw body SHA")
	}
	p := &m.Profile
	oldTotal, oldSearch := p.CostCeiling.MaxTotalAmountMicrounits, p.CostCeiling.WebSearchCost.MaxAmountMicrounits
	if oldTotal == nil || oldSearch == nil || *oldSearch > *oldTotal {
		t.Fatal("original local cost fixture lacks coherent finite ceilings")
	}
	total, search := *oldTotal-*oldSearch+20000, uint64(20000)
	p.CostCeiling.MaxTotalAmountMicrounits = &total
	p.CostCeiling.WebSearchCost = &domain.ExternalServiceCostCeilingV1{Version: 1, Kind: domain.ExternalServiceWebSearchV1,
		TenantID: g.TenantID, OwnerUserID: g.OwnerUserID, BillingOwnerUserID: g.BillingOwnerUserID,
		ResourceID: g.ResourceID, ResourceRevision: g.ResourceRevision, RevocationGeneration: g.RevocationGeneration, ProfileID: g.ProfileID, ProfileRevision: g.ProfileRevision,
		ScopedGrantDigest: semantic, Currency: "USD", PriceRevision: g.PriceRevision, PriceObservedAt: at, PriceExpiresAt: end,
		PriceEvidenceDigest: g.PriceEvidenceDigest, TermsEvidenceDigest: g.TermsEvidenceDigest, PriceState: domain.ExternalServicePriceKnownPaidV1, MaxAmountMicrounits: &search}
	// All component price intervals must contain the enclosing executable
	// interval. Keep the unrelated, still synthetic MCP declaration coherent;
	// this does not create an MCP funding source or enable that component.
	p.CostCeiling.PriceObservedAt, p.CostCeiling.PriceExpiresAt = at, end
	p.CostCeiling.MCPToolCost.PriceObservedAt, p.CostCeiling.MCPToolCost.PriceExpiresAt = at, end
	p.WebSearchGrantDigest = semantic
	p.WebSearchResourceGrant = &domain.WebSearchResourceGrantBodyV2{Version: 2, Body: grantBody.Reference}
	p.Substrate.AdmissionCostCeilingDigest, err = p.CostCeiling.Digest()
	if err != nil {
		t.Fatalf("amended original cost fixture: %v", err)
	}
	m.SearchExpenditure = &localoperator.OwnerLocalSearchExpenditureV2{Version: 2, TenantID: g.TenantID, OwnerUserID: g.OwnerUserID, BillingOwnerUserID: g.BillingOwnerUserID,
		ResourceID: g.ResourceID, ResourceRevision: g.ResourceRevision, RevocationGeneration: g.RevocationGeneration,
		GrantDigest: semantic, GrantBody: grantBody.Reference, PriceEvidence: price.Reference, TermsEvidence: terms.Reference, Currency: "USD",
		PerCallReservationMicrounits: 10000, AggregateReservationMicrounits: 20000, IssuedAt: at, ExpiresAt: end}
	m.Evidence = append(m.Evidence, pricePayload, termsPayload, price, terms, grantBody)
}

func localSearchOperatorStore(t *testing.T, amendments ...func(*localoperator.ManifestV1)) (*localoperator.Source, *Store, *localInstallationSQLFixture, ports.OwnedProfilePublicationReceiptV1, localoperator.ManifestV1) {
	t.Helper()
	root, executable, m := localOperatorFiles(t)
	localSearchDeclaration(t, &m)
	for _, amend := range amendments {
		amend(&m)
	}
	writeLocalOperatorManifest(t, root, m)
	source, err := localoperator.OpenFresh(t.Context(), root, executable)
	if err != nil {
		t.Fatalf("actual original search Source intake: %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	publication, err := source.Publication()
	if err != nil {
		t.Fatalf("original profile publication intent: %v", err)
	}
	store, f, _, _ := newLocalInstallationFixture(t)
	f.at = publication.IssuedAt
	store.ownedProfilePublicationVerifier = source
	store.attemptSpendingPublicationVerifier = source
	receipt, err := store.PublishOwnedProfile(t.Context(), publication)
	if err != nil {
		t.Fatalf("actual Source -> original Store profile publication: %v", err)
	}
	if err := source.AcceptReceipt(receipt); err != nil {
		t.Fatalf("original Source rejected actual Store postimage: %v", err)
	}
	return source, store, f, receipt, m
}

func localSearchScope(p domain.OwnedAgentProfileCurrentV1, suffix string) domain.AttemptExecutionSourceScopeV1 {
	return domain.AttemptExecutionSourceScopeV1{TenantID: p.Locator.TenantID, OwnerUserID: p.Locator.OwnerUserID,
		SessionID: domain.SessionID("search-session-" + suffix), RunID: domain.RunID("search-run-" + suffix), AttemptID: domain.AttemptID("search-attempt-" + suffix)}
}

func TestLocalSearchPublicationTwoAttemptsOriginalSourceAndStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, store, f, profile, manifest := localSearchOperatorStore(t)
		originalHeads := cloneLocalInstallationRows(f.heads)
		var grants []ports.AttemptSpendingPublicationGrantV1
		for _, suffix := range []string{"one", "two"} {
			scope := localSearchScope(profile.Profile, suffix)
			g, err := source.SpendingPublication(t.Context(), scope, domain.AttemptSpendingWebSearchV1)
			if err != nil {
				t.Fatalf("actual original search issuer for %s: %v", suffix, err)
			}
			registerActualFundingKey(f, g)
			f.at = g.IssuedAt
			statements := len(f.statements)
			r, err := store.PublishAttemptSpending(t.Context(), g)
			if err != nil || r.Replayed || r.Authorization.SourceProfile.Digest != profile.Digest || r.Authorization.ScopedGrantDigest != manifest.SearchExpenditure.GrantDigest {
				t.Fatalf("actual Source -> Store search publication %s: replay=%t err=%v", suffix, r.Replayed, err)
			}
			joined := false
			for _, statement := range f.statements[statements:] {
				joined = joined || statement == ownedProfileCurrentSQL
			}
			if !joined || r.Authorization.ValidateAgainstProfile(profile.Profile, f.at) != nil {
				t.Fatal("search spending did not join the original current profile")
			}
			if r.Authorization.ResourceID == profile.Profile.Resource.ResourceID || r.Authorization.SourceAuthorization.Ref == manifest.SearchExpenditure.GrantBody.Ref {
				t.Fatal("search resource/source envelope confused with model resource/grant body")
			}
			writes := f.execCalls
			replayed, err := store.PublishAttemptSpending(t.Context(), g)
			if err != nil || !replayed.Replayed || replayed.Digest != r.Digest || f.execCalls != writes {
				t.Fatalf("exact original search publication replay changed rows: %v", err)
			}
			second, err := source.SpendingPublication(t.Context(), scope, domain.AttemptSpendingWebSearchV1)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.PublishAttemptSpending(t.Context(), second); !errors.Is(err, domain.ErrMembershipDenied) || f.execCalls != writes {
				t.Fatalf("fresh issuer call became a search top-up: %v", err)
			}
			grants = append(grants, g)
		}
		if len(f.fundingHeads) != 2 || len(f.fundingAudits) != 2 || !reflect.DeepEqual(originalHeads, f.heads) {
			t.Fatalf("two Attempts churned profile or lost exact funding pair: heads=%d audits=%d", len(f.fundingHeads), len(f.fundingAudits))
		}
		a, _ := grants[0].Authorization.SourceCanonicalBytes()
		b, _ := grants[1].Authorization.SourceCanonicalBytes()
		if bytes.Equal(a, b) || grants[0].Authorization.SourceAuthorization == grants[1].Authorization.SourceAuthorization {
			t.Fatal("distinct Attempts reused original spending/source envelope")
		}
		for _, mutate := range []func(*ports.AttemptSpendingPublicationGrantV1){
			func(g *ports.AttemptSpendingPublicationGrantV1) {
				g.Authorization.RunID = grants[1].Authorization.RunID
				g.Authorization.Locator.AttemptID = grants[1].Authorization.Locator.AttemptID
			},
			func(g *ports.AttemptSpendingPublicationGrantV1) {
				g.Authorization.SessionID = grants[1].Authorization.SessionID
			},
		} {
			forged := grants[0].Clone()
			mutate(&forged)
			raw, err := forged.Authorization.SourceCanonicalBytes()
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(raw)
			forged.Authorization.SourceAuthorization.Digest = hex.EncodeToString(sum[:])
			forged.Authorization.SourceAuthorization.Bytes = uint64(len(raw))
			if forged.Validate() != nil || forged.Authorization.ValidateAgainstProfile(profile.Profile, f.at) != nil {
				t.Fatal("coherent cross-invocation control failed before original Source authentication")
			}
			if source.VerifyAttemptSpendingPublication(forged, f.at) == nil {
				t.Fatal("original Source authenticated a coherently rebound invocation")
			}
			writes := f.execCalls
			got, err := store.PublishAttemptSpending(t.Context(), forged)
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.AttemptSpendingPublicationReceiptV1{}) || f.execCalls != writes {
				t.Fatalf("cross-invocation source substitution mutated original publication: %v", err)
			}
		}
	})
}

func TestLocalSearchPublicationAtomicRecovery(t *testing.T) {
	for _, mode := range []string{"ambiguous", "unknown", "rollback", "audit_failure", "retry_expiry"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source, store, f, profile, _ := localSearchOperatorStore(t)
				g, err := source.SpendingPublication(t.Context(), localSearchScope(profile.Profile, mode), domain.AttemptSpendingWebSearchV1)
				if err != nil {
					t.Fatal(err)
				}
				registerActualFundingKey(f, g)
				f.at = g.IssuedAt
				switch mode {
				case "audit_failure":
					f.failFundingAudit = true
				case "retry_expiry":
					f.commitModes = []string{"rollback"}
					f.afterCommit = func() {
						// Controlled logical expiry, not a wall-clock readiness sleep.
						time.Sleep(g.ExpiresAt.Sub(time.Now()))
						f.at = time.Now().UTC().Truncate(time.Microsecond)
					}
				default:
					f.commitModes = []string{mode}
				}
				r, err := store.PublishAttemptSpending(t.Context(), g)
				if mode == "audit_failure" || mode == "retry_expiry" {
					if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(r, ports.AttemptSpendingPublicationReceiptV1{}) || len(f.fundingHeads) != 0 || len(f.fundingAudits) != 0 {
						t.Fatalf("failed/expired transaction escaped search funding pair: %v", err)
					}
					return
				}
				if mode == "unknown" {
					if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(r, ports.AttemptSpendingPublicationReceiptV1{}) {
						t.Fatal("unknown commit returned speculative search success")
					}
				} else if err != nil || mode == "ambiguous" && !r.Replayed {
					t.Fatalf("original search funding recovery %s: replay=%t err=%v", mode, r.Replayed, err)
				}
				if len(f.fundingHeads) != 1 || len(f.fundingAudits) != 1 {
					t.Fatalf("search funding pair not atomic: heads=%d audits=%d", len(f.fundingHeads), len(f.fundingAudits))
				}
				writes := f.execCalls
				recovered, err := store.PublishAttemptSpending(t.Context(), g)
				if err != nil || !recovered.Replayed || f.execCalls != writes {
					t.Fatalf("same original intent could not recover search receipt: %v", err)
				}
			})
		})
	}
}

func TestLocalSearchPublicationDeniesRetiredCurrentSource(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, store, f, profile, manifest := localSearchOperatorStore(t)
		g, err := source.SpendingPublication(t.Context(), localSearchScope(profile.Profile, "stale"), domain.AttemptSpendingWebSearchV1)
		if err != nil {
			t.Fatal(err)
		}
		registerActualFundingKey(f, g)
		child, replacement, _, _ := actualLocalReplacement(t, source, profile, manifest)
		store.ownedProfilePublicationVerifier = child
		f.at = replacement.IssuedAt
		if _, err := store.PublishOwnedProfile(t.Context(), replacement); err != nil {
			t.Fatalf("actual replacement prerequisite: %v", err)
		}
		if source.VerifyAttemptSpendingPublication(g, f.at) != nil {
			t.Fatal("stale test lost authentic original source before the current SQL gate")
		}
		queries, writes := len(f.statements), f.execCalls
		got, err := store.PublishAttemptSpending(t.Context(), g)
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.AttemptSpendingPublicationReceiptV1{}) || f.execCalls != writes || len(f.fundingHeads) != 0 || len(f.fundingAudits) != 0 {
			t.Fatalf("retired original source admitted new search spending: %v", err)
		}
		joined := false
		for _, statement := range f.statements[queries:] {
			joined = joined || statement == ownedProfileCurrentSQL
		}
		if !joined {
			t.Fatal("stale search source denied before exercising actual current-profile SQL")
		}
	})
}
