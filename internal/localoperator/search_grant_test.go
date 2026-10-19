package localoperator_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func searchResourceSemantic(body []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte("sessionless.web-search-resource-grant.v2\x00"))
	_, _ = h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func searchEvidence(t *testing.T, id string, value any) localoperator.EvidenceBodyV1 {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return fitEvidence(id, body)
}

// Actual original private-file intake with synthetic owner-accepted snapshots.
// No executable is run, service is contacted, price is vendor-verified, account
// is credited, or genuine MODEL/current Store admission is claimed by this test.
func searchSourceFiles(t *testing.T) (string, string, localoperator.ManifestV1, domain.WebSearchResourceGrantV2) {
	t.Helper()
	root, exe, m := sourceFitFiles(t)
	at, end := m.InvoiceTerms.IssuedAt, m.InvoiceTerms.ExpiresAt
	pricePayload := fitEvidence("owner-accepted-price-payload", []byte("synthetic frozen quote bytes; not a current HTTP response"))
	termsPayload := fitEvidence("owner-accepted-terms-payload", []byte("synthetic frozen terms bytes; not vendor/legal proof"))
	price := localoperator.OwnerAcceptedSearchPriceV2{Version: 2, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Origin: "https://www.tavily.com/pricing", ObservedAt: at, ExpiresAt: end,
		Payload: pricePayload.Reference, PriceRevision: "accepted-basic-price.v2", Currency: "USD", Unit: "request", TariffMicrounits: 8000}
	terms := localoperator.OwnerAcceptedSearchTermsV2{Version: 2, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Origin: "https://www.tavily.com/terms", ObservedAt: at, ExpiresAt: end,
		Payload: termsPayload.Reference, DataPolicyDigest: strings.Repeat("a", 64), CredentialAudience: domain.WebSearchCredentialAudienceV1}
	priceBody, termsBody := searchEvidence(t, "accepted-search-price", price), searchEvidence(t, "accepted-search-terms", terms)
	g := domain.WebSearchResourceGrantV2{Version: 2, TenantID: m.Configuration.TenantID, OwnerUserID: m.Configuration.OwnerUserID, BillingOwnerUserID: m.Configuration.OwnerUserID,
		ResourceID: "owner-search-resource-not-the-local-model", ResourceRevision: 7, RevocationGeneration: 9, ConsentRevision: 11,
		ProfileID: domain.WebSearchProfileV1, ProfileRevision: 3, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Endpoint: domain.WebSearchEndpointV1, Method: "POST",
		CredentialRef: "owner-search-credential-reference-only", CredentialAudience: domain.WebSearchCredentialAudienceV1,
		DataPolicyDigest: terms.DataPolicyDigest, APISchemaDigest: strings.Repeat("b", 64), AdapterArtifactDigest: strings.Repeat("c", 64),
		Currency: "USD", PriceRevision: price.PriceRevision, PriceEvidenceDigest: priceBody.Reference.Digest, TermsEvidenceDigest: termsBody.Reference.Digest,
		PerCallReservationMicrounits: 10000, AggregateReservationMicrounits: 20000, IssuedAt: at, ExpiresAt: end}
	grantBody := searchEvidence(t, "original-resource-grant-body", g)
	semantic := searchResourceSemantic(grantBody.Body)
	m.Profile.WebSearchGrantDigest = semantic
	m.Profile.WebSearchResourceGrant = &domain.WebSearchResourceGrantBodyV2{Version: 2, Body: grantBody.Reference}
	amount := uint64(20000)
	m.Profile.CostCeiling.WebSearchCost = &domain.ExternalServiceCostCeilingV1{Version: 1, Kind: domain.ExternalServiceWebSearchV1,
		TenantID: g.TenantID, OwnerUserID: g.OwnerUserID, BillingOwnerUserID: g.BillingOwnerUserID,
		ResourceID: g.ResourceID, ResourceRevision: g.ResourceRevision, RevocationGeneration: g.RevocationGeneration, ProfileID: g.ProfileID, ProfileRevision: g.ProfileRevision,
		ScopedGrantDigest: semantic, Currency: "USD", PriceRevision: g.PriceRevision, PriceObservedAt: at, PriceExpiresAt: end,
		PriceEvidenceDigest: g.PriceEvidenceDigest, TermsEvidenceDigest: g.TermsEvidenceDigest, PriceState: domain.ExternalServicePriceKnownPaidV1, MaxAmountMicrounits: &amount}
	m.Profile.CostCeiling.PriceObservedAt, m.Profile.CostCeiling.PriceExpiresAt = at, end
	m.Profile.CostCeiling.MCPToolCost.PriceObservedAt, m.Profile.CostCeiling.MCPToolCost.PriceExpiresAt = at, end
	m.Profile.CostCeiling.MaxTotalAmountMicrounits = &amount
	m.SearchExpenditure = &localoperator.OwnerLocalSearchExpenditureV2{Version: 2, TenantID: g.TenantID, OwnerUserID: g.OwnerUserID, BillingOwnerUserID: g.BillingOwnerUserID,
		ResourceID: g.ResourceID, ResourceRevision: g.ResourceRevision, RevocationGeneration: g.RevocationGeneration, GrantDigest: semantic, GrantBody: grantBody.Reference,
		PriceEvidence: priceBody.Reference, TermsEvidence: termsBody.Reference, Currency: "USD", PerCallReservationMicrounits: 10000, AggregateReservationMicrounits: 20000, IssuedAt: at, ExpiresAt: end}
	m.Evidence = append(m.Evidence, pricePayload, termsPayload, priceBody, termsBody, grantBody)
	// The original substrate/cost association must describe the amended exact
	// amount. This is a structural source template, not a replacement resource.
	rawCost, err := m.Profile.CostCeiling.Digest()
	if err != nil {
		t.Fatal("amended cost fixture invalid before source intake", err)
	}
	m.Profile.Substrate.AdmissionCostCeilingDigest = rawCost
	writeFitManifest(t, root, m)
	return root, exe, m, g
}

func acceptSearchSourceTestPostimage(t *testing.T, s *localoperator.Source) ports.OwnedProfilePublicationReceiptV1 {
	t.Helper()
	g, err := s.Publication()
	if err != nil {
		t.Fatal("original source publication unavailable", err)
	}
	digest, err := g.Profile.Digest()
	if err != nil {
		t.Fatal(err)
	}
	// Simulated exact storage postimage of this Source's actual intent. This is
	// not a Store commit/replay/currentness proof; root's joined tests own that.
	r := ports.OwnedProfilePublicationReceiptV1{Profile: g.Profile.Clone(), Digest: digest}
	if err := s.AcceptReceipt(r); err != nil {
		t.Fatal("source rejected its exact postimage DATA", err)
	}
	return r
}

func TestOwnerLocalSearchResourceGrantTwoAttemptsOriginalSource(t *testing.T) {
	synctest.Test(t, testOwnerLocalSearchResourceGrantTwoAttemptsOriginalSource)
}

func testOwnerLocalSearchResourceGrantTwoAttemptsOriginalSource(t *testing.T) {
	root, exe, m, grant := searchSourceFiles(t)
	s := openFitSource(t, t.Context(), root, exe)
	r := acceptSearchSourceTestPostimage(t, s)
	profileBefore, err := r.Profile.Digest()
	if err != nil {
		t.Fatal(err)
	}
	profileRef := domain.OwnedAgentProfileReferenceV1{Locator: r.Profile.Locator, RevocationGeneration: r.Profile.RevocationGeneration, Digest: r.Digest}
	foreignRef := profileRef
	foreignRef.Digest = strings.Repeat("f", 64)
	if body, err := s.ReadWebSearchResourceGrant(t.Context(), foreignRef, 8192); err == nil || body != nil {
		t.Fatal("foreign installed profile exposed original grant")
	}
	for _, cap := range []uint64{0, m.SearchExpenditure.GrantBody.Bytes - 1, 8193} {
		if body, err := s.ReadWebSearchResourceGrant(t.Context(), profileRef, cap); err == nil || body != nil {
			t.Fatalf("invalid/insufficient original materialization cap=%d exposed grant", cap)
		}
	}
	body, err := s.ReadWebSearchResourceGrant(t.Context(), profileRef, domain.WebSearchResourceGrantMaxBytesV2)
	if err != nil || uint64(len(body)) != m.SearchExpenditure.GrantBody.Bytes || fitDigest(body) != m.SearchExpenditure.GrantBody.Digest || searchResourceSemantic(body) != m.SearchExpenditure.GrantDigest {
		t.Fatalf("original retained grant custody bytes=%d err=%v", len(body), err)
	}
	clear(body)
	again, err := s.ReadWebSearchResourceGrant(t.Context(), profileRef, domain.WebSearchResourceGrantMaxBytesV2)
	if err != nil || searchResourceSemantic(again) != m.SearchExpenditure.GrantDigest {
		t.Fatal("returned grant body aliases original keeper", err)
	}
	var publications []ports.AttemptSpendingPublicationGrantV1
	for _, identity := range []struct{ session, run, attempt string }{{"session-A", "run-A", "attempt-A"}, {"session-B", "run-B", "attempt-B"}} {
		scope := domain.AttemptExecutionSourceScopeV1{TenantID: grant.TenantID, OwnerUserID: grant.OwnerUserID, SessionID: domain.SessionID(identity.session), RunID: domain.RunID(identity.run), AttemptID: domain.AttemptID(identity.attempt)}
		g, err := s.SpendingPublication(t.Context(), scope, domain.AttemptSpendingWebSearchV1)
		if err != nil {
			t.Fatalf("actual original source denied accepted search expenditure for %s: %v", identity.attempt, err)
		}
		v := g.Authorization
		if v.Locator.ComponentKind != domain.AttemptSpendingWebSearchV1 || v.Locator.AttemptID != scope.AttemptID || v.RunID != scope.RunID || v.SessionID != scope.SessionID || v.SourceProfile.Digest != r.Digest ||
			v.ResourceID != grant.ResourceID || v.ResourceID == r.Profile.Resource.ResourceID || v.ResourceRevision != grant.ResourceRevision || v.ResourceGeneration != grant.RevocationGeneration || v.ScopedGrantDigest != m.SearchExpenditure.GrantDigest ||
			v.PriceState != domain.ExternalServicePriceKnownPaidV1 || v.MaxAmountMicrounits == nil || *v.MaxAmountMicrounits != 20000 || v.SourceAuthorization.Ref == m.SearchExpenditure.GrantBody.Ref {
			t.Fatalf("search publication confused source/resource/amount: %+v", v)
		}
		if err := s.VerifyAttemptSpendingPublication(g, g.IssuedAt); err != nil {
			t.Fatal("original source could not authenticate exact search publication", err)
		}
		publications = append(publications, g)
	}
	a, _ := publications[0].Authorization.SourceCanonicalBytes()
	b, _ := publications[1].Authorization.SourceCanonicalBytes()
	if bytes.Equal(a, b) || publications[0].Authorization.SourceAuthorization == publications[1].Authorization.SourceAuthorization {
		t.Fatal("two Attempts reused the invocation-specific source envelope")
	}
	foreign := publications[0].Clone()
	foreign.Authorization.RunID = publications[1].Authorization.RunID
	foreign.Authorization.Locator.AttemptID = publications[1].Authorization.Locator.AttemptID
	changedSource, err := foreign.Authorization.SourceCanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	foreign.Authorization.SourceAuthorization.Digest = fitDigest(changedSource)
	foreign.Authorization.SourceAuthorization.Bytes = uint64(len(changedSource))
	if err := foreign.Authorization.ValidateAgainstProfile(r.Profile, foreign.IssuedAt); err != nil {
		t.Fatalf("coherent substitution control invalid before original Source authentication: %v", err)
	}
	if err := s.VerifyAttemptSpendingPublication(foreign, foreign.IssuedAt); err == nil {
		t.Fatal("source authenticated cross-Attempt substitution")
	}
	after, err := r.Profile.Digest()
	if err != nil || profileBefore != after || r.Profile.WebSearchGrantDigest != m.Profile.WebSearchGrantDigest {
		t.Fatal("two Attempts churned installed resource profile", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if body, err := s.ReadWebSearchResourceGrant(t.Context(), profileRef, 8192); err == nil || body != nil {
		t.Fatal("closed original source exposed resource grant")
	}
}

func TestOwnerLocalSearchResourceGrantRejectMarkersAndMismatchedEvidence(t *testing.T) {
	for _, mode := range []string{"no-expenditure", "foreign-payer", "wrong-resource", "wrong-grant", "missing-grant-body", "wrong-grant-size", "missing-price-payload", "changed-terms-body", "legacy-grant-arm", "expired-acceptance"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root, exe, m, _ := searchSourceFiles(t)
				switch mode {
				case "no-expenditure":
					m.SearchExpenditure = nil // known_paid and arbitrary review bytes remain
				case "foreign-payer":
					m.SearchExpenditure.BillingOwnerUserID = "other-payer"
				case "wrong-resource":
					m.SearchExpenditure.ResourceID = "different-search-resource"
				case "wrong-grant":
					m.SearchExpenditure.GrantDigest = m.SearchExpenditure.GrantBody.Digest
				case "missing-grant-body":
					m.Evidence = m.Evidence[:len(m.Evidence)-1]
				case "wrong-grant-size":
					m.SearchExpenditure.GrantBody.Bytes++
				case "missing-price-payload":
					m.Evidence = append(m.Evidence[:4:4], m.Evidence[5:]...)
				case "changed-terms-body":
					m.Evidence[len(m.Evidence)-2].Body = []byte(`{"version":2,"known_paid":true}`)
				case "legacy-grant-arm":
					m.Profile.WebSearchResourceGrant = nil
				case "expired-acceptance":
					m.SearchExpenditure.ExpiresAt = m.SearchExpenditure.IssuedAt
				}
				writeFitManifest(t, root, m)
				s, err := localoperator.OpenFresh(t.Context(), root, exe)
				if err == nil {
					t.Cleanup(func() { _ = s.Close() })
					r := acceptSearchSourceTestPostimage(t, s)
					ref := domain.OwnedAgentProfileReferenceV1{Locator: r.Profile.Locator, RevocationGeneration: r.Profile.RevocationGeneration, Digest: r.Digest}
					if body, err := s.ReadWebSearchResourceGrant(t.Context(), ref, 8192); err == nil || body != nil {
						t.Fatalf("%s released grant through missing/mismatched original expenditure facts", mode)
					}
					scope := domain.AttemptExecutionSourceScopeV1{TenantID: m.Configuration.TenantID, OwnerUserID: m.Configuration.OwnerUserID, SessionID: "negative-session", RunID: "negative-run", AttemptID: "negative-attempt"}
					if g, err := s.SpendingPublication(t.Context(), scope, domain.AttemptSpendingWebSearchV1); err == nil || !reflect.DeepEqual(g, ports.AttemptSpendingPublicationGrantV1{}) {
						t.Fatalf("%s obtained paid search publication: %+v err=%v", mode, g, err)
					}
				}
			})
		})
	}
}

func TestOwnerLocalSearchResourceGrantNoOwnerNoAuthority(t *testing.T) {
	var absent *localoperator.Source
	ref := domain.OwnedAgentProfileReferenceV1{}
	for _, s := range []*localoperator.Source{absent, new(localoperator.Source)} {
		if body, err := s.ReadWebSearchResourceGrant(context.Background(), ref, 8192); err == nil || body != nil {
			t.Fatal("absent original keeper exposed grant")
		}
		if g, err := s.SpendingPublication(t.Context(), domain.AttemptExecutionSourceScopeV1{}, domain.AttemptSpendingWebSearchV1); err == nil || !reflect.DeepEqual(g, ports.AttemptSpendingPublicationGrantV1{}) {
			t.Fatal("absent original keeper issued paid publication")
		}
	}
}

// Coherently rebind immutable test DATA after changing a declaration. This
// does not issue a grant/publication or replace the original Source/Store owner.
func replaceSearchEvidence(t *testing.T, m *localoperator.ManifestV1, ref domain.OwnedAgentAuthorityBodyRefV1, body []byte) domain.OwnedAgentAuthorityBodyRefV1 {
	t.Helper()
	for i := range m.Evidence {
		if m.Evidence[i].Reference == ref {
			m.Evidence[i] = fitEvidence(ref.Ref, body)
			return m.Evidence[i].Reference
		}
	}
	t.Fatal("test-only declared original evidence is absent")
	return domain.OwnedAgentAuthorityBodyRefV1{}
}

func rebindSearchGrant(t *testing.T, m *localoperator.ManifestV1, g domain.WebSearchResourceGrantV2) {
	t.Helper()
	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	ref := replaceSearchEvidence(t, m, m.Profile.WebSearchResourceGrant.Body, raw)
	digest := searchResourceSemantic(raw)
	m.Profile.WebSearchResourceGrant.Body = ref
	m.Profile.WebSearchGrantDigest = digest
	m.SearchExpenditure.GrantBody, m.SearchExpenditure.GrantDigest = ref, digest
	c := m.Profile.CostCeiling.WebSearchCost
	c.ScopedGrantDigest, c.PriceEvidenceDigest, c.TermsEvidenceDigest = digest, g.PriceEvidenceDigest, g.TermsEvidenceDigest
	cd, err := m.Profile.CostCeiling.Digest()
	if err != nil {
		t.Fatal("coherent declaration cost fixture invalid", err)
	}
	m.Profile.Substrate.AdmissionCostCeilingDigest = cd
}

func TestOwnerLocalSearchResourceGrantClosedAcceptedSnapshots(t *testing.T) {
	for _, mode := range []string{"price-unknown", "price-duplicate", "price-alias", "price-null", "price-zero", "price-over-reservation", "terms-unknown", "terms-duplicate", "terms-alias", "terms-null"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root, exe, m, g := searchSourceFiles(t)
				ref := m.SearchExpenditure.PriceEvidence
				isTerms := strings.HasPrefix(mode, "terms-")
				if isTerms {
					ref = m.SearchExpenditure.TermsEvidence
				}
				var original []byte
				for _, b := range m.Evidence {
					if b.Reference == ref {
						original = b.Body
					}
				}
				if original == nil {
					t.Fatal("snapshot fixture absent")
				}
				changed := string(original)
				switch mode {
				case "price-unknown", "terms-unknown":
					changed = strings.TrimSuffix(changed, "}") + `,"known_paid":true}`
				case "price-duplicate", "terms-duplicate":
					changed = strings.Replace(changed, `"version":2`, `"version":2,"version":2`, 1)
				case "price-alias", "terms-alias":
					changed = strings.Replace(changed, `"version":2`, `"Version":2`, 1)
				case "price-null", "terms-null":
					changed = strings.Replace(changed, `"version":2`, `"version":null`, 1)
				case "price-zero":
					changed = strings.Replace(changed, `"tariff_microunits":8000`, `"tariff_microunits":0`, 1)
				case "price-over-reservation":
					changed = strings.Replace(changed, `"tariff_microunits":8000`, `"tariff_microunits":10001`, 1)
				}
				if changed == string(original) {
					t.Fatal("causal snapshot mutation did not alter bytes")
				}
				newRef := replaceSearchEvidence(t, &m, ref, []byte(changed))
				if isTerms {
					m.SearchExpenditure.TermsEvidence, g.TermsEvidenceDigest = newRef, newRef.Digest
				} else {
					m.SearchExpenditure.PriceEvidence, g.PriceEvidenceDigest = newRef, newRef.Digest
				}
				rebindSearchGrant(t, &m, g)
				writeFitManifest(t, root, m)
				s, err := localoperator.OpenFresh(t.Context(), root, exe)
				if err == nil {
					_ = s.Close()
					t.Fatal("coherently rebound malformed snapshot entered real Source")
				}
				if !strings.Contains(err.Error(), "search expenditure") {
					t.Fatalf("snapshot negative failed before the actual acceptance gate: %v", err)
				}
			})
		})
	}
}

func TestOwnerLocalSearchResourceGrantOriginalMinimumWindows(t *testing.T) {
	for _, mode := range []string{"grant", "expenditure", "price", "terms", "profile", "parent", "attempt"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root, exe, m, g := searchSourceFiles(t)
				now := time.Now().UTC().Truncate(time.Microsecond)
				stop := now.Add(30 * time.Second)
				ctx := t.Context()
				switch mode {
				case "grant":
					g.ExpiresAt = stop
				case "expenditure":
					m.SearchExpenditure.ExpiresAt = stop
				case "price", "terms":
					ref := m.SearchExpenditure.PriceEvidence
					if mode == "terms" {
						ref = m.SearchExpenditure.TermsEvidence
					}
					var fields map[string]json.RawMessage
					for _, b := range m.Evidence {
						if b.Reference == ref {
							if err := json.Unmarshal(b.Body, &fields); err != nil {
								t.Fatal(err)
							}
						}
					}
					// Use the declared concrete schema to retain canonical field order.
					var body []byte
					if mode == "price" {
						var p localoperator.OwnerAcceptedSearchPriceV2
						raw, _ := json.Marshal(fields)
						if err := json.Unmarshal(raw, &p); err != nil {
							t.Fatal(err)
						}
						p.ExpiresAt = stop
						body, _ = json.Marshal(p)
						m.Profile.CostCeiling.WebSearchCost.PriceExpiresAt = stop
						// An independent component price interval must contain the
						// enclosing executable price interval under the original cost contract.
						m.Profile.CostCeiling.PriceExpiresAt = stop
						// The original profile also cannot outlive its enclosing price;
						// this case verifies their lawful coupled minimum, not an invalid isolation.
						m.Profile.Window.ExpiresAt = stop
					} else {
						var p localoperator.OwnerAcceptedSearchTermsV2
						raw, _ := json.Marshal(fields)
						if err := json.Unmarshal(raw, &p); err != nil {
							t.Fatal(err)
						}
						p.ExpiresAt = stop
						body, _ = json.Marshal(p)
					}
					newRef := replaceSearchEvidence(t, &m, ref, body)
					if mode == "price" {
						m.SearchExpenditure.PriceEvidence, g.PriceEvidenceDigest = newRef, newRef.Digest
					} else {
						m.SearchExpenditure.TermsEvidence, g.TermsEvidenceDigest = newRef, newRef.Digest
					}
				case "profile":
					m.Profile.Window.ExpiresAt = stop // profile is already <= config expiry
				case "parent":
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, stop)
					t.Cleanup(cancel)
				case "attempt":
					stop = now.Add(m.Profile.Limits.MaxActiveDuration + m.Profile.Limits.MaxCleanupDuration)
				}
				rebindSearchGrant(t, &m, g)
				writeFitManifest(t, root, m)
				s := openFitSource(t, ctx, root, exe)
				_ = acceptSearchSourceTestPostimage(t, s)
				scope := domain.AttemptExecutionSourceScopeV1{TenantID: g.TenantID, OwnerUserID: g.OwnerUserID, SessionID: "window-session", RunID: "window-run", AttemptID: "window-attempt"}
				publication, err := s.SpendingPublication(ctx, scope, domain.AttemptSpendingWebSearchV1)
				if err != nil || !publication.Authorization.Window.ExpiresAt.Equal(stop) {
					t.Fatalf("original %s minimum got=%s want=%s err=%v", mode, publication.Authorization.Window.ExpiresAt, stop, err)
				}
				if mode == "attempt" {
					return // a new original Attempt can have its own finite allowance
				}
				// Explicit fake-time expiry, not sleep-based readiness or a real TTL.
				time.Sleep(stop.Sub(time.Now()))
				if again, err := s.SpendingPublication(ctx, scope, domain.AttemptSpendingWebSearchV1); err == nil || !reflect.DeepEqual(again, ports.AttemptSpendingPublicationGrantV1{}) {
					t.Fatalf("expired %s reopened search spending err=%v", mode, err)
				}
			})
		})
	}
}
