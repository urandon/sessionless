package ydbstore

import (
	"bytes"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// These closed, synthetic documents are only operator declarations. Real file
// intake, receipts, source authentication and Store publications are exercised;
// no live tariff, model, MCP/search send or native database is claimed.
func localSourceMCPDeclaration(t *testing.T, m *localoperator.ManifestV1) {
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
	p, cost := &m.Profile, m.Profile.CostCeiling.MCPToolCost
	if cost == nil || cost.MaxAmountMicrounits == nil {
		t.Fatal("missing original finite MCP cost")
	}
	grant := mcpadmission.MCPToolGrantV1{Version: 1, TenantID: cost.TenantID, OwnerUserID: cost.OwnerUserID, BillingOwnerUserID: cost.BillingOwnerUserID,
		ResourceID: cost.ResourceID, ResourceRevision: cost.ResourceRevision, RevocationGeneration: cost.RevocationGeneration, ProfileID: cost.ProfileID, ProfileRevision: cost.ProfileRevision,
		DataPolicyDigest: p.PrivacyPolicyDigest, DataClass: "public", ConsentMode: "owner_public_query", ConsentRevision: 1,
		Endpoint: "https://developers.openai.com/mcp", Transport: "streamable_http", ProtocolVersion: mcpwire.ProtocolVersionV1, SchemaValidator: domain.ConfiguredToolSchemaValidatorV1, ModelDescription: "synthetic installed docs lookup",
		Descriptor: json.RawMessage(`{"annotations":{"readOnlyHint":true},"description":"synthetic descriptor, not a live catalog","inputSchema":{"additionalProperties":false,"properties":{"query":{"maxLength":30,"minLength":1,"type":"string"}},"required":["query"],"type":"object"},"name":"search_openai_docs"}`),
		Limits:     mcpwire.LimitsV1{MessageBytes: 64 << 10, AggregateBytes: 256 << 10, JSONDepth: 32, JSONMembers: 4096, SSELineBytes: 64 << 10, SSEEventBytes: 64 << 10, SSEEvents: 128, Tools: 1, DescriptorBytes: 32 << 10, ArgumentBytes: 4 << 10, ResultTextBytes: 4096, ContentItems: 16},
		IssuedAt:   m.Configuration.IssuedAt, ExpiresAt: m.Configuration.ExpiresAt}
	grantBody := body("original-mcp-grant", grant)
	priceDoc, termsDoc := body("original-mcp-price-document", "synthetic independent service price document"), body("original-mcp-terms-document", "synthetic independent service terms document")
	price := body("original-mcp-price-report", mcpadmission.MCPPriceEvidenceV1{Version: 1, GrantDigest: grantBody.Reference.Digest, Currency: cost.Currency, PriceRevision: cost.PriceRevision, PriceState: cost.PriceState, MaxAmountMicrounits: cost.MaxAmountMicrounits, SourceURL: "https://evidence.example.invalid/mcp-pricing", SourceBody: priceDoc.Reference, ObservedAt: cost.PriceObservedAt, ExpiresAt: cost.PriceExpiresAt})
	terms := body("original-mcp-terms-report", mcpadmission.MCPTermsEvidenceV1{Version: 1, GrantDigest: grantBody.Reference.Digest, SourceURL: "https://evidence.example.invalid/mcp-terms", SourceBody: termsDoc.Reference, ObservedAt: cost.PriceObservedAt, ExpiresAt: cost.PriceExpiresAt})
	cost.ScopedGrantDigest, p.MCPToolGrantDigest = grantBody.Reference.Digest, grantBody.Reference.Digest
	cost.PriceEvidenceDigest, cost.TermsEvidenceDigest = price.Reference.Digest, terms.Reference.Digest
	var err error
	p.Substrate.AdmissionCostCeilingDigest, err = p.CostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	m.Evidence = append(m.Evidence, grantBody, price, terms, priceDoc, termsDoc)
}

func localSourceFourPublications(t *testing.T) (*localoperator.Source, *Store, *localInstallationSQLFixture, ports.OwnedProfilePublicationReceiptV1, domain.OwnedAgentExecutionPackageV1) {
	t.Helper()
	source, store, f, receipt, _ := localSearchOperatorStore(t, func(m *localoperator.ManifestV1) { localSourceMCPDeclaration(t, m) })
	scope := localSearchScope(receipt.Profile, "four-components")
	pkg := localSourcePublishPackage(t, source, store, f, receipt, scope)
	return source, store, f, receipt, pkg
}

func localSourcePublishPackage(t *testing.T, source *localoperator.Source, store *Store, f *localInstallationSQLFixture, receipt ports.OwnedProfilePublicationReceiptV1, scope domain.AttemptExecutionSourceScopeV1) domain.OwnedAgentExecutionPackageV1 {
	t.Helper()
	var spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	issued := receipt.Profile.Window.PublishedAt
	for i, kind := range [4]domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1} {
		g, err := source.SpendingPublication(t.Context(), scope, kind)
		if err != nil {
			t.Fatalf("actual Source %s publication: %v", kind, err)
		}
		registerActualFundingKey(f, g)
		f.at = g.IssuedAt
		r, err := store.PublishAttemptSpending(t.Context(), g)
		if err != nil {
			t.Fatalf("actual original Store %s publication: %v", kind, err)
		}
		spending[i] = r.Authorization
		if r.Authorization.Window.PublishedAt.After(issued) {
			issued = r.Authorization.Window.PublishedAt
		}
	}
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, receipt.Profile, spending, issued)
	if err != nil {
		t.Fatalf("genuine four-head package: %v", err)
	}
	return pkg
}

// Model an already persisted exact original profile postimage. This is
// explicitly scripted SQL, NOT a fresh profile publication or current-state
// authority from receipt DATA. Actual Source funding and Store writer still
// authenticate and point-join it for each original component below.
func localSourceRecoveredFundingStore(t *testing.T, source *localoperator.Source, receipt ports.OwnedProfilePublicationReceiptV1) (*Store, *localInstallationSQLFixture) {
	t.Helper()
	store, f, _, _ := newLocalInstallationFixture(t)
	p := receipt.Profile
	raw, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
	if err != nil {
		t.Fatal(err)
	}
	f.heads[localInstallationHeadKey(p)] = []driver.Value{string(raw), int64(p.Version), int64(p.ProfileRevision), int64(p.RevocationGeneration), string(p.State), receipt.Digest, p.InstallationID, int64(p.InstallationRevision), p.Window.PublishedAt, p.Window.ExpiresAt, p.UpdatedAt}
	store.attemptSpendingPublicationVerifier = source
	return store, f
}

func TestLocalSourceVerifierInstallationImplementsOriginalPort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, _, _, _, _ := localSearchOperatorStore(t)
		if _, ok := any(source).(ports.OwnedAttemptEffectSourceVerifierV1); !ok {
			t.Fatal("original installation Source does not implement the original effect-source verifier port")
		}
	})
}

func TestLocalSourceVerifierActualFourPublications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, _, f, receipt, pkg := localSourceFourPublications(t)
		verifier, ok := any(source).(ports.OwnedAttemptEffectSourceVerifierV1)
		if !ok {
			t.Fatal("genuine four-head Source verifier missing")
		}
		before := f.execCalls
		if err := verifier.VerifyOwnedAttemptEffectSource(pkg.Source); err != nil {
			t.Fatalf("authentic original package: %v", err)
		}
		if f.execCalls != before || len(f.fundingHeads) != 4 || len(f.fundingAudits) != 4 {
			t.Fatal("verification wrote or lost original four publications")
		}
		for _, mutate := range []func(*domain.AttemptExecutionSourceV1){
			func(v *domain.AttemptExecutionSourceV1) {
				v.AgentLoopPolicyDigest = domain.AgentLoopPolicyDigestV1(strings.Repeat("f", 64))
			},
			func(v *domain.AttemptExecutionSourceV1) {
				v.HarnessBindingDigest = domain.HarnessBindingDigestV2(strings.Repeat("f", 64))
			},
			func(v *domain.AttemptExecutionSourceV1) { v.Spending[2], v.Spending[3] = v.Spending[3], v.Spending[2] },
		} {
			bad := pkg.Source.Clone()
			mutate(&bad)
			if verifier.VerifyOwnedAttemptEffectSource(bad) == nil {
				t.Fatal("modified original projection authenticated")
			}
		}
		// This differs only in a sealed source field and recomputed public hash;
		// the current shape/package are valid but the original private MAC is not.
		bad := pkg.Source.Clone()
		bad.Scope.SessionID = "coherently-foreign-session"
		for i := range bad.Spending {
			bad.Spending[i].SessionID = bad.Scope.SessionID
			raw, err := bad.Spending[i].SourceCanonicalBytes()
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(raw)
			bad.Spending[i].SourceAuthorization.Digest, bad.Spending[i].SourceAuthorization.Bytes = hex.EncodeToString(sum[:]), uint64(len(raw))
		}
		coherent, err := domain.BuildOwnedAgentExecutionPackageV1(bad.Scope, receipt.Profile, bad.Spending, bad.IssuedAt)
		if err != nil {
			t.Fatalf("coherent foreign-scope control: %v", err)
		}
		if verifier.VerifyOwnedAttemptEffectSource(coherent.Source) == nil {
			t.Fatal("public rehash replaced original private Source authentication")
		}
		// Attempt expiry is historical authentication, not permission. Do not
		// destroy/expire the installation or manufacture a renewed execution.
		time.Sleep(time.Until(pkg.Source.ExpiresAt) + time.Microsecond)
		life, err := source.ExecutionLifetime()
		if err != nil || life.Err() != nil {
			t.Fatal("historical control accidentally expired original installation")
		}
		if err := verifier.VerifyOwnedAttemptEffectSource(pkg.Source); err != nil {
			t.Fatalf("original expired Attempt history: %v", err)
		}
		_ = source.Close()
		if verifier.VerifyOwnedAttemptEffectSource(pkg.Source) == nil {
			t.Fatal("destroyed original installation authenticated history")
		}
	})
}

func TestLocalSourceVerifierCompositionSameOriginal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, store, _, _, _ := localSearchOperatorStore(t)
		parent := newOwnedAttemptEffectParentFixture(t)
		operate, err := serverlessharness.NewOperationCapabilityIssuer(func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }, bytes.NewReader(bytes.Repeat([]byte{42}, 32)), 16)
		if err != nil {
			t.Fatal(err)
		}
		b, err := NewLocalOwnedStoreBootstrapV1(source, parent.issuer.issuer, operate)
		if err != nil {
			t.Fatal(err)
		}
		keeper := &localBootstrapDenyKeeper{identity: 1}
		if got, err := b.CreateStore(store.db, Options{OwnedAttemptEffectSourceVerifier: parent.verifier}, keeper); err == nil || got != nil {
			t.Fatal("constructor accepted a foreign fixture verifier instead of its original Source")
		}
		got, err := b.CreateStore(store.db, Options{}, keeper)
		if err != nil || got == nil || !reflect.DeepEqual(any(got.ownedAttemptEffectSourceVerifier), any(source)) {
			t.Fatalf("constructor did not install same original Source: %v", err)
		}
	})
}

func TestLocalSourceVerifierForeignCopyAndInstallationExpiry(t *testing.T) {
	for _, fault := range []string{"foreign-source", "copied-source", "installation-expired"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source, _, _, _, pkg := localSourceFourPublications(t)
				if err := source.VerifyOwnedAttemptEffectSource(pkg.Source); err != nil {
					t.Fatalf("original authentic control: %v", err)
				}
				verifier := source
				switch fault {
				case "foreign-source":
					other, _, _, _, _ := localSourceFourPublications(t)
					verifier = other
				case "copied-source":
					verifier = new(localoperator.Source)
					reflect.ValueOf(verifier).Elem().Set(reflect.ValueOf(source).Elem())
				case "installation-expired":
					life, err := source.ExecutionLifetime()
					if err != nil {
						t.Fatal(err)
					}
					deadline, ok := life.Deadline()
					if !ok {
						t.Fatal("original installation has no finite deadline")
					}
					time.Sleep(time.Until(deadline))
				}
				if verifier.VerifyOwnedAttemptEffectSource(pkg.Source) == nil {
					t.Fatalf("%s authenticated original installation history", fault)
				}
				if fault != "installation-expired" && source.VerifyOwnedAttemptEffectSource(pkg.Source) != nil {
					t.Fatal("foreign/copy verification damaged original Source")
				}
			})
		})
	}
}

func TestLocalSourceVerifierAllComponentMACsAndProjection(t *testing.T) {
	for _, component := range []int{0, 1, 2, 3} {
		t.Run(string(ownedSpendingKinds[component]), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source, _, f, receipt, pkg := localSourceFourPublications(t)
				bad := pkg.Source.Clone()
				ref := &bad.Spending[component].SourceAuthorization.Ref
				last := byte('0')
				if (*ref)[len(*ref)-1] == last {
					last = '1'
				}
				*ref = (*ref)[:len(*ref)-1] + string(last)
				coherent, err := domain.BuildOwnedAgentExecutionPackageV1(bad.Scope, receipt.Profile, bad.Spending, bad.IssuedAt)
				if err != nil {
					t.Fatalf("MAC-only control must remain structurally valid: %v", err)
				}
				before := f.execCalls
				if source.VerifyOwnedAttemptEffectSource(coherent.Source) == nil || f.execCalls != before {
					t.Fatal("one component lost original MAC or verifier performed SQL")
				}
				if source.VerifyOwnedAttemptEffectSource(pkg.Source) != nil {
					t.Fatal("failed candidate changed original Source")
				}
			})
		})
	}
}

func TestLocalSourceVerifierCurrentSQLRemainsIndependent(t *testing.T) {
	for _, fault := range []string{"current-profile-revoked", "current-mcp-revoked"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalBootstrapFixture(t)
				if err := f.bootstrap.Complete(f.bridge); err != nil {
					t.Fatal(err)
				}
				if f.actual.source.VerifyOwnedAttemptEffectSource(f.parent.source) != nil {
					t.Fatal("actual original package did not authenticate before SQL mutation")
				}
				if fault == "current-profile-revoked" {
					f.parent.base.reader.profile.State = domain.OwnedAgentCurrentRevokedV1
					f.parent.base.reader.profile.RevocationGeneration++
				} else {
					f.parent.base.reader.spending[2].State = domain.OwnedAgentCurrentRevokedV1
					f.parent.base.reader.spending[2].RevocationGeneration++
				}
				f.parent.base.reader.sync(t)
				if f.actual.source.VerifyOwnedAttemptEffectSource(f.parent.source) != nil {
					t.Fatal("historical source authentication incorrectly became current-row approval")
				}
				f.parent.deny(t, f.parent.request)
				if f.parent.sql.inserts != 0 || f.parent.sql.burnUpdates != 0 {
					t.Fatal("authenticated history bypassed current SQL revocation")
				}
			})
		})
	}
}

func TestLocalSourceVerifierMCPIndependentCustodyDenials(t *testing.T) {
	for _, fault := range []string{"missing-grant", "missing-price-report", "missing-terms-report", "missing-price-document", "missing-terms-document", "invoice-as-document", "grant-as-document", "foreign-report-grant", "unknown-price"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source, store, f, receipt, _ := localSearchOperatorStore(t, func(m *localoperator.ManifestV1) {
					localSourceMCPDeclaration(t, m)
					cost := m.Profile.CostCeiling.MCPToolCost
					var grantRef domain.OwnedAgentAuthorityBodyRefV1
					for _, evidence := range m.Evidence {
						if evidence.Reference.Ref == "original-mcp-grant" {
							grantRef = evidence.Reference
						}
					}
					var kept []localoperator.EvidenceBodyV1
					for _, evidence := range m.Evidence {
						if evidence.Reference.Ref == "original-mcp-"+strings.TrimPrefix(fault, "missing-") && strings.HasPrefix(fault, "missing-") {
							continue
						}
						if evidence.Reference.Ref == "original-mcp-price-report" && !strings.HasPrefix(fault, "missing-") {
							var price mcpadmission.MCPPriceEvidenceV1
							if err := json.Unmarshal(evidence.Body, &price); err != nil {
								t.Fatal(err)
							}
							switch fault {
							case "invoice-as-document":
								price.SourceBody = m.Configuration.OperatorTerms
							case "grant-as-document":
								price.SourceBody = grantRef
							case "foreign-report-grant":
								price.GrantDigest = strings.Repeat("e", 64)
							case "unknown-price":
								price.PriceState, price.MaxAmountMicrounits = domain.ExternalServicePriceUnknownV1, nil
							}
							raw, err := json.Marshal(price)
							if err != nil {
								t.Fatal(err)
							}
							sum := sha256.Sum256(raw)
							evidence.Body, evidence.Reference.Digest, evidence.Reference.Bytes = raw, hex.EncodeToString(sum[:]), uint64(len(raw))
							cost.PriceEvidenceDigest = evidence.Reference.Digest
						}
						kept = append(kept, evidence)
					}
					m.Evidence = kept
					var err error
					m.Profile.Substrate.AdmissionCostCeilingDigest, err = m.Profile.CostCeiling.Digest()
					if err != nil {
						t.Fatalf("coherent cost before actual Source intake: %v", err)
					}
				})
				before := f.execCalls
				if _, err := source.SpendingPublication(t.Context(), localSearchScope(receipt.Profile, fault), domain.AttemptSpendingMCPToolV1); err == nil {
					t.Fatal("missing/substituted independent MCP declaration became funding")
				}
				if f.execCalls != before || len(f.fundingHeads) != 0 || len(f.fundingAudits) != 0 || store.attemptSpendingPublicationVerifier != source {
					t.Fatal("rejected custody changed original Store/funding")
				}
			})
		})
	}
}
