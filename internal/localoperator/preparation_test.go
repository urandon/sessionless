//go:build darwin || linux

package localoperator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
)

// Extend only the existing synthetic declaration DATA. The executable is an
// owned, never-executed file; no model/service or SQL is contacted. Independent
// documents below are fixture bytes, not actual vendor/review qualification.
func preparedTemplateFiles(t *testing.T) (string, string, localoperator.ManifestV1) {
	t.Helper()
	root, exe, m, _ := searchSourceFiles(t)
	cost := m.Profile.CostCeiling.MCPToolCost
	grant := mcpadmission.MCPToolGrantV1{Version: 1, TenantID: cost.TenantID, OwnerUserID: cost.OwnerUserID, BillingOwnerUserID: cost.BillingOwnerUserID,
		ResourceID: cost.ResourceID, ResourceRevision: cost.ResourceRevision, RevocationGeneration: cost.RevocationGeneration,
		ProfileID: cost.ProfileID, ProfileRevision: cost.ProfileRevision, DataPolicyDigest: m.Profile.PrivacyPolicyDigest,
		DataClass: "public", ConsentMode: "owner_public_query", ConsentRevision: 1, Endpoint: "https://developers.openai.com/mcp",
		Transport: "streamable_http", ProtocolVersion: mcpwire.ProtocolVersionV1, SchemaValidator: domain.ConfiguredToolSchemaValidatorV1,
		ModelDescription: "Synthetic unused documentation lookup",
		Descriptor:       json.RawMessage(`{"annotations":{"readOnlyHint":true},"description":"synthetic declaration","inputSchema":{"additionalProperties":false,"properties":{"query":{"maxLength":30,"minLength":1,"type":"string"}},"required":["query"],"type":"object"},"name":"search_openai_docs"}`),
		Limits: mcpwire.LimitsV1{MessageBytes: 64 << 10, AggregateBytes: 256 << 10, JSONDepth: 32, JSONMembers: 4096,
			SSELineBytes: 64 << 10, SSEEventBytes: 64 << 10, SSEEvents: 128, Tools: 1, DescriptorBytes: 32 << 10,
			ArgumentBytes: 4 << 10, ResultTextBytes: 4096, ContentItems: 16}, IssuedAt: cost.PriceObservedAt, ExpiresAt: cost.PriceExpiresAt}
	grantBody := searchEvidence(t, "prepared-mcp-grant", grant)
	priceDoc := fitEvidence("prepared-mcp-price-document", []byte("independent synthetic price document"))
	termsDoc := fitEvidence("prepared-mcp-terms-document", []byte("independent synthetic terms document"))
	price := searchEvidence(t, "prepared-mcp-price", mcpadmission.MCPPriceEvidenceV1{Version: 1, GrantDigest: grantBody.Reference.Digest,
		Currency: cost.Currency, PriceRevision: cost.PriceRevision, PriceState: cost.PriceState, MaxAmountMicrounits: cost.MaxAmountMicrounits,
		SourceURL: "https://evidence.example.invalid/mcp-price", SourceBody: priceDoc.Reference, ObservedAt: cost.PriceObservedAt, ExpiresAt: cost.PriceExpiresAt})
	terms := searchEvidence(t, "prepared-mcp-terms", mcpadmission.MCPTermsEvidenceV1{Version: 1, GrantDigest: grantBody.Reference.Digest,
		SourceURL: "https://evidence.example.invalid/mcp-terms", SourceBody: termsDoc.Reference, ObservedAt: cost.PriceObservedAt, ExpiresAt: cost.PriceExpiresAt})
	m.Profile.MCPToolGrantDigest, cost.ScopedGrantDigest = grantBody.Reference.Digest, grantBody.Reference.Digest
	cost.PriceEvidenceDigest, cost.TermsEvidenceDigest = price.Reference.Digest, terms.Reference.Digest
	m.Evidence = append(m.Evidence, grantBody, priceDoc, termsDoc, price, terms)
	preparedCostDigest(t, &m)
	return root, exe, m
}

func preparedCostDigest(t *testing.T, m *localoperator.ManifestV1) {
	t.Helper()
	digest, err := m.Profile.CostCeiling.Digest()
	if err != nil {
		t.Fatal("synthetic amended cost DATA", err)
	}
	m.Profile.Substrate.AdmissionCostCeilingDigest = digest
}

func preparedRaw(t *testing.T, m localoperator.ManifestV1) []byte {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal("encode synthetic manifest DATA", err)
	}
	return raw
}

func replacePreparedBody(t *testing.T, m *localoperator.ManifestV1, digest string, value any) domain.OwnedAgentAuthorityBodyRefV1 {
	t.Helper()
	for i, body := range m.Evidence {
		if body.Reference.Digest == digest {
			m.Evidence[i] = searchEvidence(t, body.Reference.Ref, value)
			return m.Evidence[i].Reference
		}
	}
	t.Fatal("synthetic evidence to replace is absent")
	return domain.OwnedAgentAuthorityBodyRefV1{}
}

func removePreparedBody(t *testing.T, m *localoperator.ManifestV1, digest string) {
	t.Helper()
	for i, body := range m.Evidence {
		if body.Reference.Digest == digest {
			m.Evidence = append(m.Evidence[:i], m.Evidence[i+1:]...)
			return
		}
	}
	t.Fatal("synthetic evidence to remove is absent")
}

// Re-correlate every original declaration affected by a schema change. This
// makes schema-language rejection causal, not an incidental stale hash/report
// or funding mismatch. The independent document bodies remain unchanged.
func rebindPreparedMCPSchema(t *testing.T, m *localoperator.ManifestV1, schema json.RawMessage) {
	t.Helper()
	var grant mcpadmission.MCPToolGrantV1
	for _, body := range m.Evidence {
		if body.Reference.Digest == m.Profile.MCPToolGrantDigest {
			var err error
			grant, err = mcpadmission.ParseMCPToolGrantV1(body.Body)
			if err != nil {
				t.Fatal("original synthetic MCP grant", err)
			}
		}
	}
	var descriptor map[string]json.RawMessage
	if err := json.Unmarshal(grant.Descriptor, &descriptor); err != nil {
		t.Fatal(err)
	}
	descriptor["inputSchema"] = schema
	var err error
	grant.Descriptor, err = json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	grantRef := replacePreparedBody(t, m, m.Profile.MCPToolGrantDigest, grant)
	cost := m.Profile.CostCeiling.MCPToolCost
	m.Profile.MCPToolGrantDigest, cost.ScopedGrantDigest = grantRef.Digest, grantRef.Digest
	for _, body := range m.Evidence {
		if body.Reference.Digest == cost.PriceEvidenceDigest {
			price, err := mcpadmission.ParseMCPPriceEvidenceV1(body.Body)
			if err != nil {
				t.Fatal(err)
			}
			price.GrantDigest = grantRef.Digest
			cost.PriceEvidenceDigest = replacePreparedBody(t, m, body.Reference.Digest, price).Digest
			if err := price.ValidateForCost(grant, *cost, time.Now().UTC().Truncate(time.Microsecond)); err != nil {
				t.Fatal("rebound price relation", err)
			}
		}
		if body.Reference.Digest == cost.TermsEvidenceDigest {
			terms, err := mcpadmission.ParseMCPTermsEvidenceV1(body.Body)
			if err != nil {
				t.Fatal(err)
			}
			terms.GrantDigest = grantRef.Digest
			cost.TermsEvidenceDigest = replacePreparedBody(t, m, body.Reference.Digest, terms).Digest
			if err := terms.ValidateForCost(grant, *cost, time.Now().UTC().Truncate(time.Microsecond)); err != nil {
				t.Fatal("rebound terms relation", err)
			}
		}
	}
	preparedCostDigest(t, m)
	if err := grant.ValidateForCost(*cost, time.Now().UTC().Truncate(time.Microsecond)); err != nil {
		t.Fatal("rebound MCP grant/cost relation", err)
	}
	if _, err := mcpwire.ParseDescriptorV1(grant.Descriptor, grant.Limits); err != nil {
		t.Fatal("canonical descriptor control", err)
	}
}

func TestValidateManifestTemplateClosedMCPSchema(t *testing.T) {
	for _, check := range []struct {
		name     string
		schema   json.RawMessage
		accepted bool
	}{
		{name: "supported", schema: json.RawMessage(`{"additionalProperties":false,"properties":{"query":{"maxLength":30,"minLength":1,"type":"string"}},"required":["query"],"type":"object"}`), accepted: true},
		{name: "unsupported-allOf", schema: json.RawMessage(`{"additionalProperties":false,"allOf":[],"properties":{"query":{"maxLength":30,"minLength":1,"type":"string"}},"required":["query"],"type":"object"}`)},
		{name: "unsupported-ref", schema: json.RawMessage(`{"$ref":"#","additionalProperties":false,"properties":{"query":{"maxLength":30,"minLength":1,"type":"string"}},"required":["query"],"type":"object"}`)},
		{name: "unsupported-dialect", schema: json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"query":{"maxLength":30,"minLength":1,"type":"string"}},"required":["query"],"type":"object"}`)},
	} {
		t.Run(check.name, func(t *testing.T) {
			root, exe, m := preparedTemplateFiles(t)
			rebindPreparedMCPSchema(t, &m, check.schema)
			err := localoperator.ValidateManifestTemplate(t.Context(), preparedRaw(t, m), exe)
			if check.accepted && err != nil {
				t.Fatal("supported closed schema denied", err)
			}
			if !check.accepted && !errors.Is(err, localoperator.ErrDenied) {
				t.Fatal("fully correlated unsupported schema accepted", err)
			}
			if !check.accepted {
				// Stronger complete-preparation checks must not silently change
				// historical one-shot file intake into runtime qualification.
				writeFitManifest(t, root, m)
				_ = openFitSource(t, t.Context(), root, exe)
			}
		})
	}
}

func TestValidateManifestTemplateReadOnlyWithoutInstallationOrKey(t *testing.T) {
	root, exe, m := preparedTemplateFiles(t)
	// Remove the fixture's original private intake inputs. The public API has
	// only manifest bytes and executable, not an installation-root coordinate.
	for _, name := range []string{"manifest.json", "source.key"} {
		if err := os.Remove(filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	m.KeyFingerprint = strings.Repeat("f", 64) // form only, deliberately not the fixture key
	raw := preparedRaw(t, m)
	original := bytes.Clone(raw)
	before, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	executableBytes, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := localoperator.ValidateManifestTemplate(t.Context(), raw, exe); err != nil {
			t.Fatal("complete synthetic prepared DATA denied", err)
		}
	}
	after, err := os.ReadDir(root)
	if err != nil || !reflect.DeepEqual(before, after) || !bytes.Equal(raw, original) {
		t.Fatal("read-only validation changed manifest bytes or owned files", err)
	}
	afterBytes, err := os.ReadFile(exe)
	if err != nil || !bytes.Equal(executableBytes, afterBytes) {
		t.Fatal("read-only validation changed explicit executable", err)
	}
}

func TestValidateManifestTemplateEncodingAndContextDenials(t *testing.T) {
	_, exe, m := preparedTemplateFiles(t)
	raw := preparedRaw(t, m)
	for _, check := range []struct {
		name string
		raw  []byte
	}{
		{name: "empty"}, {name: "oversized", raw: bytes.Repeat([]byte("x"), (256<<10)+1)},
		{name: "truncated", raw: raw[:len(raw)-1]}, {name: "trailing", raw: append(bytes.Clone(raw), []byte(`{}`)...)},
		{name: "whitespace-alias", raw: append([]byte(" "), raw...)},
		{name: "unknown-field", raw: bytes.Replace(raw, []byte(`{"version":1,`), []byte(`{"unknown":true,"version":1,`), 1)},
		{name: "duplicate-field", raw: bytes.Replace(raw, []byte(`{"version":1,`), []byte(`{"version":1,"version":1,`), 1)},
	} {
		t.Run(check.name, func(t *testing.T) {
			if err := localoperator.ValidateManifestTemplate(t.Context(), check.raw, exe); !errors.Is(err, localoperator.ErrDenied) {
				t.Fatal("malformed prepared DATA was not denied", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, check := range []struct {
		name string
		ctx  context.Context
		exe  string
	}{{name: "nil-context", exe: exe}, {name: "cancelled", ctx: ctx, exe: exe}, {name: "no-executable", ctx: t.Context()}, {name: "relative-executable", ctx: t.Context(), exe: "private-path-marker"}} {
		t.Run(check.name, func(t *testing.T) {
			if err := localoperator.ValidateManifestTemplate(check.ctx, raw, check.exe); !errors.Is(err, localoperator.ErrDenied) || strings.Contains(err.Error(), "private-path-marker") {
				t.Fatal("invalid context/executable was not safely denied", err)
			}
		})
	}
}

func TestValidateManifestTemplateDeclarationDenials(t *testing.T) {
	for _, check := range []struct {
		name   string
		mutate func(*testing.T, *localoperator.ManifestV1)
	}{
		{name: "wrong-version", mutate: func(_ *testing.T, m *localoperator.ManifestV1) { m.Version = 2 }},
		{name: "key-fingerprint-form", mutate: func(_ *testing.T, m *localoperator.ManifestV1) { m.KeyFingerprint = "not-a-key-fingerprint" }},
		{name: "installed-resource", mutate: func(_ *testing.T, m *localoperator.ManifestV1) {
			m.Configuration.Resource.ResourceID = "already-installed"
		}},
		{name: "installed-profile", mutate: func(_ *testing.T, m *localoperator.ManifestV1) { m.Profile.ProfileRevision = 1 }},
		{name: "installed-source", mutate: func(_ *testing.T, m *localoperator.ManifestV1) {
			m.Configuration.OriginalSource = m.Configuration.OperatorTerms
		}},
		{name: "installed-backend", mutate: func(_ *testing.T, m *localoperator.ManifestV1) {
			m.Profile.Backend.BackendProfileDigest = strings.Repeat("b", 64)
		}},
		{name: "foreign-executable", mutate: func(_ *testing.T, m *localoperator.ManifestV1) {
			m.Configuration.WorkerExecutableDigest = strings.Repeat("0", 64)
		}},
		{name: "foreign-host", mutate: func(_ *testing.T, m *localoperator.ManifestV1) {
			m.Configuration.LocalHostIdentityDigest = strings.Repeat("0", 64)
		}},
		{name: "foreign-namespace", mutate: func(_ *testing.T, m *localoperator.ManifestV1) {
			m.Configuration.LocalNetworkNamespaceIdentityDigest = strings.Repeat("0", 64)
		}},
		{name: "changed-body-bytes", mutate: func(_ *testing.T, m *localoperator.ManifestV1) {
			m.Evidence[0].Body = []byte("changed without original digest")
		}},
		{name: "missing-capability", mutate: func(t *testing.T, m *localoperator.ManifestV1) {
			removePreparedBody(t, m, m.Configuration.CapabilityEvidence.Digest)
		}},
		{name: "missing-review", mutate: func(t *testing.T, m *localoperator.ManifestV1) {
			removePreparedBody(t, m, m.Profile.ReviewReceipt.Digest)
		}},
		{name: "duplicate-reference", mutate: func(_ *testing.T, m *localoperator.ManifestV1) { m.Evidence = append(m.Evidence, m.Evidence[0]) }},
		{name: "too-many-bodies", mutate: func(_ *testing.T, m *localoperator.ManifestV1) {
			for len(m.Evidence) <= 16 {
				m.Evidence = append(m.Evidence, m.Evidence[0])
			}
		}},
		{name: "invoice-model", mutate: func(_ *testing.T, m *localoperator.ManifestV1) { m.InvoiceTerms.ModelID = "foreign-model" }},
		{name: "paid-local-invoice", mutate: func(_ *testing.T, m *localoperator.ManifestV1) { m.InvoiceTerms.ProviderInvoiceAmount = 1 }},
		{name: "unsupported-endpoint", mutate: func(_ *testing.T, m *localoperator.ManifestV1) { m.Configuration.BaseURL = "http://localhost:11434/v1" }},
		{name: "missing-calibration", mutate: func(t *testing.T, m *localoperator.ManifestV1) {
			removePreparedBody(t, m, m.Configuration.CalibrationDigest)
		}},
		{name: "duplicate-calibration", mutate: func(_ *testing.T, m *localoperator.ManifestV1) {
			for _, b := range m.Evidence {
				if b.Reference.Digest == m.Configuration.CalibrationDigest {
					b.Reference.Ref += "-duplicate"
					m.Evidence = append(m.Evidence, b)
					break
				}
			}
		}},
		{name: "foreign-calibration", mutate: func(t *testing.T, m *localoperator.ManifestV1) {
			ref := replacePreparedBody(t, m, m.Configuration.CalibrationDigest, map[string]any{"version": 1, "model_id": "foreign"})
			m.Configuration.CalibrationDigest, m.Profile.CalibrationDigest = ref.Digest, ref.Digest
		}},
		{name: "missing-MCP-grant", mutate: func(t *testing.T, m *localoperator.ManifestV1) {
			removePreparedBody(t, m, m.Profile.MCPToolGrantDigest)
		}},
		{name: "missing-MCP-price", mutate: func(t *testing.T, m *localoperator.ManifestV1) {
			removePreparedBody(t, m, m.Profile.CostCeiling.MCPToolCost.PriceEvidenceDigest)
		}},
		{name: "missing-MCP-terms", mutate: func(t *testing.T, m *localoperator.ManifestV1) {
			removePreparedBody(t, m, m.Profile.CostCeiling.MCPToolCost.TermsEvidenceDigest)
		}},
		{name: "MCP-invoice-as-document", mutate: func(t *testing.T, m *localoperator.ManifestV1) {
			cost := m.Profile.CostCeiling.MCPToolCost
			var price mcpadmission.MCPPriceEvidenceV1
			for _, b := range m.Evidence {
				if b.Reference.Digest == cost.PriceEvidenceDigest {
					if err := json.Unmarshal(b.Body, &price); err != nil {
						t.Fatal(err)
					}
				}
			}
			price.SourceBody = m.Configuration.OperatorTerms
			cost.PriceEvidenceDigest = replacePreparedBody(t, m, cost.PriceEvidenceDigest, price).Digest
			preparedCostDigest(t, m)
		}},
		{name: "missing-search-grant", mutate: func(t *testing.T, m *localoperator.ManifestV1) {
			removePreparedBody(t, m, m.Profile.WebSearchResourceGrant.Body.Digest)
		}},
		{name: "missing-search-acceptance", mutate: func(_ *testing.T, m *localoperator.ManifestV1) { m.SearchExpenditure = nil }},
		{name: "missing-search-document", mutate: func(t *testing.T, m *localoperator.ManifestV1) {
			var price localoperator.OwnerAcceptedSearchPriceV2
			for _, b := range m.Evidence {
				if b.Reference == m.SearchExpenditure.PriceEvidence {
					if err := json.Unmarshal(b.Body, &price); err != nil {
						t.Fatal(err)
					}
				}
			}
			removePreparedBody(t, m, price.Payload.Digest)
		}},
	} {
		t.Run(check.name, func(t *testing.T) {
			_, exe, m := preparedTemplateFiles(t)
			check.mutate(t, &m)
			if err := localoperator.ValidateManifestTemplate(t.Context(), preparedRaw(t, m), exe); !errors.Is(err, localoperator.ErrDenied) {
				t.Fatal("substituted prepared declaration was not denied", err)
			}
		})
	}
}

func TestValidateManifestTemplateExclusiveOriginalWindow(t *testing.T) {
	for _, position := range []string{"before", "boundary", "after"} {
		t.Run(position, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, exe, m := preparedTemplateFiles(t)
				raw := preparedRaw(t, m)
				if err := localoperator.ValidateManifestTemplate(t.Context(), raw, exe); err != nil {
					t.Fatal("live original synthetic window", err)
				}
				time.Sleep(time.Until(m.Configuration.ExpiresAt) - time.Microsecond)
				if position != "before" {
					time.Sleep(time.Microsecond)
				}
				if position == "after" {
					time.Sleep(time.Microsecond)
				}
				err := localoperator.ValidateManifestTemplate(t.Context(), raw, exe)
				if (err == nil) != (position == "before") {
					t.Fatal("original exclusive expiry mismatch", position, err)
				}
			})
		})
	}
}

func TestValidateManifestTemplateShorterServiceWindows(t *testing.T) {
	for _, component := range []string{"MCP", "search"} {
		t.Run(component, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, exe, m := preparedTemplateFiles(t)
				end := time.Now().UTC().Truncate(time.Microsecond).Add(5 * time.Minute)
				if component == "MCP" {
					cost := m.Profile.CostCeiling.MCPToolCost
					for _, b := range m.Evidence {
						if b.Reference.Digest == cost.TermsEvidenceDigest {
							var terms mcpadmission.MCPTermsEvidenceV1
							if err := json.Unmarshal(b.Body, &terms); err != nil {
								t.Fatal(err)
							}
							terms.ExpiresAt = end
							cost.TermsEvidenceDigest = replacePreparedBody(t, &m, b.Reference.Digest, terms).Digest
							break
						}
					}
					preparedCostDigest(t, &m)
				} else {
					for _, b := range m.Evidence {
						if b.Reference == m.SearchExpenditure.TermsEvidence {
							var terms localoperator.OwnerAcceptedSearchTermsV2
							if err := json.Unmarshal(b.Body, &terms); err != nil {
								t.Fatal(err)
							}
							terms.ExpiresAt = end
							m.SearchExpenditure.TermsEvidence = replacePreparedBody(t, &m, b.Reference.Digest, terms)
							// Rebind the semantic grant and cost to the changed report
							// without extending any original declaration window.
							var grant domain.WebSearchResourceGrantV2
							for _, body := range m.Evidence {
								if body.Reference == m.Profile.WebSearchResourceGrant.Body {
									if err := json.Unmarshal(body.Body, &grant); err != nil {
										t.Fatal(err)
									}
								}
							}
							grant.TermsEvidenceDigest = m.SearchExpenditure.TermsEvidence.Digest
							rebindSearchGrant(t, &m, grant)
							break
						}
					}
				}
				raw := preparedRaw(t, m)
				if err := localoperator.ValidateManifestTemplate(t.Context(), raw, exe); err != nil {
					t.Fatal("shorter still-live service window denied", err)
				}
				time.Sleep(time.Until(end))
				if !time.Now().Before(m.Configuration.ExpiresAt) {
					t.Fatal("service-boundary control reached overall profile expiry")
				}
				if err := localoperator.ValidateManifestTemplate(t.Context(), raw, exe); !errors.Is(err, localoperator.ErrDenied) {
					t.Fatal("elapsed original service window was renewed by validation", component, err)
				}
			})
		})
	}
}

func TestValidateManifestTemplateDoesNotChangeOriginalIntake(t *testing.T) {
	root, exe, m := preparedTemplateFiles(t)
	raw := preparedRaw(t, m)
	if err := localoperator.ValidateManifestTemplate(t.Context(), raw, exe); err != nil {
		t.Fatal(err)
	}
	writeFitManifest(t, root, m)
	source := openFitSource(t, t.Context(), root, exe)
	receipt := acceptSearchSourceTestPostimage(t, source)
	if bytes.Contains(preparedRaw(t, m), []byte("template-validation-resource")) || receipt.Profile.Resource.ResourceID == "template-validation-resource" || receipt.Profile.InstallationID == "template-validation-installation" || receipt.Profile.MutationID == "template-validation-mutation" {
		t.Fatal("validation projection escaped into original private-file intake")
	}
	retained, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil || !bytes.Equal(raw, retained) {
		t.Fatal("original intake rewrote exact manifest bytes", err)
	}
	resumed, err := localoperator.OpenExisting(t.Context(), root, exe, receipt)
	if err != nil {
		t.Fatal("original receipt resume after extraction", err)
	}
	t.Cleanup(func() {
		if err := resumed.Close(); err != nil {
			t.Error(err)
		}
	})
	configuration, err := resumed.Configuration()
	if err != nil || configuration.OriginalSource.Digest != fitDigest(raw) || configuration.Resource != receipt.Profile.Resource {
		t.Fatal("original receipt/raw source correlation changed", err)
	}
	ref := domain.OwnedAgentProfileReferenceV1{Locator: receipt.Profile.Locator, RevocationGeneration: receipt.Profile.RevocationGeneration, Digest: receipt.Digest}
	if _, err := resumed.ReadMCPToolGrant(t.Context(), ref, time.Now().UTC().Truncate(time.Microsecond)); err != nil {
		t.Fatal("prepared MCP declarations disagree with original Source reader", err)
	}
	for _, component := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1} {
		scope := domain.AttemptExecutionSourceScopeV1{TenantID: configuration.TenantID, OwnerUserID: configuration.OwnerUserID, SessionID: "prepared-session", RunID: "prepared-run", AttemptID: "prepared-attempt"}
		grant, err := resumed.SpendingPublication(t.Context(), scope, component)
		if err != nil || resumed.VerifyAttemptSpendingPublication(grant, grant.IssuedAt) != nil {
			t.Fatal("prepared component declarations disagree with original Source checks", component, err)
		}
	}
	// Form-only validation never replaces original key possession checks.
	m.KeyFingerprint = strings.Repeat("f", 64)
	if err := localoperator.ValidateManifestTemplate(t.Context(), preparedRaw(t, m), exe); err != nil {
		t.Fatal(err)
	}
	writeFitManifest(t, root, m)
	denied, err := localoperator.OpenFresh(t.Context(), root, exe)
	if denied != nil {
		t.Cleanup(func() { _ = denied.Close() })
	}
	if !errors.Is(err, localoperator.ErrDenied) || denied != nil {
		t.Fatal("validation bypassed original private-key matching", err)
	}
	// Complete-preparation requirements never silently strengthen the old
	// one-shot intake, which historically does not require calibration/MCP.
	legacyRoot, legacyExe, legacy := sourceFitFiles(t)
	if err := localoperator.ValidateManifestTemplate(t.Context(), preparedRaw(t, legacy), legacyExe); !errors.Is(err, localoperator.ErrDenied) {
		t.Fatal("incomplete preparation accepted", err)
	}
	_ = openFitSource(t, t.Context(), legacyRoot, legacyExe)
}

func TestValidateManifestTemplatePreservesOriginalReplacement(t *testing.T) {
	root, exe, m := preparedTemplateFiles(t)
	writeFitManifest(t, root, m)
	original := openFitSource(t, t.Context(), root, exe)
	receipt := acceptSearchSourceTestPostimage(t, original)
	nextRoot, nextExe, nextManifest := preparedTemplateFiles(t)
	if err := localoperator.ValidateManifestTemplate(t.Context(), preparedRaw(t, nextManifest), nextExe); err != nil {
		t.Fatal(err)
	}
	writeFitManifest(t, nextRoot, nextManifest)
	// A valid fresh template is not a reviewed replacement. The original
	// replacement guard requires different retained review evidence, not only
	// a changed manifest timestamp, locator or installation revision.
	unchangedReview, err := original.Replacement(t.Context(), nextRoot, nextExe, receipt)
	if unchangedReview != nil {
		t.Cleanup(func() { _ = unchangedReview.Close() })
	}
	if !errors.Is(err, localoperator.ErrDenied) || unchangedReview != nil {
		t.Fatal("unchanged original review authorized a replacement", err)
	}
	nextManifest.Profile.ReviewReceipt = replacePreparedBody(t, &nextManifest, nextManifest.Profile.ReviewReceipt.Digest, "distinct synthetic successor review fixture, not independent execution acceptance")
	if nextManifest.Profile.ReviewReceipt.Digest == receipt.Profile.ReviewReceipt.Digest {
		t.Fatal("successor fixture did not provide distinct retained review bytes")
	}
	if err := localoperator.ValidateManifestTemplate(t.Context(), preparedRaw(t, nextManifest), nextExe); err != nil {
		t.Fatal("distinct synthetic successor template denied", err)
	}
	writeFitManifest(t, nextRoot, nextManifest)
	replacement, err := original.Replacement(t.Context(), nextRoot, nextExe, receipt)
	if err != nil {
		t.Fatal("original replacement constructor after extraction", err)
	}
	t.Cleanup(func() {
		if err := replacement.Close(); err != nil {
			t.Error(err)
		}
	})
	grant, err := replacement.Publication()
	if err != nil || grant.LocalInstallation == nil || grant.LocalInstallation.Predecessor == nil || grant.Profile.Resource.ResourceID != receipt.Profile.Resource.ResourceID ||
		grant.Profile.InstallationID != receipt.Profile.InstallationID || grant.Profile.InstallationRevision != receipt.Profile.InstallationRevision+1 || grant.Profile.Resource.Revision != receipt.Profile.Resource.Revision+1 || grant.Profile.MutationID == receipt.Profile.MutationID {
		t.Fatal("replacement lost original receipt identity/revision/CAS intent", err)
	}
	if err := replacement.VerifyOwnedProfilePublication(grant, grant.IssuedAt); err != nil {
		t.Fatal("original replacement grant correlation", err)
	}
	foreign := ports.OwnedProfilePublicationReceiptV1{Profile: receipt.Profile.Clone(), Digest: strings.Repeat("0", 64)}
	denied, err := original.Replacement(t.Context(), nextRoot, nextExe, foreign)
	if denied != nil {
		t.Cleanup(func() { _ = denied.Close() })
	}
	if !errors.Is(err, localoperator.ErrDenied) || denied != nil {
		t.Fatal("foreign predecessor bypassed original replacement boundary", err)
	}
}
