package ydbstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// LocalSearchSourceFixture owns actual private-file intake and original profile
// publication over the existing scripted SQL peer. Its declarations, calibration
// and prices are synthetic DATA, not model, service, account-credit or live TLS
// evidence. Construction creates no Attempt, claim, consent or effect authority.
// The private publisher is deliberately not a BFF Store: a BFF must independently
// reopen this installation and construct its own read-only body-reader/Store.
type LocalSearchSourceFixture struct {
	Source        *localoperator.Source
	Receipt       ports.OwnedProfilePublicationReceiptV1
	Root          string
	Executable    string
	GrantV2       domain.WebSearchResourceGrantV2
	Declaration   ports.SearchCredentialDeclarationV1
	Configuration domain.ConfiguredInferenceProfileV1
	installation  *originalLocalRecoveryFixture
}

func NewLocalSearchSourceFixture(t *testing.T, modelBaseURL string, grantExpiry time.Time) *LocalSearchSourceFixture {
	t.Helper()
	root, _, m := localOperatorFiles(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := localoperator.InspectRuntime(t.Context(), executable)
	if err != nil {
		t.Fatal(err)
	}
	connector, err := localoperator.LinkedConnectorIdentity(identity.WorkerExecutableDigest)
	if err != nil {
		t.Fatal(err)
	}
	c, p := &m.Configuration, &m.Profile
	c.BaseURL, m.InvoiceTerms.BaseURL = modelBaseURL, modelBaseURL
	c.WorkerExecutableDigest, c.LocalHostIdentityDigest, c.LocalNetworkNamespaceIdentityDigest = identity.WorkerExecutableDigest, identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	p.Substrate.OuterHarnessArtifactDigest, p.Substrate.LocalHostIdentityDigest, p.Substrate.LocalNetworkNamespaceIdentityDigest = identity.WorkerExecutableDigest, identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	p.Substrate.EgressProxyArtifactDigest, p.Substrate.EgressProxyIdentityDigest = identity.WorkerExecutableDigest, connector
	p.InstructionVersion, p.CompactionPolicyDigest = compactioncontext.InstructionVersionV1, compactioncontext.CompactionPolicyDigestV1()
	p.CostCeiling.MaxPreEffectDurationPerDelivery = 10 * time.Second
	c.Limits.OperationTimeout, p.Limits.MaxNetworkOperationDuration = 5*time.Second, 5*time.Second
	// This original pre-intake profile deliberately has a small result ceiling:
	// its first bounded SEARCH turn can fit the next MODEL without compaction.
	// No accepted result/profile is edited or silently given a larger budget.
	p.Limits.MaxToolResultBytes = 4096
	evidence := func(ref string, raw []byte) localoperator.EvidenceBodyV1 {
		sum := sha256.Sum256(raw)
		return localoperator.EvidenceBodyV1{Reference: domain.OwnedAgentAuthorityBodyRefV1{Ref: ref, Digest: hex.EncodeToString(sum[:]), Bytes: uint64(len(raw))}, Body: raw}
	}
	calibration, err := json.Marshal(map[string]any{"version": 1, "algorithm": "utf8_affine.v1", "input_domain": "chat_completions_text.v1", "tenant_id": c.TenantID, "owner_user_id": c.OwnerUserID, "model_vendor_id": c.ModelVendorID, "model_id": c.ModelID, "response_model_ids": c.ResponseModelIDs, "server_id": c.ServerID, "server_version": c.ServerVersion, "protocol": c.NativeProtocolVersion, "chat_template_digest": c.ChatTemplateDigest, "estimator_version": c.TokenEstimatorVersion, "capability_evidence_digest": c.CapabilityEvidence.Digest, "model_window_tokens": c.ModelWindowTokens, "max_wire_bytes": c.Limits.RequestBytes, "max_messages": c.Limits.Messages, "max_tools": c.Limits.Tools, "byte_numerator": 1, "byte_denominator": 1, "fixed_tokens": 1, "message_tokens": 1, "tool_tokens": 1})
	if err != nil {
		t.Fatal(err)
	}
	body := evidence("synthetic-search-calibration", calibration)
	c.CalibrationDigest, p.CalibrationDigest = body.Reference.Digest, body.Reference.Digest
	m.Evidence = append(m.Evidence, body)
	invoice, err := json.Marshal(m.InvoiceTerms)
	if err != nil {
		t.Fatal(err)
	}
	body = evidence(c.OperatorTerms.Ref, invoice)
	for i := range m.Evidence {
		if m.Evidence[i].Reference.Ref == c.OperatorTerms.Ref {
			m.Evidence[i] = body
		}
	}
	c.OperatorTerms = body.Reference
	p.ProviderPriceDigest, p.ProviderTermsDigest, p.SubstratePriceDigest, p.SubstrateTermsDigest = body.Reference.Digest, body.Reference.Digest, body.Reference.Digest, body.Reference.Digest
	policy, err := ports.BuildLocalLoopbackEgressPolicyV1(*c)
	if err != nil {
		t.Fatal(err)
	}
	p.Substrate.EgressPolicyDigest, err = policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	localSearchDeclaration(t, &m)
	localSourceMCPDeclaration(t, &m)
	var grant domain.WebSearchResourceGrantV2
	for _, item := range m.Evidence {
		if item.Reference == p.WebSearchResourceGrant.Body {
			grant, err = domain.DecodeWebSearchResourceGrantV2(item.Body)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if grant.Validate() != nil {
		t.Fatal("original SEARCH declaration lacks the exact bounded grant body")
	}
	// Optional test DATA is applied before actual validation/Open/publication.
	// A zero expiry preserves the original declaration and its exact bytes.
	if !grantExpiry.IsZero() {
		if grantExpiry.Location() != time.UTC || !grantExpiry.Equal(grantExpiry.Truncate(time.Microsecond)) || !time.Now().Before(grantExpiry) || !grantExpiry.Before(grant.ExpiresAt) {
			t.Fatal("pre-intake SEARCH grant expiry must be canonical and strictly shorter")
		}
		old := p.WebSearchResourceGrant.Body
		grant.ExpiresAt = grantExpiry
		raw, err := domain.EncodeWebSearchResourceGrantV2(grant)
		if err != nil {
			t.Fatal("pre-intake shorter SEARCH grant", err)
		}
		updated := evidence(old.Ref, raw)
		replaced := false
		for i := range m.Evidence {
			if m.Evidence[i].Reference == old {
				m.Evidence[i], replaced = updated, true
			}
		}
		if !replaced {
			t.Fatal("original SEARCH grant evidence absent before intake")
		}
		p.WebSearchGrantDigest, err = grant.Digest()
		if err != nil {
			t.Fatal(err)
		}
		p.WebSearchResourceGrant.Body = updated.Reference
		p.CostCeiling.WebSearchCost.ScopedGrantDigest = p.WebSearchGrantDigest
		m.SearchExpenditure.GrantDigest, m.SearchExpenditure.GrantBody = p.WebSearchGrantDigest, updated.Reference
	}
	declaration := ports.SearchCredentialDeclarationV1{Version: 1, TenantID: c.TenantID, Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceWebSearchAccountV1, ResourceID: grant.CredentialRef, OwnerUserID: grant.OwnerUserID, Revision: 17, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 23}, Audience: grant.CredentialAudience, DeliveryKind: domain.ProviderCredentialDeliveryDirectV1}
	m.SearchCredential = &declaration
	p.Substrate.AdmissionCostCeilingDigest, err = p.CostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := localoperator.ValidateManifestTemplate(t.Context(), raw, executable); err != nil {
		t.Fatalf("complete SEARCH installation template: %v", err)
	}
	writeLocalOperatorManifest(t, root, m)
	source, err := localoperator.OpenFresh(t.Context(), root, executable)
	if err != nil {
		t.Fatalf("actual SEARCH Source intake: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Error(err)
		}
	})
	publication, err := source.Publication()
	if err != nil {
		t.Fatal(err)
	}
	store, sql, _, _ := newLocalInstallationFixture(t)
	sql.at = publication.IssuedAt
	sql.fundingKeys = make(map[string]bool)
	store.ownedProfilePublicationVerifier, store.attemptSpendingPublicationVerifier, store.ownedAttemptEffectSourceVerifier = source, source, source
	receipt, err := store.PublishOwnedProfile(t.Context(), publication)
	if err != nil {
		t.Fatalf("actual SEARCH profile publication: %v", err)
	}
	if err := source.AcceptReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	if len(sql.fundingHeads) != 0 || len(sql.fundingAudits) != 0 {
		t.Fatal("installation created Attempt spending before an original publication")
	}
	configuration, err := source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	installation := &originalLocalRecoveryFixture{root: root, exe: executable, source: source, store: store, sql: sql, receipt: receipt}
	return &LocalSearchSourceFixture{Source: source, Receipt: receipt, Root: root, Executable: executable, GrantV2: grant, Declaration: declaration, Configuration: configuration, installation: installation}
}

// PublishPackage invokes the original Source and Store for every real component
// in canonical order. Its typed result is candidate DATA, not canonical admission,
// a physical claim, consent, prepared SEARCH or permission to send.
func (f *LocalSearchSourceFixture) PublishPackage(t *testing.T, scope domain.AttemptExecutionSourceScopeV1) domain.OwnedAgentExecutionPackageV1 {
	t.Helper()
	if f == nil || f.installation == nil {
		t.Fatal("missing original SEARCH installation fixture")
	}
	i := f.installation
	return localSourcePublishPackage(t, i.source, i.store, i.sql, i.receipt, scope)
}

func TestLocalSourceSearchFixtureActualPublicationAndReopen(t *testing.T) {
	f := NewLocalSearchSourceFixture(t, "http://127.0.0.1:11434/v1", time.Time{})
	pkg := f.PublishPackage(t, localSearchScope(f.Receipt.Profile, "source-factory"))
	if len(pkg.Source.Spending) != 4 || f.Source.VerifyOwnedAttemptEffectSource(pkg.Source) != nil || len(f.installation.sql.fundingHeads) != 4 || len(f.installation.sql.fundingAudits) != 4 {
		t.Fatal("factory did not produce four actual original publications")
	}
	reopened, err := localoperator.OpenExisting(t.Context(), f.Root, f.Executable, f.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	ref := domain.OwnedAgentProfileReferenceV1{Locator: f.Receipt.Profile.Locator, RevocationGeneration: f.Receipt.Profile.RevocationGeneration, Digest: f.Receipt.Digest}
	d, err := reopened.ReadSearchCredentialDeclaration(t.Context(), ref)
	if err != nil || d != f.Declaration || reopened.VerifyOwnedAttemptEffectSource(pkg.Source) != nil {
		t.Fatalf("independently reopened Source/declaration: declaration=%+v err=%v", d, err)
	}
	raw, err := reopened.ReadWebSearchResourceGrant(t.Context(), ref, domain.WebSearchResourceGrantMaxBytesV2)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	grant, err := domain.DecodeWebSearchResourceGrantV2(raw)
	if err != nil || grant != f.GrantV2 {
		t.Fatalf("reopened original grant: %v", err)
	}
}
