package localoperator

import (
	"context"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
)

// ValidateManifestTemplate checks complete prepared ManifestV1 DATA against the
// explicitly named executable and observed boot/host/network namespace. It
// reads that executable and kernel identity only: no installation root/key,
// credential, SQL, network, process execution or publication is involved.
//
// Unlike the historical one-shot OpenFresh intake, complete preparation also
// requires retained calibration and both complete MCP/search declarations,
// including MCP inputSchema support in the actual closed model-wire compiler.
// These are structural/current-window checks, not model qualification,
// independent review, service-price truth or current Store authorization.
// KeyFingerprint is checked for form only; no possession of source.key is
// established. Success returns no IDs, Source, receipt, grant or permission.
func ValidateManifestTemplate(ctx context.Context, raw []byte, executable string) error {
	if ctx == nil || ctx.Err() != nil {
		return ErrDenied
	}
	m, err := decodeManifestTemplate(raw)
	if err != nil {
		return err
	}
	if domain.ValidateSHA256Digest("local_operator.key", m.KeyFingerprint) != nil {
		return sourceDenial("source key identity")
	}
	identity, err := InspectRuntime(ctx, executable)
	if err != nil {
		return sourceDenial("installed executable")
	}
	if err := validateManifestTemplateData(m, identity); err != nil {
		return err
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	// Existing complete profile validators require installation-owned fields.
	// This private, deterministic DATA projection never escapes this call and
	// is never used by real intake, signed, published or accepted as a receipt.
	c, p, err := materializeManifestTemplate(raw, m, "template-validation-resource", "template-validation-installation", "template-validation-mutation", 1, at)
	if err != nil {
		return err
	}
	if p.ValidateAt(at) != nil {
		return sourceDenial("profile window")
	}
	calibration, ok := templateBodyByDigest(m, c.CalibrationDigest, configuredinference.MaxContextCalibrationBytesV1, true)
	if !ok {
		return sourceDenial("calibration evidence")
	}
	if _, err := configuredinference.DecodeContextCalibrationV1(c, calibration.Body); err != nil {
		return sourceDenial("calibration evidence")
	}
	if err := validateTemplateMCP(m, c, p, at); err != nil {
		return err
	}
	search, err := acceptSearchEvidence(m, p, at)
	if err != nil || search == nil {
		return sourceDenial("search expenditure")
	}
	if ctx.Err() != nil {
		return ErrDenied
	}
	return nil
}

// These helpers are the original private-file intake's DATA checks. Keep them
// shared with preparation so a prepared template cannot use a second codec or
// weaken real intake's blank-identity, runtime, body or invoice correlations.
func decodeManifestTemplate(raw []byte) (ManifestV1, error) {
	var m ManifestV1
	if len(raw) == 0 || len(raw) > maxOperatorManifestBytes || strictJSON(raw, &m) != nil || m.Version != 1 || len(m.Evidence) == 0 || len(m.Evidence) > 16 {
		return ManifestV1{}, sourceDenial("manifest encoding")
	}
	return m, nil
}

func validateManifestTemplateData(m ManifestV1, identity RuntimeIdentity) error {
	c, p := m.Configuration, m.Profile
	if c.Resource.ResourceID != "" || c.Resource.Revision != 0 || c.ProfileRevision != 0 || c.LocalInstallationID != "" || c.LocalInstallationRevision != 0 ||
		p.Resource.ResourceID != "" || p.Resource.Revision != 0 || p.ProfileRevision != 0 || p.InstallationID != "" || p.InstallationRevision != 0 || p.RevocationGeneration != 0 || p.MutationID != "" || p.State != "" ||
		p.Locator.ResourceID != "" || p.Locator.BackendProfileDigest != "" || p.Backend.BackendProfileDigest != "" || p.Substrate.LocalInstallationID != "" || p.Substrate.LocalInstallationRevision != 0 ||
		c.OriginalSource != (domain.OwnedAgentAuthorityBodyRefV1{}) || p.InstalledManifest != (domain.OwnedAgentAuthorityBodyRefV1{}) {
		return ErrDenied
	}
	if c.WorkerExecutableDigest != identity.WorkerExecutableDigest || c.LocalHostIdentityDigest != identity.LocalHostIdentityDigest || c.LocalNetworkNamespaceIdentityDigest != identity.LocalNetworkNamespaceIdentityDigest {
		return ErrDenied
	}
	for i, body := range m.Evidence {
		if body.Reference.Validate() != nil || body.Reference.Bytes != uint64(len(body.Body)) || hash(body.Body) != body.Reference.Digest {
			return ErrDenied
		}
		for j := 0; j < i; j++ {
			if m.Evidence[j].Reference.Ref == body.Reference.Ref {
				return ErrDenied
			}
		}
	}
	if !hasBody(m.Evidence, c.CapabilityEvidence) || !hasBody(m.Evidence, p.ReviewReceipt) {
		return ErrDenied
	}
	terms, err := json.Marshal(m.InvoiceTerms)
	if err != nil || !validTerms(m.InvoiceTerms, c, p) || c.OperatorTerms.Bytes != uint64(len(terms)) || c.OperatorTerms.Digest != hash(terms) || !hasExactBody(m.Evidence, c.OperatorTerms, terms) {
		return ErrDenied
	}
	return nil
}

func materializeManifestTemplate(raw []byte, m ManifestV1, resource, installation, mutation string, revision uint64, at time.Time) (domain.ConfiguredInferenceProfileV1, domain.OwnedAgentProfileCurrentV1, error) {
	deny := func(err error) (domain.ConfiguredInferenceProfileV1, domain.OwnedAgentProfileCurrentV1, error) {
		return domain.ConfiguredInferenceProfileV1{}, domain.OwnedAgentProfileCurrentV1{}, err
	}
	sourceRef := domain.OwnedAgentAuthorityBodyRefV1{Ref: "local-source-" + hash(raw), Digest: hash(raw), Bytes: uint64(len(raw))}
	if sourceRef.Validate() != nil {
		return deny(ErrDenied)
	}
	c, p := m.Configuration.Clone(), m.Profile.Clone()
	c.Resource.ResourceID, c.Resource.Revision, c.ProfileRevision, c.LocalInstallationID, c.LocalInstallationRevision = resource, revision, revision, installation, revision
	c.OriginalSource = sourceRef
	p.Resource = c.Resource
	p.ProfileRevision, p.RevocationGeneration, p.State = revision, 1, domain.OwnedAgentCurrentActiveV1
	p.InstallationID, p.InstallationRevision, p.MutationID, p.InstalledManifest = installation, revision, mutation, sourceRef
	p.Locator.ResourceID = resource
	p.Substrate.LocalInstallationID, p.Substrate.LocalInstallationRevision = installation, revision
	var err error
	p.Backend.BackendProfileDigest, err = c.Digest()
	if err != nil {
		return deny(ErrDenied)
	}
	p.Locator.BackendProfileDigest = p.Backend.BackendProfileDigest
	p.Window.PublishedAt, p.UpdatedAt = at, at
	if c.ValidateForOwnedProfile(p) != nil || c.ValidateAt(at) != nil || p.ValidateInitial() != nil {
		return deny(sourceDenial("profile correlation"))
	}
	return c, p, nil
}

// Match existing calibration's unique digest and MCP's bounded digest lookup.
// The shared intake already authenticated every reference's bytes and hash.
func templateBodyByDigest(m ManifestV1, digest string, cap uint64, unique bool) (EvidenceBodyV1, bool) {
	var result EvidenceBodyV1
	for _, body := range m.Evidence {
		if body.Reference.Digest != digest {
			continue
		}
		if body.Reference.Bytes == 0 || body.Reference.Bytes > cap || (unique && result.Body != nil) {
			return EvidenceBodyV1{}, false
		}
		result = body
	}
	return result, result.Body != nil
}

func validateTemplateMCP(m ManifestV1, c domain.ConfiguredInferenceProfileV1, p domain.OwnedAgentProfileCurrentV1, at time.Time) error {
	deny := func() error { return sourceDenial("MCP evidence") }
	if p.Locator.InputDataClass != domain.ProviderDataPublicV1 || p.CostCeiling.MCPToolCost == nil {
		return deny()
	}
	cost := *p.CostCeiling.MCPToolCost
	body, ok := templateBodyByDigest(m, p.MCPToolGrantDigest, mcpadmission.GrantBodyBytesV1, false)
	if !ok {
		return deny()
	}
	grant, err := mcpadmission.ParseMCPToolGrantV1(body.Body)
	if err != nil || grant.ValidateForCost(cost, at) != nil || grant.DataPolicyDigest != p.PrivacyPolicyDigest {
		return deny()
	}
	descriptor, err := mcpwire.ParseDescriptorV1(grant.Descriptor, grant.Limits)
	if err != nil || configuredinference.ValidateToolSchemaV1(descriptor.InputSchema(), c.Limits) != nil {
		return deny()
	}
	priceBody, priceOK := templateBodyByDigest(m, cost.PriceEvidenceDigest, mcpadmission.ServiceEvidenceBytesV1, false)
	termsBody, termsOK := templateBodyByDigest(m, cost.TermsEvidenceDigest, mcpadmission.ServiceEvidenceBytesV1, false)
	if !priceOK || !termsOK {
		return deny()
	}
	document := func(ref domain.OwnedAgentAuthorityBodyRefV1) bool {
		if ref.Digest == c.OperatorTerms.Digest || ref.Digest == p.MCPToolGrantDigest || ref.Digest == cost.PriceEvidenceDigest || ref.Digest == cost.TermsEvidenceDigest {
			return false
		}
		_, err := exactSearchEvidence(m.Evidence, ref, mcpadmission.GrantBodyBytesV1)
		return err == nil
	}
	price, err := mcpadmission.ParseMCPPriceEvidenceV1(priceBody.Body)
	if err != nil || price.ValidateForCost(grant, cost, at) != nil || !document(price.SourceBody) {
		return deny()
	}
	terms, err := mcpadmission.ParseMCPTermsEvidenceV1(termsBody.Body)
	if err != nil || terms.ValidateForCost(grant, cost, at) != nil || !document(terms.SourceBody) {
		return deny()
	}
	return nil
}
