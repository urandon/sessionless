package domain

// ValidateForOwnedProfile correlates sealed configuration with a structural
// current-head template. It authenticates neither the installed source/terms
// nor model evidence; the actual source keeper and current Store joins remain
// required. The local operator funding producer is not implemented by this.
func (v ConfiguredInferenceProfileV1) ValidateForOwnedProfile(p OwnedAgentProfileCurrentV1) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if err := p.Validate(); err != nil {
		return err
	}
	digest, err := v.Digest()
	if err != nil {
		return err
	}
	delivery := ProviderCredentialDeliveryNoneV1
	if v.Auth == ConfiguredInferenceAuthBearerV1 {
		delivery = ProviderCredentialDeliveryDirectV1
	}
	if p.Locator.TenantID != v.TenantID || p.Locator.OwnerUserID != v.OwnerUserID || p.Resource != v.Resource || p.ProfileRevision != v.ProfileRevision ||
		p.Locator.ModelVendorID != v.ModelVendorID || p.Locator.ModelID != v.ModelID || p.Backend.BackendKind != HarnessBackendDirectOpenAICompatibleV1 || p.Backend.ProviderContractKind != ProviderContractConfiguredEndpointV1 || p.Backend.NativeProtocolVersion != v.NativeProtocolVersion || p.Backend.BackendProfileDigest != digest || p.Backend.CredentialDeliveryKind != delivery ||
		p.ProviderCatalogDigest != v.ProviderCatalogDigest || p.ProviderRouteDigest != v.ProviderRouteDigest || p.CapabilityEvidenceDigest != v.CapabilityEvidence.Digest || p.PrivacyPolicyDigest != v.PrivacyPolicyDigest || p.EffectivePolicyDigest != v.EffectivePolicyDigest || p.ModelWindowTokens != v.ModelWindowTokens || p.TokenEstimatorVersion != v.TokenEstimatorVersion || p.CalibrationDigest != v.CalibrationDigest ||
		p.InstallationID != v.LocalInstallationID || p.InstallationRevision != v.LocalInstallationRevision || p.InstalledManifest != v.OriginalSource ||
		p.Window.ObservedAt.Before(v.IssuedAt) || p.Window.ExpiresAt.After(v.ExpiresAt) {
		return configuredInferenceError("installation", "requires the exact configured source, resource, model, evidence and original window")
	}
	allowed := false
	for _, class := range v.AllowedDataClasses {
		if class == p.Locator.InputDataClass {
			allowed = true
		}
	}
	s := p.Substrate
	if !allowed || s.Kind != SubstrateLocalDevelopmentV1 || s.WorkloadMode != SubstrateWorkloadInProcessDirectV1 || s.ImageDigest != "" || s.Region != "" ||
		s.OuterHarnessArtifactDigest != v.WorkerExecutableDigest || s.LocalInstallationID != v.LocalInstallationID || s.LocalInstallationRevision != v.LocalInstallationRevision || s.LocalHostIdentityDigest != v.LocalHostIdentityDigest || s.LocalNetworkNamespaceIdentityDigest != v.LocalNetworkNamespaceIdentityDigest || s.ProfileEvidenceExpiresAt.After(v.ExpiresAt) {
		return configuredInferenceError("installation.placement", "requires the configured local executable, host and namespace")
	}
	for _, modality := range p.InputModalities {
		if (modality == AgentInputImageV1 && v.Capabilities.Image != ProviderEvidenceSupportedV1) || (modality == AgentInputFileV1 && v.Capabilities.File != ProviderEvidenceSupportedV1) {
			return configuredInferenceError("installation.modalities", "requires explicit configured support")
		}
	}
	l := p.Limits
	if v.Capabilities.ToolCalls != ProviderEvidenceSupportedV1 || l.MaxInputTokensPerCall > v.Limits.MaxInputTokens || l.MaxOutputTokensPerCall > v.Limits.MaxOutputTokens || l.MaxNetworkOperationDuration > v.Limits.OperationTimeout || l.MaxCleanupDuration > v.Limits.CleanupTimeout || l.MaxToolArgumentBytes > v.Limits.ToolArgumentsBytes || l.MaxContextViewBytes > v.Limits.RequestBytes {
		return configuredInferenceError("installation.limits", "owned loop requires supported tool calls and ceilings within configured limits")
	}
	c := p.CostCeiling
	if p.BillingOwnerUserID != v.OwnerUserID || c.ProviderPriceState != ProviderPriceKnownFreeV1 || p.SubstratePriceState != ExternalServicePriceKnownFreeV1 || c.SubstratePriceState != CostEvidenceKnownV1 || c.MaxProviderAmountMicrounits == nil || *c.MaxProviderAmountMicrounits != 0 || c.MaxSubstrateAmountMicrounits == nil || *c.MaxSubstrateAmountMicrounits != 0 || p.ProviderTermsDigest != v.OperatorTerms.Digest || p.SubstrateTermsDigest != v.OperatorTerms.Digest {
		return configuredInferenceError("installation.cost", "requires exact owner-local no-invoice terms for provider and substrate, not free external services")
	}
	return nil
}
