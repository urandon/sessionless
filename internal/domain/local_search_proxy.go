package domain

// RegisteredLocalSearchHTTPSProxyV1 is closed historical observation DATA,
// not a current producer, socket, grant or proof that DNS/TLS was enforced.
// Grant/policy/operation/source/claim/window commitments remain in the existing
// enclosing V2 record, rather than duplicating their tuples here.
type RegisteredLocalSearchHTTPSProxyV1 struct {
	ConfiguredProfileDigest        string `json:"configured_profile_digest"`
	EndpointPolicy                 string `json:"endpoint_policy"`
	ConnectorArtifactDigest        string `json:"connector_artifact_digest"`
	InstallationID                 string `json:"installation_id"`
	InstallationRevision           uint64 `json:"installation_revision"`
	HostIdentityDigest             string `json:"host_identity_digest"`
	NetworkNamespaceIdentityDigest string `json:"network_namespace_identity_digest"`
	ResolutionSetDigest            string `json:"resolution_set_digest"`
}

func (o RegisteredLocalSearchHTTPSProxyV1) validate() error {
	if o.EndpointPolicy != string(RegisteredProxyTransportLocalSearchHTTPSV1) || o.InstallationRevision == 0 || ValidateOpaqueID("search_proxy.installation", o.InstallationID) != nil {
		return ErrMembershipDenied
	}
	for _, d := range []string{o.ConfiguredProfileDigest, o.ConnectorArtifactDigest, o.HostIdentityDigest, o.NetworkNamespaceIdentityDigest, o.ResolutionSetDigest} {
		if ValidateSHA256Digest("search_proxy.observation", d) != nil {
			return ErrMembershipDenied
		}
	}
	return nil
}

// BuildRegisteredLocalSearchHTTPSProxyAttestationV1 correlates DATA only. No
// serving publication codec/holder is activated by this historical builder.
func BuildRegisteredLocalSearchHTTPSProxyAttestationV1(allocation RegisteredAllocationV2, source AttemptExecutionSourceV1, transportDigest, policyDigest string, observation RegisteredLocalSearchHTTPSProxyV1, policyBody, attestationBody OwnedAgentAuthorityBodyRefV1, window OwnedAgentAuthorityWindowV1, sequence uint64, operationDigest AttemptOperationDigestV1) (RegisteredProxyAttestationV2, error) {
	deny := func() (RegisteredProxyAttestationV2, error) {
		return RegisteredProxyAttestationV2{}, ErrMembershipDenied
	}
	sd, err := source.Digest()
	if err != nil || source.Validate() != nil || allocation.ValidateAt(window.PublishedAt) != nil || allocation.LocalRuntime == nil || allocation.Substrate.Kind != SubstrateLocalDevelopmentV1 || allocation.SourceDigest != sd || allocation.Scope != source.Scope || window.Validate() != nil || window.ObservedAt.Before(allocation.Window.ObservedAt) || window.ObservedAt.Before(source.IssuedAt) || window.ExpiresAt.After(allocation.Window.ExpiresAt) || window.ExpiresAt.After(source.ExpiresAt) || observation.validate() != nil || sequence < 2 {
		return deny()
	}
	r := allocation.LocalRuntime
	s := source.Spending[3]
	if s.Locator.ComponentKind != AttemptSpendingWebSearchV1 || s.SourceKind != AttemptSpendingOwnerLocalSourceV1 || s.ValidateAt(window.ObservedAt) != nil || window.ExpiresAt.After(s.Window.ExpiresAt) || observation.ConfiguredProfileDigest != r.ConfiguredProfileDigest || observation.ConfiguredProfileDigest != allocation.Backend.BackendProfileDigest || observation.ConnectorArtifactDigest != r.WorkerExecutableDigest || observation.ConnectorArtifactDigest != allocation.ObservedProxyArtifactDigest || observation.InstallationID != r.InstallationID || observation.InstallationRevision != r.InstallationRevision || observation.HostIdentityDigest != r.HostIdentityDigest || observation.NetworkNamespaceIdentityDigest != r.NetworkNamespaceIdentityDigest {
		return deny()
	}
	ad, err := allocation.Digest()
	if err != nil {
		return deny()
	}
	v := RegisteredProxyAttestationV2{Version: 2, TransportKind: RegisteredProxyTransportLocalSearchHTTPSV1, OperationSequence: sequence, OperationDigest: operationDigest, Scope: source.Scope, PhysicalInvocationClaimID: allocation.ParentReservation.PhysicalInvocationClaimID, AllocationID: allocation.AllocationID, AllocationGeneration: allocation.AllocationGeneration, RegisteredAllocationDigest: ad, SourceDigest: sd, ServiceKind: RegisteredProxyWebSearchV1, TransportDigest: transportDigest, GrantDigest: s.ScopedGrantDigest, PolicyDigest: policyDigest, LocalSearchHTTPS: &observation, PolicyBody: policyBody, AttestationBody: attestationBody, State: RegisteredPreparationActiveV1, RevocationGeneration: 1, Window: window, UpdatedAt: window.PublishedAt}
	if v.ValidateInitial() != nil {
		return deny()
	}
	return v.Clone(), nil
}
