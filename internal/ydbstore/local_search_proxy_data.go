package ydbstore

import (
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Read-only body correlation under the original SQL snapshot. This cannot
// create an authenticated publication, retained socket, keeper or send holder.
func readLocalSearchProxyDataV1(original domain.RegisteredProxyAttestationV2, parent domain.OwnedAttemptEffectParentV1, allocation domain.RegisteredAllocationV2, source domain.AttemptExecutionSourceV1, policyBody, attestationBody []byte) (domain.RegisteredProxyAttestationV2, error) {
	deny := func() (domain.RegisteredProxyAttestationV2, error) {
		return domain.RegisteredProxyAttestationV2{}, domain.ErrMembershipDenied
	}
	if original.TransportKind != domain.RegisteredProxyTransportLocalSearchHTTPSV1 || original.ServiceKind != domain.RegisteredProxyWebSearchV1 || original.LocalSearchHTTPS == nil {
		return deny()
	}
	p, a, err := ports.DecodeLocalSearchHTTPSProxyDataV1(policyBody, attestationBody)
	cost := parent.Authority.AdmissionCostCeiling.WebSearchCost
	if err != nil || cost == nil || p.Grant.ValidateCostAt(*cost, original.Window.ObservedAt) != nil || p.Grant.TenantID != source.Scope.TenantID || p.Grant.OwnerUserID != source.Scope.OwnerUserID || !a.ObservedAt.Equal(original.Window.ObservedAt) || original.Window.ExpiresAt.After(a.ExpiresAt) || !original.Window.PublishedAt.Before(a.ExpiresAt) {
		return deny()
	}
	gd, err := p.Grant.Digest()
	if err != nil || gd != source.Spending[3].ScopedGrantDigest || gd != original.GrantDigest {
		return deny()
	}
	pd, err := p.Digest()
	if err != nil {
		return deny()
	}
	td, err := p.TransportDigest()
	if err != nil {
		return deny()
	}
	return domain.BuildRegisteredLocalSearchHTTPSProxyAttestationV1(allocation, source, td, pd, a.Observation, ports.OwnedProxyBodyReferenceV1("policy", policyBody), ports.OwnedProxyBodyReferenceV1("attestation", attestationBody), original.Window, original.OperationSequence, original.OperationDigest)
}

func validateRetainedSearchProxyOperationV4(proxy *ownedProxySnapshot, r domain.AttemptOperationReservationV1) error {
	if proxy == nil || r.Operation.Kind != domain.AttemptOperationWebSearchV1 || r.Operation.SearchWire == nil || proxy.original.TransportKind != domain.RegisteredProxyTransportLocalSearchHTTPSV1 {
		return domain.ErrMembershipDenied
	}
	p, a, err := ports.DecodeLocalSearchHTTPSProxyDataV1(proxy.policyBody, proxy.attestationBody)
	op := r.Operation
	if err != nil || op.SearchWire.APIProfile != p.Grant.APIProfile || op.SearchWire.ArgumentsBytes == 0 || op.SearchWire.ArgumentsBytes > domain.WebSearchArgumentsBytesV1 || op.SearchWire.CompilerArtifactDigest != p.Grant.AdapterArtifactDigest || op.Deadline.After(proxy.original.Window.ExpiresAt) || op.Deadline.After(a.ExpiresAt) || r.ReservedAt.Before(proxy.original.Window.PublishedAt) || op.RequestBytes > domain.WebSearchWireBytesV1 || op.Debit.ResponseBytes > domain.WebSearchResponseBytesV1 || op.Debit.ActiveDuration > p.MaxOperationTimeout {
		return domain.ErrMembershipDenied
	}
	return nil
}
