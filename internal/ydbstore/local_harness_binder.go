package ydbstore

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// LocalHarnessBinder borrows the original installed Source and receipt. It
// recovers candidate DATA through the existing concrete Store path; canonical
// commit and admission must independently rejoin current authority. It neither
// installs a profile nor reserves a slot nor owns Source.Close.
type LocalHarnessBinder struct {
	store   *Store
	source  *localoperator.Source
	receipt ports.OwnedProfilePublicationReceiptV1
}

func NewLocalHarnessBinder(store *Store, source *localoperator.Source, receipt ports.OwnedProfilePublicationReceiptV1) (*LocalHarnessBinder, error) {
	if !store.originalLocalPublicationSource(source) || source.VerifyOriginalSpendingSources(receipt, nil) != nil {
		return nil, domain.ErrMembershipDenied
	}
	receipt.Profile = receipt.Profile.Clone()
	return &LocalHarnessBinder{store: store, source: source, receipt: receipt}, nil
}

func (b *LocalHarnessBinder) BindHarness(ctx context.Context, request ports.HarnessBindingRequest) (ports.ExecutionAuthorityV2, error) {
	deny := func() (ports.ExecutionAuthorityV2, error) {
		return ports.ExecutionAuthorityV2{}, domain.ErrMembershipDenied
	}
	if b == nil || ctx == nil || ctx.Err() != nil || request.SubscriptionConnectionID != "" || request.At.IsZero() ||
		!b.store.originalLocalPublicationSource(b.source) || request.TenantID != b.receipt.Profile.Locator.TenantID || request.OwnerUserID != b.receipt.Profile.Locator.OwnerUserID {
		return deny()
	}
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: request.TenantID, SessionID: request.SessionID, OwnerUserID: request.OwnerUserID, RunID: request.RunID, AttemptID: request.AttemptID}
	if scope.Validate() != nil {
		return deny()
	}
	p, err := b.store.RecoverLocalOwnedExecutionPackage(ctx, b.source, b.receipt, scope)
	if err != nil {
		return deny()
	}
	raw, err := domain.EncodeAttemptExecutionSourceV1(p.Source)
	if err != nil {
		return deny()
	}
	association := domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, ResourceID: p.Source.Profile.Locator.ResourceID}
	result := (ports.ManagedExecutionAuthorityV2{LocalCapacityAssociation: &association, ExecutionPlacementV2: p.Placement, HarnessBinding: p.Binding, SubstrateBinding: p.Substrate, AdmissionCostCeiling: p.Cost, OwnedExecutionSourceRecord: string(raw)}).ExecutionAuthority()
	if result.ValidateForScope(request) != nil || ctx.Err() != nil || !b.store.originalLocalPublicationSource(b.source) {
		return deny()
	}
	return result, nil
}

var _ ports.HarnessBinder = (*LocalHarnessBinder)(nil)
