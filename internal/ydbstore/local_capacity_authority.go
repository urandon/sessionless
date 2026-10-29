package ydbstore

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
)

// Reuse the original Source and current canonical joins in the caller's SAME
// callback/budget/DB clock. This is not an alternative scheduler or approval
// callback. Structural association bytes are never accepted as authentication.
func (store *Store) readLocalCapacityAuthority(ctx context.Context, q *ownedAuthorityQuery, association *domain.LocalCapacityAssociationV1, record string, placement domain.ExecutionPlacementV2, binding domain.HarnessBindingV2, substrate *domain.SubstrateBindingV1, cost *domain.AdmissionCostCeilingV2) error {
	if association == nil || q == nil || q.budget == nil || association.ValidateForOwnedSource(record, placement, binding, substrate, cost) != nil {
		return domain.ErrMembershipDenied
	}
	original, ok := store.ownedAttemptEffectSourceVerifier.(*localoperator.Source)
	source, err := domain.DecodeAttemptExecutionSourceV1([]byte(record))
	if !ok || err != nil || !store.originalLocalPublicationSource(original) || original.VerifyOwnedAttemptEffectSource(source) != nil ||
		readOwnedAdmissionPrincipal(ctx, q, source.Scope) != nil {
		return q.budget.deny()
	}
	if _, err := readOwnedAdmissionSources(ctx, q, source); err != nil {
		return err
	}
	if currentServerlessProviderResourceTx(ctx, q, binding, &source.Profile, q.at) != nil || ctx.Err() != nil || !store.originalLocalPublicationSource(original) {
		return q.budget.deny()
	}
	return nil
}
