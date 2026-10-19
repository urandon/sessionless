package ydbstore

import (
	"context"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// PreflightLocalWorkerJob is an original-factory, read-only pre-lease join.
// Expected rows are comparison DATA, never a caller-selected admission or
// permission. No allocation, effect, lease or receipt is minted here.
func (b *LocalOwnedStoreBootstrapV1) PreflightLocalWorkerJob(ctx context.Context, expected ports.WorkerJobState) error {
	if b == nil || b.self != b || ctx == nil || ctx.Err() != nil || b.store == nil || !b.store.localCompositionReady() ||
		expected.Job.Origin == nil || expected.Checkpoint != nil || validateLoadedWorkerJob(expected) != nil || expected.Run.Status.Terminal() {
		return domain.ErrMembershipDenied
	}
	configuration, err := b.source.Configuration()
	life, le := b.source.ExecutionLifetime()
	if err != nil || le != nil || life.Err() != nil || !reflect.DeepEqual(configuration, b.configuration) {
		return domain.ErrMembershipDenied
	}
	call, cancel := ownedAuthorityCallContext(ctx)
	defer cancel()
	err = b.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		loaded, err := loadOwnedWorkerJob(ctx, q, expected.Run.TenantID, expected.Run.ID, expected.Attempt.ID)
		if err != nil || !reflect.DeepEqual(loaded, expected) || loaded.Run.CancellationRequestedAt != nil ||
			loaded.Job.HarnessBinding.Profile != domain.HarnessExecutionOwnedAgentV2 || loaded.Job.SubstrateBinding == nil || loaded.Job.AdmissionCostCeiling == nil ||
			loaded.Job.ExecutionPlacementV2.Kind != domain.ExecutionPlacementManaged || loaded.Job.SubstrateBinding.Kind != domain.SubstrateLocalDevelopmentV1 ||
			configuration.ValidateForBinding(loaded.Job.HarnessBinding, *loaded.Job.SubstrateBinding, q.at) != nil {
			return q.budget.deny()
		}
		source, err := domain.DecodeAttemptExecutionSourceV1([]byte(loaded.Job.OwnedExecutionSourceRecord))
		if err != nil || source.Scope.TenantID != loaded.Run.TenantID || source.Scope.SessionID != loaded.Run.SessionID || source.Scope.RunID != loaded.Run.ID ||
			source.Scope.AttemptID != loaded.Attempt.ID || source.Scope.OwnerUserID != loaded.Job.CredentialOwnerUserID {
			return q.budget.deny()
		}
		final, err := readOwnedAdmission(ctx, q, source.Scope)
		if err != nil || !ownedAdmissionMatches(final, source, loaded.Job.OwnedExecutionSourceRecord, loaded.Reservation, loaded.Job) ||
			final.ValidateForExecution(loaded.Job.ExecutionPlacementV2, loaded.Job.HarnessBinding, *loaded.Job.SubstrateBinding, *loaded.Job.AdmissionCostCeiling) != nil ||
			b.store.ownedAttemptEffectSourceVerifier.VerifyOwnedAttemptEffectSource(source.Clone()) != nil || readOwnedAdmissionPrincipal(ctx, q, source.Scope) != nil {
			return q.budget.deny()
		}
		current, err := readOwnedAdmissionSources(ctx, q, source)
		if err != nil || configuration.ValidateForOwnedProfile(current.profile) != nil || loaded.Reservation.Status != domain.ReservationHeld ||
			!loaded.Reservation.ExpiresAt.After(q.at) || loaded.Reservation.UpdatedAt.After(q.at) ||
			loaded.Run.UpdatedAt.After(q.at) || loaded.Attempt.UpdatedAt.After(q.at) || life.Err() != nil || ctx.Err() != nil {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || ctx.Err() != nil || life.Err() != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}
