package ports

import (
	"context"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// AttachedComputeRegistration binds one existing worker to the owner's one
// pilot compute resource. This operation never supplies provider credentials
// or pretends that enrollment observed a valid provider subscription.
type AttachedComputeRegistration struct {
	Worker     domain.AttachedWorker
	ResourceID domain.SubscriptionConnectionID
	ActorID    domain.ActorID
}

// AttachedWorkerOnboardingStore authorizes current write membership inside all
// onboarding state transactions. Its identity mutations reuse AW-01 unchanged.
type AttachedWorkerOnboardingStore interface {
	AttachedWorkerStore
	RegisterAttachedComputeResource(context.Context, AttachedComputeRegistration) (ComputeConnectionState, error)
	ReconcileAttachedWorkerRotation(context.Context, domain.AttachedWorker, []byte) (domain.AttachedWorker, bool, error)
}
