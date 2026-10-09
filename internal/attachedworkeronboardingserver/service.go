// Package attachedworkeronboardingserver composes AW-01 identity lifecycle and
// one owned compute registration for privileged, private-file onboarding. It
// performs no provider, execution, transport, or credential activation.
package attachedworkeronboardingserver

import (
	"context"
	"errors"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworker"
	"gitcode.com/urandon/sessionless/internal/attachedworkeronboarding"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type Service struct {
	identity *attachedworker.Service
	store    ports.AttachedWorkerOnboardingStore
}

func New(config attachedworker.Config, store ports.AttachedWorkerOnboardingStore) (*Service, error) {
	config.RequireAdvancingRotationTime = true
	identity, err := attachedworker.New(config, store)
	if err != nil {
		return nil, err
	}
	return &Service{identity: identity, store: store}, nil
}

func (service *Service) PrepareGrant(ctx context.Context, tenant domain.TenantID, owner domain.UserID, request attachedworker.CreateEnrollmentRequest, origin string) (attachedworkeronboarding.GrantV1, error) {
	if err := attachedworkeronboarding.ValidateOrigin(origin); err != nil {
		return attachedworkeronboarding.GrantV1{}, err
	}
	grant, err := service.identity.PrepareEnrollment(ctx, tenant, owner, request)
	if err != nil {
		return attachedworkeronboarding.GrantV1{}, err
	}
	return attachedworkeronboarding.GrantV1{Version: 1, Enrollment: grant.Enrollment, ControlPlaneOrigin: origin, BootstrapSecret: grant.Secret.Bytes()}, nil
}

func (service *Service) PersistGrant(ctx context.Context, grant attachedworkeronboarding.GrantV1) error {
	if err := grant.Validate(); err != nil {
		return err
	}
	secret, err := attachedworker.ParseBootstrapSecret(grant.BootstrapSecret)
	if err != nil {
		return err
	}
	return service.identity.PersistEnrollment(ctx, attachedworker.EnrollmentGrant{Enrollment: grant.Enrollment, Secret: secret})
}

func (service *Service) Claim(ctx context.Context, claim attachedworkeronboarding.ClaimV1) (attachedworkeronboarding.ReceiptV1, error) {
	if err := claim.Validate(); err != nil {
		return attachedworkeronboarding.ReceiptV1{}, err
	}
	if claim.Enrollment.Revision != 1 || !claim.Enrollment.ConsumedAt.IsZero() {
		return attachedworkeronboarding.ReceiptV1{}, attachedworker.ErrEnrollmentDenied
	}
	enrollment, found, err := service.store.LoadAttachedWorkerEnrollment(ctx, claim.Enrollment.TenantID, claim.Enrollment.OwnerUserID, claim.Enrollment.ID)
	if err != nil {
		return attachedworkeronboarding.ReceiptV1{}, attachedworker.ErrBackend
	}
	if !found {
		return attachedworkeronboarding.ReceiptV1{}, attachedworker.ErrEnrollmentDenied
	}
	// AW-01 retains exact pristine claim replay after consumption. Compare the
	// immutable original grant, then let Claim decide its live single-use state.
	enrollment.ConsumedAt, enrollment.Revision = time.Time{}, claim.Enrollment.Revision
	if !reflect.DeepEqual(enrollment, claim.Enrollment) {
		return attachedworkeronboarding.ReceiptV1{}, attachedworker.ErrEnrollmentDenied
	}
	secret, err := attachedworker.ParseBootstrapSecret(claim.BootstrapSecret)
	if err != nil {
		return attachedworkeronboarding.ReceiptV1{}, err
	}
	worker, err := service.identity.Claim(ctx, claim.Enrollment.TenantID, claim.Enrollment.OwnerUserID, attachedworker.ClaimRequest{
		EnrollmentID: claim.Enrollment.ID, ExpectedEnrollmentRevision: claim.Enrollment.Revision, Audience: claim.Enrollment.Audience,
		BootstrapSecret: secret, IdentityPublicKey: append([]byte(nil), claim.IdentityPublicKey...), Proof: append([]byte(nil), claim.Proof...),
	})
	if err != nil {
		return attachedworkeronboarding.ReceiptV1{}, err
	}
	return service.register(ctx, worker)
}

func (service *Service) Head(ctx context.Context, tenant domain.TenantID, owner domain.UserID, worker domain.AttachedWorkerID) (domain.AttachedWorker, error) {
	return service.identity.Get(ctx, tenant, owner, worker)
}

func (service *Service) Rotate(ctx context.Context, request attachedworkeronboarding.RotationV1) (attachedworkeronboarding.ReceiptV1, error) {
	if err := request.Validate(); err != nil {
		return attachedworkeronboarding.ReceiptV1{}, err
	}
	// Resolve a previous ambiguous commit first; a changed/later worker head
	// never reconciles, even if it happens to use the proposed public key.
	worker, applied, err := service.store.ReconcileAttachedWorkerRotation(ctx, request.Worker, request.NewPublicKey)
	if err != nil {
		return attachedworkeronboarding.ReceiptV1{}, err
	}
	if applied {
		return service.register(ctx, worker)
	}
	// Enrollment does not authorize provider activation. Refuse a resource
	// already moved beyond this unknown/disabled onboarding slice before CAS.
	if _, err := service.register(ctx, request.Worker); err != nil {
		return attachedworkeronboarding.ReceiptV1{}, err
	}
	worker, err = service.identity.RotateIdentity(ctx, request.Worker.TenantID, request.Worker.OwnerUserID, attachedworker.RotateIdentityRequest{
		WorkerID: request.Worker.ID, ExpectedRevision: request.Worker.Revision, NewPublicKey: append([]byte(nil), request.NewPublicKey...),
		CurrentProof: append([]byte(nil), request.CurrentProof...), NewProof: append([]byte(nil), request.NewProof...),
	})
	if err != nil {
		if !errors.Is(err, attachedworker.ErrBackend) && !errors.Is(err, attachedworker.ErrWorkerConflict) {
			return attachedworkeronboarding.ReceiptV1{}, err
		}
		var reconcileErr error
		worker, applied, reconcileErr = service.store.ReconcileAttachedWorkerRotation(ctx, request.Worker, request.NewPublicKey)
		if reconcileErr != nil || !applied {
			return attachedworkeronboarding.ReceiptV1{}, err
		}
	}
	return service.register(ctx, worker)
}

func (service *Service) register(ctx context.Context, worker domain.AttachedWorker) (attachedworkeronboarding.ReceiptV1, error) {
	resource := attachedworkeronboarding.ResourceIDForWorker(worker.TenantID, worker.OwnerUserID, worker.ID)
	actor := attachedworkeronboarding.ActorIDForOwner(worker.TenantID, worker.OwnerUserID)
	connection, err := service.store.RegisterAttachedComputeResource(ctx, ports.AttachedComputeRegistration{Worker: worker, ResourceID: resource, ActorID: actor})
	if err != nil {
		return attachedworkeronboarding.ReceiptV1{}, err
	}
	receipt := attachedworkeronboarding.ReceiptV1{Version: 1, Worker: worker, ResourceID: resource, ActorID: actor, Entitlement: connection.Entitlement, Quota: connection.Quota}
	if err := receipt.Validate(); err != nil {
		return attachedworkeronboarding.ReceiptV1{}, err
	}
	return receipt, nil
}
