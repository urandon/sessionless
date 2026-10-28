package attachedworkertransport

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// PublishResourceGenerationBearer binds untrusted metadata to the ORIGINAL
// connected worker. Parsing is not write authority: the existing Store still
// rechecks every current identity/resource fact inside its mutation transaction.
// This makes one store call, never a mutation retry or an automatic readback.
func (service *Service) PublishResourceGenerationBearer(ctx context.Context, raw []byte, request ports.AttachedResourceGenerationPublicationV1) (ports.AttachedResourceGenerationResultV1, error) {
	zero := ports.AttachedResourceGenerationResultV1{}
	auth, err := bindResourceGenerationBearer(raw, request.Authorization)
	if err != nil {
		return zero, err
	}
	request.Authorization = auth
	if request.Validate() != nil || service == nil || service.store == nil || ctx == nil || ctx.Err() != nil {
		return zero, ErrTransportUnauthorized
	}
	store, ok := service.store.(ports.AttachedResourceGenerationStoreV1)
	if !ok {
		return zero, ErrTransportConfig
	}
	call, cancel := context.WithTimeout(ctx, ports.AttachedResourceGenerationOperationTimeoutV1)
	defer cancel()
	result, err := store.PublishAttachedResourceGenerationV1(call, request)
	if err != nil || call.Err() != nil {
		return zero, ErrTransportBackend
	}
	if err := validateResourceGenerationResult(result, auth, true); err != nil {
		return zero, err
	}
	if result.Head != request.Target {
		return zero, ErrTransportBackend
	}
	return result, nil
}

func (service *Service) ReadResourceGenerationBearer(ctx context.Context, raw []byte, request ports.AttachedResourceGenerationAuthorizationV1) (ports.AttachedResourceGenerationResultV1, error) {
	zero := ports.AttachedResourceGenerationResultV1{}
	auth, err := bindResourceGenerationBearer(raw, request)
	if err != nil {
		return zero, err
	}
	if service == nil || service.store == nil || ctx == nil || ctx.Err() != nil {
		return zero, ErrTransportUnauthorized
	}
	store, ok := service.store.(ports.AttachedResourceGenerationStoreV1)
	if !ok {
		return zero, ErrTransportConfig
	}
	call, cancel := context.WithTimeout(ctx, ports.AttachedResourceGenerationOperationTimeoutV1)
	defer cancel()
	result, err := store.ReadAttachedResourceGenerationV1(call, auth)
	if err != nil || call.Err() != nil {
		return zero, ErrTransportBackend
	}
	if err := validateResourceGenerationResult(result, auth, false); err != nil {
		return zero, err
	}
	return result, nil
}

func bindResourceGenerationBearer(raw []byte, auth ports.AttachedResourceGenerationAuthorizationV1) (ports.AttachedResourceGenerationAuthorizationV1, error) {
	bearer, err := ParseConnectionBearer(raw)
	if err != nil {
		return ports.AttachedResourceGenerationAuthorizationV1{}, ErrTransportUnauthorized
	}
	defer clear(bearer.secret.value[:])
	if auth.PresentedSecretDigest != "" || auth.TenantID != bearer.tenantID || auth.OwnerUserID != bearer.ownerUserID || auth.WorkerID != bearer.workerID || auth.ConnectionID != bearer.connectionID {
		return ports.AttachedResourceGenerationAuthorizationV1{}, ErrTransportUnauthorized
	}
	auth.PresentedSecretDigest = bearer.secret.Digest()
	if auth.Validate() != nil {
		return ports.AttachedResourceGenerationAuthorizationV1{}, ErrTransportUnauthorized
	}
	return auth, nil
}

func validateResourceGenerationResult(result ports.AttachedResourceGenerationResultV1, auth ports.AttachedResourceGenerationAuthorizationV1, publication bool) error {
	zero := domain.AttachedResourceGenerationHeadV1{}
	switch result.Status {
	case ports.AttachedWorkerExecutionDenied, ports.AttachedWorkerExecutionExpired, ports.AttachedWorkerExecutionFenced:
		if result.Head != zero {
			return ErrTransportBackend
		}
		return ErrTransportUnauthorized
	case ports.AttachedWorkerExecutionConflict:
		if result.Head != zero {
			return ErrTransportBackend
		}
		return ErrTransportConflict
	case ports.AttachedWorkerExecutionNotFound:
		if publication || result.Head != zero {
			return ErrTransportBackend
		}
		return nil
	case ports.AttachedWorkerExecutionApplied:
	case ports.AttachedWorkerExecutionReplayed:
		if !publication {
			return ErrTransportBackend
		}
	default:
		return ErrTransportBackend
	}
	head := result.Head
	if head.Validate() != nil || head.OwnerUserID != auth.OwnerUserID || head.WorkerID != auth.WorkerID || head.EnrollmentGeneration != auth.EnrollmentGeneration || head.BaselineRevision != auth.BaselineRevision {
		return ErrTransportBackend
	}
	if _, err := domain.EncodeAttachedResourceGenerationHeadV1(head); err != nil {
		return ErrTransportBackend
	}
	return nil
}
