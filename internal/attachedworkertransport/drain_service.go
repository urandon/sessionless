package attachedworkertransport

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// DrainService exposes only the accepted drain authority to composition roots
// that must not gain enrollment, attach, or attempt-exchange capabilities.
type DrainService struct {
	broker DrainBroker
}

func NewDrainService(broker DrainBroker) (*DrainService, error) {
	if broker == nil {
		return nil, ErrTransportConfig
	}
	return &DrainService{broker: broker}, nil
}

func (service *DrainService) RequestDrain(ctx context.Context, tenantID domain.TenantID, ownerUserID domain.UserID, request DrainRequest) (ports.AttachedWorkerDrainResult, error) {
	if service == nil || service.broker == nil || tenantID.Validate() != nil || ownerUserID.Validate() != nil ||
		request.WorkerID.Validate() != nil || request.ExpectedWorkerRevision == 0 {
		return ports.AttachedWorkerDrainResult{}, ErrTransportUnauthorized
	}
	result, err := service.broker.RequestAttachedWorkerDrain(ctx, ports.AttachedWorkerDrainRequest{
		TenantID: tenantID, OwnerUserID: ownerUserID, WorkerID: request.WorkerID,
		ExpectedWorkerRevision: request.ExpectedWorkerRevision,
	})
	if err != nil {
		return ports.AttachedWorkerDrainResult{}, ErrTransportBackend
	}
	if result.Status != ports.AttachedWorkerExecutionApplied && result.Status != ports.AttachedWorkerExecutionReplayed {
		return ports.AttachedWorkerDrainResult{}, ErrTransportConflict
	}
	if result.Worker.Validate() != nil || result.Worker.TenantID != tenantID || result.Worker.OwnerUserID != ownerUserID ||
		result.Worker.ID != request.WorkerID || result.Worker.DesiredState != domain.AttachedWorkerDesiredDrain {
		return ports.AttachedWorkerDrainResult{}, ErrTransportUnauthorized
	}
	return result, nil
}
