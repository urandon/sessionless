package ports

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

type AttachedWorkerDrainRequest struct {
	TenantID               domain.TenantID
	OwnerUserID            domain.UserID
	WorkerID               domain.AttachedWorkerID
	ExpectedWorkerRevision uint64
}

type AttachedWorkerControlPoll struct {
	TenantID              domain.TenantID
	OwnerUserID           domain.UserID
	WorkerID              domain.AttachedWorkerID
	ConnectionID          domain.AttachedWorkerConnectionID
	PresentedSecretDigest domain.AttachedWorkerConnectionSecretDigest
}

type AttachedWorkerControlExchange struct {
	TenantID              domain.TenantID
	OwnerUserID           domain.UserID
	WorkerID              domain.AttachedWorkerID
	ConnectionID          domain.AttachedWorkerConnectionID
	PresentedSecretDigest domain.AttachedWorkerConnectionSecretDigest
	InboundFrame          attachedworkerprotocol.FrameV1
}

type AttachedWorkerDrainResult struct {
	Status     AttachedWorkerExecutionStatus
	Worker     domain.AttachedWorker
	Connection domain.AttachedWorkerConnection
	Outbound   *domain.AttachedWorkerControlMessageV1
}

// AttachedWorkerDrainStore closes admission and advances the canonical
// protocol snapshot in serializable owner-scoped transactions. It never
// invents attempt termination: Drained is accepted only after the durable
// attempt head and protocol snapshot are both retired/idle.
type AttachedWorkerDrainStore interface {
	RequestAttachedWorkerDrain(context.Context, AttachedWorkerDrainRequest) (AttachedWorkerDrainResult, error)
	PollAttachedWorkerControl(context.Context, AttachedWorkerControlPoll) (AttachedWorkerDrainResult, error)
	ExchangeAttachedWorkerControl(context.Context, AttachedWorkerControlExchange) (AttachedWorkerDrainResult, error)
}
