package domain

import "time"

const (
	AttachedWorkerControlMessageVersionV1 uint32 = 1
	maxAttachedWorkerControlMessageBytes         = 64 << 10
)

type AttachedWorkerControlMessageKind string

const (
	AttachedWorkerControlMessageDrain   AttachedWorkerControlMessageKind = "drain"
	AttachedWorkerControlMessageDrained AttachedWorkerControlMessageKind = "drained"
)

func (kind AttachedWorkerControlMessageKind) Valid() bool {
	return kind == AttachedWorkerControlMessageDrain || kind == AttachedWorkerControlMessageDrained
}

// AttachedWorkerControlMessageV1 is the replayable owner-scoped delivery
// ledger for one semantic drain revision. A pending platform Drain has no
// envelope fields until all earlier platform frames have been acknowledged.
// Reconnect may replace only those envelope fields; the semantic revision and
// direction remain immutable.
type AttachedWorkerControlMessageV1 struct {
	Version              uint32                                  `json:"version"`
	TenantID             TenantID                                `json:"tenant_id"`
	OwnerUserID          UserID                                  `json:"owner_user_id"`
	WorkerID             AttachedWorkerID                        `json:"worker_id"`
	DrainRevision        uint64                                  `json:"drain_revision"`
	Direction            AttachedWorkerAttemptDirection          `json:"direction"`
	ConnectionGeneration uint64                                  `json:"connection_generation,omitempty"`
	EnvelopeSequence     uint64                                  `json:"envelope_sequence,omitempty"`
	Kind                 AttachedWorkerControlMessageKind        `json:"kind"`
	Fingerprint          AttachedWorkerAttemptMessageFingerprint `json:"fingerprint,omitempty"`
	Payload              []byte                                  `json:"payload,omitempty"`
	CreatedAt            time.Time                               `json:"created_at"`
}

func (message AttachedWorkerControlMessageV1) Validate() error {
	if message.Version != AttachedWorkerControlMessageVersionV1 {
		return ValidationError{Field: "attached_worker_control_message.version", Reason: "must be version 1"}
	}
	if err := message.TenantID.Validate(); err != nil {
		return err
	}
	if err := message.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := message.WorkerID.Validate(); err != nil {
		return err
	}
	if message.DrainRevision == 0 || !message.Direction.Valid() || !message.Kind.Valid() || message.CreatedAt.IsZero() {
		return ValidationError{Field: "attached_worker_control_message.routing", Reason: "must be complete"}
	}
	if (message.Kind == AttachedWorkerControlMessageDrain) != (message.Direction == AttachedWorkerAttemptPlatformToWorker) {
		return ValidationError{Field: "attached_worker_control_message.direction", Reason: "must match message kind"}
	}
	pending := message.ConnectionGeneration == 0 && message.EnvelopeSequence == 0 && message.Fingerprint == "" && len(message.Payload) == 0
	delivered := message.ConnectionGeneration > 0 && message.EnvelopeSequence > 0 && message.Fingerprint != "" && len(message.Payload) > 0
	if !pending && !delivered {
		return ValidationError{Field: "attached_worker_control_message.envelope", Reason: "must be wholly pending or wholly delivered"}
	}
	if pending && message.Kind != AttachedWorkerControlMessageDrain {
		return ValidationError{Field: "attached_worker_control_message.envelope", Reason: "only an outbound drain may be pending"}
	}
	if delivered {
		if err := message.Fingerprint.Validate(); err != nil {
			return err
		}
		if len(message.Payload) > maxAttachedWorkerControlMessageBytes {
			return ValidationError{Field: "attached_worker_control_message.payload", Reason: "must be at most 64 KiB"}
		}
	}
	return nil
}
