package domain

import "time"

// SchedulerState is the control-plane view of whether a subscription
// connection may accept another run. Provider quota remains independently
// observable and may be unknown.
type SchedulerState string

const (
	SchedulerReady             SchedulerState = "ready"
	SchedulerPressured         SchedulerState = "pressured"
	SchedulerDraining          SchedulerState = "draining"
	SchedulerBlockedUntilReset SchedulerState = "blocked_until_reset"
	SchedulerReauthRequired    SchedulerState = "reauth_required"
)

func (state SchedulerState) Valid() bool {
	switch state {
	case SchedulerReady, SchedulerPressured, SchedulerDraining,
		SchedulerBlockedUntilReset, SchedulerReauthRequired:
		return true
	default:
		return false
	}
}

type ProductLimits struct {
	MaxTenantQueueDepth uint32        `json:"max_tenant_queue_depth"`
	MaxActiveRuns       uint32        `json:"max_active_runs"`
	MaxRuntime          time.Duration `json:"max_runtime"`
	MaxTurns            uint32        `json:"max_turns"`
	MaxInputBytes       uint64        `json:"max_input_bytes"`
	MaxContextBytes     uint64        `json:"max_context_bytes"`
	MaxArtifacts        uint32        `json:"max_artifacts"`
	MaxToolEvents       uint32        `json:"max_tool_events"`
	MaxToolEventBytes   uint64        `json:"max_tool_event_bytes"`
}

func (limits ProductLimits) Validate() error {
	if limits.MaxTenantQueueDepth == 0 {
		return ValidationError{Field: "limits.max_tenant_queue_depth", Reason: "must be positive"}
	}
	if limits.MaxActiveRuns == 0 {
		return ValidationError{Field: "limits.max_active_runs", Reason: "must be positive"}
	}
	if limits.MaxRuntime <= 0 {
		return ValidationError{Field: "limits.max_runtime", Reason: "must be positive"}
	}
	if limits.MaxTurns == 0 {
		return ValidationError{Field: "limits.max_turns", Reason: "must be positive"}
	}
	if limits.MaxInputBytes == 0 || limits.MaxContextBytes == 0 {
		return ValidationError{Field: "limits.bytes", Reason: "input and context limits must be positive"}
	}
	if limits.MaxArtifacts == 0 {
		return ValidationError{Field: "limits.max_artifacts", Reason: "must be positive"}
	}
	if (limits.MaxToolEvents == 0) != (limits.MaxToolEventBytes == 0) {
		return ValidationError{
			Field:  "limits.tool_events",
			Reason: "count and byte limits must be configured together",
		}
	}
	return nil
}

// ValidateForAdmission requires the explicit tool-event budget written by new
// schedulers. Validate alone also accepts the all-zero legacy representation so
// workers can load jobs persisted before these fields were introduced.
func (limits ProductLimits) ValidateForAdmission() error {
	if err := limits.Validate(); err != nil {
		return err
	}
	if limits.MaxToolEvents == 0 {
		return ValidationError{
			Field:  "limits.tool_events",
			Reason: "count and byte limits must be positive for admission",
		}
	}
	return nil
}

// EffectiveToolEventLimits returns the explicitly admitted tool-event budget.
// Jobs persisted before these fields existed use finite limits derived from the
// already-admitted turn and context budgets so rolling upgrades remain safe.
func (limits ProductLimits) EffectiveToolEventLimits() (maxEvents uint32, maxBytes uint64) {
	if limits.MaxToolEvents != 0 {
		return limits.MaxToolEvents, limits.MaxToolEventBytes
	}
	maxEvents = limits.MaxTurns
	if maxEvents > ^uint32(0)/2 {
		maxEvents = ^uint32(0)
	} else {
		maxEvents *= 2
	}
	return maxEvents, limits.MaxContextBytes
}

type WorkloadShape struct {
	Runtime      time.Duration `json:"runtime"`
	Turns        uint32        `json:"turns"`
	InputBytes   uint64        `json:"input_bytes"`
	ContextBytes uint64        `json:"context_bytes"`
	Artifacts    uint32        `json:"artifacts"`
}

func (shape WorkloadShape) Validate() error {
	if shape.Runtime <= 0 {
		return ValidationError{Field: "workload.runtime", Reason: "must be positive"}
	}
	if shape.Turns == 0 {
		return ValidationError{Field: "workload.turns", Reason: "must be positive"}
	}
	return nil
}

// SubscriptionSchedulerSlot is the single contention row for the MVP rule
// that one subscription connection may own at most one reservation.
type SubscriptionSchedulerSlot struct {
	TenantID                 TenantID                 `json:"tenant_id"`
	SubscriptionConnectionID SubscriptionConnectionID `json:"subscription_connection_id"`
	State                    SchedulerState           `json:"state"`
	ActiveRunID              RunID                    `json:"active_run_id,omitempty"`
	ActiveReservationID      QuotaReservationID       `json:"active_reservation_id,omitempty"`
	BlockedUntil             *time.Time               `json:"blocked_until,omitempty"`
	UpdatedAt                time.Time                `json:"updated_at"`
}

func (slot SubscriptionSchedulerSlot) Validate() error {
	if err := slot.TenantID.Validate(); err != nil {
		return err
	}
	if err := slot.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	if !slot.State.Valid() {
		return ValidationError{Field: "scheduler_slot.state", Reason: "is unknown"}
	}
	if (slot.ActiveRunID == "") != (slot.ActiveReservationID == "") {
		return ValidationError{
			Field:  "scheduler_slot.active",
			Reason: "run and reservation identifiers must be present together",
		}
	}
	if slot.ActiveRunID != "" {
		if err := slot.ActiveRunID.Validate(); err != nil {
			return err
		}
		if err := slot.ActiveReservationID.Validate(); err != nil {
			return err
		}
	}
	if slot.State != SchedulerBlockedUntilReset && slot.BlockedUntil != nil {
		return ValidationError{
			Field:  "scheduler_slot.blocked_until",
			Reason: "is allowed only while blocked until reset",
		}
	}
	if slot.UpdatedAt.IsZero() {
		return ValidationError{Field: "scheduler_slot.updated_at", Reason: "must not be zero"}
	}
	return nil
}
