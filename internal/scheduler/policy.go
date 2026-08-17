// Package scheduler implements deterministic admission and durable dispatch.
package scheduler

import (
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type Snapshot struct {
	Entitlement domain.EntitlementState
	Quota       domain.ProviderQuotaState
	Slot        domain.SubscriptionSchedulerSlot
	QueueDepth  uint32
	ActiveRuns  uint32
}

type Decision struct {
	Admit   bool
	State   domain.SchedulerState
	Code    string
	RetryAt *time.Time
}

func Evaluate(
	now time.Time,
	limits domain.ProductLimits,
	shape domain.WorkloadShape,
	snapshot Snapshot,
) (Decision, error) {
	if now.IsZero() {
		return Decision{}, domain.ValidationError{Field: "scheduler.now", Reason: "must not be zero"}
	}
	if err := limits.ValidateForAdmission(); err != nil {
		return Decision{}, err
	}
	if err := shape.Validate(); err != nil {
		return Decision{}, err
	}
	if err := snapshot.Slot.Validate(); err != nil {
		return Decision{}, err
	}

	switch snapshot.Entitlement {
	case domain.EntitlementDisconnected, domain.EntitlementUnknown,
		domain.EntitlementInactive,
		domain.EntitlementReauthRequired:
		return Decision{
			State: domain.SchedulerReauthRequired,
			Code:  "subscription_reauthentication_required",
		}, nil
	}
	if snapshot.Slot.State == domain.SchedulerDraining {
		return Decision{State: domain.SchedulerDraining, Code: "subscription_draining"}, nil
	}
	if snapshot.Slot.State == domain.SchedulerBlockedUntilReset &&
		snapshot.Slot.BlockedUntil != nil &&
		now.Before(*snapshot.Slot.BlockedUntil) {
		retryAt := *snapshot.Slot.BlockedUntil
		return Decision{
			State:   domain.SchedulerBlockedUntilReset,
			Code:    "provider_quota_blocked",
			RetryAt: &retryAt,
		}, nil
	}
	if snapshot.Quota == domain.ProviderQuotaExhausted {
		return Decision{
			State: domain.SchedulerBlockedUntilReset,
			Code:  "provider_quota_exhausted_reset_unknown",
		}, nil
	}
	switch {
	case shape.Runtime > limits.MaxRuntime:
		return Decision{State: domain.SchedulerPressured, Code: "runtime_limit_exceeded"}, nil
	case shape.Turns > limits.MaxTurns:
		return Decision{State: domain.SchedulerPressured, Code: "turn_limit_exceeded"}, nil
	case shape.InputBytes > limits.MaxInputBytes:
		return Decision{State: domain.SchedulerPressured, Code: "input_limit_exceeded"}, nil
	case shape.ContextBytes > limits.MaxContextBytes:
		return Decision{State: domain.SchedulerPressured, Code: "context_limit_exceeded"}, nil
	case shape.Artifacts > limits.MaxArtifacts:
		return Decision{State: domain.SchedulerPressured, Code: "artifact_limit_exceeded"}, nil
	case snapshot.Slot.ActiveRunID != "":
		return Decision{State: domain.SchedulerPressured, Code: "subscription_slot_busy"}, nil
	case snapshot.QueueDepth >= limits.MaxTenantQueueDepth:
		return Decision{State: domain.SchedulerPressured, Code: "tenant_queue_depth_exhausted"}, nil
	case snapshot.ActiveRuns >= limits.MaxActiveRuns:
		return Decision{State: domain.SchedulerPressured, Code: "tenant_active_run_limit"}, nil
	default:
		return Decision{Admit: true, State: domain.SchedulerReady, Code: "admitted"}, nil
	}
}
