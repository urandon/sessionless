package domain

import "time"

type ReservationStatus string

const (
	ReservationHeld      ReservationStatus = "held"
	ReservationCommitted ReservationStatus = "committed"
	ReservationReleased  ReservationStatus = "released"
	ReservationExpired   ReservationStatus = "expired"
)

func (status ReservationStatus) Valid() bool {
	switch status {
	case ReservationHeld, ReservationCommitted, ReservationReleased, ReservationExpired:
		return true
	default:
		return false
	}
}

func CanTransitionReservation(from, to ReservationStatus) bool {
	return from == ReservationHeld &&
		(to == ReservationCommitted || to == ReservationReleased || to == ReservationExpired)
}

type QuotaReservation struct {
	ID                       QuotaReservationID       `json:"id"`
	TenantID                 TenantID                 `json:"tenant_id"`
	RunID                    RunID                    `json:"run_id"`
	SubscriptionConnectionID SubscriptionConnectionID `json:"subscription_connection_id"`
	Status                   ReservationStatus        `json:"status"`
	CapacityUnits            uint32                   `json:"capacity_units"`
	HeldAt                   time.Time                `json:"held_at"`
	ExpiresAt                time.Time                `json:"expires_at"`
	UpdatedAt                time.Time                `json:"updated_at"`
}

func (reservation QuotaReservation) ValidateForRun(run Run) error {
	if err := run.Validate(); err != nil {
		return err
	}
	if err := reservation.ID.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(run.TenantID, reservation.TenantID); err != nil {
		return err
	}
	if reservation.RunID != run.ID {
		return ValidationError{Field: "quota_reservation.run_id", Reason: "must reference the owning run"}
	}
	if reservation.SubscriptionConnectionID != run.SubscriptionConnectionID {
		return ValidationError{Field: "quota_reservation.subscription_connection_id", Reason: "must match the owning run"}
	}
	if !reservation.Status.Valid() {
		return ValidationError{Field: "quota_reservation.status", Reason: "is unknown"}
	}
	if reservation.CapacityUnits == 0 {
		return ValidationError{Field: "quota_reservation.capacity_units", Reason: "must be positive"}
	}
	if reservation.HeldAt.IsZero() || !reservation.ExpiresAt.After(reservation.HeldAt) {
		return ValidationError{Field: "quota_reservation.expires_at", Reason: "must be after a non-zero held_at"}
	}
	if reservation.UpdatedAt.Before(reservation.HeldAt) {
		return ValidationError{Field: "quota_reservation.updated_at", Reason: "must not be before held_at"}
	}
	return nil
}

func (reservation *QuotaReservation) Transition(to ReservationStatus, at time.Time) error {
	if reservation == nil {
		return ValidationError{Field: "quota_reservation", Reason: "must not be nil"}
	}
	if !CanTransitionReservation(reservation.Status, to) {
		return ValidationError{Field: "quota_reservation.status", Reason: "transition is not allowed"}
	}
	if at.Before(reservation.UpdatedAt) {
		return ValidationError{Field: "quota_reservation.updated_at", Reason: "transition time must not move backwards"}
	}
	reservation.Status = to
	reservation.UpdatedAt = at
	return nil
}

type ProviderQuotaState string

const (
	ProviderQuotaUnknown   ProviderQuotaState = "unknown"
	ProviderQuotaAvailable ProviderQuotaState = "available"
	ProviderQuotaLimited   ProviderQuotaState = "limited"
	ProviderQuotaExhausted ProviderQuotaState = "exhausted"
)

func (state ProviderQuotaState) Valid() bool {
	switch state {
	case ProviderQuotaUnknown, ProviderQuotaAvailable, ProviderQuotaLimited, ProviderQuotaExhausted:
		return true
	default:
		return false
	}
}

func CanTransitionProviderQuota(from, to ProviderQuotaState) bool {
	if !from.Valid() || !to.Valid() || from == to {
		return false
	}
	return true
}

// ProviderQuotaSnapshot represents only provider-observed quota. Remaining is
// nil when the provider does not expose a trustworthy value.
type ProviderQuotaSnapshot struct {
	TenantID                 TenantID                 `json:"tenant_id"`
	SubscriptionConnectionID SubscriptionConnectionID `json:"subscription_connection_id"`
	State                    ProviderQuotaState       `json:"state"`
	Remaining                *int64                   `json:"remaining,omitempty"`
	ResetAt                  *time.Time               `json:"reset_at,omitempty"`
	ObservedAt               time.Time                `json:"observed_at"`
}

func (snapshot ProviderQuotaSnapshot) Validate() error {
	if err := snapshot.TenantID.Validate(); err != nil {
		return err
	}
	if err := snapshot.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	if !snapshot.State.Valid() {
		return ValidationError{Field: "provider_quota.state", Reason: "is unknown"}
	}
	if snapshot.ObservedAt.IsZero() {
		return ValidationError{Field: "provider_quota.observed_at", Reason: "must not be zero"}
	}
	if snapshot.Remaining != nil && *snapshot.Remaining < 0 {
		return ValidationError{Field: "provider_quota.remaining", Reason: "must not be negative"}
	}
	if snapshot.State == ProviderQuotaUnknown && (snapshot.Remaining != nil || snapshot.ResetAt != nil) {
		return ValidationError{Field: "provider_quota", Reason: "unknown quota must not fabricate remaining or reset values"}
	}
	return nil
}

type UsageSource string

const (
	UsageSourceProvider UsageSource = "provider"
	UsageSourceHarness  UsageSource = "harness"
)

type UsageObservation struct {
	ID                       UsageObservationID       `json:"id"`
	TenantID                 TenantID                 `json:"tenant_id"`
	RunID                    RunID                    `json:"run_id"`
	AttemptID                AttemptID                `json:"attempt_id"`
	SubscriptionConnectionID SubscriptionConnectionID `json:"subscription_connection_id"`
	Source                   UsageSource              `json:"source"`
	InputTokens              *uint64                  `json:"input_tokens,omitempty"`
	OutputTokens             *uint64                  `json:"output_tokens,omitempty"`
	ObservedAt               time.Time                `json:"observed_at"`
}

func (observation UsageObservation) ValidateForAttempt(run Run, attempt Attempt) error {
	if err := attempt.ValidateForRun(run); err != nil {
		return err
	}
	if err := observation.ID.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(run.TenantID, observation.TenantID); err != nil {
		return err
	}
	if observation.RunID != run.ID || observation.AttemptID != attempt.ID {
		return ValidationError{Field: "usage_observation", Reason: "must reference the owning run and attempt"}
	}
	if observation.SubscriptionConnectionID != run.SubscriptionConnectionID {
		return ValidationError{Field: "usage_observation.subscription_connection_id", Reason: "must match the owning run"}
	}
	if observation.Source != UsageSourceProvider && observation.Source != UsageSourceHarness {
		return ValidationError{Field: "usage_observation.source", Reason: "is unknown"}
	}
	if observation.InputTokens == nil && observation.OutputTokens == nil {
		return ValidationError{Field: "usage_observation", Reason: "must include at least one observed token count"}
	}
	if observation.ObservedAt.IsZero() {
		return ValidationError{Field: "usage_observation.observed_at", Reason: "must not be zero"}
	}
	return nil
}

type EntitlementState string

const (
	EntitlementDisconnected   EntitlementState = "disconnected"
	EntitlementUnknown        EntitlementState = "unknown"
	EntitlementActive         EntitlementState = "active"
	EntitlementInactive       EntitlementState = "inactive"
	EntitlementReauthRequired EntitlementState = "reauthentication_required"
)

func (state EntitlementState) Valid() bool {
	switch state {
	case EntitlementDisconnected, EntitlementUnknown, EntitlementActive,
		EntitlementInactive, EntitlementReauthRequired:
		return true
	default:
		return false
	}
}

type EntitlementSnapshot struct {
	TenantID                 TenantID                 `json:"tenant_id"`
	SubscriptionConnectionID SubscriptionConnectionID `json:"subscription_connection_id"`
	State                    EntitlementState         `json:"state"`
	ObservedAt               time.Time                `json:"observed_at"`
}

func (snapshot EntitlementSnapshot) Validate() error {
	if err := snapshot.TenantID.Validate(); err != nil {
		return err
	}
	if err := snapshot.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	if !snapshot.State.Valid() {
		return ValidationError{Field: "entitlement.state", Reason: "is unknown"}
	}
	if snapshot.ObservedAt.IsZero() {
		return ValidationError{Field: "entitlement.observed_at", Reason: "must not be zero"}
	}
	return nil
}
