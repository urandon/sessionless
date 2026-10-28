package domain

import (
	"errors"
	"time"
)

type ExternalIdentityStateV2 string
type IdentityOperationKindV2 string

const (
	ExternalIdentityActiveV2   ExternalIdentityStateV2 = "active"
	ExternalIdentityUnlinkedV2 ExternalIdentityStateV2 = "unlinked"
	IdentityOperationLinkV2    IdentityOperationKindV2 = "link"
	IdentityOperationUnlinkV2  IdentityOperationKindV2 = "unlink"
)

var (
	ErrIdentityLastMethodV2          = errors.New("cannot remove the final enabled login method")
	ErrIdentityProviderUnavailableV2 = errors.New("identity provider is not enabled")
)

// ExternalIdentityV2 is the canonical ownership row, not the reverse projection.
// An unlinked row retains immutable ownership and its revision without a TTL.
type ExternalIdentityV2 struct {
	Version   uint32                  `json:"version"`
	Subject   ExternalSubject         `json:"subject"`
	UserID    UserID                  `json:"user_id"`
	CreatedAt time.Time               `json:"created_at"`
	State     ExternalIdentityStateV2 `json:"state"`
	Revision  AuthRevisionV2          `json:"security_revision"`
	UpdatedAt time.Time               `json:"updated_at"`
}

func (identity ExternalIdentityV2) Validate() error {
	if identity.Version != 2 {
		return identityValidationV2("version", "must be 2")
	}
	if err := validateIdentitySubjectV2(identity.Subject); err != nil {
		return err
	}
	if err := identity.UserID.Validate(); err != nil {
		return err
	}
	if identity.State != ExternalIdentityActiveV2 && identity.State != ExternalIdentityUnlinkedV2 {
		return identityValidationV2("state", "must be active or unlinked")
	}
	if err := identity.Revision.Validate(); err != nil {
		return err
	}
	if identity.CreatedAt.IsZero() || identity.UpdatedAt.Before(identity.CreatedAt) {
		return identityValidationV2("updated_at", "must not precede a nonzero created_at")
	}
	return nil
}

// ValidateExternalIdentityOwnershipV2 protects the immutable ownership tuple.
// It does not grant a state transition or prove either provider account.
func ValidateExternalIdentityOwnershipV2(previous, next ExternalIdentityV2) error {
	if err := previous.Validate(); err != nil {
		return err
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if previous.Subject != next.Subject || previous.UserID != next.UserID || !previous.CreatedAt.Equal(next.CreatedAt) {
		return ErrExternalIdentityConflict
	}
	if next.UpdatedAt.Before(previous.UpdatedAt) || next.Revision < previous.Revision {
		return identityValidationV2("revision", "must not move ownership time or revision backwards")
	}
	if previous.State != next.State && next.Revision <= previous.Revision {
		return identityValidationV2("revision", "must advance for a state change")
	}
	return nil
}

// ValidateExternalIdentitySetV2 accepts complete canonical ACTIVE rows only.
// The store must bound its reverse lookup with LIMIT 3 and verify correspondence
// before supplying these rows; a truncated range is not a complete active set.
func ValidateExternalIdentitySetV2(user UserID, active []ExternalIdentityV2) (AuthRevisionV2, error) {
	if err := user.Validate(); err != nil {
		return 0, err
	}
	if len(active) < 1 || len(active) > 2 {
		return 0, identityValidationV2("active_set", "must contain one or two canonical methods")
	}
	providers := make(map[IdentityProvider]bool, 2)
	var revision AuthRevisionV2
	for _, identity := range active {
		if err := identity.Validate(); err != nil {
			return 0, err
		}
		if identity.UserID != user || identity.State != ExternalIdentityActiveV2 || providers[identity.Subject.Provider] {
			return 0, identityValidationV2("active_set", "requires same-owner active methods with distinct providers")
		}
		if revision != 0 && revision != identity.Revision {
			return 0, identityValidationV2("active_set", "requires a common security revision")
		}
		providers[identity.Subject.Provider] = true
		revision = identity.Revision
	}
	return revision, nil
}

type ExternalIdentityChangePlanV2 struct {
	Active         []ExternalIdentityV2
	Unlinked       *ExternalIdentityV2
	BeforeRevision AuthRevisionV2
	AfterRevision  AuthRevisionV2
}

// PlanExternalIdentityChangeV2 is pure row planning, NOT a proof or write grant.
// Its caller must authenticate the immutable operation and its proofs, compare
// the expected revision, and read the complete active set and target in the SAME
// serializable transaction that applies this plan and its audit/session outcome.
// at is that transaction's clock, never a freshly fabricated proof observation.
func PlanExternalIdentityChangeV2(
	user UserID,
	active []ExternalIdentityV2,
	targetSubject ExternalSubject,
	existingTarget *ExternalIdentityV2,
	kind IdentityOperationKindV2,
	enabledProviders []IdentityProvider,
	at time.Time,
) (ExternalIdentityChangePlanV2, error) {
	var empty ExternalIdentityChangePlanV2
	before, err := ValidateExternalIdentitySetV2(user, active)
	if err != nil {
		return empty, err
	}
	if err := validateIdentitySubjectV2(targetSubject); err != nil {
		return empty, err
	}
	if kind != IdentityOperationLinkV2 && kind != IdentityOperationUnlinkV2 {
		return empty, identityValidationV2("kind", "must be link or unlink")
	}
	enabled, err := enabledIdentityProvidersV2(enabledProviders)
	if err != nil {
		return empty, err
	}
	if at.IsZero() {
		return empty, identityValidationV2("at", "must be the nonzero transaction time")
	}
	for _, identity := range active {
		if at.Before(identity.UpdatedAt) {
			return empty, identityValidationV2("at", "must not precede canonical identity observations")
		}
	}
	if existingTarget != nil {
		if err := existingTarget.Validate(); err != nil {
			return empty, err
		}
		if existingTarget.Subject != targetSubject || existingTarget.UserID != user {
			return empty, ErrExternalIdentityConflict
		}
		if at.Before(existingTarget.UpdatedAt) {
			return empty, identityValidationV2("at", "must not precede target observations")
		}
	}
	if kind == IdentityOperationLinkV2 {
		if !enabled[targetSubject.Provider] {
			return empty, ErrIdentityProviderUnavailableV2
		}
		for _, identity := range active {
			if identity.Subject.Provider == targetSubject.Provider {
				return empty, ErrExternalIdentityConflict
			}
		}
		if existingTarget != nil && existingTarget.State != ExternalIdentityUnlinkedV2 {
			return empty, ErrExternalIdentityConflict
		}
	} else {
		matched, usable := false, false
		for _, identity := range active {
			if identity.Subject == targetSubject {
				matched = existingTarget != nil && sameExternalIdentityV2(identity, *existingTarget)
			} else if enabled[identity.Subject.Provider] {
				usable = true
			}
		}
		if !matched {
			return empty, ErrExternalIdentityConflict
		}
		if !usable {
			return empty, ErrIdentityLastMethodV2
		}
	}
	revisions := []AuthRevisionV2{before}
	if existingTarget != nil {
		revisions = append(revisions, existingTarget.Revision)
	}
	next, err := NextAuthRevisionV2(revisions...)
	if err != nil {
		return empty, err
	}
	plan := ExternalIdentityChangePlanV2{BeforeRevision: before, AfterRevision: next, Active: make([]ExternalIdentityV2, 0, 2)}
	for _, identity := range active {
		identity.Revision, identity.UpdatedAt = next, at
		if kind == IdentityOperationUnlinkV2 && identity.Subject == targetSubject {
			identity.State = ExternalIdentityUnlinkedV2
			plan.Unlinked = &identity
		} else {
			plan.Active = append(plan.Active, identity)
		}
	}
	if kind == IdentityOperationLinkV2 {
		identity := ExternalIdentityV2{Version: 2, Subject: targetSubject, UserID: user, CreatedAt: at, State: ExternalIdentityActiveV2}
		if existingTarget != nil {
			identity = *existingTarget
			identity.State = ExternalIdentityActiveV2
		}
		identity.Revision, identity.UpdatedAt = next, at
		plan.Active = append(plan.Active, identity)
	}
	if _, err := ValidateExternalIdentitySetV2(user, plan.Active); err != nil {
		return empty, err
	}
	return plan, nil
}

func sameExternalIdentityV2(left, right ExternalIdentityV2) bool {
	return left.Version == right.Version && left.Subject == right.Subject && left.UserID == right.UserID &&
		left.State == right.State && left.Revision == right.Revision && left.CreatedAt.Equal(right.CreatedAt) && left.UpdatedAt.Equal(right.UpdatedAt)
}

func validateIdentitySubjectV2(subject ExternalSubject) error {
	if subject.Provider != IdentityProviderYandex && subject.Provider != IdentityProviderTelegram {
		return identityValidationV2("provider", "must be yandex or telegram")
	}
	return subject.Validate()
}

func enabledIdentityProvidersV2(providers []IdentityProvider) (map[IdentityProvider]bool, error) {
	if len(providers) < 1 || len(providers) > 2 {
		return nil, identityValidationV2("enabled_providers", "requires one or two configured providers")
	}
	enabled := make(map[IdentityProvider]bool, 2)
	for _, provider := range providers {
		if (provider != IdentityProviderYandex && provider != IdentityProviderTelegram) || enabled[provider] {
			return nil, identityValidationV2("enabled_providers", "requires distinct closed providers")
		}
		enabled[provider] = true
	}
	return enabled, nil
}

func identityValidationV2(field, reason string) error {
	return ValidationError{Field: "external_identity_v2." + field, Reason: reason}
}
