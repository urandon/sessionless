package webcontract

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	MaxComputeChoicesResponseBytes = 16 * 1024
	MaxComputeChoiceLabelBytes     = 128
	ComputeChoicePrivateDisclosure = "The selected provider processes the entire admitted conversation context and new input as private data. The execution host operator can technically access admitted data. Only the listed input kinds are supported. No alternate resource or provider is selected automatically."
)

var ErrInvalidComputeChoicesResponse = errors.New("compute choices response invalid")

type ComputeChoiceCoverage string
type ComputeChoicePlacement string
type ComputeChoiceState string
type ComputeChoiceReasonCode string
type ComputeChoiceFreshness string
type ComputeChoiceCapacityState string

// ComputeChoicesPageV1 is a closed public projection, not an authorization or
// eligibility constructor. Its producer must join all required current facts
// inside the resource transaction. Shape validation cannot prove those facts,
// label curation, disclosure approval, or a cursor's scope/MAC/authority expiry.
type ComputeChoicesPageV1 struct {
	Version          uint32                  `json:"version"`
	SessionID        domain.SessionID        `json:"session_id"`
	ReadAt           time.Time               `json:"read_at"`
	InputKind        computechoice.InputKind `json:"input_kind"`
	RegistryRevision string                  `json:"registry_revision"`
	Coverage         ComputeChoiceCoverage   `json:"coverage"`
	Choices          []ComputeChoiceV1       `json:"choices"`
	NextCursor       string                  `json:"next_cursor,omitempty"`
}

type ComputeChoiceV1 struct {
	ChoiceID       string                  `json:"choice_id"`
	ChoiceRevision string                  `json:"choice_revision"`
	ModelLabel     string                  `json:"model_label"`
	ProviderLabel  string                  `json:"provider_label"`
	Placement      ComputeChoicePlacement  `json:"placement"`
	State          ComputeChoiceState      `json:"state"`
	ReasonCode     ComputeChoiceReasonCode `json:"reason_code,omitempty"`
	ComputeChoiceWindowV1
	DisclosureRevision string                    `json:"disclosure_revision"`
	Disclosure         ComputeChoiceDisclosureV1 `json:"disclosure"`
	Capacity           ComputeChoiceCapacityV1   `json:"capacity"`
}

// An absent window means no complete required source window was established.
// A real window may accompany unknown semantic evidence; neither timestamp nor
// freshness turns an unknown observation into permission. Producers compute the
// shortest required expiry, never fabricate timestamps from read_at.
type ComputeChoiceWindowV1 struct {
	ObservedAt *time.Time             `json:"observed_at,omitempty"`
	ValidUntil *time.Time             `json:"valid_until,omitempty"`
	Freshness  ComputeChoiceFreshness `json:"freshness,omitempty"`
}

type ComputeChoiceCapacityV1 struct {
	State ComputeChoiceCapacityState `json:"state"`
	ComputeChoiceWindowV1
}

// Fixed reviewed copy, with a finite supported-input manifest. These fields
// describe disclosure only: acknowledgement happens on an exact canonical send.
// No provider error, host/account identity, route, price or budget is accepted.
type ComputeChoiceDisclosureV1 struct {
	DataClass          domain.ProviderDataClassV1 `json:"data_class"`
	InputKinds         []computechoice.InputKind  `json:"input_kinds"`
	ProviderProcessing bool                       `json:"provider_processing"`
	HostOperatorAccess bool                       `json:"host_operator_access"`
	Fallback           string                     `json:"fallback"`
	Text               string                     `json:"text"`
}

func (window ComputeChoiceWindowV1) valid(readAt time.Time) bool {
	if window.ObservedAt == nil || window.ValidUntil == nil || window.Freshness == "" {
		return window.ObservedAt == nil && window.ValidUntil == nil && window.Freshness == ""
	}
	return utcInstant(*window.ObservedAt) && utcInstant(*window.ValidUntil) &&
		!window.ObservedAt.After(readAt) && window.ValidUntil.After(*window.ObservedAt) &&
		(window.Freshness == "current" || window.Freshness == "expired") &&
		(window.Freshness == "current") == readAt.Before(*window.ValidUntil)
}

func (value ComputeChoicesPageV1) Validate() error {
	bad := ErrInvalidComputeChoicesResponse
	if value.Version != 1 || value.SessionID.Validate() != nil || !utcInstant(value.ReadAt) ||
		value.InputKind.Validate() != nil || !computeChoiceRevision(value.RegistryRevision) ||
		(value.Coverage != "page" && value.Coverage != "complete") ||
		(value.Coverage == "page") != (value.NextCursor != "") ||
		value.Choices == nil || len(value.Choices) > computechoice.MaxPagePositions {
		return bad
	}
	if value.NextCursor != "" && (computechoice.ListQuery{Limit: 1, InputKind: value.InputKind, Cursor: value.NextCursor}).Validate() != nil {
		return bad
	}
	seen := make(map[string]bool, len(value.Choices))
	for _, entry := range value.Choices {
		if len(entry.ChoiceID) > computechoice.MaxChoiceIDBytes || domain.ValidateOpaqueID("choice_id", entry.ChoiceID) != nil || seen[entry.ChoiceID] ||
			!computeChoiceRevision(entry.ChoiceRevision) || !computeChoiceRevision(entry.DisclosureRevision) ||
			!computeChoiceLabel(entry.ModelLabel) || !computeChoiceLabel(entry.ProviderLabel) ||
			(entry.Placement != "attached" && entry.Placement != "managed") || !entry.ComputeChoiceWindowV1.valid(value.ReadAt) {
			return bad
		}
		seen[entry.ChoiceID] = true
		switch entry.State {
		case "eligible":
			if entry.ReasonCode != "" || entry.Freshness != "current" || entry.Capacity.State != "available" || entry.Capacity.Freshness != "current" {
				return bad
			}
		case "unavailable", "unknown":
			if !computeChoiceReason(entry.ReasonCode) {
				return bad
			}
		default:
			return bad
		}
		if entry.ReasonCode == "evidence_stale" && entry.Freshness != "expired" {
			return bad
		}
		capacity := entry.Capacity
		if !capacity.ComputeChoiceWindowV1.valid(value.ReadAt) {
			return bad
		}
		// Capacity is one required source. The aggregate window must never
		// advertise freshness beyond its known expiry.
		if entry.ValidUntil != nil && capacity.ValidUntil != nil && entry.ValidUntil.After(*capacity.ValidUntil) {
			return bad
		}
		switch capacity.State {
		case "unknown":
			if capacity.ObservedAt != nil {
				return bad
			}
		case "available":
			if capacity.Freshness != "current" {
				return bad
			}
		case "unavailable":
			if capacity.ObservedAt == nil {
				return bad
			}
		default:
			return bad
		}
		d := entry.Disclosure
		if d.DataClass != domain.ProviderDataPrivateV1 || !d.ProviderProcessing || !d.HostOperatorAccess || d.Fallback != "deny" || d.Text != ComputeChoicePrivateDisclosure || len(d.InputKinds) < 1 || len(d.InputKinds) > 4 {
			return bad
		}
		kinds := make(map[computechoice.InputKind]bool, len(d.InputKinds))
		for _, kind := range d.InputKinds {
			if kind.Validate() != nil || kinds[kind] {
				return bad
			}
			kinds[kind] = true
		}
		if entry.State == "eligible" && !kinds[value.InputKind] {
			return bad
		}
	}
	return nil
}

func (value ComputeChoicesPageV1) MarshalJSON() ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	type wire ComputeChoicesPageV1
	encoded, err := json.Marshal(wire(value))
	if err != nil || len(encoded) > MaxComputeChoicesResponseBytes {
		return nil, ErrInvalidComputeChoicesResponse
	}
	return encoded, nil
}

func computeChoiceRevision(value string) bool {
	return len(value) == 64 && strings.Trim(value, "0123456789abcdef") == ""
}

func computeChoiceLabel(value string) bool {
	if value == "" || len(value) > MaxComputeChoiceLabelBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return false
		}
	}
	return true
}

func computeChoiceReason(value ComputeChoiceReasonCode) bool {
	switch value {
	case "profile_disabled", "evidence_stale", "capability_unavailable", "consent_required", "capacity_unavailable", "policy_unavailable", "unknown":
		return true
	default:
		return false
	}
}
