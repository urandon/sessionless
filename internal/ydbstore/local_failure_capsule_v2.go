package ydbstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Separate exact wire types; the common private projection is not an alternate
// decoder or a fallback. V1 accounting remains strict under its original API.
type localFailureCapsuleV1 localFailureCapsule
type localFailureCapsuleV2 struct {
	Schema         string                                  `json:"schema"`
	Version        uint32                                  `json:"version"`
	Parent         domain.OwnedAttemptEffectParentV1       `json:"parent"`
	Source         domain.AttemptExecutionSourceV1         `json:"source"`
	ProfileDigest  string                                  `json:"profile_digest"`
	FrontierDigest string                                  `json:"frontier_digest"`
	Stop           string                                  `json:"stop"`
	Cleanup        string                                  `json:"cleanup"`
	CapturedAt     time.Time                               `json:"captured_at"`
	Accounting     ports.OwnedOperationFailureAccountingV2 `json:"accounting"`
	Operations     []localFailureOperationV1               `json:"operations"`
	Events         []localCapsuleEventV1                   `json:"events"`
	Bodies         []domain.BlobRef                        `json:"retained_bodies"`
	Derived        *domain.RunDerivedContextV1             `json:"derived_context,omitempty"`
}

func (c localFailureCapsule) wireV2() localFailureCapsuleV2 {
	return localFailureCapsuleV2{Schema: c.Schema, Version: c.Version, Parent: c.Parent, Source: c.Source, ProfileDigest: c.ProfileDigest, FrontierDigest: c.FrontierDigest, Stop: c.Stop, Cleanup: c.Cleanup, CapturedAt: c.CapturedAt, Accounting: ports.OwnedOperationFailureAccountingV2(c.Accounting), Operations: c.Operations, Events: c.Events, Bodies: c.Bodies, Derived: c.Derived}
}
func (c localFailureCapsuleV2) projection() localFailureCapsule {
	return localFailureCapsule{Schema: c.Schema, Version: c.Version, Parent: c.Parent, Source: c.Source, ProfileDigest: c.ProfileDigest, FrontierDigest: c.FrontierDigest, Stop: c.Stop, Cleanup: c.Cleanup, CapturedAt: c.CapturedAt, Accounting: ports.OwnedOperationFailureAccountingV1(c.Accounting), Operations: c.Operations, Events: c.Events, Bodies: c.Bodies, Derived: c.Derived}
}

func localFailureFormatValid(c localFailureCapsule) bool {
	return c.Version == c.Accounting.Version && domain.TerminalFailureSchemaStopValid(c.Schema, c.Stop) && (c.Version == 1 && c.Schema == "sessionless.terminal-failure-custody.v1" || c.Version == 2 && c.Schema == "sessionless.terminal-failure-custody.v2")
}

// V2's explicit discriminant is checked before any consumer interprets rows.
// These are only structural checks; the original producer/retained verifier
// still independently authenticates every row, complete body and parent.
func localFailureV2BodyLanguage(c localFailureCapsule) bool {
	if len(c.Operations) < 3 || len(c.Operations) > 12 {
		return false
	}
	for i, op := range c.Operations {
		o := op.Observation
		if i == len(c.Operations)-1 {
			if op.Reservation.Operation.Kind != domain.AttemptOperationModelV1 || o.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || !ports.FailureModelUnknownReasonV2(o.Reason) || o.ResponseRef != "" || o.ResponseDigest != "" || o.ResponseBytes != 0 || string(op.Normalized) != "null" {
				return false
			}
		} else if o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || len(op.Normalized) == 0 || string(op.Normalized) == "null" {
			return false
		}
	}
	return true
}
func validateLocalFailureAccounting(v ports.OwnedOperationFailureAccountingV1, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) error {
	switch v.Version {
	case 1:
		return v.ValidateFor(parent, source)
	case 2:
		return ports.OwnedOperationFailureAccountingV2(v).ValidateFor(parent, source)
	default:
		return domain.ErrMembershipDenied
	}
}
func localFailureAccountingDigest(v ports.OwnedOperationFailureAccountingV1) (string, error) {
	switch v.Version {
	case 1:
		return v.Digest()
	case 2:
		return ports.OwnedOperationFailureAccountingV2(v).Digest()
	default:
		return "", domain.ErrMembershipDenied
	}
}
func localFailureVersionDigest(version uint32, v any) (string, error) {
	if version == 1 {
		return localFailureDigest(v)
	}
	if version != 2 {
		return "", domain.ErrMembershipDenied
	}
	body, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	defer clear(body)
	sum := sha256.Sum256(append([]byte("sessionless.model-outcome-unknown.v2\x00"), body...))
	return hex.EncodeToString(sum[:]), nil
}
