package ydbstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Explicit third wire language, not a relaxation of either older decoder.
type localFailureCapsuleV3 struct {
	Schema         string                                  `json:"schema"`
	Version        uint32                                  `json:"version"`
	Parent         domain.OwnedAttemptEffectParentV1       `json:"parent"`
	Source         domain.AttemptExecutionSourceV1         `json:"source"`
	ProfileDigest  string                                  `json:"profile_digest"`
	FrontierDigest string                                  `json:"frontier_digest"`
	Stop           string                                  `json:"stop"`
	Cleanup        string                                  `json:"cleanup"`
	CapturedAt     time.Time                               `json:"captured_at"`
	Accounting     ports.OwnedOperationFailureAccountingV3 `json:"accounting"`
	Operations     []localFailureOperationV1               `json:"operations"`
	Events         []localCapsuleEventV1                   `json:"events"`
	Bodies         []domain.BlobRef                        `json:"retained_bodies"`
	Derived        *domain.RunDerivedContextV1             `json:"derived_context,omitempty"`
}

func (c localFailureCapsule) wireV3() localFailureCapsuleV3 {
	return localFailureCapsuleV3{Schema: c.Schema, Version: c.Version, Parent: c.Parent, Source: c.Source, ProfileDigest: c.ProfileDigest, FrontierDigest: c.FrontierDigest, Stop: c.Stop, Cleanup: c.Cleanup, CapturedAt: c.CapturedAt, Accounting: ports.OwnedOperationFailureAccountingV3(c.Accounting), Operations: c.Operations, Events: c.Events, Bodies: c.Bodies, Derived: c.Derived}
}

func (c localFailureCapsuleV3) projection() localFailureCapsule {
	return localFailureCapsule{Schema: c.Schema, Version: c.Version, Parent: c.Parent, Source: c.Source, ProfileDigest: c.ProfileDigest, FrontierDigest: c.FrontierDigest, Stop: c.Stop, Cleanup: c.Cleanup, CapturedAt: c.CapturedAt, Accounting: ports.OwnedOperationFailureAccountingV1(c.Accounting), Operations: c.Operations, Events: c.Events, Bodies: c.Bodies, Derived: c.Derived}
}

func localFailureV3Digest(value any) (string, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	defer clear(body)
	sum := sha256.Sum256(append([]byte("sessionless.mcp-outcome-unknown.v3\x00"), body...))
	return hex.EncodeToString(sum[:]), nil
}

func localFailureUnknownResponseAbsent(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1) bool {
	if o.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || o.ResponseRef != "" || o.ResponseDigest != "" || o.ResponseBytes != 0 {
		return false
	}
	return r.Operation.Kind == domain.AttemptOperationModelV1 && ports.FailureModelUnknownReasonV2(o.Reason) || r.Operation.Kind == domain.AttemptOperationMCPToolV1 && ports.FailureMCPUnknownReasonV3(o.Reason) && o.Usage.State == domain.AttemptOperationUsageUnknownV1
}

func localFailureV3BodyLanguage(c localFailureCapsule) bool {
	if len(c.Operations) < 2 || len(c.Operations) > 12 {
		return false
	}
	for i, op := range c.Operations {
		if i == len(c.Operations)-1 {
			if op.Reservation.Operation.Kind != domain.AttemptOperationMCPToolV1 || c.Operations[i-1].Reservation.Operation.Kind != domain.AttemptOperationModelV1 || !localFailureUnknownResponseAbsent(op.Reservation, op.Observation) || !bytes.Equal(op.Normalized, []byte("null")) {
				return false
			}
		} else if op.Observation.Outcome != domain.AttemptOperationCompletedV1 || op.Observation.Reason != domain.AttemptOperationReasonNoneV1 || len(op.Normalized) == 0 || bytes.Equal(op.Normalized, []byte("null")) {
			return false
		}
	}
	return true
}

// Only the original historical stop owner calls this after keeper/bridge
// join. It revalidates actual retained request bytes against the original
// private grant/call and preceding complete MODEL; no active grant is reread
// at an old time and no absent remote response is decoded or invented.
func (h *LocalFailureV1) validateUnknownMCPRequest(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, request []byte) error {
	if !h.original() || h.version != 3 || len(h.ops) == 0 || !localFailureUnknownResponseAbsent(r, o) {
		return domain.ErrMembershipDenied
	}
	m := h.entry.preparedMCP.Load()
	if m == nil || m.self != m || m.entry != h.entry || m.source != h.source || !reflect.DeepEqual(m.reservation, r) || !reflect.DeepEqual(m.observation.Observation, o) {
		return domain.ErrMembershipDenied
	}
	previous := h.ops[len(h.ops)-1]
	bound, err := mcpadmission.BindConfiguredCallV1(h.entry.owner.configuration, m.grant, previous.Reservation, previous.Observation, previous.Request, previous.Response)
	if err != nil || previous.Reservation.Operation.Kind != domain.AttemptOperationModelV1 || r.Operation.ValidatePredecessor(previous.Reservation, previous.Observation) != nil || bound.ID() != r.Operation.CallID || bound.Name() != m.call.Name() || bound.ArgumentsDigest() != r.Operation.ArgumentsDigest || bound.ModelResponseDigest() != r.Operation.ModelResponseDigest || bound.GrantDigest() != r.Operation.ToolGrantDigest || bound.DescriptorDigest() != m.call.DescriptorDigest() {
		return domain.ErrMembershipDenied
	}
	exact, err := bound.Request()
	if err != nil {
		return domain.ErrMembershipDenied
	}
	body, original := exact.Body(), m.request.Body()
	defer clear(body)
	defer clear(original)
	if !bytes.Equal(body, request) || !bytes.Equal(original, request) {
		return domain.ErrMembershipDenied
	}
	return nil
}

// Request-free historical correlation only. The original producer separately
// checks the actual MODEL request/schema, original grant and exact tools/call
// bytes. A retained call never recreates that private send/first-commit owner.
func localFailureUnknownMCPPair(profile string, before, tail localFailureOperationV1) ([]ports.ExecutionToolEvent, error) {
	r, o := tail.Reservation, tail.Observation
	model, err := configuredinference.DecodeSealedRetainedResponseV2(profile, before.Reservation, before.Observation, before.Normalized)
	if model.Call != nil {
		defer clear(model.Call.Arguments)
	}
	if err != nil || before.Reservation.Operation.Kind != domain.AttemptOperationModelV1 || model.Stop != configuredinference.StopToolV1 || model.Call == nil || r.Operation.Kind != domain.AttemptOperationMCPToolV1 || !localFailureUnknownResponseAbsent(r, o) || !bytes.Equal(tail.Normalized, []byte("null")) || r.Operation.ValidatePredecessor(before.Reservation, before.Observation) != nil || r.Operation.CallID != model.Call.ID || r.Operation.ArgumentsDigest != rawLocalFailureHash(model.Call.Arguments) || r.Operation.ModelResponseDigest != before.Observation.ResponseDigest || domain.ValidateSHA256Digest("failure.grant", r.Operation.ToolGrantDigest) != nil {
		return nil, domain.ErrMembershipDenied
	}
	call, err := json.Marshal(struct {
		ID        string          `json:"call_id"`
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}{model.Call.ID, model.Call.Name, model.Call.Arguments})
	if err != nil {
		return nil, err
	}
	rd, _ := r.Digest()
	od, _ := o.Digest()
	result, err := canonicaloutput.EncodeMCPUnknownToolOutcomeV1(r.OperationDigest, rd, od)
	if err != nil {
		clear(call)
		return nil, err
	}
	return []ports.ExecutionToolEvent{{Kind: domain.SessionEventToolCall, CallID: model.Call.ID, ToolName: model.Call.Name, Payload: call}, {Kind: domain.SessionEventToolResult, CallID: model.Call.ID, ToolName: model.Call.Name, Payload: result}}, nil
}
