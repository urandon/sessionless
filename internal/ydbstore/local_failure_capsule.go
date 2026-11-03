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
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

type localFailureOperationV1 struct {
	Reservation domain.AttemptOperationReservationV1 `json:"reservation"`
	Observation domain.AttemptOperationObservationV1 `json:"observation"`
	Normalized  json.RawMessage                      `json:"normalized"`
}
type localFailureCapsule struct {
	Schema         string                                  `json:"schema"`
	Version        uint32                                  `json:"version"`
	Parent         domain.OwnedAttemptEffectParentV1       `json:"parent"`
	Source         domain.AttemptExecutionSourceV1         `json:"source"`
	ProfileDigest  string                                  `json:"profile_digest"`
	FrontierDigest string                                  `json:"frontier_digest"`
	Stop           string                                  `json:"stop"`
	Cleanup        string                                  `json:"cleanup"`
	CapturedAt     time.Time                               `json:"captured_at"`
	Accounting     ports.OwnedOperationFailureAccountingV1 `json:"accounting"`
	Operations     []localFailureOperationV1               `json:"operations"`
	Events         []localCapsuleEventV1                   `json:"events"`
	Bodies         []domain.BlobRef                        `json:"retained_bodies"`
	Derived        *domain.RunDerivedContextV1             `json:"derived_context,omitempty"`
}

func localFailureDigest(value any) (string, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("sessionless.known-mcp-failure.v1\x00"), body...))
	return hex.EncodeToString(sum[:]), nil
}

func localFailureFrontierDigest(parent domain.OwnedAttemptEffectParentV1, accounting ports.OwnedOperationFailureAccountingV1, ops []localFailureOperationV1) (string, error) {
	rows := make([]localFailureOperationV1, len(ops))
	for i, op := range ops {
		rows[i] = localFailureOperationV1{Reservation: op.Reservation, Observation: op.Observation}
	}
	pd, err := parent.Digest()
	if err != nil {
		return "", err
	}
	ad, err := localFailureAccountingDigest(accounting)
	if err != nil {
		return "", err
	}
	return localFailureVersionDigest(accounting.Version, struct {
		Parent     string
		Accounting string
		Operations []localFailureOperationV1
	}{pd, ad, rows})
}

func localFailurePlan(run domain.Run, limits domain.ProductLimits, c localFailureCapsule) (*canonicaloutput.FailurePlanV1, error) {
	deny := func() (*canonicaloutput.FailurePlanV1, error) { return nil, domain.ErrMembershipDenied }
	if !localFailureFormatValid(c) || c.Version == 3 && !localFailureV3BodyLanguage(c) || c.Cleanup != "joined_retained" || c.Parent.Validate() != nil || c.Source.ValidateForExecution(c.Parent.Authority.ExecutionPlacementV2, c.Parent.Authority.HarnessBinding, c.Parent.Authority.SubstrateBinding, c.Parent.Authority.AdmissionCostCeiling) != nil || len(c.Operations) < 2 || len(c.Operations) > 12 || c.ProfileDigest != c.Parent.Authority.HarnessBinding.Backend.BackendProfileDigest || validateLocalFailureAccounting(c.Accounting, c.Parent, c.Source) != nil || len(c.Accounting.Operations) != len(c.Operations) {
		return deny()
	}
	if c.Parent.Scope.TenantID != run.TenantID || c.Parent.Scope.SessionID != run.SessionID || c.Parent.Scope.RunID != run.ID || !domain.EqualRunDerivedContextV1(c.Derived, run.DerivedContext) {
		return deny()
	}
	fd, err := localFailureFrontierDigest(c.Parent, c.Accounting, c.Operations)
	if err != nil || fd != c.FrontierDigest {
		return deny()
	}
	if operationbody.ValidateRetainedBodiesV1(c.Accounting.Scope, c.Bodies) != nil {
		return deny()
	}
	// The retained failure has no unreserved tail or uncertain response PUT.
	// Every retained ordinary key must have its exact SQL operation
	// owner, enabling the existing deletion graph to inventory it without blob
	// discovery. Any extra attempted key keeps this invocation reconcile-only.
	ownedBodies, err := localFailureOperationBodies(c.Operations)
	if err != nil || !sameFailureBodies(ownedBodies, c.Bodies) {
		return deny()
	}
	var total uint64
	for i, item := range c.Operations {
		r, o := item.Reservation, item.Observation
		unknown := (c.Version == 2 || c.Version == 3) && i == len(c.Operations)-1
		bodyValid := localFailureBytes(item.Normalized, o.ResponseBytes, o.ResponseDigest)
		if unknown {
			bodyValid = localFailureUnknownResponseAbsent(r, o) && bytes.Equal(item.Normalized, []byte("null")) && (c.Version == 2 && r.Operation.Kind == domain.AttemptOperationModelV1 || c.Version == 3 && r.Operation.Kind == domain.AttemptOperationMCPToolV1)
		}
		if r.Operation.Scope != c.Accounting.Scope || r.Operation.Sequence != uint64(i+1) || r.ValidateForParent(c.Parent.Authority, c.Parent.Reservation) != nil || o.ValidateForReservation(r) != nil || !bodyValid || len(item.Normalized) > 64<<10 || uint64(len(item.Normalized)) > (384<<10)-total || i > 0 && r.Operation.ValidatePredecessor(c.Operations[i-1].Reservation, c.Operations[i-1].Observation) != nil {
			return deny()
		}
		if c.CapturedAt.IsZero() || c.CapturedAt != c.CapturedAt.UTC().Truncate(time.Microsecond) || o.ObservedAt.After(c.CapturedAt) {
			return deny()
		}
		total += uint64(len(item.Normalized))
		row, err := ports.BuildOwnedOperationFailureRowV1(r, o, c.Parent, c.Source)
		if c.Version == 2 {
			row, err = ports.BuildOwnedOperationFailureRowV2(r, o, c.Parent, c.Source)
		} else if c.Version == 3 {
			row, err = ports.BuildOwnedOperationFailureRowV3(r, o, c.Parent, c.Source)
		}
		if err != nil || !reflect.DeepEqual(row, c.Accounting.Operations[i]) {
			return deny()
		}
		if r.Operation.Kind != domain.AttemptOperationMCPToolV1 && !unknown {
			decoded, err := configuredinference.DecodeSealedRetainedResponseV2(c.ProfileDigest, r, o, item.Normalized)
			validCall := decoded.Call != nil && r.Operation.Kind == domain.AttemptOperationModelV1 && decoded.Stop == configuredinference.StopToolV1 && i+1 < len(c.Operations) && c.Operations[i+1].Reservation.Operation.Kind == domain.AttemptOperationMCPToolV1
			validFinal := decoded.Call == nil && decoded.Stop == configuredinference.StopFinalV1
			if decoded.Call != nil {
				clear(decoded.Call.Arguments)
			}
			if err != nil || (!validCall && !validFinal) {
				return deny()
			}
		}
	}
	var events []ports.ExecutionToolEvent
	defer func() {
		for _, event := range events {
			clear(event.Payload)
		}
	}()
	for i, item := range c.Operations {
		if item.Reservation.Operation.Kind != domain.AttemptOperationMCPToolV1 {
			continue
		}
		if i == 0 {
			return deny()
		}
		previous := c.Operations[i-1]
		if c.Version == 3 && i == len(c.Operations)-1 {
			pair, err := localFailureUnknownMCPPair(c.ProfileDigest, previous, item)
			if err != nil {
				return deny()
			}
			events = append(events, pair...)
			continue
		}
		tool, te := mcpadmission.DecodeSealedToolResultDataV1(item.Reservation, item.Observation, item.Normalized)
		model, me := configuredinference.DecodeSealedRetainedResponseV2(c.ProfileDigest, previous.Reservation, previous.Observation, previous.Normalized)
		valid := te == nil && me == nil && tool.IsError() == (i == len(c.Operations)-1) && model.Call != nil && model.Stop == configuredinference.StopToolV1 && previous.Reservation.Operation.Kind == domain.AttemptOperationModelV1
		if model.Call != nil {
			valid = valid && tool.CallID() == model.Call.ID && tool.Name() == model.Call.Name && tool.ArgumentsDigest() == rawLocalFailureHash(model.Call.Arguments) && tool.ModelResponseDigest() == previous.Observation.ResponseDigest
		}
		if !valid {
			if model.Call != nil {
				clear(model.Call.Arguments)
			}
			return deny()
		}
		if !tool.IsError() {
			pair, e := localMCPCanonicalToolEvents(model.Call.ID, model.Call.Name, model.Call.Arguments, tool)
			clear(model.Call.Arguments)
			if e != nil {
				return deny()
			}
			events = append(events, pair...)
			continue
		}
		call, e := json.Marshal(struct {
			ID        string          `json:"call_id"`
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}{model.Call.ID, model.Call.Name, model.Call.Arguments})
		clear(model.Call.Arguments)
		if e != nil {
			return deny()
		}
		rd, _ := item.Reservation.Digest()
		od, _ := item.Observation.Digest()
		result, e := localFailureToolResult(item.Reservation.OperationDigest, rd, od, tool.Content())
		if e != nil {
			clear(call)
			return deny()
		}
		events = append(events, ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: tool.CallID(), ToolName: tool.Name(), Payload: call}, ports.ExecutionToolEvent{Kind: domain.SessionEventToolResult, CallID: tool.CallID(), ToolName: tool.Name(), Payload: result})
	}
	if c.Version == 2 {
		return canonicaloutput.EncodeModelAfterMCPFailureV2(run.ID, limits, events)
	}
	if c.Version == 3 {
		return canonicaloutput.EncodeMCPOutcomeUnknownFailureV3(run.ID, limits, events)
	}
	return canonicaloutput.EncodeKnownMCPServiceFailureV1(run.ID, limits, events)
}

func localFailureOperationBodies(ops []localFailureOperationV1) ([]domain.BlobRef, error) {
	var refs []domain.BlobRef
	for _, item := range ops {
		r, o := item.Reservation, item.Observation
		request, err := operationbody.RetainedOperationBodyRefV1(r.Operation.Scope, r.Operation.RequestRef, r.Operation.RequestBytes, r.Operation.RequestDigest)
		if err != nil {
			return nil, domain.ErrMembershipDenied
		}
		bodyRefs := []domain.BlobRef{request}
		if o.Outcome == domain.AttemptOperationAcceptedOutcomeUnknownV1 {
			if !localFailureUnknownResponseAbsent(r, o) {
				return nil, domain.ErrMembershipDenied
			}
		} else {
			response, err := operationbody.RetainedOperationBodyRefV1(r.Operation.Scope, o.ResponseRef, o.ResponseBytes, o.ResponseDigest)
			if err != nil {
				return nil, domain.ErrMembershipDenied
			}
			bodyRefs = append(bodyRefs, response)
		}
		for _, ref := range bodyRefs {
			exists := false
			for _, old := range refs {
				if old == ref {
					exists = true
				}
			}
			if !exists {
				refs = append(refs, ref)
			}
		}
	}
	return refs, nil
}
func sameFailureBodies(a, b []domain.BlobRef) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]domain.BlobRef{}
	for _, ref := range a {
		seen[ref.Key] = ref
	}
	if len(seen) != len(a) {
		return false
	}
	for _, ref := range b {
		old, ok := seen[ref.Key]
		if !ok || old != ref {
			return false
		}
		delete(seen, ref.Key)
	}
	return len(seen) == 0
}

func localFailureToolResult(operation domain.AttemptOperationDigestV1, reservation domain.AttemptOperationReservationDigestV1, observation domain.AttemptOperationObservationDigestV1, content []string) ([]byte, error) {
	return json.Marshal(struct {
		Schema      string                                     `json:"schema"`
		Outcome     string                                     `json:"outcome"`
		Operation   domain.AttemptOperationDigestV1            `json:"operation_digest"`
		Reservation domain.AttemptOperationReservationDigestV1 `json:"reservation_digest"`
		Observation domain.AttemptOperationObservationDigestV1 `json:"observation_digest"`
		Error       []string                                   `json:"domain_error"`
	}{"sessionless.tool-outcome.v1", "service_error", operation, reservation, observation, content})
}
func rawLocalFailureHash(body []byte) string {
	s := sha256.Sum256(body)
	return hex.EncodeToString(s[:])
}

func encodeLocalFailureCapsule(c localFailureCapsule, limit uint64) ([]byte, error) {
	if limit == 0 || limit > domain.MaxTerminalCapsuleBytesV1 {
		return nil, domain.ErrMembershipDenied
	}
	var body []byte
	var err error
	switch c.Version {
	case 1:
		body, err = json.Marshal(localFailureCapsuleV1(c))
	case 2:
		body, err = json.Marshal(c.wireV2())
	case 3:
		body, err = json.Marshal(c.wireV3())
	default:
		return nil, domain.ErrMembershipDenied
	}
	if err != nil || uint64(len(body)) > limit {
		clear(body)
		return nil, domain.ErrMembershipDenied
	}
	return body, nil
}
func decodeLocalFailureCapsule(body []byte) (localFailureCapsule, error) {
	var c localFailureCapsule
	if _, err := strictjson.Parse(body, operationbody.LocalTerminalCapsuleBytesV1, 64, 16384); err != nil {
		return c, domain.ErrMembershipDenied
	}
	var tag struct {
		Schema  string `json:"schema"`
		Version uint32 `json:"version"`
	}
	if json.Unmarshal(body, &tag) != nil {
		return c, domain.ErrMembershipDenied
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	switch {
	case tag.Schema == "sessionless.terminal-failure-custody.v1" && tag.Version == 1:
		var wire localFailureCapsuleV1
		if d.Decode(&wire) != nil {
			return c, domain.ErrMembershipDenied
		}
		c = localFailureCapsule(wire)
	case tag.Schema == "sessionless.terminal-failure-custody.v2" && tag.Version == 2:
		var wire localFailureCapsuleV2
		if d.Decode(&wire) != nil {
			return c, domain.ErrMembershipDenied
		}
		c = wire.projection()
	case tag.Schema == "sessionless.terminal-failure-custody.v3" && tag.Version == 3:
		var wire localFailureCapsuleV3
		if d.Decode(&wire) != nil {
			return c, domain.ErrMembershipDenied
		}
		c = wire.projection()
	default:
		clearLocalFailureNormalized(c)
		return localFailureCapsule{}, domain.ErrMembershipDenied
	}
	raw, err := encodeLocalFailureCapsule(c, operationbody.LocalTerminalCapsuleBytesV1)
	defer clear(raw)
	if err != nil || !bytes.Equal(raw, body) || !localFailureFormatValid(c) || c.Version == 2 && !localFailureV2BodyLanguage(c) || c.Version == 3 && !localFailureV3BodyLanguage(c) {
		clearLocalFailureNormalized(c)
		return localFailureCapsule{}, domain.ErrMembershipDenied
	}
	return c, nil
}

func clearLocalFailureNormalized(c localFailureCapsule) {
	for _, op := range c.Operations {
		clear(op.Normalized)
	}
}

func failureMetadata(plan domain.TerminalCustodyPlanV1, c localFailureCapsule, retired string) (domain.RunTerminalFailureV1, error) {
	ad, err := localFailureAccountingDigest(c.Accounting)
	if err != nil {
		return domain.RunTerminalFailureV1{}, err
	}
	bd, err := operationbody.RetainedBodiesDigestV1(c.Bodies)
	if err != nil {
		return domain.RunTerminalFailureV1{}, err
	}
	pd, err := c.Parent.Digest()
	if err != nil {
		return domain.RunTerminalFailureV1{}, err
	}
	ed, err := localFailureVersionDigest(c.Version, c.Events)
	if err != nil {
		return domain.RunTerminalFailureV1{}, err
	}
	return domain.RunTerminalFailureV1{Version: 1, Scope: c.Accounting.Scope, ParentDigest: pd, Plan: plan, CapsuleSchema: c.Schema, AccountingDigest: ad, EventsDigest: ed, RetainedBodiesDigest: bd, RetiredAllocationDigest: retired, CapturedAt: c.CapturedAt, Stop: c.Stop, Cleanup: c.Cleanup}, nil
}
