package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strconv"
)

const MaxRunDerivedContextBytesV1 = 32 << 10

// RunDerivedContextV1 owns a bounded inventory of planned retained-object pairs,
// including pairs whose physical upload failed. A plan or selected head is DATA,
// never proof of an upload, summary fidelity, current authority or permission.
// No terminal correspondence is invented here; its actual producer is separate.
type RunDerivedContextV1 struct {
	Version   uint32                 `json:"version"`
	Inventory []DerivedContextPlanV1 `json:"inventory"`
	Head      *DerivedContextHeadV1  `json:"head,omitempty"`
}

type DerivedContextPlanV1 struct {
	Version                  uint32                              `json:"version"`
	Scope                    AttemptOperationScopeV1             `json:"scope"`
	Generation               uint64                              `json:"generation"`
	OperationSequence        uint64                              `json:"operation_sequence"`
	ReservationDigest        AttemptOperationReservationDigestV1 `json:"reservation_digest"`
	ObservationDigest        AttemptOperationObservationDigestV1 `json:"observation_digest"`
	NormalizedResponseDigest string                              `json:"normalized_response_digest"`
	SourceViewDigest         string                              `json:"source_view_digest"`
	Summary                  BlobRef                             `json:"summary"`
	Manifest                 BlobRef                             `json:"manifest"`
}

// A selected head resolves its complete scope/generation/manifest/observation
// from exactly one canonical inventory plan, not a duplicated approval DTO.
type DerivedContextHeadV1 struct {
	Version    uint32 `json:"version"`
	PlanDigest string `json:"plan_digest"`
}

func derivedContextError() error {
	return ValidationError{Field: "run.derived_context", Reason: "requires closed bounded original-scope metadata and exact retained-object identities"}
}

func derivedDigestValid(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, b := range []byte(value) {
		if (b < '0' || b > '9') && (b < 'a' || b > 'f') {
			return false
		}
	}
	return true
}

// Bound every variable-length field before JSON encoding, path work or copying.
func derivedScopeValid(scope AttemptOperationScopeV1) bool {
	for _, id := range []string{string(scope.TenantID), string(scope.SessionID), string(scope.OwnerUserID), string(scope.RunID), string(scope.AttemptID), string(scope.LeaseID), scope.PhysicalInvocationClaimID} {
		if len(id) == 0 || len(id) > 160 {
			return false
		}
	}
	for _, digest := range []string{string(scope.EffectReservationDigest), string(scope.InvocationAuthorityDigest), string(scope.HarnessBindingDigest), string(scope.AgentLoopPolicyDigest)} {
		if !derivedDigestValid(digest) {
			return false
		}
	}
	return scope.Validate() == nil
}

// DerivedContextObjectKeyV1 recognizes only the fixed producing Run namespace.
// The hash includes the entire physical scope, not a caller-chosen path suffix.
func DerivedContextObjectKeyV1(scope AttemptOperationScopeV1, generation uint64, kind, digest string) (string, error) {
	if !derivedScopeValid(scope) || generation < 1 || generation > 2 || (kind != "summary" && kind != "manifest") || !derivedDigestValid(digest) {
		return "", derivedContextError()
	}
	raw, err := json.Marshal(scope)
	if err != nil {
		return "", derivedContextError()
	}
	h := sha256.New()
	_, _ = h.Write([]byte("sessionless.derived-context-scope.v1\x00"))
	_, _ = h.Write(raw)
	return SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "derived-context-v1/" + hex.EncodeToString(h.Sum(nil)) + "/" + strconv.FormatUint(generation, 10) + "/" + kind + "/sha256/" + digest + ".json", nil
}

func (p DerivedContextPlanV1) Validate() error {
	if p.Version != 1 || !derivedScopeValid(p.Scope) || p.Generation < 1 || p.Generation > 2 || p.OperationSequence < 1 || p.OperationSequence > 12 {
		return derivedContextError()
	}
	for _, digest := range []string{string(p.ReservationDigest), string(p.ObservationDigest), p.NormalizedResponseDigest, p.SourceViewDigest} {
		if !derivedDigestValid(digest) {
			return derivedContextError()
		}
	}
	for _, object := range []struct {
		kind string
		ref  BlobRef
		max  int64
	}{{"summary", p.Summary, 32 << 10}, {"manifest", p.Manifest, 128 << 10}} {
		if len(object.ref.Key) == 0 || len(object.ref.Key) > 1024 || object.ref.TenantID != p.Scope.TenantID || object.ref.Size <= 0 || object.ref.Size > object.max || !derivedDigestValid(object.ref.SHA256) {
			return derivedContextError()
		}
		key, err := DerivedContextObjectKeyV1(p.Scope, p.Generation, object.kind, object.ref.SHA256)
		if err != nil || object.ref.Key != key || object.ref.Validate() != nil {
			return derivedContextError()
		}
	}
	return nil
}

func (p DerivedContextPlanV1) Digest() (string, error) {
	if p.Validate() != nil {
		return "", derivedContextError()
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return "", derivedContextError()
	}
	h := sha256.New()
	_, _ = h.Write([]byte("sessionless.derived-context-plan.v1\x00"))
	_, _ = h.Write(raw)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (v RunDerivedContextV1) Validate() error {
	if v.Version != 1 || len(v.Inventory) < 1 || len(v.Inventory) > 2 {
		return derivedContextError()
	}
	for i, p := range v.Inventory {
		if p.Validate() != nil {
			return derivedContextError()
		}
		first := v.Inventory[0]
		if p.Scope.TenantID != first.Scope.TenantID || p.Scope.SessionID != first.Scope.SessionID || p.Scope.RunID != first.Scope.RunID || p.Scope.OwnerUserID != first.Scope.OwnerUserID {
			return derivedContextError()
		}
		if i == 0 {
			if p.Generation != 1 {
				return derivedContextError()
			}
			continue
		}
		previous := v.Inventory[i-1]
		if p.Scope.AttemptID == previous.Scope.AttemptID {
			// Binding and policy digests themselves include AttemptID. Require
			// exact scope within an Attempt, not across a replacement Attempt.
			if p.Scope != previous.Scope || p.Generation != previous.Generation+1 || p.OperationSequence <= previous.OperationSequence {
				return derivedContextError()
			}
		} else if p.Generation != 1 {
			return derivedContextError()
		}
		if p.Summary.Key == previous.Summary.Key || p.Manifest.Key == previous.Manifest.Key || p.Summary.Key == previous.Manifest.Key || p.Manifest.Key == previous.Summary.Key {
			return derivedContextError()
		}
	}
	if v.Head != nil {
		if v.Head.Version != 1 || !derivedDigestValid(v.Head.PlanDigest) {
			return derivedContextError()
		}
		matches := 0
		for _, p := range v.Inventory {
			digest, err := p.Digest()
			if err != nil {
				return derivedContextError()
			}
			if digest == v.Head.PlanDigest {
				matches++
			}
		}
		if matches != 1 {
			return derivedContextError()
		}
	}
	return nil
}

func (v RunDerivedContextV1) ValidateForRun(run Run) error {
	if v.Validate() != nil {
		return derivedContextError()
	}
	for _, p := range v.Inventory {
		if p.Scope.TenantID != run.TenantID || p.Scope.SessionID != run.SessionID || p.Scope.RunID != run.ID {
			return derivedContextError()
		}
	}
	return nil
}

// Invalid oversized input is not copied into an unbounded allocation. The
// returned zero shape remains invalid and cannot become absent/valid authority.
func (v RunDerivedContextV1) Clone() RunDerivedContextV1 {
	if len(v.Inventory) > 2 {
		return RunDerivedContextV1{}
	}
	v.Inventory = append([]DerivedContextPlanV1(nil), v.Inventory...)
	if v.Head != nil {
		head := *v.Head
		v.Head = &head
	}
	return v
}

// Complete-metadata equality supports generic writer preservation only. It
// does not authorize a replacement; nil and a present value are distinct.
func EqualRunDerivedContextV1(left, right *RunDerivedContextV1) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.Validate() != nil || right.Validate() != nil {
		return false
	}
	a, ae := json.Marshal(left)
	b, be := json.Marshal(right)
	return ae == nil && be == nil && len(a) <= MaxRunDerivedContextBytesV1 && len(b) <= MaxRunDerivedContextBytesV1 && bytes.Equal(a, b)
}

// First scan the closed JSON shape to reject noncanonical member names,
// duplicates and oversized arrays before decoding/copying any inventory.
// encoding/json's case-insensitive field matching is not this wire contract.
func (v *RunDerivedContextV1) UnmarshalJSON(raw []byte) error {
	if v == nil || len(raw) == 0 || len(raw) > MaxRunDerivedContextBytesV1 {
		return derivedContextError()
	}
	preflight := json.NewDecoder(bytes.NewReader(raw))
	preflight.UseNumber()
	if derivedJSONValue(preflight, "metadata") != nil {
		return derivedContextError()
	}
	if _, err := preflight.Token(); err != io.EOF {
		return derivedContextError()
	}
	type plain RunDerivedContextV1
	var value plain
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&value) != nil {
		return derivedContextError()
	}
	candidate := RunDerivedContextV1(value)
	if candidate.Validate() != nil {
		return derivedContextError()
	}
	*v = candidate.Clone()
	return nil
}

func derivedJSONValue(decoder *json.Decoder, shape string) error {
	token, err := decoder.Token()
	if err != nil {
		return derivedContextError()
	}
	delim, container := token.(json.Delim)
	if shape == "value" && !container || shape == "head" && token == nil {
		return nil
	}
	if shape == "inventory" {
		if !container || delim != '[' {
			return derivedContextError()
		}
		count := 0
		for decoder.More() {
			if count >= 2 {
				return derivedContextError()
			}
			count++
			if derivedJSONValue(decoder, "plan") != nil {
				return derivedContextError()
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return derivedContextError()
		}
	} else {
		if !container || delim != '{' || shape == "value" {
			return derivedContextError()
		}
		seen := make(map[string]struct{}, 12)
		for decoder.More() {
			token, err := decoder.Token()
			key, ok := token.(string)
			if err != nil || !ok {
				return derivedContextError()
			}
			if _, duplicate := seen[key]; duplicate {
				return derivedContextError()
			}
			child := derivedJSONMember(shape, key)
			if child == "" {
				return derivedContextError()
			}
			seen[key] = struct{}{}
			if derivedJSONValue(decoder, child) != nil {
				return derivedContextError()
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return derivedContextError()
		}
	}
	return nil
}

// Only contextual canonical JSON names are accepted. The finite shape graph
// also bounds nesting; a scalar cannot introduce another object or array.
func derivedJSONMember(shape, key string) string {
	switch shape {
	case "metadata":
		switch key {
		case "version":
			return "value"
		case "inventory", "head":
			return key
		}
	case "plan":
		switch key {
		case "version", "generation", "operation_sequence", "reservation_digest", "observation_digest", "normalized_response_digest", "source_view_digest":
			return "value"
		case "scope":
			return "scope"
		case "summary", "manifest":
			return "blob"
		}
	case "scope":
		switch key {
		case "tenant_id", "session_id", "owner_user_id", "run_id", "attempt_id", "lease_id", "fence_token", "physical_invocation_claim_id", "effect_reservation_digest", "invocation_authority_digest", "harness_binding_digest", "agent_loop_policy_digest":
			return "value"
		}
	case "blob":
		switch key {
		case "tenant_id", "key", "size", "sha256":
			return "value"
		}
	case "head":
		switch key {
		case "version", "plan_digest":
			return "value"
		}
	}
	return ""
}
