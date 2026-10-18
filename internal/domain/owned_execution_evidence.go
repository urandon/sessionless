package domain

import (
	"bytes"
	"encoding/json"
	"io"
	"time"
	"unicode/utf8"
)

const MaxExecutionEvidenceBytesV2 = 32 << 10

type ExecutionEvidenceKindV2 string

const (
	ExecutionEvidenceManagedV1 ExecutionEvidenceKindV2 = "managed_v1"
	ExecutionEvidenceLocalV1   ExecutionEvidenceKindV2 = "local_v1"
)

// A closed successor, not a reinterpretation of V1. Validation is structural
// DATA correlation. Original joined owner receipts must precede first commit.
type ExecutionEvidenceV2 struct {
	Version   uint32                        `json:"version"`
	Kind      ExecutionEvidenceKindV2       `json:"kind"`
	ManagedV1 *SubstrateExecutionEvidenceV1 `json:"managed_v1,omitempty"`
	LocalV1   *LocalExecutionEvidenceV1     `json:"local_v1,omitempty"`
}

type LocalExecutionEvidenceV1 struct {
	Version                     uint32                  `json:"version"`
	Scope                       AttemptOperationScopeV1 `json:"scope"`
	ParentDigest                string                  `json:"parent_digest"`
	SourceDigest                string                  `json:"source_digest"`
	ProfileDigest               string                  `json:"profile_digest"`
	OriginalAllocationDigest    string                  `json:"original_allocation_digest"`
	RetiredAllocationDigest     string                  `json:"retired_allocation_digest"`
	FrontierDigest              string                  `json:"frontier_digest"`
	Capsule                     BlobRef                 `json:"capsule"`
	ResidualInventoryDigest     string                  `json:"residual_inventory_digest"`
	ResidualReceiptDigest       string                  `json:"residual_receipt_digest"`
	AccountingDispositionDigest string                  `json:"accounting_disposition_digest"`
	StoppedAt                   time.Time               `json:"stopped_at"`
	RetiredAt                   time.Time               `json:"retired_at"`
	ErasedAt                    time.Time               `json:"erased_at"`
	EvidenceDigest              string                  `json:"evidence_digest"`
}

func ownedExecutionEvidenceError() error {
	return ValidationError{Field: "execution_evidence.v2", Reason: "requires a closed bounded exact original execution evidence arm"}
}

func (v LocalExecutionEvidenceV1) validateContent() error {
	if v.Version != 1 || (TerminalCustodyPlanV1{Scope: v.Scope, Capsule: v.Capsule, FrontierDigest: v.FrontierDigest}).Validate() != nil {
		return ownedExecutionEvidenceError()
	}
	for _, d := range []string{v.ParentDigest, v.SourceDigest, v.ProfileDigest, v.OriginalAllocationDigest, v.RetiredAllocationDigest, v.ResidualInventoryDigest, v.ResidualReceiptDigest, v.AccountingDispositionDigest} {
		if !derivedDigestValid(d) {
			return ownedExecutionEvidenceError()
		}
	}
	if v.OriginalAllocationDigest == v.RetiredAllocationDigest {
		return ownedExecutionEvidenceError()
	}
	for _, t := range []time.Time{v.StoppedAt, v.RetiredAt, v.ErasedAt} {
		if t.IsZero() || t.Year() < 1 || t.Year() > 9999 || t.Location() != time.UTC || t.Nanosecond()%1000 != 0 {
			return ownedExecutionEvidenceError()
		}
	}
	if v.RetiredAt.Before(v.StoppedAt) || v.ErasedAt.Before(v.RetiredAt) {
		return ownedExecutionEvidenceError()
	}
	return nil
}

// Digest is unkeyed DATA integrity, NOT a signer or evidence of physical cleanup.
func (v LocalExecutionEvidenceV1) Digest() (string, error) {
	if v.validateContent() != nil {
		return "", ownedExecutionEvidenceError()
	}
	// The evidence_digest member itself is excluded, keeping the DAG acyclic.
	body, err := json.Marshal(struct {
		Version                     uint32                  `json:"version"`
		Scope                       AttemptOperationScopeV1 `json:"scope"`
		ParentDigest                string                  `json:"parent_digest"`
		SourceDigest                string                  `json:"source_digest"`
		ProfileDigest               string                  `json:"profile_digest"`
		OriginalAllocationDigest    string                  `json:"original_allocation_digest"`
		RetiredAllocationDigest     string                  `json:"retired_allocation_digest"`
		FrontierDigest              string                  `json:"frontier_digest"`
		Capsule                     BlobRef                 `json:"capsule"`
		ResidualInventoryDigest     string                  `json:"residual_inventory_digest"`
		ResidualReceiptDigest       string                  `json:"residual_receipt_digest"`
		AccountingDispositionDigest string                  `json:"accounting_disposition_digest"`
		StoppedAt                   time.Time               `json:"stopped_at"`
		RetiredAt                   time.Time               `json:"retired_at"`
		ErasedAt                    time.Time               `json:"erased_at"`
	}{v.Version, v.Scope, v.ParentDigest, v.SourceDigest, v.ProfileDigest, v.OriginalAllocationDigest, v.RetiredAllocationDigest, v.FrontierDigest, v.Capsule, v.ResidualInventoryDigest, v.ResidualReceiptDigest, v.AccountingDispositionDigest, v.StoppedAt, v.RetiredAt, v.ErasedAt})
	if err != nil {
		return "", ownedExecutionEvidenceError()
	}
	return terminalHash("sessionless.local-execution-evidence.v1\x00", body), nil
}

func (v LocalExecutionEvidenceV1) Validate() error {
	if !derivedDigestValid(v.EvidenceDigest) {
		return ownedExecutionEvidenceError()
	}
	d, err := v.Digest()
	if err != nil || d != v.EvidenceDigest {
		return ownedExecutionEvidenceError()
	}
	return nil
}

func (v LocalExecutionEvidenceV1) Clone() LocalExecutionEvidenceV1 { return v }

func (v ExecutionEvidenceV2) validateShape() error {
	if v.Version != 2 {
		return ownedExecutionEvidenceError()
	}
	switch v.Kind {
	case ExecutionEvidenceManagedV1:
		if v.ManagedV1 == nil || v.LocalV1 != nil || !managedEvidenceDataValid(*v.ManagedV1) {
			return ownedExecutionEvidenceError()
		}
	case ExecutionEvidenceLocalV1:
		if v.LocalV1 == nil || v.ManagedV1 != nil || v.LocalV1.Validate() != nil {
			return ownedExecutionEvidenceError()
		}
	default:
		return ownedExecutionEvidenceError()
	}
	return nil
}

// Reuse V1's intrinsic validators and digest unchanged. Authority-dependent
// checks still run through its original ValidateForPersistedAuthority below.
func managedEvidenceDataValid(v SubstrateExecutionEvidenceV1) bool {
	if v.Version != SubstrateExecutionEvidenceVersionV1 || len(v.ResourceObservations) != 7 || len(v.SecondaryFailureCodes) > 16 || len(v.PhysicalInvocationClaimID) > 160 || ValidateOpaqueID("execution_evidence.claim", v.PhysicalInvocationClaimID) != nil {
		return false
	}
	for _, d := range []string{string(v.InvocationAuthorityDigest), string(v.SubstrateBindingDigest), string(v.AdmissionCostCeilingDigest), string(v.PreparedInvocationDigest), string(v.EffectReservationDigest), string(v.PreparedAllocationDigest), string(v.EvidenceDigest)} {
		if !derivedDigestValid(d) {
			return false
		}
	}
	if validateSubstrateEnums(v) != nil || validateSubstrateCancellation(v.Cancellation) != nil || validateSubstrateResources(v.ResourceObservations) != nil || validateSubstrateProviderCompletion(v) != nil || validateSubstrateFailures(v) != nil {
		return false
	}
	if p := v.ProviderEvidence; p != nil {
		if p.validateIntrinsic() != nil {
			return false
		}
		d, err := p.digest()
		if err != nil || p.EvidenceDigest != d {
			return false
		}
	}
	d, err := v.digest()
	return err == nil && v.EvidenceDigest == d
}

func (v ExecutionEvidenceV2) ValidateForPersistedAuthority(a ServerlessInvocationAuthorityV1, r AttemptEffectReservationV1) error {
	if v.validateShape() != nil {
		return ownedExecutionEvidenceError()
	}
	if v.ManagedV1 != nil {
		return v.ManagedV1.ValidateForPersistedAuthority(a, r)
	}
	e := v.LocalV1
	if a.Validate() != nil || r.ValidateForAuthority(a) != nil || a.SubstrateBinding.Kind != SubstrateLocalDevelopmentV1 || a.HarnessBinding.Resource.Kind != ProviderResourceLocalEndpointV1 || a.HarnessBinding.Backend.ProviderContractKind != ProviderContractConfiguredEndpointV1 || e.Scope.ValidateForParent(a, r) != nil || e.ProfileDigest != a.HarnessBinding.Backend.BackendProfileDigest || e.StoppedAt.Before(r.ReservedAt) {
		return ownedExecutionEvidenceError()
	}
	return nil
}

func (v ExecutionEvidenceV2) Clone() ExecutionEvidenceV2 {
	if v.validateShape() != nil {
		return ExecutionEvidenceV2{}
	}
	if v.ManagedV1 != nil {
		m := v.ManagedV1.Clone()
		v.ManagedV1 = &m
	}
	if v.LocalV1 != nil {
		l := v.LocalV1.Clone()
		v.LocalV1 = &l
	}
	return v
}

func (v *LocalExecutionEvidenceV1) UnmarshalJSON(body []byte) error {
	if v == nil || len(body) == 0 || len(body) > MaxExecutionEvidenceBytesV2 || terminalJSONPreflight(body, "local") != nil {
		return ownedExecutionEvidenceError()
	}
	type plain LocalExecutionEvidenceV1
	var value plain
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&value) != nil || LocalExecutionEvidenceV1(value).Validate() != nil {
		return ownedExecutionEvidenceError()
	}
	*v = LocalExecutionEvidenceV1(value)
	return nil
}

func (v *ExecutionEvidenceV2) UnmarshalJSON(body []byte) error {
	if v == nil || len(body) == 0 || len(body) > MaxExecutionEvidenceBytesV2 || terminalJSONPreflight(body, "evidence") != nil {
		return ownedExecutionEvidenceError()
	}
	type plain ExecutionEvidenceV2
	var value plain
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&value) != nil || ExecutionEvidenceV2(value).validateShape() != nil {
		return ownedExecutionEvidenceError()
	}
	*v = ExecutionEvidenceV2(value).Clone()
	return nil
}

// Finite contextual shapes reject unknown/case-alias/duplicate decoded names
// and bound nesting/array counts BEFORE typed decoding or deep copying.
func terminalJSONPreflight(body []byte, shape string) error {
	if !terminalJSONUnicodeValid(body) {
		return ownedExecutionEvidenceError()
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	if terminalJSONValue(d, shape) != nil {
		return ownedExecutionEvidenceError()
	}
	if _, err := d.Token(); err != io.EOF {
		return ownedExecutionEvidenceError()
	}
	return nil
}

// encoding/json replaces invalid UTF-8 and unpaired UTF-16 surrogates with
// U+FFFD. Reject that lossy lexical normalization BEFORE Token can collapse
// distinct sealed evidence. This reuses only the escaped-surrogate mechanics
// of configuredinference/strict_json.go; contextual parsing remains below.
func terminalJSONUnicodeValid(body []byte) bool {
	if len(body) == 0 || len(body) > MaxExecutionEvidenceBytesV2 || !utf8.Valid(body) {
		return false
	}
	inString := false
	for i := 0; i < len(body); i++ {
		if !inString {
			if body[i] == '"' {
				inString = true
			}
			continue
		}
		switch body[i] {
		case '"':
			inString = false
		case '\\':
			i++
			if i >= len(body) {
				return false
			}
			if body[i] != 'u' {
				continue
			} // The contextual decoder checks other escapes.
			if len(body)-i < 5 {
				return false
			}
			n, ok := terminalJSONHex4(body[i+1 : i+5])
			if !ok {
				return false
			}
			i += 4
			if n >= 0xd800 && n <= 0xdbff {
				if len(body)-i < 7 || body[i+1] != '\\' || body[i+2] != 'u' {
					return false
				}
				low, ok := terminalJSONHex4(body[i+3 : i+7])
				if !ok || low < 0xdc00 || low > 0xdfff {
					return false
				}
				i += 6
			} else if n >= 0xdc00 && n <= 0xdfff {
				return false
			}
		}
	}
	return !inString
}

func terminalJSONHex4(body []byte) (uint16, bool) {
	if len(body) != 4 {
		return 0, false
	}
	var n uint16
	for _, b := range body {
		var d byte
		switch {
		case b >= '0' && b <= '9':
			d = b - '0'
		case b >= 'a' && b <= 'f':
			d = b - 'a' + 10
		case b >= 'A' && b <= 'F':
			d = b - 'A' + 10
		default:
			return 0, false
		}
		n = n*16 + uint16(d)
	}
	return n, true
}

func terminalJSONValue(d *json.Decoder, shape string) error {
	if shape == "scope" || shape == "blob" || shape == "derived" {
		if shape == "derived" {
			shape = "metadata"
		}
		return derivedJSONValue(d, shape)
	}
	t, err := d.Token()
	if err != nil {
		return ownedExecutionEvidenceError()
	}
	delim, container := t.(json.Delim)
	if shape == "scalar" {
		if container {
			return ownedExecutionEvidenceError()
		}
		return nil
	}
	if shape == "resources" || shape == "failures" {
		if t == nil && shape == "failures" {
			return nil
		}
		if !container || delim != '[' {
			return ownedExecutionEvidenceError()
		}
		max, child := 7, "resource"
		if shape == "failures" {
			max, child = 16, "scalar"
		}
		n := 0
		for d.More() {
			if n >= max {
				return ownedExecutionEvidenceError()
			}
			n++
			if terminalJSONValue(d, child) != nil {
				return ownedExecutionEvidenceError()
			}
		}
		end, e := d.Token()
		if e != nil || end != json.Delim(']') {
			return ownedExecutionEvidenceError()
		}
		return nil
	}
	if shape == "provider" && t == nil {
		return nil
	}
	if !container || delim != '{' {
		return ownedExecutionEvidenceError()
	}
	seen := make(map[string]bool, 20)
	for d.More() {
		t, e := d.Token()
		key, ok := t.(string)
		if e != nil || !ok || seen[key] {
			return ownedExecutionEvidenceError()
		}
		child := terminalJSONMember(shape, key)
		if child == "" {
			return ownedExecutionEvidenceError()
		}
		seen[key] = true
		if terminalJSONValue(d, child) != nil {
			return ownedExecutionEvidenceError()
		}
	}
	end, e := d.Token()
	if e != nil || end != json.Delim('}') {
		return ownedExecutionEvidenceError()
	}
	return nil
}

func terminalJSONMember(shape, key string) string {
	switch shape {
	case "run":
		switch key {
		case "id", "tenant_id", "session_id", "trigger_event_id", "subscription_connection_id", "status", "idempotency_key", "cancellation_requested_at", "started_at", "finished_at", "created_at", "updated_at":
			return "scalar"
		case "derived_context":
			return "derived"
		case "terminal_custody":
			return "custody"
		}
	case "custody":
		switch key {
		case "version":
			return "scalar"
		case "plan":
			return "plan"
		case "correspondence":
			return "correspondence"
		case "local_evidence":
			return "local"
		}
	case "plan":
		switch key {
		case "scope":
			return "scope"
		case "capsule":
			return "blob"
		case "frontier_digest":
			return "scalar"
		}
	case "correspondence":
		switch key {
		case "version", "capsule_digest", "terminal_assistant_event_id", "canonical_events_digest", "derived_context_digest", "accounting_disposition_digest":
			return "scalar"
		}
	case "evidence":
		switch key {
		case "version", "kind":
			return "scalar"
		case "managed_v1":
			return "managed"
		case "local_v1":
			return "local"
		}
	case "local":
		switch key {
		case "scope":
			return "scope"
		case "capsule":
			return "blob"
		case "version", "parent_digest", "source_digest", "profile_digest", "original_allocation_digest", "retired_allocation_digest", "frontier_digest", "residual_inventory_digest", "residual_receipt_digest", "accounting_disposition_digest", "stopped_at", "retired_at", "erased_at", "evidence_digest":
			return "scalar"
		}
	case "managed":
		switch key {
		case "cancellation":
			return "cancellation"
		case "provider_evidence":
			return "provider"
		case "resource_observations":
			return "resources"
		case "secondary_failure_codes":
			return "failures"
		case "version", "invocation_authority_digest", "substrate_binding_digest", "admission_cost_ceiling_digest", "prepared_invocation_digest", "effect_reservation_digest", "prepared_allocation_digest", "physical_invocation_claim_id", "allocation", "process", "credential_finalization", "cleanup", "egress", "image_attestation", "backend_attestation", "proxy_attestation", "failure_code", "evidence_digest":
			return "scalar"
		}
	case "cancellation":
		switch key {
		case "request", "backend_signal":
			return "scalar"
		}
	case "resource":
		switch key {
		case "kind", "state", "quantity", "unit", "provenance", "observed_at":
			return "scalar"
		}
	case "provider":
		switch key {
		case "version", "binding_digest", "acceptance_class", "finish_class", "route_state", "actual_model_vendor_id", "actual_model_id", "transport_kind", "transport_provider", "upstream_provider_id", "endpoint_id", "input_data_class", "effective_policy_digest", "policy_verdict", "usage_provenance", "input_tokens", "output_tokens", "failure_code", "evidence_digest":
			return "scalar"
		}
	}
	return ""
}
