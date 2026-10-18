package domain_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestExecutionEvidenceV2ManagedUsesUnchangedV1(t *testing.T) {
	e, a, r, _, _ := validSubstrateEvidence(t)
	v := domain.ExecutionEvidenceV2{Version: 2, Kind: domain.ExecutionEvidenceManagedV1, ManagedV1: &e}
	if err := v.ValidateForPersistedAuthority(a, r); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(v)
	var round domain.ExecutionEvidenceV2
	if err := json.Unmarshal(body, &round); err != nil || round.ValidateForPersistedAuthority(a, r) != nil {
		t.Fatalf("managed V1 wrapper roundtrip: %v", err)
	}
	local := localRuntimeAllocationFixture(t, false)
	// Match every persisted local digest and recompute V1's unkeyed hash so
	// the unchanged local-exclusion validator, not a foreign digest, decides.
	forged := e.Clone()
	forged.InvocationAuthorityDigest, _ = local.authority.Digest()
	forged.SubstrateBindingDigest, _ = local.authority.SubstrateBinding.Digest()
	forged.AdmissionCostCeilingDigest, _ = local.authority.AdmissionCostCeiling.Digest()
	forged.EffectReservationDigest, _ = local.parent.DigestForAuthority(local.authority)
	forged.PreparedInvocationDigest = local.preparedDigest
	forged.PreparedAllocationDigest, _ = local.prepared.DigestForBinding(local.authority.SubstrateBinding, local.authority.HarnessBinding.Backend)
	forged.PhysicalInvocationClaimID = local.parent.PhysicalInvocationClaimID
	forged.ProviderEvidence = nil
	forged.Egress = domain.SubstrateEgressDeniedV1
	forged.FailureCode = domain.SubstrateExecutionFailureEgressDeniedV1
	forged.EvidenceDigest = ""
	raw, _ := json.Marshal(forged)
	h := sha256.New()
	h.Write([]byte("sessionless.substrate-execution-evidence.v1\x00"))
	h.Write(raw)
	forged.EvidenceDigest = domain.SubstrateExecutionEvidenceDigestV1(hex.EncodeToString(h.Sum(nil)))
	wrapped := domain.ExecutionEvidenceV2{Version: 2, Kind: domain.ExecutionEvidenceManagedV1, ManagedV1: &forged}
	raw, _ = json.Marshal(wrapped)
	var structurallyValid domain.ExecutionEvidenceV2
	if err := json.Unmarshal(raw, &structurallyValid); err != nil {
		t.Fatalf("local-exclusion control must be valid intrinsic V1 DATA: %v", err)
	}
	var invalid domain.ValidationError
	if err := wrapped.ValidateForPersistedAuthority(local.authority, local.parent); !errors.As(err, &invalid) || invalid.Field != "substrate_execution_evidence.local" {
		t.Fatalf("managed wrapper must preserve exact V1 local exclusion: %v", err)
	}
	clone := v.Clone()
	clone.ManagedV1.ResourceObservations[0].Provenance = domain.SubstrateResourceProvenanceHarnessMeasuredV1
	if v.ManagedV1.ResourceObservations[0].Provenance == clone.ManagedV1.ResourceObservations[0].Provenance {
		t.Fatal("wrapper clone aliases V1 observations")
	}
}

func TestExecutionEvidenceV2LocalCorrelatesOriginalStructuralAuthority(t *testing.T) {
	f := localRuntimeAllocationFixture(t, false)
	a, r := f.authority, f.parent
	run := validRun()
	run.TenantID, run.SessionID, run.ID = f.source.Scope.TenantID, f.source.Scope.SessionID, r.RunID
	run.CreatedAt, run.UpdatedAt = f.at, f.at
	run = terminalSuccessFixture(t, run)
	e := run.TerminalCustody.LocalEvidence
	rd, _ := r.DigestForAuthority(a)
	pd, _ := a.HarnessBinding.AgentLoopPolicy.Digest()
	e.Scope = domain.AttemptOperationScopeV1{TenantID: r.TenantID, SessionID: f.source.Scope.SessionID, OwnerUserID: a.HarnessBinding.OwnerUserID, RunID: r.RunID, AttemptID: r.AttemptID, LeaseID: r.LeaseID, FenceToken: r.FenceToken, PhysicalInvocationClaimID: r.PhysicalInvocationClaimID, EffectReservationDigest: rd, InvocationAuthorityDigest: r.InvocationAuthorityDigest, HarnessBindingDigest: r.HarnessBindingDigest, AgentLoopPolicyDigest: pd}
	e.ProfileDigest = a.HarnessBinding.Backend.BackendProfileDigest
	e.Capsule.Key, _ = domain.TerminalCustodyObjectKeyV1(e.Scope, e.Capsule.SHA256)
	e.EvidenceDigest, _ = e.Digest()
	v := domain.ExecutionEvidenceV2{Version: 2, Kind: domain.ExecutionEvidenceLocalV1, LocalV1: e}
	if err := v.ValidateForPersistedAuthority(a, r); err != nil {
		t.Fatalf("validated local authority DATA: %v", err)
	}
	body, _ := json.Marshal(v)
	var round domain.ExecutionEvidenceV2
	if err := json.Unmarshal(body, &round); err != nil || round.ValidateForPersistedAuthority(a, r) != nil {
		t.Fatalf("local DATA wrapper roundtrip: %v", err)
	}
	for _, name := range []string{"scope", "profile", "pre_reservation_stop", "managed_authority"} {
		changed := v.Clone()
		switch name {
		case "scope":
			changed.LocalV1.Scope.FenceToken++
		case "profile":
			changed.LocalV1.ProfileDigest = strings.Repeat("f", 64)
		case "pre_reservation_stop":
			changed.LocalV1.StoppedAt = f.at.Add(-time.Microsecond)
		case "managed_authority":
			_, a, r, _, _ = validSubstrateEvidence(t)
		}
		changed.LocalV1.Capsule.Key, _ = domain.TerminalCustodyObjectKeyV1(changed.LocalV1.Scope, changed.LocalV1.Capsule.SHA256)
		changed.LocalV1.EvidenceDigest, _ = changed.LocalV1.Digest()
		if err := changed.LocalV1.Validate(); err != nil {
			t.Fatalf("foreign %s control must remain valid local DATA: %v", name, err)
		}
		if changed.ValidateForPersistedAuthority(a, r) == nil {
			t.Fatalf("foreign %s accepted after DATA digest recomputation", name)
		}
	}
}

func TestLocalExecutionEvidenceDigestExcludesOnlyItsOwnMember(t *testing.T) {
	e := terminalSuccessFixture(t, validRun()).TerminalCustody.LocalEvidence
	body, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	member := bytes.LastIndex(body, []byte(`,"evidence_digest":`))
	if member < 0 {
		t.Fatal("canonical evidence member missing")
	}
	withoutSelf := append(bytes.Clone(body[:member]), '}')
	h := sha256.New()
	h.Write([]byte("sessionless.local-execution-evidence.v1\x00"))
	h.Write(withoutSelf)
	if got, err := e.Digest(); err != nil || got != hex.EncodeToString(h.Sum(nil)) {
		t.Fatalf("digest must include every other exact member and exclude self: %v", err)
	}
	before, _ := e.Digest()
	e.EvidenceDigest = strings.Repeat("d", 64)
	after, _ := e.Digest()
	if before != after || e.Validate() == nil {
		t.Fatal("self hash creates a cycle or stale attached hash passed validation")
	}
}

func sealedManagedUnicodeEvidence(t *testing.T, model string) (domain.ExecutionEvidenceV2, domain.ServerlessInvocationAuthorityV1, domain.AttemptEffectReservationV1) {
	t.Helper()
	e, a, _, allocation, prepared := validSubstrateEvidence(t)
	a.HarnessBinding.ModelID = model
	if err := a.ValidateAt(a.Lease.AcquiredAt); err != nil {
		t.Fatalf("valid Unicode model authority: %v", err)
	}
	r, err := domain.BuildAttemptEffectReservationV1(a, "unicode-model-claim", nil, a.Lease.AcquiredAt)
	if err != nil {
		t.Fatal(err)
	}
	p := completedProviderEvidence(t, a.HarnessBinding)
	e.ProviderEvidence = &p
	e, err = e.SealForAuthority(a, r, allocation, prepared)
	if err != nil {
		t.Fatalf("valid sealed managed Unicode DATA: %v", err)
	}
	return domain.ExecutionEvidenceV2{Version: 2, Kind: domain.ExecutionEvidenceManagedV1, ManagedV1: &e}, a, r
}

func TestExecutionEvidenceV2RejectsLossyUnicodeBeforeDecode(t *testing.T) {
	// These are genuine domain-sealed DATA values. Literal U+FFFD is valid;
	// malformed encodings which Go silently maps onto it must not pass a seal.
	v, a, r := sealedManagedUnicodeEvidence(t, "model-\ufffd")
	body, _ := json.Marshal(v)
	for _, name := range []string{"literal_replacement", "escaped_replacement", "raw_invalid_utf8", "unpaired_high", "unpaired_low"} {
		t.Run(name, func(t *testing.T) {
			bad := bytes.Clone(body)
			replacement := []byte("\ufffd")
			switch name {
			case "escaped_replacement":
				replacement = []byte(`\ufffd`)
			case "raw_invalid_utf8":
				replacement = []byte{0xff}
			case "unpaired_high":
				replacement = []byte(`\ud800`)
			case "unpaired_low":
				replacement = []byte(`\udc00`)
			}
			bad = bytes.Replace(bad, []byte("\ufffd"), replacement, 1)
			var decoded domain.ExecutionEvidenceV2
			err := json.Unmarshal(bad, &decoded)
			valid := name == "literal_replacement" || name == "escaped_replacement"
			if valid {
				if err != nil || decoded.ValidateForPersistedAuthority(a, r) != nil {
					t.Fatalf("valid Unicode representation denied: %v", err)
				}
			} else if err == nil {
				t.Fatal("lossy Unicode accepted as correctly sealed managed DATA")
			}
		})
	}
	for _, model := range []string{"model-\U0001f680", `model-\ud800`} {
		v, a, r := sealedManagedUnicodeEvidence(t, model)
		body, _ := json.Marshal(v)
		if strings.Contains(model, "\U0001f680") {
			body = bytes.Replace(body, []byte("\U0001f680"), []byte(`\ud83d\ude80`), 1)
		}
		var decoded domain.ExecutionEvidenceV2
		if err := json.Unmarshal(body, &decoded); err != nil || decoded.ValidateForPersistedAuthority(a, r) != nil {
			t.Fatalf("paired surrogate or literal escaped backslash denied: %v", err)
		}
	}
}

func TestRunDecodeRejectsLossyUnicodeBeforeBaseValidation(t *testing.T) {
	body, _ := json.Marshal(validRun())
	for _, replacement := range [][]byte{{0xff}, []byte(`\ud800`), []byte(`\udc00`)} {
		bad := bytes.Replace(body, []byte(`"run-1"`), append(append([]byte(`"run-`), replacement...), '"'), 1)
		var run domain.Run
		if json.Unmarshal(bad, &run) == nil {
			t.Fatal("Run decoder silently normalized invalid base identity Unicode")
		}
	}
}

func TestExecutionEvidenceV2ClosedArmsAndJSON(t *testing.T) {
	e, a, r, _, _ := validSubstrateEvidence(t)
	local := terminalSuccessFixture(t, validRun()).TerminalCustody.LocalEvidence
	for _, name := range []string{"missing", "mixed", "unknown_kind", "wrong_version"} {
		v := domain.ExecutionEvidenceV2{Version: 2, Kind: domain.ExecutionEvidenceManagedV1, ManagedV1: &e}
		switch name {
		case "missing":
			v.ManagedV1 = nil
		case "mixed":
			v.LocalV1 = local
		case "unknown_kind":
			v.Kind = "legacy"
		case "wrong_version":
			v.Version = 1
		}
		if v.ValidateForPersistedAuthority(a, r) == nil {
			t.Fatalf("invalid union %s accepted", name)
		}
		body, _ := json.Marshal(v)
		var round domain.ExecutionEvidenceV2
		if json.Unmarshal(body, &round) == nil {
			t.Fatalf("invalid wire union %s accepted", name)
		}
	}
	body, _ := json.Marshal(domain.ExecutionEvidenceV2{Version: 2, Kind: domain.ExecutionEvidenceManagedV1, ManagedV1: &e})
	for _, name := range []string{"unknown", "case", "duplicate", "nested_case", "nested_duplicate", "array_count", "oversize"} {
		bad := bytes.Clone(body)
		switch name {
		case "unknown":
			bad = bytes.Replace(bad, []byte(`"version":2`), []byte(`"version":2,"unknown":1`), 1)
		case "case":
			bad = bytes.Replace(bad, []byte(`"managed_v1":`), []byte(`"Managed_v1":`), 1)
		case "duplicate":
			bad = bytes.Replace(bad, []byte(`"version":2`), []byte(`"version":2,"\u0076ersion":2`), 1)
		case "nested_case":
			bad = bytes.Replace(bad, []byte(`"cancellation":`), []byte(`"Cancellation":`), 1)
		case "nested_duplicate":
			bad = bytes.Replace(bad, []byte(`"request":"none"`), []byte(`"request":"none","request":"none"`), 1)
		case "array_count":
			bad = bytes.Replace(bad, []byte(`"secondary_failure_codes":null`), []byte(`"secondary_failure_codes":[`+strings.Repeat(`"cleanup_failed",`, 16)+`"cleanup_failed"]`), 1)
		case "oversize":
			bad = bytes.Replace(bad, []byte("{"), append([]byte("{"), bytes.Repeat([]byte(" "), domain.MaxExecutionEvidenceBytesV2)...), 1)
		}
		if bytes.Equal(bad, body) {
			t.Fatalf("mutation %s did not match canonical fixture", name)
		}
		var round domain.ExecutionEvidenceV2
		if err := json.Unmarshal(bad, &round); err == nil {
			t.Fatalf("invalid closed JSON %s accepted", name)
		}
	}
}
