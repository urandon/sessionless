package domain_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Synthetic bounded DATA, not an authentic original output/stop/erasure receipt.
func terminalCustodyFixture(t testing.TB, run domain.Run) domain.RunTerminalCustodyV1 {
	t.Helper()
	h := strings.Repeat("a", 64)
	s := domain.AttemptOperationScopeV1{TenantID: run.TenantID, SessionID: run.SessionID, OwnerUserID: "owner-1", RunID: run.ID, AttemptID: "attempt-1", LeaseID: "lease-1", FenceToken: 1, PhysicalInvocationClaimID: "claim-1", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(h), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(h), HarnessBindingDigest: domain.HarnessBindingDigestV2(h), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(h)}
	key, err := domain.TerminalCustodyObjectKeyV1(s, h)
	if err != nil {
		t.Fatal(err)
	}
	return domain.RunTerminalCustodyV1{Version: 1, Plan: domain.TerminalCustodyPlanV1{Scope: s, Capsule: domain.BlobRef{TenantID: s.TenantID, Key: key, Size: 512, SHA256: h}, FrontierDigest: h}}
}

func terminalSuccessFixture(t testing.TB, run domain.Run) domain.Run {
	t.Helper()
	v := terminalCustodyFixture(t, run)
	h := strings.Repeat("b", 64)
	d, err := domain.TerminalDerivedContextDigestV1(run.DerivedContext)
	if err != nil {
		t.Fatal(err)
	}
	v.Correspondence = &domain.TerminalCorrespondenceV1{Version: 1, CapsuleDigest: v.Plan.Capsule.SHA256, TerminalAssistantEventID: "terminal-assistant", CanonicalEventsDigest: h, DerivedContextDigest: d, AccountingDispositionDigest: h}
	e := domain.LocalExecutionEvidenceV1{Version: 1, Scope: v.Plan.Scope, ParentDigest: h, SourceDigest: h, ProfileDigest: h, OriginalAllocationDigest: h, RetiredAllocationDigest: strings.Repeat("c", 64), FrontierDigest: v.Plan.FrontierDigest, Capsule: v.Plan.Capsule, ResidualInventoryDigest: h, ResidualReceiptDigest: h, AccountingDispositionDigest: h, StoppedAt: run.CreatedAt.Add(time.Second), RetiredAt: run.CreatedAt.Add(2 * time.Second), ErasedAt: run.CreatedAt.Add(3 * time.Second)}
	e.EvidenceDigest, err = e.Digest()
	if err != nil {
		t.Fatal(err)
	}
	v.LocalEvidence = &e
	run.Status = domain.RunSucceeded
	at := run.CreatedAt.Add(4 * time.Second)
	run.UpdatedAt = at
	run.FinishedAt = &at
	run.TerminalCustody = &v
	if err := run.Validate(); err != nil {
		t.Fatalf("structural success DATA: %v", err)
	}
	return run
}

func TestRunTerminalCustodyRoundTripCloneAndLegacy(t *testing.T) {
	legacy := validRun()
	old, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(old, []byte("terminal_custody")) {
		t.Fatal("absent metadata changed legacy encoding")
	}
	for _, derived := range []bool{false, true} {
		run := legacy
		if derived {
			run = derivedContextRun(t)
		}
		run = terminalSuccessFixture(t, run)
		body, err := json.Marshal(run)
		if err != nil {
			t.Fatal(err)
		}
		var round domain.Run
		if err := json.Unmarshal(body, &round); err != nil || round.Validate() != nil || !domain.EqualRunTerminalCustodyV1(run.TerminalCustody, round.TerminalCustody) {
			t.Fatalf("terminal Run roundtrip derived=%t: %v", derived, err)
		}
		clone := run.Clone()
		clone.TerminalCustody.Correspondence.TerminalAssistantEventID = "changed"
		clone.TerminalCustody.LocalEvidence.Scope.OwnerUserID = "changed"
		clone.TerminalCustody.Plan.Capsule.Size++
		again, _ := json.Marshal(run)
		if !bytes.Equal(body, again) {
			t.Fatal("clone aliases original custody pointers")
		}
		clone = run.Clone()
		run.TerminalCustody.Correspondence.CanonicalEventsDigest = strings.Repeat("d", 64)
		if clone.TerminalCustody.Correspondence.CanonicalEventsDigest == run.TerminalCustody.Correspondence.CanonicalEventsDigest {
			t.Fatal("original aliases returned clone")
		}
	}
	if !domain.EqualRunTerminalCustodyV1(nil, nil) || domain.EqualRunTerminalCustodyV1(nil, &domain.RunTerminalCustodyV1{}) {
		t.Fatal("absent and present custody collapsed")
	}
}

func TestRunTerminalCustodyPlanResiduesAndSuccessfulCorrespondence(t *testing.T) {
	for _, status := range []domain.RunStatus{domain.RunCreated, domain.RunRunning, domain.RunFailed, domain.RunCancelled, domain.RunSucceeded} {
		run := validRun()
		v := terminalCustodyFixture(t, run)
		run.TerminalCustody = &v
		run.Status = status
		if status.Terminal() {
			at := run.CreatedAt.Add(time.Second)
			run.FinishedAt = &at
			run.UpdatedAt = at
		}
		if err := run.Validate(); err != nil {
			t.Fatalf("plan residue status=%s: %v", status, err)
		}
		if status != domain.RunSucceeded {
			success := terminalSuccessFixture(t, validRun())
			run.TerminalCustody = success.TerminalCustody
			if run.Validate() == nil {
				t.Fatalf("success correspondence permitted status=%s", status)
			}
		}
	}
}

func TestRunTerminalCustodyCleanupChronologyIsBoundToRun(t *testing.T) {
	for _, name := range []string{"exact_boundaries", "before_created", "after_finished"} {
		t.Run(name, func(t *testing.T) {
			run := terminalSuccessFixture(t, validRun())
			e := run.TerminalCustody.LocalEvidence
			e.StoppedAt = run.CreatedAt
			e.ErasedAt = *run.FinishedAt
			switch name {
			case "before_created":
				e.StoppedAt = run.CreatedAt.Add(-time.Microsecond)
			case "after_finished":
				e.ErasedAt = run.FinishedAt.Add(time.Microsecond)
			}
			var err error
			e.EvidenceDigest, err = e.Digest()
			if err != nil || e.Validate() != nil || run.TerminalCustody.Validate() != nil {
				t.Fatalf("chronology control must be intrinsically valid DATA: %v", err)
			}
			err = run.Validate()
			if (err != nil) != (name != "exact_boundaries") {
				t.Fatalf("case=%s Run chronology err=%v", name, err)
			}
		})
	}
}

func TestRunTerminalCustodyRejectsScopeKeyCutAndDigestMutations(t *testing.T) {
	for _, name := range []string{"version", "tenant", "session", "run", "owner", "foreign_capsule", "arbitrary_key", "capsule_size", "uppercase_hash", "frontier", "local_scope", "local_capsule", "local_frontier", "local_accounting", "missing_correspondence", "wrong_capsule_digest", "wrong_derived_digest", "invalid_assistant", "self_digest", "phase_order", "phase_precision"} {
		t.Run(name, func(t *testing.T) {
			run := terminalSuccessFixture(t, derivedContextRun(t))
			v := run.TerminalCustody
			switch name {
			case "version":
				v.Version = 2
			case "tenant":
				run.TenantID = "foreign"
			case "session":
				run.SessionID = "foreign"
			case "run":
				run.ID = "foreign"
			case "owner":
				v.Plan.Scope.OwnerUserID = "foreign"
				v.Plan.Capsule.Key, _ = domain.TerminalCustodyObjectKeyV1(v.Plan.Scope, v.Plan.Capsule.SHA256)
				v.LocalEvidence.Scope = v.Plan.Scope
				v.LocalEvidence.Capsule = v.Plan.Capsule
			case "foreign_capsule":
				v.Plan.Capsule.TenantID = "foreign"
			case "arbitrary_key":
				v.Plan.Capsule.Key += "/extra"
			case "capsule_size":
				v.Plan.Capsule.Size = domain.MaxTerminalCapsuleBytesV1 + 1
			case "uppercase_hash":
				v.Plan.Capsule.SHA256 = strings.Repeat("A", 64)
			case "frontier":
				v.Plan.FrontierDigest = "bad"
			case "local_scope":
				v.LocalEvidence.Scope.AttemptID = "foreign"
			case "local_capsule":
				v.LocalEvidence.Capsule.Size++
			case "local_frontier":
				v.LocalEvidence.FrontierDigest = strings.Repeat("d", 64)
			case "local_accounting":
				v.LocalEvidence.AccountingDispositionDigest = strings.Repeat("d", 64)
			case "missing_correspondence":
				v.Correspondence = nil
			case "wrong_capsule_digest":
				v.Correspondence.CapsuleDigest = strings.Repeat("d", 64)
			case "wrong_derived_digest":
				v.Correspondence.DerivedContextDigest = strings.Repeat("d", 64)
			case "invalid_assistant":
				v.Correspondence.TerminalAssistantEventID = ""
			case "self_digest":
				v.LocalEvidence.EvidenceDigest = strings.Repeat("d", 64)
			case "phase_order":
				v.LocalEvidence.RetiredAt = v.LocalEvidence.StoppedAt.Add(-time.Microsecond)
			case "phase_precision":
				v.LocalEvidence.ErasedAt = v.LocalEvidence.ErasedAt.Add(time.Nanosecond)
			}
			// Recompute unkeyed evidence when structurally possible, so a stale
			// self-hash does not mask cross-metadata correlation failures.
			if v.LocalEvidence != nil && name != "self_digest" {
				if d, err := v.LocalEvidence.Digest(); err == nil {
					v.LocalEvidence.EvidenceDigest = d
				}
			}
			if run.Validate() == nil {
				t.Fatal("mutated retained DATA accepted")
			}
		})
	}
}

func TestTerminalCustodyFixedObjectKeyAndCompleteDerivedDigest(t *testing.T) {
	run := validRun()
	v := terminalCustodyFixture(t, run)
	v.Plan.Capsule.Size = domain.MaxTerminalCapsuleBytesV1
	if v.Validate() != nil {
		t.Fatal("exact capsule cap denied")
	}
	if !strings.HasSuffix(v.Plan.Capsule.Key, "/sha256/"+v.Plan.Capsule.SHA256+".json") {
		t.Fatal("content hash must remain raw blob SHA256")
	}
	nilDigest, err := domain.TerminalDerivedContextDigestV1(nil)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	h.Write([]byte("sessionless.terminal-derived-context.v1\x00null"))
	if nilDigest != hex.EncodeToString(h.Sum(nil)) {
		t.Fatal("no-compaction digest is not canonical domain-separated null")
	}
	d := derivedContextRun(t).DerivedContext
	before, _ := domain.TerminalDerivedContextDigestV1(d)
	d.Head = nil
	after, _ := domain.TerminalDerivedContextDigestV1(d)
	if before == after || before == nilDigest || after == nilDigest {
		t.Fatal("complete derived inventory/head not included in terminal digest")
	}
}

func TestRunTerminalCustodyStrictBoundedJSON(t *testing.T) {
	run := terminalSuccessFixture(t, validRun())
	valid, _ := json.Marshal(run.TerminalCustody)
	for _, name := range []string{"unknown", "case_alias", "duplicate", "unicode_duplicate", "nested_unknown", "nested_duplicate", "array", "deep", "oversize", "missing_version", "future_version"} {
		t.Run(name, func(t *testing.T) {
			body := bytes.Clone(valid)
			switch name {
			case "unknown":
				body = bytes.Replace(body, []byte(`{"version":1`), []byte(`{"unknown":1,"version":1`), 1)
			case "case_alias":
				body = bytes.Replace(body, []byte(`"plan":`), []byte(`"Plan":`), 1)
			case "duplicate":
				body = bytes.Replace(body, []byte(`{"version":1`), []byte(`{"version":1,"version":1`), 1)
			case "unicode_duplicate":
				body = bytes.Replace(body, []byte(`{"version":1`), []byte(`{"version":1,"\u0076ersion":1`), 1)
			case "nested_unknown":
				body = bytes.Replace(body, []byte(`"scope":{`), []byte(`"scope":{"future":1,`), 1)
			case "nested_duplicate":
				body = bytes.Replace(body, []byte(`"tenant_id":"tenant-a"`), []byte(`"tenant_id":"tenant-a","tenant_id":"tenant-a"`), 1)
			case "array":
				body = bytes.Replace(body, []byte(`"plan":{`), []byte(`"plan":[{`), 1)
			case "deep":
				body = []byte(`{"version":` + strings.Repeat("[", 1000) + `1` + strings.Repeat("]", 1000) + `}`)
			case "oversize":
				body = bytes.Replace(body, []byte("{"), append([]byte("{"), bytes.Repeat([]byte(" "), domain.MaxRunTerminalCustodyBytesV1)...), 1)
			case "missing_version":
				body = bytes.Replace(body, []byte(`"version":1,`), nil, 1)
			case "future_version":
				body = bytes.Replace(body, []byte(`"version":1`), []byte(`"version":2`), 1)
			}
			var decoded domain.RunTerminalCustodyV1
			if err := json.Unmarshal(body, &decoded); err == nil {
				t.Fatal("invalid closed custody JSON accepted")
			}
		})
	}
	body, _ := json.Marshal(run)
	for _, key := range []string{`"Terminal_Custody":`, `"terminal_custody":null,"terminal_custody":`} {
		bad := bytes.Replace(body, []byte(`"terminal_custody":`), []byte(key), 1)
		var r domain.Run
		if json.Unmarshal(bad, &r) == nil {
			t.Fatal("Run-level metadata alias/duplicate accepted")
		}
	}
}

func FuzzRunTerminalCustodyClosedRoundTrip(f *testing.F) {
	v := terminalCustodyFixture(f, validRun())
	body, _ := json.Marshal(v)
	f.Add(body)
	f.Add([]byte(`{"version":1,"plan":null}`))
	f.Add([]byte(`{"version":1,"\u0076ersion":1}`))
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > domain.MaxRunTerminalCustodyBytesV1+1 {
			return
		}
		var v domain.RunTerminalCustodyV1
		if json.Unmarshal(body, &v) != nil {
			return
		}
		if err := v.Validate(); err != nil {
			t.Fatalf("decoder accepted invalid metadata: %v", err)
		}
		encoded, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var again domain.RunTerminalCustodyV1
		if json.Unmarshal(encoded, &again) != nil || !domain.EqualRunTerminalCustodyV1(&v, &again) {
			t.Fatal("closed roundtrip changed metadata")
		}
	})
}
