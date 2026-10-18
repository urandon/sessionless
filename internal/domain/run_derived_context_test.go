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

// Synthetic immutable metadata only. These digests/refs attest neither real
// generation nor an upload/current grant, and no publisher is installed here.
func derivedContextPlan(t *testing.T, run domain.Run, attempt string, generation, sequence uint64) domain.DerivedContextPlanV1 {
	t.Helper()
	hash := strings.Repeat("1", 64)
	scope := domain.AttemptOperationScopeV1{TenantID: run.TenantID, SessionID: run.SessionID, OwnerUserID: "owner-1", RunID: run.ID, AttemptID: domain.AttemptID(attempt), LeaseID: domain.LeaseID("lease-" + attempt), FenceToken: 1, PhysicalInvocationClaimID: "claim-" + attempt, EffectReservationDigest: domain.AttemptEffectReservationDigestV1(hash), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(hash), HarnessBindingDigest: domain.HarnessBindingDigestV2(hash), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(hash)}
	p := domain.DerivedContextPlanV1{Version: 1, Scope: scope, Generation: generation, OperationSequence: sequence, ReservationDigest: domain.AttemptOperationReservationDigestV1(hash), ObservationDigest: domain.AttemptOperationObservationDigestV1(hash), NormalizedResponseDigest: hash, SourceViewDigest: hash, Summary: domain.BlobRef{TenantID: run.TenantID, Size: 200, SHA256: strings.Repeat("2", 64)}, Manifest: domain.BlobRef{TenantID: run.TenantID, Size: 300, SHA256: strings.Repeat("3", 64)}}
	derivedContextKeys(t, &p)
	if err := p.Validate(); err != nil {
		t.Fatalf("synthetic plan invalid: %v", err)
	}
	return p
}

func derivedContextKeys(t *testing.T, p *domain.DerivedContextPlanV1) {
	t.Helper()
	var err error
	p.Summary.Key, err = domain.DerivedContextObjectKeyV1(p.Scope, p.Generation, "summary", p.Summary.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	p.Manifest.Key, err = domain.DerivedContextObjectKeyV1(p.Scope, p.Generation, "manifest", p.Manifest.SHA256)
	if err != nil {
		t.Fatal(err)
	}
}

func derivedContextRun(t *testing.T) domain.Run {
	t.Helper()
	run := validRun()
	first := derivedContextPlan(t, run, "attempt-1", 1, 2)
	second := derivedContextPlan(t, run, "attempt-1", 2, 4)
	digest, err := second.Digest()
	if err != nil {
		t.Fatal(err)
	}
	run.DerivedContext = &domain.RunDerivedContextV1{Version: 1, Inventory: []domain.DerivedContextPlanV1{first, second}, Head: &domain.DerivedContextHeadV1{Version: 1, PlanDigest: digest}}
	if err := run.Validate(); err != nil {
		t.Fatalf("synthetic Run metadata invalid: %v", err)
	}
	return run
}

func TestRunDerivedContextDataInventoryHeadAndCanonicalKeys(t *testing.T) {
	run := derivedContextRun(t)
	metadata := run.DerivedContext
	first := metadata.Inventory[0]
	rawScope, err := json.Marshal(first.Scope)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(append([]byte("sessionless.derived-context-scope.v1\x00"), rawScope...))
	want := domain.SessionRunObjectPrefix(run.TenantID, run.SessionID, run.ID) + "derived-context-v1/" + hex.EncodeToString(hash[:]) + "/1/summary/sha256/" + first.Summary.SHA256 + ".json"
	if first.Summary.Key != want {
		t.Fatalf("fixed scoped summary key got=%q want=%q", first.Summary.Key, want)
	}
	if first.Summary.Key == first.Manifest.Key || metadata.Inventory[1].Summary.Key == first.Summary.Key {
		t.Fatal("retained object identities collapsed")
	}
	raw, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	var round domain.Run
	if err := json.Unmarshal(raw, &round); err != nil || round.Validate() != nil {
		t.Fatalf("Run metadata roundtrip: %v", err)
	}
	if !domain.EqualRunDerivedContextV1(metadata, round.DerivedContext) {
		t.Fatal("complete metadata equality lost canonical roundtrip")
	}
	// An earlier valid head may remain selected after a later plan's upload
	// failed. The original inventory pair remains counted, never refunded.
	oldDigest, _ := first.Digest()
	round.DerivedContext.Head.PlanDigest = oldDigest
	if round.Validate() != nil {
		t.Fatal("retaining earlier head with two planned pairs must remain structural DATA")
	}
	round.DerivedContext.Head = nil
	if round.Validate() != nil {
		t.Fatal("planned inventory without active head denied")
	}
}

func TestRunDerivedContextAbsentLegacyJSONUnchanged(t *testing.T) {
	run := validRun()
	raw, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"run-1","tenant_id":"tenant-a","session_id":"session-1","trigger_event_id":"event-1","subscription_connection_id":"subscription-1","status":"created","idempotency_key":"telegram-update-1","created_at":"2026-07-28T12:00:00Z","updated_at":"2026-07-28T12:00:00Z"}`
	if string(raw) != want {
		t.Fatal("absent metadata changed legacy Run encoding")
	}
	if bytes.Contains(raw, []byte("derived_context")) {
		t.Fatal("legacy absent metadata acquired JSON field")
	}
	var round domain.Run
	if err := json.Unmarshal(raw, &round); err != nil || round.DerivedContext != nil || round.Validate() != nil {
		t.Fatalf("legacy Run changed: %v", err)
	}
	if !domain.EqualRunDerivedContextV1(nil, nil) || domain.EqualRunDerivedContextV1(nil, derivedContextRun(t).DerivedContext) {
		t.Fatal("absent/present metadata equality collapsed")
	}
}

func TestRunDerivedContextCloneOwnsInventoryHeadAndTimes(t *testing.T) {
	run := derivedContextRun(t)
	start := run.CreatedAt.Add(time.Second)
	finish := run.CreatedAt.Add(2 * time.Second)
	cancel := run.CreatedAt.Add(time.Nanosecond)
	run.StartedAt = &start
	run.FinishedAt = &finish
	run.CancellationRequestedAt = &cancel
	run.Status = domain.RunSucceeded
	run.UpdatedAt = finish
	before, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	clone := run.Clone()
	clone.DerivedContext.Inventory[0].Summary.Size++
	clone.DerivedContext.Head.PlanDigest = strings.Repeat("f", 64)
	*clone.StartedAt = clone.StartedAt.Add(time.Second)
	*clone.FinishedAt = clone.FinishedAt.Add(time.Second)
	*clone.CancellationRequestedAt = clone.CancellationRequestedAt.Add(time.Second)
	after, _ := json.Marshal(run)
	if !bytes.Equal(before, after) {
		t.Fatal("clone mutation changed original Run metadata/time pointers")
	}
	clone = run.Clone()
	run.DerivedContext.Inventory[1].Manifest.Size++
	run.DerivedContext.Head.Version = 2
	if clone.DerivedContext.Inventory[1].Manifest.Size == run.DerivedContext.Inventory[1].Manifest.Size || clone.DerivedContext.Head.Version == run.DerivedContext.Head.Version {
		t.Fatal("original mutation changed returned metadata clone")
	}
}

func TestRunDerivedContextRejectsMalformedMetadata(t *testing.T) {
	for name, mutate := range map[string]func(*domain.Run){
		"metadata_version": func(r *domain.Run) { r.DerivedContext.Version = 2 },
		"empty_inventory":  func(r *domain.Run) { r.DerivedContext.Inventory = nil },
		"too_many": func(r *domain.Run) {
			r.DerivedContext.Inventory = append(r.DerivedContext.Inventory, r.DerivedContext.Inventory[1])
		},
		"duplicate_pair": func(r *domain.Run) { r.DerivedContext.Inventory[1] = r.DerivedContext.Inventory[0] },
		"first_generation2": func(r *domain.Run) {
			r.DerivedContext.Inventory[0].Generation = 2
			derivedContextKeys(t, &r.DerivedContext.Inventory[0])
		},
		"second_generation1": func(r *domain.Run) {
			r.DerivedContext.Inventory[1].Generation = 1
			derivedContextKeys(t, &r.DerivedContext.Inventory[1])
		},
		"sequence_regression": func(r *domain.Run) { r.DerivedContext.Inventory[1].OperationSequence = 1 },
		"same_attempt_new_claim": func(r *domain.Run) {
			r.DerivedContext.Inventory[1].Scope.PhysicalInvocationClaimID = "other-claim"
			derivedContextKeys(t, &r.DerivedContext.Inventory[1])
		},
		"mixed_owner": func(r *domain.Run) {
			r.DerivedContext.Inventory[1].Scope.OwnerUserID = "other-owner"
			derivedContextKeys(t, &r.DerivedContext.Inventory[1])
		},
		"mixed_policy": func(r *domain.Run) {
			r.DerivedContext.Inventory[1].Scope.AgentLoopPolicyDigest = domain.AgentLoopPolicyDigestV1(strings.Repeat("f", 64))
			derivedContextKeys(t, &r.DerivedContext.Inventory[1])
		},
		"mixed_binding": func(r *domain.Run) {
			r.DerivedContext.Inventory[1].Scope.HarnessBindingDigest = domain.HarnessBindingDigestV2(strings.Repeat("f", 64))
			derivedContextKeys(t, &r.DerivedContext.Inventory[1])
		},
		"foreign_run": func(r *domain.Run) {
			for i := range r.DerivedContext.Inventory {
				r.DerivedContext.Inventory[i].Scope.RunID = "other-run"
				derivedContextKeys(t, &r.DerivedContext.Inventory[i])
			}
		},
		"foreign_session": func(r *domain.Run) {
			for i := range r.DerivedContext.Inventory {
				r.DerivedContext.Inventory[i].Scope.SessionID = "other-session"
				derivedContextKeys(t, &r.DerivedContext.Inventory[i])
			}
		},
		"foreign_tenant": func(r *domain.Run) {
			for i := range r.DerivedContext.Inventory {
				r.DerivedContext.Inventory[i].Scope.TenantID = "other-tenant"
				r.DerivedContext.Inventory[i].Summary.TenantID = "other-tenant"
				r.DerivedContext.Inventory[i].Manifest.TenantID = "other-tenant"
				derivedContextKeys(t, &r.DerivedContext.Inventory[i])
			}
		},
		"head_version": func(r *domain.Run) {
			r.DerivedContext.Head = &domain.DerivedContextHeadV1{Version: 2, PlanDigest: strings.Repeat("a", 64)}
		},
		"head_missing_plan": func(r *domain.Run) {
			r.DerivedContext.Head = &domain.DerivedContextHeadV1{Version: 1, PlanDigest: strings.Repeat("a", 64)}
		},
	} {
		t.Run(name, func(t *testing.T) {
			run := derivedContextRun(t)
			run.DerivedContext.Head = nil
			mutate(&run)
			if run.Validate() == nil {
				t.Fatalf("invalid %s metadata accepted", name)
			}
		})
	}
}

func TestDerivedContextPlanRejectsUnboundedAndWrongObjects(t *testing.T) {
	for name, mutate := range map[string]func(*domain.DerivedContextPlanV1){
		"version":             func(p *domain.DerivedContextPlanV1) { p.Version = 2 },
		"missing_reservation": func(p *domain.DerivedContextPlanV1) { p.ReservationDigest = "" },
		"uppercase_digest":    func(p *domain.DerivedContextPlanV1) { p.NormalizedResponseDigest = strings.Repeat("A", 64) },
		"huge_scope_digest": func(p *domain.DerivedContextPlanV1) {
			p.Scope.AgentLoopPolicyDigest = domain.AgentLoopPolicyDigestV1(strings.Repeat("a", 64<<10))
		},
		"huge_key":              func(p *domain.DerivedContextPlanV1) { p.Summary.Key = strings.Repeat("a", 64<<10) },
		"summary_size":          func(p *domain.DerivedContextPlanV1) { p.Summary.Size = (32 << 10) + 1 },
		"manifest_size":         func(p *domain.DerivedContextPlanV1) { p.Manifest.Size = (128 << 10) + 1 },
		"empty_object":          func(p *domain.DerivedContextPlanV1) { p.Summary.Size = 0 },
		"foreign_object_tenant": func(p *domain.DerivedContextPlanV1) { p.Summary.TenantID = "foreign" },
		"arbitrary_key": func(p *domain.DerivedContextPlanV1) {
			p.Summary.Key = domain.SessionRunObjectPrefix(p.Scope.TenantID, p.Scope.SessionID, p.Scope.RunID) + "derived-context-v1/anything.json"
		},
		"wrong_kind":        func(p *domain.DerivedContextPlanV1) { p.Summary.Key = p.Manifest.Key },
		"wrong_content_key": func(p *domain.DerivedContextPlanV1) { p.Summary.SHA256 = strings.Repeat("f", 64) },
		"operation_zero":    func(p *domain.DerivedContextPlanV1) { p.OperationSequence = 0 },
		"operation_13":      func(p *domain.DerivedContextPlanV1) { p.OperationSequence = 13 },
	} {
		t.Run(name, func(t *testing.T) {
			p := derivedContextRun(t).DerivedContext.Inventory[0]
			mutate(&p)
			if p.Validate() == nil {
				t.Fatalf("invalid %s plan accepted", name)
			}
			if _, err := p.Digest(); err == nil {
				t.Fatal("invalid plan received canonical digest")
			}
		})
	}
}

func TestRunDerivedContextNewAttemptDoesNotRefundInventory(t *testing.T) {
	firstBinding := ownedAgentBindingFixture(t)
	secondBinding := firstBinding.Clone()
	secondBinding.AttemptID = "attempt-replacement"
	secondBinding.AgentLoopPolicy.AttemptID = secondBinding.AttemptID
	run := validRun()
	run.TenantID, run.ID = firstBinding.TenantID, firstBinding.RunID
	planForBinding := func(binding domain.HarnessBindingV2) domain.DerivedContextPlanV1 {
		t.Helper()
		if err := binding.ValidateAt(binding.AgentLoopPolicy.IssuedAt); err != nil {
			t.Fatalf("owned binding fixture invalid: %v", err)
		}
		bindingDigest, err := binding.Digest()
		if err != nil {
			t.Fatalf("binding digest: %v", err)
		}
		policyDigest, err := binding.AgentLoopPolicy.Digest()
		if err != nil {
			t.Fatalf("policy digest: %v", err)
		}
		plan := derivedContextPlan(t, run, string(binding.AttemptID), 1, 1)
		plan.Scope.OwnerUserID = binding.OwnerUserID
		plan.Scope.HarnessBindingDigest = bindingDigest
		plan.Scope.AgentLoopPolicyDigest = policyDigest
		derivedContextKeys(t, &plan)
		return plan
	}
	first, second := planForBinding(firstBinding), planForBinding(secondBinding)
	if first.Scope.HarnessBindingDigest == second.Scope.HarnessBindingDigest || first.Scope.AgentLoopPolicyDigest == second.Scope.AgentLoopPolicyDigest {
		t.Fatal("real binding/policy digests did not bind their distinct Attempt IDs")
	}
	headDigest, err := second.Digest()
	if err != nil {
		t.Fatal(err)
	}
	run.DerivedContext = &domain.RunDerivedContextV1{Version: 1, Inventory: []domain.DerivedContextPlanV1{first, second}, Head: &domain.DerivedContextHeadV1{Version: 1, PlanDigest: headDigest}}
	if err := run.Validate(); err != nil {
		t.Fatalf("replacement Attempt with genuine distinct binding/policy digests denied: %v", err)
	}
	raw, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	var round domain.Run
	if err := json.Unmarshal(raw, &round); err != nil || round.Validate() != nil || !domain.EqualRunDerivedContextV1(run.DerivedContext, round.DerivedContext) {
		t.Fatalf("replacement Attempt Run metadata roundtrip failed: %v", err)
	}
	t.Run("replacement_owner_mismatch", func(t *testing.T) {
		candidate := run.Clone()
		candidate.DerivedContext.Head = nil
		candidate.DerivedContext.Inventory[1].Scope.OwnerUserID = "other-owner"
		derivedContextKeys(t, &candidate.DerivedContext.Inventory[1])
		if candidate.Validate() == nil {
			t.Fatal("replacement Attempt changed the Run-owned inventory owner")
		}
	})
	t.Run("replacement_generation2", func(t *testing.T) {
		candidate := run.Clone()
		candidate.DerivedContext.Head = nil
		candidate.DerivedContext.Inventory[1].Generation = 2
		derivedContextKeys(t, &candidate.DerivedContext.Inventory[1])
		if candidate.Validate() == nil {
			t.Fatal("replacement Attempt did not start its generation at one")
		}
	})
	// A replacement Attempt still consumes the same global two-pair inventory.
	run.DerivedContext.Inventory = append(run.DerivedContext.Inventory, derivedContextPlan(t, run, "attempt-3", 1, 1))
	if run.Validate() == nil {
		t.Fatal("new Attempt refunded Run-owned two-pair inventory")
	}
}

func TestRunDerivedContextStrictBoundedJSON(t *testing.T) {
	run := derivedContextRun(t)
	raw, err := json.Marshal(run.DerivedContext)
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{
		"unknown_metadata":     bytes.Replace(raw, []byte(`"version":1`), []byte(`"future":true,"version":1`), 1),
		"unknown_plan":         bytes.Replace(raw, []byte(`"inventory":[{`), []byte(`"inventory":[{"approved":true,`), 1),
		"unknown_scope":        bytes.Replace(raw, []byte(`"scope":{`), []byte(`"scope":{"approved":true,`), 1),
		"unknown_blob":         bytes.Replace(raw, []byte(`"summary":{`), []byte(`"summary":{"exists":true,`), 1),
		"unknown_head":         bytes.Replace(raw, []byte(`"head":{`), []byte(`"head":{"published":true,`), 1),
		"case_metadata":        bytes.Replace(raw, []byte(`"version":1`), []byte(`"Version":1`), 1),
		"case_plan":            bytes.Replace(raw, []byte(`"generation":1`), []byte(`"Generation":1`), 1),
		"case_scope":           bytes.Replace(raw, []byte(`"owner_user_id":`), []byte(`"OWNER_USER_ID":`), 1),
		"case_blob":            bytes.Replace(raw, []byte(`"sha256":`), []byte(`"SHA256":`), 1),
		"case_head":            bytes.Replace(raw, []byte(`"plan_digest":`), []byte(`"PLAN_DIGEST":`), 1),
		"duplicate_metadata":   bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"duplicate_escaped":    bytes.Replace(raw, []byte(`"version":1`), []byte(`"\u0076ersion":1,"version":1`), 1),
		"duplicate_nested":     bytes.Replace(raw, []byte(`"scope":{`), []byte(`"scope":{"tenant_id":"tenant-a",`), 1),
		"missing_inventory":    []byte(`{"version":1}`),
		"missing_plan_version": bytes.Replace(raw, []byte(`"inventory":[{"version":1,`), []byte(`"inventory":[{`), 1),
		"missing_scope_owner":  bytes.Replace(raw, []byte(`"owner_user_id":"owner-1",`), nil, 1),
		"missing_summary_size": bytes.Replace(raw, []byte(`"size":200,`), nil, 1),
		"object_scalar":        bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":{"version":1}`), 1),
		"array_scalar":         bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":[1]`), 1),
		"null_inventory":       []byte(`{"version":1,"inventory":null}`),
		"null":                 []byte(`null`),
		"unknown_version":      bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":2`), 1),
		// encoding/json strips whitespace OUTSIDE the value before calling
		// UnmarshalJSON. Internal padding belongs to the actual bounded value.
		"oversize": bytes.Replace(raw, []byte(`"version":`), append([]byte(`"version":`), bytes.Repeat([]byte(" "), domain.MaxRunDerivedContextBytesV1)...), 1),
		"trailing": append(append([]byte(nil), raw...), []byte(` {}`)...),
	} {
		t.Run(name, func(t *testing.T) {
			before := run.DerivedContext.Clone()
			candidate := before.Clone()
			if json.Unmarshal(body, &candidate) == nil {
				t.Fatalf("invalid %s JSON accepted", name)
			}
			if !domain.EqualRunDerivedContextV1(&before, &candidate) {
				t.Fatal("failed decode partially replaced prior metadata")
			}
		})
	}
	tooMany := run.DerivedContext.Clone()
	tooMany.Inventory = append(tooMany.Inventory, tooMany.Inventory[0])
	body, err := json.Marshal(tooMany)
	if err != nil {
		t.Fatal(err)
	}
	var candidate domain.RunDerivedContextV1
	if json.Unmarshal(body, &candidate) == nil {
		t.Fatal("third decoded inventory plan accepted")
	}

	withoutHead := run.DerivedContext.Clone()
	withoutHead.Head = nil
	body, err = json.Marshal(withoutHead)
	if err != nil {
		t.Fatal(err)
	}
	body = bytes.Replace(body, []byte(`"version":1`), []byte(`"head":null,"\u0076ersion":1`), 1)
	if err := json.Unmarshal(body, &candidate); err != nil {
		t.Fatalf("canonical decoded member name and optional null head denied: %v", err)
	}
	if !domain.EqualRunDerivedContextV1(&withoutHead, &candidate) {
		t.Fatal("optional null head changed canonical metadata")
	}
}
