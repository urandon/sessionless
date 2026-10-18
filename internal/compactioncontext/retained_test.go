package compactioncontext_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func retainedHash(body []byte) string { h := sha256.Sum256(body); return hex.EncodeToString(h[:]) }

// Synthetic calibration arithmetic and a real fixed-codec normalized response
// build validated DATA. This is not an original Source/issuer/current Store,
// measured-model qualification, physical upload or publication fixture.
func retainedEvidence(t *testing.T, c domain.ConfiguredInferenceProfileV1, p domain.AgentLoopPolicyV1, g *compactioncontext.GenerationV1,
	sequence uint64, previous *compactioncontext.RetainedEvidenceV1, summary []byte, parent *compactioncontext.RetainedV1) compactioncontext.RetainedEvidenceV1 {
	t.Helper()
	h := strings.Repeat("a", 64)
	policyDigest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.AttemptOperationScopeV1{TenantID: c.TenantID, SessionID: "session", OwnerUserID: c.OwnerUserID, RunID: p.RunID, AttemptID: p.AttemptID,
		LeaseID: "lease", FenceToken: 1, PhysicalInvocationClaimID: "claim", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(h), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(h), HarnessBindingDigest: domain.HarnessBindingDigestV2(h), AgentLoopPolicyDigest: policyDigest}
	frozen := g.FrozenRequest()
	var request struct {
		MaxTokens uint64 `json:"max_tokens"`
	}
	if json.Unmarshal(frozen.Body(), &request) != nil {
		t.Fatal("frozen request")
	}
	fit := configuredinference.ContextFitV1{RequestDigest: frozen.Digest(), CalibrationDigest: c.CalibrationDigest, InputUpperBound: uint64(len(frozen.Body())) + 1, OutputReserve: request.MaxTokens}
	fit.TotalUpperBound = fit.InputUpperBound + fit.OutputReserve
	if len(summary) == 0 {
		summary = generationSummary(t, g)
	}
	next, err := g.Rebuild(summary)
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(next.Body(), &request) != nil {
		t.Fatal("rebuilt request")
	}
	nextFit := configuredinference.ContextFitV1{RequestDigest: next.Digest(), CalibrationDigest: c.CalibrationDigest, InputUpperBound: uint64(len(next.Body())) + 1, OutputReserve: request.MaxTokens}
	nextFit.TotalUpperBound = nextFit.InputUpperBound + nextFit.OutputReserve
	at := p.IssuedAt
	cost, _ := p.AdmissionCostCeiling.Digest()
	op := domain.AttemptOperationV1{Version: 1, Scope: scope, Sequence: sequence, Kind: domain.AttemptOperationCompactionV1, CallID: fmt.Sprintf("compaction-%d", sequence),
		RequestRef: "synthetic-original-request", RequestDigest: retainedHash(frozen.Body()), RequestBytes: uint64(len(frozen.Body())), ResultBytes: 64 << 10, ContextBytes: uint64(len(frozen.Body())), SummaryBytes: 16 << 10,
		Currency: p.AdmissionCostCeiling.Currency, PriceRevision: p.AdmissionCostCeiling.PriceRevision, AdmissionCostCeilingDigest: cost, Deadline: at.Add(30 * time.Second), CleanupDuration: 30 * time.Second,
		Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, CompactionCalls: 1, InputTokens: fit.InputUpperBound, OutputTokens: fit.OutputReserve, BodyBytes: uint64(len(frozen.Body())) + (64 << 10) + (160 << 10), ResponseBytes: 64 << 10, ActiveDuration: 30 * time.Second}}
	if previous != nil {
		op.PredecessorObservationDigest, _ = previous.Observation.Digest()
	}
	if err := op.Validate(); err != nil {
		t.Fatal(err)
	}
	od, _ := op.Digest()
	aggregate := op.Debit
	if previous != nil {
		aggregate, err = previous.Reservation.AggregateDebit.Add(op.Debit)
		if err != nil {
			t.Fatal(err)
		}
	}
	r := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: od, ReservedAt: at, AggregateDebit: aggregate,
		RegisteredAllocationDigest: domain.RegisteredAllocationDigestV2(h), RegisteredProxyDigest: domain.RegisteredProxyAttestationDigestV2(h)}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	rd, _ := r.Digest()
	wire, err := json.Marshal(map[string]any{"model": c.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": string(summary)}, "finish_reason": "stop"}}})
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := frozen.NormalizeResponseBodyV1(wire, op.Kind, op.ResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: od, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1,
		ResponseRef: "synthetic-original-response", ResponseDigest: retainedHash(normalized), ResponseBytes: uint64(len(normalized)), ObservedAt: at, Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
	if err := o.ValidateForReservation(r); err != nil {
		t.Fatal(err)
	}
	return compactioncontext.RetainedEvidenceV1{Reservation: r, Observation: o, Summary: bytes.Clone(summary), GenerationFit: fit, NextFit: nextFit, SummaryTokenUpperBound: uint64(len(summary)) + 1, Parent: parent}
}

func retainedFixture(t *testing.T) (domain.ConfiguredInferenceProfileV1, domain.AgentLoopPolicyV1, *compactioncontext.GenerationV1, compactioncontext.RetainedEvidenceV1, *compactioncontext.RetainedV1) {
	t.Helper()
	c, p := generationFixture(t)
	g, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), generationRecords(t), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	e := retainedEvidence(t, c, p, g, 1, nil, nil, nil)
	if e.GenerationFit.RequestDigest == e.Reservation.Operation.RequestDigest {
		t.Fatal("fixture must distinguish typed frozen digest from raw wire SHA")
	}
	v, err := g.BuildRetainedV1(e)
	if err != nil {
		t.Fatalf("actual typed retained DATA build: %v", err)
	}
	return c, p, g, e, v
}

func TestRetainedExactRoundtripAndCopiedAccessors(t *testing.T) {
	c, p, g, e, v := retainedFixture(t)
	if v.Plan().Validate() != nil || len(v.SummaryBody()) > 32<<10 || len(v.ManifestBody()) > 128<<10 || !bytes.Equal(v.SummaryText(), e.Summary) || v.ValidateForGeneration(g) != nil {
		t.Fatal("bounded exact roundtrip")
	}
	decoded, err := compactioncontext.DecodeRetainedV1(c, p, v.Plan(), v.SummaryBody(), v.ManifestBody(), nil)
	if err != nil || decoded.ValidateForGeneration(g) != nil || !bytes.Equal(decoded.SummaryText(), e.Summary) {
		t.Fatalf("actual retained reread DATA: %v", err)
	}
	for _, body := range [][]byte{v.SummaryBody(), v.ManifestBody(), v.SummaryText(), v.Parent().Summary} {
		clear(body)
	}
	parent := v.Parent()
	parent.CoveredSourceIDs[0] = strings.Repeat("f", 64)
	parent.Plan.Generation = 2
	clear(e.Summary)
	if _, err := compactioncontext.DecodeRetainedV1(c, p, v.Plan(), v.SummaryBody(), v.ManifestBody(), nil); err != nil {
		t.Fatal("caller mutated original retained owner", err)
	}
	for _, value := range []any{v, *v} {
		if strings.Contains(fmt.Sprintf("%#v", value), "Preserve every") {
			t.Fatal("private retained content leaked through formatting")
		}
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("opaque retained owner serialized")
		}
	}
}

func TestRetainedPreservesExactSummaryWhitespace(t *testing.T) {
	c, p, g, _, _ := retainedFixture(t)
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, generationSummary(t, g), "", "  "); err != nil {
		t.Fatal(err)
	}
	summary := append([]byte(" \n"), pretty.Bytes()...)
	summary = append(summary, '\n', ' ')
	e := retainedEvidence(t, c, p, g, 1, nil, summary, nil)
	v, err := g.BuildRetainedV1(e)
	if err != nil || !bytes.Equal(v.SummaryText(), summary) {
		t.Fatalf("validated summary bytes rewritten: %v", err)
	}
	if _, err := compactioncontext.DecodeRetainedV1(c, p, v.Plan(), v.SummaryBody(), v.ManifestBody(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedBuildRequiresExactTypedEvidence(t *testing.T) {
	_, _, g, original, v := retainedFixture(t)
	for _, name := range []string{"raw_instead_of_typed_fit", "wrong_next_request", "understated_input", "understated_summary", "over_summary", "malformed_summary", "unknown_effect", "overrun_usage", "foreign_scope", "unexpected_parent"} {
		t.Run(name, func(t *testing.T) {
			e := original
			e.Reservation = original.Reservation.Clone()
			e.Observation = original.Observation.Clone()
			e.Summary = bytes.Clone(original.Summary)
			switch name {
			case "raw_instead_of_typed_fit":
				e.GenerationFit.RequestDigest = e.Reservation.Operation.RequestDigest
			case "wrong_next_request":
				e.NextFit.RequestDigest = e.GenerationFit.RequestDigest
			case "understated_input":
				e.NextFit.InputUpperBound = 1
				e.NextFit.TotalUpperBound = 1 + e.NextFit.OutputReserve
			case "understated_summary":
				e.SummaryTokenUpperBound = uint64(len(e.Summary))
			case "over_summary":
				e.SummaryTokenUpperBound = 2049
			case "malformed_summary":
				e.Summary = bytes.Replace(e.Summary, []byte(`"omissions":[]`), []byte(`"omissions":[],"extra":true`), 1)
			case "unknown_effect":
				e.Observation.Outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
				e.Observation.Reason = domain.AttemptOperationReasonTransportUnknownV1
				e.Observation.ResponseRef, e.Observation.ResponseDigest, e.Observation.ResponseBytes = "", "", 0
				if err := e.Observation.ValidateForReservation(e.Reservation); err != nil {
					t.Fatal("unknown effect must be well formed", err)
				}
			case "overrun_usage":
				amount := e.Reservation.Operation.Debit.InputTokens + 1
				e.Observation.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: &amount}
				if err := e.Observation.ValidateForReservation(e.Reservation); err != nil {
					t.Fatal("overrun must be structurally valid", err)
				}
			case "foreign_scope":
				e.Reservation.Operation.Scope.OwnerUserID = "foreign-owner"
				e.Reservation.OperationDigest, _ = e.Reservation.Operation.Digest()
				e.Observation.OperationDigest = e.Reservation.OperationDigest
				e.Observation.ReservationDigest, _ = e.Reservation.Digest()
				if e.Reservation.Validate() != nil || e.Observation.ValidateForReservation(e.Reservation) != nil {
					t.Fatal("foreign scope must remain structurally valid")
				}
			case "unexpected_parent":
				e.Parent = v
			}
			if _, err := g.BuildRetainedV1(e); err == nil {
				t.Fatal("invalid typed evidence accepted", name)
			}
		})
	}
}

func retainedModel(t *testing.T, c domain.ConfiguredInferenceProfileV1, previous compactioncontext.CompletedOperationV1, sequence uint64) compactioncontext.CompletedOperationV1 {
	t.Helper()
	frozen, err := configuredinference.FreezeRequestV1(c, c.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "original current model input"}}, MaxOutputTokens: 100})
	if err != nil {
		t.Fatal(err)
	}
	body := frozen.Body()
	op := previous.Reservation.Operation.Clone()
	op.Sequence = sequence
	op.Kind = domain.AttemptOperationModelV1
	op.CallID = fmt.Sprintf("model-%d", sequence)
	op.SummaryBytes = 0
	op.RequestDigest = retainedHash(body)
	op.RequestBytes = uint64(len(body))
	op.ContextBytes = uint64(len(body))
	op.PredecessorObservationDigest, _ = previous.Observation.Digest()
	op.Debit = domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: uint64(len(body)) + 1, OutputTokens: 100, BodyBytes: uint64(len(body)) + (64 << 10), ResponseBytes: 64 << 10, ActiveDuration: 5 * time.Second}
	op.Deadline = previous.Reservation.ReservedAt.Add(5 * time.Second)
	od, err := op.Digest()
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := previous.Reservation.AggregateDebit.Add(op.Debit)
	if err != nil {
		t.Fatal(err)
	}
	r := previous.Reservation.Clone()
	r.Operation = op
	r.OperationDigest = od
	r.AggregateDebit = aggregate
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	rd, _ := r.Digest()
	wire := []byte(fmt.Sprintf(`{"model":"%s","choices":[{"index":0,"message":{"role":"assistant","content":"completed model %d"},"finish_reason":"stop"}]}`, c.ModelID, sequence))
	normalized, err := frozen.NormalizeResponseBodyV1(wire, op.Kind, op.ResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	o := previous.Observation.Clone()
	o.OperationDigest = od
	o.ReservationDigest = rd
	o.ResponseDigest = retainedHash(normalized)
	o.ResponseBytes = uint64(len(normalized))
	if err := o.ValidateForReservation(r); err != nil {
		t.Fatal(err)
	}
	return compactioncontext.CompletedOperationV1{Reservation: r, Observation: o, Request: body, Response: normalized}
}

func TestRetainedTwoGenerationsFlattenOriginalCoverage(t *testing.T) {
	c, p, g, e, first := retainedFixture(t)
	wire, err := json.Marshal(map[string]any{"model": c.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": string(e.Summary)}, "finish_reason": "stop"}}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := g.FrozenRequest().NormalizeResponseBodyV1(wire, domain.AttemptOperationCompactionV1, e.Reservation.Operation.ResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	op1 := compactioncontext.CompletedOperationV1{Reservation: e.Reservation, Observation: e.Observation, Request: g.FrozenRequest().Body(), Response: response}
	op2 := retainedModel(t, c, op1, 2)
	op3 := retainedModel(t, c, op2, 3)
	secondProgram, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), []compactioncontext.CompletedOperationV1{op1, op2, op3}, first.Parent(), nil)
	if err != nil {
		t.Fatal("actual parent plus completed-model DATA", err)
	}
	previous := compactioncontext.RetainedEvidenceV1{Reservation: op3.Reservation, Observation: op3.Observation}
	secondEvidence := retainedEvidence(t, c, p, secondProgram, 4, &previous, nil, first)
	second, err := secondProgram.BuildRetainedV1(secondEvidence)
	if err != nil {
		t.Fatal("generation2 retained DATA", err)
	}
	decoded, err := compactioncontext.DecodeRetainedV1(c, p, second.Plan(), second.SummaryBody(), second.ManifestBody(), first)
	if err != nil || decoded.ValidateForGeneration(secondProgram) != nil {
		t.Fatal("generation2 reread", err)
	}
	if len(second.Parent().CoveredSourceIDs) <= len(first.Parent().CoveredSourceIDs) {
		t.Fatal("no new original coverage")
	}
	seen := map[string]bool{}
	for _, id := range second.Parent().CoveredSourceIDs {
		if seen[id] {
			t.Fatal("duplicated original coverage")
		}
		seen[id] = true
	}
	for _, id := range first.Parent().CoveredSourceIDs {
		if !seen[id] {
			t.Fatal("lost parent original coverage")
		}
	}
	if _, err := compactioncontext.DecodeRetainedV1(c, p, second.Plan(), second.SummaryBody(), second.ManifestBody(), nil); err == nil {
		t.Fatal("missing actual parent DATA accepted")
	}
	if _, err := compactioncontext.DecodeRetainedV1(c, p, second.Plan(), second.SummaryBody(), second.ManifestBody(), second); err == nil {
		t.Fatal("cyclic/third closure accepted")
	}
}

// Re-key mutated byte-exact objects so negatives reach structural correlation
// rather than merely fail the outer hash/key check. It does not publish data.
func retainedRekey(t *testing.T, plan domain.DerivedContextPlanV1, sb, mb []byte) domain.DerivedContextPlanV1 {
	t.Helper()
	plan.Summary.SHA256 = retainedHash(sb)
	plan.Summary.Size = int64(len(sb))
	var err error
	plan.Summary.Key, err = domain.DerivedContextObjectKeyV1(plan.Scope, plan.Generation, "summary", plan.Summary.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	plan.Manifest.SHA256 = retainedHash(mb)
	plan.Manifest.Size = int64(len(mb))
	plan.Manifest.Key, err = domain.DerivedContextObjectKeyV1(plan.Scope, plan.Generation, "manifest", plan.Manifest.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func retainedMutated(t *testing.T, v *compactioncontext.RetainedV1, sb, mb []byte) (domain.DerivedContextPlanV1, []byte, []byte) {
	t.Helper()
	plan := v.Plan()
	old, _ := json.Marshal(plan.Summary)
	plan = retainedRekey(t, plan, sb, mb)
	updated, _ := json.Marshal(plan.Summary)
	if len(mb) > compactioncontext.MaxRetainedManifestBytesV1 {
		return plan, sb, mb
	}
	if !bytes.Contains(mb, old) {
		t.Fatal("manifest summary reference fixture")
	}
	mb = bytes.Replace(mb, old, updated, 1)
	plan = retainedRekey(t, plan, sb, mb)
	return plan, sb, mb
}

func TestRetainedStrictAndCorrelatedDenials(t *testing.T) {
	c, p, _, _, v := retainedFixture(t)
	for _, name := range []string{"unknown", "duplicate", "case", "missing", "trailing", "invalid_utf8", "invalid_surrogate", "wrong_version", "wrong_compatibility", "wrong_calibration", "wrong_through", "wrong_fit", "wrong_tokens", "source_order", "duplicate_source", "bad_cut", "wrong_owner", "missing_parent", "oversize_summary", "oversize_manifest"} {
		t.Run(name, func(t *testing.T) {
			sb, mb := v.SummaryBody(), v.ManifestBody()
			plan := v.Plan()
			switch name {
			case "unknown":
				mb = append([]byte(`{"unknown":1,`), mb[1:]...)
			case "duplicate":
				mb = bytes.Replace(mb, []byte(`"closure":1`), []byte(`"closure":1,"closure":1`), 1)
			case "case":
				mb = bytes.Replace(mb, []byte(`"closure":1`), []byte(`"Closure":1`), 1)
			case "missing":
				mb = bytes.Replace(mb, []byte(`,"closure":1`), nil, 1)
			case "trailing":
				mb = append(mb, []byte(` {}`)...)
			case "invalid_utf8":
				sb = bytes.Replace(sb, []byte("Preserve every"), append([]byte{0xff}, []byte("Preserve every")...), 1)
			case "invalid_surrogate":
				sb = bytes.Replace(sb, []byte("Preserve every"), []byte(`\ud800Preserve every`), 1)
			case "wrong_version":
				sb = bytes.Replace(sb, []byte(`"version":1`), []byte(`"version":2`), 1)
				mb = bytes.Replace(mb, []byte(`"version":1`), []byte(`"version":2`), 1)
			case "wrong_compatibility":
				sb, mb = replaceRetainedCore(t, sb, mb, "compatibility_digest", strings.Repeat("f", 64))
			case "wrong_calibration":
				sb, mb = replaceRetainedCore(t, sb, mb, "calibration_digest", strings.Repeat("f", 64))
			case "wrong_through":
				sb = bytes.Replace(sb, []byte(`"through_canonical_sequence":10`), []byte(`"through_canonical_sequence":11`), 1)
				mb = bytes.Replace(mb, []byte(`"through_canonical_sequence":10`), []byte(`"through_canonical_sequence":11`), 1)
			case "wrong_fit":
				mb = bytes.Replace(mb, []byte(`"total_upper_bound":`), []byte(`"total_upper_bound":9`), 1)
			case "wrong_tokens":
				sb = bytes.Replace(sb, []byte(`"summary_token_upper_bound":`), []byte(`"summary_token_upper_bound":9`), 1)
				mb = bytes.Replace(mb, []byte(`"summary_token_upper_bound":`), []byte(`"summary_token_upper_bound":9`), 1)
			case "source_order", "duplicate_source", "bad_cut":
				var fields struct {
					Cut json.RawMessage `json:"cut"`
				}
				if json.Unmarshal(mb, &fields) != nil {
					t.Fatal("cut fixture")
				}
				old := fields.Cut
				var cut map[string]json.RawMessage
				if json.Unmarshal(old, &cut) != nil {
					t.Fatal("cut fixture")
				}
				if name == "bad_cut" {
					fields.Cut = bytes.Replace(old, []byte(`"cut":9`), []byte(`"cut":8`), 1)
				} else {
					var sources []json.RawMessage
					if json.Unmarshal(cut["sources"], &sources) != nil {
						t.Fatal("source fixture")
					}
					original, _ := json.Marshal(sources)
					if name == "source_order" {
						sources[1], sources[2] = sources[2], sources[1]
					} else {
						sources[2] = sources[1]
					}
					changed, _ := json.Marshal(sources)
					fields.Cut = bytes.Replace(old, original, changed, 1)
				}
				if bytes.Equal(old, fields.Cut) {
					t.Fatal("negative did not change cut")
				}
				mb = bytes.Replace(mb, old, fields.Cut, 1)
				sb, mb = replaceRetainedCore(t, sb, mb, "cut_digest", retainedHash(fields.Cut))
			case "wrong_owner":
				sb = bytes.ReplaceAll(sb, []byte(`"owner_user_id":"private-user"`), []byte(`"owner_user_id":"foreign-user"`))
				mb = bytes.ReplaceAll(mb, []byte(`"owner_user_id":"private-user"`), []byte(`"owner_user_id":"foreign-user"`))
			case "missing_parent":
				mb = bytes.Replace(mb, []byte(`"closure":1`), []byte(`"closure":2`), 1)
			case "oversize_summary":
				sb = bytes.Repeat([]byte(" "), compactioncontext.MaxRetainedSummaryBytesV1+1)
			case "oversize_manifest":
				mb = bytes.Repeat([]byte(" "), compactioncontext.MaxRetainedManifestBytesV1+1)
			}
			plan, sb, mb = retainedMutated(t, v, sb, mb)
			if _, err := compactioncontext.DecodeRetainedV1(c, p, plan, sb, mb, nil); err == nil {
				t.Fatal("tampered DATA accepted", name)
			}
		})
	}
}

func replaceRetainedCore(t *testing.T, sb, mb []byte, key, value string) ([]byte, []byte) {
	t.Helper()
	var envelope struct {
		Core map[string]json.RawMessage `json:"core"`
	}
	if json.Unmarshal(mb, &envelope) != nil {
		t.Fatal("core fixture")
	}
	old := envelope.Core[key]
	encoded, _ := json.Marshal(value)
	needle := append([]byte(`"`+key+`":`), old...)
	replacement := append([]byte(`"`+key+`":`), encoded...)
	if len(old) == 0 || !bytes.Contains(sb, needle) || !bytes.Contains(mb, needle) {
		t.Fatal("core mutation fixture", key)
	}
	return bytes.Replace(sb, needle, replacement, 1), bytes.Replace(mb, needle, replacement, 1)
}
