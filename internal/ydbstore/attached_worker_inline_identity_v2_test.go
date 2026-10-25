package ydbstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func maximumInlineIdentityFixtureV2(t *testing.T) AttachedWorkerOutputReceiptV2 {
	t.Helper()
	r, loaded, _ := inlineReceiptFixtureV2(t)
	// Both byte and rune maxima are admitted. Every rune needs six JSON
	// bytes, while a native final can retain the literal UTF-8 representation.
	r.Candidate.Summary = strings.Repeat("<", 31_616) + strings.Repeat("\u2028", 384)
	r.Candidate.Artifacts = nil
	if len(r.Candidate.Summary) != 32<<10 || utf8.RuneCountInString(r.Candidate.Summary) != 32_000 || r.Candidate.Validate() != nil {
		t.Fatal("maximum valid semantic answer fixture")
	}
	sum := sha256.Sum256([]byte(r.Candidate.Summary))
	r.Producer.AnswerDigest = hex.EncodeToString(sum[:])
	var err error
	r.CandidateFingerprint, err = inlineReceiptPlanFingerprintV2(r)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := attachedworkeroutput.VerifyInlineV2(r.Candidate, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer verified.Clear()
	planned := new(plannedReceiptBlobs)
	t.Cleanup(func() {
		for _, item := range planned.items {
			clear(item.body)
		}
	})
	r.Materialization, err = attachedworkeroutput.MaterializeInlineV2(t.Context(), planned, loaded, r.Binding.LeaseID, r.Binding.LeaseGeneration, verified, r.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	r.CanonicalDigest, err = attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalSucceeded, r.Materialization)
	if err != nil {
		t.Fatal(err)
	}
	r.Materialization.EvidenceDigest = r.CanonicalDigest
	r.Stage, r.BodyClaimID, r.CopyOwnerID = attachedworkeroutput.InlineCanonicalCopyingV2, strings.Repeat("c", 64), strings.Repeat("d", 64)
	if err := validateInlineFixtureV2(r); err != nil {
		t.Fatal("valid maximum canonical receipt", err)
	}
	return r
}

func TestInlineReceiptComparisonDoesNotDuplicateMaximumPayload(t *testing.T) {
	r := maximumInlineIdentityFixtureV2(t)
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	var restored AttachedWorkerOutputReceiptV2
	if json.Unmarshal(raw, &restored) != nil || !sameInlineReceiptV2(r, restored) {
		t.Fatal("maximum original receipt differs after JSON persistence")
	}
	// This measures the comparison itself, not process heap or the whole
	// invocation. The generous budget admits metadata allocations but not a
	// single large payload snapshot. Elapsed time never decides correctness.
	allocations := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if !sameInlineReceiptV2(r, restored) {
				b.Fatal("comparison changed identity")
			}
		}
	})
	if got := allocations.AllocedBytesPerOp(); got > 64<<10 {
		t.Fatalf("receipt comparison duplicates semantic payload: allocated=%d limit=%d serialized-record=%d", got, 64<<10, len(raw))
	}
	metadata, err := json.Marshal(inlineReceiptComparisonMetadataV2(r))
	if err != nil || len(metadata) > 32<<10 || inlineReceiptComparisonMetadataV2(r).Candidate.Summary != "" {
		t.Fatalf("maximum-answer metadata projection: bytes=%d err=%v", len(metadata), err)
	}
	t.Logf("comparison allocated=%d B/op, metadata=%d B, original receipt=%d B; not whole-invocation peak", allocations.AllocedBytesPerOp(), len(metadata), len(raw))
}

func TestInlineReceiptComparisonPreservesJSONIdentity(t *testing.T) {
	canonical := inlineCanonicalFixtureV2(t)
	prepared, _, _ := inlineReceiptFixtureV2(t)
	claimed, retired, ready := prepared, prepared, canonical
	claimed.Stage, claimed.BodyClaimID = attachedworkeroutput.InlineBodyClaimedV2, strings.Repeat("c", 64)
	retired.Stage = attachedworkeroutput.InlineRetiredWithoutWritesV2
	ready.Stage = attachedworkeroutput.InlineReadyV2
	for _, base := range []AttachedWorkerOutputReceiptV2{prepared, claimed, retired, canonical, ready} {
		for _, test := range []struct {
			name   string
			mutate func(*AttachedWorkerOutputReceiptV2)
		}{
			{name: "unchanged", mutate: func(*AttachedWorkerOutputReceiptV2) {}},
			{name: "version", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.Version++ }},
			{name: "stage", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.Stage = attachedworkeroutput.InlineReadyV2 }},
			{name: "tenant", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.Binding.TenantID += "-other" }},
			{name: "nonce", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.Nonce += "-other" }},
			{name: "fingerprint", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.CandidateFingerprint = strings.Repeat("e", 64) }},
			{name: "summary", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.Candidate.Summary += " changed" }},
			{name: "producer", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.Producer.FinalDigest = strings.Repeat("f", 64) }},
			{name: "observation", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.Observation.ExitCode++ }},
			{name: "observation_digest", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.ObservationDigest += "x" }},
			{name: "credential", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.CredentialRequired = !r.CredentialRequired }},
			{name: "claim", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.BodyClaimID = strings.Repeat("e", 64) }},
			{name: "copy", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.CopyOwnerID = strings.Repeat("e", 64) }},
			{name: "canonical_digest", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				r.CanonicalDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("e", 64))
			}},
			{name: "materialization_digest", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				r.Materialization.EvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("e", 64))
			}},
			{name: "expiry", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.ExpiresAt = r.ExpiresAt.Add(time.Microsecond) }},
			{name: "equal_time_different_location", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				r.CreatedAt = r.CreatedAt.In(time.FixedZone("separate UTC location", 0))
			}},
			{name: "same_instant_different_offset", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.CreatedAt = r.CreatedAt.In(time.FixedZone("plus one", 3600)) }},
			{name: "empty_artifacts", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				r.Candidate.Artifacts = []attachedworkeroutput.InlineArtifactV2{}
			}},
			{name: "nil_artifacts", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.Candidate.Artifacts = nil }},
			{name: "completion_removed", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.Materialization.Completion = nil }},
			{name: "ignored_prepared_events", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				if c := r.Materialization.Completion; c != nil {
					c.PreparedEvents = &ports.CanonicalEventMaterialSetV1{}
				}
			}},
			{name: "usage", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				if c := r.Materialization.Completion; c != nil {
					c.Usage = []domain.UsageObservation{}
				}
			}},
			{name: "substrate", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				if c := r.Materialization.Completion; c != nil {
					c.SubstrateEvidence = &domain.SubstrateExecutionEvidenceV1{}
				}
			}},
			{name: "manifest_id", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				if c := r.Materialization.Completion; c != nil {
					c.Manifest.ID += "-other"
				}
			}},
			{name: "manifest_time", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				if c := r.Materialization.Completion; c != nil {
					c.Manifest.CreatedAt = c.Manifest.CreatedAt.Add(time.Microsecond)
				}
			}},
			{name: "artifact_name", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				if len(r.Candidate.Artifacts) > 0 {
					r.Candidate.Artifacts[0].Name += "-other"
				}
			}},
			{name: "display", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				if c := r.Materialization.Completion; c != nil {
					c.Events[0].DisplayText += " changed"
				}
			}},
			{name: "event_key", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				if c := r.Materialization.Completion; c != nil {
					c.Events[0].Payload.Key += "-other"
				}
			}},
			{name: "nil_events", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				if c := r.Materialization.Completion; c != nil {
					c.Events = nil
				}
			}},
			{name: "empty_events", mutate: func(r *AttachedWorkerOutputReceiptV2) {
				if c := r.Materialization.Completion; c != nil {
					c.Events = []domain.SessionEventDraft{}
				}
			}},
		} {
			t.Run(string(base.Stage)+"/"+test.name, func(t *testing.T) {
				before, err := json.Marshal(base)
				if err != nil {
					t.Fatal(err)
				}
				var other AttachedWorkerOutputReceiptV2
				if json.Unmarshal(before, &other) != nil {
					t.Fatal("comparison original roundtrip")
				}
				test.mutate(&other)
				changed, err := json.Marshal(other)
				if err != nil {
					t.Fatal(err)
				}
				if got, want := sameInlineReceiptV2(base, other), string(before) == string(changed); got != want {
					t.Fatalf("comparison changed original JSON CAS identity: got=%t want=%t", got, want)
				}
				after, err := json.Marshal(base)
				if err != nil || string(before) != string(after) {
					t.Fatal("comparison mutated original receipt", err)
				}
				otherAfter, err := json.Marshal(other)
				if err != nil || string(changed) != string(otherAfter) {
					t.Fatal("comparison mutated compared receipt", err)
				}
			})
		}
	}
}

func TestInlineReceiptComparisonIgnoresOnlyUnserializedTimeState(t *testing.T) {
	left, _, _ := inlineReceiptFixtureV2(t)
	// One opaque sample supplies monotonic representation only; it governs no
	// authority/expiry/order decision and no elapsed duration is asserted.
	left.CreatedAt = time.Now()
	right := left
	right.CreatedAt = left.CreatedAt.Round(0)
	a, ae := json.Marshal(left)
	b, be := json.Marshal(right)
	if ae != nil || be != nil || string(a) != string(b) || !sameInlineReceiptV2(left, right) {
		t.Fatal("comparison included unserialized monotonic clock representation", ae, be)
	}
}
