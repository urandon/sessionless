package ydbstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestInlineWireV2PersistedFingerprintGolden(t *testing.T) {
	accepted := time.Date(2026, 10, 23, 8, 9, 10, 123456789, time.UTC)
	auth := ports.AttachedWorkerSealedInputAuthorization{TenantID: "wire-tenant", OwnerUserID: "wire-owner", WorkerID: "wire-worker", ConnectionID: "wire-connection", PresentedSecretDigest: "private-not-hashed", EnrollmentGeneration: 2, ConnectionGeneration: 3, RunID: "wire-run", AttemptID: "wire-attempt", AttemptSequence: 1, LeaseID: "wire-lease", LeaseGeneration: 4, FenceToken: domain.AttachedWorkerFenceToken(strings.Repeat("a", 64)), LeaseExpiresAtUnixMicro: accepted.Add(time.Minute).UnixMicro(), ContextDigest: domain.AttachedWorkerContextDigest(strings.Repeat("b", 64)), CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("c", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("d", 64)), ExpectedAttemptRevision: 99}
	request := attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: auth, Nonce: "wire-nonce", Candidate: attachedworkeroutput.InlineCandidateV2{Summary: "Original <answer>\nёж", Artifacts: []attachedworkeroutput.InlineArtifactV2{{Name: "report.json", MediaType: "application/json", Size: 19, SHA256: strings.Repeat("e", 64)}}}, Producer: attachedworkeroutput.InlineProducerBindingV2{SessionID: "wire-session", HarnessBindingDigest: domain.HarnessBindingDigestV2(strings.Repeat("f", 64)), ReservationID: "wire-reservation", AcceptedAt: accepted, SchemaDigest: strings.Repeat("1", 64), FinalDigest: strings.Repeat("2", 64), AnswerDigest: strings.Repeat("3", 64)}, Observation: attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true, CredentialReleaseRequired: true, CredentialReleased: true}, ExpiresAt: accepted.Add(12 * time.Second)}
	got, err := attachedworkeroutput.InlineOutputRequestFingerprintV2(request)
	if err != nil {
		t.Fatalf("original pure request fingerprint: %v", err)
	}
	const golden = "5ef970779c1f4288003dcba05b48ecf393a665c86d76376a940f866dc3b95bc3"
	if got != golden {
		t.Fatalf("relocated request fingerprint=%s want original persisted vector=%s", got, golden)
	}
	observation, err := request.Observation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	record := AttachedWorkerOutputReceiptV2{Binding: receiptBindingWithoutBearer(auth), Nonce: request.Nonce, Candidate: request.Candidate, Producer: request.Producer, ObservationDigest: observation, CredentialRequired: true, ExpiresAt: request.ExpiresAt}
	persisted, err := inlineReceiptPlanFingerprintV2(record)
	if err != nil || persisted != got {
		t.Fatalf("persisted=%s request=%s err=%v", persisted, got, err)
	}
	request.Authorization.PresentedSecretDigest = "different-private-not-hashed"
	request.Authorization.ExpectedAttemptRevision = 100
	withoutSecret, err := attachedworkeroutput.InlineOutputRequestFingerprintV2(request)
	if err != nil || withoutSecret != got || auth.PresentedSecretDigest != "private-not-hashed" || auth.ExpectedAttemptRevision != 99 {
		t.Fatalf("bearer/revision omission or value-copy invariant failed: hash=%s original=%s err=%v", withoutSecret, got, err)
	}
	request.Authorization.RunID = "different-wire-run"
	changed, err := attachedworkeroutput.InlineOutputRequestFingerprintV2(request)
	if err != nil || changed == got {
		t.Fatalf("changed stable scope hash=%s original=%s err=%v", changed, got, err)
	}
}

func TestInlineReceiptStreamedFingerprintsPreserveOriginalBytes(t *testing.T) {
	for _, name := range []string{"prepared", "canonical", "maximum", "maximum_artifacts", "whitespace", "escaping", "invalid_utf8", "invalid_time"} {
		t.Run(name, func(t *testing.T) {
			r := inlineCanonicalFixtureV2(t)
			switch name {
			case "prepared":
				r, _, _ = inlineReceiptFixtureV2(t)
			case "maximum":
				r = maximumInlineIdentityFixtureV2(t)
			case "maximum_artifacts":
				r = maximumInlineArtifactSQLFixtureV2(t)
			case "whitespace":
				r.Candidate.Summary = " \tOriginal answer\n"
			case "escaping":
				r.Candidate.Summary = "<>&\\\"\b\f\n\r\t\x00\x1f\u2028\u2029🙂"
			case "invalid_utf8":
				r.Candidate.Summary = "x\xff\xc0\xaf\xed\xa0\x80y"
			case "invalid_time":
				r.ExpiresAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			}
			// Independent original Marshal shape, not the new projected helper.
			raw, wantErr := json.Marshal(struct {
				Binding            ports.AttachedWorkerSealedInputAuthorization
				Nonce              domain.IdempotencyKey
				Candidate          attachedworkeroutput.InlineCandidateV2
				Producer           attachedworkeroutput.InlineProducerBindingV2
				ObservationDigest  string
				CredentialRequired bool
				ExpiresAt          time.Time
			}{r.Binding, r.Nonce, r.Candidate, r.Producer, r.ObservationDigest, r.CredentialRequired, r.ExpiresAt})
			defer clear(raw)
			digest := sha256.New()
			_, _ = digest.Write([]byte("sessionless:attached-worker-inline-plan:v2\x00"))
			_, _ = digest.Write(raw)
			got, err := inlineReceiptPlanFingerprintV2(r)
			if (err == nil) != (wantErr == nil) || err == nil && got != hex.EncodeToString(digest.Sum(nil)) {
				t.Fatalf("original plan hash bytes changed: got=%s err=%v wantErr=%v", got, err, wantErr)
			}
			if r.Materialization.Completion != nil {
				original := r.Materialization.Completion.Events[0].DisplayText
				r.Materialization.Completion.Events[0].DisplayText = r.Candidate.Summary
				want, wantErr := attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalSucceeded, r.Materialization)
				got, err := inlineReceiptCanonicalDigestV2(r.Materialization)
				if (err == nil) != (wantErr == nil) || err == nil && got != want {
					t.Fatalf("original canonical hash bytes changed: got=%s want=%s err=%v wantErr=%v", got, want, err, wantErr)
				}
				r.Materialization.Completion.Events[0].DisplayText = original
			}
		})
	}
}

func TestInlineReceiptCanonicalBuilderPreservesV1JSON(t *testing.T) {
	r := inlineCanonicalFixtureV2(t)
	for _, status := range []domain.RunStatus{domain.RunSucceeded, domain.RunFailed, domain.RunCancelled} {
		for _, eventCase := range []string{"nil", "empty", "original", "empty_display"} {
			t.Run(string(status)+"/"+eventCase, func(t *testing.T) {
				events := r.Materialization.Completion.Events
				switch eventCase {
				case "nil":
					events = nil
				case "empty":
					events = []domain.SessionEventDraft{}
				case "empty_display":
					events = append([]domain.SessionEventDraft(nil), events...)
					events[0].DisplayText = ""
				}
				identities := make([]finalizationEventIdentity, 0, len(events))
				for _, e := range events {
					identities = append(identities, finalizationEventIdentity{ID: e.ID, Kind: e.Kind, IdempotencyKey: e.IdempotencyKey, Payload: e.Payload, DisplayText: e.DisplayText})
				}
				for _, include := range []bool{false, true} {
					var manifest *domain.ArtifactManifest
					var substrate *domain.SubstrateExecutionEvidenceV1
					var reconciliation *domain.AttemptEffectReconciliationEvidenceV1
					if include {
						manifest = &r.Materialization.Completion.Manifest
						substrate, reconciliation = new(domain.SubstrateExecutionEvidenceV1), new(domain.AttemptEffectReconciliationEvidenceV1)
					}
					// Frozen original field order/tags, independent of the builder.
					raw, err := json.Marshal(struct {
						Status                       domain.RunStatus                                    `json:"status"`
						Manifest                     *domain.ArtifactManifest                            `json:"manifest,omitempty"`
						Events                       []finalizationEventIdentity                         `json:"events"`
						SubstrateEvidenceDigest      *domain.SubstrateExecutionEvidenceDigestV1          `json:"substrate_evidence_digest,omitempty"`
						ReconciliationEvidenceDigest *domain.AttemptEffectReconciliationEvidenceDigestV1 `json:"reconciliation_evidence_digest,omitempty"`
					}{status, manifest, identities, substrateEvidenceDigest(substrate), reconciliationEvidenceDigest(reconciliation)})
					if err != nil {
						t.Fatal(err)
					}
					want := sha256.Sum256(raw)
					got, err := runFinalizationDigest(status, manifest, events, substrate, reconciliation)
					if err != nil || got != hex.EncodeToString(want[:]) {
						t.Fatalf("canonical V1 JSON changed: include=%t err=%v", include, err)
					}
				}
			})
		}
	}
}

func inlineReceiptFixtureV2(t *testing.T) (AttachedWorkerOutputReceiptV2, ports.WorkerJobState, []byte) {
	t.Helper()
	loaded := currentWorkerAuthorityFixture(t)
	placement := domain.ExecutionPlacementV2{Version: 2, Kind: domain.ExecutionPlacementAttachedWorker, FallbackPolicy: domain.ExecutionFallbackDenied,
		OwnerUserID: loaded.Job.CredentialOwnerUserID, WorkerID: "worker-test", CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("inline-capability")),
		PolicyDigest: domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("inline-policy")))}
	pd, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	loaded.Job.ExecutionPlacementV2 = placement
	loaded.Job.Limits.MaxToolEvents, loaded.Job.Limits.MaxToolEventBytes = 8, 4096
	loaded.Job.Limits.MaxContextEvents = 16
	loaded.Job.HarnessBinding.ExecutionPlacementDigest = string(pd)
	loaded.Job.SubstrateBinding, loaded.Job.AdmissionCostCeiling = nil, nil
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(loaded.Job, loaded.InputManifest)
	if err != nil {
		t.Fatal(err)
	}
	harness, err := loaded.Job.HarnessBinding.Digest()
	if err != nil {
		t.Fatal(err)
	}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(loaded.Run.TenantID, placement.OwnerUserID, placement.WorkerID, loaded.Run.ID, loaded.Job.AttemptID, "inline-lease", 1)
	if err != nil {
		t.Fatal(err)
	}
	auth := ports.AttachedWorkerSealedInputAuthorization{TenantID: loaded.Run.TenantID, OwnerUserID: placement.OwnerUserID, WorkerID: placement.WorkerID,
		ConnectionID: "connection-test", EnrollmentGeneration: 1, ConnectionGeneration: 1, RunID: loaded.Run.ID, AttemptID: loaded.Job.AttemptID,
		AttemptSequence: 1, LeaseID: "inline-lease", LeaseGeneration: 1, FenceToken: fence, LeaseExpiresAtUnixMicro: loaded.Run.CreatedAt.Add(time.Minute).UnixMicro(),
		ContextDigest: contextDigest, CapabilityDigest: placement.CapabilityDigest, PolicyDigest: placement.PolicyDigest}
	content := []byte(" {\"number\":9007199254740993123456789,\"value\":1e999} \n")
	hash := sha256.Sum256(content)
	answer := sha256.Sum256([]byte("Useful answer"))
	observation := attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}
	od, err := observation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := AttachedWorkerOutputReceiptV2{Version: 2, Stage: attachedworkeroutput.InlinePreparedV2, Binding: auth, Nonce: "inline-nonce",
		Candidate: attachedworkeroutput.InlineCandidateV2{Summary: "Useful answer", Artifacts: []attachedworkeroutput.InlineArtifactV2{{Name: "report.json", MediaType: "application/json", Size: uint64(len(content)), SHA256: hex.EncodeToString(hash[:])}}},
		Producer: attachedworkeroutput.InlineProducerBindingV2{SessionID: loaded.Run.SessionID, HarnessBindingDigest: harness, ReservationID: loaded.Job.ReservationID, AcceptedAt: loaded.Run.CreatedAt,
			SchemaDigest: strings.Repeat("a", 64), FinalDigest: strings.Repeat("b", 64), AnswerDigest: hex.EncodeToString(answer[:])}, Observation: observation, ObservationDigest: od,
		CreatedAt: loaded.Run.CreatedAt.Add(time.Second), ExpiresAt: loaded.Run.CreatedAt.Add(6 * time.Second)}
	r.CandidateFingerprint, err = inlineReceiptPlanFingerprintV2(r)
	if err != nil {
		t.Fatal(err)
	}
	return r, loaded, content
}

func validateInlineFixtureV2(r AttachedWorkerOutputReceiptV2) error {
	a := r.Binding
	return (attachedWorkerReceiptEnvelope{v2: &r}).validateIdentity(a.TenantID, a.RunID, a.OwnerUserID, a.WorkerID, a.AttemptID, a.LeaseGeneration)
}

func inlineCanonicalFixtureV2(t *testing.T) AttachedWorkerOutputReceiptV2 {
	t.Helper()
	r, loaded, body := inlineReceiptFixtureV2(t)
	verified, err := attachedworkeroutput.VerifyInlineV2(r.Candidate, body)
	if err != nil {
		t.Fatal(err)
	}
	defer verified.Clear()
	blobs := &localEntryBlobs{}
	t.Cleanup(func() {
		for _, body := range blobs.objects {
			clear(body)
		}
	})
	material, err := attachedworkeroutput.MaterializeInlineV2(t.Context(), blobs, loaded, r.Binding.LeaseID, r.Binding.LeaseGeneration, verified, r.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalSucceeded, material)
	if err != nil {
		t.Fatal(err)
	}
	material.EvidenceDigest = digest
	r.Stage, r.BodyClaimID, r.CopyOwnerID = attachedworkeroutput.InlineCanonicalCopyingV2, strings.Repeat("c", 64), strings.Repeat("d", 64)
	r.CanonicalDigest, r.Materialization = digest, material
	return r
}

func marshalInlineStoredReceiptTestV2(t *testing.T, r AttachedWorkerOutputReceiptV2) []byte {
	t.Helper()
	stored, err := encodeInlineReceiptProjectionV2(r)
	if err != nil {
		t.Fatal("invalid original receipt for private storage fixture", err)
	}
	raw, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestInlineReceiptStagesKeepCanonicalOwnershipDistinct(t *testing.T) {
	prepared, _, _ := inlineReceiptFixtureV2(t)
	claimed := prepared
	claimed.Stage, claimed.BodyClaimID = attachedworkeroutput.InlineBodyClaimedV2, strings.Repeat("c", 64)
	retired := claimed
	retired.Stage = attachedworkeroutput.InlineRetiredWithoutWritesV2
	copying := inlineCanonicalFixtureV2(t)
	ready := copying
	ready.Stage = attachedworkeroutput.InlineReadyV2
	for _, r := range []AttachedWorkerOutputReceiptV2{prepared, claimed, copying, ready, retired} {
		t.Run(string(r.Stage), func(t *testing.T) {
			if err := validateInlineFixtureV2(r); err != nil {
				t.Fatalf("valid %s stage rejected: %v", r.Stage, err)
			}
			raw := marshalInlineStoredReceiptTestV2(t, r)
			envelope, err := decodeAttachedWorkerReceiptEnvelope(raw)
			if err != nil || envelope.v2 == nil || envelope.v1 != nil {
				t.Fatalf("V2 confused with V1: err=%v", err)
			}
			if envelope.ready() != (r.Stage == attachedworkeroutput.InlineReadyV2) || envelope.deletionSettled() != (r.Stage == attachedworkeroutput.InlineReadyV2 || r.Stage == attachedworkeroutput.InlineRetiredWithoutWritesV2) {
				t.Error("stage changed publication/deletion permission")
			}
			canonical, err := envelope.canonical()
			if r.Stage == attachedworkeroutput.InlineReadyV2 {
				if err != nil || canonical.CanonicalDigest != r.CanonicalDigest || len(envelope.objectRefs()) != 2 {
					t.Errorf("READY lost canonical result: %v", err)
				}
			} else if !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
				t.Errorf("nonREADY acquired canonical publisher view: %v", err)
			}
			if r.Stage == attachedworkeroutput.InlineCanonicalCopyingV2 && len(envelope.objectRefs()) != 2 {
				t.Error("ambiguous copying lost planned artifact/event inventory")
			}
			if (r.Stage == attachedworkeroutput.InlinePreparedV2 || r.Stage == attachedworkeroutput.InlineBodyClaimedV2 || r.Stage == attachedworkeroutput.InlineRetiredWithoutWritesV2) && len(envelope.objectRefs()) != 0 {
				t.Error("precopy acquired fictional storage refs")
			}
		})
	}
}

func TestInlineReceiptRejectsMalformedStageAndPlan(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*AttachedWorkerOutputReceiptV2)
	}{
		{"unknown stage", func(r *AttachedWorkerOutputReceiptV2) { r.Stage = "future" }},
		{"unknown version", func(r *AttachedWorkerOutputReceiptV2) { r.Version = 3 }},
		{"body identity without claim", func(r *AttachedWorkerOutputReceiptV2) { r.BodyClaimID = strings.Repeat("c", 64) }},
		{"copy owner before plan", func(r *AttachedWorkerOutputReceiptV2) { r.CopyOwnerID = strings.Repeat("d", 64) }},
		{"fake prebody digest", func(r *AttachedWorkerOutputReceiptV2) {
			r.CanonicalDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("a", 64))
		}},
		{"late expiry", func(r *AttachedWorkerOutputReceiptV2) { r.ExpiresAt = r.CreatedAt.Add(16 * time.Second) }},
		{"answer substituted", func(r *AttachedWorkerOutputReceiptV2) { r.Candidate.Summary = "substituted" }},
		{"schema substituted", func(r *AttachedWorkerOutputReceiptV2) { r.Producer.SchemaDigest = strings.Repeat("f", 64) }},
		{"bearer persisted", func(r *AttachedWorkerOutputReceiptV2) {
			r.Binding.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("secret"))
		}},
		{"revision persisted", func(r *AttachedWorkerOutputReceiptV2) { r.Binding.ExpectedAttemptRevision = 9 }},
		{"retired fake plan", func(r *AttachedWorkerOutputReceiptV2) {
			r.Stage = attachedworkeroutput.InlineRetiredWithoutWritesV2
			r.CopyOwnerID = strings.Repeat("d", 64)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _, _ := inlineReceiptFixtureV2(t)
			tc.change(&r)
			if err := validateInlineFixtureV2(r); !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
				t.Errorf("malformed receipt accepted: %v", err)
			}
		})
	}
}

func TestInlineReceiptCanonicalPlanBindsNamesBytesAndScope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*AttachedWorkerOutputReceiptV2)
	}{
		{"wrong canonical owner", func(r *AttachedWorkerOutputReceiptV2) { r.Materialization.Completion.TenantID = "foreign" }},
		{"wrong canonical attempt", func(r *AttachedWorkerOutputReceiptV2) { r.Materialization.Completion.AttemptID = "foreign" }},
		{"wrong canonical fence", func(r *AttachedWorkerOutputReceiptV2) { r.Materialization.Completion.Fence++ }},
		{"name substitution", func(r *AttachedWorkerOutputReceiptV2) {
			r.Materialization.Completion.Manifest.Artifacts[0].Name = "other.json"
		}},
		{"source object instead of canonical", func(r *AttachedWorkerOutputReceiptV2) {
			r.Materialization.Completion.Manifest.Artifacts[0].Blob.Key = "source/object"
		}},
		{"answer substitution", func(r *AttachedWorkerOutputReceiptV2) {
			r.Materialization.Completion.Events[0].DisplayText = "other answer"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := inlineCanonicalFixtureV2(t)
			tc.change(&r)
			// Re-sealing a contrary canonical DTO must not bless a wrong plan.
			digest, err := attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalSucceeded, r.Materialization)
			if err != nil {
				t.Fatal(err)
			}
			r.CanonicalDigest, r.Materialization.EvidenceDigest = digest, digest
			if err := validateInlineFixtureV2(r); !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
				t.Errorf("contrary canonical plan accepted: %v", err)
			}
		})
	}
}

func TestInlineReceiptVersionAndCapabilityDoNotAliasV1(t *testing.T) {
	r, _, _ := inlineReceiptFixtureV2(t)
	raw := marshalInlineStoredReceiptTestV2(t, r)
	for _, bad := range []string{`{"version":0}`, `{"version":99}`, strings.Replace(string(raw), `"version":2`, `"version":2,"Version":1`, 1), string(raw[:len(raw)-1]) + `,"ready":true}`} {
		if _, err := decodeAttachedWorkerReceiptEnvelope([]byte(bad)); !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
			t.Errorf("unknown/aliased wire accepted: %v", err)
		}
	}
	auth := r.Binding
	auth.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("actual bearer"))
	cap := inlineOutputCapabilityV2(auth, r)
	if len(cap) != 64 || strings.Contains(cap, "actual bearer") {
		t.Error("invalid compact capability")
	}
	changed := r
	changed.CandidateFingerprint = strings.Repeat("f", 64)
	if inlineOutputCapabilityV2(auth, changed) == cap {
		t.Error("capability did not bind exact immutable plan")
	}
	other := auth
	other.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("other bearer"))
	if inlineOutputCapabilityV2(other, r) == cap {
		t.Error("capability survived bearer substitution")
	}
}

func TestInlineReceiptPrivateFramingDoesNotWidenV1(t *testing.T) {
	// Dispatch alone accepts V1's historical noncanonical document; callers
	// still validate identity. The V2-only allowance must not widen that reader.
	const original = `{"version":1}`
	raw := []byte(original + strings.Repeat(" ", maxInlineReceiptRecordBytesV2-len(original)))
	value, err := decodeAttachedWorkerReceiptEnvelope(raw)
	if err != nil || value.v1 == nil || value.v2 != nil {
		t.Fatal("V1 original lexical boundary changed", err)
	}
	for _, extra := range []int{1, 42, 43} {
		if got, err := decodeAttachedWorkerReceiptEnvelope(append(raw, []byte(strings.Repeat(" ", extra))...)); !errors.Is(err, ErrAttachedWorkerAttemptConflict) || got.v1 != nil || got.v2 != nil {
			t.Fatalf("V2 private frame widened V1: extra=%d err=%v", extra, err)
		}
	}
}

func TestInlineReceiptDeletionScalarProjectionV2(t *testing.T) {
	r, _, _ := inlineReceiptFixtureV2(t)
	for _, stage := range []attachedworkeroutput.AttachedWorkerInlineStageV2{attachedworkeroutput.InlinePreparedV2, attachedworkeroutput.InlineBodyClaimedV2, attachedworkeroutput.InlineRetiredWithoutWritesV2, attachedworkeroutput.InlineCanonicalCopyingV2, attachedworkeroutput.InlineReadyV2} {
		t.Run(string(stage), func(t *testing.T) {
			candidate := r
			if stage == attachedworkeroutput.InlineCanonicalCopyingV2 || stage == attachedworkeroutput.InlineReadyV2 {
				candidate = inlineCanonicalFixtureV2(t)
			} else if stage != attachedworkeroutput.InlinePreparedV2 {
				candidate.BodyClaimID = strings.Repeat("c", 64)
			}
			candidate.Stage = stage
			raw := marshalInlineStoredReceiptTestV2(t, candidate)
			a := candidate.Binding
			cells := map[string]deletionCaptureCell{
				"tenant_id": deletionCaptureTestCell(t, "s", a.TenantID), "run_id": deletionCaptureTestCell(t, "s", a.RunID),
				"owner_user_id": deletionCaptureTestCell(t, "s", a.OwnerUserID), "worker_id": deletionCaptureTestCell(t, "s", a.WorkerID),
				"attempt_id": deletionCaptureTestCell(t, "s", a.AttemptID), "lease_generation": deletionCaptureTestCell(t, "u", a.LeaseGeneration),
				"canonical_digest":      deletionCaptureTestCell(t, "s", candidate.CanonicalDigest),
				"candidate_fingerprint": deletionCaptureTestCell(t, "s", candidate.CandidateFingerprint),
				"created_at":            deletionCaptureTestCell(t, "t", candidate.CreatedAt),
			}
			if err := validateDeletionCaptureScalars("attached_worker_output_receipts", cells, raw); err != nil {
				t.Fatalf("V2 scalar projection stage=%s: %v", stage, err)
			}
			for key, cell := range cells {
				original := cell
				if key == "created_at" {
					cell = deletionCaptureTestCell(t, "t", candidate.CreatedAt.Add(time.Second))
				} else if key == "lease_generation" {
					cell = deletionCaptureTestCell(t, "u", a.LeaseGeneration+1)
				} else {
					cell = deletionCaptureTestCell(t, "s", "substituted")
				}
				cells[key] = cell
				if err := validateDeletionCaptureScalars("attached_worker_output_receipts", cells, raw); err == nil {
					t.Errorf("stage=%s accepted substituted scalar %s", stage, key)
				}
				delete(cells, key)
				if err := validateDeletionCaptureScalars("attached_worker_output_receipts", cells, raw); err == nil {
					t.Errorf("stage=%s accepted missing scalar %s", stage, key)
				}
				cells[key] = original
			}
		})
	}
}

func TestInlineReceiptFreshPhaseRejectsCanonicalCancellation(t *testing.T) {
	r, loaded, _ := inlineReceiptFixtureV2(t)
	at := r.CreatedAt
	if err := validateInlineLoadedJobPhaseV2(loaded, r, at, false); err != nil {
		t.Fatalf("uncancelled original job rejected: %v", err)
	}
	loaded.Run.CancellationRequestedAt = &at
	if err := validateInlineLoadedJobPhaseV2(loaded, r, at, false); !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
		t.Fatalf("fresh phase accepted canonical cancellation before attached-state projection: %v", err)
	}
}

func TestInlineReceiptReadyRecoveryKeepsOriginalEvidenceTime(t *testing.T) {
	r, loaded, _ := inlineReceiptFixtureV2(t)
	binding, _ := computeChoiceEvidenceFixture(t, r.CreatedAt, computeChoiceAttachedPin{
		tenantID: loaded.Run.TenantID, ownerUserID: loaded.Job.CredentialOwnerUserID, subscriptionID: loaded.Run.SubscriptionConnectionID,
	})
	binding.RunID, binding.AttemptID = loaded.Run.ID, loaded.Job.AttemptID
	binding.ExecutionPlacementDigest = loaded.Job.HarnessBinding.ExecutionPlacementDigest
	loaded.Job.HarnessBinding = binding
	if err := validateLoadedWorkerJob(loaded); err != nil {
		t.Fatalf("original subscription aggregate invalid: %v", err)
	}
	if err := binding.ValidateAt(r.CreatedAt); err != nil {
		t.Fatalf("original subscription evidence invalid at receipt creation: %v", err)
	}
	expiry := *binding.EvidenceExpiresAt
	// There is no clock race: the accepted and later times are explicit.
	later := expiry.Add(time.Hour)
	if err := validateInlineLoadedJobPhaseV2(loaded, r, later, false); !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
		t.Fatalf("fresh export accepted expired evidence: %v", err)
	}
	if err := validateInlineLoadedJobPhaseV2(loaded, r, later, true); err != nil {
		t.Fatalf("immutable READY metadata recovery required fresh execution evidence: %v", err)
	}
}

func TestInlineReceiptReadyMetadataDoesNotRenewExpiredLease(t *testing.T) {
	r := inlineCanonicalFixtureV2(t)
	a := domain.AttachedWorkerAttemptV1{State: domain.AttachedWorkerAttemptClaimed, LeaseExpiresAt: r.ExpiresAt}
	if attachedWorkerOutputReceiptStatusAdmissible(a, domain.AttachedWorkerTerminalSucceeded, r.ExpiresAt.Add(time.Hour)) {
		t.Fatal("expired lease unexpectedly granted fresh terminal output authority")
	}
	if !inlineReadyAttemptMetadataV2(a, r.CanonicalDigest) {
		t.Fatal("already READY metadata hidden after original lease expiry")
	}
	a.State, a.TerminalStatus = domain.AttachedWorkerAttemptTerminalPending, domain.AttachedWorkerTerminalSucceeded
	a.TerminalEvidenceDigest = r.CanonicalDigest
	if !inlineReadyAttemptMetadataV2(a, r.CanonicalDigest) {
		t.Fatal("exact pending terminal receipt metadata rejected")
	}
	a.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("f", 64))
	if inlineReadyAttemptMetadataV2(a, r.CanonicalDigest) {
		t.Fatal("conflicting terminal digest accepted as original READY metadata")
	}
}
