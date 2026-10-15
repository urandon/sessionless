package ydbstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Grammar/replay tests of the actual builder used inside admission. These do
// not execute YDB or manufacture currentness/private producer evidence.
func uploadAdmissionData(t *testing.T) AttachedWorkerOutputUploadAdmissionV1 {
	t.Helper()
	prior := uploadReceiptData(t)
	id := prior.UploadPlan.Identity
	auth := id.Binding
	auth.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("test-owned-admission-bearer"))
	observation := prior.Observation
	observation.CredentialReleaseRequired, observation.CredentialReleased = true, true
	digest, err := observation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	schema := sha256.Sum256([]byte(attachedworkeroutput.StructuredFinalSchemaV1))
	return AttachedWorkerOutputUploadAdmissionV1{Authorization: auth, SessionID: id.SessionID, HarnessBindingDigest: id.HarnessBindingDigest,
		ProfileDigest: id.ProfileDigest, SchemaDigest: hex.EncodeToString(schema[:]), FinalDigest: id.FinalDigest, AnswerDigest: id.AnswerDigest,
		ProducerDigest: id.ProducerDigest, Nonce: id.Nonce, RemainingNanoseconds: uint64(15 * time.Second), Observation: observation,
		ObservationDigest: digest, Artifacts: []attachedworkeroutput.UploadArtifactMetadataV1{prior.UploadPlan.Ordinals[0].Metadata}}
}

func TestAttemptOutputUploadAdmissionPreflight(t *testing.T) {
	request := uploadAdmissionData(t)
	if err := request.validate(); err != nil {
		t.Fatal("bounded original metadata:", err)
	}
	cases := map[string]func(*AttachedWorkerOutputUploadAdmissionV1){
		"missing revision":         func(r *AttachedWorkerOutputUploadAdmissionV1) { r.Authorization.ExpectedAttemptRevision = 0 },
		"no bearer binding":        func(r *AttachedWorkerOutputUploadAdmissionV1) { r.Authorization.PresentedSecretDigest = "" },
		"wrong schema":             func(r *AttachedWorkerOutputUploadAdmissionV1) { r.SchemaDigest = strings.Repeat("b", 64) },
		"wrong observation digest": func(r *AttachedWorkerOutputUploadAdmissionV1) { r.ObservationDigest = strings.Repeat("b", 64) },
		"not cleaned":              func(r *AttachedWorkerOutputUploadAdmissionV1) { r.Observation.CleanupSucceeded = false },
		"zero remaining":           func(r *AttachedWorkerOutputUploadAdmissionV1) { r.RemainingNanoseconds = 0 },
		"larger than cohort":       func(r *AttachedWorkerOutputUploadAdmissionV1) { r.RemainingNanoseconds++ },
		"unbounded failure code": func(r *AttachedWorkerOutputUploadAdmissionV1) {
			r.Observation.ProcessFailureCode = strings.Repeat("x", 256<<10)
		},
		"too many ordinals": func(r *AttachedWorkerOutputUploadAdmissionV1) {
			r.Artifacts = append(r.Artifacts, r.Artifacts[0], r.Artifacts[0])
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := uploadAdmissionData(t)
			mutate(&r)
			if r.validate() == nil {
				t.Fatalf("invalid admission metadata accepted: %s", name)
			}
		})
	}
	encoded, err := json.Marshal(request)
	if err != nil || string(encoded) != `"[REDACTED]"` {
		t.Fatalf("private admission serialization: %s err=%v", encoded, err)
	}
}

func TestAttemptOutputUploadAdmissionReplayRetainsSpentPlanAndCutoff(t *testing.T) {
	request := uploadAdmissionData(t)
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	plan, err := request.planAt(at, at.Add(15*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Identity.Binding.PresentedSecretDigest != "" || plan.Identity.Binding.ExpectedAttemptRevision != request.Authorization.ExpectedAttemptRevision {
		t.Fatal("plan retained secret digest or discarded original accepted revision")
	}
	spent, err := plan.SpendOrdinal(0, strings.Repeat("c", 64), at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	prior := AttachedWorkerOutputReceiptV1{Version: 1, Binding: receiptBindingWithoutBearer(request.Authorization), Nonce: request.Nonce,
		Observation: request.Observation, ObservationDigest: request.ObservationDigest, CredentialRequired: true,
		HarnessBindingDigest: request.HarnessBindingDigest, Status: domain.AttachedWorkerTerminalSucceeded, CreatedAt: at, UploadPlan: &spent}
	if err := validateUploadReceiptData(prior); err != nil {
		t.Fatal("strict admitted record:", err)
	}
	request.RemainingNanoseconds = uint64(time.Second)
	if !sameUploadAdmission(prior, request) || prior.UploadPlan.Identity.ExpiresAt != at.Add(15*time.Second) || prior.UploadPlan.Ordinals[0].State != attachedworkeroutput.UploadOrdinalWritePendingV1 {
		t.Fatal("metadata replay reset original cutoff or spent ordinal")
	}
	for _, mutate := range []func(*AttachedWorkerOutputUploadAdmissionV1){
		func(r *AttachedWorkerOutputUploadAdmissionV1) { r.Nonce = "foreign-nonce" },
		func(r *AttachedWorkerOutputUploadAdmissionV1) { r.FinalDigest = strings.Repeat("d", 64) },
		func(r *AttachedWorkerOutputUploadAdmissionV1) { r.Authorization.ExpectedAttemptRevision++ },
		func(r *AttachedWorkerOutputUploadAdmissionV1) { r.Artifacts[0].Name = "changed.txt" },
	} {
		r := uploadAdmissionData(t)
		mutate(&r)
		if sameUploadAdmission(prior, r) {
			t.Fatal("changed original metadata replayed")
		}
	}
	prior.UploadPlan.Phase = attachedworkeroutput.UploadPlanSettledAbandonedV1
	if sameUploadAdmission(prior, request) {
		t.Fatal("settled plan reopened")
	}
}

func TestAttemptOutputUploadAdmissionRetainsPinnedJobLimits(t *testing.T) {
	metadata := uploadAdmissionData(t).Artifacts
	if !uploadMetadataWithinJobLimits(metadata, domain.ProductLimits{MaxArtifacts: 1}) {
		t.Fatal("valid empty text counted as nonzero bytes")
	}
	if uploadMetadataWithinJobLimits(metadata, domain.ProductLimits{}) {
		t.Fatal("empty text did not consume an artifact ordinal")
	}
	metadata[0].Size = 32 << 10
	if !uploadMetadataWithinJobLimits(metadata, domain.ProductLimits{MaxArtifacts: 1, MaxInputBytes: 32 << 10}) ||
		uploadMetadataWithinJobLimits(metadata, domain.ProductLimits{MaxArtifacts: 1, MaxInputBytes: (32 << 10) - 1}) {
		t.Fatal("body size did not intersect original job product limit")
	}
}
