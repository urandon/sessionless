package ydbstore

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Pure record grammar fixtures: these do NOT manufacture a producer/sink
// witness, authorize a transaction, or prove native deletion serialization.
func uploadReceiptData(t *testing.T) AttachedWorkerOutputReceiptV1 {
	t.Helper()
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	digest := strings.Repeat("a", 64)
	binding := ports.AttachedWorkerSealedInputAuthorization{TenantID: "upload-tenant", OwnerUserID: "upload-owner", WorkerID: "upload-worker",
		ConnectionID: "upload-connection", EnrollmentGeneration: 2, ConnectionGeneration: 3, RunID: "upload-run", AttemptID: "upload-attempt", AttemptSequence: 1,
		LeaseID: "upload-lease", LeaseGeneration: 5, FenceToken: domain.AttachedWorkerFenceToken(digest), LeaseExpiresAtUnixMicro: at.Add(time.Minute).UnixMicro(),
		ContextDigest: domain.AttachedWorkerContextDigest(digest), CapabilityDigest: domain.AttachedWorkerCapabilityDigest(digest), PolicyDigest: domain.AttachedWorkerPolicyDigest(digest), ExpectedAttemptRevision: 7}
	plan, err := attachedworkeroutput.NewAttemptOutputUploadPlanV1(attachedworkeroutput.UploadPlanIdentityV1{Binding: binding, SessionID: "upload-session",
		HarnessBindingDigest: domain.HarnessBindingDigestV2(digest), ProfileDigest: digest, SchemaDigest: digest, FinalDigest: digest, AnswerDigest: digest, ProducerDigest: digest,
		Nonce: "upload-nonce", CreatedAt: at, ExpiresAt: at.Add(15 * time.Second)}, []attachedworkeroutput.UploadArtifactMetadataV1{
		{Ordinal: 0, Name: "empty.txt", MediaType: "text/plain", Size: 0, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", ContentMD5: "1B2M2Y8AsgTpgAmY7PhCfg=="},
	})
	if err != nil {
		t.Fatal(err)
	}
	observation := attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}
	observationDigest, err := observation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return AttachedWorkerOutputReceiptV1{Version: 1, Binding: receiptBindingWithoutBearer(binding), Nonce: plan.Identity.Nonce,
		Observation: observation, ObservationDigest: observationDigest, HarnessBindingDigest: plan.Identity.HarnessBindingDigest,
		Status: domain.AttachedWorkerTerminalSucceeded, CreatedAt: at, UploadPlan: &plan}
}

func validateUploadReceiptData(record AttachedWorkerOutputReceiptV1) error {
	a := record.Binding
	return validateAttachedWorkerReceiptRecord(record, a.TenantID, a.RunID, a.OwnerUserID, a.WorkerID, a.AttemptID, a.LeaseGeneration)
}

func TestAttemptOutputUploadReceiptStrictNoncanonicalUnion(t *testing.T) {
	record := uploadReceiptData(t)
	if err := validateUploadReceiptData(record); err != nil {
		t.Fatalf("valid active pre-Put DATA rejected: %v", err)
	}
	if attachedWorkerReceiptAllowsDeletion(record) || len(attachedWorkerReceiptObjectRefs(record)) != 0 {
		t.Fatal("active planned key treated as settled custody or existing object")
	}
	cases := map[string]func(*AttachedWorkerOutputReceiptV1){
		"false ready":          func(r *AttachedWorkerOutputReceiptV1) { r.Ready = true },
		"canonical copy owner": func(r *AttachedWorkerOutputReceiptV1) { r.CopyInProgress = true },
		"canonical digest": func(r *AttachedWorkerOutputReceiptV1) {
			r.CanonicalDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("b", 64))
		},
		"candidate identity": func(r *AttachedWorkerOutputReceiptV1) { r.CandidateFingerprint = strings.Repeat("b", 64) },
		"materialization":    func(r *AttachedWorkerOutputReceiptV1) { r.Materialization.Completion = &ports.WorkerCompletion{} },
		"planned ref": func(r *AttachedWorkerOutputReceiptV1) {
			r.SourceRefs = []domain.BlobRef{{TenantID: r.Binding.TenantID, Key: r.UploadPlan.Ordinals[0].Key}}
		},
		"nonce mismatch": func(r *AttachedWorkerOutputReceiptV1) { r.Nonce = "other-nonce" },
		"scope mismatch": func(r *AttachedWorkerOutputReceiptV1) { r.Binding.ConnectionID = "other-connection" },
		"profile mismatch": func(r *AttachedWorkerOutputReceiptV1) {
			r.HarnessBindingDigest = domain.HarnessBindingDigestV2(strings.Repeat("b", 64))
		},
		"time mismatch":       func(r *AttachedWorkerOutputReceiptV1) { r.CreatedAt = r.CreatedAt.Add(time.Microsecond) },
		"failure replacement": func(r *AttachedWorkerOutputReceiptV1) { r.Status = domain.AttachedWorkerTerminalFailed },
		"no cleanup":          func(r *AttachedWorkerOutputReceiptV1) { r.Observation.CleanupSucceeded = false },
		"bound without sources": func(r *AttachedWorkerOutputReceiptV1) {
			r.UploadPlan.Phase = attachedworkeroutput.UploadPlanReceiptBoundV1
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			candidate := uploadReceiptData(t)
			mutate(&candidate)
			if validateUploadReceiptData(candidate) == nil || attachedWorkerReceiptAllowsDeletion(candidate) {
				t.Fatalf("malformed noncanonical receipt accepted (%s)", name)
			}
		})
	}
}

func TestAttemptOutputUploadReceiptRetainsPendingAndExactSourceInventory(t *testing.T) {
	record := uploadReceiptData(t)
	spent, err := record.UploadPlan.SpendOrdinal(0, strings.Repeat("c", 64), record.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	record.UploadPlan = &spent
	if validateUploadReceiptData(record) != nil || attachedWorkerReceiptAllowsDeletion(record) {
		t.Fatal("spent/unknown source body did not retain deletion barrier")
	}
	spent.Phase = attachedworkeroutput.UploadPlanSettledAbandonedV1
	if validateUploadReceiptData(record) == nil || attachedWorkerReceiptAllowsDeletion(record) {
		t.Fatal("abandonment erased an ambiguous source body")
	}
	// Actual verification/owner evidence is a separate unimplemented service
	// obligation. These are only the resulting closed storage DATA alternatives.
	ordinal := &spent.Ordinals[0]
	ordinal.State = attachedworkeroutput.UploadOrdinalSourceVerifiedV1
	ordinal.Source = &domain.BlobRef{TenantID: record.Binding.TenantID, Key: ordinal.Key, Size: ordinal.Metadata.Size, SHA256: ordinal.Metadata.SHA256}
	if err := validateUploadReceiptData(record); err != nil || !attachedWorkerReceiptAllowsDeletion(record) {
		t.Fatalf("settled noncanonical exact-ref DATA rejected: %v", err)
	}
	refs := attachedWorkerReceiptObjectRefs(record)
	if len(refs) != 1 || refs[0] != *ordinal.Source || record.Ready {
		t.Fatalf("settled DATA lost exact ref or became canonical: refs=%#v ready=%t", refs, record.Ready)
	}
	ordinal.Source.Key += "-foreign"
	if validateUploadReceiptData(record) == nil || attachedWorkerReceiptAllowsDeletion(record) {
		t.Fatal("foreign verified ref admitted into deletion inventory")
	}
}

func TestAttemptOutputUploadReceiptPublicProjectionAndIndexBinding(t *testing.T) {
	record := uploadReceiptData(t)
	public := record.PublicReceipt()
	if public.UploadPlan != nil || record.UploadPlan == nil {
		t.Fatal("projection retained private upload metadata or changed original")
	}
	encoded, err := json.Marshal(public)
	if err != nil || strings.Contains(string(encoded), "upload_plan") {
		t.Fatalf("wire projection leaked private upload plan: err=%v", err)
	}
	a := record.Binding
	if err := validateAttachedWorkerReceiptIndex(record, a.TenantID, a.RunID, a.OwnerUserID, a.WorkerID, a.AttemptID, a.LeaseGeneration, "", "", record.CreatedAt); err != nil {
		t.Fatalf("matching pre-Put index rejected: %v", err)
	}
	for _, change := range []string{"canonical", "candidate", "time", "owner"} {
		digest, fingerprint, createdAt, owner := "", "", record.CreatedAt, a.OwnerUserID
		switch change {
		case "canonical":
			digest = strings.Repeat("a", 64)
		case "candidate":
			fingerprint = strings.Repeat("a", 64)
		case "time":
			createdAt = createdAt.Add(time.Microsecond)
		case "owner":
			owner = "foreign-owner"
		}
		if validateAttachedWorkerReceiptIndex(record, a.TenantID, a.RunID, owner, a.WorkerID, a.AttemptID, a.LeaseGeneration, digest, fingerprint, createdAt) == nil {
			t.Errorf("index/payload mismatch %s accepted", change)
		}
	}
}

func TestAttemptOutputUploadReceiptBoundExactCandidateReplay(t *testing.T) {
	record := uploadReceiptData(t)
	ordinal := &record.UploadPlan.Ordinals[0]
	ordinal.State = attachedworkeroutput.UploadOrdinalSourceVerifiedV1
	ordinal.GrantDigest = strings.Repeat("d", 64)
	ordinal.Source = &domain.BlobRef{TenantID: record.Binding.TenantID, Key: ordinal.Key, Size: ordinal.Metadata.Size, SHA256: ordinal.Metadata.SHA256}
	record.UploadPlan.Phase = attachedworkeroutput.UploadPlanReceiptBoundV1
	record.SourceRefs = []domain.BlobRef{*ordinal.Source}
	request := AttachedWorkerOutputReceiptRequest{Authorization: record.Binding, Nonce: record.Nonce, Observation: record.Observation,
		Candidate: attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: "answer", Artifacts: []attachedworkeroutput.ArtifactCandidate{
			{Name: ordinal.Metadata.Name, MediaType: ordinal.Metadata.MediaType, Source: *ordinal.Source},
		}}}
	var err error
	record.CandidateFingerprint, err = attachedworkeroutput.CandidateFingerprint(request.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	record.Materialization = ports.AttachedWorkerTerminalMaterialization{Completion: &ports.WorkerCompletion{
		Manifest: domain.ArtifactManifest{ID: "upload-canonical-manifest"},
		Events:   []domain.SessionEventDraft{{ID: "upload-canonical-event", Kind: domain.SessionEventAssistantMessage, IdempotencyKey: "upload-canonical-key", DisplayText: "answer"}},
	}}
	record.CanonicalDigest, err = attachedWorkerTerminalMaterializationDigest(record.Status, record.Materialization)
	if err != nil {
		t.Fatal(err)
	}
	record.Materialization.EvidenceDigest = record.CanonicalDigest
	// This checks the exact candidate decision used by the real transaction,
	// with a strict-valid bound canonical storage fixture. It is not native YDB,
	// actual object existence or authenticated end-to-end receipt proof.
	for _, ready := range []bool{false, true} {
		record.Ready = ready
		if err := validateUploadReceiptData(record); err != nil {
			t.Fatalf("strict bound record ready=%t: %v", ready, err)
		}
		if !sameOutputReceiptCandidate(record, request, record.CandidateFingerprint, record.ObservationDigest, record.HarnessBindingDigest) {
			t.Fatalf("exact receipt_bound candidate rejected at real comparison gate (ready=%t)", ready)
		}
		other := request
		other.Nonce = "divergent-nonce"
		if sameOutputReceiptCandidate(record, other, record.CandidateFingerprint, record.ObservationDigest, record.HarnessBindingDigest) {
			t.Fatalf("divergent bound candidate accepted (ready=%t)", ready)
		}
	}
	for _, phase := range []attachedworkeroutput.UploadPlanPhaseV1{attachedworkeroutput.UploadPlanActiveV1, attachedworkeroutput.UploadPlanSettledAbandonedV1} {
		record.UploadPlan.Phase = phase
		if sameOutputReceiptCandidate(record, request, record.CandidateFingerprint, record.ObservationDigest, record.HarnessBindingDigest) {
			t.Fatalf("ordinary Create bypassed %s plan", phase)
		}
	}
	record.UploadPlan = nil
	if !sameOutputReceiptCandidate(record, request, record.CandidateFingerprint, record.ObservationDigest, record.HarnessBindingDigest) {
		t.Fatal("existing canonical no-upload exact candidate replay changed")
	}
}
