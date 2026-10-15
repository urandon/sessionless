package attachedworkeroutput

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func uploadTestDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func uploadTestMetadata(ordinal uint32, name, mediaType, body string) UploadArtifactMetadataV1 {
	md5sum := md5.Sum([]byte(body))
	return UploadArtifactMetadataV1{Ordinal: ordinal, Name: name, MediaType: mediaType, Size: int64(len(body)),
		SHA256: uploadTestDigest(body), ContentMD5: base64.StdEncoding.EncodeToString(md5sum[:])}
}

func uploadTestIdentity() UploadPlanIdentityV1 {
	at := time.Date(2026, 10, 15, 11, 0, 0, 0, time.UTC)
	return UploadPlanIdentityV1{
		Binding: ports.AttachedWorkerSealedInputAuthorization{
			TenantID: "upload-tenant", OwnerUserID: "upload-owner", WorkerID: "upload-worker", ConnectionID: "upload-connection",
			EnrollmentGeneration: 3, ConnectionGeneration: 5, RunID: "upload-run", AttemptID: "upload-attempt", AttemptSequence: 1,
			LeaseID: "upload-lease", LeaseGeneration: 7, FenceToken: domain.AttachedWorkerFenceToken(uploadTestDigest("opaque fence")),
			LeaseExpiresAtUnixMicro: at.Add(time.Minute).UnixMicro(), ContextDigest: domain.AttachedWorkerContextDigest(uploadTestDigest("context")),
			CapabilityDigest: domain.AttachedWorkerCapabilityDigest(uploadTestDigest("capability")), PolicyDigest: domain.AttachedWorkerPolicyDigest(uploadTestDigest("policy")),
			ExpectedAttemptRevision: 11,
		},
		SessionID: "upload-session", HarnessBindingDigest: domain.HarnessBindingDigestV2(uploadTestDigest("harness")),
		ProfileDigest: uploadTestDigest("profile"), SchemaDigest: uploadTestDigest(StructuredFinalSchemaV1),
		FinalDigest: uploadTestDigest("provider final"), AnswerDigest: uploadTestDigest("answer"), ProducerDigest: uploadTestDigest("original invocation"),
		Nonce: "upload-nonce", CreatedAt: at, ExpiresAt: at.Add(AttemptOutputUploadMaxDuration),
	}
}

func uploadTestPlan(t *testing.T) AttemptOutputUploadPlanV1 {
	t.Helper()
	plan, err := NewAttemptOutputUploadPlanV1(uploadTestIdentity(), []UploadArtifactMetadataV1{
		uploadTestMetadata(0, "empty.txt", "text/plain", ""),
		uploadTestMetadata(1, "data.json", "application/json", `{ "n": 900719925474099312345 }`),
	})
	if err != nil {
		t.Fatalf("construct bounded two-artifact upload DATA: %v", err)
	}
	return plan
}

func TestAttemptOutputUploadPlanPreservesEmptyAndMaxArtifacts(t *testing.T) {
	plan := uploadTestPlan(t)
	if err := plan.Validate(); err != nil {
		t.Fatalf("validate declared empty text plus JSON metadata: %v", err)
	}
	wantPrefix := domain.SessionRunObjectPrefix(plan.Identity.Binding.TenantID, plan.Identity.SessionID, plan.Identity.Binding.RunID) +
		"attached-worker/candidates/upload-attempt/" + plan.PlanDigest + "/"
	if plan.Ordinals[0].Key != wantPrefix+"0" || plan.Ordinals[1].Key != wantPrefix+"1" || plan.Ordinals[0].Metadata.Size != 0 {
		t.Fatalf("server-derived destinations/empty text differ: %#v", plan.Ordinals)
	}
	maxBody := strings.Repeat("x", int(AttemptOutputUploadMaxArtifactBytes))
	metadata := []UploadArtifactMetadataV1{uploadTestMetadata(0, "one.txt", "text/plain", maxBody), uploadTestMetadata(1, "two.txt", "text/plain", maxBody)}
	maximum, err := NewAttemptOutputUploadPlanV1(uploadTestIdentity(), metadata)
	if err != nil || maximum.Validate() != nil {
		t.Fatalf("maximum two32KiB/64KiB valid metadata rejected: construct=%v", err)
	}
	metadata[0].Name = "changed.txt"
	if maximum.Ordinals[0].Metadata.Name != "one.txt" {
		t.Fatal("plan retained mutable caller metadata slice")
	}
}

func TestNewAttemptOutputUploadPlanRejectsInvalidMetadataBeforeSealing(t *testing.T) {
	cases := map[string]func(*UploadArtifactMetadataV1){
		"ordinal":            func(a *UploadArtifactMetadataV1) { a.Ordinal = 1 },
		"path name":          func(a *UploadArtifactMetadataV1) { a.Name = "../output" },
		"name bytes":         func(a *UploadArtifactMetadataV1) { a.Name = strings.Repeat("n", 256) },
		"invalid utf8":       func(a *UploadArtifactMetadataV1) { a.Name = string([]byte{0xff}) },
		"control":            func(a *UploadArtifactMetadataV1) { a.Name = "output\n" },
		"binary":             func(a *UploadArtifactMetadataV1) { a.MediaType = "image/png" },
		"empty json":         func(a *UploadArtifactMetadataV1) { a.MediaType = "application/json" },
		"negative size":      func(a *UploadArtifactMetadataV1) { a.Size = -1 },
		"size over":          func(a *UploadArtifactMetadataV1) { a.Size = AttemptOutputUploadMaxArtifactBytes + 1 },
		"empty sha mismatch": func(a *UploadArtifactMetadataV1) { a.SHA256 = uploadTestDigest("nonempty") },
		"empty md5 mismatch": func(a *UploadArtifactMetadataV1) {
			a.ContentMD5 = uploadTestMetadata(0, "other", "text/plain", "nonempty").ContentMD5
		},
		"md5 alias": func(a *UploadArtifactMetadataV1) { a.ContentMD5 += "\n" },
		"huge md5":  func(a *UploadArtifactMetadataV1) { a.ContentMD5 = strings.Repeat("A", 64<<10) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			metadata := uploadTestMetadata(0, "empty.txt", "text/plain", "")
			mutate(&metadata)
			if _, err := NewAttemptOutputUploadPlanV1(uploadTestIdentity(), []UploadArtifactMetadataV1{metadata}); err == nil {
				t.Fatalf("invalid metadata %s acquired a sealed plan digest", name)
			}
		})
	}
	if _, err := NewAttemptOutputUploadPlanV1(uploadTestIdentity(), []UploadArtifactMetadataV1{
		uploadTestMetadata(0, "same.txt", "text/plain", ""), uploadTestMetadata(1, "SAME.TXT", "text/plain", ""),
	}); err == nil {
		t.Fatal("case-colliding complete declared cohort acquired a plan digest")
	}
}

func TestAttemptOutputUploadPlanRejectsMalformedWholePlan(t *testing.T) {
	cases := map[string]func(*AttemptOutputUploadPlanV1){
		"version": func(p *AttemptOutputUploadPlanV1) { p.Version++ },
		"phase":   func(p *AttemptOutputUploadPlanV1) { p.Phase = "ready" },
		"bearer persisted": func(p *AttemptOutputUploadPlanV1) {
			p.Identity.Binding.PresentedSecretDigest = domain.AttachedWorkerConnectionSecretDigest(uploadTestDigest("secret"))
		},
		"wildcard head": func(p *AttemptOutputUploadPlanV1) { p.Identity.Binding.ExpectedAttemptRevision = 0 },
		"fresh window": func(p *AttemptOutputUploadPlanV1) {
			p.Identity.ExpiresAt = p.Identity.CreatedAt.Add(15*time.Second + time.Microsecond)
		},
		"after lease": func(p *AttemptOutputUploadPlanV1) {
			p.Identity.Binding.LeaseExpiresAtUnixMicro = p.Identity.ExpiresAt.Add(-time.Microsecond).UnixMicro()
		},
		"empty window":      func(p *AttemptOutputUploadPlanV1) { p.Identity.ExpiresAt = p.Identity.CreatedAt },
		"noncanonical time": func(p *AttemptOutputUploadPlanV1) { p.Identity.CreatedAt = p.Identity.CreatedAt.Add(time.Nanosecond) },
		"foreign key":       func(p *AttemptOutputUploadPlanV1) { p.Ordinals[0].Key = "tenants/foreign/object" },
		"ordinal":           func(p *AttemptOutputUploadPlanV1) { p.Ordinals[1].Metadata.Ordinal = 0 },
		"case collision":    func(p *AttemptOutputUploadPlanV1) { p.Ordinals[1].Metadata.Name = "EMPTY.TXT" },
		"invalid utf8":      func(p *AttemptOutputUploadPlanV1) { p.Ordinals[0].Metadata.Name = string([]byte{0xff}) },
		"name path":         func(p *AttemptOutputUploadPlanV1) { p.Ordinals[0].Metadata.Name = "../output" },
		"name control":      func(p *AttemptOutputUploadPlanV1) { p.Ordinals[0].Metadata.Name = "output\n.txt" },
		"binary":            func(p *AttemptOutputUploadPlanV1) { p.Ordinals[0].Metadata.MediaType = "application/octet-stream" },
		"negative size":     func(p *AttemptOutputUploadPlanV1) { p.Ordinals[0].Metadata.Size = -1 },
		"too large": func(p *AttemptOutputUploadPlanV1) {
			p.Ordinals[0].Metadata.Size = AttemptOutputUploadMaxArtifactBytes + 1
		},
		"digest alias": func(p *AttemptOutputUploadPlanV1) {
			p.Ordinals[0].Metadata.SHA256 = strings.ToUpper(p.Ordinals[0].Metadata.SHA256)
		},
		"md5 newline alias": func(p *AttemptOutputUploadPlanV1) { p.Ordinals[0].Metadata.ContentMD5 += "\n" },
		"planned source": func(p *AttemptOutputUploadPlanV1) {
			p.Ordinals[0].Source = &domain.BlobRef{TenantID: p.Identity.Binding.TenantID, Key: p.Ordinals[0].Key, SHA256: p.Ordinals[0].Metadata.SHA256}
		},
		"unknown ordinal state": func(p *AttemptOutputUploadPlanV1) { p.Ordinals[0].State = "uploaded" },
		"bound before source":   func(p *AttemptOutputUploadPlanV1) { p.Phase = UploadPlanReceiptBoundV1 },
		"third artifact":        func(p *AttemptOutputUploadPlanV1) { p.Ordinals = append(p.Ordinals, p.Ordinals[0]) },
		"no artifact":           func(p *AttemptOutputUploadPlanV1) { p.Ordinals = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			plan := uploadTestPlan(t)
			mutate(&plan)
			if plan.Validate() == nil {
				t.Fatalf("accepted malformed whole plan (%s)", name)
			}
		})
	}
}

func TestAttemptOutputUploadPlanRejectsMetadataBeforeEncoding(t *testing.T) {
	plan := uploadTestPlan(t)
	// Test-owned invalid metadata, allocated before measurement. The threshold
	// distinguishes encoding this 256KiB string from constant-size validation;
	// it is NOT a product/native heap budget or a zero-allocation promise.
	plan.Ordinals[0].Metadata.Name = strings.Repeat("x", 256<<10)
	var result error
	measurement := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result = plan.Validate()
		}
	})
	if result != ErrCandidateInvalid {
		t.Fatalf("overbound metadata validation=%v, want invalid", result)
	}
	if got := measurement.AllocedBytesPerOp(); got >= 64<<10 {
		t.Fatalf("rejected 256KiB metadata reached dependent encoder/hash allocation: got %d bytes/op, want <64KiB", got)
	}
}

func TestAttemptOutputUploadSpendIsIrreversibleAndDoesNotRenewWindow(t *testing.T) {
	original := uploadTestPlan(t)
	at := original.Identity.CreatedAt.Add(time.Second)
	spent, err := original.SpendOrdinal(0, uploadTestDigest("first grant"), at)
	if err != nil || spent.Validate() != nil {
		t.Fatalf("first irreversible spend failed: %v", err)
	}
	if original.Ordinals[0].State != UploadOrdinalUnissuedV1 || spent.Ordinals[0].State != UploadOrdinalWritePendingV1 ||
		spent.PlanDigest != original.PlanDigest || spent.Identity.ExpiresAt != original.Identity.ExpiresAt {
		t.Fatal("spend mutated original, plan identity or original cutoff")
	}
	for _, digest := range []string{uploadTestDigest("first grant"), uploadTestDigest("replacement grant")} {
		if _, err := spent.SpendOrdinal(0, digest, at); err == nil {
			t.Fatal("retry issued another grant for spent ordinal")
		}
	}
	for name, instant := range map[string]time.Time{"before admission": original.Identity.CreatedAt.Add(-time.Microsecond), "at cutoff": original.Identity.ExpiresAt} {
		if _, err := original.SpendOrdinal(0, uploadTestDigest("grant"), instant); err == nil {
			t.Errorf("%s spend accepted", name)
		}
	}
	spent.Phase = UploadPlanSettledAbandonedV1
	if spent.Validate() == nil {
		t.Fatal("ambiguous issued capability no longer blocks deletion disposition")
	}
	// This mutation is DATA only, not a genuine no-send witness. The future
	// custody transaction must accept it only from its real authenticated owner.
	spent.Ordinals[0].State = UploadOrdinalNotDispatchedV1
	if err := spent.Validate(); err != nil {
		t.Fatalf("definitive no-send noncanonical DATA disposition rejected: %v", err)
	}
	if _, err := spent.SpendOrdinal(1, uploadTestDigest("late grant"), at); err == nil {
		t.Fatal("abandoned plan reopened unissued ordinal")
	}
}

func TestAttemptOutputUploadExactVerifiedRefsAndCloneIsolation(t *testing.T) {
	plan := uploadTestPlan(t)
	for i := range plan.Ordinals {
		ordinal := &plan.Ordinals[i]
		ordinal.State = UploadOrdinalSourceVerifiedV1
		ordinal.GrantDigest = uploadTestDigest("consumed grant " + ordinal.Key)
		ordinal.Source = &domain.BlobRef{TenantID: plan.Identity.Binding.TenantID, Key: ordinal.Key, Size: ordinal.Metadata.Size, SHA256: ordinal.Metadata.SHA256}
	}
	plan.Phase = UploadPlanReceiptBoundV1
	if err := plan.Validate(); err != nil {
		t.Fatalf("complete exact source DATA rejected: %v", err)
	}
	clone := plan.Clone()
	clone.Ordinals[0].Source.Key += "-foreign"
	if plan.Validate() != nil || clone.Validate() == nil {
		t.Fatal("clone retained ref alias or foreign source passed exact validation")
	}
	plan.Phase = UploadPlanSettledAbandonedV1
	if err := plan.Validate(); err != nil {
		t.Fatalf("known source noncanonical inventory DATA rejected: %v", err)
	}
	if len(plan.Ordinals) != 2 || plan.Ordinals[0].Source == nil {
		t.Fatal("noncanonical disposition lost actual written-source inventory")
	}
}

func TestAttemptOutputUploadDigestBindsFullImmutableIdentity(t *testing.T) {
	original := uploadTestPlan(t)
	cases := map[string]func(*UploadPlanIdentityV1){
		"tenant":     func(i *UploadPlanIdentityV1) { i.Binding.TenantID = "other-tenant" },
		"session":    func(i *UploadPlanIdentityV1) { i.SessionID = "other-session" },
		"owner":      func(i *UploadPlanIdentityV1) { i.Binding.OwnerUserID = "other-owner" },
		"worker":     func(i *UploadPlanIdentityV1) { i.Binding.WorkerID = "other-worker" },
		"connection": func(i *UploadPlanIdentityV1) { i.Binding.ConnectionID = "other-connection" },
		"run":        func(i *UploadPlanIdentityV1) { i.Binding.RunID = "other-run" },
		"attempt":    func(i *UploadPlanIdentityV1) { i.Binding.AttemptID = "other-attempt" },
		"lease":      func(i *UploadPlanIdentityV1) { i.Binding.LeaseID = "other-lease" },
		"head":       func(i *UploadPlanIdentityV1) { i.Binding.ExpectedAttemptRevision++ },
		"profile":    func(i *UploadPlanIdentityV1) { i.ProfileDigest = uploadTestDigest("other profile") },
		"schema":     func(i *UploadPlanIdentityV1) { i.SchemaDigest = uploadTestDigest("other schema") },
		"final":      func(i *UploadPlanIdentityV1) { i.FinalDigest = uploadTestDigest("other final") },
		"answer":     func(i *UploadPlanIdentityV1) { i.AnswerDigest = uploadTestDigest("other answer") },
		"producer":   func(i *UploadPlanIdentityV1) { i.ProducerDigest = uploadTestDigest("other producer") },
		"nonce":      func(i *UploadPlanIdentityV1) { i.Nonce = "other-nonce" },
		"cutoff":     func(i *UploadPlanIdentityV1) { i.ExpiresAt = i.ExpiresAt.Add(-time.Microsecond) },
	}
	metadata := []UploadArtifactMetadataV1{original.Ordinals[0].Metadata, original.Ordinals[1].Metadata}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			identity := original.Identity
			mutate(&identity)
			other, err := NewAttemptOutputUploadPlanV1(identity, metadata)
			if err != nil || other.PlanDigest == original.PlanDigest {
				t.Fatalf("immutable %s not independently constructible/bound: %v", name, err)
			}
		})
	}
}
