package attachedworkerdaemontransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

type sealedSourceFixture struct {
	input     SealedInputV1
	calls     int
	seen      MaterializationRequestV1
	afterLoad func()
	err       error
}

func (source *sealedSourceFixture) Load(_ context.Context, request MaterializationRequestV1) (SealedInputV1, error) {
	source.calls++
	source.seen = request
	if source.afterLoad != nil {
		source.afterLoad()
	}
	return source.input, source.err
}

func sealedMaterializerFixture(t *testing.T) (*BoundMaterializer, *sealedSourceFixture, MaterializationRequestV1) {
	t.Helper()
	contextBytes := []byte("synthetic canonical context")
	artifactBytes := []byte("synthetic artifact")
	placement := domain.ExecutionPlacementV2{
		Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: "owner-1", WorkerID: "worker-1",
		CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("synthetic capability")),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("synthetic policy"))),
	}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(
		"tenant-1", "owner-1", "run-1", "attempt-1", "subscription-1", time.Unix(1, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	binding := authority.HarnessBinding.Clone()
	binding.ExecutionPlacementDigest = string(placementDigest)
	job := domain.WorkerJob{
		TenantID: "tenant-1", RunID: "run-1", SessionID: "session-1", TriggerEventID: "event-1",
		AttemptID: "attempt-1", ReservationID: "reservation-1", InputManifestID: "manifest-1",
		ContextSnapshot:       sealedBlob("tenant-1", "context", contextBytes),
		CredentialOwnerUserID: "owner-1", ExecutionPlacementV2: placement, HarnessBinding: binding,
		Limits: domain.ProductLimits{
			MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 10,
			MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 100,
			MaxArtifacts: 10, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18,
		},
	}
	manifest := domain.ArtifactManifest{
		ID: job.InputManifestID, TenantID: job.TenantID, RunID: job.RunID,
		CreatedAt: time.Unix(10, 0).UTC(),
		Artifacts: []domain.Artifact{{Name: "alpha", MediaType: "text/plain", Blob: sealedBlob("tenant-1", "alpha", artifactBytes)}},
	}
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(20, 0).UTC()
	request := MaterializationRequestV1{
		TenantID: job.TenantID, OwnerUserID: placement.OwnerUserID, WorkerID: placement.WorkerID,
		EnrollmentGeneration: 2, ConnectionGeneration: 3, ConnectionID: "connection-1", AttemptSequence: 1,
		Attempt: attachedworkerprotocol.AttemptBindingV1{
			RunID: string(job.RunID), AttemptID: string(job.AttemptID), LeaseID: "lease-1",
			LeaseGeneration: 4, FenceToken: "fence-1", ExpiresAtUnixMicro: now.Add(time.Minute).UnixMicro(),
			ContextDigest:    mustSealedDigest(t, string(contextDigest)),
			CapabilityDigest: mustSealedDigest(t, string(placement.CapabilityDigest)),
			PolicyDigest:     mustSealedDigest(t, string(placement.PolicyDigest)),
		},
	}
	source := &sealedSourceFixture{input: SealedInputV1{
		Job: job, Manifest: manifest, Context: contextBytes,
		Artifacts: []SealedArtifactV1{{Name: "alpha", Body: artifactBytes}},
	}}
	materializer, err := NewBoundMaterializer(source, 4096, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return materializer, source, request
}

func sealedBlob(tenant domain.TenantID, name string, body []byte) domain.BlobRef {
	digest := sha256.Sum256(body)
	return domain.BlobRef{
		TenantID: tenant, Key: "tenants/" + string(tenant) + "/" + name,
		Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:]),
	}
}

func mustSealedDigest(t *testing.T, encoded string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestBoundMaterializerSealsAcceptedSyntheticInput(t *testing.T) {
	materializer, source, request := sealedMaterializerFixture(t)
	result, err := materializer.Materialize(context.Background(), request)
	if err != nil {
		t.Fatalf("Materialize accepted input: %v", err)
	}
	if source.calls != 1 || !sameAttemptBinding(source.seen.Attempt, request.Attempt) ||
		source.seen.ConnectionGeneration != request.ConnectionGeneration {
		t.Fatalf("source received wrong request: calls=%d generation=%d", source.calls, source.seen.ConnectionGeneration)
	}
	if len(result.ReadRoots) != 0 || result.Credential != nil {
		t.Fatalf("synthetic input gained host or credential authority: %+v", result)
	}
	var envelope sealedEnvelopeV1
	if err := json.Unmarshal(result.Stdin, &envelope); err != nil {
		t.Fatalf("decode synthetic envelope: %v", err)
	}
	if envelope.Version != 1 || envelope.Kind != "sessionless.attached-worker.synthetic-input.v1" ||
		string(envelope.Context) != "synthetic canonical context" || len(envelope.Artifacts) != 1 ||
		envelope.Artifacts[0].Name != "alpha" || string(envelope.Artifacts[0].Body) != "synthetic artifact" {
		t.Fatalf("unexpected envelope: version=%d kind=%q context=%q artifacts=%d", envelope.Version, envelope.Kind, envelope.Context, len(envelope.Artifacts))
	}
	if !bytes.Equal(source.input.Context, make([]byte, len(source.input.Context))) ||
		!bytes.Equal(source.input.Artifacts[0].Body, make([]byte, len(source.input.Artifacts[0].Body))) {
		t.Fatal("transferred source buffers were not cleared")
	}
}

func TestBoundMaterializerRejectsInvalidAuthorityBeforeSource(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*MaterializationRequestV1)
	}{
		{name: "missing connection", mutate: func(request *MaterializationRequestV1) { request.ConnectionID = "" }},
		{name: "missing generation", mutate: func(request *MaterializationRequestV1) { request.ConnectionGeneration = 0 }},
		{name: "invalid attempt sequence", mutate: func(request *MaterializationRequestV1) { request.AttemptSequence = 2 }},
		{name: "expired lease", mutate: func(request *MaterializationRequestV1) {
			request.Attempt.ExpiresAtUnixMicro = time.Unix(20, 0).UnixMicro()
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			materializer, source, request := sealedMaterializerFixture(t)
			test.mutate(&request)
			if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, ErrSealedInputInvalid) {
				t.Fatalf("invalid %s returned %v", test.name, err)
			}
			if source.calls != 0 {
				t.Fatalf("invalid %s reached source: calls=%d", test.name, source.calls)
			}
		})
	}
}

func TestBoundMaterializerRejectsSwappedOrTamperedInput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*SealedInputV1)
		want   error
	}{
		{name: "cross owner", mutate: func(input *SealedInputV1) { input.Job.ExecutionPlacementV2.OwnerUserID = "owner-2" }, want: ErrSealedInputInvalid},
		{name: "cross credential owner", mutate: func(input *SealedInputV1) { input.Job.CredentialOwnerUserID = "owner-2" }, want: ErrSealedInputInvalid},
		{name: "cross attempt", mutate: func(input *SealedInputV1) { input.Job.AttemptID = "attempt-2" }, want: ErrSealedInputInvalid},
		{name: "context digest drift", mutate: func(input *SealedInputV1) { input.Job.Limits.MaxTurns++ }, want: ErrSealedInputInvalid},
		{name: "context bytes drift", mutate: func(input *SealedInputV1) { input.Context[0] ^= 1 }, want: ErrSealedInputInvalid},
		{name: "artifact bytes drift", mutate: func(input *SealedInputV1) { input.Artifacts[0].Body[0] ^= 1 }, want: ErrSealedInputInvalid},
		{name: "duplicate artifact", mutate: func(input *SealedInputV1) { input.Artifacts = append(input.Artifacts, input.Artifacts[0]) }, want: ErrSealedInputInvalid},
		{name: "context window unsupported", mutate: func(input *SealedInputV1) {
			input.Job.ContextWindow = &domain.SessionContextWindow{AfterSequence: 1, ThroughSequence: 1}
		}, want: ErrSealedInputInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			materializer, source, request := sealedMaterializerFixture(t)
			test.mutate(&source.input)
			if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, test.want) {
				t.Fatalf("tampered %s returned %v, want %v", test.name, err, test.want)
			}
			if source.calls != 1 {
				t.Fatalf("tampered %s source calls=%d", test.name, source.calls)
			}
		})
	}
}

func TestBoundMaterializerRejectsResealedForeignHarnessBinding(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*domain.HarnessBindingV1)
	}{
		{name: "tenant", mutate: func(binding *domain.HarnessBindingV1) { binding.TenantID = "tenant-2" }},
		{name: "owner", mutate: func(binding *domain.HarnessBindingV1) {
			binding.OwnerUserID = "owner-2"
			binding.Resource.OwnerUserID = "owner-2"
		}},
		{name: "run", mutate: func(binding *domain.HarnessBindingV1) { binding.RunID = "run-2" }},
		{name: "attempt", mutate: func(binding *domain.HarnessBindingV1) { binding.AttemptID = "attempt-2" }},
		{name: "placement", mutate: func(binding *domain.HarnessBindingV1) {
			binding.ExecutionPlacementDigest = strings.Repeat("a", 64)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			materializer, source, request := sealedMaterializerFixture(t)
			test.mutate(&source.input.Job.HarnessBinding)
			resealSealedRequest(t, &source.input, &request)
			if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, ErrSealedInputInvalid) {
				t.Fatalf("foreign %s binding returned %v", test.name, err)
			}
		})
	}
}

func TestBoundMaterializerRejectsOversizedMetadataBeforeDigest(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*SealedInputV1)
	}{
		{name: "manifest collection", mutate: func(input *SealedInputV1) {
			input.Manifest.Artifacts = make([]domain.Artifact, maxSealedCollectionItems+1)
		}},
		{name: "server collection", mutate: func(input *SealedInputV1) {
			input.Job.AllowedMCPServers = make([]string, maxSealedCollectionItems+1)
		}},
		{name: "oversized key", mutate: func(input *SealedInputV1) {
			input.Job.ContextSnapshot.Key += strings.Repeat("x", maxSealedMetadataValue)
		}},
		{name: "oversized context", mutate: func(input *SealedInputV1) {
			input.Context = make([]byte, defaultMaxSealedInputBytes+1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			materializer, source, request := sealedMaterializerFixture(t)
			test.mutate(&source.input)
			if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, ErrSealedInputInvalid) {
				t.Fatalf("oversized %s returned %v", test.name, err)
			}
			if source.calls != 1 {
				t.Fatalf("oversized %s source calls=%d", test.name, source.calls)
			}
		})
	}
}

func TestBoundMaterializerDoesNotTraverseTimestampLocationInternals(t *testing.T) {
	zone, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	materializer, source, request := sealedMaterializerFixture(t)
	source.input.Job.CreatedAt = time.Unix(10, 0).In(zone)
	source.input.Manifest.CreatedAt = time.Unix(10, 0).In(zone)
	if _, err := materializer.Materialize(context.Background(), request); err != nil {
		t.Fatalf("valid timestamp location affected metadata budget: %v", err)
	}
}

func TestBoundMaterializerRejectsOversizedEnvelopeAndRedactsErrors(t *testing.T) {
	materializer, source, request := sealedMaterializerFixture(t)
	materializer.maxBytes = len(source.input.Context) + len(source.input.Artifacts[0].Body)
	_, err := materializer.Materialize(context.Background(), request)
	if !errors.Is(err, ErrSealedInputInvalid) {
		t.Fatalf("oversized envelope error=%v", err)
	}
	if strings.Contains(err.Error(), "synthetic canonical context") || strings.Contains(err.Error(), "tenants/") {
		t.Fatalf("error disclosed source content or blob key: %v", err)
	}
}

func TestBoundMaterializerEnforcesAdmittedBudgetsAndLeaseAfterLoad(t *testing.T) {
	t.Run("input budget", func(t *testing.T) {
		materializer, source, request := sealedMaterializerFixture(t)
		source.input.Job.Limits.MaxInputBytes = 1
		resealSealedRequest(t, &source.input, &request)
		if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, ErrSealedInputInvalid) {
			t.Fatalf("input budget error=%v", err)
		}
	})
	t.Run("context budget", func(t *testing.T) {
		materializer, source, request := sealedMaterializerFixture(t)
		source.input.Job.Limits.MaxContextBytes = 1
		resealSealedRequest(t, &source.input, &request)
		if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, ErrSealedInputInvalid) {
			t.Fatalf("context budget error=%v", err)
		}
	})
	t.Run("artifact count", func(t *testing.T) {
		materializer, source, request := sealedMaterializerFixture(t)
		second := []byte("second synthetic artifact")
		source.input.Manifest.Artifacts = append(source.input.Manifest.Artifacts, domain.Artifact{
			Name: "beta", MediaType: "text/plain", Blob: sealedBlob("tenant-1", "beta", second),
		})
		source.input.Artifacts = append(source.input.Artifacts, SealedArtifactV1{Name: "beta", Body: second})
		source.input.Job.Limits.MaxArtifacts = 1
		resealSealedRequest(t, &source.input, &request)
		if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, ErrSealedInputInvalid) {
			t.Fatalf("artifact count error=%v", err)
		}
	})
	t.Run("lease expires during load", func(t *testing.T) {
		materializer, source, request := sealedMaterializerFixture(t)
		now := time.Unix(20, 0).UTC()
		materializer.now = func() time.Time { return now }
		source.afterLoad = func() { now = now.Add(2 * time.Minute) }
		if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, ErrSealedInputInvalid) {
			t.Fatalf("late load error=%v", err)
		}
		if source.calls != 1 || !bytes.Equal(source.input.Context, make([]byte, len(source.input.Context))) {
			t.Fatalf("late input not cleared: calls=%d", source.calls)
		}
	})
}

func TestBoundMaterializerRejectsSupportedDigestButUnsupportedMode(t *testing.T) {
	materializer, source, request := sealedMaterializerFixture(t)
	source.input.Job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 1}
	resealSealedRequest(t, &source.input, &request)
	if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, ErrSealedInputUnsupported) {
		t.Fatalf("valid context window returned %v, want unsupported", err)
	}
}

func TestBoundMaterializerCanonicalizesArtifactOrder(t *testing.T) {
	materializer, source, request := sealedMaterializerFixture(t)
	second := []byte("second synthetic artifact")
	source.input.Manifest.Artifacts = append(source.input.Manifest.Artifacts, domain.Artifact{
		Name: "beta", MediaType: "text/plain", Blob: sealedBlob("tenant-1", "beta", second),
	})
	source.input.Artifacts = []SealedArtifactV1{
		{Name: "beta", Body: second}, source.input.Artifacts[0],
	}
	resealSealedRequest(t, &source.input, &request)
	result, err := materializer.Materialize(context.Background(), request)
	if err != nil {
		t.Fatalf("materialize reordered artifacts: %v", err)
	}
	var envelope sealedEnvelopeV1
	if err := json.Unmarshal(result.Stdin, &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Artifacts) != 2 || envelope.Artifacts[0].Name != "alpha" || envelope.Artifacts[1].Name != "beta" {
		t.Fatalf("artifact order=%v", envelope.Artifacts)
	}
}

func TestBoundMaterializerSanitizesSourceFailureAndClearsBuffers(t *testing.T) {
	materializer, source, request := sealedMaterializerFixture(t)
	source.err = errors.New("secret source token")
	if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, ErrMaterializationFailed) ||
		strings.Contains(err.Error(), "secret source token") {
		t.Fatalf("source failure was not sanitized: %v", err)
	}
	if source.calls != 1 || !bytes.Equal(source.input.Context, make([]byte, len(source.input.Context))) {
		t.Fatalf("failed source buffers not cleared: calls=%d", source.calls)
	}
}

func TestBoundMaterializerRunsOnlyAfterAdapterAcceptance(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		adapter, source, session := boundAdapterFixture(t)
		invocation, available, err := adapter.Next(context.Background())
		if err != nil || !available || source.calls != 1 {
			t.Fatalf("accepted Next available=%t calls=%d error=%v", available, source.calls, err)
		}
		if len(invocation.Process.AdditionalReadRoots) != 0 || invocation.Credential != nil || len(invocation.Process.Stdin) == 0 {
			t.Fatalf("accepted invocation gained forbidden authority: %+v", invocation)
		}
		if len(session.actions) != 2 || session.actions[1].LeaseClaim == nil {
			t.Fatalf("materialization did not follow exact claim: actions=%d", len(session.actions))
		}
		if err := adapter.Complete(context.Background(), invocation.Identity, successfulResult(), nil); err != nil {
			t.Fatalf("complete accepted synthetic attempt: %v", err)
		}
	})
	t.Run("cancelled before acceptance", func(t *testing.T) {
		adapter, source, session := boundAdapterFixture(t)
		session.cancelOnClaim = true
		if _, available, err := adapter.Next(context.Background()); available || !errors.Is(err, ErrAttemptCancelled) {
			t.Fatalf("cancelled Next available=%t error=%v", available, err)
		}
		if source.calls != 0 {
			t.Fatalf("pre-accept cancellation reached source: calls=%d", source.calls)
		}
	})
}

func boundAdapterFixture(t *testing.T) (*Adapter, *sealedSourceFixture, *fakeSession) {
	t.Helper()
	materializer, source, request := sealedMaterializerFixture(t)
	base := newAdapterFixture(t)
	base.session.snapshot.TenantID = request.TenantID
	base.session.snapshot.OwnerUserID = request.OwnerUserID
	base.session.snapshot.WorkerID = request.WorkerID
	base.session.snapshot.EnrollmentGeneration = request.EnrollmentGeneration
	base.session.snapshot.ConnectionGeneration = request.ConnectionGeneration
	base.session.snapshot.ConnectionID = request.ConnectionID
	base.session.snapshot.CapabilityDigest = source.input.Job.ExecutionPlacementV2.CapabilityDigest
	base.session.binding = request.Attempt
	base.config.Profile.CapabilityDigest = source.input.Job.ExecutionPlacementV2.CapabilityDigest
	base.config.Now = func() time.Time { return time.Unix(20, 0).UTC() }
	base.session.snapshot.AuthenticationExpires = func() *time.Time {
		value := time.Unix(20, 0).Add(time.Hour).UTC()
		return &value
	}()
	adapter, err := New(base.session, materializer, base.config)
	if err != nil {
		t.Fatalf("construct bound adapter: %v", err)
	}
	return adapter, source, base.session
}

func resealSealedRequest(t *testing.T, input *SealedInputV1, request *MaterializationRequestV1) {
	t.Helper()
	digest, err := domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	request.Attempt.ContextDigest = mustSealedDigest(t, string(digest))
}
