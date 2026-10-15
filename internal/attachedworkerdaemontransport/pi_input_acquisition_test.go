package attachedworkerdaemontransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Structural synthetic metadata only, not an installed/native qualified P.
func piAcquisitionProfile(t *testing.T, artifact attachedworkerdaemon.ExecutableDigest) domain.AttachedPiSDKNativeProfileV1 {
	t.Helper()
	p := domain.AttachedPiSDKNativeProfileV1{Version: 1, ProfileID: domain.AttachedPiSDKProfileIDV1, NativeProtocolVersion: domain.AttachedPiSDKNativeProtocolV1,
		Sources:  domain.AttachedPiSDKNativeSourcesV1{PiSourceRevision: "6c4f360264397c59801f6da2bdac13e3b1fcbe91", OpenAISDKVersion: "6.40.0", OpenAISDKIntegrity: "sha512-MWtTjd/gQt4jpbji61NTgFWJLoY/PdRJ6wG9/ZDRMYNMlBKrCrSlkLI+KgHP1vR1qT6LKSAyAqIxno6lcK9JiA==", NodeVersion: "fixture"},
		Model:    domain.AttachedPiSDKNativeModelV1{ModelVendorID: "fixture-vendor", ModelID: "fixture", Endpoint: "https://fixture.invalid/v1/responses", ModelContextTokens: 8192, MaxBilledInputTokens: 2048, MaxOutputTokens: 1024},
		Platform: domain.AttachedPiSDKNativePlatformV1{OS: "fixture-os", Architecture: "fixture-arch"},
		Limits:   domain.AttachedPiSDKNativeLimitsV1{CanonicalInputBytes: 1 << 20, NativeRequestBytes: 1 << 20, PreSendAllocationBytes: 128 << 20, RequestHeaderBytes: 4096, ResponseHeaderBytes: 4096, ResponseBodyBytes: 1 << 20, ResponseChunkBytes: 32 << 10, SSELineBytes: 32 << 10, SSEEventBytes: 128 << 10, SSEEvents: 512, JSONNodes: 16 << 20, ResponseCopyWorkBytes: 16 << 20, ResponseAllocationBytes: 8 << 20, ProcessMemoryBytes: 256 << 20, ProcessDiskBytes: 1 << 20, ProcessFrameBytes: 1 << 20, ProcessStdoutBytes: 8 << 20, ProcessEvents: 2048, JSONDepth: 32, RawFinalBytes: 192 << 10, StructuredFinalBytes: 96 << 10, AnswerBytes: 16 << 10, ArtifactBytes: 16 << 10, ArtifactTotalBytes: 48 << 10},
		Behavior: domain.AttachedPiSDKNativeBehaviorV1{ProviderAPI: "openai-responses", ImageDerivation: "NoTransformOriginal", ImageDetail: "auto", Transport: "http-sse", LocalToolRegistry: "empty", MaxPhysicalSends: 1}}
	for _, target := range []any{&p.Sources, &p.Model, &p.Platform} {
		v := reflect.ValueOf(target).Elem()
		for i := 0; i < v.NumField(); i++ {
			if strings.HasSuffix(v.Type().Field(i).Name, "Digest") {
				h := sha256.Sum256([]byte(v.Type().Name() + v.Type().Field(i).Name))
				v.Field(i).SetString(hex.EncodeToString(h[:]))
			}
		}
	}
	p.Sources.NativeArtifactDigest = hex.EncodeToString(artifact[:])
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	return p
}

func piAcquisitionFixture(t *testing.T) (*BoundMaterializer, *sealedSourceFixture, MaterializationRequestV1, domain.AttachedPiSDKNativeProfileV1) {
	t.Helper()
	m, source, request := canonicalMaterializerFixture(t)
	p := piAcquisitionProfile(t, attachedworkerdaemon.ExecutableDigest(sha256.Sum256([]byte("synthetic Pi artifact"))))
	d, err := p.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	job := &source.input.Job
	job.HarnessBinding.Backend = d
	job.HarnessBinding.ModelVendorID = p.Model.ModelVendorID
	job.HarnessBinding.ModelID = p.Model.ModelID
	job.HarnessBinding.Resource = domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceAPIAccountV1, ResourceID: "api-1", OwnerUserID: request.OwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	job.HarnessBinding.AttachedPiAPIInvocationPolicyDigest = domain.AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("a", 64))
	expiry := source.input.AcceptedAttempt.LeaseExpiresAt
	job.HarnessBinding.EvidenceExpiresAt = &expiry
	resealSealedRequest(t, &source.input, &request)
	m, err = NewPiBoundMaterializer(source, p, m.maxBytes, m.now)
	if err != nil {
		t.Fatal(err)
	}
	return m, source, request, p
}

func TestPiInputAcquisitionMovePreservesOriginalOwnerAndBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, source, request, p := piAcquisitionFixture(t)
		original, cancel := context.WithTimeout(t.Context(), time.Hour)
		defer cancel()
		a, err := m.beginOriginalInput(original, request)
		if err != nil {
			t.Fatal(err)
		}
		defer a.Clear()
		request, err = a.Request()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = a.Request(); err == nil {
			t.Fatal("second acquisition transfer succeeded")
		}
		ledger := a.OriginalLedger().(*codexexec.CanonicalMediaLedgerV1)
		if err = a.Reserve(123, 0, 7); err != nil {
			t.Fatal(err)
		}
		before := ledger.Accounting()
		body := source.input.CanonicalContext.Attachments[0].Body
		proof := source.input.CanonicalContext
		child, stop := context.WithCancel(original)
		input, err := m.Materialize(child, request)
		stop()
		if err != nil {
			t.Fatalf("Pi materialize: %v", err)
		}
		defer clearMaterializedInput(&input)
		if input.VerifiedInput.CanonicalContext != proof || &input.VerifiedInput.CanonicalContext.Attachments[0].Body[0] != &body[0] || len(input.Stdin) != 0 {
			t.Fatal("canonical input was cloned, lost or made synthetic")
		}
		if second, err := m.Materialize(original, request); err == nil || second.VerifiedInput != nil || source.calls != 1 {
			clearMaterializedInput(&second)
			t.Fatal("same acquisition materialized twice")
		}
		fixture := newAdapterFixture(t)
		fixture.adapter.config.MaxInputBytes = m.maxBytes
		copy(fixture.adapter.config.Profile.ExecutableDigest[:], mustSealedDigest(t, p.Sources.NativeArtifactDigest))
		head := input.VerifiedInput.AcceptedAttempt
		identity := attachedworkerdaemon.InvocationIdentity{TenantID: head.TenantID, OwnerUserID: head.OwnerUserID, WorkerID: head.WorkerID, RunID: head.RunID, AttemptID: head.AttemptID, LeaseID: head.LeaseID, FenceToken: head.LeaseGeneration}
		inv, err := fixture.adapter.invocationFromInput(identity, request, &input)
		if err != nil {
			t.Fatal(err)
		}
		defer inv.Clear()
		clearMaterializedInput(&input)
		if inv.InputAcquisition != a || a.OriginalContext() != original || original.Err() != nil || inv.VerifiedInput.CanonicalContext != proof || len(inv.Process.Stdin) != 0 || string(body) != "file body 1" {
			t.Fatal("ownership/context lost across materializer child cancellation and adapter transfer")
		}
		if now := ledger.Accounting(); now.CopyBytes <= before.CopyBytes || now.Nodes <= before.Nodes || now.LiveBytes == 0 {
			t.Fatalf("original debit lost: before=%+v after=%+v", before, now)
		}
		identityBefore := inv.Identity
		ownedArgs, ownedEnv := inv.Process.Arguments, inv.Process.Environment
		inv.ClearInput()
		if inv.Identity != identityBefore || !bytes.Equal(body, make([]byte, len(body))) || ledger.Accounting().LiveBytes != 0 || ledger.Accounting().CopyBytes == 0 {
			t.Fatal("retirement lost identity, retained buffers or refunded cumulative work")
		}
		if !reflect.DeepEqual(inv.Process, attachedworkerdaemon.AttemptSpec{}) {
			t.Fatal("owned process projection retained after live retirement")
		}
		for _, v := range ownedArgs {
			if v != "" {
				t.Fatal("owned argv retained after retirement")
			}
		}
		for _, v := range ownedEnv {
			if v != (attachedworkerdaemon.EnvironmentVariable{}) {
				t.Fatal("owned environment retained after retirement")
			}
		}
		if fixture.adapter.config.Profile.Arguments[0] != "--attached" {
			t.Fatal("owned process cleanup mutated installed local profile")
		}
		if err = a.Reserve(0, 0, 0); err == nil {
			t.Fatal("retired acquisition reused")
		}
	})
}

func TestPiInputAcquisitionRejectsUnfundedAndForeignRequestsBeforeSource(t *testing.T) {
	for _, mode := range []string{"missing", "foreign", "closed", "exhausted"} {
		t.Run(mode, func(t *testing.T) {
			m, source, request, _ := piAcquisitionFixture(t)
			a, err := m.beginOriginalInput(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Clear()
			request, err = a.Request()
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "missing":
				request.acquisition = nil
			case "foreign":
				request.ConnectionGeneration++
			case "closed":
				a.Clear()
			case "exhausted":
				r := a.ledger.Remaining()
				if a.Reserve(r.CopyBytes, 0, 0) != nil {
					t.Fatal("fixture exhaustion failed")
				}
			}
			input, err := m.Materialize(t.Context(), request)
			defer clearMaterializedInput(&input)
			if !errors.Is(err, ErrSealedInputInvalid) || input.VerifiedInput != nil {
				t.Fatalf("mode=%s result=%v calls=%d", mode, err, source.calls)
			}
			if mode != "exhausted" && source.calls != 0 {
				t.Fatalf("mode=%s source called before owner validation: %d", mode, source.calls)
			}
		})
	}
}

func TestPiInputAcquisitionActualAdapterNextUsesOneOriginalOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, source, request, p := piAcquisitionFixture(t)
		f := newAdapterFixture(t)
		f.session.snapshot.TenantID = request.TenantID
		f.session.snapshot.OwnerUserID = request.OwnerUserID
		f.session.snapshot.WorkerID = request.WorkerID
		f.session.snapshot.ConnectionID = request.ConnectionID
		f.session.snapshot.ConnectionGeneration = request.ConnectionGeneration
		f.session.snapshot.EnrollmentGeneration = request.EnrollmentGeneration
		f.session.snapshot.CapabilityDigest = source.input.Job.ExecutionPlacementV2.CapabilityDigest
		f.session.binding = request.Attempt
		f.config.Profile.CapabilityDigest = f.session.snapshot.CapabilityDigest
		copy(f.config.Profile.ExecutableDigest[:], mustSealedDigest(t, p.Sources.NativeArtifactDigest))
		f.config.MaxInputBytes = m.maxBytes
		f.config.Now = m.now
		var err error
		f.adapter, err = New(f.session, m, f.config)
		if err != nil {
			t.Fatal(err)
		}
		original, cancel := context.WithTimeout(t.Context(), time.Hour)
		defer cancel()
		var seen *PiInputAcquisitionV1
		source.afterLoad = func() {
			seen = source.seen.PiInputAcquisitionV1()
			if seen == nil || seen.OriginalContext() != original || seen.OriginalLedger() == nil {
				t.Error("source reached without original owner/ledger")
			}
		}
		inv, available, err := f.adapter.Next(original)
		if err != nil || !available {
			t.Fatalf("actual Next: available=%t err=%v state=%s", available, err, f.session.snapshot.State)
		}
		defer inv.Clear()
		if inv.InputAcquisition != seen || inv.InputAcquisition.OriginalContext() != original || original.Err() != nil || len(inv.Process.Stdin) != 0 || inv.Credential != nil || len(f.session.actions) != 2 {
			t.Fatal("actual accepted-claim/materialize/adapter path lost owner or fabricated stdin/credential")
		}
		if f.session.snapshot.State != attachedworkersession.StateReady {
			t.Fatal("test silently created another attached session")
		}
		ledger := seen.OriginalLedger().(*codexexec.CanonicalMediaLedgerV1)
		if ledger.Accounting().CopyBytes == 0 || ledger.Accounting().LiveBytes == 0 {
			t.Fatal("funding vanished before Run")
		}
		active := f.adapter.currentActive()
		digests := [][]byte{active.request.Attempt.ContextDigest, active.request.Attempt.CapabilityDigest, active.request.Attempt.PolicyDigest}
		retained := uint64(len(digests[0]) + len(digests[1]) + len(digests[2]))
		inv.ClearInput() // actual Daemon order: joined Run/Clear, then Complete
		work := ledger.Accounting().CopyBytes
		if ledger.Accounting().LiveBytes != retained {
			t.Fatalf("active control debt lost before ACK: got=%d want=%d", ledger.Accounting().LiveBytes, retained)
		}
		f.session.terminalErr = errors.New("synthetic lost terminal acknowledgement")
		result := successfulResult()
		if err := f.adapter.Complete(t.Context(), inv.Identity, result, nil); !errors.Is(err, ErrReconciliationRequired) || ledger.Accounting().LiveBytes != retained || f.adapter.currentActive() == nil {
			t.Fatalf("ambiguous ACK retired original active debt: err=%v live=%d", err, ledger.Accounting().LiveBytes)
		}
		f.session.terminalErr = nil
		if err := f.adapter.Complete(t.Context(), inv.Identity, result, nil); err != nil {
			t.Fatal(err)
		}
		if ledger.Accounting().LiveBytes != 0 || ledger.Accounting().CopyBytes != work || f.adapter.currentActive() != nil {
			t.Fatal("exact ACK did not retire control partition without cumulative refund")
		}
		for _, d := range digests {
			if !bytes.Equal(d, make([]byte, len(d))) {
				t.Fatal("retired active digest bytes retained")
			}
		}
		if err := f.adapter.Complete(t.Context(), inv.Identity, result, nil); err != nil || ledger.Accounting().LiveBytes != 0 {
			t.Fatal("repeated Complete double-retired original control partition")
		}
	})
}
