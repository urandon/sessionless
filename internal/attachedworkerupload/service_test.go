package attachedworkerupload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeruploadwire"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Actual connection parser/binder and control service, synthetic store results.
// This is not native SQL/current Source/provider/profile or writer evidence.
type controlStoreFixture struct {
	ports.AttachedWorkerTransportStore
	result      ydbstore.AttachedWorkerOutputUploadAdmissionResultV1
	claimExpiry time.Time
	err         error
	calls       int
	auth        ports.AttachedWorkerSealedInputAuthorization
	ctx         context.Context
	admit       ydbstore.AttachedWorkerOutputUploadAdmissionV1
	claim       ydbstore.AttachedWorkerOutputUploadClaimV1
	reconcile   ydbstore.AttachedWorkerOutputUploadReconciliationV1
}

func (s *controlStoreFixture) AdmitAttachedWorkerOutputUpload(ctx context.Context, r ydbstore.AttachedWorkerOutputUploadAdmissionV1) (ydbstore.AttachedWorkerOutputUploadAdmissionResultV1, error) {
	s.calls++
	s.ctx, s.auth, s.admit = ctx, r.Authorization, r
	return s.result, s.err
}
func (s *controlStoreFixture) ClaimAttachedWorkerOutputUpload(ctx context.Context, r ydbstore.AttachedWorkerOutputUploadClaimV1) (ydbstore.AttachedWorkerOutputUploadClaimResultV1, error) {
	s.calls++
	s.ctx, s.auth, s.claim = ctx, r.Authorization, r
	return ydbstore.AttachedWorkerOutputUploadClaimResultV1{Status: s.result.Status, AcceptedAttempt: s.result.AcceptedAttempt,
		CheckedAt: s.result.CheckedAt, ExpiresAt: s.claimExpiry, Plan: s.result.Plan}, s.err
}
func (s *controlStoreFixture) ReconcileAttachedWorkerOutputUpload(ctx context.Context, r ydbstore.AttachedWorkerOutputUploadReconciliationV1) (ydbstore.AttachedWorkerOutputUploadAdmissionResultV1, error) {
	s.calls++
	s.ctx, s.auth, s.reconcile = ctx, r.Authorization, r
	return s.result, s.err
}

type noControlIDs struct{}

func (noControlIDs) NewID(context.Context, ports.IDKind) (string, error) {
	return "", errors.New("control must not create an independent identity")
}

func controlFixture(t *testing.T) (*Service, *controlStoreFixture, []byte, attachedworkeruploadwire.Request) {
	t.Helper()
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	digest := strings.Repeat("a", 64)
	head := domain.AttachedWorkerAttemptV1{Version: 1, TenantID: "control-tenant", OwnerUserID: "control-owner", WorkerID: "control-worker",
		ConnectionID: "control-connection", ExecutionConnectionID: "control-connection", EnrollmentGeneration: 2, ConnectionGeneration: 3, ExecutionConnectionGeneration: 3,
		RunID: "control-run", AttemptID: "control-attempt", ReservationID: "control-reservation", LeaseID: "control-lease", LeaseGeneration: 4,
		ContextDigest: domain.AttachedWorkerContextDigest(digest), CapabilityDigest: domain.AttachedWorkerCapabilityDigest(digest), PolicyDigest: domain.AttachedWorkerPolicyDigest(digest),
		State: domain.AttachedWorkerAttemptClaimed, PlatformAttemptSequence: 1, WorkerAttemptSequence: 1, ProgressSequence: 1,
		CreatedAt: at, UpdatedAt: at, LeaseExpiresAt: at.Add(time.Minute), Revision: 7}
	var err error
	head.FenceToken, err = domain.NewAttachedWorkerFenceTokenV1(head.TenantID, head.OwnerUserID, head.WorkerID, head.RunID, head.AttemptID, head.LeaseID, head.LeaseGeneration)
	if err != nil || head.Validate() != nil {
		t.Fatal("invalid structural head fixture:", err)
	}
	auth := ports.AttachedWorkerSealedInputAuthorization{TenantID: head.TenantID, OwnerUserID: head.OwnerUserID, WorkerID: head.WorkerID, ConnectionID: head.ConnectionID,
		EnrollmentGeneration: head.EnrollmentGeneration, ConnectionGeneration: head.ConnectionGeneration, RunID: head.RunID, AttemptID: head.AttemptID,
		AttemptSequence: 1, LeaseID: head.LeaseID, LeaseGeneration: head.LeaseGeneration, FenceToken: head.FenceToken, LeaseExpiresAtUnixMicro: head.LeaseExpiresAt.UnixMicro(),
		ContextDigest: head.ContextDigest, CapabilityDigest: head.CapabilityDigest, PolicyDigest: head.PolicyDigest, ExpectedAttemptRevision: head.Revision}
	observation := attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true,
		CredentialReleaseRequired: true, CredentialReleased: true}
	observationDigest, err := observation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	schema := sha256.Sum256([]byte(attachedworkeroutput.StructuredFinalSchemaV1))
	request := attachedworkeruploadwire.Request{Version: 1, Operation: attachedworkeruploadwire.Admit, Authorization: auth, SessionID: "control-session",
		HarnessBindingDigest: domain.HarnessBindingDigestV2(digest), ProfileDigest: digest, SchemaDigest: hex.EncodeToString(schema[:]), FinalDigest: digest,
		AnswerDigest: digest, ProducerDigest: digest, Nonce: "control-nonce", CallerDeadline: attachedworkeruploadwire.CallerDeadline{Version: 1, RemainingNanoseconds: uint64(15 * time.Second)},
		Observation: observation, ObservationDigest: observationDigest, Artifacts: []attachedworkeroutput.UploadArtifactMetadataV1{
			{Ordinal: 0, Name: "empty.txt", MediaType: "text/plain", Size: 0, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", ContentMD5: "1B2M2Y8AsgTpgAmY7PhCfg=="},
		}}
	plan, err := attachedworkeroutput.NewAttemptOutputUploadPlanV1(attachedworkeroutput.UploadPlanIdentityV1{Binding: auth, SessionID: request.SessionID,
		HarnessBindingDigest: request.HarnessBindingDigest, ProfileDigest: request.ProfileDigest, SchemaDigest: request.SchemaDigest,
		FinalDigest: request.FinalDigest, AnswerDigest: request.AnswerDigest, ProducerDigest: request.ProducerDigest,
		Nonce: request.Nonce, CreatedAt: at, ExpiresAt: at.Add(15 * time.Second)}, request.Artifacts)
	if err != nil {
		t.Fatal(err)
	}
	store := &controlStoreFixture{result: ydbstore.AttachedWorkerOutputUploadAdmissionResultV1{Status: ports.AttachedWorkerExecutionApplied,
		AcceptedAttempt: head, CheckedAt: at.Add(time.Second), Plan: plan}, claimExpiry: at.Add(3 * time.Second)}
	binder, err := attachedworkertransport.NewService(attachedworkertransport.ServiceConfig{IDs: noControlIDs{}, Audience: "sessionless:attached-worker:v1",
		PlatformOffer:       attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}},
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, ChallengeLifetime: 5 * time.Minute, ChallengeRetention: time.Hour,
		PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour, CheckpointInterval: attachedworkertransport.MinimumHeartbeatInterval}, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(binder, store)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := attachedworkertransport.ParseConnectionSecret(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	token, err := attachedworkertransport.NewConnectionBearer(auth.TenantID, auth.OwnerUserID, auth.WorkerID, auth.ConnectionID, secret)
	if err != nil {
		t.Fatal(err)
	}
	return service, store, token.Bytes(), request
}

func TestAttemptOutputUploadServiceBindsOriginalBeforeStore(t *testing.T) {
	service, store, bearer, request := controlFixture(t)
	reply, err := service.Execute(t.Context(), bearer, request)
	if err != nil || reply.Status != ports.AttachedWorkerExecutionApplied || reply.Capability != nil || store.calls != 1 || store.ctx != t.Context() ||
		store.auth.PresentedSecretDigest == "" || store.admit.Observation != request.Observation || store.admit.ObservationDigest != request.ObservationDigest ||
		store.admit.SessionID != request.SessionID || reply.PlanDigest != store.result.Plan.PlanDigest || len(reply.Ordinals) != 1 {
		t.Fatalf("admit original context/auth/metadata association failed: err=%v calls=%d", err, store.calls)
	}
	if err := attachedworkeruploadwire.CompareReply(request, request, reply, nil); err != nil {
		t.Fatal("actual service reply did not reconstruct exact original plan:", err)
	}
	request.Authorization.OwnerUserID = "foreign-owner"
	a := &request.Authorization
	a.FenceToken, err = domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Execute(t.Context(), bearer, request); !errors.Is(err, ErrUnauthorized) || store.calls != 1 {
		t.Fatalf("foreign owner reached store: err=%v calls=%d", err, store.calls)
	}
}

func TestAttemptOutputUploadServiceClaimIsNotBodyAuthority(t *testing.T) {
	service, store, bearer, admission := controlFixture(t)
	plan, err := store.result.Plan.SpendOrdinal(0, strings.Repeat("b", 64), store.result.CheckedAt)
	if err != nil {
		t.Fatal(err)
	}
	store.result.Plan = plan
	claim := attachedworkeruploadwire.Request{Version: 1, Operation: attachedworkeruploadwire.Claim, Authorization: admission.Authorization,
		Nonce: admission.Nonce, PlanDigest: plan.PlanDigest, Ordinal: 0, CallerDeadline: admission.CallerDeadline}
	reply, err := service.Execute(t.Context(), bearer, claim)
	if err != nil || reply.Capability != nil || reply.ExpiresAt != plan.Identity.ExpiresAt || store.claim.Ordinal != 0 ||
		store.claim.RemainingNanoseconds != claim.CallerDeadline.RemainingNanoseconds || reply.Ordinals[0].State != attachedworkeroutput.UploadOrdinalWritePendingV1 {
		t.Fatalf("claim metadata changed plan or granted a body: err=%v calls=%d", err, store.calls)
	}
	// The narrower claimExpiry is NOT forged into an upload capability or a
	// renewed plan. Future genuine signer integration must preserve it itself.
	if store.claimExpiry == reply.ExpiresAt {
		t.Fatal("fixture did not distinguish immutable plan from narrower claim expiry")
	}
	reconcile := attachedworkeruploadwire.Request{Version: 1, Operation: attachedworkeruploadwire.Reconcile, Authorization: admission.Authorization,
		Nonce: admission.Nonce, ProducerDigest: admission.ProducerDigest, FinalDigest: admission.FinalDigest, CallerDeadline: admission.CallerDeadline}
	store.result.Status = ports.AttachedWorkerExecutionReplayed
	observed, err := service.Execute(t.Context(), bearer, reconcile)
	if err != nil || observed.Capability != nil || observed.Ordinals[0] != reply.Ordinals[0] || store.reconcile.ProducerDigest != admission.ProducerDigest {
		t.Fatal("reconcile reissued/settled pending or changed original producer")
	}
}

func TestAttemptOutputUploadServiceRejectsForeignOrUnavailableSnapshot(t *testing.T) {
	for name, mutate := range map[string]func(*controlStoreFixture){
		"wrong nonce":      func(s *controlStoreFixture) { s.result.Plan.Identity.Nonce = "other-nonce" },
		"wrong revision":   func(s *controlStoreFixture) { s.result.AcceptedAttempt.Revision++ },
		"wrong head owner": func(s *controlStoreFixture) { s.result.AcceptedAttempt.OwnerUserID = "other-owner" },
		"cancelled head":   func(s *controlStoreFixture) { s.result.AcceptedAttempt.CancelRevision = 1 },
		"expired":          func(s *controlStoreFixture) { s.result.CheckedAt = s.result.Plan.Identity.ExpiresAt },
		"unknown commit":   func(s *controlStoreFixture) { s.err = errors.New("private backend details must not leak") },
	} {
		t.Run(name, func(t *testing.T) {
			service, store, bearer, request := controlFixture(t)
			mutate(store)
			reply, err := service.Execute(t.Context(), bearer, request)
			if !errors.Is(err, ErrUnavailable) || reply.Status != ports.AttachedWorkerExecutionDenied || reply.Capability != nil || reply.PlanDigest != "" {
				t.Fatalf("invalid store success leaked: %s err=%v", name, err)
			}
		})
	}
}
