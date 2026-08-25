package attachedworker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var attachedWorkerTestTime = time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)

func TestEnrollmentClaimIsSingleUseUnderConcurrency(t *testing.T) {
	service, store, clock := newAttachedWorkerTestService(t)
	grant := createAttachedWorkerTestEnrollment(t, service, clock)

	const contenders = 16
	requests := make([]ClaimRequest, contenders)
	for index := range requests {
		requests[index], _ = signedAttachedWorkerClaim(t, grant)
	}
	start := make(chan struct{})
	results := make(chan error, contenders)
	var wait sync.WaitGroup
	for contender := range contenders {
		wait.Add(1)
		go func(contender int) {
			defer wait.Done()
			<-start
			_, err := service.Claim(context.Background(), "tenant-a", "owner-a", requests[contender])
			results <- err
		}(contender)
	}
	close(start)
	wait.Wait()
	close(results)
	successes, consumed := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrEnrollmentConsumed):
			consumed++
		default:
			t.Fatalf("unexpected concurrent claim error: %v", err)
		}
	}
	if successes != 1 || consumed != contenders-1 {
		t.Fatalf("claim outcomes: success=%d consumed=%d", successes, consumed)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.workers) != 1 || len(store.audits) != 2 {
		t.Fatalf("persisted workers=%d audits=%d, want 1 and create+claim", len(store.workers), len(store.audits))
	}
}

func TestEnrollmentScopeReplayExpiryAudienceAndProof(t *testing.T) {
	t.Run("cross owner matrix fails closed", func(t *testing.T) {
		service, _, clock := newAttachedWorkerTestService(t)
		grant := createAttachedWorkerTestEnrollment(t, service, clock)
		request, _ := signedAttachedWorkerClaim(t, grant)
		for _, scope := range []struct {
			tenant domain.TenantID
			owner  domain.UserID
		}{
			{tenant: "tenant-a", owner: "owner-b"},
			{tenant: "tenant-b", owner: "owner-a"},
			{tenant: "tenant-b", owner: "owner-b"},
		} {
			if _, err := service.Claim(context.Background(), scope.tenant, scope.owner, request); !errors.Is(err, ErrEnrollmentDenied) {
				t.Fatalf("claim scope %s/%s error = %v", scope.tenant, scope.owner, err)
			}
			if _, err := service.Get(context.Background(), scope.tenant, scope.owner, grant.Enrollment.WorkerID); !errors.Is(err, ErrWorkerNotFound) {
				t.Fatalf("get scope %s/%s error = %v", scope.tenant, scope.owner, err)
			}
		}
	})

	t.Run("exact replay remains successful through retention", func(t *testing.T) {
		service, _, clock := newAttachedWorkerTestService(t)
		grant := createAttachedWorkerTestEnrollment(t, service, clock)
		request, _ := signedAttachedWorkerClaim(t, grant)
		if _, err := service.Claim(context.Background(), "tenant-a", "owner-a", request); err != nil {
			t.Fatal(err)
		}
		wrongSecret, err := ParseBootstrapSecret(bytes.Repeat([]byte{0x77}, bootstrapSecretBytes))
		if err != nil {
			t.Fatal(err)
		}
		wrongRequest := request
		wrongRequest.BootstrapSecret = wrongSecret
		if _, err := service.Claim(context.Background(), "tenant-a", "owner-a", wrongRequest); !errors.Is(err, ErrEnrollmentDenied) {
			t.Fatalf("wrong secret learned consumed state: %v", err)
		}
		clock.Set(grant.Enrollment.ExpiresAt.Add(time.Minute))
		if !clock.Now().Before(grant.Enrollment.RetainUntil) {
			t.Fatal("fixture moved beyond replay-retention window")
		}
		if _, err := service.Claim(context.Background(), "tenant-a", "owner-a", request); err != nil {
			t.Fatalf("retained replay error = %v", err)
		}
	})

	t.Run("expiry is exclusive at exact boundary", func(t *testing.T) {
		service, _, clock := newAttachedWorkerTestService(t)
		grant := createAttachedWorkerTestEnrollment(t, service, clock)
		request, _ := signedAttachedWorkerClaim(t, grant)
		clock.Set(grant.Enrollment.ExpiresAt)
		if _, err := service.Claim(context.Background(), "tenant-a", "owner-a", request); !errors.Is(err, ErrEnrollmentExpired) {
			t.Fatalf("boundary expiry error = %v", err)
		}
	})

	t.Run("store transaction clock is authoritative", func(t *testing.T) {
		service, store, serviceClock := newAttachedWorkerTestService(t)
		grant := createAttachedWorkerTestEnrollment(t, service, serviceClock)
		request, _ := signedAttachedWorkerClaim(t, grant)

		serviceClock.Set(grant.Enrollment.ExpiresAt.Add(time.Minute))
		store.clock = &mutableClock{now: grant.Enrollment.ExpiresAt.Add(-time.Microsecond)}
		if _, err := service.Claim(context.Background(), "tenant-a", "owner-a", request); err != nil {
			t.Fatalf("valid transaction-time claim rejected by skewed service clock: %v", err)
		}
	})

	t.Run("audience is exact and proof is audience bound", func(t *testing.T) {
		service, _, clock := newAttachedWorkerTestService(t)
		grant := createAttachedWorkerTestEnrollment(t, service, clock)
		request, _ := signedAttachedWorkerClaim(t, grant)
		request.Audience = "sessionless:attached-worker:other"
		if _, err := service.Claim(context.Background(), "tenant-a", "owner-a", request); !errors.Is(err, ErrEnrollmentDenied) {
			t.Fatalf("wrong audience error = %v", err)
		}
	})

	t.Run("generated key proof is mandatory", func(t *testing.T) {
		service, _, clock := newAttachedWorkerTestService(t)
		grant := createAttachedWorkerTestEnrollment(t, service, clock)
		request, _ := signedAttachedWorkerClaim(t, grant)
		request.Proof[0] ^= 0xff
		if _, err := service.Claim(context.Background(), "tenant-a", "owner-a", request); !errors.Is(err, ErrInvalidProof) {
			t.Fatalf("invalid proof error = %v", err)
		}
	})

	t.Run("wrong bootstrap is denied before store", func(t *testing.T) {
		service, _, clock := newAttachedWorkerTestService(t)
		grant := createAttachedWorkerTestEnrollment(t, service, clock)
		request, _ := signedAttachedWorkerClaim(t, grant)
		wrong, err := ParseBootstrapSecret(bytes.Repeat([]byte{0x99}, bootstrapSecretBytes))
		if err != nil {
			t.Fatal(err)
		}
		request.BootstrapSecret = wrong
		if _, err := service.Claim(context.Background(), "tenant-a", "owner-a", request); !errors.Is(err, ErrEnrollmentDenied) {
			t.Fatalf("wrong bootstrap error = %v", err)
		}
	})
}

func TestClaimProofTranscriptBindsExpectedEnrollmentRevision(t *testing.T) {
	service, _, clock := newAttachedWorkerTestService(t)
	grant := createAttachedWorkerTestEnrollment(t, service, clock)
	request, _ := signedAttachedWorkerClaim(t, grant)
	original, err := ClaimProofTranscript(grant.Enrollment, request.ExpectedEnrollmentRevision, request.IdentityPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	other, err := ClaimProofTranscript(grant.Enrollment, request.ExpectedEnrollmentRevision+1, request.IdentityPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(original, other) {
		t.Fatal("claim proof transcript did not bind expected enrollment revision")
	}
}

func TestEnrollmentCreationUsesShortTTLRetentionAndRedactedSecret(t *testing.T) {
	service, store, clock := newAttachedWorkerTestService(t)
	grant := createAttachedWorkerTestEnrollment(t, service, clock)
	if got, want := grant.Enrollment.RetainUntil, grant.Enrollment.ExpiresAt.Add(24*time.Hour); !got.Equal(want) {
		t.Fatalf("retain_until = %v, want %v", got, want)
	}
	raw := grant.Secret.Bytes()
	if grant.Enrollment.BootstrapDigest != domain.DigestWorkerBootstrap(raw) {
		t.Fatal("persisted digest does not match delivered bootstrap")
	}
	if fmt.Sprint(grant.Secret) != "[REDACTED]" || fmt.Sprintf("%#v", grant.Secret) != "[REDACTED]" {
		t.Fatalf("secret formatting leaked: %v / %#v", grant.Secret, grant.Secret)
	}
	encodedSecret, err := json.Marshal(grant.Secret)
	if err != nil || string(encodedSecret) != `"[REDACTED]"` {
		t.Fatalf("secret JSON = %s err=%v", encodedSecret, err)
	}
	store.mu.Lock()
	persisted := store.enrollments[enrollmentKey("tenant-a", "owner-a", grant.Enrollment.ID)]
	store.mu.Unlock()
	encodedEnrollment, err := json.Marshal(persisted)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encodedEnrollment, raw) || strings.Contains(string(encodedEnrollment), fmt.Sprintf("%x", raw)) {
		t.Fatalf("raw bootstrap leaked into persisted enrollment: %s", encodedEnrollment)
	}

	if _, err := service.CreateEnrollment(context.Background(), "tenant-a", "owner-a", CreateEnrollmentRequest{
		DisplayName: "too long", Audience: "sessionless:attached-worker:v1", ExpiresAt: clock.Now().Add(11 * time.Minute),
	}); err == nil {
		t.Fatal("enrollment beyond configured short TTL accepted")
	}
}

func TestAttachedWorkerAuditPaginationIsInclusiveFromRevisionZero(t *testing.T) {
	service, store, clock := newAttachedWorkerTestService(t)
	grant := createAttachedWorkerTestEnrollment(t, service, clock)
	request, _ := signedAttachedWorkerClaim(t, grant)
	if _, err := service.Claim(context.Background(), "tenant-a", "owner-a", request); err != nil {
		t.Fatal(err)
	}
	events, err := store.ListAttachedWorkerAuditEvents(
		context.Background(), "tenant-a", "owner-a", grant.Enrollment.WorkerID, 0, 10,
	)
	if err != nil || len(events) != 2 || events[0].WorkerRevision != 0 || events[1].WorkerRevision != 1 {
		t.Fatalf("audit page from 0 = %+v err=%v", events, err)
	}
	next, err := store.ListAttachedWorkerAuditEvents(
		context.Background(), "tenant-a", "owner-a", grant.Enrollment.WorkerID, 1, 10,
	)
	if err != nil || len(next) != 1 || next[0].WorkerRevision != 1 {
		t.Fatalf("audit page from 1 = %+v err=%v", next, err)
	}
}

func TestRenameRotateConnectionAndRevokeFences(t *testing.T) {
	service, _, clock := newAttachedWorkerTestService(t)
	grant := createAttachedWorkerTestEnrollment(t, service, clock)
	claim, oldPrivate := signedAttachedWorkerClaim(t, grant)
	worker, err := service.Claim(context.Background(), "tenant-a", "owner-a", claim)
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(time.Second)
	renamed, err := service.Rename(context.Background(), "tenant-a", "owner-a", RenameRequest{
		WorkerID: worker.ID, ExpectedRevision: worker.Revision, DisplayName: "desk worker",
	})
	if err != nil || renamed.Revision != 2 || renamed.EnrollmentGeneration != 1 || renamed.ConnectionGeneration != 0 {
		t.Fatalf("renamed = %+v err=%v", renamed, err)
	}
	if _, err := service.Claim(context.Background(), "tenant-a", "owner-a", claim); !errors.Is(err, ErrEnrollmentConsumed) {
		t.Fatalf("claim replay after rename error = %v, want consumed", err)
	}

	newPublic, newPrivate, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	rotationTranscript, err := RotationProofTranscript(renamed, renamed.Revision, newPublic)
	if err != nil {
		t.Fatal(err)
	}
	validCurrentProof := ed25519.Sign(oldPrivate, rotationTranscript)
	validNewProof := ed25519.Sign(newPrivate, rotationTranscript)
	for _, invalid := range []struct {
		name         string
		currentProof []byte
		newProof     []byte
	}{
		{name: "missing current proof", newProof: validNewProof},
		{name: "missing new proof", currentProof: validCurrentProof},
		{name: "new proof signed by old key", currentProof: validCurrentProof, newProof: validCurrentProof},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			if _, err := service.RotateIdentity(context.Background(), "tenant-a", "owner-a", RotateIdentityRequest{
				WorkerID: renamed.ID, ExpectedRevision: renamed.Revision, NewPublicKey: newPublic,
				CurrentProof: invalid.currentProof, NewProof: invalid.newProof,
			}); !errors.Is(err, ErrInvalidProof) {
				t.Fatalf("invalid dual proof error = %v", err)
			}
		})
	}
	clock.Advance(time.Second)
	rotated, err := service.RotateIdentity(context.Background(), "tenant-a", "owner-a", RotateIdentityRequest{
		WorkerID: renamed.ID, ExpectedRevision: renamed.Revision, NewPublicKey: newPublic,
		CurrentProof: validCurrentProof, NewProof: validNewProof,
	})
	if err != nil || rotated.Revision != 3 || rotated.EnrollmentGeneration != 2 || !bytes.Equal(rotated.IdentityPublicKey, newPublic) {
		t.Fatalf("rotated = %+v err=%v", rotated, err)
	}
	if _, err := service.RotateIdentity(context.Background(), "tenant-a", "owner-a", RotateIdentityRequest{
		WorkerID: renamed.ID, ExpectedRevision: renamed.Revision, NewPublicKey: newPublic,
		CurrentProof: validCurrentProof, NewProof: validNewProof,
	}); !errors.Is(err, ErrWorkerConflict) {
		t.Fatalf("rotation replay error = %v", err)
	}

	thirdPublic, thirdPrivate, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	thirdTranscript, err := RotationProofTranscript(rotated, rotated.Revision, thirdPublic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RotateIdentity(context.Background(), "tenant-a", "owner-a", RotateIdentityRequest{
		WorkerID: rotated.ID, ExpectedRevision: rotated.Revision, NewPublicKey: thirdPublic,
		CurrentProof: ed25519.Sign(oldPrivate, thirdTranscript), NewProof: ed25519.Sign(thirdPrivate, thirdTranscript),
	}); !errors.Is(err, ErrInvalidProof) {
		t.Fatalf("old identity authorized another rotation: %v", err)
	}
	if !ed25519.Verify(newPublic, thirdTranscript, ed25519.Sign(newPrivate, thirdTranscript)) {
		t.Fatal("new identity fixture cannot prove possession")
	}

	clock.Advance(time.Second)
	connected, err := service.AdvanceConnectionGeneration(context.Background(), "tenant-a", "owner-a", WorkerRevisionRequest{
		WorkerID: rotated.ID, ExpectedRevision: rotated.Revision,
	})
	if err != nil || connected.Revision != 4 || connected.EnrollmentGeneration != 2 || connected.ConnectionGeneration != 1 {
		t.Fatalf("connected = %+v err=%v", connected, err)
	}

	clock.Advance(time.Second)
	revoked, err := service.Revoke(context.Background(), "tenant-a", "owner-a", WorkerRevisionRequest{
		WorkerID: connected.ID, ExpectedRevision: connected.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if revoked.DesiredState != domain.AttachedWorkerDesiredRevoked ||
		revoked.ObservedState != connected.ObservedState || revoked.Revision != 5 ||
		revoked.EnrollmentGeneration != 3 || revoked.ConnectionGeneration != 2 || revoked.RevokedAt.IsZero() {
		t.Fatalf("deny-first revoked worker = %+v", revoked)
	}
	if _, err := service.Rename(context.Background(), "tenant-a", "owner-a", RenameRequest{
		WorkerID: revoked.ID, ExpectedRevision: revoked.Revision, DisplayName: "must fail",
	}); !errors.Is(err, ErrWorkerRevoked) {
		t.Fatalf("post-revoke rename error = %v", err)
	}
	if _, err := service.AdvanceConnectionGeneration(context.Background(), "tenant-b", "owner-a", WorkerRevisionRequest{
		WorkerID: revoked.ID, ExpectedRevision: revoked.Revision,
	}); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("cross-tenant connection advance error = %v", err)
	}
	if _, err := service.Revoke(context.Background(), "tenant-a", "owner-b", WorkerRevisionRequest{
		WorkerID: revoked.ID, ExpectedRevision: revoked.Revision,
	}); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("cross-owner revoke error = %v", err)
	}
}

func TestServiceRequestsCannotSupplyAuthoritativeOwnerScope(t *testing.T) {
	for _, request := range []any{
		CreateEnrollmentRequest{}, ClaimRequest{}, RenameRequest{}, RotateIdentityRequest{}, WorkerRevisionRequest{},
	} {
		requestType := reflect.TypeOf(request)
		if _, found := requestType.FieldByName("TenantID"); found {
			t.Fatalf("%s must not carry tenant authority", requestType.Name())
		}
		if _, found := requestType.FieldByName("OwnerUserID"); found {
			t.Fatalf("%s must not carry owner authority", requestType.Name())
		}
	}
}

func TestPublicBackendErrorsAreSanitized(t *testing.T) {
	service, store, clock := newAttachedWorkerTestService(t)
	grant := createAttachedWorkerTestEnrollment(t, service, clock)
	request, _ := signedAttachedWorkerClaim(t, grant)
	store.backendErr = errors.New("ydb row leaked bootstrap=super-secret")
	_, err := service.Claim(context.Background(), "tenant-a", "owner-a", request)
	if !errors.Is(err, ErrBackend) || strings.Contains(err.Error(), "ydb") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("public error was not sanitized: %v", err)
	}
}

func newAttachedWorkerTestService(t *testing.T) (*Service, *memoryAttachedWorkerStore, *mutableClock) {
	t.Helper()
	clock := &mutableClock{now: attachedWorkerTestTime}
	store := newMemoryAttachedWorkerStore(clock)
	service, err := New(Config{
		Clock: clock, IDs: &sequenceAttachedWorkerIDs{},
		Random:           bytes.NewReader(bytes.Repeat([]byte{0x42}, 4096)),
		MaxEnrollmentTTL: 10 * time.Minute, EnrollmentRetention: 24 * time.Hour,
	}, store)
	if err != nil {
		t.Fatal(err)
	}
	return service, store, clock
}

func createAttachedWorkerTestEnrollment(t *testing.T, service *Service, clock *mutableClock) EnrollmentGrant {
	t.Helper()
	grant, err := service.CreateEnrollment(context.Background(), "tenant-a", "owner-a", CreateEnrollmentRequest{
		DisplayName: "laptop", Audience: "sessionless:attached-worker:v1", ExpiresAt: clock.Now().Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	return grant
}

func signedAttachedWorkerClaim(t *testing.T, grant EnrollmentGrant) (ClaimRequest, ed25519.PrivateKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	transcript, err := ClaimProofTranscript(grant.Enrollment, grant.Enrollment.Revision, publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return ClaimRequest{
		EnrollmentID: grant.Enrollment.ID, ExpectedEnrollmentRevision: grant.Enrollment.Revision,
		Audience:        grant.Enrollment.Audience,
		BootstrapSecret: grant.Secret, IdentityPublicKey: publicKey, Proof: ed25519.Sign(privateKey, transcript),
	}, privateKey
}

type mutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *mutableClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *mutableClock) Set(value time.Time) {
	clock.mu.Lock()
	clock.now = value
	clock.mu.Unlock()
}

func (clock *mutableClock) Advance(delta time.Duration) { clock.Set(clock.Now().Add(delta)) }

type sequenceAttachedWorkerIDs struct {
	mu     sync.Mutex
	worker int
	enroll int
}

func (ids *sequenceAttachedWorkerIDs) NewID(_ context.Context, kind ports.IDKind) (string, error) {
	ids.mu.Lock()
	defer ids.mu.Unlock()
	switch kind {
	case ports.IDAttachedWorker:
		ids.worker++
		return fmt.Sprintf("wrk_test_%d", ids.worker), nil
	case ports.IDAttachedWorkerEnrollment:
		ids.enroll++
		return fmt.Sprintf("wen_test_%d", ids.enroll), nil
	default:
		return "", errors.New("unsupported test ID kind")
	}
}

type memoryAttachedWorkerStore struct {
	mu          sync.Mutex
	enrollments map[string]domain.AttachedWorkerEnrollment
	workers     map[string]domain.AttachedWorker
	audits      []domain.AttachedWorkerAuditEvent
	backendErr  error
	clock       *mutableClock
}

func newMemoryAttachedWorkerStore(clock *mutableClock) *memoryAttachedWorkerStore {
	return &memoryAttachedWorkerStore{
		enrollments: make(map[string]domain.AttachedWorkerEnrollment),
		workers:     make(map[string]domain.AttachedWorker),
		clock:       clock,
	}
}

func (store *memoryAttachedWorkerStore) CreateAttachedWorkerEnrollment(_ context.Context, enrollment domain.AttachedWorkerEnrollment, audit domain.AttachedWorkerAuditEvent) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.backendErr != nil {
		return store.backendErr
	}
	if enrollment.Validate() != nil || audit.Validate() != nil {
		return errors.New("invalid enrollment mutation")
	}
	key := enrollmentKey(enrollment.TenantID, enrollment.OwnerUserID, enrollment.ID)
	if _, exists := store.enrollments[key]; exists {
		return errors.New("duplicate enrollment")
	}
	store.enrollments[key] = enrollment
	store.audits = append(store.audits, audit)
	return nil
}

func (store *memoryAttachedWorkerStore) LoadAttachedWorkerEnrollment(_ context.Context, tenantID domain.TenantID, ownerUserID domain.UserID, enrollmentID domain.AttachedWorkerEnrollmentID) (domain.AttachedWorkerEnrollment, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.backendErr != nil {
		return domain.AttachedWorkerEnrollment{}, false, store.backendErr
	}
	enrollment, found := store.enrollments[enrollmentKey(tenantID, ownerUserID, enrollmentID)]
	return enrollment, found, nil
}

func (store *memoryAttachedWorkerStore) ClaimAttachedWorkerEnrollment(_ context.Context, mutation ports.AttachedWorkerClaimMutation) (ports.AttachedWorkerClaimResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.backendErr != nil {
		return ports.AttachedWorkerClaimResult{}, store.backendErr
	}
	key := enrollmentKey(mutation.TenantID, mutation.OwnerUserID, mutation.EnrollmentID)
	enrollment, found := store.enrollments[key]
	if !found || mutation.PresentedAudience != enrollment.Audience || !equalDigest(mutation.PresentedDigest, enrollment.BootstrapDigest) {
		return ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerDenied}, nil
	}
	if !enrollment.ConsumedAt.IsZero() {
		worker := store.workers[attachedWorkerKey(mutation.TenantID, mutation.OwnerUserID, enrollment.WorkerID)]
		if enrollment.Revision == mutation.ExpectedEnrollmentRevision+1 && worker.Revision == 1 &&
			worker.DisplayName == enrollment.DisplayName && bytes.Equal(worker.IdentityPublicKey, mutation.IdentityPublicKey) &&
			worker.EnrollmentGeneration == 1 && worker.ConnectionGeneration == 0 &&
			worker.DesiredState == domain.AttachedWorkerDesiredActive && worker.ObservedState == domain.AttachedWorkerObservedOffline &&
			worker.CreatedAt.Equal(enrollment.ConsumedAt) && worker.UpdatedAt.Equal(enrollment.ConsumedAt) {
			return ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerClaimed, Worker: cloneWorker(worker)}, nil
		}
		return ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerConsumed}, nil
	}
	at := canonicalPersistenceTime(store.clock.Now())
	if !at.Before(enrollment.ExpiresAt) {
		return ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerExpired}, nil
	}
	if mutation.ExpectedEnrollmentRevision != enrollment.Revision {
		return ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerConflict}, nil
	}
	if len(mutation.IdentityPublicKey) != ed25519.PublicKeySize {
		return ports.AttachedWorkerClaimResult{}, errors.New("invalid claim mutation")
	}
	worker := domain.AttachedWorker{
		TenantID: mutation.TenantID, OwnerUserID: mutation.OwnerUserID, ID: enrollment.WorkerID,
		DisplayName: enrollment.DisplayName, IdentityPublicKey: cloneBytes(mutation.IdentityPublicKey),
		EnrollmentGeneration: 1, DesiredState: domain.AttachedWorkerDesiredActive,
		ObservedState: domain.AttachedWorkerObservedOffline, Revision: 1, CreatedAt: at, UpdatedAt: at,
	}
	audit := auditForWorker(worker, domain.AttachedWorkerAuditEnrollmentClaimed, enrollment.ID, at)
	workerKey := attachedWorkerKey(mutation.TenantID, mutation.OwnerUserID, worker.ID)
	if _, exists := store.workers[workerKey]; exists {
		return ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerConflict}, nil
	}
	enrollment.ConsumedAt = at
	enrollment.Revision++
	store.enrollments[key] = enrollment
	store.workers[workerKey] = cloneWorker(worker)
	store.audits = append(store.audits, audit)
	return ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerClaimed, Worker: cloneWorker(worker)}, nil
}

func (store *memoryAttachedWorkerStore) LoadAttachedWorker(_ context.Context, tenantID domain.TenantID, ownerUserID domain.UserID, workerID domain.AttachedWorkerID) (domain.AttachedWorker, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.backendErr != nil {
		return domain.AttachedWorker{}, false, store.backendErr
	}
	worker, found := store.workers[attachedWorkerKey(tenantID, ownerUserID, workerID)]
	return cloneWorker(worker), found, nil
}

func (store *memoryAttachedWorkerStore) ListAttachedWorkers(_ context.Context, tenantID domain.TenantID, ownerUserID domain.UserID, after domain.AttachedWorkerID, limit uint64) ([]domain.AttachedWorker, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.backendErr != nil {
		return nil, store.backendErr
	}
	result := make([]domain.AttachedWorker, 0, limit)
	for _, worker := range store.workers {
		if worker.TenantID == tenantID && worker.OwnerUserID == ownerUserID && worker.ID > after {
			result = append(result, cloneWorker(worker))
		}
	}
	sort.Slice(result, func(left, right int) bool { return result[left].ID < result[right].ID })
	if uint64(len(result)) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (store *memoryAttachedWorkerStore) CompareAndSwapAttachedWorker(_ context.Context, mutation ports.AttachedWorkerCASMutation) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.backendErr != nil {
		return false, store.backendErr
	}
	key := attachedWorkerKey(mutation.Next.TenantID, mutation.Next.OwnerUserID, mutation.Next.ID)
	current, found := store.workers[key]
	if !found || current.Revision != mutation.ExpectedRevision {
		return false, nil
	}
	if mutation.Next.Validate() != nil || mutation.Audit.Validate() != nil || mutation.Next.Revision != current.Revision+1 {
		return false, errors.New("invalid attached worker CAS")
	}
	store.workers[key] = cloneWorker(mutation.Next)
	store.audits = append(store.audits, mutation.Audit)
	return true, nil
}

func (store *memoryAttachedWorkerStore) RevokeAttachedWorker(_ context.Context, mutation ports.AttachedWorkerRevokeMutation) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.backendErr != nil {
		return false, store.backendErr
	}
	key := attachedWorkerKey(mutation.TenantID, mutation.OwnerUserID, mutation.WorkerID)
	current, found := store.workers[key]
	if !found || current.Revision != mutation.ExpectedRevision {
		return false, nil
	}
	if mutation.Next.Validate() != nil || mutation.Audit.Validate() != nil ||
		mutation.Next.DesiredState != domain.AttachedWorkerDesiredRevoked || mutation.Next.ObservedState != current.ObservedState ||
		mutation.Next.EnrollmentGeneration != current.EnrollmentGeneration+1 ||
		mutation.Next.ConnectionGeneration != current.ConnectionGeneration+1 || mutation.Next.Revision != current.Revision+1 {
		return false, errors.New("invalid attached worker revoke")
	}
	store.workers[key] = cloneWorker(mutation.Next)
	store.audits = append(store.audits, mutation.Audit)
	return true, nil
}

func (store *memoryAttachedWorkerStore) ListAttachedWorkerAuditEvents(_ context.Context, tenantID domain.TenantID, ownerUserID domain.UserID, workerID domain.AttachedWorkerID, fromWorkerRevision uint64, limit uint64) ([]domain.AttachedWorkerAuditEvent, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.backendErr != nil {
		return nil, store.backendErr
	}
	result := make([]domain.AttachedWorkerAuditEvent, 0, limit)
	for _, event := range store.audits {
		if event.TenantID == tenantID && event.OwnerUserID == ownerUserID && event.WorkerID == workerID && event.WorkerRevision >= fromWorkerRevision {
			result = append(result, event)
		}
	}
	sort.Slice(result, func(left, right int) bool { return result[left].WorkerRevision < result[right].WorkerRevision })
	if uint64(len(result)) > limit {
		result = result[:limit]
	}
	return result, nil
}

func enrollmentKey(tenantID domain.TenantID, ownerUserID domain.UserID, enrollmentID domain.AttachedWorkerEnrollmentID) string {
	return string(tenantID) + "\x00" + string(ownerUserID) + "\x00" + string(enrollmentID)
}

func attachedWorkerKey(tenantID domain.TenantID, ownerUserID domain.UserID, workerID domain.AttachedWorkerID) string {
	return string(tenantID) + "\x00" + string(ownerUserID) + "\x00" + string(workerID)
}

var _ ports.AttachedWorkerStore = (*memoryAttachedWorkerStore)(nil)
